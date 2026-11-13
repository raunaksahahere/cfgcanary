package auth

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// Reloader serves a certificate and CA bundle that can be replaced on disk
// without a restart (certificate rotation): files are re-read when their
// modification time changes, at most once per second.
type Reloader struct {
	certFile, keyFile, caFile string

	mu      sync.Mutex
	cert    *tls.Certificate
	pool    *x509.CertPool
	stamp   string
	checked time.Time
}

// NewReloader loads the files once and returns a reloader.
func NewReloader(certFile, keyFile, caFile string) (*Reloader, error) {
	r := &Reloader{certFile: certFile, keyFile: keyFile, caFile: caFile}
	if err := r.load(); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *Reloader) stampNow() string {
	s := ""
	for _, f := range []string{r.certFile, r.keyFile, r.caFile} {
		if st, err := os.Stat(f); err == nil {
			s += fmt.Sprint(st.ModTime().UnixNano(), st.Size(), ";")
		}
	}
	return s
}

func (r *Reloader) load() error {
	cert, err := tls.LoadX509KeyPair(r.certFile, r.keyFile)
	if err != nil {
		return fmt.Errorf("loading certificate: %w", err)
	}
	pem, err := os.ReadFile(r.caFile)
	if err != nil {
		return fmt.Errorf("loading CA bundle: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return errors.New("CA bundle contains no certificates")
	}
	r.mu.Lock()
	r.cert, r.pool, r.stamp = &cert, pool, r.stampNow()
	r.mu.Unlock()
	return nil
}

func (r *Reloader) current() (*tls.Certificate, *x509.CertPool) {
	r.mu.Lock()
	if time.Since(r.checked) > time.Second {
		r.checked = time.Now()
		if s := r.stampNow(); s != r.stamp {
			r.mu.Unlock()
			_ = r.load() // keep the old material if the new files are broken
			r.mu.Lock()
		}
	}
	defer r.mu.Unlock()
	return r.cert, r.pool
}

// ServerTLS returns a TLS config that requires and verifies client
// certificates against the CA bundle.
func (r *Reloader) ServerTLS() *tls.Config {
	return &tls.Config{
		MinVersion: tls.VersionTLS12,
		ClientAuth: tls.RequireAnyClientCert, // verified in VerifyConnection against the current pool
		GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
			c, _ := r.current()
			return c, nil
		},
		VerifyConnection: func(cs tls.ConnectionState) error {
			if len(cs.PeerCertificates) == 0 {
				return errors.New("client certificate required")
			}
			_, pool := r.current()
			inter := x509.NewCertPool()
			for _, c := range cs.PeerCertificates[1:] {
				inter.AddCert(c)
			}
			_, err := cs.PeerCertificates[0].Verify(x509.VerifyOptions{
				Roots: pool, Intermediates: inter, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
			})
			return err
		},
	}
}

// ClientTLS returns a TLS config that presents the certificate and verifies
// the server's certificate and host name against the CA bundle.
func (r *Reloader) ClientTLS(serverName string) *tls.Config {
	return &tls.Config{
		MinVersion: tls.VersionTLS12,
		ServerName: serverName,
		GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			c, _ := r.current()
			return c, nil
		},
		// Verify with the current pool so CA rotation needs no restart.
		InsecureSkipVerify: true, //nolint:gosec // verification is done in VerifyConnection below
		VerifyConnection: func(cs tls.ConnectionState) error {
			if len(cs.PeerCertificates) == 0 {
				return errors.New("server presented no certificate")
			}
			_, pool := r.current()
			inter := x509.NewCertPool()
			for _, c := range cs.PeerCertificates[1:] {
				inter.AddCert(c)
			}
			_, err := cs.PeerCertificates[0].Verify(x509.VerifyOptions{
				Roots: pool, Intermediates: inter, DNSName: serverName, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
			})
			return err
		},
	}
}

func identityFromTLS(state tls.ConnectionState, trustDomain string) (Identity, error) {
	if len(state.PeerCertificates) == 0 {
		return Identity{}, ErrNoIdentity
	}
	return Parse(state.PeerCertificates[0], trustDomain)
}

func grpcIdentity(ctx context.Context, trustDomain string) (context.Context, error) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "no peer")
	}
	ti, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "mTLS required")
	}
	id, err := identityFromTLS(ti.State, trustDomain)
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, err.Error())
	}
	return WithIdentity(ctx, id), nil
}

// UnaryInterceptor attaches the caller's identity to the context.
func UnaryInterceptor(trustDomain string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		ctx, err := grpcIdentity(ctx, trustDomain)
		if err != nil {
			return nil, err
		}
		return h(ctx, req)
	}
}

type wrapped struct {
	grpc.ServerStream
	ctx context.Context
}

func (w wrapped) Context() context.Context { return w.ctx }

// StreamInterceptor attaches the caller's identity to the stream context.
func StreamInterceptor(trustDomain string) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, h grpc.StreamHandler) error {
		ctx, err := grpcIdentity(ss.Context(), trustDomain)
		if err != nil {
			return err
		}
		return h(srv, wrapped{ss, ctx})
	}
}

// HTTPMiddleware attaches the caller's identity for the REST gateway.
func HTTPMiddleware(trustDomain string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.TLS == nil {
			http.Error(w, `{"code":16,"message":"mTLS required"}`, http.StatusUnauthorized)
			return
		}
		id, err := identityFromTLS(*r.TLS, trustDomain)
		if err != nil {
			http.Error(w, `{"code":16,"message":"client certificate has no cfgcanary identity"}`, http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r.WithContext(WithIdentity(r.Context(), id)))
	})
}
