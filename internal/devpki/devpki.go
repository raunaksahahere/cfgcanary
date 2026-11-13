// Package devpki creates development certificates, signing keys and an RBAC
// policy for local use and tests. It is not a production CA: keys are written
// unencrypted to a local directory that must stay out of version control.
package devpki

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/raunaksahahere/cfgcanary/internal/sign"
)

// CA is a certificate authority.
type CA struct {
	Cert    *x509.Certificate
	Key     *ecdsa.PrivateKey
	CertPEM []byte
}

func serial() *big.Int {
	n, _ := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	return n
}

// NewCA creates a self-signed CA valid for ttl.
func NewCA(name string, ttl time.Duration) (*CA, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	tmpl := &x509.Certificate{
		SerialNumber: serial(), Subject: pkix.Name{CommonName: name, Organization: []string{"cfgcanary development"}},
		NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(ttl),
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign, BasicConstraintsValid: true, IsCA: true, MaxPathLenZero: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return nil, err
	}
	c, _ := x509.ParseCertificate(der)
	return &CA{Cert: c, Key: key, CertPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}, nil
}

// LoadCA reads ca.pem and ca-key.pem from dir.
func LoadCA(dir string) (*CA, error) {
	cp, err := os.ReadFile(filepath.Join(dir, "ca.pem"))
	if err != nil {
		return nil, err
	}
	kp, err := os.ReadFile(filepath.Join(dir, "ca-key.pem"))
	if err != nil {
		return nil, err
	}
	cb, _ := pem.Decode(cp)
	kb, _ := pem.Decode(kp)
	if cb == nil || kb == nil {
		return nil, errors.New("devpki: unreadable CA files")
	}
	c, err := x509.ParseCertificate(cb.Bytes)
	if err != nil {
		return nil, err
	}
	k, err := x509.ParseECPrivateKey(kb.Bytes)
	if err != nil {
		return nil, err
	}
	return &CA{Cert: c, Key: k, CertPEM: cp}, nil
}

// Spec describes a leaf certificate.
type Spec struct {
	CommonName string
	URI        string // SPIFFE ID
	DNS        []string
	IPs        []net.IP
	Server     bool
	Client     bool
	NotBefore  time.Time
	NotAfter   time.Time
}

// Issue returns PEM certificate and key for spec.
func (ca *CA) Issue(s Spec) (certPEM, keyPEM []byte, err error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	if s.NotBefore.IsZero() {
		s.NotBefore = time.Now().Add(-time.Minute)
	}
	if s.NotAfter.IsZero() {
		s.NotAfter = time.Now().Add(90 * 24 * time.Hour)
	}
	tmpl := &x509.Certificate{
		SerialNumber: serial(), Subject: pkix.Name{CommonName: s.CommonName}, NotBefore: s.NotBefore, NotAfter: s.NotAfter,
		KeyUsage: x509.KeyUsageDigitalSignature, DNSNames: s.DNS, IPAddresses: s.IPs,
	}
	if s.Server {
		tmpl.ExtKeyUsage = append(tmpl.ExtKeyUsage, x509.ExtKeyUsageServerAuth)
	}
	if s.Client {
		tmpl.ExtKeyUsage = append(tmpl.ExtKeyUsage, x509.ExtKeyUsageClientAuth)
	}
	if s.URI != "" {
		u, err := url.Parse(s.URI)
		if err != nil {
			return nil, nil, err
		}
		tmpl.URIs = []*url.URL{u}
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.Cert, &key.PublicKey, ca.Key)
	if err != nil {
		return nil, nil, err
	}
	kd, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, nil, err
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kd}), nil
}

// WriteIssued issues a certificate and writes <base>.pem and <base>-key.pem.
func (ca *CA) WriteIssued(dir, base string, s Spec) error {
	c, k, err := ca.Issue(s)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, base+".pem"), c, 0o644); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, base+"-key.pem"), k, 0o600)
}

// Options for Bootstrap.
type Options struct {
	TrustDomain string
	// ServerNames are DNS names put in the server certificate.
	ServerNames []string
	// Users maps user names to RBAC roles.
	Users map[string][]string
	// Services maps service names to RBAC roles.
	Services map[string][]string
}

// DefaultOptions is the layout used by the Compose demo and tests.
func DefaultOptions() Options {
	return Options{
		TrustDomain: "cfgcanary.local",
		ServerNames: []string{"localhost", "cfgcanary", "cfgcanary-server", "server", "host.docker.internal"},
		Users: map[string][]string{
			"admin": {"admin"}, "operator": {"releaser"}, "viewer": {"viewer"}, "oncall": {"operator", "breakglass"},
		},
		Services: map[string][]string{"k8s-controller": {"releaser"}},
	}
}

func exists(p string) bool { _, err := os.Stat(p); return err == nil }

// Bootstrap creates everything a local deployment needs in dir. Existing files
// are kept, so it is safe to run repeatedly.
func Bootstrap(dir string, o Options) ([]string, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	var created []string
	var ca *CA
	var err error
	if exists(filepath.Join(dir, "ca.pem")) {
		if ca, err = LoadCA(dir); err != nil {
			return nil, err
		}
	} else {
		if ca, err = NewCA("cfgcanary development CA", 5*365*24*time.Hour); err != nil {
			return nil, err
		}
		kd, _ := x509.MarshalECPrivateKey(ca.Key)
		if err := os.WriteFile(filepath.Join(dir, "ca.pem"), ca.CertPEM, 0o644); err != nil {
			return nil, err
		}
		if err := os.WriteFile(filepath.Join(dir, "ca-key.pem"), pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kd}), 0o600); err != nil {
			return nil, err
		}
		created = append(created, "ca.pem", "ca-key.pem")
	}
	issue := func(sub, base string, s Spec) error {
		if exists(filepath.Join(dir, sub, base+".pem")) {
			return nil
		}
		created = append(created, filepath.Join(sub, base+".pem"))
		return ca.WriteIssued(filepath.Join(dir, sub), base, s)
	}
	if err := issue("", "server", Spec{CommonName: "cfgcanary server", URI: "spiffe://" + o.TrustDomain + "/service/cfgcanary-server",
		DNS: o.ServerNames, IPs: []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback}, Server: true, Client: true}); err != nil {
		return nil, err
	}
	rbac := map[string][]string{}
	for name, roles := range o.Users {
		uri := "spiffe://" + o.TrustDomain + "/user/" + name
		rbac[uri] = roles
		if err := issue("users", name, Spec{CommonName: name, URI: uri, Client: true}); err != nil {
			return nil, err
		}
	}
	for name, roles := range o.Services {
		uri := "spiffe://" + o.TrustDomain + "/service/" + name
		rbac[uri] = roles
		if err := issue("services", name, Spec{CommonName: name, URI: uri, Client: true}); err != nil {
			return nil, err
		}
	}
	if !exists(filepath.Join(dir, "rbac.yaml")) {
		b, _ := json.MarshalIndent(map[string]any{"subjects": rbac}, "", "  ")
		if err := os.WriteFile(filepath.Join(dir, "rbac.yaml"), b, 0o644); err != nil {
			return nil, err
		}
		created = append(created, "rbac.yaml")
	}
	var signers []sign.TrustedKey
	for _, p := range []sign.Purpose{sign.PurposeArtifact, sign.PurposeDelivery} {
		path := filepath.Join(dir, string(p)+".key")
		if exists(path) {
			key, err := sign.LoadKey(path)
			if err != nil {
				return nil, err
			}
			signers = append(signers, sign.Trusted(sign.NewSigner(p, key).Public(), p))
			continue
		}
		key, err := sign.GenerateKey()
		if err != nil {
			return nil, err
		}
		if err := sign.WriteKey(path, key); err != nil {
			return nil, err
		}
		created = append(created, string(p)+".key")
		signers = append(signers, sign.Trusted(sign.NewSigner(p, key).Public(), p))
	}
	tb, _ := json.MarshalIndent(map[string]any{"keys": signers}, "", "  ")
	if err := os.WriteFile(filepath.Join(dir, "trust.json"), tb, 0o644); err != nil {
		return nil, err
	}
	return created, nil
}

// AgentCert issues a short-lived agent certificate (used by the simulator).
func (ca *CA) AgentCert(trustDomain, env, instance string, ttl time.Duration) ([]byte, []byte, error) {
	return ca.Issue(Spec{CommonName: instance, URI: fmt.Sprintf("spiffe://%s/env/%s/agent/%s", trustDomain, env, instance),
		Client: true, NotAfter: time.Now().Add(ttl)})
}
