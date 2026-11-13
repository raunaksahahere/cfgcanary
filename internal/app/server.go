// Package app assembles the control plane from options. The CLI and the
// integration tests start it through the same code path.
package app

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/raunaksahahere/cfgcanary/internal/auth"
	"github.com/raunaksahahere/cfgcanary/internal/blob"
	"github.com/raunaksahahere/cfgcanary/internal/buildinfo"
	"github.com/raunaksahahere/cfgcanary/internal/controller"
	"github.com/raunaksahahere/cfgcanary/internal/gate"
	"github.com/raunaksahahere/cfgcanary/internal/server"
	"github.com/raunaksahahere/cfgcanary/internal/sign"
	"github.com/raunaksahahere/cfgcanary/internal/store"
	"github.com/raunaksahahere/cfgcanary/internal/validate"
)

// ServerOptions configures a control-plane replica.
type ServerOptions struct {
	GRPCListen, RESTListen, MetricsListen string
	// Listeners override the addresses (tests use :0 listeners).
	GRPCListener, RESTListener, MetricsListener net.Listener

	DBURL      string
	DBMaxConns int32
	S3         blob.Config

	TLSCert, TLSKey, TLSCA   string
	ArtifactKey, DeliveryKey string
	RBACFile                 string
	TrustDomain              string
	PrometheusURL            string

	// Controller runs the release controller campaign in this process.
	Controller     bool
	Holder         string
	ControllerTick time.Duration
	GCInterval     time.Duration
	Server         server.Config
	// ValidatorConcurrency bounds concurrent WASM validations.
	ValidatorConcurrency int
	ShutdownTimeout      time.Duration
	Logger               *slog.Logger
	// Ready is called once everything is listening.
	Ready func(*Running)
}

// Running describes a started replica.
type Running struct {
	GRPCAddr, RESTAddr, MetricsAddr string
	Store                           *store.Store
	Blobs                           *blob.Store
	Server                          *server.Server
	Controller                      *controller.Controller
	Authorizer                      *auth.Authorizer
	Registry                        *prometheus.Registry
}

func listen(l net.Listener, addr string) (net.Listener, error) {
	if l != nil {
		return l, nil
	}
	if addr == "" {
		return nil, nil
	}
	return net.Listen("tcp", addr)
}

// RunServer runs a replica until ctx ends, then shuts down gracefully.
func RunServer(ctx context.Context, o ServerOptions) error {
	log := o.Logger
	if log == nil {
		log = slog.New(slog.NewJSONHandler(os.Stderr, nil))
	}
	if o.TrustDomain == "" {
		o.TrustDomain = "cfgcanary.local"
	}
	o.Server.TrustDomain = o.TrustDomain
	if o.ShutdownTimeout <= 0 {
		o.ShutdownTimeout = 15 * time.Second
	}
	if o.Holder == "" {
		h, _ := os.Hostname()
		o.Holder = fmt.Sprintf("%s/%d", h, os.Getpid())
	}

	tlsMat, err := auth.NewReloader(o.TLSCert, o.TLSKey, o.TLSCA)
	if err != nil {
		return fmt.Errorf("server TLS: %w", err)
	}
	policy, err := auth.LoadPolicy(o.RBACFile)
	if err != nil {
		return fmt.Errorf("RBAC policy: %w", err)
	}
	authz := auth.NewAuthorizer(policy)
	ak, err := sign.LoadKey(o.ArtifactKey)
	if err != nil {
		return fmt.Errorf("artifact signing key: %w", err)
	}
	dk, err := sign.LoadKey(o.DeliveryKey)
	if err != nil {
		return fmt.Errorf("delivery signing key: %w", err)
	}

	st, err := store.Open(ctx, o.DBURL, o.DBMaxConns)
	if err != nil {
		return err
	}
	defer st.Close()
	mctx, cancel := context.WithTimeout(ctx, time.Minute)
	err = st.Migrate(mctx)
	cancel()
	if err != nil {
		return fmt.Errorf("migrations: %w", err)
	}
	blobs, err := blob.New(o.S3)
	if err != nil {
		return err
	}
	bctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	err = blobs.EnsureBucket(bctx)
	cancel()
	if err != nil {
		return err
	}

	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	sb := validate.NewSandbox(max(o.ValidatorConcurrency, 2))
	defer sb.Close(context.Background())

	var ctrl *controller.Controller
	if o.Controller {
		var prom *gate.Prometheus
		if o.PrometheusURL != "" {
			prom = &gate.Prometheus{URL: o.PrometheusURL}
		}
		ctrl = controller.New(st, controller.Config{Holder: o.Holder, Tick: o.ControllerTick, GCInterval: o.GCInterval,
			DeleteObject: blobs.Delete, Prometheus: prom, Logger: log, Registerer: reg})
	}
	version, commit, date, _ := buildinfo.Info()
	srv, err := server.New(o.Server, server.Deps{
		Store: st, Blobs: blobs, Validator: validate.New(o.Server.HardMaxBytes, sb),
		ArtifactSigner: sign.NewSigner(sign.PurposeArtifact, ak), DeliverySigner: sign.NewSigner(sign.PurposeDelivery, dk),
		Authorizer: authz, Logger: log, Registerer: reg, Version: version, Commit: commit, BuildDate: date,
		Leader: func() (bool, int64) {
			if ctrl == nil {
				return false, 0
			}
			return ctrl.Leader()
		},
	})
	if err != nil {
		return err
	}

	gl, err := listen(o.GRPCListener, o.GRPCListen)
	if err != nil {
		return err
	}
	rl, err := listen(o.RESTListener, o.RESTListen)
	if err != nil {
		return err
	}
	ml, err := listen(o.MetricsListener, o.MetricsListen)
	if err != nil {
		return err
	}
	if gl == nil {
		return errors.New("a gRPC listen address is required")
	}
	gs := srv.GRPCServer(tlsMat.ServerTLS())

	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	var wg sync.WaitGroup
	errc := make(chan error, 4)
	wg.Add(1)
	go func() { defer wg.Done(); srv.Run(runCtx) }()
	if ctrl != nil {
		wg.Add(1)
		go func() { defer wg.Done(); ctrl.Run(runCtx) }()
	}
	go func() {
		if err := gs.Serve(gl); err != nil {
			errc <- fmt.Errorf("gRPC server: %w", err)
		}
	}()
	var servers []*http.Server
	if rl != nil {
		gw, err := srv.Gateway(runCtx)
		if err != nil {
			return err
		}
		hs := &http.Server{Handler: gw, TLSConfig: tlsMat.ServerTLS(), ReadHeaderTimeout: 10 * time.Second,
			ReadTimeout: time.Minute, WriteTimeout: time.Minute, IdleTimeout: 2 * time.Minute}
		servers = append(servers, hs)
		go func() {
			if err := hs.Serve(tls.NewListener(rl, hs.TLSConfig)); err != nil && !errors.Is(err, http.ErrServerClosed) {
				errc <- fmt.Errorf("REST server: %w", err)
			}
		}()
	}
	if ml != nil {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{Registry: reg}))
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok\n")) })
		mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
			pctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
			defer cancel()
			if err := st.Pool.Ping(pctx); err != nil {
				http.Error(w, "database unavailable", http.StatusServiceUnavailable)
				return
			}
			_, _ = w.Write([]byte("ready\n"))
		})
		hs := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
		servers = append(servers, hs)
		go func() {
			if err := hs.Serve(ml); err != nil && !errors.Is(err, http.ErrServerClosed) {
				errc <- fmt.Errorf("metrics server: %w", err)
			}
		}()
	}
	run := &Running{GRPCAddr: gl.Addr().String(), Store: st, Blobs: blobs, Server: srv, Controller: ctrl, Authorizer: authz, Registry: reg}
	if rl != nil {
		run.RESTAddr = rl.Addr().String()
	}
	if ml != nil {
		run.MetricsAddr = ml.Addr().String()
	}
	log.Info("cfgcanary server started", "grpc", run.GRPCAddr, "rest", run.RESTAddr, "metrics", run.MetricsAddr,
		"controller", o.Controller, "version", version)
	if o.Ready != nil {
		o.Ready(run)
	}

	var runErr error
	select {
	case <-ctx.Done():
	case runErr = <-errc:
	}
	log.Info("shutting down")
	srv.Drain()
	stop()
	shCtx, cancel := context.WithTimeout(context.Background(), o.ShutdownTimeout)
	defer cancel()
	done := make(chan struct{})
	go func() { gs.GracefulStop(); close(done) }()
	select {
	case <-done:
	case <-shCtx.Done():
		gs.Stop() // watch streams never end on their own
	}
	for _, hs := range servers {
		_ = hs.Shutdown(shCtx)
	}
	wg.Wait()
	return runErr
}
