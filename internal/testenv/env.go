// Package testenv starts a real control plane against the integration-test
// services (deploy/compose/test-deps.yaml) for integration and chaos tests.
package testenv

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"

	pb "github.com/raunaksahahere/cfgcanary/api/gen/cfgcanary/v1"
	"github.com/raunaksahahere/cfgcanary/internal/app"
	"github.com/raunaksahahere/cfgcanary/internal/backoff"
	"github.com/raunaksahahere/cfgcanary/internal/blob"
	"github.com/raunaksahahere/cfgcanary/internal/client"
	"github.com/raunaksahahere/cfgcanary/internal/devpki"
	"github.com/raunaksahahere/cfgcanary/internal/server"
	"github.com/raunaksahahere/cfgcanary/internal/sim"
	"github.com/raunaksahahere/cfgcanary/internal/testutil"
)

// Env is a running control plane.
type Env struct {
	T        testing.TB
	Dir      string
	PKI      string
	DBURL    string
	S3       blob.Config
	CA       *devpki.CA
	Trust    []byte
	Opts     app.ServerOptions
	Run      *app.Running
	grpcAddr string
	cancel   context.CancelFunc
	done     chan error
}

// Option customises the environment.
type Option func(*app.ServerOptions)

// WithPrometheus enables the gate with the test Prometheus.
func WithPrometheus() Option {
	return func(o *app.ServerOptions) { o.PrometheusURL = testutil.PrometheusURL() }
}

// WithServer adjusts server limits.
func WithServer(f func(*server.Config)) Option { return func(o *app.ServerOptions) { f(&o.Server) } }

// Start boots a control plane with a fresh database and bucket.
func Start(t testing.TB, opts ...Option) *Env {
	t.Helper()
	testutil.RequireIntegration(t)
	e := &Env{T: t, Dir: t.TempDir()}
	e.PKI = filepath.Join(e.Dir, "pki")
	if _, err := devpki.Bootstrap(e.PKI, devpki.DefaultOptions()); err != nil {
		t.Fatal(err)
	}
	var err error
	if e.CA, err = devpki.LoadCA(e.PKI); err != nil {
		t.Fatal(err)
	}
	e.Trust, _ = os.ReadFile(filepath.Join(e.PKI, "trust.json"))
	e.DBURL = testutil.FreshDatabase(t)
	ep, ak, sk := testutil.S3()
	e.S3 = blob.Config{Endpoint: ep, Bucket: testutil.RandomName("cfgc-"), AccessKey: ak, SecretKey: sk}
	e.Opts = app.ServerOptions{
		DBURL: e.DBURL, DBMaxConns: 30, S3: e.S3,
		TLSCert: filepath.Join(e.PKI, "server.pem"), TLSKey: filepath.Join(e.PKI, "server-key.pem"), TLSCA: filepath.Join(e.PKI, "ca.pem"),
		ArtifactKey: filepath.Join(e.PKI, "artifact.key"), DeliveryKey: filepath.Join(e.PKI, "delivery.key"),
		RBACFile: filepath.Join(e.PKI, "rbac.yaml"), Controller: true, ControllerTick: 100 * time.Millisecond,
		Server: server.Config{SweepInterval: time.Second, AckFlush: 50 * time.Millisecond, MembershipWindow: time.Minute},
		Logger: slog.New(slog.NewTextHandler(testWriter{t}, &slog.HandlerOptions{Level: slog.LevelWarn})),
	}
	for _, o := range opts {
		o(&e.Opts)
	}
	e.StartServer()
	t.Cleanup(e.StopServer)
	return e
}

type testWriter struct{ t testing.TB }

func (w testWriter) Write(p []byte) (int, error) {
	if os.Getenv("CFGCANARY_TEST_LOGS") == "1" {
		w.t.Logf("%s", p)
	}
	return len(p), nil
}

// StartServer starts (or restarts) the replica on the same gRPC address.
func (e *Env) StartServer() {
	e.T.Helper()
	addr := e.grpcAddr
	if addr == "" {
		addr = "127.0.0.1:0"
	}
	var gl net.Listener
	var err error
	for i := 0; i < 50; i++ { // the old listener may linger briefly
		if gl, err = net.Listen("tcp", addr); err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil {
		e.T.Fatal(err)
	}
	e.grpcAddr = gl.Addr().String()
	o := e.Opts
	o.GRPCListener = gl
	o.RESTListen = "127.0.0.1:0"
	o.MetricsListen = "127.0.0.1:0"
	ready := make(chan *app.Running, 1)
	o.Ready = func(r *app.Running) { ready <- r }
	ctx, cancel := context.WithCancel(context.Background())
	e.cancel, e.done = cancel, make(chan error, 1)
	go func() { e.done <- app.RunServer(ctx, o) }()
	select {
	case e.Run = <-ready:
	case err := <-e.done:
		e.T.Fatalf("server failed to start: %v", err)
	case <-time.After(60 * time.Second):
		e.T.Fatal("server did not start")
	}
}

// StopServer stops the replica.
func (e *Env) StopServer() {
	if e.cancel != nil {
		e.cancel()
		<-e.done
		e.cancel = nil
	}
}

// GRPCAddr is the server's gRPC address.
func (e *Env) GRPCAddr() string { return e.grpcAddr }

// Conn returns a client connection authenticated as a dev user or service
// ("users/admin", "users/viewer", "services/k8s-controller", ...).
func (e *Env) Conn(who string) *grpc.ClientConn {
	e.T.Helper()
	cc, err := client.Dial(client.Options{Server: e.grpcAddr, ServerName: "localhost",
		Cert: filepath.Join(e.PKI, who+".pem"), Key: filepath.Join(e.PKI, who+"-key.pem"), CA: filepath.Join(e.PKI, "ca.pem")})
	if err != nil {
		e.T.Fatal(err)
	}
	e.T.Cleanup(func() { cc.Close() })
	return cc
}

// Control returns a ControlService client for a dev identity.
func (e *Env) Control(who string) pb.ControlServiceClient {
	return pb.NewControlServiceClient(e.Conn(who))
}

// FleetOptions returns fleet options wired to this environment.
func (e *Env) FleetOptions(n int, config string) sim.Options {
	return sim.Options{
		N: n, Environment: "prod", Config: config, Server: e.grpcAddr, ServerName: "localhost",
		CAFile: filepath.Join(e.PKI, "ca.pem"), CA: e.CA, TrustBundle: e.Trust, Root: filepath.Join(e.Dir, "fleet-"+config),
		AllowedOrigins: []string{"http://" + e.S3.Endpoint}, Backoff: backoff.Policy{Base: 50 * time.Millisecond, Cap: 2 * time.Second},
		Seed: 42, Reconcile: 5 * time.Second, HealthyAfter: time.Hour,
	}
}

// Fleet starts a fleet and stops it when the test ends.
func (e *Env) Fleet(o sim.Options) *sim.Fleet {
	e.T.Helper()
	f, err := sim.NewFleet(o)
	if err != nil {
		e.T.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	f.Start(ctx)
	e.T.Cleanup(func() { cancel(); f.Stop() })
	return f
}

// ServeMetrics exposes reg on an address the test Prometheus can scrape and
// registers it through file-based service discovery.
func (e *Env) ServeMetrics(reg *prometheus.Registry) {
	e.T.Helper()
	bind := os.Getenv("CFGCANARY_TEST_BIND")
	if bind == "" {
		bind = dockerBridgeIP()
	}
	l, err := net.Listen("tcp", net.JoinHostPort(bind, "0"))
	if err != nil {
		e.T.Fatal(err)
	}
	hs := &http.Server{Handler: promhttp.HandlerFor(reg, promhttp.HandlerOpts{}), ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = hs.Serve(l) }()
	_, port, _ := net.SplitHostPort(l.Addr().String())
	name := testutil.RandomName("t") + ".json"
	path := filepath.Join(testutil.RepoRoot(), "deploy", "compose", "testdeps", "targets", name)
	b, _ := json.Marshal([]map[string]any{{"targets": []string{"host.docker.internal:" + port}, "labels": map[string]string{"test": name}}})
	if err := os.WriteFile(path, b, 0o644); err != nil {
		e.T.Fatal(err)
	}
	e.T.Cleanup(func() { os.Remove(path); hs.Close() })
}

func dockerBridgeIP() string {
	if ifc, err := net.InterfaceByName("docker0"); err == nil {
		if addrs, err := ifc.Addrs(); err == nil {
			for _, a := range addrs {
				if ipn, ok := a.(*net.IPNet); ok && ipn.IP.To4() != nil {
					return ipn.IP.String()
				}
			}
		}
	}
	return "0.0.0.0"
}

// RatelimitsSpec returns the example config spec with the given rollout
// policy for environment "prod".
func RatelimitsSpec(name string, p *pb.RolloutPolicy, withWasm []byte) *pb.ConfigSpec {
	root := testutil.RepoRoot()
	schema, _ := os.ReadFile(filepath.Join(root, "examples", "ratelimits", "schema.json"))
	cue, _ := os.ReadFile(filepath.Join(root, "examples", "ratelimits", "policy.cue"))
	spec := &pb.ConfigSpec{
		Name: name, Description: "example rate limits", Format: "json", FailureMode: pb.FailureMode_FAILURE_MODE_FAIL_STATIC,
		MaxStaleness: durationpb.New(10 * time.Minute),
		Validation: &pb.ValidationPolicy{
			Limits:     &pb.Limits{MaxBytes: 64 << 10, MaxItems: map[string]int64{"/features": 200, "/limits": 100}},
			JsonSchema: string(schema), Cue: string(cue),
			Relative: &pb.RelativeGuards{MaxSizeRatio: 2, MaxItemsRatio: map[string]float64{"/features": 2}},
		},
		Environments: map[string]*pb.RolloutPolicy{"prod": p},
	}
	if withWasm != nil {
		spec.Validation.Wasm = &pb.WasmValidator{Module: withWasm, Timeout: durationpb.New(2 * time.Second)}
	}
	return spec
}

// Payload renders an example ratelimits payload.
func Payload(features, timeoutMS int) []byte {
	type feature struct {
		Name           string `json:"name"`
		Enabled        bool   `json:"enabled"`
		RolloutPercent int    `json:"rollout_percent"`
		Rule           string `json:"rule,omitempty"`
	}
	fs := make([]feature, features)
	for i := range fs {
		fs[i] = feature{Name: fmt.Sprintf("feature-%03d", i), Enabled: i%3 != 0, RolloutPercent: (i * 7) % 101, Rule: "region == eu"}
	}
	b, _ := json.MarshalIndent(map[string]any{
		"schema_version": 1, "service": "checkout", "timeout_ms": timeoutMS, "features": fs,
		"limits": []map[string]any{{"route": "/api/pay", "rps": 200, "burst": 400}, {"route": "/api/cart", "rps": 500, "burst": 1000}},
	}, "", "  ")
	return b
}
