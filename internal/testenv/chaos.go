package testenv

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"strings"
	"testing"
	"time"

	toxiproxy "github.com/Shopify/toxiproxy/v2/client"

	"github.com/raunaksahahere/cfgcanary/internal/app"
	"github.com/raunaksahahere/cfgcanary/internal/testutil"
)

// Toxiproxy publishes ports 56000-56009 (see deploy/compose/test-deps.yaml).
const proxyPortLo, proxyPortHi = 56000, 56009

// Proxy is a Toxiproxy proxy reachable from the host.
type Proxy struct {
	*toxiproxy.Proxy
	// Addr is the host-side address (127.0.0.1:port).
	Addr string
}

// NewProxy creates a proxy to upstream (an address as seen from the
// Toxiproxy container, e.g. "postgres:5432" or "host.docker.internal:1234").
func NewProxy(t testing.TB, upstream string) *Proxy {
	t.Helper()
	testutil.RequireIntegration(t)
	cl := toxiproxy.NewClient(strings.TrimPrefix(testutil.ToxiproxyURL(), "http://"))
	for port := proxyPortLo; port <= proxyPortHi; port++ {
		name := testutil.RandomName("p")
		p, err := cl.CreateProxy(name, fmt.Sprintf("0.0.0.0:%d", port), upstream)
		if err != nil {
			continue // port in use by another proxy
		}
		t.Cleanup(func() { _ = p.Delete() })
		return &Proxy{Proxy: p, Addr: fmt.Sprintf("127.0.0.1:%d", port)}
	}
	t.Fatal("no free Toxiproxy port (56000-56009); a previous test may have leaked proxies")
	return nil
}

// ResetProxies deletes every proxy (for a clean start).
func ResetProxies(t testing.TB) {
	cl := toxiproxy.NewClient(strings.TrimPrefix(testutil.ToxiproxyURL(), "http://"))
	ps, err := cl.Proxies()
	if err != nil {
		t.Fatalf("toxiproxy unavailable: %v", err)
	}
	for _, p := range ps {
		_ = p.Delete()
	}
}

// Behind selects which dependencies sit behind Toxiproxy.
type Behind struct {
	DB, S3, Prometheus bool
	// GRPC binds the server on the Docker bridge so agents can be routed
	// through a proxy.
	GRPC bool
}

// ChaosEnv is an Env with proxies.
type ChaosEnv struct {
	*Env
	DBProxy, S3Proxy, PromProxy *Proxy
}

// StartChaos boots a control plane with the requested dependencies proxied.
func StartChaos(t testing.TB, b Behind, opts ...Option) *ChaosEnv {
	t.Helper()
	testutil.RequireIntegration(t)
	ResetProxies(t)
	ce := &ChaosEnv{}
	var pre []Option
	if b.DB {
		ce.DBProxy = NewProxy(t, "postgres:5432")
	}
	if b.S3 {
		ce.S3Proxy = NewProxy(t, "s3:9000")
	}
	if b.Prometheus {
		ce.PromProxy = NewProxy(t, "prometheus:9090")
		pre = append(pre, func(o *app.ServerOptions) { o.PrometheusURL = "http://" + ce.PromProxy.Addr })
	}
	all := append(pre, opts...)
	if b.GRPC {
		all = append(all, func(o *app.ServerOptions) { o.GRPCListen = "bridge" })
	}
	ce.Env = start(t, func(e *Env) {
		if ce.DBProxy != nil {
			u, _ := url.Parse(e.DBURL)
			u.Host = ce.DBProxy.Addr
			e.Opts.DBURL = u.String()
		}
		if ce.S3Proxy != nil {
			e.S3.Endpoint = ce.S3Proxy.Addr
			e.Opts.S3 = e.S3
		}
	}, all...)
	return ce
}

// AgentProxy returns a proxy in front of the server's gRPC listener (the
// environment must have been started with Behind{GRPC: true}).
func (e *Env) AgentProxy(t testing.TB) *Proxy {
	_, port, _ := net.SplitHostPort(e.grpcAddr)
	return NewProxy(t, "host.docker.internal:"+port)
}

// Replica is an additional control-plane replica on the same database.
type Replica struct {
	Run    *app.Running
	cancel context.CancelFunc
	done   chan error
}

// Stop stops the replica.
func (r *Replica) Stop() {
	if r.cancel != nil {
		r.cancel()
		<-r.done
		r.cancel = nil
	}
}

// AddReplica starts another replica sharing this environment's database,
// bucket and keys.
func (e *Env) AddReplica(holder string) *Replica {
	e.T.Helper()
	o := e.Opts
	o.Holder = holder
	o.GRPCListener, o.RESTListener, o.MetricsListener = nil, nil, nil
	o.GRPCListen, o.RESTListen, o.MetricsListen = "127.0.0.1:0", "", ""
	if strings.HasPrefix(e.grpcAddr, bridgeIP()+":") {
		o.GRPCListen = bridgeIP() + ":0"
	}
	ready := make(chan *app.Running, 1)
	o.Ready = func(r *app.Running) { ready <- r }
	o.Logger = slog.New(slog.NewTextHandler(testWriter{e.T}, &slog.HandlerOptions{Level: slog.LevelWarn}))
	ctx, cancel := context.WithCancel(context.Background())
	r := &Replica{cancel: cancel, done: make(chan error, 1)}
	go func() { r.done <- app.RunServer(ctx, o) }()
	select {
	case r.Run = <-ready:
	case err := <-r.done:
		e.T.Fatalf("replica failed: %v", err)
	case <-time.After(60 * time.Second):
		e.T.Fatal("replica did not start")
	}
	e.T.Cleanup(r.Stop)
	return r
}

func bridgeIP() string { return dockerBridgeIP() }
