package sim

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"

	"github.com/raunaksahahere/cfgcanary/examples/ratelimits"
	"github.com/raunaksahahere/cfgcanary/internal/agent"
	"github.com/raunaksahahere/cfgcanary/internal/backoff"
	"github.com/raunaksahahere/cfgcanary/internal/client"
	"github.com/raunaksahahere/cfgcanary/internal/devpki"
	"github.com/raunaksahahere/cfgcanary/internal/sign"
	"github.com/raunaksahahere/cfgcanary/sdk"
)

// Options configure a fleet.
type Options struct {
	N           int
	Prefix      string // instance IDs are <prefix>-<nnnn>
	Environment string
	Config      string
	TrustDomain string
	Server      string
	ServerName  string
	CAFile      string
	// CA mints one client certificate per agent (development only).
	CA          *devpki.CA
	TrustBundle []byte
	Root        string
	// AllowedOrigins for artifact URLs.
	AllowedOrigins []string
	Backoff        backoff.Policy
	Seed           uint64
	// Reconcile interval for agents.
	Reconcile time.Duration
	// RPS per instance of the simulated workload; 0 disables it.
	RPS float64
	// Registry receives SDK and app metrics (nil: none).
	Registry prometheus.Registerer
	// Dialer routes connections (e.g. through Toxiproxy).
	Dialer   func(ctx context.Context, addr string) (net.Conn, error)
	Observer agent.Observer
	Logger   *slog.Logger
	// HealthyAfter for SDK crash-loop accounting.
	HealthyAfter time.Duration
	// StartSpread spreads initial connections over this duration.
	StartSpread time.Duration
}

// Member is one simulated instance.
type Member struct {
	ID       string
	Agent    *agent.Agent
	workload *Workload
	dir      string

	mu  sync.RWMutex
	sdk *sdk.Client[ratelimits.Config]
	// accepted records when each generation was published by the SDK;
	// versions records when each version was first served.
	accepted map[int64]time.Time
	versions map[int64]time.Time
	// history records every change of served version (bounded).
	history []VersionChange
	cancel  context.CancelFunc
	done    chan struct{}
	dial    func(ctx context.Context) (*grpc.ClientConn, error)
	opt     *Options
	rng     *rand.Rand
}

// Fleet is a set of members.
type Fleet struct {
	Opt       Options
	Members   []*Member
	sdkM      *sdk.Metrics
	appM      *AppMetrics
	agentM    *agent.Metrics
	running   atomic.Int32
	stopTraff context.CancelFunc
	wg        sync.WaitGroup
}

// NewFleet prepares certificates and cache directories.
func NewFleet(o Options) (*Fleet, error) {
	if o.Prefix == "" {
		o.Prefix = "agent"
	}
	if o.TrustDomain == "" {
		o.TrustDomain = "cfgcanary.local"
	}
	if o.Logger == nil {
		o.Logger = slog.New(slog.DiscardHandler)
	}
	if o.HealthyAfter <= 0 {
		o.HealthyAfter = 10 * time.Second
	}
	if _, err := sign.ParseBundle(o.TrustBundle); err != nil {
		return nil, err
	}
	f := &Fleet{Opt: o}
	f.agentM = agent.NewMetrics(o.Registry)
	if o.Registry != nil {
		f.sdkM = sdk.NewMetrics(o.Registry)
		f.appM = NewAppMetrics(o.Registry)
	}
	bundle, _ := sign.ParseBundle(o.TrustBundle)
	for i := range o.N {
		id := fmt.Sprintf("%s-%04d", o.Prefix, i)
		dir := filepath.Join(o.Root, id)
		tlsDir := filepath.Join(dir, "tls")
		if err := os.MkdirAll(tlsDir, 0o700); err != nil {
			return nil, err
		}
		certFile, keyFile := filepath.Join(tlsDir, "agent.pem"), filepath.Join(tlsDir, "agent-key.pem")
		if _, err := os.Stat(certFile); err != nil {
			c, k, err := o.CA.AgentCert(o.TrustDomain, o.Environment, id, 30*24*time.Hour)
			if err != nil {
				return nil, err
			}
			if err := os.WriteFile(certFile, c, 0o600); err != nil {
				return nil, err
			}
			if err := os.WriteFile(keyFile, k, 0o600); err != nil {
				return nil, err
			}
		}
		copts := client.Options{Server: o.Server, ServerName: o.ServerName, Cert: certFile, Key: keyFile, CA: o.CAFile, Dialer: o.Dialer}
		creds, err := copts.Credentials()
		if err != nil {
			return nil, err
		}
		m := &Member{ID: id, dir: dir, accepted: map[int64]time.Time{}, versions: map[int64]time.Time{}, opt: &f.Opt,
			rng: rand.New(rand.NewPCG(o.Seed+uint64(i)*7919, uint64(i)))}
		m.dial = func(ctx context.Context) (*grpc.ClientConn, error) { return client.DialWith(copts, creds) }
		m.Agent, err = agent.New(agent.Config{
			InstanceID: id, Environment: o.Environment, Root: filepath.Join(dir, "cache"), Configs: []string{o.Config},
			Trust: bundle, Dial: m.dial, AllowedOrigins: o.AllowedOrigins, Backoff: o.Backoff,
			Rand: rand.New(rand.NewPCG(o.Seed+uint64(i), 17)), ReconcileInterval: o.Reconcile,
			StatusPoll: time.Second, Metrics: f.agentM, Observer: o.Observer, Logger: o.Logger,
			OnApplied: func(string) { m.reload() },
		})
		if err != nil {
			return nil, err
		}
		if o.RPS > 0 {
			m.workload = NewWorkload(id, o.RPS, f.appM, o.Seed+uint64(i), m.Get)
		}
		f.Members = append(f.Members, m)
	}
	return f, nil
}

func (m *Member) reload() {
	m.mu.RLock()
	c := m.sdk
	m.mu.RUnlock()
	if c != nil {
		c.Reload()
	}
}

// Get returns the member's current config, if it has one.
func (m *Member) Get() (ratelimits.Config, bool) {
	m.mu.RLock()
	c := m.sdk
	m.mu.RUnlock()
	if c == nil {
		return ratelimits.Config{}, false
	}
	return c.Get(), true
}

// Info returns the member's SDK info.
func (m *Member) Info() (sdk.Info, bool) {
	m.mu.RLock()
	c := m.sdk
	m.mu.RUnlock()
	if c == nil {
		return sdk.Info{}, false
	}
	return c.Info(), true
}

// VersionChange is one change of the served version.
type VersionChange struct {
	At         time.Time
	Version    int64
	Generation int64
}

func (m *Member) record(i sdk.Info) {
	now := time.Now()
	if n := len(m.history); n == 0 || m.history[n-1].Version != i.Version {
		if n >= 256 {
			m.history = m.history[1:]
		}
		m.history = append(m.history, VersionChange{At: now, Version: i.Version, Generation: i.Generation})
	}
	if _, ok := m.accepted[i.Generation]; !ok {
		m.accepted[i.Generation] = now
	}
	if _, ok := m.versions[i.Version]; !ok {
		m.versions[i.Version] = now
	}
}

// History returns the member's version changes.
func (m *Member) History() []VersionChange {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return append([]VersionChange(nil), m.history...)
}

// FirstServedAt returns when the member first served a version.
func (m *Member) FirstServedAt(version int64) (time.Time, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	t, ok := m.versions[version]
	return t, ok
}

// EverServed reports whether the member's application ever served a version.
func (m *Member) EverServed(version int64) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.versions[version]
	return ok
}

// AcceptedAt returns when a generation was published by the member's SDK.
func (m *Member) AcceptedAt(gen int64) (time.Time, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	t, ok := m.accepted[gen]
	return t, ok
}

// Start runs the member's agent and, once a config is available, its SDK.
func (m *Member) Start(ctx context.Context, f *Fleet) {
	ctx, cancel := context.WithCancel(ctx)
	m.cancel, m.done = cancel, make(chan struct{})
	f.running.Add(1)
	go func() {
		defer close(m.done)
		defer f.running.Add(-1)
		var wg sync.WaitGroup
		wg.Add(1)
		go func() { defer wg.Done(); _ = m.Agent.Run(ctx) }()
		c, err := sdk.Open(ctx, sdk.Options[ratelimits.Config]{
			Dir: filepath.Join(m.dir, "cache"), Config: m.opt.Config, Environment: m.opt.Environment,
			TrustBundle: m.opt.TrustBundle, Parse: ratelimits.Parse, Clone: ratelimits.Config.Clone,
			InstanceID: m.ID, Metrics: f.sdkM, HealthyAfter: m.opt.HealthyAfter, PollInterval: time.Second,
			OnStatus: m.Agent.NotifySDK,
			OnChange: func(i sdk.Info) {
				m.mu.Lock()
				m.record(i)
				m.mu.Unlock()
			},
		})
		if err == nil {
			m.mu.Lock()
			m.sdk = c
			// Record the generation that was published during Open.
			if i := c.Info(); i.Generation > 0 {
				m.record(i)
			}
			m.mu.Unlock()
			<-ctx.Done()
			c.Close()
		}
		wg.Wait()
	}()
}

// Stop stops the member and waits.
func (m *Member) Stop() {
	if m.cancel != nil {
		m.cancel()
		<-m.done
		m.mu.Lock()
		m.sdk = nil
		m.mu.Unlock()
	}
}

// Start runs every member (spread over StartSpread) and the workload loop.
func (f *Fleet) Start(ctx context.Context) {
	tctx, cancel := context.WithCancel(ctx)
	f.stopTraff = cancel
	for i, m := range f.Members {
		if f.Opt.StartSpread > 0 && len(f.Members) > 1 {
			d := time.Duration(float64(f.Opt.StartSpread) * float64(i) / float64(len(f.Members)))
			go func(m *Member) {
				select {
				case <-ctx.Done():
				case <-time.After(d):
					m.Start(ctx, f)
				}
			}(m)
			continue
		}
		m.Start(ctx, f)
	}
	if f.Opt.RPS > 0 {
		f.wg.Add(1)
		go func() {
			defer f.wg.Done()
			t := time.NewTicker(250 * time.Millisecond)
			defer t.Stop()
			for {
				select {
				case <-tctx.Done():
					return
				case <-t.C:
					for _, m := range f.Members {
						if m.workload != nil {
							m.workload.Step(250 * time.Millisecond)
						}
					}
				}
			}
		}()
	}
}

// Stop stops every member.
func (f *Fleet) Stop() {
	if f.stopTraff != nil {
		f.stopTraff()
	}
	var wg sync.WaitGroup
	for _, m := range f.Members {
		wg.Add(1)
		go func(m *Member) { defer wg.Done(); m.Stop() }(m)
	}
	wg.Wait()
	f.wg.Wait()
}

// Versions counts members by the version their SDK serves (0: none).
func (f *Fleet) Versions() map[int64]int {
	out := map[int64]int{}
	for _, m := range f.Members {
		i, ok := m.Info()
		if !ok {
			out[0]++
			continue
		}
		out[i.Version]++
	}
	return out
}

// Served lists members serving a version.
func (f *Fleet) Served(version int64) []string {
	var out []string
	for _, m := range f.Members {
		if i, ok := m.Info(); ok && i.Version == version {
			out = append(out, m.ID)
		}
	}
	sort.Strings(out)
	return out
}
