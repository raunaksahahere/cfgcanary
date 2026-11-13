// Package agent runs next to (or inside) a workload. It watches the control
// plane for the desired state of its configs, verifies and durably applies
// each update, and reports what it applied and what the application's SDK
// accepted.
//
// Failure handling follows docs/invariants.md: nothing that goes wrong on the
// network, in the object store, on disk or in verification changes what the
// application is serving.
package agent

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/raunaksahahere/cfgcanary/api/gen/cfgcanary/v1"
	"github.com/raunaksahahere/cfgcanary/internal/backoff"
	"github.com/raunaksahahere/cfgcanary/internal/cache"
	"github.com/raunaksahahere/cfgcanary/internal/sign"
)

// Version is reported to the server.
var Version = "dev"

// Config configures an agent.
type Config struct {
	InstanceID  string
	Environment string
	Labels      map[string]string
	// Root is the cache directory shared with the SDK.
	Root    string
	Configs []string
	Trust   *sign.Bundle
	// Dial opens a connection to the control plane. A new connection is
	// opened for every watch attempt, so reconnect timing is governed by this
	// package's backoff, not by gRPC's internal one.
	Dial func(ctx context.Context) (*grpc.ClientConn, error)
	// AllowedOrigins lists scheme://host[:port] prefixes artifact URLs may
	// use. An update whose URL is elsewhere is refused.
	AllowedOrigins []string
	HTTPClient     *http.Client
	// MaxBytes is the largest payload the agent will download.
	MaxBytes          int64
	ReconcileInterval time.Duration
	Backoff           backoff.Policy
	// Rand seeds jitter; nil uses a random seed.
	Rand          *rand.Rand
	RetainObjects int
	// StatusPoll is how often the SDK status file is checked when no
	// in-process notification arrives.
	StatusPoll time.Duration
	// OnApplied is called after a new pointer is durable (in-process SDKs use
	// it to reload immediately).
	OnApplied func(config string)
	Logger    *slog.Logger
	Metrics   *Metrics
	// Observer receives connection events (used by the simulator).
	Observer Observer
}

// Observer receives events; every method must be cheap and non-blocking.
type Observer interface {
	Connecting(instance string)
	Connected(instance string)
	// Received is called when a new generation arrives, before verification
	// and the durable write (fan-out latency without local disk costs).
	Received(instance, config string, generation, version int64)
	Applied(instance, config string, generation, version int64)
}

type configState struct {
	dir *cache.Dir
	// applied is the generation of the last durably applied update.
	applied     int64
	version     int64
	lastSDKSeq  int64
	lastFailGen int64
}

// Agent is one agent instance.
type Agent struct {
	cfg     Config
	log     *slog.Logger
	bo      *backoff.Backoff
	metrics *Metrics

	mu      sync.Mutex
	configs map[string]*configState
	pending map[string]*pb.InstanceConfigStatus

	kick       chan struct{} // status pending
	sdkNotify  chan struct{}
	connected  atomic.Bool
	heartbeat  atomic.Int64
	client     atomic.Pointer[clientRef]
	reconnects atomic.Int64
}

type clientRef struct {
	c pb.ConfigServiceClient
}

// New validates the configuration and recovers local state.
func New(cfg Config) (*Agent, error) {
	switch {
	case cfg.InstanceID == "" || cfg.Environment == "":
		return nil, errors.New("agent: instance id and environment are required")
	case cfg.Root == "":
		return nil, errors.New("agent: cache directory is required")
	case cfg.Trust == nil:
		return nil, errors.New("agent: trust bundle is required")
	case len(cfg.Configs) == 0:
		return nil, errors.New("agent: at least one config is required")
	}
	if cfg.MaxBytes <= 0 {
		cfg.MaxBytes = 4 << 20
	}
	if cfg.ReconcileInterval <= 0 {
		cfg.ReconcileInterval = 30 * time.Second
	}
	if cfg.RetainObjects <= 0 {
		cfg.RetainObjects = 4
	}
	if cfg.StatusPoll <= 0 {
		cfg.StatusPoll = 250 * time.Millisecond
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = &http.Client{Timeout: 30 * time.Second}
	}
	// Never follow redirects: the URL must stay inside the allowed origins.
	hc := *cfg.HTTPClient
	hc.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	cfg.HTTPClient = &hc
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	if cfg.Metrics == nil {
		cfg.Metrics = NewMetrics(nil)
	}
	a := &Agent{
		cfg: cfg, log: cfg.Logger.With("instance", cfg.InstanceID), bo: backoff.New(cfg.Backoff, cfg.Rand),
		metrics: cfg.Metrics, configs: map[string]*configState{}, pending: map[string]*pb.InstanceConfigStatus{},
		kick: make(chan struct{}, 1), sdkNotify: make(chan struct{}, 1),
	}
	a.heartbeat.Store(cache.ReadHeartbeat(cfg.Root))
	for _, name := range cfg.Configs {
		d, err := cache.Open(cfg.Root, name)
		if err != nil {
			return nil, err
		}
		st := &configState{dir: d}
		// Recover: only a pointer that still verifies counts as applied.
		if p, _, err := d.ReadPointer(); err == nil && !p.Current.Zero() {
			if _, err := d.Verify(cfg.Trust, p.Current, cfg.Environment, cfg.MaxBytes); err == nil {
				st.applied, st.version = p.Current.Generation, p.Current.Version
			} else {
				a.log.Warn("cached config failed verification on start; will refetch", "config", name, "err", err)
			}
		}
		a.configs[name] = st
		a.metrics.applies.WithLabelValues(name, "applied").Add(0)
		a.metrics.stale.WithLabelValues(name).Add(0)
	}
	return a, nil
}

// Connected reports whether a watch stream is open.
func (a *Agent) Connected() bool { return a.connected.Load() }

// Applied returns the generation and version last applied for a config.
func (a *Agent) Applied(config string) (generation, version int64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if st := a.configs[config]; st != nil {
		return st.applied, st.version
	}
	return 0, 0
}

// NotifySDK tells the agent that an in-process SDK wrote a new status.
func (a *Agent) NotifySDK() {
	select {
	case a.sdkNotify <- struct{}{}:
	default:
	}
}

func (a *Agent) subscriptions() []*pb.Subscription {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]*pb.Subscription, 0, len(a.configs))
	for _, name := range a.cfg.Configs {
		st := a.configs[name]
		out = append(out, &pb.Subscription{Config: name, LastGeneration: st.applied, LastVersion: st.version})
	}
	return out
}

func (a *Agent) contact() {
	n := a.heartbeat.Add(1)
	if err := cache.BumpHeartbeat(a.cfg.Root, n); err != nil {
		a.log.Debug("heartbeat write failed", "err", err)
	}
}

// Run watches until ctx is cancelled. It never returns early because of
// control-plane, network or object-store failures.
func (a *Agent) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); a.reportLoop(ctx) }()
	go func() { defer wg.Done(); a.sdkLoop(ctx) }()
	attempt := 0
	for ctx.Err() == nil {
		started := time.Now()
		err := a.watchOnce(ctx)
		a.connected.Store(false)
		if ctx.Err() != nil {
			break
		}
		// A stream that stayed up for a while resets the backoff.
		if time.Since(started) > 2*a.bo.Ceiling(3) {
			attempt = 0
		}
		d := a.bo.Delay(attempt)
		attempt++
		a.reconnects.Add(1)
		a.metrics.reconnects.Inc()
		a.log.Debug("watch ended; reconnecting", "err", err, "delay", d, "attempt", attempt)
		t := time.NewTimer(d)
		select {
		case <-ctx.Done():
			t.Stop()
		case <-t.C:
		}
	}
	wg.Wait()
	return nil
}

func (a *Agent) watchOnce(ctx context.Context) error {
	if a.cfg.Observer != nil {
		a.cfg.Observer.Connecting(a.cfg.InstanceID)
	}
	dctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	conn, err := a.cfg.Dial(dctx)
	cancel()
	if err != nil {
		return err
	}
	defer conn.Close()
	client := pb.NewConfigServiceClient(conn)
	sctx, stop := context.WithCancel(ctx)
	defer stop()
	stream, err := client.Watch(sctx, &pb.WatchRequest{
		InstanceId: a.cfg.InstanceID, Labels: a.cfg.Labels, Subscriptions: a.subscriptions(), AgentVersion: Version,
	})
	if err != nil {
		return err
	}
	// The stream is only known to be accepted once the server sends headers
	// (admission control may refuse it).
	if _, err := stream.Header(); err != nil {
		return err
	}
	a.client.Store(&clientRef{c: client})
	defer a.client.Store(nil)
	a.connected.Store(true)
	a.metrics.connected.Inc()
	defer a.metrics.connected.Dec()
	if a.cfg.Observer != nil {
		a.cfg.Observer.Connected(a.cfg.InstanceID)
	}
	a.contact()
	a.requeueAll() // re-report latest statuses on every new connection

	// Periodic reconciliation against the authoritative snapshot.
	recDone := make(chan struct{})
	go func() {
		defer close(recDone)
		for {
			t := time.NewTimer(a.bo.Jitter(a.cfg.ReconcileInterval, 0.2))
			select {
			case <-sctx.Done():
				t.Stop()
				return
			case <-t.C:
			}
			if err := a.Reconcile(sctx, client); err != nil {
				a.log.Debug("reconcile failed", "err", err)
				if status.Code(err) == codes.Unauthenticated || status.Code(err) == codes.PermissionDenied {
					stop()
					return
				}
			}
		}
	}()
	defer func() { stop(); <-recDone }()
	for {
		u, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return errors.New("server closed the stream")
			}
			return err
		}
		a.contact()
		a.Handle(sctx, client, u)
	}
}

// Reconcile fetches the authoritative desired state and applies anything
// newer than what is held. Correctness never depends on receiving every
// stream message.
func (a *Agent) Reconcile(ctx context.Context, client pb.ConfigServiceClient) error {
	rctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	resp, err := client.GetDesired(rctx, &pb.GetDesiredRequest{InstanceId: a.cfg.InstanceID, Subscriptions: a.subscriptions()})
	if err != nil {
		return err
	}
	a.contact()
	for _, u := range resp.Updates {
		a.Handle(ctx, client, u)
	}
	// Re-report the latest status: acknowledgements the server accepted but
	// could not store (for example during a database outage) are repaired
	// within one reconcile interval. Bounded: one status per config.
	a.requeueAll()
	return nil
}

// failure is a payload-free reason for FAILED.
type failure struct {
	reason string
	err    error
}

func (f *failure) Error() string { return f.reason + ": " + f.err.Error() }

// Handle processes one update. It is safe to call with duplicate, stale or
// reordered updates.
func (a *Agent) Handle(ctx context.Context, client pb.ConfigServiceClient, u *pb.ConfigUpdate) {
	a.mu.Lock()
	st := a.configs[u.Config]
	if st == nil || u.Kind != pb.ConfigUpdate_KIND_DESIRED {
		a.mu.Unlock()
		return
	}
	if u.Generation <= st.applied {
		a.mu.Unlock()
		a.metrics.stale.WithLabelValues(u.Config).Inc()
		return
	}
	a.mu.Unlock()
	if a.cfg.Observer != nil {
		a.cfg.Observer.Received(a.cfg.InstanceID, u.Config, u.Generation, u.Version)
	}

	err := a.apply(ctx, client, st, u)
	if err != nil {
		var f *failure
		reason := "error"
		if errors.As(err, &f) {
			reason = f.reason
		}
		a.metrics.applies.WithLabelValues(u.Config, "failed_"+reason).Inc()
		a.log.Warn("update not applied; keeping current config", "config", u.Config, "generation", u.Generation, "version", u.Version, "reason", reason, "err", err)
		a.mu.Lock()
		first := st.lastFailGen != u.Generation
		st.lastFailGen = u.Generation
		a.mu.Unlock()
		if first {
			a.queue(u.Config, &pb.InstanceConfigStatus{Config: u.Config, Generation: u.Generation, Version: u.Version,
				State: pb.ApplyState_APPLY_STATE_FAILED, Reason: reason})
		}
		return
	}
	a.metrics.applies.WithLabelValues(u.Config, "applied").Inc()
	a.metrics.generation.WithLabelValues(u.Config).Set(float64(u.Generation))
	a.queue(u.Config, &pb.InstanceConfigStatus{Config: u.Config, Generation: u.Generation, Version: u.Version, State: pb.ApplyState_APPLY_STATE_APPLIED})
	if a.cfg.Observer != nil {
		a.cfg.Observer.Applied(a.cfg.InstanceID, u.Config, u.Generation, u.Version)
	}
	if a.cfg.OnApplied != nil {
		a.cfg.OnApplied(u.Config)
	}
}

func (a *Agent) apply(ctx context.Context, client pb.ConfigServiceClient, st *configState, u *pb.ConfigUpdate) error {
	e := cache.Entry{
		Config: u.Config, Environment: u.Environment, Version: u.Version, Generation: u.Generation, SHA256: u.Sha256,
		Size: u.Size, Ring: u.Ring, ReleaseID: u.ReleaseId,
		Artifact: signed(u.Artifact), Delivery: signed(u.Delivery),
	}
	return a.applyEntry(ctx, st, e, func(ctx context.Context) ([]byte, error) {
		return a.download(ctx, client, u)
	})
}

// applyEntry verifies statements, obtains the bytes with fetch when the object
// is not already held, stores them and swaps the pointer.
func (a *Agent) applyEntry(ctx context.Context, st *configState, e cache.Entry, fetch func(context.Context) ([]byte, error)) error {
	if e.Environment != a.cfg.Environment {
		return &failure{"scope", fmt.Errorf("update is for environment %q", e.Environment)}
	}
	art, del, err := a.cfg.Trust.VerifyUpdate(e.Artifact, e.Delivery, sign.Scope{Config: e.Config, Environment: e.Environment, Ring: e.Ring})
	if err != nil {
		reason := "signature"
		if errors.Is(err, sign.ErrScope) {
			reason = "scope"
		}
		return &failure{reason, err}
	}
	if art.Version != e.Version || art.SHA256 != e.SHA256 || art.Size != e.Size || del.Generation != e.Generation {
		return &failure{"signature", errors.New("update fields do not match the signed statements")}
	}
	if art.Size > a.cfg.MaxBytes {
		return &failure{"too_large", fmt.Errorf("artifact is %d bytes; local limit %d", art.Size, a.cfg.MaxBytes)}
	}
	e.FailureMode, e.MaxStalenessSeconds = del.FailureMode, del.MaxStalenessSeconds
	if _, err := st.dir.ReadObject(e.SHA256, e.Size); err != nil {
		data, err := fetch(ctx)
		if err != nil {
			return err
		}
		if int64(len(data)) != art.Size || cache.Digest(data) != art.SHA256 {
			return &failure{"digest", errors.New("downloaded bytes do not match the signed digest")}
		}
		if err := st.dir.PutObject(e.SHA256, data); err != nil {
			return &failure{"disk", err}
		}
	}
	if _, err := st.dir.Swap(e); err != nil {
		return &failure{"disk", err}
	}
	a.mu.Lock()
	if e.Generation > st.applied {
		st.applied, st.version = e.Generation, e.Version
	}
	a.mu.Unlock()
	keep := []string{e.SHA256}
	if p, _, err := st.dir.ReadPointer(); err == nil {
		keep = append(keep, p.Current.SHA256, p.Previous.SHA256)
	}
	s := st.dir.ReadSDKState()
	keep = append(keep, s.Accepted.SHA256)
	_, _ = st.dir.GC(a.cfg.RetainObjects, keep...)
	return nil
}

func signed(s *pb.SignedStatement) sign.Signed {
	if s == nil {
		return sign.Signed{}
	}
	return sign.Signed{Payload: s.Payload, Signature: s.Signature, KeyID: s.KeyId}
}

func (a *Agent) originAllowed(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil || u.User != nil || u.Host == "" {
		return false
	}
	origin := u.Scheme + "://" + u.Host
	for _, o := range a.cfg.AllowedOrigins {
		if strings.TrimSuffix(o, "/") == origin {
			return true
		}
	}
	return false
}

func (a *Agent) download(ctx context.Context, client pb.ConfigServiceClient, u *pb.ConfigUpdate) ([]byte, error) {
	urlStr := u.Url
	if client == nil && strings.HasPrefix(urlStr, "file://") {
		// File-source mode: the bytes come from a projected volume.
		data, err := readLocal(strings.TrimPrefix(urlStr, "file://"), u.Size)
		if err != nil {
			return nil, &failure{"download", err}
		}
		return data, nil
	}
	for attempt := 0; attempt < 2; attempt++ {
		if !a.originAllowed(urlStr) {
			return nil, &failure{"untrusted_url", errors.New("artifact URL is outside the allowed origins")}
		}
		data, code, err := a.get(ctx, urlStr, u.Size)
		if err == nil {
			return data, nil
		}
		// An expired or invalid presigned URL: ask for a fresh one. The
		// content identity (digest, signatures) does not change.
		if attempt == 0 && client != nil && (code == http.StatusForbidden || code == http.StatusBadRequest) {
			rctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			r, rerr := client.RefreshArtifact(rctx, &pb.RefreshArtifactRequest{InstanceId: a.cfg.InstanceID, Config: u.Config, Version: u.Version})
			cancel()
			if rerr == nil {
				urlStr = r.Url
				continue
			}
		}
		return nil, &failure{"download", err}
	}
	return nil, &failure{"download", errors.New("artifact could not be downloaded")}
}

func (a *Agent) get(ctx context.Context, raw string, size int64) ([]byte, int, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, raw, nil)
	if err != nil {
		return nil, 0, err
	}
	resp, err := a.cfg.HTTPClient.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		return nil, resp.StatusCode, fmt.Errorf("object store returned HTTP %d", resp.StatusCode)
	}
	// Read at most one byte more than the signed size.
	b, err := io.ReadAll(io.LimitReader(resp.Body, size+1))
	if err != nil {
		return nil, resp.StatusCode, err
	}
	if int64(len(b)) != size {
		return nil, resp.StatusCode, fmt.Errorf("artifact is %d bytes; signed size is %d", len(b), size)
	}
	return b, resp.StatusCode, nil
}

// --- status reporting ---------------------------------------------------------

func (a *Agent) queue(config string, s *pb.InstanceConfigStatus) {
	a.mu.Lock()
	// Fill in what the application is serving, from the SDK when it has
	// reported, otherwise the agent's pointer.
	if st := a.configs[config]; st != nil && s.ServingVersion == 0 {
		if sdk, err := st.dir.ReadSDKStatus(); err == nil {
			s.ServingVersion, s.ServingGeneration = sdk.ServingVersion, sdk.ServingGeneration
		}
	}
	cur := a.pending[config]
	// Coalesce: keep only the newest status per config (bounded memory).
	if cur == nil || s.Generation > cur.Generation || (s.Generation == cur.Generation && rankState(s.State) >= rankState(cur.State)) {
		a.pending[config] = s
	}
	a.mu.Unlock()
	select {
	case a.kick <- struct{}{}:
	default:
	}
}

func rankState(s pb.ApplyState) int {
	switch s {
	case pb.ApplyState_APPLY_STATE_ACCEPTED, pb.ApplyState_APPLY_STATE_REJECTED:
		return 2
	case pb.ApplyState_APPLY_STATE_CRASH_LOOP:
		return 3
	}
	return 1
}

// requeueAll re-reports the latest known status of every config.
func (a *Agent) requeueAll() {
	for _, name := range a.cfg.Configs {
		a.mu.Lock()
		st := a.configs[name]
		gen, ver := st.applied, st.version
		a.mu.Unlock()
		if sdk, err := st.dir.ReadSDKStatus(); err == nil && sdk.Generation >= gen && sdk.Generation > 0 {
			a.queue(name, sdkToProto(name, sdk))
		} else if gen > 0 {
			a.queue(name, &pb.InstanceConfigStatus{Config: name, Generation: gen, Version: ver, State: pb.ApplyState_APPLY_STATE_APPLIED})
		}
	}
}

func sdkToProto(config string, s cache.SDKStatus) *pb.InstanceConfigStatus {
	state := pb.ApplyState(pb.ApplyState_value["APPLY_STATE_"+s.State])
	return &pb.InstanceConfigStatus{Config: config, Generation: s.Generation, Version: s.Version, State: state,
		Reason: s.Reason, ServingVersion: s.ServingVersion, ServingGeneration: s.ServingGeneration}
}

// sdkLoop forwards SDK status changes.
func (a *Agent) sdkLoop(ctx context.Context) {
	t := time.NewTicker(a.cfg.StatusPoll)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		case <-a.sdkNotify:
		}
		for _, name := range a.cfg.Configs {
			a.mu.Lock()
			st := a.configs[name]
			last := st.lastSDKSeq
			a.mu.Unlock()
			s, err := st.dir.ReadSDKStatus()
			if err != nil || s.Seq == last {
				continue
			}
			a.mu.Lock()
			st.lastSDKSeq = s.Seq
			a.mu.Unlock()
			a.queue(name, sdkToProto(name, s))
		}
	}
}

// reportLoop sends pending statuses with bounded retries. Statuses that cannot
// be delivered stay coalesced in memory (one per config) and are re-sent on
// the next connection; the server also learns state from reconciliation.
func (a *Agent) reportLoop(ctx context.Context) {
	attempt := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-a.kick:
		}
		for ctx.Err() == nil {
			ref := a.client.Load()
			a.mu.Lock()
			batch := make([]*pb.InstanceConfigStatus, 0, len(a.pending))
			for _, s := range a.pending {
				batch = append(batch, s)
			}
			a.mu.Unlock()
			if len(batch) == 0 || ref == nil {
				break // wait for the next kick (a new connection requeues)
			}
			rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			_, err := ref.c.ReportStatus(rctx, &pb.ReportStatusRequest{InstanceId: a.cfg.InstanceID, Statuses: batch})
			cancel()
			if err == nil {
				attempt = 0
				a.mu.Lock()
				for _, s := range batch {
					if cur := a.pending[s.Config]; cur == s {
						delete(a.pending, s.Config)
					}
				}
				a.mu.Unlock()
				a.metrics.reports.WithLabelValues("ok").Inc()
				continue
			}
			a.metrics.reports.WithLabelValues("error").Inc()
			if attempt >= 6 {
				attempt = 0
				break // give up until the next kick
			}
			d := a.bo.Delay(attempt)
			attempt++
			t := time.NewTimer(d)
			select {
			case <-ctx.Done():
				t.Stop()
				return
			case <-t.C:
			}
		}
	}
}
