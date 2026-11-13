// Package sdk is the cfgcanary consumer library.
//
// An application opens a Client for one config with its own parser. The
// client reads what the cfgcanary agent wrote to the local cache, verifies it
// again (signatures, digest, scope), parses it, and publishes an immutable
// snapshot. Get returns the published snapshot and never fails once Open has
// succeeded. A payload the parser rejects is reported and the previous
// snapshot (the last-known-good) keeps being served.
//
//	cl, err := sdk.Open(ctx, sdk.Options[ratelimits.Config]{
//		Dir: "/var/lib/cfgcanary", Config: "ratelimits", Environment: "prod",
//		TrustBundleFile: "/etc/cfgcanary/trust.json",
//		Parse: ratelimits.Parse, Clone: ratelimits.Config.Clone,
//	})
//	if err != nil { /* *sdk.InitError: no usable config and no bootstrap */ }
//	cfg := cl.Get()
//
// What last-known-good does not mean: an old value is not proven safe forever,
// and a config the parser accepts can still harm the service. The first is
// handled by the staleness policy (FailClosed), the second by cfgcanary's
// canary gate. See docs/invariants.md.
package sdk

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/raunaksahahere/cfgcanary/internal/cache"
	"github.com/raunaksahahere/cfgcanary/internal/sign"
)

// Options configures a Client.
type Options[T any] struct {
	// Dir is the cache directory the agent writes (its --cache-dir).
	Dir         string
	Config      string
	Environment string
	// TrustBundleFile or TrustBundle (JSON) lists the keys whose signatures
	// are accepted. Required.
	TrustBundleFile string
	TrustBundle     []byte
	// Parse is the application's own parser. It must be deterministic and
	// must not retain the input slice.
	Parse func([]byte) (T, error)
	// Clone returns a deep copy. Required when T contains maps, slices,
	// pointers, channels, functions or interfaces, unless AllowShared is set.
	Clone func(T) T
	// AllowShared disables the immutability check: callers promise never to
	// mutate values returned by Get.
	AllowShared bool
	// Bootstrap is served, after passing Parse, when no usable cached config
	// exists at start. Without it Open waits for the first config.
	Bootstrap []byte
	// FailClosed declares that serving a stale value is unsafe for this
	// config (for example an allow-list). A Bootstrap is then refused unless
	// BootstrapDenyAll is set to say it is a deny-everything default. The
	// mode in the signed delivery statement also applies once a config has
	// been received.
	FailClosed       bool
	BootstrapDenyAll bool
	// CrashLoopThreshold is the number of process starts tolerated on a new
	// config that has not been marked healthy before it is quarantined.
	// Default 3.
	CrashLoopThreshold int
	// HealthyAfter marks a newly served config healthy after this much
	// uptime (monotonic clock). Default 30s. MarkHealthy does it explicitly.
	HealthyAfter time.Duration
	// PollInterval is how often the cache is checked. Default 250ms.
	PollInterval time.Duration
	// MaxBytes is the largest payload accepted. Default 4 MiB.
	MaxBytes int64
	// InstanceID labels metrics (cfgcanary_instance).
	InstanceID string
	Metrics    *Metrics
	Logger     *slog.Logger
	// OnChange is called after a new snapshot is published.
	OnChange func(Info)
	// OnStatus is called after a status is written for the agent (in-process
	// agents use it to report immediately).
	OnStatus func()
}

// Info describes the published snapshot.
type Info struct {
	Version    int64
	Generation int64
	ReleaseID  string
	SHA256     string
	// Source is "cache" or "bootstrap".
	Source string
	// FailClosed is the effective failure mode.
	FailClosed bool
}

// InitError is returned by Open when no config could be served.
type InitError struct {
	Config string
	Reason string
	Err    error
}

func (e *InitError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("cfgcanary sdk: config %q is not available: %s: %v", e.Config, e.Reason, e.Err)
	}
	return fmt.Sprintf("cfgcanary sdk: config %q is not available: %s", e.Config, e.Reason)
}

func (e *InitError) Unwrap() error { return e.Err }

// ErrStale is returned by GetFresh when a fail-closed config is older than its
// maximum staleness.
var ErrStale = errors.New("cfgcanary sdk: config is stale and the config is fail-closed")

type snapshot[T any] struct {
	value    T
	info     Info
	entry    cache.Entry
	maxStale time.Duration
	// published is monotonic (time.Now carries a monotonic reading).
	published time.Time
}

// Client serves one config.
type Client[T any] struct {
	opt    Options[T]
	dir    *cache.Dir
	trust  *sign.Bundle
	log    *slog.Logger
	cur    atomic.Pointer[snapshot[T]]
	reload chan struct{}
	cancel context.CancelFunc
	done   chan struct{}

	mu          sync.Mutex // guards state below (loop goroutine and MarkHealthy)
	state       cache.SDKState
	seq         int64
	trialSince  time.Time
	lastBeat    int64
	lastContact time.Time
	closeOnce   sync.Once
}

// Open loads the config. It returns once a snapshot is published, or a
// *InitError when none can be (after waiting for ctx when there is neither a
// cached config nor a bootstrap).
func Open[T any](ctx context.Context, opt Options[T]) (*Client[T], error) {
	if opt.Parse == nil || opt.Config == "" || opt.Environment == "" || opt.Dir == "" {
		return nil, errors.New("cfgcanary sdk: Dir, Config, Environment and Parse are required")
	}
	if opt.Clone == nil && !opt.AllowShared {
		if path, ok := mutable(reflect.TypeFor[T]()); ok {
			return nil, fmt.Errorf("cfgcanary sdk: %v contains mutable references (%s); set Options.Clone or AllowShared", reflect.TypeFor[T](), path)
		}
	}
	if opt.FailClosed && opt.Bootstrap != nil && !opt.BootstrapDenyAll {
		return nil, errors.New("cfgcanary sdk: a fail-closed config only accepts a Bootstrap marked BootstrapDenyAll")
	}
	// A broken bootstrap is a programming error: report it now, not on the
	// cold start where it would be needed.
	if opt.Bootstrap != nil {
		if _, err := opt.Parse(opt.Bootstrap); err != nil {
			return nil, fmt.Errorf("cfgcanary sdk: bootstrap value rejected by the parser: %w", err)
		}
	}
	if opt.CrashLoopThreshold <= 0 {
		opt.CrashLoopThreshold = 3
	}
	if opt.HealthyAfter <= 0 {
		opt.HealthyAfter = 30 * time.Second
	}
	if opt.PollInterval <= 0 {
		opt.PollInterval = 250 * time.Millisecond
	}
	if opt.MaxBytes <= 0 {
		opt.MaxBytes = 4 << 20
	}
	if opt.Logger == nil {
		opt.Logger = slog.New(slog.DiscardHandler)
	}
	var trust *sign.Bundle
	var err error
	switch {
	case opt.TrustBundle != nil:
		trust, err = sign.ParseBundle(opt.TrustBundle)
	case opt.TrustBundleFile != "":
		trust, err = sign.LoadBundle(opt.TrustBundleFile)
	default:
		err = errors.New("a trust bundle is required")
	}
	if err != nil {
		return nil, fmt.Errorf("cfgcanary sdk: %w", err)
	}
	dir, err := cache.Open(opt.Dir, opt.Config)
	if err != nil {
		return nil, fmt.Errorf("cfgcanary sdk: %w", err)
	}
	c := &Client[T]{opt: opt, dir: dir, trust: trust, log: opt.Logger.With("config", opt.Config),
		reload: make(chan struct{}, 1), done: make(chan struct{})}
	c.state = dir.ReadSDKState()
	if st, err := dir.ReadSDKStatus(); err == nil {
		c.seq = st.Seq
	}
	c.lastBeat = cache.ReadHeartbeat(opt.Dir)
	c.lastContact = time.Now()

	lastErr := c.start()
	if c.cur.Load() == nil {
		// Wait for a first usable config.
		t := time.NewTicker(opt.PollInterval)
		defer t.Stop()
	wait:
		for c.cur.Load() == nil {
			select {
			case <-ctx.Done():
				reason := "no cached config and no bootstrap value"
				if lastErr != nil {
					reason = "no usable config"
				}
				return nil, &InitError{Config: opt.Config, Reason: reason, Err: errors.Join(lastErr, ctx.Err())}
			case <-t.C:
				if err := c.poll(); err != nil {
					lastErr = err
				}
				if c.cur.Load() != nil {
					break wait
				}
			}
		}
	}
	lctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel
	go c.loop(lctx)
	return c, nil
}

// start runs the startup sequence: crash-loop accounting, then the first
// usable of current, last-known-good, previous, bootstrap.
func (c *Client[T]) start() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	p, _, perr := c.dir.ReadPointer()
	var lastErr error
	if perr != nil && !errors.Is(perr, cache.ErrNoPointer) {
		lastErr = perr
	}
	cur := p.Current
	// Crash-loop accounting: a process start while serving an unproven trial.
	if !cur.Zero() && same(c.state.Trial, cur) && !sameEntry(c.state.Accepted, cur) {
		c.state.TrialStarts++
		if c.state.TrialStarts > c.opt.CrashLoopThreshold {
			c.state.Quarantined = markOf(cur)
			c.log.Error("config crash-looped; quarantined, serving last-known-good", "version", cur.Version, "generation", cur.Generation, "starts", c.state.TrialStarts)
			c.metricsCrashLoop()
			c.writeStatus(cache.SDKStatus{Generation: cur.Generation, Version: cur.Version, State: "CRASH_LOOP",
				Reason: fmt.Sprintf("%d starts without becoming healthy", c.state.TrialStarts)})
		}
		_ = c.dir.WriteSDKState(c.state)
	}
	// Candidates in order of preference.
	type cand struct {
		e     cache.Entry
		trial bool
	}
	var cands []cand
	if !cur.Zero() && !same(c.state.Quarantined, cur) && !same(c.state.Rejected, cur) {
		cands = append(cands, cand{cur, !sameContent(c.state.Accepted, cur)})
	}
	if !c.state.Accepted.Zero() {
		cands = append(cands, cand{c.state.Accepted, false})
	}
	if !p.Previous.Zero() && !same(c.state.Quarantined, p.Previous) {
		cands = append(cands, cand{p.Previous, false})
	}
	for _, k := range cands {
		v, err := c.dir.Verify(c.trust, k.e, c.opt.Environment, c.opt.MaxBytes)
		if err != nil {
			lastErr = err
			c.log.Warn("cached config failed verification", "version", k.e.Version, "err", err)
			continue
		}
		val, err := c.opt.Parse(v.Payload)
		if err != nil {
			lastErr = err
			c.metricsValidationFailure()
			if k.e.Generation == cur.Generation && k.e.SHA256 == cur.SHA256 {
				c.state.Rejected = markOf(cur)
				c.writeStatus(cache.SDKStatus{Generation: cur.Generation, Version: cur.Version, State: "REJECTED", Reason: reason(err)})
			}
			continue
		}
		c.publish(val, v, "cache")
		if k.trial {
			if !same(c.state.Trial, k.e) {
				c.state.Trial, c.state.TrialStarts = markOf(k.e), 1
			}
			c.trialSince = time.Now()
		} else if !sameEntry(c.state.Accepted, k.e) && sameContent(c.state.Accepted, k.e) {
			c.state.Accepted = k.e // newer generation of the same content
		} else if c.state.Accepted.Zero() {
			c.state.Accepted = k.e
		}
		if k.e.Generation == cur.Generation && k.e.SHA256 == cur.SHA256 {
			c.writeStatus(cache.SDKStatus{Generation: cur.Generation, Version: cur.Version, State: "ACCEPTED"})
		} else if !cur.Zero() && !same(c.state.Quarantined, cur) && !same(c.state.Rejected, cur) {
			c.writeStatus(cache.SDKStatus{Generation: cur.Generation, Version: cur.Version, State: "REJECTED", Reason: "current entry unusable"})
		} else {
			c.rewriteServing()
		}
		_ = c.dir.WriteSDKState(c.state)
		return nil
	}
	if c.opt.Bootstrap != nil {
		val, err := c.opt.Parse(c.opt.Bootstrap)
		if err != nil {
			return fmt.Errorf("bootstrap value rejected by the parser: %w", err)
		}
		c.cur.Store(&snapshot[T]{value: val, info: Info{Source: "bootstrap", FailClosed: c.opt.FailClosed}, published: time.Now()})
		c.updateInfoMetric(Info{Source: "bootstrap"})
		return nil
	}
	return lastErr
}

func reason(err error) string {
	s := err.Error()
	if len(s) > 200 {
		s = s[:200] + "..."
	}
	return s
}

func markOf(e cache.Entry) cache.Mark {
	return cache.Mark{Generation: e.Generation, Version: e.Version, SHA256: e.SHA256}
}
func same(m cache.Mark, e cache.Entry) bool {
	return m.Generation == e.Generation && m.SHA256 == e.SHA256 && m.Generation != 0
}
func sameEntry(a, b cache.Entry) bool {
	return a.Generation == b.Generation && a.SHA256 == b.SHA256 && !a.Zero()
}
func sameContent(a, b cache.Entry) bool { return !a.Zero() && a.SHA256 == b.SHA256 }

func (c *Client[T]) publish(val T, v cache.Verified, source string) {
	info := Info{Version: v.Entry.Version, Generation: v.Entry.Generation, ReleaseID: v.Delivery.ReleaseID, SHA256: v.Entry.SHA256,
		Source: source, FailClosed: c.opt.FailClosed || v.Delivery.FailureMode == "fail_closed"}
	snap := &snapshot[T]{value: val, info: info, entry: v.Entry, published: time.Now(),
		maxStale: time.Duration(v.Delivery.MaxStalenessSeconds) * time.Second}
	c.cur.Store(snap)
	c.updateInfoMetric(info)
	if c.opt.OnChange != nil {
		c.opt.OnChange(info)
	}
}

// rewriteServing refreshes the serving fields of the last status.
func (c *Client[T]) rewriteServing() {
	if st, err := c.dir.ReadSDKStatus(); err == nil {
		c.writeStatus(cache.SDKStatus{Generation: st.Generation, Version: st.Version, State: st.State, Reason: st.Reason})
	}
}

func (c *Client[T]) writeStatus(s cache.SDKStatus) {
	if snap := c.cur.Load(); snap != nil {
		s.ServingVersion, s.ServingGeneration = snap.info.Version, snap.info.Generation
	}
	c.seq++
	s.Seq = c.seq
	if err := c.dir.WriteSDKStatus(s); err != nil {
		c.log.Warn("could not write status for the agent", "err", err)
		return
	}
	if c.opt.OnStatus != nil {
		c.opt.OnStatus()
	}
}

func (c *Client[T]) loop(ctx context.Context) {
	defer close(c.done)
	t := time.NewTicker(c.opt.PollInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		case <-c.reload:
		}
		_ = c.poll()
	}
}

// poll checks for a new pointer, the heartbeat and the healthy timer.
func (c *Client[T]) poll() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if n := cache.ReadHeartbeat(c.opt.Dir); n != c.lastBeat {
		c.lastBeat, c.lastContact = n, time.Now()
	}
	snap := c.cur.Load()
	if snap != nil && !c.trialSince.IsZero() && time.Since(c.trialSince) >= c.opt.HealthyAfter {
		c.markHealthyLocked()
	}
	c.updateStaleMetric()
	p, _, err := c.dir.ReadPointer()
	if err != nil {
		return err
	}
	cur := p.Current
	if cur.Zero() || (snap != nil && snap.entry.Generation >= cur.Generation && snap.info.Source == "cache") {
		return nil
	}
	if same(c.state.Quarantined, cur) || same(c.state.Rejected, cur) {
		return nil
	}
	v, err := c.dir.Verify(c.trust, cur, c.opt.Environment, c.opt.MaxBytes)
	if err != nil {
		// The agent verified it; failing here means the cache changed under
		// us. Keep serving and let the agent's next apply fix it.
		c.log.Warn("new cached config failed verification", "version", cur.Version, "err", err)
		return err
	}
	if snap != nil && snap.info.Source == "cache" && snap.entry.SHA256 == cur.SHA256 {
		// Same content, newer generation (next ring, or a re-delivery): no
		// re-parse needed.
		c.publish(snap.value, v, "cache")
		if sameContent(c.state.Accepted, cur) {
			c.state.Accepted = cur
		} else if !c.state.Trial.Zero() {
			c.state.Trial = markOf(cur)
		}
		c.writeStatus(cache.SDKStatus{Generation: cur.Generation, Version: cur.Version, State: "ACCEPTED"})
		_ = c.dir.WriteSDKState(c.state)
		return nil
	}
	val, err := c.opt.Parse(v.Payload)
	if err != nil {
		c.metricsValidationFailure()
		c.state.Rejected = markOf(cur)
		c.log.Warn("parser rejected new config; keeping last-known-good", "version", cur.Version, "generation", cur.Generation, "err", err)
		c.writeStatus(cache.SDKStatus{Generation: cur.Generation, Version: cur.Version, State: "REJECTED", Reason: reason(err)})
		_ = c.dir.WriteSDKState(c.state)
		return err
	}
	c.publish(val, v, "cache")
	if sameContent(c.state.Accepted, cur) {
		c.state.Accepted, c.state.Trial, c.state.TrialStarts, c.trialSince = cur, cache.Mark{}, 0, time.Time{}
	} else {
		c.state.Trial, c.state.TrialStarts, c.trialSince = markOf(cur), 1, time.Now()
	}
	c.writeStatus(cache.SDKStatus{Generation: cur.Generation, Version: cur.Version, State: "ACCEPTED"})
	_ = c.dir.WriteSDKState(c.state)
	return nil
}

// Get returns the published snapshot. It never blocks and never fails.
func (c *Client[T]) Get() T {
	s := c.cur.Load()
	if c.opt.Clone != nil {
		return c.opt.Clone(s.value)
	}
	return s.value
}

// Info describes what Get returns.
func (c *Client[T]) Info() Info { return c.cur.Load().info }

// Stale reports whether the agent has not contacted the control plane for
// longer than the config's max staleness.
func (c *Client[T]) Stale() bool {
	s := c.cur.Load()
	c.mu.Lock()
	last := c.lastContact
	c.mu.Unlock()
	return s.maxStale > 0 && time.Since(last) > s.maxStale
}

// Usable is false when the config is fail-closed and stale: the application
// should then refuse or degrade the behaviour the config controls.
func (c *Client[T]) Usable() bool {
	return !(c.cur.Load().info.FailClosed && c.Stale())
}

// GetFresh returns the snapshot, or ErrStale when the config is fail-closed
// and stale.
func (c *Client[T]) GetFresh() (T, error) {
	if !c.Usable() {
		var zero T
		return zero, ErrStale
	}
	return c.Get(), nil
}

// MarkHealthy records that the application is healthy on the served config,
// making it the last-known-good for crash-loop fallback.
func (c *Client[T]) MarkHealthy() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.markHealthyLocked()
}

func (c *Client[T]) markHealthyLocked() {
	s := c.cur.Load()
	if s == nil || s.info.Source != "cache" || c.state.Trial.Zero() {
		return
	}
	c.state.Accepted, c.state.Trial, c.state.TrialStarts, c.trialSince = s.entry, cache.Mark{}, 0, time.Time{}
	_ = c.dir.WriteSDKState(c.state)
}

// Reload asks the client to check the cache now (agents in the same process
// call it after applying).
func (c *Client[T]) Reload() {
	select {
	case c.reload <- struct{}{}:
	default:
	}
}

// Close stops background work. Get keeps returning the last snapshot.
func (c *Client[T]) Close() {
	c.closeOnce.Do(func() {
		if c.cancel != nil {
			c.cancel()
			<-c.done
		}
		c.deleteInfoMetric()
	})
}

// mutable reports whether t can reach shared mutable memory.
func mutable(t reflect.Type) (string, bool) {
	seen := map[reflect.Type]bool{}
	var walk func(t reflect.Type, path string) (string, bool)
	walk = func(t reflect.Type, path string) (string, bool) {
		if seen[t] {
			return "", false
		}
		seen[t] = true
		switch t.Kind() {
		case reflect.Map, reflect.Slice, reflect.Pointer, reflect.Chan, reflect.Func, reflect.Interface, reflect.UnsafePointer:
			return path + " (" + t.Kind().String() + ")", true
		case reflect.Array:
			return walk(t.Elem(), path+"[]")
		case reflect.Struct:
			for i := range t.NumField() {
				f := t.Field(i)
				if p, ok := walk(f.Type, path+"."+f.Name); ok {
					return p, true
				}
			}
		}
		return "", false
	}
	return walk(t, t.String())
}
