// Package controller drives active releases. One replica leads at a time:
// leadership is a PostgreSQL advisory lock held on a dedicated connection,
// and every write carries the epoch claimed when the lock was acquired. The
// store rejects writes whose epoch is not current (docs/invariants.md I4).
package controller

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/attribute"

	pb "github.com/raunaksahahere/cfgcanary/api/gen/cfgcanary/v1"
	"github.com/raunaksahahere/cfgcanary/internal/gate"
	"github.com/raunaksahahere/cfgcanary/internal/policy"
	"github.com/raunaksahahere/cfgcanary/internal/rollout"
	"github.com/raunaksahahere/cfgcanary/internal/store"
	"github.com/raunaksahahere/cfgcanary/internal/telemetry"
)

// Config configures the controller.
type Config struct {
	// Holder identifies this replica in controller_leader.
	Holder string
	// Tick is the reconcile interval.
	Tick time.Duration
	// ProbeInterval is how often the lock connection is checked.
	ProbeInterval time.Duration
	// RetryInterval is how often a follower tries to take the lock.
	RetryInterval time.Duration
	// ReleaseTimeout bounds work on one release per tick.
	ReleaseTimeout time.Duration
	GCInterval     time.Duration
	GCRetain       int
	GCGrace        time.Duration
	// DeleteObject removes a blob (for GC); nil disables GC.
	DeleteObject func(ctx context.Context, sha string) error
	Prometheus   *gate.Prometheus
	Logger       *slog.Logger
	Registerer   prometheus.Registerer
}

// Controller is the release controller.
type Controller struct {
	cfg     Config
	store   *store.Store
	log     *slog.Logger
	m       *metrics
	leader  atomic.Bool
	epoch   atomic.Int64
	lockPID atomic.Int32

	mu         sync.Mutex
	lastActive map[string]string // target -> release id
}

// New creates a controller.
func New(st *store.Store, cfg Config) *Controller {
	if cfg.Tick <= 0 {
		cfg.Tick = 250 * time.Millisecond
	}
	if cfg.ProbeInterval <= 0 {
		cfg.ProbeInterval = time.Second
	}
	if cfg.RetryInterval <= 0 {
		cfg.RetryInterval = 2 * time.Second
	}
	if cfg.ReleaseTimeout <= 0 {
		cfg.ReleaseTimeout = 30 * time.Second
	}
	if cfg.GCInterval <= 0 {
		cfg.GCInterval = 10 * time.Minute
	}
	if cfg.GCRetain <= 0 {
		cfg.GCRetain = 10
	}
	if cfg.GCGrace <= 0 {
		cfg.GCGrace = time.Hour
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	return &Controller{cfg: cfg, store: st, log: cfg.Logger.With("component", "controller", "holder", cfg.Holder),
		m: newMetrics(cfg.Registerer), lastActive: map[string]string{}}
}

// Leader reports whether this replica leads, and its epoch.
func (c *Controller) Leader() (bool, int64) { return c.leader.Load(), c.epoch.Load() }

// LockBackendPID is the PostgreSQL backend holding the lock (for chaos tests).
func (c *Controller) LockBackendPID() int32 { return c.lockPID.Load() }

// Run campaigns for leadership and leads while elected, until ctx ends.
func (c *Controller) Run(ctx context.Context) {
	for ctx.Err() == nil {
		if err := c.campaign(ctx); err != nil && ctx.Err() == nil {
			c.log.Warn("leadership attempt ended", "err", err)
		}
		d := time.Duration((0.5 + rand.Float64()) * float64(c.cfg.RetryInterval))
		select {
		case <-ctx.Done():
		case <-time.After(d):
		}
	}
}

func (c *Controller) campaign(ctx context.Context) error {
	cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	conn, err := pgx.Connect(cctx, c.store.DSN())
	cancel()
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())
	var got bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, store.LockController).Scan(&got); err != nil || !got {
		return err
	}
	var pid int32
	_ = conn.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&pid)
	epoch, err := store.ClaimEpoch(ctx, conn, c.cfg.Holder)
	if err != nil {
		return err
	}
	lctx, stop := context.WithCancel(ctx)
	defer stop()
	c.epoch.Store(epoch)
	c.lockPID.Store(pid)
	c.leader.Store(true)
	c.m.leader.Set(1)
	c.m.epoch.Set(float64(epoch))
	c.log.Info("became leader", "epoch", epoch)
	defer func() {
		c.leader.Store(false)
		c.m.leader.Set(0)
		c.log.Info("leadership ended", "epoch", epoch)
	}()
	// The lock lives exactly as long as this session: probe it, and stop
	// leading the moment it fails.
	probeDone := make(chan error, 1)
	go func() {
		t := time.NewTicker(c.cfg.ProbeInterval)
		defer t.Stop()
		for {
			select {
			case <-lctx.Done():
				probeDone <- nil
				return
			case <-t.C:
			}
			pctx, cancel := context.WithTimeout(lctx, c.cfg.ProbeInterval*2)
			var one int
			err := conn.QueryRow(pctx, `SELECT 1`).Scan(&one)
			cancel()
			if err != nil && lctx.Err() == nil {
				c.log.Error("lost the leadership lock connection; stopping", "err", err)
				stop()
				probeDone <- err
				return
			}
		}
	}()
	err = c.lead(lctx, epoch)
	stop()
	<-probeDone
	return err
}

func (c *Controller) lead(ctx context.Context, epoch int64) error {
	tick := time.NewTicker(c.cfg.Tick)
	defer tick.Stop()
	gc := time.NewTicker(c.cfg.GCInterval)
	defer gc.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-gc.C:
			if c.cfg.DeleteObject != nil {
				st, err := c.store.GC(ctx, c.cfg.GCRetain, c.cfg.GCGrace, c.cfg.DeleteObject)
				c.log.Info("garbage collection", "versions_purged", st.VersionsPurged, "blobs_deleted", st.BlobsDeleted, "errors", st.Errors, "err", err)
				_ = c.store.PruneIdempotency(ctx, 7*24*time.Hour)
			}
		case <-tick.C:
			if err := c.Reconcile(ctx, epoch); errors.Is(err, store.ErrFenced) {
				c.log.Error("fenced: another controller holds a newer epoch", "epoch", epoch)
				return err
			}
		}
	}
}

// Reconcile performs one pass over active releases.
func (c *Controller) Reconcile(ctx context.Context, epoch int64) error {
	rels, err := c.store.ActiveReleases(ctx)
	if err != nil {
		c.m.errors.Inc()
		return err
	}
	active := map[string]string{}
	for _, r := range rels {
		active[r.Config+"/"+r.Env] = r.ID
		rctx, cancel := context.WithTimeout(ctx, c.cfg.ReleaseTimeout)
		err := c.reconcileOne(rctx, epoch, r)
		cancel()
		if errors.Is(err, store.ErrFenced) {
			return err
		}
		if err != nil && !errors.Is(err, store.ErrConflict) && !errors.Is(err, store.ErrPrecondition) && ctx.Err() == nil {
			c.m.errors.Inc()
			c.log.Warn("reconcile failed", "release", r.ID, "err", err)
		}
	}
	c.finished(ctx, active)
	return nil
}

func (c *Controller) finished(ctx context.Context, active map[string]string) {
	c.mu.Lock()
	prev := c.lastActive
	c.lastActive = active
	c.mu.Unlock()
	for target, id := range prev {
		if active[target] == id {
			continue
		}
		if r, err := c.store.ReleaseRow(ctx, id); err == nil {
			c.m.observeFinal(r)
		}
	}
}

func (c *Controller) reconcileOne(ctx context.Context, epoch int64, r *store.ReleaseRow) (err error) {
	ctx, span := telemetry.Tracer().Start(ctx, "reconcile release")
	span.SetAttributes(attribute.String("cfgcanary.release", r.ID), attribute.String("cfgcanary.state", string(r.R.State)),
		attribute.Int("cfgcanary.ring", int(r.R.ActiveRing)), attribute.Int64("cfgcanary.epoch", epoch))
	defer func() {
		if err != nil {
			span.RecordError(err)
		}
		span.End()
	}()
	counts, err := c.store.Counts(ctx, r)
	if err != nil {
		return err
	}
	cnt, rb := store.Summarize(r, counts)
	now, err := c.store.Now(ctx)
	if err != nil {
		return err
	}
	c.m.observe(r, counts, rb)
	pol := policy.Rollout(r.Policy)
	act := rollout.Decide(r.R, pol, cnt, rb, now)
	base := store.EventMeta{Actor: "controller", Epoch: epoch}
	if act.EvaluateGate {
		last := r.R.ActiveRing == int32(len(r.Policy.Rings))-1
		req := gate.Request{Config: r.Config, Environment: r.Env, Release: r.ID, Candidate: r.R.Candidate, Baseline: r.R.Baseline,
			WindowStart: r.R.BakeStarted, Now: now, Policy: r.Policy.Gate, Look: int(r.R.LooksDone) + 1,
			HardOnly: last && r.Policy.FinalStage == pb.FinalStage_FINAL_STAGE_HARD_THRESHOLDS_ONLY}
		var ev gate.Evidence
		if c.cfg.Prometheus == nil {
			ev = gate.Evidence{Verdict: gate.Inconclusive, Reason: "no Prometheus configured", Look: req.Look, Looks: int(r.Policy.Gate.Looks)}
		} else {
			gctx, gspan := telemetry.Tracer().Start(ctx, "gate evaluate")
			ev = gate.Evaluate(gctx, c.cfg.Prometheus, req)
			gspan.SetAttributes(attribute.String("cfgcanary.verdict", string(ev.Verdict)), attribute.Int("cfgcanary.look", req.Look))
			gspan.End()
		}
		raw, _ := json.Marshal(ev)
		meta := base
		meta.Gate = &store.GateRecord{Ring: r.R.ActiveRing, Look: int32(req.Look), Verdict: string(ev.Verdict), Reason: ev.Reason, Evidence: raw}
		meta.Message = "gate look " + string(ev.Verdict) + ": " + ev.Reason
		_, _, err := c.store.ApplyEvent(ctx, r.ID, rollout.Event{Kind: rollout.EvGateLook, Verdict: rollout.Verdict(ev.Verdict),
			ExpectStateVersion: r.R.StateVersion}, meta)
		if err == nil {
			c.m.gate(r, string(ev.Verdict))
			c.log.Info("gate look", "release", r.ID, "ring", r.R.ActiveRing, "look", req.Look, "verdict", ev.Verdict, "reason", ev.Reason)
		}
		return err
	}
	if act.Event == "" {
		return nil
	}
	meta := base
	meta.Message = act.Reason
	ev := rollout.Event{Kind: act.Event, Reason: act.Reason, ExpectStateVersion: r.R.StateVersion, Converged: act.Converged}
	if act.Event == rollout.EvFinishRollback {
		meta.Rollback = &pb.RollbackStatus{FallbackVersion: r.R.Baseline, Generation: r.R.RollbackGeneration, Exposed: rb.Exposed,
			Confirmed: rb.Confirmed, Pending: rb.Pending, Unreachable: rb.Unreachable, Converged: act.Converged}
		meta.Detail = map[string]any{"exposed": rb.Exposed, "confirmed": rb.Confirmed, "unreachable": rb.Unreachable, "converged": act.Converged}
	}
	if act.Event == rollout.EvAutoRollback {
		meta.Detail = map[string]any{"sdk_rejections": cnt.Rejected, "crash_loops": cnt.CrashLoop}
	}
	_, _, err = c.store.ApplyEvent(ctx, r.ID, ev, meta)
	if err == nil {
		c.m.transition(r, string(act.Event))
		if act.Event == rollout.EvFinishRollback {
			c.m.rollbackSeconds.WithLabelValues(r.Config, r.Env).Observe(now.Sub(r.R.RollbackStarted).Seconds())
		}
		c.log.Info("release transition", "release", r.ID, "event", act.Event, "reason", act.Reason, "ring", r.R.ActiveRing)
	}
	return err
}
