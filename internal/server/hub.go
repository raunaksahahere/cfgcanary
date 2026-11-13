package server

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/raunaksahahere/cfgcanary/api/gen/cfgcanary/v1"
	"github.com/raunaksahahere/cfgcanary/internal/policy"
	"github.com/raunaksahahere/cfgcanary/internal/ring"
	"github.com/raunaksahahere/cfgcanary/internal/sign"
	"github.com/raunaksahahere/cfgcanary/internal/store"
)

type targetKey struct{ config, env string }

func (k targetKey) String() string { return k.config + "/" + k.env }

// snapshot is the desired state of one target at one generation, with
// everything needed to build updates cheaply for thousands of streams.
type snapshot struct {
	row       *store.TargetRow
	rp        ring.Policy
	hasPolicy bool
	versions  map[int64]*pb.ConfigVersion
	failMode  string
	maxStale  int64

	mu         sync.Mutex
	deliveries map[[3]int64]sign.Signed // (version, ringLo, ringHi)
}

// ringOf returns the server-assigned ring of an instance.
func (s *snapshot) ringOf(instance string) int32 {
	if !s.hasPolicy {
		return 0
	}
	return s.rp.Assign(instance)
}

// rangeFor returns the ring range a delivery of version v is valid for.
func (s *snapshot) rangeFor(v int64) (int32, int32) {
	t := s.row.T
	if t.Candidate != 0 && v == t.Candidate {
		return 0, t.ActiveRing
	}
	if t.Candidate != 0 {
		return t.ActiveRing + 1, ring.Holdout
	}
	return 0, ring.Holdout
}

type targetEntry struct {
	mu      sync.Mutex // serializes loads
	snap    *snapshot
	stale   bool
	streams map[*watcher]struct{}
}

// Hub fans desired state out to watch streams.
type Hub struct {
	srv *Server
	log *slog.Logger

	mu      sync.Mutex
	targets map[targetKey]*targetEntry
	// connected instances per environment, for last_seen updates.
	connected map[string]map[string]int
}

func newHub(s *Server) *Hub {
	return &Hub{srv: s, log: s.log.With("component", "hub"), targets: map[targetKey]*targetEntry{}, connected: map[string]map[string]int{}}
}

func (h *Hub) entry(k targetKey) *targetEntry {
	h.mu.Lock()
	defer h.mu.Unlock()
	e := h.targets[k]
	if e == nil {
		e = &targetEntry{streams: map[*watcher]struct{}{}, stale: true}
		h.targets[k] = e
	}
	return e
}

// snapshot returns the current snapshot of a target, loading it if stale.
func (h *Hub) snapshot(ctx context.Context, k targetKey) (*snapshot, error) {
	e := h.entry(k)
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.snap != nil && !e.stale {
		return e.snap, nil
	}
	snap, err := h.load(ctx, k)
	if err != nil {
		return nil, err
	}
	e.snap, e.stale = snap, false
	return snap, nil
}

func (h *Hub) load(ctx context.Context, k targetKey) (*snapshot, error) {
	st := h.srv.store
	row, err := st.TargetRow(ctx, k.config, k.env)
	if err != nil {
		return nil, err
	}
	cfg, err := st.GetConfig(ctx, k.config)
	if err != nil {
		return nil, err
	}
	s := &snapshot{row: row, versions: map[int64]*pb.ConfigVersion{}, deliveries: map[[3]int64]sign.Signed{},
		failMode: "fail_static", maxStale: int64(cfg.Spec.GetMaxStaleness().AsDuration().Seconds())}
	if cfg.Spec.FailureMode == pb.FailureMode_FAILURE_MODE_FAIL_CLOSED {
		s.failMode = "fail_closed"
	}
	if len(row.RingPolicy.GetRings()) > 0 {
		s.rp, s.hasPolicy = policy.Ring(row.RingPolicy), true
	}
	for _, v := range []int64{row.T.Baseline, row.T.Candidate} {
		if v == 0 {
			continue
		}
		cv, err := st.GetVersion(ctx, k.config, v)
		if err != nil {
			return nil, err
		}
		s.versions[v] = cv
	}
	return s, nil
}

// invalidate marks a target stale and wakes its streams.
func (h *Hub) invalidate(k targetKey) {
	e := h.entry(k)
	e.mu.Lock()
	e.stale = true
	ws := make([]*watcher, 0, len(e.streams))
	for w := range e.streams {
		ws = append(ws, w)
	}
	e.mu.Unlock()
	for _, w := range ws {
		w.markDirty(k.config)
	}
	h.srv.metrics.notifications.Inc()
}

func (h *Hub) invalidateAll() {
	h.mu.Lock()
	keys := make([]targetKey, 0, len(h.targets))
	for k := range h.targets {
		keys = append(keys, k)
	}
	h.mu.Unlock()
	for _, k := range keys {
		h.invalidate(k)
	}
}

func (h *Hub) register(w *watcher) {
	for _, c := range w.configs {
		e := h.entry(targetKey{c, w.env})
		e.mu.Lock()
		e.streams[w] = struct{}{}
		e.mu.Unlock()
	}
	h.mu.Lock()
	m := h.connected[w.env]
	if m == nil {
		m = map[string]int{}
		h.connected[w.env] = m
	}
	m[w.instance]++
	h.mu.Unlock()
}

func (h *Hub) unregister(w *watcher) {
	for _, c := range w.configs {
		e := h.entry(targetKey{c, w.env})
		e.mu.Lock()
		delete(e.streams, w)
		e.mu.Unlock()
	}
	h.mu.Lock()
	if m := h.connected[w.env]; m != nil {
		if m[w.instance]--; m[w.instance] <= 0 {
			delete(m, w.instance)
		}
	}
	h.mu.Unlock()
}

// update builds the update an instance should receive for a target.
func (h *Hub) update(ctx context.Context, k targetKey, instance string) (*pb.ConfigUpdate, error) {
	s, err := h.snapshot(ctx, k)
	if err != nil {
		return nil, err
	}
	return h.build(ctx, k, s, s.ringOf(instance))
}

// updateForRing builds what an instance pinned to ring r would receive.
func (h *Hub) updateForRing(ctx context.Context, k targetKey, r int32) (*pb.ConfigUpdate, error) {
	s, err := h.snapshot(ctx, k)
	if err != nil {
		return nil, err
	}
	return h.build(ctx, k, s, r)
}

func (h *Hub) build(ctx context.Context, k targetKey, s *snapshot, r int32) (*pb.ConfigUpdate, error) {
	var err error
	v := s.row.T.DesiredFor(r)
	u := &pb.ConfigUpdate{Config: k.config, Environment: k.env, Generation: s.row.T.Generation, Ring: r, ReleaseId: s.row.ReleaseID}
	if v == 0 {
		u.Kind = pb.ConfigUpdate_KIND_NO_CONFIG
		return u, nil
	}
	cv := s.versions[v]
	if cv == nil {
		return nil, errors.New("desired version is not loaded")
	}
	lo, hi := s.rangeFor(v)
	key := [3]int64{v, int64(lo), int64(hi)}
	s.mu.Lock()
	del, ok := s.deliveries[key]
	s.mu.Unlock()
	if !ok {
		del, err = h.srv.deliverySigner.Sign(sign.Delivery{Config: k.config, Environment: k.env, Generation: s.row.T.Generation,
			Version: v, SHA256: cv.Sha256, ReleaseID: s.row.ReleaseID, RingLo: lo, RingHi: hi,
			FailureMode: s.failMode, MaxStalenessSeconds: s.maxStale})
		if err != nil {
			return nil, err
		}
		s.mu.Lock()
		s.deliveries[key] = del
		s.mu.Unlock()
	}
	url, exp, err := h.srv.presign(ctx, cv.Sha256)
	if err != nil {
		return nil, err
	}
	u.Kind, u.Version, u.Sha256, u.Size = pb.ConfigUpdate_KIND_DESIRED, v, cv.Sha256, cv.Size
	u.Url, u.UrlExpires = url, timestamppb.New(exp)
	u.Artifact = cv.Artifact
	u.Delivery = &pb.SignedStatement{Payload: del.Payload, Signature: del.Signature, KeyId: del.KeyID}
	return u, nil
}

// listen wakes streams on PostgreSQL notifications. After any interruption
// every target is treated as changed, because notifications may have been
// missed; correctness never depends on receiving them.
func (h *Hub) listen(ctx context.Context) {
	attempt := 0
	for ctx.Err() == nil {
		err := h.listenOnce(ctx)
		if ctx.Err() != nil {
			return
		}
		h.log.Warn("notification listener interrupted; falling back to sweeps until it reconnects", "err", err)
		h.invalidateAll()
		d := h.srv.backoff.Delay(attempt)
		attempt++
		select {
		case <-ctx.Done():
			return
		case <-time.After(d):
		}
	}
}

func (h *Hub) listenOnce(ctx context.Context) error {
	cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	conn, err := pgx.Connect(cctx, h.srv.store.DSN())
	cancel()
	if err != nil {
		return err
	}
	defer conn.Close(context.WithoutCancel(ctx))
	if _, err := conn.Exec(ctx, "LISTEN "+store.NotifyChannel); err != nil {
		return err
	}
	h.invalidateAll() // catch up on anything missed while not listening
	for {
		n, err := conn.WaitForNotification(ctx)
		if err != nil {
			return err
		}
		config, env, ok := strings.Cut(n.Payload, "/")
		if ok {
			h.invalidate(targetKey{config, env})
		}
	}
}

// sweep periodically reloads targets that have streams and wakes them if the
// generation changed, and refreshes last_seen of connected instances.
func (h *Hub) sweep(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	touch := time.NewTicker(h.srv.cfg.TouchInterval)
	defer touch.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-touch.C:
			h.touch(ctx)
			continue
		case <-t.C:
		}
		h.mu.Lock()
		type item struct {
			k targetKey
			e *targetEntry
		}
		var items []item
		for k, e := range h.targets {
			items = append(items, item{k, e})
		}
		h.mu.Unlock()
		for _, it := range items {
			it.e.mu.Lock()
			n := len(it.e.streams)
			var gen int64 = -1
			if it.e.snap != nil {
				gen = it.e.snap.row.T.Generation
			}
			it.e.mu.Unlock()
			if n == 0 {
				continue
			}
			lctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			row, err := h.srv.store.TargetRow(lctx, it.k.config, it.k.env)
			cancel()
			if err == nil && row.T.Generation != gen {
				h.invalidate(it.k)
			}
		}
	}
}

func (h *Hub) touch(ctx context.Context) {
	h.mu.Lock()
	batch := map[string][]string{}
	for env, m := range h.connected {
		for id := range m {
			batch[env] = append(batch[env], id)
		}
	}
	h.mu.Unlock()
	for env, ids := range batch {
		for len(ids) > 0 {
			n := min(len(ids), 1000)
			tctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			if err := h.srv.store.TouchInstances(tctx, env, ids[:n]); err != nil {
				h.log.Debug("touch failed", "err", err)
			}
			cancel()
			ids = ids[n:]
		}
	}
}

// streamCount returns the number of open streams.
func (h *Hub) streamCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for _, m := range h.connected {
		for _, c := range m {
			n += c
		}
	}
	return n
}
