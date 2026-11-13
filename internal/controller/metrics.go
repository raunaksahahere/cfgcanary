package controller

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/raunaksahahere/cfgcanary/internal/ring"
	"github.com/raunaksahahere/cfgcanary/internal/rollout"
	"github.com/raunaksahahere/cfgcanary/internal/store"
)

// Cardinality: every series is per (config, environment), with at most 16
// rings, 7 statuses and 7 states, and per-release series are reset when the
// release ends.
type metrics struct {
	leader, epoch   prometheus.Gauge
	errors          prometheus.Counter
	activeRing      *prometheus.GaugeVec
	state           *prometheus.GaugeVec
	ringInstances   *prometheus.GaugeVec
	rollback        *prometheus.GaugeVec
	looks           *prometheus.CounterVec
	lastVerdict     *prometheus.GaugeVec
	transitions     *prometheus.CounterVec
	rollbackSeconds *prometheus.HistogramVec
}

var allStates = []rollout.State{rollout.Pending, rollout.Rolling, rollout.Baking, rollout.Halted, rollout.RollingBack, rollout.RolledBack, rollout.Complete}

func newMetrics(reg prometheus.Registerer) *metrics {
	m := &metrics{
		leader: prometheus.NewGauge(prometheus.GaugeOpts{Name: "cfgcanary_controller_is_leader", Help: "1 when this replica leads."}),
		epoch:  prometheus.NewGauge(prometheus.GaugeOpts{Name: "cfgcanary_controller_epoch", Help: "Fencing epoch claimed by this replica."}),
		errors: prometheus.NewCounter(prometheus.CounterOpts{Name: "cfgcanary_controller_reconcile_errors_total", Help: "Reconcile errors."}),
		activeRing: prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "cfgcanary_release_active_ring",
			Help: "Highest ring exposed to the candidate (-1 when none)."}, []string{"config", "environment"}),
		state: prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "cfgcanary_release_state",
			Help: "1 for the state of the latest release of a target."}, []string{"config", "environment", "state"}),
		ringInstances: prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "cfgcanary_release_ring_instances",
			Help: "Instances per ring of the active release, by status."}, []string{"config", "environment", "ring", "status"}),
		rollback: prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "cfgcanary_release_rollback_instances",
			Help: "Rollback convergence of exposed instances."}, []string{"config", "environment", "status"}),
		looks: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "cfgcanary_gate_looks_total",
			Help: "Gate looks by verdict."}, []string{"config", "environment", "verdict"}),
		lastVerdict: prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "cfgcanary_gate_last_verdict",
			Help: "Last gate verdict: 1 pass, 0 inconclusive, -1 fail."}, []string{"config", "environment"}),
		transitions: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "cfgcanary_release_transitions_total",
			Help: "Controller-driven release transitions."}, []string{"config", "environment", "event"}),
		rollbackSeconds: prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "cfgcanary_rollback_duration_seconds",
			Help:    "Time from rollback start until every reachable exposed instance confirmed the fallback (or timeout).",
			Buckets: []float64{0.5, 1, 2, 5, 10, 20, 30, 60, 120, 300}}, []string{"config", "environment"}),
	}
	if reg != nil {
		reg.MustRegister(m.leader, m.epoch, m.errors, m.activeRing, m.state, m.ringInstances, m.rollback, m.looks, m.lastVerdict, m.transitions, m.rollbackSeconds)
	}
	return m
}

func (m *metrics) setState(r *store.ReleaseRow) {
	for _, s := range allStates {
		v := 0.0
		if s == r.R.State {
			v = 1
		}
		m.state.WithLabelValues(r.Config, r.Env, string(s)).Set(v)
	}
	m.activeRing.WithLabelValues(r.Config, r.Env).Set(float64(r.R.ActiveRing))
}

func (m *metrics) observe(r *store.ReleaseRow, counts []store.RingCounts, rb rollout.RollbackCounts) {
	m.setState(r)
	for _, c := range counts {
		name := strconv.Itoa(int(c.Ring))
		if c.Ring == ring.Holdout {
			name = "holdout"
		}
		for status, v := range map[string]int32{"eligible": c.C.Eligible, "reachable": c.C.Reachable, "applied": c.C.Applied,
			"accepted": c.C.Accepted, "rejected": c.C.Rejected, "failed": c.C.Failed, "crash_loop": c.C.CrashLoop} {
			m.ringInstances.WithLabelValues(r.Config, r.Env, name, status).Set(float64(v))
		}
	}
	for status, v := range map[string]int32{"exposed": rb.Exposed, "confirmed": rb.Confirmed, "pending": rb.Pending, "unreachable": rb.Unreachable} {
		m.rollback.WithLabelValues(r.Config, r.Env, status).Set(float64(v))
	}
}

func (m *metrics) observeFinal(r *store.ReleaseRow) {
	m.setState(r)
	m.ringInstances.DeletePartialMatch(prometheus.Labels{"config": r.Config, "environment": r.Env})
}

func (m *metrics) gate(r *store.ReleaseRow, verdict string) {
	m.looks.WithLabelValues(r.Config, r.Env, verdict).Inc()
	v := 0.0
	switch verdict {
	case "PASS":
		v = 1
	case "FAIL":
		v = -1
	}
	m.lastVerdict.WithLabelValues(r.Config, r.Env).Set(v)
}

func (m *metrics) transition(r *store.ReleaseRow, event string) {
	m.transitions.WithLabelValues(r.Config, r.Env, event).Inc()
}
