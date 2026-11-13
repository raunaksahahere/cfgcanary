package agent

import "github.com/prometheus/client_golang/prometheus"

// Metrics are shared by every agent in a process (the simulator runs many),
// so they carry no instance label and their cardinality is bounded by the
// number of configs.
type Metrics struct {
	connected  prometheus.Gauge
	reconnects prometheus.Counter
	applies    *prometheus.CounterVec
	stale      *prometheus.CounterVec
	generation *prometheus.GaugeVec
	reports    *prometheus.CounterVec
}

// NewMetrics registers agent metrics with reg (nil: unregistered).
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		connected: prometheus.NewGauge(prometheus.GaugeOpts{Name: "cfgcanary_agent_streams_connected",
			Help: "Watch streams currently open from this process."}),
		reconnects: prometheus.NewCounter(prometheus.CounterOpts{Name: "cfgcanary_agent_reconnects_total",
			Help: "Watch stream reconnect attempts."}),
		applies: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "cfgcanary_agent_applies_total",
			Help: "Updates processed, by result (applied, failed_<reason>)."}, []string{"config", "result"}),
		stale: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "cfgcanary_agent_stale_updates_total",
			Help: "Updates dropped because their generation was not newer than the applied one."}, []string{"config"}),
		generation: prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "cfgcanary_agent_applied_generation",
			Help: "Generation last applied (the maximum across agents in this process)."}, []string{"config"}),
		reports: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "cfgcanary_agent_status_reports_total",
			Help: "Status report RPCs by result."}, []string{"result"}),
	}
	if reg != nil {
		reg.MustRegister(m.connected, m.reconnects, m.applies, m.stale, m.generation, m.reports)
	}
	return m
}
