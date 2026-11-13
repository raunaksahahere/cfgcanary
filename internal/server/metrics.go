package server

import "github.com/prometheus/client_golang/prometheus"

type metrics struct {
	reg           prometheus.Registerer
	admissions    *prometheus.CounterVec
	sent          *prometheus.CounterVec
	slow          prometheus.Counter
	acks          *prometheus.CounterVec
	notifications prometheus.Counter
	denied        *prometheus.CounterVec
	submissions   *prometheus.CounterVec
	rpcs          *prometheus.CounterVec
}

func newMetrics(reg prometheus.Registerer) *metrics {
	m := &metrics{
		reg: reg,
		admissions: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "cfgcanary_server_stream_admissions_total",
			Help: "Watch stream admission decisions."}, []string{"result"}),
		sent: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "cfgcanary_server_updates_sent_total",
			Help: "Updates sent on watch streams."}, []string{"kind"}),
		slow: prometheus.NewCounter(prometheus.CounterOpts{Name: "cfgcanary_server_slow_consumers_total",
			Help: "Streams closed because the client could not take a message within the send timeout."}),
		acks: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "cfgcanary_server_acks_total",
			Help: "Acknowledgement rows by outcome."}, []string{"result"}),
		notifications: prometheus.NewCounter(prometheus.CounterOpts{Name: "cfgcanary_server_target_invalidations_total",
			Help: "Desired-state change notifications processed."}),
		denied: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "cfgcanary_server_denied_total",
			Help: "Requests refused by authorization."}, []string{"action"}),
		submissions: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "cfgcanary_server_submissions_total",
			Help: "Version submissions by result and first failing stage."}, []string{"result", "stage"}),
		rpcs: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "cfgcanary_server_agent_rpcs_total",
			Help: "Agent RPCs by method and result."}, []string{"method", "result"}),
	}
	if reg != nil {
		reg.MustRegister(m.admissions, m.sent, m.slow, m.acks, m.notifications, m.denied, m.submissions, m.rpcs)
	}
	return m
}

func (m *metrics) streamsFn(f func() int) {
	if m.reg == nil {
		return
	}
	m.reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "cfgcanary_server_streams",
		Help: "Open watch streams on this replica."}, func() float64 { return float64(f()) }))
}
