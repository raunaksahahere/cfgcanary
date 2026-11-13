// Package sim runs fleets of real agents, real SDK clients and a simulated
// sample application in one process. It is the basis of the Compose demo,
// the integration tests and the scale benchmarks: the delivery path is the
// production code; only the workload is synthetic.
package sim

import (
	"math"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/raunaksahahere/cfgcanary/examples/ratelimits"
)

// AppMetrics are the sample application's request metrics, labelled by
// instance so the gate can compare cohorts.
type AppMetrics struct {
	Requests *prometheus.CounterVec
	Errors   *prometheus.CounterVec
	Duration *prometheus.HistogramVec
	Restarts *prometheus.CounterVec
}

// NewAppMetrics registers the sample application's metrics.
func NewAppMetrics(reg prometheus.Registerer) *AppMetrics {
	m := &AppMetrics{
		Requests: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "demo_requests_total",
			Help: "Requests handled by the sample application."}, []string{"cfgcanary_instance"}),
		Errors: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "demo_request_errors_total",
			Help: "Requests that failed (dependency call timed out)."}, []string{"cfgcanary_instance"}),
		Duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "demo_request_duration_seconds",
			Help:    "Request latency.",
			Buckets: []float64{0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5}}, []string{"cfgcanary_instance"}),
		Restarts: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "demo_restarts_total",
			Help: "Process restarts of the sample application."}, []string{"cfgcanary_instance"}),
	}
	if reg != nil {
		reg.MustRegister(m.Requests, m.Errors, m.Duration, m.Restarts)
	}
	return m
}

// Workload models one instance of the sample application. Each request calls
// a dependency whose latency is lognormal (median DependencyMedian); the
// request fails if the call exceeds the configured timeout_ms. A config with a
// timeout below typical dependency latency is therefore schema-valid, accepted
// by the parser, and harmful at runtime.
type Workload struct {
	Instance         string
	RPS              float64
	DependencyMedian time.Duration
	Sigma            float64
	Metrics          *AppMetrics
	Get              func() (ratelimits.Config, bool)
	rng              *rand.Rand
	mu               sync.Mutex
	requests, errors prometheus.Counter
	duration         prometheus.Observer
}

// NewWorkload creates a workload with its own random stream.
func NewWorkload(instance string, rps float64, m *AppMetrics, seed uint64, get func() (ratelimits.Config, bool)) *Workload {
	w := &Workload{Instance: instance, RPS: rps, DependencyMedian: 40 * time.Millisecond, Sigma: 0.5, Metrics: m, Get: get,
		rng: rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))}
	if m != nil {
		w.requests = m.Requests.WithLabelValues(instance)
		w.errors = m.Errors.WithLabelValues(instance)
		w.duration = m.Duration.WithLabelValues(instance)
		// Initialise series so "no errors" is distinguishable from "no data".
		w.errors.Add(0)
	}
	return w
}

func (w *Workload) poisson(lambda float64) int {
	// Knuth for small lambda, normal approximation for large.
	if lambda > 50 {
		return max(0, int(math.Round(lambda+math.Sqrt(lambda)*w.rng.NormFloat64())))
	}
	l, k, p := math.Exp(-lambda), 0, 1.0
	for {
		p *= w.rng.Float64()
		if p <= l {
			return k
		}
		k++
	}
}

// Step simulates requests for an interval.
func (w *Workload) Step(dt time.Duration) {
	cfg, ok := w.Get()
	if !ok || w.Metrics == nil {
		return
	}
	timeout := time.Duration(cfg.TimeoutMS) * time.Millisecond
	w.mu.Lock()
	n := w.poisson(w.RPS * dt.Seconds())
	for range n {
		lat := time.Duration(float64(w.DependencyMedian) * math.Exp(w.Sigma*w.rng.NormFloat64()))
		w.requests.Inc()
		if lat > timeout {
			w.errors.Inc()
			lat = timeout
		}
		w.duration.Observe(lat.Seconds())
	}
	w.mu.Unlock()
}
