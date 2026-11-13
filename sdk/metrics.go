package sdk

import (
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Metrics are the SDK's Prometheus series. One Metrics may be shared by many
// clients (the simulator runs thousands in one process).
//
// Cardinality: cfgcanary_sdk_config_info has exactly one series per (client,
// config); when the served version changes the old series is deleted, not
// left to go stale. The gate joins application metrics with it on the
// instance label to tell canary from control.
type Metrics struct {
	info       *prometheus.GaugeVec
	published  *prometheus.GaugeVec
	failures   *prometheus.CounterVec
	crashLoops *prometheus.CounterVec
	stale      *prometheus.GaugeVec
	age        *prometheus.GaugeVec

	mu   sync.Mutex
	last map[string]prometheus.Labels // instance/config -> labels in use
}

// NewMetrics registers SDK metrics with reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		info: prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "cfgcanary_sdk_config_info",
			Help: "1 for the config version the application is serving."},
			[]string{"cfgcanary_instance", "config", "environment", "config_version", "release", "source"}),
		published: prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "cfgcanary_sdk_config_published_timestamp_seconds",
			Help: "Unix time at which this instance began serving this version under this release. The gate uses it to exclude instances that switched during the evaluation window."},
			[]string{"cfgcanary_instance", "config", "environment", "config_version", "release", "source"}),
		failures: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "cfgcanary_sdk_validation_failures_total",
			Help: "Payloads the application's parser rejected."}, []string{"cfgcanary_instance", "config"}),
		crashLoops: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "cfgcanary_sdk_crash_loops_total",
			Help: "Configs quarantined after repeated unhealthy starts."}, []string{"cfgcanary_instance", "config"}),
		stale: prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "cfgcanary_sdk_stale",
			Help: "1 when the agent has not reached the control plane within the config's max staleness."},
			[]string{"cfgcanary_instance", "config"}),
		age: prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "cfgcanary_sdk_config_age_seconds",
			Help: "Seconds since the served snapshot was published by this process."}, []string{"cfgcanary_instance", "config"}),
		last: map[string]prometheus.Labels{},
	}
	if reg != nil {
		reg.MustRegister(m.info, m.published, m.failures, m.crashLoops, m.stale, m.age)
	}
	return m
}

func (c *Client[T]) labels() (string, string) { return c.opt.InstanceID, c.opt.Config }

func (c *Client[T]) updateInfoMetric(i Info) {
	m := c.opt.Metrics
	if m == nil {
		return
	}
	inst, cfg := c.labels()
	l := prometheus.Labels{"cfgcanary_instance": inst, "config": cfg, "environment": c.opt.Environment,
		"config_version": strconv.FormatInt(i.Version, 10), "release": i.ReleaseID, "source": i.Source}
	key := inst + "\x00" + cfg
	m.mu.Lock()
	changed := true
	if old, ok := m.last[key]; ok {
		if equal(old, l) {
			changed = false
		} else {
			m.info.Delete(old)
			m.published.Delete(old)
		}
	}
	m.last[key] = l
	m.mu.Unlock()
	m.info.With(l).Set(1)
	if changed {
		// Same content and release under a newer generation keeps the
		// original timestamp: the instance has served it all along.
		m.published.With(l).Set(float64(time.Now().UnixMilli()) / 1000)
	}
	m.age.WithLabelValues(inst, cfg).Set(0)
}

func equal(a, b prometheus.Labels) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

func (c *Client[T]) deleteInfoMetric() {
	m := c.opt.Metrics
	if m == nil {
		return
	}
	inst, cfg := c.labels()
	key := inst + "\x00" + cfg
	m.mu.Lock()
	if old, ok := m.last[key]; ok {
		m.info.Delete(old)
		m.published.Delete(old)
		delete(m.last, key)
	}
	m.mu.Unlock()
	m.stale.DeleteLabelValues(inst, cfg)
	m.age.DeleteLabelValues(inst, cfg)
}

func (c *Client[T]) metricsValidationFailure() {
	if m := c.opt.Metrics; m != nil {
		m.failures.WithLabelValues(c.labels()).Inc()
	}
}

func (c *Client[T]) metricsCrashLoop() {
	if m := c.opt.Metrics; m != nil {
		m.crashLoops.WithLabelValues(c.labels()).Inc()
	}
}

// updateStaleMetric is called with c.mu held.
func (c *Client[T]) updateStaleMetric() {
	m := c.opt.Metrics
	s := c.cur.Load()
	if m == nil || s == nil {
		return
	}
	inst, cfg := c.labels()
	stale := 0.0
	if s.maxStale > 0 && time.Since(c.lastContact) > s.maxStale {
		stale = 1
	}
	m.stale.WithLabelValues(inst, cfg).Set(stale)
	m.age.WithLabelValues(inst, cfg).Set(time.Since(s.published).Seconds())
}
