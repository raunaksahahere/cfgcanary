package gate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	pb "github.com/raunaksahahere/cfgcanary/api/gen/cfgcanary/v1"
)

// Prometheus is a minimal client for the instant-query HTTP API.
type Prometheus struct {
	URL    string
	Client *http.Client
}

// Point is one element of an instant vector.
type Point struct {
	Labels    map[string]string
	Value     float64
	Timestamp time.Time
}

// Query runs an instant query at ts.
func (p *Prometheus) Query(ctx context.Context, q string, ts time.Time) ([]Point, error) {
	hc := p.Client
	if hc == nil {
		hc = &http.Client{Timeout: 10 * time.Second}
	}
	form := url.Values{"query": {q}, "time": {strconv.FormatFloat(float64(ts.UnixMilli())/1000, 'f', 3, 64)}}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimSuffix(p.URL, "/")+"/api/v1/query", strings.NewReader(form.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var body struct {
		Status string `json:"status"`
		Error  string `json:"error"`
		Data   struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Metric map[string]string `json:"metric"`
				Value  [2]any            `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	dec := json.NewDecoder(http.MaxBytesReader(nil, resp.Body, 32<<20))
	if err := dec.Decode(&body); err != nil {
		return nil, fmt.Errorf("prometheus: HTTP %d: %w", resp.StatusCode, err)
	}
	if body.Status != "success" {
		return nil, fmt.Errorf("prometheus: %s", body.Error)
	}
	if body.Data.ResultType != "vector" {
		return nil, fmt.Errorf("prometheus: unexpected result type %q", body.Data.ResultType)
	}
	out := make([]Point, 0, len(body.Data.Result))
	for _, r := range body.Data.Result {
		tsf, _ := r.Value[0].(float64)
		vs, _ := r.Value[1].(string)
		v, err := strconv.ParseFloat(vs, 64)
		if err != nil {
			v = math.NaN()
		}
		out = append(out, Point{Labels: r.Metric, Value: v, Timestamp: time.UnixMilli(int64(tsf * 1000))})
	}
	return out, nil
}

// Request is one gate look.
type Request struct {
	Config, Environment, Release string
	Candidate, Baseline          int64
	// WindowStart is when the ring started baking; evidence is gathered from
	// then until Now.
	WindowStart time.Time
	Now         time.Time
	Policy      *pb.GatePolicy
	Look        int
	HardOnly    bool
}

// Evidence is recorded with every look.
type Evidence struct {
	Verdict         Verdict        `json:"verdict"`
	Reason          string         `json:"reason"`
	Look            int            `json:"look"`
	Looks           int            `json:"looks"`
	WindowSeconds   int64          `json:"window_seconds"`
	CanaryEligible  int            `json:"canary_eligible"`
	ControlEligible int            `json:"control_eligible"`
	StaleExcluded   int            `json:"stale_excluded"`
	LateExcluded    int            `json:"late_excluded"`
	Metrics         []MetricResult `json:"metrics"`
	Queries         []string       `json:"queries,omitempty"`
}

func escape(v string) string {
	return strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`).Replace(v)
}

func selector(name string, labels map[string]string) string {
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteString(name + "{")
	for i, k := range keys {
		if i > 0 {
			b.WriteString(",")
		}
		fmt.Fprintf(&b, `%s="%s"`, k, escape(labels[k]))
	}
	b.WriteString("}")
	return b.String()
}

// PublishedMetric is the SDK series that records, per instance, when it began
// serving a version under a release.
const PublishedMetric = "cfgcanary_sdk_config_published_timestamp_seconds"

// Evaluate gathers evidence and returns a verdict. Any failure to obtain
// clean evidence yields INCONCLUSIVE.
func Evaluate(ctx context.Context, prom *Prometheus, req Request) Evidence {
	g := req.Policy
	ev := Evidence{Look: req.Look, Looks: int(g.Looks)}
	incon := func(format string, a ...any) Evidence {
		ev.Verdict, ev.Reason = Inconclusive, fmt.Sprintf(format, a...)
		return ev
	}
	ctx, cancel := context.WithTimeout(ctx, g.EvalTimeout.AsDuration())
	defer cancel()
	window := req.Now.Sub(req.WindowStart).Truncate(time.Second)
	if window < 2*time.Second {
		return incon("evaluation window of %s is too short", window)
	}
	ev.WindowSeconds = int64(window.Seconds())
	label := g.InstanceLabel
	maxAge := g.MaxSampleAge.AsDuration()

	// Cohort membership from the SDK's own report of what it serves.
	eligible := func(version int64) (map[string]bool, error) {
		q := selector(PublishedMetric, map[string]string{"config": req.Config, "environment": req.Environment,
			"release": req.Release, "config_version": strconv.FormatInt(version, 10)})
		ev.Queries = append(ev.Queries, q)
		pts, err := prom.Query(ctx, q, req.Now)
		if err != nil {
			return nil, err
		}
		out := map[string]bool{}
		for _, p := range pts {
			id := p.Labels["cfgcanary_instance"]
			switch {
			case id == "" || math.IsNaN(p.Value):
				continue
			case req.Now.Sub(p.Timestamp) > maxAge:
				ev.StaleExcluded++
			case p.Value > float64(req.WindowStart.Unix())+1:
				// Started serving after the window began: its counters mix
				// versions, so it is not evidence.
				ev.LateExcluded++
			default:
				out[id] = true
			}
		}
		return out, nil
	}
	canaryIDs, err := eligible(req.Candidate)
	if err != nil {
		return incon("prometheus query failed: %v", err)
	}
	var controlIDs map[string]bool
	if !req.HardOnly {
		if controlIDs, err = eligible(req.Baseline); err != nil {
			return incon("prometheus query failed: %v", err)
		}
	}
	ev.CanaryEligible, ev.ControlEligible = len(canaryIDs), len(controlIDs)

	rng := fmt.Sprintf("[%ds]", ev.WindowSeconds)
	perInstance := func(expr string) (map[string]float64, error) {
		q := fmt.Sprintf("sum by (%s) (increase(%s%s))", label, expr, rng)
		ev.Queries = append(ev.Queries, q)
		pts, err := prom.Query(ctx, q, req.Now)
		if err != nil {
			return nil, err
		}
		out := map[string]float64{}
		for _, p := range pts {
			if id := p.Labels[label]; id != "" {
				out[id] = p.Value
			}
		}
		return out, nil
	}
	params := Params{AlphaHarm: g.AlphaHarm, AlphaNonInferiority: g.AlphaNoninferiority, Looks: int(g.Looks),
		Metrics: len(g.Metrics), MinInstances: int(g.MinInstances), MaxControlProportion: g.MaxControlProportion, HardOnly: req.HardOnly}
	for _, m := range g.Metrics {
		var bad, total map[string]float64
		var err error
		switch m.Kind {
		case pb.MetricKind_METRIC_KIND_RATIO:
			if total, err = perInstance(selector(m.Metric, m.Matchers)); err == nil {
				bad, err = perInstance(selector(m.BadMetric, m.Matchers))
			}
		case pb.MetricKind_METRIC_KIND_HISTOGRAM_OVER:
			var good map[string]float64
			if total, err = perInstance(selector(m.Metric+"_count", m.Matchers)); err == nil {
				bm := map[string]string{"le": m.Le}
				for k, v := range m.Matchers {
					bm[k] = v
				}
				if good, err = perInstance(selector(m.Metric+"_bucket", bm)); err == nil {
					bad = map[string]float64{}
					for id, t := range total {
						g, ok := good[id]
						if !ok {
							bad[id] = math.NaN() // bucket missing: not evidence
							continue
						}
						bad[id] = t - g
					}
				}
			}
		case pb.MetricKind_METRIC_KIND_INSTANCE_EVENT:
			var inc map[string]float64
			if inc, err = perInstance(selector(m.Metric, m.Matchers)); err == nil {
				bad, total = map[string]float64{}, map[string]float64{}
				// Instances with no series at all had no events only if they
				// are known to be serving; count them as zero-event.
				for _, ids := range []map[string]bool{canaryIDs, controlIDs} {
					for id := range ids {
						total[id] = 1
						if v, ok := inc[id]; ok && (math.IsNaN(v) || v > 0.5) {
							bad[id] = 1
							if math.IsNaN(v) {
								bad[id] = math.NaN()
							}
						} else {
							bad[id] = 0
						}
					}
				}
			}
		default:
			err = errors.New("unknown metric kind")
		}
		if err != nil {
			return incon("prometheus query failed for %s: %v", m.Name, err)
		}
		collect := func(ids map[string]bool) []Sample {
			var out []Sample
			for id := range ids {
				t, ok := total[id]
				if !ok {
					continue // no data from this instance: excluded, never assumed healthy
				}
				b := bad[id]
				out = append(out, Sample{Instance: id, Bad: b, Total: t})
			}
			sort.Slice(out, func(i, j int) bool { return out[i].Instance < out[j].Instance })
			return out
		}
		spec := MetricSpec{Name: m.Name, Margin: m.Margin, HardMax: m.HardMax, MinEvents: float64(m.MinEvents), Guard: m.GuardOnly}
		ev.Metrics = append(ev.Metrics, EvaluateMetric(spec, collect(canaryIDs), collect(controlIDs), params))
	}
	ev.Verdict, ev.Reason = Combine(ev.Metrics)
	return ev
}
