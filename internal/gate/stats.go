// Package gate decides whether a canary cohort is healthy enough to promote.
//
// Each metric is a proportion (failed requests, requests slower than a
// histogram bucket boundary, instances that restarted). At each of K
// predeclared looks, two one-sided tests run per metric:
//
//   - harm: H0 "canary is no worse than control" (p_c - p_k <= 0). Rejecting
//     it is evidence of degradation -> FAIL.
//   - non-inferiority: H0 "canary is worse by at least the margin"
//     (p_c - p_k >= margin). Rejecting it for every metric is evidence the
//     canary is within the tolerated degradation -> PASS.
//
// Anything else is INCONCLUSIVE. Lack of significance is never treated as
// safety. Error control: alpha is split across the K looks (Bonferroni), and
// the harm alpha also across metrics; promotion requires every metric to pass
// (intersection-union, no correction needed). Requests from one instance are
// correlated, so the variance used is the larger of the binomial variance and
// a between-instance (cluster-robust) estimate, with Student-t critical
// values on the instance degrees of freedom. See docs/decisions/0004 and
// docs/design-guide.md for assumptions and limits.
package gate

import (
	"fmt"
	"math"
	"sort"

	"gonum.org/v1/gonum/stat/distuv"
)

// Verdict of a look.
type Verdict string

const (
	Pass         Verdict = "PASS"
	Fail         Verdict = "FAIL"
	Inconclusive Verdict = "INCONCLUSIVE"
)

// Sample is one instance's counts for a metric over the evaluation window.
type Sample struct {
	Instance string  `json:"instance"`
	Bad      float64 `json:"bad"`
	Total    float64 `json:"total"`
}

// MetricSpec is the decision rule for one metric.
type MetricSpec struct {
	Name      string
	Margin    float64 // tolerated absolute degradation
	HardMax   float64 // 0: disabled
	MinEvents float64 // per cohort
	// Guard metrics fail on harm or hard limits but never need to show
	// non-inferiority.
	Guard bool
}

// Params are shared by all metrics of a look.
type Params struct {
	AlphaHarm            float64
	AlphaNonInferiority  float64
	Looks                int // K
	Metrics              int // M
	MinInstances         int
	MaxControlProportion float64 // 0: disabled
	// HardOnly is the final-stage policy without a control cohort: only hard
	// thresholds are evaluated.
	HardOnly bool
}

// MetricResult is the evidence for one metric.
type MetricResult struct {
	Name             string  `json:"name"`
	Verdict          Verdict `json:"verdict"`
	Reason           string  `json:"reason,omitempty"`
	CanaryInstances  int     `json:"canary_instances"`
	ControlInstances int     `json:"control_instances"`
	CanaryEvents     float64 `json:"canary_events"`
	ControlEvents    float64 `json:"control_events"`
	CanaryP          float64 `json:"canary_p"`
	ControlP         float64 `json:"control_p"`
	ZHarm            float64 `json:"z_harm"`
	CritHarm         float64 `json:"crit_harm"`
	ZNonInferiority  float64 `json:"z_noninferiority"`
	CritNonInf       float64 `json:"crit_noninferiority"`
	DF               float64 `json:"df"`
}

type agg struct {
	m       int
	n, bad  float64
	p       float64
	vBinom  float64
	vClust  float64
	invalid string
}

func aggregate(s []Sample) agg {
	a := agg{m: len(s)}
	for _, x := range s {
		switch {
		case math.IsNaN(x.Bad) || math.IsNaN(x.Total) || math.IsInf(x.Bad, 0) || math.IsInf(x.Total, 0):
			a.invalid = "non-finite sample"
		case x.Bad < 0 || x.Total < 0 || x.Bad > x.Total*(1+1e-9)+1e-9:
			a.invalid = "inconsistent sample (bad > total or negative)"
		}
		a.n += x.Total
		a.bad += math.Min(x.Bad, x.Total)
	}
	if a.n > 0 {
		a.p = a.bad / a.n
		a.vBinom = a.p * (1 - a.p) / a.n
		if a.m >= 2 {
			var ss float64
			for _, x := range s {
				d := math.Min(x.Bad, x.Total) - a.p*x.Total
				ss += d * d
			}
			a.vClust = float64(a.m) / float64(a.m-1) * ss / (a.n * a.n)
		}
	}
	return a
}

func tQuantile(p, df float64) float64 {
	if df < 1 {
		df = 1
	}
	return distuv.StudentsT{Mu: 0, Sigma: 1, Nu: df}.Quantile(p)
}

// RestrictedMLE returns the maximum-likelihood estimates of (p1, p2) under the
// constraint p1 - p2 = delta, for x1/n1 and x2/n2 binomial observations
// (Farrington & Manning 1990; Miettinen & Nurminen 1985 closed form).
func RestrictedMLE(x1, n1, x2, n2, delta float64) (float64, float64) {
	p1, p2 := x1/n1, x2/n2
	theta := n2 / n1
	a := 1 + theta
	b := -(1 + theta + p1 + theta*p2 + delta*(theta+2))
	c := delta*delta + delta*(2*p1+theta+1) + p1 + theta*p2
	d := -p1 * delta * (1 + delta)
	v := b*b*b/(27*a*a*a) - b*c/(6*a*a) + d/(2*a)
	u := math.Sqrt(math.Max(b*b/(9*a*a)-c/(3*a), 0))
	if v < 0 {
		u = -u
	}
	var r1 float64
	if u == 0 {
		r1 = -b / (3 * a)
	} else {
		arg := v / (u * u * u)
		arg = math.Max(-1, math.Min(1, arg))
		w := (math.Pi + math.Acos(arg)) / 3
		r1 = 2*u*math.Cos(w) - b/(3*a)
	}
	// Numerical guard: clamp into the feasible region.
	lo, hi := math.Max(0, delta), math.Min(1, 1+delta)
	r1 = math.Max(lo, math.Min(hi, r1))
	return r1, r1 - delta
}

// EvaluateMetric applies the decision rule to canary and control samples.
func EvaluateMetric(spec MetricSpec, canary, control []Sample, p Params) MetricResult {
	c := aggregate(canary)
	r := MetricResult{Name: spec.Name, CanaryInstances: c.m, CanaryEvents: c.n, CanaryP: c.p}
	incon := func(format string, a ...any) MetricResult {
		r.Verdict, r.Reason = Inconclusive, fmt.Sprintf(format, a...)
		return r
	}
	if c.invalid != "" {
		return incon("canary: %s", c.invalid)
	}
	if c.m < p.MinInstances {
		return incon("canary has %d instances with data; need %d", c.m, p.MinInstances)
	}
	if c.n < spec.MinEvents {
		return incon("canary has %.0f events; need %.0f", c.n, spec.MinEvents)
	}
	// Hard threshold: absolute, needs no control.
	if spec.HardMax > 0 && c.p >= spec.HardMax {
		r.Verdict, r.Reason = Fail, fmt.Sprintf("canary proportion %.4f is at or above the hard limit %.4f", c.p, spec.HardMax)
		return r
	}
	if p.HardOnly {
		if spec.Guard {
			r.Verdict, r.Reason = Pass, "guard metric, final stage: below the hard limit"
			return r
		}
		if spec.HardMax <= 0 {
			return incon("final stage has no control cohort and this metric has no hard limit")
		}
		r.Verdict, r.Reason = Pass, "final stage: below the hard limit (no comparative evidence)"
		return r
	}
	k := aggregate(control)
	r.ControlInstances, r.ControlEvents, r.ControlP = k.m, k.n, k.p
	switch {
	case k.invalid != "":
		return incon("control: %s", k.invalid)
	case k.m < p.MinInstances:
		return incon("control has %d instances with data; need %d", k.m, p.MinInstances)
	case k.n < spec.MinEvents:
		return incon("control has %.0f events; need %.0f", k.n, spec.MinEvents)
	case p.MaxControlProportion > 0 && k.p > p.MaxControlProportion:
		return incon("control cohort is itself degraded (%.4f > %.4f); comparison is not meaningful", k.p, p.MaxControlProportion)
	}
	looks := math.Max(1, float64(p.Looks))
	metrics := math.Max(1, float64(p.Metrics))
	r.DF = float64(min(c.m, k.m) - 1)
	r.CritHarm = tQuantile(1-p.AlphaHarm/(looks*metrics), r.DF)
	r.CritNonInf = tQuantile(1-p.AlphaNonInferiority/looks, r.DF)
	diff := c.p - k.p

	// Harm test. Observed variances vanish at the boundaries (all or no
	// events), so the pooled variance under H0 (p_c = p_k) is a floor: the
	// score-test form, which cannot report "no harm" merely because the
	// cohorts are degenerate.
	pooled := (c.bad + k.bad) / (c.n + k.n)
	v0 := pooled * (1 - pooled) * (1/c.n + 1/k.n)
	vh := math.Max(math.Max(c.vBinom, c.vClust)+math.Max(k.vBinom, k.vClust), v0)
	if vh > 0 {
		r.ZHarm = diff / math.Sqrt(vh)
	}
	if r.ZHarm > r.CritHarm {
		r.Verdict, r.Reason = Fail, fmt.Sprintf("canary is worse than control: %.4f vs %.4f (z=%.2f > %.2f)", c.p, k.p, r.ZHarm, r.CritHarm)
		return r
	}
	if spec.Guard {
		r.Verdict, r.Reason = Pass, fmt.Sprintf("guard metric: no harm detected (%.4f vs %.4f); not evidence of safety", c.p, k.p)
		return r
	}
	// Non-inferiority test with variance under the null boundary.
	t1, t2 := RestrictedMLE(c.bad, c.n, k.bad, k.n, spec.Margin)
	vni := math.Max(t1*(1-t1)/c.n+t2*(1-t2)/k.n, c.vClust+k.vClust)
	if vni <= 0 {
		return incon("zero variance under the null hypothesis")
	}
	r.ZNonInferiority = (diff - spec.Margin) / math.Sqrt(vni)
	if r.ZNonInferiority < -r.CritNonInf {
		r.Verdict, r.Reason = Pass, fmt.Sprintf("within margin %.4f: %.4f vs %.4f (z=%.2f < -%.2f)", spec.Margin, c.p, k.p, r.ZNonInferiority, r.CritNonInf)
		return r
	}
	return incon("not enough evidence that the canary is within margin %.4f (z=%.2f, need < -%.2f)", spec.Margin, r.ZNonInferiority, r.CritNonInf)
}

// Combine folds metric results into one verdict: any FAIL fails, all PASS
// passes, anything else is inconclusive.
func Combine(results []MetricResult) (Verdict, string) {
	if len(results) == 0 {
		return Inconclusive, "no metrics evaluated"
	}
	sorted := append([]MetricResult(nil), results...)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].Name < sorted[j].Name })
	for _, r := range sorted {
		if r.Verdict == Fail {
			return Fail, r.Name + ": " + r.Reason
		}
	}
	for _, r := range sorted {
		if r.Verdict != Pass {
			return Inconclusive, r.Name + ": " + r.Reason
		}
	}
	return Pass, "all metrics within margin"
}
