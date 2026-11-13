package gate

import (
	"math"
	"math/rand/v2"
	"testing"
)

func cohort(m int, n, p float64, rng *rand.Rand) []Sample {
	out := make([]Sample, m)
	for i := range out {
		bad := 0.0
		for range int(n) {
			if rng.Float64() < p {
				bad++
			}
		}
		out[i] = Sample{Instance: string(rune('a' + i%26)), Bad: bad, Total: n}
	}
	return out
}

var params = Params{AlphaHarm: 0.01, AlphaNonInferiority: 0.05, Looks: 4, Metrics: 1, MinInstances: 3}
var spec = MetricSpec{Name: "errors", Margin: 0.02, MinEvents: 100}

func TestRestrictedMLEMaximizesConstrainedLikelihood(t *testing.T) {
	ll := func(x1, n1, x2, n2, p1, p2 float64) float64 {
		f := func(x, n, p float64) float64 {
			p = math.Min(math.Max(p, 1e-12), 1-1e-12)
			return x*math.Log(p) + (n-x)*math.Log(1-p)
		}
		return f(x1, n1, p1) + f(x2, n2, p2)
	}
	rng := rand.New(rand.NewPCG(3, 3))
	for range 300 {
		n1, n2 := float64(10+rng.IntN(5000)), float64(10+rng.IntN(5000))
		x1, x2 := math.Floor(rng.Float64()*n1*0.3), math.Floor(rng.Float64()*n2*0.3)
		delta := 0.001 + rng.Float64()*0.2
		r1, r2 := RestrictedMLE(x1, n1, x2, n2, delta)
		if math.Abs(r1-r2-delta) > 1e-9 || r1 < 0 || r1 > 1 || r2 < 0 {
			t.Fatalf("constraint violated: %v %v delta %v", r1, r2, delta)
		}
		best := ll(x1, n1, x2, n2, r1, r2)
		for g := delta; g <= 1; g += 0.0005 {
			if v := ll(x1, n1, x2, n2, g, g-delta); v > best+1e-6 {
				t.Fatalf("x1=%v n1=%v x2=%v n2=%v delta=%v: grid p1=%v beats MLE %v (%v > %v)", x1, n1, x2, n2, delta, g, r1, v, best)
			}
		}
	}
}

func TestDecisions(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	healthy := EvaluateMetric(spec, cohort(10, 2000, 0.01, rng), cohort(30, 2000, 0.01, rng), params)
	if healthy.Verdict != Pass {
		t.Fatalf("healthy canary: %+v", healthy)
	}
	bad := EvaluateMetric(spec, cohort(10, 2000, 0.08, rng), cohort(30, 2000, 0.01, rng), params)
	if bad.Verdict != Fail {
		t.Fatalf("degraded canary: %+v", bad)
	}
	// Zero errors on both sides is the normal healthy case and must be able
	// to pass (a rate-ratio SPRT cannot).
	zero := EvaluateMetric(spec, cohort(10, 2000, 0, rng), cohort(30, 2000, 0, rng), params)
	if zero.Verdict != Pass {
		t.Fatalf("zero-error cohorts: %+v", zero)
	}
}

// Missing, insufficient or malformed evidence is never a pass.
func TestMissingDataNeverPasses(t *testing.T) {
	rng := rand.New(rand.NewPCG(5, 5))
	good := cohort(10, 1000, 0.01, rng)
	cases := map[string]struct {
		canary, control []Sample
		p               Params
	}{
		"no canary":           {nil, good, params},
		"no control":          {good, nil, params},
		"too few instances":   {good[:2], good, params},
		"too few events":      {[]Sample{{"a", 0, 10}, {"b", 0, 10}, {"c", 0, 10}}, good, params},
		"NaN":                 {append([]Sample{{"x", math.NaN(), 100}}, good...), good, params},
		"Inf":                 {append([]Sample{{"x", 1, math.Inf(1)}}, good...), good, params},
		"bad > total":         {append([]Sample{{"x", 50, 10}}, good...), good, params},
		"negative":            {append([]Sample{{"x", -1, 10}}, good...), good, params},
		"control degraded":    {good, cohort(10, 1000, 0.3, rng), Params{AlphaHarm: 0.01, AlphaNonInferiority: 0.05, Looks: 4, Metrics: 1, MinInstances: 3, MaxControlProportion: 0.1}},
		"hard-only, no limit": {good, nil, Params{AlphaHarm: 0.01, AlphaNonInferiority: 0.05, Looks: 1, Metrics: 1, MinInstances: 3, HardOnly: true}},
	}
	for name, c := range cases {
		r := EvaluateMetric(spec, c.canary, c.control, c.p)
		if r.Verdict == Pass {
			t.Errorf("%s: passed (%s)", name, r.Reason)
		}
	}
	if v, _ := Combine(nil); v != Inconclusive {
		t.Fatal("no metrics passed")
	}
	if v, _ := Combine([]MetricResult{{Name: "a", Verdict: Pass}, {Name: "b", Verdict: Inconclusive}}); v != Inconclusive {
		t.Fatal("one inconclusive metric passed")
	}
}

func TestHardThreshold(t *testing.T) {
	s := spec
	s.HardMax = 0.05
	canary := []Sample{{"a", 30, 300}, {"b", 30, 300}, {"c", 30, 300}}
	if r := EvaluateMetric(s, canary, nil, Params{Looks: 1, Metrics: 1, MinInstances: 3, HardOnly: true}); r.Verdict != Fail {
		t.Fatalf("%+v", r)
	}
	ok := []Sample{{"a", 1, 300}, {"b", 0, 300}, {"c", 0, 300}}
	if r := EvaluateMetric(s, ok, nil, Params{Looks: 1, Metrics: 1, MinInstances: 3, HardOnly: true}); r.Verdict != Pass {
		t.Fatalf("%+v", r)
	}
}

// One bad instance among many must not be hidden by request-level pooling
// into a falsely confident pass: the cluster variance widens the interval.
func TestClusterVarianceWidens(t *testing.T) {
	canary := []Sample{{"a", 0, 5000}, {"b", 0, 5000}, {"c", 400, 5000}}
	control := []Sample{{"d", 50, 5000}, {"e", 50, 5000}, {"f", 50, 5000}, {"g", 50, 5000}}
	r := EvaluateMetric(MetricSpec{Name: "e", Margin: 0.02, MinEvents: 100}, canary, control, params)
	if r.Verdict == Pass {
		t.Fatalf("heterogeneous canary passed: %+v", r)
	}
}

// Under H0 at the margin, the per-look false-pass rate must stay below
// alpha_ni/K (and so below alpha_ni overall).
func TestFalsePassRateAtMargin(t *testing.T) {
	rng := rand.New(rand.NewPCG(11, 11))
	passes := 0
	const trials = 2000
	for range trials {
		r := EvaluateMetric(spec, cohort(20, 500, 0.03, rng), cohort(40, 500, 0.01, rng), params)
		if r.Verdict == Pass {
			passes++
		}
	}
	if rate := float64(passes) / trials; rate > params.AlphaNonInferiority/float64(params.Looks)*1.5 {
		t.Fatalf("false-pass rate at the margin %.4f exceeds alpha/K", rate)
	}
}

func FuzzEvaluate(f *testing.F) {
	f.Add(1.0, 100.0, 2.0, 100.0, 0.02)
	f.Fuzz(func(t *testing.T, cb, cn, kb, kn, margin float64) {
		s := MetricSpec{Name: "m", Margin: math.Abs(math.Mod(margin, 1)), MinEvents: 1}
		c := []Sample{{"a", cb, cn}, {"b", cb, cn}, {"c", cb, cn}}
		k := []Sample{{"d", kb, kn}, {"e", kb, kn}, {"f", kb, kn}}
		r := EvaluateMetric(s, c, k, params)
		if r.Verdict == Pass && (math.IsNaN(cb+cn+kb+kn) || cb > cn || kb > kn || cn <= 0 || kn <= 0) {
			t.Fatalf("passed on invalid input %+v", r)
		}
	})
}

func TestGuardMetric(t *testing.T) {
	g := MetricSpec{Name: "restarts", Margin: 0.2, MinEvents: 3, Guard: true}
	none := []Sample{{"a", 0, 1}, {"b", 0, 1}, {"c", 0, 1}, {"d", 0, 1}, {"e", 0, 1}}
	ctl := make([]Sample, 40)
	for i := range ctl {
		ctl[i] = Sample{string(rune('A' + i)), 0, 1}
	}
	if r := EvaluateMetric(g, none, ctl, params); r.Verdict != Pass {
		t.Fatalf("guard without harm: %+v", r)
	}
	// Every canary restarted: demonstrated harm still fails a guard.
	all := []Sample{{"a", 1, 1}, {"b", 1, 1}, {"c", 1, 1}, {"d", 1, 1}, {"e", 1, 1}}
	if r := EvaluateMetric(g, all, ctl, params); r.Verdict != Fail {
		t.Fatalf("guard with harm: %+v", r)
	}
	// The same data as an evidence metric cannot pass with 5 instances.
	g.Guard = false
	if r := EvaluateMetric(g, none, ctl, params); r.Verdict == Pass {
		t.Fatalf("5-instance restart evidence passed: %+v", r)
	}
}
