package bench

import (
	"fmt"
	"math"
	"math/rand/v2"
	"testing"

	"github.com/raunaksahahere/cfgcanary/internal/gate"
)

// GateScenario is one simulated configuration of the gate.
type GateScenario struct {
	CanaryInstances  int     `json:"canary_instances"`
	ControlInstances int     `json:"control_instances"`
	RequestsPerLook  int     `json:"requests_per_instance_per_look"`
	BaselineP        float64 `json:"baseline_error_rate"`
	Delta            float64 `json:"true_degradation"`
	ICC              float64 `json:"intra_instance_correlation"`
	Reps             int     `json:"replications"`
	PPromote         float64 `json:"p_promote"`
	PRollback        float64 `json:"p_rollback"`
	PHalt            float64 `json:"p_halt"`
	MeanLooks        float64 `json:"mean_looks_to_decision"`
}

// binomial draws Binomial(n, p) exactly for small n and with a continuity-
// corrected normal approximation otherwise.
func binomial(rng *rand.Rand, n int, p float64) float64 {
	if p <= 0 {
		return 0
	}
	if p >= 1 {
		return float64(n)
	}
	if float64(n)*p < 15 || float64(n)*(1-p) < 15 {
		k := 0
		for range n {
			if rng.Float64() < p {
				k++
			}
		}
		return float64(k)
	}
	x := math.Round(float64(n)*p + math.Sqrt(float64(n)*p*(1-p))*rng.NormFloat64())
	return math.Max(0, math.Min(float64(n), x))
}

// instanceRates draws per-instance error rates with mean p and intra-class
// correlation icc (Beta distribution); icc 0 means identical instances.
func instanceRates(rng *rand.Rand, m int, p, icc float64) []float64 {
	out := make([]float64, m)
	if icc <= 0 {
		for i := range out {
			out[i] = p
		}
		return out
	}
	// Beta(a, b) with mean p and ICC = 1/(a+b+1).
	s := 1/icc - 1
	a, b := p*s, (1-p)*s
	for i := range out {
		x := gammaRand(rng, a)
		y := gammaRand(rng, b)
		out[i] = x / (x + y)
	}
	return out
}

// gammaRand is Marsaglia-Tsang for shape >= 1, boosted for shape < 1.
func gammaRand(rng *rand.Rand, k float64) float64 {
	if k < 1 {
		return gammaRand(rng, k+1) * math.Pow(rng.Float64(), 1/k)
	}
	d := k - 1.0/3
	c := 1 / math.Sqrt(9*d)
	for {
		x := rng.NormFloat64()
		v := 1 + c*x
		if v <= 0 {
			continue
		}
		v = v * v * v
		u := rng.Float64()
		if math.Log(u) < 0.5*x*x+d-d*v+d*math.Log(v) {
			return d * v
		}
	}
}

func simulate(sc GateScenario, looks int, seed uint64) GateScenario {
	rng := rand.New(rand.NewPCG(seed, seed^0xabcdef))
	spec := gate.MetricSpec{Name: "errors", Margin: 0.02, MinEvents: 100}
	params := gate.Params{AlphaHarm: 0.01, AlphaNonInferiority: 0.05, Looks: looks, Metrics: 1, MinInstances: 3}
	var promote, rollback, halt, lookSum int
	for range sc.Reps {
		cr := instanceRates(rng, sc.CanaryInstances, sc.BaselineP+sc.Delta, sc.ICC)
		kr := instanceRates(rng, sc.ControlInstances, sc.BaselineP, sc.ICC)
		c := make([]gate.Sample, sc.CanaryInstances)
		k := make([]gate.Sample, sc.ControlInstances)
		decided := false
		for look := 1; look <= looks; look++ {
			for i := range c {
				c[i].Bad += binomial(rng, sc.RequestsPerLook, cr[i])
				c[i].Total += float64(sc.RequestsPerLook)
			}
			for i := range k {
				k[i].Bad += binomial(rng, sc.RequestsPerLook, kr[i])
				k[i].Total += float64(sc.RequestsPerLook)
			}
			switch gate.EvaluateMetric(spec, c, k, params).Verdict {
			case gate.Pass:
				promote++
			case gate.Fail:
				rollback++
			default:
				continue
			}
			lookSum += look
			decided = true
			break
		}
		if !decided {
			halt++
			lookSum += looks
		}
	}
	n := float64(sc.Reps)
	sc.PPromote, sc.PRollback, sc.PHalt, sc.MeanLooks = float64(promote)/n, float64(rollback)/n, float64(halt)/n, float64(lookSum)/n
	return sc
}

// TestGatePower simulates the production gate statistics (internal/gate) over
// K = 4 sequential looks for 5, 50 and 500 canary instances.
func TestGatePower(t *testing.T) {
	requireBench(t)
	var out []GateScenario
	seed := uint64(1)
	for _, m := range []int{5, 50, 500} {
		reps := 1000
		if m == 500 {
			reps = 400
		}
		for _, traffic := range []int{200, 20} {
			for _, icc := range []float64{0, 0.02} {
				for _, delta := range []float64{0, 0.005, 0.02, 0.05} {
					sc := GateScenario{CanaryInstances: m, ControlInstances: 4 * m, RequestsPerLook: traffic, BaselineP: 0.01, Delta: delta, ICC: icc, Reps: reps}
					seed++
					r := simulate(sc, 4, seed)
					out = append(out, r)
					t.Logf("m=%d traffic=%d icc=%.2f delta=%.3f: promote=%.3f rollback=%.3f halt=%.3f looks=%.2f", m, traffic, icc, delta, r.PPromote, r.PRollback, r.PHalt, r.MeanLooks)
				}
			}
		}
	}
	writeJSON(t, "gate_power.json", map[string]any{"environment": environment("CFGCANARY_BENCH=1 go test ./bench -run TestGatePower"),
		"model": map[string]any{
			"procedure":     "internal/gate EvaluateMetric: harm test (alpha 0.01 split over 4 looks) and non-inferiority test (margin 0.02 absolute, alpha 0.05 split over 4 looks), cluster-robust variance, Student-t on instance degrees of freedom; promote on PASS, roll back on FAIL, halt after look 4",
			"control":       "4 x canary instances, same baseline rate",
			"traffic":       "requests per instance per look; evidence accumulates across looks",
			"heterogeneity": "per-instance rates drawn from a Beta distribution with the given intra-class correlation (0 = identical instances)",
			"deltas":        "0 (no change), 0.005 (inside the margin), 0.02 (exactly at the margin), 0.05 (harmful)",
			"limits":        "single metric; independent requests within an instance given its rate; no time trends or correlated incidents; binomial approximated by a normal for large n",
		}, "results": out})
	var rows []string
	for _, r := range out {
		rows = append(rows, fmt.Sprintf("%d,%d,%d,%.2f,%.3f,%d,%.4f,%.4f,%.4f,%.2f", r.CanaryInstances, r.ControlInstances, r.RequestsPerLook, r.ICC, r.Delta, r.Reps, r.PPromote, r.PRollback, r.PHalt, r.MeanLooks))
	}
	writeCSV(t, "gate_power.csv", "canary,control,requests_per_look,icc,delta,reps,p_promote,p_rollback,p_halt,mean_looks", rows)
}
