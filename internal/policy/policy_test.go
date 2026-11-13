package policy

import (
	"strings"
	"testing"

	pb "github.com/raunaksahahere/cfgcanary/api/gen/cfgcanary/v1"
)

func rings(bps ...int32) []*pb.Ring {
	var out []*pb.Ring
	for _, b := range bps {
		out = append(out, &pb.Ring{CumulativeBps: b})
	}
	return out
}

func TestNormalizeDefaults(t *testing.T) {
	p, err := Normalize("limits", "prod", &pb.RolloutPolicy{Rings: rings(1000, 5000, 9000)})
	if err != nil {
		t.Fatal(err)
	}
	if p.Salt != "limits/prod" || p.FinalStage != pb.FinalStage_FINAL_STAGE_HOLDOUT || p.Promotion != pb.Promotion_PROMOTION_MANUAL ||
		p.Gate.Mode != pb.GateMode_GATE_MODE_NONE || p.Rings[1].Name != "ring-1" {
		t.Fatalf("%v", p)
	}
	r := Rollout(p)
	if r.Rings != 3 || r.Gated || r.Auto || r.AcceptanceThreshold != 0.9 {
		t.Fatalf("%+v", r)
	}
}

func TestNormalizeRejects(t *testing.T) {
	metric := &pb.GateMetric{Name: "errors", Kind: pb.MetricKind_METRIC_KIND_RATIO, Metric: "req_total", BadMetric: "req_failed_total", Margin: 0.02}
	cases := map[string]*pb.RolloutPolicy{
		"no rings":                 {},
		"ungated auto":             {Rings: rings(10000), Promotion: pb.Promotion_PROMOTION_AUTO},
		"final stage mismatch":     {Rings: rings(10000), FinalStage: pb.FinalStage_FINAL_STAGE_HOLDOUT},
		"gate without metrics":     {Rings: rings(10000), Gate: &pb.GatePolicy{Mode: pb.GateMode_GATE_MODE_PROMETHEUS}},
		"metric without margin":    {Rings: rings(10000), Gate: &pb.GatePolicy{Mode: pb.GateMode_GATE_MODE_PROMETHEUS, Metrics: []*pb.GateMetric{{Name: "e", Kind: pb.MetricKind_METRIC_KIND_RATIO, Metric: "a", BadMetric: "b"}}}},
		"promql injection":         {Rings: rings(10000), Gate: &pb.GatePolicy{Mode: pb.GateMode_GATE_MODE_PROMETHEUS, Metrics: []*pb.GateMetric{{Name: "e", Kind: pb.MetricKind_METRIC_KIND_RATIO, Metric: `a{x="y"} or b`, BadMetric: "b", Margin: 0.1}}}},
		"histogram without le":     {Rings: rings(10000), Gate: &pb.GatePolicy{Mode: pb.GateMode_GATE_MODE_PROMETHEUS, Metrics: []*pb.GateMetric{{Name: "l", Kind: pb.MetricKind_METRIC_KIND_HISTOGRAM_OVER, Metric: "h", Margin: 0.1}}}},
		"one instance cohort gate": {Rings: rings(10000), Gate: &pb.GatePolicy{Mode: pb.GateMode_GATE_MODE_PROMETHEUS, MinInstances: 1, Metrics: []*pb.GateMetric{metric}}},
	}
	for name, p := range cases {
		if _, err := Normalize("c", "e", p); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	ok, err := Normalize("c", "e", &pb.RolloutPolicy{Rings: rings(1000, 10000), Gate: &pb.GatePolicy{Mode: pb.GateMode_GATE_MODE_PROMETHEUS, Metrics: []*pb.GateMetric{metric}}})
	if err != nil || ok.Promotion != pb.Promotion_PROMOTION_AUTO || ok.Gate.Looks != 4 || ok.Gate.Metrics[0].MinEvents != 100 {
		t.Fatalf("%v %v", err, ok)
	}
	if !strings.Contains(ok.FinalStage.String(), "HARD_THRESHOLDS") {
		t.Fatal(ok.FinalStage)
	}
}
