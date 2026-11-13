// Package policy normalizes and validates rollout policies and converts them
// to the forms the pure packages need. A release stores the normalized policy,
// so later changes to a config never alter a release in flight.
package policy

import (
	"errors"
	"fmt"
	"regexp"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	pb "github.com/raunaksahahere/cfgcanary/api/gen/cfgcanary/v1"
	"github.com/raunaksahahere/cfgcanary/internal/ring"
	"github.com/raunaksahahere/cfgcanary/internal/rollout"
)

var (
	metricName = regexp.MustCompile(`^[a-zA-Z_:][a-zA-Z0-9_:]*$`)
	labelName  = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)
)

func dur(d *durationpb.Duration, def time.Duration) *durationpb.Duration {
	if d == nil || d.AsDuration() <= 0 {
		return durationpb.New(def)
	}
	return d
}

// Normalize returns a copy of p with defaults filled in, or an error
// describing the first problem.
func Normalize(config, env string, in *pb.RolloutPolicy) (*pb.RolloutPolicy, error) {
	if in == nil {
		return nil, errors.New("rollout policy is missing")
	}
	p := proto.Clone(in).(*pb.RolloutPolicy)
	if p.Salt == "" {
		p.Salt = config + "/" + env
	}
	rp := Ring(p)
	if err := rp.Validate(); err != nil {
		return nil, fmt.Errorf("rings: %w", err)
	}
	seen := map[string]bool{}
	for i, r := range p.Rings {
		if r.Name == "" {
			r.Name = fmt.Sprintf("ring-%d", i)
		}
		if seen[r.Name] {
			return nil, fmt.Errorf("rings: duplicate name %q", r.Name)
		}
		seen[r.Name] = true
	}
	want := pb.FinalStage_FINAL_STAGE_HARD_THRESHOLDS_ONLY
	if rp.HasHoldout() {
		want = pb.FinalStage_FINAL_STAGE_HOLDOUT
	}
	if p.FinalStage == pb.FinalStage_FINAL_STAGE_UNSPECIFIED {
		p.FinalStage = want
	} else if p.FinalStage != want {
		return nil, fmt.Errorf("final_stage %s does not match the rings: the last ring ends at %d bps, which implies %s",
			p.FinalStage, p.Rings[len(p.Rings)-1].CumulativeBps, want)
	}
	if p.MinInstances <= 0 {
		p.MinInstances = 1
	}
	if p.AcceptanceThreshold == 0 {
		p.AcceptanceThreshold = 0.9
	}
	if p.AcceptanceThreshold <= 0 || p.AcceptanceThreshold > 1 {
		return nil, errors.New("acceptance_threshold must be in (0, 1]")
	}
	if p.MinReachableFraction == 0 {
		p.MinReachableFraction = 0.5
	}
	if p.MinReachableFraction <= 0 || p.MinReachableFraction > 1 {
		return nil, errors.New("min_reachable_fraction must be in (0, 1]")
	}
	p.RolloutTimeout = dur(p.RolloutTimeout, 5*time.Minute)
	p.RollbackTimeout = dur(p.RollbackTimeout, 2*time.Minute)
	p.ReachableWindow = dur(p.ReachableWindow, 30*time.Second)
	if p.MaxSdkRejections < 0 || p.MaxCrashLoops < 0 {
		return nil, errors.New("max_sdk_rejections and max_crash_loops cannot be negative")
	}
	if p.Gate == nil {
		p.Gate = &pb.GatePolicy{}
	}
	g := p.Gate
	if g.Mode == pb.GateMode_GATE_MODE_UNSPECIFIED {
		g.Mode = pb.GateMode_GATE_MODE_NONE
	}
	g.MinBake = dur(g.MinBake, 30*time.Second)
	switch g.Mode {
	case pb.GateMode_GATE_MODE_NONE:
		if p.Promotion == pb.Promotion_PROMOTION_UNSPECIFIED {
			p.Promotion = pb.Promotion_PROMOTION_MANUAL
		}
		if p.Promotion != pb.Promotion_PROMOTION_MANUAL {
			return nil, errors.New("gate.mode none requires promotion: manual (nothing would decide when to promote)")
		}
		if len(g.Metrics) > 0 {
			return nil, errors.New("gate.metrics are set but gate.mode is none")
		}
	case pb.GateMode_GATE_MODE_PROMETHEUS:
		if p.Promotion == pb.Promotion_PROMOTION_UNSPECIFIED {
			p.Promotion = pb.Promotion_PROMOTION_AUTO
		}
		g.LookInterval = dur(g.LookInterval, 15*time.Second)
		g.MaxSampleAge = dur(g.MaxSampleAge, 30*time.Second)
		g.EvalTimeout = dur(g.EvalTimeout, 10*time.Second)
		if g.Looks == 0 {
			g.Looks = 4
		}
		if g.Looks < 1 || g.Looks > 100 {
			return nil, errors.New("gate.looks must be between 1 and 100")
		}
		if g.AlphaHarm == 0 {
			g.AlphaHarm = 0.01
		}
		if g.AlphaNoninferiority == 0 {
			g.AlphaNoninferiority = 0.05
		}
		if g.AlphaHarm <= 0 || g.AlphaHarm >= 0.5 || g.AlphaNoninferiority <= 0 || g.AlphaNoninferiority >= 0.5 {
			return nil, errors.New("gate.alpha_harm and gate.alpha_noninferiority must be in (0, 0.5)")
		}
		if g.MinInstances == 0 {
			g.MinInstances = 3
		}
		if g.MinInstances < 2 {
			return nil, errors.New("gate.min_instances must be at least 2 (variance between instances needs two)")
		}
		if g.OnFail == pb.OnFail_ON_FAIL_UNSPECIFIED {
			g.OnFail = pb.OnFail_ON_FAIL_ROLLBACK
		}
		if g.MaxControlProportion < 0 || g.MaxControlProportion > 1 {
			return nil, errors.New("gate.max_control_proportion must be in [0, 1]")
		}
		if len(g.Metrics) == 0 {
			return nil, errors.New("gate.mode prometheus needs at least one metric")
		}
		if len(g.Metrics) > 8 {
			return nil, errors.New("at most 8 gate metrics")
		}
		names := map[string]bool{}
		for i, m := range g.Metrics {
			if m.Name == "" || names[m.Name] {
				return nil, fmt.Errorf("gate.metrics[%d]: name is empty or duplicated", i)
			}
			names[m.Name] = true
			if !metricName.MatchString(m.Metric) {
				return nil, fmt.Errorf("gate metric %q: metric %q is not a valid Prometheus metric name", m.Name, m.Metric)
			}
			switch m.Kind {
			case pb.MetricKind_METRIC_KIND_RATIO:
				if !metricName.MatchString(m.BadMetric) {
					return nil, fmt.Errorf("gate metric %q: bad_metric is required for kind ratio", m.Name)
				}
			case pb.MetricKind_METRIC_KIND_HISTOGRAM_OVER:
				if m.Le == "" {
					return nil, fmt.Errorf("gate metric %q: le (a bucket boundary) is required for kind histogram_over", m.Name)
				}
			case pb.MetricKind_METRIC_KIND_INSTANCE_EVENT:
			default:
				return nil, fmt.Errorf("gate metric %q: kind is required", m.Name)
			}
			for k := range m.Matchers {
				if !labelName.MatchString(k) {
					return nil, fmt.Errorf("gate metric %q: matcher label %q is invalid", m.Name, k)
				}
			}
			if m.Margin <= 0 || m.Margin >= 1 {
				return nil, fmt.Errorf("gate metric %q: margin must be in (0, 1); it is the tolerated absolute degradation", m.Name)
			}
			if m.HardMax < 0 || m.HardMax > 1 {
				return nil, fmt.Errorf("gate metric %q: hard_max must be in [0, 1]", m.Name)
			}
			if m.MinEvents == 0 {
				m.MinEvents = 100
				if m.Kind == pb.MetricKind_METRIC_KIND_INSTANCE_EVENT {
					m.MinEvents = int64(g.MinInstances)
				}
			}
		}
	default:
		return nil, errors.New("gate.mode is invalid")
	}
	return p, nil
}

// Ring converts to the assignment policy.
func Ring(p *pb.RolloutPolicy) ring.Policy {
	th := make([]int32, len(p.GetRings()))
	for i, r := range p.GetRings() {
		th[i] = r.CumulativeBps
	}
	return ring.Policy{Salt: p.GetSalt(), Thresholds: th, Pinned: p.GetPinnedGroups()}
}

// Rollout converts a normalized policy for the state machine.
func Rollout(p *pb.RolloutPolicy) rollout.Policy {
	g := p.GetGate()
	return rollout.Policy{
		Rings:                int32(len(p.GetRings())),
		AcceptanceThreshold:  p.GetAcceptanceThreshold(),
		MinReachableFraction: p.GetMinReachableFraction(),
		RolloutTimeout:       p.GetRolloutTimeout().AsDuration(),
		RollbackTimeout:      p.GetRollbackTimeout().AsDuration(),
		Auto:                 p.GetPromotion() == pb.Promotion_PROMOTION_AUTO,
		Gated:                g.GetMode() == pb.GateMode_GATE_MODE_PROMETHEUS,
		MinBake:              g.GetMinBake().AsDuration(),
		LookInterval:         g.GetLookInterval().AsDuration(),
		Looks:                g.GetLooks(),
		RollbackOnFail:       g.GetOnFail() != pb.OnFail_ON_FAIL_HALT,
		MaxSDKRejections:     p.GetMaxSdkRejections(),
		MaxCrashLoops:        p.GetMaxCrashLoops(),
	}
}
