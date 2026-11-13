package integration

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	pb "github.com/raunaksahahere/cfgcanary/api/gen/cfgcanary/v1"
	"github.com/raunaksahahere/cfgcanary/internal/testenv"
	"github.com/raunaksahahere/cfgcanary/internal/testutil"
)

// GatedPolicy is the demo's gated policy: three rings ending at 90%, so a 10%
// holdout stays on the baseline as the control for the final comparison.
func gatedPolicy() *pb.RolloutPolicy {
	return &pb.RolloutPolicy{
		Rings:        []*pb.Ring{{Name: "canary", CumulativeBps: 1500}, {Name: "early", CumulativeBps: 4000}, {Name: "broad", CumulativeBps: 9000}},
		MinInstances: 3, AcceptanceThreshold: 0.9, MinReachableFraction: 0.8,
		RolloutTimeout: durationpb.New(time.Minute), RollbackTimeout: durationpb.New(30 * time.Second),
		ReachableWindow: durationpb.New(20 * time.Second), Promotion: pb.Promotion_PROMOTION_AUTO,
		Gate: &pb.GatePolicy{
			Mode: pb.GateMode_GATE_MODE_PROMETHEUS, MinBake: durationpb.New(4 * time.Second), LookInterval: durationpb.New(2 * time.Second),
			Looks: 10, AlphaHarm: 0.01, AlphaNoninferiority: 0.05, MinInstances: 3, MaxSampleAge: durationpb.New(15 * time.Second),
			EvalTimeout: durationpb.New(5 * time.Second), OnFail: pb.OnFail_ON_FAIL_ROLLBACK, MaxControlProportion: 0.2,
			Metrics: []*pb.GateMetric{
				{Name: "errors", Kind: pb.MetricKind_METRIC_KIND_RATIO, Metric: "demo_requests_total", BadMetric: "demo_request_errors_total",
					Margin: 0.02, HardMax: 0.5, MinEvents: 200},
				{Name: "latency_over_100ms", Kind: pb.MetricKind_METRIC_KIND_HISTOGRAM_OVER, Metric: "demo_request_duration_seconds", Le: "0.1",
					Margin: 0.05, MinEvents: 200},
				{Name: "restarts", Kind: pb.MetricKind_METRIC_KIND_INSTANCE_EVENT, Metric: "demo_restarts_total", Margin: 0.5, GuardOnly: true},
			},
		},
	}
}

func TestGatedReleaseAndCanaryRollback(t *testing.T) {
	e := testenv.Start(t, testenv.WithPrometheus())
	admin := e.Control("users/admin")
	// Seed a baseline with a manual, ungated policy, then switch the
	// environment to the gated policy (a normal config update).
	spec := testenv.RatelimitsSpec("ratelimits", manualPolicy(10000), nil)
	register(t, admin, spec)
	reg := prometheus.NewRegistry()
	fo := e.FleetOptions(50, "ratelimits")
	fo.RPS, fo.Registry = 40, reg
	fleet := e.Fleet(fo)
	e.ServeMetrics(reg)
	waitInstances(t, admin, 50)
	v1 := mustVersion(t, admin, "ratelimits", testenv.Payload(10, 250))
	cr, err := admin.CreateRelease(ctx(t), &pb.CreateReleaseRequest{Config: "ratelimits", Environment: "prod", Version: v1})
	testutil.Must(t, err, "seed release")
	promoteThrough(t, admin, cr.Release.Id, 1)
	waitRelease(t, admin, cr.Release.Id, time.Minute, "seed complete", func(r *pb.Release) bool { return r.State == pb.ReleaseState_RELEASE_STATE_COMPLETE })
	testutil.Eventually(t, 30*time.Second, "fleet on v1", func() bool { return len(fleet.Served(v1)) == 50 })

	spec2 := proto.Clone(spec).(*pb.ConfigSpec)
	spec2.Environments["prod"] = gatedPolicy()
	register(t, admin, spec2)

	// A good change promotes automatically, ring by ring, on PASS evidence.
	v2 := mustVersion(t, admin, "ratelimits", testenv.Payload(11, 250))
	plan, err := admin.CreateRelease(ctx(t), &pb.CreateReleaseRequest{Config: "ratelimits", Environment: "prod", Version: v2, DryRun: true})
	testutil.Must(t, err, "plan")
	if len(plan.Plan.Blockers) > 0 {
		t.Fatalf("plan blockers: %v", plan.Plan.Blockers)
	}
	t.Logf("cohorts: ring0=%d ring1=%d ring2=%d holdout=%d", plan.Plan.Rings[0].Members, plan.Plan.Rings[1].Members, plan.Plan.Rings[2].Members, plan.Plan.HoldoutMembers)
	cr, err = admin.CreateRelease(ctx(t), &pb.CreateReleaseRequest{Config: "ratelimits", Environment: "prod", Version: v2})
	testutil.Must(t, err, "create v2")
	good := waitRelease(t, admin, cr.Release.Id, 4*time.Minute, "gated release to finish", func(r *pb.Release) bool {
		return r.State == pb.ReleaseState_RELEASE_STATE_COMPLETE || r.State == pb.ReleaseState_RELEASE_STATE_HALTED || r.State == pb.ReleaseState_RELEASE_STATE_ROLLED_BACK
	})
	if good.State != pb.ReleaseState_RELEASE_STATE_COMPLETE {
		t.Fatalf("good release ended %v: %s %s", good.State, good.Reason, good.Message)
	}
	for _, r := range good.Rings {
		if r.GateVerdict != pb.GateVerdict_GATE_VERDICT_PASS {
			t.Fatalf("ring %d completed without a PASS verdict: %v %s", r.Index, r.GateVerdict, r.GateReason)
		}
	}
	testutil.Eventually(t, 30*time.Second, "fleet on v2", func() bool { return len(fleet.Served(v2)) == 50 })

	// A globally valid config that degrades the application at runtime.
	bad := testenv.Payload(11, 20)
	sub := submit(t, admin, "ratelimits", bad)
	if !sub.Report.Ok {
		t.Fatalf("the harmful config must pass validation (it is schema- and parser-valid): %v", sub.Report.Issues)
	}
	v3 := sub.Version.Version
	plan, _ = admin.CreateRelease(ctx(t), &pb.CreateReleaseRequest{Config: "ratelimits", Environment: "prod", Version: v3, DryRun: true})
	ring0 := map[string]bool{}
	cr, err = admin.CreateRelease(ctx(t), &pb.CreateReleaseRequest{Config: "ratelimits", Environment: "prod", Version: v3})
	testutil.Must(t, err, "create v3")
	r := waitRelease(t, admin, cr.Release.Id, 3*time.Minute, "rollback", func(r *pb.Release) bool {
		return r.State == pb.ReleaseState_RELEASE_STATE_ROLLED_BACK || r.State == pb.ReleaseState_RELEASE_STATE_COMPLETE
	})
	if r.State != pb.ReleaseState_RELEASE_STATE_ROLLED_BACK || r.Reason != "gate_fail" {
		t.Fatalf("harmful release ended %v (%s): %s", r.State, r.Reason, r.Message)
	}
	if r.Rings[0].GateVerdict != pb.GateVerdict_GATE_VERDICT_FAIL {
		t.Fatalf("ring 0 verdict %v", r.Rings[0].GateVerdict)
	}
	if !r.Rollback.Converged || r.Rollback.FallbackVersion != v2 || r.Rollback.Unreachable != 0 {
		t.Fatalf("rollback status %+v", r.Rollback)
	}
	// Per-agent evidence: only ring-0 members ever served v3; later rings
	// never accepted it, and everyone is back on v2.
	exposed := 0
	for _, m := range fleet.Members {
		if m.EverServed(v3) {
			exposed++
			ring0[m.ID] = true
		}
	}
	if exposed == 0 || exposed > int(plan.Plan.Rings[0].Members) {
		t.Fatalf("%d instances served the harmful config; ring 0 has %d", exposed, plan.Plan.Rings[0].Members)
	}
	for _, rs := range r.Rings[1:] {
		if rs.Counts.Accepted != 0 || rs.Counts.Applied != 0 {
			t.Fatalf("ring %d acknowledged the harmful config: %+v", rs.Index, rs.Counts)
		}
	}
	testutil.Eventually(t, 30*time.Second, "fleet back on v2", func() bool { return len(fleet.Served(v2)) == 50 })
	t.Logf("harmful config served by %d/50 instances (ring 0 = %d) before rollback", exposed, plan.Plan.Rings[0].Members)
}
