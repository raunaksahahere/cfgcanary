package bench

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	pb "github.com/raunaksahahere/cfgcanary/api/gen/cfgcanary/v1"
	"github.com/raunaksahahere/cfgcanary/internal/testenv"
	"github.com/raunaksahahere/cfgcanary/internal/testutil"
)

// BlastResult is one harmful-change run. Every count has the fleet size as
// its denominator.
type BlastResult struct {
	System          string  `json:"system"`
	Measured        bool    `json:"measured"`
	Fleet           int     `json:"fleet"`
	Eligible        int     `json:"eligible"`
	Targeted        int     `json:"targeted"`
	AgentApplied    int     `json:"agent_applied"`
	SDKAccepted     int     `json:"sdk_accepted"`
	Affected        int     `json:"affected"`
	DetectSeconds   float64 `json:"time_to_detect_seconds"`
	RollbackSeconds float64 `json:"time_to_rollback_seconds"`
	Unreachable     int     `json:"unreachable_at_rollback"`
	Notes           string  `json:"notes"`
}

func gated() *pb.RolloutPolicy {
	return &pb.RolloutPolicy{
		Rings:        []*pb.Ring{{Name: "canary", CumulativeBps: 1500}, {Name: "early", CumulativeBps: 4000}, {Name: "broad", CumulativeBps: 9000}},
		MinInstances: 3, AcceptanceThreshold: 0.9, MinReachableFraction: 0.8, RolloutTimeout: durationpb.New(2 * time.Minute),
		RollbackTimeout: durationpb.New(time.Minute), ReachableWindow: durationpb.New(30 * time.Second), Promotion: pb.Promotion_PROMOTION_AUTO,
		Gate: &pb.GatePolicy{Mode: pb.GateMode_GATE_MODE_PROMETHEUS, MinBake: durationpb.New(10 * time.Second), LookInterval: durationpb.New(5 * time.Second),
			Looks: 6, AlphaHarm: 0.01, AlphaNoninferiority: 0.05, MinInstances: 3, MaxSampleAge: durationpb.New(15 * time.Second),
			EvalTimeout: durationpb.New(5 * time.Second), OnFail: pb.OnFail_ON_FAIL_ROLLBACK, MaxControlProportion: 0.2,
			Metrics: []*pb.GateMetric{
				{Name: "errors", Kind: pb.MetricKind_METRIC_KIND_RATIO, Metric: "demo_requests_total", BadMetric: "demo_request_errors_total", Margin: 0.02, MinEvents: 200},
				{Name: "latency_over_100ms", Kind: pb.MetricKind_METRIC_KIND_HISTOGRAM_OVER, Metric: "demo_request_duration_seconds", Le: "0.1", Margin: 0.05, MinEvents: 200},
			}},
	}
}

func payloadFile(t *testing.T, name string) []byte {
	b, err := os.ReadFile(filepath.Join(testutil.RepoRoot(), "examples", "ratelimits", "payloads", name))
	testutil.Must(t, err, name)
	return b
}

// TestBlastRadius runs the harmful-timeout change through the gated pipeline
// (statistical gate only: no hard_max, so detection is by the sequential
// test), repeated, and the same change through an isolated naive push-to-all
// mode of the same system.
func TestBlastRadius(t *testing.T) {
	requireBench(t)
	var out []BlastResult
	for rep := range 3 {
		out = append(out, runBlast(t, rep, false))
	}
	out = append(out, runBlast(t, 0, true))
	out = append(out, reloaderModel(50, false), reloaderModel(50, true))
	writeJSON(t, "blast_radius.json", map[string]any{"environment": environment("CFGCANARY_BENCH=1 go test ./bench -run TestBlastRadius -timeout 30m"),
		"definitions": map[string]string{
			"eligible":      "instances in the fleet when the release was created",
			"targeted":      "instances whose ring was exposed to the harmful version",
			"agent_applied": "instances whose agent verified and durably applied it",
			"sdk_accepted":  "instances whose application (SDK) served it",
			"affected":      "instances that served it while producing errors (the harmful config makes requests time out)",
			"detect":        "release created until rollback started (cfgcanary: gate FAIL; naive: no detector in the delivery path)",
			"rollback":      "rollback started until every reachable exposed instance confirmed the fallback (server view)",
			"workload":      "50 simulated instances, 40 requests/s each, dependency latency lognormal (median 40 ms, sigma 0.5); harmful change sets timeout_ms 250 -> 20; Prometheus scrapes every 1 s; gate: errors and latency, min bake 10 s, look every 5 s, 6 looks",
		}, "results": out})
}

func runBlast(t *testing.T, rep int, naive bool) BlastResult {
	e := testenv.Start(t, testenv.WithPrometheus())
	admin := e.Control("users/admin")
	seedSpec := testenv.RatelimitsSpec("ratelimits", testenv.ManualPolicy(10000), nil)
	testenv.Register(t, admin, seedSpec)
	reg := prometheus.NewRegistry()
	fo := e.FleetOptions(50, "ratelimits")
	fo.RPS, fo.Registry, fo.Seed = 40, reg, uint64(100+rep)
	rec := newRecorder()
	fo.Observer = rec
	f := e.Fleet(fo)
	e.ServeMetrics(reg)
	testenv.WaitInstances(t, admin, 50)
	v1 := testenv.MustVersion(t, admin, "ratelimits", payloadFile(t, "v2-good.json"))
	testenv.PromoteThrough(t, admin, testenv.Create(t, admin, "ratelimits", v1), 1)
	testutil.Eventually(t, time.Minute, "fleet on v1", func() bool { return len(f.Served(v1)) == 50 })
	time.Sleep(15 * time.Second) // let the baseline accumulate metrics

	res := BlastResult{System: "cfgcanary (gated rings 15/40/90% + holdout)", Measured: true, Fleet: 50}
	spec := proto.Clone(seedSpec).(*pb.ConfigSpec)
	if naive {
		// Isolated benchmark mode: one 100% ring, size-only validation, no
		// gate. Not a supported configuration of the product.
		res.System = "naive push-to-all (isolated benchmark mode of the same system)"
		p := testenv.ManualPolicy(10000)
		p.Promotion = pb.Promotion_PROMOTION_MANUAL
		spec.Environments["prod"] = p
		spec.Validation = &pb.ValidationPolicy{Limits: &pb.Limits{MaxBytes: 1 << 20}}
	} else {
		spec.Environments["prod"] = gated()
	}
	testenv.Register(t, admin, spec)
	ctx := context.Background()
	sub, err := admin.SubmitVersion(ctx, &pb.SubmitVersionRequest{Name: "ratelimits", Payload: payloadFile(t, "v3-harmful-timeout.json")})
	testutil.Must(t, err, "submit")
	v3 := sub.Version.Version
	cr, err := admin.CreateRelease(ctx, &pb.CreateReleaseRequest{Config: "ratelimits", Environment: "prod", Version: v3})
	testutil.Must(t, err, "create")
	created := cr.Release.Created.AsTime()
	var r *pb.Release
	if naive {
		r = testenv.WaitRelease(t, admin, cr.Release.Id, time.Minute, "baking", testenv.Baking(0))
		testutil.Eventually(t, time.Minute, "everyone on v3", func() bool { return len(f.Served(v3)) == 50 })
		res.Notes = "nothing in the delivery path detects runtime harm; exposure reached the whole fleet in " +
			time.Since(created).Round(100*time.Millisecond).String() + ", and the config stays until a human notices"
		res.DetectSeconds = -1
		res.RollbackSeconds = -1
	} else {
		r = testenv.WaitRelease(t, admin, cr.Release.Id, 3*time.Minute, "rollback converged", func(r *pb.Release) bool {
			return r.State == pb.ReleaseState_RELEASE_STATE_ROLLED_BACK && r.Rollback != nil && r.Rollback.Converged
		})
		res.DetectSeconds = r.Rollback.Started.AsTime().Sub(created).Seconds()
		res.RollbackSeconds = r.Rollback.ConvergedAt.AsTime().Sub(r.Rollback.Started.AsTime()).Seconds()
		res.Unreachable = int(r.Rollback.Unreachable)
		res.Notes = "verdict: " + r.Rings[0].GateReason
	}
	res.Eligible = 0
	for _, rs := range r.Rings {
		res.Eligible += int(rs.Counts.Eligible)
		if rs.Index <= max(r.ActiveRing, 0) {
			res.Targeted += int(rs.Counts.Eligible)
		}
	}
	if r.Holdout != nil {
		res.Eligible += int(r.Holdout.Eligible)
	}
	for _, m := range f.Members {
		if _, ok := rec.appliedAt(m.ID, v3); ok {
			res.AgentApplied++
		}
		if m.EverServed(v3) {
			res.SDKAccepted++
			res.Affected++ // every instance serving timeout_ms=20 fails most requests
		}
	}
	return res
}

// reloaderModel is a labelled model (not a test of Stakater Reloader): a
// ConfigMap change triggers a rolling restart with maxUnavailable 25%; each
// batch restarts in restartSeconds; readiness checks only liveness of the
// process. A config that crashes the parser fails readiness, so the rollout
// stalls after the first batch; a config that is harmful but starts fine
// passes readiness and reaches every pod.
func reloaderModel(n int, crashes bool) BlastResult {
	const restartSeconds = 15.0
	batch := (n + 3) / 4
	r := BlastResult{System: "rolling restart on ConfigMap change (Reloader-style MODEL, not the real product)", Measured: false, Fleet: n,
		Eligible: n, DetectSeconds: -1, RollbackSeconds: -1}
	if crashes {
		r.Targeted, r.AgentApplied, r.SDKAccepted, r.Affected = batch, batch, batch, batch
		r.Notes = "parser-crashing config: first batch (25%) crash-loops, rollout stalls; no content validation, no automatic rollback; " +
			"pods keep crash-looping (no last-known-good)"
	} else {
		r.Targeted, r.AgentApplied, r.SDKAccepted, r.Affected = n, n, n, n
		r.Notes = "runtime-harmful but startable config: passes readiness, every batch restarts onto it in ~4 x " + fmt.Sprint(restartSeconds) + " s; nothing compares error rates"
	}
	return r
}
