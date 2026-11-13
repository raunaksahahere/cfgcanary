package integration

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	pb "github.com/raunaksahahere/cfgcanary/api/gen/cfgcanary/v1"
	"github.com/raunaksahahere/cfgcanary/internal/testenv"
	"github.com/raunaksahahere/cfgcanary/internal/testutil"
)

func readPayload(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(testutil.RepoRoot(), "examples", "ratelimits", "payloads", name))
	testutil.Must(t, err, name)
	return b
}

// Violations an ingress validator can detect are blocked before any rollout:
// they reach zero agents. (The source's "oversized config reaches only ring
// 0" is resolved this way; runtime-harmful configs are covered by the gate
// test.)
func TestSubmissionRejectionReachesZeroAgents(t *testing.T) {
	e := testenv.Start(t)
	admin := e.Control("users/admin")
	wasm := testutil.BuildWasm(t, "examples/ratelimits/validator")
	register(t, admin, testenv.RatelimitsSpec("ratelimits", manualPolicy(10000), wasm))
	fleet := e.Fleet(e.FleetOptions(10, "ratelimits"))
	waitInstances(t, admin, 10)
	v1 := mustVersion(t, admin, "ratelimits", readPayload(t, "v2-good.json"))
	cr, err := admin.CreateRelease(ctx(t), &pb.CreateReleaseRequest{Config: "ratelimits", Environment: "prod", Version: v1})
	testutil.Must(t, err, "release")
	promoteThrough(t, admin, cr.Release.Id, 1)
	testutil.Eventually(t, 30*time.Second, "fleet on v1", func() bool { return len(fleet.Served(v1)) == 10 })
	before, _ := admin.GetTarget(ctx(t), &pb.GetTargetRequest{Config: "ratelimits", Environment: "prod"})

	cases := map[string]string{
		"bad-oversized.json":        "limits/too_large",
		"bad-over-cardinality.json": "limits/too_many_items",
		"bad-malformed.json":        "json/syntax",
		"bad-blank-service.json":    "schema/",
		"bad-doubled.json":          "relative/size_grew",
		"bad-parser-rule.json":      "wasm/rejected",
	}
	for file, want := range cases {
		r := submit(t, admin, "ratelimits", readPayload(t, file))
		if r.Report.Ok || r.Version != nil {
			t.Fatalf("%s was accepted", file)
		}
		got := r.Report.Issues[0].Stage + "/" + r.Report.Issues[0].Code
		if len(got) < len(want) || got[:len(want)] != want {
			t.Errorf("%s: first issue %s, want %s (%v)", file, got, want, r.Report.Issues)
		}
	}
	// Nothing was stored, no release can reference it, no agent changed.
	vs, _ := admin.ListVersions(ctx(t), &pb.ListVersionsRequest{Name: "ratelimits"})
	if len(vs.Versions) != 1 {
		t.Fatalf("%d versions stored", len(vs.Versions))
	}
	after, _ := admin.GetTarget(ctx(t), &pb.GetTargetRequest{Config: "ratelimits", Environment: "prod"})
	if after.Generation != before.Generation {
		t.Fatal("a rejected submission changed desired state")
	}
	time.Sleep(time.Second)
	if n := len(fleet.Served(v1)); n != 10 {
		t.Fatalf("only %d/10 instances still serve v1", n)
	}
	for _, m := range fleet.Members {
		for v := v1 + 1; v < v1+10; v++ {
			if m.EverServed(v) {
				t.Fatalf("%s served a version that was never stored", m.ID)
			}
		}
	}
	ev, _ := admin.ListAudit(ctx(t), &pb.ListAuditRequest{Config: "ratelimits", Action: "version.reject", PageSize: 50})
	if len(ev.Events) != len(cases) {
		t.Fatalf("%d rejections audited, want %d", len(ev.Events), len(cases))
	}
}

// A payload that passes the server's (deliberately loose) validation but that
// the consumer's parser rejects: SDKs keep their last-known-good, report
// REJECTED (never counted as applied-and-accepted), and the controller rolls
// back automatically.
func TestSDKRejectionKeepsLKGAndRollsBack(t *testing.T) {
	e := testenv.Start(t)
	admin := e.Control("users/admin")
	pol := manualPolicy(3000, 10000)
	register(t, admin, testenv.RatelimitsSpec("ratelimits", pol, nil)) // no WASM stage
	fleet := e.Fleet(e.FleetOptions(20, "ratelimits"))
	waitInstances(t, admin, 20)
	v1 := mustVersion(t, admin, "ratelimits", readPayload(t, "v2-good.json"))
	cr, err := admin.CreateRelease(ctx(t), &pb.CreateReleaseRequest{Config: "ratelimits", Environment: "prod", Version: v1})
	testutil.Must(t, err, "seed")
	promoteThrough(t, admin, cr.Release.Id, 2)
	testutil.Eventually(t, 30*time.Second, "fleet on v1", func() bool { return len(fleet.Served(v1)) == 20 })

	sub := submit(t, admin, "ratelimits", readPayload(t, "bad-parser-rule.json"))
	if !sub.Report.Ok {
		t.Fatalf("without the WASM stage the server must accept this payload: %v", sub.Report.Issues)
	}
	v2 := sub.Version.Version
	cr, err = admin.CreateRelease(ctx(t), &pb.CreateReleaseRequest{Config: "ratelimits", Environment: "prod", Version: v2})
	testutil.Must(t, err, "bad release")
	r := waitRelease(t, admin, cr.Release.Id, time.Minute, "automatic rollback", func(r *pb.Release) bool {
		return r.State == pb.ReleaseState_RELEASE_STATE_ROLLED_BACK
	})
	if r.Reason != "sdk_rejected" {
		t.Fatalf("rolled back for %q", r.Reason)
	}
	if r.Rings[0].Counts.Rejected == 0 || r.Rings[0].Counts.Accepted != 0 {
		t.Fatalf("ring 0 counts %+v: rejections must be counted and never as accepted", r.Rings[0].Counts)
	}
	if r.Rings[1].Counts.Applied != 0 {
		t.Fatalf("ring 1 applied the rejected config: %+v", r.Rings[1].Counts)
	}
	for _, m := range fleet.Members {
		if m.EverServed(v2) {
			t.Fatalf("%s served a payload its parser rejected", m.ID)
		}
	}
	if n := len(fleet.Served(v1)); n != 20 {
		t.Fatalf("%d/20 instances on the last-known-good", n)
	}
}
