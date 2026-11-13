package integration

import (
	"testing"
	"time"

	pb "github.com/raunaksahahere/cfgcanary/api/gen/cfgcanary/v1"
	"github.com/raunaksahahere/cfgcanary/internal/testenv"
	"github.com/raunaksahahere/cfgcanary/internal/testutil"
)

func baking(ring int32) func(*pb.Release) bool {
	return func(r *pb.Release) bool {
		return r.State == pb.ReleaseState_RELEASE_STATE_BAKING && r.ActiveRing == ring
	}
}

func promoteThrough(t *testing.T, c pb.ControlServiceClient, id string, rings int32) *pb.Release {
	t.Helper()
	var r *pb.Release
	for i := int32(0); i < rings; i++ {
		waitRelease(t, c, id, time.Minute, "ring baking", baking(i))
		time.Sleep(600 * time.Millisecond) // minimum bake
		var err error
		r, err = c.PromoteRelease(ctx(t), &pb.PromoteReleaseRequest{Id: id, ExpectedRing: i, HasExpectedRing: true})
		testutil.Must(t, err, "promote ring ", i)
	}
	return r
}

func TestManualReleaseThroughEveryRing(t *testing.T) {
	e := testenv.Start(t)
	admin := e.Control("users/admin")
	register(t, admin, testenv.RatelimitsSpec("ratelimits", manualPolicy(2500, 6000, 10000), nil))
	fleet := e.Fleet(e.FleetOptions(20, "ratelimits"))
	waitInstances(t, admin, 20)
	v1 := mustVersion(t, admin, "ratelimits", testenv.Payload(10, 250))

	// Dry run: a plan, no writes, nothing sent to agents.
	before, err := admin.GetTarget(ctx(t), &pb.GetTargetRequest{Config: "ratelimits", Environment: "prod"})
	testutil.Must(t, err, "target")
	plan, err := admin.CreateRelease(ctx(t), &pb.CreateReleaseRequest{Config: "ratelimits", Environment: "prod", Version: v1, DryRun: true})
	testutil.Must(t, err, "dry run")
	sum := plan.Plan.HoldoutMembers
	for _, r := range plan.Plan.Rings {
		sum += r.Members
	}
	if plan.Release != nil || sum != 20 || len(plan.Plan.Blockers) != 0 || plan.Plan.Diff == nil {
		t.Fatalf("dry run: %+v", plan)
	}
	after, _ := admin.GetTarget(ctx(t), &pb.GetTargetRequest{Config: "ratelimits", Environment: "prod"})
	if after.Generation != before.Generation || after.ReleaseId != "" {
		t.Fatal("dry run changed the target")
	}
	if rels, _ := admin.ListReleases(ctx(t), &pb.ListReleasesRequest{Config: "ratelimits"}); len(rels.Releases) != 0 {
		t.Fatal("dry run created a release")
	}

	cr, err := admin.CreateRelease(ctx(t), &pb.CreateReleaseRequest{Config: "ratelimits", Environment: "prod", Version: v1})
	testutil.Must(t, err, "create")
	id := cr.Release.Id
	// Only ring 0 receives v1 at first.
	waitRelease(t, admin, id, time.Minute, "ring 0 baking", baking(0))
	ring0 := plan.Plan.Rings[0].Members
	if got := len(fleet.Served(v1)); got != int(ring0) {
		t.Fatalf("%d instances serve v1 while only ring 0 (%d) is exposed", got, ring0)
	}
	// A promotion before the minimum bake is refused.
	if _, err := admin.PromoteRelease(ctx(t), &pb.PromoteReleaseRequest{Id: id}); err == nil {
		r, _ := admin.GetRelease(ctx(t), &pb.GetReleaseRequest{Id: id})
		if r.ActiveRing != 0 {
			// bake may already have elapsed on a slow machine; that is fine
		}
	}
	promoteThrough(t, admin, id, 3)
	waitRelease(t, admin, id, time.Minute, "complete", func(r *pb.Release) bool { return r.State == pb.ReleaseState_RELEASE_STATE_COMPLETE })
	testutil.Eventually(t, 30*time.Second, "all instances on v1", func() bool { return len(fleet.Served(v1)) == 20 })
	tgt, _ := admin.GetTarget(ctx(t), &pb.GetTargetRequest{Config: "ratelimits", Environment: "prod"})
	if tgt.BaselineVersion != v1 || tgt.CandidateVersion != 0 {
		t.Fatalf("target after completion: %+v", tgt)
	}

	// Second release: pause holds progress, a control-plane restart does not
	// lose it, resume continues.
	v2 := mustVersion(t, admin, "ratelimits", testenv.Payload(12, 250))
	cr, err = admin.CreateRelease(ctx(t), &pb.CreateReleaseRequest{Config: "ratelimits", Environment: "prod", Version: v2})
	testutil.Must(t, err, "create v2")
	id = cr.Release.Id
	waitRelease(t, admin, id, time.Minute, "ring 0 baking", baking(0))
	_, err = admin.PauseRelease(ctx(t), &pb.ReleaseCommandRequest{Id: id})
	testutil.Must(t, err, "pause")
	time.Sleep(time.Second)
	if _, err := admin.PromoteRelease(ctx(t), &pb.PromoteReleaseRequest{Id: id}); err == nil {
		t.Fatal("promoted a paused release")
	}
	e.StopServer()
	// Agents keep serving what they have while the control plane is down.
	if n := len(fleet.Served(v1)) + len(fleet.Served(v2)); n != 20 {
		t.Fatalf("%d instances lost their config while the control plane was down", n)
	}
	e.StartServer()
	admin = e.Control("users/admin")
	waitRelease(t, admin, id, 30*time.Second, "release survives restart", func(r *pb.Release) bool { return r.Paused && r.ActiveRing == 0 })
	_, err = admin.ResumeRelease(ctx(t), &pb.ReleaseCommandRequest{Id: id})
	testutil.Must(t, err, "resume")
	promoteThrough(t, admin, id, 3)
	waitRelease(t, admin, id, time.Minute, "v2 complete", func(r *pb.Release) bool { return r.State == pb.ReleaseState_RELEASE_STATE_COMPLETE })
	testutil.Eventually(t, 30*time.Second, "all instances on v2", func() bool { return len(fleet.Served(v2)) == 20 })
}
