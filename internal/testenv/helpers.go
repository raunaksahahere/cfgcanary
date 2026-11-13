package testenv

import (
	"context"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	pb "github.com/raunaksahahere/cfgcanary/api/gen/cfgcanary/v1"
	"github.com/raunaksahahere/cfgcanary/internal/testutil"
)

// Ctx returns a context bounded by the test.
func Ctx(t testing.TB) context.Context {
	c, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	return c
}

// ManualPolicy is an ungated policy with manual promotion.
func ManualPolicy(bps ...int32) *pb.RolloutPolicy {
	var rings []*pb.Ring
	for _, b := range bps {
		rings = append(rings, &pb.Ring{CumulativeBps: b})
	}
	return &pb.RolloutPolicy{
		Rings: rings, MinInstances: 1, AcceptanceThreshold: 1, MinReachableFraction: 0.5,
		RolloutTimeout: durationpb.New(time.Minute), RollbackTimeout: durationpb.New(time.Minute),
		ReachableWindow: durationpb.New(5 * time.Second), Promotion: pb.Promotion_PROMOTION_MANUAL,
		Gate: &pb.GatePolicy{Mode: pb.GateMode_GATE_MODE_NONE, MinBake: durationpb.New(300 * time.Millisecond)},
	}
}

// Register registers a config.
func Register(t testing.TB, c pb.ControlServiceClient, spec *pb.ConfigSpec) {
	t.Helper()
	_, err := c.RegisterConfig(Ctx(t), &pb.RegisterConfigRequest{Spec: spec})
	testutil.Must(t, err, "register")
}

// MustVersion submits a payload that must be accepted.
func MustVersion(t testing.TB, c pb.ControlServiceClient, name string, payload []byte) int64 {
	t.Helper()
	r, err := c.SubmitVersion(Ctx(t), &pb.SubmitVersionRequest{Name: name, Payload: payload})
	testutil.Must(t, err, "submit")
	if !r.Report.Ok {
		t.Fatalf("rejected: %v", r.Report.Issues)
	}
	return r.Version.Version
}

// Create creates a release.
func Create(t testing.TB, c pb.ControlServiceClient, name string, v int64) string {
	t.Helper()
	r, err := c.CreateRelease(Ctx(t), &pb.CreateReleaseRequest{Config: name, Environment: "prod", Version: v})
	testutil.Must(t, err, "create release")
	return r.Release.Id
}

// WaitRelease polls until cond holds.
func WaitRelease(t testing.TB, c pb.ControlServiceClient, id string, timeout time.Duration, what string, cond func(*pb.Release) bool) *pb.Release {
	t.Helper()
	var last *pb.Release
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		r, err := c.GetRelease(ctx, &pb.GetReleaseRequest{Id: id})
		cancel()
		if err == nil {
			last = r
			if cond(r) {
				return r
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s; last: state %v ring %d reason %q message %q", what, last.GetState(), last.GetActiveRing(), last.GetReason(), last.GetMessage())
	return nil
}

// Baking matches a release baking at ring.
func Baking(ring int32) func(*pb.Release) bool {
	return func(r *pb.Release) bool {
		return r.State == pb.ReleaseState_RELEASE_STATE_BAKING && r.ActiveRing == ring
	}
}

// State matches a release state.
func State(s pb.ReleaseState) func(*pb.Release) bool {
	return func(r *pb.Release) bool { return r.State == s }
}

// PromoteThrough promotes ring by ring until the release completes.
func PromoteThrough(t testing.TB, c pb.ControlServiceClient, id string, rings int32) {
	t.Helper()
	for i := int32(0); i < rings; i++ {
		WaitRelease(t, c, id, time.Minute, "ring baking", Baking(i))
		time.Sleep(400 * time.Millisecond)
		for attempt := 0; ; attempt++ {
			_, err := c.PromoteRelease(Ctx(t), &pb.PromoteReleaseRequest{Id: id, ExpectedRing: i, HasExpectedRing: true})
			if err == nil {
				break
			}
			if attempt > 20 {
				t.Fatalf("promote ring %d: %v", i, err)
			}
			time.Sleep(200 * time.Millisecond)
		}
	}
	WaitRelease(t, c, id, time.Minute, "complete", State(pb.ReleaseState_RELEASE_STATE_COMPLETE))
}

// WaitInstances waits until n instances registered.
func WaitInstances(t testing.TB, c pb.ControlServiceClient, n int) {
	t.Helper()
	testutil.Eventually(t, time.Minute, "instances to register", func() bool {
		r, err := c.ListInstances(context.Background(), &pb.ListInstancesRequest{Environment: "prod", PageSize: 1000})
		return err == nil && len(r.Instances) >= n
	})
}
