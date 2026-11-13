package integration

import (
	"context"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	pb "github.com/raunaksahahere/cfgcanary/api/gen/cfgcanary/v1"
	"github.com/raunaksahahere/cfgcanary/internal/testenv"
	"github.com/raunaksahahere/cfgcanary/internal/testutil"
)

func ctx(t *testing.T) context.Context {
	c, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	return c
}

func manualPolicy(bps ...int32) *pb.RolloutPolicy {
	var rings []*pb.Ring
	for _, b := range bps {
		rings = append(rings, &pb.Ring{CumulativeBps: b})
	}
	return &pb.RolloutPolicy{
		Rings: rings, MinInstances: 1, AcceptanceThreshold: 1, MinReachableFraction: 0.5,
		RolloutTimeout: durationpb.New(time.Minute), RollbackTimeout: durationpb.New(30 * time.Second),
		ReachableWindow: durationpb.New(20 * time.Second), Promotion: pb.Promotion_PROMOTION_MANUAL,
		Gate: &pb.GatePolicy{Mode: pb.GateMode_GATE_MODE_NONE, MinBake: durationpb.New(500 * time.Millisecond)},
	}
}

func submit(t *testing.T, c pb.ControlServiceClient, name string, payload []byte) *pb.SubmitVersionResponse {
	t.Helper()
	r, err := c.SubmitVersion(ctx(t), &pb.SubmitVersionRequest{Name: name, Payload: payload})
	testutil.Must(t, err, "submit")
	return r
}

func mustVersion(t *testing.T, c pb.ControlServiceClient, name string, payload []byte) int64 {
	t.Helper()
	r := submit(t, c, name, payload)
	if !r.Report.Ok || r.Version == nil {
		t.Fatalf("submission rejected: %v", r.Report.Issues)
	}
	return r.Version.Version
}

func waitRelease(t *testing.T, c pb.ControlServiceClient, id string, timeout time.Duration, what string, cond func(*pb.Release) bool) *pb.Release {
	t.Helper()
	var last *pb.Release
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		r, err := c.GetRelease(context.Background(), &pb.GetReleaseRequest{Id: id})
		if err == nil {
			last = r
			if cond(r) {
				return r
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s; last state %v ring %d reason %q message %q rings %v", what, last.GetState(), last.GetActiveRing(), last.GetReason(), last.GetMessage(), last.GetRings())
	return nil
}

func waitInstances(t *testing.T, c pb.ControlServiceClient, n int) {
	t.Helper()
	testutil.Eventually(t, time.Minute, "instances to register", func() bool {
		r, err := c.ListInstances(context.Background(), &pb.ListInstancesRequest{Environment: "prod", PageSize: 500})
		return err == nil && len(r.Instances) >= n
	})
}

func register(t *testing.T, c pb.ControlServiceClient, spec *pb.ConfigSpec) {
	t.Helper()
	_, err := c.RegisterConfig(ctx(t), &pb.RegisterConfigRequest{Spec: spec})
	testutil.Must(t, err, "register")
}

var _ = testenv.Payload
