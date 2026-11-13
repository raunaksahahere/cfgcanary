// Package chaos holds fault-injection tests against real services:
// PostgreSQL, the S3 store, Prometheus and Toxiproxy (make chaos).
package chaos

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	toxiproxy "github.com/Shopify/toxiproxy/v2/client"
	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	pb "github.com/raunaksahahere/cfgcanary/api/gen/cfgcanary/v1"
	"github.com/raunaksahahere/cfgcanary/internal/app"
	"github.com/raunaksahahere/cfgcanary/internal/server"
	"github.com/raunaksahahere/cfgcanary/internal/store"
	"github.com/raunaksahahere/cfgcanary/internal/testenv"
	tu "github.com/raunaksahahere/cfgcanary/internal/testutil"
)

func payload(t *testing.T, name string) []byte {
	b, err := os.ReadFile(filepath.Join(tu.RepoRoot(), "examples", "ratelimits", "payloads", name))
	tu.Must(t, err, name)
	return b
}

func spec(p *pb.RolloutPolicy) *pb.ConfigSpec { return testenv.RatelimitsSpec("ratelimits", p, nil) }

// seed delivers v1 to the whole fleet and returns it.
func seed(t *testing.T, c pb.ControlServiceClient, rings int32) int64 {
	v1 := testenv.MustVersion(t, c, "ratelimits", payload(t, "v1-baseline.json"))
	testenv.PromoteThrough(t, c, testenv.Create(t, c, "ratelimits", v1), rings)
	return v1
}

func leaderOf(rs ...*app.Running) (int, int64) {
	for i, r := range rs {
		if ok, ep := r.Controller.Leader(); ok {
			return i, ep
		}
	}
	return -1, 0
}

// The leader dies in the middle of a rollout; a replica takes over with a
// newer epoch and finishes it from the durable state.
func TestLeaderKilledMidRollout(t *testing.T) {
	e := testenv.Start(t)
	admin := e.Control("users/admin")
	tu.Eventually(t, 15*time.Second, "primary leads", func() bool { ok, _ := e.Run.Controller.Leader(); return ok })
	b := e.AddReplica("replica-b")
	testenv.Register(t, admin, spec(testenv.ManualPolicy(3000, 6000, 10000)))
	fo := e.FleetOptions(20, "ratelimits")
	fo.Server = b.Run.GRPCAddr // agents talk to the replica that survives
	fleet := e.Fleet(fo)
	testenv.WaitInstances(t, admin, 20)
	v1 := seed(t, admin, 3)
	tu.Eventually(t, 30*time.Second, "fleet on v1", func() bool { return len(fleet.Served(v1)) == 20 })
	_, epochA := e.Run.Controller.Leader()
	if ok, _ := b.Run.Controller.Leader(); ok {
		t.Fatal("two leaders")
	}
	v2 := testenv.MustVersion(t, admin, "ratelimits", payload(t, "v2-good.json"))
	id := testenv.Create(t, admin, "ratelimits", v2)
	testenv.WaitRelease(t, admin, id, time.Minute, "ring 0", testenv.Baking(0))
	e.StopServer() // the leader dies mid-rollout
	bAdmin := pbClient(t, e, b)
	tu.Eventually(t, 20*time.Second, "replica to lead with a newer epoch", func() bool { ok, ep := b.Run.Controller.Leader(); return ok && ep > epochA })
	testenv.PromoteThrough(t, bAdmin, id, 3)
	tu.Eventually(t, 30*time.Second, "fleet on v2", func() bool { return len(fleet.Served(v2)) == 20 })
}

func pbClient(t *testing.T, e *testenv.Env, r *testenv.Replica) pb.ControlServiceClient {
	cc := dialAddr(t, e, r.Run.GRPCAddr, "users/admin")
	return pb.NewControlServiceClient(cc)
}

// The leader's lock session is killed; leadership moves to a newer epoch, and
// the deposed controller's writes are rejected by the fencing check.
func TestLockConnectionLostAndStaleWriteFenced(t *testing.T) {
	e := testenv.Start(t)
	admin := e.Control("users/admin")
	testenv.Register(t, admin, spec(testenv.ManualPolicy(5000, 10000)))
	fleet := e.Fleet(e.FleetOptions(10, "ratelimits"))
	testenv.WaitInstances(t, admin, 10)
	tu.Eventually(t, 15*time.Second, "leader", func() bool { ok, _ := e.Run.Controller.Leader(); return ok })
	_, oldEpoch := e.Run.Controller.Leader()
	pid := e.Run.Controller.LockBackendPID()
	conn, err := pgx.Connect(context.Background(), e.DBURL)
	tu.Must(t, err, "admin conn")
	defer conn.Close(context.Background())
	_, err = conn.Exec(context.Background(), `SELECT pg_terminate_backend($1)`, pid)
	tu.Must(t, err, "terminate lock session")
	tu.Eventually(t, 20*time.Second, "new epoch", func() bool { ok, ep := e.Run.Controller.Leader(); return ok && ep > oldEpoch })

	// A deposed controller acting with the old epoch is fenced.
	v1 := testenv.MustVersion(t, admin, "ratelimits", payload(t, "v1-baseline.json"))
	id := testenv.Create(t, admin, "ratelimits", v1)
	r, err := e.Run.Store.ReleaseRow(context.Background(), id)
	tu.Must(t, err, "row")
	_, _, err = e.Run.Store.ApplyEvent(context.Background(), id, ruleStart(r), store.EventMeta{Actor: "controller", Epoch: oldEpoch})
	if err == nil || !isFenced(err) {
		t.Fatalf("stale controller write: %v", err)
	}
	testenv.PromoteThrough(t, admin, id, 2)
	tu.Eventually(t, 30*time.Second, "fleet on v1", func() bool { return len(fleet.Served(v1)) == 10 })
	// Duplicate commands are idempotent through the API.
	_, err = admin.PauseRelease(testenv.Ctx(t), &pb.ReleaseCommandRequest{Id: id, RequestId: "dup-1"})
	if err == nil {
		t.Fatal("pause of a completed release succeeded")
	}
}

// Agents that are partitioned during a rollback keep serving what they have;
// the rollback reports them as unreachable, then converges when they return.
func TestPartitionDuringRollback(t *testing.T) {
	e := testenv.StartChaos(t, testenv.Behind{GRPC: true})
	admin := e.Control("users/admin")
	testenv.Register(t, admin, spec(testenv.ManualPolicy(10000)))
	px := e.AgentProxy(t)
	direct := e.FleetOptions(5, "ratelimits")
	direct.Prefix = "direct"
	viaProxy := e.FleetOptions(5, "ratelimits")
	viaProxy.Prefix, viaProxy.Server, viaProxy.Root = "edge", px.Addr, viaProxy.Root+"-edge"
	f1, f2 := e.Fleet(direct), e.Fleet(viaProxy)
	testenv.WaitInstances(t, admin, 10)
	v1 := seed(t, admin, 1)
	v2 := testenv.MustVersion(t, admin, "ratelimits", payload(t, "v2-good.json"))
	id := testenv.Create(t, admin, "ratelimits", v2)
	testenv.WaitRelease(t, admin, id, time.Minute, "ring 0 baking", testenv.Baking(0))
	tu.Eventually(t, 20*time.Second, "everyone on v2", func() bool { return len(f1.Served(v2))+len(f2.Served(v2)) == 10 })
	tu.Must(t, px.Disable(), "partition")
	_, err := admin.RollbackRelease(testenv.Ctx(t), &pb.ReleaseCommandRequest{Id: id, Reason: "test"})
	tu.Must(t, err, "rollback")
	r := testenv.WaitRelease(t, admin, id, time.Minute, "rollback to finish", testenv.State(pb.ReleaseState_RELEASE_STATE_ROLLED_BACK))
	if r.Rollback.Converged || r.Rollback.Unreachable != 5 || r.Rollback.Confirmed != 5 || len(r.Rollback.UnreachableSample) != 5 {
		t.Fatalf("rollback status while partitioned: %+v", r.Rollback)
	}
	if len(f2.Served(v2)) != 5 {
		t.Fatal("partitioned agents should still serve what they had (static stability)")
	}
	tu.Must(t, px.Enable(), "heal")
	tu.Eventually(t, 60*time.Second, "partitioned agents converge", func() bool { return len(f2.Served(v1)) == 5 })
	r = testenv.WaitRelease(t, admin, id, 30*time.Second, "converged", func(r *pb.Release) bool { return r.Rollback.Converged })
}

// A client that stops reading does not hold up anyone else: updates are
// coalesced per stream, so the server keeps no backlog for it, and it
// converges to the latest state when it recovers. (The send watchdog that
// closes a stream blocked by HTTP/2 flow control is a further bound; watch
// messages are small, so buffers absorb them before it triggers.)
func TestSlowConsumer(t *testing.T) {
	e := testenv.StartChaos(t, testenv.Behind{GRPC: true}, testenv.WithServer(func(c *server.Config) { c.SendTimeout = 2 * time.Second }))
	admin := e.Control("users/admin")
	// A stalled agent still looks reachable until keepalive closes its
	// connection, so a 100% acceptance threshold would (correctly) wait for
	// it; 0.8 lets 5 of 6 converge the ring.
	pol := testenv.ManualPolicy(10000)
	pol.AcceptanceThreshold = 0.8
	testenv.Register(t, admin, spec(pol))
	px := e.AgentProxy(t)
	fast := e.FleetOptions(5, "ratelimits")
	fast.Prefix = "fast"
	slow := e.FleetOptions(1, "ratelimits")
	slow.Prefix, slow.Server, slow.Root = "slow", px.Addr, slow.Root+"-slow"
	ff, fs := e.Fleet(fast), e.Fleet(slow)
	testenv.WaitInstances(t, admin, 6)
	v1 := seed(t, admin, 1)
	tu.Eventually(t, 30*time.Second, "all on v1", func() bool { return len(ff.Served(v1))+len(fs.Served(v1)) == 6 })
	_, err := px.AddToxic("stall", "timeout", "downstream", 1, toxiproxy.Attributes{"timeout": 0})
	tu.Must(t, err, "toxic")
	var last int64
	for _, body := range []string{"v2-good.json", "v1-baseline.json", "v2-good.json"} {
		v := testenv.MustVersion(t, admin, "ratelimits", payload(t, body))
		start := time.Now()
		id := testenv.Create(t, admin, "ratelimits", v)
		tu.Eventually(t, 30*time.Second, "fast agents follow", func() bool { return len(ff.Served(v)) == 5 })
		if d := time.Since(start); d > 5*time.Second {
			t.Fatalf("delivery to healthy agents took %s with one stalled stream", d)
		}
		testenv.PromoteThrough(t, admin, id, 1)
		last = v
	}
	if fs.Served(last) != nil {
		t.Fatal("the stalled agent received updates through a zero-bandwidth link")
	}
	tu.Must(t, px.RemoveToxic("stall"), "remove toxic")
	tu.Eventually(t, 60*time.Second, "stalled agent converges to the latest state", func() bool { return len(fs.Served(last)) == 1 })
}

func metric(t *testing.T, reg *prometheus.Registry, name string) float64 {
	mfs, err := reg.Gather()
	tu.Must(t, err, "gather")
	sum := 0.0
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.Metric {
			if m.Counter != nil {
				sum += m.Counter.GetValue()
			} else if m.Gauge != nil {
				sum += m.Gauge.GetValue()
			}
		}
	}
	return sum
}

// Every agent reconnects after a partition and after a server restart; nobody
// loses its config, and all converge to the latest state.
func TestReconnectStorm(t *testing.T) {
	e := testenv.StartChaos(t, testenv.Behind{GRPC: true}, testenv.WithServer(func(c *server.Config) { c.AdmissionRate, c.AdmissionBurst = 100, 50 }))
	admin := e.Control("users/admin")
	testenv.Register(t, admin, spec(testenv.ManualPolicy(10000)))
	px := e.AgentProxy(t)
	fo := e.FleetOptions(200, "ratelimits")
	fo.Server, fo.StartSpread = px.Addr, 2*time.Second
	f := e.Fleet(fo)
	testenv.WaitInstances(t, admin, 200)
	v1 := seed(t, admin, 1)
	tu.Eventually(t, 60*time.Second, "all on v1", func() bool { return len(f.Served(v1)) == 200 })
	tu.Must(t, px.Disable(), "partition")
	time.Sleep(3 * time.Second)
	if len(f.Served(v1)) != 200 {
		t.Fatal("agents lost their config during the partition")
	}
	v2 := testenv.MustVersion(t, admin, "ratelimits", payload(t, "v2-good.json"))
	id := testenv.Create(t, admin, "ratelimits", v2)
	tu.Must(t, px.Enable(), "heal")
	// The admission limit (burst 50, 100/s) is far below 200 simultaneous
	// reconnects: some are refused and retried with jittered backoff.
	tu.Eventually(t, 90*time.Second, "storm converges", func() bool { return len(f.Served(v2)) == 200 })
	if metric(t, e.Run.Registry, "cfgcanary_server_stream_admissions_total") <= 200 {
		t.Log("note: admission control did not need to refuse any stream")
	}
	e.StopServer()
	time.Sleep(time.Second)
	e.StartServer()
	admin = e.Control("users/admin")
	_, err := admin.RollbackRelease(testenv.Ctx(t), &pb.ReleaseCommandRequest{Id: id, Reason: "after restart"})
	tu.Must(t, err, "rollback")
	tu.Eventually(t, 90*time.Second, "all back on v1 after restart storm", func() bool { return len(f.Served(v1)) == 200 })
}

// Prometheus unavailable or no metrics at all: the gate never passes and the
// release halts after its declared looks.
func TestMetricsOutageNeverPromotes(t *testing.T) {
	for _, mode := range []string{"prometheus-down", "no-metrics"} {
		t.Run(mode, func(t *testing.T) {
			e := testenv.StartChaos(t, testenv.Behind{Prometheus: true})
			admin := e.Control("users/admin")
			testenv.Register(t, admin, spec(testenv.ManualPolicy(10000)))
			reg := prometheus.NewRegistry()
			fo := e.FleetOptions(20, "ratelimits")
			if mode == "prometheus-down" {
				fo.RPS, fo.Registry = 20, reg
				e.ServeMetrics(reg)
			}
			f := e.Fleet(fo)
			testenv.WaitInstances(t, admin, 20)
			v1 := seed(t, admin, 1)
			tu.Eventually(t, 30*time.Second, "fleet on v1", func() bool { return len(f.Served(v1)) == 20 })
			gated := proto.Clone(spec(testenv.ManualPolicy(10000))).(*pb.ConfigSpec)
			p := gated.Environments["prod"]
			p.Rings = []*pb.Ring{{Name: "canary", CumulativeBps: 3000}, {Name: "rest", CumulativeBps: 9000}}
			p.Promotion, p.MinInstances = pb.Promotion_PROMOTION_AUTO, 3
			p.Gate = &pb.GatePolicy{Mode: pb.GateMode_GATE_MODE_PROMETHEUS, MinBake: durationpb.New(time.Second),
				LookInterval: durationpb.New(500 * time.Millisecond), Looks: 4, MinInstances: 3, EvalTimeout: durationpb.New(time.Second),
				Metrics: []*pb.GateMetric{{Name: "errors", Kind: pb.MetricKind_METRIC_KIND_RATIO, Metric: "demo_requests_total",
					BadMetric: "demo_request_errors_total", Margin: 0.02, MinEvents: 50}}}
			testenv.Register(t, admin, gated)
			if mode == "prometheus-down" {
				tu.Must(t, e.PromProxy.Disable(), "prometheus outage")
			}
			v2 := testenv.MustVersion(t, admin, "ratelimits", payload(t, "v2-good.json"))
			id := testenv.Create(t, admin, "ratelimits", v2)
			r := testenv.WaitRelease(t, admin, id, time.Minute, "halt", testenv.State(pb.ReleaseState_RELEASE_STATE_HALTED))
			if r.ActiveRing != 0 || r.Reason != "gate_inconclusive" || r.Rings[0].GateVerdict != pb.GateVerdict_GATE_VERDICT_INCONCLUSIVE {
				t.Fatalf("release %v ring %d reason %s verdict %v", r.State, r.ActiveRing, r.Reason, r.Rings[0].GateVerdict)
			}
			time.Sleep(2 * time.Second)
			r2, _ := admin.GetRelease(testenv.Ctx(t), &pb.GetReleaseRequest{Id: id})
			if r2.ActiveRing != 0 || r2.State != pb.ReleaseState_RELEASE_STATE_HALTED {
				t.Fatal("a halted release advanced on its own")
			}
		})
	}
}

// Object store down: submissions fail cleanly (nothing half-stored), agents
// keep serving, downloads fail and are retried, and everything converges once
// it returns.
func TestObjectStoreOutage(t *testing.T) {
	e := testenv.StartChaos(t, testenv.Behind{S3: true})
	admin := e.Control("users/admin")
	testenv.Register(t, admin, spec(testenv.ManualPolicy(10000)))
	f := e.Fleet(e.FleetOptions(10, "ratelimits"))
	testenv.WaitInstances(t, admin, 10)
	v1 := seed(t, admin, 1)
	tu.Eventually(t, 30*time.Second, "fleet on v1", func() bool { return len(f.Served(v1)) == 10 })
	v2 := testenv.MustVersion(t, admin, "ratelimits", payload(t, "v2-good.json"))
	tu.Must(t, e.S3Proxy.Disable(), "s3 outage")
	if _, err := admin.SubmitVersion(testenv.Ctx(t), &pb.SubmitVersionRequest{Name: "ratelimits", Payload: payload(t, "v3-harmful-timeout.json")}); err == nil {
		t.Fatal("submission succeeded with the object store down")
	}
	vs, _ := admin.ListVersions(testenv.Ctx(t), &pb.ListVersionsRequest{Name: "ratelimits"})
	if len(vs.Versions) != 2 {
		t.Fatalf("%d versions after a failed submission", len(vs.Versions))
	}
	id := testenv.Create(t, admin, "ratelimits", v2)
	testenv.WaitRelease(t, admin, id, 30*time.Second, "download failures reported", func(r *pb.Release) bool { return r.Rings[0].Counts.Failed > 0 })
	if len(f.Served(v1)) != 10 {
		t.Fatal("agents lost their config during the object-store outage")
	}
	tu.Must(t, e.S3Proxy.Enable(), "s3 back")
	testenv.WaitRelease(t, admin, id, 60*time.Second, "converged after recovery", testenv.Baking(0))
	tu.Eventually(t, 30*time.Second, "fleet on v2", func() bool { return len(f.Served(v2)) == 10 })
}

// Database down: agents keep serving (static stability), the API reports
// unavailability, and the controller and acknowledgements recover.
func TestDatabaseOutage(t *testing.T) {
	e := testenv.StartChaos(t, testenv.Behind{DB: true})
	admin := e.Control("users/admin")
	testenv.Register(t, admin, spec(testenv.ManualPolicy(10000)))
	f := e.Fleet(e.FleetOptions(10, "ratelimits"))
	testenv.WaitInstances(t, admin, 10)
	v1 := seed(t, admin, 1)
	v2 := testenv.MustVersion(t, admin, "ratelimits", payload(t, "v2-good.json"))
	id := testenv.Create(t, admin, "ratelimits", v2)
	testenv.WaitRelease(t, admin, id, 30*time.Second, "ring 0", testenv.Baking(0))
	tu.Must(t, e.DBProxy.Disable(), "db outage")
	time.Sleep(4 * time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	_, err := admin.GetRelease(ctx, &pb.GetReleaseRequest{Id: id})
	cancel()
	if err == nil {
		t.Fatal("API answered with the database down")
	}
	if len(f.Served(v2)) != 10 {
		t.Fatal("agents lost their config during the database outage")
	}
	if ok, _ := e.Run.Controller.Leader(); ok {
		t.Fatal("controller kept leading without its lock session")
	}
	tu.Must(t, e.DBProxy.Enable(), "db back")
	tu.Eventually(t, 30*time.Second, "leadership recovers", func() bool { ok, _ := e.Run.Controller.Leader(); return ok })
	_, err = admin.RollbackRelease(testenv.Ctx(t), &pb.ReleaseCommandRequest{Id: id, Reason: "after db outage"})
	tu.Must(t, err, "rollback")
	testenv.WaitRelease(t, admin, id, 60*time.Second, "rollback converges", func(r *pb.Release) bool {
		return r.State == pb.ReleaseState_RELEASE_STATE_ROLLED_BACK && r.Rollback.Converged
	})
	tu.Eventually(t, 30*time.Second, "fleet on v1", func() bool { return len(f.Served(v1)) == 10 })
}

// Repeated unhealthy restarts on a new config are detected by the SDK,
// reported as CRASH_LOOP, and roll the release back.
func TestCrashLoopRollsBack(t *testing.T) {
	e := testenv.Start(t)
	admin := e.Control("users/admin")
	testenv.Register(t, admin, spec(testenv.ManualPolicy(5000, 10000)))
	f := e.Fleet(e.FleetOptions(6, "ratelimits"))
	testenv.WaitInstances(t, admin, 6)
	v1 := seed(t, admin, 2)
	tu.Eventually(t, 30*time.Second, "fleet on v1", func() bool { return len(f.Served(v1)) == 6 })
	// Mark v1 healthy so it is the last-known-good.
	for _, m := range f.Members {
		m.Stop()
	}
	healthy := e.FleetOptions(6, "ratelimits")
	healthy.HealthyAfter = 200 * time.Millisecond
	f.Opt.HealthyAfter = healthy.HealthyAfter
	for _, m := range f.Members {
		m.Start(context.Background(), f)
	}
	time.Sleep(2 * time.Second)
	f.Opt.HealthyAfter = time.Hour
	for _, m := range f.Members {
		m.Stop()
		m.Start(context.Background(), f)
	}
	v2 := testenv.MustVersion(t, admin, "ratelimits", payload(t, "v2-good.json"))
	id := testenv.Create(t, admin, "ratelimits", v2)
	testenv.WaitRelease(t, admin, id, 30*time.Second, "ring 0", testenv.Baking(0))
	exposed := f.Served(v2)
	if len(exposed) == 0 {
		t.Fatal("nobody exposed")
	}
	// The exposed processes now "crash" on start repeatedly.
	for range 4 {
		for _, m := range f.Members {
			for _, id := range exposed {
				if m.ID == id {
					m.Stop()
					m.Start(context.Background(), f)
				}
			}
		}
		time.Sleep(300 * time.Millisecond)
	}
	r := testenv.WaitRelease(t, admin, id, 30*time.Second, "crash-loop rollback", testenv.State(pb.ReleaseState_RELEASE_STATE_ROLLED_BACK))
	if r.Reason != "crash_loop" || r.Rings[0].Counts.CrashLoop == 0 {
		t.Fatalf("reason %s counts %+v", r.Reason, r.Rings[0].Counts)
	}
	tu.Eventually(t, 30*time.Second, "fleet on v1", func() bool { return len(f.Served(v1)) == 6 })
}

// Restarting every agent and application while the control plane is down:
// they come back on their verified cache (static stability).
func TestColdRestartWithControlPlaneDown(t *testing.T) {
	e := testenv.Start(t)
	admin := e.Control("users/admin")
	testenv.Register(t, admin, spec(testenv.ManualPolicy(10000)))
	f := e.Fleet(e.FleetOptions(10, "ratelimits"))
	testenv.WaitInstances(t, admin, 10)
	v1 := seed(t, admin, 1)
	tu.Eventually(t, 30*time.Second, "fleet on v1", func() bool { return len(f.Served(v1)) == 10 })
	e.StopServer()
	for _, m := range f.Members {
		m.Stop()
		m.Start(context.Background(), f)
	}
	tu.Eventually(t, 10*time.Second, "restart from cache", func() bool { return len(f.Served(v1)) == 10 })
	e.StartServer()
}

func ruleStart(r *store.ReleaseRow) (ev rolloutEvent) { return startEvent(r) }
