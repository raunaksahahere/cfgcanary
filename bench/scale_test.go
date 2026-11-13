package bench

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	pb "github.com/raunaksahahere/cfgcanary/api/gen/cfgcanary/v1"
	"github.com/raunaksahahere/cfgcanary/internal/backoff"
	"github.com/raunaksahahere/cfgcanary/internal/policy"
	"github.com/raunaksahahere/cfgcanary/internal/ring"
	"github.com/raunaksahahere/cfgcanary/internal/sim"
	"github.com/raunaksahahere/cfgcanary/internal/testenv"
	"github.com/raunaksahahere/cfgcanary/internal/testutil"
)

// RingResult is propagation for one ring.
type RingResult struct {
	Ring    int32 `json:"ring"`
	Members int   `json:"members"`
	// Received: ring exposure committed -> the update reached the agent
	// (control-plane fan-out, before any local disk work).
	Received dist `json:"received"`
	// Applied: ring exposure committed in PostgreSQL -> agent durably
	// applied the version (pointer swapped). Accepted: -> the SDK published
	// it to the application.
	Applied  dist `json:"applied"`
	Accepted dist `json:"accepted"`
	Missing  int  `json:"missing"`
}

// ScaleResult is one fleet size.
type ScaleResult struct {
	Agents             int          `json:"agents"`
	ConnectSeconds     float64      `json:"connect_seconds"`
	SeedSeconds        float64      `json:"seed_seconds"`
	Rings              []RingResult `json:"rings"`
	ReleaseSeconds     float64      `json:"release_seconds"`
	RollbackExposed    int          `json:"rollback_exposed"`
	RollbackPerAgent   dist         `json:"rollback_per_agent"`
	RollbackServerSecs float64      `json:"rollback_server_converged_seconds"`
	RollbackMissing    int          `json:"rollback_missing"`
	Server             struct {
		IdleCPUCores         float64 `json:"idle_cpu_cores"`
		IdleRSSMB            float64 `json:"idle_rss_mb"`
		ReleaseCPUCores      float64 `json:"release_cpu_cores"`
		PeakRSSMB            float64 `json:"peak_rss_mb"`
		Goroutines           float64 `json:"goroutines"`
		CPUCoresPer1kIdle    float64 `json:"idle_cpu_cores_per_1k_streams"`
		RSSMBPer1kStreams    float64 `json:"rss_mb_per_1k_streams"`
		ReleaseCPUCoresPer1k float64 `json:"release_cpu_cores_per_1k_streams"`
	} `json:"server"`
	FleetProcessRSSMB float64 `json:"fleet_process_rss_mb"`
	Notes             string  `json:"notes,omitempty"`
}

func scalePolicy() *pb.RolloutPolicy {
	return &pb.RolloutPolicy{
		Rings:        []*pb.Ring{{Name: "canary", CumulativeBps: 1000}, {Name: "half", CumulativeBps: 5000}, {Name: "all", CumulativeBps: 10000}},
		MinInstances: 1, AcceptanceThreshold: 1, MinReachableFraction: 0.5, RolloutTimeout: durationpb.New(10 * time.Minute),
		RollbackTimeout: durationpb.New(10 * time.Minute), ReachableWindow: durationpb.New(2 * time.Minute), Promotion: pb.Promotion_PROMOTION_MANUAL,
		Gate: &pb.GatePolicy{Mode: pb.GateMode_GATE_MODE_NONE, MinBake: durationpb.New(time.Millisecond)},
	}
}

func rssMB() float64 {
	b, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return 0
	}
	for _, l := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(l, "VmRSS:") {
			kb, _ := strconv.Atoi(strings.Fields(l)[1])
			return float64(kb) / 1024
		}
	}
	return 0
}

func sizes() []int {
	s := os.Getenv("CFGCANARY_BENCH_SIZES")
	if s == "" {
		s = "50,100,1000,5000"
	}
	var out []int
	for _, f := range strings.Split(s, ",") {
		if n, err := strconv.Atoi(strings.TrimSpace(f)); err == nil {
			out = append(out, n)
		}
	}
	return out
}

// TestScale measures per-ring propagation, rollback convergence and control
// plane resources for each fleet size.
func TestScale(t *testing.T) {
	requireBench(t)
	// Thousands of simulated agents in one process block in fsync at the
	// same moment, and each blocked syscall holds an OS thread. Raise Go's
	// thread cap (default 10000) for the simulator; the OS/cgroup task limit
	// still applies and is recorded in the results if it is hit.
	debug.SetMaxThreads(30000)
	var results []ScaleResult
	for _, n := range sizes() {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			r := runScale(t, n)
			results = append(results, r)
			t.Logf("N=%d: %+v", n, r)
		})
	}
	writeJSON(t, "scale.json", map[string]any{"environment": environment("CFGCANARY_BENCH=1 go test ./bench -run TestScale -timeout 90m"),
		"definitions": map[string]string{
			"received": "ring exposure transition committed in PostgreSQL until the update reached the agent (before verification and the durable write): the control plane's fan-out latency",
			"disk":     "all simulated agents share one disk; every generation change makes each agent fsync its pointer and the SDK its state and status files, so applied/accepted at large N include fsync contention that separate hosts would not have",
			"applied":  "ring exposure transition committed in PostgreSQL (release ring 'started' timestamp, database clock) until the agent durably swapped its cache pointer to the new version; all clocks on one host",
			"accepted": "same start until the SDK published the version to the application (first time the member served it)",
			"rollback": "operator rollback committed until each exposed agent's SDK served the fallback version again; server-converged = until GetRelease reported every exposed instance confirmed",
			"server":   "separate cfgcanary server process; CPU from process_cpu_seconds_total rate, RSS from process_resident_memory_bytes; idle = 20 s with all streams open and no changes",
			"workload": "manual promotion, gate none, rings 10%/50%/100%, payload 5.7 KB, agents dial with full-jitter backoff (base 250 ms, cap 10 s), reconcile every 60 s, SDK poll 1 s plus in-process notification",
		}, "results": results})
	var rows []string
	for _, r := range results {
		for _, rr := range r.Rings {
			rows = append(rows, fmt.Sprintf("%d,%d,%d,%.1f,%.1f,%.1f,%.1f,%.1f,%.1f,%.1f,%.1f", r.Agents, rr.Ring, rr.Members,
				rr.Received.P50, rr.Received.P99, rr.Applied.P50, rr.Applied.P99, rr.Applied.Max, rr.Accepted.P50, rr.Accepted.P99, rr.Accepted.Max))
		}
	}
	writeCSV(t, "scale_propagation.csv", "agents,ring,members,received_p50_ms,received_p99_ms,applied_p50_ms,applied_p99_ms,applied_max_ms,accepted_p50_ms,accepted_p99_ms,accepted_max_ms", rows)
}

func runScale(t *testing.T, n int) ScaleResult {
	res := ScaleResult{Agents: n}
	h := startHarness(t, "--admission-rate", "1000", "--admission-burst", "2000", "--max-streams", "20000")
	ctx := context.Background()
	_, err := h.admin.RegisterConfig(ctx, &pb.RegisterConfigRequest{Spec: testenv.RatelimitsSpec("ratelimits", scalePolicy(), nil)})
	testutil.Must(t, err, "register")
	rec := newRecorder()
	fo := sim.Options{N: n, Environment: "prod", Config: "ratelimits", Server: h.grpcAddr, ServerName: "localhost",
		CAFile: filepath.Join(h.pki, "ca.pem"), CA: h.ca, TrustBundle: h.trust, Root: filepath.Join(h.dir, "fleet"),
		AllowedOrigins: []string{"http://" + func() string { ep, _, _ := testutil.S3(); return ep }()},
		Backoff:        backoff.Policy{Base: 250 * time.Millisecond, Cap: 10 * time.Second}, Seed: 7, Reconcile: time.Minute,
		HealthyAfter: time.Hour, StartSpread: time.Duration(n/250+1) * time.Second, Observer: rec}
	f, err := sim.NewFleet(fo)
	testutil.Must(t, err, "fleet")
	fctx, cancel := context.WithCancel(ctx)
	defer func() { cancel(); f.Stop() }()
	start := time.Now()
	f.Start(fctx)
	testutil.Eventually(t, 10*time.Minute, "all streams open", func() bool { return h.metrics()["cfgcanary_server_streams"] >= float64(n) })
	res.ConnectSeconds = time.Since(start).Seconds()

	// Seed v1 (first deployment, includes initial downloads).
	start = time.Now()
	v1 := version(t, h, "v1-baseline.json")
	promoteAll(t, h, create(t, h, v1), n)
	testutil.Eventually(t, 10*time.Minute, "fleet on v1", func() bool { return len(f.Served(v1)) == n })
	res.SeedSeconds = time.Since(start).Seconds()

	// Idle resource sample with every stream open.
	m0, t0 := h.metrics(), time.Now()
	time.Sleep(20 * time.Second)
	m1 := h.metrics()
	res.Server.IdleCPUCores = (m1["process_cpu_seconds_total"] - m0["process_cpu_seconds_total"]) / time.Since(t0).Seconds()
	res.Server.IdleRSSMB = m1["process_resident_memory_bytes"] / 1e6
	res.Server.Goroutines = m1["go_goroutines"]

	// Measured release of v2, promoting each ring as soon as it converges.
	v2 := version(t, h, "v2-good.json")
	m0, t0 = h.metrics(), time.Now()
	id := create(t, h, v2)
	rel := promoteAll(t, h, id, n)
	res.ReleaseSeconds = time.Since(t0).Seconds()
	m1 = h.metrics()
	res.Server.ReleaseCPUCores = (m1["process_cpu_seconds_total"] - m0["process_cpu_seconds_total"]) / time.Since(t0).Seconds()
	testutil.Eventually(t, 5*time.Minute, "fleet on v2", func() bool { return len(f.Served(v2)) == n })

	rp := policy.Ring(rel.Policy)
	byRing := map[int32][]*sim.Member{}
	for _, m := range f.Members {
		r := rp.Assign(m.ID)
		if r == ring.Holdout {
			continue
		}
		byRing[r] = append(byRing[r], m)
	}
	for i, rs := range rel.Rings {
		started := rs.Started.AsTime()
		rr := RingResult{Ring: int32(i), Members: len(byRing[int32(i)])}
		var received, applied, accepted []time.Duration
		for _, m := range byRing[int32(i)] {
			if at, ok := rec.receivedAt(m.ID, v2); ok {
				received = append(received, at.Sub(started))
			}
			if at, ok := rec.appliedAt(m.ID, v2); ok {
				applied = append(applied, at.Sub(started))
			} else {
				rr.Missing++
			}
			if at, ok := m.FirstServedAt(v2); ok {
				accepted = append(accepted, at.Sub(started))
			}
		}
		rr.Received, rr.Applied, rr.Accepted = summarize(received), summarize(applied), summarize(accepted)
		res.Rings = append(res.Rings, rr)
	}

	// Rollback after two rings were exposed.
	v3 := version(t, h, "v3-harmful-timeout.json")
	id = create(t, h, v3)
	for r := int32(0); r < 2; r++ {
		waitRing(t, h, id, r, n)
		if r == 0 {
			_, err := h.admin.PromoteRelease(ctx, &pb.PromoteReleaseRequest{Id: id, ExpectedRing: 0, HasExpectedRing: true})
			testutil.Must(t, err, "promote")
		}
	}
	exposed := map[string]bool{}
	for _, m := range f.Members {
		if r := rp.Assign(m.ID); r != ring.Holdout && r <= 1 {
			exposed[m.ID] = true
		}
	}
	testutil.Eventually(t, 5*time.Minute, "exposed rings on v3", func() bool { return len(f.Served(v3)) == len(exposed) })
	rb, err := h.admin.RollbackRelease(ctx, &pb.ReleaseCommandRequest{Id: id, Reason: "bench"})
	testutil.Must(t, err, "rollback")
	rbStart := rb.Rollback.Started.AsTime()
	r := waitRelease(t, h, id, 10*time.Minute, func(r *pb.Release) bool {
		return r.State == pb.ReleaseState_RELEASE_STATE_ROLLED_BACK && r.Rollback != nil && r.Rollback.Converged && r.Rollback.ConvergedAt != nil
	})
	res.RollbackServerSecs = r.Rollback.ConvergedAt.AsTime().Sub(rbStart).Seconds()
	testutil.Eventually(t, 5*time.Minute, "fleet back on v2", func() bool { return len(f.Served(v2)) == n })
	var rbd []time.Duration
	for _, m := range f.Members {
		if !exposed[m.ID] {
			continue
		}
		res.RollbackExposed++
		found := false
		for _, c := range m.History() {
			if c.Version == v2 && c.At.After(rbStart) {
				rbd = append(rbd, c.At.Sub(rbStart))
				found = true
				break
			}
		}
		if !found {
			res.RollbackMissing++
		}
	}
	res.RollbackPerAgent = summarize(rbd)
	peak := h.metrics()["process_resident_memory_bytes"] / 1e6
	res.Server.PeakRSSMB = max(peak, res.Server.IdleRSSMB)
	k := float64(n) / 1000
	res.Server.CPUCoresPer1kIdle = res.Server.IdleCPUCores / k
	res.Server.RSSMBPer1kStreams = res.Server.IdleRSSMB / k
	res.Server.ReleaseCPUCoresPer1k = res.Server.ReleaseCPUCores / k
	runtime.GC()
	res.FleetProcessRSSMB = rssMB()
	return res
}

func version(t *testing.T, h *harness, file string) int64 {
	b, err := os.ReadFile(filepath.Join(testutil.RepoRoot(), "examples", "ratelimits", "payloads", file))
	testutil.Must(t, err, file)
	r, err := h.admin.SubmitVersion(context.Background(), &pb.SubmitVersionRequest{Name: "ratelimits", Payload: b})
	testutil.Must(t, err, "submit")
	if !r.Report.Ok {
		t.Fatalf("rejected: %v", r.Report.Issues)
	}
	return r.Version.Version
}

func create(t *testing.T, h *harness, v int64) string {
	r, err := h.admin.CreateRelease(context.Background(), &pb.CreateReleaseRequest{Config: "ratelimits", Environment: "prod", Version: v})
	testutil.Must(t, err, "create")
	return r.Release.Id
}

func waitRelease(t *testing.T, h *harness, id string, timeout time.Duration, cond func(*pb.Release) bool) *pb.Release {
	var last *pb.Release
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		r, err := h.admin.GetRelease(ctx, &pb.GetReleaseRequest{Id: id})
		cancel()
		if err == nil {
			last = r
			if cond(r) {
				return r
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timeout; last %v ring %d %s", last.GetState(), last.GetActiveRing(), last.GetMessage())
	return nil
}

func waitRing(t *testing.T, h *harness, id string, ringIdx int32, n int) *pb.Release {
	return waitRelease(t, h, id, 10*time.Minute, func(r *pb.Release) bool {
		return r.State == pb.ReleaseState_RELEASE_STATE_BAKING && r.ActiveRing == ringIdx
	})
}

// promoteAll exposes each ring as soon as the previous one converged.
func promoteAll(t *testing.T, h *harness, id string, n int) *pb.Release {
	rings := 3
	for r := int32(0); r < int32(rings); r++ {
		waitRing(t, h, id, r, n)
		_, err := h.admin.PromoteRelease(context.Background(), &pb.PromoteReleaseRequest{Id: id, ExpectedRing: r, HasExpectedRing: true})
		testutil.Must(t, err, "promote")
	}
	return waitRelease(t, h, id, 10*time.Minute, func(r *pb.Release) bool { return r.State == pb.ReleaseState_RELEASE_STATE_COMPLETE })
}
