package bench

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	pb "github.com/raunaksahahere/cfgcanary/api/gen/cfgcanary/v1"
	"github.com/raunaksahahere/cfgcanary/internal/backoff"
	"github.com/raunaksahahere/cfgcanary/internal/sim"
	"github.com/raunaksahahere/cfgcanary/internal/testenv"
	"github.com/raunaksahahere/cfgcanary/internal/testutil"
)

// StormResult is one reconnect-storm run.
type StormResult struct {
	Mode             string  `json:"mode"`
	Agents           int     `json:"agents"`
	DownSeconds      float64 `json:"down_seconds"`
	Attempts         int     `json:"attempts"`
	PeakPer100ms     int     `json:"peak_attempts_per_100ms"`
	PeakPerSecond    int     `json:"peak_attempts_per_second"`
	PeakAfterRestart int     `json:"peak_attempts_per_100ms_after_restart"`
	ReconnectSeconds float64 `json:"seconds_from_restart_to_all_connected"`
	AdmissionRejects float64 `json:"admission_rejections"`
	Curve            []int   `json:"curve_per_100ms"`
}

// TestReconnectStorm kills the control plane under N connected agents, keeps
// it down, restarts it, and records every connection attempt, with full
// jitter and, in the same isolated harness, without jitter.
func TestReconnectStorm(t *testing.T) {
	requireBench(t)
	n := 1000
	if s := os.Getenv("CFGCANARY_BENCH_STORM_AGENTS"); s != "" {
		n, _ = strconv.Atoi(s)
	}
	var results []StormResult
	for _, mode := range []string{"full-jitter", "no-jitter"} {
		t.Run(mode, func(t *testing.T) {
			r := runStorm(t, n, mode == "no-jitter")
			r.Mode = mode
			results = append(results, r)
			t.Logf("%s: attempts=%d peak/100ms=%d peak/s=%d reconnect=%.1fs", mode, r.Attempts, r.PeakPer100ms, r.PeakPerSecond, r.ReconnectSeconds)
		})
	}
	writeJSON(t, "storm.json", map[string]any{"environment": environment("CFGCANARY_BENCH=1 go test ./bench -run TestReconnectStorm -timeout 30m"),
		"definitions": map[string]string{
			"attempt":   "one watch attempt by one agent: a new TLS connection and Watch call (counted at the client when it starts dialing)",
			"window":    "from SIGKILL of the control plane until every agent has a new stream; the server is down for down_seconds then restarted on the same address",
			"backoff":   "capped exponential, base 250 ms, cap 10 s; full-jitter draws uniformly from [0, ceiling), no-jitter waits exactly the ceiling (baseline mode used only in this harness)",
			"admission": "server admission limit raised to 100000/s so the curves show client behaviour alone",
		}, "results": results})
	var rows []string
	for _, r := range results {
		for i, c := range r.Curve {
			rows = append(rows, fmt.Sprintf("%s,%d,%d", r.Mode, i*100, c))
		}
	}
	writeCSV(t, "storm_curve.csv", "mode,ms_since_kill,attempts", rows)
}

func runStorm(t *testing.T, n int, noJitter bool) StormResult {
	res := StormResult{Agents: n, DownSeconds: 5}
	h := startHarness(t, "--admission-rate", "100000", "--admission-burst", "100000")
	ctx := context.Background()
	_, err := h.admin.RegisterConfig(ctx, &pb.RegisterConfigRequest{Spec: testenv.RatelimitsSpec("ratelimits", scalePolicy(), nil)})
	testutil.Must(t, err, "register")
	rec := newRecorder()
	ep, _, _ := testutil.S3()
	f, err := sim.NewFleet(sim.Options{N: n, Environment: "prod", Config: "ratelimits", Server: h.grpcAddr, ServerName: "localhost",
		CAFile: filepath.Join(h.pki, "ca.pem"), CA: h.ca, TrustBundle: h.trust, Root: filepath.Join(h.dir, "fleet"),
		AllowedOrigins: []string{"http://" + ep}, Backoff: backoff.Policy{Base: 250 * time.Millisecond, Cap: 10 * time.Second, NoJitter: noJitter},
		Seed: 11, Reconcile: time.Minute, HealthyAfter: time.Hour, StartSpread: time.Duration(n/250+1) * time.Second, Observer: rec})
	testutil.Must(t, err, "fleet")
	fctx, cancel := context.WithCancel(ctx)
	defer func() { cancel(); f.Stop() }()
	f.Start(fctx)
	testutil.Eventually(t, 5*time.Minute, "all streams open", func() bool { return h.metrics()["cfgcanary_server_streams"] >= float64(n) })
	time.Sleep(3 * time.Second)

	kill := time.Now()
	h.kill(true)
	time.Sleep(time.Duration(res.DownSeconds * float64(time.Second)))
	restart := time.Now()
	h.start()
	testutil.Eventually(t, 5*time.Minute, "everyone reconnected", func() bool { return rec.connectedSince(restart) >= n })
	res.ReconnectSeconds = time.Since(restart).Seconds()
	res.AdmissionRejects = h.metrics()["cfgcanary_server_stream_admissions_total"]

	attempts := rec.attemptsSince(kill)
	res.Attempts = len(attempts)
	end := time.Since(kill)
	buckets := make([]int, int(end/(100*time.Millisecond))+1)
	for _, a := range attempts {
		buckets[int(a.Sub(kill)/(100*time.Millisecond))]++
	}
	res.Curve = buckets
	restartBucket := int(restart.Sub(kill) / (100 * time.Millisecond))
	for i, c := range buckets {
		res.PeakPer100ms = max(res.PeakPer100ms, c)
		if i >= restartBucket {
			res.PeakAfterRestart = max(res.PeakAfterRestart, c)
		}
	}
	for i := 0; i+10 <= len(buckets); i++ {
		s := 0
		for _, c := range buckets[i : i+10] {
			s += c
		}
		res.PeakPerSecond = max(res.PeakPerSecond, s)
	}
	return res
}
