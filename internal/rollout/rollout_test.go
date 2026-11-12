package rollout

import (
	"errors"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/raunaksahahere/cfgcanary/internal/ring"
)

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func policy() Policy {
	return Policy{
		Rings: 3, AcceptanceThreshold: 0.9, MinReachableFraction: 0.5,
		RolloutTimeout: time.Minute, RollbackTimeout: time.Minute, Auto: true, Gated: true,
		MinBake: 10 * time.Second, LookInterval: 5 * time.Second, Looks: 3, RollbackOnFail: true,
	}
}

func fresh() (Release, Target) {
	return Release{State: Pending, ActiveRing: -1, Candidate: 8, Baseline: 7, StateVersion: 1},
		Target{Generation: 4, Baseline: 7, ActiveRing: -1, PreviousGood: 6}
}

func must(t *testing.T, r Release, tg Target, p Policy, ev Event, now time.Time) (Release, Target) {
	t.Helper()
	nr, nt, err := Transition(r, tg, p, ev, now)
	if err != nil {
		t.Fatalf("%s: %v", ev.Kind, err)
	}
	return nr, nt
}

func TestHappyPathThroughEveryRing(t *testing.T) {
	p := policy()
	r, tg := fresh()
	now := t0
	r, tg = must(t, r, tg, p, Event{Kind: EvStart}, now)
	for ringIdx := int32(0); ringIdx < 3; ringIdx++ {
		if r.State != Rolling || r.ActiveRing != ringIdx || tg.ActiveRing != ringIdx || tg.Candidate != 8 {
			t.Fatalf("ring %d: unexpected %+v %+v", ringIdx, r, tg)
		}
		if got := Decide(r, p, Counts{Eligible: 10, Reachable: 10, Accepted: 8}, RollbackCounts{}, now); got.Event != "" {
			t.Fatalf("advanced below acceptance threshold: %+v", got)
		}
		if got := Decide(r, p, Counts{Eligible: 10, Reachable: 10, Accepted: 9}, RollbackCounts{}, now); got.Event != EvBeginBake {
			t.Fatalf("did not begin bake: %+v", got)
		}
		r, tg = must(t, r, tg, p, Event{Kind: EvBeginBake}, now)
		if got := Decide(r, p, Counts{Eligible: 10, Reachable: 10, Accepted: 9}, RollbackCounts{}, now.Add(9*time.Second)); got.EvaluateGate {
			t.Fatal("looked before minimum bake")
		}
		now = now.Add(10 * time.Second)
		if got := Decide(r, p, Counts{Eligible: 10, Reachable: 10, Accepted: 9}, RollbackCounts{}, now); !got.EvaluateGate {
			t.Fatalf("no gate look after bake: %+v", got)
		}
		r, tg = must(t, r, tg, p, Event{Kind: EvGateLook, Verdict: VerdictPass}, now)
		if got := Decide(r, p, Counts{Eligible: 10, Reachable: 10, Accepted: 9}, RollbackCounts{}, now); got.Event != EvAdvance {
			t.Fatalf("no advance after pass: %+v", got)
		}
		r, tg = must(t, r, tg, p, Event{Kind: EvAdvance}, now)
	}
	if r.State != Complete || tg.Baseline != 8 || tg.PreviousGood != 7 || tg.Candidate != 0 {
		t.Fatalf("not complete: %+v %+v", r, tg)
	}
	if tg.Generation != 4+4 { // three rings and completion
		t.Fatalf("generation %d", tg.Generation)
	}
}

func TestRollbackIsNewGenerationOlderVersion(t *testing.T) {
	p := policy()
	r, tg := fresh()
	r, tg = must(t, r, tg, p, Event{Kind: EvStart}, t0)
	g := tg.Generation
	r, tg = must(t, r, tg, p, Event{Kind: EvRollback, Reason: "operator"}, t0)
	if tg.Generation != g+1 || tg.DesiredFor(0) != 7 || r.State != RollingBack || r.RollbackGeneration != tg.Generation {
		t.Fatalf("rollback did not issue a newer generation for the older version: %+v %+v", r, tg)
	}
	if _, _, err := Transition(r, tg, p, Event{Kind: EvRollback}, t0); !errors.Is(err, ErrNoop) {
		t.Fatalf("second rollback: %v", err)
	}
}

func TestGateVerdicts(t *testing.T) {
	p := policy()
	bake := func() (Release, Target) {
		r, tg := fresh()
		r, tg = must(t, r, tg, p, Event{Kind: EvStart}, t0)
		return must(t, r, tg, p, Event{Kind: EvBeginBake}, t0)
	}
	r, tg := bake()
	r, _ = must(t, r, tg, p, Event{Kind: EvGateLook, Verdict: VerdictFail}, t0)
	if r.State != RollingBack || r.Reason != "gate_fail" {
		t.Fatalf("fail did not roll back: %+v", r)
	}
	p2 := p
	p2.RollbackOnFail = false
	r, tg = bake()
	r, _ = must(t, r, tg, p2, Event{Kind: EvGateLook, Verdict: VerdictFail}, t0)
	if r.State != Halted {
		t.Fatalf("on_fail=halt did not halt: %+v", r)
	}
}

// Missing evidence must never promote, no matter how long we wait.
func TestInconclusiveHaltsAtLastLook(t *testing.T) {
	p := policy()
	r, tg := fresh()
	r, tg = must(t, r, tg, p, Event{Kind: EvStart}, t0)
	r, tg = must(t, r, tg, p, Event{Kind: EvBeginBake}, t0)
	now := t0.Add(time.Hour)
	for i := range 3 {
		if _, _, err := Transition(r, tg, p, Event{Kind: EvAdvance}, now); err == nil {
			t.Fatal("advanced without a gate pass")
		}
		r, tg = must(t, r, tg, p, Event{Kind: EvGateLook, Verdict: VerdictInconclusive}, now)
		if i < 2 && r.State != Baking {
			t.Fatalf("halted early at look %d", i+1)
		}
		now = now.Add(time.Hour)
	}
	if r.State != Halted || r.Reason != "gate_inconclusive" {
		t.Fatalf("not halted: %+v", r)
	}
	if got := Decide(r, p, Counts{Eligible: 5, Reachable: 5, Accepted: 5}, RollbackCounts{}, now.Add(24*time.Hour)); got.Event != "" || got.EvaluateGate {
		t.Fatalf("halted release acted on its own: %+v", got)
	}
	if _, _, err := Transition(r, tg, p, Event{Kind: EvPromote}, now); err == nil {
		t.Fatal("manual promote bypassed the gate")
	}
	// Break-glass override is explicit.
	r2, _, err := Transition(r, tg, p, Event{Kind: EvPromote, Override: true}, now)
	if err != nil || !r2.OverrideUsed || r2.ActiveRing != 1 {
		t.Fatalf("override: %v %+v", err, r2)
	}
}

func TestPausedDoesNotAdvance(t *testing.T) {
	p := policy()
	r, tg := fresh()
	r, tg = must(t, r, tg, p, Event{Kind: EvStart}, t0)
	r, tg = must(t, r, tg, p, Event{Kind: EvBeginBake}, t0)
	r, tg = must(t, r, tg, p, Event{Kind: EvGateLook, Verdict: VerdictPass}, t0.Add(10*time.Second))
	r, tg = must(t, r, tg, p, Event{Kind: EvPause}, t0.Add(11*time.Second))
	later := t0.Add(48 * time.Hour)
	if got := Decide(r, p, Counts{Eligible: 5, Reachable: 5, Accepted: 5}, RollbackCounts{}, later); got.Event != "" || got.EvaluateGate {
		t.Fatalf("paused release advanced because time passed: %+v", got)
	}
	for _, k := range []EventKind{EvAdvance, EvPromote, EvGateLook, EvBeginBake, EvStart} {
		if _, _, err := Transition(r, tg, p, Event{Kind: k, Verdict: VerdictPass}, later); !errors.Is(err, ErrPaused) {
			t.Fatalf("%s while paused: %v", k, err)
		}
	}
	// Hard evidence still rolls back while paused.
	if got := Decide(r, p, Counts{Eligible: 5, Reachable: 5, Rejected: 1}, RollbackCounts{}, later); got.Event != EvAutoRollback {
		t.Fatalf("paused release ignored SDK rejection: %+v", got)
	}
	// Resume discards the old pass: evidence is gathered again.
	r, _ = must(t, r, tg, p, Event{Kind: EvResume}, later)
	if r.GatePassed || r.LooksDone != 0 || !r.BakeStarted.Equal(later) {
		t.Fatalf("resume reused stale evidence: %+v", r)
	}
}

func TestDecideRollingEdges(t *testing.T) {
	p := policy()
	r := Release{State: Rolling, ActiveRing: 0, RingStarted: t0}
	cases := []struct {
		name string
		c    Counts
		at   time.Duration
		want EventKind
		why  string
	}{
		{"empty cohort", Counts{}, 0, EvHalt, "empty_cohort"},
		{"waiting for reachable", Counts{Eligible: 10, Reachable: 2, Accepted: 2}, time.Second, "", ""},
		{"too few reachable at timeout", Counts{Eligible: 10, Reachable: 2, Accepted: 2}, time.Minute, EvHalt, "too_few_reachable"},
		{"convergence timeout", Counts{Eligible: 10, Reachable: 10, Accepted: 3}, time.Minute, EvHalt, "convergence_timeout"},
		{"applied is not accepted", Counts{Eligible: 10, Reachable: 10, Applied: 10, Accepted: 0}, time.Second, "", ""},
		{"sdk rejection", Counts{Eligible: 10, Reachable: 10, Accepted: 9, Rejected: 1}, time.Second, EvAutoRollback, "sdk_rejected"},
		{"crash loop", Counts{Eligible: 10, Reachable: 10, Accepted: 9, CrashLoop: 1}, time.Second, EvAutoRollback, "crash_loop"},
	}
	for _, c := range cases {
		got := Decide(r, p, c.c, RollbackCounts{}, t0.Add(c.at))
		if got.Event != c.want || got.Reason != c.why {
			t.Errorf("%s: got %+v", c.name, got)
		}
	}
}

func TestRollbackConvergence(t *testing.T) {
	p := policy()
	r := Release{State: RollingBack, RollbackStarted: t0}
	if got := Decide(r, p, Counts{}, RollbackCounts{Exposed: 5, Confirmed: 3, Pending: 2}, t0.Add(time.Second)); got.Event != "" {
		t.Fatalf("finished with pending agents: %+v", got)
	}
	if got := Decide(r, p, Counts{}, RollbackCounts{Exposed: 5, Confirmed: 4, Unreachable: 1}, t0.Add(time.Second)); got.Event != EvFinishRollback || got.Converged {
		t.Fatalf("unreachable agents reported as converged: %+v", got)
	}
	if got := Decide(r, p, Counts{}, RollbackCounts{Exposed: 5, Confirmed: 5}, t0.Add(time.Second)); got.Event != EvFinishRollback || !got.Converged {
		t.Fatalf("%+v", got)
	}
	if got := Decide(r, p, Counts{}, RollbackCounts{Exposed: 5, Confirmed: 3, Pending: 2}, t0.Add(time.Minute)); got.Event != EvFinishRollback || got.Converged {
		t.Fatalf("timeout: %+v", got)
	}
}

func TestStaleDecisionRejected(t *testing.T) {
	p := policy()
	r, tg := fresh()
	r, tg = must(t, r, tg, p, Event{Kind: EvStart}, t0)
	r, tg = must(t, r, tg, p, Event{Kind: EvBeginBake}, t0)
	r, tg = must(t, r, tg, p, Event{Kind: EvGateLook, Verdict: VerdictPass}, t0.Add(10*time.Second))
	decidedOn := r.StateVersion // an old leader decides to advance here...
	r, tg = must(t, r, tg, p, Event{Kind: EvRollback}, t0.Add(11*time.Second))
	// ...and tries to commit after the rollback.
	if _, _, err := Transition(r, tg, p, Event{Kind: EvAdvance, ExpectStateVersion: decidedOn}, t0.Add(12*time.Second)); !errors.Is(err, ErrStale) {
		t.Fatalf("stale promotion after rollback: %v", err)
	}
	if _, _, err := Transition(r, tg, p, Event{Kind: EvAdvance}, t0.Add(12*time.Second)); err == nil {
		t.Fatal("promotion applied during rollback")
	}
}

func TestKillSwitch(t *testing.T) {
	p := policy()
	r, tg := fresh()
	r, tg = must(t, r, tg, p, Event{Kind: EvStart}, t0)
	r, tg = must(t, r, tg, p, Event{Kind: EvKill, Reason: "incident"}, t0)
	if !tg.Frozen || r.State != RollingBack || tg.DesiredFor(0) != 7 {
		t.Fatalf("%+v %+v", r, tg)
	}
	if _, _, err := Transition(r, tg, p, Event{Kind: EvKill}, t0); !errors.Is(err, ErrNoop) {
		t.Fatalf("repeat kill: %v", err)
	}
	// No active release: fall back to previous known-good.
	tg2 := Target{Generation: 9, Baseline: 8, PreviousGood: 7, ActiveRing: -1}
	er, nt, err := KillWithoutRelease(tg2, "incident", t0)
	if err != nil || nt.Baseline != 7 || nt.Generation != 10 || !nt.Frozen || er.State != RollingBack || er.Candidate != 8 {
		t.Fatalf("%v %+v %+v", err, er, nt)
	}
	if _, _, err := KillWithoutRelease(nt, "again", t0); !errors.Is(err, ErrNoop) {
		t.Fatalf("repeat: %v", err)
	}
	if _, _, err := KillWithoutRelease(Target{Baseline: 1}, "x", t0); !errors.Is(err, ErrPrecondition) {
		t.Fatalf("no fallback: %v", err)
	}
}

// simAgent models a consumer that only ever applies a higher generation,
// receiving updates late, duplicated and out of order.
type simAgent struct {
	ring       int32
	generation int64
	version    int64
}

type sent struct {
	generation, version int64
}

// TestProperty_RandomInterleavings drives random sequences of controller
// decisions, operator commands, stale (old-leader) commits and delayed agent
// messages, and checks the invariants from docs/invariants.md after every
// step.
func TestProperty_RandomInterleavings(t *testing.T) {
	for seed := uint64(1); seed <= 400; seed++ {
		rng := rand.New(rand.NewPCG(seed, seed*7919))
		p := policy()
		p.Rings = int32(1 + rng.IntN(4))
		p.Auto = rng.IntN(2) == 0
		p.RollbackOnFail = rng.IntN(2) == 0
		r, tg := fresh()
		now := t0
		agents := make([]*simAgent, 40)
		for i := range agents {
			rg := int32(rng.IntN(int(p.Rings) + 1))
			if rg == p.Rings {
				rg = ring.Holdout
			}
			agents[i] = &simAgent{ring: rg, version: tg.Baseline, generation: tg.Generation}
		}
		// Every message ever produced for each ring; agents may receive any of
		// them at any later time.
		history := map[int32][]sent{}
		record := func() {
			for _, a := range agents {
				history[a.ring] = append(history[a.ring], sent{tg.Generation, tg.DesiredFor(a.ring)})
			}
		}
		record()
		var staleVersions []int64
		everRolledBack := false
		kinds := []EventKind{EvStart, EvBeginBake, EvGateLook, EvAdvance, EvHalt, EvFinishRollback, EvAutoRollback, EvPromote, EvPause, EvResume, EvRollback, EvKill}
		verdicts := []Verdict{VerdictPass, VerdictPass, VerdictInconclusive, VerdictFail}

		for step := range 120 {
			now = now.Add(time.Duration(rng.IntN(8000)) * time.Millisecond)
			ev := Event{Kind: kinds[rng.IntN(len(kinds))], Verdict: verdicts[rng.IntN(len(verdicts))], Override: rng.IntN(25) == 0, Converged: rng.IntN(2) == 0}
			if ev.Kind == EvKill || ev.Kind == EvRollback || ev.Kind == EvAutoRollback {
				if rng.IntN(6) != 0 { // keep emergencies rarer so runs get far
					ev.Kind = EvGateLook
				}
			}
			// One in five controller events comes from a deposed leader that
			// decided on an older snapshot.
			if len(staleVersions) > 0 && rng.IntN(5) == 0 {
				ev.ExpectStateVersion = staleVersions[rng.IntN(len(staleVersions))]
			}
			prevR, prevT := r, tg
			nr, nt, err := Transition(r, tg, p, ev, now)
			if err != nil {
				if nr != prevR || nt != prevT {
					t.Fatalf("seed %d step %d: failed transition mutated state", seed, step)
				}
			} else {
				if ev.ExpectStateVersion != 0 && ev.ExpectStateVersion != prevR.StateVersion {
					t.Fatalf("seed %d: stale event committed", seed)
				}
				staleVersions = append(staleVersions, prevR.StateVersion)
				r, tg = nr, nt

				// I1: generations never decrease, and increase whenever any
				// ring's desired version changes.
				if tg.Generation < prevT.Generation {
					t.Fatalf("seed %d: generation decreased", seed)
				}
				for _, a := range agents {
					if tg.DesiredFor(a.ring) != prevT.DesiredFor(a.ring) && tg.Generation == prevT.Generation {
						t.Fatalf("seed %d: desired version changed without a new generation", seed)
					}
				}
				if r.StateVersion != prevR.StateVersion+1 {
					t.Fatalf("seed %d: state version did not advance", seed)
				}
				// Rings are exposed one at a time, in order.
				if r.State.Active() && r.State != RollingBack && r.ActiveRing > prevR.ActiveRing+1 {
					t.Fatalf("seed %d: skipped a ring", seed)
				}
				// Promotion without override requires a recorded pass.
				if (ev.Kind == EvAdvance || (ev.Kind == EvPromote && !ev.Override)) && !prevR.GatePassed {
					t.Fatalf("seed %d: promoted ring %d without gate pass", seed, prevR.ActiveRing)
				}
				if prevR.Paused && r.ActiveRing > prevR.ActiveRing {
					t.Fatalf("seed %d: advanced while paused", seed)
				}
				// I7: once rolled back, the candidate is never desired again.
				if r.State == RollingBack || r.State == RolledBack {
					everRolledBack = true
				}
				if everRolledBack {
					for _, a := range agents {
						if tg.DesiredFor(a.ring) == r.Candidate && r.Candidate != tg.Baseline {
							t.Fatalf("seed %d: candidate desired after rollback", seed)
						}
					}
					if r.State == Complete {
						t.Fatalf("seed %d: completed after rollback", seed)
					}
				}
				if !prevR.State.Active() {
					t.Fatalf("seed %d: terminal state %s changed", seed, prevR.State)
				}
				record()
			}

			// Deliver a few arbitrary old or new messages.
			for range 6 {
				a := agents[rng.IntN(len(agents))]
				h := history[a.ring]
				m := h[rng.IntN(len(h))]
				if m.generation > a.generation {
					a.generation, a.version = m.generation, m.version
				}
			}
			for _, a := range agents {
				// I2: an agent never holds the candidate unless its ring was
				// exposed at the generation it holds.
				if a.version == r.Candidate && r.Candidate != r.Baseline {
					if a.ring == ring.Holdout && r.State != Complete {
						t.Fatalf("seed %d: holdout agent got the candidate before completion", seed)
					}
				}
				// An agent that has caught up holds exactly the desired version.
				if a.generation == tg.Generation && a.version != tg.DesiredFor(a.ring) {
					t.Fatalf("seed %d: caught-up agent holds %d, desired %d", seed, a.version, tg.DesiredFor(a.ring))
				}
			}
		}
		// Final reconciliation: everyone reconnects and reads current truth.
		for _, a := range agents {
			if tg.Generation > a.generation {
				a.generation, a.version = tg.Generation, tg.DesiredFor(a.ring)
			}
			if everRolledBack && a.version == r.Candidate && r.Candidate != tg.Baseline {
				t.Fatalf("seed %d: agent still on rolled-back candidate after reconciling", seed)
			}
		}
	}
}

// TestCoverage_EveryStateReached documents which states and reasons the table
// and property tests drive the machine through.
func TestCoverage_EveryStateReached(t *testing.T) {
	seen := map[State]bool{}
	for seed := uint64(1); seed <= 200; seed++ {
		rng := rand.New(rand.NewPCG(seed, 99))
		p := policy()
		r, tg := fresh()
		kinds := []EventKind{EvStart, EvBeginBake, EvGateLook, EvAdvance, EvHalt, EvFinishRollback, EvResume}
		if seed%2 == 0 { // half the runs include operator rollbacks
			kinds = append(kinds, EvRollback)
		}
		for i := range 600 {
			seen[r.State] = true
			now := t0.Add(time.Duration(i) * time.Minute) // each step is past the minimum bake
			nr, nt, err := Transition(r, tg, p, Event{Kind: kinds[rng.IntN(len(kinds))], Verdict: VerdictPass, Converged: true}, now)
			if err == nil {
				r, tg = nr, nt
			}
		}
		seen[r.State] = true
	}
	for _, s := range []State{Pending, Rolling, Baking, Halted, RollingBack, RolledBack, Complete} {
		if !seen[s] {
			t.Errorf("state %s never reached", s)
		}
	}
}
