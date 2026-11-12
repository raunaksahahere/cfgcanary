// Package rollout is the release state machine. It is pure: no clocks, no
// database, no network. The store loads rows, calls Transition, and persists
// the result in one transaction; the controller calls Decide to learn what to
// do next. Property tests therefore exercise the same code that runs in
// production. See docs/invariants.md and docs/ARCHITECTURE.md.
package rollout

import (
	"errors"
	"fmt"
	"time"

	"github.com/raunaksahahere/cfgcanary/internal/ring"
)

// State of a release.
type State string

const (
	Pending     State = "PENDING"
	Rolling     State = "ROLLING"
	Baking      State = "BAKING"
	Halted      State = "HALTED"
	RollingBack State = "ROLLING_BACK"
	RolledBack  State = "ROLLED_BACK"
	Complete    State = "COMPLETE"
)

// Active reports whether the release still owns its target.
func (s State) Active() bool {
	switch s {
	case Pending, Rolling, Baking, Halted, RollingBack:
		return true
	}
	return false
}

// Verdict of one gate look.
type Verdict string

const (
	VerdictPass         Verdict = "PASS"
	VerdictFail         Verdict = "FAIL"
	VerdictInconclusive Verdict = "INCONCLUSIVE"
)

// Policy is the part of the frozen rollout policy the state machine needs.
type Policy struct {
	Rings                int32
	AcceptanceThreshold  float64
	MinReachableFraction float64
	RolloutTimeout       time.Duration
	RollbackTimeout      time.Duration
	Auto                 bool
	Gated                bool // false: GATE_MODE_NONE
	MinBake              time.Duration
	LookInterval         time.Duration
	Looks                int32
	RollbackOnFail       bool
	MaxSDKRejections     int32
	MaxCrashLoops        int32
}

// Release is the mutable state of one release.
type Release struct {
	State        State
	Paused       bool
	ActiveRing   int32 // -1 before the first ring
	Candidate    int64
	Baseline     int64
	Kind         string // "" or "emergency"
	Reason       string
	StateVersion int64
	// Generation at which the active ring was exposed, and of the rollback.
	RingGeneration     int64
	RollbackGeneration int64
	RingStarted        time.Time
	BakeStarted        time.Time
	LastLook           time.Time
	RollbackStarted    time.Time
	LooksDone          int32
	GatePassed         bool
	// OverrideUsed records that some ring was promoted by break-glass.
	OverrideUsed bool
	// RollbackConverged is set when the rollback finished with every exposed
	// member confirmed.
	RollbackConverged bool
}

// Target is the desired state of a (config, environment).
type Target struct {
	Generation   int64
	Baseline     int64
	Candidate    int64
	ActiveRing   int32
	PreviousGood int64
	Frozen       bool
}

// DesiredFor returns the version an instance in the given ring should run
// (0 when it has nothing to run).
func (t Target) DesiredFor(r int32) int64 {
	if t.Candidate != 0 && r != ring.Holdout && r <= t.ActiveRing {
		return t.Candidate
	}
	return t.Baseline
}

// Counts summarises acknowledgements of exposed members for the current ring
// generation.
type Counts struct {
	Eligible, Reachable, Applied, Accepted, Rejected, Failed, CrashLoop int32
}

// RollbackCounts summarises convergence of a rollback.
type RollbackCounts struct {
	Exposed, Confirmed, Pending, Unreachable int32
}

// EventKind enumerates everything that can change a release.
type EventKind string

const (
	EvStart          EventKind = "start"           // controller
	EvBeginBake      EventKind = "begin_bake"      // controller
	EvGateLook       EventKind = "gate_look"       // controller, carries Verdict
	EvAdvance        EventKind = "advance"         // controller (auto promotion)
	EvHalt           EventKind = "halt"            // controller
	EvFinishRollback EventKind = "finish_rollback" // controller
	EvAutoRollback   EventKind = "auto_rollback"   // controller
	EvPromote        EventKind = "promote"         // operator
	EvPause          EventKind = "pause"           // operator
	EvResume         EventKind = "resume"          // operator
	EvRollback       EventKind = "rollback"        // operator
	EvKill           EventKind = "kill_switch"     // operator, break-glass
)

// Event is one requested change.
type Event struct {
	Kind    EventKind
	Reason  string
	Verdict Verdict
	// ExpectStateVersion, when non-zero, must equal Release.StateVersion.
	// The controller always sets it: a decision made on an old snapshot must
	// not commit.
	ExpectStateVersion int64
	// ExpectRing guards operator promotes against double application.
	ExpectRing *int32
	// Override skips gate and bake requirements (break-glass only; the
	// caller is responsible for authorization and auditing).
	Override bool
	// Converged qualifies EvFinishRollback.
	Converged bool
}

// Errors. ErrNoop means the command is already satisfied; callers report the
// current state as success.
var (
	ErrStale        = errors.New("release changed since the decision was made")
	ErrNoop         = errors.New("already in the requested state")
	ErrNotActive    = errors.New("release is not active")
	ErrPaused       = errors.New("release is paused")
	ErrPrecondition = errors.New("precondition failed")
)

func precond(format string, a ...any) error {
	return fmt.Errorf("%w: %s", ErrPrecondition, fmt.Sprintf(format, a...))
}

// Transition applies ev. It returns the new release and target, or an error
// and the inputs unchanged. Every successful transition increments
// StateVersion; the generation increases exactly when the mapping from ring to
// desired version changes (or must be re-authorized for a new ring range).
func Transition(r Release, t Target, p Policy, ev Event, now time.Time) (Release, Target, error) {
	if ev.ExpectStateVersion != 0 && ev.ExpectStateVersion != r.StateVersion {
		return r, t, ErrStale
	}
	if !r.State.Active() {
		if ev.Kind == EvRollback || ev.Kind == EvKill {
			if r.State == RolledBack {
				return r, t, ErrNoop
			}
		}
		return r, t, ErrNotActive
	}
	nr, nt := r, t
	forwardBlocked := func() error {
		if r.Paused {
			return ErrPaused
		}
		return nil
	}
	expose := func(ringIdx int32) {
		nr.State, nr.ActiveRing = Rolling, ringIdx
		nr.RingStarted, nr.BakeStarted, nr.LastLook = now, time.Time{}, time.Time{}
		nr.LooksDone, nr.GatePassed = 0, false
		nt.Candidate, nt.ActiveRing = r.Candidate, ringIdx
		nt.Generation++
		nr.RingGeneration = nt.Generation
	}
	complete := func() {
		nr.State = Complete
		nt.PreviousGood, nt.Baseline = t.Baseline, r.Candidate
		nt.Candidate, nt.ActiveRing = 0, -1
		nt.Generation++
	}
	rollback := func(reason string) {
		nr.Reason, nr.Paused = reason, false
		if r.State == Pending {
			// Nothing was ever exposed.
			nr.State, nr.RollbackConverged = RolledBack, true
			return
		}
		nr.State, nr.RollbackStarted = RollingBack, now
		nt.Candidate, nt.ActiveRing = 0, -1
		nt.Generation++
		nr.RollbackGeneration = nt.Generation
	}
	last := r.ActiveRing == p.Rings-1

	switch ev.Kind {
	case EvStart:
		if err := forwardBlocked(); err != nil {
			return r, t, err
		}
		if r.State != Pending {
			return r, t, precond("start requires PENDING, release is %s", r.State)
		}
		expose(0)

	case EvBeginBake:
		if err := forwardBlocked(); err != nil {
			return r, t, err
		}
		if r.State != Rolling {
			return r, t, precond("begin_bake requires ROLLING, release is %s", r.State)
		}
		nr.State, nr.BakeStarted = Baking, now

	case EvGateLook:
		if err := forwardBlocked(); err != nil {
			return r, t, err
		}
		if r.State != Baking || r.GatePassed {
			return r, t, precond("gate look requires BAKING without a recorded pass")
		}
		if r.LooksDone >= p.Looks {
			return r, t, precond("all %d declared looks are used", p.Looks)
		}
		nr.LooksDone, nr.LastLook = r.LooksDone+1, now
		switch ev.Verdict {
		case VerdictPass:
			nr.GatePassed = true
		case VerdictFail:
			if p.RollbackOnFail {
				rollback("gate_fail")
			} else {
				nr.State, nr.Reason = Halted, "gate_fail"
			}
		case VerdictInconclusive:
			if nr.LooksDone >= p.Looks {
				nr.State, nr.Reason = Halted, "gate_inconclusive"
			}
		default:
			return r, t, precond("unknown verdict %q", ev.Verdict)
		}

	case EvAdvance, EvPromote:
		if ev.Kind == EvPromote && ev.ExpectRing != nil && *ev.ExpectRing != r.ActiveRing {
			return r, t, precond("expected active ring %d, release is at ring %d", *ev.ExpectRing, r.ActiveRing)
		}
		if err := forwardBlocked(); err != nil {
			return r, t, err
		}
		if ev.Override && ev.Kind == EvPromote {
			if r.State != Baking && r.State != Halted && r.State != Rolling {
				return r, t, precond("override promote requires an exposed ring, release is %s", r.State)
			}
			nr.OverrideUsed = true
		} else {
			if r.State != Baking {
				return r, t, precond("promotion requires BAKING, release is %s", r.State)
			}
			if now.Sub(r.BakeStarted) < p.MinBake {
				return r, t, precond("minimum bake of %s has not elapsed", p.MinBake)
			}
			if p.Gated && !r.GatePassed {
				return r, t, precond("gate has not passed for ring %d", r.ActiveRing)
			}
			if ev.Kind == EvAdvance && !p.Auto {
				return r, t, precond("policy requires manual promotion")
			}
		}
		nr.Reason = ""
		if last {
			complete()
		} else {
			expose(r.ActiveRing + 1)
		}

	case EvHalt:
		if r.State != Rolling && r.State != Baking {
			return r, t, precond("halt requires ROLLING or BAKING, release is %s", r.State)
		}
		nr.State, nr.Reason = Halted, ev.Reason

	case EvPause:
		if r.State == RollingBack {
			return r, t, precond("a rollback cannot be paused")
		}
		if r.Paused {
			return r, t, ErrNoop
		}
		nr.Paused = true

	case EvResume:
		switch {
		case r.Paused:
			nr.Paused = false
			// Evidence gathered before or during the pause is not reused.
			if r.State == Baking {
				nr.BakeStarted, nr.LastLook, nr.LooksDone, nr.GatePassed = now, time.Time{}, 0, false
			}
			if r.State == Rolling {
				nr.RingStarted = now
			}
		case r.State == Halted:
			// Re-evaluate the same ring from fresh evidence.
			nr.State, nr.Reason = Rolling, ""
			nr.RingStarted, nr.BakeStarted, nr.LastLook = now, time.Time{}, time.Time{}
			nr.LooksDone, nr.GatePassed = 0, false
		default:
			return r, t, ErrNoop
		}

	case EvRollback, EvAutoRollback, EvKill:
		if r.State == RollingBack {
			if ev.Kind == EvKill && !t.Frozen {
				nt.Frozen = true
				break
			}
			return r, t, ErrNoop
		}
		rollback(ev.Reason)
		if ev.Kind == EvKill {
			nt.Frozen = true
		}

	case EvFinishRollback:
		if r.State != RollingBack {
			return r, t, precond("finish_rollback requires ROLLING_BACK, release is %s", r.State)
		}
		nr.State, nr.RollbackConverged = RolledBack, ev.Converged

	default:
		return r, t, precond("unknown event %q", ev.Kind)
	}
	nr.StateVersion++
	return nr, nt, nil
}

// KillWithoutRelease reverts a target that has no active release to its
// previous known-good version and freezes it. It returns the emergency
// release that tracks convergence.
func KillWithoutRelease(t Target, reason string, now time.Time) (Release, Target, error) {
	if t.Frozen {
		return Release{}, t, ErrNoop
	}
	if t.PreviousGood == 0 || t.PreviousGood == t.Baseline {
		return Release{}, t, precond("no previous known-good version to fall back to")
	}
	nt := t
	bad := t.Baseline
	nt.Baseline, nt.PreviousGood = t.PreviousGood, t.PreviousGood
	nt.Candidate, nt.ActiveRing = 0, -1
	nt.Frozen = true
	nt.Generation++
	return Release{
		State: RollingBack, ActiveRing: -1, Candidate: bad, Baseline: nt.Baseline, Kind: "emergency",
		Reason: reason, StateVersion: 1, RollbackGeneration: nt.Generation, RollbackStarted: now,
	}, nt, nil
}

// Action is what the controller should do next for a release.
type Action struct {
	Event EventKind // "" means wait
	// EvaluateGate asks the controller to run a gate look and then submit
	// EvGateLook with the verdict.
	EvaluateGate bool
	Reason       string
	Converged    bool
}

// Decide is the controller's policy: given a release and current evidence,
// what should happen now? It never returns an operator-only event.
func Decide(r Release, p Policy, c Counts, rb RollbackCounts, now time.Time) Action {
	switch r.State {
	case RollingBack:
		if rb.Pending == 0 {
			return Action{Event: EvFinishRollback, Converged: rb.Unreachable == 0}
		}
		if now.Sub(r.RollbackStarted) >= p.RollbackTimeout {
			return Action{Event: EvFinishRollback, Converged: false, Reason: "rollback_timeout"}
		}
		return Action{}
	case Pending:
		if r.Paused {
			return Action{}
		}
		return Action{Event: EvStart}
	case Rolling, Baking:
		// Hard safety evidence acts even while paused: a pause stops
		// progress, it does not keep a rejected config in place.
		if c.Rejected > p.MaxSDKRejections {
			return Action{Event: EvAutoRollback, Reason: "sdk_rejected"}
		}
		if c.CrashLoop > p.MaxCrashLoops {
			return Action{Event: EvAutoRollback, Reason: "crash_loop"}
		}
		if r.Paused {
			return Action{}
		}
	default:
		return Action{}
	}

	if r.State == Rolling {
		timedOut := now.Sub(r.RingStarted) >= p.RolloutTimeout
		if c.Eligible == 0 {
			return Action{Event: EvHalt, Reason: "empty_cohort"}
		}
		if float64(c.Reachable) < p.MinReachableFraction*float64(c.Eligible) || c.Reachable == 0 {
			if timedOut {
				return Action{Event: EvHalt, Reason: "too_few_reachable"}
			}
			return Action{}
		}
		if float64(c.Accepted) >= p.AcceptanceThreshold*float64(c.Reachable) {
			return Action{Event: EvBeginBake}
		}
		if timedOut {
			return Action{Event: EvHalt, Reason: "convergence_timeout"}
		}
		return Action{}
	}

	// Baking.
	if now.Sub(r.BakeStarted) < p.MinBake {
		return Action{}
	}
	if !p.Gated {
		return Action{} // waits for an operator
	}
	if r.GatePassed {
		if p.Auto {
			return Action{Event: EvAdvance}
		}
		return Action{}
	}
	if r.LooksDone >= p.Looks {
		return Action{Event: EvHalt, Reason: "gate_inconclusive"}
	}
	if r.LooksDone == 0 || now.Sub(r.LastLook) >= p.LookInterval {
		return Action{EvaluateGate: true}
	}
	return Action{}
}
