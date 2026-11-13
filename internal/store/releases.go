package store

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	pb "github.com/raunaksahahere/cfgcanary/api/gen/cfgcanary/v1"
	"github.com/raunaksahahere/cfgcanary/internal/policy"
	"github.com/raunaksahahere/cfgcanary/internal/ring"
	"github.com/raunaksahahere/cfgcanary/internal/rollout"
)

const activeStates = `('PENDING','ROLLING','BAKING','HALTED','ROLLING_BACK')`

// RingProgress is the durable per-ring history of a release.
type RingProgress struct {
	Index       int32      `json:"index"`
	Phase       string     `json:"phase"` // pending, rolling, baking, passed, rolled_back
	Generation  int64      `json:"generation,omitempty"`
	Started     *time.Time `json:"started,omitempty"`
	BakeStarted *time.Time `json:"bake_started,omitempty"`
	LooksDone   int32      `json:"looks_done,omitempty"`
	Verdict     string     `json:"verdict,omitempty"`
	Reason      string     `json:"reason,omitempty"`
}

// ReleaseRow is a release as stored.
type ReleaseRow struct {
	ID, Config, Env string
	R               rollout.Release
	ExposedRing     int32
	FirstGeneration int64
	Epoch           int64
	Policy          *pb.RolloutPolicy
	Rings           []RingProgress
	Message, Note   string
	CreatedBy       string
	Created         time.Time
	Updated         time.Time
	Finished        *time.Time
	RollbackSummary *pb.RollbackStatus
}

// TargetRow is a target as stored.
type TargetRow struct {
	Config, Env  string
	T            rollout.Target
	ReleaseID    string
	FrozenReason string
	RingPolicy   *pb.RolloutPolicy
	Updated      time.Time
}

// ProtoTarget converts to the API type.
func (t *TargetRow) ProtoTarget() *pb.Target {
	return &pb.Target{Config: t.Config, Environment: t.Env, Generation: t.T.Generation, BaselineVersion: t.T.Baseline,
		CandidateVersion: t.T.Candidate, ActiveRing: t.T.ActiveRing, ReleaseId: t.ReleaseID,
		PreviousGoodVersion: t.T.PreviousGood, Frozen: t.T.Frozen, FrozenReason: t.FrozenReason, Updated: ts(t.Updated)}
}

const targetCols = `config, environment, generation, baseline_version, candidate_version, active_ring, release_id,
	previous_good_version, frozen, frozen_reason, ring_policy, updated_at`

func scanTarget(row pgx.Row) (*TargetRow, error) {
	var t TargetRow
	var pol []byte
	if err := row.Scan(&t.Config, &t.Env, &t.T.Generation, &t.T.Baseline, &t.T.Candidate, &t.T.ActiveRing, &t.ReleaseID,
		&t.T.PreviousGood, &t.T.Frozen, &t.FrozenReason, &pol, &t.Updated); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	t.RingPolicy = &pb.RolloutPolicy{}
	if err := pjsonUn.Unmarshal(pol, t.RingPolicy); err != nil {
		return nil, err
	}
	return &t, nil
}

func lockTarget(ctx context.Context, tx pgx.Tx, config, env string) (*TargetRow, error) {
	t, err := scanTarget(tx.QueryRow(ctx, `SELECT `+targetCols+` FROM targets WHERE config = $1 AND environment = $2 FOR UPDATE`, config, env))
	if errors.Is(err, ErrNotFound) {
		return nil, wrap(ErrNotFound, "config %q has no environment %q", config, env)
	}
	return t, err
}

// GetTarget returns a target.
func (s *Store) GetTarget(ctx context.Context, config, env string) (*pb.Target, error) {
	t, err := s.TargetRow(ctx, config, env)
	if err != nil {
		return nil, err
	}
	return t.ProtoTarget(), nil
}

// TargetRow returns a target row.
func (s *Store) TargetRow(ctx context.Context, config, env string) (*TargetRow, error) {
	t, err := scanTarget(s.Pool.QueryRow(ctx, `SELECT `+targetCols+` FROM targets WHERE config = $1 AND environment = $2`, config, env))
	if errors.Is(err, ErrNotFound) {
		return nil, wrap(ErrNotFound, "config %q has no environment %q", config, env)
	}
	return t, err
}

// TargetsForEnv lists every target of an environment.
func (s *Store) TargetsForEnv(ctx context.Context, env string) ([]*TargetRow, error) {
	rows, err := s.Pool.Query(ctx, `SELECT `+targetCols+` FROM targets WHERE environment = $1 OR $1 = '' ORDER BY config, environment`, env)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*TargetRow
	for rows.Next() {
		t, err := scanTarget(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

func (s *Store) persistTarget(ctx context.Context, tx pgx.Tx, t *TargetRow) error {
	pol, err := pjson.Marshal(t.RingPolicy)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `UPDATE targets SET generation=$3, baseline_version=$4, candidate_version=$5, active_ring=$6,
		release_id=$7, previous_good_version=$8, frozen=$9, frozen_reason=$10, ring_policy=$11, updated_at=now()
		WHERE config=$1 AND environment=$2`,
		t.Config, t.Env, t.T.Generation, t.T.Baseline, t.T.Candidate, t.T.ActiveRing, t.ReleaseID, t.T.PreviousGood,
		t.T.Frozen, t.FrozenReason, pol)
	return err
}

const releaseCols = `id, config, environment, version, baseline_version, kind, state, paused, active_ring, exposed_ring,
	reason, message, note, state_version, epoch, first_generation, ring_generation, rollback_generation,
	ring_started_at, bake_started_at, last_look_at, rollback_started_at, looks_done, gate_passed, override_used,
	rollback_converged, policy, rings, rollback_summary, created_by, created_at, updated_at, finished_at`

func tv(t *time.Time) time.Time {
	if t == nil {
		return time.Time{}
	}
	return *t
}

func tp(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

func scanRelease(row pgx.Row) (*ReleaseRow, error) {
	var r ReleaseRow
	var state string
	var ringStarted, bakeStarted, lastLook, rbStarted *time.Time
	var pol, rings, rb []byte
	if err := row.Scan(&r.ID, &r.Config, &r.Env, &r.R.Candidate, &r.R.Baseline, &r.R.Kind, &state, &r.R.Paused, &r.R.ActiveRing,
		&r.ExposedRing, &r.R.Reason, &r.Message, &r.Note, &r.R.StateVersion, &r.Epoch, &r.FirstGeneration, &r.R.RingGeneration,
		&r.R.RollbackGeneration, &ringStarted, &bakeStarted, &lastLook, &rbStarted, &r.R.LooksDone, &r.R.GatePassed,
		&r.R.OverrideUsed, &r.R.RollbackConverged, &pol, &rings, &rb, &r.CreatedBy, &r.Created, &r.Updated, &r.Finished); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	r.R.State = rollout.State(state)
	r.R.RingStarted, r.R.BakeStarted, r.R.LastLook, r.R.RollbackStarted = tv(ringStarted), tv(bakeStarted), tv(lastLook), tv(rbStarted)
	r.Policy = &pb.RolloutPolicy{}
	if err := pjsonUn.Unmarshal(pol, r.Policy); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(rings, &r.Rings); err != nil {
		return nil, err
	}
	if len(rb) > 0 {
		r.RollbackSummary = &pb.RollbackStatus{}
		if err := pjsonUn.Unmarshal(rb, r.RollbackSummary); err != nil {
			return nil, err
		}
	}
	return &r, nil
}

// ReleaseRow returns a release as stored.
func (s *Store) ReleaseRow(ctx context.Context, id string) (*ReleaseRow, error) {
	r, err := scanRelease(s.Pool.QueryRow(ctx, `SELECT `+releaseCols+` FROM releases WHERE id = $1`, id))
	if errors.Is(err, ErrNotFound) {
		return nil, wrap(ErrNotFound, "release %q", id)
	}
	return r, err
}

// ActiveReleases lists releases the controller must reconcile.
func (s *Store) ActiveReleases(ctx context.Context) ([]*ReleaseRow, error) {
	rows, err := s.Pool.Query(ctx, `SELECT `+releaseCols+` FROM releases WHERE state IN `+activeStates+` ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*ReleaseRow
	for rows.Next() {
		r, err := scanRelease(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func newReleaseID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return "rel-" + hex.EncodeToString(b[:])
}

// members returns instance IDs of an environment seen within the window.
func members(ctx context.Context, q querier, env string, window time.Duration) ([]string, error) {
	rows, err := q.Query(ctx, `SELECT instance_id FROM instances WHERE environment = $1
		AND last_seen > now() - make_interval(secs => $2) ORDER BY instance_id`, env, window.Seconds())
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

// PlanInput is what CreateRelease and PlanRelease need.
type PlanInput struct {
	Config, Env string
	Version     int64
	// MembershipWindow bounds which known instances count as fleet members.
	MembershipWindow time.Duration
}

// plan computes cohort sizes and blockers. It reads only.
func plan(ctx context.Context, q querier, in PlanInput, lockVersion bool) (*pb.ReleasePlan, *TargetRow, []string, []int32, error) {
	cfg, err := getConfig(ctx, q, in.Config)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	raw, ok := cfg.Spec.Environments[in.Env]
	if !ok {
		return nil, nil, nil, nil, wrap(ErrNotFound, "config %q has no environment %q", in.Config, in.Env)
	}
	pol, err := policy.Normalize(in.Config, in.Env, raw)
	if err != nil {
		return nil, nil, nil, nil, wrap(ErrPrecondition, "rollout policy of %s/%s is invalid: %v", in.Config, in.Env, err)
	}
	tgt, err := scanTarget(q.QueryRow(ctx, `SELECT `+targetCols+` FROM targets WHERE config = $1 AND environment = $2`, in.Config, in.Env))
	if err != nil {
		return nil, nil, nil, nil, err
	}
	p := &pb.ReleasePlan{Config: in.Config, Environment: in.Env, Version: in.Version, BaselineVersion: tgt.T.Baseline, Policy: pol}

	lock := ""
	if lockVersion {
		// Serializes with the garbage collector (see Store.GC).
		lock = " FOR SHARE"
	}
	v, err := scanVersion(q.QueryRow(ctx, `SELECT `+versionCols+` FROM config_versions WHERE config = $1 AND version = $2 AND purged_at IS NULL`+lock, in.Config, in.Version))
	switch {
	case errors.Is(err, ErrNotFound):
		p.Blockers = append(p.Blockers, fmt.Sprintf("version %d of %q does not exist or was purged", in.Version, in.Config))
	case err != nil:
		return nil, nil, nil, nil, err
	default:
		allowed := false
		for _, e := range v.Environments {
			allowed = allowed || e == in.Env
		}
		if !allowed {
			p.Blockers = append(p.Blockers, fmt.Sprintf("version %d was not submitted for environment %q", in.Version, in.Env))
		}
	}
	if in.Version == tgt.T.Baseline {
		p.Blockers = append(p.Blockers, fmt.Sprintf("version %d is already the baseline", in.Version))
	}
	if tgt.T.Frozen {
		p.Blockers = append(p.Blockers, "target is frozen by a kill switch ("+tgt.FrozenReason+"); unfreeze it first")
	}
	var activeID string
	err = q.QueryRow(ctx, `SELECT id FROM releases WHERE config = $1 AND environment = $2 AND state IN `+activeStates, in.Config, in.Env).Scan(&activeID)
	if err == nil {
		p.Blockers = append(p.Blockers, "release "+activeID+" is still active for this target")
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return nil, nil, nil, nil, err
	}

	ids, err := members(ctx, q, in.Env, in.MembershipWindow)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	rp := policy.Ring(pol)
	assigned := make([]int32, len(ids))
	p.Rings = make([]*pb.PlanRing, len(pol.Rings))
	for i, r := range pol.Rings {
		p.Rings[i] = &pb.PlanRing{Index: int32(i), Name: r.Name, CumulativeBps: r.CumulativeBps}
	}
	for i, id := range ids {
		a := rp.Assign(id)
		assigned[i] = a
		if a == ring.Holdout {
			p.HoldoutMembers++
			continue
		}
		pr := p.Rings[a]
		pr.Members++
		if len(pr.Sample) < 5 {
			pr.Sample = append(pr.Sample, id)
		}
	}
	p.TotalMembers = int32(len(ids))
	// Reachability is informational in the plan; all counted members were
	// seen within the membership window.
	for _, pr := range p.Rings {
		pr.Reachable = pr.Members
	}
	gated := pol.Gate.Mode == pb.GateMode_GATE_MODE_PROMETHEUS
	if len(p.Rings) > 0 {
		first := p.Rings[0].Members
		if first < pol.MinInstances {
			p.Blockers = append(p.Blockers, fmt.Sprintf("first ring %q would have %d members; the policy requires at least %d (a percentage does not guarantee a non-empty cohort in a small fleet)",
				p.Rings[0].Name, first, pol.MinInstances))
		}
		if control := p.TotalMembers - first; gated && control < pol.MinInstances {
			p.Blockers = append(p.Blockers, fmt.Sprintf("control cohort for the first ring would have %d members; the policy requires at least %d", control, pol.MinInstances))
		}
	}
	exposed := int32(0)
	for i, pr := range p.Rings {
		if pr.Members == 0 && i > 0 {
			p.Blockers = append(p.Blockers, fmt.Sprintf("ring %q has no members", pr.Name))
		}
		exposed += pr.Members
		if gated {
			last := i == len(p.Rings)-1
			if exposed < pol.Gate.MinInstances {
				p.Warnings = append(p.Warnings, fmt.Sprintf("after ring %q only %d instances are exposed; the gate needs %d per cohort and would be inconclusive", pr.Name, exposed, pol.Gate.MinInstances))
			}
			if control := p.TotalMembers - exposed; control < pol.Gate.MinInstances && !(last && pol.FinalStage == pb.FinalStage_FINAL_STAGE_HARD_THRESHOLDS_ONLY) {
				p.Warnings = append(p.Warnings, fmt.Sprintf("at ring %q the control cohort has %d instances; the gate needs %d and would be inconclusive", pr.Name, control, pol.Gate.MinInstances))
			}
		}
	}
	if pol.FinalStage == pb.FinalStage_FINAL_STAGE_HARD_THRESHOLDS_ONLY && gated {
		p.Warnings = append(p.Warnings, "the last ring reaches 100%: no control cohort exists for it, so only hard thresholds and SDK evidence apply there")
	}
	return p, tgt, ids, assigned, nil
}

// PlanRelease computes a release plan without writing anything.
func (s *Store) PlanRelease(ctx context.Context, in PlanInput) (*pb.ReleasePlan, error) {
	p, _, _, _, err := plan(ctx, s.Pool, in, false)
	return p, err
}

// CreateRelease validates the plan under the target lock and inserts a
// PENDING release with its frozen policy and members.
func (s *Store) CreateRelease(ctx context.Context, in PlanInput, actor, requestID, note string) (*ReleaseRow, *pb.ReleasePlan, error) {
	var out *ReleaseRow
	var outPlan *pb.ReleasePlan
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		ref, seen, err := idempotent(ctx, tx, actor, requestID, "CreateRelease")
		if err != nil {
			return err
		}
		if seen {
			out, err = scanRelease(tx.QueryRow(ctx, `SELECT `+releaseCols+` FROM releases WHERE id = $1`, ref))
			return err
		}
		if _, err := lockTarget(ctx, tx, in.Config, in.Env); err != nil {
			return err
		}
		p, tgt, ids, assigned, err := plan(ctx, tx, in, true)
		if err != nil {
			return err
		}
		outPlan = p
		if len(p.Blockers) > 0 {
			return wrap(ErrPrecondition, "%s", p.Blockers[0])
		}
		id := newReleaseID()
		pol, err := pjson.Marshal(p.Policy)
		if err != nil {
			return err
		}
		rings := make([]RingProgress, len(p.Policy.Rings))
		for i := range rings {
			rings[i] = RingProgress{Index: int32(i), Phase: "pending"}
		}
		ringsJSON, _ := json.Marshal(rings)
		if _, err := tx.Exec(ctx, `INSERT INTO releases (id, config, environment, version, baseline_version, state, policy, rings, created_by, note)
			VALUES ($1,$2,$3,$4,$5,'PENDING',$6,$7,$8,$9)`,
			id, in.Config, in.Env, in.Version, tgt.T.Baseline, pol, ringsJSON, actor, truncate(note, 500)); err != nil {
			if isUnique(err) {
				return wrap(ErrAlreadyExists, "another release is active for %s/%s", in.Config, in.Env)
			}
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO release_members (release_id, instance_id, ring)
			SELECT $1, * FROM unnest($2::text[], $3::int[])`, id, ids, assigned); err != nil {
			return err
		}
		tgt.ReleaseID, tgt.RingPolicy = id, p.Policy
		if err := s.persistTarget(ctx, tx, tgt); err != nil {
			return err
		}
		if err := insertAudit(ctx, tx, Audit{Actor: actor, Action: "release.create", Config: in.Config, Environment: in.Env, Release: id,
			Generation: tgt.T.Generation, Reason: note,
			Detail: map[string]any{"version": in.Version, "baseline_version": tgt.T.Baseline, "members": len(ids)}}); err != nil {
			return err
		}
		if err := remember(ctx, tx, actor, requestID, "CreateRelease", id); err != nil {
			return err
		}
		out, err = scanRelease(tx.QueryRow(ctx, `SELECT `+releaseCols+` FROM releases WHERE id = $1`, id))
		return err
	})
	return out, outPlan, err
}

// GateRecord is the evidence of one gate look.
type GateRecord struct {
	Ring, Look int32
	Verdict    string
	Reason     string
	Evidence   json.RawMessage
}

// EventMeta describes who is applying an event and what to record with it.
type EventMeta struct {
	Actor string
	// Epoch is the controller's fencing token; 0 for operator commands.
	Epoch     int64
	RequestID string
	Method    string
	Message   string
	Gate      *GateRecord
	Rollback  *pb.RollbackStatus
	Detail    map[string]any
	// AuditAction overrides "release.<event>".
	AuditAction string
}

// ApplyEvent applies one state-machine event to a release atomically with the
// target update, the generation bump, the audit event and the notification.
// noop is true when the command was already satisfied.
func (s *Store) ApplyEvent(ctx context.Context, id string, ev rollout.Event, meta EventMeta) (row *ReleaseRow, noop bool, err error) {
	err = s.inTx(ctx, func(tx pgx.Tx) error {
		noop = false
		if meta.Epoch > 0 {
			if err := checkEpoch(ctx, tx, meta.Epoch); err != nil {
				return err
			}
		}
		_, seen, err := idempotent(ctx, tx, meta.Actor, meta.RequestID, meta.Method)
		if err != nil {
			return err
		}
		var config, env string
		if err := tx.QueryRow(ctx, `SELECT config, environment FROM releases WHERE id = $1`, id).Scan(&config, &env); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return wrap(ErrNotFound, "release %q", id)
			}
			return err
		}
		// Lock order is always target, then release.
		tgt, err := lockTarget(ctx, tx, config, env)
		if err != nil {
			return err
		}
		rel, err := scanRelease(tx.QueryRow(ctx, `SELECT `+releaseCols+` FROM releases WHERE id = $1 FOR UPDATE`, id))
		if err != nil {
			return err
		}
		if seen {
			row, noop = rel, true
			return nil
		}
		row, noop, err = s.applyLocked(ctx, tx, tgt, rel, ev, meta)
		if err != nil {
			return err
		}
		if noop {
			return nil
		}
		return remember(ctx, tx, meta.Actor, meta.RequestID, meta.Method, id)
	})
	return row, noop, err
}

func mapRolloutErr(err error) error {
	switch {
	case errors.Is(err, rollout.ErrStale):
		return wrap(ErrConflict, "%v", err)
	case errors.Is(err, rollout.ErrPaused), errors.Is(err, rollout.ErrPrecondition), errors.Is(err, rollout.ErrNotActive):
		return wrap(ErrPrecondition, "%v", err)
	}
	return err
}

func (s *Store) applyLocked(ctx context.Context, tx pgx.Tx, tgt *TargetRow, rel *ReleaseRow, ev rollout.Event, meta EventMeta) (*ReleaseRow, bool, error) {
	var now time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return nil, false, err
	}
	old := rel.R
	oldGen := tgt.T.Generation
	nr, nt, err := rollout.Transition(rel.R, tgt.T, policy.Rollout(rel.Policy), ev, now)
	if errors.Is(err, rollout.ErrNoop) {
		return rel, true, nil
	}
	if err != nil {
		return nil, false, mapRolloutErr(err)
	}
	rel.R, tgt.T = nr, nt
	if meta.Epoch > 0 {
		rel.Epoch = meta.Epoch
	}
	if nr.ActiveRing > rel.ExposedRing {
		rel.ExposedRing = nr.ActiveRing
	}
	if old.State == rollout.Pending && nr.State == rollout.Rolling {
		rel.FirstGeneration = nt.Generation
	}
	if meta.Message != "" {
		rel.Message = truncate(meta.Message, 500)
	}
	updateRings(rel, old, ev, meta, now)
	if !nr.State.Active() {
		rel.Finished = &now
		tgt.ReleaseID = ""
		if meta.Rollback != nil {
			rel.RollbackSummary = meta.Rollback
		}
	}
	if ev.Kind == rollout.EvKill {
		tgt.FrozenReason = truncate(ev.Reason, 300)
	}
	if err := s.persistRelease(ctx, tx, rel); err != nil {
		return nil, false, err
	}
	if err := s.persistTarget(ctx, tx, tgt); err != nil {
		return nil, false, err
	}
	if g := meta.Gate; g != nil {
		ev := g.Evidence
		if len(ev) == 0 {
			ev = json.RawMessage("{}")
		}
		if _, err := tx.Exec(ctx, `INSERT INTO gate_evaluations (release_id, ring, look, verdict, reason, evidence, epoch)
			VALUES ($1,$2,$3,$4,$5,$6,$7)`, rel.ID, g.Ring, g.Look, g.Verdict, truncate(g.Reason, 500), []byte(ev), meta.Epoch); err != nil {
			return nil, false, err
		}
	}
	action := meta.AuditAction
	if action == "" {
		action = "release." + string(ev.Kind)
	}
	detail := map[string]any{"from": string(old.State), "to": string(nr.State), "active_ring": nr.ActiveRing, "state_version": nr.StateVersion}
	if ev.Override {
		detail["override"] = true
	}
	if ev.Kind == rollout.EvGateLook {
		detail["verdict"] = string(ev.Verdict)
	}
	for k, v := range meta.Detail {
		detail[k] = v
	}
	reason := ev.Reason
	if reason == "" {
		reason = nr.Reason
	}
	if err := insertAudit(ctx, tx, Audit{Actor: meta.Actor, Action: action, Config: rel.Config, Environment: rel.Env, Release: rel.ID,
		Generation: nt.Generation, Epoch: meta.Epoch, Reason: reason, Detail: detail}); err != nil {
		return nil, false, err
	}
	if nt.Generation != oldGen {
		if err := notify(ctx, tx, rel.Config, rel.Env); err != nil {
			return nil, false, err
		}
	}
	return rel, false, nil
}

func updateRings(rel *ReleaseRow, old rollout.Release, ev rollout.Event, meta EventMeta, now time.Time) {
	n := rel.R
	at := func(i int32) *RingProgress {
		if i >= 0 && int(i) < len(rel.Rings) {
			return &rel.Rings[i]
		}
		return &RingProgress{}
	}
	switch {
	case n.State == rollout.RollingBack || n.State == rollout.RolledBack:
		for i := range rel.Rings {
			if int32(i) <= rel.ExposedRing {
				rel.Rings[i].Phase = "rolled_back"
			}
		}
	case n.State == rollout.Complete:
		for i := range rel.Rings {
			rel.Rings[i].Phase = "passed"
		}
	case n.State == rollout.Rolling && (n.ActiveRing != old.ActiveRing || old.State == rollout.Halted || old.State == rollout.Pending):
		for i := int32(0); i < n.ActiveRing; i++ {
			at(i).Phase = "passed"
		}
		*at(n.ActiveRing) = RingProgress{Index: n.ActiveRing, Phase: "rolling", Generation: n.RingGeneration, Started: &now}
	case n.State == rollout.Baking && old.State == rollout.Rolling:
		r := at(n.ActiveRing)
		r.Phase, r.BakeStarted = "baking", &now
	}
	if ev.Kind == rollout.EvGateLook {
		r := at(old.ActiveRing)
		r.LooksDone, r.Verdict = old.LooksDone+1, string(ev.Verdict)
		if meta.Gate != nil {
			r.Reason = truncate(meta.Gate.Reason, 300)
		}
	}
	if ev.Kind == rollout.EvResume && n.State == rollout.Baking && old.Paused {
		r := at(n.ActiveRing)
		r.BakeStarted, r.LooksDone, r.Verdict, r.Reason = &now, 0, "", ""
	}
}

func (s *Store) persistRelease(ctx context.Context, tx pgx.Tx, r *ReleaseRow) error {
	rings, err := json.Marshal(r.Rings)
	if err != nil {
		return err
	}
	var rb []byte
	if r.RollbackSummary != nil {
		if rb, err = pjson.Marshal(r.RollbackSummary); err != nil {
			return err
		}
	}
	_, err = tx.Exec(ctx, `UPDATE releases SET state=$2, paused=$3, active_ring=$4, exposed_ring=$5, reason=$6, message=$7,
		state_version=$8, epoch=$9, first_generation=$10, ring_generation=$11, rollback_generation=$12,
		ring_started_at=$13, bake_started_at=$14, last_look_at=$15, rollback_started_at=$16, looks_done=$17,
		gate_passed=$18, override_used=$19, rollback_converged=$20, rings=$21, rollback_summary=$22,
		finished_at=$23, updated_at=now() WHERE id=$1`,
		r.ID, string(r.R.State), r.R.Paused, r.R.ActiveRing, r.ExposedRing, r.R.Reason, r.Message, r.R.StateVersion, r.Epoch,
		r.FirstGeneration, r.R.RingGeneration, r.R.RollbackGeneration, tp(r.R.RingStarted), tp(r.R.BakeStarted),
		tp(r.R.LastLook), tp(r.R.RollbackStarted), r.R.LooksDone, r.R.GatePassed, r.R.OverrideUsed, r.R.RollbackConverged,
		rings, rb, r.Finished)
	return err
}

// KillSwitch returns a target to its previous known-good state and freezes
// it. With an active release it rolls that release back; without one it
// reverts the baseline and records an emergency release to track convergence.
func (s *Store) KillSwitch(ctx context.Context, config, env, reason, actor, requestID string, membershipWindow time.Duration) (rel *ReleaseRow, tgt *TargetRow, noop bool, err error) {
	err = s.inTx(ctx, func(tx pgx.Tx) error {
		noop, rel = false, nil
		_, seen, err := idempotent(ctx, tx, actor, requestID, "KillSwitch")
		if err != nil {
			return err
		}
		tgt, err = lockTarget(ctx, tx, config, env)
		if err != nil {
			return err
		}
		active, err := scanRelease(tx.QueryRow(ctx, `SELECT `+releaseCols+` FROM releases
			WHERE config = $1 AND environment = $2 AND state IN `+activeStates+` FOR UPDATE`, config, env))
		if err != nil && !errors.Is(err, ErrNotFound) {
			return err
		}
		if seen {
			rel, noop = active, true
			return nil
		}
		meta := EventMeta{Actor: actor, AuditAction: "target.kill_switch", Detail: map[string]any{"break_glass": true}}
		if active != nil {
			rel, noop, err = s.applyLocked(ctx, tx, tgt, active, rollout.Event{Kind: rollout.EvKill, Reason: reason}, meta)
			if err != nil {
				return err
			}
			if noop {
				return nil
			}
			return remember(ctx, tx, actor, requestID, "KillSwitch", rel.ID)
		}
		var now time.Time
		if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
			return err
		}
		er, nt, err := rollout.KillWithoutRelease(tgt.T, reason, now)
		if errors.Is(err, rollout.ErrNoop) {
			noop = true
			return nil
		}
		if err != nil {
			return mapRolloutErr(err)
		}
		cfg, err := getConfig(ctx, tx, config)
		if err != nil {
			return err
		}
		pol, err := policy.Normalize(config, env, cfg.Spec.Environments[env])
		if err != nil {
			// The emergency path must not depend on a valid rollout policy.
			pol, _ = policy.Normalize(config, env, &pb.RolloutPolicy{Rings: []*pb.Ring{{Name: "all", CumulativeBps: 10000}}})
		}
		polJSON, err := pjson.Marshal(pol)
		if err != nil {
			return err
		}
		id := newReleaseID()
		rings, _ := json.Marshal([]RingProgress{{Index: 0, Phase: "rolled_back"}})
		if _, err := tx.Exec(ctx, `INSERT INTO releases (id, config, environment, version, baseline_version, kind, state,
			active_ring, exposed_ring, reason, state_version, rollback_generation, rollback_started_at, policy, rings, created_by)
			VALUES ($1,$2,$3,$4,$5,'emergency','ROLLING_BACK',-1,0,$6,1,$7,$8,$9,$10,$11)`,
			id, config, env, er.Candidate, er.Baseline, truncate(reason, 300), er.RollbackGeneration, now, polJSON, rings, actor); err != nil {
			return err
		}
		ids, err := members(ctx, tx, env, membershipWindow)
		if err != nil {
			return err
		}
		zeros := make([]int32, len(ids))
		if _, err := tx.Exec(ctx, `INSERT INTO release_members (release_id, instance_id, ring)
			SELECT $1, * FROM unnest($2::text[], $3::int[])`, id, ids, zeros); err != nil {
			return err
		}
		tgt.T, tgt.ReleaseID, tgt.FrozenReason = nt, id, truncate(reason, 300)
		if err := s.persistTarget(ctx, tx, tgt); err != nil {
			return err
		}
		if err := insertAudit(ctx, tx, Audit{Actor: actor, Action: "target.kill_switch", Config: config, Environment: env, Release: id,
			Generation: nt.Generation, Reason: reason,
			Detail: map[string]any{"break_glass": true, "from_version": er.Candidate, "to_version": er.Baseline, "members": len(ids)}}); err != nil {
			return err
		}
		if err := notify(ctx, tx, config, env); err != nil {
			return err
		}
		if err := remember(ctx, tx, actor, requestID, "KillSwitch", id); err != nil {
			return err
		}
		rel, err = scanRelease(tx.QueryRow(ctx, `SELECT `+releaseCols+` FROM releases WHERE id = $1`, id))
		return err
	})
	return rel, tgt, noop, err
}

// Unfreeze lifts a kill-switch freeze.
func (s *Store) Unfreeze(ctx context.Context, config, env, reason, actor string) (*pb.Target, error) {
	var out *pb.Target
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		tgt, err := lockTarget(ctx, tx, config, env)
		if err != nil {
			return err
		}
		if tgt.T.Frozen {
			tgt.T.Frozen, tgt.FrozenReason = false, ""
			if err := s.persistTarget(ctx, tx, tgt); err != nil {
				return err
			}
			if err := insertAudit(ctx, tx, Audit{Actor: actor, Action: "target.unfreeze", Config: config, Environment: env,
				Generation: tgt.T.Generation, Reason: reason}); err != nil {
				return err
			}
		}
		out = tgt.ProtoTarget()
		return nil
	})
	return out, err
}

// RingCounts are acknowledgement counts for members of exactly one ring.
type RingCounts struct {
	Ring int32
	C    rollout.Counts
	// Rollback convergence for this ring's members.
	Confirmed, PendingRB, UnreachableRB int32
}

// Counts returns per-ring acknowledgement counts for a release.
func (s *Store) Counts(ctx context.Context, r *ReleaseRow) ([]RingCounts, error) {
	window := r.Policy.GetReachableWindow().AsDuration().Seconds()
	rows, err := s.Pool.Query(ctx, `
		WITH m AS (
			SELECT m.ring,
			       COALESCE(i.last_seen > now() - make_interval(secs => $4), false) AS reachable,
			       COALESCE(s.generation >= $5 AND s.version = $6, false)            AS current,
			       s.state,
			       COALESCE(s.generation >= $7 AND s.serving_version = $8, false)    AS confirmed,
			       EXISTS (SELECT 1 FROM instance_incidents x WHERE x.environment = $2 AND x.config = $3 AND x.version = $6
			               AND x.instance_id = m.instance_id AND x.kind = 'REJECTED' AND x.generation >= $9)   AS rejected,
			       EXISTS (SELECT 1 FROM instance_incidents x WHERE x.environment = $2 AND x.config = $3 AND x.version = $6
			               AND x.instance_id = m.instance_id AND x.kind = 'CRASH_LOOP' AND x.generation >= $9) AS crashed
			FROM release_members m
			LEFT JOIN instances i ON i.environment = $2 AND i.instance_id = m.instance_id
			LEFT JOIN instance_status s ON s.environment = $2 AND s.config = $3 AND s.instance_id = m.instance_id
			WHERE m.release_id = $1)
		SELECT ring, count(*), count(*) FILTER (WHERE reachable),
		       count(*) FILTER (WHERE current AND state IN ('APPLIED','ACCEPTED')),
		       count(*) FILTER (WHERE reachable AND current AND state = 'ACCEPTED'),
		       count(*) FILTER (WHERE rejected), count(*) FILTER (WHERE current AND state = 'FAILED'),
		       count(*) FILTER (WHERE crashed),
		       count(*) FILTER (WHERE confirmed), count(*) FILTER (WHERE reachable AND NOT confirmed),
		       count(*) FILTER (WHERE NOT reachable AND NOT confirmed)
		FROM m GROUP BY ring ORDER BY ring`,
		r.ID, r.Env, r.Config, window, r.R.RingGeneration, r.R.Candidate,
		max64(r.R.RollbackGeneration, 1), r.R.Baseline, r.FirstGeneration)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []RingCounts
	for rows.Next() {
		var c RingCounts
		if err := rows.Scan(&c.Ring, &c.C.Eligible, &c.C.Reachable, &c.C.Applied, &c.C.Accepted, &c.C.Rejected, &c.C.Failed,
			&c.C.CrashLoop, &c.Confirmed, &c.PendingRB, &c.UnreachableRB); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// Summarize folds per-ring counts into what the state machine needs: counts
// over every exposed ring, and rollback convergence over every ring that was
// ever exposed.
func Summarize(r *ReleaseRow, rings []RingCounts) (rollout.Counts, rollout.RollbackCounts) {
	var c rollout.Counts
	var rb rollout.RollbackCounts
	for _, rc := range rings {
		if rc.Ring != ring.Holdout && rc.Ring <= r.R.ActiveRing {
			c.Eligible += rc.C.Eligible
			c.Reachable += rc.C.Reachable
			c.Applied += rc.C.Applied
			c.Accepted += rc.C.Accepted
			c.Failed += rc.C.Failed
		}
		if rc.Ring != ring.Holdout && rc.Ring <= r.ExposedRing {
			// Incidents count for every ring that ever saw the candidate.
			c.Rejected += rc.C.Rejected
			c.CrashLoop += rc.C.CrashLoop
			rb.Exposed += rc.C.Eligible
			rb.Confirmed += rc.Confirmed
			rb.Pending += rc.PendingRB
			rb.Unreachable += rc.UnreachableRB
		}
	}
	return c, rb
}

// UnconfirmedSample lists a few exposed members that have not confirmed a
// rollback, split by reachability.
func (s *Store) UnconfirmedSample(ctx context.Context, r *ReleaseRow, limit int) (unreachable []string, err error) {
	window := r.Policy.GetReachableWindow().AsDuration().Seconds()
	rows, err := s.Pool.Query(ctx, `
		SELECT m.instance_id FROM release_members m
		LEFT JOIN instances i ON i.environment = $2 AND i.instance_id = m.instance_id
		LEFT JOIN instance_status s ON s.environment = $2 AND s.config = $3 AND s.instance_id = m.instance_id
		WHERE m.release_id = $1 AND m.ring <= $4
		  AND NOT COALESCE(s.generation >= $5 AND s.serving_version = $6, false)
		  AND NOT COALESCE(i.last_seen > now() - make_interval(secs => $7), false)
		ORDER BY m.instance_id LIMIT $8`,
		r.ID, r.Env, r.Config, r.ExposedRing, max64(r.R.RollbackGeneration, 1), r.R.Baseline, window, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

// ListReleases pages through releases, newest first.
func (s *Store) ListReleases(ctx context.Context, config, env string, activeOnly bool, pageSize int, before time.Time) ([]*ReleaseRow, time.Time, error) {
	pageSize = clampPage(pageSize)
	if before.IsZero() {
		before = time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC)
	}
	q := `SELECT ` + releaseCols + ` FROM releases WHERE ($1 = '' OR config = $1) AND ($2 = '' OR environment = $2) AND created_at < $3`
	if activeOnly {
		q += ` AND state IN ` + activeStates
	}
	rows, err := s.Pool.Query(ctx, q+` ORDER BY created_at DESC LIMIT $4`, config, env, before, pageSize+1)
	if err != nil {
		return nil, time.Time{}, err
	}
	defer rows.Close()
	var out []*ReleaseRow
	for rows.Next() {
		r, err := scanRelease(rows)
		if err != nil {
			return nil, time.Time{}, err
		}
		out = append(out, r)
	}
	var next time.Time
	if len(out) > pageSize {
		out = out[:pageSize]
		next = out[pageSize-1].Created
	}
	return out, next, rows.Err()
}

// EnsureMember adds a late joiner to an active release's member list.
func (s *Store) EnsureMember(ctx context.Context, releaseID, instanceID string, ringIdx int32) error {
	_, err := s.Pool.Exec(ctx, `INSERT INTO release_members (release_id, instance_id, ring, late)
		SELECT $1, $2, $3, true WHERE EXISTS (SELECT 1 FROM releases WHERE id = $1 AND state IN `+activeStates+`)
		ON CONFLICT DO NOTHING`, releaseID, instanceID, ringIdx)
	return err
}
