package store

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jackc/pgx/v5"

	pb "github.com/raunaksahahere/cfgcanary/api/gen/cfgcanary/v1"
)

// UpsertInstance records that an instance exists and was just seen.
func (s *Store) UpsertInstance(ctx context.Context, env, id, group string, labels map[string]string, agentVersion string) error {
	if labels == nil {
		labels = map[string]string{}
	}
	lj, err := json.Marshal(labels)
	if err != nil {
		return err
	}
	_, err = s.Pool.Exec(ctx, `INSERT INTO instances (environment, instance_id, grp, labels, agent_version)
		VALUES ($1,$2,$3,$4,$5)
		ON CONFLICT (environment, instance_id) DO UPDATE
		SET grp = EXCLUDED.grp, labels = EXCLUDED.labels, agent_version = EXCLUDED.agent_version, last_seen = now()`,
		env, id, group, lj, truncate(agentVersion, 64))
	return err
}

// TouchInstances refreshes last_seen for connected instances in one statement.
func (s *Store) TouchInstances(ctx context.Context, env string, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	_, err := s.Pool.Exec(ctx, `UPDATE instances SET last_seen = now() WHERE environment = $1 AND instance_id = ANY($2)`, env, ids)
	return err
}

// StatusRow is one acknowledgement.
type StatusRow struct {
	Env, Instance, Config string
	Generation, Version   int64
	State, Reason         string
	ServingVersion        int64
	ServingGeneration     int64
}

func rank(state string) int {
	switch state {
	case "ACCEPTED", "REJECTED":
		return 2
	case "CRASH_LOOP":
		return 3
	}
	return 1 // APPLIED, FAILED
}

// UpsertStatuses stores a batch of acknowledgements. A row replaces the stored
// one only if its generation is higher, or equal with the same or a later
// lifecycle state, so late or duplicated reports cannot move status backwards
// (docs/invariants.md I1). SDK rejections and crash loops are also recorded as
// incidents that outlive later status updates.
func (s *Store) UpsertStatuses(ctx context.Context, batch []StatusRow) error {
	if len(batch) == 0 {
		return nil
	}
	// One row per key per statement: keep the newest.
	type key struct{ env, inst, cfg string }
	best := map[key]StatusRow{}
	var incidents []StatusRow
	for _, r := range batch {
		if r.State == "REJECTED" || r.State == "CRASH_LOOP" {
			incidents = append(incidents, r)
		}
		k := key{r.Env, r.Instance, r.Config}
		cur, ok := best[k]
		if !ok || r.Generation > cur.Generation || (r.Generation == cur.Generation && rank(r.State) >= rank(cur.State)) {
			best[k] = r
		}
	}
	n := len(best)
	envs, insts, cfgs := make([]string, 0, n), make([]string, 0, n), make([]string, 0, n)
	gens, vers, sv, sg := make([]int64, 0, n), make([]int64, 0, n), make([]int64, 0, n), make([]int64, 0, n)
	states, reasons, ranks := make([]string, 0, n), make([]string, 0, n), make([]int32, 0, n)
	for _, r := range best {
		envs, insts, cfgs = append(envs, r.Env), append(insts, r.Instance), append(cfgs, r.Config)
		gens, vers = append(gens, r.Generation), append(vers, r.Version)
		sv, sg = append(sv, r.ServingVersion), append(sg, r.ServingGeneration)
		states, reasons, ranks = append(states, r.State), append(reasons, truncate(r.Reason, 200)), append(ranks, int32(rank(r.State)))
	}
	return s.inTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `
			INSERT INTO instance_status AS s (environment, instance_id, config, generation, version, state, reason, serving_version, serving_generation)
			SELECT e, i, c, g, v, st, rs, sv, sg
			FROM unnest($1::text[], $2::text[], $3::text[], $4::bigint[], $5::bigint[], $6::text[], $7::text[], $8::bigint[], $9::bigint[], $10::int[])
			     AS u(e, i, c, g, v, st, rs, sv, sg, rk)
			ORDER BY e, c, i
			ON CONFLICT (environment, config, instance_id) DO UPDATE
			SET generation = EXCLUDED.generation, version = EXCLUDED.version, state = EXCLUDED.state, reason = EXCLUDED.reason,
			    serving_version = EXCLUDED.serving_version, serving_generation = EXCLUDED.serving_generation, updated_at = now()
			WHERE EXCLUDED.generation > s.generation
			   OR (EXCLUDED.generation = s.generation AND
			       (CASE EXCLUDED.state WHEN 'ACCEPTED' THEN 2 WHEN 'REJECTED' THEN 2 WHEN 'CRASH_LOOP' THEN 3 ELSE 1 END) >=
			       (CASE s.state WHEN 'ACCEPTED' THEN 2 WHEN 'REJECTED' THEN 2 WHEN 'CRASH_LOOP' THEN 3 ELSE 1 END))`,
			envs, insts, cfgs, gens, vers, states, reasons, sv, sg, ranks); err != nil {
			return err
		}
		for _, r := range incidents {
			if _, err := tx.Exec(ctx, `INSERT INTO instance_incidents (environment, config, version, instance_id, kind, generation, reason)
				VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT (environment, config, version, instance_id, kind)
				DO UPDATE SET generation = GREATEST(instance_incidents.generation, EXCLUDED.generation)`,
				r.Env, r.Config, r.Version, r.Instance, r.State, r.Generation, truncate(r.Reason, 200)); err != nil {
				return err
			}
		}
		return nil
	})
}

// ListInstances pages through instances of an environment with their statuses.
func (s *Store) ListInstances(ctx context.Context, env string, pageSize int, afterEnv, after string, connectedWindow time.Duration) ([]*pb.Instance, string, error) {
	pageSize = clampPage(pageSize)
	rows, err := s.Pool.Query(ctx, `SELECT environment, instance_id, grp, labels, agent_version, last_seen,
			last_seen > now() - make_interval(secs => $4)
		FROM instances WHERE ($1 = '' OR environment = $1) AND (environment, instance_id) > ($5, $2)
		ORDER BY environment, instance_id LIMIT $3`, env, after, pageSize+1, connectedWindow.Seconds(), afterEnv)
	if err != nil {
		return nil, "", err
	}
	var out []*pb.Instance
	index := map[string]*pb.Instance{}
	var ids []string
	for rows.Next() {
		var in pb.Instance
		var labels []byte
		var seen time.Time
		if err := rows.Scan(&in.Environment, &in.InstanceId, &in.Group, &labels, &in.AgentVersion, &seen, &in.Connected); err != nil {
			rows.Close()
			return nil, "", err
		}
		_ = json.Unmarshal(labels, &in.Labels)
		in.LastSeen = ts(seen)
		out = append(out, &in)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	next := ""
	if len(out) > pageSize {
		out = out[:pageSize]
		next = out[pageSize-1].Environment + "/" + out[pageSize-1].InstanceId
	}
	for _, in := range out {
		index[in.Environment+"\x00"+in.InstanceId] = in
		ids = append(ids, in.InstanceId)
	}
	srows, err := s.Pool.Query(ctx, `SELECT environment, instance_id, config, generation, version, state, reason, serving_version, serving_generation
		FROM instance_status WHERE ($1 = '' OR environment = $1) AND instance_id = ANY($2) ORDER BY config`, env, ids)
	if err != nil {
		return nil, "", err
	}
	defer srows.Close()
	for srows.Next() {
		var e, id, state string
		st := &pb.InstanceConfigStatus{}
		if err := srows.Scan(&e, &id, &st.Config, &st.Generation, &st.Version, &state, &st.Reason, &st.ServingVersion, &st.ServingGeneration); err != nil {
			return nil, "", err
		}
		st.State = pb.ApplyState(pb.ApplyState_value["APPLY_STATE_"+state])
		if in := index[e+"\x00"+id]; in != nil {
			in.Statuses = append(in.Statuses, st)
		}
	}
	return out, next, srows.Err()
}

// ListAudit pages through audit events, newest first.
func (s *Store) ListAudit(ctx context.Context, config, env, release, action string, pageSize int, before int64) ([]*pb.AuditEvent, int64, error) {
	pageSize = clampPage(pageSize)
	if before <= 0 {
		before = 1<<62 - 1
	}
	rows, err := s.Pool.Query(ctx, `SELECT id, at, actor, action, outcome, config, environment, release_id, generation, epoch, reason, detail
		FROM audit_events WHERE id < $1 AND ($2 = '' OR config = $2) AND ($3 = '' OR environment = $3)
		AND ($4 = '' OR release_id = $4) AND ($5 = '' OR action = $5) ORDER BY id DESC LIMIT $6`,
		before, config, env, release, action, pageSize+1)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	var out []*pb.AuditEvent
	for rows.Next() {
		var e pb.AuditEvent
		var at time.Time
		var detail []byte
		if err := rows.Scan(&e.Id, &at, &e.Actor, &e.Action, &e.Outcome, &e.Config, &e.Environment, &e.ReleaseId, &e.Generation, &e.Epoch, &e.Reason, &detail); err != nil {
			return nil, 0, err
		}
		e.Time, e.Detail = ts(at), string(detail)
		out = append(out, &e)
	}
	var next int64
	if len(out) > pageSize {
		out = out[:pageSize]
		next = out[pageSize-1].Id
	}
	return out, next, rows.Err()
}

// GateEvaluations returns the evidence recorded for a release, oldest first.
func (s *Store) GateEvaluations(ctx context.Context, releaseID string) ([]GateRecord, error) {
	rows, err := s.Pool.Query(ctx, `SELECT ring, look, verdict, reason, evidence FROM gate_evaluations WHERE release_id = $1 ORDER BY id`, releaseID)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (GateRecord, error) {
		var g GateRecord
		var ev []byte
		err := r.Scan(&g.Ring, &g.Look, &g.Verdict, &g.Reason, &ev)
		g.Evidence = ev
		return g, err
	})
}
