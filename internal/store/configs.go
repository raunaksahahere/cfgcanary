package store

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/raunaksahahere/cfgcanary/api/gen/cfgcanary/v1"
	"github.com/raunaksahahere/cfgcanary/internal/sign"
)

var (
	pjson   = protojson.MarshalOptions{UseProtoNames: true}
	pjsonUn = protojson.UnmarshalOptions{DiscardUnknown: true}
)

func ts(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t)
}

// UpsertConfig creates or updates a config's spec and makes sure a target row
// exists for each of its environments. The WASM module bytes are never stored
// here; the spec only carries the module digest.
func (s *Store) UpsertConfig(ctx context.Context, spec *pb.ConfigSpec, expectedRevision int64, actor string) (*pb.Config, error) {
	spec = proto.Clone(spec).(*pb.ConfigSpec)
	if w := spec.GetValidation().GetWasm(); w != nil {
		w.Module = nil
	}
	raw, err := pjson.Marshal(spec)
	if err != nil {
		return nil, err
	}
	var out *pb.Config
	err = s.inTx(ctx, func(tx pgx.Tx) error {
		var rev int64
		err := tx.QueryRow(ctx, `SELECT spec_revision FROM configs WHERE name = $1 FOR UPDATE`, spec.Name).Scan(&rev)
		action := "config.update"
		switch {
		case errors.Is(err, pgx.ErrNoRows):
			if expectedRevision > 0 {
				return wrap(ErrPrecondition, "config %q does not exist", spec.Name)
			}
			action = "config.register"
			if _, err := tx.Exec(ctx, `INSERT INTO configs (name, spec) VALUES ($1, $2)`, spec.Name, raw); err != nil {
				return err
			}
			rev = 1
		case err != nil:
			return err
		default:
			if expectedRevision > 0 && expectedRevision != rev {
				return wrap(ErrPrecondition, "config %q is at revision %d, expected %d", spec.Name, rev, expectedRevision)
			}
			// An environment with an active release cannot be removed.
			rows, err := tx.Query(ctx, `SELECT DISTINCT environment FROM releases
				WHERE config = $1 AND state IN ('PENDING','ROLLING','BAKING','HALTED','ROLLING_BACK')`, spec.Name)
			if err != nil {
				return err
			}
			active, err := pgx.CollectRows(rows, pgx.RowTo[string])
			if err != nil {
				return err
			}
			for _, env := range active {
				if _, ok := spec.Environments[env]; !ok {
					return wrap(ErrPrecondition, "environment %q has an active release and cannot be removed", env)
				}
			}
			rev++
			if _, err := tx.Exec(ctx, `UPDATE configs SET spec = $2, spec_revision = $3, updated_at = now() WHERE name = $1`, spec.Name, raw, rev); err != nil {
				return err
			}
		}
		for env := range spec.Environments {
			if _, err := tx.Exec(ctx, `INSERT INTO targets (config, environment) VALUES ($1, $2) ON CONFLICT DO NOTHING`, spec.Name, env); err != nil {
				return err
			}
		}
		if err := insertAudit(ctx, tx, Audit{Actor: actor, Action: action, Config: spec.Name, Detail: map[string]any{"spec_revision": rev}}); err != nil {
			return err
		}
		out, err = getConfig(ctx, tx, spec.Name)
		return err
	})
	return out, err
}

type querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

func scanConfig(row pgx.Row) (*pb.Config, error) {
	var raw []byte
	var c pb.Config
	var created, updated time.Time
	if err := row.Scan(&raw, &c.SpecRevision, &c.LatestVersion, &created, &updated); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	c.Spec = &pb.ConfigSpec{}
	if err := pjsonUn.Unmarshal(raw, c.Spec); err != nil {
		return nil, fmt.Errorf("stored config spec is unreadable: %w", err)
	}
	c.Created, c.Updated = ts(created), ts(updated)
	return &c, nil
}

func getConfig(ctx context.Context, q querier, name string) (*pb.Config, error) {
	c, err := scanConfig(q.QueryRow(ctx, `SELECT spec, spec_revision, version_seq, created_at, updated_at FROM configs WHERE name = $1`, name))
	if errors.Is(err, ErrNotFound) {
		return nil, wrap(ErrNotFound, "config %q", name)
	}
	return c, err
}

// GetConfig returns one config.
func (s *Store) GetConfig(ctx context.Context, name string) (*pb.Config, error) {
	return getConfig(ctx, s.Pool, name)
}

// ListConfigs pages through configs by name.
func (s *Store) ListConfigs(ctx context.Context, pageSize int, after string) ([]*pb.Config, string, error) {
	pageSize = clampPage(pageSize)
	rows, err := s.Pool.Query(ctx, `SELECT spec, spec_revision, version_seq, created_at, updated_at
		FROM configs WHERE name > $1 ORDER BY name LIMIT $2`, after, pageSize+1)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	var out []*pb.Config
	for rows.Next() {
		c, err := scanConfig(rows)
		if err != nil {
			return nil, "", err
		}
		out = append(out, c)
	}
	next := ""
	if len(out) > pageSize {
		out = out[:pageSize]
		next = out[pageSize-1].Spec.Name
	}
	return out, next, rows.Err()
}

func clampPage(n int) int {
	if n <= 0 {
		return 50
	}
	if n > 500 {
		return 500
	}
	return n
}

// BlobBegin registers the intent to upload an object, or refreshes an
// existing row so the collector leaves it alone. It blocks while the
// collector is deleting the same digest. It reports whether the object is
// already committed.
func (s *Store) BlobBegin(ctx context.Context, sha string, size int64, kind string) (committed bool, err error) {
	var state string
	err = s.Pool.QueryRow(ctx, `INSERT INTO blobs (sha256, size, kind, state) VALUES ($1, $2, $3, 'pending')
		ON CONFLICT (sha256) DO UPDATE SET touched_at = now() RETURNING state`, sha, size, kind).Scan(&state)
	return state == "committed", err
}

// BlobCommit marks an uploaded object as committed (used for WASM modules;
// payload blobs are committed with their version).
func (s *Store) BlobCommit(ctx context.Context, sha string) error {
	tag, err := s.Pool.Exec(ctx, `UPDATE blobs SET state = 'committed', touched_at = now() WHERE sha256 = $1`, sha)
	if err == nil && tag.RowsAffected() == 0 {
		return wrap(ErrConflict, "blob %s disappeared before it was committed; retry", sha[:12])
	}
	return err
}

const versionCols = `config, version, sha256, size, description, environments, created_by, created_at,
	artifact_payload, artifact_signature, artifact_key_id, purged_at`

func scanVersion(row pgx.Row) (*pb.ConfigVersion, error) {
	var v pb.ConfigVersion
	var created time.Time
	var purged *time.Time
	a := &pb.SignedStatement{}
	if err := row.Scan(&v.Config, &v.Version, &v.Sha256, &v.Size, &v.Description, &v.Environments, &v.CreatedBy, &created,
		&a.Payload, &a.Signature, &a.KeyId, &purged); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	v.Created, v.Artifact, v.Purged = ts(created), a, purged != nil
	return &v, nil
}

// InsertVersion allocates the next version number, signs the artifact
// statement for it and stores the version, all in one transaction. The blob
// must already be uploaded (publish before reference).
func (s *Store) InsertVersion(ctx context.Context, config, sha string, size int64, description string, envs []string,
	actor, requestID string, signer func(sign.Artifact) (sign.Signed, error)) (*pb.ConfigVersion, error) {
	var out *pb.ConfigVersion
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		ref, seen, err := idempotent(ctx, tx, actor, requestID, "SubmitVersion")
		if err != nil {
			return err
		}
		if seen {
			n, _ := strconv.ParseInt(ref, 10, 64)
			out, err = getVersion(ctx, tx, config, n)
			return err
		}
		var version int64
		err = tx.QueryRow(ctx, `UPDATE configs SET version_seq = version_seq + 1 WHERE name = $1 RETURNING version_seq`, config).Scan(&version)
		if errors.Is(err, pgx.ErrNoRows) {
			return wrap(ErrNotFound, "config %q", config)
		}
		if err != nil {
			return err
		}
		tag, err := tx.Exec(ctx, `UPDATE blobs SET state = 'committed', touched_at = now() WHERE sha256 = $1`, sha)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return wrap(ErrConflict, "blob %s disappeared before it was referenced; retry the submission", sha[:12])
		}
		signed, err := signer(sign.Artifact{Config: config, Version: version, SHA256: sha, Size: size, Environments: envs})
		if err != nil {
			return fmt.Errorf("signing artifact: %w", err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO config_versions
			(config, version, sha256, size, description, environments, created_by, artifact_payload, artifact_signature, artifact_key_id)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
			config, version, sha, size, truncate(description, 500), envs, actor, signed.Payload, signed.Signature, signed.KeyID); err != nil {
			return err
		}
		if err := insertAudit(ctx, tx, Audit{Actor: actor, Action: "version.submit", Config: config,
			Detail: map[string]any{"version": version, "sha256": sha, "size": size, "environments": envs}}); err != nil {
			return err
		}
		if err := remember(ctx, tx, actor, requestID, "SubmitVersion", strconv.FormatInt(version, 10)); err != nil {
			return err
		}
		out, err = getVersion(ctx, tx, config, version)
		return err
	})
	return out, err
}

func getVersion(ctx context.Context, q querier, config string, version int64) (*pb.ConfigVersion, error) {
	v, err := scanVersion(q.QueryRow(ctx, `SELECT `+versionCols+` FROM config_versions WHERE config = $1 AND version = $2`, config, version))
	if errors.Is(err, ErrNotFound) {
		return nil, wrap(ErrNotFound, "version %d of config %q", version, config)
	}
	return v, err
}

// GetVersion returns one version.
func (s *Store) GetVersion(ctx context.Context, config string, version int64) (*pb.ConfigVersion, error) {
	return getVersion(ctx, s.Pool, config, version)
}

// ListVersions pages through versions, newest first.
func (s *Store) ListVersions(ctx context.Context, config string, pageSize int, before int64) ([]*pb.ConfigVersion, int64, error) {
	pageSize = clampPage(pageSize)
	if before <= 0 {
		before = 1<<62 - 1
	}
	rows, err := s.Pool.Query(ctx, `SELECT `+versionCols+` FROM config_versions
		WHERE config = $1 AND version < $2 ORDER BY version DESC LIMIT $3`, config, before, pageSize+1)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	var out []*pb.ConfigVersion
	for rows.Next() {
		v, err := scanVersion(rows)
		if err != nil {
			return nil, 0, err
		}
		out = append(out, v)
	}
	var next int64
	if len(out) > pageSize {
		out = out[:pageSize]
		next = out[pageSize-1].Version
	}
	return out, next, rows.Err()
}

// GCStats reports what a collection pass did.
type GCStats struct {
	VersionsPurged int
	BlobsDeleted   int
	BlobsKept      int
	Errors         int
}

// GC purges versions beyond each config's retention that nothing references,
// then deletes blobs no version or config references.
//
// Protected: versions referenced by a target (baseline, candidate, previous
// known-good), by any active release (candidate or baseline), the newest
// retain_versions of each config, and anything younger than grace. Uploads in
// flight are protected by their blob row's touched_at.
//
// Races (docs/invariants.md): release creation takes FOR SHARE on its version
// row and re-reads purged_at; the collector takes FOR UPDATE on candidates and
// re-checks references in a later statement, so whichever commits first wins
// and the other observes it.
func (s *Store) GC(ctx context.Context, defaultRetain int, grace time.Duration, deleteObject func(ctx context.Context, sha string) error) (GCStats, error) {
	var st GCStats
	if defaultRetain < 1 {
		defaultRetain = 1
	}
	const unreferenced = `
		NOT EXISTS (SELECT 1 FROM targets t WHERE t.config = v.config
			AND v.version IN (t.baseline_version, t.candidate_version, t.previous_good_version))
		AND NOT EXISTS (SELECT 1 FROM releases r WHERE r.config = v.config
			AND r.state IN ('PENDING','ROLLING','BAKING','HALTED','ROLLING_BACK')
			AND v.version IN (r.version, r.baseline_version))`
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		st.VersionsPurged = 0
		// Statement 1: lock candidates. Blocks behind concurrent release
		// creation holding FOR SHARE on the same rows.
		rows, err := tx.Query(ctx, `
			SELECT v.config, v.version FROM config_versions v JOIN configs c ON c.name = v.config
			WHERE v.purged_at IS NULL AND v.created_at < now() - make_interval(secs => $2)
			  AND v.version <= c.version_seq - GREATEST(COALESCE(NULLIF((c.spec->>'retain_versions')::int, 0), $1), 1)
			  AND `+unreferenced+`
			ORDER BY v.config, v.version FOR UPDATE OF v`, defaultRetain, grace.Seconds())
		if err != nil {
			return err
		}
		type key struct {
			config  string
			version int64
		}
		cands, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (key, error) {
			var k key
			return k, r.Scan(&k.config, &k.version)
		})
		if err != nil {
			return err
		}
		// Statement 2: a fresh snapshot re-checks references now that the
		// locks are held.
		for _, k := range cands {
			tag, err := tx.Exec(ctx, `UPDATE config_versions v SET purged_at = now()
				WHERE v.config = $1 AND v.version = $2 AND v.purged_at IS NULL AND `+unreferenced, k.config, k.version)
			if err != nil {
				return err
			}
			st.VersionsPurged += int(tag.RowsAffected())
		}
		if st.VersionsPurged > 0 {
			return insertAudit(ctx, tx, Audit{Actor: "controller", Action: "gc.purge_versions", Detail: map[string]any{"count": st.VersionsPurged}})
		}
		return nil
	})
	if err != nil {
		return st, err
	}

	const blobUnreferenced = `
		NOT EXISTS (SELECT 1 FROM config_versions v WHERE v.sha256 = b.sha256 AND v.purged_at IS NULL)
		AND NOT EXISTS (SELECT 1 FROM configs c WHERE c.spec #>> '{validation,wasm,sha256}' = b.sha256)
		AND b.touched_at < now() - make_interval(secs => $1)`
	rows, err := s.Pool.Query(ctx, `SELECT b.sha256 FROM blobs b WHERE `+blobUnreferenced+` LIMIT 1000`, grace.Seconds())
	if err != nil {
		return st, err
	}
	shas, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return st, err
	}
	for _, sha := range shas {
		deleted := false
		err := s.inTx(ctx, func(tx pgx.Tx) error {
			deleted = false
			var one int
			// Lock the row, then re-check in a new statement. An uploader's
			// BlobBegin for the same digest waits here until we commit.
			err := tx.QueryRow(ctx, `SELECT 1 FROM blobs WHERE sha256 = $1 FOR UPDATE`, sha).Scan(&one)
			if errors.Is(err, pgx.ErrNoRows) {
				return nil
			}
			if err != nil {
				return err
			}
			err = tx.QueryRow(ctx, `SELECT 1 FROM blobs b WHERE b.sha256 = $2 AND `+blobUnreferenced, grace.Seconds(), sha).Scan(&one)
			if errors.Is(err, pgx.ErrNoRows) {
				return nil // referenced or touched since the scan
			}
			if err != nil {
				return err
			}
			dctx, cancel := context.WithTimeout(ctx, 15*time.Second)
			defer cancel()
			if err := deleteObject(dctx, sha); err != nil {
				return fmt.Errorf("deleting object: %w", err)
			}
			_, err = tx.Exec(ctx, `DELETE FROM blobs WHERE sha256 = $1`, sha)
			deleted = err == nil
			return err
		})
		switch {
		case err != nil:
			st.Errors++
		case deleted:
			st.BlobsDeleted++
		default:
			st.BlobsKept++
		}
	}
	return st, nil
}
