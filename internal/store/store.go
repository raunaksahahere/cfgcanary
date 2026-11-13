// Package store is cfgcanary's PostgreSQL layer. PostgreSQL is the single
// authority for configs, versions, releases, desired state, acknowledgements
// and the audit log (docs/invariants.md I3). Every state transition is one
// transaction that also writes its audit event.
package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/raunaksahahere/cfgcanary/migrations"
)

// Errors returned by the store. The API layer maps them to gRPC codes.
var (
	ErrNotFound      = errors.New("not found")
	ErrAlreadyExists = errors.New("already exists")
	ErrConflict      = errors.New("conflict")
	ErrPrecondition  = errors.New("precondition failed")
	ErrFenced        = errors.New("controller epoch is stale: this process is no longer the leader")
	ErrInvalid       = errors.New("invalid argument")
)

func wrap(base error, format string, a ...any) error {
	return fmt.Errorf("%w: %s", base, fmt.Sprintf(format, a...))
}

// NotifyChannel carries "config/environment" whenever desired state changes.
const NotifyChannel = "cfgcanary_desired"

// advisory lock keys
const (
	lockMigrate    int64 = 0x63666763_00000001
	LockController int64 = 0x63666763_00000002
)

// Store wraps a connection pool.
type Store struct {
	Pool *pgxpool.Pool
	dsn  string
}

// Open connects and verifies the connection.
func Open(ctx context.Context, dsn string, maxConns int32) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parsing database URL: %w", redact(err))
	}
	if maxConns > 0 {
		cfg.MaxConns = maxConns
	}
	cfg.MaxConnIdleTime = 5 * time.Minute
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, redact(err)
	}
	pctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := pool.Ping(pctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("connecting to PostgreSQL: %w", redact(err))
	}
	return &Store{Pool: pool, dsn: dsn}, nil
}

// DSN returns the connection string, for the controller's dedicated lock
// connection and the hub's LISTEN connection.
func (s *Store) DSN() string { return s.dsn }

// Close releases the pool.
func (s *Store) Close() { s.Pool.Close() }

// redact strips anything that looks like a connection string from an error.
func redact(err error) error {
	if err == nil {
		return nil
	}
	msg := err.Error()
	for _, scheme := range []string{"postgres://", "postgresql://"} {
		for {
			i := strings.Index(msg, scheme)
			if i < 0 {
				break
			}
			j := i
			for j < len(msg) && msg[j] != ' ' && msg[j] != '"' && msg[j] != ')' {
				j++
			}
			msg = msg[:i] + "<database-url>" + msg[j:]
		}
	}
	return errors.New(msg)
}

// Migrate applies embedded migrations that have not run yet. It is safe to
// run concurrently from several replicas.
func (s *Store) Migrate(ctx context.Context) error {
	names, err := fs.Glob(migrations.FS, "*.sql")
	if err != nil {
		return err
	}
	sort.Strings(names)
	conn, err := s.Pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, lockMigrate); err != nil {
		return err
	}
	defer func() {
		_, _ = conn.Exec(context.WithoutCancel(ctx), `SELECT pg_advisory_unlock($1)`, lockMigrate)
	}()
	if _, err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		name text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
		return err
	}
	for _, name := range names {
		var done bool
		if err := conn.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE name = $1)`, name).Scan(&done); err != nil {
			return err
		}
		if done {
			continue
		}
		sql, err := migrations.FS.ReadFile(name)
		if err != nil {
			return err
		}
		tx, err := conn.Begin(ctx)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, string(sql)); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("migration %s: %w", name, err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations (name) VALUES ($1)`, name); err != nil {
			_ = tx.Rollback(ctx)
			return err
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("migration %s: %w", name, err)
		}
	}
	return nil
}

// inTx runs fn in a READ COMMITTED transaction, retrying deadlocks and
// serialization failures a bounded number of times.
func (s *Store) inTx(ctx context.Context, fn func(tx pgx.Tx) error) error {
	var last error
	for attempt := 0; attempt < 4; attempt++ {
		tx, err := s.Pool.Begin(ctx)
		if err != nil {
			return err
		}
		err = fn(tx)
		if err == nil {
			err = tx.Commit(ctx)
		}
		if err == nil {
			return nil
		}
		_ = tx.Rollback(context.WithoutCancel(ctx))
		var pg *pgconn.PgError
		if errors.As(err, &pg) && (pg.Code == "40001" || pg.Code == "40P01") {
			last = err
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(10*(attempt+1)) * time.Millisecond):
			}
			continue
		}
		return err
	}
	return last
}

func isUnique(err error) bool {
	var pg *pgconn.PgError
	return errors.As(err, &pg) && pg.Code == "23505"
}

// Now returns the database clock. Durable deadlines (bake, timeouts) use one
// clock source so replicas with skewed clocks agree.
func (s *Store) Now(ctx context.Context) (time.Time, error) {
	var t time.Time
	err := s.Pool.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&t)
	return t, err
}

// Audit is one audit event.
type Audit struct {
	Actor, Action, Outcome       string
	Config, Environment, Release string
	Generation, Epoch            int64
	Reason                       string
	Detail                       map[string]any
}

type execer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

func insertAudit(ctx context.Context, q execer, a Audit) error {
	if a.Outcome == "" {
		a.Outcome = "allowed"
	}
	detail := []byte("{}")
	if len(a.Detail) > 0 {
		b, err := json.Marshal(a.Detail)
		if err != nil {
			return err
		}
		detail = b
	}
	_, err := q.Exec(ctx, `INSERT INTO audit_events
		(actor, action, outcome, config, environment, release_id, generation, epoch, reason, detail)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
		a.Actor, a.Action, a.Outcome, a.Config, a.Environment, a.Release, a.Generation, a.Epoch, truncate(a.Reason, 500), detail)
	return err
}

// RecordAudit writes an audit event outside any other transaction (used for
// denied attempts).
func (s *Store) RecordAudit(ctx context.Context, a Audit) error {
	return insertAudit(ctx, s.Pool, a)
}

func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}

func notify(ctx context.Context, tx pgx.Tx, config, env string) error {
	_, err := tx.Exec(ctx, `SELECT pg_notify($1, $2)`, NotifyChannel, config+"/"+env)
	return err
}

// checkEpoch is the fencing check. It must run inside the transaction that
// performs the controller's write: the shared row lock orders it against a
// successor's increment (docs/invariants.md I4).
func checkEpoch(ctx context.Context, tx pgx.Tx, epoch int64) error {
	var cur int64
	if err := tx.QueryRow(ctx, `SELECT epoch FROM controller_leader WHERE id = 1 FOR SHARE`).Scan(&cur); err != nil {
		return err
	}
	if cur != epoch {
		return fmt.Errorf("%w (mine %d, current %d)", ErrFenced, epoch, cur)
	}
	return nil
}

// ClaimEpoch increments the fencing token and records the new holder. It is
// called once by a controller right after it acquires the advisory lock, on
// the connection that holds the lock.
func ClaimEpoch(ctx context.Context, conn *pgx.Conn, holder string) (int64, error) {
	var epoch int64
	err := conn.QueryRow(ctx, `UPDATE controller_leader
		SET epoch = epoch + 1, holder = $1, acquired_at = now() WHERE id = 1 RETURNING epoch`, holder).Scan(&epoch)
	return epoch, err
}

// CurrentEpoch reads the fencing token.
func (s *Store) CurrentEpoch(ctx context.Context) (epoch int64, holder string, err error) {
	err = s.Pool.QueryRow(ctx, `SELECT epoch, holder FROM controller_leader WHERE id = 1`).Scan(&epoch, &holder)
	return
}

// idempotent looks up or reserves a request ID inside tx. It returns the
// stored result reference when the request was already executed.
func idempotent(ctx context.Context, tx pgx.Tx, subject, requestID, method string) (ref string, seen bool, err error) {
	if requestID == "" {
		return "", false, nil
	}
	if len(requestID) > 128 {
		return "", false, wrap(ErrInvalid, "request_id is longer than 128 characters")
	}
	// Serialize concurrent retries of the same request.
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended(length($1)::text || ':' || $1 || $2, 42))`, subject, requestID); err != nil {
		return "", false, err
	}
	var m string
	err = tx.QueryRow(ctx, `SELECT method, result_ref FROM idempotency WHERE subject = $1 AND request_id = $2`, subject, requestID).Scan(&m, &ref)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	if m != method {
		return "", false, wrap(ErrConflict, "request_id was already used for %s", m)
	}
	return ref, true, nil
}

func remember(ctx context.Context, tx pgx.Tx, subject, requestID, method, ref string) error {
	if requestID == "" {
		return nil
	}
	_, err := tx.Exec(ctx, `INSERT INTO idempotency (subject, request_id, method, result_ref) VALUES ($1,$2,$3,$4)`,
		subject, requestID, method, ref)
	return err
}

// PruneIdempotency drops request records older than the retention window.
func (s *Store) PruneIdempotency(ctx context.Context, olderThan time.Duration) error {
	_, err := s.Pool.Exec(ctx, `DELETE FROM idempotency WHERE created_at < now() - make_interval(secs => $1)`, olderThan.Seconds())
	return err
}
