package store_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/types/known/durationpb"

	pb "github.com/raunaksahahere/cfgcanary/api/gen/cfgcanary/v1"
	"github.com/raunaksahahere/cfgcanary/internal/blob"
	"github.com/raunaksahahere/cfgcanary/internal/rollout"
	"github.com/raunaksahahere/cfgcanary/internal/sign"
	"github.com/raunaksahahere/cfgcanary/internal/store"
	"github.com/raunaksahahere/cfgcanary/internal/testutil"
)

type fixture struct {
	t      *testing.T
	s      *store.Store
	signer *sign.Signer
	ctx    context.Context
}

func newFixture(t *testing.T) *fixture {
	dsn := testutil.FreshDatabase(t)
	ctx := context.Background()
	s, err := store.Open(ctx, dsn, 20)
	testutil.Must(t, err, "open")
	t.Cleanup(s.Close)
	testutil.Must(t, s.Migrate(ctx), "migrate")
	testutil.Must(t, s.Migrate(ctx), "migrate twice")
	k, _ := sign.GenerateKey()
	return &fixture{t: t, s: s, signer: sign.NewSigner(sign.PurposeArtifact, k), ctx: ctx}
}

func (f *fixture) register(name string, rings ...int32) {
	var rs []*pb.Ring
	for _, b := range rings {
		rs = append(rs, &pb.Ring{CumulativeBps: b})
	}
	_, err := f.s.UpsertConfig(f.ctx, &pb.ConfigSpec{
		Name: name, Validation: &pb.ValidationPolicy{Limits: &pb.Limits{MaxBytes: 1 << 16}},
		Environments: map[string]*pb.RolloutPolicy{"prod": {
			Rings: rs, ReachableWindow: durationpb.New(time.Hour),
			Gate: &pb.GatePolicy{Mode: pb.GateMode_GATE_MODE_NONE, MinBake: durationpb.New(time.Millisecond)},
		}},
	}, 0, "alice")
	testutil.Must(f.t, err, "register")
}

func (f *fixture) instances(n int) {
	for i := range n {
		testutil.Must(f.t, f.s.UpsertInstance(f.ctx, "prod", fmt.Sprintf("agent-%03d", i), "", nil, "test"), "instance")
	}
}

func (f *fixture) version(config, body string) int64 {
	sha := blob.Digest([]byte(body))
	_, err := f.s.BlobBegin(f.ctx, sha, int64(len(body)), "payload")
	testutil.Must(f.t, err, "blob begin")
	v, err := f.s.InsertVersion(f.ctx, config, sha, int64(len(body)), "", []string{"prod"}, "alice", "", f.signer.Sign2)
	testutil.Must(f.t, err, "insert version")
	return v.Version
}

func (f *fixture) setBaseline(config string, v int64) {
	// Directly establish a baseline as a completed release would.
	_, err := f.s.Pool.Exec(f.ctx, `UPDATE targets SET baseline_version = $3, previous_good_version = baseline_version, generation = generation + 1
		WHERE config = $1 AND environment = $2`, config, "prod", v)
	testutil.Must(f.t, err, "baseline")
}

func (f *fixture) create(config string, v int64) *store.ReleaseRow {
	r, _, err := f.s.CreateRelease(f.ctx, store.PlanInput{Config: config, Env: "prod", Version: v, MembershipWindow: time.Hour}, "alice", "", "")
	testutil.Must(f.t, err, "create release")
	return r
}

func (f *fixture) apply(id string, ev rollout.Event, actor string) (*store.ReleaseRow, bool, error) {
	return f.s.ApplyEvent(f.ctx, id, ev, store.EventMeta{Actor: actor, Method: string(ev.Kind)})
}

func TestTransitionAndAuditAtomic(t *testing.T) {
	f := newFixture(t)
	f.register("limits", 3000, 10000)
	f.instances(20)
	v1 := f.version("limits", `{"a":1}`)
	f.setBaseline("limits", v1)
	v2 := f.version("limits", `{"a":2}`)
	r := f.create("limits", v2)

	tgt0, _ := f.s.TargetRow(f.ctx, "limits", "prod")
	r, _, err := f.apply(r.ID, rollout.Event{Kind: rollout.EvStart}, "controller")
	testutil.Must(t, err, "start")
	tgt, _ := f.s.TargetRow(f.ctx, "limits", "prod")
	if tgt.T.Generation != tgt0.T.Generation+1 || tgt.T.Candidate != v2 || tgt.T.ActiveRing != 0 || r.R.State != rollout.Rolling {
		t.Fatalf("start did not update desired state: %+v %+v", tgt.T, r.R)
	}
	ev, _, err := f.s.ListAudit(f.ctx, "limits", "prod", r.ID, "release.start", 10, 0)
	testutil.Must(t, err, "audit")
	if len(ev) != 1 || ev[0].Generation != tgt.T.Generation {
		t.Fatalf("audit event missing or wrong generation: %v", ev)
	}

	// A transition whose audit insert fails must leave no trace: force a
	// failure by making the audit table reject inserts for a moment.
	_, err = f.s.Pool.Exec(f.ctx, `CREATE FUNCTION fail_audit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected'; END $$;
		CREATE TRIGGER fail_audit BEFORE INSERT ON audit_events FOR EACH ROW EXECUTE FUNCTION fail_audit();`)
	testutil.Must(t, err, "inject")
	if _, _, err := f.apply(r.ID, rollout.Event{Kind: rollout.EvRollback, Reason: "x"}, "bob"); err == nil {
		t.Fatal("rollback succeeded although its audit insert failed")
	}
	_, err = f.s.Pool.Exec(f.ctx, `DROP TRIGGER fail_audit ON audit_events`)
	testutil.Must(t, err, "uninject")
	after, _ := f.s.TargetRow(f.ctx, "limits", "prod")
	rr, _ := f.s.ReleaseRow(f.ctx, r.ID)
	if after.T != tgt.T || rr.R.State != rollout.Rolling {
		t.Fatalf("partial transition committed: %+v %s", after.T, rr.R.State)
	}
}

func TestDatabaseInvariantsEnforced(t *testing.T) {
	f := newFixture(t)
	f.register("limits", 10000)
	v := f.version("limits", `{}`)
	if _, err := f.s.Pool.Exec(f.ctx, `UPDATE config_versions SET sha256 = sha256 WHERE version = $1`, v); err != nil {
		t.Fatalf("no-op update rejected: %v", err)
	}
	if _, err := f.s.Pool.Exec(f.ctx, `UPDATE config_versions SET size = size + 1 WHERE version = $1`, v); err == nil {
		t.Fatal("version content was mutated")
	}
	if _, err := f.s.Pool.Exec(f.ctx, `DELETE FROM config_versions`); err == nil {
		t.Fatal("version deleted")
	}
	if _, err := f.s.Pool.Exec(f.ctx, `UPDATE audit_events SET actor = 'mallory'`); err == nil {
		t.Fatal("audit log updated")
	}
	if _, err := f.s.Pool.Exec(f.ctx, `TRUNCATE audit_events`); err == nil {
		t.Fatal("audit log truncated")
	}
	if _, err := f.s.Pool.Exec(f.ctx, `UPDATE targets SET generation = generation - 1 WHERE config = 'limits'`); err == nil {
		t.Fatal("generation decreased")
	}
	if _, err := f.s.Pool.Exec(f.ctx, `UPDATE targets SET baseline_version = 99 WHERE config = 'limits'`); err == nil {
		t.Fatal("desired state changed without a generation bump")
	}
}

func TestConcurrentCreateOneWins(t *testing.T) {
	f := newFixture(t)
	f.register("limits", 5000, 10000)
	f.instances(10)
	f.version("limits", `{"a":1}`)
	v2 := f.version("limits", `{"a":2}`)
	var wg sync.WaitGroup
	var wins, conflicts atomic.Int32
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, err := f.s.CreateRelease(f.ctx, store.PlanInput{Config: "limits", Env: "prod", Version: v2, MembershipWindow: time.Hour}, "alice", "", "")
			switch {
			case err == nil:
				wins.Add(1)
			case errors.Is(err, store.ErrPrecondition), errors.Is(err, store.ErrAlreadyExists):
				conflicts.Add(1)
			default:
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}
	wg.Wait()
	if wins.Load() != 1 || conflicts.Load() != 7 {
		t.Fatalf("wins=%d conflicts=%d", wins.Load(), conflicts.Load())
	}
}

func TestDuplicateCommandsIdempotent(t *testing.T) {
	f := newFixture(t)
	f.register("limits", 5000, 10000)
	f.instances(10)
	v := f.version("limits", `{"a":1}`)
	in := store.PlanInput{Config: "limits", Env: "prod", Version: v, MembershipWindow: time.Hour}
	r1, _, err := f.s.CreateRelease(f.ctx, in, "alice", "req-1", "")
	testutil.Must(t, err, "create")
	r2, _, err := f.s.CreateRelease(f.ctx, in, "alice", "req-1", "")
	testutil.Must(t, err, "retry create")
	if r1.ID != r2.ID {
		t.Fatal("retried create made a second release")
	}
	_, _, err = f.s.ApplyEvent(f.ctx, r1.ID, rollout.Event{Kind: rollout.EvStart}, store.EventMeta{Actor: "controller"})
	testutil.Must(t, err, "start")
	_, _, err = f.s.ApplyEvent(f.ctx, r1.ID, rollout.Event{Kind: rollout.EvBeginBake}, store.EventMeta{Actor: "controller"})
	testutil.Must(t, err, "bake")
	time.Sleep(5 * time.Millisecond)
	ring0 := int32(0)
	promote := rollout.Event{Kind: rollout.EvPromote, ExpectRing: &ring0}
	a, noop, err := f.s.ApplyEvent(f.ctx, r1.ID, promote, store.EventMeta{Actor: "alice", RequestID: "p-1", Method: "PromoteRelease"})
	testutil.Must(t, err, "promote")
	if noop || a.R.ActiveRing != 1 {
		t.Fatalf("first promote: noop=%v ring=%d", noop, a.R.ActiveRing)
	}
	b, noop, err := f.s.ApplyEvent(f.ctx, r1.ID, promote, store.EventMeta{Actor: "alice", RequestID: "p-1", Method: "PromoteRelease"})
	testutil.Must(t, err, "retried promote")
	if !noop || b.R.ActiveRing != 1 {
		t.Fatalf("retried promote advanced again: ring=%d", b.R.ActiveRing)
	}
	// Without a request id, the expected-ring guard rejects a double click.
	if _, _, err := f.s.ApplyEvent(f.ctx, r1.ID, promote, store.EventMeta{Actor: "alice", Method: "PromoteRelease"}); !errors.Is(err, store.ErrPrecondition) {
		t.Fatalf("stale promote: %v", err)
	}
	// Reusing a request id for another command is a conflict.
	if _, _, err := f.s.ApplyEvent(f.ctx, r1.ID, rollout.Event{Kind: rollout.EvPause}, store.EventMeta{Actor: "alice", RequestID: "p-1", Method: "PauseRelease"}); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("request id reuse: %v", err)
	}
}

func TestRollbackRacesPromote(t *testing.T) {
	for i := range 20 {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			f := newFixture(t)
			f.register("limits", 3000, 6000, 10000)
			f.instances(20)
			v1 := f.version("limits", `{"a":1}`)
			f.setBaseline("limits", v1)
			v2 := f.version("limits", `{"a":2}`)
			r := f.create("limits", v2)
			r, _, _ = f.apply(r.ID, rollout.Event{Kind: rollout.EvStart}, "controller")
			r, _, _ = f.apply(r.ID, rollout.Event{Kind: rollout.EvBeginBake}, "controller")
			time.Sleep(3 * time.Millisecond)
			decided := r.R.StateVersion
			var wg sync.WaitGroup
			wg.Add(2)
			var promoteErr, rollbackErr error
			go func() {
				defer wg.Done()
				_, _, promoteErr = f.s.ApplyEvent(f.ctx, r.ID, rollout.Event{Kind: rollout.EvPromote, ExpectStateVersion: decided}, store.EventMeta{Actor: "alice"})
			}()
			go func() {
				defer wg.Done()
				_, _, rollbackErr = f.s.ApplyEvent(f.ctx, r.ID, rollout.Event{Kind: rollout.EvRollback, Reason: "race"}, store.EventMeta{Actor: "bob"})
			}()
			wg.Wait()
			if rollbackErr != nil {
				t.Fatalf("rollback must always win or apply: %v", rollbackErr)
			}
			final, _ := f.s.ReleaseRow(f.ctx, r.ID)
			tgt, _ := f.s.TargetRow(f.ctx, "limits", "prod")
			if final.R.State != rollout.RollingBack || tgt.T.Candidate != 0 || tgt.T.DesiredFor(0) != v1 || tgt.T.DesiredFor(2) != v1 {
				t.Fatalf("after race: state=%s target=%+v promoteErr=%v", final.R.State, tgt.T, promoteErr)
			}
		})
	}
}

func TestStaleLeaderWriteRejected(t *testing.T) {
	f := newFixture(t)
	f.register("limits", 5000, 10000)
	f.instances(10)
	v := f.version("limits", `{"a":1}`)
	r := f.create("limits", v)
	connA, err := pgx.Connect(f.ctx, f.s.DSN())
	testutil.Must(t, err, "conn")
	defer connA.Close(f.ctx)
	epochA, err := store.ClaimEpoch(f.ctx, connA, "A")
	testutil.Must(t, err, "claim A")
	if _, _, err := f.s.ApplyEvent(f.ctx, r.ID, rollout.Event{Kind: rollout.EvStart}, store.EventMeta{Actor: "controller", Epoch: epochA}); err != nil {
		t.Fatalf("leader A write: %v", err)
	}
	epochB, err := store.ClaimEpoch(f.ctx, connA, "B")
	testutil.Must(t, err, "claim B")
	if epochB <= epochA {
		t.Fatal("epoch did not increase")
	}
	if _, _, err := f.s.ApplyEvent(f.ctx, r.ID, rollout.Event{Kind: rollout.EvBeginBake}, store.EventMeta{Actor: "controller", Epoch: epochA}); !errors.Is(err, store.ErrFenced) {
		t.Fatalf("stale leader write: %v", err)
	}
	rr, _ := f.s.ReleaseRow(f.ctx, r.ID)
	if rr.R.State != rollout.Rolling {
		t.Fatal("fenced write changed state")
	}
	if _, _, err := f.s.ApplyEvent(f.ctx, r.ID, rollout.Event{Kind: rollout.EvBeginBake}, store.EventMeta{Actor: "controller", Epoch: epochB}); err != nil {
		t.Fatalf("new leader write: %v", err)
	}
}

func TestStatusOldGenerationIgnored(t *testing.T) {
	f := newFixture(t)
	f.register("limits", 10000)
	f.instances(1)
	put := func(gen, ver int64, state string) {
		testutil.Must(t, f.s.UpsertStatuses(f.ctx, []store.StatusRow{{Env: "prod", Instance: "agent-000", Config: "limits",
			Generation: gen, Version: ver, State: state, ServingVersion: ver, ServingGeneration: gen}}), "status")
	}
	get := func() *pb.InstanceConfigStatus {
		in, _, err := f.s.ListInstances(f.ctx, "prod", 10, "", "", time.Hour)
		testutil.Must(t, err, "list")
		return in[0].Statuses[0]
	}
	put(5, 2, "ACCEPTED")
	put(6, 1, "APPLIED")  // rollback generation
	put(5, 2, "ACCEPTED") // delayed duplicate of the old ack
	if s := get(); s.Generation != 6 || s.Version != 1 {
		t.Fatalf("old ack overwrote newer status: %+v", s)
	}
	put(6, 1, "ACCEPTED")
	put(6, 1, "APPLIED") // reordered earlier report for the same generation
	if s := get(); s.State != pb.ApplyState_APPLY_STATE_ACCEPTED {
		t.Fatalf("status moved backwards within a generation: %v", s.State)
	}
}

func TestKillSwitchWithoutRelease(t *testing.T) {
	f := newFixture(t)
	f.register("limits", 10000)
	f.instances(3)
	v1 := f.version("limits", `{"a":1}`)
	f.setBaseline("limits", v1)
	v2 := f.version("limits", `{"a":2}`)
	f.setBaseline("limits", v2)
	rel, tgt, noop, err := f.s.KillSwitch(f.ctx, "limits", "prod", "incident-42", "oncall", "", time.Hour)
	testutil.Must(t, err, "kill")
	if noop || tgt.T.Baseline != v1 || !tgt.T.Frozen || rel.R.Kind != "emergency" || rel.R.State != rollout.RollingBack {
		t.Fatalf("kill: noop=%v target=%+v rel=%+v", noop, tgt.T, rel.R)
	}
	_, _, noop, err = f.s.KillSwitch(f.ctx, "limits", "prod", "again", "oncall", "", time.Hour)
	if err != nil || !noop {
		t.Fatalf("repeat kill: noop=%v err=%v", noop, err)
	}
	// Frozen targets refuse new releases.
	_, _, err = f.s.CreateRelease(f.ctx, store.PlanInput{Config: "limits", Env: "prod", Version: v2, MembershipWindow: time.Hour}, "alice", "", "")
	if err == nil || !strings.Contains(err.Error(), "frozen") {
		t.Fatalf("release on frozen target: %v", err)
	}
}

// The collector must not delete a version that a concurrent release creation
// is about to reference, and must not delete blobs that are referenced.
func TestGCRaceWithReleaseCreation(t *testing.T) {
	f := newFixture(t)
	f.register("limits", 5000, 10000)
	f.instances(10)
	var vs []int64
	for i := range 6 {
		vs = append(vs, f.version("limits", fmt.Sprintf(`{"v":%d}`, i)))
	}
	f.setBaseline("limits", vs[4])
	_, err := f.s.Pool.Exec(f.ctx, `UPDATE configs SET spec = jsonb_set(spec, '{retain_versions}', '1')`)
	testutil.Must(t, err, "retain")
	deleted := map[string]bool{}
	var mu sync.Mutex
	del := func(_ context.Context, sha string) error { mu.Lock(); deleted[sha] = true; mu.Unlock(); return nil }

	var wg sync.WaitGroup
	var created *store.ReleaseRow
	var createErr error
	wg.Add(2)
	go func() {
		defer wg.Done()
		created, _, createErr = f.s.CreateRelease(f.ctx, store.PlanInput{Config: "limits", Env: "prod", Version: vs[1], MembershipWindow: time.Hour}, "alice", "", "")
	}()
	go func() {
		defer wg.Done()
		_, _ = f.s.GC(f.ctx, 1, 0, del)
	}()
	wg.Wait()
	_, err = f.s.GC(f.ctx, 1, 0, del)
	testutil.Must(t, err, "gc")

	v1, _ := f.s.GetVersion(f.ctx, "limits", vs[1])
	if createErr == nil {
		if v1.Purged || deleted[v1.Sha256] {
			t.Fatal("GC purged a version referenced by a release created concurrently")
		}
		_ = created
	} else if !strings.Contains(createErr.Error(), "purged") {
		t.Fatalf("create failed for another reason: %v", createErr)
	}
	for _, keep := range []int64{vs[4], vs[5]} { // baseline and newest
		v, _ := f.s.GetVersion(f.ctx, "limits", keep)
		if v.Purged || deleted[v.Sha256] {
			t.Fatalf("GC removed protected version %d", keep)
		}
	}
	v0, _ := f.s.GetVersion(f.ctx, "limits", vs[0])
	if !v0.Purged || !deleted[v0.Sha256] {
		t.Fatal("GC did not collect an unreferenced old version")
	}
}

func TestBlobStoreRoundTrip(t *testing.T) {
	testutil.RequireIntegration(t)
	ep, ak, sk := testutil.S3()
	bs, err := blob.New(blob.Config{Endpoint: ep, Bucket: testutil.RandomName("b-"), AccessKey: ak, SecretKey: sk})
	testutil.Must(t, err, "blob")
	ctx := context.Background()
	testutil.Must(t, bs.EnsureBucket(ctx), "bucket")
	data := []byte(`{"hello":"world"}`)
	sha := blob.Digest(data)
	if err := bs.Put(ctx, blob.Digest([]byte("other")), data); !errors.Is(err, blob.ErrCorrupt) {
		t.Fatal("stored bytes under the wrong digest")
	}
	testutil.Must(t, bs.Put(ctx, sha, data), "put")
	got, err := bs.Get(ctx, sha, 1024)
	testutil.Must(t, err, "get")
	if string(got) != string(data) {
		t.Fatal("round trip")
	}
	if _, err := bs.Get(ctx, sha, 4); !errors.Is(err, blob.ErrCorrupt) {
		t.Fatal("size limit not enforced")
	}
	u, _, err := bs.Presign(ctx, sha, time.Minute)
	testutil.Must(t, err, "presign")
	if !strings.HasPrefix(u, bs.Origin()+"/") || !strings.Contains(u, "X-Amz-Signature") {
		t.Fatalf("presigned URL %q", u)
	}
	testutil.Must(t, bs.Delete(ctx, sha), "delete")
	testutil.Must(t, bs.Delete(ctx, sha), "delete twice")
}
