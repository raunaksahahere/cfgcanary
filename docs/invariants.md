# Failure model and invariants

This page is the contract the code is tested against. Each invariant names the mechanism that
enforces it and the test that tries to break it. Where a promise cannot be made, it says so.

Terms used throughout:

| Term | Meaning |
|---|---|
| **config** | A named, registered configuration with a validation policy. |
| **version** | An immutable payload of a config: `(config, version number, sha256, size)`. |
| **target** | A `(config, environment)` pair. Releases and desired state are per target. |
| **generation** | A per-target integer that increases every time the target's desired state changes. |
| **baseline** | The version a target's agents run when no release exposes them to anything newer. |
| **candidate** | The version a release is trying to roll out. |
| **ring** | An ordered cohort. Ring `i` receives the candidate once the release's active ring is `>= i`. |
| **epoch** | The controller leader's fencing token. |

There are four distinct facts about "is the config out?" and the system never merges them:

1. **Desired** – what PostgreSQL says an instance should run (`targets` row + ring).
2. **Applied** – the agent verified the artifact and durably replaced its local pointer.
3. **Accepted** – the application's own parser, running in the SDK, accepted it and published it.
4. **Healthy** – service metrics for the cohort running it look no worse than the control cohort.

## I1. Immutable content, increasing delivery generations

- A version row and its blob are never updated. Blobs are stored under `sha256/<hex>`; the
  version number is allocated from a per-config sequence inside the insert transaction.
- `targets.generation` only increases, and it increases in the same transaction as every change
  to `(baseline, candidate, active_ring)`.
- **Rollback is a new generation that points at an older version.** Nothing in the system
  compares version numbers to decide ordering; only generations (per target) and epochs
  (controller) order events.
- An agent applies an update only if `generation > last applied generation` for that config. A
  stale or replayed `ConfigUpdate` is dropped. A status report is stored only if its generation
  is `>=` the stored one for that `(instance, target)`, so an old acknowledgement cannot
  overwrite a newer one, and acknowledgements for generation `g` are never counted toward a
  ring that started at generation `> g`.

Counterexamples considered: *rollback from v8 to v7 after agents saw v8* (allowed: generation
12 → 13, version 8 → 7); *a delayed stream message for generation 12 arriving after 13* (dropped
by the agent); *an ack for the candidate arriving after rollback* (stored generation is already
13 or the ring counter ignores generation 12 rows).

Tests: `internal/rollout` property tests (`TestProperty_GenerationMonotonic`),
`internal/agent` (`TestStaleUpdateIgnored`, `TestRollbackAppliesOlderVersion`),
`internal/store` (`TestStatusOldGenerationIgnored`).

## I2. Controlled exposure

- The server computes an instance's ring. Inputs are the **authenticated** identity (URI SAN in
  the client certificate) and the release's frozen policy snapshot. `instance_id` and `labels`
  in a request are checked against the certificate and are otherwise informational.
- Ring assignment, version 1: `bucket = BE64(SHA-256("cfgcanary/ring/v1" 0x00 salt 0x00
  instance_id)[0:8]) mod 10000`. Rings are cumulative thresholds in basis points. The salt and
  thresholds are frozen into the release when it is created; reconnects, leader changes and
  server restarts cannot reshuffle cohorts.
- Pinned groups: a certificate identity of the form `.../group/<g>` may only claim instance IDs
  `<g>/<suffix>`, and policy may pin a group to a ring. An agent cannot move itself to another
  group because the group is in its certificate, not in its request.
- An instance whose ring is `> active_ring` is only ever sent the baseline, is refused a
  presigned URL for the candidate, and (hash mode) would also fail its own check of the signed
  delivery statement, which names the ring range it is valid for.
- **Late joiners** are placed by the same function and receive whatever their ring is entitled
  to at that moment. They are added to the release's member list with `late = true`.
- **Offline members** stay in the denominator as *eligible* but are reported separately as
  unreachable. Convergence thresholds are evaluated over reachable members; a ring with too few
  reachable members halts instead of passing vacuously.
- A percentage threshold does not guarantee a non-empty cohort in a small fleet. Release
  planning prints actual cohort sizes and refuses to create a release whose canary or control
  cohort is smaller than the policy's `min_instances`.

Tests: `internal/ring` (`TestStableAcrossCalls`, `TestProperty_DisjointCumulative`),
`test/integration` (`TestLaterRingsNeverReceiveCandidate`, `TestSpoofedInstanceRejected`,
`TestGroupCannotEscape`).

## I3. One durable authority

- PostgreSQL holds releases, ring progress, desired state, acknowledgements and the audit log.
  Process memory holds only caches that are rebuilt from PostgreSQL.
- Every transition is one transaction that (a) locks the target row `FOR UPDATE`, (b) rechecks
  the precondition it was decided on (`releases.state_version`), (c) writes the release row,
  the target row and the generation bump, and (d) inserts the audit event. Either all of it
  commits or none.
- At most one active release per target: a partial unique index over
  `releases(config, environment) WHERE state IN (active states)`. A second create returns
  `ALREADY_EXISTS` with the active release ID.
- Commands carry an optional `request_id`. A repeated command with the same `(subject,
  request_id)` returns the recorded result instead of acting twice. Commands without one are
  guarded by `expected_ring` / current state and return `FAILED_PRECONDITION` when stale.

Tests: `test/integration` (`TestConcurrentCreateOneWins`, `TestDuplicatePromoteIdempotent`,
`TestRollbackRacesPromote`, `TestTransitionAndAuditAtomic`).

## I4. Fenced leadership

- One controller is active. It holds `pg_advisory_lock` on a **dedicated** connection. On
  acquiring it, it runs `UPDATE controller_leader SET epoch = epoch + 1 ... RETURNING epoch`
  and remembers the result as its token.
- Every controller transaction starts with `SELECT epoch FROM controller_leader FOR SHARE` and
  aborts with `ErrFenced` unless it equals the controller's token. The row lock orders this
  check against a successor's increment: a stale write either commits before the successor's
  increment (and is then visible to the successor's first read) or sees the new epoch and
  aborts. The epoch is also written to the release row and audit event for forensics.
- The lock connection is probed continuously. If the probe fails, or the session is gone, the
  leadership context is cancelled and in-flight work stops; the fencing check covers the window
  in which the old controller has not noticed yet.
- API commands from operators are not leader-bound. They are serialized against the controller
  by the target row lock and the `state_version` recheck (I3).

Counterexample considered: *leader A decides "promote ring 0", stalls, loses its session; B
becomes leader and rolls back; A resumes and commits.* A's transaction reads epoch `e+1 ≠ e`
and aborts. Even without the epoch, the `state_version` recheck would fail; both are kept
because they fail independently.

Tests: `test/integration` (`TestStaleLeaderWriteRejected`, `TestLockConnectionLossDemotes`,
`TestLeaderKilledMidPromotion`).

## I5. Fail-safe consumers

- The agent acknowledges `APPLIED` only after: delivery and artifact signatures verify against
  the trust bundle; scope (config, environment, ring range) matches; the downloaded bytes match
  the signed size and SHA-256; the object file is fsynced; and the pointer file is replaced by
  write-temp → fsync → rename → fsync(directory).
- The SDK reports `ACCEPTED` only after the application's parser returned no error and the new
  snapshot was published. If the parser rejects it, the SDK keeps serving the last-known-good
  (LKG) snapshot and reports `REJECTED`. A rejected payload is never counted as applied-and-
  healthy on the server: ring convergence counts `ACCEPTED`.
- Any failure in retrieval, verification, disk or network leaves the current pointer and the
  LKG object untouched. The pointer keeps `current`, `lkg` and `previous`; objects they
  reference are not garbage-collected from the local cache.
- On start, the agent and SDK re-verify the signature and digest of what the pointer names. A
  corrupt current entry falls back to `lkg`, then `previous`. A corrupt pointer file falls back
  to its retained predecessor.
- **Cold start with no usable LKG**: `sdk.Open` returns the explicitly configured bootstrap
  value if one was supplied (it is passed through the same parser), otherwise it blocks until a
  config is accepted or the context ends and then returns a typed `*sdk.InitError`. `Get()`
  exists only on an initialized client, so it never fails and never returns a zero value.
- `Get()` returns an immutable snapshot. `Open` refuses a snapshot type that contains maps,
  slices, pointers, channels, functions or interfaces unless the caller supplies a `Clone`
  function (applied on every `Get`) or explicitly opts into sharing.
- **Crash loops**: the SDK counts process starts since the current config was last marked
  healthy. After `CrashLoopThreshold` unhealthy starts on a config newer than the LKG, it
  quarantines that generation, reverts to the LKG and reports `CRASH_LOOP`.

What LKG does **not** mean: an old value is not proven safe forever. See
[Fail-open and fail-closed](#fail-open-and-fail-closed-per-config-type).

Tests: `internal/agent` (`TestBadSignatureKeepsLKG`, `TestBadDigestKeepsLKG`,
`TestCrashBetweenObjectAndPointer`, `TestCorruptPointerFallsBack`, `TestDiskErrorsKeepLKG`),
`sdk` (`TestColdStartNoCache`, `TestBootstrap`, `TestRejectKeepsLKG`, `TestCrashLoopQuarantine`,
`TestSnapshotImmutability`).

## I6. Safe gating

- A gate look returns exactly one of `PASS`, `FAIL`, `INCONCLUSIVE`. Missing series, stale
  samples, too few instances or requests, NaN/Inf, a query error or timeout, and a degraded
  control cohort all produce `INCONCLUSIVE` with a reason. Only `PASS` can promote.
- Queries select by config, environment, release, ring and `config_version`, so traffic from
  another config, another release or the wrong version cannot count as evidence.
- The number of looks is declared in the policy. Error probability is split across looks and
  metrics (see [the gate design](design-guide.md#the-health-gate)). After the last look, a
  release that has not passed is `HALTED`; it is never promoted by the passage of time.
- `FAIL`, or a hard threshold (SDK rejections, crash loops, absolute error ceiling), starts a
  rollback under the policy's `on_fail` rule (default `rollback`).
- A paused release does not advance. Bake time elapsing while paused does not count as a look,
  and resuming re-evaluates from fresh evidence.

Tests: `internal/gate` (`TestMissingDataNeverPasses` table, `TestStaleSamples`, `TestNaN`,
`TestControlDegraded`, `FuzzEvaluate`), `internal/rollout` (`TestPausedDoesNotAdvance`,
`TestInconclusiveHaltsAtLastLook`).

## I7. Explicit emergency precedence

- `rollback` and `kill-switch` win over everything pending. Each takes the target row lock,
  sets the desired state back to the baseline (or, for a kill switch with no active release, to
  the previous known-good version), bumps the generation, moves the active release to
  `ROLLING_BACK`, and writes an audit event, in one transaction.
- A promotion decided before the rollback cannot commit after it: its `state_version` no longer
  matches (I3), and if it came from an old leader it is also fenced (I4).
- A kill switch freezes the target. No release can be created for it until an administrator
  unfreezes it.
- Both are repeatable: a second identical command is a no-op that reports the current state.
- **Connected** agents are notified immediately and normally converge within a watch round
  trip. **Disconnected** agents keep running what they have until they reconnect and reconcile.
  The CLI reports confirmed, pending and unreachable agents separately and never reports a
  rollback as complete on the strength of the database row alone.

Tests: `test/integration` (`TestKillSwitchSupersedesPromotion`, `TestKillSwitchFreezes`,
`TestRollbackWhileDisconnected`), `internal/rollout` property tests.

## I8. Bounded work

| Pressure | Bound |
|---|---|
| Slow watch client | One coalescing slot per `(stream, config)`; a newer state replaces an unsent older one. Send deadline per message; a stream that cannot take a message in time is closed. |
| Reconnect storm | Token-bucket admission plus a stream cap; rejected clients get `RESOURCE_EXHAUSTED` and back off with full jitter. |
| Acknowledgement flood | Bounded batch queue; when full the RPC returns `RESOURCE_EXHAUSTED` and the agent retries a bounded number of times, then relies on periodic reconciliation. |
| Metrics unavailable | Per-query timeout and a total evaluation deadline; result is `INCONCLUSIVE`. |
| Malformed or huge payload | Size limit enforced on the wire before buffering; decompressed-size limit; JSON depth and cardinality limits before validation. |
| Validator hang or bomb | wazero with a memory page limit, a wall-clock deadline that closes the module, bounded stdout, a concurrency semaphore. |
| Controller stuck on one release | Each release is reconciled under its own deadline; an error on one does not block the others. |
| Shutdown | Every goroutine is tied to a context; servers drain with a deadline. Tests run under `goleak`-style checks for the agent and hub. |

## First start, staleness and operator intervention

- The SDK tracks the age of the last successful contact with its source using the monotonic
  clock. `MaxStaleness` (per config) turns into one of two behaviours, below.
- The agent's reconnect delay uses the monotonic clock and injected randomness. Ordering never
  uses wall-clock timestamps; timestamps are recorded for humans only.

### Fail-open and fail-closed per config type

Every registered config declares `failure_mode`. It is delivered to the SDK in the signed
delivery statement, so the consumer does not have to guess.

| Mode | When to choose it | New payload rejected | Source unreachable longer than `max_staleness` | No LKG at start |
|---|---|---|---|---|
| `fail_static` (default) | Routing tables, limits, feature data: an old value is better than none. | Keep LKG, report `REJECTED`. | Keep serving LKG; `Stale()` is true and a metric is raised. | Bootstrap value, else `InitError`. |
| `fail_closed` | Allow-lists, authorization policy: serving a stale permit may be worse than refusing. | Keep LKG, report `REJECTED`. | `Get()` still returns the snapshot, but `Usable()` is false and `GetFresh()` returns `ErrStale`. The application must refuse or degrade. | `InitError`; a bootstrap value is rejected at `Open` for this mode unless it is marked explicitly as a deny-all default. |

Neither mode makes a runtime-bad canary healthy: a config that parses but hurts the service is
caught by the gate, not by LKG.

Operator intervention is required when: a release is `HALTED`; a target is frozen by a kill
switch; a rollback reports unreachable agents that do not return; an agent reports
`NO_USABLE_CONFIG`; a `fail_closed` consumer is stale.

## What is not promised

- Disconnected agents are not rolled back until they reconnect.
- A canary cohort that does not exercise the faulty code path will not reveal the fault.
- Statistical gates bound error rates under stated assumptions; they are not proofs of safety.
- A compromise of the control plane's signing keys, of PostgreSQL, or of the certificate
  authority is outside the model.
- Config payloads are not a secrets channel. They are stored and cached unencrypted.
