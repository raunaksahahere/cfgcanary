# Design guide: how cfgcanary works and why

A compact guide to the code paths and the reasoning behind them, so you can explain and defend
each decision. Read [invariants.md](invariants.md) for the contract and
[ARCHITECTURE.md](ARCHITECTURE.md) for the component map; this page explains the *why*.

## 1. Follow one config change through the code

| Step | What happens | Code |
|---|---|---|
| Submit | `ControlService.SubmitVersion` authorizes, picks the relative-change base (each environment's deployed baseline), runs the pipeline, uploads the blob, then inserts the version and signs its artifact statement in one transaction | `internal/server/controlsvc.go`, `internal/validate`, `internal/store/configs.go` |
| Plan / create | `PlanRelease` computes real cohort sizes from current members; `CreateRelease` freezes the policy and the member list under the target row lock | `internal/store/releases.go` (`plan`, `CreateRelease`) |
| Start a ring | The controller's `Decide` returns `start`/`advance`; `ApplyEvent` runs `rollout.Transition`, bumps the generation, writes the audit event and `NOTIFY` in one transaction | `internal/controller`, `internal/rollout`, `internal/store/releases.go` (`applyLocked`) |
| Fan out | Every replica's hub hears the notification, marks the target dirty for each stream; each stream's sender builds the update for *its* instance's ring and sends it | `internal/server/hub.go`, `configsvc.go` (`Watch`) |
| Apply | The agent drops stale generations, verifies both signatures, scope and ring range, downloads by digest from an allowed origin, writes the object, swaps the pointer, reports `APPLIED` | `internal/agent/agent.go` (`Handle`, `applyEntry`), `internal/cache` |
| Accept | The SDK re-verifies, parses with the application's parser, publishes an immutable snapshot (or keeps LKG), writes its status; the agent forwards `ACCEPTED`/`REJECTED` | `sdk/sdk.go` (`poll`, `start`) |
| Gate | During the bake, the controller asks Prometheus for per-instance counts of canary and control cohorts and applies the statistics | `internal/gate` |
| Finish | `advance` to the next ring, `complete` (baseline := candidate) or `auto_rollback` (a new generation pointing at the baseline) | `internal/rollout` |

Everything that decides is pure (`rollout.Transition`, `rollout.Decide`, `ring.Assign`,
`gate.EvaluateMetric`, `validate.Validator`); everything that does I/O calls it. That is why the
property tests in `internal/rollout` exercise the production logic directly.

## 2. Generations, not versions, order everything

A version is content; a generation is *a decision about who should run what*. Rolling back from
v8 to v7 is generation 13 pointing at version 7. Agents apply only a higher generation, and the
server stores an acknowledgement only if its generation is not older than the stored one.
Consequences:

- A delayed message for the rolled-back version (generation 12) can never reapply it.
- A late acknowledgement for generation 12 can never make a ring look converged after the rollback.
- Nothing compares timestamps, so clock skew between hosts cannot reorder state. Wall-clock time is
  used for humans and for the gate's evaluation window, never for ordering.

The database enforces it too: a trigger rejects a generation decrease and a desired-state change
without a generation bump.

## 3. One authority, and fencing

PostgreSQL owns release state. Every transition locks the target row, re-checks the release's
`state_version` (the snapshot the decision was made on), writes release + target + audit, and
notifies, in one transaction. A second active release for a target is impossible (partial unique
index). Retried commands carry a `request_id` and return the original result.

**Leader election** is `pg_try_advisory_lock` on a *dedicated* connection. On acquiring it, the
controller increments `controller_leader.epoch` and keeps the value as its fencing token. Every
controller transaction begins with `SELECT epoch ... FOR SHARE` and aborts unless it matches.

Why both a lock and a token? The lock tells a process it *was* leader the last time it checked.
A process can stall (GC pause, VM freeze, slow network), lose its session, and resume with a
decision still in memory. The token check runs inside the write's own transaction, and the row
lock orders it against a successor's increment: either the stale write commits before the
successor takes over (and the successor sees it), or it sees the new epoch and aborts. The chaos
test `TestLockConnectionLostAndStaleWriteFenced` kills the lock session with
`pg_terminate_backend` and then attempts exactly such a write.

Operator commands do not need leadership: the row lock and `state_version` serialize them with the
controller (`TestRollbackRacesPromote` races them 20 times).

## 4. Static stability: what keeps running when things break

Static stability means a system keeps doing what it was doing when its dependencies fail, rather
than needing them to keep working. Here:

- Agents hold a verified, durable copy of every config they serve. If the control plane, the
  database, the object store or the network goes away, applications keep their current snapshot.
  The chaos suite stops each dependency and checks that nothing an application serves changes.
- The cache is crash-consistent: objects are named by digest and written before the pointer that
  references them; the pointer is replaced by write-temp, fsync, rename, fsync(directory); the
  previous pointer is kept so a torn or corrupt pointer falls back instead of erasing state.
- Everything loaded from disk is re-verified (signatures, scope, digest). A corrupted current entry
  falls back to the last-known-good, then the previous one.
- Correctness never depends on receiving every stream message: on connect, and every reconcile
  interval, the agent asks for the authoritative snapshot and applies anything newer.

Static stability is also why the gate halts instead of promoting when Prometheus is unavailable:
"no evidence" must not turn into "safe".

## 5. Jitter: why reconnects are randomized

When a control plane restarts, every agent loses its stream at the same instant. If every agent
retries after exactly 250 ms, then 500 ms, then 1 s, they arrive together each time: a self-inflicted
denial of service (Google's 2025-06-12 incident report names the missing randomized backoff as
what slowed recovery). cfgcanary uses capped exponential backoff with **full jitter**: each delay is
uniform on `[0, min(cap, base × 2^attempt))`.

Measured on 1,000 agents with the server killed for 5 s (`bench/results/storm.json`):

| | Peak in any 100 ms | Peak in any 100 ms after restart | Restart → everyone reconnected |
|---|---:|---:|---:|
| full jitter | 520 | 36 | 9.8 s |
| no jitter | 990 | 728 | 3.3 s |

Be precise about the trade-off: jitter flattens the burst the server must absorb at the moment it
comes back, but full reconnection takes longer, because some agents drew long delays. That is the
right trade for a server that has just restarted. The server also has its own protection, a
token-bucket admission limit and a stream cap, so even synchronized clients are refused (and back
off) rather than overloading it. Randomness is injected (`backoff.New(policy, rng)`), so tests and
benchmarks are reproducible.

## 6. The health gate: what the statistics do and do not say

Each metric is a proportion: failed requests, requests slower than a histogram bucket boundary,
instances that restarted. Latency is compared as *the fraction over a boundary*, never as a ratio
of interpolated p95 values (a p95 ratio is not a statistical test).

At each of K predeclared looks, per metric:

- **Harm test**: H0 "the canary is not worse than the tolerated degradation" (difference ≤ margin;
  ≤ 0 for guard metrics). Rejecting it means degradation beyond the tolerance → FAIL.
- **Non-inferiority test**: H0 "the canary is worse by at least the margin". Rejecting it for
  every evidence metric means the canary is within tolerance → PASS.
- Anything else is INCONCLUSIVE; after the last look the release halts.

This is a three-zone decision: promote when shown inside the margin, roll back when shown outside
it, halt when it cannot tell. "Not significantly worse" is never treated as "safe".

Choices that keep it honest:

- **Variance.** Requests from one instance are correlated (a bad host makes all its requests bad),
  so the variance is the larger of the binomial variance and a between-instance (cluster-robust)
  estimate, and critical values are Student-t on *instance* degrees of freedom. Under the null
  boundary the binomial part uses the Farrington-Manning restricted maximum-likelihood estimate,
  which stays positive when both cohorts have zero errors, the normal healthy case. (A Wald SPRT on
  the error-rate ratio gets no information from zero-error cohorts and could never promote.)
- **Multiple looks.** Alpha is split across the K looks (Bonferroni), and the harm alpha also
  across metrics. Conservative, but valid without assuming a spending function.
- **Contamination.** Cohorts come from the SDK's own `cfgcanary_sdk_config_info` series filtered
  by config, environment, release and version. Instances that switched version during the
  evaluation window are excluded (their counters mix versions), and stale samples are excluded.
- **Missing evidence.** Too few instances or events, NaN, a query error, or a control cohort that is
  itself degraded is INCONCLUSIVE.
- **Final stage.** If the last ring is 100%, no concurrent control exists, so only hard thresholds
  apply there (the plan warns). Ending the last ring below 100% keeps a **holdout** as control for
  the final comparison; it converts when the release completes.

What the simulation (`bench/results/gate_power.json`) shows, and what to say about small canaries:

- With 50 or 500 canary instances it promotes no-change and inside-margin releases, rolls back a
  +5 point degradation every time, and promotes an at-the-margin release 2–6% of the time
  (the 5% error budget, within Monte Carlo error).
- With 5 instances, or 20 requests per instance per look, it mostly **halts**: it never promotes
  harm, but it cannot produce evidence. Size cohorts for power, not percentages, and use `hard_max`
  for severe failures that should roll back immediately (that is what stopped the demo's 92% error
  rate on the first look).
- Assumptions: requests within an instance are independent given its rate; no time trends;
  heterogeneity across instances is modelled, correlated incidents across instances are not.

## 7. Fail-open versus fail-closed

"Keep the last-known-good" is the right default for routing tables, limits and feature data: an
old value is better than none (`fail_static`). It is the wrong default for an allow-list or an
authorization policy, where serving a revoked permission for hours may be worse than refusing
(`fail_closed`). The mode is declared per config, carried in the signed delivery statement, and
enforced by the SDK: after `max_staleness` without the agent reaching the control plane,
`Usable()` turns false and `GetFresh()` returns `ErrStale`. Staleness is measured with the SDK's
monotonic clock against a counter the agent bumps, so a skewed wall clock cannot make a stale cache
look fresh.

At first start with no cache, the SDK serves a bootstrap value only if the application supplied one
(and, for fail-closed configs, only if it is marked as a deny-all default); otherwise `Open`
returns a typed `*sdk.InitError`. It never returns a zero value while pretending to have a config.

## 8. Defending the scope

- **Why not a feature-flag service?** Flags are small values toggled at runtime. This is staged
  delivery of large structured artifacts with content validation, consumer-side verification and
  last-known-good semantics.
- **Why not just Argo Rollouts?** It gates *deployments*. A config change does not need a new pod,
  and a pod restart is not a content-aware rollout.
- **Why PostgreSQL and not etcd/Raft?** Transactions make "transition + generation + audit" atomic,
  advisory locks give election, and operating it is familiar. The cost: the control plane depends on
  PostgreSQL availability (agents do not).
- **What does the simulator prove?** That the real agent, SDK, server and controller code behaves
  as claimed with N goroutine-instances on one host. It does not prove production readiness,
  multi-host network behaviour, or numbers at other scales.

## 9. Interview questions you should be able to answer

1. What happens if the leader dies halfway through promoting ring 2? (§3)
2. A rollback and a promotion arrive at the same time. Which wins and why? (§3, invariants I7)
3. How does an agent know an update is not stale or meant for another ring? (§2, ARCHITECTURE "Trust")
4. What does the gate do with 5 canaries and very little traffic? (§6)
5. Why is the p95 latency ratio not used? (§6)
6. What happens to a disconnected instance during a kill switch? (README demo, invariants I7)
7. When is fail-closed the right choice? (§7)
