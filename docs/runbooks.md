# Runbooks

Operational procedures for cfgcanary v0.1.0. Commands assume the CLI is configured for your
control plane (see [usage.md](usage.md)). Every privileged action below is recorded in
`cfgcanary audit`.

## Emergency: a bad config is out

1. **Is a release still active?** `cfgcanary releases list --config NAME --active`
   - Yes: `cfgcanary releases rollback ID --reason "INC-123"`. Every ring returns to the
     baseline in one new generation; pending promotions can no longer commit.
   - No (the change already completed), or you are unsure: use the kill switch (needs the
     `breakglass` role): `cfgcanary kill-switch NAME --environment prod --reason "INC-123"`.
     It reverts the target to its previous known-good version, freezes it (no new releases), and
     creates an emergency release that tracks convergence.
2. **Watch convergence**: `cfgcanary releases status ID --watch`. The rollback line separates
   *confirmed* (instance reported serving the fallback), *pending* (reachable, not yet confirmed)
   and *unreachable* (not seen within the reachable window; a sample is listed). Connected
   instances normally confirm within a second; unreachable ones apply the fallback when they
   reconnect, and the counts update live.
3. **Do not declare recovery from the database row.** Recovery is the rollback line reading
   `converged`, plus your service metrics.
4. After the incident: `cfgcanary unfreeze NAME --reason "..."` (admin).

## A release is HALTED

`cfgcanary releases status ID` shows the reason:

| Reason | Meaning | Action |
|---|---|---|
| `gate_inconclusive` | The declared looks ended without evidence either way (missing or stale metrics, too few instances or requests, a degraded control) | Check Prometheus and the gate evidence (`gate ring N:` lines). Fix the metrics, then `releases resume ID` (fresh evidence is gathered), or roll back. Only with good reason: `releases promote ID --override --reason ...` (break-glass, audited) |
| `convergence_timeout` | Too few reachable ring members accepted within `rollout_timeout` | `cfgcanary instances --environment E` shows `FAILED` reasons (`signature`, `scope`, `digest`, `download`, `disk`, `untrusted_url`). Fix the cause, then resume or roll back |
| `too_few_reachable` | Fewer than `min_reachable_fraction` of the ring is connected | Check agent connectivity and the control plane's stream count |
| `empty_cohort` | A ring has no members | Adjust the rings or the fleet; roll back |
| `gate_fail` with `on_fail: halt` | The gate showed degradation and the policy says halt | Roll back unless you understand why |

A halted release never advances on its own.

## A release is stuck ROLLING or BAKING

- `ROLLING` waits for `acceptance_threshold` of *reachable* ring members to report `ACCEPTED`.
  An instance whose network path is stalled but whose TCP connection is still open looks
  reachable until gRPC keepalive closes it (about 40 s with defaults), so a 100% acceptance
  threshold can wait that long for it. Prefer a threshold below 1.0 for large rings.
- In Kubernetes mode, kubelet refreshes ConfigMap volumes periodically (up to about a minute), so
  each promotion can take that long to reach pods.
- `BAKING` waits for `min_bake`, then looks every `look_interval`. Paused time does not count.
- Check the controller: `cfgcanary info` shows whether the replica you reached leads and its epoch;
  `cfgcanary_controller_is_leader` and `cfgcanary_controller_reconcile_errors_total` in Prometheus.

## Metrics outage (Prometheus down or scrapes failing)

Gated releases cannot pass while evidence is missing: each look is `INCONCLUSIVE`, and after the
last look the release halts. Nothing is promoted by the passage of time. Agents and applications are
unaffected. When Prometheus is back, `releases resume ID`.

## Control plane, database or object-store outage

- **Agents and applications keep serving their verified cache** (static stability). Nothing to do
  for them; they reconnect with jittered backoff and reconcile when the control plane returns.
- **Control plane down**: no new releases or transitions. Restart it; state is in PostgreSQL.
- **PostgreSQL down**: the API returns `UNAVAILABLE`; the controller loses its lock session and
  stops leading until the database returns. Acknowledgements the server could not store are
  re-sent by agents within one reconcile interval.
- **Object store down**: submissions fail without storing anything; agents report `FAILED`
  (`download`) for new versions and keep their current config, then retry on reconciliation.

## Graceful shutdown and upgrades

- `SIGTERM`/`SIGINT` on `cfgcanary server`: watch streams are ended with `UNAVAILABLE` (agents
  reconnect elsewhere with backoff), in-flight RPCs drain for up to 15 s, the controller releases
  its lock, buffered acknowledgements are flushed.
- **Rolling upgrade**: run at least two replicas behind the Service, upgrade one at a time. Only one
  replica leads the controller; the next one takes over with a newer epoch within a few seconds.
- **Interrupted upgrade**: migrations run in a transaction each, under an advisory lock, and are
  recorded in `schema_migrations`; a replica that crashes mid-migration leaves the previous schema,
  and the next start retries. v0.1.0 has one migration.
- Agents and SDKs only add fields within `v1`; upgrade the control plane first, then agents.

## Backups and recovery

- **PostgreSQL** holds everything that matters (configs, versions, releases, desired state,
  acknowledgements, audit). Back it up with your usual tooling (`pg_dump`, WAL archiving).
- **Object store**: blobs are content-addressed and immutable; back up the bucket or enable
  versioning. A version whose blob is missing cannot be delivered; agents report `download` and
  keep serving.
- **Signing keys and CA**: back up offline. Losing the delivery key means agents cannot verify new
  generations until they trust a new key (see rotation).
- **Restore**: restore PostgreSQL, then the bucket, then start the control plane. Restoring an
  older database makes generations go backwards relative to agents; agents ignore updates whose
  generation is not newer, so after an old restore issue a new release or kill switch to move
  generations past what agents hold.

## Certificate and key rotation

- **Server and client certificates** are re-read from disk when they change (checked at most once
  per second), for both the server and agents: replace the files (Kubernetes: update the Secret)
  without restarting. To rotate the **CA**, put old and new CA certificates in the bundle, reissue
  leaf certificates, then remove the old CA.
- **Signing keys**: (1) generate a new key, (2) add its public key to every agent's trust bundle
  (`trust.json`) and roll that out, (3) switch the server to the new key (`--artifact-key` /
  `--delivery-key`) and restart replicas, (4) remove the old key from bundles once no cached entry
  needs it. Cached entries are re-verified on load, so do not drop an old key while instances may
  still restart from content it signed.
- **RBAC policy**: edit the file and send `SIGHUP` to each replica (Kubernetes: update the Secret
  and restart, or roll the Deployment). A broken file is rejected and the old policy kept.

## Retention and garbage collection

The leader runs garbage collection every 10 minutes. It keeps, per config, the newest
`retain_versions` versions (default 10), anything referenced by a target (baseline, candidate,
previous known-good) or an active release, and anything younger than one hour. Purged versions
keep their metadata row; their blobs are deleted once nothing references them. Agents keep their
current, previous and last-known-good objects plus a few recent ones.

## Development stack (Compose)

- `scripts/demo.sh down` removes containers **and** volumes. If you delete only the credentials
  volume, PostgreSQL keeps its data with the old password and the server cannot connect; remove
  both (`docker compose -f deploy/compose/compose.yaml down -v`).
- Cut the agents off: `docker compose -f deploy/compose/compose.yaml exec toxiproxy /toxiproxy-cli toggle agents`.
