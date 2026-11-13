# Protocol and API

The contract is [`api/cfgcanary/v1/cfgcanary.proto`](../api/cfgcanary/v1/cfgcanary.proto). Go
stubs (`api/gen`) and the OpenAPI document
([`api/openapi/cfgcanary/v1/cfgcanary.swagger.json`](../api/openapi/cfgcanary/v1/cfgcanary.swagger.json))
are generated from it by `make gen` with pinned tool versions; CI fails if they drift.

Two services share one mTLS gRPC listener (default `:8443`):

- **`ConfigService`**, for agents: `Watch`, `GetDesired`, `ReportStatus`, `RefreshArtifact`.
- **`ControlService`**, for operators and automation; also served as REST/JSON by a gateway on its
  own mTLS listener (default `:8444`).

## Authentication and authorization

Both listeners require a client certificate signed by the configured CA, with the client-auth
extended key usage and exactly one URI SAN in the trust domain:

| SPIFFE ID | May use |
|---|---|
| `spiffe://TD/env/ENV/agent/ID` | `ConfigService` as instance `ID` in environment `ENV` |
| `spiffe://TD/env/ENV/group/G` | `ConfigService` as any instance `G/<suffix>`; policy may pin group `G` to a ring |
| `spiffe://TD/user/NAME`, `spiffe://TD/service/NAME` | `ControlService`, with the roles the RBAC file grants |

Authorization is checked at the top of every `ControlService` method, so gRPC and REST behave
identically. Refused attempts return `PERMISSION_DENIED` (HTTP 403) and are written to the audit
log with the caller's identity and no request payload.

## Watch

```protobuf
rpc Watch(WatchRequest) returns (stream ConfigUpdate);

message WatchRequest {
  string instance_id = 1;                  // must match the certificate
  map<string, string> labels = 2;          // informational only; never affects the ring
  repeated Subscription subscriptions = 3; // {config, last_version, last_generation}
  string agent_version = 4;
}
```

1. The server checks the identity, then admission (token bucket and stream cap). A refused stream
   gets `RESOURCE_EXHAUSTED` before any header; the agent backs off with full jitter.
2. On admission the server records the instance, sends a `cfgcanary-admitted` header, and adds the
   instance to an active release's cohort if it joined late.
3. For each subscription whose current generation is newer than `last_generation`, it sends one
   `ConfigUpdate`. After that, it sends one whenever the target's desired state changes.

```protobuf
message ConfigUpdate {
  Kind kind = 1;            // DESIRED, or NO_CONFIG when the target has nothing for this ring
  string config = 2;
  string environment = 3;
  int64 generation = 4;     // orders everything; a rollback is a newer generation
  int64 version = 5;
  string sha256 = 6;
  int64 size = 7;
  string url = 8;           // short-lived presigned GET in the private bucket
  google.protobuf.Timestamp url_expires = 9;
  SignedStatement artifact = 10;  // binds bytes to config, version, environments
  SignedStatement delivery = 11;  // authorizes this generation for a ring range
  int32 ring = 12;          // the ring the server assigned to this instance
  string release_id = 13;
}
```

**Delivery semantics.** Each stream has one coalescing "dirty" slot per subscription. A newer
change replaces an unsent older one, so a slow client is never sent a backlog and the server keeps
no per-stream queue. Intermediate generations may be skipped; the agent only needs the latest.
If a send blocks on HTTP/2 flow control longer than the send timeout (default 10 s), the stream is
closed. Correctness does not depend on the stream: agents call `GetDesired` on connect and every
reconcile interval (default 30 s, jittered) and apply anything newer.

**Ring assignment** is computed by the server from the authenticated instance ID and the release's
frozen policy: `bucket = BE64(SHA-256("cfgcanary/ring/v1" ‖ 0x00 ‖ salt ‖ 0x00 ‖ instance)[0:8]) mod
10000`, compared with the rings' cumulative basis points. Pinned groups override hashing.

## Signed statements

```protobuf
message SignedStatement { bytes payload = 1; bytes signature = 2; string key_id = 3; }
```

`payload` is JSON; `signature` is ed25519 over `"cfgcanary/<purpose>/v1\n" ‖ payload`;
`key_id` is the first 16 hex characters of SHA-256 of the public key. Verifiers check the
signature over the exact bytes received, then decode.

| Purpose | Fields |
|---|---|
| `artifact` | `config`, `version`, `sha256`, `size`, `environments` |
| `delivery` | `config`, `environment`, `generation`, `version`, `sha256`, `release_id`, `ring_lo`, `ring_hi`, `failure_mode`, `max_staleness_seconds` |

An agent applies an update only if both statements verify against keys trusted *for that
purpose*, the artifact covers the environment, the two statements agree on version and digest,
its ring lies in `[ring_lo, ring_hi]`, and the downloaded bytes match the signed size and digest.
For the candidate, the range is `[0, active_ring]`; for the baseline during a release,
`[active_ring + 1, holdout]`.

Trust bundle (`trust.json`):

```json
{"keys": [{"key_id": "3f1c…", "public_key": "<base64 ed25519>", "purposes": ["artifact"]},
          {"key_id": "9a02…", "public_key": "<base64 ed25519>", "purposes": ["delivery"]}]}
```

## ReportStatus

```protobuf
message InstanceConfigStatus {
  string config = 1; int64 generation = 2; int64 version = 3;
  ApplyState state = 4;      // APPLIED, ACCEPTED, REJECTED, FAILED, CRASH_LOOP
  string reason = 5;         // machine-readable, never payload content
  int64 serving_version = 6; int64 serving_generation = 7;
}
```

The server stores the latest status per (instance, config), ordered by generation, then by
lifecycle stage (`APPLIED` < `ACCEPTED`/`REJECTED` < `CRASH_LOOP`), never by arrival time. A
report for a generation the target has not reached is ignored. Reports are batched into
PostgreSQL; when the queue is full the RPC returns `RESOURCE_EXHAUSTED`. Agents re-send their
latest status on every new connection and every reconciliation, which repairs acknowledgements
lost to a database outage. `REJECTED` and `CRASH_LOOP` are also recorded as per-version incidents,
so they still count after the instance's latest status has moved on.

## RefreshArtifact

Returns a fresh presigned URL for `(config, version)` only if that version is what the caller's
ring should run right now. Content identity (digest and signatures) never changes.

## ControlService (REST mapping)

| Method | HTTP | Permission |
|---|---|---|
| `RegisterConfig` | `POST /v1/configs` | admin |
| `GetConfig`, `ListConfigs` | `GET /v1/configs/{name}`, `GET /v1/configs` | read |
| `SubmitVersion` | `POST /v1/configs/{name}/versions` (`payload` base64 in JSON; `dry_run`) | submit |
| `GetVersion`, `ListVersions`, `GetVersionPayload` | `GET /v1/configs/{name}/versions[/{version}[/payload]]` | read |
| `DiffVersions` | `GET /v1/configs/{name}/diff?from=&to=` | read |
| `CreateRelease` | `POST /v1/releases` (`dry_run` needs only read) | submit |
| `GetRelease`, `ListReleases` | `GET /v1/releases/{id}`, `GET /v1/releases` | read |
| `PromoteRelease` | `POST /v1/releases/{id}:promote` (`override` needs break-glass and a reason) | operate |
| `PauseRelease`, `ResumeRelease`, `RollbackRelease` | `POST /v1/releases/{id}:pause|:resume|:rollback` | operate |
| `KillSwitch` | `POST /v1/targets/{config}/{environment}:kill` (reason required) | break-glass |
| `UnfreezeTarget` | `POST /v1/targets/{config}/{environment}:unfreeze` | admin |
| `GetTarget`, `GetRingProjection` | `GET /v1/targets/{config}/{environment}[/rings]` | read |
| `ListInstances`, `ListAudit`, `GetServerInfo` | `GET /v1/instances`, `GET /v1/audit`, `GET /v1/info` | read |

Mutating commands accept a `request_id`: a retry with the same ID from the same identity returns
the original result; reusing an ID for a different command is `ABORTED`. Commands without one are
guarded by state (for example `expected_ring` on promote) and return `FAILED_PRECONDITION` when
stale. List methods page with `page_size` (max 500) and an opaque `page_token`.

Error codes: `INVALID_ARGUMENT` (bad input), `NOT_FOUND`, `ALREADY_EXISTS` (another active release),
`FAILED_PRECONDITION` (state does not allow it: paused, bake not elapsed, gate not passed, frozen),
`ABORTED` (concurrent change or stale epoch; retry after re-reading), `PERMISSION_DENIED`,
`UNAUTHENTICATED`, `RESOURCE_EXHAUSTED` (admission or queue limits), `UNAVAILABLE` (storage down).

Limits: messages up to the server's payload limit plus 1 MiB (gRPC) or 4/3 of it plus 1 MiB
(REST, base64); `max_payload_bytes` defaults to 4 MiB and each config sets its own lower limit.

## Compatibility

Within `cfgcanary/v1` fields are only added. Statement formats are versioned by their signing
prefix (`/v1`). See [compatibility.md](compatibility.md).
