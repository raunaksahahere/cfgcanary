# Requirements and evidence

Source: *Engineering Projects Worth Building* (research dated 3–4 Nov 2026), section
**P2 · cfgcanary — progressive configuration delivery with blast-radius control**, printed pages
30–36. "Page" below is the printed page number.

Two kinds of rows:

- **S** – a requirement stated in the source.
- **E** – an engineering decision added for this build (the source is silent or only implies it).

Status values: `done` (implemented and verified by the command shown), `partial` (what is missing
is stated), `future` (explicitly deferred; only the source's "optional advanced features").

Verification commands are run from the repository root. `make test` needs only Go;
`make test-integration` and `make chaos` need Docker (PostgreSQL, the S3 store, Prometheus).

<!-- The table below is maintained by hand and checked at release time. -->

| # | Kind | Requirement | Page | Implementation | Verification | Status |
|---|---|---|---|---|---|---|
| 1 | S | Register configs, submit versions, create / pause / promote / roll back releases, kill switch; RBAC; audit | 32 | `internal/server`, `internal/store`, `internal/auth` | planned | planned |
| 2 | S | JSON Schema / CUE validation | 32 | `internal/validate` | planned | planned |
| 3 | S | Size and cardinality limits ("≤ 200 features") | 32 | `internal/validate/limits.go` | planned | planned |
| 4 | S | Relative-change guards (block when size > 2× previous) | 32 | `internal/validate/relative.go` | planned | planned |
| 5 | S | Run the consumer's own parser in a sandbox (wazero) | 32, 33 | `internal/validate/wasm.go`, `examples/` | planned | planned |
| 6 | S | PostgreSQL for metadata and the release state machine; object store for immutable content-addressed blobs | 32 | `internal/store`, `internal/blob`, `migrations/` | planned | planned |
| 7 | S | Ring state machine PENDING → RING0 → BAKE → … → COMPLETE, HALTED, ROLLED_BACK | 32 | `internal/rollout` | `go test ./internal/rollout` | done |
| 8 | S | Single active leader via advisory lock, fencing tokens on writes | 32 | `internal/controller`, `internal/store` | planned | planned |
| 9 | S | Long-lived gRPC watch streams; each agent receives the desired version for its ring; resume with `last_version` | 32 | `internal/server/hub.go`, `internal/agent` | planned | planned |
| 10 | S | Ring assignment by consistent hash of instance ID | 33 | `internal/ring` | `go test ./internal/ring` | done |
| 11 | S | Agent: download by hash, verify signature, atomic write (temp → fsync → rename), notify app, report status, crash-loop detection | 33 | `internal/agent`, `sdk` | planned | planned |
| 12 | S | SDK: `Get()` always returns last-known-good; validates on load, keeps LKG on failure; `config_version` metric label | 33 | `sdk` | planned | planned |
| 13 | S | Health gate: canary vs control on error rate, latency, restarts, SDK validation failures, sequential tests; missing data → halt, never promote | 33 | `internal/gate` | planned | planned |
| 14 | S | Kill switch: desired = previous on every ring immediately; break-glass, audited | 33 | `internal/rollout`, `internal/server` | planned | planned |
| 15 | S | API: `ConfigService.Watch`, `POST /v1/configs/{name}/versions`, `POST /v1/releases`, `:promote`, `:pause`, `:rollback`, `GET /v1/releases/{id}` | 33 | `api/cfgcanary/v1/cfgcanary.proto` | planned | planned |
| 16 | S | mTLS between agents and control plane; HTTP/2 flow control | 33 | `internal/auth`, `internal/server` | planned | planned |
| 17 | S | Reconnect with exponential backoff and full jitter; server-side admission control | 33 | `internal/agent/backoff.go`, `internal/server/admission.go` | planned | planned |
| 18 | S | Blobs come from the object store; control plane off the data path | 33 | `internal/blob` (presigned URLs) | planned | planned |
| 19 | S | Immutable blobs garbage-collected after N releases; agent-local cache; append-only audit log | 33 | `internal/blob/gc.go`, `internal/agent/cache.go`, `migrations/` | planned | planned |
| 20 | S | Per-agent identity (SPIFFE-style certificates), signed artifacts (ed25519), RBAC on promote and kill switch, WASM-sandboxed validators, no secrets in payloads | 33 | `internal/auth`, `internal/sign`, `internal/validate` | planned | planned |
| 21 | S | docker compose for development: Postgres, MinIO, Prometheus, Grafana, control plane, N simulated agents | 33 | `deploy/compose` | planned | planned |
| 22 | S | Kubernetes mode (source: optional; this build: required): kind + Helm, ConfigRelease CRD writing per-ring ConfigMaps | 33, 34 | `internal/kube`, `deploy/helm`, `deploy/kind` | planned | planned |
| 23 | S | Failure modes: control plane down → LKG; leader loss → fencing; metrics missing → halt; bad config → SDK reject or health rollback; reconnect storm → jitter + admission; clock skew → monotonic versions | 33 | see [invariants.md](invariants.md) | planned | planned |
| 24 | S | Tests: state-machine unit tests; property tests (versions never regress, no agent applies a version its ring is not permitted); 200–1,000 simulated agents; chaos (kill leader, Toxiproxy partition, oversized or blank-field config); propagation load tests | 33 | `internal/rollout`, `test/`, `bench/` | planned | planned |
| 25 | S | Production features: audit trail, RBAC, per-ring convergence metrics, dry-run releases, release diffs in the CLI, runbooks | 34 | `cmd/cfgcanary`, `docs/runbooks.md` | planned | planned |
| 26 | S | Demo: a Cloudflare-style bad file confined to ring 0 with automatic rollback; propagation latency at 1,000 simulated agents published; docs explain the failure model | 34 | `deploy/compose`, `bench/` | planned | planned |
| 27 | S | Metrics: blast radius, time to detect / roll back, propagation p50/p99 at 100 / 1,000 / 5,000, reconnect peak with vs without jitter, client crashes across chaos, CPU/memory per 1k streams, state-machine coverage | 36 | `bench/` | planned | planned |
| 28 | S | Fail-open vs fail-closed documented per config type | 34 | [invariants.md](invariants.md#fail-open-and-fail-closed-per-config-type), `sdk` | planned | planned |
| 29 | S | Apache-2.0; README order: incident pattern → five-minute compose demo → architecture → guarantees and non-guarantees → benchmarks | 36 | `README.md`, `LICENSE` | planned | planned |
| 30 | S | Optional advanced: Argo Rollouts analysis, OpenFeature provider, multi-region rings, automated bake-time tuning | 34 | not built | – | future |
| 31 | E | Rollback is a newer generation pointing at an older version; ordering uses generations and epochs only | – | `internal/rollout` | `go test ./internal/rollout` | done |
| 32 | E | The source's "oversized config only reaches ring 0" is split in two: violations an ingress validator knows are blocked before rollout (zero agents); a schema-valid, operationally harmful config is caught by the canary gate | 34 | `test/chaos` | planned | planned |
| 33 | E | Delivery statements signed separately from artifacts, naming generation, environment and ring range | – | `internal/sign` | `go test ./internal/sign` | done |
| 34 | E | Holdout cohort as control for the final comparison, or an explicit hard-thresholds-only final stage | – | `internal/rollout`, `internal/gate` | planned | planned |
| 35 | E | Pinned groups for explicit ring assignment by certificate group (used by Kubernetes ring ConfigMaps) | – | `internal/ring`, `internal/auth` | planned | planned |
| 36 | E | Idempotency keys on mutating commands | – | `internal/store` | planned | planned |
| 37 | E | Naive push-to-all and Reloader-style rolling-restart baselines as labelled models | – | `bench/` | planned | planned |
