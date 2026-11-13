# Requirements and evidence

Source: *Engineering Projects Worth Building* (research dated 3–4 Nov 2026), section
**P2 · cfgcanary — progressive configuration delivery with blast-radius control**, printed pages
30–36. "Page" is the printed page number.

- **S**: a requirement stated in the source. **E**: an engineering decision added for this build.
- Status: `done` (implemented and verified as shown), `future` (only the source's explicitly
  optional advanced features).

Verification keys (run from the repository root):

| Key | Command | Needs |
|---|---|---|
| **unit** | `make test` (unit, property and fuzz-seed tests, `-race`) | Go |
| **it** | `make test-integration` (`internal/store`, `test/integration`) | Docker |
| **chaos** | `make chaos` (`test/chaos`) | Docker |
| **demo** | `scripts/demo.sh` (Compose stack, `cfgcanary demo`) | Docker |
| **kind** | `scripts/kind-demo.sh` (Helm on kind, checked end to end) | Docker |
| **bench** | `scripts/bench.sh` → `bench/results` | Docker |
| **ci** | GitHub Actions `CI` workflow (lint, generate, vuln, unit, it, chaos, Helm, Compose demo) | GitHub |

All of these passed for the v0.1.0 release candidate (CI run on the release commit; local runs of
demo, kind and bench recorded in their result files).

| # | Kind | Requirement | Page | Implementation | Verification | Status |
|---|---|---|---|---|---|---|
| 1 | S | Register configs, submit versions, create / pause / promote / roll back releases, kill switch; RBAC; audit | 32 | `internal/server/controlsvc.go`, `internal/store`, `internal/auth` | it: `TestManualReleaseThroughEveryRing`, `TestCLI`, `TestTransportAndIdentity`; store `TestKillSwitchWithoutRelease` | done |
| 2 | S | JSON Schema / CUE validation | 32 | `internal/validate/pipeline.go` | unit: `TestPipelineRejections`, `TestCheckPolicy`, `FuzzValidate` | done |
| 3 | S | Size and cardinality limits ("≤ 200 features") | 32 | `internal/validate` (limits before parsing, JSON-pointer cardinality) | unit: `TestPipelineRejections`; it: `TestSubmissionRejectionReachesZeroAgents` | done |
| 4 | S | Relative-change guards (block when size > 2× previous) | 32 | `internal/validate` (`checkRelative`), base = each environment's deployed baseline | unit: `TestRelativeGuardBaseHandling`; it: rejection test (`bad-doubled.json`) | done |
| 5 | S | Run the consumer's own parser in a sandbox (wazero) | 32–33 | `internal/validate/wasm.go` (ABI `cfgcanary-validator/v1`), `examples/ratelimits/validator` | unit: `TestConsumerParserClosesSchemaGap`, `TestSandboxFailuresReject` (timeout, trap, output, memory), `TestSandboxEscapeAttempts` | done |
| 6 | S | PostgreSQL for metadata and the state machine; object store for immutable content-addressed blobs | 32 | `migrations/0001_init.sql`, `internal/store`, `internal/blob` | it: `TestDatabaseInvariantsEnforced`, `TestBlobStoreRoundTrip` | done |
| 7 | S | Ring state machine (PENDING → ring → BAKE → … → COMPLETE, HALTED, ROLLED_BACK) | 32 | `internal/rollout` (pure) | unit: table, property (`TestProperty_RandomInterleavings`) and coverage tests (93.6% statements) | done |
| 8 | S | Single active leader via advisory lock, fencing tokens on writes | 32 | `internal/controller`, `internal/store` (`checkEpoch` in every controller transaction) | it: `TestStaleLeaderWriteRejected`; chaos: `TestLockConnectionLostAndStaleWriteFenced`, `TestLeaderKilledMidRollout` | done |
| 9 | S | Long-lived gRPC watch streams; desired version per ring; resume with last version | 32 | `internal/server/hub.go`, `configsvc.go`; `internal/agent` | it: all release tests; chaos: `TestReconnectStorm`, `TestSlowConsumer` | done |
| 10 | S | Ring assignment by consistent hash of instance ID | 33 | `internal/ring` (versioned hash, frozen policy, pinned groups) | unit: `TestBucketIsPinnedToV1`, `TestProperty_DisjointCumulative` | done |
| 11 | S | Agent: download by hash, verify signature, atomic write (temp → fsync → rename), notify, report, crash-loop detection | 33 | `internal/agent`, `internal/cache`, `sdk` | unit: `TestVerificationFailuresKeepCurrent`, `TestDiskErrorsKeepCurrent`, `TestCrashBetweenObjectAndPointer`; chaos: `TestCrashLoopRollsBack` | done |
| 12 | S | SDK: `Get()` returns last-known-good; validates on load, keeps LKG on failure; `config_version` metric label | 33 | `sdk` | unit: `TestRejectKeepsLKG`, `TestCorruptCacheFallsBack`, `TestColdStartNoCache`, `TestBootstrap`; it: `TestSDKRejectionKeepsLKGAndRollsBack` | done |
| 13 | S | Health gate: canary vs control on errors, latency, restarts, SDK validation failures with sequential tests; missing data halts | 33 | `internal/gate` (three-zone tests over predeclared looks, ADR 0004) | unit: `TestMissingDataNeverPasses`, `TestThreeZones`, `TestRestrictedMLEMaximizesConstrainedLikelihood`; it: `TestGatedReleaseAndCanaryRollback`; chaos: `TestMetricsOutageNeverPromotes`; bench: `gate_power.json` | done |
| 14 | S | Kill switch: previous version on every ring immediately; break-glass, audited | 33 | `rollout.KillWithoutRelease`, `store.KillSwitch`, `ControlService.KillSwitch` | unit: `TestKillSwitch`; it: `TestKillSwitchWithoutRelease`, `TestCLI` (denied without break-glass); demo (kill switch under partition) | done |
| 15 | S | API: `ConfigService.Watch`; `POST /v1/configs/{name}/versions`, `POST /v1/releases`, `:promote`, `:pause`, `:rollback`, `GET /v1/releases/{id}` | 33 | `api/cfgcanary/v1/cfgcanary.proto`, gateway, OpenAPI | it: `TestTransportAndIdentity` (REST); ci: generated code must match | done |
| 16 | S | mTLS between agents and control plane; HTTP/2 flow control | 33 | `internal/auth/tls.go` (reloadable certificates), gRPC transport | it: unknown CA, expired certificate, wrong key usage, missing identity | done |
| 17 | S | Reconnect with exponential backoff and full jitter; server-side admission control | 33 | `internal/backoff`, `internal/server` (token bucket and stream cap) | unit: `TestFullJitterBounds`; chaos: `TestReconnectStorm`; bench: `storm.json` | done |
| 18 | S | Blobs from the object store; control plane off the data path | 33 | presigned GETs to a private bucket; agents restricted to allowed origins | it: `TestTamperedArtifactRefused`, `TestRefreshArtifactEntitlement` | done |
| 19 | S | Blobs garbage-collected after N releases; agent-local cache; append-only audit log | 33 | `store.GC` (`retain_versions`), `cache.GC`, audit trigger | it: `TestGCRaceWithReleaseCreation`, `TestDatabaseInvariantsEnforced`; unit: `TestCacheBounded` | done |
| 20 | S | SPIFFE-style identities, signed artifacts (ed25519), RBAC on promote and kill switch, WASM validators, no secrets in payloads | 33 | `internal/auth`, `internal/sign`, `internal/validate`; documented in SECURITY.md | unit: `TestParse`, `TestRBAC`, `TestVerifyUpdate`; it: `TestCLI`, `TestTransportAndIdentity` | done |
| 21 | S | docker compose: Postgres, MinIO, Prometheus, Grafana, control plane, N simulated agents | 33 | `deploy/compose` (S3 store: maintained MinIO fork, ADR 0003; plus Toxiproxy) | demo; ci: Compose demo job | done |
| 22 | S | Kubernetes mode: kind + Helm, ConfigRelease CRD writing per-ring ConfigMaps (optional in the source; required here) | 33–34 | `internal/kube`, `deploy/helm/cfgcanary`, `deploy/kind` | kind (install, ring rollout, rejection, upgrade, uninstall); ci: Helm lint | done |
| 23 | S | Failure modes: control plane down → LKG; leader loss → fencing; metrics missing → halt; bad config → SDK reject or rollback; reconnect storm → jitter + admission; clock skew → generations, not timestamps | 33 | see [invariants.md](invariants.md) | chaos suite (12 scenarios, 0 client crashes, `chaos.json`) | done |
| 24 | S | Tests: state machine; property tests; 200–1,000 simulated agents; chaos (leader kill, Toxiproxy partition, oversized or blank-field config); load tests | 33 | `internal/rollout`, `test/integration`, `test/chaos`, `bench` | unit, it, chaos, bench | done |
| 25 | S | Audit trail, RBAC, per-ring convergence metrics, dry-run releases, release diffs in the CLI, runbooks | 34 | CLI (`releases plan`, `diff`, `audit`), controller metrics, [runbooks.md](runbooks.md) | it: `TestManualReleaseThroughEveryRing` (dry run writes nothing), `TestCLI` | done |
| 26 | S | Demo: Cloudflare-style bad file confined; propagation at 1,000 agents published; failure model documented | 34 | see row 32; `bench/results/scale.json` | demo; bench | done |
| 27 | S | Metrics: blast radius, time to detect / roll back, propagation p50/p99 at 100 / 1,000 / 5,000, reconnect peak with vs without jitter, client crashes, CPU/memory per 1k streams, state-machine coverage | 36 | `bench` | bench: `blast_radius.json`, `scale.json`, `storm.json`, `chaos.json`, summary | done |
| 28 | S | Fail-open vs fail-closed documented per config type | 34 | `failure_mode` per config, carried in the delivery statement; SDK `Usable`/`GetFresh` | unit: `TestStalenessFailClosed`, `TestBootstrap` | done |
| 29 | S | Apache-2.0; README: incident pattern → five-minute compose demo → architecture → guarantees → benchmarks | 36 | `LICENSE`, `README.md` | review | done |
| 30 | S | Optional advanced: Argo Rollouts analysis integration, OpenFeature provider, multi-region rings, automated bake-time tuning | 34 | not built | – | future |
| 31 | E | Rollback is a newer generation pointing at an older version; ordering by generations and epochs only | – | `internal/rollout`, database trigger | unit: `TestRollbackIsNewGenerationOlderVersion`; it: `TestStatusOldGenerationIgnored` | done |
| 32 | E | The source's "oversized config reaches only ring 0" is resolved honestly: ingress-detectable violations reach zero agents; a schema-valid, runtime-harmful config is caught by the canary gate | 34 | validation pipeline; gate | it: `TestSubmissionRejectionReachesZeroAgents`, `TestGatedReleaseAndCanaryRollback`; bench: `blast_radius.json` | done |
| 33 | E | Delivery statements signed separately from artifacts, naming generation, environment and ring range | – | `internal/sign` | unit: `TestVerifyUpdate` (scope, ring range, purpose confusion) | done |
| 34 | E | Holdout cohort as final control, or an explicit hard-thresholds-only final stage | – | `policy.Normalize` (`final_stage`), `gate.Params.HardOnly` | unit: `TestHardThreshold`, `TestNormalizeRejects` | done |
| 35 | E | Pinned groups: ring assignment by certificate group (Kubernetes) | – | `internal/ring`, `auth.Identity.MayClaim` | unit: `TestPinnedGroupOverridesHash`, `TestClaims`; kind | done |
| 36 | E | Idempotency keys on mutating commands | – | `internal/store` (`request_id`) | it: `TestDuplicateCommandsIdempotent` | done |
| 37 | E | Naive push-to-all and Reloader-style rolling restart baselines, labelled | – | `bench/blast_test.go` | bench: `blast_radius.json` | done |
| 38 | E | Three-zone gate (harm tested against the margin) after the power simulation showed spurious rollbacks | – | `internal/gate`, ADR 0004 | unit: `TestThreeZones`; bench: `gate_power.json` | done |
| 39 | E | Guard-only gate metrics for rare events; at least one evidence metric required | – | `internal/gate`, `internal/policy` | unit: `TestGuardMetric` | done |
| 40 | E | Agents re-report status on every reconciliation (repairs acknowledgements lost in a database outage) | – | `internal/agent` | chaos: `TestDatabaseOutage` | done |
| 41 | E | OpenTelemetry trace propagation over gRPC, OTLP export when configured | – | `internal/telemetry` | [observability.md](observability.md) | done |
| 42 | E | Release archives for Linux/macOS amd64/arm64 verified on native runners; multi-arch image | – | `scripts/dist.sh`, `.github/workflows/release.yml` | release workflow (`verify` matrix) | done |
