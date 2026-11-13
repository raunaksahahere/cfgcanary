# Changelog

All notable changes are recorded here. The project follows [Semantic Versioning](https://semver.org/);
see [docs/compatibility.md](docs/compatibility.md) for what is stable in 0.x.

## [0.1.0] - 2026-11-13

Initial alpha release.

### Added
- Control plane (`cfgcanary server`): configs, immutable content-addressed versions, structural
  diffs, release plans (dry run), releases with ring-by-ring rollout, manual promotion, pause,
  resume, rollback and an audited break-glass kill switch; gRPC API with a REST gateway and
  OpenAPI document.
- Validation pipeline: byte, depth and cardinality limits, strict JSON (duplicate keys, UTF-8),
  JSON Schema 2020-12, CUE, relative-change guards against each environment's deployed baseline,
  and the consumer's own parser run as a WASM module in a wazero sandbox
  (ABI `cfgcanary-validator/v1`).
- PostgreSQL authority: atomic transitions with audit and generation bumps, immutability and
  monotonic-generation triggers, append-only audit log, idempotent commands, race-safe garbage
  collection of versions and S3 blobs.
- Release controller: advisory-lock leadership with a fencing epoch checked in every write,
  durable state machine (`PENDING`, `ROLLING`, `BAKING`, `HALTED`, `ROLLING_BACK`, `ROLLED_BACK`,
  `COMPLETE`, pause flag), automatic rollback on SDK rejections, crash loops or gate failure.
- Prometheus health gate: canary versus control per metric (ratio, histogram over a bucket,
  instance events), harm and non-inferiority tests with instance-clustered variance and
  Farrington-Manning restricted variance, alpha split over predeclared looks, guard metrics,
  hard limits, holdout cohort; missing or stale evidence never promotes.
- Agent: watch streams with full-jitter reconnects and periodic snapshot reconciliation,
  ed25519 artifact and delivery statements, origin allow-list, crash-consistent cache with
  last-known-good, bounded status reporting, file-source mode for Kubernetes.
- Go SDK: immutable snapshots, typed `InitError`, validated bootstrap values, parser rejection
  keeps last-known-good, crash-loop quarantine, monotonic staleness with fail-closed mode,
  `config_version` metrics.
- mTLS with SPIFFE-style identities, server-side RBAC (viewer, submitter, operator, releaser,
  admin, breakglass), certificate reload without restart.
- Kubernetes: `ConfigRelease` CRD and controller, per-ring ConfigMap projection, Helm chart with
  values schema, kind example.
- Compose demonstration stack with Grafana dashboard, `cfgcanary demo`, Toxiproxy fault proxy and
  50 simulated instances.
- Integration, chaos, property and fuzz tests; benchmark harness and results.

[0.1.0]: https://github.com/raunaksahahere/cfgcanary/releases/tag/v0.1.0
