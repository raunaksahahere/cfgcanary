# Contributing

Thanks for helping. cfgcanary is a safety tool, so changes are judged first on whether they keep
the promises in [docs/invariants.md](docs/invariants.md).

## Getting started

```bash
make help              # every target
make test              # unit, property and fuzz-seed tests with -race (needs Go 1.27)
make test-integration  # starts PostgreSQL, S3, Prometheus, Toxiproxy in Docker
make chaos             # fault-injection suite
make lint              # gofmt, vet, staticcheck
make gen               # regenerate protobuf, gateway and OpenAPI code after editing the .proto
```

## Expectations for a change

- **Tests that try to break it.** A behaviour change needs a test at the level where it can fail
  for real: a table or property test for `internal/rollout` and `internal/gate`, an integration or
  chaos test for anything involving PostgreSQL, S3, the network or the disk. Do not mock away the
  failure you are claiming to handle.
- **Invariants first.** If a change touches ordering, exposure, leadership, consumer safety or
  gating, update [docs/invariants.md](docs/invariants.md) and name the test that enforces it.
- **Decisions.** Record non-obvious design choices as a new ADR in [docs/decisions](docs/decisions).
- **No secrets.** Never commit keys, certificates or credentials; `.dev/` is ignored for a reason.
- **Measured claims only.** Numbers in docs must come from `bench/results`.
- Conventional commit messages (`feat:`, `fix:`, `docs:`, `test:`, `ci:`) and a short rationale.

## Good first areas

Validators for other formats, SDKs in other languages, sources (Git, OCI) and sinks, dashboards,
and documentation.

## Code of conduct

Be respectful and constructive. Harassment is not tolerated.
