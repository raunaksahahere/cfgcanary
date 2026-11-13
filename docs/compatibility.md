# Versioning and compatibility

cfgcanary follows [Semantic Versioning](https://semver.org/). **v0.1.0 is an initial alpha
release**: it is complete and tested for the scope in [requirements.md](requirements.md), but it
has not run in production, and interfaces may change in 0.x minor releases.

What is versioned, and how:

| Surface | Promise in 0.x |
|---|---|
| gRPC/REST API `cfgcanary.v1` | Fields and methods are only added within `v1`; removals or semantic changes would be a new API version |
| Signed statement formats | Versioned by their signing prefix (`cfgcanary/artifact/v1`, `cfgcanary/delivery/v1`); a new format gets a new prefix and agents accept both during a transition |
| Agent cache layout and SDK state files | Readable by the next minor release; a change ships a migration in the agent |
| Ring hash (`cfgcanary/ring/v1`) | Never changes within a release line, because it would reshuffle cohorts |
| CLI flags, exit codes, JSON envelope `cfgcanary/v1` | May change in a minor release, with a CHANGELOG entry |
| Go SDK (`github.com/raunaksahahere/cfgcanary/sdk`) | May change in a minor release, with a CHANGELOG entry |
| PostgreSQL schema | Migrated forward automatically on start; downgrades are not supported (restore a backup) |
| Helm chart and CRD `cfgcanary.io/v1alpha1` | Alpha; may change in a minor release |

**Upgrade order**: control plane first (one replica at a time), then agents and applications.
Agents ignore unknown fields, and the server accepts reports from older agents.

**Supported platforms**: Linux amd64 is tested end to end; Linux arm64 and macOS amd64/arm64
binaries are cross-compiled (see [usage.md](usage.md#platform-support)).
