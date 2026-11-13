# Observability

## Metrics

The server exposes Prometheus metrics on `--metrics-listen` (default `127.0.0.1:9464`, plain HTTP,
with `/healthz` and `/readyz`). Agents expose theirs on `--metrics-listen`; the SDK registers its
series with the registry you pass. The Compose stack provisions a Grafana dashboard
([`deploy/compose/grafana/dashboards/cfgcanary.json`](../deploy/compose/grafana/dashboards/cfgcanary.json))
built only from these series.

| Metric | Labels | Meaning |
|---|---|---|
| `cfgcanary_release_state` | config, environment, state | 1 for the latest release's state |
| `cfgcanary_release_active_ring` | config, environment | highest ring exposed (-1: none) |
| `cfgcanary_release_ring_instances` | config, environment, ring, status | eligible, reachable, applied, accepted, rejected, failed, crash_loop per ring of the active release (convergence) |
| `cfgcanary_release_rollback_instances` | config, environment, status | exposed, confirmed, pending, unreachable |
| `cfgcanary_rollback_duration_seconds` | config, environment | histogram: rollback start until every reachable exposed instance confirmed |
| `cfgcanary_gate_looks_total`, `cfgcanary_gate_last_verdict` | config, environment, verdict | gate evaluations (last: 1 PASS, 0 INCONCLUSIVE, -1 FAIL) |
| `cfgcanary_release_transitions_total` | config, environment, event | controller-driven transitions |
| `cfgcanary_controller_is_leader`, `cfgcanary_controller_epoch` | | leadership and fencing epoch |
| `cfgcanary_controller_reconcile_errors_total` | | reconcile failures |
| `cfgcanary_server_streams` | | open watch streams |
| `cfgcanary_server_stream_admissions_total` | result | admitted / rejected |
| `cfgcanary_server_slow_consumers_total` | | streams closed by the send watchdog |
| `cfgcanary_server_updates_sent_total` | kind | updates sent |
| `cfgcanary_server_acks_total` | result | stored, store_error, queue_full, ignored |
| `cfgcanary_server_submissions_total` | result, stage | stored / rejected (first failing stage) / dry_run |
| `cfgcanary_server_denied_total` | action | authorization refusals |
| `cfgcanary_agent_streams_connected`, `cfgcanary_agent_reconnects_total` | | agent connectivity |
| `cfgcanary_agent_applies_total` | config, result | applied, failed_<reason> |
| `cfgcanary_agent_stale_updates_total` | config | updates dropped as not newer |
| `cfgcanary_sdk_config_info` | cfgcanary_instance, config, environment, config_version, release, source | 1 for what the application serves |
| `cfgcanary_sdk_config_published_timestamp_seconds` | same | when this instance began serving it |
| `cfgcanary_sdk_validation_failures_total`, `cfgcanary_sdk_crash_loops_total` | cfgcanary_instance, config | parser rejections and quarantines |
| `cfgcanary_sdk_stale`, `cfgcanary_sdk_config_age_seconds` | cfgcanary_instance, config | staleness and age of the served snapshot |

Plus Go runtime and process metrics (`process_cpu_seconds_total`, `process_resident_memory_bytes`,
`go_goroutines`).

**Cardinality.** Control-plane series are per (config, environment), with at most 16 rings and a
fixed set of statuses, states and verdicts; per-release ring series are deleted when the release
ends. SDK series are per (instance, config): `config_version` and `release` change values, but the
SDK deletes the previous series when they change, so each instance has exactly one info series per
config rather than a growing set of stale ones. A fleet of N instances and C configs produces about
6 x N x C SDK series. Counters are created at zero so the first event is visible to `rate()`.

## Tracing

All gRPC calls carry W3C trace context (OpenTelemetry). Set the standard
`OTEL_EXPORTER_OTLP_ENDPOINT` (and related `OTEL_*` variables) on the server and CLI to export spans
over OTLP/gRPC; without it, context is still propagated but nothing is exported. Spans cover each
RPC, validation (`validate`, with the stages run and the outcome), each controller reconcile of a
release, and each gate evaluation. The Compose file has an optional Jaeger profile:

```bash
COMPOSE_PROFILES=tracing OTEL_EXPORTER_OTLP_ENDPOINT=http://jaeger:4317 \
  docker compose -f deploy/compose/compose.yaml up -d
# http://localhost:16686
```

## Logs

Structured JSON on stderr (`--log-level`). Logs and audit events never contain payload contents or
credentials; database URLs are redacted from errors.

## Audit

`cfgcanary audit` (or `GET /v1/audit`) reads the append-only audit log in PostgreSQL: every
config registration, submission and rejection, release command, controller transition (with its
epoch), gate look, kill switch, unfreeze and denied attempt. A trigger rejects `UPDATE`, `DELETE`
and `TRUNCATE` on the table.
