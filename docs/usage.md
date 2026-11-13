# Using cfgcanary

This page covers the CLI, the config spec, the Go SDK and the agent. For the API see
[protocol.md](protocol.md); for operations see [runbooks.md](runbooks.md).

## Install

- **Prebuilt binaries** for Linux and macOS (amd64, arm64) are attached to each
  [GitHub release](https://github.com/raunaksahahere/cfgcanary/releases), with `SHA256SUMS`.
  No Go toolchain is needed.
- **Container image**: see the release notes for the published image, or build it with
  `docker build -t cfgcanary:dev .`.
- **From source**: `go install github.com/raunaksahahere/cfgcanary/cmd/cfgcanary@v0.1.0` (Go 1.27+).

`cfgcanary version` prints the version, commit and build date.

## Identity and connection

Every operator command talks to the control plane over mutual TLS. The server decides what you may
do from the SPIFFE ID in your client certificate (`spiffe://<trust-domain>/user/<name>`) and its
RBAC policy.

| Flag | Environment | Meaning |
|---|---|---|
| `--server` | `CFGCANARY_SERVER` | gRPC address, default `localhost:8443` |
| `--server-name` | `CFGCANARY_SERVER_NAME` | TLS name to verify (default: the host of `--server`) |
| `--cert --key --ca` | `CFGCANARY_CERT/KEY/CA` | client certificate, key and CA bundle |
| `--pki-dir --as NAME` | `CFGCANARY_PKI_DIR`, `CFGCANARY_AS` | development shortcut: use `<pki-dir>/users/<NAME>.pem` (from `cfgcanary dev bootstrap`) |
| `-o json` | `CFGCANARY_OUTPUT` | machine output (below) |
| `--timeout` | | per-request timeout, default 30 s |

Roles: `viewer` (read, plan), `submitter` (read, submit versions, create releases), `operator`
(read, promote, pause, resume, roll back), `releaser` (submitter + operator), `admin` (everything
except break-glass, including config registration and unfreeze), `breakglass` (kill switch and
override promotion). Workload certificates have no roles; they can only use the agent API.

## Commands

| Command | Purpose |
|---|---|
| `cfgcanary init [--dir D --name N]` | Scaffold `spec.yaml`, `schema.json`, `payload.json` |
| `cfgcanary configs register -f spec.yaml` | Create or update a config, its validation and rollout policies |
| `cfgcanary configs get NAME` / `configs list` | Show configs |
| `cfgcanary validate NAME -f payload.json` | Run the full pipeline without storing anything (exit 3 if rejected) |
| `cfgcanary versions submit NAME -f payload.json [--environment E]` | Validate and store an immutable version |
| `cfgcanary versions list NAME` / `versions payload NAME V` | Inspect versions |
| `cfgcanary diff NAME FROM TO` | Structural diff (`FROM 0` = empty document) |
| `cfgcanary releases plan NAME V [--environment E]` | Dry run: cohorts per ring, blockers, warnings, diff. Writes nothing, contacts no agent |
| `cfgcanary releases create NAME V [--wait]` | Create a release; the controller starts ring 0 |
| `cfgcanary releases status ID [--watch]` | Per-ring eligible / reachable / applied / accepted / rejected / failed counts, gate verdicts, rollback convergence |
| `cfgcanary releases list [--config --environment --active]` | List releases |
| `cfgcanary releases promote ID [--expect-ring N]` | Expose the next ring; bake and gate still apply |
| `cfgcanary releases promote ID --override --reason R` | Break-glass: skip bake and gate (audited) |
| `cfgcanary releases pause ID` / `resume ID` | Stop and resume forward progress |
| `cfgcanary releases rollback ID [--reason R]` | Return every ring to the baseline |
| `cfgcanary kill-switch NAME --reason R` | Break-glass: revert to the previous known-good version now and freeze the target |
| `cfgcanary unfreeze NAME` | Lift a freeze (admin) |
| `cfgcanary target NAME` | Desired state: generation, baseline, candidate, active ring |
| `cfgcanary instances [--environment E]` | What each instance applied and serves |
| `cfgcanary audit [--config --release --action --limit]` | The append-only audit log, including denied attempts |
| `cfgcanary info` | Server version, leadership, your identity and roles |
| `cfgcanary doctor` | Check Docker, ports, the dev PKI and connectivity |
| `cfgcanary demo` | Run the demonstration scenario against a running stack |
| `cfgcanary server`, `agent`, `k8s-controller`, `sim`, `demo-app` | Long-running processes (`--help` lists every flag and its environment variable) |
| `cfgcanary dev bootstrap`, `dev issue` | Development certificates, keys and credentials |

### Exit codes

| Code | Meaning |
|---:|---|
| 0 | success |
| 1 | error |
| 2 | usage error or invalid argument |
| 3 | validation rejected the payload (nothing stored) |
| 4 | precondition failed or conflict (e.g. a release is already active, wrong expected ring, paused) |
| 5 | unauthenticated or permission denied |
| 6 | not found |
| 7 | unavailable, timed out or rate limited (retry) |
| 8 | a waited-for release ended rolled back or halted |

### JSON output

`-o json` prints one envelope per result; field names follow the protobuf definitions
(snake_case), and `apiVersion` changes only with an incompatible format change.

```json
{"apiVersion": "cfgcanary/v1", "kind": "Release", "data": {"id": "rel-...", "state": "RELEASE_STATE_BAKING", ...}}
```

Errors are also enveloped (`"kind": "Error"`, with `message` and `exit_code`). `releases status
--watch -o json` prints one envelope per change.

## The config spec

```yaml
name: ratelimits
format: json
failure_mode: FAILURE_MODE_FAIL_STATIC     # or FAILURE_MODE_FAIL_CLOSED
max_staleness: 600s
retain_versions: 20                       # garbage collection keeps at least this many
validation:
  limits:
    max_bytes: 65536                      # checked before parsing; required
    max_depth: 16
    max_items: {"/features": 200, "/tenants/*/rules": 50}   # JSON pointers, * matches every member
  json_schema_file: schema.json           # draft 2020-12; remote $ref is refused
  cue_file: policy.cue                    # unified with the payload, must be concrete
  relative:                               # against each environment's deployed baseline
    max_size_ratio: 2
    min_size_ratio: 0.5
    max_items_ratio: {"/features": 2}
    min_items_ratio: {"/features": 0.5}
  wasm:
    module_file: validator.wasm           # the consumer's parser (ABI below)
    timeout: 2s
    memory_limit_mib: 32
environments:
  prod:
    rings:                                # cumulative share of the hash space, basis points
      - {name: canary, cumulative_bps: 1500}
      - {name: early, cumulative_bps: 4000}
      - {name: broad, cumulative_bps: 9000}  # < 10000 leaves a holdout as the final control
    pinned_groups: {ring0: 0}             # group certificates pinned to a ring (Kubernetes mode)
    min_instances: 3                      # first ring and its control must have at least this many
    acceptance_threshold: 0.9             # of reachable ring members that must have ACCEPTED
    min_reachable_fraction: 0.8
    rollout_timeout: 120s
    rollback_timeout: 60s
    reachable_window: 30s
    promotion: PROMOTION_AUTO             # MANUAL is required when the gate is NONE
    max_sdk_rejections: 0                 # rejections or crash loops above this roll back
    max_crash_loops: 0
    gate:
      mode: GATE_MODE_PROMETHEUS
      min_bake: 20s
      look_interval: 10s
      looks: 6                            # error probability is split across these
      alpha_harm: 0.01
      alpha_noninferiority: 0.05
      min_instances: 3
      max_sample_age: 30s
      eval_timeout: 10s
      on_fail: ON_FAIL_ROLLBACK
      max_control_proportion: 0.2         # a degraded control makes the comparison inconclusive
      instance_label: cfgcanary_instance
      metrics:
        - {name: errors, kind: METRIC_KIND_RATIO, metric: http_requests_total, bad_metric: http_request_errors_total,
           margin: 0.02, hard_max: 0.5, min_events: 500}
        - {name: slow, kind: METRIC_KIND_HISTOGRAM_OVER, metric: http_request_duration_seconds, le: "0.25",
           margin: 0.05, min_events: 500}
        - {name: restarts, kind: METRIC_KIND_INSTANCE_EVENT, metric: process_restarts_total, margin: 0.5, guard_only: true}
```

The full schema is the `ConfigSpec` message in [`api/cfgcanary/v1/cfgcanary.proto`](../api/cfgcanary/v1/cfgcanary.proto).
`*_file` keys are a CLI convenience resolved relative to the spec file. Registration validates the
whole spec (policies are normalized with defaults), so a broken policy is refused when written.

### Gate metrics need the instance label

The gate compares instances serving the candidate with instances serving the baseline under the
same release. It learns who serves what from the SDK's `cfgcanary_sdk_config_info` and
`cfgcanary_sdk_config_published_timestamp_seconds` series, joined on `instance_label` with your
application's metrics. Label your request metrics with the same instance value you pass to the
SDK as `Options.InstanceID` (the example uses `cfgcanary_instance`).

### WASM validator ABI (`cfgcanary-validator/v1`)

A WASI preview 1 command. The payload is standard input; the module prints one JSON object
`{"ok": true|false, "errors": ["..."]}` to standard output and exits 0. It gets no files, no
environment, no arguments, no network, a fixed clock, a memory ceiling and a deadline. A timeout, trap,
non-zero exit or malformed or oversized output rejects the payload. In Go:

```bash
GOOS=wasip1 GOARCH=wasm go build -o validator.wasm ./examples/ratelimits/validator
```

Import the *same* parsing package your application uses (see `examples/ratelimits`), so the
validator rejects exactly what the consumer would.

## The Go SDK

```go
import (
	"github.com/raunaksahahere/cfgcanary/sdk"
	"github.com/prometheus/client_golang/prometheus"
)

client, err := sdk.Open(ctx, sdk.Options[MyConfig]{
	Dir:             "/var/lib/cfgcanary",        // the agent's --cache-dir
	Config:          "ratelimits",
	Environment:     "prod",
	TrustBundleFile: "/etc/cfgcanary/trust.json",
	Parse:           ParseMyConfig,               // your parser: func([]byte) (MyConfig, error)
	Clone:           MyConfig.Clone,              // required if MyConfig has maps, slices, pointers...
	InstanceID:      hostname,                    // the metric label the gate joins on
	Metrics:         sdk.NewMetrics(prometheus.DefaultRegisterer),
	Bootstrap:       defaultConfigJSON,           // optional: served when no cache exists yet
})
var initErr *sdk.InitError
if errors.As(err, &initErr) {
	// No usable config and no bootstrap: fail readiness, do not serve.
}
cfg := client.Get()           // never fails, never blocks; an immutable snapshot
info := client.Info()         // version, generation, release, source
if !client.Usable() { ... }   // false when a fail-closed config is stale
client.MarkHealthy()          // optional: make the served config last-known-good now
```

- `Get` returns the last config your parser accepted. A new payload your parser rejects is
  reported as `REJECTED` and never replaces it.
- Each process start while serving a config not yet marked healthy counts toward
  `CrashLoopThreshold` (default 3); beyond it the config is quarantined, the SDK serves the
  last-known-good and reports `CRASH_LOOP`. A config becomes healthy after `HealthyAfter` (default
  30 s, monotonic clock) or `MarkHealthy`.
- Metrics: `cfgcanary_sdk_config_info{config, environment, config_version, release, source,
  cfgcanary_instance}` (one series per instance; the old one is deleted on change),
  `cfgcanary_sdk_config_published_timestamp_seconds`, `cfgcanary_sdk_validation_failures_total`,
  `cfgcanary_sdk_crash_loops_total`, `cfgcanary_sdk_stale`, `cfgcanary_sdk_config_age_seconds`.

The SDK reads the files the agent writes; it does not talk to the network. Run the agent next to
the application (sidecar, or a host daemon) with the same cache directory.

## The agent

```bash
cfgcanary agent --instance host-17 --environment prod --config ratelimits \
  --cache-dir /var/lib/cfgcanary --trust-bundle /etc/cfgcanary/trust.json \
  --allowed-origins https://s3.internal:9000 \
  --server cfgcanary.internal:8443 --cert agent.pem --key agent-key.pem --ca ca.pem
```

The certificate must carry `spiffe://<trust-domain>/env/prod/agent/host-17` (or a group identity
`.../group/<g>`, with instance IDs `<g>/<suffix>`). `--allowed-origins` lists the only places
artifacts may be downloaded from; redirects are never followed. With `--source-dir` the agent
reads a projected directory (a Kubernetes ring ConfigMap) instead of watching; see
[kubernetes.md](kubernetes.md).

## Platform support

The server, agent and CLI are tested on Linux (amd64). Release binaries are also cross-compiled
for Linux arm64 and macOS amd64/arm64; on macOS the agent's atomic-write sequence relies on
`rename(2)` and `fsync(2)`, but crash consistency is tested only on Linux with ext4. Windows is not
supported in v0.1.0.
