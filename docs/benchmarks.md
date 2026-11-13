# Benchmarks: method, reproduction and limits

The numbers are in [`bench/results/summary.md`](../bench/results/summary.md), generated from the raw
JSON/CSV next to it by `go run ./bench/report`. Each raw file records the hardware, Go version,
commit, timestamp and the command that produced it.

## Reproduce

```bash
make test-deps-up          # PostgreSQL, S3, Prometheus, Toxiproxy in Docker
scripts/bench.sh           # every measurement, then the summary
# or one at a time:
CFGCANARY_BENCH=1 go test ./bench -run TestBlastRadius -timeout 30m -v
CFGCANARY_BENCH=1 go test ./bench -run TestReconnectStorm -timeout 30m -v
CFGCANARY_BENCH=1 CFGCANARY_BENCH_SIZES=50,100,1000,5000 go test ./bench -run TestScale -timeout 120m -v
CFGCANARY_BENCH=1 go test ./bench -run TestGatePower -v
go test -json ./test/chaos | go run ./bench/chaosjson -coverage "$(go test -cover ./internal/rollout | sed -n 's/.*coverage: \([0-9.]*%\).*/\1/p')"
```

## What each measurement is

| File | Measurement | Method |
|---|---|---|
| `blast_radius.json` | Instances targeted, applied, accepted and affected by a schema-valid, runtime-harmful change; time to detect and to roll back | 50 simulated instances with a synthetic workload; `timeout_ms` 250 → 20 makes ~92% of requests time out. Gated policy (rings 15/40/90% + holdout, statistical gate only), 3 repetitions. The naive row uses the same system in an isolated benchmark mode (one 100% ring, size-only validation, no gate). The Reloader row is a **parameterised model**, not a test of that product |
| `storm.json`, `storm_curve.csv` | Connection attempts per 100 ms when 1,000 agents lose the control plane (SIGKILL, down 5 s, restarted) | Same harness twice: full jitter, and a no-jitter baseline mode that exists only for this comparison. Server admission raised so the curve shows client behaviour |
| `scale.json`, `scale_propagation.csv` | Per-ring propagation (received, applied, served), rollback convergence, control-plane CPU and memory, at 50, 100, 1,000, 5,000 agents | Control plane as a separate `cfgcanary server` process; agents and SDKs as goroutines (real code, own certificate, own cache directory, own TLS connection). Manual promotion of each ring as soon as the previous converged |
| `gate_power.json`, `gate_power.csv` | P(promote), P(roll back), P(halt) of the production gate code for 5, 50, 500 canaries | Monte Carlo over 4 sequential looks, 400–1,000 replications per cell, with instance heterogeneity and low-traffic variants |
| `chaos.json` | Results of the chaos suite and client crashes | `test/chaos` against real services with Toxiproxy faults |

## Environment of the published results

AMD Ryzen 5 5600H (6 cores / 12 threads), 7 GB RAM, Linux, Go 1.27.4, PostgreSQL 18.6 with
`fsync=off` on tmpfs (the test-dependency stack), `pgsty/minio` as the S3 store, Prometheus v3.16.0.
Everything ran on that one machine.

## Limits and caveats

- **One host.** Thousands of agents, the control plane, PostgreSQL and the object store share 12
  hardware threads and one disk. Each generation change makes every agent fsync its cache pointer
  and the SDK its state, so at 1,000 and 5,000 agents the *applied* and *served* times measure
  fsync and scheduler contention that separate hosts would not share. The *received* column (update
  delivered to the agent, before disk work) is the closer measure of the control plane's fan-out,
  and it is also inflated by CPU contention at 5,000.
- **Task limit at 5,000 agents.** The first 5,000-agent attempt aborted with
  `runtime/cgo: pthread_create failed: Resource temporarily unavailable`: the session's cgroup
  allowed 7,177 tasks (`pids.max`), and 5,000 agents blocking in fsync at once need one OS thread
  each. The published run used a systemd user scope with `TasksMax=15500` and raised Go's thread
  cap for the benchmark process only. No product code was changed for it.
- **Database durability off.** The test PostgreSQL runs with `fsync=off` on tmpfs, which makes
  control-plane writes faster than a durable production database.
- **Synthetic workload.** Error rates come from a lognormal latency model, not a real service.
- **Gate simulation assumptions.** Independent requests within an instance given its rate; no time
  trends; one metric at a time (see ADR 0004).
- **Naive and Reloader rows** are comparison baselines, labelled as such: the naive mode is the same
  system with safeguards removed in an isolated benchmark configuration; the Reloader row is a model.
- Not measured: multi-host networks, real production traffic, other hardware, long-duration soak.
