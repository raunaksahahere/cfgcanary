#!/usr/bin/env bash
# Runs every measurement and regenerates bench/results/summary.md.
# Needs the integration-test services: make test-deps-up.
#   scripts/bench.sh                    all measurements (about 45-75 minutes)
#   CFGCANARY_BENCH_SIZES=50,1000 scripts/bench.sh   fewer fleet sizes
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p bench/results bench/logs
export CFGCANARY_BENCH=1 CFGCANARY_IT=1
run() { echo "== $1"; shift; "$@" 2>&1 | tee "bench/logs/$(date +%s)-$1.log" | grep -E '^(---|ok|FAIL|PASS)|wrote' || true; }
go test -count=1 -run TestGatePower -v ./bench | grep -E 'wrote|^(ok|FAIL)'
go test -count=1 -run TestBlastRadius -timeout 30m -v ./bench | grep -E 'wrote|^(ok|FAIL)'
go test -count=1 -run TestReconnectStorm -timeout 30m -v ./bench | grep -E 'wrote|^(ok|FAIL)|attempts='
go test -count=1 -run TestScale -timeout 120m -v ./bench | grep -E 'wrote|^(ok|FAIL)|N='
cov=$(go test -count=1 -cover ./internal/rollout | sed -n 's/.*coverage: \([0-9.]*%\).*/\1/p')
go test -json -count=1 -timeout 30m ./test/chaos | go run ./bench/chaosjson -coverage "$cov"
go run ./bench/report
