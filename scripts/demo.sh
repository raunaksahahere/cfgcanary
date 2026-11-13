#!/usr/bin/env bash
# End-to-end demonstration on the local Compose stack. Needs only Docker.
#
#   scripts/demo.sh          build, start, run the scenario, show outage survival
#                            and the kill switch under a network partition
#   scripts/demo.sh down     stop the stack and delete its volumes
#
# Every step prints what it is doing; the timings it reports are measured.
set -euo pipefail
cd "$(dirname "$0")/.."
COMPOSE=(docker compose --progress quiet -f deploy/compose/compose.yaml)
cli() { "${COMPOSE[@]}" run --rm --no-deps -T cli "$@"; }
as() { local who=$1; shift; "${COMPOSE[@]}" run --rm --no-deps -T -e CFGCANARY_AS="$who" cli "$@"; }
prom() { # instant query against the stack's Prometheus, printed compactly
  curl -fsS --get http://127.0.0.1:9090/api/v1/query --data-urlencode "query=$1" |
    sed -e 's/.*"result":\[//' -e 's/\]}}$//' -e 's/{"metric":{/\n  /g' -e 's/"value":\[[0-9.]*,/ = /g' -e 's/[]}]//g' -e 's/"//g'
  echo
}
step() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
t0=$SECONDS

if [[ "${1:-}" == "down" ]]; then
  "${COMPOSE[@]}" down -v
  exit 0
fi

step "build and start the stack (PostgreSQL, S3, Prometheus, Grafana, control plane, fault proxy, 50 agents)"
s=$SECONDS
"${COMPOSE[@]}" build -q
b=$((SECONDS - s)); s=$SECONDS
"${COMPOSE[@]}" up -d --wait --quiet-pull
echo "image build: ${b}s; start until every service is healthy: $((SECONDS - s))s"

step "scenario: invalid submissions, a gated rollout, and a harmful change caught in the canary"
s=$SECONDS
cli demo
echo "scenario: $((SECONDS - s))s"

step "control-plane outage: stop the server; applications keep serving their last-known-good config"
"${COMPOSE[@]}" stop server
sleep 12
echo "instances serving each version while the control plane is down (from the SDKs' own metrics):"
prom 'count by (config_version) (cfgcanary_sdk_config_info)'
"${COMPOSE[@]}" start server
"${COMPOSE[@]}" up -d --wait server
sleep 5
echo "after restart, watch streams re-established: $(curl -fsS --get http://127.0.0.1:9090/api/v1/query --data-urlencode 'query=sum(cfgcanary_server_streams)' | sed -E 's/.*,"([0-9]+)"\].*/\1/')"

step "kill switch while every agent is partitioned from the control plane"
"${COMPOSE[@]}" exec -T toxiproxy /toxiproxy-cli toggle agents >/dev/null
echo "link between agents and control plane: cut (Toxiproxy)"
as oncall kill-switch ratelimits --reason "demo: emergency revert during a partition"
sleep 35   # longer than the policy's 30s reachable window
id=$(as viewer -o json releases list --config ratelimits | sed -n 's/.*"id": "\(rel-[0-9a-f]*\)".*/\1/p' | head -1)
echo "while partitioned:"
as viewer releases status "$id" | grep -E '^(state|rollback|unreachable)' || true
"${COMPOSE[@]}" exec -T toxiproxy /toxiproxy-cli toggle agents >/dev/null
echo "link restored; agents reconnect with jittered backoff and reconcile"
for _ in $(seq 1 60); do
  if as viewer releases status "$id" | grep -q '(converged)'; then break; fi
  sleep 1
done
as viewer releases status "$id" | grep -E '^(state|rollback)'
as admin unfreeze ratelimits --reason "demo finished"

step "audit trail (newest first)"
as viewer audit --config ratelimits --limit 12

echo
echo "Grafana: http://localhost:3000 (anonymous viewer)  Prometheus: http://localhost:9090"
echo "total demo time: $((SECONDS - t0))s   stop with: scripts/demo.sh down"
