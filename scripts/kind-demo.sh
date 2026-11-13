#!/usr/bin/env bash
# Kubernetes mode on kind: install the Helm chart, roll a config out through
# per-ring ConfigMaps to workloads using the agent sidecar and the SDK, check
# every step, upgrade the release, uninstall, and delete the cluster.
#
#   scripts/kind-demo.sh            run everything (KEEP=1 keeps the cluster)
#
# Needs docker, kind, kubectl and helm (make tools installs pinned versions
# into .tools/bin). Development credentials go to .dev/kind (git-ignored).
set -euo pipefail
cd "$(dirname "$0")/.."
export PATH="$PWD/.tools/bin:$PATH"
CLUSTER=cfgcanary
DEV=.dev/kind
NS=cfgcanary
step() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
die() { echo "FAILED: $*" >&2; exit 1; }
wait_for() { # wait_for <seconds> <description> <command...>
  local n=$1 what=$2; shift 2
  for _ in $(seq 1 "$n"); do if "$@" >/dev/null 2>&1; then return 0; fi; sleep 1; done
  die "timed out waiting for $what"
}
t0=$SECONDS

step "tools and image"
for t in docker kind kubectl helm; do command -v $t >/dev/null || die "$t not found (make tools)"; done
docker build -q -t cfgcanary:dev --build-arg VERSION=0.1.0 . >/dev/null
if [[ ! -x bin/cfgcanary ]]; then # the CLI for the host, from the image (Linux)
  id=$(docker create cfgcanary:dev); mkdir -p bin; docker cp "$id:/usr/local/bin/cfgcanary" bin/cfgcanary; docker rm "$id" >/dev/null
fi
CLI=(bin/cfgcanary --server localhost:18443 --server-name cfgcanary --pki-dir "$DEV/pki")

step "kind cluster"
kind get clusters 2>/dev/null | grep -qx "$CLUSTER" || kind create cluster --config deploy/kind/kind-config.yaml --wait 120s
kind load docker-image cfgcanary:dev --name "$CLUSTER" >/dev/null
# PostgreSQL and the S3 store are pulled by the node from their registries.

step "development PKI, group certificates and secrets"
bin/cfgcanary dev bootstrap --dir "$DEV" --db-host postgres:5432
for r in 0 1 2; do
  [[ -f $DEV/groups/ring$r.pem ]] || bin/cfgcanary --pki-dir "$DEV/pki" dev issue --uri "spiffe://cfgcanary.local/env/prod/group/ring$r" --out "$DEV/groups" --name "ring$r" >/dev/null
done
apply() { kubectl apply -f - >/dev/null; }
kubectl create namespace $NS --dry-run=client -o yaml | apply
kubectl create namespace demo --dry-run=client -o yaml | apply
P=$DEV/pki
kubectl -n $NS create secret generic cfgcanary-server-tls --from-file=tls.crt=$P/server.pem --from-file=tls.key=$P/server-key.pem --from-file=ca.crt=$P/ca.pem --dry-run=client -o yaml | apply
kubectl -n $NS create secret generic cfgcanary-signing-keys --from-file=artifact.key=$P/artifact.key --from-file=delivery.key=$P/delivery.key --dry-run=client -o yaml | apply
kubectl -n $NS create secret generic cfgcanary-rbac --from-file=rbac.yaml=$P/rbac.yaml --dry-run=client -o yaml | apply
kubectl -n $NS create secret generic postgres-password --from-file=password=$DEV/secrets/postgres-password --dry-run=client -o yaml | apply
kubectl -n $NS create secret generic cfgcanary-db --from-file=url=$DEV/secrets/db-url --dry-run=client -o yaml | apply
kubectl -n $NS create secret generic cfgcanary-s3 --from-file=access-key=$DEV/secrets/s3-access-key --from-file=secret-key=$DEV/secrets/s3-secret-key --dry-run=client -o yaml | apply
kubectl -n $NS create secret generic cfgcanary-k8s-controller --from-file=tls.crt=$P/services/k8s-controller.pem --from-file=tls.key=$P/services/k8s-controller-key.pem --from-file=ca.crt=$P/ca.pem --dry-run=client -o yaml | apply
kubectl -n demo create configmap cfgcanary-trust --from-file=trust.json=$P/trust.json --dry-run=client -o yaml | apply
for r in 0 1 2; do
  kubectl -n demo create secret generic cfgcanary-group-ring$r --from-file=tls.crt=$DEV/groups/ring$r.pem --from-file=tls.key=$DEV/groups/ring$r-key.pem --from-file=ca.crt=$P/ca.pem --dry-run=client -o yaml | apply
done

step "dependencies and helm install"
kubectl apply -f deploy/kind/deps.yaml >/dev/null
kubectl -n $NS rollout status deploy/postgres deploy/minio --timeout=180s >/dev/null
s=$SECONDS
helm upgrade --install cfgcanary deploy/helm/cfgcanary -n $NS --set image.repository=cfgcanary --set image.tag=dev \
  --set image.pullPolicy=Never --set server.objectStore.endpoint=minio:9000 --set server.objectStore.publicEndpoint=minio.cfgcanary.svc:9000 \
  --wait --timeout 180s >/dev/null
echo "helm install: $((SECONDS - s))s"
kubectl -n $NS get deploy

kubectl -n $NS port-forward svc/cfgcanary 18443:8443 >/dev/null 2>&1 &
PF=$!
trap 'kill $PF 2>/dev/null || true; [[ "${KEEP:-}" == 1 ]] || kind delete cluster --name '"$CLUSTER"' >/dev/null 2>&1 || true' EXIT
wait_for 30 "port-forward" "${CLI[@]}" info
"${CLI[@]}" info

step "register the config (ring-pinned groups, manual promotion) and start the workloads"
make -s wasm >/dev/null 2>&1 || { id=$(docker create cfgcanary:dev); docker cp "$id:/usr/share/cfgcanary/examples/ratelimits/validator.wasm" examples/ratelimits/validator.wasm; docker rm "$id" >/dev/null; }
"${CLI[@]}" configs register -f examples/ratelimits/spec-k8s.yaml
kubectl apply -f deploy/kind/workloads.yaml >/dev/null
kubectl -n demo create configmap ratelimits-v1 --from-file=payload.json=examples/ratelimits/payloads/v1-baseline.json --dry-run=client -o yaml | apply
kubectl -n demo create configmap ratelimits-v2 --from-file=payload.json=examples/ratelimits/payloads/v2-good.json --dry-run=client -o yaml | apply
kubectl -n demo create configmap ratelimits-bad --from-file=payload.json=examples/ratelimits/payloads/bad-parser-rule.json --dry-run=client -o yaml | apply
connected() { [[ $("${CLI[@]}" instances --environment prod | grep -c '^ring') -ge 6 ]]; }
wait_for 120 "6 pods to register through their agents" connected

serving() { # serving <ring> <version>: count pods of a ring whose SDK serves version
  "${CLI[@]}" instances --environment prod | awk -v r="^ring$1/" -v v="v$2" '$1 ~ r && $8 == v' | wc -l
}
phase() { kubectl -n demo get configrelease ratelimits -o jsonpath='{.status.phase} {.status.activeRing}'; }
release() { kubectl -n demo get configrelease ratelimits -o jsonpath='{.status.releaseId}'; }
promote_all() {
  local id; id=$(release)
  for r in 0 1 2; do
    wait_for 120 "ring $r baking" sh -c "[ \"\$(kubectl -n demo get configrelease ratelimits -o jsonpath='{.status.phase} {.status.activeRing}')\" = 'BAKING $r' ]"
    sleep 11 # the policy's minimum bake
    "${CLI[@]}" releases promote "$id" --expect-ring $r >/dev/null
    echo "promoted past ring $r"
  done
  wait_for 120 "release complete" sh -c "[ \"\$(kubectl -n demo get configrelease ratelimits -o jsonpath='{.status.phase}')\" = COMPLETE ]"
}

step "ConfigRelease v1: only ring 0 receives it until promoted"
cat <<YAML | kubectl apply -f - >/dev/null
apiVersion: cfgcanary.io/v1alpha1
kind: ConfigRelease
metadata: {name: ratelimits, namespace: demo}
spec:
  config: ratelimits
  environment: prod
  payloadFrom: {configMapKeyRef: {name: ratelimits-v1, key: payload.json}}
YAML
wait_for 120 "ring 0 baking" sh -c "[ \"\$(kubectl -n demo get configrelease ratelimits -o jsonpath='{.status.phase} {.status.activeRing}')\" = 'BAKING 0' ]"
kubectl -n demo get configreleases
for r in 0 1 2; do
  bytes=$(kubectl -n demo get configmap ratelimits-prod-ring-$r -o jsonpath='{.data.payload}' | wc -c)
  echo "ConfigMap ratelimits-prod-ring-$r: payload ${bytes} bytes"
done
[[ $(serving 0 1) -eq 2 && $(serving 1 1) -eq 0 && $(serving 2 1) -eq 0 ]] || die "v1 exposure: ring0=$(serving 0 1) ring1=$(serving 1 1) ring2=$(serving 2 1)"
echo "ring0 pods serving v1: 2/2; ring1 and ring2: 0 (their ConfigMaps carry no payload yet; pods not ready)"
promote_all
wait_for 60 "all pods on v1" sh -c "[ \$(bin/cfgcanary --server localhost:18443 --server-name cfgcanary --pki-dir $DEV/pki instances --environment prod | awk '\$8==\"v1\"' | wc -l) -eq 6 ]"
kubectl -n demo rollout status deploy/demo-ring0 deploy/demo-ring1 deploy/demo-ring2 --timeout=60s >/dev/null
echo "all 6 pods serve v1 and are ready"

step "ConfigRelease v2 (edit spec): rings 1 and 2 keep v1 until promoted"
kubectl -n demo patch configrelease ratelimits --type merge -p '{"spec":{"payloadFrom":{"configMapKeyRef":{"name":"ratelimits-v2","key":"payload.json"}}}}' >/dev/null
wait_for 120 "v2 ring 0 baking" sh -c "[ \"\$(kubectl -n demo get configrelease ratelimits -o jsonpath='{.status.version} {.status.phase} {.status.activeRing}')\" = '2 BAKING 0' ]"
wait_for 60 "ring 0 on v2" sh -c "[ \$(bin/cfgcanary --server localhost:18443 --server-name cfgcanary --pki-dir $DEV/pki instances --environment prod | awk '\$1 ~ /^ring0\\// && \$8==\"v2\"' | wc -l) -eq 2 ]"
[[ $(serving 1 1) -eq 2 && $(serving 2 1) -eq 2 ]] || die "later rings moved early: ring1 v1=$(serving 1 1) ring2 v1=$(serving 2 1)"
"${CLI[@]}" instances --environment prod
promote_all
wait_for 60 "all pods on v2" sh -c "[ \$(bin/cfgcanary --server localhost:18443 --server-name cfgcanary --pki-dir $DEV/pki instances --environment prod | awk '\$8==\"v2\"' | wc -l) -eq 6 ]"
echo "all 6 pods serve v2"

step "an invalid payload is rejected by the ConfigRelease and reaches no pod"
cat <<YAML | kubectl apply -f - >/dev/null
apiVersion: cfgcanary.io/v1alpha1
kind: ConfigRelease
metadata: {name: ratelimits-bad, namespace: demo}
spec:
  config: ratelimits
  projectRings: false
  payloadFrom: {configMapKeyRef: {name: ratelimits-bad, key: payload.json}}
YAML
wait_for 60 "rejection" sh -c "[ \"\$(kubectl -n demo get configrelease ratelimits-bad -o jsonpath='{.status.phase}')\" = Rejected ]"
kubectl -n demo get configrelease ratelimits-bad -o jsonpath='{.status.issues[0]}{"\n"}'
[[ $(serving 0 2) -eq 2 && $(serving 1 2) -eq 2 && $(serving 2 2) -eq 2 ]] || die "a rejected payload changed what pods serve"

step "helm upgrade (2 server replicas) keeps serving"
s=$SECONDS
helm upgrade cfgcanary deploy/helm/cfgcanary -n $NS --reuse-values --set server.replicas=2 --wait --timeout 180s >/dev/null
echo "helm upgrade: $((SECONDS - s))s"
kill $PF 2>/dev/null || true
kubectl -n $NS port-forward svc/cfgcanary 18443:8443 >/dev/null 2>&1 &
PF=$!
wait_for 30 "port-forward" "${CLI[@]}" info
"${CLI[@]}" releases status "$(release)" | sed -n 1,3p
[[ $(kubectl -n $NS get deploy cfgcanary -o jsonpath='{.status.readyReplicas}') == 2 ]] || die "upgrade did not reach 2 ready replicas"

step "helm uninstall"
helm uninstall cfgcanary -n $NS --wait >/dev/null
[[ -z $(kubectl -n $NS get deploy -l app.kubernetes.io/instance=cfgcanary -o name) ]] || die "resources left after uninstall"
echo "uninstalled; the ConfigRelease CRD is kept (Helm never deletes CRDs) and workloads keep serving from their caches:"
kubectl -n demo get pods -l app=demo -o custom-columns='POD:.metadata.name,READY:.status.containerStatuses[1].ready'
echo
echo "kind example passed in $((SECONDS - t0))s"
