# Kubernetes mode

cfgcanary runs on Kubernetes with a Helm chart, and adds a `ConfigRelease` custom resource for
teams that manage config through manifests or GitOps. Kubernetes mode adds a delivery channel; it
does not replace the agent/SDK safety model, and it does not create a second source of truth.

## Components

| Piece | What it does |
|---|---|
| Control plane (`cfgcanary server`) | Same as everywhere: PostgreSQL holds release state |
| `cfgcanary k8s-controller` | Reconciles `ConfigRelease` objects: submits the payload, creates the release (request IDs derived from the object's UID and generation, so retries never duplicate), mirrors status, maps `spec.paused` to pause/resume, writes ring ConfigMaps. One replica acts at a time (Lease) |
| Ring ConfigMaps `<config>-<env>-ring-<n>` | `payload` and `envelope.json` (version, generation, digest, ring, and the signed artifact and delivery statements): exactly what an instance pinned to ring *n* would receive over a watch stream |
| Agent sidecar (`cfgcanary agent --source-dir`) | Reads its ring's ConfigMap volume, verifies signatures, scope, ring and digest, writes the cache atomically, reports status with a group certificate |
| Your application + SDK | Reads the cache through the SDK: last-known-good, rejection reporting, crash-loop guard |

```yaml
apiVersion: cfgcanary.io/v1alpha1
kind: ConfigRelease
metadata: {name: ratelimits, namespace: demo}
spec:
  config: ratelimits          # registered with `cfgcanary configs register`
  environment: prod
  payloadFrom: {configMapKeyRef: {name: ratelimits-v2, key: payload.json}}   # or payload: "<text>"
  paused: false
  projectRings: true          # write ring ConfigMaps in this namespace
status:                       # written by the controller
  observedGeneration: 2
  version: 2
  releaseId: rel-49f3e51c96d62b1a
  phase: BAKING
  activeRing: 0
  rings: [{index: 0, name: ring0, phase: baking, eligible: 2, accepted: 2}, ...]
  conditions:
    - {type: Validated, status: "True", reason: ValidationPassed}
    - {type: Released, status: Unknown, reason: Progressing}
```

Editing `spec` (a new payload) increments `metadata.generation`, which creates a new version and
release. A payload that fails validation sets `phase: Rejected` with the issues and `Validated:
False`; nothing changes for any pod. Deleting a `ConfigRelease` does not roll anything back:
release state lives in the control plane (use `cfgcanary releases rollback`).

## Assigning workloads to rings

Rings are not labels a pod can choose. A pod's ring comes from its **group certificate**
(`spiffe://TD/env/prod/group/ring1`), which the policy pins to a ring:

```yaml
environments:
  prod:
    rings: [{name: ring0, cumulative_bps: 1000}, {name: ring1, cumulative_bps: 5000}, {name: ring2, cumulative_bps: 10000}]
    pinned_groups: {ring0: 0, ring1: 1, ring2: 2}
```

Each ring's Deployment mounts `cfgcanary-group-ringN` (the certificate) and the ring's ConfigMap.
The group certificate can only claim instance IDs `ringN/<suffix>` (the pod name), so a pod in
ring 2 cannot pose as ring 0. See [`deploy/kind/workloads.yaml`](../deploy/kind/workloads.yaml).

## What a ConfigMap alone cannot promise

| Without the agent sidecar | With it |
|---|---|
| No signature or digest check; whatever is in the ConfigMap is used | Verified against the trust bundle, scope and ring range |
| Projected volumes update files via a symlink swap, but the application may read mid-change, and a bad file replaces the good one | The cache keeps the previous verified config; the SDK keeps last-known-good |
| No acknowledgement: the control plane cannot tell who applied what, so a gated release halts waiting | `APPLIED` / `ACCEPTED` / `REJECTED` per pod |
| Update delay: kubelet refreshes ConfigMap volumes periodically (up to about a minute) | Same delay; for immediate delivery run the agent in watch mode instead of `--source-dir` |

ConfigMaps are limited to 1 MiB, which bounds payload size in this mode.

## Install with Helm

The chart is in [`deploy/helm/cfgcanary`](../deploy/helm/cfgcanary) (values schema included). It
references Secrets you create; no credential is a chart value:

| Secret | Keys |
|---|---|
| `cfgcanary-server-tls` | `tls.crt`, `tls.key`, `ca.crt` (server certificate with the Service DNS name) |
| `cfgcanary-signing-keys` | `artifact.key`, `delivery.key` |
| `cfgcanary-rbac` | `rbac.yaml` |
| `cfgcanary-db` | `url` (PostgreSQL URL) |
| `cfgcanary-s3` | `access-key`, `secret-key` |
| `cfgcanary-k8s-controller` | `tls.crt`, `tls.key`, `ca.crt` (a `service` identity with the releaser role) |

```bash
helm install cfgcanary deploy/helm/cfgcanary -n cfgcanary \
  --set server.objectStore.endpoint=minio:9000 --set server.prometheusUrl=http://prometheus:9090
```

Helm installs the CRD from `crds/` on first install and never upgrades or deletes it; apply
`deploy/helm/cfgcanary/crds/` with `kubectl apply` when it changes.

## Verified example on kind

[`scripts/kind-demo.sh`](../scripts/kind-demo.sh) (or `make kind-demo`) creates a kind cluster,
generates development certificates and Secrets, installs PostgreSQL and the S3 store (development
manifests), installs the chart, runs three ring Deployments of two pods each, and checks:

1. A `ConfigRelease` for v1 reaches only ring 0 until promoted; rings 1 and 2 have empty ConfigMaps
   and their pods are not ready.
2. After promotion every pod serves v1; editing the resource to v2 leaves rings 1 and 2 on v1 until
   promoted.
3. An invalid payload is `Rejected` and changes nothing.
4. `helm upgrade` to two server replicas keeps serving; `helm uninstall` removes the chart's
   resources while workloads keep serving from their caches.

It passed end to end in 640 s on the test laptop (kind v0.33.0, Kubernetes v1.37.0, Helm v4.3.1).
