# 0007. Kubernetes mode is a client and a projection, not a second controller of truth

**Status:** accepted

**Context.** P2 lists an optional `ConfigRelease` CRD that writes per-ring ConfigMaps. Two state
machines (one in etcd via the CR, one in PostgreSQL) would disagree under failure.

**Decision.** The Kubernetes controller calls the cfgcanary API. `.spec` is intent; `.status`
mirrors the PostgreSQL release. Per-ring ConfigMaps contain the payload and the signed envelope
for whatever that ring's desired state is. Pods consume them through the agent in file-source
mode plus the SDK, and report status with a group certificate whose group is pinned to a ring by
policy.

**Consequences.** Payloads are limited by the ConfigMap size limit. Kubelet sync delay applies. A
bare ConfigMap mount without the agent gets no verification, LKG or acknowledgement, and a gated
release will halt waiting for it.
