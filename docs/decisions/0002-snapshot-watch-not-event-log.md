# 0002. Watch streams carry the latest desired state, not an event log

**Status:** accepted

**Context.** Agents disconnect, lag and reconnect in herds. A gap-free event log per target would
have to be retained, trimmed and replayed, and a replay could re-deliver a rollout that was
already rolled back.

**Decision.** The server keeps one coalescing slot per `(stream, config)`. A change marks the
slot dirty; the sender reads the *current* desired state when it is ready to send. Agents send
`last_generation` on connect and on a periodic `GetDesired` reconciliation, and ignore anything
with a generation not greater than what they applied. PostgreSQL `LISTEN/NOTIFY` wakes replicas,
with a periodic sweep as the fallback.

**Consequences.** Intermediate generations may be skipped by a slow agent, which is the desired
behaviour. Memory per stream is constant. Ordering needs only the per-target generation.
