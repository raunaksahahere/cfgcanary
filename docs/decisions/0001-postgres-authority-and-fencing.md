# 0001. PostgreSQL is the single authority; leadership is fenced

**Status:** accepted

**Context.** The release state machine must survive controller crashes and must not be driven by
two controllers at once. P2 (p. 33) proposes PostgreSQL with advisory locks and names etcd as the
Kubernetes-native alternative.

**Decision.** All release truth lives in PostgreSQL. One controller is elected with
`pg_try_advisory_lock` on a dedicated connection. On election it increments
`controller_leader.epoch`; every controller transaction re-reads that row `FOR SHARE` and aborts
if the epoch is not its own. Operator commands are serialized by the target row lock and an
optimistic `state_version` check instead of by leadership.

**Why not only the advisory lock.** A session-level lock tells a process it *was* leader when it
last checked. A stalled process can lose the session and still hold a decision in memory. The
check has to be in the same transaction as the write
([Kleppmann, "How to do distributed locking"](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html)).

**Consequences.** PostgreSQL availability bounds control-plane availability. Agents do not depend
on it (static stability). No Raft to operate.
