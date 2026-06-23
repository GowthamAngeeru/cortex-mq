# Cortex-MQ: Architecture Reference

Technical reference for system internals, design decisions, and
operational characteristics.

---

## System overview

Three components, one communication contract: all inter-component
traffic is HTTP/2 gRPC, all shared state is Redis.

```
[Task Producer] ──gRPC──► [Cortex-MQ Broker (Rust/Tokio)]

                  │

┌─────────────────┼─────────────────┐

gRPC            gRPC               gRPC

│                 │                 │

[Worker A]    [Worker B]       [Worker C]

(Python)       (Python)         (Python)

```

The broker does not execute work. It routes, leases, monitors, and
recovers. All execution happens inside workers.

---

## The hash ring (`src/core/hash_ring.rs`)

### Data structures

```rust
pub struct HashRing {
    ring: BTreeMap<u64, String>,              // position → node_id
    nodes: HashMap<String, NodeInfo>,         // node_id → health state
    node_vnodes: HashMap<String, Vec<u64>>,   // node_id → positions (reverse index)
    virtual_nodes: usize,
}
```

`BTreeMap` is required for the ring — not `HashMap`. Routing requires
finding the nearest clockwise neighbor to an arbitrary hash position,
which is a range query (`ring.range(hash..)`). `BTreeMap` supports this
in O(log n). `HashMap` does not support ordered range queries.

The `node_vnodes` reverse index exists for O(virtual_nodes) node removal.
Without it, removal requires scanning the entire ring to find a node's
virtual positions — O(ring_size). At 100 nodes × 150 virtual nodes =
15,000 entries, the naive approach iterates 15,000 entries per removal.
The reverse index reduces this to 150 lookups.

### Hashing: FNV-1a

```rust
const FNV_OFFSET_BASIS: u64 = 14695981039346656037;
const FNV_PRIME: u64 = 1099511628211;

fn fnv1a(data: &[u8]) -> u64 {
    let mut hash = FNV_OFFSET_BASIS;
    for &byte in data {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}
```

Zero initialization cost per call, branchless inner loop, adequate
distribution for fixed-length UUID routing keys (36 bytes). At 150
virtual nodes per physical node, the coefficient of variation across
positions stays below 5% in practice. Cryptographic properties and
DoS resistance are not requirements for a trusted internal cluster.

### Routing: two-mode lookup

```rust
// Mode 1: capacity-aware (normal operation)
pub fn get_node(&self, task_id: &str) -> Option<&str>
// Routes to Healthy nodes with capacity and CPU < 90%

// Mode 2: alive-only fallback (cluster under stress)
pub fn get_node_force(&self, task_id: &str) -> Option<&str>
// Routes to any non-Dead node regardless of load
```

`get_node_force` exists for the case where all nodes are at capacity but
a task must be routed — for example, during a traffic spike before new
workers join the cluster. Returning `None` in this case would cause task
submission to fail, which is worse than routing to an overloaded node.

### Node state machine

```

heartbeat OK + CPU < soft limit
             │
┌────────────▼─────────┐
│        Healthy       │
└────────────┬─────────┘
             │
CPU ≥ soft limit (90%)
OR 1-2 missed heartbeats
             │
┌────────────▼─────────┐
│        Suspect       │◄── new heartbeat received
└────────────┬─────────┘ (CPU back below limit)
             │
3 missed heartbeats
             │
┌────────────▼─────────┐
│           Dead       │
└────────────┬─────────┘
             │
heartbeat received
(node came back)
             │
┌────────────▼─────────┐
│    Healthy (revived) │
└──────────────────────┘

┌───────────────────┐

│ Draining │ ← operator-initiated graceful shutdown

└───────────────────┘ completes current leases, accepts no new tasks
```

Routing eligibility:

| State    | `get_node` eligible         | `get_node_force` eligible |
| -------- | --------------------------- | ------------------------- |
| Healthy  | Yes (if capacity available) | Yes                       |
| Suspect  | No                          | Yes                       |
| Dead     | No                          | No                        |
| Draining | No                          | Yes                       |

The `Suspect` state is critical for LLM workloads. An agent calling GPT-4
saturates CPU during token generation for hundreds of milliseconds. A
binary Healthy/Dead model would mark this node dead and cascade its tasks
to already-loaded neighbors — amplifying the failure. `Suspect` absorbs
the spike without triggering recovery.

### Virtual node collision handling

When a virtual node position collides with an existing position, the broker
retries with a salted key (`{node_id}:vnode:{i}:salt:{attempt}`) up to 3
times before skipping that virtual node and logging a warning. This makes
collision handling deterministic and observable rather than silently wrong.

---

## The state layer (`src/core/state.rs`)

`GlobalState` wraps a Redis connection pool and provides all persistence
operations:

| Operation                 | Redis command                          | Notes                                      |
| ------------------------- | -------------------------------------- | ------------------------------------------ |
| `push_assigned_task`      | `LPUSH cortex:queue:{node_id}`         | Node-specific task queue                   |
| `fetch_assigned_task`     | `RPOP cortex:queue:{node_id}`          | Atomic pop — only one worker gets the task |
| `save_task_snapshot`      | `SETEX cortex:task:{task_id} {ttl}`    | Payload backup with TTL                    |
| `acquire_task_lock`       | `SET cortex:lock:{task_id} NX PX {ms}` | Idempotency guard, distributed lock        |
| `force_release_task_lock` | `DEL cortex:lock:{task_id}`            | Explicit release on completion/failure     |
| `push_dlq`                | `LPUSH cortex:dlq`                     | Quarantine for exhausted tasks             |
| `dlq_size`                | `LLEN cortex:dlq`                      | Depth telemetry for dashboard              |

**Idempotency:** Before accepting `SubmitTask`, the broker checks
`load_task_snapshot`. If task_id already exists, it returns
`Status::already_exists`. Combined with the `SET NX` lock, this prevents
duplicate injection under network retry conditions.

**Serialization boundary:** Prost-generated types implement binary Protobuf
serialization that conflicts with Redis client expectations. Task metadata
crossing the Redis boundary is wrapped in `serde_json::Value`. JSON
serialization cost is negligible — Redis round-trip latency is 3 orders of
magnitude larger.

---

## The heartbeat monitor (`src/core/broker.rs`)

A background Tokio task runs on `HEARTBEAT_INTERVAL` (default 10s):
every HEARTBEAT_INTERVAL seconds:

for each node in ring:

if now() - node.last_heartbeat > HEARTBEAT_INTERVAL:

node.missed_heartbeats += 1

if missed_heartbeats >= MISSED_HEARTBEAT_THRESHOLD (default 3):

mark_dead(node_id)

This is pull-based liveness detection: the broker determines health by
observing absence of pings, not by workers declaring themselves healthy.
Workers cannot lie about their own liveness.

Node failure detection window: `MISSED_HEARTBEAT_THRESHOLD × HEARTBEAT_INTERVAL`
= 30 seconds at default settings. Task reclamation happens when the next
`ClaimTask` request arrives — a worker on a healthy node polls every 2
seconds, so reclamation typically completes within 32 seconds of a crash.

---

## The worker (`cortex-swarm/worker.py`)

### Concurrency model

Each worker runs three concurrent asyncio tasks:
Worker process

├── \_heartbeat_loop — pings broker every 5s with CPU/memory/active count

├── \_claim_loop — polls for tasks when below MAX_CONCURRENT capacity

└── \_execute_task(n) — one per claimed task, bounded by lease TTL

Task isolation: each `_execute_task` is an independent asyncio `Task`.
Agent execution failure does not affect other running agents on the same
worker. Done callbacks catch silent exceptions from background task panics.

### Lease enforcement in the worker

When execution begins:

```python
lease_remaining = max(0, lease_expires_at_unix - int(time.time()))
await asyncio.wait_for(
    self._run_agent_pipeline(assignment),
    timeout=float(lease_remaining)
)
```

If the LLM call exceeds the lease, the worker reports `FailTask` with
`is_retryable=True` before the broker independently reclaims. This prevents
the race condition where both lease reclaim and worker completion fire
simultaneously for the same task_id.

### Autonomous reconnection

If 3 consecutive heartbeats fail:

1. `self.stub = None` — blocks new claim attempts immediately
2. Close existing gRPC channel
3. `_connect_with_retry` with exponential backoff (1s base, 2x multiplier, 60s cap)
4. Reset failure counter on success

Workers survive broker restarts without operator intervention.

---

## Task lifecycle

Producer: SubmitTask(task_id, payload)

│

├─ Broker: check Redis for task_id → Status::already_exists if duplicate

│

├─ Broker: hash_ring.get_node(task_id) → node_id

│ (returns None if no healthy node → Status::resource_exhausted)

│

├─ Broker: SET cortex:lock:{task_id} NX PX {lease_ms}

│ (returns None if contention → Status::aborted)

│

├─ Broker: SETEX cortex:task:{task_id} {payload} {ttl}

│

├─ Broker: LPUSH cortex:queue:{node_id} {task_metadata_json}

│

│ ← TaskAssignment returned to producer

│

Worker: RPOP cortex:queue:{node_id} → task_metadata

│

Worker: asyncio.wait_for(agent_pipeline, timeout=lease_remaining)

│

├─► Success

│ Worker: CompleteTask(task_id)

│ Broker: DEL cortex:lock:{task_id}

│ Broker: DEL cortex:task:{task_id}

│

└─► Failure

Worker: FailTask(task_id, is_retryable, attempt_number)

│

├─► is_retryable=True AND attempts < max_retries

│ Broker: re-route to healthy node, re-enqueue

│

└─► is_retryable=False OR attempts exhausted

Broker: LPUSH cortex:dlq {failure_metadata}

Broker: DEL cortex:lock, DEL cortex:task

---

## Failure modes

| Failure               | Detection mechanism                    | Recovery                                                                             |
| --------------------- | -------------------------------------- | ------------------------------------------------------------------------------------ |
| Worker crash mid-task | Heartbeat timeout (3 × interval = 30s) | Lease expires, broker reclaims on next healthy worker claim                          |
| Worker CPU spike      | Heartbeat reports CPU ≥ 90%            | Node → Suspect, excluded from new routing, existing leases respected                 |
| Broker restart        | Worker heartbeat RPC fails             | Worker exponential backoff reconnect; broker reconstructs ring from Redis on startup |
| Redis unavailable     | Redis operation returns error          | Broker returns `Status::internal`, task not accepted, no data loss                   |
| Duplicate submission  | task_id found in Redis snapshot        | `Status::already_exists` returned immediately                                        |
| Poison payload        | Task fails max_retries times           | Moved to DLQ, rest of cluster unaffected                                             |
| Network partition     | Worker heartbeat failures accumulate   | Worker reconnects; broker marks node Dead after threshold                            |
| Clock skew            | Lease expires prematurely              | 5-second tolerance window built into lease TTL calculation                           |

---

## Known limitations

**Broker is a single point of failure.** A broker crash loses in-memory
hash ring state. Redis retains task snapshots and leases (no task data is
lost), but the cluster cannot accept new work until the broker restarts.
Target fix: Raft consensus across 3 broker replicas.

**No backpressure from broker to producer.** Tasks submitted faster than
workers can claim them cause Redis queues to grow unbounded. Target fix:
per-node queue depth limits with `Status::resource_exhausted` backpressure.

**Lease requires clock synchronization.** Adequate for single-datacenter
with NTP. Multi-region deployment requires logical clocks.

**Workers are trusted.** A malicious worker could claim tasks and never
report completion, holding leases until expiry. Cortex-MQ is designed for
trusted internal clusters.

---

## Operational reference

```bash
# Check cluster health
grpcurl -plaintext localhost:50051 cortex.BrokerService/GetSystemStatus

# Inspect DLQ
redis-cli LRANGE cortex:dlq 0 -1

# Check a specific task snapshot
redis-cli GET cortex:task:{task_id}

# Release a stuck lock manually
redis-cli DEL cortex:lock:{task_id}

# Inspect a node's pending task queue
redis-cli LRANGE cortex:queue:{node_id} 0 -1

# Count pending tasks across all nodes
redis-cli KEYS cortex:queue:* | xargs redis-cli LLEN
```
