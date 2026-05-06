# Cortex-MQ System Architecture

This document details the design decisions, data flow, and fault-tolerance mechanisms of the Cortex-MQ distributed orchestration system.

## High-Level Topology

The system is designed as a 3-tier microservice architecture communicating exclusively over **HTTP/2 gRPC**.

1. **Cortex-Broker (Rust / Tokio / Tonic)**
   - Acts as the central nervous system.
   - Maintains the global state of the cluster using Redis.
   - Exposes a gRPC API for Worker Nodes to claim tasks, send heartbeats, and report failures.
   - Exposes a read-only gRPC API for the Monitor to scrape cluster telemetry.

2. **Cortex-Swarm (Python / Asyncio / LangGraph)**
   - A dynamic pool of worker nodes that actually execute the AI workloads.
   - Workers are stateless; they maintain a long-lived gRPC connection to the Broker.
   - If a worker crashes mid-task, its heartbeat drops, and the Broker reclaims its leased task for another node.

3. **Cortex-Monitor (Next.js / Node.js)**
   - Because browsers cannot speak raw HTTP/2 gRPC directly, the Next.js API layer (`route.ts`) acts as a secure Server-Side Bridge.
   - It translates the Rust gRPC stream into standard JSON for the React frontend to visualize in real-time.

---

## Core System Mechanics

### 1. Consistent Hashing & Task Routing

To ensure tasks are evenly distributed without a centralized bottleneck, Cortex-MQ uses a **Consistent Hash Ring**.

- When a Python worker connects, it is assigned `VIRTUAL_NODES` (default 150) on the ring.
- When a task is injected, its `UUID` is hashed to a point on the ring, and the task is routed to the nearest virtual node.
- _Benefit:_ When a worker connects or disconnects, only a fraction of tasks need to be re-routed, minimizing cluster thrashing.

### 2. Lease Management & Dead Letter Queue (DLQ)

AI tasks (like calling LLM APIs) take unpredictable amounts of time.

- When a worker claims a task, it is granted a **Lease** (e.g., 300 seconds).
- If the worker does not report back `CompleteTask` or `FailTask` before the lease expires, the Broker assumes the node died.
- The task is automatically stripped from the dead node and placed back into the pending queue.
- If a task fails repeatedly beyond its `max_retries`, it is quarantined into the **Dead Letter Queue (DLQ)** for manual inspection, preventing poison-pill payloads from endlessly crashing the swarm.

### 3. Heartbeat Telemetry & Self-Healing

Python workers run a background `asyncio` task that pulses a `HeartbeatPing` to the Rust broker every 5 seconds.

- The ping includes the node's current CPU usage, memory usage, and active task count.
- If the Rust broker misses 3 consecutive heartbeats from a node, it is officially marked offline and evicted from the Hash Ring.
- On the Python side, if the worker detects the network connection dropped, it engages an **Exponential Backoff Algorithm** to autonomously reconnect without overloading the broker.

---

## Future Extensibility

- **Skill-Based Routing:** Expanding the ClaimRequest protobuf to include worker "tags" (e.g., `gpu_enabled`, `code_reviewer`), allowing the Rust broker to route specialized AI tasks to specific sub-swarms.
- **Persistent Storage:** Migrating the in-memory/Redis state to PostgreSQL for long-term audit logging of all AI task resolutions.
