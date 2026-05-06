# Cortex-MQ 🧠⚡

**A High-Performance, Distributed Orchestration System for AI Agent Swarms.**

Cortex-MQ is a production-grade message broker and telemetry engine designed to orchestrate, route, and monitor autonomous AI agents (powered by LangGraph). Built with a **Rust/gRPC** backend, a **Python** worker swarm, and a **Next.js** real-time dashboard, it demonstrates advanced system design, fault tolerance, and cross-language microservice architecture.

## 🚀 System Highlights

- **Rust-Powered Message Broker:** A high-concurrency, memory-safe gRPC server managing task leases, dead-letter queues (DLQ), and cluster state via Redis.
- **Consistent Hashing Task Routing:** Implements a custom virtual-node hash ring to evenly distribute AI workloads across connected Python workers.
- **Fault-Tolerant Swarm:** Python workers feature autonomous heartbeat monitoring, exponential backoff reconnections, and automatic task requeuing on node failure.
- **Real-Time Telemetry:** A Next.js 14 dashboard bridging gRPC to the browser to visualize live cluster topology, CPU load, and task resolution metrics.
- **Containerized Infrastructure:** Fully orchestrated via Docker Compose for one-click, isolated deployments.

## 🛠️ Tech Stack

| Component    | Technology                   | Description                                   |
| :----------- | :--------------------------- | :-------------------------------------------- |
| **Broker**   | Rust, Tokio, gRPC, Redis     | Core task routing and cluster management.     |
| **Workers**  | Python, LangGraph, OpenAI    | Autonomous AI agents executing LLM pipelines. |
| **Frontend** | Next.js, React, Tailwind CSS | Live cluster visualization and telemetry UI.  |
| **DevOps**   | Docker, Docker Compose       | Multi-stage, optimized container builds.      |

## ⚡ Quick Start

Booting the entire distributed cluster takes less than a minute.

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running.
- An OpenAI API Key (for the Python LangGraph agents).

### 1. Configure Environment

Create a `.env` file inside the `cortex-swarm/` directory:

```env
OPENAI_API_KEY="sk-your-key-here"
# Optional: LangSmith Telemetry
LANGSMITH_API_KEY="your-langsmith-key"
LANGSMITH_PROJECT="cortex-mq"

2. Ignite the Cluster
From the root directory, build and launch the Docker Swarm:

Bash
docker compose up --build

3. Monitor & Trigger
Dashboard: Open http://localhost:3000 to view the live cluster topology.

Trigger Task: Open a local terminal, navigate to cortex-swarm/, and run python trigger.py to inject an AI task into the hash ring. Watch the Swarm resolve it in real-time!
```

Engineered by [Gowtham Angeeru/https://github.com/GowthamAngeeru] - May 2026
