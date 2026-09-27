# ⚡ TaskFlow

> A production-ready, distributed background job processing system built with **Go**, **Redis**, and **PostgreSQL**, featuring strict priority scheduling, atomic delayed execution, exponential backoff retries with full jitter, dead letter queues (DLQ), worker lease management, and an embedded real-time web monitoring dashboard.

---

## 🏛️ System Architecture

```
                          +------------------------------------------+
                          |        Client / External Services        |
                          +------------------------------------------+
                                               |
                                               v
                          +------------------------------------------+
                          |        TaskFlow REST API Server          |
                          |      (Embedded Monitoring Dashboard)     |
                          +------------------------------------------+
                                    |                      |
                   1. Enqueue Job   |                      | 2. Persist State
                                    v                      v
        +-----------------------------------+     +-----------------------------------+
        |       Redis Queue Broker          |     |        PostgreSQL Database        |
        |-----------------------------------|     |-----------------------------------|
        | • taskflow:queue:high   (List)    |     | • jobs (State, Payloads, Results) |
        | • taskflow:queue:default(List)    |     | • job_logs (Attempt Audit Trail)  |
        | • taskflow:queue:low    (List)    |     | • Auto-Migrations & Composite Idx |
        | • taskflow:scheduled    (ZSET)    |     +-----------------------------------+
        | • taskflow:queue:dlq    (List)    |                      ^
        +-----------------------------------+                      |
                        |                                          |
                        | 3. Blocking Dequeue (BRPOP)              | 4. Update Status & Logs
                        v                                          |
        +----------------------------------------------------------+
        |                 Distributed Worker Pool                  |
        |----------------------------------------------------------|
        | • N Concurrent Goroutine Workers                         |
        | • Thread-Safe Handler Registry (Email, Webhook, Image)   |
        | • Atomic Lua Delayed Migrator (ZSET -> Ready Queues)     |
        | • Exponential Backoff with Full Jitter                   |
        | • Distributed Lease Locks & Panic Recovery               |
        | • Graceful OS Signal Termination (SIGINT/SIGTERM)        |
        +----------------------------------------------------------+
```

---

## ✨ Core Features

- **Multi-Level Priority Queuing**: Strict priority ordering (`high` $\rightarrow$ `default` $\rightarrow$ `low`) using Redis `BRPOP` lists with zero-CPU idle sleeping.
- **Delayed & Scheduled Jobs**: Redis Sorted Set (`ZSET`) with Unix millisecond scores, migrated atomically to ready queues via custom Lua scripts to prevent duplicate executions across distributed nodes.
- **Durable State & Audit Logs**: PostgreSQL maintains full job lifecycles and step-by-step execution history per worker attempt.
- **Resilient Retry Mechanism**: Exponential backoff with full jitter formula: $\text{delay} = \min(\text{MaxCap}, \text{BaseDelay} \times 2^{\text{attempt}-1}) \times \text{jitter}$.
- **Dead Letter Queue (DLQ)**: Jobs exceeding `max_retries` are safely quarantined in `taskflow:queue:dlq` for triage.
- **Worker Crash Recovery & Leases**: Leases (`locked_by`, `locked_until`) and goroutine panic recovery protect worker nodes against unexpected failures.
- **Embedded Real-time Dashboard**: Built-in Tailwind web monitoring UI embedded into the Go binary (`//go:embed`) with zero external build dependencies.

---

## 🚀 Quickstart

### Option 1: One-Command Docker Compose (Recommended)

```bash
docker-compose up --build
```
- **Web Dashboard & API**: [http://localhost:8080](http://localhost:8080)
- **PostgreSQL**: `localhost:5432`
- **Redis**: `localhost:6379`

---

### Option 2: Running Locally (Native Go)

#### 1. Configure Environment:
```bash
cp .env.example .env
```

#### 2. Start PostgreSQL & Redis:
- Ensure PostgreSQL is running on port `5432`.
- Ensure Redis is running on port `6379`.

#### 3. Run the API Server:
```bash
go run cmd/api/main.go
```

#### 4. Run the Background Worker:
In a separate terminal window:
```bash
go run cmd/worker/main.go
```

---

## 📡 REST API Reference

| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `POST` | `/api/v1/jobs` | Enqueue a new background task |
| `GET` | `/api/v1/jobs` | List jobs with filters (`status`, `priority`, `type`, `search`, pagination) |
| `GET` | `/api/v1/jobs/:id` | Get job details and complete execution attempt logs |
| `POST` | `/api/v1/jobs/:id/retry` | Manually re-enqueue a failed or dead-letter job |
| `DELETE` | `/api/v1/jobs/:id` | Cancel a pending or scheduled job |
| `GET` | `/api/v1/stats` | Telemetry breakdown (DB counts & Redis queue depths) |
| `GET` | `/health` | Healthcheck (PostgreSQL & Redis connection status) |
| `GET` | `/` or `/dashboard` | Embedded real-time web monitoring UI |

### Example: Enqueue a Delayed Job with High Priority
```bash
curl -X POST http://localhost:8080/api/v1/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "type": "send_email",
    "priority": "high",
    "delay_seconds": 15,
    "max_retries": 3,
    "payload": {
      "to": "user@example.com",
      "subject": "Delayed Activation"
    }
  }'
```

---

## 🧪 Testing

Run the entire automated test suite:
```bash
go test -v ./...
```

Compile and verify all binaries:
```bash
go build ./...
```

---

## 💡 System Design & Interview Concepts

### 1. Why Dual-Storage (PostgreSQL + Redis)?
- **Redis** delivers sub-millisecond in-memory throughput for queue operations ($100\text{k}+$ ops/sec) with blocking pop primitives.
- **PostgreSQL** provides ACID durability, schema validation, complex querying/filtering for dashboards, and permanent audit history.

### 2. Why Atomic Lua Scripts for Scheduled Migrations?
In distributed environments with multiple worker instances polling Redis for delayed jobs, checking `ZRANGEBYSCORE` and removing with `ZREM` across separate network calls creates a **race condition** where two workers can process the exact same scheduled task. Executing the migration inside a Redis **Lua script** runs atomically on the Redis single-threaded event loop, guaranteeing exactly-once scheduling.

### 3. How Exponential Backoff with Jitter Prevents Cascading Failures:
Without jitter, if downstream dependencies (e.g. email or payment gateways) suffer an outage, all failed jobs retry at identical intervals, causing **thundering herd** traffic spikes that repeatedly crash the downstream service. Full jitter randomizes retry timing across workers, smoothing out traffic spikes.