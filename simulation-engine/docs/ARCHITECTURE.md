# Architecture

## System Overview

```
┌──────────────┐         ┌──────────────────────────────  ┐
│  Client/CLI  │──HTTP──▶│         FastAPI Server         │
└──────────────┘         │  - Job CRUD + Control API      │
                         │  - DAG Submission              │
                         │  - LLM Decomposition Endpoint  │
                         │  - Health + Metrics            │
                         └───────┬──────────┬─────────────┘
                                 │          │
                    ┌────────────┘          └────────────┐
                    ▼                                    ▼
         ┌──────────────────┐                ┌──────────────────┐
         │   PostgreSQL 16  │                │     Redis 7      │
         │  (Source of Truth)│                │  (Queue + Signal)│
         │                  │                │                  │
         │  - jobs          │                │  - Streams per   │
         │  - job_attempts  │                │    priority band │
         │  - job_events    │                │  - Consumer      │
         │  - checkpoints   │                │    groups        │
         │  - dag_graphs    │                │  - Pub/Sub for   │
         │  - dag_edges     │                │    control       │
         │  - worker_registry│               │  - Heartbeat     │
         └──────────────────┘                │    keys w/ TTL   │
                                             └────────┬─────────┘
                                                      │
                  ┌───────────────┬────────────────────┼───────────┐
                  ▼               ▼                    ▼           ▼
          ┌────────────┐  ┌────────────┐  ┌────────────┐  ┌────────────┐
          │  Worker 1  │  │  Worker 2  │  │  Worker 3  │  │  Worker N  │
          │            │  │            │  │            │  │            │
          │  Claim     │  │  Claim     │  │  Claim     │  │  Claim     │
          │  Execute   │  │  Execute   │  │  Execute   │  │  Execute   │
          │  Heartbeat │  │  Heartbeat │  │  Heartbeat │  │  Heartbeat │
          │  Checkpoint│  │  Checkpoint│  │  Checkpoint│  │  Checkpoint│
          └────────────┘  └────────────┘  └────────────┘  └────────────┘

          ┌─────────────────┐    ┌─────────────────┐
          │  Reaper Process  │    │  Retry Scheduler │
          │  (every 30s)     │    │  (every 10s)     │
          │                  │    │                  │
          │  Detect stale    │    │  Find due retries│
          │  leases          │    │  Re-enqueue jobs │
          │  Reclaim dead    │    └─────────────────┘
          │  jobs            │
          │  Reconcile queue │
          └─────────────────┘
```

## Component Responsibilities

### FastAPI Server

The HTTP API layer handling all client interactions. Routes are organized under `/api/v1` with separate modules for jobs, control operations, DAGs, metrics, and health checks. All request validation uses Pydantic v2 models. Database sessions and Redis clients are injected via FastAPI's dependency system. The server never directly modifies job status — all mutations go through the state machine.

### Workers

Custom asyncio processes that claim jobs from Redis streams, execute simulation steps, and manage the heartbeat/checkpoint lifecycle. Each worker generates a unique ID, registers in the `worker_registry` table, and polls Redis streams in strict priority order (critical > high > normal > low). Workers handle pause/cancel signals between simulation steps by subscribing to Redis Pub/Sub channels. On completion, workers record results and notify the DAG executor if the job is part of a dependency graph.

### Reaper Process

A stateless background process that runs every 30 seconds to detect and recover from worker failures. It reclaims jobs with expired leases (worker died mid-execution), force-cancels stale cancelling jobs, and reconciles queue state by re-enqueuing orphaned jobs that exist in Postgres but are missing from Redis. Uses `FOR UPDATE SKIP LOCKED` to prevent conflicts with concurrent reapers.

### Retry Scheduler

A stateless background process that runs every 10 seconds to find jobs in `retry_scheduled` status whose `next_retry_at` has passed. It transitions them back to `queued` and re-enqueues them into the appropriate Redis priority stream, applying priority aging to prevent starvation.

### DAG Executor

Manages the lifecycle of directed acyclic graphs of jobs. When a node completes, the executor checks if its children's dependencies are all satisfied and enqueues them. When a node fails permanently (dead_letter), the executor applies the DAG's failure policy: `fail_fast` cancels everything, `skip_downstream` cancels only the failed node's descendants, and `retry_node` lets the normal retry mechanism handle it before falling back to skip_downstream.

### LLM Planner

Translates natural language research goals into executable DAG specifications using an OpenAI-compatible LLM API. Constructs a system prompt with available simulation types and their parameter schemas, calls the API, parses the JSON response, and validates the result through the DAG validation pipeline before creation.

## Data Flows

### Job Submission

1. Client sends `POST /api/v1/jobs` with type, params, priority
2. FastAPI validates request with Pydantic schema
3. Check backpressure — reject with 429 if queue exceeds limit
4. Check idempotency key — return existing job if duplicate
5. `INSERT` into `jobs` table with `status='pending'`
6. `transition_job(pending -> queued)` — atomic UPDATE with event log
7. `XADD` to Redis stream `jobs:priority:{level}`
8. Return job ID to client

### Job Execution

1. Worker calls `XREADGROUP` from Redis streams in priority order
2. Atomic `UPDATE jobs SET status='running' WHERE id=? AND status='queued'`
3. If 0 rows affected → another worker claimed it → XACK and skip
4. If 1 row → create `job_attempts` row, set `lease_expires_at`
5. Check circuit breaker — if open, push job back to queued
6. Load checkpoint if resuming (restore state, set `starting_step`)
7. Execute simulation steps in a loop:
   - Check for pause/cancel signals via Redis Pub/Sub
   - Execute one simulation step
   - Refresh heartbeat (Redis TTL + Postgres lease)
   - Periodic checkpoint write
8. On completion → `transition_job(running -> completed)` → XACK → notify DAG executor
9. On failure → classify error → retry or dead_letter → notify circuit breaker

### Job Cancellation

1. Client sends `POST /api/v1/jobs/{id}/cancel`
2. If queued → `transition_job(queued -> cancelled)`
3. If running → `transition_job(running -> cancelling)` → `PUBLISH` cancel signal on `job:{id}:control`
4. Worker receives signal → stops at next step boundary → `transition_job(cancelling -> cancelled)`
5. If paused → `transition_job(paused -> cancelled)` directly

### Pause / Resume

1. Pause: API sets `status='pausing'` → publishes pause signal → worker checkpoints state → sets `status='paused'`
2. Resume: API sets `status='resuming'` → re-enqueues to Redis → new worker claims → loads checkpoint → continues from `checkpoint.step + 1`

### DAG Execution

1. Client submits DAG spec (manual or LLM-decomposed)
2. Validate structure: cycles, references, node limits
3. Create `dag_graphs` row + `jobs` rows + `dag_edges` rows in one transaction
4. Enqueue root nodes (nodes with no dependencies)
5. As each node completes: check if children are unblocked → enqueue them
6. When all nodes reach terminal state: determine DAG status (completed / partially_failed / failed)

## Correctness Guarantees

### No job is ever lost

- PostgreSQL is the single source of truth. Even if Redis crashes, all job state is in Postgres.
- The reaper reconciles queue state: if a job is `queued` in Postgres but missing from Redis, it gets re-enqueued.
- Worker crashes are detected by expired leases. The reaper reclaims stale jobs and transitions them to `failed` for retry.

### No job is ever duplicated

- Atomic `UPDATE...WHERE status='queued'` prevents two workers from claiming the same job. Only one UPDATE can match and return a row.
- Idempotency keys prevent duplicate submissions from creating multiple jobs.

### No job is ever stuck

- The reaper runs every 30 seconds and catches: stale running jobs (expired lease), stale pausing jobs, stale cancelling jobs.
- Terminal states (completed, cancelled, dead_letter) have no outgoing transitions — once there, a job stays there.
- The retry scheduler picks up `retry_scheduled` jobs whose timer has expired.

### Checkpoints survive failures

- Atomic checkpoint protocol: mark old checkpoints invalid, insert new one, within a single transaction.
- If the write fails, rollback preserves the previous checkpoint.
- At most one valid checkpoint per job at any time.

## Technology Choices

| Technology | Role | Justification |
|---|---|---|
| FastAPI | HTTP API | Native async/await, Pydantic v2 validation, automatic OpenAPI docs |
| PostgreSQL 16 | Source of truth | ACID transactions for state machine correctness, row-level locking, JSONB for flexible params |
| Redis 7 | Queue + signaling | Streams with consumer groups for reliable delivery, Pub/Sub for real-time control, key expiry for heartbeat TTL |
| asyncio workers | Job execution | Full control over claim-heartbeat-checkpoint-cancel lifecycle; Celery/RQ don't support pause/resume or checkpointing |
| OpenAI SDK (compatible) | LLM integration | Direct API access via any OpenAI-compatible endpoint (Groq, Anthropic, Ollama); no LangChain abstraction overhead |
| SQLAlchemy 2.0 | ORM | Async support via asyncpg, declarative models, migration tooling via Alembic |
| structlog | Logging | Structured JSON output for production, console output for development |
