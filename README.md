# Resumable Agentic Simulation Pipeline

A distributed job execution engine purpose-built for long-running scientific simulations. Jobs can be submitted, monitored, paused, resumed, and cancelled — with correctness guarantees across all of it.

PostgreSQL is the single source of truth for all job state. Redis provides high-throughput queuing and real-time signaling. Custom asyncio workers implement a claim-heartbeat-checkpoint lifecycle that guarantees **no job is ever silently lost, duplicated, or stuck**.

```
Client ─── HTTP ──▶ FastAPI Server ──┬──▶ PostgreSQL 16 (source of truth)
                                     └──▶ Redis 7 (queue + signals)
                                              │
                              ┌────────────────┼────────────────┐
                              ▼                ▼                ▼
                          Worker 1         Worker 2         Worker N

                          Reaper (every 30s)    Retry Scheduler (every 10s)
```

## Quick Demo

```bash
cd simulation-engine
docker compose up -d                # Start Postgres + Redis
pip install -e ".[dev]"             # Install dependencies
alembic upgrade head                # Run migrations
cp .env.example .env                # Create config (edit LLM_API_KEY for AI features)

# Start services (each in a separate terminal)
uvicorn src.main:app --host 0.0.0.0 --port 8000 --reload   # API server
python scripts/run_worker.py                                  # Worker

# Seed demo data and watch it execute
python scripts/seed_demo.py         # Submits 8 jobs + a 5-node DAG
```

Open **http://localhost:5173** (dashboard) to watch jobs flow through `queued → running → completed` in real-time. API docs at **http://localhost:8000/docs**.

```bash
# Prove concurrent claim safety: 3 workers racing for 50 jobs, zero duplicates
python scripts/demo_concurrent.py
```

## Features Implemented

### Level 1 — Core

- **Async job queue with workers**: FastAPI server accepts job submissions, enqueues to Redis Streams by priority, workers claim via `XREADGROUP`
- **Persistent job state**: All state lives in PostgreSQL with ACID transactions. 11 job statuses governed by an atomic state machine (`UPDATE...WHERE status IN (expected)`)
- **Status API**: Full CRUD — submit, get, list (with filters/pagination), event audit log, checkpoint inspection
- **Clean cancellation**: Cancel from any cancelable state. Running jobs receive a signal via Redis Pub/Sub and stop at the next step boundary
- **Correctness on worker death**: Heartbeat-based leases (Redis TTL + Postgres `lease_expires_at`). Reaper detects expired leases every 30s and reclaims stale jobs for retry

### Level 2 — Reliability

- **Retry with exponential backoff**: Configurable per job — max retries, base backoff, multiplier, ceiling. Transient errors trigger retry; permanent errors go straight to dead letter
- **Per job-type circuit breaker**: Redis-backed failure counter per job type. Trips after N failures in a time window, enters half-open after cooldown to probe recovery. A broken simulation type blocks only itself
- **Priority scheduling with starvation prevention**: 4 priority bands (critical/high/normal/low). Jobs waiting longer than threshold get bumped one level (priority aging)
- **Backpressure**: Queue depth check before enqueue — returns `429 Retry-After: 30` when the system is overloaded
- **Pause/resume with checkpointing**: Workers serialize simulation state to a checkpoint table. Resume loads the latest checkpoint and continues from `step + 1`. Survives worker restarts and cross-machine migration
- **Idempotency keys**: Duplicate submissions with the same key return the existing job instead of creating a new one
- **Graceful shutdown**: Workers finish in-flight work and checkpoint before exiting on SIGTERM/SIGINT
- **Queue reconciliation**: Reaper detects jobs that are `queued` in Postgres but missing from Redis (e.g. after Redis crash) and re-enqueues them

### Level 3 — Intelligence

- **DAG orchestration**: Submit a graph of jobs with dependencies. Root nodes enqueue immediately; children enqueue when all parents complete. Three failure policies: `fail_fast`, `skip_downstream`, `retry_node`
- **LLM-powered decomposition**: `POST /api/v1/dags/decompose` accepts a natural language research goal and uses an LLM (OpenAI-compatible API) to break it into a validated DAG of simulation sub-jobs
- **Output validation**: LLM responses are parsed (handling markdown fences, extra text), validated against the DAG schema (cycle detection, reference integrity, node limits), and rejected with detailed errors if invalid
- **Decomposition quality heuristics**: Beyond schema validation, the system flags suspicious LLM output — over-granular decompositions, missing post-processing nodes when the goal implies comparison, fully sequential chains where parallelism is possible, and excessive DAG depth

### Bonus

- **React dashboard**: Real-time Mission Control with job metrics, job explorer, DAG visualizer, job submission form, and system health view
- **Structured logging**: JSON or console output via structlog with context binding
- **OpenAPI docs**: Auto-generated at `/docs` by FastAPI
- **Full test suite**: Unit tests (state machine, scheduler, circuit breaker, DAG validator) + integration tests (full lifecycle, cancel/pause, DAG execution, edge cases) running against real Postgres and Redis via testcontainers

### Known Limitations

- Simulation steps are mocked (`asyncio.sleep`) — real simulation binary integration would require a process executor with stdout/stderr capture
- No authentication or authorization — production would need JWT/API key middleware
- Single-node Redis — no Sentinel or Cluster configuration for Redis HA
- LLM decomposition quality depends on the model; no automatic quality scoring of the generated DAG beyond schema validation
- Workers poll Redis streams rather than using push — adds small latency (configurable, default 500ms block timeout)
- No persistent storage for simulation artifacts (input/output files) — only JSON params/results in Postgres

## Setup & Run

### Prerequisites

- Python 3.11+
- Docker (for PostgreSQL 16 and Redis 7)
- Node.js 18+ (optional, for dashboard)

### Quick Start

```bash
# 1. Clone and enter the project
cd simulation-engine

# 2. Start Postgres + Redis
docker compose up -d

# 3. Create virtualenv and install
python -m venv .venv
source .venv/bin/activate    # or .venv\Scripts\activate on Windows
pip install -e ".[dev]"

# 4. Configure environment
cp .env.example .env
# Edit .env — set LLM_API_KEY if you want LLM decomposition

# 5. Run database migrations
alembic upgrade head

# 6. Start services (each in a separate terminal)
uvicorn src.main:app --host 0.0.0.0 --port 8000 --reload   # API server
python scripts/run_worker.py                                  # Worker
python scripts/run_reaper.py                                  # Reaper
python scripts/run_retry_scheduler.py                         # Retry scheduler
```

On Windows, `start.bat` in the project root opens everything in separate terminals.

### Environment Variables

All settings are in `.env.example`. Key ones:

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | `postgresql+asyncpg://postgres:postgres@localhost:5433/simengine` | PostgreSQL connection |
| `REDIS_URL` | `redis://localhost:6379/0` | Redis connection |
| `LLM_API_KEY` | `""` | API key for LLM decomposition (any OpenAI-compatible provider) |
| `LLM_BASE_URL` | `https://api.groq.com/openai/v1` | LLM API base URL |
| `LLM_MODEL` | `moonshotai/kimi-k2-instruct` | Model for DAG decomposition |
| `WORKER_LEASE_TIMEOUT_SECONDS` | `30` | Worker lease duration before reaper reclaims |
| `DEFAULT_MAX_RETRIES` | `3` | Default retry attempts per job |
| `AGING_THRESHOLD_SECONDS` | `300` | Queue wait time before priority bump |
| `CIRCUIT_BREAKER_FAILURE_THRESHOLD` | `5` | Failures of same type to trip breaker |
| `BACKPRESSURE_QUEUE_DEPTH_LIMIT` | `10000` | Queue depth that triggers 429 |

### Dashboard (Optional)

```bash
cd dashboard
npm install
npm run dev
# Opens at http://localhost:5173
```

### Running Tests

```bash
# Ensure Docker services are running
docker compose up -d && alembic upgrade head

pytest tests/ -v              # All tests
pytest tests/unit/ -v         # Unit tests only
pytest tests/integration/ -v  # Integration tests (needs Postgres + Redis)
```

## API Examples

```bash
# Submit a job
curl -X POST http://localhost:8000/api/v1/jobs \
  -H "Content-Type: application/json" \
  -d '{"type": "simulation", "priority": "normal", "params": {"steps": 100}, "max_retries": 3}'

# Check status
curl http://localhost:8000/api/v1/jobs/{job_id}

# Pause / Resume / Cancel
curl -X POST http://localhost:8000/api/v1/jobs/{job_id}/pause
curl -X POST http://localhost:8000/api/v1/jobs/{job_id}/resume
curl -X POST http://localhost:8000/api/v1/jobs/{job_id}/cancel

# Submit a DAG
curl -X POST http://localhost:8000/api/v1/dags \
  -H "Content-Type: application/json" \
  -d '{
    "name": "mesh_convergence_study",
    "failure_policy": "skip_downstream",
    "nodes": [
      {"node_id": "mesh_coarse", "job_type": "mesh_generation", "params": {"resolution": 50000}},
      {"node_id": "mesh_fine", "job_type": "mesh_generation", "params": {"resolution": 200000}},
      {"node_id": "sim_coarse", "job_type": "cfd_simulation", "params": {"solver": "openfoam"}, "depends_on": ["mesh_coarse"]},
      {"node_id": "sim_fine", "job_type": "cfd_simulation", "params": {"solver": "openfoam"}, "depends_on": ["mesh_fine"]},
      {"node_id": "compare", "job_type": "post_processing", "params": {"analysis_type": "comparison"}, "depends_on": ["sim_coarse", "sim_fine"]}
    ]
  }'

# LLM decomposition
curl -X POST http://localhost:8000/api/v1/dags/decompose \
  -H "Content-Type: application/json" \
  -d '{"instruction": "Evaluate drag coefficient convergence for NACA 0012 at Re=1e6 using k-omega SST with 50k, 100k, and 200k cell meshes", "dry_run": false}'

# System metrics
curl http://localhost:8000/api/v1/metrics
```

## Key Architectural Decisions

**1. PostgreSQL as single source of truth, not Redis.**
Redis is fast but volatile. By keeping all job state in Postgres with ACID transactions, we guarantee that no job is ever lost — even if Redis crashes and loses its entire queue. The reaper process reconciles any drift by scanning Postgres for jobs that should be in Redis but aren't. This trade-off costs us a database write on every state transition, but for long-running simulations (minutes to hours per job) that overhead is negligible.

**2. Custom asyncio workers instead of Celery or RQ.**
Off-the-shelf task queues don't support mid-execution pause/resume with persistent checkpointing, custom lease semantics, or fine-grained heartbeat management. Building workers from scratch with asyncio gives full control over the claim → heartbeat → checkpoint → signal-check → complete lifecycle. The cost is more code to maintain, but it's the only way to get true resumability for long-running simulations that can be paused on one machine and resumed on another.

**3. Atomic WHERE-guarded state transitions instead of optimistic locking.**
Every state change is a single `UPDATE jobs SET status=$new WHERE id=$id AND status IN ($expected) RETURNING *`. If the WHERE matches 0 rows, someone else already moved the job — no read-modify-write race is possible. This makes concurrent worker claims naturally safe without distributed locks. The trade-off is that callers must handle the "0 rows updated" case, but that's simpler than managing version counters or advisory locks.

**4. Per job-type circuit breaker instead of global throttling.**
A broken simulation binary (e.g., a segfaulting CFD solver) should only block that job type, not the entire system. The circuit breaker tracks failures per type in Redis, trips after a configurable threshold, and half-opens after cooldown to probe recovery. Healthy job types continue executing normally. This prevents retry storms without over-protecting the system.

**5. OpenAI-compatible SDK for LLM integration, not LangChain.**
The LLM planner uses the `openai` Python SDK pointed at any OpenAI-compatible endpoint (Groq, Anthropic, Ollama, etc.) via `LLM_BASE_URL`. This is a thin wrapper — one API call, one JSON parse, one schema validation. No LangChain or LlamaIndex. Every line of the decomposition pipeline is visible in `src/dag/llm_planner.py` (~100 lines). The system prompt, parsing logic, and error handling are all explicit.

## Project Structure

```
simulation-engine/          # Python backend
├── src/
│   ├── main.py             # FastAPI app entry point
│   ├── config.py           # Pydantic settings (env vars)
│   ├── models/             # SQLAlchemy models (job, checkpoint, dag, worker)
│   ├── schemas/            # Pydantic request/response schemas
│   ├── api/routes/         # HTTP endpoints (jobs, control, dag, metrics, health)
│   ├── core/               # State machine, scheduler, circuit breaker, reaper
│   ├── db/repositories/    # Database CRUD operations
│   ├── queue/              # Redis stream wrappers
│   ├── worker/             # Worker process (executor, checkpoint, heartbeat, signals)
│   └── dag/                # DAG executor, validator, LLM planner
├── scripts/                # Worker, reaper, retry scheduler, seed_demo, demo_concurrent
├── tests/                  # Unit + integration tests
├── alembic/                # Database migrations
├── docs/                   # ARCHITECTURE.md, STATE_MACHINE.md, API.md, TESTING.md
└── docker-compose.yml      # Postgres 16 + Redis 7

dashboard/                  # React + Vite frontend
├── src/pages/              # Dashboard, JobExplorer, DagVisualizer, SubmitJob, SystemHealth
└── src/api/client.js       # Axios HTTP client
```

## What I Would Do Differently With More Time

**Horizontal worker scaling with proper load balancing.** Currently workers independently poll Redis streams, which works but doesn't account for worker capacity or locality. With more time I'd add a centralized scheduler that assigns jobs to workers based on their current load, available memory, and proximity to cached simulation data — particularly important when simulation binaries need large input files that are expensive to transfer.

I'd also add **real simulation binary integration** with process isolation (containerized execution), artifact storage (S3/MinIO for input meshes and output fields), and streaming log capture. The current mock simulation runner proves the checkpointing and control flow work correctly, but production use needs the ability to wrap arbitrary binaries and detect their failure modes (segfault vs. OOM vs. convergence failure) for smarter error classification.

Finally, **observability**: Prometheus metrics export, distributed tracing with OpenTelemetry across API → queue → worker, and alerting on circuit breaker trips and dead letter growth. The structured logging foundation is there, but production needs dashboards that show queue depth trends, p99 job latency, and worker utilization over time.

## Documentation

- [Architecture](simulation-engine/docs/ARCHITECTURE.md) — System design, data flows, correctness guarantees
- [State Machine](simulation-engine/docs/STATE_MACHINE.md) — Complete state diagram and transition rules
- [API Reference](simulation-engine/docs/API.md) — All endpoints with request/response examples
- [Testing Guide](simulation-engine/docs/TESTING.md) — How to run and interpret tests
