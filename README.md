# Scientific Simulation Engine

A distributed job execution engine purpose-built for long-running scientific simulations. Provides job submission, monitoring, pause/resume with checkpointing, cancellation, priority scheduling, automatic retry with circuit breaking, DAG orchestration, and LLM-powered decomposition of natural-language research goals into executable dependency graphs.

PostgreSQL is the single source of truth for all job state. Redis provides high-throughput queuing and real-time signaling. Custom asyncio workers implement a claim-heartbeat-checkpoint lifecycle that guarantees no job is ever silently lost, duplicated, or stuck.

## Architecture

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for detailed system design.

```
Client ─── HTTP ──▶ FastAPI Server ──┬──▶ PostgreSQL (source of truth)
                                     └──▶ Redis (queue + signals)
                                              │
                              ┌────────────────┼────────────────┐
                              ▼                ▼                ▼
                          Worker 1         Worker 2         Worker N

                          Reaper (every 30s)    Retry Scheduler (every 10s)
```

## Quick Start

```bash
# Start infrastructure
docker-compose up -d

# Install dependencies
pip install -e ".[dev]"

# Run database migrations
alembic upgrade head

# Start the API server
uvicorn src.main:app --host 0.0.0.0 --port 8000

# Start a worker (separate terminal)
python scripts/run_worker.py

# Start the reaper (separate terminal)
python scripts/run_reaper.py

# Start the retry scheduler (separate terminal)
python scripts/run_retry_scheduler.py
```

## API Examples

### Submit a job

```bash
curl -X POST http://localhost:8000/api/v1/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "type": "simulation",
    "priority": "normal",
    "params": {"steps": 100},
    "max_retries": 3
  }'
```

### Check job status

```bash
curl http://localhost:8000/api/v1/jobs/{job_id}
```

### Pause a running job

```bash
curl -X POST http://localhost:8000/api/v1/jobs/{job_id}/pause
```

### Resume a paused job

```bash
curl -X POST http://localhost:8000/api/v1/jobs/{job_id}/resume
```

### Cancel a job

```bash
curl -X POST http://localhost:8000/api/v1/jobs/{job_id}/cancel
```

### Submit a DAG

```bash
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
```

### Decompose with LLM

```bash
curl -X POST http://localhost:8000/api/v1/dags/decompose \
  -H "Content-Type: application/json" \
  -d '{
    "instruction": "Evaluate drag coefficient convergence for NACA 0012 at Re=1e6 using k-omega SST with 50k, 100k, and 200k cell meshes",
    "dry_run": false
  }'
```

### System metrics

```bash
curl http://localhost:8000/api/v1/metrics
```

## Configuration

All settings are configurable via environment variables or a `.env` file. See `.env.example` for all options.

Key settings:

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | `postgresql+asyncpg://postgres:postgres@localhost:5433/simengine` | PostgreSQL connection URL |
| `REDIS_URL` | `redis://localhost:6379/0` | Redis connection URL |
| `WORKER_LEASE_TIMEOUT_SECONDS` | `30` | Worker lease duration |
| `DEFAULT_MAX_RETRIES` | `3` | Default retry attempts |
| `AGING_THRESHOLD_SECONDS` | `300` | Queue time before priority bump |
| `CIRCUIT_BREAKER_FAILURE_THRESHOLD` | `5` | Failures to trip breaker |
| `ANTHROPIC_API_KEY` | `""` | API key for LLM decomposition |

## Testing

```bash
# Run all tests
pytest tests/ -v

# Unit tests only
pytest tests/unit/ -v

# Integration tests only
pytest tests/integration/ -v
```

See [docs/TESTING.md](docs/TESTING.md) for test infrastructure requirements.

## Design Decisions

1. **PostgreSQL as single source of truth over Redis for state.** Redis is fast but not durable enough for job state that must survive crashes. By keeping all state in Postgres with ACID transactions, we guarantee that no job is ever lost, even if Redis crashes and loses its queue. The reaper reconciles any drift.

2. **Custom asyncio workers over Celery/RQ.** Off-the-shelf task queues don't support pause/resume with checkpointing, custom lease semantics, or the claim-heartbeat-checkpoint lifecycle needed for long-running scientific simulations. Custom workers give full control over every step.

3. **WHERE-guarded atomic state transitions over optimistic locking.** Instead of read-modify-write with version checks, every state change is a single `UPDATE...WHERE status IN (expected)` returning the updated row. This eliminates the read-write race entirely and makes concurrent claims naturally safe.

4. **Per job-type circuit breaker over global throttling.** A broken simulation binary should only block that job type, not the entire system. Redis-backed circuit breaker state is shared across all workers, preventing retry storms while letting healthy job types continue.

5. **Checkpoint-based pause/resume over process suspension.** Suspending OS processes doesn't survive worker restarts or migrations. By serializing simulation state to a checkpoint table and resuming from `checkpoint.step + 1`, jobs can be paused on one machine and resumed on any other.

## Documentation

- [Architecture](docs/ARCHITECTURE.md) — System design, data flows, correctness guarantees
- [State Machine](docs/STATE_MACHINE.md) — Complete state diagram and transition rules
- [API Reference](docs/API.md) — All endpoints with examples
- [Testing Guide](docs/TESTING.md) — How to run and interpret tests
