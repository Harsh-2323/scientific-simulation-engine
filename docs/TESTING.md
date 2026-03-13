# Testing Guide

## Prerequisites

Tests run against real PostgreSQL and Redis instances via Docker Compose.

```bash
# Start infrastructure
docker-compose up -d

# Run database migrations
alembic upgrade head

# Install dev dependencies
pip install -e ".[dev]"
```

## Running Tests

### All tests

```bash
pytest tests/ -v
```

### Unit tests

```bash
pytest tests/unit/ -v
```

Pure logic tests that validate the state machine transition table, priority aging calculations, circuit breaker state transitions, and DAG validation (cycle detection, topological sort, root node identification).

Tests included:
- `test_state_machine.py` — Valid/invalid transitions, event logging, optimistic locking, concurrent claims, terminal state enforcement
- `test_scheduler.py` — Priority aging with various queue times, cap at critical, no aging below threshold
- `test_circuit_breaker.py` — Closed/open/half-open state transitions, threshold behavior, probe gating, independent per job type
- `test_dag_validator.py` — Linear and diamond DAGs, cycle detection, self-references, missing dependencies, duplicate node IDs, size limits, semantic warnings

### Integration tests

```bash
pytest tests/integration/ -v
```

End-to-end tests that exercise the full job lifecycle against real Postgres and Redis. Each test gets a fresh database session and Redis client.

Tests included:
- `test_job_lifecycle.py` — Full lifecycle (pending -> completed), retry lifecycle, dead letter lifecycle, API endpoint tests (submit, get, list, health, metrics), idempotency key behavior
- `test_cancel_pause.py` — Cancel from queued/running/paused/pending states, pause/resume flow with worker acknowledgment
- `test_dag_execution.py` — Diamond DAG execution order, linear DAG progression, skip_downstream failure policy, fail_fast failure policy

## Test Infrastructure

### Database session isolation

Each test gets its own database session via the `db_session` fixture. The session uses `NullPool` to prevent asyncpg connection state from leaking between tests. Jobs created by `job_factory` are committed to the real database.

### Redis isolation

Each test gets its own Redis client via the `redis_client` fixture. The database is flushed (`FLUSHDB`) after each test to prevent state leakage.

### API client

The `api_client` fixture provides an `httpx.AsyncClient` connected to the FastAPI app via `ASGITransport`. The Redis dependency is overridden to use the test Redis client.

### Fixtures

| Fixture | Scope | Description |
|---|---|---|
| `db_session` | function | Fresh async database session with NullPool |
| `redis_client` | function | Redis client, flushed after each test |
| `api_client` | function | HTTP test client with Redis override |
| `job_factory` | function | Creates and commits test jobs with defaults |
| `dag_factory` | function | Creates test DAGs with jobs and edges |

## Environment Variables for LLM Tests

LLM decomposition tests require a valid Anthropic API key:

```bash
export ANTHROPIC_API_KEY="sk-ant-..."
pytest tests/integration/ -v -k "llm or decompose"
```

Without the key, LLM tests will fail with a 502 error (expected behavior — the API call fails).

## Interpreting Failures

### asyncpg connection errors

If you see `cannot perform operation: another operation is in progress`, the session isn't being properly flushed between operations. The `transition_job()` function flushes after each event write to prevent this.

### PostgreSQL ENUM type errors

If you see `column "status" is of type job_status but expression is of type character varying`, the SQLAlchemy model column types don't match the migration. Ensure columns use `Enum(..., name='job_status', create_type=False)` instead of `String(50)`.

### Redis connection refused

Ensure Docker Compose services are running: `docker-compose ps`. Redis should be on port 6379.

### Migration errors

If tables don't exist, run `alembic upgrade head`. If you get enum type conflicts, drop and recreate: `alembic downgrade base && alembic upgrade head`.
