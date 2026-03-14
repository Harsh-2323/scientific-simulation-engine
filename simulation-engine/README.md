# Simulation Engine — Backend

The Python backend for the simulation pipeline. See the [project README](../README.md) for full documentation.

## Dev Quickstart

```bash
docker compose up -d
pip install -e ".[dev]"
alembic upgrade head

# Start services (each in a separate terminal)
uvicorn src.main:app --host 0.0.0.0 --port 8000 --reload
python scripts/run_worker.py
python scripts/run_reaper.py
python scripts/run_retry_scheduler.py
```

## Tests

```bash
pytest tests/unit/ -v         # Pure logic — no DB needed
pytest tests/integration/ -v  # Needs Postgres + Redis running
```

## Scripts

| Script | Purpose |
|---|---|
| `scripts/run_worker.py` | Start a worker process |
| `scripts/run_reaper.py` | Start the stale-lease reaper |
| `scripts/run_retry_scheduler.py` | Start the retry scheduler |
| `scripts/seed_demo.py` | Seed demo jobs + DAG for evaluation |
| `scripts/demo_concurrent.py` | Concurrent worker stress test |

## Configuration

Copy `.env.example` to `.env` and edit as needed. See the [project README](../README.md#environment-variables) for all options.
