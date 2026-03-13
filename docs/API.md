# API Reference

All endpoints are under the `/api/v1` prefix. The API server runs on port 8000 by default.

Authentication is not implemented for this version. In production, add middleware for JWT/API key validation in `src/main.py` before the router includes.

## Jobs

### POST /api/v1/jobs — Submit a job

Creates a new simulation job, transitions it to `queued`, and enqueues it in Redis.

**Request:**
```json
{
  "type": "simulation",
  "priority": "normal",
  "params": {"steps": 100, "reynolds_number": 1e6},
  "max_retries": 3,
  "idempotency_key": "run-001",
  "metadata": {"project": "airfoil-study"}
}
```

**Response (201):**
```json
{
  "id": "a1b2c3d4-...",
  "type": "simulation",
  "status": "queued",
  "priority": "normal",
  "params": {"steps": 100, "reynolds_number": 1000000.0},
  "created_at": "2026-03-12T10:00:00Z",
  "queued_at": "2026-03-12T10:00:00Z",
  "progress_percent": 0.0,
  "retry_count": 0,
  "max_retries": 3
}
```

**Error responses:**
- `409 Conflict` — Idempotency key already exists with different parameters
- `429 Too Many Requests` — Queue depth exceeds backpressure limit (includes `Retry-After: 30` header)

### GET /api/v1/jobs/{job_id} — Get job details

Returns a single job with its execution attempts.

**Response (200):**
```json
{
  "id": "a1b2c3d4-...",
  "type": "simulation",
  "status": "running",
  "priority": "normal",
  "params": {"steps": 100},
  "created_at": "2026-03-12T10:00:00Z",
  "queued_at": "2026-03-12T10:00:01Z",
  "started_at": "2026-03-12T10:00:02Z",
  "progress_percent": 45.0,
  "progress_message": "Step 45/100",
  "worker_id": "host1:12345:ab3f",
  "retry_count": 0,
  "max_retries": 3,
  "attempts": [
    {
      "id": "e5f6g7h8-...",
      "worker_id": "host1:12345:ab3f",
      "attempt_number": 1,
      "started_at": "2026-03-12T10:00:02Z",
      "finished_at": null,
      "status": "running",
      "error_message": null
    }
  ]
}
```

**Error responses:**
- `404 Not Found` — Job does not exist

### GET /api/v1/jobs — List jobs

Returns paginated job list with optional filters.

**Query parameters:**
| Parameter | Type | Default | Description |
|---|---|---|---|
| `status` | string | - | Filter by status |
| `priority` | string | - | Filter by priority |
| `type` | string | - | Filter by job type |
| `dag_id` | UUID | - | Filter by DAG membership |
| `created_after` | datetime | - | Filter by creation time |
| `created_before` | datetime | - | Filter by creation time |
| `limit` | int | 50 | Max results (1-200) |
| `offset` | int | 0 | Pagination offset |
| `sort_by` | string | `created_at` | Sort field |
| `sort_order` | string | `desc` | Sort direction |

**Response (200):**
```json
{
  "items": [{"id": "...", "type": "simulation", "status": "completed", ...}],
  "total": 142,
  "limit": 50,
  "offset": 0
}
```

## Job Control

### POST /api/v1/jobs/{job_id}/cancel — Cancel a job

Cancels a job that is queued, running, or paused.

- **queued** → immediately transitions to `cancelled`
- **running** → transitions to `cancelling`, publishes cancel signal to worker
- **paused** → immediately transitions to `cancelled`

**Response (200):** Updated job with new status

**Error responses:**
- `404 Not Found` — Job does not exist
- `409 Conflict` — Job is in a status that cannot be cancelled

### POST /api/v1/jobs/{job_id}/pause — Pause a running job

Transitions a running job to `pausing` and publishes a pause signal. The worker will checkpoint state at the next step boundary and transition to `paused`.

**Response (200):** Updated job with `status: "pausing"`

**Error responses:**
- `404 Not Found` — Job does not exist
- `409 Conflict` — Job is not currently running

### POST /api/v1/jobs/{job_id}/resume — Resume a paused job

Verifies a valid checkpoint exists, transitions to `resuming`, and re-enqueues the job. A worker will claim it, load the checkpoint, and continue from the saved step.

**Response (200):** Updated job with `status: "resuming"`

**Error responses:**
- `404 Not Found` — Job does not exist
- `409 Conflict` — Job is not paused, or no valid checkpoint exists

## Job History

### GET /api/v1/jobs/{job_id}/events — Job audit log

Returns the full state transition history for a job, in chronological order.

**Response (200):**
```json
{
  "items": [
    {
      "id": "...",
      "event_type": "status_change",
      "old_status": "pending",
      "new_status": "queued",
      "triggered_by": "api",
      "timestamp": "2026-03-12T10:00:00Z",
      "metadata": {}
    }
  ],
  "total": 3
}
```

### GET /api/v1/jobs/{job_id}/checkpoints — List checkpoints

Returns all checkpoints for a job with data previews (first 3 keys).

**Response (200):**
```json
{
  "items": [
    {
      "id": "...",
      "step_number": 50,
      "created_at": "2026-03-12T10:05:00Z",
      "is_valid": true,
      "data_preview": {"step": 50, "velocity": 12.5, "pressure": 101325}
    }
  ],
  "total": 1
}
```

## DAGs

### POST /api/v1/dags — Submit a DAG

Validates the graph structure and creates all jobs and edges in a single transaction. Root nodes are immediately enqueued.

**Request:**
```json
{
  "name": "convergence_study",
  "description": "Mesh convergence analysis",
  "failure_policy": "skip_downstream",
  "nodes": [
    {"node_id": "mesh_coarse", "job_type": "mesh_generation", "params": {"resolution": 50000}},
    {"node_id": "sim_coarse", "job_type": "cfd_simulation", "params": {}, "depends_on": ["mesh_coarse"]},
    {"node_id": "analysis", "job_type": "post_processing", "params": {}, "depends_on": ["sim_coarse"]}
  ]
}
```

**Response (201):**
```json
{
  "id": "...",
  "name": "convergence_study",
  "status": "running",
  "failure_policy": "skip_downstream",
  "created_at": "2026-03-12T10:00:00Z",
  "nodes": [
    {"node_id": "mesh_coarse", "job_id": "...", "job_type": "mesh_generation", "job_status": "queued"},
    {"node_id": "sim_coarse", "job_id": "...", "job_type": "cfd_simulation", "job_status": "pending"},
    {"node_id": "analysis", "job_id": "...", "job_type": "post_processing", "job_status": "pending"}
  ],
  "edges": [
    {"parent_node_id": "mesh_coarse", "child_node_id": "sim_coarse"},
    {"parent_node_id": "sim_coarse", "child_node_id": "analysis"}
  ]
}
```

**Error responses:**
- `422 Unprocessable Entity` — DAG validation failed (cycles, missing references, size limits)

### GET /api/v1/dags/{dag_id} — Get DAG details

Returns a DAG with all its nodes (jobs) and edges.

**Error responses:**
- `404 Not Found`

### GET /api/v1/dags — List DAGs

Returns paginated list of DAGs. Supports `limit` and `offset` query parameters.

### POST /api/v1/dags/{dag_id}/cancel — Cancel a DAG

Cancels all non-terminal jobs in the DAG and sets DAG status to `cancelled`.

**Error responses:**
- `404 Not Found`
- `409 Conflict` — DAG is already in a terminal state

### POST /api/v1/dags/decompose — LLM decomposition

Uses the Anthropic API to decompose a natural language instruction into a DAG specification.

**Request:**
```json
{
  "instruction": "Evaluate drag coefficient for NACA 0012 at Re=1e6 with 50k and 100k meshes",
  "available_types": ["mesh_generation", "cfd_simulation", "post_processing"],
  "failure_policy": "skip_downstream",
  "dry_run": false
}
```

**Response (201):**
```json
{
  "dag": {"id": "...", "name": "...", "status": "running", ...},
  "dag_spec": {"name": "...", "nodes": [...]},
  "validation_result": {"valid": true, "errors": [], "warnings": []},
  "llm_model": "claude-sonnet-4-20250514",
  "llm_latency_ms": 2340
}
```

**Error responses:**
- `422 Unprocessable Entity` — LLM output failed validation
- `502 Bad Gateway` — LLM API call failed

## System

### GET /api/v1/health — Health check

Reports connectivity status of PostgreSQL and Redis.

**Response (200):**
```json
{
  "status": "healthy",
  "postgres": true,
  "redis": true,
  "timestamp": "2026-03-12T10:00:00Z"
}
```

Status values: `healthy` (both up), `degraded` (one down), `unhealthy` (both down).

### GET /api/v1/metrics — System metrics

Returns operational metrics snapshot.

**Response (200):**
```json
{
  "queue_depth": {"critical": 0, "high": 2, "normal": 15, "low": 45},
  "total_queue_depth": 62,
  "active_workers": 4,
  "running_jobs": 4,
  "completed_last_hour": 127,
  "failed_last_hour": 3,
  "dead_letter_count": 1
}
```
