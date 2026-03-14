# Dashboard — Frontend

React + Vite frontend for the simulation engine. Provides real-time monitoring via polling the backend API.

## Pages

| Route | Page | Description |
|---|---|---|
| `/` | Mission Control | Queue depth, running jobs, throughput, system status |
| `/jobs` | Job Explorer | Filterable job list with detail modal (pause/resume/cancel) |
| `/dags` | DAG Planner | LLM decomposition + ReactFlow DAG visualization |
| `/submit` | Submit Job | Form to dispatch a simulation job |
| `/health` | System Health | Postgres/Redis status, dead letter count |

## Setup

```bash
npm install
npm run dev
# Opens at http://localhost:5173
```

Requires the backend API running at `http://localhost:8000`.
