"""
Seed the system with demo data for evaluation.

Submits a mix of jobs at different priorities and a sample DAG,
giving the dashboard and API something to display immediately.

Usage:
    python scripts/seed_demo.py
"""

import asyncio
import sys

import httpx

API = "http://localhost:8000/api/v1"


async def main() -> None:
    async with httpx.AsyncClient(base_url=API, timeout=15) as client:
        # 1. Check health
        print("Checking API health...")
        try:
            r = await client.get("/health")
            health = r.json()
            if not health.get("postgres") or not health.get("redis"):
                print(f"WARNING: services not fully healthy: {health}")
        except httpx.ConnectError:
            print("ERROR: Cannot reach API at localhost:8000. Is the server running?")
            sys.exit(1)

        # 2. Submit individual jobs at various priorities
        print("\nSubmitting demo jobs...")
        jobs = [
            {"type": "cfd_simulation",       "priority": "critical", "params": {"steps": 30,  "step_duration_seconds": 0.05}, "max_retries": 3},
            {"type": "cfd_simulation",       "priority": "high",     "params": {"steps": 50,  "step_duration_seconds": 0.05}, "max_retries": 3},
            {"type": "thermal_simulation",   "priority": "normal",   "params": {"steps": 40,  "step_duration_seconds": 0.05}, "max_retries": 2},
            {"type": "structural_analysis",  "priority": "normal",   "params": {"steps": 60,  "step_duration_seconds": 0.05}, "max_retries": 3},
            {"type": "simulation",           "priority": "low",      "params": {"steps": 80,  "step_duration_seconds": 0.05}, "max_retries": 1},
            {"type": "cfd_simulation",       "priority": "normal",   "params": {"steps": 100, "step_duration_seconds": 0.05}, "max_retries": 3},
            {"type": "post_processing",      "priority": "low",      "params": {"steps": 20,  "step_duration_seconds": 0.05}, "max_retries": 2},
            {"type": "simulation",           "priority": "high",     "params": {"steps": 45,  "step_duration_seconds": 0.05}, "max_retries": 3},
        ]

        submitted = []
        for job in jobs:
            r = await client.post("/jobs", json=job)
            if r.status_code == 201:
                data = r.json()
                submitted.append(data["id"])
                print(f"  [{data['priority']:>8}] {data['type']:<25} → {data['id'][:12]}...")
            else:
                print(f"  FAILED: {r.status_code} {r.text[:80]}")

        # 3. Submit a DAG (mesh convergence study)
        print("\nSubmitting demo DAG (mesh convergence study)...")
        dag_spec = {
            "name": "mesh_convergence_study",
            "description": "Compare coarse vs fine mesh CFD simulation results",
            "failure_policy": "skip_downstream",
            "nodes": [
                {
                    "node_id": "mesh_coarse",
                    "job_type": "simulation",
                    "params": {"steps": 25, "step_duration_seconds": 0.05, "mesh_resolution": 50000},
                },
                {
                    "node_id": "mesh_fine",
                    "job_type": "simulation",
                    "params": {"steps": 25, "step_duration_seconds": 0.05, "mesh_resolution": 200000},
                },
                {
                    "node_id": "sim_coarse",
                    "job_type": "cfd_simulation",
                    "params": {"steps": 40, "step_duration_seconds": 0.05, "solver": "openfoam"},
                    "depends_on": ["mesh_coarse"],
                },
                {
                    "node_id": "sim_fine",
                    "job_type": "cfd_simulation",
                    "params": {"steps": 40, "step_duration_seconds": 0.05, "solver": "openfoam"},
                    "depends_on": ["mesh_fine"],
                },
                {
                    "node_id": "compare_results",
                    "job_type": "post_processing",
                    "params": {"steps": 15, "step_duration_seconds": 0.05, "analysis_type": "comparison"},
                    "depends_on": ["sim_coarse", "sim_fine"],
                },
            ],
        }
        r = await client.post("/dags", json=dag_spec)
        if r.status_code == 201:
            dag = r.json()
            print(f"  DAG created: {dag['id'][:12]}... ({len(dag['nodes'])} nodes)")
            for node in dag["nodes"]:
                print(f"    {node['node_id']:<20} → {node['job_status']}")
        else:
            print(f"  FAILED: {r.status_code} {r.text[:120]}")

        # 4. Summary
        print(f"\nDone! Seeded {len(submitted)} jobs + 1 DAG.")
        print(f"Open the dashboard at http://localhost:5173 to watch them execute.")
        print(f"API docs at http://localhost:8000/docs")

        # 5. Quick metrics check
        await asyncio.sleep(1)
        r = await client.get("/metrics")
        if r.status_code == 200:
            m = r.json()
            print(f"\nCurrent queue: {m.get('total_queue_depth', 0)} jobs queued, "
                  f"{m.get('running_jobs', 0)} running")


if __name__ == "__main__":
    asyncio.run(main())
