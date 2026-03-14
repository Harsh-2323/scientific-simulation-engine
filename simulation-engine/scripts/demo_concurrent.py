"""
Concurrent worker stress test.

Spawns multiple workers and submits many jobs simultaneously to prove
that atomic claims prevent duplicates and all jobs complete exactly once.

Usage:
    python scripts/demo_concurrent.py
    python scripts/demo_concurrent.py --workers 5 --jobs 100
"""

import asyncio
import sys
import time

import httpx

sys.path.insert(0, ".")

from src.config import settings
from src.worker.base import Worker

API = "http://localhost:8000/api/v1"


async def submit_jobs(count: int) -> list[str]:
    """Submit N jobs via the API and return their IDs."""
    job_ids = []
    async with httpx.AsyncClient(base_url=API, timeout=15) as client:
        for i in range(count):
            priority = ["critical", "high", "normal", "low"][i % 4]
            r = await client.post("/jobs", json={
                "type": "simulation",
                "priority": priority,
                "params": {"steps": 20, "step_duration_seconds": 0.01},
                "max_retries": 1,
            })
            if r.status_code == 201:
                job_ids.append(r.json()["id"])
            else:
                print(f"  Submit failed: {r.status_code}")
    return job_ids


async def run_worker(worker_id: str, stop_event: asyncio.Event) -> None:
    """Run a worker until the stop event is set."""
    worker = Worker(worker_id=worker_id)

    async def watch_stop():
        await stop_event.wait()
        await worker.shutdown()

    asyncio.create_task(watch_stop())

    try:
        await worker.start()
    except asyncio.CancelledError:
        pass


async def poll_completion(job_ids: list[str], timeout: float = 120) -> dict:
    """Poll until all jobs reach a terminal state or timeout."""
    terminal = {"completed", "failed", "cancelled", "dead_letter"}
    start = time.time()

    async with httpx.AsyncClient(base_url=API, timeout=15) as client:
        while time.time() - start < timeout:
            statuses = {}
            for jid in job_ids:
                r = await client.get(f"/jobs/{jid}")
                if r.status_code == 200:
                    statuses[jid] = r.json()["status"]

            done = sum(1 for s in statuses.values() if s in terminal)
            if done == len(job_ids):
                return statuses

            elapsed = time.time() - start
            print(f"  [{elapsed:.0f}s] {done}/{len(job_ids)} terminal...", end="\r")
            await asyncio.sleep(1)

    return statuses


async def main() -> None:
    # Parse args
    num_workers = 3
    num_jobs = 50
    args = sys.argv[1:]
    for i, arg in enumerate(args):
        if arg == "--workers" and i + 1 < len(args):
            num_workers = int(args[i + 1])
        elif arg == "--jobs" and i + 1 < len(args):
            num_jobs = int(args[i + 1])

    print(f"Concurrent Worker Demo")
    print(f"  Workers: {num_workers}")
    print(f"  Jobs:    {num_jobs}")
    print()

    # Check API is up
    async with httpx.AsyncClient(base_url=API, timeout=5) as client:
        try:
            await client.get("/health")
        except httpx.ConnectError:
            print("ERROR: API not reachable at localhost:8000")
            sys.exit(1)

    # Submit all jobs
    print("Submitting jobs...")
    start = time.time()
    job_ids = await submit_jobs(num_jobs)
    submit_time = time.time() - start
    print(f"  Submitted {len(job_ids)} jobs in {submit_time:.1f}s")

    # Start workers
    print(f"\nStarting {num_workers} workers...")
    stop_event = asyncio.Event()
    worker_tasks = []
    for i in range(num_workers):
        wid = f"demo-worker-{i+1}"
        task = asyncio.create_task(run_worker(wid, stop_event))
        worker_tasks.append(task)
        print(f"  Started {wid}")

    # Wait for all jobs to complete
    print(f"\nWaiting for completion...")
    exec_start = time.time()
    statuses = await poll_completion(job_ids)
    exec_time = time.time() - exec_start

    # Stop workers
    stop_event.set()
    await asyncio.sleep(1)
    for t in worker_tasks:
        t.cancel()
    await asyncio.gather(*worker_tasks, return_exceptions=True)

    # Report results
    print(f"\n{'='*50}")
    print(f"RESULTS")
    print(f"{'='*50}")

    counts = {}
    for s in statuses.values():
        counts[s] = counts.get(s, 0) + 1

    for status, count in sorted(counts.items()):
        print(f"  {status:<20} {count}")

    completed = counts.get("completed", 0)
    total = len(job_ids)

    print(f"\n  Total:       {total}")
    print(f"  Completed:   {completed} ({completed/total*100:.0f}%)")
    print(f"  Exec time:   {exec_time:.1f}s")
    print(f"  Throughput:   {completed/exec_time:.1f} jobs/s")

    # Verify no duplicates: each job should have exactly 1 completion event
    print(f"\nVerifying no duplicate executions...")
    duplicates = 0
    async with httpx.AsyncClient(base_url=API, timeout=15) as client:
        for jid in job_ids[:10]:  # Spot-check first 10
            r = await client.get(f"/jobs/{jid}/events")
            if r.status_code == 200:
                events = r.json()["items"]
                completions = [e for e in events if e["new_status"] == "completed"]
                if len(completions) > 1:
                    duplicates += 1
                    print(f"  DUPLICATE: {jid} completed {len(completions)} times!")

    if duplicates == 0:
        print(f"  No duplicates detected (checked {min(10, len(job_ids))} jobs)")
    else:
        print(f"  WARNING: {duplicates} duplicate completions found!")

    print()


if __name__ == "__main__":
    asyncio.run(main())
