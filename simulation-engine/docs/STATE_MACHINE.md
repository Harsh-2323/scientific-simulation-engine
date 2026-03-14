# Job State Machine

All job status changes MUST go through `transition_job()` in `src/core/state_machine.py`. Direct assignment of `job.status` is forbidden because it bypasses the atomic WHERE-guard that prevents race conditions.

## State Diagram

```
                          ┌─────────┐
                  ┌──────▶│completed│ (terminal)
                  │       └─────────┘
                  │
┌───────┐    ┌──────┐    ┌───────┐    ┌──────────┐    ┌───────┐
│pending│───▶│queued│───▶│running│───▶│ failed   │───▶│dead_  │ (terminal)
└───┬───┘    └──┬───┘    └──┬─┬──┘    └────┬─────┘    │letter │
    │           │           │ │            │          └───────┘
    │           │           │ │            │
    │           │           │ │     ┌──────┴──────┐    ┌──────┐
    │           │           │ │     │retry_       │───▶│queued│
    │           │           │ │     │scheduled    │    └──────┘
    │           │           │ │     └─────────────┘
    │           │           │ │
    │           │           │ ├──▶┌──────────┐    ┌──────┐
    │           │           │ │   │cancelling│───▶│cancel│ (terminal)
    │           │           │ │   └──────────┘    │led   │
    │     cancel│     cancel│ │                   └──┬───┘
    └───────────┼───────────┘ │                      │
                │             │                      │
                │             ├──▶┌───────┐    ┌─────┘
                │             │   │pausing│───▶│
                │             │   └───────┘    │
                │             │          │     │
                │             │          ▼     │
                │             │   ┌──────┐     │
                │             │   │paused│─────┘
                │             │   └──┬───┘
                │             │      │
                │             │      ▼
                │             │   ┌────────┐
                │             └───│resuming│
                │                 └────────┘
```

## All States

| State | Description | Terminal? |
|---|---|---|
| `pending` | Job created, not yet queued for execution | No |
| `queued` | Job is in a Redis priority stream, waiting for a worker to claim it | No |
| `running` | A worker has claimed the job and is actively executing simulation steps | No |
| `completed` | Job finished successfully with results | Yes |
| `failed` | Job execution failed (transient or permanent error). May be retried or dead-lettered | No |
| `cancelling` | Cancel signal sent to the running worker. Waiting for worker to acknowledge at next step boundary | No |
| `cancelled` | Job was cancelled by the API, DAG executor, or during cleanup | Yes |
| `pausing` | Pause signal sent to the running worker. Worker will checkpoint state and yield | No |
| `paused` | Job is paused with a valid checkpoint. Worker released. Can be resumed or cancelled | No |
| `resuming` | Paused job has been re-enqueued. Waiting for a worker to claim and load checkpoint | No |
| `retry_scheduled` | Job failed with a transient error and is waiting for its backoff timer to expire | No |
| `dead_letter` | Job exhausted all retries or failed with a permanent error. Requires manual intervention | Yes |

## All Transitions

| From | To | Who | When |
|---|---|---|---|
| `pending` | `queued` | API / DAG executor | Job submitted or DAG root node enqueued |
| `pending` | `cancelled` | API / DAG executor | Job cancelled before execution or DAG cancellation |
| `queued` | `running` | Worker | Worker claims job via atomic UPDATE with WHERE guard |
| `queued` | `cancelled` | API | User cancels a queued job |
| `running` | `completed` | Worker | Simulation steps finished successfully |
| `running` | `failed` | Worker | Simulation step threw an exception |
| `running` | `cancelling` | API | User requests cancellation of a running job |
| `running` | `pausing` | API | User requests pause of a running job |
| `running` | `queued` | Worker | Circuit breaker rejects job type; push back to queue |
| `pausing` | `paused` | Worker | Worker saved checkpoint and yielded |
| `pausing` | `failed` | Reaper | Worker died during pause (lease expired) |
| `paused` | `resuming` | API | User resumes a paused job |
| `paused` | `cancelled` | API | User cancels a paused job |
| `resuming` | `running` | Worker | Worker loaded checkpoint and started executing |
| `resuming` | `failed` | Worker / Reaper | Worker crashed during resume |
| `cancelling` | `cancelled` | Worker | Worker acknowledged cancel signal |
| `cancelling` | `failed` | Reaper | Worker died during cancellation (lease expired) |
| `failed` | `retry_scheduled` | Worker | Transient error with retries remaining |
| `failed` | `dead_letter` | Worker | Permanent error or retries exhausted |
| `retry_scheduled` | `queued` | Retry scheduler | Backoff timer expired |

## Atomic Transition Protocol

Every transition uses a single SQL statement:

```sql
UPDATE jobs
SET status = $new_status, updated_at = NOW(), ...
WHERE id = $job_id AND status IN ($expected_statuses)
RETURNING *;
```

- If the UPDATE matches 1 row → transition succeeded, return the updated job.
- If the UPDATE matches 0 rows → someone else already changed the status (race lost), return `None`.
- Every successful transition writes a `job_events` row recording old_status, new_status, triggered_by, and timestamp.

This pattern eliminates read-modify-write races entirely. Two workers trying to claim the same job will both execute the UPDATE, but only one will find `status='queued'` and match the WHERE clause.

## Correctness Argument

**No job can be lost:** Every non-terminal state has at least one outgoing transition. The reaper catches jobs stuck in transient states (running/pausing/cancelling with expired leases) and moves them forward. The retry scheduler catches jobs in `retry_scheduled` whose timer expired. Every code path eventually leads to a terminal state.

**No job can be duplicated:** The WHERE guard on `status` ensures exactly one UPDATE succeeds when multiple workers race to claim. Idempotency keys prevent duplicate submissions. XACK in Redis after claim prevents redelivery.

**No job can be stuck:** Terminal states have no outgoing transitions, so they're final. Non-terminal states are monitored by the reaper (lease expiry) and retry scheduler (backoff expiry). Even if both crash, restarting them picks up exactly where they left off (stateless, idempotent).
