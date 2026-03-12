"""Initial schema — all enums, tables, indexes, and triggers.

Revision ID: 001
Revises: None
Create Date: 2026-03-12
"""
from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic
revision: str = "001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create all enums, tables, indexes, triggers, and functions."""

    # ── Enum types ──────────────────────────────────────────────
    op.execute("""
        CREATE TYPE job_status AS ENUM (
            'pending',
            'queued',
            'running',
            'completed',
            'failed',
            'cancelling',
            'cancelled',
            'pausing',
            'paused',
            'resuming',
            'retry_scheduled',
            'dead_letter'
        );
    """)

    op.execute("""
        CREATE TYPE job_priority AS ENUM (
            'critical',
            'high',
            'normal',
            'low'
        );
    """)

    op.execute("""
        CREATE TYPE error_type AS ENUM (
            'transient',
            'permanent'
        );
    """)

    op.execute("""
        CREATE TYPE dag_status AS ENUM (
            'pending',
            'running',
            'completed',
            'failed',
            'cancelled',
            'partially_failed'
        );
    """)

    op.execute("""
        CREATE TYPE dag_failure_policy AS ENUM (
            'fail_fast',
            'skip_downstream',
            'retry_node'
        );
    """)

    # ── Trigger function (shared by jobs and dag_graphs) ────────
    op.execute("""
        CREATE OR REPLACE FUNCTION update_updated_at()
        RETURNS TRIGGER AS $$
        BEGIN
            NEW.updated_at = NOW();
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    """)

    # ── dag_graphs (must come before jobs due to FK reference) ──
    op.execute("""
        CREATE TABLE dag_graphs (
            id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            name                    VARCHAR(512) NOT NULL,
            description             TEXT,
            status                  dag_status NOT NULL DEFAULT 'pending',
            created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            completed_at            TIMESTAMPTZ,
            natural_language_input  TEXT,
            raw_llm_output          JSONB,
            failure_policy          dag_failure_policy NOT NULL DEFAULT 'skip_downstream',
            metadata                JSONB NOT NULL DEFAULT '{}'
        );
    """)

    op.execute("""
        CREATE TRIGGER dag_graphs_updated_at
            BEFORE UPDATE ON dag_graphs
            FOR EACH ROW EXECUTE FUNCTION update_updated_at();
    """)

    # ── jobs ────────────────────────────────────────────────────
    op.execute("""
        CREATE TABLE jobs (
            id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            type                VARCHAR(255) NOT NULL,
            status              job_status NOT NULL DEFAULT 'pending',
            priority            job_priority NOT NULL DEFAULT 'normal',
            params              JSONB NOT NULL DEFAULT '{}',
            result              JSONB,
            created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            queued_at           TIMESTAMPTZ,
            started_at          TIMESTAMPTZ,
            completed_at        TIMESTAMPTZ,
            worker_id           VARCHAR(255),
            lease_expires_at    TIMESTAMPTZ,
            retry_count         INTEGER NOT NULL DEFAULT 0,
            max_retries         INTEGER NOT NULL DEFAULT 3,
            next_retry_at       TIMESTAMPTZ,
            backoff_seconds     FLOAT NOT NULL DEFAULT 5.0,
            backoff_multiplier  FLOAT NOT NULL DEFAULT 2.0,
            max_backoff_seconds FLOAT NOT NULL DEFAULT 300.0,
            idempotency_key     VARCHAR(255) UNIQUE,
            error_message       TEXT,
            error_type          error_type,
            progress_percent    FLOAT DEFAULT 0.0,
            progress_message    VARCHAR(1024),
            dag_id              UUID REFERENCES dag_graphs(id) ON DELETE SET NULL,
            dag_node_id         VARCHAR(255),
            metadata            JSONB NOT NULL DEFAULT '{}'
        );
    """)

    # Jobs indexes
    op.execute("CREATE INDEX idx_jobs_status ON jobs(status);")
    op.execute("CREATE INDEX idx_jobs_priority_status ON jobs(priority, status);")
    op.execute(
        "CREATE INDEX idx_jobs_status_lease ON jobs(status, lease_expires_at) "
        "WHERE status = 'running';"
    )
    op.execute(
        "CREATE INDEX idx_jobs_retry_scheduled ON jobs(next_retry_at) "
        "WHERE status = 'retry_scheduled';"
    )
    op.execute("CREATE INDEX idx_jobs_dag_id ON jobs(dag_id) WHERE dag_id IS NOT NULL;")
    op.execute(
        "CREATE INDEX idx_jobs_idempotency_key ON jobs(idempotency_key) "
        "WHERE idempotency_key IS NOT NULL;"
    )
    op.execute("CREATE INDEX idx_jobs_worker_id ON jobs(worker_id) WHERE worker_id IS NOT NULL;")
    op.execute("CREATE INDEX idx_jobs_created_at ON jobs(created_at);")

    # Jobs updated_at trigger
    op.execute("""
        CREATE TRIGGER jobs_updated_at
            BEFORE UPDATE ON jobs
            FOR EACH ROW EXECUTE FUNCTION update_updated_at();
    """)

    # ── checkpoints ─────────────────────────────────────────────
    # (created before job_attempts because job_attempts has FK to checkpoints)
    op.execute("""
        CREATE TABLE checkpoints (
            id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            job_id              UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
            attempt_id          UUID,
            step_number         INTEGER NOT NULL,
            data                JSONB NOT NULL,
            created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            is_valid            BOOLEAN NOT NULL DEFAULT TRUE
        );
    """)

    op.execute("CREATE INDEX idx_checkpoints_job_id ON checkpoints(job_id);")
    op.execute(
        "CREATE INDEX idx_checkpoints_job_valid ON checkpoints(job_id, is_valid, step_number DESC);"
    )

    # ── job_attempts ────────────────────────────────────────────
    op.execute("""
        CREATE TABLE job_attempts (
            id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            job_id              UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
            worker_id           VARCHAR(255) NOT NULL,
            attempt_number      INTEGER NOT NULL,
            started_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            finished_at         TIMESTAMPTZ,
            status              VARCHAR(50) NOT NULL DEFAULT 'running',
            error_message       TEXT,
            checkpoint_id       UUID REFERENCES checkpoints(id) ON DELETE SET NULL,
            UNIQUE(job_id, attempt_number)
        );
    """)

    op.execute("CREATE INDEX idx_attempts_job_id ON job_attempts(job_id);")

    # Add the FK from checkpoints.attempt_id to job_attempts now that both tables exist
    op.execute("""
        ALTER TABLE checkpoints
        ADD CONSTRAINT fk_checkpoints_attempt_id
        FOREIGN KEY (attempt_id) REFERENCES job_attempts(id) ON DELETE SET NULL;
    """)

    # ── job_events ──────────────────────────────────────────────
    op.execute("""
        CREATE TABLE job_events (
            id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            job_id              UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
            event_type          VARCHAR(100) NOT NULL,
            old_status          job_status,
            new_status          job_status,
            triggered_by        VARCHAR(100) NOT NULL,
            timestamp           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            metadata            JSONB NOT NULL DEFAULT '{}'
        );
    """)

    op.execute("CREATE INDEX idx_events_job_id ON job_events(job_id);")
    op.execute("CREATE INDEX idx_events_timestamp ON job_events(timestamp);")

    # ── dag_edges ───────────────────────────────────────────────
    op.execute("""
        CREATE TABLE dag_edges (
            id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            dag_id              UUID NOT NULL REFERENCES dag_graphs(id) ON DELETE CASCADE,
            parent_node_id      VARCHAR(255) NOT NULL,
            child_node_id       VARCHAR(255) NOT NULL,
            UNIQUE(dag_id, parent_node_id, child_node_id)
        );
    """)

    op.execute("CREATE INDEX idx_dag_edges_dag_id ON dag_edges(dag_id);")
    op.execute("CREATE INDEX idx_dag_edges_parent ON dag_edges(dag_id, parent_node_id);")
    op.execute("CREATE INDEX idx_dag_edges_child ON dag_edges(dag_id, child_node_id);")

    # ── worker_registry ─────────────────────────────────────────
    op.execute("""
        CREATE TABLE worker_registry (
            id                  VARCHAR(255) PRIMARY KEY,
            hostname            VARCHAR(512) NOT NULL,
            pid                 INTEGER,
            last_heartbeat      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            status              VARCHAR(50) NOT NULL DEFAULT 'idle',
            started_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            current_job_id      UUID REFERENCES jobs(id) ON DELETE SET NULL,
            metadata            JSONB NOT NULL DEFAULT '{}'
        );
    """)

    op.execute("CREATE INDEX idx_worker_heartbeat ON worker_registry(last_heartbeat);")


def downgrade() -> None:
    """Drop all tables, triggers, functions, and enums in reverse order."""

    op.execute("DROP TABLE IF EXISTS worker_registry CASCADE;")
    op.execute("DROP TABLE IF EXISTS dag_edges CASCADE;")
    op.execute("DROP TABLE IF EXISTS job_events CASCADE;")
    op.execute("DROP TABLE IF EXISTS job_attempts CASCADE;")
    op.execute("DROP TABLE IF EXISTS checkpoints CASCADE;")
    op.execute("DROP TABLE IF EXISTS jobs CASCADE;")
    op.execute("DROP TABLE IF EXISTS dag_graphs CASCADE;")

    op.execute("DROP FUNCTION IF EXISTS update_updated_at() CASCADE;")

    op.execute("DROP TYPE IF EXISTS dag_failure_policy;")
    op.execute("DROP TYPE IF EXISTS dag_status;")
    op.execute("DROP TYPE IF EXISTS error_type;")
    op.execute("DROP TYPE IF EXISTS job_priority;")
    op.execute("DROP TYPE IF EXISTS job_status;")
