"""Pipeline run lifecycle — creates and closes pipeline_runs rows."""
from __future__ import annotations

import logging
import subprocess
import uuid

import psycopg

from tasks.config import (
    POSTGRES_DSN, CLIP_MODEL_VERSION, SBERT_MODEL_VERSION, OLLAMA_MODEL, PROMPT_VERSION,
    record_task_retries,
)

log = logging.getLogger(__name__)


def create_run(dag_run_id: str, limit: int, trigger: str) -> str:
    """Insert a pipeline_runs row with status='running'; post Slack notification; return run_id.

    Idempotent: if the setup task is retried, ON CONFLICT returns the existing run_id
    rather than creating a duplicate row.
    """
    import tasks.notify as t_notify  # late import avoids circular at parse time

    run_id = str(uuid.uuid4())
    git_sha = _git_sha()

    with psycopg.connect(POSTGRES_DSN) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipeline_runs
                (run_id, dag_run_id, status, limit_param, git_sha,
                 clip_version, sbert_version, llm_model, prompt_version)
            VALUES (%s, %s, 'running', %s, %s, %s, %s, %s, %s)
            ON CONFLICT (dag_run_id) DO UPDATE
                SET started_at = pipeline_runs.started_at  -- no-op; just return the row
            RETURNING run_id
            """,
            (
                uuid.UUID(run_id), dag_run_id, limit, git_sha,
                CLIP_MODEL_VERSION, SBERT_MODEL_VERSION, OLLAMA_MODEL, PROMPT_VERSION,
            ),
        )
        returned_run_id = str(cur.fetchone()[0])
        conn.commit()

    run_id = returned_run_id
    log.info(
        "[run_id=%s] pipeline started dag_run_id=%s limit=%d trigger=%s git_sha=%s",
        run_id, dag_run_id, limit, trigger, git_sha,
    )
    record_task_retries(run_id, "setup")
    t_notify.run_started(run_id, limit, trigger)
    return run_id


def finish_run(run_id: str, status: str) -> None:
    """Set ended_at=NOW() and status unconditionally; persist total duration metric."""
    from tasks.config import record_metric, record_text_metric
    rid = uuid.UUID(run_id)
    with psycopg.connect(POSTGRES_DSN) as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE pipeline_runs SET ended_at = NOW(), status = %s WHERE run_id = %s
            RETURNING EXTRACT(EPOCH FROM (ended_at - started_at))::float
            """,
            (status, rid),
        )
        row = cur.fetchone()
        conn.commit()
    duration_s = row[0] if row else 0.0
    record_metric(run_id, "run.total_duration_s", duration_s)
    record_text_metric(run_id, "run.final_status", status)
    log.info("[run_id=%s] pipeline finished status=%s", run_id, status)


def finish_run_if_not_terminal(run_id: str, status: str) -> None:
    """Set ended_at and status only when the run is not already in a terminal state.

    Prevents the DAG failure callback from overwriting 'paused-by-audit' that the
    audit task sets before it raises.
    """
    from tasks.config import record_metric, record_text_metric
    rid = uuid.UUID(run_id)
    with psycopg.connect(POSTGRES_DSN) as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE pipeline_runs SET ended_at = NOW(), status = %s
            WHERE run_id = %s AND status NOT IN ('paused-by-audit', 'failed')
            RETURNING EXTRACT(EPOCH FROM (ended_at - started_at))::float, status
            """,
            (status, rid),
        )
        row = cur.fetchone()
        if not row:
            cur.execute(
                """
                SELECT EXTRACT(EPOCH FROM (COALESCE(ended_at, NOW()) - started_at))::float, status
                FROM pipeline_runs WHERE run_id = %s
                """,
                (rid,),
            )
            row = cur.fetchone()
        conn.commit()
    if row:
        duration_s, final_status = row
        record_metric(run_id, "run.total_duration_s", duration_s or 0.0)
        record_text_metric(run_id, "run.final_status", final_status)


def _git_sha() -> str:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True, text=True, timeout=5,
        )
        if result.returncode == 0:
            return result.stdout.strip()
    except Exception:
        pass
    return "unknown"
