"""RICO pipeline DAG — orchestration only. Business logic lives in tasks/.

Stage order:
    setup
      └─ ingest
           └─ parse
                ├─ embed_image  ─┐
                ├─ embed_text   ─┤─ load ─ audit ─ eval
                └─ extract     ─┘
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

import psycopg
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.python import get_current_context

import tasks.audit as t_audit
import tasks.embed_image as t_embed_image
import tasks.embed_text as t_embed_text
import tasks.eval as t_eval
import tasks.extract as t_extract
import tasks.ingest as t_ingest
import tasks.load as t_load
import tasks.notify as t_notify
import tasks.parse as t_parse
import tasks.tracing as t_tracing

log = logging.getLogger(__name__)

_DEFAULT_ARGS = {
    "owner": "rico",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=60),
}


# ── DAG-level callbacks ───────────────────────────────────────────────────────

def _lookup_run_id_and_duration(airflow_run_id: str) -> tuple[str | None, float]:
    """Resolve dag_run_id → (run_id, elapsed_seconds). Returns (None, 0.0) on error."""
    dsn = os.environ.get("POSTGRES_DSN", "postgresql://rico:rico@postgres:5432/rico")
    try:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT run_id, started_at FROM pipeline_runs WHERE dag_run_id = %s LIMIT 1",
                (airflow_run_id,),
            )
            row = cur.fetchone()
            if not row:
                return None, 0.0
            run_id = str(row[0])
            started_at = row[1]
            from datetime import timezone
            elapsed = (datetime.now(tz=timezone.utc) - started_at).total_seconds()
            return run_id, max(elapsed, 0.0)
    except Exception:
        log.exception("could not look up run_id for dag_run_id=%s", airflow_run_id)
        return None, 0.0


def _on_dag_success(context: dict) -> None:
    dag_run = context.get("dag_run")
    if not dag_run:
        return
    run_id, duration_s = _lookup_run_id_and_duration(dag_run.run_id)
    if run_id:
        t_tracing.finish_run(run_id, "succeeded")
        t_notify.run_finished(run_id, "succeeded", duration_s, "")


def _lookup_final_status(airflow_run_id: str) -> str:
    """Return the current status from pipeline_runs, defaulting to 'failed' on error."""
    dsn = os.environ.get("POSTGRES_DSN", "postgresql://rico:rico@postgres:5432/rico")
    try:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT status FROM pipeline_runs WHERE dag_run_id = %s LIMIT 1",
                (airflow_run_id,),
            )
            row = cur.fetchone()
            return row[0] if row else "failed"
    except Exception:
        return "failed"


def _on_dag_failure(context: dict) -> None:
    dag_run = context.get("dag_run")
    if not dag_run:
        return
    run_id, duration_s = _lookup_run_id_and_duration(dag_run.run_id)
    if run_id:
        # Don't overwrite 'paused-by-audit' — the audit task sets that itself.
        t_tracing.finish_run_if_not_terminal(run_id, "failed")
        actual_status = _lookup_final_status(dag_run.run_id)
        t_notify.run_finished(run_id, actual_status, duration_s, "")


# ── DAG definition ────────────────────────────────────────────────────────────

@dag(
    dag_id="rico_pipeline",
    description="RICO multimodal pipeline: ingest → embed → extract → audit → eval",
    start_date=datetime(2026, 5, 1),
    schedule=None,           # manual trigger; change to a cron string for scheduled runs
    catchup=False,
    max_active_runs=1,
    default_args=_DEFAULT_ARGS,
    params={
        "limit": Param(
            default=5,
            type="integer",
            minimum=1,
            description="Number of screens to process in this run",
        ),
    },
    on_success_callback=_on_dag_success,
    on_failure_callback=_on_dag_failure,
    tags=["rico", "multimodal"],
)
def rico_pipeline() -> None:

    @task
    def setup() -> str:
        """Create the pipeline_runs row; notify Slack; return run_id."""
        ctx = get_current_context()
        return t_tracing.create_run(
            dag_run_id=ctx["run_id"],
            limit=ctx["params"]["limit"],
            trigger=ctx["dag_run"].run_type,
        )

    @task
    def ingest(pipeline_run_id: str) -> dict:
        ctx = get_current_context()
        return t_ingest.run(pipeline_run_id, limit=ctx["params"]["limit"])

    @task
    def parse(pipeline_run_id: str) -> dict:
        """Returns {str(screen_id): text_representation} for every screen in this run."""
        return t_parse.run(pipeline_run_id)

    @task
    def embed_image(pipeline_run_id: str) -> dict:
        return t_embed_image.run(pipeline_run_id)

    @task
    def embed_text(pipeline_run_id: str, text_reps: dict) -> dict:
        return t_embed_text.run(pipeline_run_id, text_reps)

    @task
    def extract(pipeline_run_id: str, text_reps: dict) -> dict:
        return t_extract.run(pipeline_run_id, text_reps)

    @task
    def load(pipeline_run_id: str) -> dict:
        return t_load.run(pipeline_run_id)

    @task(retries=0)
    def audit(pipeline_run_id: str) -> None:
        """Duplicate-detection circuit breaker. Raises on violations — halts the DAG."""
        t_audit.run(pipeline_run_id)

    @task
    def eval_(pipeline_run_id: str) -> dict:
        return t_eval.run(pipeline_run_id)

    # ── Wire the pipeline ─────────────────────────────────────────────────────

    run_id      = setup()

    ingest_done = ingest(run_id)

    text_reps   = parse(run_id)
    ingest_done >> text_reps          # parse after ingest

    img_done    = embed_image(run_id)
    txt_done    = embed_text(run_id, text_reps)   # XCom dep → runs after parse
    ext_done    = extract(run_id, text_reps)      # XCom dep → runs after parse
    text_reps   >> img_done           # ordering dep — embed_image reads PNGs, not text_reps

    load_done   = load(run_id)
    [img_done, txt_done, ext_done] >> load_done

    audit_done  = audit(run_id)
    load_done   >> audit_done

    eval_(run_id) << audit_done       # eval only runs if audit passes


rico_pipeline()
