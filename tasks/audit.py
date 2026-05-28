"""Audit stage: duplicate-detection circuit breaker.

Checks two invariants for the current run:
  1. Every screen ingested in this run has both an image and a text embedding
     (completeness check — the uniqueness check is vacuous because screens_metadata
     has a BIGINT PRIMARY KEY that already enforces uniqueness at the DB level).
  2. No (screen_id, model_version, embedding_kind) appears more than once in
     screens_embeddings for the screens processed in this run.

The second check uses (screen_id, model_version, embedding_kind) — intentionally
omitting model_name — so that inserting a row with a different model_name alias for
the same logical embedding is caught as a duplicate.

On failure: updates pipeline_runs to 'paused-by-audit', writes to audit_results,
posts to Slack, then raises AuditFailedError so Airflow marks the task failed and
downstream eval does not run.
"""
from __future__ import annotations

import json
import logging
import uuid

import psycopg

from tasks.config import POSTGRES_DSN, record_task_duration

log = logging.getLogger(__name__)


class AuditFailedError(Exception):
    """Raised when the duplicate-detection audit finds violations."""


def run(run_id: str) -> None:
    with record_task_duration(run_id, "audit"):
        _run(run_id)


def _run(run_id: str) -> None:
    duplicates = _find_duplicates(run_id)
    passed = len(duplicates) == 0

    _write_audit_result(run_id, passed, duplicates)

    if not passed:
        _mark_paused(run_id)
        _notify_slack(run_id, duplicates)
        log.error(
            "[run_id=%s] AUDIT FAILED — %d duplicate(s):\n%s",
            run_id, len(duplicates), json.dumps(duplicates, indent=2),
        )
        raise AuditFailedError(
            f"{len(duplicates)} duplicate(s) detected. "
            f"run_id={run_id}. Details: {json.dumps(duplicates)}"
        )

    log.info("[run_id=%s] audit passed — no duplicates found", run_id)


# ── Duplicate detection ───────────────────────────────────────────────────────

def _find_duplicates(run_id: str) -> list[dict]:
    rid = uuid.UUID(run_id)
    dupes: list[dict] = []

    with psycopg.connect(POSTGRES_DSN) as conn, conn.cursor() as cur:

        # 1. Completeness check: every screen in this run must have both an
        #    image embedding and a text embedding. The uniqueness check against
        #    screens_metadata itself is vacuous because screen_id is a BIGINT PK.
        cur.execute(
            """
            SELECT sm.screen_id,
                   COUNT(CASE WHEN se.embedding_kind = 'image' THEN 1 END) AS n_image,
                   COUNT(CASE WHEN se.embedding_kind = 'text'  THEN 1 END) AS n_text
            FROM screens_metadata sm
            LEFT JOIN screens_embeddings se ON se.screen_id = sm.screen_id
            WHERE sm.run_id = %s
            GROUP BY sm.screen_id
            HAVING COUNT(CASE WHEN se.embedding_kind = 'image' THEN 1 END) = 0
                OR COUNT(CASE WHEN se.embedding_kind = 'text'  THEN 1 END) = 0
            """,
            (rid,),
        )
        for screen_id, n_img, n_txt in cur.fetchall():
            dupes.append({
                "check": "missing_embedding",
                "screen_id": screen_id,
                "n_image": n_img,
                "n_text": n_txt,
            })

        # 2. screens_embeddings: each (screen_id, model_version, embedding_kind)
        #    should appear at most once for the screens in this run.
        cur.execute(
            """
            SELECT screen_id, model_version, embedding_kind, COUNT(*)::int
            FROM screens_embeddings
            WHERE screen_id IN (
                SELECT screen_id FROM screens_metadata WHERE run_id = %s
            )
            GROUP BY screen_id, model_version, embedding_kind
            HAVING COUNT(*) > 1
            """,
            (rid,),
        )
        for screen_id, model_ver, kind, cnt in cur.fetchall():
            dupes.append({
                "check": "duplicate_embedding",
                "table": "screens_embeddings",
                "screen_id": screen_id,
                "model_version": model_ver,
                "embedding_kind": kind,
                "count": cnt,
            })

    return dupes


# ── Side effects on failure ───────────────────────────────────────────────────

def _write_audit_result(run_id: str, passed: bool, duplicates: list[dict]) -> None:
    rid = uuid.UUID(run_id)
    details = json.dumps({"duplicates": duplicates})
    with psycopg.connect(POSTGRES_DSN) as conn, conn.cursor() as cur:
        # Delete first so retries don't accumulate duplicate audit rows.
        cur.execute(
            "DELETE FROM audit_results WHERE run_id = %s AND audit_name = 'duplicate_detection'",
            (rid,),
        )
        cur.execute(
            """
            INSERT INTO audit_results (run_id, audit_name, passed, details)
            VALUES (%s, %s, %s, %s::jsonb)
            """,
            (rid, "duplicate_detection", passed, details),
        )
        conn.commit()


def _mark_paused(run_id: str) -> None:
    from tasks.config import record_metric, record_text_metric
    rid = uuid.UUID(run_id)
    with psycopg.connect(POSTGRES_DSN) as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE pipeline_runs SET status = 'paused-by-audit', ended_at = NOW() WHERE run_id = %s
            RETURNING EXTRACT(EPOCH FROM (ended_at - started_at))::float
            """,
            (rid,),
        )
        row = cur.fetchone()
        conn.commit()
    if row:
        record_metric(run_id, "run.total_duration_s", row[0] or 0.0)
        record_text_metric(run_id, "run.final_status", "paused-by-audit")


def _notify_slack(run_id: str, duplicates: list[dict]) -> None:
    try:
        import tasks.notify as t_notify
        t_notify.audit_failed(run_id, duplicates)
    except Exception as exc:
        log.warning("[run_id=%s] Slack audit notification failed (non-fatal): %s", run_id, exc)
