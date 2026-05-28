"""Eval stage: recall@5 self-test against SBERT text vectors → screens_eval."""
from __future__ import annotations

import hashlib
import logging
import uuid

import psycopg
from pgvector.psycopg import register_vector

from tasks.config import POSTGRES_DSN, SBERT_MODEL_VERSION, record_task_duration, record_metric

log = logging.getLogger(__name__)

_NEAREST_SQL = """
SELECT screen_id FROM screens_embeddings
WHERE embedding_kind = 'text'
  AND screen_id IN (SELECT screen_id FROM screens_metadata WHERE run_id = %s)
ORDER BY vector <-> %s::vector
LIMIT %s
"""


def run(run_id: str) -> dict[str, float]:
    with record_task_duration(run_id, "eval"):
        result = _run(run_id)
    n_queries = int(result["n_queries"])
    record_metric(run_id, "task.eval.row_count_in", n_queries)
    record_metric(run_id, "task.eval.row_count_out", 1 if n_queries else 0)
    return result


def _run(run_id: str) -> dict[str, float]:
    rid = uuid.UUID(run_id)

    with psycopg.connect(POSTGRES_DSN) as conn:
        register_vector(conn)
        with conn.cursor() as cur:
            _ensure_eval_schema(cur)

            # Fetch text vectors for this run's screens.
            cur.execute(
                """
                SELECT screen_id, vector, source_fingerprint FROM screens_embeddings
                WHERE run_id = %s AND embedding_kind = 'text'
                ORDER BY screen_id
                """,
                (rid,),
            )
            rows = cur.fetchall()

            if not rows:
                log.warning("[run_id=%s] eval: no text vectors found", run_id)
                return {"recall_at_5": 0.0, "n_queries": 0}

            k = min(5, len(rows))
            hits = 0
            eval_fingerprint = _eval_fingerprint(rows)

            # Self-test: query each screen with its own vector; it must appear in top-k.
            for expected_id, vec, _source_fingerprint in rows:
                cur.execute(_NEAREST_SQL, (rid, vec, k))
                top_k = [r[0] for r in cur.fetchall()]
                if expected_id in top_k:
                    hits += 1

            recall = hits / len(rows)

            cur.execute(
                """
                INSERT INTO screens_eval
                    (embedding_model_version, n_queries, recall_at_5, run_id, source_fingerprint)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (embedding_model_version, source_fingerprint)
                    WHERE source_fingerprint IS NOT NULL
                DO UPDATE SET
                    n_queries   = EXCLUDED.n_queries,
                    recall_at_5 = EXCLUDED.recall_at_5,
                    run_id      = EXCLUDED.run_id,
                    created_at  = NOW()
                """,
                (SBERT_MODEL_VERSION, len(rows), recall, rid, eval_fingerprint),
            )
            conn.commit()

    log.info(
        "[run_id=%s] eval recall@%d=%.3f (self-test, n=%d)",
        run_id, k, recall, len(rows),
    )
    return {"recall_at_5": recall, "n_queries": len(rows)}


def _eval_fingerprint(rows: list[tuple]) -> str:
    material = "|".join(
        f"{screen_id}:{source_fingerprint or ''}"
        for screen_id, _vec, source_fingerprint in rows
    )
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def _ensure_eval_schema(cur) -> None:
    cur.execute(
        """
        ALTER TABLE screens_eval
            ADD COLUMN IF NOT EXISTS source_fingerprint TEXT
        """
    )
    cur.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS screens_eval_model_source_uq
            ON screens_eval (embedding_model_version, source_fingerprint)
            WHERE source_fingerprint IS NOT NULL
        """
    )
