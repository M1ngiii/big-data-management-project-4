"""Eval stage: recall@5 self-test against SBERT text vectors → screens_eval."""
from __future__ import annotations

import logging
import uuid

import psycopg
from pgvector.psycopg import register_vector

from tasks.config import POSTGRES_DSN, SBERT_MODEL_VERSION, record_task_duration

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
        return _run(run_id)


def _run(run_id: str) -> dict[str, float]:
    rid = uuid.UUID(run_id)

    with psycopg.connect(POSTGRES_DSN) as conn:
        register_vector(conn)
        with conn.cursor() as cur:

            # Fetch text vectors for this run's screens.
            cur.execute(
                """
                SELECT screen_id, vector FROM screens_embeddings
                WHERE run_id = %s AND embedding_kind = 'text'
                ORDER BY screen_id
                """,
                (rid,),
            )
            rows = cur.fetchall()

            if not rows:
                log.warning("[run_id=%s] eval: no text vectors found", run_id)
                return {"recall_at_5": 0.0}

            k = min(5, len(rows))
            hits = 0

            # Self-test: query each screen with its own vector; it must appear in top-k.
            for expected_id, vec in rows:
                cur.execute(_NEAREST_SQL, (rid, vec, k))
                top_k = [r[0] for r in cur.fetchall()]
                if expected_id in top_k:
                    hits += 1

            recall = hits / len(rows)

            # Idempotent: delete any prior eval rows for this run before inserting.
            cur.execute("DELETE FROM screens_eval WHERE run_id = %s", (rid,))
            cur.execute(
                """
                INSERT INTO screens_eval (embedding_model_version, n_queries, recall_at_5, run_id)
                VALUES (%s, %s, %s, %s)
                """,
                (SBERT_MODEL_VERSION, len(rows), recall, rid),
            )
            conn.commit()

    log.info(
        "[run_id=%s] eval recall@%d=%.3f (self-test, n=%d)",
        run_id, k, recall, len(rows),
    )
    return {"recall_at_5": recall}
