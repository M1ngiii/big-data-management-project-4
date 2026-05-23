"""Embed-text stage: SBERT all-MiniLM-L6-v2 text embeddings → screens_embeddings."""
from __future__ import annotations

import hashlib
import logging
import uuid

import psycopg
from pgvector.psycopg import register_vector

from tasks.config import POSTGRES_DSN, SBERT_MODEL_VERSION, record_task_duration, record_metric

log = logging.getLogger(__name__)

_UPSERT_SQL = """
INSERT INTO screens_embeddings
    (screen_id, model_name, model_version, embedding_kind, vector, run_id, source_fingerprint)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (screen_id, model_name, model_version, embedding_kind) DO UPDATE SET
    vector             = EXCLUDED.vector,
    run_id             = EXCLUDED.run_id,
    source_fingerprint = EXCLUDED.source_fingerprint
"""


def run(run_id: str, text_reps: dict) -> dict[str, int]:
    with record_task_duration(run_id, "embed_text"):
        result = _run(run_id, text_reps)
    record_metric(run_id, "task.embed_text.row_count_out", result["embedded"])
    return result


def _run(run_id: str, text_reps: dict) -> dict[str, int]:
    from sentence_transformers import SentenceTransformer  # heavy import — keep inside function

    if not text_reps:
        log.warning("[run_id=%s] embed_text: no text representations provided", run_id)
        return {"embedded": 0}

    model = SentenceTransformer(SBERT_MODEL_VERSION)
    log.info("[run_id=%s] SBERT loaded %s", run_id, SBERT_MODEL_VERSION)

    # text_reps keys are str(screen_id) — XCom JSON serialises int keys to strings
    screen_ids = list(text_reps.keys())
    texts      = [text_reps[k] for k in screen_ids]

    vecs = model.encode(texts, normalize_embeddings=True).astype("float32")

    with psycopg.connect(POSTGRES_DSN) as conn:
        register_vector(conn)
        with conn.cursor() as cur:
            for sid_str, text, vec in zip(screen_ids, texts, vecs):
                fingerprint = hashlib.sha256(text.encode()).hexdigest()
                cur.execute(_UPSERT_SQL, (
                    int(sid_str), "sentence-transformers", SBERT_MODEL_VERSION, "text",
                    vec, uuid.UUID(run_id), fingerprint,
                ))
        conn.commit()

    log.info("[run_id=%s] embed_text done embedded=%d", run_id, len(screen_ids))
    return {"embedded": len(screen_ids)}
