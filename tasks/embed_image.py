"""Embed-image stage: CLIP ViT-B/32 image embeddings → screens_embeddings."""
from __future__ import annotations

import hashlib
import logging
import uuid
from io import BytesIO

import numpy as np
import psycopg
import torch
from pgvector.psycopg import register_vector
from PIL import Image

from tasks.config import (
    POSTGRES_DSN, MINIO_BUCKET, s3_client,
    CLIP_ARCH, CLIP_PRETRAINED, CLIP_MODEL_VERSION, record_task_duration, record_metric,
)

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


def run(run_id: str) -> dict[str, int]:
    with record_task_duration(run_id, "embed_image"):
        result = _run(run_id)
    record_metric(run_id, "task.embed_image.row_count_in", result["input"])
    record_metric(run_id, "task.embed_image.row_count_out", result["embedded"])
    return result


def _run(run_id: str) -> dict[str, int]:
    import open_clip  # heavy import — keep inside function so DAG parsing stays fast

    s3 = s3_client()

    with psycopg.connect(POSTGRES_DSN) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT screen_id, png_path FROM screens_metadata WHERE run_id = %s ORDER BY screen_id",
            (uuid.UUID(run_id),),
        )
        screens = cur.fetchall()

    if not screens:
        log.warning("[run_id=%s] embed_image: no screens found for this run", run_id)
        return {"input": 0, "embedded": 0}

    model, _, preprocess = open_clip.create_model_and_transforms(CLIP_ARCH, pretrained=CLIP_PRETRAINED)
    model.eval()
    log.info("[run_id=%s] CLIP loaded arch=%s pretrained=%s", run_id, CLIP_ARCH, CLIP_PRETRAINED)

    screen_ids, raw_bytes, tensors = [], [], []
    for screen_id, png_path in screens:
        raw = s3.get_object(Bucket=MINIO_BUCKET, Key=png_path)["Body"].read()
        img = Image.open(BytesIO(raw)).convert("RGB")
        screen_ids.append(screen_id)
        raw_bytes.append(raw)
        tensors.append(preprocess(img))

    images_tensor = torch.stack(tensors)
    with torch.no_grad():
        vecs = model.encode_image(images_tensor)
        vecs = vecs / vecs.norm(dim=-1, keepdim=True)
    vecs_np = vecs.cpu().numpy().astype("float32")

    with psycopg.connect(POSTGRES_DSN) as conn:
        register_vector(conn)
        with conn.cursor() as cur:
            for screen_id, raw, vec in zip(screen_ids, raw_bytes, vecs_np):
                fingerprint = hashlib.sha256(raw).hexdigest()
                cur.execute(_UPSERT_SQL, (
                    screen_id, "open-clip", CLIP_MODEL_VERSION, "image",
                    vec, uuid.UUID(run_id), fingerprint,
                ))
        conn.commit()

    log.info("[run_id=%s] embed_image done embedded=%d", run_id, len(screen_ids))
    return {"input": len(screens), "embedded": len(screen_ids)}
