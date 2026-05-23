"""Ingest stage: stream RICO screens from HuggingFace → MinIO + screens_metadata."""
from __future__ import annotations

import hashlib
import itertools
import logging
import uuid
from io import BytesIO

import psycopg
from datasets import load_dataset

from tasks.config import POSTGRES_DSN, MINIO_BUCKET, s3_client, record_task_duration, record_metric

log = logging.getLogger(__name__)

_UPSERT_SQL = """
INSERT INTO screens_metadata
    (screen_id, app_package, category, png_path, hierarchy_json_path,
     run_id, source_fingerprint)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (screen_id) DO UPDATE SET
    run_id             = EXCLUDED.run_id,
    source_fingerprint = EXCLUDED.source_fingerprint,
    app_package        = EXCLUDED.app_package,
    category           = EXCLUDED.category,
    updated_at         = NOW()
"""


def run(run_id: str, limit: int) -> dict[str, int]:
    with record_task_duration(run_id, "ingest"):
        result = _run(run_id, limit)
    record_metric(run_id, "task.ingest.row_count_out", result["ingested"])
    return result


def _run(run_id: str, limit: int) -> dict[str, int]:
    s3 = s3_client()
    screens = _stream_screens(limit)

    ingested = 0
    with psycopg.connect(POSTGRES_DSN) as conn, conn.cursor() as cur:
        for row in screens:
            sid = int(row["screenId"])
            png_key  = f"screens/{sid}.png"
            hier_key = f"screens/{sid}.json"

            png_bytes = _to_png_bytes(row["image"])
            hier_bytes = row["view_hierarchy"].encode("utf-8")
            fingerprint = hashlib.sha256(png_bytes).hexdigest()

            s3.put_object(Bucket=MINIO_BUCKET, Key=png_key,  Body=png_bytes)
            s3.put_object(Bucket=MINIO_BUCKET, Key=hier_key, Body=hier_bytes)

            cur.execute(
                _UPSERT_SQL,
                (
                    sid, row["app_package_name"], row["category"],
                    png_key, hier_key, uuid.UUID(run_id), fingerprint,
                ),
            )
            ingested += 1
            log.info(
                "[run_id=%s] ingested screen %d category=%r png=%dB",
                run_id, sid, row["category"], len(png_bytes),
            )
        conn.commit()

    log.info("[run_id=%s] ingest done ingested=%d", run_id, ingested)
    return {"ingested": ingested}



def _stream_screens(limit: int) -> list[dict]:
    ds = load_dataset(
        "rootsautomation/RICO-Screen2Words",
        split="train",
        streaming=True,
    )
    return list(itertools.islice(ds, limit))


def _to_png_bytes(image) -> bytes:
    from PIL import Image
    buf = BytesIO()
    image.convert("RGB").save(buf, format="PNG")
    return buf.getvalue()
