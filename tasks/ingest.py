"""Ingest stage: stream RICO screens from HuggingFace → MinIO + screens_metadata."""
from __future__ import annotations

import hashlib
import itertools
import logging
import os
import uuid
from io import BytesIO
from pathlib import Path

import psycopg
from datasets import load_dataset

from tasks.config import POSTGRES_DSN, MINIO_BUCKET, s3_client, record_task_duration, record_metric

log = logging.getLogger(__name__)

_CHOSEN_SCREENS_PATH = os.environ.get("CHOSEN_SCREENS_PATH", "/opt/airflow/chosen_screens.txt")

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
    record_metric(run_id, "task.ingest.row_count_in", limit)
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
    chosen_ids = _chosen_screen_ids()[:limit]
    if not chosen_ids:
        return list(itertools.islice(ds, limit))

    wanted_ids = set(chosen_ids)
    chosen_rows: dict[int, dict] = {}
    fallback_rows: list[dict] = []

    for row in ds:
        screen_id = int(row["screenId"])
        if screen_id in wanted_ids:
            chosen_rows.setdefault(screen_id, row)
        elif len(fallback_rows) < limit:
            fallback_rows.append(row)

        if len(chosen_rows) == len(chosen_ids) and len(chosen_rows) + len(fallback_rows) >= limit:
            break

    rows = [chosen_rows[sid] for sid in chosen_ids if sid in chosen_rows]
    row_ids = {int(row["screenId"]) for row in rows}
    for row in fallback_rows:
        if len(rows) >= limit:
            break
        screen_id = int(row["screenId"])
        if screen_id not in row_ids:
            rows.append(row)
            row_ids.add(screen_id)

    return rows[:limit]


def _chosen_screen_ids() -> list[int]:
    path = Path(_CHOSEN_SCREENS_PATH)
    if not path.exists():
        log.warning("chosen screens file not found at %s; using first streamed rows", path)
        return []

    ids: list[int] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        try:
            ids.append(int(stripped.split()[0]))
        except ValueError:
            log.warning("ignoring invalid chosen screen line: %r", line)
    return ids


def _to_png_bytes(image) -> bytes:
    from PIL import Image
    buf = BytesIO()
    image.convert("RGB").save(buf, format="PNG")
    return buf.getvalue()
