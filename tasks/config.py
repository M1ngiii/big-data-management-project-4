"""Shared constants read from environment. Import this in every task module."""
from __future__ import annotations

import time
import uuid
import contextlib
import logging
import os

log = logging.getLogger(__name__)

POSTGRES_DSN    = os.environ.get("POSTGRES_DSN",  "postgresql://rico:rico@postgres:5432/rico")
MINIO_URL       = os.environ.get("MINIO_URL",     "http://minio:9000")
MINIO_KEY       = os.environ.get("MINIO_KEY",     "minioadmin")
MINIO_SECRET    = os.environ.get("MINIO_SECRET",  "minioadmin")
MINIO_BUCKET    = os.environ.get("MINIO_BUCKET",  "rico-raw")
OLLAMA_URL      = os.environ.get("OLLAMA_URL",    "http://ollama:11434")
OLLAMA_MODEL    = os.environ.get("OLLAMA_MODEL",  "qwen2.5:3b")

CLIP_ARCH           = "ViT-B-32"
CLIP_PRETRAINED     = "laion2b_s34b_b79k"
CLIP_MODEL_VERSION  = f"open-clip-{CLIP_ARCH}-{CLIP_PRETRAINED.replace('_', '-')}"
SBERT_MODEL_VERSION = "sentence-transformers/all-MiniLM-L6-v2"
PROMPT_VERSION      = "v1"


def s3_client():
    import boto3
    return boto3.client(
        "s3",
        endpoint_url=MINIO_URL,
        aws_access_key_id=MINIO_KEY,
        aws_secret_access_key=MINIO_SECRET,
        region_name="us-east-1",
    )


def record_metric(run_id: str, metric_name: str, value: float) -> None:
    """Persist a single numeric metric to pipeline_metrics (non-fatal on error)."""
    import psycopg
    try:
        rid = uuid.UUID(run_id)
        with psycopg.connect(POSTGRES_DSN) as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pipeline_metrics (run_id, metric_name, metric_value)
                VALUES (%s, %s, %s)
                ON CONFLICT (run_id, metric_name) DO UPDATE
                    SET metric_value = EXCLUDED.metric_value
                """,
                (rid, metric_name, value),
            )
            conn.commit()
    except Exception as exc:
        log.warning("could not record metric %s: %s", metric_name, exc)


def record_text_metric(run_id: str, metric_name: str, value: str) -> None:
    """Persist a single text metric to pipeline_metrics (non-fatal on error)."""
    import psycopg
    try:
        rid = uuid.UUID(run_id)
        with psycopg.connect(POSTGRES_DSN) as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pipeline_metrics (run_id, metric_name, metric_text)
                VALUES (%s, %s, %s)
                ON CONFLICT (run_id, metric_name) DO UPDATE
                    SET metric_text = EXCLUDED.metric_text,
                        metric_value = NULL
                """,
                (rid, metric_name, value),
            )
            conn.commit()
    except Exception as exc:
        log.warning("could not record text metric %s: %s", metric_name, exc)


def record_task_retries(run_id: str, task_name: str) -> None:
    """Persist the number of completed retries for the current Airflow task."""
    try:
        from airflow.operators.python import get_current_context

        ctx = get_current_context()
        task_instance = ctx.get("ti")
        try_number = getattr(task_instance, "try_number", 1)
        retries = max(int(try_number) - 1, 0)
        record_metric(run_id, f"task.{task_name}.retries", retries)
    except Exception as exc:
        log.debug("could not record retry count for task %s: %s", task_name, exc)


@contextlib.contextmanager
def record_task_duration(run_id: str, task_name: str):
    """Context manager that measures wall-clock duration and writes it to pipeline_metrics."""
    import psycopg
    record_task_retries(run_id, task_name)
    t0 = time.perf_counter()
    try:
        yield
    finally:
        duration_s = time.perf_counter() - t0
        try:
            rid = uuid.UUID(run_id)
            with psycopg.connect(POSTGRES_DSN) as conn, conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO pipeline_metrics (run_id, metric_name, metric_value)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (run_id, metric_name) DO UPDATE
                        SET metric_value = EXCLUDED.metric_value
                    """,
                    (rid, f"task.{task_name}.duration_s", duration_s),
                )
                conn.commit()
        except Exception as exc:
            log.warning("could not record duration for task %s: %s", task_name, exc)
