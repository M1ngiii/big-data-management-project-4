"""Load stage: verify row counts, collect + persist pipeline metrics, log summary."""
from __future__ import annotations

import logging
import uuid

import psycopg

from tasks.config import POSTGRES_DSN, record_task_duration, record_metric

log = logging.getLogger(__name__)


def run(run_id: str) -> dict:
    with record_task_duration(run_id, "load"):
        metrics = _collect_metrics(run_id)
        _persist_metrics(run_id, metrics)
        record_metric(run_id, "task.load.row_count_in", _load_row_count_in(metrics))
        record_metric(run_id, "task.load.row_count_out", len(metrics))
        _log_summary(run_id, metrics)
        log.info("[run_id=%s] load done", run_id)
        return metrics


def summary_for_run(run_id: str) -> str:
    """Return the one-line metrics summary used in end-of-run notifications."""
    try:
        rid = uuid.UUID(run_id)
        with psycopg.connect(POSTGRES_DSN) as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT metric_name, metric_value
                FROM pipeline_metrics
                WHERE run_id = %s AND metric_value IS NOT NULL
                """,
                (rid,),
            )
            metrics = {name: value for name, value in cur.fetchall()}
        return _format_summary(metrics)
    except Exception as exc:
        log.warning("[run_id=%s] could not build metrics summary: %s", run_id, exc)
        return ""


# ── Metric collection ─────────────────────────────────────────────────────────

def _collect_metrics(run_id: str) -> dict:
    m: dict[str, float] = {}
    rid = uuid.UUID(run_id)

    with psycopg.connect(POSTGRES_DSN) as conn, conn.cursor() as cur:

        # ── screens_metadata ──────────────────────────────────────────────────
        cur.execute(
            """
            SELECT
                COUNT(*)::int,
                COUNT(extraction_payload)::int,
                COUNT(CASE WHEN confidence >= 0.5 THEN 1 END)::int,
                COUNT(DISTINCT app_package)::int,
                COUNT(DISTINCT category)::int
            FROM screens_metadata WHERE run_id = %s
            """,
            (rid,),
        )
        total, has_extract, high_conf, n_pkg, n_cat = cur.fetchone()

        m["meta.row_count"]            = total
        m["meta.pct_extracted"]        = _pct(has_extract, total)
        m["meta.pct_high_confidence"]  = _pct(high_conf, total)
        m["meta.distinct_packages"]    = n_pkg
        m["meta.distinct_categories"]  = n_cat

        cur.execute(
            "SELECT COUNT(*)::int FROM screens_review_queue WHERE run_id = %s", (rid,)
        )
        queued = cur.fetchone()[0]
        m["meta.pct_review_queue"] = _pct(queued, total)

        # ── screens_embeddings (one group per model_version + embedding_kind) ─
        cur.execute(
            """
            SELECT
                model_version, embedding_kind,
                COUNT(*)::int,
                MIN(vector_dims(vector))::int,
                MAX(vector_dims(vector))::int,
                AVG(vector_dims(vector))::float,
                COUNT(CASE WHEN (vector <#> vector) = 0.0 THEN 1 END)::int
            FROM screens_embeddings
            WHERE run_id = %s
            GROUP BY model_version, embedding_kind
            """,
            (rid,),
        )
        for ver, kind, n, min_d, max_d, avg_d, zero_n in cur.fetchall():
            tag = f"emb.{kind}"
            version_tag = f"emb.{_metric_token(ver)}.{kind}"
            m[f"{tag}.row_count"]      = n
            m[f"{tag}.min_dims"]       = min_d
            m[f"{tag}.max_dims"]       = max_d
            m[f"{tag}.avg_dims"]       = avg_d
            m[f"{tag}.dims_consistent"] = 1.0 if min_d == max_d else 0.0
            m[f"{tag}.pct_zero_norm"]  = _pct(zero_n, n)
            m[f"{version_tag}.row_count"]      = n
            m[f"{version_tag}.min_dims"]       = min_d
            m[f"{version_tag}.max_dims"]       = max_d
            m[f"{version_tag}.avg_dims"]       = avg_d
            m[f"{version_tag}.dims_consistent"] = 1.0 if min_d == max_d else 0.0
            m[f"{version_tag}.pct_zero_norm"]  = _pct(zero_n, n)
            if min_d != max_d:
                log.warning(
                    "[run_id=%s] inconsistent vector dims for %s/%s: min=%d max=%d",
                    run_id, kind, ver, min_d, max_d,
                )

    return m


# ── Persistence ───────────────────────────────────────────────────────────────

def _persist_metrics(run_id: str, metrics: dict) -> None:
    rid = uuid.UUID(run_id)
    with psycopg.connect(POSTGRES_DSN) as conn, conn.cursor() as cur:
        for name, value in metrics.items():
            cur.execute(
                """
                INSERT INTO pipeline_metrics (run_id, metric_name, metric_value)
                VALUES (%s, %s, %s)
                ON CONFLICT (run_id, metric_name) DO UPDATE
                    SET metric_value = EXCLUDED.metric_value
                """,
                (rid, name, float(value)),
            )
        conn.commit()


# ── Log summary ───────────────────────────────────────────────────────────────

def _log_summary(run_id: str, m: dict) -> None:
    log.info("[run_id=%s] RUN METRICS | %s", run_id, _format_summary(m))


def _format_summary(m: dict) -> str:
    if not m:
        return ""

    image_dims_ok = bool(_metric(m, "emb.image.dims_consistent"))
    text_dims_ok = bool(_metric(m, "emb.text.dims_consistent"))
    zero_norm_pct = max(
        _metric(m, "emb.image.pct_zero_norm"),
        _metric(m, "emb.text.pct_zero_norm"),
    )

    return (
        f"screens={_int_metric(m, 'meta.row_count')} "
        f"extracted={_metric(m, 'meta.pct_extracted'):.0f}% "
        f"high_conf={_metric(m, 'meta.pct_high_confidence'):.0f}% "
        f"review_queue={_metric(m, 'meta.pct_review_queue'):.0f}% "
        f"packages={_int_metric(m, 'meta.distinct_packages')} "
        f"categories={_int_metric(m, 'meta.distinct_categories')} "
        f"image_rows={_int_metric(m, 'emb.image.row_count')} "
        f"text_rows={_int_metric(m, 'emb.text.row_count')} "
        f"dims_ok={str(image_dims_ok and text_dims_ok).lower()} "
        f"zero_norm={zero_norm_pct:.0f}%"
    )


def _metric(m: dict, name: str) -> float:
    return float(m.get(name) or 0.0)


def _int_metric(m: dict, name: str) -> int:
    return int(_metric(m, name))


def _metric_token(value: str) -> str:
    return value.replace("/", "_").replace(" ", "_")


def _load_row_count_in(m: dict) -> int:
    return (
        _int_metric(m, "meta.row_count")
        + _int_metric(m, "emb.image.row_count")
        + _int_metric(m, "emb.text.row_count")
    )


def _pct(num: int, denom: int) -> float:
    return round(num / denom * 100, 1) if denom else 0.0
