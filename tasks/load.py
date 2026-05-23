"""Load stage: verify row counts, collect + persist pipeline metrics, log summary."""
from __future__ import annotations

import logging
import uuid

import psycopg

from tasks.config import POSTGRES_DSN, record_task_duration

log = logging.getLogger(__name__)


def run(run_id: str) -> dict:
    with record_task_duration(run_id, "load"):
        metrics = _collect_metrics(run_id)
        _persist_metrics(run_id, metrics)
        _log_summary(run_id, metrics)
        log.info("[run_id=%s] load done", run_id)
        return metrics


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
                COUNT(CASE WHEN (vector <#> vector) = 0.0 THEN 1 END)::int
            FROM screens_embeddings
            WHERE run_id = %s
            GROUP BY model_version, embedding_kind
            """,
            (rid,),
        )
        for ver, kind, n, min_d, max_d, zero_n in cur.fetchall():
            tag = f"emb.{kind}"
            m[f"{tag}.row_count"]      = n
            m[f"{tag}.min_dims"]       = min_d
            m[f"{tag}.max_dims"]       = max_d
            m[f"{tag}.dims_consistent"] = 1.0 if min_d == max_d else 0.0
            m[f"{tag}.pct_zero_norm"]  = _pct(zero_n, n)
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
    log.info(
        "[run_id=%s] RUN METRICS | "
        "screens=%s  extracted=%.0f%%  high_conf=%.0f%%  review_queue=%.0f%%  "
        "img_rows=%s  txt_rows=%s  "
        "img_dims_ok=%s  txt_dims_ok=%s",
        run_id,
        int(m.get("meta.row_count", 0)),
        m.get("meta.pct_extracted", 0),
        m.get("meta.pct_high_confidence", 0),
        m.get("meta.pct_review_queue", 0),
        int(m.get("emb.image.row_count", 0)),
        int(m.get("emb.text.row_count", 0)),
        bool(m.get("emb.image.dims_consistent", 0)),
        bool(m.get("emb.text.dims_consistent", 0)),
    )


def _pct(num: int, denom: int) -> float:
    return round(num / denom * 100, 1) if denom else 0.0
