"""Parse stage: fetch view-hierarchy JSON from MinIO → flat text representations."""
from __future__ import annotations

import json
import logging
import uuid

import psycopg

from tasks.config import POSTGRES_DSN, MINIO_BUCKET, s3_client, record_task_duration, record_metric

log = logging.getLogger(__name__)


def run(run_id: str) -> dict[str, str]:
    """Return {str(screen_id): text_representation} for every screen in this run."""
    with record_task_duration(run_id, "parse"):
        result = _run(run_id)
    record_metric(run_id, "task.parse.row_count_in", len(result))
    record_metric(run_id, "task.parse.row_count_out", len(result))
    return result


def _run(run_id: str) -> dict[str, str]:
    s3 = s3_client()

    with psycopg.connect(POSTGRES_DSN) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT screen_id, hierarchy_json_path FROM screens_metadata WHERE run_id = %s",
            (uuid.UUID(run_id),),
        )
        screens = cur.fetchall()

    text_reps: dict[str, str] = {}
    for screen_id, json_path in screens:
        raw = s3.get_object(Bucket=MINIO_BUCKET, Key=json_path)["Body"].read().decode("utf-8")
        elements = parse_hierarchy(raw)
        rep = text_representation(elements)
        text_reps[str(screen_id)] = rep
        log.info("[run_id=%s] parsed screen %d text_len=%d", run_id, screen_id, len(rep))

    log.info("[run_id=%s] parse done screens=%d", run_id, len(text_reps))
    return text_reps


def parse_hierarchy(raw_json: str) -> list[tuple[str, str, tuple[int, int, int, int]]]:
    """Iterative DFS over RICO view hierarchy — returns (element_type, text, bounds)."""
    tree = json.loads(raw_json)
    root = tree.get("activity", {}).get("root", tree) if isinstance(tree, dict) else tree

    elements: list[tuple[str, str, tuple[int, int, int, int]]] = []
    stack = [root]
    while stack:
        node = stack.pop()
        if not isinstance(node, dict):
            continue
        text = (node.get("text") or "").strip()
        cls  = (node.get("class") or "").strip()
        if text or cls:
            element_type = cls.rsplit(".", 1)[-1] if cls else ""
            raw_bounds = node.get("bounds") or [0, 0, 0, 0]
            bounds = tuple(int(b) for b in raw_bounds) if len(raw_bounds) == 4 else (0, 0, 0, 0)
            elements.append((element_type, text, bounds))
        children = node.get("children")
        if isinstance(children, list):
            stack.extend(reversed(children))
    return elements


def text_representation(elements: list[tuple[str, str, tuple[int, int, int, int]]]) -> str:
    """Sort elements in reading order (top-to-bottom, left-to-right) and join their texts."""
    with_text = [e for e in elements if e[1]]
    in_order  = sorted(with_text, key=lambda e: (e[2][1], e[2][0]))
    return " ".join(text for _, text, _ in in_order)
