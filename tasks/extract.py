"""Extract stage: Ollama LLM → screens_metadata + screens_review_queue."""
from __future__ import annotations

import hashlib
import json
import logging
import uuid

import psycopg
import requests

from tasks.config import POSTGRES_DSN, OLLAMA_URL, OLLAMA_MODEL, PROMPT_VERSION, record_task_duration, record_metric

log = logging.getLogger(__name__)

_PROMPT_V1 = """\
You are a UI structure extractor for Android app screenshots.

Given the visible text from one screen's view hierarchy, return a single
JSON object with these fields:

- "title": a short string naming the screen (e.g. "Login", "Settings",
  "Search results"). Empty string if unclear.
- "elements": a list of {"type": string, "text": string} objects, one
  per salient interactive or informational element you can identify.
- "confidence": a number in [0.0, 1.0] expressing how confident you are
  in the extraction.

Visible text:
{hierarchy_text}

Respond with valid JSON only — no commentary, no Markdown fences.
"""

_UPDATE_SQL = """
UPDATE screens_metadata
SET extraction_payload = %s::jsonb,
    prompt_version     = %s,
    confidence         = %s,
    updated_at         = NOW()
WHERE screen_id = %s
"""

_INSERT_REVIEW_SQL = """
INSERT INTO screens_review_queue (screen_id, reason, raw_output, run_id, source_fingerprint)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (screen_id, source_fingerprint, reason)
    WHERE source_fingerprint IS NOT NULL
DO UPDATE SET
    raw_output = EXCLUDED.raw_output,
    run_id     = EXCLUDED.run_id,
    created_at = NOW()
"""


def run(run_id: str, text_reps: dict) -> dict[str, int]:
    record_metric(run_id, "task.extract.row_count_in", len(text_reps))
    with record_task_duration(run_id, "extract"):
        result = _run(run_id, text_reps)
    record_metric(run_id, "task.extract.row_count_out", result["extracted"])
    record_metric(run_id, "task.extract.row_count_queued", result["queued"])
    return result


def _run(run_id: str, text_reps: dict) -> dict[str, int]:
    extracted, queued = 0, 0

    with psycopg.connect(POSTGRES_DSN) as conn, conn.cursor() as cur:
        _ensure_review_queue_schema(cur)
        for sid_str, text in text_reps.items():
            screen_id   = int(sid_str)
            fingerprint = hashlib.sha256(text.encode()).hexdigest()
            raw_output  = None
            try:
                prompt = _PROMPT_V1.replace("{hierarchy_text}", text)
                resp = requests.post(
                    f"{OLLAMA_URL}/api/generate",
                    json={"model": OLLAMA_MODEL, "prompt": prompt, "stream": False},
                    timeout=120,
                )
                resp.raise_for_status()
                raw_output = resp.json()["response"]   # captured before json.loads
                payload = json.loads(raw_output)
                confidence = float(payload.get("confidence", 0.0))
                body = {k: v for k, v in payload.items() if k != "confidence"}
                cur.execute(_UPDATE_SQL, (json.dumps(body), PROMPT_VERSION, confidence, screen_id))
                extracted += 1
                log.info(
                    "[run_id=%s] extracted screen %d confidence=%.2f",
                    run_id, screen_id, confidence,
                )
            except Exception as exc:
                log.warning(
                    "[run_id=%s] screen %d extraction failed: %s", run_id, screen_id, exc,
                )
                cur.execute(_INSERT_REVIEW_SQL, (screen_id, str(exc), raw_output, uuid.UUID(run_id), fingerprint))
                queued += 1
        conn.commit()

    log.info("[run_id=%s] extract done extracted=%d queued=%d", run_id, extracted, queued)
    return {"extracted": extracted, "queued": queued}


def _ensure_review_queue_schema(cur) -> None:
    cur.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS screens_review_queue_source_reason_uq
            ON screens_review_queue (screen_id, source_fingerprint, reason)
            WHERE source_fingerprint IS NOT NULL
        """
    )

