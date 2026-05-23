"""Slack notifications — run_started, audit_failed, run_finished.

All public functions catch and log exceptions; a Slack failure must never
fail the pipeline.
"""
from __future__ import annotations

import logging
import os

import requests

log = logging.getLogger(__name__)


def run_started(run_id: str, limit: int, trigger: str) -> None:
    _post({
        "text": (
            f":rocket: *RICO pipeline started*\n"
            f"• `run_id`: `{run_id}`\n"
            f"• limit: {limit}\n"
            f"• trigger: {trigger}"
        )
    })


def audit_failed(run_id: str, duplicates: list[dict], log_url: str = "") -> None:
    lines = "\n".join(f"  • {d}" for d in duplicates[:20])
    tail = f"\n_...and {len(duplicates) - 20} more_" if len(duplicates) > 20 else ""
    _post({
        "text": (
            f":rotating_light: *AUDIT FAILED — pipeline halted*\n"
            f"• `run_id`: `{run_id}`\n"
            f"• {len(duplicates)} duplicate(s) found:\n"
            f"```{lines}{tail}```"
            + (f"\n• log: {log_url}" if log_url else "")
        )
    })


def run_finished(run_id: str, status: str, duration_s: float, summary: str) -> None:
    icon = {
        "succeeded":      ":white_check_mark:",
        "failed":         ":x:",
        "paused-by-audit": ":warning:",
    }.get(status, ":grey_question:")
    _post({
        "text": (
            f"{icon} *RICO pipeline {status}*\n"
            f"• `run_id`: `{run_id}`\n"
            + (f"• duration: {duration_s:.0f}s\n" if duration_s else "")
            + (f"• {summary}" if summary else "")
        )
    })


def _post(payload: dict) -> None:
    url = os.environ.get("SLACK_WEBHOOK_URL", "")
    if not url:
        log.debug("SLACK_WEBHOOK_URL not set — skipping Slack notification")
        return
    try:
        resp = requests.post(url, json=payload, timeout=10)
        resp.raise_for_status()
    except Exception as exc:
        log.warning("Slack notification failed (non-fatal): %s", exc)
