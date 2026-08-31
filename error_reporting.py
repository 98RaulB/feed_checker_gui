# error_reporting.py — best-effort Slack alerts for unexpected failures.
#
# Enabled by ONE secret: set SLACK_WEBHOOK_URL (a Slack incoming-webhook URL)
# in the deployment's environment — on Streamlit Community Cloud, app
# Settings -> Secrets, which are exposed as env vars:
#     SLACK_WEBHOOK_URL = "https://hooks.slack.com/services/T000/B000/xxxx"
# Without it, report_error() is a silent no-op, so local runs and tests never
# try to post anywhere.
#
# Design constraints:
#   * NEVER let reporting break the app — every failure path returns False.
#   * Don't spam: identical errors are muted for DEDUPE_SECONDS per process
#     (Streamlit reruns the script on every interaction, so the same crash
#     would otherwise fire once per rerun).
#   * streamlit-free, like feed_download — usable from any layer.
from __future__ import annotations

import os
import threading
import time
import traceback

import requests

WEBHOOK_ENV = "SLACK_WEBHOOK_URL"
DEDUPE_SECONDS = 600
_TRACEBACK_TAIL = 1500

_lock = threading.Lock()
_last_sent: dict[tuple, float] = {}


def _should_send(key: tuple) -> bool:
    now = time.monotonic()
    with _lock:
        last = _last_sent.get(key)
        if last is not None and now - last < DEDUPE_SECONDS:
            return False
        _last_sent[key] = now
        return True


def report_error(
    context: str,
    exc: BaseException | None = None,
    *,
    source: str = "",
    shop: bool = False,
) -> bool:
    """Post one Slack message about an unexpected failure.

    Returns True only when a message was actually delivered — callers use
    this to decide whether to tell the user "our team has been notified"."""
    webhook = os.getenv(WEBHOOK_ENV, "").strip()
    if not webhook:
        return False

    exc_name = type(exc).__name__ if exc is not None else "-"
    key = (context, exc_name, str(exc)[:200] if exc is not None else "")
    if not _should_send(key):
        return False

    mode = "shop" if shop else "internal"
    lines = [f":rotating_light: *Feed Checker ({mode})* — {context}"]
    if source:
        lines.append(f"Source: `{source[:300]}`")
    if exc is not None:
        tb = "".join(
            traceback.format_exception(type(exc), exc, exc.__traceback__)
        )[-_TRACEBACK_TAIL:]
        lines.append(f"```{tb}```")
    try:
        response = requests.post(
            webhook, json={"text": "\n".join(lines)}, timeout=5
        )
        return response.status_code < 300
    except Exception:
        return False
