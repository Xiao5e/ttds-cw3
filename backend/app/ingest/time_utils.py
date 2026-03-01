# backend/app/ingest/time_utils.py
from __future__ import annotations
from datetime import datetime, timezone

def now_iso() -> str:
    """
    Return the current time in UTC as an ISO 8601 string.

    Format: YYYY-MM-DDTHH:MM:SS.ssssssZ

    Example: "2026-02-27T14:35:21.123456Z"
    """
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")