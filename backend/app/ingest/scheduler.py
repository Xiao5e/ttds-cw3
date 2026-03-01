"""
RSS Scheduler

This module runs a background scheduler that periodically pulls documents from
multiple RSS sources and ingests them into the local storage/index.

Key responsibilities
--------------------
1) Load RSS source configuration from sources.json
2) For each enabled source:
   - Check whether it is "due" to run (based on per-source FeedState.next_run_iso)
   - Fetch RSS items
   - Deduplicate using ingest_state.json (seen_ids)
   - Ingest new docs into DocumentStore
   - Update search index
   - Update ingest_state.json safely (merge seen_ids + feeds + last_run_iso)
3) Append a lightweight run record into daily JSONL "history" files
4) Periodically cleanup old history files (keep last N days)

State files
-----------
- ingest_state.json:
    Stores deduplication IDs (seen_ids), the latest overall run time (last_run_iso),
    and per-source scheduling state (feeds[src_id] = FeedState).

- feed_history/feed_runs_YYYY-MM-DD.jsonl:
    Append-only run history logs (one JSON object per line).
    Used for debugging/monitoring; the scheduler itself only needs ingest_state.json.
"""
from __future__ import annotations

import os
import asyncio
import json
import time
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional, Iterable

from .state import IngestState, FeedState
from .sources.medium_rss import fetch_documents
from ..config import PROCESSED_DIR, SOURCES_DIR
from ..storage.document_store import DocumentStore
from ..storage.index_store import IndexStore
from ..indexing.indexer import update_index

from .state_io import update_state


# Scheduler timing controls
# Main loop sleep interval
DEFAULT_TICK_S = 10
# Default per-source interval (seconds)
DEFAULT_INTERVAL_S = 180
# Max backoff on errors (seconds)
MAX_BACKOFF_S = 3600  # 1 hour

# Feed run history settings (JSONL logs)
HIST_DIR = PROCESSED_DIR / "feed_history"
RETENTION_DAYS = 14


def _now_utc() -> datetime:
    """Return current UTC time as a timezone-aware datetime."""
    return datetime.now(timezone.utc)


def _hist_path_for_now(now: datetime) -> Path:
    """
    Build the history file path for a given UTC time.

    We rotate history logs by day:
        feed_history/feed_runs_YYYY-MM-DD.jsonl
    """
    # document is stored in feed_history/feed_runs_YYYY-MM-DD.jsonl, time is from _now_utc()
    day = now.strftime("%Y-%m-%d")
    return HIST_DIR / f"feed_runs_{day}.jsonl"


def _to_iso(dt: datetime) -> str:
    """Convert datetime to ISO-8601 UTC string ending with 'Z'."""
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_iso(s: Optional[str]) -> Optional[datetime]:
    """
    Parse an ISO-8601 string into datetime.

    Accepts strings ending with 'Z' and converts them into '+00:00'.
    Returns None if input is empty.
    """
    if not s:
        return None
    # simple parse for "...Z"
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    return datetime.fromisoformat(s)


def _load_sources(path: Path) -> Dict[str, Any]:
    """Load sources.json config as a Python dict."""
    return json.loads(path.read_text(encoding="utf-8"))


def _interval_for_source(cfg: Dict[str, Any], src: Dict[str, Any]) -> int:
    """
    Determine polling interval (seconds) for one source.

    Priority:
      1) src["interval_s"]
      2) cfg["default_interval_s"]
      3) DEFAULT_INTERVAL_S
    """
    # src.interval_s > cfg.default_interval_s > hard default
    if "interval_s" in src:
        return int(src["interval_s"])
    if "default_interval_s" in cfg:
        return int(cfg["default_interval_s"])
    return DEFAULT_INTERVAL_S


def _limit_for_source(cfg: Dict[str, Any], src: Dict[str, Any]) -> int:
    """
    Determine fetch limit for one source.

    Priority:
      1) src["limit"]
      2) cfg["default_limit"]
      3) 10
    """
    if "limit" in src:
        return int(src["limit"])
    if "default_limit" in cfg:
        return int(cfg["default_limit"])
    return 10


def _compute_backoff(interval_s: int, fail_count: int) -> int:
    """
    Exponential backoff: interval * 2^fail_count, capped at MAX_BACKOFF_S.
    Used after errors to avoid hammering a failing feed.
    """
    # interval * 2^fail_count，upper limit is MAX_BACKOFF_S
    return min(int(interval_s * (2 ** fail_count)), MAX_BACKOFF_S)


def append_feed_run(now: datetime, rec: Dict[str, Any]) -> None:
    """
    Append one run record to the daily JSONL history file.

    This is best-effort only --- If writing fails, we log a warning but do not crash the scheduler.
    """
    try:
        path = _hist_path_for_now(now)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    except Exception as e:
        print(f"[scheduler] WARN history_write_failed err={e!r}", flush=True)


def cleanup_feed_history(retention_days: int = RETENTION_DAYS) -> None:
    """
    Delete old daily history files based on file modification time.

    Only files matching: feed_runs_*.jsonl

    are cleaned up, and anything older than retention_days is removed.
    """
    HIST_DIR.mkdir(parents=True, exist_ok=True)
    now_ts = _now_utc().timestamp()
    keep_s = retention_days * 86400

    for p in HIST_DIR.glob("feed_runs_*.jsonl"):
        try:
            if now_ts - p.stat().st_mtime > keep_s:
                p.unlink()
        except Exception:
            pass


async def scheduler_loop(
    store: DocumentStore,
    index: IndexStore,
    sources_path: Path,
    state_path: Path,
    tick_s: int = DEFAULT_TICK_S,
) -> None:
    """
    Main scheduler loop (runs forever).

    The loop:
    - Loads sources config
    - For each enabled source:
        - Reloads latest ingest_state.json (avoid stale state / lost updates)
        - Checks if the source is due
        - Fetches documents
        - Deduplicates using seen_ids
        - Ingests new docs into the store (offloaded to thread to avoid blocking event loop)
        - Updates feed state (next_run, last_checked, fail_count)
        - Appends one JSONL history record
        - Writes merged state back using update_state() (locked + atomic write)
    - Sleeps tick_s seconds and repeats
    """
    # run once at startup, load initial state, then every source will reload (avoid stale)
    state = IngestState.load(state_path)
    if state.feeds is None:
        state.feeds = {}

    # record last_cleanup_day
    last_cleanup_day: Optional[str] = None

    while True:
        # Load sources.json; if broken, wait and retry
        try:
            cfg = _load_sources(sources_path)
        except Exception as e:
            print(f"[scheduler] ERROR loading sources: {sources_path} err={e}")
            await asyncio.sleep(tick_s)
            continue

        # Daily cleanup of history logs (UTC day)
        day = _now_utc().strftime("%Y-%m-%d")
        if day != last_cleanup_day:
            cleanup_feed_history(retention_days=RETENTION_DAYS)
            last_cleanup_day = day

        sources = cfg.get("sources", []) or []

        for src in sources:
            if not src.get("enabled", True):
                continue

            now = _now_utc()

            # Reload state before each source to avoid overwriting
            # updates made by other processes (e.g., backfill) or other sources.
            state = IngestState.load(state_path)
            if state.feeds is None:
                state.feeds = {}

            # Source config fields
            src_id = src["id"]
            url = src["url"]
            lang = src.get("lang", "en")
            doc_prefix = src.get("doc_prefix", src_id)
            title_prefix = src.get("title_prefix", f"[{src_id}]")

            interval_s = _interval_for_source(cfg, src)
            limit = _limit_for_source(cfg, src)

            # Read existing per-source feed state
            fs = state.feeds.get(src_id) or FeedState()
            next_run = _parse_iso(fs.next_run_iso)

            # Skip if not due yet
            if next_run is not None and now < next_run:
                continue

            print(f"[scheduler] due source={src_id} interval_s={interval_s} limit={limit}")

            add_ids: list[str] = []

            # Diagnostics fields for history record
            t0 = time.time()
            ok = True
            err = None
            fetched_n = 0
            new_n = 0
            ingested_n = 0

            try:
                # 1) Fetch from RSS
                all_docs = fetch_documents(
                    url,
                    limit=limit,
                    doc_prefix=doc_prefix,
                    lang=lang,
                    title_prefix=title_prefix,
                )
                fetched_n = len(all_docs)

                # 2) Deduplicate using state.seen_ids
                new_docs = [d for d in all_docs if d.doc_id not in state.seen_ids]
                new_n = len(new_docs)

                # 3) Ingest into store
                # Use asyncio.to_thread so this does not block the event loop.
                ingested_n = await asyncio.to_thread(store.add_documents, new_docs, persist=True)

                # 4) Update index for new docs
                if ingested_n > 0:
                    update_index(new_docs, index)

                add_ids = [d.doc_id for d in new_docs]

                # 5) Update feed state on success
                fs.fail_count = 0
                fs.last_checked_iso = _to_iso(now)
                fs.next_run_iso = _to_iso(now + timedelta(seconds=interval_s))

                print(
                    f"[scheduler] source={src_id} fetched={fetched_n} "
                    f"new={new_n} ingested={ingested_n}"
                )

            except Exception as e:
                # On failure, record error and schedule with backoff
                ok = False
                err = repr(e)

                fs.fail_count = int(fs.fail_count or 0) + 1
                backoff_s = _compute_backoff(interval_s, fs.fail_count)
                fs.last_checked_iso = _to_iso(now)
                fs.next_run_iso = _to_iso(now + timedelta(seconds=backoff_s))

                print(f"[scheduler] source={src_id} ERROR={e} fail_count={fs.fail_count} next_in={backoff_s}s")

            # Append one line to run history (JSONL append-only）
            dur_ms = int((time.time() - t0) * 1000)
            append_feed_run(now, {
                "ts": _to_iso(now),
                "src_id": src_id,
                "url": url,
                "ok": ok,
                "err": err,
                "interval_s": interval_s,
                "limit": limit,
                "fetched": fetched_n,
                "new": new_n,
                "ingested": ingested_n,
                "fail_count": int(fs.fail_count or 0),
                "last_checked_iso": fs.last_checked_iso,
                "next_run_iso": fs.next_run_iso,
                "duration_ms": dur_ms,
            })

            # Persist merged state:
            # - add_ids -> seen_ids union
            # - update feeds[src_id]
            # - update last_run_iso
            # state only keep latest, and write back via update_state
            state = update_state(
                state_path,
                add_doc_ids=add_ids,
                feed_updates={src_id: fs},
                touch_last_run=True,
            )

        # Wait before next scheduler iteration
        await asyncio.sleep(tick_s)


def start_scheduler_task(store: DocumentStore, index: IndexStore) -> asyncio.Task:
    """
    Start the scheduler loop as a background asyncio Task.

    This is usually called from app startup code.

    It also:
    - Performs an initial cleanup of old history files
    - Attaches a callback to print exceptions if the task crashes
    """
    sources_path = SOURCES_DIR / "sources.json"
    state_path = PROCESSED_DIR / "ingest_state.json"
    print(f"[scheduler] sources_path={sources_path}")
    print(f"[scheduler] state_path={state_path}")

    # One-time cleanup at startup
    cleanup_feed_history(retention_days=RETENTION_DAYS)

    task = asyncio.create_task(scheduler_loop(store, index, sources_path, state_path))

    def _done(t: asyncio.Task):
        """Log task crash/cancel events."""
        try:
            exc = t.exception()
            if exc:
                print(f"[scheduler] TASK CRASHED: {exc!r}", flush=True)
        except asyncio.CancelledError:
            print("[scheduler] TASK CANCELLED", flush=True)

    task.add_done_callback(_done)
    return task
