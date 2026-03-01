"""
State I/O utilities for ingest_state.json.

Purpose
- Provide safe, concurrent, atomic updates to the ingest state file
- Deal with Multiple processes (RSS scheduler + backfill) may update the same state file.

What it helps
1) Lock before modifying the file.
2) Load the latest state from disk inside the lock.
3) Write using atomic replace (write temp file → replace).
4) Write the full payload (never partially overwrite fields).
"""
# backend/app/ingest/state_io.py
from __future__ import annotations

import json, os, tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Iterable, Optional, Dict, Any

from .state import IngestState, FeedState
from .time_utils import now_iso  # 或者把 now_iso 挪到公共 util

import time


"""
---------------------------------------------------------------------------
File Locking
---------------------------------------------------------------------------
Ensures only one process writes to ingest_state.json at a time.
Uses a companion ".lock" file with fcntl exclusive locking.
"""
@contextmanager
def locked_file(path: Path):
    """
    Acquire an exclusive file lock on `path`.

    This prevents concurrent writers from corrupting the state file.
    """
    lock_path = str(path) + ".lock"
    fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o644)
    try:
        import fcntl
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield
    finally:
        try:
            import fcntl
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)


"""
Atomic Write
---------------------------------------------------------------------------
Prevents partial file corruption:
1) Write JSON to a temporary file
2) Atomically replace the target file
---------------------------------------------------------------------------
"""
def atomic_write_json(path: Path, payload: Dict[str, Any], *, indent: int = 2) -> None:
    """
    Atomically write JSON to disk.

    Guarantees:
    - File is either fully written or not changed at all.
    - No half-written/corrupted JSON.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=path.name + ".", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=indent)
        os.replace(tmp, path) # atomic replacement
    finally:
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except Exception:
            pass


"""
Convert IngestState into a JSON-serializable dictionary.
"""
def state_to_payload(state: IngestState) -> Dict[str, Any]:
    """
    Convert IngestState into a JSON-serializable dictionary.
    """
    feeds_out: Dict[str, Any] = {}
    for k, v in (getattr(state, "feeds", {}) or {}).items():
        if isinstance(v, FeedState):
            feeds_out[k] = v.to_dict()
        elif isinstance(v, dict):
            feeds_out[k] = v
        else:
            # prevent json from crashing
            feeds_out[k] = getattr(v, "to_dict", lambda: {})()

    return {
        "seen_ids": sorted(list(state.seen_ids)),
        "last_run_iso": getattr(state, "last_run_iso", None),
        "feeds": feeds_out,
    }


def update_state_seen_ids(
    state_path: Path,
    *,
    add_doc_ids: Iterable[str],
    set_last_run: bool = True,
) -> IngestState:
    """
    Safely update seen_ids in ingest_state.json.

    Steps:
    1) Acquire lock
    2) Load latest state (including feeds)
    3) Union new doc_ids
    4) Optionally update last_run_iso
    5) Atomically write full payload

    Returns: The updated IngestState object.
    """
    add_doc_ids = list(add_doc_ids)
    if not add_doc_ids and not set_last_run:
        return IngestState.load(state_path)

    with locked_file(state_path):
        latest = IngestState.load(state_path)
        for did in add_doc_ids:
            latest.seen_ids.add(did)
        if set_last_run:
            latest.last_run_iso = now_iso()

        payload = state_to_payload(latest)

        # make sure seen_ids is a list and saved in multiple lines (readable)
        if isinstance(payload.get("seen_ids"), set):
            payload["seen_ids"] = sorted(list(payload["seen_ids"]))

        atomic_write_json(state_path, payload, indent=2)
        return latest
    

"""
Unified Update (seen_ids + feeds)
---------------------------------------------------------------------------
Used by scheduler to update:
- new document IDs
- per-source FeedState
- last_run timestamp
"""
def update_state(
    state_path: Path,
    *,
    add_doc_ids: Optional[Iterable[str]] = None,
    feed_updates: Optional[Dict[str, FeedState]] = None,
    touch_last_run: bool = True,
) -> IngestState:
    """
    Safely update ingest state (seen_ids + feeds).

    Steps:
    1) Acquire exclusive lock
    2) Load latest state from disk
    3) Merge:
         - seen_ids
         - feed_updates (per source)
    4) Optionally update last_run_iso
    5) Atomically write full state
    6) Return updated state

    This is the main entry point for scheduler updates.
    """
    add_doc_ids = list(add_doc_ids or [])
    feed_updates = feed_updates or {}

    t0 = time.time()
    with locked_file(state_path):
        t1 = time.time()
        if t1 - t0 > 1.0:
            print(f"[state_io] update_state waited_lock={(t1 - t0):.3f}s", flush=True)

        latest = IngestState.load(state_path)
        if latest.feeds is None:
            latest.feeds = {}

        # Merge seen_ids
        for did in add_doc_ids:
            latest.seen_ids.add(did)

        # Merge feed states
        for src_id, fs in feed_updates.items():
            latest.feeds[src_id] = fs

        if touch_last_run:
            latest.last_run_iso = now_iso()

        t2 = time.time()
        atomic_write_json(state_path, state_to_payload(latest), indent=2)
        t3 = time.time()

        if t3 - t2 > 0.5:
            print(f"[state_io] update_state write_time={(t3 - t2):.3f}s", flush=True)

        return latest