"""
State models for the ingest pipeline.

This file defines:
1) FeedState  -> per-RSS-source scheduling metadata
2) IngestState -> global ingest state stored on disk (JSON)

Purpose
-------
Persist minimal state between runs so that:

- It will not re-ingest already seen documents (dedup via seen_ids)
- Each RSS source remembers its scheduling/backoff state
- The scheduler can resume safely after restart
"""
from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
import json
from typing import Set, Optional, Dict, Any

"""
---------------------------------------------------------------------------
FeedState
---------------------------------------------------------------------------
Represents the runtime state of a single RSS source.

Each RSS source has its own:
- scheduling info (when to run next)
- failure counter (for exponential backoff)
- HTTP cache headers (etag / last_modified)

This allows independent retry logic per source.
---------------------------------------------------------------------------
"""
# the state of a single RSS feed
@dataclass
class FeedState:
    # ISO timestamp of the next scheduled run (UTC string)
    next_run_iso: Optional[str] = None

    # ISO timestamp of the last checked time (UTC string)
    last_checked_iso: Optional[str] = None

    # Number of consecutive failures (for exponential backoff)
    fail_count: int = 0

    # HTTP ETag for conditional requests (If-None-Match)
    etag: Optional[str] = None

    # HTTP Last-Modified header for conditional requests
    last_modified: Optional[str] = None

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "FeedState":
        """
        Create a FeedState from a JSON dictionary.
        Used when loading ingest_state.json from disk.
        """
        return FeedState(
            next_run_iso=d.get("next_run_iso"),
            last_checked_iso=d.get("last_checked_iso"),
            fail_count=int(d.get("fail_count", 0)),
            etag=d.get("etag"),
            last_modified=d.get("last_modified"),
        )

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert FeedState into a JSON-serializable dictionary.
        Used when saving ingest_state.json.
        """
        return {
            "next_run_iso": self.next_run_iso,
            "last_checked_iso": self.last_checked_iso,
            "fail_count": self.fail_count,
            "etag": self.etag,
            "last_modified": self.last_modified,
        }


"""
---------------------------------------------------------------------------
IngestState
---------------------------------------------------------------------------
Represents the global ingest state stored in:
    data/processed/ingest_state.json

It contains:
- seen_ids      : all document IDs that were already ingested
- last_run_iso  : last time any ingest process ran
- feeds         : per-source scheduling state
"""
@dataclass
class IngestState:
    # Set of document IDs already ingested (for deduplication)
    seen_ids: Set[str]
    # Last global ingest run timestamp (UTC ISO string)
    last_run_iso: Optional[str] = None
    # Mapping: source_id -> FeedState
    feeds: Dict[str, FeedState] = None

    @staticmethod
    def load(path: Path) -> "IngestState":
        """
        Load ingest state from a JSON file.

        If the file does not exist --- Return an empty state.

        This method reconstructs:
        - seen_ids as a Python set
        - feeds as FeedState objects
        """
        if not path.exists():
            return IngestState(seen_ids=set(), last_run_iso=None, feeds={})

        data = json.loads(path.read_text(encoding="utf-8"))

        feeds_raw = data.get("feeds", {}) or {}
        feeds = {
            k: FeedState.from_dict(v)
            for k, v in feeds_raw.items()
        }

        return IngestState(
            seen_ids=set(data.get("seen_ids", [])),
            last_run_iso=data.get("last_run_iso"),
            feeds=feeds,
        )


    def save(self, path: Path) -> None:
        """
        Save the current ingest state to disk as JSON.

        What does it do:
        - Ensures parent directory exists
        - Sorts seen_ids for stable output
        - Serializes FeedState objects
        """
        path.parent.mkdir(parents=True, exist_ok=True)

        payload = {
            "seen_ids": sorted(list(self.seen_ids)),
            "last_run_iso": self.last_run_iso,
            "feeds": {
                k: v.to_dict()
                for k, v in (self.feeds or {}).items()
            },
        }

        path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8"
        )

