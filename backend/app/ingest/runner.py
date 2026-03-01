"""
Manual RSS ingestion runner (Module C demo).

Purpose: This script performs a one-off ingestion from multiple RSS sources.

It:
1) Reads configured RSS sources from sources.json
2) Fetches documents from each source
3) Deduplicates using ingest_state.json (seen_ids)
4) Sends new documents to the backend ingest API
5) Updates ingest_state.json safely

This is intended for manual testing or demo runs.
For automated periodic ingestion, use the scheduler instead.
"""
from __future__ import annotations
import argparse
from pathlib import Path
from typing import List, Dict, Any
import json
import requests

from .state import IngestState
from .sources.medium_rss import fetch_documents  # 通用 'RSS fetcher
from ..schemas import Document
from ..config import DATA_DIR, PROCESSED_DIR, SOURCES_DIR
from .state_io import update_state

# DEFAULT_STATE = Path("data/processed/ingest_state.json")
# DEFAULT_SOURCES = Path("settings/sources.json")

# Default configuration paths and API endpoint
DEFAULT_API = "http://127.0.0.1:8000"
DEFAULT_STATE = PROCESSED_DIR / "ingest_state.json"
DEFAULT_SOURCES = SOURCES_DIR / "sources.json"


def ingest(api_base: str, docs: List[Document]) -> dict:
    """
    Send documents to the backend ingest API.

    Parameters
    ----------
    api_base:
        Base URL of backend service (e.g., http://127.0.0.1:8000)
    docs:
        List of Document objects to ingest

    Returns: JSON response from backend.
    """
    payload = {"docs": [d.model_dump() for d in docs]}
    r = requests.post(f"{api_base.rstrip('/')}/admin/ingest", json=payload, timeout=20)
    r.raise_for_status()
    return r.json()


def load_sources(path: Path) -> Dict[str, Any]:
    """
    Load RSS source configuration from JSON file.
    """
    data = json.loads(path.read_text(encoding="utf-8"))
    return data


def main():
    """
    Main entry point.

    Steps:
    1) Parse CLI arguments
    2) Load sources configuration
    3) Load ingest_state.json
    4) For each enabled source:
         - Fetch documents
         - Deduplicate using seen_ids
         - Ingest new documents via API
         - Update seen_ids
    5) Persist updated state
    """
    parser = argparse.ArgumentParser(description="Module C: Live ingestion demo (multi RSS sources)")
    parser.add_argument("--api", default=DEFAULT_API)
    parser.add_argument("--sources", default=str(DEFAULT_SOURCES))
    parser.add_argument("--state", default=str(DEFAULT_STATE))
    parser.add_argument("--limit", type=int, default=None, help="Override per-source/default limit")
    args = parser.parse_args()

    sources_path = Path(args.sources)
    state_path = Path(args.state)

    cfg = load_sources(sources_path)
    default_limit = int(cfg.get("default_limit", 10))
    sources = cfg.get("sources", [])

    state = IngestState.load(state_path)

    total_fetched = 0
    total_new = 0
    total_ingested = 0

    for src in sources:
        if not src.get("enabled", True):
            continue

        src_id = src["id"]
        url = src["url"]
        lang = src.get("lang", "en")
        doc_prefix = src.get("doc_prefix", src_id)
        title_prefix = src.get("title_prefix", f"[{src_id}]")
        limit = args.limit if args.limit is not None else int(src.get("limit", default_limit))

        # Fetch RSS documents
        try:
            all_docs = fetch_documents(url, limit=limit, doc_prefix=doc_prefix, lang=lang, title_prefix=title_prefix)
        except Exception as e:
            print(f"[runner] source={src_id} ERROR fetching: {e}")
            continue

        total_fetched += len(all_docs)

        # Deduplicate using seen_ids
        new_docs = [d for d in all_docs if d.doc_id not in state.seen_ids]
        total_new += len(new_docs)

        print(f"[runner] source={src_id} fetched={len(all_docs)} new={len(new_docs)}")

        if not new_docs:
            continue

        # Print sample of new documents
        for d in new_docs[:5]:
            print(f"  + {d.doc_id} | {d.title[:80]}")
        if len(new_docs) > 5:
            print(f"  ... ({len(new_docs)-5} more)")

        # Send new documents to backend
        try:
            result = ingest(args.api, new_docs)
            print(f"[runner] source={src_id} ingest_result={result}")
            total_ingested += int(result.get("ingested", 0))
        except Exception as e:
            print(f"[runner] source={src_id} ERROR ingesting: {e}")
            continue

        # Update in-memory seen_ids
        for d in new_docs:
            state.seen_ids.add(d.doc_id)

    # Persist updated state safely
    update_state(state_path, add_doc_ids=[d.doc_id for d in new_docs], touch_last_run=True)
    print(f"[runner] DONE fetched={total_fetched} new={total_new} ingested={total_ingested}")
    print(f"[runner] state_saved={state_path}")


if __name__ == "__main__":
    main()
