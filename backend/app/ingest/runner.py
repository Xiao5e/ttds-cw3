from __future__ import annotations
import argparse
from pathlib import Path
from typing import List

import requests

from .state import IngestState
from .sources.medium_rss import fetch_documents
from ..schemas import Document

DEFAULT_FEED = "https://medium.com/feed/swlh"
DEFAULT_API = "http://127.0.0.1:8000"
DEFAULT_STATE = Path("data/processed/ingest_state_medium_swlh.json")

def ingest(api_base: str, docs: List[Document]) -> dict:
    payload = {"docs": [d.model_dump() for d in docs]}
    r = requests.post(f"{api_base.rstrip('/')}/admin/ingest", json=payload, timeout=20)
    r.raise_for_status()
    return r.json()

def main():
    parser = argparse.ArgumentParser(description="Module C: Live ingestion demo (Medium RSS)")
    parser.add_argument("--feed", default=DEFAULT_FEED)
    parser.add_argument("--api", default=DEFAULT_API)
    parser.add_argument("--limit", type=int, default=10)
    parser.add_argument("--state", default=str(DEFAULT_STATE))
    args = parser.parse_args()

    state_path = Path(args.state)
    state = IngestState.load(state_path)

    all_docs = fetch_documents(args.feed, limit=args.limit)
    new_docs = [d for d in all_docs if d.doc_id not in state.seen_ids]

    print(f"[runner] fetched={len(all_docs)} new={len(new_docs)} feed={args.feed}")

    if not new_docs:
        print("[runner] no new documents. done.")
        return

    # 可读日志（demo 很有用）
    for d in new_docs:
        print(f"  + {d.doc_id} | {d.title[:80]}")

    result = ingest(args.api, new_docs)
    print(f"[runner] ingest_result={result}")

    # 更新 state
    for d in new_docs:
        state.seen_ids.add(d.doc_id)
    state.save(state_path)
    print(f"[runner] state_saved={state_path}")

if __name__ == "__main__":
    main()
