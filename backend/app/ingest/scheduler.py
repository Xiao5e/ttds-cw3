"""Scheduler (stub for demo)."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional, Iterable

from .state import IngestState, FeedState
from .sources.medium_rss import fetch_documents
from ..config import PROCESSED_DIR, SOURCES_DIR
from ..storage.document_store import DocumentStore
from ..storage.index_store import IndexStore
from ..indexing.indexer import update_index


DEFAULT_TICK_S = 10
DEFAULT_INTERVAL_S = 180
# DEFAULT_INTERVAL_S = 60 # test
MAX_BACKOFF_S = 3600  # 1 hour


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _to_iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_iso(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    # simple parse for "...Z"
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    return datetime.fromisoformat(s)


def _load_sources(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _interval_for_source(cfg: Dict[str, Any], src: Dict[str, Any]) -> int:
    # 优先级：src.interval_s > cfg.default_interval_s > hard default
    if "interval_s" in src:
        return int(src["interval_s"])
    if "default_interval_s" in cfg:
        return int(cfg["default_interval_s"])
    return DEFAULT_INTERVAL_S


def _limit_for_source(cfg: Dict[str, Any], src: Dict[str, Any]) -> int:
    # 你之前的逻辑：src.limit > cfg.default_limit > 10
    if "limit" in src:
        return int(src["limit"])
    if "default_limit" in cfg:
        return int(cfg["default_limit"])
    return 10


def _compute_backoff(interval_s: int, fail_count: int) -> int:
    # interval * 2^fail_count，上限 MAX_BACKOFF_S
    return min(int(interval_s * (2 ** fail_count)), MAX_BACKOFF_S)


async def scheduler_loop(
    store: DocumentStore,
    index: IndexStore,
    sources_path: Path,
    state_path: Path,
    tick_s: int = DEFAULT_TICK_S,
) -> None:
    state = IngestState.load(state_path)
    if state.feeds is None:
        state.feeds = {}

    while True:
        try:
            cfg = _load_sources(sources_path)
        except Exception as e:
            print(f"[scheduler] ERROR loading sources: {sources_path} err={e}")
            await asyncio.sleep(tick_s)
            continue

        sources = cfg.get("sources", []) or []
        now = _now_utc()

        for src in sources:
            if not src.get("enabled", True):
                continue

            src_id = src["id"]
            url = src["url"]
            lang = src.get("lang", "en")
            doc_prefix = src.get("doc_prefix", src_id)
            title_prefix = src.get("title_prefix", f"[{src_id}]")

            interval_s = _interval_for_source(cfg, src)
            limit = _limit_for_source(cfg, src)

            fs = state.feeds.get(src_id) or FeedState()
            next_run = _parse_iso(fs.next_run_iso)

            # 第一次运行：立刻抓（next_run 为空）
            if next_run is not None and now < next_run:
                continue

            print(f"[scheduler] due source={src_id} interval_s={interval_s} limit={limit}")

            try:
                all_docs = fetch_documents(
                    url,
                    limit=limit,
                    doc_prefix=doc_prefix,
                    lang=lang,
                    title_prefix=title_prefix,
                )
                new_docs = [d for d in all_docs if d.doc_id not in state.seen_ids]

                ingested = store.add_documents(new_docs, persist=True)
                if ingested > 0:
                    update_index(new_docs, index)

                for d in new_docs:
                    state.seen_ids.add(d.doc_id)

                fs.fail_count = 0
                fs.last_checked_iso = _to_iso(now)
                fs.next_run_iso = _to_iso(now + timedelta(seconds=interval_s))
                state.feeds[src_id] = fs

                print(f"[scheduler] source={src_id} fetched={len(all_docs)} new={len(new_docs)} ingested={ingested}")

            except Exception as e:
                fs.fail_count = int(fs.fail_count or 0) + 1
                backoff_s = _compute_backoff(interval_s, fs.fail_count)
                fs.last_checked_iso = _to_iso(now)
                fs.next_run_iso = _to_iso(now + timedelta(seconds=backoff_s))
                state.feeds[src_id] = fs
                print(f"[scheduler] source={src_id} ERROR={e} fail_count={fs.fail_count} next_in={backoff_s}s")

            # 每个源处理完就落盘一次，减少崩溃损失
            state.last_run_iso = _to_iso(_now_utc())
            state.save(state_path)

        await asyncio.sleep(tick_s)


def start_scheduler_task(store: DocumentStore, index: IndexStore) -> asyncio.Task:
    sources_path = SOURCES_DIR / "sources.json"
    state_path = PROCESSED_DIR / "ingest_state.json"
    print(f"[scheduler] sources_path={sources_path}")
    print(f"[scheduler] state_path={state_path}")
    return asyncio.create_task(scheduler_loop(store, index, sources_path, state_path))
