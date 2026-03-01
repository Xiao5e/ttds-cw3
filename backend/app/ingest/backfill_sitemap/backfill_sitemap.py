"""
Sitemap backfill script (one-off / batch mode).

Goal
----
Fetch candidate article URLs from one or more sitemaps, download HTML, convert to plain text,
and ingest them into the backend via /admin/ingest.

Key requirements
----------------
1) Deduplication:
   - Each URL is mapped to a stable global doc_id (make_global_doc_id).
   - We skip doc_ids that already exist in ingest_state.json (state.seen_ids).

2) Safety under concurrent writers (scheduler + backfill):
   - We never rewrite ingest_state.json from a stale in-memory copy.
   - After each successful ingest, we merge doc_ids into state using state_io.update_state_seen_ids,
     which performs: (file lock) + (load latest state) + (union seen_ids) + (atomic write).
   - This prevents "lost updates" when multiple processes update the same state file.

3) Crash/partial-run tolerance:
   - We ingest documents in small batches (batch_size).
   - We only mark doc_ids as seen AFTER the ingest API call succeeds.
   - If ingest fails, we keep the current batch in memory (kept) and do not advance state.

4) Observability for debugging:
   - Optionally write every prepared document metadata (site/doc_id/url/title) to a jsonl file.
   - Print progress per site and per flush.

Usage (examples)
----------------
- Dry run with small scale:
  python -m ...backfill_sitemap --dry-run --scale 0.01 --max-total 50

- Only run one site:
  python -m ...backfill_sitemap --only-site govuk --scale 0.1

- Real ingest:
  python -m ...backfill_sitemap --api http://127.0.0.1:8000 --batch-size 50
"""
from __future__ import annotations
import argparse
from dataclasses import dataclass
from typing import Dict, List, Optional
from datetime import datetime, timezone
from pathlib import Path
import requests
from bs4 import BeautifulSoup
import os, json, tempfile
from contextlib import contextmanager

from .sitemap_client import iter_urls_from_sitemap
from .url_filters import is_article_url

from ..state import IngestState
from ...schemas import Document
from ...config import PROCESSED_DIR
from ..backfill_cc.doc_id import make_global_doc_id
from ..backfill_cc.client import fetch_live_html
from ..state_io import update_state_seen_ids, update_state   # 新增 import
from ..time_utils import now_iso

import json

from datetime import datetime, timezone
import time

''' Test functions '''
def _append_jsonl(path: str, obj: dict) -> None:
    """
    Append one JSON object as a JSONL line.
    Used for auditing which URLs/doc_ids were prepared during a run.
    """
    if not path:
        return
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")


''' Helper Functions '''
def html_to_text(html: str) -> str:
    """Convert HTML to plain text. Used as the document body."""
    soup = BeautifulSoup(html or "", "html.parser")
    return soup.get_text(separator=" ", strip=True)


def html_title(html: str) -> str:
    """Extract the <title> text from HTML if available."""
    soup = BeautifulSoup(html or "", "html.parser")
    return soup.title.string.strip() if soup.title and soup.title.string else ""


def ingest_via_api(api_base: str, docs: List[Document]) -> Dict:
    """
    POST documents to backend /admin/ingest.
    If this call fails, we must NOT write doc_ids into state.seen_ids.
    """
    payload = {"docs": [d.model_dump() for d in docs]}
    r = requests.post(f"{api_base.rstrip('/')}/admin/ingest", json=payload, timeout=60)
    r.raise_for_status()
    return r.json()


# Flush helper outside main()
def flush_kept(
    *,
    kept: List[Document],
    args,
    state,
    state_path: Path,
    ingested_site: int,
    total_ingested: int,
    reason: str,
):
    """
    Flush one batch:
    1) Call ingest API
    2) If success: clear kept, then update state.seen_ids via update_state_seen_ids()
    3) Return updated counters and refreshed state

    Important invariant:
    - doc_ids are written into ingest_state.json ONLY after ingest succeeds.
    """
    # return if no need to flush
    if args.dry_run or not kept:
        return ingested_site, total_ingested, state

    # get the doc_id before flushing
    flushed_ids = [d.doc_id for d in kept]
    sample_ids = flushed_ids[:3] # for tests (debug)

    # 1) ingest first
    # write into the db (and then write state)
    try:
        res = ingest_via_api(args.api, kept)
    except Exception as e:
        print(f"[sitemap] FLUSH FAILED reason={reason} error={e!r}", flush=True)
        return ingested_site, total_ingested, state
    ing = int(res.get("ingested", 0))
    ingested_site += ing
    total_ingested += ing

    # 2) remove kept to prevent duplicate flush
    kept.clear()

    # 3) lock + atom-writein, put flushed_ids into ingest_state.json
    state = update_state_seen_ids(state_path, add_doc_ids=flushed_ids, set_last_run=True)

    ''' comment if needed (below) '''
    # 4) check if the newly flushed doc_ids are in the state
    try:
        latest = IngestState.load(state_path)
        hits = sum((doc_id in latest.seen_ids) for doc_id in sample_ids)
        print(
            f"[DEBUG] verify_after_save: state_contains_first3={hits}/{len(sample_ids)} "
            f"(file_seen_ids={len(latest.seen_ids)})",
            flush=True
        )
        if hits == 0 and sample_ids:
            print("[ALERT] Newly flushed doc_ids NOT found in ingest_state.json!", flush=True)
    except Exception as e:
        print(f"[DEBUG] verify_read_failed: {e!r}", flush=True)
    ''' comment if needed (above) '''

    print(f"[sitemap] FLUSH reason={reason} ingest_result={res}", flush=True)
    return ingested_site, total_ingested, state


@dataclass(frozen=True)
class SiteCfg:
    name: str
    sitemaps: list[str]
    quota: int


SITES: List[SiteCfg] = [
    # NASA
    SiteCfg("nasa", [
        "https://www.nasa.gov/wp-sitemap-news-1.xml",
        "https://www.nasa.gov/wp-sitemap-news-2.xml",
        "https://www.nasa.gov/wp-sitemap-news-3.xml",
    ], 10000),

    # GOV.UK
    SiteCfg("govuk", ["https://www.gov.uk/sitemap.xml"], 10000),

    # Python official docs (standard urlset)
    SiteCfg("python_docs", ["https://docs.python.org/sitemap.xml"], 10000),  # urlset 标准 XML [web:29][web:30]

    # Django official docs (standard urlset)
    SiteCfg("django_docs", ["https://docs.djangoproject.com/en/5.2/sitemap.xml"], 10000),  # 版本号可调 [web:34][web:39]

    # # Wikipedia
    # SiteCfg("wikipedia", ["https://en.wikipedia.org/sitemap-index.xml"], 10000),  # 标准 sitemapindex [web:11][web:37],

    # HowStuffWorks
    SiteCfg("howstuffworks", ["https://www.howstuffworks.com/www-sitemap-index.xml"], 10000),

    # National Geographic
    SiteCfg("natgeo", ["https://www.nationalgeographic.com/sitemaps/sitemap.xml"], 10000),

    # Smithsonian
    SiteCfg(
        "smithsonian",
        [
            # Some example sitemaps
            "https://www.smithsonianmag.com/sitemap-articles-2024-01.xml",
            "https://www.smithsonianmag.com/sitemap-articles-2024-02.xml",
            "https://www.smithsonianmag.com/sitemap-articles-2024-03.xml",
            "https://www.smithsonianmag.com/sitemap-articles-2024-04.xml",
            "https://www.smithsonianmag.com/sitemap-articles-2024-05.xml",
            "https://www.smithsonianmag.com/sitemap-articles-2023-01.xml",
            "https://www.smithsonianmag.com/sitemap-articles-2023-02.xml",
            "https://www.smithsonianmag.com/sitemap-articles-2023-03.xml",
            "https://www.smithsonianmag.com/sitemap-articles-2023-04.xml",
            "https://www.smithsonianmag.com/sitemap-articles-2023-05.xml",
            "https://www.smithsonianmag.com/sitemap-articles-2022-10.xml",
            "https://www.smithsonianmag.com/sitemap-articles-2022-11.xml",
            "https://www.smithsonianmag.com/sitemap-articles-2022-12.xml",
            # can add 2021/2020 or earlier, or 2025 or later
        ],
        quota=10000,  # can be larger
    ),

    # The Guardian
    SiteCfg("guardian", [
        "https://www.theguardian.com/sitemaps/news.xml",
    ], 10000),

    # MIT News
    SiteCfg("mit_news", [
        "https://news.mit.edu/sitemap.xml",
    ], 10000),

    # TechCrunch (fixed redirect target)
    SiteCfg("techcrunch", [
        "https://techcrunch.com/sitemap.xml",
    ], 10000),

    # World Bank
    SiteCfg("world_bank", [
        "https://www.worldbank.org/sitemap.xml",
    ], 10000),
]


def main():
    global_prepared = 0

    p = argparse.ArgumentParser(description="One-off sitemap backfill (Wikipedia+BBC+NASA+GOV.UK)")
    p.add_argument("--api", default="http://127.0.0.1:8000")
    p.add_argument("--state", default=str(PROCESSED_DIR / "ingest_state.json"))
    p.add_argument("--timeout", type=int, default=20)
    p.add_argument("--batch-size", type=int, default=50)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--scale", type=float, default=1.0, help="scale quotas, e.g. 0.01 -> ~1k total")
    # for validation
    p.add_argument("--max-total", type=int, default=0, help="Stop after preparing this many docs total (0=unlimited)")
    p.add_argument("--only-site", type=str, default="", help="Only run one site by name, e.g. govuk")
    p.add_argument("--log-jsonl", type=str, default="", help="Write prepared doc_id/url/title to this jsonl for audit")
    
    args = p.parse_args()

    state_path = Path(args.state)
    print("DEBUG state_path resolved =", state_path.resolve())

    state = IngestState.load(state_path)

    batch_size = max(1, int(args.batch_size))
    timeout_s = int(args.timeout)

    total_prepared = 0
    total_ingested = 0

    for site in SITES:
        # before each site, reload to keep the latest state
        state = IngestState.load(state_path)

        # experiment code, delete after it is done
        # if passed only_site, run only this site
        # else run all sites
        # use parameter only_site from args
        if args.only_site and site.name != args.only_site:
            continue
        ####

        quota = max(0, int(site.quota * float(args.scale)))
        if quota <= 0:
            continue

        print(f"[sitemap] site={site.name} quota={quota} sitemaps={site.sitemaps}", flush=True)

        kept: List[Document] = []
        prepared_site = 0
        ingested_site = 0

        # try each sitemap candidate one by one
        sitemap_worked = False
        for sitemap_url in site.sitemaps:
            if prepared_site >= quota:
                break

            for item in iter_urls_from_sitemap(sitemap_url, timeout_s=timeout_s, max_urls=quota * 5):
                if prepared_site >= quota:
                    break

                url = item.loc

                # Show why we skip URLs (first 20 only)
                if prepared_site < 20:
                    print(f"[DEBUG-url] site={site.name} url={url}", flush=True)

                ok_article = is_article_url(site.name, url)
                if prepared_site < 20:
                    print(f"[DEBUG-skip] site={site.name} stage=is_article_url ok={ok_article}", flush=True)
                if not ok_article:
                    continue
                #####


                doc_id = make_global_doc_id(url)
                seen = doc_id in state.seen_ids
                if prepared_site < 20:
                    print(f"[DEBUG-skip] site={site.name} stage=seen_ids seen={seen} doc_id={doc_id}", flush=True)
                if seen:
                    continue
                #####

                # --- DEBUG replacement START (replace the above original try/except block) ---

                try:
                    html = fetch_live_html(url, timeout_s=timeout_s)
                except Exception as e:
                    # Only print the first few failures to avoid spamming the terminal
                    if prepared_site < 20:
                        print(
                            f"[DEBUG-skip] site={site.name} stage=fetch_live_html url={url} err={e!r}",
                            flush=True,
                        )
                    continue

                title = html_title(html) or f"[{site.name.upper()}] {url}"
                body = html_to_text(html)

                if len(body) < 200:
                    if prepared_site < 20:
                        print(
                            f"[DEBUG-skip] site={site.name} stage=short_body url={url} len={len(body)} title={title[:60]!r}",
                            flush=True,
                        )
                    continue

                # --- DEBUG replacement END ---


                kept.append(
                    Document(
                        doc_id=doc_id,
                        title=title,
                        body=body,
                        url=url,
                        timestamp=None,
                        lang="en",
                    )
                )
                prepared_site += 1
                sitemap_worked = True


                """Test code"""
                global_prepared += 1
                _append_jsonl(args.log_jsonl, {"site": site.name, "doc_id": doc_id, "url": url, "title": title})
                print(f"[sitemap] PREPARED site={site.name} doc_id={doc_id} url={url}", flush=True)


                """Test code (delete after)"""
                # max --- fill
                if len(kept) >= batch_size:
                    ingested_site, total_ingested, state = flush_kept(
                        kept=kept,
                        args=args,
                        state_path=state_path,
                        state=state,
                        ingested_site=ingested_site,
                        total_ingested=total_ingested,
                        reason="batch_full",
                    )
                    print(f"[sitemap] site={site.name} progress prepared={prepared_site} ingested={ingested_site}", flush=True)

                # max-total: flush and out
                if args.max_total and global_prepared >= args.max_total:
                    ingested_site, total_ingested, state = flush_kept(
                        kept=kept,
                        args=args,
                        state_path=state_path,
                        state=state,
                        ingested_site=ingested_site,
                        total_ingested=total_ingested,
                        reason="max_total_reached",
                    )
                    print(f"[sitemap] STOP max_total reached. prepared={global_prepared} state={state_path}", flush=True)
                    return


                if args.dry_run and prepared_site <= 5:
                    print(f"  + {doc_id} | {title[:80]}", flush=True)

            # if the sitemap worked, no need to try other entries
            if sitemap_worked:
                break

        if not sitemap_worked:
            print(f"[sitemap] site={site.name} WARN no usable sitemap (all failed or empty)", flush=True)

        ingested_site, total_ingested, state = flush_kept(
            kept=kept,
            args=args,
            state_path=state_path,
            state=state,
            ingested_site=ingested_site,
            total_ingested=total_ingested,
            reason="site_done",
        )

        total_prepared += prepared_site
        print(f"[sitemap] site={site.name} DONE prepared={prepared_site} ingested={ingested_site}", flush=True)


    state = update_state(state_path, touch_last_run=True)

    print(f"[sitemap] ALL DONE prepared={total_prepared} ingested={total_ingested} state={state_path}", flush=True)


if __name__ == "__main__":
    main()
