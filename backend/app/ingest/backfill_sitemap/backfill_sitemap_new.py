# backfill_sitemap_new.py
from __future__ import annotations

import argparse
from dataclasses import dataclass
from typing import Dict, List, Optional, Iterator, Tuple
from datetime import datetime, timezone
from pathlib import Path
import requests
from bs4 import BeautifulSoup
import json
import time
import random
import threading
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, Future, wait, FIRST_COMPLETED

from .sitemap_client import iter_urls_from_sitemap
from .url_filters import is_article_url

from ..state import IngestState
from ...schemas import Document
from ...config import PROCESSED_DIR
from ..backfill_cc.doc_id import make_global_doc_id
from ..backfill_cc.client import fetch_live_html
from ..state_io import update_state_seen_ids, update_state  # keep
from app.ingest.backfill_sitemap.url_filters import canonical_url
from ..time_utils import now_iso

from collections import defaultdict


# -----------------------------
# Test/Audit helpers (keep)
# -----------------------------
def _append_jsonl(path: str, obj: dict) -> None:
    """
    Append one JSON object as a JSONL line.
    Used for auditing which URLs/doc_ids were prepared during a run.
    """
    if not path:
        return
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")


# -----------------------------
# HTML helpers (keep)
# -----------------------------
def html_to_text(html: str) -> str:
    """Convert HTML to plain text. Used as the document body."""
    soup = BeautifulSoup(html or "", "html.parser")
    return soup.get_text(separator=" ", strip=True)


def html_title(html: str) -> str:
    """Extract the <title> text from HTML if available."""
    soup = BeautifulSoup(html or "", "html.parser")
    return soup.title.string.strip() if soup.title and soup.title.string else ""


# -----------------------------
# Ingest helpers (keep)
# -----------------------------
def ingest_via_api(api_base: str, docs: List[Document]) -> Dict:
    """
    POST documents to backend /admin/ingest.
    If this call fails, we must NOT write doc_ids into state.seen_ids.
    """
    payload = {"docs": [d.model_dump() for d in docs]}
    url = f"{api_base.rstrip('/')}/admin/ingest"
    try:
        r = requests.post(url, json=payload, timeout=60)
    except Exception as e:
        print(f"[ingest_http] POST failed url={url} docs={len(docs)} err={e!r}", flush=True)
        raise

    # Print response body head on HTTP errors for easier production debugging
    if r.status_code >= 400:
        print(f"[ingest_http] status={r.status_code} url={url} docs={len(docs)} body={r.text[:500]!r}", flush=True)
        r.raise_for_status()

    try:
        j = r.json()
    except Exception as e:
        print(f"[ingest_http] json_parse_failed url={url} status={r.status_code} body_head={r.text[:200]!r} err={e!r}", flush=True)
        raise

    # print out the skip Reason
    print(f"[ingest_http] ok status={r.status_code} docs={len(docs)} resp={j}", flush=True)
    return j


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
    if args.dry_run or not kept:
        return ingested_site, total_ingested, state

    flushed_ids = [d.doc_id for d in kept]

    flushed_urls = [d.url for d in kept] # debug
    print(f"[sitemap] FLUSH_BEGIN reason={reason} docs={len(kept)} id0={flushed_ids[:2]} url0={flushed_urls[:2]}", flush=True)
    sample_ids = flushed_ids[:3]  # debug

    try:
        res = ingest_via_api(args.api, kept)
    except Exception as e:
        print(f"[sitemap] FLUSH FAILED reason={reason} error={e!r}", flush=True)
        return ingested_site, total_ingested, state

    ing = int(res.get("ingested", 0))
    ingested_site += ing
    total_ingested += ing

    kept.clear()

    # state write ONLY after ingest success
    state = update_state_seen_ids(state_path, add_doc_ids=flushed_ids, set_last_run=True)

    # optional verify (keep your debug block style)
    try:
        latest = IngestState.load(state_path)
        hits = sum((doc_id in latest.seen_ids) for doc_id in sample_ids)
        print(
            f"[DEBUG] verify_after_save: state_contains_first3={hits}/{len(sample_ids)} "
            f"(file_seen_ids={len(latest.seen_ids)})",
            flush=True,
        )
        if hits == 0 and sample_ids:
            print("[ALERT] Newly flushed doc_ids NOT found in ingest_state.json!", flush=True)
    except Exception as e:
        print(f"[DEBUG] verify_read_failed: {e!r}", flush=True)

    print(f"[sitemap] FLUSH reason={reason} ingest_result={res}", flush=True)
    return ingested_site, total_ingested, state


# -----------------------------
# Site config
# -----------------------------
@dataclass(frozen=True)
class SiteCfg:
    name: str
    sitemaps: list[str]
    quota: int


SITES: List[SiteCfg] = [
    SiteCfg(
        "nasa",
        [
            "https://www.nasa.gov/wp-sitemap-news-1.xml",
            "https://www.nasa.gov/wp-sitemap-news-2.xml",
            "https://www.nasa.gov/wp-sitemap-news-3.xml",
        ],
        10000,
    ),
    SiteCfg("govuk", ["https://www.gov.uk/sitemap.xml"], 10000),
    SiteCfg("python_docs", ["https://docs.python.org/sitemap.xml"], 10000),
    # SiteCfg("django_docs", ["https://docs.djangoproject.com/en/5.2/sitemap.xml"], 10000),
    SiteCfg("howstuffworks", ["https://www.howstuffworks.com/www-sitemap-index.xml"], 10000),
    SiteCfg("natgeo", ["https://www.nationalgeographic.com/sitemaps/sitemap.xml"], 10000),
    SiteCfg(
        "smithsonian",
        [
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
        ],
        quota=10000,
    ),
    SiteCfg("guardian", ["https://www.theguardian.com/sitemaps/news.xml"], 10000),
    SiteCfg("mit_news", ["https://news.mit.edu/sitemap.xml"], 10000),
    SiteCfg("techcrunch", ["https://techcrunch.com/sitemap.xml"], 10000),
    SiteCfg("world_bank", ["https://www.worldbank.org/sitemap.xml"], 10000),
]


# ============================================================
# Per-host limiter + penalty box
# ============================================================
@dataclass
class HostPolicy:
    min_delay: float
    jitter: float
    next_allowed_ts: float = 0.0

    # penalty/backoff
    fail_count: int = 0
    backoff_s: float = 0.0
    penalty_until_ts: float = 0.0


def _default_policy_for_host(host: str) -> HostPolicy:
    """
    Assign slightly more conservative pacing to commonly protected publishers.
    """
    h = host.lower()

    # slightly more conservative for commonly protected publishers
    if any(x in h for x in ("nationalgeographic.com", "techcrunch.com", "theguardian.com")):
        return HostPolicy(min_delay=2.8, jitter=1.6)
    if any(x in h for x in ("howstuffworks.com", "news.mit.edu")):
        return HostPolicy(min_delay=2.0, jitter=1.2)

    return HostPolicy(min_delay=1.5, jitter=1.0)


class HostLimiter:
    """
    Thread-safe per-host limiter.

    - Normal pacing: min_delay + jitter
    - On failures: penalty box with exponential backoff (60s -> 120s -> 240s ... capped)
    """
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._policies: Dict[str, HostPolicy] = {}


    def _get(self, host: str) -> HostPolicy:
        with self._lock:
            pol = self._policies.get(host)
            if pol is None:
                pol = _default_policy_for_host(host)
                self._policies[host] = pol
            return pol


    def acquire(self, host: str) -> None:
        """
        Block until this host is allowed (rate limit + penalty box).
        """
        while True:
            pol = self._get(host)
            now = time.time()
            with self._lock:
                wait_until = max(pol.next_allowed_ts, pol.penalty_until_ts)
                if now >= wait_until:
                    # reserve next slot now to prevent thundering herd
                    delay = pol.min_delay + random.random() * pol.jitter
                    pol.next_allowed_ts = now + delay
                    return
                sleep_s = wait_until - now

            # sleep outside lock
            time.sleep(min(sleep_s, 5.0))


    def mark_ok(self, host: str) -> None:
        """Clear penalty state for host after a successful fetch"""
        pol = self._get(host)
        with self._lock:
            pol.fail_count = 0
            pol.backoff_s = 0.0
            pol.penalty_until_ts = 0.0


    def penalize(self, host: str, *, base: float = 10.0, cap: float = 1800.0) -> float:
        """
        Put host into penalty box using exponential backoff.
        Returns the backoff applied.
        """
        pol = self._get(host)
        now = time.time()
        with self._lock:
            pol.fail_count += 1
            if pol.backoff_s <= 0:
                pol.backoff_s = base
            else:
                pol.backoff_s = min(cap, pol.backoff_s * 2.0)

            pol.penalty_until_ts = max(pol.penalty_until_ts, now + pol.backoff_s)
            return pol.backoff_s


# ============================================================
# Round-robin URL production across sites
# ============================================================
@dataclass
class SiteRuntime:
    cfg: SiteCfg
    quota: int
    timeout_s: int
    prepared: int = 0
    ingested: int = 0
    sitemap_iter: Optional[Iterator] = None
    sitemap_worked: bool = False
    done: bool = False
    scheduled: int = 0

    def __post_init__(self) -> None:
        self.sitemap_iter = self._iter_all_urls()


    def _iter_all_urls(self) -> Iterator[str]:
        """
        Iterate URLs from the first sitemap that yields usable items.
        Matches your old behavior: "first sitemap that works -> stop trying others".
        """
        for sm in self.cfg.sitemaps:
            if self.prepared >= self.quota:
                return

            any_yielded = False
            try:
                for item in iter_urls_from_sitemap(
                    sm,
                    timeout_s=self.timeout_s,
                    max_urls=self.quota * 5,
                ):
                    any_yielded = True
                    yield item.loc
            except Exception:
                continue

            if any_yielded:
                self.sitemap_worked = True
                return

        self.sitemap_worked = False
        return


    def next_url(self) -> Optional[str]:
        """Return next URL from sitemap iterator, or None when exhausted"""
        if self.done or self.prepared >= self.quota:
            self.done = True
            return None
        if self.sitemap_iter is None:
            self.done = True
            return None
        try:
            return next(self.sitemap_iter)
        except StopIteration:
            self.done = True
            return None


def _build_site_runtime(cfg: SiteCfg, *, quota: int, timeout_s: int) -> SiteRuntime:
    """
    Build runtime and wrap sitemap iteration.

    This over-samples sitemap entries (quota * 50) because:
    - many URLs may be filtered out by is_article_url()
    - many URLs may already be in seen_ids
    Without over-sampling, a site can appear "empty" even when the sitemap is large.
    """
    rt = SiteRuntime(cfg=cfg, quota=quota, timeout_s=timeout_s)

    def _gen() -> Iterator[str]:
        # try each sitemap candidate one by one; stop after first worked
        for sm in cfg.sitemaps:
            if rt.prepared >= rt.quota:
                return

            any_yielded = False
            # keep same "quota*5" sampling behavior
            max_urls = rt.quota * 50
            if cfg.name == "techcrunch":
                max_urls = max(max_urls, 2000)  # ✅ 最小改动：保证能越过 newsletters 前缀
            for item in iter_urls_from_sitemap(sm, timeout_s=timeout_s, max_urls=max_urls):
                url = item.loc
                any_yielded = True
                yield url

            if any_yielded:
                rt.sitemap_worked = True
                return

        rt.sitemap_worked = False
        return

    rt.sitemap_iter = _gen()
    return rt


# ============================================================
# Concurrent fetch task
# ============================================================
@dataclass(frozen=True)
class FetchJob:
    site_name: str
    url: str
    doc_id: str


@dataclass
class FetchResult:
    job: FetchJob
    ok: bool
    skip_reason: str = ""
    title: str = ""
    body: str = ""
    err_repr: str = ""


def _host_of(url: str) -> str:
    try:
        return (urlparse(url).netloc or "").lower()
    except Exception:
        return ""


def _fetch_worker(
    job: FetchJob,
    *,
    timeout_s: int,
    limiter: HostLimiter,
    debug_first_n: int,
    debug_counter_ref: List[int],
    debug_lock: threading.Lock,
) -> FetchResult:
    """
    Runs in thread pool: per-host acquire -> fetch_live_html -> parse -> return.
    """
    host = _host_of(job.url) or "unknown-host"

    limiter.acquire(host)

    try:
        html = fetch_live_html(job.url, timeout_s=timeout_s)
    except Exception as e:
        # backoff = limiter.penalize(host)
        status = None
        try:
            resp = getattr(e, "response", None)
            if resp is not None:
                status = resp.status_code
        except Exception:
            pass

        if status in (404, 410):
            backoff = 0.0  # no penalty for known dead links (make it faster)
        else:
            backoff = limiter.penalize(host)

        # controlled debug output
        with debug_lock:
            if debug_counter_ref[0] < debug_first_n:
                debug_counter_ref[0] += 1
                # print(
                #     f"[DEBUG-skip] site={job.site_name} stage=fetch_live_html host={host} "
                #     f"backoff={int(backoff)}s url={job.url} err={e!r}",
                #     flush=True,
                # )
        return FetchResult(job=job, ok=False, skip_reason="fetch_error", err_repr=repr(e))

    limiter.mark_ok(host)

    title = html_title(html) or f"[{job.site_name.upper()}] {job.url}"
    body = html_to_text(html)

    if len(body) < 200:
        with debug_lock:
            if debug_counter_ref[0] < debug_first_n:
                debug_counter_ref[0] += 1
                print(
                    f"[DEBUG-skip] site={job.site_name} stage=short_body host={host} len={len(body)} url={job.url}",
                    flush=True,
                )
        return FetchResult(job=job, ok=False, skip_reason="short_body", title=title, body=body)

    return FetchResult(job=job, ok=True, title=title, body=body)


# ============================================================
# Main
# CLI usage (backfill_sitemap_new)
# ============================================================
# Run a one-off sitemap backfill to fetch article pages and ingest them into your backend.
#
# Basic example (all configured sites):
#   python -m app.ingest.backfill_sitemap.backfill_sitemap_new \
#     --scale 0.5 \
#     --workers 12 \
#     --batch-size 200 \
#     --timeout 60 \
#     --max-total 50000
#
# Flags:
#   --api         Backend base URL (default: http://127.0.0.1:8000)
#                The script will POST to: {api}/admin/ingest
#
#   --state       Path to ingest_state.json (default: data/processed/ingest_state.json)
#                The script reads seen_ids from this file, and appends new doc_ids only
#                AFTER a successful ingest batch.
#
#   --timeout     Fetch timeout (seconds) for sitemap fetch + HTML fetch (default: 20)
#                Use higher values if sites are slow / you see frequent read timeouts.
#
#   --workers     Number of concurrent fetch threads (default: 12)
#                Throughput depends on per-host limiter; raising workers too much may not help.
#
#   --batch-size  Number of accepted documents to buffer before ingesting (default: 50)
#                Larger batches reduce ingest API overhead; smaller batches reduce memory usage.
#
#   --scale       Multiplier applied to each site's quota (default: 1.0)
#                Example: 0.5 means each site target is half its configured quota.
#
#   --max-total   Stop after preparing this many total documents across all sites (default: 0 = unlimited)
#                The script will flush the current batch before stopping.
#
#   --only-site   Only run a single site by name, e.g. govuk / nasa / techcrunch (default: "")
#                Example:
#                  python -m app.ingest.backfill_sitemap.backfill_sitemap_new --only-site govuk --scale 0.2
#
#   --dry-run     Do everything except POST to ingest API and writing seen_ids to state.
#                Useful for validating sitemap parsing + URL filtering safely.
def main():
    global_prepared = 0

    p = argparse.ArgumentParser(description="One-off sitemap backfill (round-robin + per-host limiter + penalty box)")
    p.add_argument("--api", default="http://127.0.0.1:8000")
    p.add_argument("--state", default=str(PROCESSED_DIR / "ingest_state.json"))
    p.add_argument("--timeout", type=int, default=20)
    p.add_argument("--batch-size", type=int, default=50)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--scale", type=float, default=1.0, help="scale quotas, e.g. 0.01 -> ~1k total")
    p.add_argument("--max-total", type=int, default=0, help="Stop after preparing this many docs total (0=unlimited)")
    p.add_argument("--only-site", type=str, default="", help="Only run one site by name, e.g. govuk")
    p.add_argument("--log-jsonl", type=str, default="", help="Write prepared doc_id/url/title to this jsonl for audit")

    # NEW: concurrency
    p.add_argument("--workers", type=int, default=12, help="Concurrent fetch workers (thread pool)")

    args = p.parse_args()

    state_path = Path(args.state)
    print("DEBUG state_path resolved =", state_path.resolve())

    batch_size = max(1, int(args.batch_size))
    timeout_s = int(args.timeout)
    workers = max(1, int(args.workers))

    # load initial state
    state = IngestState.load(state_path)

    # Concurrency notes:
    # - seen_ids_local: snapshot of ingest_state.json seen_ids (plus newly flushed ids after reload)
    # - pending_ids: doc_ids currently scheduled/in-flight (to avoid duplicate work across threads)
    # Both are protected by seen_lock
    seen_ids_local = set(state.seen_ids)
    pending_ids: set[str] = set()
    seen_lock = threading.Lock()

    total_prepared = 0
    total_ingested = 0

    # build runtimes (respect only-site and scale)
    runtimes: List[SiteRuntime] = []
    for cfg in SITES:
        if args.only_site and cfg.name != args.only_site:
            continue
        quota = max(0, int(cfg.quota * float(args.scale)))
        if quota <= 0:
            continue

        rt = _build_site_runtime(cfg, quota=quota, timeout_s=timeout_s)
        runtimes.append(rt)
        print(f"[sitemap] site={cfg.name} quota={quota} sitemaps={cfg.sitemaps}", flush=True)

    if not runtimes:
        print("[sitemap] No sites selected (quota=0 or only-site mismatch).", flush=True)
        return

    # per-host limiter shared by workers
    limiter = HostLimiter()

    kept: List[Document] = []

    # debug limiter: print first N debug lines from worker stage logs
    debug_lock = threading.Lock()
    debug_counter_ref = [0]  # mutable int
    debug_first_n = 20

    # map site_name -> runtime for easy updates
    rt_by_name: Dict[str, SiteRuntime] = {rt.cfg.name: rt for rt in runtimes}

    # active pool (dynamic)
    active = list(runtimes)
    rr_idx = 0

    # submit loop structures
    inflight: Dict[Future, FetchJob] = {}

    # debug counters
    debug_counters = defaultdict(lambda: defaultdict(int))  # site -> reason -> count


    def _next_job_round_robin() -> Optional[FetchJob]:
        """
        Round-robin over ACTIVE sites (dynamic pool),
        but scan multiple URLs per site per turn to avoid "all sites filtered -> None" early stop.
        Thread-safe via seen_lock for seen/pending.
        """
        nonlocal rr_idx, active

        if not active:
            return None

        # tune: how deep to scan within the same site before moving to next site
        PER_SITE_SCAN = 20  # 100~500 都行；大一点更不容易被 seen/filters 卡住
        # tune: overall cap per call, prevents infinite loop if everything is exhausted/seen
        MAX_TOTAL_SCAN = max(20000, len(active) * PER_SITE_SCAN * 200)

        def _dbg(site: str, reason: str) -> None:
            # count not print
            debug_counters[site][reason] += 1

        def _remove_active_at(i: int):
            """Remove active[i] and keep rr_idx stable."""
            nonlocal rr_idx, active
            active.pop(i)
            if active:
                rr_idx %= len(active)
            else:
                rr_idx = 0

        scanned = 0

        # We try at most len(active) "turns" per call, but each turn scans PER_SITE_SCAN urls from that site.
        # while active and turns < len(active) and scanned < MAX_TOTAL_SCAN:
        while active and scanned < MAX_TOTAL_SCAN:
            i = rr_idx % len(active)
            rt = active[i]
            rr_idx += 1

            site = rt.cfg.name

            # remove finished site ASAP
            if rt.done or rt.scheduled >= rt.quota:
                rt.done = True
                _dbg(site, "done_or_quota")
                _remove_active_at(i)
                continue

            # scan multiple urls within this site
            for _ in range(PER_SITE_SCAN):
                if scanned >= MAX_TOTAL_SCAN:
                    break
                scanned += 1

                url = rt.next_url()
                if not url:
                    rt.done = True
                    _dbg(site, "no_next_url")
                    _remove_active_at(i)
                    break  # stop scanning this site

                # TechCrunch newsletters skip（do not count against quota）
                if site == "techcrunch":
                    pth = (urlparse(url).path or "")
                    if "/newsletters/" in pth:
                        _dbg(site, "skip_newsletters")
                        continue

                if not is_article_url(site, url):
                    _dbg(site, "filtered_by_is_article_url")
                    continue

                parsed = urlparse(url)
                url_clean = parsed._replace(query="", fragment="").geturl()
                doc_id = make_global_doc_id(url_clean)

                with seen_lock:
                    if (doc_id in seen_ids_local) or (doc_id in pending_ids):
                        _dbg(site, "seen_or_pending")
                        continue
                    pending_ids.add(doc_id)

                rt.scheduled += 1
                return FetchJob(site_name=site, url=url, doc_id=doc_id)

        return None


    def _mark_job_done(job: FetchJob) -> None:
        with seen_lock:
            pending_ids.discard(job.doc_id)

    def _accept_doc(res: FetchResult) -> None:
        nonlocal global_prepared, total_prepared

        rt = rt_by_name[res.job.site_name]

        doc = Document(
            doc_id=res.job.doc_id,
            title=res.title,
            body=res.body,
            url=canonical_url(res.job.url),
            timestamp=None,
            lang="en",
        )
        kept.append(doc)

        rt.prepared += 1
        global_prepared += 1

        _append_jsonl(args.log_jsonl, {"site": rt.cfg.name, "doc_id": doc.doc_id, "url": doc.url, "title": doc.title})

        if global_prepared % 50 == 0:
            print(f"[sitemap] PREPARED total={global_prepared}", flush=True)

        if args.dry_run and rt.prepared <= 5:
            print(f"  + {doc.doc_id} | {doc.title[:80]}", flush=True)

        total_prepared += 1


    idle_spins = 0
    IDLE_MAX = 20

    # main concurrent fetch loop
    with ThreadPoolExecutor(max_workers=workers) as ex:
        # keep pipeline full but bounded
        max_inflight = max(workers * 2, 8)

        while True:
            # stop condition: max-total reached (but flush kept first)
            if args.max_total and global_prepared >= args.max_total:
                rt_any = next(iter(rt_by_name.values()))
                rt_any.ingested, total_ingested, state = flush_kept(
                    kept=kept,
                    args=args,
                    state_path=state_path,
                    state=state,
                    ingested_site=rt_any.ingested,
                    total_ingested=total_ingested,
                    reason="max_total_reached",
                )
                print(f"[sitemap] STOP max_total reached. prepared={global_prepared} state={state_path}", flush=True)
                break

            # submit more jobs if possible
            while len(inflight) < max_inflight:
                job = _next_job_round_robin()
                if job is None:
                    break

                fut = ex.submit(
                    _fetch_worker,
                    job,
                    timeout_s=timeout_s,
                    limiter=limiter,
                    debug_first_n=debug_first_n,
                    debug_counter_ref=debug_counter_ref,
                    debug_lock=debug_lock,
                )
                inflight[fut] = job

            # if no inflight and cannot submit => all done
            if not inflight:
                # if a task is still active but not yet completed, try a few times to prevent premature termination
                if active and idle_spins < IDLE_MAX:
                    idle_spins += 1
                    continue
                break
            else:
                idle_spins = 0

            done, _ = wait(inflight.keys(), return_when=FIRST_COMPLETED)
            for fut in done:
                job = inflight.pop(fut)
                try:
                    res: FetchResult = fut.result()
                except Exception as e:
                    # should not happen; treat as failure + release pending
                    _mark_job_done(job)
                    with debug_lock:
                        if debug_counter_ref[0] < debug_first_n:
                            debug_counter_ref[0] += 1
                            print(f"[DEBUG-skip] site={job.site_name} stage=worker_crash url={job.url} err={e!r}", flush=True)
                    continue

                # release pending id now that job finished
                _mark_job_done(res.job)

                if not res.ok:
                    # rate_limited needs to be rolled back as well, otherwise it will waste quota
                    rt = rt_by_name[res.job.site_name]
                    rt.scheduled = max(0, rt.scheduled - 1)

                    # check if the limiter is consuming and releasing data
                    debug_counters[res.job.site_name][f"skip_{res.skip_reason or 'unknown'}"] += 1

                    continue

                # accept
                _accept_doc(res)

                # if we reached per-site quota, mark runtime done
                rt = rt_by_name[res.job.site_name]
                rt.sitemap_worked = True

                if rt.prepared >= rt.quota:
                    rt.done = True


            # We used a "pass" above to avoid double-ingest; flush will happen below,
            # but only when kept >= batch_size (checked again here in main loop)
            if len(kept) >= batch_size:
                # Use a dummy ingested_site counter (0) – total_ingested remains accurate.
                dummy_ingested_site = 0
                dummy_ingested_site, total_ingested, state = flush_kept(
                    kept=kept,
                    args=args,
                    state_path=state_path,
                    state=state,
                    ingested_site=dummy_ingested_site,
                    total_ingested=total_ingested,
                    reason="batch_full",
                )

                # After successful flush, add those ids into local seen set
                # flush_kept has already cleared kept, so we can't read from kept now.
                # Solution: update_state_seen_ids writes to file; reload state to update local set.
                try:
                    latest = IngestState.load(state_path)
                    with seen_lock:
                        seen_ids_local = set(latest.seen_ids)
                except Exception:
                    # best-effort: keep existing seen_ids_local
                    pass

    # after loop: flush remaining
    if kept:
        dummy_ingested_site = 0
        dummy_ingested_site, total_ingested, state = flush_kept(
            kept=kept,
            args=args,
            state_path=state_path,
            state=state,
            ingested_site=dummy_ingested_site,
            total_ingested=total_ingested,
            reason="all_done",
        )
        try:
            latest = IngestState.load(state_path)
            with seen_lock:
                seen_ids_local = set(latest.seen_ids)
        except Exception:
            pass

    # site warnings (keep the spirit of your logs)
    for rt in runtimes:
        if rt.prepared == 0:
            print(f"[sitemap] site={rt.cfg.name} WARN no usable sitemap (all failed or empty)", flush=True)
        elif rt.prepared == 0:
            print(f"[sitemap] site={rt.cfg.name} INFO sitemap ok but 0 passed filters", flush=True)
        print(f"[sitemap] site={rt.cfg.name} DONE prepared={rt.prepared}", flush=True)


    # Code for Debugging (Summary after the data fill)
    print("\n[DBG-summary] drop reasons per site:", flush=True)
    for site, reasons in debug_counters.items():
        items = sorted(reasons.items(), key=lambda x: -x[1])
        top = " ".join([f"{k}={v}" for k, v in items[:8]])
        print(f"[DBG-summary] {site}: {top}", flush=True)
    print("", flush=True)

    state = update_state(state_path, touch_last_run=True)
    print(f"[sitemap] ALL DONE prepared={total_prepared} ingested={total_ingested} state={state_path}", flush=True)


if __name__ == "__main__":
    main()