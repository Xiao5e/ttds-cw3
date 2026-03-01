# 递归读 sitemap index，但“达到 quota 就停”
"""
sitemap_client.py

Recursive sitemap reader with early-stop support (quota-aware).

Purpose
-------
Provides a generator `iter_urls_from_sitemap()` that:

1) Downloads a sitemap (URL or sitemap index)
2) Parses XML (supports both <urlset> and <sitemapindex>)
3) Recursively follows nested sitemaps
4) Yields individual URL entries (SitemapItem)
5) Stops early when `max_urls` is reached


This is designed for batch backfill use cases (before initialization), not high-frequency scheduler polling.
"""
# backend/app/ingest/backfill_sitemap/sitemap_client.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterator, Optional
import gzip
import time
import json
import requests
from xml.etree import ElementTree as ET

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


@dataclass(frozen=True)
class SitemapItem:
    """
    Represents one URL entry from a sitemap.

    loc:
        The absolute URL of the page.
        Example:
            https://example.com/articles/ai-overview

    lastmod:
        Optional last modification timestamp from the sitemap XML.
        Examples:
            "2025-02-25T14:30:00+00:00"
            "2024-12-01"
    """
    loc: str
    lastmod: Optional[str] = None


def _session() -> requests.Session:
    """
    Create a shared HTTP session with:

    - Retry logic for transient errors
    - Connection pooling for efficiency
    - Backoff to avoid hammering servers

    Retry is triggered for:
    - 429 (Too Many Requests)
    - 5xx server errors
    - Connection and read errors
    """
    s = requests.Session()
    retry = Retry(
        total=4,
        connect=4,
        read=4,
        backoff_factor=0.8,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "HEAD"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s


_HTTP = _session()

DEBUG_SITEMAP = False # for testing

def _get_bytes(url: str, timeout_s: int) -> Optional[bytes]:
    """
    Fetch raw bytes from a sitemap URL.

    Uses:
    - Separate connect/read timeout (connect=5s, read=timeout_s)
    - Retry via session
    - Custom User-Agent to avoid blocking

    Returns:
        Raw bytes if successful,
        None if request fails or status is not usable.
    """
    try:
        r = _HTTP.get(
            url,
            timeout=(5, timeout_s),  # (connect_timeout, read_timeout)
            headers={
                "User-Agent": "ttds-cw3-sitemap-backfill/1.0",
                "Accept": "application/xml,text/xml,*/*",
            },
            allow_redirects=True,
        )
    except (requests.exceptions.ReadTimeout,
            requests.exceptions.ConnectTimeout,
            requests.exceptions.ConnectionError):
        return None
    
    if DEBUG_SITEMAP:
        ct = r.headers.get("Content-Type")
        print(f"[sitemap_http] url={url} status={r.status_code} final={r.url} ct={ct}", flush=True)
        # 打印前 80 bytes 看看像不像 XML
        print(f"[sitemap_http] head={r.content[:80]!r}", flush=True)

    # 这些码你之前直接吞了；测试阶段建议保留日志
    # Explicitly ignore certain status codes (404/403/451/406/429)
    # These are common for blocked or missing sitemap entries.
    if r.status_code in (403, 404, 406, 429, 451):
        return None
    if r.status_code >= 400:
        return None

    # Accessing r.content may trigger full response download.
    # We assume timeout settings already protect us from hanging.
    try:
        return r.content
    except requests.exceptions.RequestException:
        return None


def _maybe_gunzip(raw: bytes) -> bytes:
    """
    Decompress gzip-compressed sitemap content.

    Instead of relying on `.gz` filename suffix,
    we inspect the first two bytes for gzip magic header (0x1F, 0x8B).

    If decompression fails, return original raw bytes.
    """
    if len(raw) >= 2 and raw[0] == 0x1F and raw[1] == 0x8B:
        try:
            return gzip.decompress(raw)
        except Exception:
            return raw
    return raw


def _localname(tag: str) -> str:
    """
    Strip XML namespace and lowercase.
    '{ns}urlset' -> 'urlset'
    """
    return tag.split("}", 1)[-1].lower() if tag else ""


def iter_urls_from_sitemap(
    sitemap_url: str,
    *,
    timeout_s: int = 20,
    max_urls: int = 100000,
    _depth: int = 0,
    _max_depth: int = 5,
) -> Iterator[SitemapItem]:
    """
    Recursively iterate over URLs in a sitemap.

    Supports:
    - <urlset> (direct list of URLs)
    - <sitemapindex> (list of nested sitemaps)
    - gzip-compressed content

    Parameters
    ----------
    sitemap_url: Entry URL of the sitemap.

    timeout_s: Read timeout for HTTP requests.

    max_urls: Maximum number of URLs to yield (early stop).

    _depth / _max_depth: Internal recursion control to prevent infinite nesting.

    Yields
    ------
    SitemapItem objects (loc + lastmod).

    Early termination: Stops yielding once `max_urls` is reached.
    """
    # Prevent runaway recursion (e.g., circular references)
    if _depth > _max_depth:
        return

    raw = _get_bytes(sitemap_url, timeout_s=timeout_s)
    if raw is None:
        # print(f"[DEBUG] sitemap={sitemap_url} _get_bytes=None")
        return

    xml_bytes = _maybe_gunzip(raw)

    try:
        root = ET.fromstring(xml_bytes)
    except Exception as e:
        print(f"[DEBUG] sitemap={sitemap_url} parse_error={e!r}")
        print(f"[DEBUG] first_200_bytes={xml_bytes[:200]!r}")
        return

    root_name = _localname(root.tag)
    # print(f"[DEBUG] sitemap={sitemap_url} root_tag={root.tag!r} local={root_name!r}")
    if DEBUG_SITEMAP:
        print(f"[DEBUG] [sitemap_xml] url={sitemap_url} root={root_name}", flush=True)

    count = 0

    # -----------------------------------------------------------------
    # Case 1: sitemap index (nested sitemaps)
    if root_name == "sitemapindex":
        # <sitemap><loc>child</loc></sitemap>
        for sm in root.findall(".//{*}sitemap"):
            loc_el = sm.find("{*}loc")
            loc = (loc_el.text or "").strip() if loc_el is not None and loc_el.text else ""
            if not loc:
                continue

            # stop shortly, avoid being seen as an attack
            time.sleep(0.05)

            for item in iter_urls_from_sitemap(
                loc,
                timeout_s=timeout_s,
                max_urls=max_urls,
                _depth=_depth + 1,
                _max_depth=_max_depth,
            ):
                yield item
                count += 1
                if count >= max_urls:
                    return

    # -----------------------------------------------------------------
    # Case 2: regular urlset
    elif root_name == "urlset":
        for u in root.findall(".//{*}url"):
            loc_el = u.find("{*}loc")
            loc = (loc_el.text or "").strip() if loc_el is not None and loc_el.text else ""
            if not loc:
                continue

            # print(f"[DEBUG-url] sitemap={sitemap_url} loc={loc}")

            last_el = u.find("{*}lastmod")
            lastmod = (last_el.text or "").strip() if last_el is not None and last_el.text else None

            yield SitemapItem(loc=loc, lastmod=lastmod)
            count += 1
            if count >= max_urls:
                return

    # -----------------------------------------------------------------
    # Unknown root tag (ignore)
    else:
        return
