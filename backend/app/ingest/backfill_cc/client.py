from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Dict, Any
import json
import requests


from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def _session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=0.8,                 # 0.8s, 1.6s, 3.2s...
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "HEAD"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s

_HTTP = _session()


@dataclass
class CCRecord:
    url: str
    timestamp: str
    status: int
    mime: str


def _cdx_base(index_name: str) -> str:
    return f"https://index.commoncrawl.org/{index_name}-index"


def _parse_jsonl_records(text: str, limit: int) -> List[CCRecord]:
    records: List[CCRecord] = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        obj = json.loads(line)
        records.append(
            CCRecord(
                url=obj.get("url", "") or "",
                timestamp=obj.get("timestamp", "") or "",
                status=int(obj.get("status", 0) or 0),
                mime=obj.get("mime", "") or "",
            )
        )
        if len(records) >= limit:
            break
    return records


def _get_jsonl_or_empty(base: str, params: Dict[str, Any], timeout_s: int) -> List[CCRecord]:
    """
    Common Crawl Index often returns 404 for 'no results'.
    Treat 404 as empty list (NOT an exception).
    """
    # r = requests.get(base, params=params, timeout=timeout_s)
    r = _HTTP.get(base, params=params, timeout=timeout_s)

    if r.status_code == 404:
        # "no captures found" (common behavior)
        return []

    r.raise_for_status()
    # Some endpoints might return empty body even with 200; handle gracefully
    if not (r.text or "").strip():
        return []
    # caller will parse with limit
    return _parse_jsonl_records(r.text, int(params.get("limit", "200")))


def get_latest_cc_index_candidates(timeout_s: int = 15, max_candidates: int = 3) -> List[str]:
    """
    Return newest N CC index IDs.
    Used for automatic fallback when one index fails.
    """
    try:
        r = _HTTP.get("https://index.commoncrawl.org/collinfo.json", timeout=timeout_s)
        r.raise_for_status()
        data = r.json()
        if isinstance(data, list):
            ids = [item.get("id") for item in data if item.get("id")]
            return ids[:max_candidates]
    except Exception:
        pass

    return ["CC-MAIN-2025-49"]



def _get_jsonl_or_empty_with_fallback(
    index_candidates: List[str],
    build_params_fn,
    timeout_s: int,
) -> List[CCRecord]:

    for idx in index_candidates:
        base = _cdx_base(idx)
        params = build_params_fn(idx)

        try:
            r = _HTTP.get(base, params=params, timeout=timeout_s)

            # 404: maybe no captures OR bad index
            if r.status_code == 404:
                continue

            r.raise_for_status()

            if not (r.text or "").strip():
                return []

            return _parse_jsonl_records(r.text, int(params.get("limit", "200")))

        except requests.RequestException:
            # try next index
            continue

    # all indexes failed
    return []



def query_cdx_domain(
    domain: str,
    index_name: str,
    limit: int,
    from_ts: Optional[str] = None,
    to_ts: Optional[str] = None,
    timeout_s: int = 25,
) -> List[CCRecord]:
    """
    Query Common Crawl CDX Index for a domain.

    Key fixes:
    - Use matchType=domain (more standard / robust than 'domain/*' without matchType)
    - Treat 404 as empty results (no crash)
    """
    candidates = [index_name] + get_latest_cc_index_candidates()

    def _params_builder(idx):
        return {
            "url": domain,
            "matchType": "domain",
            "output": "json",
            "filter": [
                "status:200",
                "mime:text/html",
            ],
            "limit": str(limit),
            "collapse": "urlkey",
            **({"from": from_ts} if from_ts else {}),
            **({"to": to_ts} if to_ts else {}),
        }

    return _get_jsonl_or_empty_with_fallback(
        candidates,
        _params_builder,
        timeout_s,
    )


# def query_cdx_prefix(
#     prefix: str,
#     index_name: str,
#     limit: int = 200,
#     timeout_s: int = 25,
# ) -> List[CCRecord]:
#     """
#     Broad sampling / discovery query using matchType=prefix.

#     Important fix:
#     - 404 should be treated as empty (normal when a random prefix hits nothing).
#     """
#     base = _cdx_base(index_name)

#     params: Dict[str, Any] = {
#         "url": prefix,
#         "matchType": "prefix",
#         "output": "json",
#         "filter": [
#             "status:200",
#             "mime:text/html",
#         ],
#         "limit": str(limit),
#         "collapse": "urlkey",
#     }

#     return _get_jsonl_or_empty(base, params, timeout_s)



def query_cdx_prefix(prefix: str, index_name: str, limit: int = 200, timeout_s: int = 25) -> List[CCRecord]:
    candidates = [index_name] + get_latest_cc_index_candidates()

    def _params_builder(idx):
        return {
            "url": prefix,
            "matchType": "prefix",
            "output": "json",
            "filter": ["status:200", "mime:text/html"],
            "limit": str(limit),
            "collapse": "urlkey",
        }

    return _get_jsonl_or_empty_with_fallback(candidates, _params_builder, timeout_s)



def query_cdx_wildcard_tld(tld: str, index_name: str, limit: int = 200, timeout_s: int = 25) -> List[CCRecord]:
    candidates = [index_name] + get_latest_cc_index_candidates()
    tld = (tld or "").strip().lstrip(".")

    def _params_builder(idx):
        return {
            "url": f"*.{tld}/*",
            "output": "json",
            "filter": ["status:200", "mime:text/html"],
            "limit": str(limit),
            "collapse": "urlkey",
        }

    return _get_jsonl_or_empty_with_fallback(candidates, _params_builder, timeout_s)



def fetch_live_html(url: str, timeout_s: int = 15, max_bytes: int = 2_000_000) -> str:
    """
    Fetch page HTML from live web.
    Cheap + simple for demo backfill.

    Note: Some sites block bots; backfill script should catch errors & skip.
    """
    r = _HTTP.get(
        url,
        timeout=timeout_s,
        headers={
            "User-Agent": "ttds-cw3-cc-backfill/1.0",
            "Accept": "text/html,application/xhtml+xml",
        },
        allow_redirects=True,
    )

    # 403/451 等：直接跳过
    if r.status_code in (401, 403, 406, 429, 451):
        raise RuntimeError(f"blocked status={r.status_code}")

    r.raise_for_status()

    ctype = (r.headers.get("Content-Type") or "").lower()
    if "text/html" not in ctype and "application/xhtml" not in ctype:
        raise RuntimeError(f"not_html content-type={ctype}")

    # 控制最大响应体（防止爆内存/慢）
    content = r.content
    if len(content) > max_bytes:
        content = content[:max_bytes]

    return content.decode(r.encoding or "utf-8", errors="ignore")