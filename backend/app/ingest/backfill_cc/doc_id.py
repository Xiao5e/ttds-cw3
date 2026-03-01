from __future__ import annotations
import hashlib
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode


def normalize_url(url: str) -> str:
    """
    Normalize URL for stable hashing:
    - lowercase scheme + host
    - drop fragment
    - drop default ports
    - sort query params
    - remove trailing slash (except root)
    """
    url = (url or "").strip()
    if not url:
        return ""

    parts = urlsplit(url)
    scheme = (parts.scheme or "http").lower()
    netloc = (parts.netloc or "").lower()

    # remove default ports
    if scheme == "http" and netloc.endswith(":80"):
        netloc = netloc[:-3]
    if scheme == "https" and netloc.endswith(":443"):
        netloc = netloc[:-4]

    path = parts.path or "/"
    if path != "/" and path.endswith("/"):
        path = path[:-1]

    # sort query params
    query_pairs = parse_qsl(parts.query, keep_blank_values=True)
    query_pairs.sort()
    query = urlencode(query_pairs, doseq=True)

    # drop fragment
    fragment = ""

    return urlunsplit((scheme, netloc, path, query, fragment))


def make_doc_id(doc_prefix: str, url: str, n: int = 16) -> str:
    """
    doc_id = <prefix>-<sha1(normalized_url)[:n]>
    """
    base = normalize_url(url)
    h = hashlib.sha1(base.encode("utf-8")).hexdigest()[:n]
    return f"{doc_prefix}-{h}"


def make_global_doc_id(url: str, n: int = 16) -> str:
    """
    doc_id = u-<sha1(normalized_url)[:n]>
    """
    base = normalize_url(url)
    h = hashlib.sha1(base.encode("utf-8")).hexdigest()[:n]
    return f"u-{h}"
