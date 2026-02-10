from __future__ import annotations
from typing import List, Optional
import hashlib
from datetime import timezone

import requests
import feedparser
from dateutil import parser as dtparser
from bs4 import BeautifulSoup

from ...schemas import Document

# def _stable_doc_id(guid: Optional[str], link: str) -> str:
#     base = guid or link
#     return "md-" + hashlib.sha1(base.encode("utf-8")).hexdigest()[:16]

def _stable_doc_id(prefix: str, guid: Optional[str], link: str) -> str:
    base = guid or link
    # prefix ensures global uniqueness across sources (hn/bbc/arxiv/...)
    return f"{prefix}-" + hashlib.sha1(base.encode("utf-8")).hexdigest()[:16]

def _to_iso8601(raw_time: Optional[str]) -> Optional[str]:
    if not raw_time:
        return None
    dt = dtparser.parse(raw_time)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

def _html_to_text(html: str) -> str:
    soup = BeautifulSoup(html or "", "html.parser")
    return soup.get_text(separator=" ", strip=True)

# def fetch_documents(feed_url: str, limit: int = 10, timeout_s: int = 15) -> List[Document]:
#     """
#     Fetch Medium RSS and map to List[Document].
#     """
#     resp = requests.get(feed_url, timeout=timeout_s, headers={"User-Agent": "ttds-cw3-ingest/1.0"})
#     resp.raise_for_status()
#     feed = feedparser.parse(resp.text)

#     docs: List[Document] = []
#     for entry in feed.entries[:limit]:
#         guid = getattr(entry, "id", None) or getattr(entry, "guid", None)
#         link = getattr(entry, "link", "")

#         title = (getattr(entry, "title", "") or "").strip()

#         body_html = ""
#         if hasattr(entry, "content") and entry.content:
#             body_html = entry.content[0].value or ""
#         else:
#             body_html = getattr(entry, "summary", "") or ""

#         body = _html_to_text(body_html)
#         timestamp = _to_iso8601(getattr(entry, "published", None))

#         docs.append(
#             Document(
#                 doc_id=_stable_doc_id(guid, link),
#                 title=title,
#                 body=body,
#                 url=link,
#                 timestamp=timestamp,
#                 lang="en",
#             )
#         )

#     return docs

def fetch_documents(
    feed_url: str,
    limit: int = 10,
    timeout_s: int = 15,
    doc_prefix: str = "md",
    lang: str = "en",
    title_prefix: str = "",
) -> List[Document]:
    """
    Fetch RSS and map to List[Document].

    This function is intentionally generic (works for Medium, arXiv, BBC, HN, TechCrunch, MIT, ...).
    """
    resp = requests.get(
        feed_url,
        timeout=timeout_s,
        headers={"User-Agent": "ttds-cw3-ingest/1.0"},
    )
    resp.raise_for_status()
    feed = feedparser.parse(resp.text)

    docs: List[Document] = []
    for entry in feed.entries[:limit]:
        guid = getattr(entry, "id", None) or getattr(entry, "guid", None)
        link = getattr(entry, "link", "") or ""

        raw_title = (getattr(entry, "title", "") or "").strip()
        title = f"{title_prefix} {raw_title}".strip() if title_prefix else raw_title

        body_html = ""
        if hasattr(entry, "content") and entry.content:
            body_html = entry.content[0].value or ""
        else:
            body_html = getattr(entry, "summary", "") or ""

        body = _html_to_text(body_html)

        # different feeds sometimes use published / updated
        timestamp = _to_iso8601(getattr(entry, "published", None) or getattr(entry, "updated", None))

        docs.append(
            Document(
                doc_id=_stable_doc_id(doc_prefix, guid, link),
                title=title,
                body=body,
                url=link,
                timestamp=timestamp,
                lang=lang,
            )
        )

    return docs
