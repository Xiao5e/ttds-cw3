'''
Test fetching RSS feed from Medium
'''

import requests
import feedparser
from datetime import timezone
from dateutil import parser as dtparser
from bs4 import BeautifulSoup
import hashlib

from app.schemas import Document

FEED_URL = "https://medium.com/feed/swlh"

def entry_to_document(entry) -> Document:
    # get document by doc_id
    guid = getattr(entry, "id", None) or getattr(entry, "guid", None)
    link = getattr(entry, "link", "")
    base = guid or link
    doc_id = "md-" + hashlib.sha1(base.encode("utf-8")).hexdigest()[:16]

    # fetch title
    title = getattr(entry, "title", "").strip()

    # fetch body（in Medium, usually at 'summary' or 'content'）
    # 3) body（Medium 通常在 summary 或 content）
    body_html = ""
    if hasattr(entry, "content") and entry.content:
        body_html = entry.content[0].value
    else:
        body_html = getattr(entry, "summary", "")

    # 👉 清洗 HTML → 纯文本
    soup = BeautifulSoup(body_html, "html.parser")
    body = soup.get_text(separator=" ", strip=True)


    # timestamp → ISO 8601
    raw_time = getattr(entry, "published", None)
    timestamp = None
    if raw_time:
        dt = dtparser.parse(raw_time)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        timestamp = dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

    # build Document（following schemas.py）
    return Document(
        doc_id=doc_id,
        title=title,
        body=body,
        url=link,
        timestamp=timestamp,
        lang="en",
    )

def main():
    print(f"Fetching RSS from: {FEED_URL}")
    resp = requests.get(
        FEED_URL,
        timeout=15,
        headers={"User-Agent": "ttds-cw3-test/1.0"}
    )
    resp.raise_for_status()

    feed = feedparser.parse(resp.text)
    print(f"Total entries fetched: {len(feed.entries)}")
    print("=" * 60)

    # fetch all
    # entries = feed.entries[:5]
    entries = feed.entries
    docs = [entry_to_document(entry) for entry in entries]
    print(f"Will ingest {len(docs)} docs from this RSS fetch.")

    print("Documents generated from RSS entry:")
    print("=" * 60)
    print([doc.model_dump() for doc in docs])
    print("=" * 60)

    # ===== Step 2.4：ingest 这一篇 =====
    print("Ingesting this document via /admin/ingest ...")
    ingest_payload = {
        "docs": [doc.model_dump() for doc in docs]
    }


    resp = requests.post(
        "http://localhost:8000/admin/ingest",
        json=ingest_payload,
        timeout=15
    )
    resp.raise_for_status()

    print("Ingest response:")
    print(resp.json())

if __name__ == "__main__":
    main()

