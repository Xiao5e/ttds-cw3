"""Build index from persisted docs.
Usage:
  python scripts/build_index.py
"""
import sys
from pathlib import Path

# Allow running from repo root
sys.path.append(str(Path(__file__).resolve().parents[1] / "backend"))

from app.storage.document_store import DocumentStore
from app.storage.index_store import IndexStore
from app.indexing.indexer import build_index

def main():
    store = DocumentStore()
    store.load_if_exists()
    index = IndexStore()
    build_index(store.all(), index)
    print(f"Built index: version={index.index_version}, docs={len(store)}")

if __name__ == "__main__":
    main()
