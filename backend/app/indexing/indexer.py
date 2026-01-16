from __future__ import annotations
from typing import Iterable, List, Optional
from collections import Counter

from ..schemas import Document
from .tokenizer import tokenize_en
from ..storage.index_store import IndexStore
from ..utils.logging import get_logger

logger = get_logger(__name__)

def _doc_terms(doc: Document) -> List[str]:
    # For demo: index title + body
    return tokenize_en((doc.title or "") + " " + (doc.body or ""))

def build_index(docs: Iterable[Document], index: IndexStore) -> str:
    """Full build."""
    index.postings.clear()
    index.doc_len.clear()

    for doc in docs:
        terms = _doc_terms(doc)
        index.doc_len[doc.doc_id] = len(terms)
        tf = Counter(terms)
        for term, freq in tf.items():
            index.postings.setdefault(term, []).append((doc.doc_id, freq))

    index.bump_version()
    index.save()
    logger.info(f"Built index: docs={len(index.doc_len)}, terms={len(index.postings)}, version={index.index_version}")
    return index.index_version

def update_index(new_docs: Iterable[Document], index: IndexStore) -> str:
    """Incremental update: append new docs' postings."""
    added = 0
    for doc in new_docs:
        if doc.doc_id in index.doc_len:
            continue
        terms = _doc_terms(doc)
        index.doc_len[doc.doc_id] = len(terms)
        tf = Counter(terms)
        for term, freq in tf.items():
            index.postings.setdefault(term, []).append((doc.doc_id, freq))
        added += 1

    if added > 0:
        index.bump_version()
        index.save()
    logger.info(f"Updated index: added={added}, version={index.index_version}")
    return index.index_version
