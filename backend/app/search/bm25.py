from __future__ import annotations
from typing import Dict, List, Tuple
import math

from ..storage.index_store import IndexStore

def bm25_scores(query_terms: List[str], index: IndexStore, k1: float = 1.2, b: float = 0.75) -> Dict[str, float]:
    # Compute BM25 over in-memory postings
    N = max(1, len(index.doc_len))
    avgdl = sum(index.doc_len.values()) / N

    # Precompute document frequencies
    scores: Dict[str, float] = {}
    seen_terms = set(query_terms)

    for term in seen_terms:
        postings = index.postings.get(term, [])
        df = len(postings)
        if df == 0:
            continue
        # IDF with +1 to avoid negative for very frequent terms in tiny demo
        idf = math.log(1 + (N - df + 0.5) / (df + 0.5))
        for doc_id, tf in postings:
            dl = index.doc_len.get(doc_id, 0) or 1
            denom = tf + k1 * (1 - b + b * (dl / avgdl))
            score = idf * (tf * (k1 + 1) / denom)
            scores[doc_id] = scores.get(doc_id, 0.0) + score

    return scores
