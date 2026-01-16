from __future__ import annotations
from typing import List, Optional
import datetime

from ..schemas import SearchRequest, SearchResponse, SearchResult
from ..storage.document_store import DocumentStore
from ..storage.index_store import IndexStore
from ..indexing.tokenizer import tokenize_en
from .query_parser import parse_query
from .bm25 import bm25_scores
from ..utils.timing import timer_ms
from ..utils.logging import get_logger

logger = get_logger(__name__)

def _make_snippet(text: str, terms: List[str], max_len: int = 180) -> str:
    if not text:
        return ""
    lower = text.lower()
    pos = -1
    for t in terms:
        p = lower.find(t.lower())
        if p != -1:
            pos = p
            break
    if pos == -1:
        snippet = text[:max_len]
    else:
        start = max(0, pos - 60)
        snippet = text[start:start + max_len]
    return snippet.replace("\n", " ").strip()

def _filter_doc(doc, filters) -> bool:
    if not filters:
        return True
    if filters.lang and doc.lang != filters.lang:
        return False
    # time filter (best-effort ISO compare)
    if (filters.time_from or filters.time_to) and doc.timestamp:
        try:
            ts = datetime.datetime.fromisoformat(doc.timestamp.replace("Z","+00:00"))
            if filters.time_from:
                t0 = datetime.datetime.fromisoformat(filters.time_from.replace("Z","+00:00"))
                if ts < t0: return False
            if filters.time_to:
                t1 = datetime.datetime.fromisoformat(filters.time_to.replace("Z","+00:00"))
                if ts > t1: return False
        except Exception:
            pass
    return True

def search(req: SearchRequest, store: DocumentStore, index: IndexStore, prf_expand=None) -> SearchResponse:
    parsed = parse_query(req.query)
    q_terms = parsed.terms

    with timer_ms() as took:
        scores = bm25_scores(q_terms, index)
        # Optional PRF hook: prf_expand(query, top_docs)->expanded_terms
        if req.use_prf and prf_expand is not None:
            top_docs = sorted(scores.items(), key=lambda x: x[1], reverse=True)[:5]
            expanded_terms = prf_expand(req.query, [doc_id for doc_id,_ in top_docs], store)
            if expanded_terms:
                scores2 = bm25_scores(q_terms + expanded_terms, index)
                # simple blend
                for d, s in scores2.items():
                    scores[d] = max(scores.get(d, 0.0), s)

        ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)

        results: List[SearchResult] = []
        for doc_id, score in ranked:
            doc = store.get(doc_id)
            if not doc:
                continue
            if not _filter_doc(doc, req.filters):
                continue
            field_text = doc.body
            snippet = _make_snippet(field_text, q_terms)
            results.append(SearchResult(
                doc_id=doc.doc_id,
                title=doc.title,
                snippet=snippet,
                score=float(score),
                url=doc.url,
                timestamp=doc.timestamp,
                lang=doc.lang
            ))
            if len(results) >= max(1, req.top_k):
                break

    return SearchResponse(
        query=req.query,
        took_ms=took(),
        total_hits=len(ranked),
        results=results
    )
