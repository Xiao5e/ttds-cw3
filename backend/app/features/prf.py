from __future__ import annotations
from typing import List
from collections import Counter

from ..storage.document_store import DocumentStore
from ..indexing.tokenizer import tokenize_en

STOP = set(["the","a","an","and","or","to","of","in","on","for","with","is","are","was","were","be","as","by"])

def expand_query(query: str, top_doc_ids: List[str], store: DocumentStore, topn: int = 5) -> List[str]:
    q_terms = set(tokenize_en(query))
    counter = Counter()
    for doc_id in top_doc_ids:
        doc = store.get(doc_id)
        if not doc:
            continue
        terms = tokenize_en(doc.title + " " + doc.body)
        for t in terms:
            if t in STOP or t in q_terms or len(t) <= 2:
                continue
            counter[t] += 1
    return [t for t,_ in counter.most_common(topn)]
