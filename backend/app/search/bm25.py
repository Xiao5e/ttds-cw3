from __future__ import annotations
from typing import Dict, List, Tuple, Optional, Set
import math

from ..storage.index_store import IndexStore


def bm25_scores(query_terms: List[str],
                index: IndexStore,
                target_docs: Optional[Set[str]] = None,
                k1: float = 1.2,
                b: float = 0.75) -> Dict[str, float]:
    """
        计算BM25分数

        Args:
            query_terms: 查询词项列表
            index: 索引存储
            target_docs: 可选，要计算分数的文档ID集合。如果为None，则计算所有文档
            k1: BM25参数k1
            b: BM25参数b

        Returns:
            文档ID到分数的映射。如果指定了target_docs，则只返回target_docs中的文档分数
    """
    if not query_terms:
        return {}

    # Compute BM25 over in-memory postings
    N = max(1, len(index.doc_len))
    avgdl = sum(index.doc_len.values()) / N

    # Precompute document frequencies
    term_idf: Dict[str, float] = {}
    seen_terms = set(query_terms)

    for term in seen_terms:
        postings = index.postings.get(term, [])
        df = len(postings)
        if df == 0:
            continue
        # IDF with +1 to avoid negative for very frequent terms in tiny demo
        idf = math.log(1 + (N - df + 0.5) / (df + 0.5))
        term_idf[term] = idf

        # 如果没有指定目标文档，则计算所有文档
    if target_docs is None:
        scores: Dict[str, float] = {}

        for term, idf in term_idf.items():
            for doc_id, tf in index.postings.get(term, []):
                dl = index.doc_len.get(doc_id, 0) or 1
                denom = tf + k1 * (1 - b + b * (dl / avgdl))
                score = idf * (tf * (k1 + 1) / denom)
                scores[doc_id] = scores.get(doc_id, 0.0) + score

        return scores

    # 如果指定了目标文档，只计算这些文档的分数
    else:
        if not target_docs:
            return {}

        scores: Dict[str, float] = {}

        # 为了优化性能，先为每个词项建立文档到TF的映射
        term_doc_tf: Dict[str, Dict[str, int]] = {}
        for term, idf in term_idf.items():
            doc_tf_map = {}
            for doc_id, tf in index.postings.get(term, []):
                if doc_id in target_docs:
                    doc_tf_map[doc_id] = tf
            if doc_tf_map:
                term_doc_tf[term] = doc_tf_map

        # 计算每个目标文档的分数
        for doc_id in target_docs:
            score = 0.0
            dl = index.doc_len.get(doc_id, 0) or 1

            for term, idf in term_idf.items():
                # 检查该文档是否包含该词项
                if term in term_doc_tf and doc_id in term_doc_tf[term]:
                    tf = term_doc_tf[term][doc_id]
                    denom = tf + k1 * (1 - b + b * (dl / avgdl))
                    score += idf * (tf * (k1 + 1) / denom)

            if score > 0:  # 只记录有分数的文档
                scores[doc_id] = score

        return scores


def bm25_score_single_doc(query_terms: List[str], index: IndexStore,
                          doc_id: str, k1: float = 1.2, b: float = 0.75) -> float:
    """
    计算单个文档的BM25分数

    Args:
        query_terms: 查询词项列表
        index: 索引存储
        doc_id: 文档ID
        k1: BM25参数k1
        b: BM25参数b

    Returns:
        文档的BM25分数
    """
    N = max(1, len(index.doc_len))
    avgdl = sum(index.doc_len.values()) / N

    score = 0.0
    seen_terms = set(query_terms)
    dl = index.doc_len.get(doc_id, 0) or 1

    for term in seen_terms:
        postings = index.postings.get(term, [])
        if not postings:
            continue

        # 检查该文档是否包含该词项
        tf = 0
        for pid, term_tf in postings:
            if pid == doc_id:
                tf = term_tf
                break

        if tf > 0:
            df = len(postings)
            idf = math.log(1 + (N - df + 0.5) / (df + 0.5))
            denom = tf + k1 * (1 - b + b * (dl / avgdl))
            score += idf * (tf * (k1 + 1) / denom)

    return score
