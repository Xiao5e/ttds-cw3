from __future__ import annotations
from typing import List, Optional, Dict
import datetime
import re

from ..schemas import SearchRequest, SearchResponse, SearchResult
from ..storage.document_store import DocumentStore
from ..storage.index_store import IndexStore
from ..indexing.tokenizer import tokenize_en
from .query_parser import parse_query
from .bm25 import bm25_scores
from ..utils.timing import timer_ms
from ..utils.logging import get_logger

logger = get_logger(__name__)


# 生成结果摘要
def _make_snippet(text: str, terms: List[str], max_len: int = 180) -> str:
    """生成结果摘要，基于查询词"""
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


# 过滤文档
def _filter_doc(doc, filters) -> bool:
    if not filters:
        return True
    if filters.lang and doc.lang != filters.lang:
        return False

    # time filter (best-effort ISO compare)
    if (filters.time_from or filters.time_to) and doc.timestamp:
        try:
            ts = datetime.datetime.fromisoformat(doc.timestamp.replace("Z", "+00:00"))
            if filters.time_from:
                t0 = datetime.datetime.fromisoformat(filters.time_from.replace("Z", "+00:00"))
                if ts < t0: return False
            if filters.time_to:
                t1 = datetime.datetime.fromisoformat(filters.time_to.replace("Z", "+00:00"))
                if ts > t1: return False
        except Exception:
            pass

    return True


def _extract_query_terms(query: str) -> List[str]:
    """
    从查询字符串中提取词项用于BM25计算

    对于简单查询：直接分词
    对于复杂查询：提取所有词项，忽略操作符
    """
    # 如果是简单查询（不包含布尔操作符），直接分词
    if not any(op in query.upper() for op in [" AND ", " OR ", " NOT ", "#", "("]):
        return tokenize_en(query)

    # 对于复杂查询，提取所有词项
    terms = []

    # 1. 提取短语中的词项
    phrase_matches = re.findall(r'"([^"]+)"', query)
    for phrase in phrase_matches:
        terms.extend(tokenize_en(phrase))

    # 2. 提取邻近查询中的词项
    proximity_matches = re.findall(r'#\d+\(([^,]+),([^)]+)\)', query)
    for term1, term2 in proximity_matches:
        terms.extend(tokenize_en(term1.strip()))
        terms.extend(tokenize_en(term2.strip()))

    # 3. 提取普通词项（排除操作符）
    # 先移除已处理的短语和邻近查询
    cleaned_query = query
    for phrase in phrase_matches:
        cleaned_query = cleaned_query.replace(f'"{phrase}"', '')
    for term1, term2 in proximity_matches:
        cleaned_query = cleaned_query.replace(f'#({term1},{term2})', '')

    # 移除布尔操作符
    cleaned_query = re.sub(r'\b(?:AND|OR|NOT)\b', ' ', cleaned_query, flags=re.IGNORECASE)
    # 移除括号
    cleaned_query = re.sub(r'[()]', ' ', cleaned_query)

    # 提取剩余词项
    for word in re.findall(r'\b\w+\b', cleaned_query.lower()):
        if word and word not in ['and', 'or', 'not']:
            terms.extend(tokenize_en(word))

    return list(set(terms))  # 去重


def _is_simple_query(query: str) -> bool:
    """判断是否为简单查询（不包含布尔操作符、短语、邻近查询）"""
    # 检查是否包含布尔操作符
    if any(op in query.upper() for op in [" AND ", " OR ", " NOT "]):
        return False

    # 检查是否包含邻近查询
    if '#' in query:
        return False

    # 检查是否包含短语查询
    if '"' in query:
        return False

    # 检查是否包含括号
    if '(' in query or ')' in query:
        return False

    return True


def _filter_scores_by_docs(scores: dict, target_docs: set) -> dict:
    """过滤分数，只保留目标文档的分数"""
    return {doc_id: score for doc_id, score in scores.items() if doc_id in target_docs}


# 搜索
def search(req: SearchRequest, store: DocumentStore, index: IndexStore, prf_expand=None) -> SearchResponse:
    """
        搜索函数 - 使用query_parser进行解析，使用bm25_scores进行评分

        参数：
            req: 搜索请求
            store: 文档存储
            index: 索引存储
            prf_expand: 伪相关反馈扩展函数

        返回：
            搜索响应
    """

    # BM25
    with timer_ms() as took:
        # 1. 使用parse_query执行查询解析，获取匹配的文档
        matched_docs = parse_query(req.query, index)

        # 2. 如果没有匹配的文档，直接返回空结果
        if not matched_docs:
            return SearchResponse(
                query=req.query,
                took_ms=took(),
                total_hits=0,
                results=[]
            )

        # 3. 提取查询词项用于BM25计算
        query_terms = _extract_query_terms(req.query)

        # 4. 只计算匹配文档的BM25分数
        scores = bm25_scores(query_terms, index, target_docs=matched_docs)

        # 5. PRF扩展（仅对简单查询启用）
        if req.use_prf and prf_expand is not None and _is_simple_query(req.query):
            # 获取前几个文档进行扩展
            top_docs = sorted(scores.items(), key=lambda x: x[1], reverse=True)[:5]

            if top_docs:
                # 扩展查询词
                expanded_terms = prf_expand(req.query, [doc_id for doc_id, _ in top_docs], store)
                if expanded_terms:
                    # 合并原始词项和扩展词项
                    all_terms = query_terms + expanded_terms

                    # 用扩展词重新计算分数（仍然只计算匹配文档）
                    expanded_scores = bm25_scores(all_terms, index, target_docs=matched_docs)

                    # 合并分数：取最大值
                    for doc_id, score in expanded_scores.items():
                        scores[doc_id] = max(scores.get(doc_id, 0.0), score)

        # 6. 排序结果
        ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)

        # 7. 过滤和格式化结果
        results: List[SearchResult] = []
        for doc_id, score in ranked:
            doc = store.get(doc_id)
            if not doc:
                continue

            if not _filter_doc(doc, req.filters):
                continue

            # 为摘要生成提取词项（使用原始查询）
            if _is_simple_query(req.query):
                snippet_terms = tokenize_en(req.query)
            else:
                snippet_terms = _extract_query_terms(req.query)

            snippet = _make_snippet(doc.body, snippet_terms)

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

    # 返回结果
    return SearchResponse(
        query=req.query,
        took_ms=took(),
        total_hits=len(ranked),
        results=results
    )

