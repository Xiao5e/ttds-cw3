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
    """
    从文档文本中生成摘要片段，突出显示查询词项。

    参数:
        text: 原始文档文本
        terms: 查询词项列表
        max_len: 摘要片段的最大长度（默认180字符）

    返回:
        摘要字符串，包含查询词项上下文
    """
    if not text:
        return ""

    # 转换为小写以便搜索
    lower = text.lower()
    pos = -1

    # 查找第一个出现的查询词项位置
    for t in terms:
        p = lower.find(t.lower())
        if p != -1:
            pos = p
            break

    # 根据是否找到查询词项生成摘要
    if pos == -1:
        # 未找到查询词项，返回文本开头部分
        snippet = text[:max_len]
    else:
        # 以查询词项为中心，向前后扩展
        start = max(0, pos - 60)  # 向前取60字符作为上下文
        snippet = text[start:start + max_len]

    # 清理格式：移除换行符，去除首尾空格
    return snippet.replace("\n", " ").strip()


def _filter_doc(doc, filters) -> bool:
    """
    根据过滤条件检查文档是否符合要求。

    参数:
        doc: 文档对象
        filters: 过滤条件对象

    返回:
        bool: 文档是否通过所有过滤条件
    """
    if not filters:
        return True

    # 语言过滤
    if filters.lang and doc.lang != filters.lang:
        return False

    # 时间范围过滤（尽力解析ISO格式时间）
    if (filters.time_from or filters.time_to) and doc.timestamp:
        try:
            # 解析文档时间戳（处理时区格式）
            ts = datetime.datetime.fromisoformat(doc.timestamp.replace("Z", "+00:00"))

            # 检查起始时间过滤
            if filters.time_from:
                t0 = datetime.datetime.fromisoformat(filters.time_from.replace("Z", "+00:00"))
                if ts < t0:
                    return False

            # 检查结束时间过滤
            if filters.time_to:
                t1 = datetime.datetime.fromisoformat(filters.time_to.replace("Z", "+00:00"))
                if ts > t1:
                    return False
        except Exception:
            # 时间解析失败时跳过时间过滤（降级处理）
            pass

    return True


# 短语匹配
def phrase_match(text: str, phrase: str) -> bool:
    return phrase.lower() in text.lower()


# 布尔匹配
def eval_boolean(expr: str, index: IndexStore) -> set[str]:
    tokens = expr.split()
    result = None
    op = None

    for tok in tokens:
        if tok in {"AND", "OR"}:
            op = tok
        elif tok == "NOT":
            op = "NOT"
        else:
            docs = set(doc_id for doc_id, _ in index.postings.get(tok, []))

            if result is None:
                result = docs
            elif op == "AND":
                result &= docs
            elif op == "OR":
                result |= docs
            elif op == "NOT":
                result -= docs

    return result or set()


# 候选文档
def _candidate_docs(parsed, index: IndexStore) -> set[str]:
    # Boolean 查询
    if parsed.boolean:
        return eval_boolean(parsed.boolean, index)

    # 普通关键词：取 postings union
    cand = set()
    for t in parsed.terms:
        cand |= {doc_id for doc_id, _ in index.postings.get(t, [])}
    return cand


def search(req: SearchRequest, store: DocumentStore, index: IndexStore, prf_expand=None) -> SearchResponse:
    """
    主搜索函数：执行完整搜索流程，包括查询解析、检索、排序、过滤和结果构建。

    参数:
        req: 搜索请求对象，包含查询字符串、过滤条件等
        store: 文档存储对象，用于获取文档元数据和内容
        index: 倒排索引存储对象，用于快速检索
        prf_expand: 可选函数，用于伪相关反馈查询扩展

    返回:
        SearchResponse: 搜索结果响应对象

    流程:
        1. 解析查询字符串
        2. 计算BM25相关性分数
        3. 可选执行伪相关反馈扩展
        4. 排序和过滤文档
        5. 构建结果摘要
        6. 封装搜索结果

    示例:
        >>> request = SearchRequest(query="artificial intelligence", top_k=10)
        >>> response = search(request, document_store, index_store)
    """
    # 步骤1: 解析查询
    parsed = parse_query(req.query)
    q_terms = parsed.terms  # 查询词项列表

    # 使用计时器记录搜索耗时
    with timer_ms() as took:
        # 获取候选文档
        candidates = _candidate_docs(parsed, index)
        # 步骤2: 基础BM25检索
        scores = bm25_scores(q_terms, index)

        # 步骤3: 伪相关反馈扩展（可选）
        if req.use_prf and prf_expand is not None:
            # 获取前5个最相关文档
            top_docs = sorted(scores.items(), key=lambda x: x[1], reverse=True)[:5]

            # 扩展查询词项
            expanded_terms = prf_expand(req.query, [doc_id for doc_id, _ in top_docs], store)

            if expanded_terms:
                # 使用扩展后的查询重新计算分数
                scores2 = bm25_scores(q_terms + expanded_terms, index)

                # 简单混合策略：取两次分数的最大值
                for d, s in scores2.items():
                    scores[d] = max(scores.get(d, 0.0), s)

        # 步骤4: 按分数降序排序
        ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)

        # 步骤5: 构建搜索结果
        results: List[SearchResult] = []

        for doc_id, score in ranked:
            # 获取文档详情
            doc = store.get(doc_id)
            if not doc:
                continue

            # 应用过滤条件
            if not _filter_doc(doc, req.filters):
                continue

            if parsed.phrase and not phrase_match(doc.body, parsed.phrase):
                continue

            # 生成摘要片段
            field_text = doc.body
            snippet = _make_snippet(field_text, q_terms)
            
            # 生成结果
            results.append(SearchResult(
                doc_id=doc.doc_id,
                title=doc.title,
                snippet=snippet,
                score=float(score),
                url=doc.url,
                timestamp=doc.timestamp,
                lang=doc.lang
            ))

            # 达到请求的返回数量时停止
            if len(results) >= max(1, req.top_k):
                break

    # 步骤6: 封装最终响应
    return SearchResponse(
        query=req.query,  # 原始查询字符串
        took_ms=took(),  # 搜索耗时（毫秒）
        total_hits=len(ranked),  # 符合条件的所有文档数量
        results=results  # 实际返回的搜索结果列表
    )