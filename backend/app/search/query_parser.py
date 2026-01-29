from __future__ import annotations
from dataclasses import dataclass
from typing import List, Literal, Optional, Tuple
import re

from ..indexing.tokenizer import tokenize_en


@dataclass
class ParsedQuery:
    raw: str
    terms: List[str]

    phrase: Optional[str] = None
    boolean_expr: Optional[str] = None
    mode: Literal["free", "phrase", "boolean"] = "free"


_PHRASE_RE = re.compile(r'"([^"]+)"')
_BOOL_RE = re.compile(r'\b(AND|OR|NOT)\b')


def parse_query(q: str) -> ParsedQuery:
    # 清理输入
    q = (q or "").strip()

    # 尝试匹配短语查询
    m = _PHRASE_RE.search(q)
    if m:
        # 提取短语内容
        phrase = m.group(1)
        # 对短语进行分词
        terms = tokenize_en(phrase)
        # 返回短语查询对象
        return ParsedQuery(raw=q, terms=terms, phrase=phrase, mode="phrase")

    # Boolean query
    if _BOOL_RE.search(q):
        return ParsedQuery(
            raw=q,
            terms=tokenize_en(q),
            mode="boolean",
            boolean_expr=q
        )

    # 3. 自由查询（默认）
    return ParsedQuery(raw=q, terms=tokenize_en(q), mode="free")