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
    mode: Literal["free", "phrase"] = "free"

_PHRASE_RE = re.compile(r'"([^"]+)"')

def parse_query(q: str) -> ParsedQuery:
    q = (q or "").strip()
    m = _PHRASE_RE.search(q)
    if m:
        phrase = m.group(1)
        terms = tokenize_en(phrase)
        return ParsedQuery(raw=q, terms=terms, phrase=phrase, mode="phrase")
    # free query
    return ParsedQuery(raw=q, terms=tokenize_en(q), mode="free")
