import re
from typing import List

_WORD_RE = re.compile(r"[A-Za-z0-9]+")

def tokenize_en(text: str) -> List[str]:
    # Minimal tokenizer: lowercase alnum words
    return [m.group(0).lower() for m in _WORD_RE.finditer(text or "")]
