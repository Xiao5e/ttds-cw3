from __future__ import annotations
from typing import Dict, List, Tuple, Optional
from pathlib import Path
import json
import time

from ..config import INDEX_DIR

Postings = Dict[str, List[Tuple[str, int]]]  # term -> [(doc_id, tf)]
DocLen = Dict[str, int]

class IndexStore:
    """Demo index store:
    - Keeps inverted index + doc lengths in memory
    - Can save/load to data/index/index.json for persistence
    """

    def __init__(self, index_path: Optional[Path] = None):
        self.index_path = index_path or (INDEX_DIR / "index.json")
        self.index_path.parent.mkdir(parents=True, exist_ok=True)

        self.postings: Postings = {}
        self.doc_len: DocLen = {}
        self.index_version: str = "dev-0"

    def save(self) -> None:
        payload = {
            "index_version": self.index_version,
            "doc_len": self.doc_len,
            "postings": {t: v for t, v in self.postings.items()},
        }
        with self.index_path.open("w", encoding="utf-8") as f:
            json.dump(payload, f)

    def load_if_exists(self) -> None:
        if not self.index_path.exists():
            return
        with self.index_path.open("r", encoding="utf-8") as f:
            payload = json.load(f)
        self.index_version = payload.get("index_version", "dev-0")
        self.doc_len = {k: int(v) for k, v in payload.get("doc_len", {}).items()}
        self.postings = {t: [(doc, int(tf)) for doc, tf in lst] for t, lst in payload.get("postings", {}).items()}

    def bump_version(self) -> None:
        self.index_version = f"dev-{int(time.time())}"
