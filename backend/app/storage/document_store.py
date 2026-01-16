from __future__ import annotations
from typing import Dict, Iterable, List, Optional
from pathlib import Path
import json

from ..schemas import Document
from ..config import PROCESSED_DIR

class DocumentStore:
    """A tiny document store for the demo.
    - Keeps docs in memory
    - Also appends to data/processed/docs.jsonl for reproducibility
    """

    def __init__(self, persist_path: Optional[Path] = None):
        self.docs: Dict[str, Document] = {}
        self.persist_path = persist_path or (PROCESSED_DIR / "docs.jsonl")
        self.persist_path.parent.mkdir(parents=True, exist_ok=True)

    def load_if_exists(self) -> None:
        if not self.persist_path.exists():
            return
        with self.persist_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                doc = Document.model_validate_json(line)
                self.docs[doc.doc_id] = doc

    def add_documents(self, docs: Iterable[Document], persist: bool = True) -> int:
        count = 0
        lines: List[str] = []
        for doc in docs:
            if doc.doc_id in self.docs:
                continue
            self.docs[doc.doc_id] = doc
            count += 1
            if persist:
                lines.append(doc.model_dump_json())
        if persist and lines:
            with self.persist_path.open("a", encoding="utf-8") as f:
                for ln in lines:
                    f.write(ln + "\n")
        return count

    def get(self, doc_id: str) -> Optional[Document]:
        return self.docs.get(doc_id)

    def all(self) -> List[Document]:
        return list(self.docs.values())

    def __len__(self) -> int:
        return len(self.docs)
