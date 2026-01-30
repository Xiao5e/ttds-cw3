from __future__ import annotations
from dataclasses import dataclass, asdict
from pathlib import Path
import json
from typing import Set, Optional

@dataclass
class IngestState:
    seen_ids: Set[str]
    last_run_iso: Optional[str] = None  # 预留字段，方便未来扩展

    @staticmethod
    def load(path: Path) -> "IngestState":
        if not path.exists():
            return IngestState(seen_ids=set(), last_run_iso=None)
        data = json.loads(path.read_text(encoding="utf-8"))
        return IngestState(
            seen_ids=set(data.get("seen_ids", [])),
            last_run_iso=data.get("last_run_iso"),
        )

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        data = asdict(self)
        data["seen_ids"] = sorted(list(self.seen_ids))
        path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
