# from __future__ import annotations
# from dataclasses import dataclass, asdict
# from pathlib import Path
# import json
# from typing import Set, Optional

# @dataclass
# class IngestState:
#     seen_ids: Set[str]
#     last_run_iso: Optional[str] = None  # 预留字段，方便未来扩展

#     @staticmethod
#     def load(path: Path) -> "IngestState":
#         if not path.exists():
#             return IngestState(seen_ids=set(), last_run_iso=None)
#         data = json.loads(path.read_text(encoding="utf-8"))
#         return IngestState(
#             seen_ids=set(data.get("seen_ids", [])),
#             last_run_iso=data.get("last_run_iso"),
#         )

#     def save(self, path: Path) -> None:
#         path.parent.mkdir(parents=True, exist_ok=True)
#         data = asdict(self)
#         data["seen_ids"] = sorted(list(self.seen_ids))
#         path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")



from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
import json
from typing import Set, Optional, Dict, Any

# 每个 RSS 源的调度状态
@dataclass
class FeedState:
    next_run_iso: Optional[str] = None
    last_checked_iso: Optional[str] = None
    fail_count: int = 0
    etag: Optional[str] = None
    last_modified: Optional[str] = None

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "FeedState":
        return FeedState(
            next_run_iso=d.get("next_run_iso"),
            last_checked_iso=d.get("last_checked_iso"),
            fail_count=int(d.get("fail_count", 0)),
            etag=d.get("etag"),
            last_modified=d.get("last_modified"),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "next_run_iso": self.next_run_iso,
            "last_checked_iso": self.last_checked_iso,
            "fail_count": self.fail_count,
            "etag": self.etag,
            "last_modified": self.last_modified,
        }


@dataclass
class IngestState:
    seen_ids: Set[str]
    last_run_iso: Optional[str] = None
    feeds: Dict[str, FeedState] = None  # source_id -> FeedState

    @staticmethod
    def load(path: Path) -> "IngestState":
        if not path.exists():
            return IngestState(seen_ids=set(), last_run_iso=None, feeds={})

        data = json.loads(path.read_text(encoding="utf-8"))

        feeds_raw = data.get("feeds", {}) or {}
        feeds = {
            k: FeedState.from_dict(v)
            for k, v in feeds_raw.items()
        }

        return IngestState(
            seen_ids=set(data.get("seen_ids", [])),
            last_run_iso=data.get("last_run_iso"),
            feeds=feeds,
        )

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)

        payload = {
            "seen_ids": sorted(list(self.seen_ids)),
            "last_run_iso": self.last_run_iso,
            "feeds": {
                k: v.to_dict()
                for k, v in (self.feeds or {}).items()
            },
        }

        path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8"
        )

