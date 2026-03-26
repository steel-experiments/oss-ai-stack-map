from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any


class CacheStore:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)

    def get_json(self, namespace: str, key: str) -> Any | None:
        path = self._path(namespace, key)
        if not path.exists():
            return None
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)

    def set_json(self, namespace: str, key: str, payload: Any) -> None:
        path = self._path(namespace, key)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle)

    def _path(self, namespace: str, key: str) -> Path:
        digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
        return self.root / namespace / f"{digest}.json"
