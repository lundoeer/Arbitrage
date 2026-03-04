from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict


class JsonlWriter:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = open(self.path, "a", encoding="utf-8")

    def write(self, payload: Dict[str, Any]) -> None:
        self._fh.write(json.dumps(payload, ensure_ascii=True) + "\n")
        self._fh.flush()

    def close(self) -> None:
        self._fh.close()


class NullWriter:
    def write(self, payload: Dict[str, Any]) -> None:  # noqa: ARG002
        return

    def close(self) -> None:
        return
