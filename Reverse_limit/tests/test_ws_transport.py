from __future__ import annotations

import json
from pathlib import Path

from scripts.common.ws_transport import JsonlWriter


def test_jsonl_writer_appends_json_lines(tmp_path: Path) -> None:
    path = tmp_path / "sample.jsonl"
    writer = JsonlWriter(path)
    writer.write({"kind": "first", "value": 1})
    writer.write({"kind": "second", "value": 2})
    writer.close()

    lines = path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 2
    first = json.loads(lines[0])
    second = json.loads(lines[1])
    assert first == {"kind": "first", "value": 1}
    assert second == {"kind": "second", "value": 2}

