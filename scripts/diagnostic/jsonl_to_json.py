#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, List


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Convert a JSONL file into a JSON array file."
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Input JSONL file path.",
    )
    parser.add_argument(
        "--output",
        default="",
        help="Output JSON path. Defaults to <input_stem>.json in the same folder.",
    )
    parser.add_argument(
        "--pretty",
        action="store_true",
        help="Pretty-print output JSON with indentation.",
    )
    parser.add_argument(
        "--skip-invalid-lines",
        action="store_true",
        help="Skip malformed JSONL lines instead of failing.",
    )
    return parser


def _default_output_path(input_path: Path) -> Path:
    return input_path.with_suffix(".json")


def _read_jsonl(input_path: Path, *, skip_invalid_lines: bool) -> tuple[List[Any], int]:
    items: List[Any] = []
    skipped = 0
    with input_path.open("r", encoding="utf-8") as src:
        for line_no, line in enumerate(src, start=1):
            raw = line.strip()
            if not raw:
                continue
            try:
                items.append(json.loads(raw))
            except Exception as exc:
                if skip_invalid_lines:
                    skipped += 1
                    continue
                raise ValueError(
                    f"Invalid JSON at line {line_no} in {input_path}: {exc}"
                ) from exc
    return items, skipped


def main() -> None:
    args = _build_parser().parse_args()
    input_path = Path(str(args.input).strip())
    if not input_path.exists():
        raise FileNotFoundError(f"Input file does not exist: {input_path}")
    if input_path.is_dir():
        raise IsADirectoryError(f"Input path is a directory, expected file: {input_path}")

    output_raw = str(args.output).strip()
    output_path = Path(output_raw) if output_raw else _default_output_path(input_path)
    if output_path.resolve() == input_path.resolve():
        raise RuntimeError("Output path must be different from input path.")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    items, skipped = _read_jsonl(
        input_path,
        skip_invalid_lines=bool(args.skip_invalid_lines),
    )
    with output_path.open("w", encoding="utf-8") as dst:
        if bool(args.pretty):
            json.dump(items, dst, ensure_ascii=True, indent=2)
            dst.write("\n")
        else:
            json.dump(items, dst, ensure_ascii=True)

    print(f"Wrote {len(items)} rows to {output_path}")
    if skipped > 0:
        print(f"Skipped {skipped} malformed rows")


if __name__ == "__main__":
    main()
