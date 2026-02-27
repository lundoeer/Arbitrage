#!/usr/bin/env python3
"""
Convert resolved_15m_pairs.log into CSV.
"""

from __future__ import annotations

import argparse
import csv
import re
from pathlib import Path
from typing import Dict, List

BASE_PATTERN = re.compile(
    r"^(?P<window_end>[^|]+)\s+\|\s+"
    r"kalshi=(?P<kalshi_ticker>[^:]+):(?P<kalshi_raw>[^()]+)\((?P<kalshi_norm>[^()]*)\)\s+\|\s+"
    r"polymarket=(?P<pm_slug>[^:]+):(?P<pm_raw>[^()]+)\((?P<pm_norm>[^()]*)\)\s+\|\s+"
    r"(?P<comparison>[^|]+)"
    r"(?P<extras>.*)$"
)


def _parse_extras(raw_extras: str) -> Dict[str, str]:
    extras: Dict[str, str] = {}
    text = raw_extras.strip()
    if not text:
        return extras
    for part in text.split("|"):
        part = part.strip()
        if not part or "=" not in part:
            continue
        key, value = part.split("=", 1)
        extras[key.strip()] = value.strip()
    return extras


def run(*, input_log: Path, output_csv: Path) -> Dict[str, int]:
    if not input_log.exists():
        raise FileNotFoundError(f"Input log does not exist: {input_log}")

    rows: List[Dict[str, str]] = []
    extras_order: List[str] = []
    extras_seen: set[str] = set()
    skipped = 0

    for raw_line in input_log.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        match = BASE_PATTERN.match(line)
        if not match:
            skipped += 1
            continue

        extras = _parse_extras(match.group("extras") or "")
        for key in extras.keys():
            if key in extras_seen:
                continue
            extras_seen.add(key)
            extras_order.append(key)

        row = {
            "window_end": match.group("window_end").strip(),
            "kalshi_ticker": match.group("kalshi_ticker").strip(),
            "kalshi_resolution_raw": match.group("kalshi_raw").strip(),
            "kalshi_resolution_normalized": match.group("kalshi_norm").strip(),
            "polymarket_slug": match.group("pm_slug").strip(),
            "polymarket_resolution_raw": match.group("pm_raw").strip(),
            "polymarket_resolution_normalized": match.group("pm_norm").strip(),
            "comparison": match.group("comparison").strip(),
        }
        row.update(extras)
        rows.append(row)

    base_columns = [
        "window_end",
        "kalshi_ticker",
        "kalshi_resolution_raw",
        "kalshi_resolution_normalized",
        "polymarket_slug",
        "polymarket_resolution_raw",
        "polymarket_resolution_normalized",
        "comparison",
    ]
    preferred_extras = [
        "kalshi_target",
        "kalshi_end",
        "polymarket_target",
        "polymarket_end",
    ]
    ordered_extras: List[str] = []
    for key in preferred_extras:
        if key in extras_seen:
            ordered_extras.append(key)
    for key in extras_order:
        if key not in ordered_extras:
            ordered_extras.append(key)

    columns = base_columns + ordered_extras

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    with open(output_csv, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    return {
        "rows_written": len(rows),
        "rows_skipped": skipped,
        "column_count": len(columns),
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Convert resolved_15m_pairs.log to CSV.")
    parser.add_argument("--input-log", default="data/diagnostic/resolved_15m_pairs.log")
    parser.add_argument("--output-csv", default="data/diagnostic/resolved_15m_pairs.csv")
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    summary = run(input_log=Path(args.input_log), output_csv=Path(args.output_csv))
    print("Log to CSV complete")
    print(f"Rows written: {summary['rows_written']}")
    print(f"Rows skipped: {summary['rows_skipped']}")
    print(f"Columns: {summary['column_count']}")
    print(f"Output CSV: {args.output_csv}")


if __name__ == "__main__":
    main()
