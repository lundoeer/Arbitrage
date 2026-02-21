#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, Optional

from scripts.common.utils import as_dict as _as_dict, as_float as _as_float


def _extract_gross_edge(payload: Dict[str, Any], *, root_key: str, branch_key: str) -> Optional[float]:
    root = _as_dict(payload.get(root_key))
    branch = _as_dict(root.get(branch_key))
    return _as_float(branch.get("gross_edge"))


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Extract a compact 6-field JSONL view from gross edge snapshot JSONL."
    )
    parser.add_argument(
        "--input",
        default="data/gross_edge_snapshot__20260220T032702Z.jsonl",
        help="Input gross edge snapshot JSONL path.",
    )
    parser.add_argument(
        "--output",
        default="",
        help="Output JSONL path. Defaults to <input_stem>__six_fields.jsonl in same folder.",
    )
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    input_path = Path(args.input)
    if not input_path.exists():
        raise FileNotFoundError(f"Input file does not exist: {input_path}")

    if str(args.output).strip():
        output_path = Path(args.output)
    else:
        output_path = input_path.with_name(f"{input_path.stem}__six_fields.jsonl")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    written = 0
    skipped = 0
    with input_path.open("r", encoding="utf-8") as src, output_path.open("w", encoding="utf-8") as dst:
        for line in src:
            raw = line.strip()
            if not raw:
                continue
            try:
                payload = json.loads(raw)
            except Exception:
                skipped += 1
                continue

            marketpair = _as_dict(payload.get("marketpair"))
            polymarket_market_id = str(marketpair.get("polymarket_market_id") or "").strip()
            kalshi_ticker = str(marketpair.get("kalshi_ticker") or "").strip()
            marketpair_compact = f"{polymarket_market_id}-{kalshi_ticker}" if (polymarket_market_id or kalshi_ticker) else ""

            age_value = payload.get("marketpair_age_in_seconds")
            marketpair_age_in_seconds = int(age_value) if isinstance(age_value, (int, float)) else None

            out_row = {
                "polymarket_market_id-kalshi_ticker": marketpair_compact,
                "marketpair_age_in_seconds": marketpair_age_in_seconds,
                "buy_polymarket_yes_and_kalshi_no_gross_edge": _extract_gross_edge(
                    payload,
                    root_key="gross_edge_thresholds",
                    branch_key="buy_polymarket_yes_and_kalshi_no",
                ),
                "buy_kalshi_yes_and_polymarket_no_gross_edge": _extract_gross_edge(
                    payload,
                    root_key="gross_edge_thresholds",
                    branch_key="buy_kalshi_yes_and_polymarket_no",
                ),
                "buy_polymarket_yes_and_kalshi_no_gross_edge_bids": _extract_gross_edge(
                    payload,
                    root_key="gross_edge_thresholds_bids",
                    branch_key="buy_polymarket_yes_and_kalshi_no",
                ),
                "buy_kalshi_yes_and_polymarket_no_gross_edge_bids": _extract_gross_edge(
                    payload,
                    root_key="gross_edge_thresholds_bids",
                    branch_key="buy_kalshi_yes_and_polymarket_no",
                ),
            }
            dst.write(json.dumps(out_row, ensure_ascii=True) + "\n")
            written += 1

    print(f"Wrote {written} rows to {output_path}")
    if skipped > 0:
        print(f"Skipped {skipped} malformed rows")


if __name__ == "__main__":
    main()
