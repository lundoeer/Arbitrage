#!/usr/bin/env python3
"""
Build a consolidated buy-decision dataset with ask/size + gross edge features.

Inputs:
- data/buy_decision_log__*.jsonl
- _deprecated/data/buy_decision_log__*.jsonl
- data/diagnostic/resolved_15m_pairs.log

Output:
- data/diagnostic/buy_decision_ask_edge_with_resolved.csv
- data/diagnostic/buy_decision_ask_edge_with_resolved_summary.json
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.diagnostic.compare_resolved_15m_pairs import _read_pair_log


BUY_CANDIDATE_A = "buy_kalshi_yes_and_polymarket_no"
BUY_CANDIDATE_B = "buy_polymarket_yes_and_kalshi_no"


def _parse_iso_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    raw = str(value).strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _safe_float(value: Any) -> Optional[float]:
    try:
        return float(value)
    except Exception:
        return None


def _safe_int(value: Any) -> Optional[int]:
    try:
        return int(value)
    except Exception:
        return None


def _find_candidate_gross_edge(payload: Dict[str, Any], candidate_name: str) -> Dict[str, Optional[float]]:
    # Preferred location.
    candidates = payload.get("buy_signal", {}).get("candidates", [])
    if isinstance(candidates, list):
        for item in candidates:
            if not isinstance(item, dict):
                continue
            if str(item.get("name", "")).strip() == candidate_name:
                return {
                    "gross_edge": _safe_float(item.get("gross_edge")),
                    "total_ask": _safe_float(item.get("total_ask")),
                    "yes_ask": _safe_float(item.get("yes_ask")),
                    "no_ask": _safe_float(item.get("no_ask")),
                }

    # Fallback location.
    candidates = payload.get("quote_sanity", {}).get("canonical", {}).get("cross_venue_buy_candidates", [])
    if isinstance(candidates, list):
        for item in candidates:
            if not isinstance(item, dict):
                continue
            if str(item.get("name", "")).strip() == candidate_name:
                return {
                    "gross_edge": _safe_float(item.get("gross_edge")),
                    "total_ask": _safe_float(item.get("total_ask")),
                    "yes_ask": _safe_float(item.get("yes_ask")),
                    "no_ask": _safe_float(item.get("no_ask")),
                }

    return {"gross_edge": None, "total_ask": None, "yes_ask": None, "no_ask": None}


def _load_resolved_index(pair_log_path: Path) -> Dict[str, Any]:
    rows = _read_pair_log(pair_log_path)

    by_epoch_and_ticker: Dict[Tuple[int, str], Dict[str, Any]] = {}
    by_epoch: Dict[int, List[Dict[str, Any]]] = {}
    seen: set[Tuple[str, str, str]] = set()

    for row in rows:
        window_end_raw = str(row.get("window_end", "")).strip()
        kalshi_ticker = str(row.get("kalshi_ticker", "")).strip()
        polymarket_slug = str(row.get("polymarket_slug", "")).strip()
        if not window_end_raw:
            continue

        dedupe_key = (window_end_raw, kalshi_ticker, polymarket_slug)
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)

        dt = _parse_iso_datetime(window_end_raw)
        if dt is None:
            continue
        epoch_ms = int(round(dt.timestamp() * 1000))

        outcome_text = (
            f"kalshi={kalshi_ticker}:{row.get('kalshi_resolution_raw')}({row.get('kalshi_resolution_normalized')}) | "
            f"polymarket={polymarket_slug}:{row.get('polymarket_resolution_raw')}({row.get('polymarket_resolution_normalized')})"
        )
        indexed = {
            "window_end": window_end_raw,
            "epoch_ms": epoch_ms,
            "kalshi_ticker": kalshi_ticker,
            "polymarket_slug": polymarket_slug,
            "comparison": str(row.get("comparison", "")).strip(),
            "market_outcome": outcome_text,
        }

        if kalshi_ticker:
            by_epoch_and_ticker[(epoch_ms, kalshi_ticker)] = indexed
        by_epoch.setdefault(epoch_ms, []).append(indexed)

    return {
        "by_epoch_and_ticker": by_epoch_and_ticker,
        "by_epoch": by_epoch,
        "rows_indexed": len(seen),
    }


def _match_resolved(
    *,
    resolved_index: Dict[str, Any],
    market_window_end_epoch_ms: Optional[int],
    kalshi_ticker: str,
) -> Tuple[Optional[Dict[str, Any]], str]:
    if market_window_end_epoch_ms is None:
        return None, "none"

    by_epoch_and_ticker = resolved_index["by_epoch_and_ticker"]
    by_epoch = resolved_index["by_epoch"]
    key = (market_window_end_epoch_ms, kalshi_ticker)

    if kalshi_ticker and key in by_epoch_and_ticker:
        return by_epoch_and_ticker[key], "epoch+ticker"

    candidates = by_epoch.get(market_window_end_epoch_ms, [])
    if len(candidates) == 1:
        return candidates[0], "epoch_unique"
    if len(candidates) > 1:
        return None, "epoch_ambiguous"
    return None, "none"


def _iter_buy_decision_files() -> Iterable[Tuple[str, Path]]:
    roots = (
        ("data", PROJECT_ROOT / "data"),
        ("deprecated", PROJECT_ROOT / "_deprecated" / "data"),
    )
    for source_group, root in roots:
        for path in sorted(root.glob("buy_decision_log__*.jsonl")):
            yield source_group, path


def run(
    *,
    pair_log_path: Path,
    output_csv_path: Path,
    output_summary_path: Path,
) -> Dict[str, Any]:
    resolved_index = _load_resolved_index(pair_log_path)

    output_csv_path.parent.mkdir(parents=True, exist_ok=True)
    columns = [
        "source_group",
        "source_file",
        "line_number",
        "ts",
        "recv_ms",
        "market_window_end_epoch_ms",
        "market_window_end",
        "polymarket_event_slug",
        "polymarket_market_id",
        "polymarket_condition_id",
        "polymarket_token_yes",
        "polymarket_token_no",
        "kalshi_ticker",
        "polymarket_yes_best_ask_price",
        "polymarket_yes_best_ask_size",
        "polymarket_no_best_ask_price",
        "polymarket_no_best_ask_size",
        "kalshi_yes_best_ask_price",
        "kalshi_yes_best_ask_size",
        "kalshi_no_best_ask_price",
        "kalshi_no_best_ask_size",
        "gross_edge_buy_kalshi_yes_and_polymarket_no",
        "gross_edge_buy_polymarket_yes_and_kalshi_no",
        "total_ask_buy_kalshi_yes_and_polymarket_no",
        "total_ask_buy_polymarket_yes_and_kalshi_no",
        "resolved_match_type",
        "resolved_comparison",
        "resolved_market_outcome",
        "resolved_window_end",
        "resolved_polymarket_slug",
    ]

    files_scanned = 0
    rows_written = 0
    rows_skipped_non_buy_decision = 0
    rows_json_decode_error = 0
    rows_missing_market_end = 0
    match_counts: Dict[str, int] = {"epoch+ticker": 0, "epoch_unique": 0, "epoch_ambiguous": 0, "none": 0}

    with open(output_csv_path, "w", encoding="utf-8", newline="") as out_f:
        writer = csv.DictWriter(out_f, fieldnames=columns)
        writer.writeheader()

        for source_group, path in _iter_buy_decision_files():
            files_scanned += 1
            with open(path, "r", encoding="utf-8") as in_f:
                for line_number, raw_line in enumerate(in_f, start=1):
                    line = raw_line.strip()
                    if not line:
                        continue

                    try:
                        payload = json.loads(line)
                    except json.JSONDecodeError:
                        rows_json_decode_error += 1
                        continue

                    if str(payload.get("kind", "")).strip() != "buy_decision":
                        rows_skipped_non_buy_decision += 1
                        continue

                    market = payload.get("market", {})
                    quote_legs = payload.get("quote_sanity", {}).get("legs", {})

                    market_window_end_epoch_ms = _safe_int(market.get("market_window_end_epoch_ms"))
                    if market_window_end_epoch_ms is None:
                        rows_missing_market_end += 1

                    market_window_end = None
                    if market_window_end_epoch_ms is not None:
                        market_window_end = datetime.fromtimestamp(
                            market_window_end_epoch_ms / 1000.0,
                            tz=timezone.utc,
                        ).isoformat()

                    kalshi_ticker = str(market.get("kalshi_ticker", "")).strip()
                    resolved_row, match_type = _match_resolved(
                        resolved_index=resolved_index,
                        market_window_end_epoch_ms=market_window_end_epoch_ms,
                        kalshi_ticker=kalshi_ticker,
                    )
                    match_counts[match_type] = match_counts.get(match_type, 0) + 1

                    edge_a = _find_candidate_gross_edge(payload, BUY_CANDIDATE_A)
                    edge_b = _find_candidate_gross_edge(payload, BUY_CANDIDATE_B)

                    def leg_field(leg_name: str, field_name: str) -> Optional[float]:
                        leg = quote_legs.get(leg_name, {})
                        if not isinstance(leg, dict):
                            return None
                        return _safe_float(leg.get(field_name))

                    row = {
                        "source_group": source_group,
                        "source_file": str(path.relative_to(PROJECT_ROOT)),
                        "line_number": line_number,
                        "ts": payload.get("ts"),
                        "recv_ms": payload.get("recv_ms"),
                        "market_window_end_epoch_ms": market_window_end_epoch_ms,
                        "market_window_end": market_window_end,
                        "polymarket_event_slug": market.get("polymarket_event_slug"),
                        "polymarket_market_id": market.get("polymarket_market_id"),
                        "polymarket_condition_id": market.get("polymarket_condition_id"),
                        "polymarket_token_yes": market.get("polymarket_token_yes"),
                        "polymarket_token_no": market.get("polymarket_token_no"),
                        "kalshi_ticker": kalshi_ticker,
                        "polymarket_yes_best_ask_price": leg_field("polymarket_yes", "best_ask"),
                        "polymarket_yes_best_ask_size": leg_field("polymarket_yes", "best_ask_size"),
                        "polymarket_no_best_ask_price": leg_field("polymarket_no", "best_ask"),
                        "polymarket_no_best_ask_size": leg_field("polymarket_no", "best_ask_size"),
                        "kalshi_yes_best_ask_price": leg_field("kalshi_yes", "best_ask"),
                        "kalshi_yes_best_ask_size": leg_field("kalshi_yes", "best_ask_size"),
                        "kalshi_no_best_ask_price": leg_field("kalshi_no", "best_ask"),
                        "kalshi_no_best_ask_size": leg_field("kalshi_no", "best_ask_size"),
                        "gross_edge_buy_kalshi_yes_and_polymarket_no": edge_a["gross_edge"],
                        "gross_edge_buy_polymarket_yes_and_kalshi_no": edge_b["gross_edge"],
                        "total_ask_buy_kalshi_yes_and_polymarket_no": edge_a["total_ask"],
                        "total_ask_buy_polymarket_yes_and_kalshi_no": edge_b["total_ask"],
                        "resolved_match_type": match_type,
                        "resolved_comparison": resolved_row["comparison"] if resolved_row else None,
                        "resolved_market_outcome": resolved_row["market_outcome"] if resolved_row else None,
                        "resolved_window_end": resolved_row["window_end"] if resolved_row else None,
                        "resolved_polymarket_slug": resolved_row["polymarket_slug"] if resolved_row else None,
                    }
                    writer.writerow(row)
                    rows_written += 1

    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "inputs": {
            "pair_log": str(pair_log_path),
            "buy_decision_sources": [
                "data/buy_decision_log__*.jsonl",
                "_deprecated/data/buy_decision_log__*.jsonl",
            ],
        },
        "outputs": {
            "dataset_csv": str(output_csv_path),
            "summary_json": str(output_summary_path),
        },
        "counts": {
            "resolved_rows_indexed": resolved_index["rows_indexed"],
            "files_scanned": files_scanned,
            "rows_written": rows_written,
            "rows_skipped_non_buy_decision": rows_skipped_non_buy_decision,
            "rows_json_decode_error": rows_json_decode_error,
            "rows_missing_market_end": rows_missing_market_end,
            "resolved_match_type_counts": match_counts,
        },
    }
    output_summary_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)
    return summary


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build buy-decision ask/edge dataset joined with resolved outcomes."
    )
    parser.add_argument("--pair-log", default="data/diagnostic/resolved_15m_pairs.log")
    parser.add_argument(
        "--output-csv",
        default="data/diagnostic/buy_decision_ask_edge_with_resolved.csv",
    )
    parser.add_argument(
        "--output-summary",
        default="data/diagnostic/buy_decision_ask_edge_with_resolved_summary.json",
    )
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    summary = run(
        pair_log_path=Path(args.pair_log),
        output_csv_path=Path(args.output_csv),
        output_summary_path=Path(args.output_summary),
    )
    counts = summary["counts"]
    print("Buy decision ask/edge dataset build complete")
    print(f"Files scanned: {counts['files_scanned']}")
    print(f"Rows written: {counts['rows_written']}")
    print(f"Resolved matches (epoch+ticker): {counts['resolved_match_type_counts']['epoch+ticker']}")
    print(f"Resolved matches (epoch_unique): {counts['resolved_match_type_counts']['epoch_unique']}")
    print(f"Output CSV: {summary['outputs']['dataset_csv']}")
    print(f"Output summary: {summary['outputs']['summary_json']}")


if __name__ == "__main__":
    main()
