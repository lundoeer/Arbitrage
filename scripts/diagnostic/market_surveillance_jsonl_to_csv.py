#!/usr/bin/env python3
"""
Convert market surveillance JSONL snapshots to a second-aligned CSV.

Alignment rules (per kalshi_ticker):
1) Build a full-second timeline from min..max (UTC, inclusive) using timestamp fields:
   - underlying_prices.polymarket_chainlink.source_timestamp_ms
   - underlying_prices.kalshi_site.source_timestamp_ms
   - underlying_prices.kalshi_site.metadata.selected_timestamp_ms
     (fallback: selected_second_ms)
   - ts rounded down to second
2) Fill fields by timestamp basis:
   - Polymarket chainlink fields by polymarket source_timestamp_ms
   - Kalshi site fields by kalshi source_timestamp_ms
   - best_asks / kalshi_ticker / health.errors by ts-rounded-to-second
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def _safe_int(value: Any) -> Optional[int]:
    try:
        return int(value)
    except Exception:
        return None


def _safe_float(value: Any) -> Optional[float]:
    try:
        return float(value)
    except Exception:
        return None


def _parse_iso_to_epoch_ms(value: Any) -> Optional[int]:
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(round(dt.timestamp() * 1000.0))


def _floor_second_ms(epoch_ms: int) -> int:
    return (epoch_ms // 1000) * 1000


def _to_utc_second_iso(second_ms: int) -> str:
    return datetime.fromtimestamp(second_ms / 1000.0, tz=timezone.utc).isoformat()


def _as_json_array_text(value: Any) -> str:
    if isinstance(value, list):
        return json.dumps(value, separators=(",", ":"), ensure_ascii=True)
    if value is None:
        return "[]"
    return json.dumps([str(value)], separators=(",", ":"), ensure_ascii=True)


@dataclass
class PairAccumulator:
    min_second_ms: Optional[int] = None
    max_second_ms: Optional[int] = None
    window_end_ms: Optional[int] = None
    polymarket_by_second: Dict[int, Dict[str, Any]] = field(default_factory=dict)
    kalshi_by_second: Dict[int, Dict[str, Any]] = field(default_factory=dict)
    ts_by_second: Dict[int, Dict[str, Any]] = field(default_factory=dict)

    def add_timeline_second(self, second_ms: Optional[int]) -> None:
        if second_ms is None:
            return
        if self.min_second_ms is None or second_ms < self.min_second_ms:
            self.min_second_ms = second_ms
        if self.max_second_ms is None or second_ms > self.max_second_ms:
            self.max_second_ms = second_ms


def run(
    *,
    input_jsonl_path: Path,
    output_csv_path: Path,
    kalshi_ticker_filter: Optional[str] = None,
) -> Dict[str, Any]:
    if not input_jsonl_path.exists():
        raise FileNotFoundError(f"Input JSONL not found: {input_jsonl_path}")

    pairs: Dict[str, PairAccumulator] = {}
    lines_total = 0
    lines_json_error = 0
    lines_without_ticker = 0

    with open(input_jsonl_path, "r", encoding="utf-8") as f:
        for raw_line in f:
            lines_total += 1
            line = raw_line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                lines_json_error += 1
                continue

            pair = payload.get("pair", {})
            kalshi_ticker = str(pair.get("kalshi_ticker", "")).strip()
            if not kalshi_ticker:
                lines_without_ticker += 1
                continue
            if kalshi_ticker_filter and kalshi_ticker != kalshi_ticker_filter:
                continue

            acc = pairs.setdefault(kalshi_ticker, PairAccumulator())
            window_end_ms = _parse_iso_to_epoch_ms(pair.get("window_end"))
            if window_end_ms is not None:
                acc.window_end_ms = window_end_ms
            underlying_prices = payload.get("underlying_prices", {})

            polymarket = (
                underlying_prices.get("polymarket_chainlink")
                or underlying_prices.get("polymarket_chainling")
                or {}
            )
            kalshi_site = underlying_prices.get("kalshi_site") or {}
            kalshi_meta = kalshi_site.get("metadata") or {}

            pm_source_ts_ms = _safe_int(polymarket.get("source_timestamp_ms"))
            pm_second_ms = _floor_second_ms(pm_source_ts_ms) if pm_source_ts_ms is not None else None
            if pm_second_ms is not None:
                acc.add_timeline_second(pm_second_ms)
                acc.polymarket_by_second[pm_second_ms] = {
                    "polymarket_chainlink_price_usd": _safe_float(polymarket.get("price_usd")),
                    "polymarket_chainlink_source_timestamp_ms": pm_source_ts_ms,
                }

            kx_source_ts_ms = _safe_int(kalshi_site.get("source_timestamp_ms"))
            kx_second_ms = _floor_second_ms(kx_source_ts_ms) if kx_source_ts_ms is not None else None
            kx_selected_ts_ms = _safe_int(
                kalshi_meta.get("selected_timestamp_ms")
                if "selected_timestamp_ms" in kalshi_meta
                else kalshi_meta.get("selected_second_ms")
            )
            if kx_second_ms is not None:
                acc.add_timeline_second(kx_second_ms)
                acc.kalshi_by_second[kx_second_ms] = {
                    "kalshi_site_price_usd": _safe_float(kalshi_site.get("price_usd")),
                    "kalshi_site_source_timestamp_ms": kx_source_ts_ms,
                    "kalshi_site_selected_timestamp_ms": kx_selected_ts_ms,
                }
            if kx_selected_ts_ms is not None:
                acc.add_timeline_second(_floor_second_ms(kx_selected_ts_ms))

            ts_ms = _parse_iso_to_epoch_ms(payload.get("ts"))
            ts_second_ms = _floor_second_ms(ts_ms) if ts_ms is not None else None
            if ts_second_ms is not None:
                acc.add_timeline_second(ts_second_ms)
                best_asks = payload.get("best_asks") or {}
                recv_ms = _safe_int(payload.get("recv_ms"))
                existing = acc.ts_by_second.get(ts_second_ms)

                should_replace = False
                if existing is None:
                    should_replace = True
                else:
                    old_recv = _safe_int(existing.get("recv_ms"))
                    if old_recv is None:
                        should_replace = recv_ms is not None
                    elif recv_ms is not None and recv_ms >= old_recv:
                        should_replace = True

                if should_replace:
                    pm_yes = best_asks.get("polymarket_yes") or {}
                    pm_no = best_asks.get("polymarket_no") or {}
                    kx_yes = best_asks.get("kalshi_yes") or {}
                    kx_no = best_asks.get("kalshi_no") or {}
                    acc.ts_by_second[ts_second_ms] = {
                        "recv_ms": recv_ms,
                        "ts_rounded_second_ms": ts_second_ms,
                        "best_asks_polymarket_yes_price": _safe_float(pm_yes.get("price")),
                        "best_asks_polymarket_yes_size": _safe_float(pm_yes.get("size")),
                        "best_asks_polymarket_no_price": _safe_float(pm_no.get("price")),
                        "best_asks_polymarket_no_size": _safe_float(pm_no.get("size")),
                        "best_asks_kalshi_yes_price": _safe_float(kx_yes.get("price")),
                        "best_asks_kalshi_yes_size": _safe_float(kx_yes.get("size")),
                        "best_asks_kalshi_no_price": _safe_float(kx_no.get("price")),
                        "best_asks_kalshi_no_size": _safe_float(kx_no.get("size")),
                        "health_errors": _as_json_array_text((payload.get("health") or {}).get("errors")),
                    }

    output_csv_path.parent.mkdir(parents=True, exist_ok=True)
    columns = [
        "kalshi_ticker",
        "timeline_second_utc",
        "timeline_second_ms",
        "market_age_seconds",
        "polymarket_chainlink_price_usd",
        "polymarket_chainlink_source_timestamp_ms",
        "kalshi_site_price_usd",
        "kalshi_site_source_timestamp_ms",
        "kalshi_site_selected_timestamp_ms",
        "ts_rounded_second_ms",
        "best_asks_polymarket_yes_price",
        "best_asks_polymarket_yes_size",
        "best_asks_polymarket_no_price",
        "best_asks_polymarket_no_size",
        "best_asks_kalshi_yes_price",
        "best_asks_kalshi_yes_size",
        "best_asks_kalshi_no_price",
        "best_asks_kalshi_no_size",
        "health_errors",
    ]

    rows_written = 0
    with open(output_csv_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()

        for kalshi_ticker in sorted(pairs.keys()):
            acc = pairs[kalshi_ticker]
            if acc.min_second_ms is None or acc.max_second_ms is None:
                continue

            for second_ms in range(acc.min_second_ms, acc.max_second_ms + 1000, 1000):
                pm_row = acc.polymarket_by_second.get(second_ms, {})
                kx_row = acc.kalshi_by_second.get(second_ms, {})
                ts_row = acc.ts_by_second.get(second_ms, {})
                market_age_seconds = (
                    second_ms + 900_000 - acc.window_end_ms
                    if acc.window_end_ms is not None
                    else None
                )
                if market_age_seconds is not None:
                    market_age_seconds = market_age_seconds / 1000.0
                writer.writerow(
                    {
                        "kalshi_ticker": kalshi_ticker,
                        "timeline_second_utc": _to_utc_second_iso(second_ms),
                        "timeline_second_ms": second_ms,
                        "market_age_seconds": market_age_seconds,
                        "polymarket_chainlink_price_usd": pm_row.get("polymarket_chainlink_price_usd"),
                        "polymarket_chainlink_source_timestamp_ms": pm_row.get("polymarket_chainlink_source_timestamp_ms"),
                        "kalshi_site_price_usd": kx_row.get("kalshi_site_price_usd"),
                        "kalshi_site_source_timestamp_ms": kx_row.get("kalshi_site_source_timestamp_ms"),
                        "kalshi_site_selected_timestamp_ms": kx_row.get("kalshi_site_selected_timestamp_ms"),
                        "ts_rounded_second_ms": ts_row.get("ts_rounded_second_ms"),
                        "best_asks_polymarket_yes_price": ts_row.get("best_asks_polymarket_yes_price"),
                        "best_asks_polymarket_yes_size": ts_row.get("best_asks_polymarket_yes_size"),
                        "best_asks_polymarket_no_price": ts_row.get("best_asks_polymarket_no_price"),
                        "best_asks_polymarket_no_size": ts_row.get("best_asks_polymarket_no_size"),
                        "best_asks_kalshi_yes_price": ts_row.get("best_asks_kalshi_yes_price"),
                        "best_asks_kalshi_yes_size": ts_row.get("best_asks_kalshi_yes_size"),
                        "best_asks_kalshi_no_price": ts_row.get("best_asks_kalshi_no_price"),
                        "best_asks_kalshi_no_size": ts_row.get("best_asks_kalshi_no_size"),
                        "health_errors": ts_row.get("health_errors"),
                    }
                )
                rows_written += 1

    return {
        "input_jsonl": str(input_jsonl_path),
        "output_csv": str(output_csv_path),
        "kalshi_ticker_filter": kalshi_ticker_filter,
        "lines_total": lines_total,
        "lines_json_error": lines_json_error,
        "lines_without_ticker": lines_without_ticker,
        "kalshi_ticker_count": len(pairs),
        "rows_written": rows_written,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Convert market surveillance JSONL to second-aligned CSV.")
    parser.add_argument(
        "--input-jsonl",
        default="data/diagnostic/market_surveillance/btc_15m_per_second.jsonl",
    )
    parser.add_argument(
        "--output-csv",
        default="data/diagnostic/market_surveillance/btc_15m_per_second_aligned.csv",
    )
    parser.add_argument(
        "--kalshi-ticker",
        default=None,
        help="Optional exact kalshi_ticker filter. If set, only that market pair is processed.",
    )
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    summary = run(
        input_jsonl_path=Path(args.input_jsonl),
        output_csv_path=Path(args.output_csv),
        kalshi_ticker_filter=str(args.kalshi_ticker).strip() if args.kalshi_ticker else None,
    )
    print("Market surveillance JSONL -> CSV complete")
    print(f"Input: {summary['input_jsonl']}")
    print(f"Output: {summary['output_csv']}")
    print(f"Kalshi filter: {summary['kalshi_ticker_filter']}")
    print(f"Lines read: {summary['lines_total']}")
    print(f"Rows written: {summary['rows_written']}")
    print(f"Kalshi tickers: {summary['kalshi_ticker_count']}")
    print(f"JSON decode errors: {summary['lines_json_error']}")
    print(f"Missing ticker lines: {summary['lines_without_ticker']}")


if __name__ == "__main__":
    main()
