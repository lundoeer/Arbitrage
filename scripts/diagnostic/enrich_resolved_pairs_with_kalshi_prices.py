#!/usr/bin/env python3
"""
Fill missing Kalshi price fields in resolved_15m_pairs.log.

Selection rule:
- row already has polymarket_target and polymarket_end
- row is missing kalshi_target and/or kalshi_end

This script does not depend on a Polymarket validation log.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Tuple

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.common.api_transport import ApiTransport

KALSHI_DEFAULT_API_BASE = "https://api.elections.kalshi.com/trade-api/v2"
OVERLOAD_HTTP_STATUS_CODES = frozenset({408, 425, 429, 500, 502, 503, 504, 520, 522, 524, 529})

BASE_PATTERN = re.compile(
    r"^(?P<window_end>[^|]+)\s+\|\s+"
    r"kalshi=(?P<kalshi_ticker>[^:]+):(?P<kalshi_raw>[^()]+)\((?P<kalshi_norm>[^()]*)\)\s+\|\s+"
    r"polymarket=(?P<pm_slug>[^:]+):(?P<pm_raw>[^()]+)\((?P<pm_norm>[^()]*)\)\s+\|\s+"
    r"(?P<comparison>[^|]+)"
    r"(?P<extras>.*)$"
)


def _load_runtime_config(config_path: Path) -> Dict[str, Any]:
    if not config_path.exists():
        raise FileNotFoundError(f"Missing config file: {config_path}")
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)


def _resolve_kalshi_api_key(config: Dict[str, Any]) -> str:
    env_candidates = ["KALSHI_READONLY_API_KEY", "KALSHI_API_KEY", "kalshiapi"]
    for env_key in env_candidates:
        value = os.getenv(env_key, "").strip()
        if value:
            return value
    return str(config.get("api", {}).get("readonly_api_key") or "").strip()


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


def _format_value(value: Any, decimals: int) -> str:
    if value is None:
        raise RuntimeError("Price value is missing.")
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            raise RuntimeError("Price value is empty.")
        try:
            numeric = float(stripped)
            return f"{numeric:.{decimals}f}"
        except ValueError:
            return stripped
    if isinstance(value, (int, float)):
        return f"{float(value):.{decimals}f}"
    raise RuntimeError(f"Unsupported price value type: {type(value)}")


def _is_overload_error(exc: Exception) -> Tuple[bool, int | None]:
    status_code = getattr(exc, "status_code", None)
    if isinstance(status_code, int):
        return status_code in OVERLOAD_HTTP_STATUS_CODES, status_code
    match = re.search(r"\bHTTP\s+(?P<status>\d{3})\b", str(exc))
    if not match:
        return False, None
    parsed = int(match.group("status"))
    return parsed in OVERLOAD_HTTP_STATUS_CODES, parsed


def _parse_line(line: str) -> Dict[str, Any]:
    match = BASE_PATTERN.match(line.strip())
    if not match:
        raise RuntimeError(f"Unparseable log line: {line}")
    return {
        "window_end": match.group("window_end").strip(),
        "kalshi_ticker": match.group("kalshi_ticker").strip(),
        "kalshi_raw": match.group("kalshi_raw").strip(),
        "kalshi_norm": match.group("kalshi_norm").strip(),
        "pm_slug": match.group("pm_slug").strip(),
        "pm_raw": match.group("pm_raw").strip(),
        "pm_norm": match.group("pm_norm").strip(),
        "comparison": match.group("comparison").strip(),
        "extras": _parse_extras(match.group("extras") or ""),
    }


def _render_line(row: Dict[str, Any]) -> str:
    extras = dict(row["extras"])
    ordered_keys = [
        "kalshi_target",
        "kalshi_end",
        "polymarket_target",
        "polymarket_end",
    ]
    segments = [
        f"{row['window_end']}",
        f"kalshi={row['kalshi_ticker']}:{row['kalshi_raw']}({row['kalshi_norm']})",
        f"polymarket={row['pm_slug']}:{row['pm_raw']}({row['pm_norm']})",
        row["comparison"],
    ]
    for key in ordered_keys:
        if key in extras:
            segments.append(f"{key}={extras[key]}")
    for key, value in extras.items():
        if key in ordered_keys:
            continue
        segments.append(f"{key}={value}")
    return " | ".join(segments)


def _row_has_polymarket_prices(extras: Dict[str, str]) -> bool:
    return bool(extras.get("polymarket_target", "").strip() and extras.get("polymarket_end", "").strip())


def _row_has_kalshi_prices(extras: Dict[str, str]) -> bool:
    return bool(extras.get("kalshi_target", "").strip() and extras.get("kalshi_end", "").strip())


def _fetch_kalshi_prices(
    transport: ApiTransport,
    *,
    base_url: str,
    ticker: str,
) -> Tuple[str, str]:
    _, payload = transport.request_json("GET", f"{base_url}/markets/{ticker}")
    if not isinstance(payload, dict):
        raise RuntimeError(f"Unexpected Kalshi payload for ticker {ticker}")
    market = payload.get("market")
    if not isinstance(market, dict):
        raise RuntimeError(f"Kalshi payload missing market object for ticker {ticker}")
    target = _format_value(market.get("floor_strike"), decimals=2)
    end = _format_value(market.get("expiration_value"), decimals=2)
    return target, end


def run(
    *,
    config_path: Path,
    pair_log_path: Path,
    only_missing: bool = True,
    max_lines: int | None = None,
    dry_run: bool = False,
    progress_every: int = 200,
    sleep_ms: int = 0,
    max_consecutive_overload: int = 5,
) -> Dict[str, Any]:
    if not pair_log_path.exists():
        raise FileNotFoundError(f"Missing pair log: {pair_log_path}")

    load_dotenv(dotenv_path=".env", override=False)
    config = _load_runtime_config(config_path)
    kalshi_base_url = str(config.get("api", {}).get("base_url") or KALSHI_DEFAULT_API_BASE).rstrip("/")
    kalshi_api_key = _resolve_kalshi_api_key(config)
    if not kalshi_api_key:
        raise RuntimeError("Missing Kalshi API key in .env or config/run_config.json")

    transport = ApiTransport(
        default_headers={
            "Authorization": f"Bearer {kalshi_api_key}",
            "Content-Type": "application/json",
            "User-Agent": "Arbitrage-Kalshi-Only-Enrichment/1.0",
        }
    )

    raw_lines = pair_log_path.read_text(encoding="utf-8").splitlines()
    parsed_rows: List[Dict[str, Any]] = []
    for raw in raw_lines:
        line = raw.strip()
        if not line:
            continue
        parsed_rows.append(_parse_line(line))

    candidates: List[Dict[str, Any]] = []
    missing_polymarket_rows = 0
    already_has_kalshi_rows = 0
    for row in parsed_rows:
        extras = row["extras"]
        has_pm = _row_has_polymarket_prices(extras)
        has_k = _row_has_kalshi_prices(extras)
        if not has_pm:
            missing_polymarket_rows += 1
            continue
        if has_k:
            already_has_kalshi_rows += 1
            if only_missing:
                continue
        candidates.append(row)

    if max_lines is not None and max_lines > 0:
        candidates = candidates[: max_lines]

    if dry_run:
        return {
            "pair_log": str(pair_log_path),
            "mode": {
                "dry_run": True,
                "only_missing": bool(only_missing),
                "max_lines": max_lines,
            },
            "totals": {
                "rows_total": len(parsed_rows),
                "rows_selected": len(candidates),
                "rows_missing_polymarket_prices": missing_polymarket_rows,
                "rows_already_have_kalshi_prices": already_has_kalshi_rows,
                "rows_updated": 0,
            },
        }

    kalshi_cache: Dict[str, Tuple[str, str]] = {}
    failed_kalshi_fetches = 0
    processed = 0
    rows_updated = 0
    overload_termination_status_code: int | None = None
    terminated_early = False
    consecutive_overload_errors = 0
    progress_every_safe = max(1, int(progress_every))
    sleep_seconds = max(0.0, float(sleep_ms) / 1000.0)
    started_at = time.time()

    for row in candidates:
        ticker = row["kalshi_ticker"]
        processed += 1

        if ticker not in kalshi_cache:
            try:
                kalshi_cache[ticker] = _fetch_kalshi_prices(
                    transport,
                    base_url=kalshi_base_url,
                    ticker=ticker,
                )
                consecutive_overload_errors = 0
            except Exception as exc:
                failed_kalshi_fetches += 1
                is_overload, overload_status = _is_overload_error(exc)
                if is_overload:
                    consecutive_overload_errors += 1
                else:
                    consecutive_overload_errors = 0
                if max_consecutive_overload > 0 and consecutive_overload_errors >= max_consecutive_overload:
                    terminated_early = True
                    overload_termination_status_code = overload_status
                    break
                continue

        kalshi_target, kalshi_end = kalshi_cache[ticker]
        before = dict(row["extras"])
        row["extras"]["kalshi_target"] = kalshi_target
        row["extras"]["kalshi_end"] = kalshi_end
        if row["extras"] != before:
            rows_updated += 1

        should_print_progress = (
            processed == 1
            or processed % progress_every_safe == 0
            or processed == len(candidates)
            or terminated_early
        )
        if should_print_progress:
            elapsed = max(0.001, time.time() - started_at)
            rate_per_min = (processed / elapsed) * 60.0
            remaining = max(0, len(candidates) - processed)
            eta_seconds = (remaining / processed) * elapsed if processed > 0 else 0.0
            eta_minutes = int(round(max(0.0, eta_seconds) / 60.0))
            print(
                f"[{processed}/{len(candidates)}] updated={rows_updated} "
                f"rate={rate_per_min:.2f}/min eta~{eta_minutes}m"
            )

        if sleep_seconds > 0 and processed < len(candidates):
            time.sleep(sleep_seconds)

    if rows_updated > 0:
        rendered = [_render_line(row) for row in parsed_rows]
        pair_log_path.write_text("\n".join(rendered) + ("\n" if rendered else ""), encoding="utf-8")

    return {
        "pair_log": str(pair_log_path),
        "mode": {
            "dry_run": False,
            "only_missing": bool(only_missing),
            "max_lines": max_lines,
            "progress_every": progress_every_safe,
            "sleep_ms": int(max(0, sleep_ms)),
        },
        "totals": {
            "rows_total": len(parsed_rows),
            "rows_selected": len(candidates),
            "rows_updated": rows_updated,
            "rows_processed": processed,
            "rows_missing_polymarket_prices": missing_polymarket_rows,
            "rows_already_have_kalshi_prices": already_has_kalshi_rows,
            "kalshi_fetch_failures": failed_kalshi_fetches,
            "terminated_early": terminated_early,
            "overload_termination_status_code": overload_termination_status_code,
        },
        "unique_kalshi_markets_fetched": len(kalshi_cache),
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fill missing Kalshi target/end prices for resolved pairs that already have Polymarket prices."
    )
    parser.add_argument("--config", default="config/run_config.json")
    parser.add_argument("--pair-log", default="data/diagnostic/resolved_15m_pairs.log")
    parser.add_argument(
        "--only-missing",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Only update rows missing Kalshi target/end prices.",
    )
    parser.add_argument("--max-lines", type=int, default=None, help="Process at most this many selected rows.")
    parser.add_argument("--dry-run", action="store_true", help="Estimate selected rows only; do not write updates.")
    parser.add_argument("--progress-every", type=int, default=200, help="Print progress every N selected rows.")
    parser.add_argument("--sleep-ms", type=int, default=0, help="Optional sleep between rows (milliseconds).")
    parser.add_argument(
        "--max-consecutive-overload",
        type=int,
        default=5,
        help="Terminate early after this many consecutive overload HTTP errors (0 disables).",
    )
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    summary = run(
        config_path=Path(args.config),
        pair_log_path=Path(args.pair_log),
        only_missing=bool(args.only_missing),
        max_lines=args.max_lines,
        dry_run=bool(args.dry_run),
        progress_every=args.progress_every,
        sleep_ms=args.sleep_ms,
        max_consecutive_overload=args.max_consecutive_overload,
    )
    print("Kalshi-only enrichment complete")
    print(f"Pair log: {summary['pair_log']}")
    totals = summary.get("totals", {})
    print(f"Rows selected: {totals.get('rows_selected')}")
    print(f"Rows updated: {totals.get('rows_updated')}")
    print(f"Rows missing Polymarket prices: {totals.get('rows_missing_polymarket_prices')}")
    print(f"Rows already have Kalshi prices: {totals.get('rows_already_have_kalshi_prices')}")
    print(f"Kalshi fetch failures: {totals.get('kalshi_fetch_failures')}")
    print(f"Unique Kalshi tickers fetched: {summary.get('unique_kalshi_markets_fetched')}")
    if totals.get("terminated_early"):
        print(
            "Terminated early due to overload responses "
            f"(status={totals.get('overload_termination_status_code')})"
        )


if __name__ == "__main__":
    main()
