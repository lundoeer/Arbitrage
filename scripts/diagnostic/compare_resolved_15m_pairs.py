#!/usr/bin/env python3
"""
Compare resolved BTC 15m market outcomes between Kalshi and Polymarket.

Outputs:
- One-line-per-pair log file with venue outcomes.
- JSON summary file with totals for same vs different resolutions.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.common.api_transport import ApiTransport

logging.getLogger("dotenv.main").setLevel(logging.ERROR)


WINDOW_SECONDS = 900
POLYMARKET_GAMMA_API = "https://gamma-api.polymarket.com"
KALSHI_DEFAULT_API_BASE = "https://api.elections.kalshi.com/trade-api/v2"
KALSHI_RESOLVED_STATUSES = {"finalized", "settled"}


def _parse_iso_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    raw = value.strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _is_15m_bucket_aligned(dt: datetime) -> bool:
    return int(dt.timestamp()) % WINDOW_SECONDS == 0


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


def _parse_json_list(value: Any) -> List[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except ValueError:
            return []
        return parsed if isinstance(parsed, list) else []
    return []


def _normalize_binary_outcome(label: Optional[str]) -> Optional[str]:
    if not label:
        return None
    lowered = label.strip().lower()
    if lowered in {"yes", "up"}:
        return "YES"
    if lowered in {"no", "down"}:
        return "NO"
    return None


def _extract_polymarket_resolution(market: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    outcomes = _parse_json_list(market.get("outcomes"))
    prices_raw = _parse_json_list(market.get("outcomePrices"))
    if not outcomes or not prices_raw or len(outcomes) != len(prices_raw):
        return None, None

    prices: List[float] = []
    for item in prices_raw:
        try:
            prices.append(float(item))
        except Exception:
            return None, None

    if not prices:
        return None, None

    best_index = max(range(len(prices)), key=lambda idx: prices[idx])
    best_price = prices[best_index]
    # Resolved binary markets should have a winner near 1.0.
    if best_price < 0.99:
        return None, None

    raw_label = str(outcomes[best_index]).strip()
    normalized = _normalize_binary_outcome(raw_label)
    return raw_label, normalized


def _derive_polymarket_slug_from_window_end(window_end: datetime) -> str:
    window_start = window_end - timedelta(seconds=WINDOW_SECONDS)
    return f"btc-updown-15m-{int(window_start.timestamp())}"


def _fetch_kalshi_resolved_markets(
    transport: ApiTransport,
    *,
    base_url: str,
    series_ticker: str,
    page_size: int,
    max_pages: Optional[int],
    max_markets: Optional[int],
) -> List[Dict[str, Any]]:
    resolved: List[Dict[str, Any]] = []
    cursor: Optional[str] = None
    page = 0

    while True:
        page += 1
        if max_pages is not None and page > max_pages:
            break

        params: Dict[str, Any] = {"series_ticker": series_ticker, "limit": page_size}
        if cursor:
            params["cursor"] = cursor

        _, payload = transport.request_json("GET", f"{base_url}/markets", params=params)
        if not isinstance(payload, dict):
            raise RuntimeError("Unexpected Kalshi /markets payload")

        markets = payload.get("markets", [])
        if not isinstance(markets, list) or not markets:
            break

        for market in markets:
            status = str(market.get("status", "")).lower()
            if status not in KALSHI_RESOLVED_STATUSES:
                continue
            result = _normalize_binary_outcome(str(market.get("result", "")).strip())
            if result is None:
                continue
            close_dt = _parse_iso_datetime(market.get("close_time"))
            if not close_dt or not _is_15m_bucket_aligned(close_dt):
                continue
            resolved.append(market)
            if max_markets is not None and len(resolved) >= max_markets:
                return resolved

        cursor = payload.get("cursor")
        if not cursor:
            break

    return resolved


def _fetch_polymarket_event_by_slug(
    transport: ApiTransport,
    slug: str,
) -> Optional[Dict[str, Any]]:
    _, payload = transport.request_json("GET", f"{POLYMARKET_GAMMA_API}/events", params={"slug": slug})
    if not isinstance(payload, list) or not payload:
        return None
    event = payload[0]
    return event if isinstance(event, dict) else None


def _find_polymarket_market_in_event(event: Dict[str, Any], slug: str) -> Optional[Dict[str, Any]]:
    markets = event.get("markets", [])
    if not isinstance(markets, list):
        return None
    for market in markets:
        if str(market.get("slug", "")).strip() == slug:
            return market
    return markets[0] if markets else None


def _row_to_log_line(row: Dict[str, Any]) -> str:
    line = (
        f"{row['window_end']} | "
        f"kalshi={row['kalshi_ticker']}:{row['kalshi_resolution_raw']}({row['kalshi_resolution_normalized']}) | "
        f"polymarket={row['polymarket_slug']}:{row['polymarket_resolution_raw']}({row['polymarket_resolution_normalized']}) | "
        f"{row['comparison']}"
    )
    extra_keys = [
        "kalshi_target",
        "kalshi_end",
        "polymarket_target",
        "polymarket_end",
    ]
    for key in extra_keys:
        value = row.get(key)
        if value is None:
            continue
        line += f" | {key}={value}"
    return line


def _row_key(row: Dict[str, Any]) -> str:
    return "|".join(
        [
            str(row.get("window_end", "")),
            str(row.get("kalshi_ticker", "")),
            str(row.get("polymarket_slug", "")),
        ]
    )


def _parse_log_line(line: str) -> Optional[Dict[str, Any]]:
    # Expected format:
    # {window_end} | kalshi={ticker}:{raw}({norm}) | polymarket={slug}:{raw}({norm}) | {comparison}
    parts = [part.strip() for part in line.strip().split(" | ")]
    if len(parts) < 4:
        return None

    window_end = parts[0]
    kalshi_part = parts[1]
    polymarket_part = parts[2]
    comparison = parts[3]

    if not kalshi_part.startswith("kalshi=") or not polymarket_part.startswith("polymarket="):
        return None

    def _parse_side(part: str, prefix: str) -> Optional[Tuple[str, str, str]]:
        payload = part[len(prefix) :]
        if ":" not in payload:
            return None
        ident, outcome_part = payload.split(":", 1)
        match = re.match(r"^(?P<raw>.*)\((?P<norm>[^()]*)\)$", outcome_part.strip())
        if not match:
            return None
        raw_value = match.group("raw").strip()
        norm_value = match.group("norm").strip()
        return ident.strip(), raw_value, norm_value

    kalshi_parsed = _parse_side(kalshi_part, "kalshi=")
    polymarket_parsed = _parse_side(polymarket_part, "polymarket=")
    if not kalshi_parsed or not polymarket_parsed:
        return None

    kalshi_ticker, kalshi_raw, kalshi_norm = kalshi_parsed
    polymarket_slug, polymarket_raw, polymarket_norm = polymarket_parsed

    parsed: Dict[str, Any] = {
        "window_end": window_end,
        "kalshi_ticker": kalshi_ticker,
        "kalshi_resolution_raw": kalshi_raw,
        "kalshi_resolution_normalized": kalshi_norm,
        "polymarket_slug": polymarket_slug,
        "polymarket_resolution_raw": polymarket_raw,
        "polymarket_resolution_normalized": polymarket_norm,
        "comparison": comparison,
    }
    for extra in parts[4:]:
        if "=" not in extra:
            continue
        key, value = extra.split("=", 1)
        parsed[key.strip()] = value.strip()
    return parsed


def _read_pair_log(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []

    parsed_rows: List[Dict[str, Any]] = []
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        row = _parse_log_line(line)
        if row:
            parsed_rows.append(row)
    return parsed_rows


def _append_new_pairs(path: Path, candidate_rows: List[Dict[str, Any]]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    existing_rows = _read_pair_log(path)
    unique_existing_rows: List[Dict[str, Any]] = []
    existing_keys: set[str] = set()
    for row in existing_rows:
        key = _row_key(row)
        if key in existing_keys:
            continue
        existing_keys.add(key)
        unique_existing_rows.append(row)

    # Keep the log canonical if duplicates were already present.
    if len(unique_existing_rows) != len(existing_rows):
        with open(path, "w", encoding="utf-8") as f:
            for row in unique_existing_rows:
                f.write(_row_to_log_line(row) + "\n")

    new_rows: List[Dict[str, Any]] = []
    for row in candidate_rows:
        key = _row_key(row)
        if key in existing_keys:
            continue
        existing_keys.add(key)
        new_rows.append(row)

    if not new_rows:
        return 0

    with open(path, "a", encoding="utf-8") as f:
        for row in new_rows:
            f.write(_row_to_log_line(row) + "\n")

    return len(new_rows)


def _write_summary(path: Path, summary: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)


def run(
    *,
    config_path: Path,
    pair_log_path: Path,
    summary_path: Path,
    page_size: int,
    max_pages: Optional[int],
    max_markets: Optional[int],
) -> Dict[str, Any]:
    load_dotenv(dotenv_path=".env", override=False)
    config = _load_runtime_config(config_path)

    kalshi_base_url = str(config.get("api", {}).get("base_url") or KALSHI_DEFAULT_API_BASE).rstrip("/")
    kalshi_series = str(config.get("markets", {}).get("target_series") or "KXBTC15M").upper()
    kalshi_api_key = _resolve_kalshi_api_key(config)
    if not kalshi_api_key:
        raise RuntimeError("Missing Kalshi API key in .env or config/run_config.json")

    kalshi_transport = ApiTransport(
        default_headers={
            "Authorization": f"Bearer {kalshi_api_key}",
            "Content-Type": "application/json",
            "User-Agent": "Arbitrage-Resolution-Diagnostic/1.0",
        }
    )
    polymarket_transport = ApiTransport(
        default_headers={
            "User-Agent": "Arbitrage-Resolution-Diagnostic/1.0",
            "Accept": "application/json, text/plain, */*",
        }
    )

    kalshi_resolved = _fetch_kalshi_resolved_markets(
        kalshi_transport,
        base_url=kalshi_base_url,
        series_ticker=kalshi_series,
        page_size=page_size,
        max_pages=max_pages,
        max_markets=max_markets,
    )

    rows: List[Dict[str, Any]] = []
    polymarket_found = 0

    for market in kalshi_resolved:
        close_dt = _parse_iso_datetime(market.get("close_time"))
        if not close_dt:
            continue

        kalshi_raw = str(market.get("result", "")).strip()
        kalshi_norm = _normalize_binary_outcome(kalshi_raw)
        if kalshi_norm is None:
            continue

        slug = _derive_polymarket_slug_from_window_end(close_dt)
        event = _fetch_polymarket_event_by_slug(polymarket_transport, slug)

        pm_market_id = None
        pm_raw = None
        pm_norm = None
        pm_found = False

        if event:
            pm_market = _find_polymarket_market_in_event(event, slug)
            if pm_market:
                pm_found = True
                polymarket_found += 1
                pm_market_id = str(pm_market.get("id", "")).strip() or None
                pm_raw, pm_norm = _extract_polymarket_resolution(pm_market)

        comparison = "unknown"
        if pm_norm in {"YES", "NO"}:
            comparison = "same" if pm_norm == kalshi_norm else "different"
        elif not pm_found:
            comparison = "missing_polymarket"
        else:
            comparison = "polymarket_unresolved_or_unknown"

        rows.append(
            {
                "window_end": close_dt.isoformat(),
                "kalshi_ticker": str(market.get("ticker", "")),
                "kalshi_status": str(market.get("status", "")),
                "kalshi_resolution_raw": kalshi_raw,
                "kalshi_resolution_normalized": kalshi_norm,
                "polymarket_slug": slug,
                "polymarket_market_id": pm_market_id,
                "polymarket_found": pm_found,
                "polymarket_resolution_raw": pm_raw,
                "polymarket_resolution_normalized": pm_norm,
                "comparison": comparison,
            }
        )

    rows.sort(key=lambda row: row["window_end"])

    paired_rows = [row for row in rows if row["polymarket_resolution_normalized"] in {"YES", "NO"}]
    appended_new_pairs = _append_new_pairs(pair_log_path, paired_rows)
    full_log_rows = _read_pair_log(pair_log_path)
    same_count = sum(1 for row in full_log_rows if row.get("comparison") == "same")
    different_count = sum(1 for row in full_log_rows if row.get("comparison") == "different")

    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "equivalence": {
            "kalshi_yes_or_no": {"yes": "YES", "no": "NO"},
            "polymarket_up_or_down": {"up": "YES", "down": "NO"},
        },
        "totals": {
            "kalshi_resolved_markets_scanned": len(kalshi_resolved),
            "polymarket_events_found_this_run": polymarket_found,
            "paired_with_resolved_outcomes_this_run": len(paired_rows),
            "new_pairs_appended_to_log": appended_new_pairs,
            "pairs_in_log_total": len(full_log_rows),
            "resolved_same": same_count,
            "resolved_different": different_count,
            "missing_polymarket_this_run": sum(1 for row in rows if row["comparison"] == "missing_polymarket"),
            "polymarket_unresolved_or_unknown_this_run": sum(
                1 for row in rows if row["comparison"] == "polymarket_unresolved_or_unknown"
            ),
        },
        "output_files": {
            "pair_log": str(pair_log_path),
            "summary_json": str(summary_path),
        },
        "different_examples": [row for row in full_log_rows if row.get("comparison") == "different"][:50],
    }
    _write_summary(summary_path, summary)
    return summary


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Compare resolved BTC 15m outcomes between Kalshi and Polymarket."
    )
    parser.add_argument("--config", default="config/run_config.json")
    parser.add_argument("--pair-log", default="data/diagnostic/resolved_15m_pairs.log")
    parser.add_argument("--summary", default="data/diagnostic/resolution_comparison_summary.json")
    parser.add_argument("--page-size", type=int, default=500)
    parser.add_argument("--max-pages", type=int, default=None)
    parser.add_argument("--max-markets", type=int, default=None)
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    summary = run(
        config_path=Path(args.config),
        pair_log_path=Path(args.pair_log),
        summary_path=Path(args.summary),
        page_size=args.page_size,
        max_pages=args.max_pages,
        max_markets=args.max_markets,
    )

    totals = summary["totals"]
    print("Resolved 15m comparison complete")
    print(f"Kalshi resolved scanned: {totals['kalshi_resolved_markets_scanned']}")
    print(f"Resolved pairs compared this run: {totals['paired_with_resolved_outcomes_this_run']}")
    print(f"New pairs appended: {totals['new_pairs_appended_to_log']}")
    print(f"Resolved pairs in log (total): {totals['pairs_in_log_total']}")
    print(f"Resolved same (log total): {totals['resolved_same']}")
    print(f"Resolved different (log total): {totals['resolved_different']}")
    print(f"Missing on Polymarket this run: {totals['missing_polymarket_this_run']}")
    print(
        "Polymarket unresolved/unknown this run: "
        f"{totals['polymarket_unresolved_or_unknown_this_run']}"
    )
    print(f"Pair log: {summary['output_files']['pair_log']}")
    print(f"Summary:  {summary['output_files']['summary_json']}")


if __name__ == "__main__":
    main()
