#!/usr/bin/env python3
"""
Enrich resolved BTC 15m pair log lines with Kalshi and Polymarket price values.

For each line in data/diagnostic/resolved_15m_pairs.log, append/update:
- kalshi_target (floor_strike)
- kalshi_end (expiration_value)
- polymarket_target (Chainlink window start price)
- polymarket_end (Chainlink window end price)
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.common.api_transport import ApiTransport

logging.getLogger("dotenv.main").setLevel(logging.ERROR)

KALSHI_DEFAULT_API_BASE = "https://api.elections.kalshi.com/trade-api/v2"

BASE_PATTERN = re.compile(
    r"^(?P<window_end>[^|]+)\s+\|\s+"
    r"kalshi=(?P<kalshi_ticker>[^:]+):(?P<kalshi_raw>[^()]+)\((?P<kalshi_norm>[^()]*)\)\s+\|\s+"
    r"polymarket=(?P<pm_slug>[^:]+):(?P<pm_raw>[^()]+)\((?P<pm_norm>[^()]*)\)\s+\|\s+"
    r"(?P<comparison>[^|]+)"
    r"(?P<extras>.*)$"
)
POLYMARKET_VALIDATION_PATTERN = re.compile(
    r"^(?P<slug>[^|]+)\s+\|\s+window_end=.*\|\s+target=(?P<target>[^|]+)\s+\|\s+"
    r"end=(?P<end>[^|]+)\s+\|"
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

    parts = [part.strip() for part in text.split("|")]
    for part in parts:
        if not part:
            continue
        if "=" not in part:
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


def _load_polymarket_prices_from_validation_log(path: Path) -> Dict[str, Tuple[str, str]]:
    if not path.exists():
        raise FileNotFoundError(f"Missing Polymarket validation log: {path}")

    prices: Dict[str, Tuple[str, str]] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line:
            continue
        match = POLYMARKET_VALIDATION_PATTERN.match(line)
        if not match:
            continue
        slug = match.group("slug").strip()
        target = _format_value(match.group("target").strip(), decimals=8)
        end = _format_value(match.group("end").strip(), decimals=8)
        prices[slug] = (target, end)

    if not prices:
        raise RuntimeError(f"No parseable Polymarket prices found in validation log: {path}")
    return prices


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
        (
            f"kalshi={row['kalshi_ticker']}:"
            f"{row['kalshi_raw']}({row['kalshi_norm']})"
        ),
        (
            f"polymarket={row['pm_slug']}:"
            f"{row['pm_raw']}({row['pm_norm']})"
        ),
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
    polymarket_validation_log_path: Path,
) -> Dict[str, Any]:
    if not pair_log_path.exists():
        raise FileNotFoundError(f"Missing pair log: {pair_log_path}")

    load_dotenv(dotenv_path=".env", override=False)
    config = _load_runtime_config(config_path)
    kalshi_base_url = str(config.get("api", {}).get("base_url") or KALSHI_DEFAULT_API_BASE).rstrip("/")
    kalshi_api_key = _resolve_kalshi_api_key(config)
    if not kalshi_api_key:
        raise RuntimeError("Missing Kalshi API key in .env or config/run_config.json")

    kalshi_transport = ApiTransport(
        default_headers={
            "Authorization": f"Bearer {kalshi_api_key}",
            "Content-Type": "application/json",
            "User-Agent": "Arbitrage-Resolved-Pair-Enrichment/1.0",
        }
    )
    polymarket_price_map = _load_polymarket_prices_from_validation_log(polymarket_validation_log_path)

    raw_lines = pair_log_path.read_text(encoding="utf-8").splitlines()
    parsed_rows: List[Dict[str, Any]] = []
    for raw in raw_lines:
        line = raw.strip()
        if not line:
            continue
        parsed_rows.append(_parse_line(line))

    kalshi_cache: Dict[str, Tuple[str, str]] = {}
    for row in parsed_rows:
        ticker = row["kalshi_ticker"]
        slug = row["pm_slug"]

        if ticker not in kalshi_cache:
            kalshi_cache[ticker] = _fetch_kalshi_prices(
                kalshi_transport,
                base_url=kalshi_base_url,
                ticker=ticker,
            )
        if slug not in polymarket_price_map:
            raise RuntimeError(
                f"Slug {slug} missing in data/diagnostic/polymarket_chainlink_price_validation.log"
            )

        kalshi_target, kalshi_end = kalshi_cache[ticker]
        pm_target, pm_end = polymarket_price_map[slug]
        row["extras"]["kalshi_target"] = kalshi_target
        row["extras"]["kalshi_end"] = kalshi_end
        row["extras"]["polymarket_target"] = pm_target
        row["extras"]["polymarket_end"] = pm_end

    rendered = [_render_line(row) for row in parsed_rows]
    pair_log_path.write_text("\n".join(rendered) + ("\n" if rendered else ""), encoding="utf-8")

    return {
        "pair_log": str(pair_log_path),
        "lines_updated": len(parsed_rows),
        "unique_kalshi_markets": len(kalshi_cache),
        "unique_polymarket_markets": len({row["pm_slug"] for row in parsed_rows}),
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Append Kalshi and Polymarket target/end prices to resolved_15m_pairs.log."
    )
    parser.add_argument("--config", default="config/run_config.json")
    parser.add_argument("--pair-log", default="data/diagnostic/resolved_15m_pairs.log")
    parser.add_argument(
        "--polymarket-validation-log",
        default="data/diagnostic/polymarket_chainlink_price_validation.log",
    )
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    summary = run(
        config_path=Path(args.config),
        pair_log_path=Path(args.pair_log),
        polymarket_validation_log_path=Path(args.polymarket_validation_log),
    )
    print("Resolved pair log enrichment complete")
    print(f"Pair log: {summary['pair_log']}")
    print(f"Lines updated: {summary['lines_updated']}")
    print(f"Unique Kalshi tickers fetched: {summary['unique_kalshi_markets']}")
    print(f"Unique Polymarket slugs fetched: {summary['unique_polymarket_markets']}")


if __name__ == "__main__":
    main()
