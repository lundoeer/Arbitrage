#!/usr/bin/env python3
"""
Log pair price/depth + websocket health once per second for the active BTC 15m pair.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.common.decision_runtime import (  # noqa: E402
    SharePriceRuntime,
    build_quote_sanity_and_canonical as _build_quote_sanity_and_canonical,
)
from scripts.common.kalshi_auth import resolve_kalshi_ws_headers as _resolve_kalshi_ws_headers  # noqa: E402
from scripts.common.market_selection import (  # noqa: E402
    load_selected_markets as _load_selected_markets,
    safe_name as _safe_name,
)
from scripts.common.run_config import load_health_config_from_run_config  # noqa: E402
from scripts.common.ws_collectors import KalshiWsCollector, PolymarketWsCollector  # noqa: E402
from scripts.common.ws_transport import JsonlWriter, now_ms, utc_now_iso  # noqa: E402
from scripts.common.utils import as_dict as _as_dict  # noqa: E402


def _leg_depth_snapshot(leg: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "best_bid": leg.get("best_bid"),
        "best_bid_size": leg.get("best_bid_size"),
        "best_ask": leg.get("best_ask"),
        "best_ask_size": leg.get("best_ask_size"),
        "mid": leg.get("mid"),
        "spread": leg.get("spread"),
        "spread_bps": leg.get("spread_bps"),
        "source_timestamp_ms": leg.get("source_timestamp_ms"),
        "recv_timestamp_ms": leg.get("recv_timestamp_ms"),
        "quote_age_ms": leg.get("quote_age_ms"),
        # "Depth at current price" is interpreted as available size at current best bid/ask.
        "depth_at_current_bid_price": leg.get("best_bid_size"),
        "depth_at_current_ask_price": leg.get("best_ask_size"),
    }


async def _run_for_duration(
    *,
    kalshi_collector: KalshiWsCollector,
    polymarket_collector: PolymarketWsCollector,
    runtime: SharePriceRuntime,
    sample_writer: JsonlWriter,
    duration_seconds: int,
) -> Dict[str, Any]:
    stats: Dict[str, Any] = {
        "samples": 0,
        "can_trade_true_samples": 0,
        "can_trade_false_samples": 0,
        "last_sample": None,
    }

    stop_event = asyncio.Event()
    tasks = [
        asyncio.create_task(kalshi_collector.run(stop_event)),
        asyncio.create_task(polymarket_collector.run(stop_event)),
    ]

    try:
        end_ms = now_ms() + int(max(1, duration_seconds) * 1000)
        while now_ms() < end_ms:
            now_epoch_ms = now_ms()
            prices = runtime.snapshot(now_epoch_ms=now_epoch_ms)
            quotes = _as_dict(prices.get("quotes"))
            legs = _as_dict(quotes.get("legs"))
            quote_sanity = _build_quote_sanity_and_canonical(quotes)

            kalshi_health = kalshi_collector.health_snapshot(now_epoch_ms=now_epoch_ms)
            polymarket_health = polymarket_collector.health_snapshot(now_epoch_ms=now_epoch_ms)
            health_can_trade = bool(kalshi_health.get("decision_ok")) and bool(polymarket_health.get("decision_ok"))
            decision_ready = bool(_as_dict(quote_sanity.get("canonical")).get("decision_ready"))
            can_trade = health_can_trade and decision_ready

            sample = {
                "ts": utc_now_iso(),
                "recv_ms": now_epoch_ms,
                "kind": "pair_price_depth_health_sample",
                "can_trade": can_trade,
                "health_can_trade": health_can_trade,
                "decision_ready": decision_ready,
                "hard_gate_state": "open" if can_trade else "blocked",
                "quote_sanity": quote_sanity,
                "prices_and_depth": {
                    "polymarket_yes": _leg_depth_snapshot(_as_dict(legs.get("polymarket_yes"))),
                    "polymarket_no": _leg_depth_snapshot(_as_dict(legs.get("polymarket_no"))),
                    "kalshi_yes": _leg_depth_snapshot(_as_dict(legs.get("kalshi_yes"))),
                    "kalshi_no": _leg_depth_snapshot(_as_dict(legs.get("kalshi_no"))),
                },
                "health": {
                    "kalshi_ws": kalshi_health,
                    "polymarket_ws": polymarket_health,
                },
            }
            sample_writer.write(sample)

            stats["samples"] += 1
            if can_trade:
                stats["can_trade_true_samples"] += 1
            else:
                stats["can_trade_false_samples"] += 1
            stats["last_sample"] = sample

            await asyncio.sleep(1.0)
    finally:
        stop_event.set()
        await asyncio.wait(tasks, timeout=10)
        for task in tasks:
            if not task.done():
                task.cancel()

    return stats


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Log prices/depth at current price and websocket health once per second for active BTC 15m pair."
    )
    parser.add_argument("--duration-seconds", type=int, default=90)
    parser.add_argument("--config", default="config/run_config.json")
    parser.add_argument("--discovery-output", default="data/market_discovery_latest.json")
    parser.add_argument("--pair-cache", default="data/market_pair_cache.json")
    parser.add_argument("--skip-discovery", action="store_true")
    parser.add_argument(
        "--custom-feature-enabled",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "Enable Polymarket custom websocket features in subscribe payload "
            "(default: true). Use --no-custom-feature-enabled to disable."
        ),
    )
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    load_dotenv(dotenv_path=".env", override=False)
    config_path = Path(args.config)

    session_started = datetime.now(timezone.utc)
    run_id = session_started.strftime("%Y%m%dT%H%M%SZ")
    selection = _load_selected_markets(
        config_path=config_path,
        discovery_output=Path(args.discovery_output),
        pair_cache_path=Path(args.pair_cache),
        run_discovery_first=not bool(args.skip_discovery),
    )

    pm_slug = str(selection["polymarket"]["event_slug"])
    pm_market_id = str(selection["polymarket"]["market_id"])
    pm_yes = str(selection["polymarket"]["token_yes"])
    pm_no = str(selection["polymarket"]["token_no"])
    kx_ticker = str(selection["kalshi"]["ticker"])

    pm_name = f"{_safe_name(pm_slug)}__{_safe_name(pm_market_id)}__{run_id}"
    kx_name = f"{_safe_name(kx_ticker)}__{run_id}"

    pm_raw_path = PROJECT_ROOT / "data" / "websocket_poly" / f"raw_market_price_depth__{pm_name}.jsonl"
    pm_events_path = PROJECT_ROOT / "data" / "websocket_poly" / f"events_market_price_depth__{pm_name}.jsonl"
    kx_raw_path = PROJECT_ROOT / "data" / "websocket_kalshi" / f"raw_price_depth__{kx_name}.jsonl"
    kx_events_path = PROJECT_ROOT / "data" / "websocket_kalshi" / f"events_price_depth__{kx_name}.jsonl"
    sample_log_path = PROJECT_ROOT / "data" / f"websocket_pair_price_depth_health__{run_id}.jsonl"

    health_config = load_health_config_from_run_config(config_path=config_path)

    kalshi_headers = _resolve_kalshi_ws_headers()
    runtime = SharePriceRuntime(polymarket_token_yes=pm_yes, polymarket_token_no=pm_no)

    pm_raw_writer = JsonlWriter(pm_raw_path)
    pm_event_writer = JsonlWriter(pm_events_path)
    kx_raw_writer = JsonlWriter(kx_raw_path)
    kx_event_writer = JsonlWriter(kx_events_path)
    sample_writer = JsonlWriter(sample_log_path)

    polymarket_collector: Optional[PolymarketWsCollector] = None
    kalshi_collector: Optional[KalshiWsCollector] = None
    stats: Dict[str, Any] = {}

    try:
        polymarket_collector = PolymarketWsCollector(
            token_yes=pm_yes,
            token_no=pm_no,
            custom_feature_enabled=bool(args.custom_feature_enabled),
            raw_writer=pm_raw_writer,
            event_writer=pm_event_writer,
            health_config=health_config,
            on_event=runtime.apply_polymarket_event,
        )
        kalshi_collector = KalshiWsCollector(
            market_ticker=kx_ticker,
            channels=["ticker", "trade"],
            headers=kalshi_headers,
            raw_writer=kx_raw_writer,
            event_writer=kx_event_writer,
            health_config=health_config,
            on_event=runtime.apply_kalshi_event,
        )

        stats = asyncio.run(
            _run_for_duration(
                kalshi_collector=kalshi_collector,
                polymarket_collector=polymarket_collector,
                runtime=runtime,
                sample_writer=sample_writer,
                duration_seconds=int(args.duration_seconds),
            )
        )
    finally:
        for writer in (pm_raw_writer, pm_event_writer, kx_raw_writer, kx_event_writer, sample_writer):
            try:
                writer.close()
            except Exception:
                pass

    if polymarket_collector is None or kalshi_collector is None:
        raise RuntimeError("Collectors were not initialized.")

    session_ended = datetime.now(timezone.utc)
    summary = {
        "run_id": run_id,
        "session_started_at": session_started.isoformat(),
        "session_ended_at": session_ended.isoformat(),
        "duration_seconds_requested": int(args.duration_seconds),
        "selected_markets": {
            "kalshi": selection.get("kalshi"),
            "polymarket": selection.get("polymarket"),
        },
        "polymarket_custom_feature_enabled": bool(args.custom_feature_enabled),
        "counts": {
            "samples": int(stats.get("samples", 0)),
            "can_trade_true_samples": int(stats.get("can_trade_true_samples", 0)),
            "can_trade_false_samples": int(stats.get("can_trade_false_samples", 0)),
            "kalshi_raw_messages": int(kalshi_collector.message_count),
            "polymarket_raw_messages": int(polymarket_collector.message_count),
        },
        "last_sample": stats.get("last_sample"),
        "output_files": {
            "sample_log": str(sample_log_path),
            "kalshi_raw": str(kx_raw_path),
            "kalshi_events": str(kx_events_path),
            "polymarket_raw": str(pm_raw_path),
            "polymarket_events": str(pm_events_path),
        },
        "generated_at": utc_now_iso(),
    }

    summary_path = PROJECT_ROOT / "data" / f"websocket_pair_price_depth_health_summary__{run_id}.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print("Pair price/depth/health logger complete")
    print(f"Run ID: {run_id}")
    print(f"Polymarket market: {pm_slug} ({pm_market_id})")
    print(f"Kalshi market: {kx_ticker}")
    print(f"Samples written: {summary['counts']['samples']}")
    print(f"Sample log: {sample_log_path}")
    print(f"Summary: {summary_path}")


if __name__ == "__main__":
    main()
