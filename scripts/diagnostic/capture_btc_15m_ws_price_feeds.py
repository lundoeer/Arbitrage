#!/usr/bin/env python3
"""
Capture share price feeds for the active BTC 15m pair:
- Polymarket CLOB market websocket (share prices from market channel)
- Kalshi websocket ticker + trade channels
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
from scripts.common.run_config import health_config_to_dict, load_health_config_from_run_config  # noqa: E402
from scripts.common.ws_collectors import KalshiWsCollector, PolymarketWsCollector  # noqa: E402
from scripts.common.ws_transport import JsonlWriter, now_ms, utc_now_iso  # noqa: E402
from scripts.common.utils import as_dict as _as_dict  # noqa: E402


async def _run_collectors_for_duration(
    *,
    kalshi_collector: KalshiWsCollector,
    polymarket_collector: PolymarketWsCollector,
    runtime: SharePriceRuntime,
    duration_seconds: int,
    health_writer: JsonlWriter,
    feed_writer: JsonlWriter,
    health_poll_seconds: float,
    feed_poll_seconds: float,
) -> Dict[str, Any]:
    stats: Dict[str, Any] = {
        "samples": 0,
        "can_trade_true_samples": 0,
        "can_trade_false_samples": 0,
        "last_gate_state": None,
        "feed_samples": 0,
        "decision_ready_true_samples": 0,
        "decision_ready_false_samples": 0,
        "last_feed": None,
    }

    async def _health_loop(stop: asyncio.Event) -> None:
        poll_s = max(0.2, float(health_poll_seconds))
        while not stop.is_set():
            now_epoch_ms = now_ms()
            k = kalshi_collector.health_snapshot(now_epoch_ms=now_epoch_ms)
            p = polymarket_collector.health_snapshot(now_epoch_ms=now_epoch_ms)
            can_trade = bool(k.get("decision_ok")) and bool(p.get("decision_ok"))
            stats["samples"] += 1
            if can_trade:
                stats["can_trade_true_samples"] += 1
            else:
                stats["can_trade_false_samples"] += 1
            payload = {
                "ts": utc_now_iso(),
                "recv_ms": now_epoch_ms,
                "kind": "health_gate",
                "can_trade": can_trade,
                "hard_gate_state": "open" if can_trade else "blocked",
                "kalshi": k,
                "polymarket_market": p,
            }
            stats["last_gate_state"] = payload
            health_writer.write(payload)
            await asyncio.sleep(poll_s)

    async def _feed_loop(stop: asyncio.Event) -> None:
        poll_s = max(0.1, float(feed_poll_seconds))
        while not stop.is_set():
            now_epoch_ms = now_ms()
            k = kalshi_collector.health_snapshot(now_epoch_ms=now_epoch_ms)
            p = polymarket_collector.health_snapshot(now_epoch_ms=now_epoch_ms)
            health_can_trade = bool(k.get("decision_ok")) and bool(p.get("decision_ok"))
            prices = runtime.snapshot(now_epoch_ms=now_epoch_ms)
            quote_sanity = _build_quote_sanity_and_canonical(_as_dict(prices.get("quotes")))
            decision_ready = bool(_as_dict(quote_sanity.get("canonical")).get("decision_ready"))
            can_trade = health_can_trade and decision_ready

            if decision_ready:
                stats["decision_ready_true_samples"] += 1
            else:
                stats["decision_ready_false_samples"] += 1

            payload = {
                "ts": utc_now_iso(),
                "recv_ms": now_epoch_ms,
                "kind": "share_price_feed",
                "can_trade": can_trade,
                "health_can_trade": health_can_trade,
                "decision_ready": decision_ready,
                "hard_gate_state": "open" if can_trade else "blocked",
                "health_reasons": {
                    "kalshi": {
                        "transport_reasons": list(k.get("transport_reasons") or []),
                        "market_data_reasons": list(k.get("market_data_reasons") or []),
                    },
                    "polymarket_market": {
                        "transport_reasons": list(p.get("transport_reasons") or []),
                        "market_data_reasons": list(p.get("market_data_reasons") or []),
                    },
                },
                "quote_sanity": quote_sanity,
                "prices": prices,
            }
            feed_writer.write(payload)
            stats["feed_samples"] += 1
            stats["last_feed"] = payload
            await asyncio.sleep(poll_s)

    stop_event = asyncio.Event()
    tasks = [
        asyncio.create_task(kalshi_collector.run(stop_event)),
        asyncio.create_task(polymarket_collector.run(stop_event)),
        asyncio.create_task(_health_loop(stop_event)),
        asyncio.create_task(_feed_loop(stop_event)),
    ]
    try:
        await asyncio.sleep(max(1, int(duration_seconds)))
    finally:
        stop_event.set()
        await asyncio.wait(tasks, timeout=10)
        for task in tasks:
            if not task.done():
                task.cancel()
    return stats


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Capture share prices from Polymarket market channel + Kalshi ticker/trade."
    )
    parser.add_argument("--duration-seconds", type=int, default=120)
    parser.add_argument("--config", default="config/run_config.json")
    parser.add_argument("--discovery-output", default="data/market_discovery_latest.json")
    parser.add_argument("--pair-cache", default="data/market_pair_cache.json")
    parser.add_argument("--skip-discovery", action="store_true")
    parser.add_argument("--health-poll-seconds", type=float, default=1.0)
    parser.add_argument("--feed-poll-seconds", type=float, default=0.5)
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
    pm_raw_path = PROJECT_ROOT / "data" / "websocket_poly" / f"raw_market_price__{pm_name}.jsonl"
    pm_events_path = PROJECT_ROOT / "data" / "websocket_poly" / f"events_market_price__{pm_name}.jsonl"
    kx_raw_path = PROJECT_ROOT / "data" / "websocket_kalshi" / f"raw_price__{kx_name}.jsonl"
    kx_events_path = PROJECT_ROOT / "data" / "websocket_kalshi" / f"events_price__{kx_name}.jsonl"
    health_path = PROJECT_ROOT / "data" / f"websocket_share_price_health__{run_id}.jsonl"
    feed_path = PROJECT_ROOT / "data" / f"websocket_share_price_feed__{run_id}.jsonl"

    health_config = load_health_config_from_run_config(config_path=config_path)
    kalshi_headers = _resolve_kalshi_ws_headers()
    runtime = SharePriceRuntime(polymarket_token_yes=pm_yes, polymarket_token_no=pm_no)

    pm_raw_writer = JsonlWriter(pm_raw_path)
    pm_event_writer = JsonlWriter(pm_events_path)
    kx_raw_writer = JsonlWriter(kx_raw_path)
    kx_event_writer = JsonlWriter(kx_events_path)
    health_writer = JsonlWriter(health_path)
    feed_writer = JsonlWriter(feed_path)

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
            _run_collectors_for_duration(
                kalshi_collector=kalshi_collector,
                polymarket_collector=polymarket_collector,
                runtime=runtime,
                duration_seconds=int(args.duration_seconds),
                health_writer=health_writer,
                feed_writer=feed_writer,
                health_poll_seconds=float(args.health_poll_seconds),
                feed_poll_seconds=float(args.feed_poll_seconds),
            )
        )
    finally:
        for writer in (pm_raw_writer, pm_event_writer, kx_raw_writer, kx_event_writer, health_writer, feed_writer):
            try:
                writer.close()
            except Exception:
                pass

    if polymarket_collector is None or kalshi_collector is None:
        raise RuntimeError("Collectors were not initialized.")

    final_prices = runtime.snapshot(now_epoch_ms=now_ms())
    quotes = _as_dict(final_prices.get("quotes"))
    legs = _as_dict(quotes.get("legs"))
    poly_yes = _as_dict(legs.get("polymarket_yes"))
    kx_yes = _as_dict(legs.get("kalshi_yes"))
    if poly_yes.get("best_bid") is None and poly_yes.get("best_ask") is None:
        raise RuntimeError("No Polymarket market-channel share quote captured.")
    if kx_yes.get("best_bid") is None and kx_yes.get("best_ask") is None:
        raise RuntimeError("No Kalshi ticker share quote captured.")

    session_ended = datetime.now(timezone.utc)
    summary = {
        "run_id": run_id,
        "session_started_at": session_started.isoformat(),
        "session_ended_at": session_ended.isoformat(),
        "duration_seconds_requested": int(args.duration_seconds),
        "discovery_generated_at": selection.get("discovery_generated_at"),
        "selected_markets": {
            "kalshi": selection.get("kalshi"),
            "polymarket": selection.get("polymarket"),
        },
        "counts": {
            "kalshi_raw_messages": kalshi_collector.message_count,
            "kalshi_normalized_events": kalshi_collector.event_count,
            "polymarket_raw_messages": polymarket_collector.message_count,
            "polymarket_normalized_events": polymarket_collector.event_count,
            "share_price_feed_samples": int(stats.get("feed_samples", 0)),
            "decision_ready_true_samples": int(stats.get("decision_ready_true_samples", 0)),
            "decision_ready_false_samples": int(stats.get("decision_ready_false_samples", 0)),
        },
        "polymarket_custom_feature_enabled": bool(args.custom_feature_enabled),
        "health": {
            "config": {
                **health_config_to_dict(health_config),
                "poll_seconds": max(0.2, float(args.health_poll_seconds)),
                "feed_poll_seconds": max(0.1, float(args.feed_poll_seconds)),
            },
            "gate_samples": {
                "samples": int(stats.get("samples", 0)),
                "can_trade_true_samples": int(stats.get("can_trade_true_samples", 0)),
                "can_trade_false_samples": int(stats.get("can_trade_false_samples", 0)),
            },
            "final_gate_state": stats.get("last_gate_state"),
            "final_share_price_feed": stats.get("last_feed"),
            "final_stream_health": {
                "kalshi": kalshi_collector.health_snapshot(),
                "polymarket_market": polymarket_collector.health_snapshot(),
            },
        },
        "final_prices": final_prices,
        "output_files": {
            "kalshi_raw": str(kx_raw_path),
            "kalshi_events": str(kx_events_path),
            "polymarket_raw": str(pm_raw_path),
            "polymarket_events": str(pm_events_path),
            "health_gate": str(health_path),
            "share_price_feed": str(feed_path),
        },
        "generated_at": utc_now_iso(),
    }

    summary_path = PROJECT_ROOT / "data" / f"websocket_share_price_capture_summary__{run_id}.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print("Websocket share price feed capture complete")
    print(f"Run ID: {run_id}")
    print(f"Polymarket market: {pm_slug} ({pm_market_id})")
    print(f"Kalshi market: {kx_ticker}")
    print(f"Polymarket raw messages: {polymarket_collector.message_count}")
    print(f"Kalshi raw messages: {kalshi_collector.message_count}")
    print(f"Summary: {summary_path}")


if __name__ == "__main__":
    main()
