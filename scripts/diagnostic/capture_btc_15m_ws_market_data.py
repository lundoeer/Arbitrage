#!/usr/bin/env python3
"""
Capture realtime BTC 15m market data via websocket for Polymarket and Kalshi.

Outputs:
- Raw websocket frames in JSONL (one file per venue)
- Normalized market-data events in JSONL (one file per venue)
- Session summary JSON
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

from scripts.common.kalshi_auth import resolve_kalshi_ws_headers as _resolve_kalshi_ws_headers  # noqa: E402
from scripts.common.market_selection import (  # noqa: E402
    load_selected_markets as _load_selected_markets,
    safe_name as _safe_name,
)
from scripts.common.normalized_books import NormalizedBookRuntime  # noqa: E402
from scripts.common.run_config import health_config_to_dict, load_health_config_from_run_config  # noqa: E402
from scripts.common.ws_collectors import KalshiWsCollector, PolymarketWsCollector  # noqa: E402
from scripts.common.ws_transport import JsonlWriter, now_ms, utc_now_iso  # noqa: E402


async def _run_collectors_for_duration(
    *,
    kalshi_collector: KalshiWsCollector,
    polymarket_collector: PolymarketWsCollector,
    book_runtime: NormalizedBookRuntime,
    duration_seconds: int,
    health_writer: JsonlWriter,
    health_poll_seconds: float,
    executable_feed_writer: JsonlWriter,
    executable_feed_poll_seconds: float,
) -> Dict[str, Any]:
    gate_stats: Dict[str, Any] = {
        "samples": 0,
        "can_trade_true_samples": 0,
        "can_trade_false_samples": 0,
        "kalshi_transport_blocked_samples": 0,
        "kalshi_market_data_blocked_samples": 0,
        "polymarket_transport_blocked_samples": 0,
        "polymarket_market_data_blocked_samples": 0,
        "both_decision_blocked_samples": 0,
        "last_gate_state": None,
    }
    feed_stats: Dict[str, Any] = {
        "samples": 0,
        "last_feed": None,
    }

    async def _health_gate_loop(stop: asyncio.Event) -> None:
        poll_s = max(0.2, float(health_poll_seconds))
        while not stop.is_set():
            now_epoch_ms = now_ms()
            k = kalshi_collector.health_snapshot(now_epoch_ms=now_epoch_ms)
            p = polymarket_collector.health_snapshot(now_epoch_ms=now_epoch_ms)
            k_decision_ok = bool(k.get("decision_ok"))
            p_decision_ok = bool(p.get("decision_ok"))
            can_trade = bool(k_decision_ok and p_decision_ok)

            gate_stats["samples"] += 1
            if can_trade:
                gate_stats["can_trade_true_samples"] += 1
            else:
                gate_stats["can_trade_false_samples"] += 1
            if not bool(k.get("transport_ok")):
                gate_stats["kalshi_transport_blocked_samples"] += 1
            if not bool(k.get("market_data_ok")):
                gate_stats["kalshi_market_data_blocked_samples"] += 1
            if not bool(p.get("transport_ok")):
                gate_stats["polymarket_transport_blocked_samples"] += 1
            if not bool(p.get("market_data_ok")):
                gate_stats["polymarket_market_data_blocked_samples"] += 1
            if (not k_decision_ok) and (not p_decision_ok):
                gate_stats["both_decision_blocked_samples"] += 1

            gate_payload = {
                "ts": utc_now_iso(),
                "recv_ms": now_epoch_ms,
                "kind": "health_gate",
                "can_trade": can_trade,
                "hard_gate_state": "open" if can_trade else "blocked",
                "kalshi": k,
                "polymarket": p,
            }
            gate_stats["last_gate_state"] = gate_payload
            health_writer.write(gate_payload)
            await asyncio.sleep(poll_s)

    async def _executable_feed_loop(stop: asyncio.Event) -> None:
        poll_s = max(0.1, float(executable_feed_poll_seconds))
        while not stop.is_set():
            now_epoch_ms = now_ms()
            k = kalshi_collector.health_snapshot(now_epoch_ms=now_epoch_ms)
            p = polymarket_collector.health_snapshot(now_epoch_ms=now_epoch_ms)
            can_trade = bool(k.get("decision_ok")) and bool(p.get("decision_ok"))
            quotes = book_runtime.executable_price_feed(now_epoch_ms=now_epoch_ms)

            feed_payload = {
                "ts": utc_now_iso(),
                "recv_ms": now_epoch_ms,
                "kind": "executable_price_feed",
                "can_trade": can_trade,
                "hard_gate_state": "open" if can_trade else "blocked",
                "health_reasons": {
                    "kalshi": {
                        "transport_reasons": list(k.get("transport_reasons") or []),
                        "market_data_reasons": list(k.get("market_data_reasons") or []),
                    },
                    "polymarket": {
                        "transport_reasons": list(p.get("transport_reasons") or []),
                        "market_data_reasons": list(p.get("market_data_reasons") or []),
                    },
                },
                "quotes": quotes,
            }
            executable_feed_writer.write(feed_payload)
            feed_stats["samples"] += 1
            feed_stats["last_feed"] = feed_payload
            await asyncio.sleep(poll_s)

    stop_event = asyncio.Event()
    tasks = [
        asyncio.create_task(kalshi_collector.run(stop_event)),
        asyncio.create_task(polymarket_collector.run(stop_event)),
        asyncio.create_task(_health_gate_loop(stop_event)),
        asyncio.create_task(_executable_feed_loop(stop_event)),
    ]
    try:
        await asyncio.sleep(max(1, int(duration_seconds)))
    finally:
        stop_event.set()
        await asyncio.wait(tasks, timeout=10)
        for task in tasks:
            if not task.done():
                task.cancel()
    gate_stats["feed"] = feed_stats
    return gate_stats


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Capture Kalshi + Polymarket websocket market data for active BTC 15m markets."
    )
    parser.add_argument("--duration-seconds", type=int, default=120)
    parser.add_argument("--config", default="config/run_config.json")
    parser.add_argument("--discovery-output", default="data/market_discovery_latest.json")
    parser.add_argument("--pair-cache", default="data/market_pair_cache.json")
    parser.add_argument("--skip-discovery", action="store_true")
    parser.add_argument(
        "--kalshi-channels",
        default="ticker,orderbook_delta",
        help="Comma-separated Kalshi WS channels.",
    )
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

    pm_slug = selection["polymarket"]["event_slug"]
    pm_market_id = selection["polymarket"]["market_id"]
    pm_yes = selection["polymarket"]["token_yes"]
    pm_no = selection["polymarket"]["token_no"]
    kx_ticker = selection["kalshi"]["ticker"]

    pm_name = f"{_safe_name(pm_slug)}__{_safe_name(pm_market_id)}__{run_id}"
    kx_name = f"{_safe_name(kx_ticker)}__{run_id}"

    pm_raw_path = PROJECT_ROOT / "data" / "websocket_poly" / f"raw__{pm_name}.jsonl"
    pm_events_path = PROJECT_ROOT / "data" / "websocket_poly" / f"events__{pm_name}.jsonl"
    kx_raw_path = PROJECT_ROOT / "data" / "websocket_kalshi" / f"raw__{kx_name}.jsonl"
    kx_events_path = PROJECT_ROOT / "data" / "websocket_kalshi" / f"events__{kx_name}.jsonl"
    health_path = PROJECT_ROOT / "data" / f"websocket_health__{run_id}.jsonl"
    book_state_path = PROJECT_ROOT / "data" / f"websocket_book_state__{run_id}.json"
    executable_feed_path = PROJECT_ROOT / "data" / f"websocket_executable_feed__{run_id}.jsonl"

    kalshi_channels = [c.strip() for c in str(args.kalshi_channels).split(",") if c.strip()]
    if not kalshi_channels:
        raise RuntimeError("At least one Kalshi channel must be provided.")
    kalshi_headers = _resolve_kalshi_ws_headers()
    health_config = load_health_config_from_run_config(config_path=config_path)
    book_runtime = NormalizedBookRuntime(
        polymarket_token_yes=pm_yes,
        polymarket_token_no=pm_no,
        max_depth_levels=30,
    )

    pm_raw_writer = JsonlWriter(pm_raw_path)
    pm_event_writer = JsonlWriter(pm_events_path)
    kx_raw_writer = JsonlWriter(kx_raw_path)
    kx_event_writer = JsonlWriter(kx_events_path)
    health_writer = JsonlWriter(health_path)
    executable_feed_writer = JsonlWriter(executable_feed_path)

    polymarket_collector: Optional[PolymarketWsCollector] = None
    kalshi_collector: Optional[KalshiWsCollector] = None
    gate_stats: Dict[str, Any] = {
        "samples": 0,
        "can_trade_true_samples": 0,
        "can_trade_false_samples": 0,
        "kalshi_transport_blocked_samples": 0,
        "kalshi_market_data_blocked_samples": 0,
        "polymarket_transport_blocked_samples": 0,
        "polymarket_market_data_blocked_samples": 0,
        "both_decision_blocked_samples": 0,
        "last_gate_state": None,
    }

    try:
        polymarket_collector = PolymarketWsCollector(
            token_yes=pm_yes,
            token_no=pm_no,
            custom_feature_enabled=bool(args.custom_feature_enabled),
            raw_writer=pm_raw_writer,
            event_writer=pm_event_writer,
            health_config=health_config,
            on_event=book_runtime.apply_polymarket_event,
        )
        kalshi_collector = KalshiWsCollector(
            market_ticker=kx_ticker,
            channels=kalshi_channels,
            headers=kalshi_headers,
            raw_writer=kx_raw_writer,
            event_writer=kx_event_writer,
            health_config=health_config,
            on_event=book_runtime.apply_kalshi_event,
        )
        gate_stats = asyncio.run(
            _run_collectors_for_duration(
                kalshi_collector=kalshi_collector,
                polymarket_collector=polymarket_collector,
                book_runtime=book_runtime,
                duration_seconds=args.duration_seconds,
                health_writer=health_writer,
                health_poll_seconds=float(args.health_poll_seconds),
                executable_feed_writer=executable_feed_writer,
                executable_feed_poll_seconds=float(args.feed_poll_seconds),
            )
        )
    finally:
        for writer in (
            pm_raw_writer,
            pm_event_writer,
            kx_raw_writer,
            kx_event_writer,
            health_writer,
            executable_feed_writer,
        ):
            try:
                writer.close()
            except Exception:
                pass

    if polymarket_collector is None or kalshi_collector is None:
        raise RuntimeError("Websocket collectors were not initialized.")

    book_state = book_runtime.snapshot()
    book_state_path.write_text(json.dumps(book_state, indent=2), encoding="utf-8")

    session_ended = datetime.now(timezone.utc)
    summary = {
        "run_id": run_id,
        "session_started_at": session_started.isoformat(),
        "session_ended_at": session_ended.isoformat(),
        "duration_seconds_requested": int(args.duration_seconds),
        "discovery_generated_at": selection.get("discovery_generated_at"),
        "selected_markets": selection,
        "kalshi_channels": kalshi_channels,
        "polymarket_custom_feature_enabled": bool(args.custom_feature_enabled),
        "counts": {
            "kalshi_raw_messages": kalshi_collector.message_count,
            "kalshi_normalized_events": kalshi_collector.event_count,
            "polymarket_raw_messages": polymarket_collector.message_count,
            "polymarket_normalized_events": polymarket_collector.event_count,
        },
        "health": {
            "config": {
                **health_config_to_dict(health_config),
                "poll_seconds": max(0.2, float(args.health_poll_seconds)),
                "feed_poll_seconds": max(0.1, float(args.feed_poll_seconds)),
            },
            "gate_samples": {
                "samples": int(gate_stats.get("samples", 0)),
                "can_trade_true_samples": int(gate_stats.get("can_trade_true_samples", 0)),
                "can_trade_false_samples": int(gate_stats.get("can_trade_false_samples", 0)),
                "kalshi_transport_blocked_samples": int(gate_stats.get("kalshi_transport_blocked_samples", 0)),
                "kalshi_market_data_blocked_samples": int(gate_stats.get("kalshi_market_data_blocked_samples", 0)),
                "polymarket_transport_blocked_samples": int(
                    gate_stats.get("polymarket_transport_blocked_samples", 0)
                ),
                "polymarket_market_data_blocked_samples": int(
                    gate_stats.get("polymarket_market_data_blocked_samples", 0)
                ),
                "both_decision_blocked_samples": int(gate_stats.get("both_decision_blocked_samples", 0)),
                "executable_feed_samples": int((gate_stats.get("feed") or {}).get("samples", 0)),
            },
            "final_gate_state": gate_stats.get("last_gate_state"),
            "final_executable_feed": (gate_stats.get("feed") or {}).get("last_feed"),
            "final_stream_health": {
                "kalshi": kalshi_collector.health_snapshot(),
                "polymarket": polymarket_collector.health_snapshot(),
            },
        },
        "output_files": {
            "kalshi_raw": str(kx_raw_path),
            "kalshi_events": str(kx_events_path),
            "polymarket_raw": str(pm_raw_path),
            "polymarket_events": str(pm_events_path),
            "health_gate": str(health_path),
            "normalized_book_state": str(book_state_path),
            "executable_price_feed": str(executable_feed_path),
        },
        "generated_at": utc_now_iso(),
    }

    summary_path = PROJECT_ROOT / "data" / f"websocket_capture_summary__{run_id}.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print("Websocket market data capture complete")
    print(f"Run ID: {run_id}")
    print(f"Polymarket: {pm_slug} ({pm_market_id})")
    print(f"Kalshi: {kx_ticker}")
    print(f"Kalshi raw messages: {kalshi_collector.message_count}")
    print(f"Polymarket raw messages: {polymarket_collector.message_count}")
    print(
        "Hard gate (can_trade) false samples: "
        f"{summary['health']['gate_samples']['can_trade_false_samples']} / "
        f"{summary['health']['gate_samples']['samples']}"
    )
    print(f"Summary: {summary_path}")


if __name__ == "__main__":
    main()
