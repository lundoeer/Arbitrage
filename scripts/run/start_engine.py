#!/usr/bin/env python3
"""
Production websocket engine for active BTC 15m pair.

Flow:
- Load configuration and CLI arguments
- Setup loggers and API/Websocket clients
- Execute the Core ArbitrageEngine instance
"""

from __future__ import annotations

import asyncio
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.run.engine_cli import build_parser
from scripts.common.engine_logger import EngineLogger
from scripts.common.engine_setup import (
    build_buy_execution_clients,
    build_position_components,
)
from scripts.common.kalshi_auth import resolve_kalshi_ws_headers
from scripts.common.market_selection import load_selected_markets, safe_name
from scripts.common.position_polling import capture_account_portfolio_snapshot, PositionReconcileLoopConfig
from scripts.common.run_config import (
    load_buy_execution_runtime_config_from_run_config,
    load_decision_config_from_run_config,
    load_health_config_from_run_config,
    load_position_monitoring_runtime_config_from_run_config,
    health_config_to_dict,
    decision_config_to_dict,
    buy_execution_runtime_config_to_dict,
    position_monitoring_runtime_config_to_dict,
)
from scripts.common.ws_collectors import KalshiWsCollector, PolymarketWsCollector
from scripts.common.ws_transport import NullWriter, JsonlWriter, now_ms
from scripts.run.arbitrage_engine import ArbitrageEngine


def _parse_iso_to_epoch_ms(value: Any) -> Optional[int]:
    raw = str(value or "").strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp() * 1000)


def main() -> None:
    args = build_parser().parse_args()
    load_dotenv(dotenv_path=".env", override=False)
    config_path = Path(args.config)

    session_started = datetime.now(timezone.utc)
    run_id = session_started.strftime("%Y%m%dT%H%M%SZ")

    kalshi_channels = [c.strip() for c in str(args.kalshi_channels).split(",") if c.strip()]
    if not kalshi_channels:
        raise RuntimeError("At least one Kalshi channel must be provided.")

    # 1. Load Configurations
    health_config = load_health_config_from_run_config(config_path=config_path)
    decision_config = load_decision_config_from_run_config(config_path=config_path)
    buy_execution_config = load_buy_execution_runtime_config_from_run_config(config_path=config_path)
    position_monitoring_config = load_position_monitoring_runtime_config_from_run_config(config_path=config_path)

    buy_execution_requested = (
        bool(args.enable_buy_execution)
        if args.enable_buy_execution is not None
        else bool(buy_execution_config.enabled)
    )
    buy_execution_cooldown_ms = (
        max(0, int(args.buy_execution_cooldown_ms))
        if args.buy_execution_cooldown_ms is not None
        else max(0, int(buy_execution_config.cooldown_ms))
    )
    buy_execution_max_attempts = (
        max(0, int(args.max_buy_execution_attempts))
        if args.max_buy_execution_attempts is not None
        else max(0, int(buy_execution_config.max_attempts_per_run))
    )
    buy_execution_parallel_leg_timeout_ms = max(100, int(buy_execution_config.parallel_leg_timeout_ms))

    position_monitoring_requested = (
        bool(args.enable_position_monitoring)
        if args.enable_position_monitoring is not None
        else bool(position_monitoring_config.enabled)
    )
    polymarket_user_ws_enabled = (
        bool(args.polymarket_user_ws_enabled)
        if args.polymarket_user_ws_enabled is not None
        else bool(position_monitoring_config.polymarket_user_ws_enabled)
    )
    kalshi_market_positions_ws_enabled = (
        bool(args.kalshi_market_positions_ws_enabled)
        if args.kalshi_market_positions_ws_enabled is not None
        else bool(position_monitoring_config.kalshi_market_positions_ws_enabled)
    )
    kalshi_user_orders_ws_enabled = (
        bool(args.kalshi_user_orders_ws_enabled)
        if args.kalshi_user_orders_ws_enabled is not None
        else bool(position_monitoring_config.kalshi_user_orders_ws_enabled)
    )
    position_polymarket_poll_seconds = (
        max(0.2, float(args.position_polymarket_poll_seconds))
        if args.position_polymarket_poll_seconds is not None
        else max(0.2, float(position_monitoring_config.polymarket_poll_seconds))
    )
    position_kalshi_poll_seconds = (
        max(0.2, float(args.position_kalshi_poll_seconds))
        if args.position_kalshi_poll_seconds is not None
        else max(0.2, float(position_monitoring_config.kalshi_poll_seconds))
    )
    position_polymarket_orders_poll_seconds = (
        max(0.2, float(args.position_polymarket_orders_poll_seconds))
        if args.position_polymarket_orders_poll_seconds is not None
        else max(0.2, float(position_monitoring_config.polymarket_orders_poll_seconds))
    )
    position_kalshi_orders_poll_seconds = (
        max(0.2, float(args.position_kalshi_orders_poll_seconds))
        if args.position_kalshi_orders_poll_seconds is not None
        else max(0.2, float(position_monitoring_config.kalshi_orders_poll_seconds))
    )
    position_loop_sleep_seconds = (
        max(0.05, float(args.position_loop_sleep_seconds))
        if args.position_loop_sleep_seconds is not None
        else max(0.05, float(position_monitoring_config.loop_sleep_seconds))
    )
    position_reconcile_config = PositionReconcileLoopConfig(
        polymarket_poll_seconds=position_polymarket_poll_seconds,
        kalshi_poll_seconds=position_kalshi_poll_seconds,
        polymarket_orders_poll_seconds=position_polymarket_orders_poll_seconds,
        kalshi_orders_poll_seconds=position_kalshi_orders_poll_seconds,
        loop_sleep_seconds=position_loop_sleep_seconds,
    )

    account_snapshot_logging_enabled = bool(args.log_account_snapshots)

    if buy_execution_requested:
        print("Buy execution requested; clients will initialize per discovered market segment.")
    if position_monitoring_requested:
        print("Position monitoring requested; adapters will initialize per discovered market segment.")
    if account_snapshot_logging_enabled:
        print("Account snapshot logging enabled; boundary snapshots will be written per run/market.")

    # 2. Setup Central Logger Context
    logger = EngineLogger(run_id=run_id, project_root=PROJECT_ROOT)
    logger.setup_writers(
        log_decisions=bool(args.log_decisions),
        log_buy_decisions=bool(args.log_buy_decisions),
        log_buy_execution=bool(args.log_buy_execution),
        log_positions=bool(args.log_positions),
        log_raw_events=bool(args.log_raw_events),
        log_account_snapshots=bool(args.log_account_snapshots),
        log_edge_snapshots=bool(args.log_edge_snapshots),
        log_runtime_memory=bool(args.log_runtime_memory),
    )

    run_start_deadline_ms = None if int(args.duration_seconds) <= 0 else int(now_ms() + (int(args.duration_seconds) * 1000))
    
    # 3. Instantiate the Core Engine
    engine = ArbitrageEngine(
        logger=logger,
        health_config=health_config,
        decision_config=decision_config,
        buy_execution_config=buy_execution_config,
        position_monitoring_config=position_monitoring_config,
        position_reconcile_config=position_reconcile_config,
        buy_execution_requested=buy_execution_requested,
        position_monitoring_requested=position_monitoring_requested,
        account_snapshot_logging_enabled=account_snapshot_logging_enabled,
        buy_execution_cooldown_ms=buy_execution_cooldown_ms,
        buy_execution_max_attempts=buy_execution_max_attempts,
        buy_execution_parallel_leg_timeout_ms=buy_execution_parallel_leg_timeout_ms,
        polymarket_user_ws_enabled=polymarket_user_ws_enabled,
        kalshi_market_positions_ws_enabled=kalshi_market_positions_ws_enabled,
        kalshi_user_orders_ws_enabled=kalshi_user_orders_ws_enabled,
        kalshi_channels=kalshi_channels,
        args=args,
    )

    try:
        while True:
            loop_now_ms = now_ms()
            if run_start_deadline_ms is not None and loop_now_ms >= run_start_deadline_ms:
                break

            run_discovery_first = True if engine.segment_index > 0 else (not bool(args.skip_discovery))
            try:
                selection = load_selected_markets(
                    config_path=config_path,
                    discovery_output=Path(args.discovery_output),
                    pair_cache_path=Path(args.pair_cache),
                    run_discovery_first=run_discovery_first,
                )
            except Exception as exc:
                print(f"Market discovery failed: {exc}")
                if run_start_deadline_ms is not None and now_ms() >= run_start_deadline_ms:
                    break
                time.sleep(1.0)
                continue

            market_window_end_epoch_ms = _parse_iso_to_epoch_ms(
                selection.get("polymarket", {}).get("window_end")
                or selection.get("kalshi", {}).get("window_end")
            )
            if market_window_end_epoch_ms is None:
                print("Discovery returned market without window_end; retrying discovery.")
                if run_start_deadline_ms is not None and now_ms() >= run_start_deadline_ms:
                    break
                time.sleep(1.0)
                continue

            segment_now_ms = now_ms()
            seconds_to_market_end = int(max(0, (int(market_window_end_epoch_ms) - int(segment_now_ms) + 999) // 1000))
            if seconds_to_market_end <= 0:
                time.sleep(0.5)
                continue

            remaining_seconds = None
            if run_start_deadline_ms is not None:
                remaining_seconds = int(max(0, (int(run_start_deadline_ms) - int(segment_now_ms) + 999) // 1000))
                if remaining_seconds <= 0:
                    break
            segment_duration_seconds = (
                seconds_to_market_end if remaining_seconds is None else min(seconds_to_market_end, remaining_seconds)
            )
            if segment_duration_seconds <= 0:
                break

            engine.run_segment(
                selection=selection,
                market_window_end_epoch_ms=market_window_end_epoch_ms,
                segment_duration_seconds=segment_duration_seconds,
            )

        # Post-run End of Loop Execution
        if position_monitoring_requested or account_snapshot_logging_enabled:
            engine.capture_run_end_snapshot()

    except KeyboardInterrupt:
        pass
    finally:
        logger.close()

    session_ended = datetime.now(timezone.utc)
    engine.print_summary(session_started, session_ended, args)


if __name__ == "__main__":
    main()
