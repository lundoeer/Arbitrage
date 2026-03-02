"""CLI argument parser for the arbitrage engine."""

from __future__ import annotations

import argparse


def build_parser() -> argparse.ArgumentParser:
    """Construct the argument parser for the production arbitrage engine."""
    parser = argparse.ArgumentParser(
        description="Run production websocket arbitrage engine (optional logging)."
    )
    parser.add_argument(
        "--duration-seconds",
        type=int,
        default=0,
        help="Run duration. 0 means run until interrupted.",
    )
    parser.add_argument("--config", default="config/run_config.json")
    parser.add_argument("--discovery-output", default="data/market_discovery_latest.json")
    parser.add_argument("--pair-cache", default="data/market_pair_cache.json")
    parser.add_argument("--skip-discovery", action="store_true")
    parser.add_argument(
        "--market-setup-file",
        default="",
        help="Manual setup file path. When provided, discovery is bypassed (hard override).",
    )
    parser.add_argument(
        "--market-setup-strict",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Fail on unknown keys in manual market setup file (default: true).",
    )
    parser.add_argument(
        "--kalshi-channels",
        default="ticker,orderbook_delta",
        help="Comma-separated Kalshi WS channels.",
    )
    parser.add_argument("--decision-poll-seconds", type=float, default=0.2)
    parser.add_argument(
        "--custom-feature-enabled",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Enable Polymarket custom websocket features in subscribe payload (default: true).",
    )
    parser.add_argument(
        "--log-raw-events",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Enable raw and normalized event logging to data/websocket_* folders (default: false).",
    )
    parser.add_argument(
        "--log-runtime-memory",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Enable SharePriceRuntime memory snapshots to data/websocket_share_price_runtime__*.jsonl.",
    )
    parser.add_argument(
        "--log-summary",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Enable final run summary JSON output even when other logging is disabled (default: false).",
    )
    parser.add_argument(
        "--log-decisions",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Enable per-sample decision logging to data/decision_log__*.jsonl (default: false).",
    )
    parser.add_argument(
        "--log-buy-decisions",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Enable buy-only decision logging (can_trade=true) to data/buy_decision_log__*.jsonl (default: false).",
    )
    parser.add_argument(
        "--log-buy-execution",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Enable buy execution logging to data/buy_execution_log__*.jsonl (default: false).",
    )
    parser.add_argument(
        "--log-edge-snapshots",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Enable 2s edge snapshot logging to data/gross_edge_snapshot__*.jsonl (default: false).",
    )
    parser.add_argument(
        "--edge-snapshot-poll-seconds",
        type=float,
        default=2.0,
        help="Edge snapshot poll interval seconds when --log-edge-snapshots is on (default: 2.0).",
    )
    parser.add_argument(
        "--runtime-memory-poll-seconds",
        type=float,
        default=1.0,
        help="Memory snapshot poll interval seconds when --log-runtime-memory is on (default: 1.0).",
    )
    parser.add_argument(
        "--enable-buy-execution",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Enable live buy execution when can_trade and FSM permits (default from config.buy_execution.enabled).",
    )
    parser.add_argument(
        "--buy-execution-cooldown-ms",
        type=int,
        default=None,
        help="Cooldown after each submit attempt before FSM re-arms (default from config.buy_execution.cooldown_ms).",
    )
    parser.add_argument(
        "--max-buy-execution-attempts",
        type=int,
        default=None,
        help=(
            "Maximum buy execution attempts per full engine run "
            "(default from config.buy_execution.max_attempts_per_run). 0 means unlimited."
        ),
    )
    parser.add_argument(
        "--enable-position-monitoring",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Enable position monitoring (REST polling + optional user position streams).",
    )
    parser.add_argument(
        "--polymarket-user-ws-enabled",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Enable Polymarket user websocket ingestion for position monitoring (default: true).",
    )
    parser.add_argument(
        "--kalshi-market-positions-ws-enabled",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Enable Kalshi market_positions websocket ingestion for position monitoring (default: true).",
    )
    parser.add_argument(
        "--kalshi-user-orders-ws-enabled",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Enable Kalshi user_orders websocket ingestion for order-state monitoring (default: true).",
    )
    parser.add_argument(
        "--position-polymarket-poll-seconds",
        type=float,
        default=None,
        help="Polymarket positions poll interval seconds (default: 10.0).",
    )
    parser.add_argument(
        "--position-kalshi-poll-seconds",
        type=float,
        default=None,
        help="Kalshi positions poll interval seconds (default: 20.0).",
    )
    parser.add_argument(
        "--position-polymarket-orders-poll-seconds",
        type=float,
        default=None,
        help="Polymarket orders poll interval seconds (default: 10.0).",
    )
    parser.add_argument(
        "--position-kalshi-orders-poll-seconds",
        type=float,
        default=None,
        help="Kalshi orders poll interval seconds (default: 20.0).",
    )
    parser.add_argument(
        "--position-loop-sleep-seconds",
        type=float,
        default=None,
        help="Internal sleep cadence for position reconcile loop (default: 0.2).",
    )
    parser.add_argument(
        "--log-positions",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Enable position monitoring event/reconcile logging to data/position_monitoring_log__*.jsonl.",
    )
    parser.add_argument(
        "--log-account-snapshots",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "Enable account/portfolio boundary snapshot logging (run/market start+end) "
            "to data/account_portfolio_snapshot_log__*.jsonl (default: true)."
        ),
    )
    return parser
