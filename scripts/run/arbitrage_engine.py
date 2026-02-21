#!/usr/bin/env python3
"""
Production websocket engine for active BTC 15m pair.

Flow:
- collect
- normalize
- apply to in-memory runtime
- evaluate trade gating/decision

This entrypoint defaults to no raw/event/feed file logging.
Use --log-raw-events to enable raw+normalized event capture.
Use --log-summary for summary-only output.
Use --log-decisions / --log-buy-decisions for decision JSONL output.
"""

from __future__ import annotations

import asyncio
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.common.decision_runtime import DecisionRuntime, SharePriceRuntime  # noqa: E402
from scripts.common.api_transport import ApiTransport, RetryConfig  # noqa: E402
from scripts.common.buy_execution import (  # noqa: E402
    BuyExecutionClients,
    BuyExecutionPlan,
    BuyIdempotencyState,
    KalshiApiBuyClient,
    build_polymarket_api_buy_client_from_env,
    execute_cross_venue_buy,
)
from scripts.common.buy_fsm import BuyFsmRuntime, BuyFsmState  # noqa: E402
from scripts.common.kalshi_auth import resolve_kalshi_ws_headers  # noqa: E402
from scripts.common.market_selection import load_selected_markets, safe_name  # noqa: E402
from scripts.common.run_config import (  # noqa: E402
    BuyExecutionRuntimeConfig,
    buy_execution_runtime_config_to_dict,
    decision_config_to_dict,
    health_config_to_dict,
    load_buy_execution_runtime_config_from_run_config,
    load_decision_config_from_run_config,
    load_health_config_from_run_config,
)
from scripts.common.ws_collectors import KalshiWsCollector, PolymarketWsCollector  # noqa: E402
from scripts.common.ws_transport import JsonlWriter, NullWriter, now_ms, utc_now_iso  # noqa: E402
from scripts.common.utils import as_dict as _as_dict, as_float as _as_float  # noqa: E402
from scripts.common.edge_snapshots import build_edge_snapshot as _build_edge_snapshot  # noqa: E402
from scripts.run.engine_cli import build_parser as _build_parser  # noqa: E402




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


def _runtime_memory_snapshot(runtime: SharePriceRuntime, now_epoch_ms: int) -> Dict[str, Any]:
    books: Dict[str, Any] = {}
    for venue, outcomes in runtime.book_runtime.books.items():
        books[venue] = {}
        for outcome, book in outcomes.items():
            books[venue][outcome] = {
                "bids_levels": [[round(float(p), 6), round(float(s), 6)] for p, s in book.bids.sorted_levels(side="bid")],
                "asks_levels": [[round(float(p), 6), round(float(s), 6)] for p, s in book.asks.sorted_levels(side="ask")],
                "source_timestamp_ms": book.source_timestamp_ms,
                "recv_timestamp_ms": book.recv_timestamp_ms,
            }
    return {
        "books": books,
        "quotes": runtime.book_runtime.executable_price_feed(now_epoch_ms=now_epoch_ms),
    }






def _build_buy_execution_transport(*, buy_execution_config: BuyExecutionRuntimeConfig) -> ApiTransport:
    api_retry = buy_execution_config.api_retry
    if not bool(api_retry.enabled):
        return ApiTransport(timeout_seconds=10)

    methods = {"GET", "HEAD", "OPTIONS"}
    if bool(api_retry.include_post):
        methods.add("POST")
    retry_config = RetryConfig(
        max_attempts=max(1, int(api_retry.max_attempts)),
        base_backoff_seconds=max(0.0, float(api_retry.base_backoff_seconds)),
        jitter_ratio=max(0.0, float(api_retry.jitter_ratio)),
        retry_methods=frozenset(methods),
    )
    return ApiTransport(timeout_seconds=10, retry_config=retry_config)


def _build_buy_execution_clients(
    *,
    enable_buy_execution: bool,
    buy_execution_config: BuyExecutionRuntimeConfig,
) -> tuple[bool, BuyExecutionClients, list[str]]:
    if not bool(enable_buy_execution):
        return False, BuyExecutionClients(), []

    errors: list[str] = []
    polymarket_client = None
    kalshi_client = None
    transport = _build_buy_execution_transport(buy_execution_config=buy_execution_config)
    try:
        polymarket_client = build_polymarket_api_buy_client_from_env(transport=transport)
    except Exception as exc:
        errors.append(f"polymarket_client_init_failed:{type(exc).__name__}:{exc}")
    try:
        kalshi_client = KalshiApiBuyClient(transport=transport)
    except Exception as exc:
        errors.append(f"kalshi_client_init_failed:{type(exc).__name__}:{exc}")

    enabled = polymarket_client is not None and kalshi_client is not None and not errors
    if not enabled and not errors:
        errors.append("buy_clients_not_available")
    return enabled, BuyExecutionClients(polymarket=polymarket_client, kalshi=kalshi_client), errors










async def _run_engine(
    *,
    kalshi_collector: KalshiWsCollector,
    polymarket_collector: PolymarketWsCollector,
    runtime: SharePriceRuntime,
    duration_seconds: int,
    decision_poll_seconds: float,
    decision_config: Any,
    market_window_end_epoch_ms: Optional[int],
    market_context: Dict[str, Any],
    runtime_memory_writer: Optional[JsonlWriter],
    runtime_memory_poll_seconds: float,
    decision_writer: Optional[JsonlWriter],
    buy_decision_writer: Optional[JsonlWriter],
    buy_execution_writer: Optional[JsonlWriter],
    edge_snapshot_writer: Optional[JsonlWriter],
    edge_snapshot_poll_seconds: float,
    buy_execution_enabled: bool,
    buy_execution_clients: BuyExecutionClients,
    buy_idempotency_state: BuyIdempotencyState,
    buy_execution_cooldown_ms: int,
    buy_execution_max_attempts: int,
    buy_execution_attempt_state: Dict[str, int],
) -> Dict[str, Any]:
    buy_fsm = BuyFsmRuntime.initialize()
    cooldown_ms = max(0, int(buy_execution_cooldown_ms))
    max_attempts = max(0, int(buy_execution_max_attempts))
    if "attempts_used" not in buy_execution_attempt_state:
        buy_execution_attempt_state["attempts_used"] = 0
    stats: Dict[str, Any] = {
        "decision_samples": 0,
        "can_trade_true_samples": 0,
        "can_trade_false_samples": 0,
        "decision_ready_true_samples": 0,
        "decision_ready_false_samples": 0,
        "runtime_memory_samples": 0,
        "decision_logged_samples": 0,
        "buy_decision_logged_samples": 0,
        "edge_snapshot_samples": 0,
        "buy_execution_attempts": 0,
        "buy_execution_submitted": 0,
        "buy_execution_partially_submitted": 0,
        "buy_execution_rejected": 0,
        "buy_execution_skipped_idempotent": 0,
        "buy_execution_errors": 0,
        "buy_execution_disabled_signals": 0,
        "buy_execution_blocked_fsm_signals": 0,
        "buy_execution_blocked_max_attempts": 0,
        "last_decision": None,
        "last_buy_execution": None,
        "buy_fsm": buy_fsm.snapshot(),
    }

    async def _decision_loop(stop: asyncio.Event) -> None:
        poll_s = max(0.05, float(decision_poll_seconds))
        while not stop.is_set():
            now_epoch_ms = now_ms()
            buy_fsm.maybe_rearm(now_epoch_ms=now_epoch_ms)
            k_health = kalshi_collector.health_snapshot(now_epoch_ms=now_epoch_ms)
            p_health = polymarket_collector.health_snapshot(now_epoch_ms=now_epoch_ms)
            prices = runtime.snapshot(now_epoch_ms=now_epoch_ms)
            quotes = _as_dict(prices.get("quotes"))
            decision = DecisionRuntime.evaluate(
                kalshi_health=k_health,
                polymarket_health=p_health,
                quotes=quotes,
                market_context=market_context,
                decision_config=decision_config,
                now_epoch_ms=now_epoch_ms,
                market_window_end_epoch_ms=market_window_end_epoch_ms,
            )

            stats["decision_samples"] += 1
            if decision.can_trade:
                stats["can_trade_true_samples"] += 1
            else:
                stats["can_trade_false_samples"] += 1
            if decision.decision_ready:
                stats["decision_ready_true_samples"] += 1
            else:
                stats["decision_ready_false_samples"] += 1

            buy_execution_event: Optional[Dict[str, Any]] = None
            buy_fsm_before = buy_fsm.snapshot()
            if bool(decision.can_trade):
                attempts_used = int(buy_execution_attempt_state.get("attempts_used", 0))
                if max_attempts > 0 and attempts_used >= max_attempts:
                    stats["buy_execution_blocked_max_attempts"] += 1
                    buy_execution_event = {
                        "status": "blocked_max_attempts",
                        "reason": f"max_buy_execution_attempts_reached:{max_attempts}",
                        "attempts_used": attempts_used,
                    }
                elif not bool(buy_execution_enabled):
                    stats["buy_execution_disabled_signals"] += 1
                    buy_execution_event = {
                        "status": "skipped_disabled",
                        "reason": "buy_execution_not_enabled",
                    }
                elif not buy_fsm.can_accept_new_signal():
                    stats["buy_execution_blocked_fsm_signals"] += 1
                    buy_execution_event = {
                        "status": "blocked_fsm",
                        "reason": f"fsm_state={buy_fsm.state.value}",
                    }
                elif not isinstance(decision.execution_plan, dict):
                    stats["buy_execution_errors"] += 1
                    buy_execution_event = {
                        "status": "error",
                        "reason": "missing_execution_plan",
                        "execution_plan_reasons": list(decision.execution_plan_reasons),
                    }
                else:
                    plan_payload = dict(decision.execution_plan)
                    signal_id = str(plan_payload.get("signal_id") or "").strip()
                    if not signal_id:
                        stats["buy_execution_errors"] += 1
                        buy_execution_event = {
                            "status": "error",
                            "reason": "execution_plan_missing_signal_id",
                        }
                    else:
                        buy_fsm.begin_submission(
                            signal_id=signal_id,
                            now_epoch_ms=now_epoch_ms,
                            reason="decision_can_trade",
                        )
                        buy_execution_attempt_state["attempts_used"] = int(attempts_used + 1)
                        stats["buy_execution_attempts"] += 1
                        try:
                            plan = BuyExecutionPlan.from_dict(plan_payload)
                            result = execute_cross_venue_buy(
                                plan=plan,
                                clients=buy_execution_clients,
                                state=buy_idempotency_state,
                                now_epoch_ms=now_epoch_ms,
                            )
                            result_payload = result.to_dict()
                            buy_fsm.complete_submission(
                                result_payload=result_payload,
                                now_epoch_ms=now_ms(),
                                cooldown_ms=cooldown_ms,
                            )
                            stats["last_buy_execution"] = result_payload
                            status = str(result_payload.get("status") or "")
                            if status == "submitted":
                                stats["buy_execution_submitted"] += 1
                            elif status == "partially_submitted":
                                stats["buy_execution_partially_submitted"] += 1
                            elif status == "rejected":
                                stats["buy_execution_rejected"] += 1
                            elif status == "skipped_idempotent":
                                stats["buy_execution_skipped_idempotent"] += 1
                            else:
                                stats["buy_execution_errors"] += 1
                            buy_execution_event = {
                                "status": "executed",
                                "result": result_payload,
                            }
                        except Exception as exc:
                            error_payload = {
                                "signal_id": signal_id,
                                "status": "error",
                                "error": f"{type(exc).__name__}:{exc}",
                                "at_ms": now_ms(),
                            }
                            buy_fsm.fail_submission(
                                signal_id=signal_id,
                                error_payload=error_payload,
                                now_epoch_ms=now_ms(),
                                reason="submit_exception",
                            )
                            stats["buy_execution_errors"] += 1
                            stats["last_buy_execution"] = error_payload
                            buy_execution_event = {
                                "status": "error",
                                "error": error_payload,
                            }

            buy_fsm_after = buy_fsm.snapshot()
            decision_payload = {
                "can_trade": decision.can_trade,
                "health_can_trade": decision.health_can_trade,
                "decision_ready": decision.decision_ready,
                "buy_signal_ready": decision.buy_signal_ready,
                "hard_gate_state": decision.hard_gate_state,
                "health_reasons": decision.health_reasons,
                "quote_sanity": decision.quote_sanity,
                "execution_gate": decision.execution_gate,
                "buy_signal": decision.buy_signal,
                "execution_plan": decision.execution_plan,
                "execution_plan_reasons": decision.execution_plan_reasons,
                "gate_reasons": decision.gate_reasons,
                "buy_execution_enabled": bool(buy_execution_enabled),
                "buy_execution_max_attempts": int(max_attempts),
                "buy_execution_attempts_used": int(buy_execution_attempt_state.get("attempts_used", 0)),
                "buy_execution": buy_execution_event,
                "buy_fsm": buy_fsm_after,
            }
            stats["last_decision"] = decision_payload
            stats["buy_fsm"] = buy_fsm_after

            if buy_execution_writer is not None and buy_execution_event is not None:
                buy_execution_writer.write(
                    {
                        "ts": utc_now_iso(),
                        "recv_ms": now_epoch_ms,
                        "kind": "buy_execution",
                        "market": market_context,
                        "execution_plan": decision.execution_plan,
                        "execution_plan_reasons": decision.execution_plan_reasons,
                        "buy_execution_enabled": bool(buy_execution_enabled),
                        "buy_execution": buy_execution_event,
                        "buy_fsm_before": buy_fsm_before,
                        "buy_fsm_after": buy_fsm_after,
                    }
                )
            if decision_writer is not None:
                decision_writer.write(
                    {
                        "ts": utc_now_iso(),
                        "recv_ms": now_epoch_ms,
                        "kind": "decision",
                        "market": market_context,
                        **decision_payload,
                    }
                )
                stats["decision_logged_samples"] += 1
            if buy_decision_writer is not None and bool(decision.can_trade):
                buy_decision_writer.write(
                    {
                        "ts": utc_now_iso(),
                        "recv_ms": now_epoch_ms,
                        "kind": "buy_decision",
                        "market": market_context,
                        **decision_payload,
                    }
                )
                stats["buy_decision_logged_samples"] += 1
            await asyncio.sleep(poll_s)

    async def _runtime_memory_loop(stop: asyncio.Event) -> None:
        if runtime_memory_writer is None:
            return
        poll_s = max(0.2, float(runtime_memory_poll_seconds))
        while not stop.is_set():
            now_epoch_ms = now_ms()
            runtime_memory_writer.write(
                {
                    "ts": utc_now_iso(),
                    "recv_ms": now_epoch_ms,
                    "kind": "share_price_runtime_memory",
                    "memory": _runtime_memory_snapshot(runtime, now_epoch_ms),
                }
            )
            stats["runtime_memory_samples"] += 1
            await asyncio.sleep(poll_s)

    async def _edge_snapshot_loop(stop: asyncio.Event) -> None:
        if edge_snapshot_writer is None:
            return
        poll_s = max(0.2, float(edge_snapshot_poll_seconds))
        min_gross_edge_threshold = _as_float(
            _as_dict(_as_dict(decision_config).get("buy")).get("min_gross_edge_threshold")
        )
        if min_gross_edge_threshold is None:
            min_gross_edge_threshold = _as_float(
                getattr(getattr(decision_config, "buy", None), "min_gross_edge_threshold", None)
            )
        if min_gross_edge_threshold is None:
            min_gross_edge_threshold = 0.04

        market_duration_seconds = _as_float(
            _as_dict(_as_dict(decision_config).get("execution")).get("market_duration_seconds")
        )
        if market_duration_seconds is None:
            market_duration_seconds = _as_float(
                getattr(getattr(decision_config, "execution", None), "market_duration_seconds", None)
            )
        if market_duration_seconds is None:
            market_duration_seconds = 900.0

        while not stop.is_set():
            now_epoch_ms = now_ms()
            quotes = _as_dict(runtime.snapshot(now_epoch_ms=now_epoch_ms).get("quotes"))
            payload = _build_edge_snapshot(
                now_epoch_ms=now_epoch_ms,
                market_context=market_context,
                market_window_end_epoch_ms=market_window_end_epoch_ms,
                market_duration_seconds=int(market_duration_seconds),
                min_gross_edge_threshold=float(min_gross_edge_threshold),
                quotes=quotes,
            )
            edge_snapshot_writer.write(
                {
                    "ts": utc_now_iso(),
                    "recv_ms": now_epoch_ms,
                    "kind": "gross_edge_snapshot",
                    **payload,
                }
            )
            stats["edge_snapshot_samples"] += 1
            await asyncio.sleep(poll_s)

    stop_event = asyncio.Event()
    tasks = [
        asyncio.create_task(kalshi_collector.run(stop_event)),
        asyncio.create_task(polymarket_collector.run(stop_event)),
        asyncio.create_task(_decision_loop(stop_event)),
        asyncio.create_task(_runtime_memory_loop(stop_event)),
        asyncio.create_task(_edge_snapshot_loop(stop_event)),
    ]
    try:
        if int(duration_seconds) > 0:
            await asyncio.sleep(int(duration_seconds))
        else:
            while not stop_event.is_set():
                await asyncio.sleep(1.0)
    finally:
        stop_event.set()
        await asyncio.wait(tasks, timeout=10)
        for task in tasks:
            if not task.done():
                task.cancel()

    return stats






def main() -> None:
    args = _build_parser().parse_args()
    load_dotenv(dotenv_path=".env", override=False)
    config_path = Path(args.config)

    session_started = datetime.now(timezone.utc)
    run_id = session_started.strftime("%Y%m%dT%H%M%SZ")

    kalshi_channels = [c.strip() for c in str(args.kalshi_channels).split(",") if c.strip()]
    if not kalshi_channels:
        raise RuntimeError("At least one Kalshi channel must be provided.")

    health_config = load_health_config_from_run_config(config_path=config_path)
    decision_config = load_decision_config_from_run_config(config_path=config_path)
    buy_execution_config = load_buy_execution_runtime_config_from_run_config(config_path=config_path)
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
    buy_execution_enabled_last = False
    buy_execution_setup_errors: list[str] = []
    if bool(buy_execution_requested):
        print("Buy execution requested; clients will initialize per discovered market segment.")
    buy_idempotency_state = BuyIdempotencyState()
    buy_execution_attempt_state: Dict[str, int] = {"attempts_used": 0}

    output_files: Dict[str, Any] = {}
    raw_event_segments: list[Dict[str, Any]] = []
    market_segments: list[Dict[str, Any]] = []

    runtime_memory_writer: Optional[JsonlWriter] = None
    if bool(args.log_runtime_memory):
        runtime_memory_path = PROJECT_ROOT / "data" / f"websocket_share_price_runtime__{run_id}.jsonl"
        runtime_memory_writer = JsonlWriter(runtime_memory_path)
        output_files["runtime_memory"] = str(runtime_memory_path)

    decision_writer: Optional[JsonlWriter] = None
    if bool(args.log_decisions):
        decision_log_path = PROJECT_ROOT / "data" / f"decision_log__{run_id}.jsonl"
        decision_writer = JsonlWriter(decision_log_path)
        output_files["decision_log"] = str(decision_log_path)

    buy_decision_writer: Optional[JsonlWriter] = None
    if bool(args.log_buy_decisions):
        buy_decision_log_path = PROJECT_ROOT / "data" / f"buy_decision_log__{run_id}.jsonl"
        buy_decision_writer = JsonlWriter(buy_decision_log_path)
        output_files["buy_decision_log"] = str(buy_decision_log_path)

    buy_execution_writer: Optional[JsonlWriter] = None
    if bool(args.log_buy_execution):
        buy_execution_log_path = PROJECT_ROOT / "data" / f"buy_execution_log__{run_id}.jsonl"
        buy_execution_writer = JsonlWriter(buy_execution_log_path)
        output_files["buy_execution_log"] = str(buy_execution_log_path)

    edge_snapshot_writer: Optional[JsonlWriter] = None
    if bool(args.log_edge_snapshots):
        edge_snapshot_path = PROJECT_ROOT / "data" / f"gross_edge_snapshot__{run_id}.jsonl"
        edge_snapshot_writer = JsonlWriter(edge_snapshot_path)
        output_files["gross_edge_snapshot"] = str(edge_snapshot_path)

    writers = []
    if runtime_memory_writer is not None:
        writers.append(runtime_memory_writer)
    if decision_writer is not None:
        writers.append(decision_writer)
    if buy_decision_writer is not None:
        writers.append(buy_decision_writer)
    if buy_execution_writer is not None:
        writers.append(buy_execution_writer)
    if edge_snapshot_writer is not None:
        writers.append(edge_snapshot_writer)

    stats: Dict[str, Any] = {
        "decision_samples": 0,
        "can_trade_true_samples": 0,
        "can_trade_false_samples": 0,
        "decision_ready_true_samples": 0,
        "decision_ready_false_samples": 0,
        "runtime_memory_samples": 0,
        "decision_logged_samples": 0,
        "buy_decision_logged_samples": 0,
        "edge_snapshot_samples": 0,
        "buy_execution_attempts": 0,
        "buy_execution_submitted": 0,
        "buy_execution_partially_submitted": 0,
        "buy_execution_rejected": 0,
        "buy_execution_skipped_idempotent": 0,
        "buy_execution_errors": 0,
        "buy_execution_disabled_signals": 0,
        "buy_execution_blocked_fsm_signals": 0,
        "buy_execution_blocked_max_attempts": 0,
        "last_decision": None,
        "last_buy_execution": None,
        "buy_fsm": None,
    }
    total_kalshi_raw_messages = 0
    total_kalshi_normalized_events = 0
    total_polymarket_raw_messages = 0
    total_polymarket_normalized_events = 0
    last_kalshi_health: Dict[str, Any] = {}
    last_polymarket_health: Dict[str, Any] = {}
    last_selection: Dict[str, Any] = {}
    first_selection: Dict[str, Any] = {}
    last_market_window_end_epoch_ms: Optional[int] = None
    run_start_deadline_ms = None if int(args.duration_seconds) <= 0 else int(now_ms() + (int(args.duration_seconds) * 1000))
    segment_index = 0

    try:
        while True:
            loop_now_ms = now_ms()
            if run_start_deadline_ms is not None and loop_now_ms >= run_start_deadline_ms:
                break

            run_discovery_first = True if segment_index > 0 else (not bool(args.skip_discovery))
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

            pm_slug = str(selection["polymarket"]["event_slug"])
            pm_market_id = str(selection["polymarket"]["market_id"])
            pm_yes = str(selection["polymarket"]["token_yes"])
            pm_no = str(selection["polymarket"]["token_no"])
            kx_ticker = str(selection["kalshi"]["ticker"])
            segment_buy_execution_enabled = False
            segment_buy_execution_clients = BuyExecutionClients()
            segment_buy_execution_setup_errors: list[str] = []
            if bool(buy_execution_requested):
                (
                    segment_buy_execution_enabled,
                    segment_buy_execution_clients,
                    segment_buy_execution_setup_errors,
                ) = _build_buy_execution_clients(
                    enable_buy_execution=True,
                    buy_execution_config=buy_execution_config,
                )
                buy_execution_enabled_last = bool(segment_buy_execution_enabled)
                for entry in segment_buy_execution_setup_errors:
                    if entry not in buy_execution_setup_errors:
                        buy_execution_setup_errors.append(entry)
                if not bool(segment_buy_execution_enabled):
                    print(
                        "Buy execution setup failed for discovered market segment; "
                        "execution disabled for this segment:"
                    )
                    for entry in segment_buy_execution_setup_errors:
                        print(f"  - {entry}")
                else:
                    print("Buy execution enabled for current market segment.")
            else:
                buy_execution_enabled_last = False

            if segment_index == 0:
                first_selection = selection
            last_selection = selection
            last_market_window_end_epoch_ms = int(market_window_end_epoch_ms)
            runtime = SharePriceRuntime(polymarket_token_yes=pm_yes, polymarket_token_no=pm_no)

            if bool(args.log_raw_events):
                segment_tag = f"{run_id}__seg{segment_index + 1:03d}"
                pm_name = f"{safe_name(pm_slug)}__{safe_name(pm_market_id)}__{segment_tag}"
                kx_name = f"{safe_name(kx_ticker)}__{segment_tag}"
                pm_raw_path = PROJECT_ROOT / "data" / "websocket_poly" / f"raw_engine__{pm_name}.jsonl"
                pm_events_path = PROJECT_ROOT / "data" / "websocket_poly" / f"events_engine__{pm_name}.jsonl"
                kx_raw_path = PROJECT_ROOT / "data" / "websocket_kalshi" / f"raw_engine__{kx_name}.jsonl"
                kx_events_path = PROJECT_ROOT / "data" / "websocket_kalshi" / f"events_engine__{kx_name}.jsonl"
                pm_raw_writer = JsonlWriter(pm_raw_path)
                pm_event_writer = JsonlWriter(pm_events_path)
                kx_raw_writer = JsonlWriter(kx_raw_path)
                kx_event_writer = JsonlWriter(kx_events_path)
                raw_event_segments.append(
                    {
                        "segment_index": int(segment_index + 1),
                        "polymarket_raw": str(pm_raw_path),
                        "polymarket_events": str(pm_events_path),
                        "kalshi_raw": str(kx_raw_path),
                        "kalshi_events": str(kx_events_path),
                    }
                )
            else:
                null_writer = NullWriter()
                pm_raw_writer = null_writer
                pm_event_writer = null_writer
                kx_raw_writer = null_writer
                kx_event_writer = null_writer

            segment_started = datetime.now(timezone.utc)
            polymarket_collector: Optional[PolymarketWsCollector] = None
            kalshi_collector: Optional[KalshiWsCollector] = None
            segment_stats: Dict[str, Any] = {}
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
                    channels=kalshi_channels,
                    headers_factory=resolve_kalshi_ws_headers,
                    raw_writer=kx_raw_writer,
                    event_writer=kx_event_writer,
                    health_config=health_config,
                    on_event=runtime.apply_kalshi_event,
                )
                segment_stats = asyncio.run(
                    _run_engine(
                        kalshi_collector=kalshi_collector,
                        polymarket_collector=polymarket_collector,
                        runtime=runtime,
                        duration_seconds=int(segment_duration_seconds),
                        decision_poll_seconds=float(args.decision_poll_seconds),
                        decision_config=decision_config,
                        market_window_end_epoch_ms=market_window_end_epoch_ms,
                        market_context={
                            "polymarket_event_slug": pm_slug,
                            "polymarket_market_id": pm_market_id,
                            "polymarket_token_yes": pm_yes,
                            "polymarket_token_no": pm_no,
                            "kalshi_ticker": kx_ticker,
                            "market_window_end_epoch_ms": market_window_end_epoch_ms,
                        },
                        runtime_memory_writer=runtime_memory_writer,
                        runtime_memory_poll_seconds=float(args.runtime_memory_poll_seconds),
                        decision_writer=decision_writer,
                        buy_decision_writer=buy_decision_writer,
                        buy_execution_writer=buy_execution_writer,
                        edge_snapshot_writer=edge_snapshot_writer,
                        edge_snapshot_poll_seconds=float(args.edge_snapshot_poll_seconds),
                        buy_execution_enabled=bool(segment_buy_execution_enabled),
                        buy_execution_clients=segment_buy_execution_clients,
                        buy_idempotency_state=buy_idempotency_state,
                        buy_execution_cooldown_ms=buy_execution_cooldown_ms,
                        buy_execution_max_attempts=buy_execution_max_attempts,
                        buy_execution_attempt_state=buy_execution_attempt_state,
                    )
                )
            finally:
                try:
                    pm_raw_writer.close()
                except Exception:
                    pass
                try:
                    pm_event_writer.close()
                except Exception:
                    pass
                try:
                    kx_raw_writer.close()
                except Exception:
                    pass
                try:
                    kx_event_writer.close()
                except Exception:
                    pass

            segment_ended = datetime.now(timezone.utc)
            segment_index += 1

            for key in (
                "decision_samples",
                "can_trade_true_samples",
                "can_trade_false_samples",
                "decision_ready_true_samples",
                "decision_ready_false_samples",
                "runtime_memory_samples",
                "decision_logged_samples",
                "buy_decision_logged_samples",
                "edge_snapshot_samples",
                "buy_execution_attempts",
                "buy_execution_submitted",
                "buy_execution_partially_submitted",
                "buy_execution_rejected",
                "buy_execution_skipped_idempotent",
                "buy_execution_errors",
                "buy_execution_disabled_signals",
                "buy_execution_blocked_fsm_signals",
                "buy_execution_blocked_max_attempts",
            ):
                stats[key] = int(stats.get(key, 0)) + int(segment_stats.get(key, 0))
            stats["last_decision"] = segment_stats.get("last_decision")
            stats["last_buy_execution"] = segment_stats.get("last_buy_execution")
            stats["buy_fsm"] = segment_stats.get("buy_fsm")

            total_kalshi_raw_messages += int(kalshi_collector.message_count if kalshi_collector else 0)
            total_kalshi_normalized_events += int(kalshi_collector.event_count if kalshi_collector else 0)
            total_polymarket_raw_messages += int(polymarket_collector.message_count if polymarket_collector else 0)
            total_polymarket_normalized_events += int(polymarket_collector.event_count if polymarket_collector else 0)
            last_kalshi_health = kalshi_collector.health_snapshot() if kalshi_collector else {}
            last_polymarket_health = polymarket_collector.health_snapshot() if polymarket_collector else {}

            market_segments.append(
                {
                    "segment_index": int(segment_index),
                    "segment_started_at": segment_started.isoformat(),
                    "segment_ended_at": segment_ended.isoformat(),
                    "segment_duration_seconds_requested": int(segment_duration_seconds),
                    "polymarket_event_slug": pm_slug,
                    "polymarket_market_id": pm_market_id,
                    "kalshi_ticker": kx_ticker,
                    "market_window_end_epoch_ms": int(market_window_end_epoch_ms),
                    "counts": {
                        "kalshi_raw_messages": int(kalshi_collector.message_count if kalshi_collector else 0),
                        "kalshi_normalized_events": int(kalshi_collector.event_count if kalshi_collector else 0),
                        "polymarket_raw_messages": int(polymarket_collector.message_count if polymarket_collector else 0),
                        "polymarket_normalized_events": int(
                            polymarket_collector.event_count if polymarket_collector else 0
                        ),
                        "decision_samples": int(segment_stats.get("decision_samples", 0)),
                        "can_trade_true_samples": int(segment_stats.get("can_trade_true_samples", 0)),
                        "can_trade_false_samples": int(segment_stats.get("can_trade_false_samples", 0)),
                        "buy_execution_attempts": int(segment_stats.get("buy_execution_attempts", 0)),
                        "buy_execution_submitted": int(segment_stats.get("buy_execution_submitted", 0)),
                        "buy_execution_partially_submitted": int(
                            segment_stats.get("buy_execution_partially_submitted", 0)
                        ),
                        "buy_execution_rejected": int(segment_stats.get("buy_execution_rejected", 0)),
                        "buy_execution_skipped_idempotent": int(
                            segment_stats.get("buy_execution_skipped_idempotent", 0)
                        ),
                        "buy_execution_errors": int(segment_stats.get("buy_execution_errors", 0)),
                        "buy_execution_disabled_signals": int(
                            segment_stats.get("buy_execution_disabled_signals", 0)
                        ),
                        "buy_execution_blocked_fsm_signals": int(
                            segment_stats.get("buy_execution_blocked_fsm_signals", 0)
                        ),
                        "buy_execution_blocked_max_attempts": int(
                            segment_stats.get("buy_execution_blocked_max_attempts", 0)
                        ),
                    },
                }
            )
    except KeyboardInterrupt:
        pass
    finally:
        for writer in writers:
            try:
                writer.close()
            except Exception:
                pass

    session_ended = datetime.now(timezone.utc)

    print("Arbitrage websocket engine stopped")
    print(f"Started: {session_started.isoformat()}")
    print(f"Ended:   {session_ended.isoformat()}")
    print(f"Market segments: {len(market_segments)}")
    if market_segments:
        first_segment = market_segments[0]
        last_segment = market_segments[-1]
        print(
            "First market: "
            f"{first_segment.get('polymarket_event_slug')} ({first_segment.get('polymarket_market_id')})"
        )
        print(f"First Kalshi: {first_segment.get('kalshi_ticker')}")
        if len(market_segments) > 1:
            print(
                "Last market: "
                f"{last_segment.get('polymarket_event_slug')} ({last_segment.get('polymarket_market_id')})"
            )
            print(f"Last Kalshi: {last_segment.get('kalshi_ticker')}")
    print(
        "Messages: "
        f"kalshi={int(total_kalshi_raw_messages)}, polymarket={int(total_polymarket_raw_messages)}"
    )
    print(
        "Decision samples: "
        f"{int(stats.get('decision_samples', 0))}, "
        f"can_trade_true={int(stats.get('can_trade_true_samples', 0))}, "
        f"can_trade_false={int(stats.get('can_trade_false_samples', 0))}"
    )
    print(
        "Buy execution: "
        f"enabled={bool(buy_execution_enabled_last)}, "
        f"max_attempts={int(buy_execution_max_attempts)}, "
        f"attempts_used={int(buy_execution_attempt_state.get('attempts_used', 0))}, "
        f"attempts={int(stats.get('buy_execution_attempts', 0))}, "
        f"submitted={int(stats.get('buy_execution_submitted', 0))}, "
        f"partial={int(stats.get('buy_execution_partially_submitted', 0))}, "
        f"rejected={int(stats.get('buy_execution_rejected', 0))}, "
        f"errors={int(stats.get('buy_execution_errors', 0))}"
    )
    if (
        bool(args.log_summary)
        or bool(args.log_raw_events)
        or bool(args.log_runtime_memory)
        or bool(args.log_decisions)
        or bool(args.log_buy_decisions)
        or bool(args.log_buy_execution)
        or bool(args.log_edge_snapshots)
    ):
        summary = {
            "run_id": run_id,
            "session_started_at": session_started.isoformat(),
            "session_ended_at": session_ended.isoformat(),
            "duration_seconds_requested": int(args.duration_seconds),
            "selected_markets": first_selection,
            "selected_markets_last": last_selection,
            "market_segments": market_segments,
            "kalshi_channels": kalshi_channels,
            "health_config": health_config_to_dict(health_config),
            "decision_config": decision_config_to_dict(decision_config),
            "market_window_end_epoch_ms": last_market_window_end_epoch_ms,
            "polymarket_custom_feature_enabled": bool(args.custom_feature_enabled),
            "buy_execution": {
                "configured": buy_execution_runtime_config_to_dict(buy_execution_config),
                "requested": bool(buy_execution_requested),
                "enabled": bool(buy_execution_enabled_last),
                "cooldown_ms": int(buy_execution_cooldown_ms),
                "max_attempts": int(buy_execution_max_attempts),
                "attempts_used": int(buy_execution_attempt_state.get("attempts_used", 0)),
                "cli_overrides": {
                    "enable_buy_execution": args.enable_buy_execution,
                    "buy_execution_cooldown_ms": args.buy_execution_cooldown_ms,
                    "max_buy_execution_attempts": args.max_buy_execution_attempts,
                },
                "setup_errors": list(buy_execution_setup_errors),
            },
            "counts": {
                "kalshi_raw_messages": int(total_kalshi_raw_messages),
                "kalshi_normalized_events": int(total_kalshi_normalized_events),
                "polymarket_raw_messages": int(total_polymarket_raw_messages),
                "polymarket_normalized_events": int(total_polymarket_normalized_events),
                "decision_samples": int(stats.get("decision_samples", 0)),
                "runtime_memory_samples": int(stats.get("runtime_memory_samples", 0)),
                "decision_logged_samples": int(stats.get("decision_logged_samples", 0)),
                "buy_decision_logged_samples": int(stats.get("buy_decision_logged_samples", 0)),
                "edge_snapshot_samples": int(stats.get("edge_snapshot_samples", 0)),
                "can_trade_true_samples": int(stats.get("can_trade_true_samples", 0)),
                "can_trade_false_samples": int(stats.get("can_trade_false_samples", 0)),
                "buy_execution_attempts": int(stats.get("buy_execution_attempts", 0)),
                "buy_execution_submitted": int(stats.get("buy_execution_submitted", 0)),
                "buy_execution_partially_submitted": int(stats.get("buy_execution_partially_submitted", 0)),
                "buy_execution_rejected": int(stats.get("buy_execution_rejected", 0)),
                "buy_execution_skipped_idempotent": int(stats.get("buy_execution_skipped_idempotent", 0)),
                "buy_execution_errors": int(stats.get("buy_execution_errors", 0)),
                "buy_execution_disabled_signals": int(stats.get("buy_execution_disabled_signals", 0)),
                "buy_execution_blocked_fsm_signals": int(stats.get("buy_execution_blocked_fsm_signals", 0)),
                "buy_execution_blocked_max_attempts": int(stats.get("buy_execution_blocked_max_attempts", 0)),
            },
            "final_stream_health": {
                "kalshi": last_kalshi_health,
                "polymarket": last_polymarket_health,
            },
            "final_decision": stats.get("last_decision"),
            "final_buy_execution": stats.get("last_buy_execution"),
            "final_buy_fsm": stats.get("buy_fsm"),
            "output_files": output_files,
            "generated_at": utc_now_iso(),
        }
        if raw_event_segments:
            output_files["raw_event_segments"] = raw_event_segments
        summary_path = PROJECT_ROOT / "data" / f"arbitrage_engine_summary__{run_id}.json"
        summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
        print(f"Summary: {summary_path}")
        if bool(args.log_raw_events):
            print(f"Raw/event log segments: {len(raw_event_segments)}")
            for segment in raw_event_segments:
                print(
                    "  Segment "
                    f"{segment['segment_index']}: "
                    f"poly_raw={segment['polymarket_raw']} kalshi_raw={segment['kalshi_raw']}"
                )
        if bool(args.log_runtime_memory):
            print(f"Runtime memory log: {output_files['runtime_memory']}")
        if bool(args.log_decisions):
            print(f"Decision log: {output_files['decision_log']}")
        if bool(args.log_buy_decisions):
            print(f"Buy decision log: {output_files['buy_decision_log']}")
        if bool(args.log_buy_execution):
            print(f"Buy execution log: {output_files['buy_execution_log']}")
        if bool(args.log_edge_snapshots):
            print(f"Gross edge snapshot log: {output_files['gross_edge_snapshot']}")


if __name__ == "__main__":
    main()
