import asyncio
import sys
from typing import Any, Dict, Optional

from scripts.common.buy_execution import BuyExecutionClients, BuyExecutionPlan, BuyIdempotencyState, execute_cross_venue_buy
from scripts.common.buy_fsm import BuyFsmRuntime
from scripts.common.decision_runtime import DecisionRuntime, SharePriceRuntime
from scripts.common.engine_logger import EngineLogger
from scripts.common.edge_snapshots import build_edge_snapshot as _build_edge_snapshot
from scripts.common.position_polling import PositionReconcileLoop
from scripts.common.position_runtime import PositionRuntime
from scripts.common.ws_collectors import (
    KalshiMarketPositionsWsCollector,
    KalshiUserOrdersWsCollector,
    KalshiWsCollector,
    PolymarketUserWsCollector,
    PolymarketWsCollector,
)
from scripts.common.ws_transport import now_ms, utc_now_iso
from scripts.common.utils import as_dict as _as_dict, as_float as _as_float


async def run_core_loop(
    *,
    logger: EngineLogger,
    kalshi_collector: KalshiWsCollector,
    polymarket_collector: PolymarketWsCollector,
    kalshi_market_positions_collector: Optional[KalshiMarketPositionsWsCollector],
    kalshi_user_orders_collector: Optional[KalshiUserOrdersWsCollector],
    polymarket_user_collector: Optional[PolymarketUserWsCollector],
    runtime: SharePriceRuntime,
    position_runtime: Optional[PositionRuntime],
    position_reconcile_loop: Optional[PositionReconcileLoop],
    position_event_state: Optional[Dict[str, int]],
    duration_seconds: int,
    decision_poll_seconds: float,
    decision_config: Any,
    market_window_end_epoch_ms: Optional[int],
    market_context: Dict[str, Any],
    runtime_memory_poll_seconds: float,
    edge_snapshot_poll_seconds: float,
    buy_execution_enabled: bool,
    buy_execution_clients: BuyExecutionClients,
    buy_idempotency_state: BuyIdempotencyState,
    buy_execution_cooldown_ms: int,
    buy_execution_max_attempts: int,
    buy_execution_parallel_leg_timeout_ms: int,
    buy_execution_attempt_state: Dict[str, int],
) -> Dict[str, Any]:
    buy_fsm = BuyFsmRuntime.initialize()
    cooldown_ms = max(0, int(buy_execution_cooldown_ms))
    max_attempts = max(0, int(buy_execution_max_attempts))
    parallel_leg_timeout_ms = max(100, int(buy_execution_parallel_leg_timeout_ms))
    if "attempts_used" not in buy_execution_attempt_state:
        buy_execution_attempt_state["attempts_used"] = 0
    if "submission_seq" not in buy_execution_attempt_state:
        buy_execution_attempt_state["submission_seq"] = 0
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
        "buy_execution_blocked_position_health": 0,
        "position_monitoring_enabled": bool(position_runtime is not None),
        "position_poll_iterations": 0,
        "position_poll_polymarket_success": 0,
        "position_poll_polymarket_failure": 0,
        "position_poll_kalshi_success": 0,
        "position_poll_kalshi_failure": 0,
        "position_poll_polymarket_orders_success": 0,
        "position_poll_polymarket_orders_failure": 0,
        "position_poll_kalshi_orders_success": 0,
        "position_poll_kalshi_orders_failure": 0,
        "position_ws_polymarket_user_events": 0,
        "position_ws_kalshi_market_position_events": 0,
        "position_ws_kalshi_user_order_events": 0,
        "position_health_state_changes": 0,
        "position_health_allowed_samples": 0,
        "position_health_blocked_samples": 0,
        "position_health_bootstrap_completed_transitions": 0,
        "position_health_hard_stale_transitions": 0,
        "last_position_health": None,
        "last_position_poll": None,
        "last_decision": None,
        "last_buy_execution": None,
        "buy_fsm": buy_fsm.snapshot(),
    }
    position_gate_last_allowed: Optional[bool] = None
    position_bootstrap_last: Optional[bool] = None
    position_hard_stale_last: Optional[bool] = None

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

    async def _decision_loop(stop: asyncio.Event) -> None:
        nonlocal position_gate_last_allowed, position_bootstrap_last, position_hard_stale_last
        poll_s = max(0.05, float(decision_poll_seconds))
        while not stop.is_set():
            now_epoch_ms = now_ms()
            buy_fsm.maybe_rearm(now_epoch_ms=now_epoch_ms)
            position_health = None
            if position_runtime is not None:
                position_health = position_runtime.refresh_health(now_epoch_ms=now_epoch_ms)
                stats["last_position_health"] = position_health
                position_allowed = bool(_as_dict(position_health).get("buy_execution_allowed"))
                bootstrap_completed = bool(_as_dict(position_health).get("bootstrap_completed"))
                hard_stale = bool(_as_dict(position_health).get("hard_stale"))
                if position_allowed:
                    stats["position_health_allowed_samples"] += 1
                else:
                    stats["position_health_blocked_samples"] += 1

                if position_gate_last_allowed is None or position_gate_last_allowed != position_allowed:
                    stats["position_health_state_changes"] += 1
                    print(
                        "Position health gate transition: "
                        f"buy_execution_allowed={position_allowed}, "
                        f"bootstrap_completed={bootstrap_completed}, "
                        f"hard_stale={hard_stale}",
                        file=sys.stderr,
                    )
                    logger.write_position_event(
                        recv_ms=now_epoch_ms,
                        sub_kind="health_transition",
                        payload={"position_health": position_health},
                    )

                if position_bootstrap_last is not None and (not position_bootstrap_last) and bootstrap_completed:
                    stats["position_health_bootstrap_completed_transitions"] += 1
                if position_hard_stale_last is not None and (not position_hard_stale_last) and hard_stale:
                    stats["position_health_hard_stale_transitions"] += 1

                position_gate_last_allowed = position_allowed
                position_bootstrap_last = bootstrap_completed
                position_hard_stale_last = hard_stale
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
                elif position_runtime is not None and not bool(_as_dict(position_health).get("buy_execution_allowed")):
                    stats["buy_execution_blocked_position_health"] += 1
                    buy_execution_event = {
                        "status": "blocked_position_health",
                        "reason": "position_buy_execution_not_allowed",
                        "position_health": position_health,
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
                    base_signal_id = str(plan_payload.get("signal_id") or "").strip()
                    if not base_signal_id:
                        stats["buy_execution_errors"] += 1
                        buy_execution_event = {
                            "status": "error",
                            "reason": "execution_plan_missing_signal_id",
                        }
                    else:
                        plan_gate = None
                        if position_runtime is not None:
                            plan_gate = position_runtime.evaluate_execution_plan(
                                execution_plan=plan_payload,
                                now_epoch_ms=now_epoch_ms,
                            )
                            position_health = _as_dict(plan_gate.get("position_health"))
                            stats["last_position_health"] = position_health
                        if plan_gate is not None and not bool(plan_gate.get("allowed")):
                            stats["buy_execution_blocked_position_health"] += 1
                            buy_execution_event = {
                                "status": "blocked_position_health",
                                "reason": "position_buy_execution_not_allowed",
                                "position_health": position_health,
                                "position_gate_reasons": list(plan_gate.get("reasons") or []),
                                "blocked_markets": list(plan_gate.get("blocked_markets") or []),
                                "blocked_legs": list(plan_gate.get("blocked_legs") or []),
                            }
                        else:
                            submission_seq = int(buy_execution_attempt_state.get("submission_seq", 0)) + 1
                            buy_execution_attempt_state["submission_seq"] = int(submission_seq)
                            signal_id = f"{base_signal_id}__s{submission_seq}"
                            plan_payload["signal_id"] = signal_id
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
                                    parallel_leg_timeout_ms=parallel_leg_timeout_ms,
                                )
                                result_payload = result.to_dict()
                                if position_runtime is not None:
                                    position_runtime.apply_buy_execution_result(
                                        result_payload=result_payload,
                                        now_epoch_ms=now_ms(),
                                    )
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
                "position_monitoring_enabled": bool(position_runtime is not None),
                "buy_execution_max_attempts": int(max_attempts),
                "buy_execution_attempts_used": int(buy_execution_attempt_state.get("attempts_used", 0)),
                "buy_execution": buy_execution_event,
                "position_health": position_health,
                "buy_fsm": buy_fsm_after,
            }
            stats["last_decision"] = decision_payload
            stats["buy_fsm"] = buy_fsm_after

            if buy_execution_event is not None:
                logger.write_buy_execution(
                    recv_ms=now_epoch_ms,
                    payload={
                        "market": market_context,
                        "execution_plan": decision.execution_plan,
                        "execution_plan_reasons": decision.execution_plan_reasons,
                        "buy_execution_enabled": bool(buy_execution_enabled),
                        "buy_execution_parallel_leg_timeout_ms": int(parallel_leg_timeout_ms),
                        "buy_execution": buy_execution_event,
                        "buy_fsm_before": buy_fsm_before,
                        "buy_fsm_after": buy_fsm_after,
                    }
                )
            
            logger.write_decision(
                recv_ms=now_epoch_ms,
                payload={"market": market_context, **decision_payload}
            )
            stats["decision_logged_samples"] += 1
            
            if bool(decision.can_trade):
                logger.write_buy_decision(
                    recv_ms=now_epoch_ms,
                    payload={"market": market_context, **decision_payload}
                )
                stats["buy_decision_logged_samples"] += 1

            await asyncio.sleep(poll_s)

    async def _runtime_memory_loop(stop: asyncio.Event) -> None:
        poll_s = max(0.2, float(runtime_memory_poll_seconds))
        while not stop.is_set():
            now_epoch_ms = now_ms()
            logger.write_runtime_memory(
                recv_ms=now_epoch_ms,
                memory=_runtime_memory_snapshot(runtime, now_epoch_ms),
            )
            stats["runtime_memory_samples"] += 1
            await asyncio.sleep(poll_s)

    async def _edge_snapshot_loop(stop: asyncio.Event) -> None:
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
            logger.write_edge_snapshot(
                recv_ms=now_epoch_ms,
                payload=payload,
            )
            stats["edge_snapshot_samples"] += 1
            await asyncio.sleep(poll_s)

    async def _position_poll_loop(stop: asyncio.Event) -> None:
        if position_reconcile_loop is None:
            return

        def _write_position_poll_raw_entries(result_payload: Dict[str, Any]) -> None:
            venues = _as_dict(result_payload).get("venues")
            venue_rows = venues if isinstance(venues, dict) else {}
            for venue_name in ("polymarket", "kalshi"):
                venue_payload = _as_dict(venue_rows.get(venue_name))
                if not venue_payload:
                    continue
                logger.write_position_poll_raw(
                    recv_ms=now_ms(),
                    venue=venue_name,
                    payload={
                        "market": market_context,
                        "at_ms": _as_dict(result_payload).get("at_ms"),
                        "result": {
                            "status": venue_payload.get("status"),
                            "snapshot": venue_payload.get("snapshot"),
                            "reconcile": venue_payload.get("reconcile"),
                            "orders": venue_payload.get("orders"),
                        },
                    },
                )

        poll_s = max(0.05, float(position_reconcile_loop.config.loop_sleep_seconds))
        bootstrap_result = position_reconcile_loop.run_once(now_epoch_ms=now_ms(), force=True)
        stats["last_position_poll"] = bootstrap_result
        _write_position_poll_raw_entries(bootstrap_result)

        logger.write_position_event(
            recv_ms=now_ms(),
            sub_kind="poll_snapshot",
            payload={
                "result": bootstrap_result,
                "position_health": None if position_runtime is None else position_runtime.refresh_health(now_epoch_ms=now_ms()),
            }
        )

        while not stop.is_set():
            result = position_reconcile_loop.run_once(now_epoch_ms=now_ms(), force=False)
            if bool(_as_dict(result).get("venues")):
                stats["last_position_poll"] = result
                _write_position_poll_raw_entries(result)
                logger.write_position_event(
                    recv_ms=now_ms(),
                    sub_kind="poll_snapshot",
                    payload={
                        "result": result,
                        "position_health": None if position_runtime is None else position_runtime.refresh_health(now_epoch_ms=now_ms()),
                    }
                )

            if position_event_state is not None:
                stats["position_ws_polymarket_user_events"] = int(position_event_state.get("polymarket_user_events", 0))
                stats["position_ws_kalshi_market_position_events"] = int(
                    position_event_state.get("kalshi_market_position_events", 0)
                )
                stats["position_ws_kalshi_user_order_events"] = int(
                    position_event_state.get("kalshi_user_order_events", 0)
                )
            stats["position_poll_iterations"] = int(position_reconcile_loop.stats.get("poll_iterations", 0))
            stats["position_poll_polymarket_success"] = int(position_reconcile_loop.stats.get("polymarket_success", 0))
            stats["position_poll_polymarket_failure"] = int(position_reconcile_loop.stats.get("polymarket_failure", 0))
            stats["position_poll_kalshi_success"] = int(position_reconcile_loop.stats.get("kalshi_success", 0))
            stats["position_poll_kalshi_failure"] = int(position_reconcile_loop.stats.get("kalshi_failure", 0))
            stats["position_poll_polymarket_orders_success"] = int(
                position_reconcile_loop.stats.get("polymarket_orders_success", 0)
            )
            stats["position_poll_polymarket_orders_failure"] = int(
                position_reconcile_loop.stats.get("polymarket_orders_failure", 0)
            )
            stats["position_poll_kalshi_orders_success"] = int(
                position_reconcile_loop.stats.get("kalshi_orders_success", 0)
            )
            stats["position_poll_kalshi_orders_failure"] = int(
                position_reconcile_loop.stats.get("kalshi_orders_failure", 0)
            )
            await asyncio.sleep(poll_s)

    stop_event = asyncio.Event()
    tasks = [
        asyncio.create_task(kalshi_collector.run(stop_event)),
        asyncio.create_task(polymarket_collector.run(stop_event)),
        asyncio.create_task(_decision_loop(stop_event)),
        asyncio.create_task(_runtime_memory_loop(stop_event)),
        asyncio.create_task(_edge_snapshot_loop(stop_event)),
        asyncio.create_task(_position_poll_loop(stop_event)),
    ]
    if polymarket_user_collector is not None:
        tasks.append(asyncio.create_task(polymarket_user_collector.run(stop_event)))
    if kalshi_market_positions_collector is not None:
        tasks.append(asyncio.create_task(kalshi_market_positions_collector.run(stop_event)))
    if kalshi_user_orders_collector is not None:
        tasks.append(asyncio.create_task(kalshi_user_orders_collector.run(stop_event)))
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
    if position_reconcile_loop is not None:
        stats["position_poll_iterations"] = int(position_reconcile_loop.stats.get("poll_iterations", 0))
        stats["position_poll_polymarket_success"] = int(position_reconcile_loop.stats.get("polymarket_success", 0))
        stats["position_poll_polymarket_failure"] = int(position_reconcile_loop.stats.get("polymarket_failure", 0))
        stats["position_poll_kalshi_success"] = int(position_reconcile_loop.stats.get("kalshi_success", 0))
        stats["position_poll_kalshi_failure"] = int(position_reconcile_loop.stats.get("kalshi_failure", 0))
        stats["position_poll_polymarket_orders_success"] = int(
            position_reconcile_loop.stats.get("polymarket_orders_success", 0)
        )
        stats["position_poll_polymarket_orders_failure"] = int(
            position_reconcile_loop.stats.get("polymarket_orders_failure", 0)
        )
        stats["position_poll_kalshi_orders_success"] = int(
            position_reconcile_loop.stats.get("kalshi_orders_success", 0)
        )
        stats["position_poll_kalshi_orders_failure"] = int(
            position_reconcile_loop.stats.get("kalshi_orders_failure", 0)
        )
    if position_event_state is not None:
        stats["position_ws_polymarket_user_events"] = int(position_event_state.get("polymarket_user_events", 0))
        stats["position_ws_kalshi_market_position_events"] = int(
            position_event_state.get("kalshi_market_position_events", 0)
        )
        stats["position_ws_kalshi_user_order_events"] = int(
            position_event_state.get("kalshi_user_order_events", 0)
        )
    if position_runtime is not None:
        stats["last_position_health"] = position_runtime.refresh_health(now_epoch_ms=now_ms())

    return stats
