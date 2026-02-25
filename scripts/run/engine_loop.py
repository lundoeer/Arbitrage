import asyncio
import sys
from typing import Any, Dict, Optional

from scripts.common.buy_execution import BuyExecutionClients, BuyExecutionPlan, BuyIdempotencyState, execute_cross_venue_buy
from scripts.common.buy_fsm import BuyFsmRuntime
from scripts.common.decision_runtime import DecisionRuntime, SharePriceRuntime
from scripts.common.engine_logger import EngineLogger
from scripts.common.edge_snapshots import build_edge_snapshot as _build_edge_snapshot
from scripts.common.execution_lock import ExecutionLockRuntime
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
    sell_execution_enabled: bool,
    sell_execution_clients: BuyExecutionClients,
    buy_idempotency_state: BuyIdempotencyState,
    buy_execution_cooldown_ms: int,
    buy_execution_max_attempts: int,
    buy_execution_parallel_leg_timeout_ms: int,
    sell_execution_parallel_leg_timeout_ms: int,
    buy_execution_attempt_state: Dict[str, int],
) -> Dict[str, Any]:
    buy_fsm = BuyFsmRuntime.initialize()
    execution_lock = ExecutionLockRuntime.initialize()
    execution_lock_requires_position_monitoring = bool(position_runtime is not None and position_reconcile_loop is not None)
    cooldown_ms = max(0, int(buy_execution_cooldown_ms))
    max_attempts = max(0, int(buy_execution_max_attempts))
    buy_parallel_leg_timeout_ms = max(100, int(buy_execution_parallel_leg_timeout_ms))
    sell_parallel_leg_timeout_ms = max(100, int(sell_execution_parallel_leg_timeout_ms))
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
        "sell_execution_attempts": 0,
        "sell_execution_submitted": 0,
        "sell_execution_partially_submitted": 0,
        "sell_execution_rejected": 0,
        "sell_execution_skipped_idempotent": 0,
        "sell_execution_errors": 0,
        "sell_execution_disabled_signals": 0,
        "sell_execution_blocked_fsm_signals": 0,
        "sell_execution_blocked_position_health": 0,
        "execution_lock_blocked_signals": 0,
        "execution_lock_transitions": 0,
        "execution_lock_requires_position_monitoring": bool(execution_lock_requires_position_monitoring),
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
        "last_sell_execution": None,
        "buy_fsm": buy_fsm.snapshot(),
        "execution_lock": execution_lock.snapshot(),
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

    def _record_execution_lock_update(
        *,
        now_epoch_ms: int,
        source: str,
        update: Optional[Dict[str, Any]],
    ) -> None:
        payload = dict(update or {})
        stats["execution_lock"] = execution_lock.snapshot()
        if not bool(payload.get("transitioned")):
            return
        stats["execution_lock_transitions"] += 1
        logger.write_position_event(
            recv_ms=now_epoch_ms,
            sub_kind="execution_lock_transition",
            payload={
                "source": source,
                "update": payload,
                "execution_lock": execution_lock.snapshot(),
            },
        )

    async def _decision_loop(stop: asyncio.Event) -> None:
        nonlocal position_gate_last_allowed, position_bootstrap_last, position_hard_stale_last
        poll_s = max(0.05, float(decision_poll_seconds))
        while not stop.is_set():
            now_epoch_ms = now_ms()
            buy_fsm.maybe_rearm(now_epoch_ms=now_epoch_ms)
            if position_runtime is not None:
                _record_execution_lock_update(
                    now_epoch_ms=now_epoch_ms,
                    source="decision_tick_terminal_refresh",
                    update=execution_lock.refresh_terminal_status(
                        has_open_orders_for_venue=position_runtime.has_open_orders_for_venue,
                        now_epoch_ms=now_epoch_ms,
                    ),
                )
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
            position_snapshot_for_decision = (
                position_runtime.snapshot(now_epoch_ms=now_epoch_ms) if position_runtime is not None else None
            )
            decision = DecisionRuntime.evaluate(
                kalshi_health=k_health,
                polymarket_health=p_health,
                quotes=quotes,
                market_context=market_context,
                decision_config=decision_config,
                position_snapshot=position_snapshot_for_decision,
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

            execution_event: Optional[Dict[str, Any]] = None
            selected_action_hint = decision.selected_action_hint
            preferred_actions: list[str] = []
            if decision.can_sell:
                preferred_actions.append("sell")
            if decision.can_buy:
                preferred_actions.append("buy")
            selected_action = None
            for candidate_action in preferred_actions:
                if candidate_action == "sell" and bool(sell_execution_enabled):
                    selected_action = "sell"
                    break
                if candidate_action == "buy" and bool(buy_execution_enabled):
                    selected_action = "buy"
                    break
            if selected_action is None and preferred_actions:
                selected_action = preferred_actions[0]
            buy_fsm_before = buy_fsm.snapshot()
            execution_lock_before = execution_lock.snapshot()
            if bool(decision.can_trade) and selected_action in {"buy", "sell"}:
                attempts_used = int(buy_execution_attempt_state.get("attempts_used", 0))
                plan_from_decision: Optional[Dict[str, Any]]
                plan_reasons: list[str]
                execution_enabled: bool
                execution_clients: BuyExecutionClients
                parallel_leg_timeout_ms: int
                if selected_action == "sell":
                    plan_from_decision = (
                        dict(decision.sell_execution_plan) if isinstance(decision.sell_execution_plan, dict) else None
                    )
                    plan_reasons = list(decision.sell_execution_plan_reasons)
                    execution_enabled = bool(sell_execution_enabled)
                    execution_clients = sell_execution_clients
                    parallel_leg_timeout_ms = int(sell_parallel_leg_timeout_ms)
                else:
                    plan_from_decision = dict(decision.execution_plan) if isinstance(decision.execution_plan, dict) else None
                    plan_reasons = list(decision.execution_plan_reasons)
                    execution_enabled = bool(buy_execution_enabled)
                    execution_clients = buy_execution_clients
                    parallel_leg_timeout_ms = int(buy_parallel_leg_timeout_ms)

                if selected_action == "buy" and max_attempts > 0 and attempts_used >= max_attempts:
                    stats["buy_execution_blocked_max_attempts"] += 1
                    execution_event = {
                        "status": "blocked_max_attempts",
                        "reason": f"max_buy_execution_attempts_reached:{max_attempts}",
                        "action": selected_action,
                        "attempts_used": attempts_used,
                    }
                elif not bool(execution_enabled):
                    if selected_action == "sell":
                        stats["sell_execution_disabled_signals"] += 1
                    else:
                        stats["buy_execution_disabled_signals"] += 1
                    execution_event = {
                        "status": "skipped_disabled",
                        "reason": f"{selected_action}_execution_not_enabled",
                        "action": selected_action,
                    }
                elif position_runtime is not None and not bool(_as_dict(position_health).get("buy_execution_allowed")):
                    if selected_action == "sell":
                        stats["sell_execution_blocked_position_health"] += 1
                    else:
                        stats["buy_execution_blocked_position_health"] += 1
                    execution_event = {
                        "status": "blocked_position_health",
                        "reason": f"position_{selected_action}_execution_not_allowed",
                        "action": selected_action,
                        "position_health": position_health,
                    }
                elif not bool(execution_lock_requires_position_monitoring):
                    stats["execution_lock_blocked_signals"] += 1
                    execution_event = {
                        "status": "blocked_execution_lock",
                        "reason": "execution_lock_requires_position_monitoring",
                        "action": selected_action,
                        "execution_lock": execution_lock.snapshot(),
                    }
                elif not bool(execution_lock.can_start_execution().get("allowed")):
                    lock_gate = execution_lock.can_start_execution()
                    stats["execution_lock_blocked_signals"] += 1
                    execution_event = {
                        "status": "blocked_execution_lock",
                        "reason": str(lock_gate.get("reason") or "execution_lock_active"),
                        "action": selected_action,
                        "execution_lock": execution_lock.snapshot(),
                    }
                elif not buy_fsm.can_accept_new_signal():
                    if selected_action == "sell":
                        stats["sell_execution_blocked_fsm_signals"] += 1
                    else:
                        stats["buy_execution_blocked_fsm_signals"] += 1
                    execution_event = {
                        "status": "blocked_fsm",
                        "action": selected_action,
                        "reason": f"fsm_state={buy_fsm.state.value}",
                    }
                elif not isinstance(plan_from_decision, dict):
                    if selected_action == "sell":
                        stats["sell_execution_errors"] += 1
                    else:
                        stats["buy_execution_errors"] += 1
                    execution_event = {
                        "status": "error",
                        "action": selected_action,
                        "reason": "missing_execution_plan",
                        "execution_plan_reasons": plan_reasons,
                    }
                else:
                    plan_payload = dict(plan_from_decision)
                    base_signal_id = str(plan_payload.get("signal_id") or "").strip()
                    if not base_signal_id:
                        if selected_action == "sell":
                            stats["sell_execution_errors"] += 1
                        else:
                            stats["buy_execution_errors"] += 1
                        execution_event = {
                            "status": "error",
                            "action": selected_action,
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
                            if selected_action == "sell":
                                stats["sell_execution_blocked_position_health"] += 1
                            else:
                                stats["buy_execution_blocked_position_health"] += 1
                            execution_event = {
                                "status": "blocked_position_health",
                                "reason": f"position_{selected_action}_execution_not_allowed",
                                "action": selected_action,
                                "position_health": position_health,
                                "position_gate_reasons": list(plan_gate.get("reasons") or []),
                                "blocked_markets": list(plan_gate.get("blocked_markets") or []),
                                "blocked_legs": list(plan_gate.get("blocked_legs") or []),
                            }
                        else:
                            attempted_venues: list[str] = []
                            for leg_payload in list(plan_payload.get("legs") or []):
                                leg = _as_dict(leg_payload)
                                venue = str(leg.get("venue") or "").strip().lower()
                                if venue in {"polymarket", "kalshi"} and venue not in attempted_venues:
                                    attempted_venues.append(venue)
                            begin_lock = execution_lock.begin_execution(
                                action_type=selected_action,
                                attempted_venues=attempted_venues,
                                now_epoch_ms=now_epoch_ms,
                            )
                            _record_execution_lock_update(
                                now_epoch_ms=now_epoch_ms,
                                source="decision_begin_execution",
                                update=begin_lock,
                            )
                            if not bool(begin_lock.get("accepted")):
                                stats["execution_lock_blocked_signals"] += 1
                                execution_event = {
                                    "status": "blocked_execution_lock",
                                    "reason": str(begin_lock.get("reason") or "execution_lock_active"),
                                    "action": selected_action,
                                    "execution_lock": execution_lock.snapshot(),
                                }
                            else:
                                submission_seq = int(buy_execution_attempt_state.get("submission_seq", 0)) + 1
                                buy_execution_attempt_state["submission_seq"] = int(submission_seq)
                                signal_id = f"{base_signal_id}__s{submission_seq}"
                                plan_payload["signal_id"] = signal_id
                                buy_fsm.begin_submission(
                                    signal_id=signal_id,
                                    now_epoch_ms=now_epoch_ms,
                                    reason=f"decision_{selected_action}_can_trade",
                                )
                                if selected_action == "buy":
                                    buy_execution_attempt_state["attempts_used"] = int(attempts_used + 1)
                                    stats["buy_execution_attempts"] += 1
                                else:
                                    stats["sell_execution_attempts"] += 1
                                try:
                                    plan = BuyExecutionPlan.from_dict(plan_payload)
                                    result = execute_cross_venue_buy(
                                        plan=plan,
                                        clients=execution_clients,
                                        state=buy_idempotency_state,
                                        now_epoch_ms=now_epoch_ms,
                                        parallel_leg_timeout_ms=parallel_leg_timeout_ms,
                                    )
                                    result_payload = result.to_dict()
                                    if position_runtime is not None:
                                        position_runtime.apply_execution_result(
                                            result_payload=result_payload,
                                            now_epoch_ms=now_ms(),
                                        )
                                    _record_execution_lock_update(
                                        now_epoch_ms=now_ms(),
                                        source="decision_record_execution_result",
                                        update=execution_lock.record_execution_result(
                                            result_payload=result_payload,
                                            now_epoch_ms=now_ms(),
                                        ),
                                    )
                                    if position_runtime is not None:
                                        _record_execution_lock_update(
                                            now_epoch_ms=now_ms(),
                                            source="decision_post_submit_terminal_refresh",
                                            update=execution_lock.refresh_terminal_status(
                                                has_open_orders_for_venue=position_runtime.has_open_orders_for_venue,
                                                now_epoch_ms=now_ms(),
                                            ),
                                        )
                                    buy_fsm.complete_submission(
                                        result_payload=result_payload,
                                        now_epoch_ms=now_ms(),
                                        cooldown_ms=cooldown_ms,
                                    )
                                    if selected_action == "sell":
                                        stats["last_sell_execution"] = result_payload
                                    else:
                                        stats["last_buy_execution"] = result_payload
                                    status = str(result_payload.get("status") or "")
                                    if status == "submitted":
                                        stats[f"{selected_action}_execution_submitted"] += 1
                                    elif status == "partially_submitted":
                                        stats[f"{selected_action}_execution_partially_submitted"] += 1
                                    elif status == "rejected":
                                        stats[f"{selected_action}_execution_rejected"] += 1
                                    elif status == "skipped_idempotent":
                                        stats[f"{selected_action}_execution_skipped_idempotent"] += 1
                                    else:
                                        stats[f"{selected_action}_execution_errors"] += 1
                                    execution_event = {
                                        "status": "executed",
                                        "action": selected_action,
                                        "result": result_payload,
                                    }
                                except Exception as exc:
                                    error_payload = {
                                        "signal_id": signal_id,
                                        "status": "error",
                                        "error": f"{type(exc).__name__}:{exc}",
                                        "at_ms": now_ms(),
                                    }
                                    _record_execution_lock_update(
                                        now_epoch_ms=now_ms(),
                                        source="decision_record_execution_error",
                                        update=execution_lock.record_execution_result(
                                            result_payload={
                                                "signal_id": signal_id,
                                                "status": "error",
                                                "completed_at_ms": now_ms(),
                                                "legs": [
                                                    {
                                                        "venue": str(_as_dict(raw_leg).get("venue") or "").strip().lower(),
                                                        "submitted": False,
                                                    }
                                                    for raw_leg in list(plan_payload.get("legs") or [])
                                                ],
                                            },
                                            now_epoch_ms=now_ms(),
                                        ),
                                    )
                                    buy_fsm.fail_submission(
                                        signal_id=signal_id,
                                        error_payload=error_payload,
                                        now_epoch_ms=now_ms(),
                                        reason="submit_exception",
                                    )
                                    stats[f"{selected_action}_execution_errors"] += 1
                                    if selected_action == "sell":
                                        stats["last_sell_execution"] = error_payload
                                    else:
                                        stats["last_buy_execution"] = error_payload
                                    execution_event = {
                                        "status": "error",
                                        "action": selected_action,
                                        "error": error_payload,
                                    }

            buy_fsm_after = buy_fsm.snapshot()
            execution_lock_after = execution_lock.snapshot()
            decision_payload = {
                "can_trade": decision.can_trade,
                "can_buy": decision.can_buy,
                "can_sell": decision.can_sell,
                "selected_action_hint": selected_action_hint,
                "selected_action": selected_action,
                "health_can_trade": decision.health_can_trade,
                "decision_ready": decision.decision_ready,
                "buy_signal_ready": decision.buy_signal_ready,
                "sell_signal_ready": decision.sell_signal_ready,
                "hard_gate_state": decision.hard_gate_state,
                "health_reasons": decision.health_reasons,
                "quote_sanity": decision.quote_sanity,
                "execution_gate": decision.execution_gate,
                "buy_signal": decision.buy_signal,
                "sell_signal": decision.sell_signal,
                "execution_plan": decision.execution_plan,
                "execution_plan_reasons": decision.execution_plan_reasons,
                "sell_execution_plan": decision.sell_execution_plan,
                "sell_execution_plan_reasons": decision.sell_execution_plan_reasons,
                "gate_reasons": decision.gate_reasons,
                "buy_execution_enabled": bool(buy_execution_enabled),
                "sell_execution_enabled": bool(sell_execution_enabled),
                "position_monitoring_enabled": bool(position_runtime is not None),
                "buy_execution_max_attempts": int(max_attempts),
                "buy_execution_attempts_used": int(buy_execution_attempt_state.get("attempts_used", 0)),
                "execution": execution_event,
                # Backward compatibility for existing log consumers.
                "buy_execution": execution_event,
                "position_health": position_health,
                "buy_fsm": buy_fsm_after,
                "execution_lock": execution_lock_after,
                "execution_lock_block_reason": (
                    str(_as_dict(execution_event).get("reason"))
                    if str(_as_dict(execution_event).get("status") or "") == "blocked_execution_lock"
                    else None
                ),
            }
            stats["last_decision"] = decision_payload
            stats["buy_fsm"] = buy_fsm_after
            stats["execution_lock"] = execution_lock_after

            if execution_event is not None:
                logger.write_buy_execution(
                    recv_ms=now_epoch_ms,
                    payload={
                        "market": market_context,
                        "execution_plan": decision.execution_plan,
                        "execution_plan_reasons": decision.execution_plan_reasons,
                        "sell_execution_plan": decision.sell_execution_plan,
                        "sell_execution_plan_reasons": decision.sell_execution_plan_reasons,
                        "selected_action": selected_action,
                        "buy_execution_enabled": bool(buy_execution_enabled),
                        "sell_execution_enabled": bool(sell_execution_enabled),
                        "buy_execution_parallel_leg_timeout_ms": int(buy_parallel_leg_timeout_ms),
                        "sell_execution_parallel_leg_timeout_ms": int(sell_parallel_leg_timeout_ms),
                        "execution": execution_event,
                        # Backward compatibility for existing log consumers.
                        "buy_execution": execution_event,
                        "buy_fsm_before": buy_fsm_before,
                        "buy_fsm_after": buy_fsm_after,
                        "execution_lock_before": execution_lock_before,
                        "execution_lock_after": execution_lock_after,
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
        venues_bootstrap = _as_dict(bootstrap_result).get("venues")
        for venue_name in ("polymarket", "kalshi"):
            venue_payload = _as_dict(_as_dict(venues_bootstrap).get(venue_name))
            if str(venue_payload.get("status") or "").strip().lower() != "ok":
                continue
            _record_execution_lock_update(
                now_epoch_ms=now_ms(),
                source=f"position_poll_success:{venue_name}",
                update=execution_lock.mark_positions_reconcile_success(
                    venue=venue_name,
                    poll_at_ms=int(_as_dict(bootstrap_result).get("at_ms") or now_ms()),
                    now_epoch_ms=now_ms(),
                ),
            )

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
                venues = _as_dict(result).get("venues")
                for venue_name in ("polymarket", "kalshi"):
                    venue_payload = _as_dict(_as_dict(venues).get(venue_name))
                    if str(venue_payload.get("status") or "").strip().lower() != "ok":
                        continue
                    _record_execution_lock_update(
                        now_epoch_ms=now_ms(),
                        source=f"position_poll_success:{venue_name}",
                        update=execution_lock.mark_positions_reconcile_success(
                            venue=venue_name,
                            poll_at_ms=int(_as_dict(result).get("at_ms") or now_ms()),
                            now_epoch_ms=now_ms(),
                        ),
                    )
                if position_runtime is not None:
                    _record_execution_lock_update(
                        now_epoch_ms=now_ms(),
                        source="position_poll_terminal_refresh",
                        update=execution_lock.refresh_terminal_status(
                            has_open_orders_for_venue=position_runtime.has_open_orders_for_venue,
                            now_epoch_ms=now_ms(),
                        ),
                    )
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
        _record_execution_lock_update(
            now_epoch_ms=now_ms(),
            source="shutdown_terminal_refresh",
            update=execution_lock.refresh_terminal_status(
                has_open_orders_for_venue=position_runtime.has_open_orders_for_venue,
                now_epoch_ms=now_ms(),
            ),
        )
    stats["execution_lock"] = execution_lock.snapshot()

    return stats
