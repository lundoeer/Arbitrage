from __future__ import annotations

import time
from typing import Any, Dict

from scripts.common.buy_execution import (
    BuyExecutionClients,
    BuyExecutionPlan,
    BuyIdempotencyState,
    execute_cross_venue_buy,
)


class _DelayBuyClient:
    def __init__(self, *, venue: str, delay_seconds: float, should_fail: bool = False) -> None:
        self.venue = str(venue)
        self.delay_seconds = float(delay_seconds)
        self.should_fail = bool(should_fail)
        self.calls = 0

    def place_buy_order(
        self,
        *,
        instrument_id: str,
        side: str,
        size: float,
        order_kind: str,
        limit_price: float | None,
        time_in_force: str | None,
        client_order_id: str,
        planning_reference_best_ask: float | None = None,
    ) -> Dict[str, Any]:
        self.calls += 1
        time.sleep(self.delay_seconds)
        if self.should_fail:
            raise RuntimeError(f"submit_failed:{self.venue}")
        return {
            "ok": True,
            "venue": self.venue,
            "request": {
                "instrument_id": instrument_id,
                "side": side,
                "size": float(size),
                "order_kind": order_kind,
                "limit_price": None if limit_price is None else float(limit_price),
                "time_in_force": time_in_force,
                "client_order_id": client_order_id,
                "planning_reference_best_ask": (
                    None if planning_reference_best_ask is None else float(planning_reference_best_ask)
                ),
            },
            "response": {"id": f"{self.venue}-{self.calls}"},
        }


def _two_leg_plan(signal_id: str) -> BuyExecutionPlan:
    return BuyExecutionPlan.from_dict(
        {
            "signal_id": signal_id,
            "market": {
                "polymarket_market_id": "pm-1",
                "kalshi_ticker": "KXBTC15M-TEST",
            },
            "created_at_ms": 1730000000000,
            "execution_mode": "two_leg_parallel",
            "legs": [
                {
                    "venue": "kalshi",
                    "side": "yes",
                    "action": "buy",
                    "instrument_id": "KXBTC15M-TEST",
                    "order_kind": "limit",
                    "limit_price": 0.45,
                    "size": 1.0,
                    "time_in_force": "fok",
                    "client_order_id_seed": "kx-leg",
                },
                {
                    "venue": "polymarket",
                    "side": "no",
                    "action": "buy",
                    "instrument_id": "pm-token-no",
                    "order_kind": "limit",
                    "limit_price": 0.55,
                    "size": 1.0,
                    "time_in_force": "fok",
                    "client_order_id_seed": "pm-leg",
                },
            ],
            "max_quote_age_ms": 150,
        }
    )


def test_parallel_leg_submit_runtime_two_500ms_legs_finishes_under_900ms() -> None:
    kalshi = _DelayBuyClient(venue="kalshi", delay_seconds=0.5, should_fail=False)
    polymarket = _DelayBuyClient(venue="polymarket", delay_seconds=0.5, should_fail=False)
    clients = BuyExecutionClients(polymarket=polymarket, kalshi=kalshi)
    state = BuyIdempotencyState()
    plan = _two_leg_plan(signal_id="sig-parallel-runtime-1")

    started = time.perf_counter()
    result = execute_cross_venue_buy(
        plan=plan,
        clients=clients,
        state=state,
        now_epoch_ms=1730000000100,
        parallel_leg_timeout_ms=3000,
    )
    elapsed = time.perf_counter() - started

    assert result.status == "submitted"
    assert elapsed < 0.9
    assert len(result.legs) == 2
    assert abs(result.legs[0].submit_started_ms - result.legs[1].submit_started_ms) < 250


def test_parallel_submit_one_success_one_failure_is_partially_submitted() -> None:
    kalshi = _DelayBuyClient(venue="kalshi", delay_seconds=0.05, should_fail=False)
    polymarket = _DelayBuyClient(venue="polymarket", delay_seconds=0.05, should_fail=True)
    clients = BuyExecutionClients(polymarket=polymarket, kalshi=kalshi)
    state = BuyIdempotencyState()
    plan = _two_leg_plan(signal_id="sig-parallel-partial-1")

    result = execute_cross_venue_buy(
        plan=plan,
        clients=clients,
        state=state,
        now_epoch_ms=1730000000200,
        parallel_leg_timeout_ms=2000,
    )

    assert result.status == "partially_submitted"
    assert sum(1 for leg in result.legs if leg.submitted) == 1
    assert sum(1 for leg in result.legs if not leg.submitted) == 1


def test_parallel_submit_idempotency_short_circuit_skips_all_submits() -> None:
    kalshi = _DelayBuyClient(venue="kalshi", delay_seconds=0.05, should_fail=False)
    polymarket = _DelayBuyClient(venue="polymarket", delay_seconds=0.05, should_fail=False)
    clients = BuyExecutionClients(polymarket=polymarket, kalshi=kalshi)
    state = BuyIdempotencyState()
    plan = _two_leg_plan(signal_id="sig-parallel-idempotent-1")

    first = execute_cross_venue_buy(
        plan=plan,
        clients=clients,
        state=state,
        now_epoch_ms=1730000000300,
        parallel_leg_timeout_ms=2000,
    )
    second = execute_cross_venue_buy(
        plan=plan,
        clients=clients,
        state=state,
        now_epoch_ms=1730000000400,
        parallel_leg_timeout_ms=2000,
    )

    assert first.status == "submitted"
    assert second.status == "skipped_idempotent"
    assert second.legs == []
    assert kalshi.calls == 1
    assert polymarket.calls == 1


def test_parallel_submit_timeout_marks_leg_failed_and_returns_promptly() -> None:
    kalshi = _DelayBuyClient(venue="kalshi", delay_seconds=1.0, should_fail=False)
    polymarket = _DelayBuyClient(venue="polymarket", delay_seconds=0.1, should_fail=False)
    clients = BuyExecutionClients(polymarket=polymarket, kalshi=kalshi)
    state = BuyIdempotencyState()
    plan = _two_leg_plan(signal_id="sig-parallel-timeout-1")

    started = time.perf_counter()
    result = execute_cross_venue_buy(
        plan=plan,
        clients=clients,
        state=state,
        now_epoch_ms=1730000000500,
        parallel_leg_timeout_ms=200,
    )
    elapsed = time.perf_counter() - started

    assert elapsed < 0.7
    assert result.status == "partially_submitted"
    timed_out = [leg for leg in result.legs if (leg.error or "").startswith("timeout_after_ms:")]
    assert len(timed_out) == 1
    assert timed_out[0].error == "timeout_after_ms:200"


def test_parallel_submit_results_preserve_original_leg_order() -> None:
    kalshi = _DelayBuyClient(venue="kalshi", delay_seconds=0.3, should_fail=False)
    polymarket = _DelayBuyClient(venue="polymarket", delay_seconds=0.05, should_fail=False)
    clients = BuyExecutionClients(polymarket=polymarket, kalshi=kalshi)
    state = BuyIdempotencyState()
    plan = _two_leg_plan(signal_id="sig-parallel-order-1")

    result = execute_cross_venue_buy(
        plan=plan,
        clients=clients,
        state=state,
        now_epoch_ms=1730000000600,
        parallel_leg_timeout_ms=2000,
    )

    assert result.status == "submitted"
    assert [leg.leg_index for leg in result.legs] == [0, 1]
    assert [leg.venue for leg in result.legs] == [plan.legs[0].venue, plan.legs[1].venue]


def test_parallel_preflight_rejects_polymarket_market_buy_without_planning_reference_best_ask() -> None:
    kalshi = _DelayBuyClient(venue="kalshi", delay_seconds=0.05, should_fail=False)
    polymarket = _DelayBuyClient(venue="polymarket", delay_seconds=0.05, should_fail=False)
    clients = BuyExecutionClients(polymarket=polymarket, kalshi=kalshi)
    state = BuyIdempotencyState()
    plan = BuyExecutionPlan.from_dict(
        {
            "signal_id": "sig-parallel-preflight-best-ask-1",
            "market": {"polymarket_market_id": "pm-1", "kalshi_ticker": "KXBTC15M-TEST"},
            "created_at_ms": 1730000000000,
            "execution_mode": "two_leg_parallel",
            "legs": [
                {
                    "venue": "kalshi",
                    "side": "yes",
                    "action": "buy",
                    "instrument_id": "KXBTC15M-TEST",
                    "order_kind": "market",
                    "limit_price": 0.65,
                    "size": 2.0,
                    "time_in_force": "fak",
                },
                {
                    "venue": "polymarket",
                    "side": "no",
                    "action": "buy",
                    "instrument_id": "pm-token-no",
                    "order_kind": "market",
                    "limit_price": 0.55,
                    "size": 2.0,
                    "time_in_force": "fak",
                    # no metadata.planning_reference_best_ask on purpose
                },
            ],
        }
    )

    result = execute_cross_venue_buy(
        plan=plan,
        clients=clients,
        state=state,
        now_epoch_ms=1730000000700,
        parallel_leg_timeout_ms=2000,
    )

    assert result.status == "rejected"
    assert result.error == "preflight_validation_failed"
    assert kalshi.calls == 0
    assert polymarket.calls == 0
    assert len(result.legs) == 2
    assert any((leg.error or "").startswith("preflight_missing_positive_planning_reference_best_ask") for leg in result.legs)
