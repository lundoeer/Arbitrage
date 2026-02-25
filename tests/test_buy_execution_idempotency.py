from __future__ import annotations

from typing import Any, Dict

from scripts.common.buy_execution import (
    BuyExecutionClients,
    BuyExecutionPlan,
    BuyIdempotencyState,
    execute_cross_venue_buy,
)


class _DummyBuyClient:
    def __init__(self, venue: str) -> None:
        self.venue = str(venue)
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


def test_execute_cross_venue_buy_is_idempotent_for_same_signal() -> None:
    kalshi = _DummyBuyClient("kalshi")
    polymarket = _DummyBuyClient("polymarket")
    clients = BuyExecutionClients(polymarket=polymarket, kalshi=kalshi)
    state = BuyIdempotencyState()
    plan = _two_leg_plan(signal_id="sig-idempotent-1")

    first = execute_cross_venue_buy(plan=plan, clients=clients, state=state, now_epoch_ms=1730000000100)
    assert first.status == "submitted"
    assert len(first.legs) == 2
    assert kalshi.calls == 1
    assert polymarket.calls == 1

    second = execute_cross_venue_buy(plan=plan, clients=clients, state=state, now_epoch_ms=1730000000200)
    assert second.status == "skipped_idempotent"
    assert second.legs == []
    assert kalshi.calls == 1
    assert polymarket.calls == 1


def test_execute_cross_venue_buy_skips_when_signal_already_in_flight() -> None:
    kalshi = _DummyBuyClient("kalshi")
    polymarket = _DummyBuyClient("polymarket")
    clients = BuyExecutionClients(polymarket=polymarket, kalshi=kalshi)
    state = BuyIdempotencyState()
    signal_id = "sig-in-flight-1"
    state.mark_in_flight(signal_id=signal_id, client_order_ids={"leg_1": "existing"})
    plan = _two_leg_plan(signal_id=signal_id)

    result = execute_cross_venue_buy(plan=plan, clients=clients, state=state, now_epoch_ms=1730000000300)
    assert result.status == "skipped_idempotent"
    assert result.idempotency.get("prior_status") == "in_flight"
    assert kalshi.calls == 0
    assert polymarket.calls == 0
