from __future__ import annotations

from typing import Any, Dict

from scripts.common.buy_execution import (
    BuyExecutionClients,
    BuyExecutionPlan,
    BuyIdempotencyState,
    execute_cross_venue_buy,
)


class _DummyDualActionClient:
    def __init__(self, venue: str) -> None:
        self.venue = str(venue)
        self.buy_calls = 0
        self.sell_calls = 0

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
        self.buy_calls += 1
        return {"ok": True, "venue": self.venue}

    def place_sell_order(
        self,
        *,
        instrument_id: str,
        side: str,
        size: float,
        order_kind: str,
        limit_price: float | None,
        time_in_force: str | None,
        client_order_id: str,
    ) -> Dict[str, Any]:
        self.sell_calls += 1
        return {"ok": True, "venue": self.venue}


def test_parallel_sell_plan_uses_sell_client_methods() -> None:
    kalshi = _DummyDualActionClient("kalshi")
    polymarket = _DummyDualActionClient("polymarket")
    clients = BuyExecutionClients(polymarket=polymarket, kalshi=kalshi)
    state = BuyIdempotencyState()
    plan = BuyExecutionPlan.from_dict(
        {
            "signal_id": "sig-sell-parallel-1",
            "market": {"polymarket_market_id": "pm-1", "kalshi_ticker": "KXBTC15M-TEST"},
            "created_at_ms": 1730000000000,
            "execution_mode": "two_leg_parallel",
            "legs": [
                {
                    "venue": "kalshi",
                    "side": "no",
                    "action": "sell",
                    "instrument_id": "KXBTC15M-TEST",
                    "order_kind": "market",
                    "limit_price": 0.45,
                    "size": 1.0,
                    "time_in_force": "fak",
                },
                {
                    "venue": "polymarket",
                    "side": "yes",
                    "action": "sell",
                    "instrument_id": "pm-token-yes",
                    "order_kind": "market",
                    "limit_price": 0.65,
                    "size": 1.0,
                    "time_in_force": "fak",
                },
            ],
        }
    )

    result = execute_cross_venue_buy(
        plan=plan,
        clients=clients,
        state=state,
        now_epoch_ms=1730000000100,
        parallel_leg_timeout_ms=2000,
    )

    assert result.status == "submitted"
    assert kalshi.sell_calls == 1
    assert polymarket.sell_calls == 1
    assert kalshi.buy_calls == 0
    assert polymarket.buy_calls == 0
