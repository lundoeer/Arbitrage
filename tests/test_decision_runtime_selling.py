from __future__ import annotations

from typing import Any, Dict

from scripts.common.decision_runtime import DecisionRuntime
from scripts.common.run_config import (
    BuyDecisionConfig,
    DecisionConfig,
    ExecutionDecisionConfig,
    SellDecisionConfig,
)


def _market_context() -> Dict[str, Any]:
    return {
        "polymarket_event_slug": "btc-updown-15m-123",
        "polymarket_market_id": "pm-market-1",
        "polymarket_token_yes": "pm-yes-token",
        "polymarket_token_no": "pm-no-token",
        "kalshi_ticker": "KXBTC15M-TEST",
        "market_window_end_epoch_ms": 1_750_000_000_000,
    }


def _quotes() -> Dict[str, Any]:
    return {
        "legs": {
            "polymarket_yes": {"best_bid": 0.66, "best_ask": 0.67, "best_bid_size": 6.0, "best_ask_size": 6.0},
            "polymarket_no": {"best_bid": 0.29, "best_ask": 0.30, "best_bid_size": 8.0, "best_ask_size": 8.0},
            "kalshi_yes": {"best_bid": 0.34, "best_ask": 0.35, "best_bid_size": 10.0, "best_ask_size": 10.0},
            "kalshi_no": {"best_bid": 0.45, "best_ask": 0.46, "best_bid_size": 5.0, "best_ask_size": 5.0},
        }
    }


def _decision_config() -> DecisionConfig:
    return DecisionConfig(
        buy=BuyDecisionConfig(
            min_gross_edge_threshold=0.04,
            max_spend_per_market_usd=10.0,
            max_size_cap_per_leg=20.0,
            min_size_per_leg_contracts=0.0,
            min_notional_per_leg_usd=0.0,
            best_ask_size_safety_factor=1.0,
        ),
        sell=SellDecisionConfig(
            min_gross_edge_threshold=0.05,
            max_size_cap_per_leg=0.0,
            min_size_per_leg_contracts=0.0,
            min_notional_per_leg_usd=0.0,
            best_bid_size_safety_factor=1.0,
            market_emulation_slippage=0.02,
        ),
        execution=ExecutionDecisionConfig(
            best_ask_and_bids_at_max=0.95,
            best_ask_and_bids_at_min=0.05,
            market_start_trade_time_seconds=60,
            market_end_trade_time_seconds=60,
            market_duration_seconds=900,
        ),
    )


def _health() -> Dict[str, Any]:
    return {"decision_ok": True, "transport_reasons": [], "market_data_reasons": []}


def _position_snapshot(*, pm_yes: float, kx_no: float) -> Dict[str, Any]:
    return {
        "positions_by_key": {
            "polymarket|pm-yes-token|yes": {"net_contracts": float(pm_yes)},
            "kalshi|KXBTC15M-TEST|no": {"net_contracts": float(kx_no)},
        }
    }


def test_sell_selected_over_buy_when_both_are_eligible() -> None:
    decision = DecisionRuntime.evaluate(
        kalshi_health=_health(),
        polymarket_health=_health(),
        quotes=_quotes(),
        market_context=_market_context(),
        decision_config=_decision_config(),
        position_snapshot=_position_snapshot(pm_yes=10.0, kx_no=8.0),
        now_epoch_ms=1_740_000_000_000,
        market_window_end_epoch_ms=1_740_000_300_000,
    )

    assert decision.can_trade is True
    assert decision.can_buy is True
    assert decision.can_sell is True
    assert decision.selected_action_hint == "sell"
    assert decision.sell_execution_plan is not None
    legs = decision.sell_execution_plan["legs"]
    assert all(leg["action"] == "sell" for leg in legs)
    assert legs[0]["limit_price"] == 0.64
    assert legs[1]["limit_price"] == 0.43
    assert legs[0]["size"] == 5.0
    assert legs[1]["size"] == 5.0


def test_insufficient_sell_positions_does_not_block_buy() -> None:
    decision = DecisionRuntime.evaluate(
        kalshi_health=_health(),
        polymarket_health=_health(),
        quotes=_quotes(),
        market_context=_market_context(),
        decision_config=_decision_config(),
        position_snapshot=_position_snapshot(pm_yes=0.0, kx_no=0.0),
        now_epoch_ms=1_740_000_000_000,
        market_window_end_epoch_ms=1_740_000_300_000,
    )

    assert decision.can_trade is True
    assert decision.can_buy is True
    assert decision.can_sell is False
    assert decision.selected_action_hint == "buy"
    assert decision.execution_plan is not None
    assert decision.sell_execution_plan is None
    assert "insufficient_available_position:polymarket_yes" in decision.sell_execution_plan_reasons
