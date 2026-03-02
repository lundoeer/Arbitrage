from __future__ import annotations

from typing import Any, Dict

from scripts.common.decision_runtime import DecisionRuntime
from scripts.common.run_config import (
    BuyDecisionConfig,
    DecisionConfig,
    ExecutionDecisionConfig,
    SellDecisionConfig,
)


def _quotes() -> Dict[str, Any]:
    return {
        "legs": {
            "polymarket_yes": {"best_bid": 0.40, "best_ask": 0.41, "best_bid_size": 10.0, "best_ask_size": 10.0},
            "polymarket_no": {"best_bid": 0.58, "best_ask": 0.59, "best_bid_size": 10.0, "best_ask_size": 10.0},
            "kalshi_yes": {"best_bid": 0.39, "best_ask": 0.40, "best_bid_size": 10.0, "best_ask_size": 10.0},
            "kalshi_no": {"best_bid": 0.57, "best_ask": 0.58, "best_bid_size": 10.0, "best_ask_size": 10.0},
        }
    }


def _decision_config() -> DecisionConfig:
    return DecisionConfig(
        buy=BuyDecisionConfig(
            min_gross_edge_threshold=0.0,
            max_spend_per_market_usd=10.0,
            max_size_cap_per_leg=10.0,
            min_size_per_leg_contracts=0.0,
            min_notional_per_leg_usd=0.0,
            best_ask_size_safety_factor=1.0,
        ),
        sell=SellDecisionConfig(
            min_gross_edge_threshold=0.0,
            max_size_cap_per_leg=0.0,
            min_size_per_leg_contracts=0.0,
            min_notional_per_leg_usd=0.0,
            best_bid_size_safety_factor=1.0,
            market_emulation_slippage=0.02,
        ),
        execution=ExecutionDecisionConfig(
            best_ask_and_bids_at_max=0.95,
            best_ask_and_bids_at_min=0.05,
            market_start_trade_time_seconds=0,
            market_end_trade_time_seconds=0,
            market_duration_seconds=900,
        ),
    )


def _health() -> Dict[str, Any]:
    return {"decision_ok": True, "transport_reasons": [], "market_data_reasons": []}


def _market_context(*, disable_market_not_open_check: bool) -> Dict[str, Any]:
    return {
        "polymarket_event_slug": "lol-wb-tes-2026-03-02",
        "polymarket_market_id": "1465732",
        "polymarket_token_yes": "pm-yes",
        "polymarket_token_no": "pm-no",
        "kalshi_ticker": "KXLOLGAME-26MAR02TESWB-WB",
        "disable_market_not_open_check": bool(disable_market_not_open_check),
    }


def test_market_not_open_blocks_without_override() -> None:
    now_ms = 1_740_000_000_000
    window_end_ms = now_ms + (3600 * 1000)
    decision = DecisionRuntime.evaluate(
        kalshi_health=_health(),
        polymarket_health=_health(),
        quotes=_quotes(),
        market_context=_market_context(disable_market_not_open_check=False),
        decision_config=_decision_config(),
        now_epoch_ms=now_ms,
        market_window_end_epoch_ms=window_end_ms,
    )
    reasons = list(decision.execution_gate.get("time_window", {}).get("reasons") or [])
    assert "market_not_open" in reasons
    assert decision.execution_gate.get("time_window", {}).get("market_not_open_check_disabled") is False
    assert decision.execution_gate.get("pass") is False


def test_market_not_open_skipped_with_override() -> None:
    now_ms = 1_740_000_000_000
    window_end_ms = now_ms + (3600 * 1000)
    decision = DecisionRuntime.evaluate(
        kalshi_health=_health(),
        polymarket_health=_health(),
        quotes=_quotes(),
        market_context=_market_context(disable_market_not_open_check=True),
        decision_config=_decision_config(),
        now_epoch_ms=now_ms,
        market_window_end_epoch_ms=window_end_ms,
    )
    reasons = list(decision.execution_gate.get("time_window", {}).get("reasons") or [])
    assert "market_not_open" not in reasons
    assert decision.execution_gate.get("time_window", {}).get("market_not_open_check_disabled") is True
    assert decision.execution_gate.get("pass") is True
