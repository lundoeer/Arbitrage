from __future__ import annotations

from typing import Any, Dict, Tuple

from scripts.common.decision_runtime import _build_execution_plan
from scripts.common.run_config import BuyDecisionConfig, DecisionConfig, ExecutionDecisionConfig, SellDecisionConfig


def _market_context() -> Dict[str, Any]:
    return {
        "polymarket_event_slug": "btc-updown-15m-123",
        "polymarket_market_id": "pm-market-1",
        "polymarket_token_yes": "pm-yes-token",
        "polymarket_token_no": "pm-no-token",
        "kalshi_ticker": "KXBTC15M-TEST",
        "market_window_end_epoch_ms": 1_750_000_000_000,
    }


def _buy_signal(*, yes_leg: str = "polymarket_yes", no_leg: str = "kalshi_no", total_ask: float = 0.80) -> Dict[str, Any]:
    return {
        "best_candidate": {
            "name": "buy_polymarket_yes_and_kalshi_no",
            "yes_leg": yes_leg,
            "no_leg": no_leg,
            "total_ask": float(total_ask),
        }
    }


def _quotes(
    *,
    yes_leg: str = "polymarket_yes",
    no_leg: str = "kalshi_no",
    yes_ask: float = 0.40,
    no_ask: float = 0.40,
    yes_ask_size: float = 20.0,
    no_ask_size: float = 20.0,
) -> Dict[str, Any]:
    return {
        "legs": {
            yes_leg: {
                "best_ask": float(yes_ask),
                "best_ask_size": float(yes_ask_size),
                "quote_age_ms": 50,
            },
            no_leg: {
                "best_ask": float(no_ask),
                "best_ask_size": float(no_ask_size),
                "quote_age_ms": 50,
            },
        }
    }


def _decision_config(**buy_overrides: Any) -> DecisionConfig:
    buy_payload = {
        "min_gross_edge_threshold": 0.05,
        "max_spend_per_market_usd": 10.0,
        "max_size_cap_per_leg": 20.0,
        "min_size_per_leg_contracts": 0.0,
        "min_notional_per_leg_usd": 0.0,
        "best_ask_size_safety_factor": 0.5,
        "market_emulation_slippage": 0.02,
        **buy_overrides,
    }
    buy = BuyDecisionConfig(**buy_payload)
    return DecisionConfig(
        buy=buy,
        sell=SellDecisionConfig(),
        execution=ExecutionDecisionConfig(),
    )


def _build(
    *,
    cfg: DecisionConfig,
    yes_ask: float = 0.40,
    no_ask: float = 0.40,
    yes_ask_size: float = 20.0,
    no_ask_size: float = 20.0,
    total_ask: float = 0.80,
) -> Tuple[Dict[str, Any] | None, list[str]]:
    return _build_execution_plan(
        buy_signal=_buy_signal(total_ask=total_ask),
        quotes=_quotes(
            yes_ask=yes_ask,
            no_ask=no_ask,
            yes_ask_size=yes_ask_size,
            no_ask_size=no_ask_size,
        ),
        market_context=_market_context(),
        decision_config=cfg,
        now_epoch_ms=1_740_000_000_000,
        market_window_end_epoch_ms=1_750_000_000_000,
    )


def test_budget_cap_controls_size() -> None:
    plan, reasons = _build(cfg=_decision_config())
    assert reasons == []
    assert plan is not None
    assert plan["legs"][0]["size"] == 10.0
    assert plan["legs"][1]["size"] == 10.0
    assert plan["policy"]["computed_caps"]["cap_by_spend"] == 12.5


def test_config_size_cap_controls_size() -> None:
    plan, reasons = _build(cfg=_decision_config(max_size_cap_per_leg=3.0))
    assert reasons == []
    assert plan is not None
    assert plan["legs"][0]["size"] == 3.0
    assert plan["legs"][1]["size"] == 3.0


def test_top_of_book_cap_controls_size() -> None:
    plan, reasons = _build(
        cfg=_decision_config(),
        yes_ask_size=8.0,
        no_ask_size=6.0,
    )
    assert reasons == []
    assert plan is not None
    # min(8,6) * 0.5 = 3.0
    assert plan["legs"][0]["size"] == 3.0
    assert plan["legs"][1]["size"] == 3.0


def test_safety_factor_reduces_size() -> None:
    plan_low, reasons_low = _build(
        cfg=_decision_config(best_ask_size_safety_factor=0.5),
        yes_ask_size=10.0,
        no_ask_size=10.0,
    )
    plan_high, reasons_high = _build(
        cfg=_decision_config(best_ask_size_safety_factor=1.0),
        yes_ask_size=10.0,
        no_ask_size=10.0,
    )
    assert reasons_low == []
    assert reasons_high == []
    assert plan_low is not None
    assert plan_high is not None
    assert plan_low["legs"][0]["size"] == 5.0
    assert plan_high["legs"][0]["size"] == 10.0


def test_min_size_per_leg_contracts_blocks_when_not_met() -> None:
    plan, reasons = _build(
        cfg=_decision_config(min_size_per_leg_contracts=7.0),
        yes_ask_size=10.0,
        no_ask_size=10.0,
    )
    assert plan is None
    assert reasons == ["computed_size_below_min_size_per_leg_contracts"]


def test_min_notional_per_leg_usd_blocks_yes_leg() -> None:
    plan, reasons = _build(
        cfg=_decision_config(min_notional_per_leg_usd=3.0),
        yes_ask=0.20,
        no_ask=0.60,
        total_ask=0.80,
        yes_ask_size=20.0,
        no_ask_size=20.0,
    )
    assert plan is None
    assert reasons == ["computed_notional_below_min_notional_per_leg_usd:polymarket_yes"]


def test_min_notional_per_leg_usd_blocks_no_leg() -> None:
    plan, reasons = _build(
        cfg=_decision_config(min_notional_per_leg_usd=3.0),
        yes_ask=0.60,
        no_ask=0.20,
        total_ask=0.80,
        yes_ask_size=20.0,
        no_ask_size=20.0,
    )
    assert plan is None
    assert reasons == ["computed_notional_below_min_notional_per_leg_usd:kalshi_no"]


def test_zero_thresholds_disable_min_checks() -> None:
    plan, reasons = _build(
        cfg=_decision_config(min_size_per_leg_contracts=0.0, min_notional_per_leg_usd=0.0),
        yes_ask=0.20,
        no_ask=0.20,
        total_ask=0.40,
    )
    assert reasons == []
    assert plan is not None


def test_invalid_safety_factor_blocks_plan() -> None:
    plan, reasons = _build(cfg=_decision_config(best_ask_size_safety_factor=0.0))
    assert plan is None
    assert reasons == ["invalid_best_ask_size_safety_factor"]


def test_missing_best_ask_size_blocks_plan() -> None:
    plan, reasons = _build_execution_plan(
        buy_signal=_buy_signal(),
        quotes={
            "legs": {
                "polymarket_yes": {"best_ask": 0.40, "quote_age_ms": 50},
                "kalshi_no": {"best_ask": 0.40, "best_ask_size": 10.0, "quote_age_ms": 50},
            }
        },
        market_context=_market_context(),
        decision_config=_decision_config(),
        now_epoch_ms=1_740_000_000_000,
        market_window_end_epoch_ms=1_750_000_000_000,
    )
    assert plan is None
    assert reasons == ["missing_or_invalid_best_ask_size:polymarket_yes"]


def test_policy_contains_detailed_sizing_fields() -> None:
    plan, reasons = _build(cfg=_decision_config())
    assert reasons == []
    assert plan is not None
    policy = plan["policy"]
    assert policy["sizing_mode"] == "top_of_book_budget_caps_with_min_thresholds"
    assert policy["best_ask_size_safety_factor"] == 0.5
    assert policy["market_emulation_slippage"] == 0.02
    assert "computed_caps" in policy
    assert "inputs" in policy
    assert "final_leg_notional_usd" in policy
    assert policy["size_rounding"] == "floor_to_whole_contract"


def test_buy_market_emulation_slippage_is_configurable() -> None:
    plan_default, reasons_default = _build(cfg=_decision_config(market_emulation_slippage=0.02))
    plan_custom, reasons_custom = _build(cfg=_decision_config(market_emulation_slippage=0.05))
    assert reasons_default == []
    assert reasons_custom == []
    assert plan_default is not None
    assert plan_custom is not None
    default_limit = float(plan_default["legs"][0]["limit_price"])
    custom_limit = float(plan_custom["legs"][0]["limit_price"])
    assert default_limit == 0.42
    assert custom_limit == 0.45
    assert plan_custom["legs"][0]["metadata"]["market_emulation_slippage"] == 0.05
