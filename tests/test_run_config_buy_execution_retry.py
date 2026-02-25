from __future__ import annotations

import json
from pathlib import Path

from scripts.common.run_config import (
    buy_execution_runtime_config_to_dict,
    decision_config_to_dict,
    load_buy_execution_runtime_config_from_run_config,
    load_decision_config_from_run_config,
    load_position_monitoring_runtime_config_from_run_config,
    load_sell_execution_runtime_config_from_run_config,
    position_monitoring_runtime_config_to_dict,
    sell_execution_runtime_config_to_dict,
)


def _write_config(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_buy_execution_retry_defaults_when_missing(tmp_path: Path) -> None:
    config_path = tmp_path / "run_config_defaults.json"
    _write_config(
        config_path,
        {
            "buy_execution": {
                "enabled": False,
                "cooldown_ms": 0,
                "max_attempts_per_run": 1,
            }
        },
    )
    config = load_buy_execution_runtime_config_from_run_config(config_path=config_path)
    assert config.api_retry.enabled is True
    assert config.api_retry.max_attempts == 3
    assert config.api_retry.base_backoff_seconds == 0.5
    assert config.api_retry.jitter_ratio == 0.2
    assert config.api_retry.include_post is True
    assert config.parallel_leg_timeout_ms == 4000


def test_buy_execution_retry_custom_values_round_trip(tmp_path: Path) -> None:
    config_path = tmp_path / "run_config_custom.json"
    _write_config(
        config_path,
        {
            "buy_execution": {
                "enabled": True,
                "cooldown_ms": 2500,
                "max_attempts_per_run": 2,
                "parallel_leg_timeout_ms": 2750,
                "api_retry": {
                    "enabled": True,
                    "max_attempts": 4,
                    "base_backoff_seconds": 0.25,
                    "jitter_ratio": 0.1,
                    "include_post": False,
                },
            }
        },
    )
    config = load_buy_execution_runtime_config_from_run_config(config_path=config_path)
    serialized = buy_execution_runtime_config_to_dict(config)
    assert serialized["enabled"] is True
    assert serialized["cooldown_ms"] == 2500
    assert serialized["max_attempts_per_run"] == 2
    assert serialized["parallel_leg_timeout_ms"] == 2750
    assert serialized["api_retry"] == {
        "enabled": True,
        "max_attempts": 4,
        "base_backoff_seconds": 0.25,
        "jitter_ratio": 0.1,
        "include_post": False,
    }


def test_decision_buy_sizing_defaults_when_missing(tmp_path: Path) -> None:
    config_path = tmp_path / "run_config_decision_defaults.json"
    _write_config(config_path, {})
    cfg = load_decision_config_from_run_config(config_path=config_path)
    assert cfg.buy.min_gross_edge_threshold == 0.04
    assert cfg.buy.max_spend_per_market_usd == 10.0
    assert cfg.buy.max_size_cap_per_leg == 20.0
    assert cfg.buy.min_size_per_leg_contracts == 0.0
    assert cfg.buy.min_notional_per_leg_usd == 0.0
    assert cfg.buy.best_ask_size_safety_factor == 0.5
    assert cfg.buy.market_emulation_slippage == 0.02
    assert cfg.sell.min_gross_edge_threshold == 0.0
    assert cfg.sell.max_size_cap_per_leg == 0.0
    assert cfg.sell.min_size_per_leg_contracts == 0.0
    assert cfg.sell.min_notional_per_leg_usd == 0.0
    assert cfg.sell.best_bid_size_safety_factor == 1.0
    assert cfg.sell.market_emulation_slippage == 0.02


def test_decision_buy_sizing_custom_values_round_trip(tmp_path: Path) -> None:
    config_path = tmp_path / "run_config_decision_custom.json"
    _write_config(
        config_path,
        {
            "buy": {
                "min_gross_edge_threshold": 0.05,
                "max_spend_per_market_usd": 12.5,
                "max_size_cap_per_leg": 15.0,
                "min_size_per_leg_contracts": 2.0,
                "min_notional_per_leg_usd": 1.75,
                "best_ask_size_safety_factor": 0.7,
                "market_emulation_slippage": 0.03,
            },
            "sell": {
                "min_gross_edge_threshold": 0.03,
                "max_size_cap_per_leg": 9.0,
                "min_size_per_leg_contracts": 2.0,
                "min_notional_per_leg_usd": 1.25,
                "best_bid_size_safety_factor": 0.6,
                "market_emulation_slippage": 0.02,
            },
        },
    )
    cfg = load_decision_config_from_run_config(config_path=config_path)
    serialized = decision_config_to_dict(cfg)
    assert serialized["buy"] == {
        "min_gross_edge_threshold": 0.05,
        "max_spend_per_market_usd": 12.5,
        "max_size_cap_per_leg": 15.0,
        "min_size_per_leg_contracts": 2.0,
        "min_notional_per_leg_usd": 1.75,
        "best_ask_size_safety_factor": 0.7,
        "market_emulation_slippage": 0.03,
    }
    assert serialized["sell"] == {
        "min_gross_edge_threshold": 0.03,
        "max_size_cap_per_leg": 9.0,
        "min_size_per_leg_contracts": 2.0,
        "min_notional_per_leg_usd": 1.25,
        "best_bid_size_safety_factor": 0.6,
        "market_emulation_slippage": 0.02,
    }


def test_sell_execution_defaults_when_missing(tmp_path: Path) -> None:
    config_path = tmp_path / "run_config_sell_defaults.json"
    _write_config(config_path, {})
    cfg = load_sell_execution_runtime_config_from_run_config(config_path=config_path)
    assert cfg.enabled is False
    assert cfg.parallel_leg_timeout_ms == 4000
    assert cfg.api_retry.enabled is True
    assert cfg.api_retry.max_attempts == 3
    assert cfg.api_retry.base_backoff_seconds == 0.5
    assert cfg.api_retry.jitter_ratio == 0.2
    assert cfg.api_retry.include_post is True


def test_sell_execution_custom_values_round_trip(tmp_path: Path) -> None:
    config_path = tmp_path / "run_config_sell_custom.json"
    _write_config(
        config_path,
        {
            "sell_execution": {
                "enabled": True,
                "parallel_leg_timeout_ms": 2500,
                "api_retry": {
                    "enabled": True,
                    "max_attempts": 4,
                    "base_backoff_seconds": 0.25,
                    "jitter_ratio": 0.1,
                    "include_post": False,
                },
            }
        },
    )
    cfg = load_sell_execution_runtime_config_from_run_config(config_path=config_path)
    serialized = sell_execution_runtime_config_to_dict(cfg)
    assert serialized == {
        "enabled": True,
        "parallel_leg_timeout_ms": 2500,
        "api_retry": {
            "enabled": True,
            "max_attempts": 4,
            "base_backoff_seconds": 0.25,
            "jitter_ratio": 0.1,
            "include_post": False,
        },
    }


def test_position_monitoring_defaults_when_missing(tmp_path: Path) -> None:
    config_path = tmp_path / "run_config_position_defaults.json"
    _write_config(config_path, {})
    cfg = load_position_monitoring_runtime_config_from_run_config(config_path=config_path)
    assert cfg.enabled is False
    assert cfg.require_bootstrap_before_buy is True
    assert cfg.polymarket_user_ws_enabled is True
    assert cfg.kalshi_market_positions_ws_enabled is True
    assert cfg.kalshi_user_orders_ws_enabled is True
    assert cfg.polymarket_poll_seconds == 10.0
    assert cfg.kalshi_poll_seconds == 20.0
    assert cfg.polymarket_orders_poll_seconds == 10.0
    assert cfg.kalshi_orders_poll_seconds == 20.0
    assert cfg.loop_sleep_seconds == 0.2
    assert cfg.drift_tolerance_contracts == 0.0
    assert cfg.max_exposure_per_market_usd == 0.0
    assert cfg.include_pending_orders_in_exposure is True
    assert cfg.stale_warning_seconds == 60
    assert cfg.stale_error_seconds == 180


def test_position_monitoring_custom_values_round_trip(tmp_path: Path) -> None:
    config_path = tmp_path / "run_config_position_custom.json"
    _write_config(
        config_path,
        {
            "position_monitoring": {
                "enabled": True,
                "require_bootstrap_before_buy": False,
                "polymarket_user_ws_enabled": False,
                "kalshi_market_positions_ws_enabled": True,
                "kalshi_user_orders_ws_enabled": False,
                "polymarket_poll_seconds": 7.5,
                "kalshi_poll_seconds": 11.0,
                "polymarket_orders_poll_seconds": 8.0,
                "kalshi_orders_poll_seconds": 12.0,
                "loop_sleep_seconds": 0.1,
                "drift_tolerance_contracts": 0.25,
                "max_exposure_per_market_usd": 12.5,
                "include_pending_orders_in_exposure": False,
                "stale_warning_seconds": 45,
                "stale_error_seconds": 90,
            }
        },
    )
    cfg = load_position_monitoring_runtime_config_from_run_config(config_path=config_path)
    serialized = position_monitoring_runtime_config_to_dict(cfg)
    assert serialized == {
        "enabled": True,
        "require_bootstrap_before_buy": False,
        "polymarket_user_ws_enabled": False,
        "kalshi_market_positions_ws_enabled": True,
        "kalshi_user_orders_ws_enabled": False,
        "polymarket_poll_seconds": 7.5,
        "kalshi_poll_seconds": 11.0,
        "polymarket_orders_poll_seconds": 8.0,
        "kalshi_orders_poll_seconds": 12.0,
        "loop_sleep_seconds": 0.1,
        "drift_tolerance_contracts": 0.25,
        "max_exposure_per_market_usd": 12.5,
        "include_pending_orders_in_exposure": False,
        "stale_warning_seconds": 45,
        "stale_error_seconds": 90,
    }
