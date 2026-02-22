from __future__ import annotations

import json
from pathlib import Path

from scripts.common.run_config import (
    buy_execution_runtime_config_to_dict,
    load_buy_execution_runtime_config_from_run_config,
    load_position_monitoring_runtime_config_from_run_config,
    position_monitoring_runtime_config_to_dict,
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


def test_buy_execution_retry_custom_values_round_trip(tmp_path: Path) -> None:
    config_path = tmp_path / "run_config_custom.json"
    _write_config(
        config_path,
        {
            "buy_execution": {
                "enabled": True,
                "cooldown_ms": 2500,
                "max_attempts_per_run": 2,
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
    assert serialized["api_retry"] == {
        "enabled": True,
        "max_attempts": 4,
        "base_backoff_seconds": 0.25,
        "jitter_ratio": 0.1,
        "include_post": False,
    }


def test_position_monitoring_defaults_when_missing(tmp_path: Path) -> None:
    config_path = tmp_path / "run_config_position_defaults.json"
    _write_config(config_path, {})
    cfg = load_position_monitoring_runtime_config_from_run_config(config_path=config_path)
    assert cfg.enabled is False
    assert cfg.require_bootstrap_before_buy is True
    assert cfg.polymarket_user_ws_enabled is True
    assert cfg.kalshi_market_positions_ws_enabled is True
    assert cfg.polymarket_poll_seconds == 10.0
    assert cfg.kalshi_poll_seconds == 20.0
    assert cfg.loop_sleep_seconds == 0.2
    assert cfg.drift_tolerance_contracts == 0.0
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
                "polymarket_poll_seconds": 7.5,
                "kalshi_poll_seconds": 11.0,
                "loop_sleep_seconds": 0.1,
                "drift_tolerance_contracts": 0.25,
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
        "polymarket_poll_seconds": 7.5,
        "kalshi_poll_seconds": 11.0,
        "loop_sleep_seconds": 0.1,
        "drift_tolerance_contracts": 0.25,
        "stale_warning_seconds": 45,
        "stale_error_seconds": 90,
    }
