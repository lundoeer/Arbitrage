from __future__ import annotations

import json
from pathlib import Path

from scripts.common.run_config import (
    buy_execution_runtime_config_to_dict,
    load_buy_execution_runtime_config_from_run_config,
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
