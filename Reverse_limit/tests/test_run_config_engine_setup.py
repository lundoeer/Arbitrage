from __future__ import annotations

import json
from pathlib import Path

from scripts.common.engine_setup import build_buy_execution_transport
from scripts.common.run_config import load_buy_execution_runtime_config_from_run_config


def _write_config(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_load_buy_execution_runtime_config_reads_values_exactly(tmp_path: Path) -> None:
    config_path = tmp_path / "run_config.json"
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

    cfg = load_buy_execution_runtime_config_from_run_config(config_path=config_path)
    assert cfg.enabled is True
    assert cfg.cooldown_ms == 2500
    assert cfg.max_attempts_per_run == 2
    assert cfg.parallel_leg_timeout_ms == 2750
    assert cfg.api_retry.enabled is True
    assert cfg.api_retry.max_attempts == 4
    assert cfg.api_retry.base_backoff_seconds == 0.25
    assert cfg.api_retry.jitter_ratio == 0.1
    assert cfg.api_retry.include_post is False


def test_load_buy_execution_runtime_config_clamps_invalid_values(tmp_path: Path) -> None:
    config_path = tmp_path / "run_config_bad_values.json"
    _write_config(
        config_path,
        {
            "buy_execution": {
                "enabled": True,
                "cooldown_ms": -7,
                "max_attempts_per_run": -1,
                "parallel_leg_timeout_ms": 0,
                "api_retry": {
                    "enabled": True,
                    "max_attempts": 0,
                    "base_backoff_seconds": -10,
                    "jitter_ratio": 99,
                    "include_post": True,
                },
            }
        },
    )

    cfg = load_buy_execution_runtime_config_from_run_config(config_path=config_path)
    assert cfg.cooldown_ms == 0
    assert cfg.max_attempts_per_run == 0
    assert cfg.parallel_leg_timeout_ms == 100
    assert cfg.api_retry.max_attempts == 1
    assert cfg.api_retry.base_backoff_seconds == 0.0
    assert cfg.api_retry.jitter_ratio == 1.0


def test_build_buy_execution_transport_respects_include_post_true(tmp_path: Path) -> None:
    config_path = tmp_path / "run_config_post_enabled.json"
    _write_config(
        config_path,
        {
            "buy_execution": {
                "api_retry": {
                    "enabled": True,
                    "max_attempts": 3,
                    "base_backoff_seconds": 0.5,
                    "jitter_ratio": 0.2,
                    "include_post": True,
                }
            }
        },
    )

    cfg = load_buy_execution_runtime_config_from_run_config(config_path=config_path)
    transport = build_buy_execution_transport(buy_execution_config=cfg)
    assert "POST" in transport.retry.retry_methods


def test_build_buy_execution_transport_excludes_post_when_disabled(tmp_path: Path) -> None:
    config_path = tmp_path / "run_config_post_disabled.json"
    _write_config(
        config_path,
        {
            "buy_execution": {
                "api_retry": {
                    "enabled": True,
                    "max_attempts": 3,
                    "base_backoff_seconds": 0.5,
                    "jitter_ratio": 0.2,
                    "include_post": False,
                }
            }
        },
    )

    cfg = load_buy_execution_runtime_config_from_run_config(config_path=config_path)
    transport = build_buy_execution_transport(buy_execution_config=cfg)
    assert "POST" not in transport.retry.retry_methods

