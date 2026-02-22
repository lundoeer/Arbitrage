from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional

from scripts.common.utils import as_dict as _as_dict
from scripts.common.ws_transport import WsHealthConfig


def _to_float(value: Any, *, default: float, min_value: float) -> float:
    try:
        parsed = float(value)
    except Exception:
        parsed = float(default)
    if parsed < min_value:
        return float(min_value)
    return float(parsed)


def _to_int(value: Any, *, default: int, min_value: int) -> int:
    try:
        parsed = int(float(value))
    except Exception:
        parsed = int(default)
    if parsed < min_value:
        return int(min_value)
    return int(parsed)


def _to_bool(value: Any, *, default: bool) -> bool:
    if value is None:
        return bool(default)
    if isinstance(value, bool):
        return bool(value)
    if isinstance(value, (int, float)):
        return bool(value != 0)
    text = str(value).strip().lower()
    if text in {"true", "1", "yes", "on"}:
        return True
    if text in {"false", "0", "no", "off"}:
        return False
    return bool(default)


def _to_optional_positive_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        parsed = int(float(value))
    except Exception:
        return None
    if parsed <= 0:
        return None
    return int(parsed)


def _to_float_bounded(value: Any, *, default: float, min_value: float, max_value: float) -> float:
    parsed = _to_float(value, default=default, min_value=min_value)
    if parsed > float(max_value):
        return float(max_value)
    return float(parsed)


@dataclass(frozen=True)
class BuyDecisionConfig:
    min_gross_edge_threshold: float = 0.04
    max_spend_per_market_usd: float = 10.0
    max_size_cap_per_leg: float = 20.0


@dataclass(frozen=True)
class ExecutionDecisionConfig:
    best_ask_and_bids_at_max: float = 0.95
    best_ask_and_bids_at_min: float = 0.05
    market_start_trade_time_seconds: int = 120
    market_end_trade_time_seconds: int = 120
    market_duration_seconds: int = 900


@dataclass(frozen=True)
class DecisionConfig:
    buy: BuyDecisionConfig
    execution: ExecutionDecisionConfig


@dataclass(frozen=True)
class BuyExecutionApiRetryConfig:
    enabled: bool = True
    max_attempts: int = 3
    base_backoff_seconds: float = 0.5
    jitter_ratio: float = 0.2
    include_post: bool = True


@dataclass(frozen=True)
class BuyExecutionRuntimeConfig:
    enabled: bool = False
    cooldown_ms: int = 0
    max_attempts_per_run: int = 1
    api_retry: BuyExecutionApiRetryConfig = field(default_factory=BuyExecutionApiRetryConfig)


@dataclass(frozen=True)
class PositionMonitoringRuntimeConfig:
    enabled: bool = False
    require_bootstrap_before_buy: bool = True
    polymarket_user_ws_enabled: bool = True
    kalshi_market_positions_ws_enabled: bool = True
    polymarket_poll_seconds: float = 10.0
    kalshi_poll_seconds: float = 20.0
    loop_sleep_seconds: float = 0.2
    drift_tolerance_contracts: float = 0.0
    stale_warning_seconds: int = 60
    stale_error_seconds: int = 180


def load_health_config_from_run_config(*, config_path: Path) -> WsHealthConfig:
    payload = json.loads(config_path.read_text(encoding="utf-8"))
    health = _as_dict(payload.get("health"))
    transport_stale_s = _to_float(
        health.get("transport_heartbeat_stale_seconds"),
        default=1.5,
        min_value=0.5,
    )
    market_data_stale_s = _to_float(
        health.get("market_data_stale_seconds"),
        default=transport_stale_s,
        min_value=0.5,
    )
    max_lag_ms = _to_int(health.get("max_lag_ms"), default=700, min_value=1)
    require_source_timestamp = bool(health.get("require_source_timestamp", True))
    max_source_age_ms = _to_optional_positive_int(health.get("max_source_age_ms"))
    return WsHealthConfig(
        transport_heartbeat_stale_seconds=transport_stale_s,
        market_data_stale_seconds=market_data_stale_s,
        max_lag_ms=max_lag_ms,
        require_source_timestamp=require_source_timestamp,
        max_source_age_ms=max_source_age_ms,
    )


def health_config_to_dict(config: WsHealthConfig) -> Dict[str, Any]:
    return {
        "transport_heartbeat_stale_seconds": float(config.transport_heartbeat_stale_seconds),
        "market_data_stale_seconds": float(config.market_data_stale_seconds),
        "max_lag_ms": int(config.max_lag_ms),
        "require_source_timestamp": bool(config.require_source_timestamp),
        "max_source_age_ms": int(config.max_source_age_ms) if config.max_source_age_ms is not None else None,
    }


def load_decision_config_from_run_config(*, config_path: Path) -> DecisionConfig:
    payload = json.loads(config_path.read_text(encoding="utf-8"))
    buy = _as_dict(payload.get("buy"))
    execution = _as_dict(payload.get("execution"))

    min_price = _to_float_bounded(
        execution.get("best_ask_and_bids_at_min"),
        default=0.05,
        min_value=0.0,
        max_value=1.0,
    )
    max_price = _to_float_bounded(
        execution.get("best_ask_and_bids_at_max"),
        default=0.95,
        min_value=0.0,
        max_value=1.0,
    )
    if min_price >= max_price:
        min_price = 0.05
        max_price = 0.95

    return DecisionConfig(
        buy=BuyDecisionConfig(
            min_gross_edge_threshold=_to_float_bounded(
                buy.get("min_gross_edge_threshold"),
                default=0.04,
                min_value=0.0,
                max_value=1.0,
            ),
            max_spend_per_market_usd=_to_float(
                buy.get("max_spend_per_market_usd"),
                default=10.0,
                min_value=0.0,
            ),
            max_size_cap_per_leg=_to_float(
                buy.get("max_size_cap_per_leg"),
                default=20.0,
                min_value=0.0,
            ),
        ),
        execution=ExecutionDecisionConfig(
            best_ask_and_bids_at_max=max_price,
            best_ask_and_bids_at_min=min_price,
            market_start_trade_time_seconds=_to_int(
                execution.get("market_start_trade_time_seconds"),
                default=120,
                min_value=0,
            ),
            market_end_trade_time_seconds=_to_int(
                execution.get("market_end_trade_time_seconds"),
                default=120,
                min_value=0,
            ),
            market_duration_seconds=_to_int(
                execution.get("market_duration_seconds"),
                default=900,
                min_value=60,
            ),
        ),
    )


def decision_config_to_dict(config: DecisionConfig) -> Dict[str, Any]:
    return {
        "buy": {
            "min_gross_edge_threshold": float(config.buy.min_gross_edge_threshold),
            "max_spend_per_market_usd": float(config.buy.max_spend_per_market_usd),
            "max_size_cap_per_leg": float(config.buy.max_size_cap_per_leg),
        },
        "execution": {
            "best_ask_and_bids_at_max": float(config.execution.best_ask_and_bids_at_max),
            "best_ask_and_bids_at_min": float(config.execution.best_ask_and_bids_at_min),
            "market_start_trade_time_seconds": int(config.execution.market_start_trade_time_seconds),
            "market_end_trade_time_seconds": int(config.execution.market_end_trade_time_seconds),
            "market_duration_seconds": int(config.execution.market_duration_seconds),
        },
    }


def load_buy_execution_runtime_config_from_run_config(*, config_path: Path) -> BuyExecutionRuntimeConfig:
    payload = json.loads(config_path.read_text(encoding="utf-8"))
    buy_execution = _as_dict(payload.get("buy_execution"))
    api_retry = _as_dict(buy_execution.get("api_retry"))
    return BuyExecutionRuntimeConfig(
        enabled=_to_bool(buy_execution.get("enabled"), default=False),
        cooldown_ms=_to_int(buy_execution.get("cooldown_ms"), default=0, min_value=0),
        max_attempts_per_run=_to_int(
            buy_execution.get("max_attempts_per_run"),
            default=1,
            min_value=0,
        ),
        api_retry=BuyExecutionApiRetryConfig(
            enabled=_to_bool(api_retry.get("enabled"), default=True),
            max_attempts=_to_int(api_retry.get("max_attempts"), default=3, min_value=1),
            base_backoff_seconds=_to_float(
                api_retry.get("base_backoff_seconds"),
                default=0.5,
                min_value=0.0,
            ),
            jitter_ratio=_to_float_bounded(
                api_retry.get("jitter_ratio"),
                default=0.2,
                min_value=0.0,
                max_value=1.0,
            ),
            include_post=_to_bool(api_retry.get("include_post"), default=True),
        ),
    )


def buy_execution_runtime_config_to_dict(config: BuyExecutionRuntimeConfig) -> Dict[str, Any]:
    return {
        "enabled": bool(config.enabled),
        "cooldown_ms": int(config.cooldown_ms),
        "max_attempts_per_run": int(config.max_attempts_per_run),
        "api_retry": {
            "enabled": bool(config.api_retry.enabled),
            "max_attempts": int(config.api_retry.max_attempts),
            "base_backoff_seconds": float(config.api_retry.base_backoff_seconds),
            "jitter_ratio": float(config.api_retry.jitter_ratio),
            "include_post": bool(config.api_retry.include_post),
        },
    }


def load_position_monitoring_runtime_config_from_run_config(*, config_path: Path) -> PositionMonitoringRuntimeConfig:
    payload = json.loads(config_path.read_text(encoding="utf-8"))
    monitor = _as_dict(payload.get("position_monitoring"))
    return PositionMonitoringRuntimeConfig(
        enabled=_to_bool(monitor.get("enabled"), default=False),
        require_bootstrap_before_buy=_to_bool(monitor.get("require_bootstrap_before_buy"), default=True),
        polymarket_user_ws_enabled=_to_bool(monitor.get("polymarket_user_ws_enabled"), default=True),
        kalshi_market_positions_ws_enabled=_to_bool(monitor.get("kalshi_market_positions_ws_enabled"), default=True),
        polymarket_poll_seconds=_to_float(
            monitor.get("polymarket_poll_seconds"),
            default=10.0,
            min_value=0.2,
        ),
        kalshi_poll_seconds=_to_float(
            monitor.get("kalshi_poll_seconds"),
            default=20.0,
            min_value=0.2,
        ),
        loop_sleep_seconds=_to_float(
            monitor.get("loop_sleep_seconds"),
            default=0.2,
            min_value=0.05,
        ),
        drift_tolerance_contracts=_to_float(
            monitor.get("drift_tolerance_contracts"),
            default=0.0,
            min_value=0.0,
        ),
        stale_warning_seconds=_to_int(
            monitor.get("stale_warning_seconds"),
            default=60,
            min_value=1,
        ),
        stale_error_seconds=_to_int(
            monitor.get("stale_error_seconds"),
            default=180,
            min_value=1,
        ),
    )


def position_monitoring_runtime_config_to_dict(config: PositionMonitoringRuntimeConfig) -> Dict[str, Any]:
    return {
        "enabled": bool(config.enabled),
        "require_bootstrap_before_buy": bool(config.require_bootstrap_before_buy),
        "polymarket_user_ws_enabled": bool(config.polymarket_user_ws_enabled),
        "kalshi_market_positions_ws_enabled": bool(config.kalshi_market_positions_ws_enabled),
        "polymarket_poll_seconds": float(config.polymarket_poll_seconds),
        "kalshi_poll_seconds": float(config.kalshi_poll_seconds),
        "loop_sleep_seconds": float(config.loop_sleep_seconds),
        "drift_tolerance_contracts": float(config.drift_tolerance_contracts),
        "stale_warning_seconds": int(config.stale_warning_seconds),
        "stale_error_seconds": int(config.stale_error_seconds),
    }
