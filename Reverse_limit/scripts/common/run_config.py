from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from scripts.common.utils import as_dict as _as_dict


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


def _to_float_bounded(value: Any, *, default: float, min_value: float, max_value: float) -> float:
    parsed = _to_float(value, default=default, min_value=min_value)
    if parsed > float(max_value):
        return float(max_value)
    return float(parsed)


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
    parallel_leg_timeout_ms: int = 4000
    api_retry: BuyExecutionApiRetryConfig = field(default_factory=BuyExecutionApiRetryConfig)


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
        parallel_leg_timeout_ms=_to_int(
            buy_execution.get("parallel_leg_timeout_ms"),
            default=4000,
            min_value=100,
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
