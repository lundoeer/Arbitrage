from __future__ import annotations

from scripts.common.api_transport import ApiTransport, RetryConfig
from scripts.common.run_config import BuyExecutionRuntimeConfig


def build_buy_execution_transport(*, buy_execution_config: BuyExecutionRuntimeConfig) -> ApiTransport:
    api_retry = buy_execution_config.api_retry
    if not bool(api_retry.enabled):
        return ApiTransport(timeout_seconds=10)

    methods = {"GET", "HEAD", "OPTIONS"}
    if bool(api_retry.include_post):
        methods.add("POST")
    retry_config = RetryConfig(
        max_attempts=max(1, int(api_retry.max_attempts)),
        base_backoff_seconds=max(0.0, float(api_retry.base_backoff_seconds)),
        jitter_ratio=max(0.0, float(api_retry.jitter_ratio)),
        retry_methods=frozenset(methods),
    )
    return ApiTransport(timeout_seconds=10, retry_config=retry_config)
