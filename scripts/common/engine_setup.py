from __future__ import annotations

from typing import Any, Dict, List, Optional

from scripts.common.api_transport import ApiTransport, RetryConfig
from scripts.common.buy_execution import (
    BuyExecutionClients,
    KalshiApiBuyClient,
    build_polymarket_api_buy_client_from_env,
)
from scripts.common.kalshi_auth import resolve_kalshi_ws_headers
from scripts.common.position_polling import (
    KalshiPositionsPollClient,
    PolymarketAccountPollClient,
    PolymarketPositionsPollClient,
    PositionPollClientConfig,
)
from scripts.common.run_config import BuyExecutionRuntimeConfig
from scripts.common.utils import get_env_or_none
from scripts.common.ws_collectors import (
    KalshiMarketPositionsWsCollector,
    KalshiWsCollector,
    PolymarketUserWsCollector,
    PolymarketWsCollector,
)
from scripts.common.ws_transport import NullWriter, WsHealthConfig


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


def build_buy_execution_clients(
    *,
    enable_buy_execution: bool,
    buy_execution_config: BuyExecutionRuntimeConfig,
) -> tuple[bool, BuyExecutionClients, list[str]]:
    if not bool(enable_buy_execution):
        return False, BuyExecutionClients(), []

    errors: list[str] = []
    polymarket_client = None
    kalshi_client = None
    transport = build_buy_execution_transport(buy_execution_config=buy_execution_config)
    try:
        polymarket_client = build_polymarket_api_buy_client_from_env(
            transport=transport,
        )
    except Exception as exc:
        errors.append(f"polymarket_client_init_failed:{type(exc).__name__}:{exc}")
    try:
        kalshi_client = KalshiApiBuyClient(transport=transport)
    except Exception as exc:
        errors.append(f"kalshi_client_init_failed:{type(exc).__name__}:{exc}")

    enabled = polymarket_client is not None and kalshi_client is not None and not errors
    if not enabled and not errors:
        errors.append("buy_clients_not_available")
    return enabled, BuyExecutionClients(polymarket=polymarket_client, kalshi=kalshi_client), errors


def resolve_polymarket_position_user_address_from_env() -> str:
    for key in ("POLYMARKET_FUNDER", "POLYMARKET_ADDRESS", "POLYMARKET_SIGNER_ADDRESS"):
        value = get_env_or_none(key)
        if value:
            return value
    return ""


def build_position_components(
    *,
    enable_position_monitoring: bool,
    enable_account_snapshots: bool,
    pm_yes: str,
    pm_no: str,
    pm_condition_id: str,
    kx_ticker: str,
    polymarket_user_ws_enabled: bool,
    kalshi_market_positions_ws_enabled: bool,
    health_config: Optional[WsHealthConfig],
    on_pm_user_event: Any,
    on_kx_market_position_event: Any,
) -> tuple[
    Optional[PolymarketPositionsPollClient],
    Optional[PolymarketAccountPollClient],
    Optional[KalshiPositionsPollClient],
    Optional[PolymarketUserWsCollector],
    Optional[KalshiMarketPositionsWsCollector],
    list[str],
]:
    pm_poll_client = None
    pm_account_poll_client = None
    kx_poll_client = None
    pm_user_collector = None
    kx_market_positions_collector = None
    setup_errors: list[str] = []

    snapshot_capture_requested = enable_position_monitoring or enable_account_snapshots

    if snapshot_capture_requested:
        pm_user_address = resolve_polymarket_position_user_address_from_env()
        if not pm_user_address:
            msg = "polymarket_position_user_missing_env:POLYMARKET_FUNDER|POLYMARKET_ADDRESS|POLYMARKET_SIGNER_ADDRESS"
            setup_errors.append(msg)
        elif not pm_condition_id:
            msg = "polymarket_condition_id_missing_from_selection"
            setup_errors.append(msg)
        else:
            try:
                pm_poll_client = PolymarketPositionsPollClient(
                    user_address=pm_user_address,
                    condition_id=pm_condition_id,
                    token_yes=pm_yes,
                    token_no=pm_no,
                    config=PositionPollClientConfig(),
                )
            except Exception as exc:
                setup_errors.append(f"polymarket_positions_poll_client_init_failed:{type(exc).__name__}:{exc}")
        
        try:
            pm_account_poll_client = PolymarketAccountPollClient()
        except Exception as exc:
            setup_errors.append(f"polymarket_account_poll_client_init_failed:{type(exc).__name__}:{exc}")
        
        try:
            kx_poll_client = KalshiPositionsPollClient(
                market_ticker=kx_ticker,
                config=PositionPollClientConfig(),
            )
        except Exception as exc:
            setup_errors.append(f"kalshi_positions_poll_client_init_failed:{type(exc).__name__}:{exc}")

    if enable_position_monitoring:
        if polymarket_user_ws_enabled and pm_condition_id:
            try:
                pm_user_collector = PolymarketUserWsCollector(
                    condition_id=pm_condition_id,
                    raw_writer=NullWriter(),
                    event_writer=NullWriter(),
                    health_config=health_config,
                    on_event=on_pm_user_event,
                )
            except Exception as exc:
                setup_errors.append(f"polymarket_user_ws_init_failed:{type(exc).__name__}:{exc}")

        if kalshi_market_positions_ws_enabled:
            try:
                kx_market_positions_collector = KalshiMarketPositionsWsCollector(
                    market_ticker=kx_ticker,
                    headers_factory=resolve_kalshi_ws_headers,
                    raw_writer=NullWriter(),
                    event_writer=NullWriter(),
                    health_config=health_config,
                    on_event=on_kx_market_position_event,
                )
            except Exception as exc:
                setup_errors.append(f"kalshi_market_positions_ws_init_failed:{type(exc).__name__}:{exc}")

    return (
        pm_poll_client,
        pm_account_poll_client,
        kx_poll_client,
        pm_user_collector,
        kx_market_positions_collector,
        setup_errors,
    )
