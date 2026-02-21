from __future__ import annotations

from typing import Any, Callable, Dict, Iterable, List, Optional

from scripts.common.kalshi_auth import resolve_kalshi_ws_headers
from scripts.common.ws_normalization import normalize_kalshi_event, normalize_polymarket_event
from scripts.common.ws_transport import BaseWsCollector, CollectorWriter, WsHealthConfig


class PolymarketWsCollector(BaseWsCollector):
    WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

    def __init__(
        self,
        *,
        token_yes: str,
        token_no: str,
        custom_feature_enabled: bool = True,
        raw_writer: CollectorWriter,
        event_writer: CollectorWriter,
        health_config: Optional[WsHealthConfig] = None,
        on_event: Optional[Callable[[Dict[str, Any]], None]] = None,
    ) -> None:
        self.token_yes = str(token_yes)
        self.token_no = str(token_no)
        self.asset_ids = [self.token_yes, self.token_no]
        self.custom_feature_enabled = bool(custom_feature_enabled)
        super().__init__(
            name="polymarket_ws",
            url=self.WS_URL,
            raw_writer=raw_writer,
            event_writer=event_writer,
            health=health_config,
            on_event=on_event,
        )

    def subscription_payloads(self) -> Iterable[Dict[str, Any]]:
        # Both key names are used in public examples; send both for compatibility.
        yield {
            "type": "MARKET",
            "asset_ids": self.asset_ids,
            "assets_ids": self.asset_ids,
            "custom_feature_enabled": bool(self.custom_feature_enabled),
        }

    def normalize_event(self, message: Dict[str, Any], recv_ms: int) -> Iterable[Dict[str, Any]]:
        yield from normalize_polymarket_event(message, recv_ms)


class KalshiWsCollector(BaseWsCollector):
    WS_URL = "wss://api.elections.kalshi.com/trade-api/ws/v2"

    def __init__(
        self,
        *,
        market_ticker: str,
        channels: List[str],
        headers: Optional[Dict[str, str]] = None,
        headers_factory: Optional[Callable[[], Dict[str, str]]] = None,
        raw_writer: CollectorWriter,
        event_writer: CollectorWriter,
        health_config: Optional[WsHealthConfig] = None,
        on_event: Optional[Callable[[Dict[str, Any]], None]] = None,
    ) -> None:
        self.market_ticker = str(market_ticker)
        self.channels = list(channels)
        # If neither headers nor factory provided, resolve once immediately.
        if headers is None and headers_factory is None:
            headers = resolve_kalshi_ws_headers()
        super().__init__(
            name="kalshi_ws",
            url=self.WS_URL,
            raw_writer=raw_writer,
            event_writer=event_writer,
            headers=headers or {},
            headers_factory=headers_factory,
            health=health_config,
            on_event=on_event,
        )

    def subscription_payloads(self) -> Iterable[Dict[str, Any]]:
        yield {
            "id": 1,
            "cmd": "subscribe",
            "params": {
                "channels": self.channels,
                "market_ticker": self.market_ticker,
            },
        }

    def normalize_event(self, message: Dict[str, Any], recv_ms: int) -> Iterable[Dict[str, Any]]:
        yield from normalize_kalshi_event(message, recv_ms, market_ticker=self.market_ticker)
