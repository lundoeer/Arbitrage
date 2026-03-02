#!/usr/bin/env python3
"""
Phase 1 market surveillance skeleton.

This module provides:
- CLI entrypoint
- JSONL snapshot writer and status writer
- Minimal tick loop wired to mock source adapters
"""

from __future__ import annotations

import asyncio
import argparse
import os
import json
import re
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, List, Optional, Protocol, Sequence

import requests

from scripts.common.decision_runtime import SharePriceRuntime
from scripts.common.kalshi_auth import resolve_kalshi_ws_headers
from scripts.common.market_selection import load_selected_markets
from scripts.common.run_config import load_health_config_from_run_config
from scripts.common.utils import now_ms as _now_ms
from scripts.common.ws_collectors import KalshiWsCollector, PolymarketWsCollector
from scripts.common.ws_transport import NullWriter, WsHealthConfig

try:
    import websockets
except Exception:
    websockets = None


BEST_ASK_LEGS = ("polymarket_yes", "polymarket_no", "kalshi_yes", "kalshi_no")
KALSHI_BTC_CURRENT_URL = (
    "https://kalshi-public-docs.s3.amazonaws.com/external/crypto/btc_current.json"
    "?allowRequestEvenIfPageIsHidden=true"
)
POLYMARKET_RTDS_WS_URL = "wss://ws-live-data.polymarket.com"
CHAINLINK_BTC_STREAM_PAGE_URL = "https://data.chain.link/streams/btc-usd"
CHAINLINK_QUERY_TIMESCALE_ENDPOINT = "https://data.chain.link/api/query-timescale"


def _as_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None


def _as_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(float(value))
    except Exception:
        return None


def _default_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _utc_iso_with_millis(epoch_ms: Optional[int] = None) -> str:
    if epoch_ms is None:
        dt = datetime.now(timezone.utc)
    else:
        dt = datetime.fromtimestamp(float(epoch_ms) / 1000.0, tz=timezone.utc)
    return dt.isoformat(timespec="milliseconds")


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def _default_summary_path(*, run_id: str) -> Path:
    return Path("data/diagnostic/market_surveillance") / f"market_surveillance_summary__{run_id}.json"


class JsonlFileWriter:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = open(self.path, "a", encoding="utf-8")

    def write(self, payload: Dict[str, Any]) -> None:
        self._fh.write(json.dumps(payload, ensure_ascii=True) + "\n")
        self._fh.flush()

    def close(self) -> None:
        self._fh.close()


class StatusFileWriter:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def write(self, payload: Dict[str, Any]) -> None:
        tmp_path = self.path.with_suffix(self.path.suffix + ".tmp")
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
        tmp_path.replace(self.path)


@dataclass(frozen=True)
class UnderlyingPricePoint:
    source: str
    price_usd: Optional[float]
    source_timestamp_ms: Optional[int]
    max_age_ms: Optional[int] = None
    metadata: Optional[Dict[str, Any]] = None


class UnderlyingPriceAdapter(Protocol):
    def read(self, *, at_ms: int) -> UnderlyingPricePoint: ...


class BestAskAdapter(Protocol):
    def read(self, *, at_ms: int) -> Dict[str, Dict[str, Optional[float]]]: ...


class PairSelector(Protocol):
    def select(self, *, run_discovery_first: bool) -> Dict[str, Any]: ...


class CollectorRunner(Protocol):
    async def run(self, stop_event: asyncio.Event) -> None: ...


CollectorFactory = Callable[
    [Dict[str, Any], SharePriceRuntime, WsHealthConfig],
    Sequence[CollectorRunner],
]
ResolvedPairsRunner = Callable[[], Dict[str, Any]]


@dataclass
class QuoteRuntimeSession:
    pair: Dict[str, Any]
    runtime: SharePriceRuntime
    collectors: Sequence[CollectorRunner]
    stop_event: asyncio.Event
    tasks: List[asyncio.Task[Any]]


class DiscoveryPairSelector:
    def __init__(
        self,
        *,
        config_path: Path,
        discovery_output: Path,
        pair_cache_path: Path,
    ) -> None:
        self.config_path = config_path
        self.discovery_output = discovery_output
        self.pair_cache_path = pair_cache_path

    def select(self, *, run_discovery_first: bool) -> Dict[str, Any]:
        selected = load_selected_markets(
            config_path=self.config_path,
            discovery_output=self.discovery_output,
            pair_cache_path=self.pair_cache_path,
            run_discovery_first=bool(run_discovery_first),
        )
        polymarket = dict(selected.get("polymarket") or {})
        kalshi = dict(selected.get("kalshi") or {})
        return {
            "kalshi_ticker": kalshi.get("ticker"),
            "polymarket_slug": polymarket.get("event_slug"),
            "polymarket_market_id": polymarket.get("market_id"),
            "polymarket_token_yes": polymarket.get("token_yes"),
            "polymarket_token_no": polymarket.get("token_no"),
            "window_end": polymarket.get("window_end") or kalshi.get("window_end"),
            "condition_id": polymarket.get("condition_id"),
            "discovery_generated_at": selected.get("discovery_generated_at"),
        }


class RuntimeBestAskAdapter:
    def __init__(self, *, runtime: SharePriceRuntime) -> None:
        self.runtime = runtime

    def read(self, *, at_ms: int) -> Dict[str, Dict[str, Optional[float]]]:
        snapshot = self.runtime.snapshot(now_epoch_ms=at_ms)
        quotes = snapshot.get("quotes") if isinstance(snapshot, dict) else {}
        legs = quotes.get("legs") if isinstance(quotes, dict) else {}
        result: Dict[str, Dict[str, Optional[float]]] = {}
        for leg in BEST_ASK_LEGS:
            payload = legs.get(leg) if isinstance(legs, dict) else {}
            row = payload if isinstance(payload, dict) else {}
            result[leg] = {
                "price": row.get("best_ask"),
                "size": row.get("best_ask_size"),
            }
        return result


class KalshiWebsitePriceAdapter:
    def __init__(
        self,
        *,
        session: Optional[requests.Session] = None,
        url: str = KALSHI_BTC_CURRENT_URL,
        timeout_seconds: float = 10.0,
        max_age_ms: int = 15_000,
        fetch_min_interval_ms: int = 2_500,
        cache_window_seconds: int = 600,
    ) -> None:
        self.session = session or requests.Session()
        self.url = str(url)
        self.timeout_seconds = max(1.0, float(timeout_seconds))
        self.max_age_ms = max(1_000, int(max_age_ms))
        self.fetch_min_interval_ms = max(0, int(fetch_min_interval_ms))
        self.cache_window_seconds = max(60, int(cache_window_seconds))
        self._last_fetch_ms: Optional[int] = None
        self._second_cache: Dict[int, float] = {}
        self._latest_price_usd: Optional[float] = None
        self._latest_source_timestamp_ms: Optional[int] = None
        self._latest_metadata: Dict[str, Any] = {}

    @staticmethod
    def _header_ts_ms(headers: Dict[str, Any]) -> Optional[int]:
        for key in ("Last-Modified", "Date"):
            raw = str(headers.get(key) or "").strip()
            if not raw:
                continue
            try:
                dt = parsedate_to_datetime(raw)
            except Exception:
                continue
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.astimezone(timezone.utc).timestamp() * 1000)
        return None

    @classmethod
    def _parse_payload(
        cls,
        *,
        payload: Dict[str, Any],
        headers: Dict[str, Any],
    ) -> Dict[str, Any]:
        if not isinstance(payload, dict):
            raise RuntimeError("Kalshi btc_current payload must be a JSON object.")

        timeseries = payload.get("timeseries")
        second_series: Any = None
        if isinstance(timeseries, dict):
            second_series = timeseries.get("second")
        elif isinstance(timeseries, list):
            second_series = timeseries

        normalized_seconds: List[float] = []
        if isinstance(second_series, list):
            for raw in second_series:
                candidate = _as_float(raw)
                if candidate is None or candidate <= 0:
                    continue
                normalized_seconds.append(float(candidate))

        source_ts_ms = _as_int(payload.get("maturity_ts_ms"))
        source_ts_origin = "maturity_ts_ms"
        if source_ts_ms is None:
            source_ts_ms = cls._header_ts_ms(headers)
            source_ts_origin = "header_ts"

        candlestick_close: Optional[float] = None
        candlesticks = payload.get("candlesticks")
        if isinstance(candlesticks, dict):
            one_min = candlesticks.get("1M")
            if isinstance(one_min, dict):
                close_value = _as_float(one_min.get("close"))
                if close_value is not None and close_value > 0:
                    candlestick_close = float(close_value)

        if not normalized_seconds and candlestick_close is None:
            raise RuntimeError("Kalshi btc_current payload missing usable BTC price.")

        return {
            "series": normalized_seconds,
            "source_ts_ms": source_ts_ms,
            "source_ts_origin": source_ts_origin,
            "candlestick_close": candlestick_close,
        }

    def _update_cache_from_payload(
        self,
        *,
        payload: Dict[str, Any],
        headers: Dict[str, Any],
        fetch_at_ms: int,
    ) -> None:
        parsed = self._parse_payload(payload=payload, headers=headers)
        second_series = list(parsed.get("series") or [])
        source_ts_ms = _as_int(parsed.get("source_ts_ms"))
        source_ts_origin = str(parsed.get("source_ts_origin") or "unknown")
        candlestick_close = _as_float(parsed.get("candlestick_close"))

        if source_ts_ms is None:
            source_ts_ms = int(fetch_at_ms)
            source_ts_origin = "fetch_at_ms"

        if second_series:
            anchor_ms = int(source_ts_ms)
            start_ms = int(anchor_ms - ((len(second_series) - 1) * 1000))
            for idx, price in enumerate(second_series):
                ts_ms = int(start_ms + (idx * 1000))
                self._second_cache[ts_ms] = float(price)
            self._latest_source_timestamp_ms = anchor_ms
            self._latest_price_usd = float(second_series[-1])
            price_field = "timeseries.second[rolling_buffer]"
        elif candlestick_close is not None:
            self._latest_source_timestamp_ms = int(source_ts_ms)
            self._latest_price_usd = float(candlestick_close)
            price_field = "candlesticks.1M.close"
        else:
            raise RuntimeError("Kalshi btc_current payload missing usable BTC price.")

        prune_before_ms = int(fetch_at_ms - (self.cache_window_seconds * 1000))
        self._second_cache = {
            ts_ms: price
            for ts_ms, price in self._second_cache.items()
            if int(ts_ms) >= prune_before_ms
        }

        self._latest_metadata = {
            "url": self.url,
            "price_field": price_field,
            "timeseries_second_count": len(second_series),
            "series_anchor_ts_ms": self._latest_source_timestamp_ms,
            "series_anchor_source": source_ts_origin,
            "cache_window_seconds": int(self.cache_window_seconds),
        }

    def _fetch(self, *, at_ms: int) -> None:
        response = self.session.get(self.url, timeout=self.timeout_seconds)
        if response.status_code != 200:
            raise RuntimeError(f"Kalshi btc_current request failed status={response.status_code}")
        payload = response.json()
        self._update_cache_from_payload(
            payload=payload if isinstance(payload, dict) else {},
            headers=dict(response.headers),
            fetch_at_ms=int(at_ms),
        )
        self._last_fetch_ms = int(at_ms)

    def _best_cache_second_ms(self, *, target_second_ms: int) -> Optional[int]:
        if not self._second_cache:
            return None
        if target_second_ms in self._second_cache:
            return int(target_second_ms)
        earlier = [ts_ms for ts_ms in self._second_cache.keys() if int(ts_ms) <= int(target_second_ms)]
        if earlier:
            return int(max(earlier))
        return int(max(self._second_cache.keys()))

    def read(self, *, at_ms: int) -> UnderlyingPricePoint:
        now_ms = int(at_ms)
        target_second_ms = int((now_ms // 1000) * 1000)
        needs_fetch = (
            self._last_fetch_ms is None
            or int(now_ms - int(self._last_fetch_ms)) >= int(self.fetch_min_interval_ms)
            or target_second_ms not in self._second_cache
        )
        fetch_error: Optional[str] = None
        if needs_fetch:
            try:
                self._fetch(at_ms=now_ms)
            except Exception as exc:
                fetch_error = str(exc)
                if not self._second_cache and self._latest_price_usd is None:
                    raise

        selected_second_ms = self._best_cache_second_ms(target_second_ms=target_second_ms)
        if selected_second_ms is not None:
            price = float(self._second_cache[selected_second_ms])
            metadata = dict(self._latest_metadata)
            metadata.update(
                {
                    "requested_second_ms": int(target_second_ms),
                    "selected_second_ms": int(selected_second_ms),
                    "selected_second_offset_ms": int(target_second_ms - selected_second_ms),
                    "selected_second_exact": bool(selected_second_ms == target_second_ms),
                    "cache_second_count": len(self._second_cache),
                }
            )
            if fetch_error is not None:
                metadata["fetch_error"] = fetch_error
            return UnderlyingPricePoint(
                source="kalshi_btc_current_json",
                price_usd=price,
                source_timestamp_ms=int(selected_second_ms),
                max_age_ms=int(self.max_age_ms),
                metadata=metadata,
            )

        if self._latest_price_usd is None:
            raise RuntimeError("Kalshi btc_current adapter has no cached price.")
        metadata = dict(self._latest_metadata)
        if fetch_error is not None:
            metadata["fetch_error"] = fetch_error
        metadata["selected_second_exact"] = False
        return UnderlyingPricePoint(
            source="kalshi_btc_current_json",
            price_usd=float(self._latest_price_usd),
            source_timestamp_ms=self._latest_source_timestamp_ms,
            max_age_ms=int(self.max_age_ms),
            metadata=metadata,
        )


class ChainlinkPriceAdapter:
    _NEXT_DATA_PATTERN = re.compile(
        r'<script id="__NEXT_DATA__" type="application/json">(?P<data>.*?)</script>',
        re.DOTALL,
    )

    def __init__(
        self,
        *,
        session: Optional[requests.Session] = None,
        stream_page_url: str = CHAINLINK_BTC_STREAM_PAGE_URL,
        timeout_seconds: float = 20.0,
        stream_live_max_age_ms: int = 15_000,
    ) -> None:
        self.session = session or requests.Session()
        self.stream_page_url = str(stream_page_url)
        self.timeout_seconds = max(1.0, float(timeout_seconds))
        self.stream_live_max_age_ms = max(1_000, int(stream_live_max_age_ms))
        self._stream_feed_id: Optional[str] = None
        self._stream_divisor: Optional[float] = None

    @classmethod
    def _extract_next_data_json(cls, html: str) -> Dict[str, Any]:
        match = cls._NEXT_DATA_PATTERN.search(str(html))
        if not match:
            raise RuntimeError("Could not find __NEXT_DATA__ on Chainlink stream page.")
        try:
            payload = json.loads(match.group("data"))
        except Exception as exc:
            raise RuntimeError("Could not parse Chainlink __NEXT_DATA__ JSON.") from exc
        if not isinstance(payload, dict):
            raise RuntimeError("Chainlink __NEXT_DATA__ payload is not a JSON object.")
        return payload

    @staticmethod
    def _to_epoch_ms_from_iso(value: Any) -> Optional[int]:
        raw = str(value or "").strip()
        if not raw:
            return None
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(raw)
        except ValueError:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.astimezone(timezone.utc).timestamp() * 1000)

    @staticmethod
    def _extract_stream_feed_id(next_data: Dict[str, Any]) -> str:
        page_props = next_data.get("props", {}).get("pageProps", {})
        stream_data = page_props.get("streamData", {}) if isinstance(page_props, dict) else {}
        stream_metadata = stream_data.get("streamMetadata", {}) if isinstance(stream_data, dict) else {}
        feed_id = str(stream_metadata.get("feedId") or "").strip()
        if not feed_id:
            raise RuntimeError("Chainlink stream page missing feedId.")
        return feed_id

    @staticmethod
    def _extract_stream_divisor(next_data: Dict[str, Any]) -> float:
        page_props = next_data.get("props", {}).get("pageProps", {})
        stream_data = page_props.get("streamData", {}) if isinstance(page_props, dict) else {}
        stream_metadata = stream_data.get("streamMetadata", {}) if isinstance(stream_data, dict) else {}
        multiply = _as_float(stream_metadata.get("multiply"))
        if multiply is None or multiply <= 0:
            decimals = _as_int(stream_metadata.get("decimals"))
            if decimals is not None and decimals >= 0:
                multiply = float(10 ** int(decimals))
        if multiply is None or multiply <= 0:
            multiply = 1e18
        return float(multiply)

    @staticmethod
    def _extract_latest_live_report_node(payload: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(payload, dict):
            raise RuntimeError("Chainlink query-timescale payload must be an object.")
        data = payload.get("data")
        if not isinstance(data, dict):
            raise RuntimeError("Chainlink query-timescale payload missing `data` object.")
        live = data.get("liveStreamReports")
        nodes = live.get("nodes") if isinstance(live, dict) else None
        if not isinstance(nodes, list) or not nodes:
            raise RuntimeError("Chainlink query-timescale payload has no live stream nodes.")
        candidates = [node for node in nodes if isinstance(node, dict)]
        if not candidates:
            raise RuntimeError("Chainlink live stream nodes payload is empty.")

        def _node_ts(node: Dict[str, Any]) -> int:
            ts_ms = ChainlinkPriceAdapter._to_epoch_ms_from_iso(node.get("validFromTimestamp"))
            return int(ts_ms or 0)

        latest = max(candidates, key=_node_ts)
        return latest

    @staticmethod
    def _extract_price_from_live_report_node(node: Dict[str, Any], *, divisor: float) -> tuple[float, str]:
        for field in ("price", "mid", "open", "bid", "ask"):
            value = _as_float(node.get(field))
            if value is None:
                continue
            if divisor > 1.0:
                value = float(value) / float(divisor)
            if value > 0:
                return float(value), field
        raise RuntimeError("Could not extract numeric price from Chainlink live stream node.")

    def _stream_context(self) -> tuple[str, float]:
        if self._stream_feed_id and self._stream_divisor:
            return str(self._stream_feed_id), float(self._stream_divisor)

        page_response = self.session.get(self.stream_page_url, timeout=self.timeout_seconds, allow_redirects=True)
        if page_response.status_code != 200:
            raise RuntimeError(f"Chainlink stream page request failed status={page_response.status_code}")
        next_data = self._extract_next_data_json(page_response.text)
        feed_id = self._extract_stream_feed_id(next_data)
        divisor = self._extract_stream_divisor(next_data)
        self._stream_feed_id = str(feed_id)
        self._stream_divisor = float(divisor)
        return str(feed_id), float(divisor)

    def _read_stream_live(self) -> UnderlyingPricePoint:
        feed_id, divisor = self._stream_context()
        response = self.session.get(
            CHAINLINK_QUERY_TIMESCALE_ENDPOINT,
            params={
                "query": "LIVE_STREAM_REPORTS_QUERY",
                "variables": json.dumps({"feedId": feed_id}, separators=(",", ":")),
            },
            timeout=self.timeout_seconds,
            allow_redirects=False,
        )
        if response.status_code != 200:
            raise RuntimeError(f"Chainlink query-timescale request failed status={response.status_code}")
        payload = response.json()
        latest_node = self._extract_latest_live_report_node(payload)
        price, price_field = self._extract_price_from_live_report_node(latest_node, divisor=divisor)
        source_ts_ms = self._to_epoch_ms_from_iso(latest_node.get("validFromTimestamp"))
        return UnderlyingPricePoint(
            source="chainlink_streams_query_timescale_live",
            price_usd=float(price),
            source_timestamp_ms=source_ts_ms,
            max_age_ms=self.stream_live_max_age_ms,
            metadata={
                "stream_page_url": self.stream_page_url,
                "query_endpoint": CHAINLINK_QUERY_TIMESCALE_ENDPOINT,
                "feed_id": feed_id,
                "divisor": float(divisor),
                "price_field": price_field,
                "valid_from_timestamp": latest_node.get("validFromTimestamp"),
            },
        )

    def read(self, *, at_ms: int) -> UnderlyingPricePoint:
        del at_ms
        return self._read_stream_live()


class PolymarketRtdsChainlinkPriceAdapter:
    def __init__(
        self,
        *,
        ws_url: str = POLYMARKET_RTDS_WS_URL,
        symbol: str = "BTC/USD",
        max_age_ms: int = 15_000,
        recv_timeout_seconds: float = 30.0,
        reconnect_base_seconds: float = 1.0,
        reconnect_max_seconds: float = 30.0,
        cache_window_seconds: int = 600,
    ) -> None:
        self.ws_url = str(ws_url)
        self.symbol = str(symbol).strip() or "BTC/USD"
        self._symbol_key = self.symbol.lower()
        self.max_age_ms = max(1_000, int(max_age_ms))
        self.recv_timeout_seconds = max(5.0, float(recv_timeout_seconds))
        self.reconnect_base_seconds = max(0.2, float(reconnect_base_seconds))
        self.reconnect_max_seconds = max(self.reconnect_base_seconds, float(reconnect_max_seconds))
        self.cache_window_seconds = max(60, int(cache_window_seconds))

        self._lock = threading.Lock()
        self._started = False
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._start_at_ms: Optional[int] = None
        self._second_cache: Dict[int, float] = {}
        self._latest_price_usd: Optional[float] = None
        self._latest_source_timestamp_ms: Optional[int] = None
        self._message_count = 0
        self._last_error: Optional[str] = None
        self._last_connect_ms: Optional[int] = None
        self._last_disconnect_ms: Optional[int] = None
        self._active_loop: Optional[asyncio.AbstractEventLoop] = None
        self._active_ws: Optional[Any] = None

    @staticmethod
    def _subscription_payload(*, symbol: str) -> Dict[str, Any]:
        return {
            "action": "subscribe",
            "subscriptions": [
                {
                    "topic": "crypto_prices_chainlink",
                    "type": "*",
                    "filters": json.dumps({"symbol": str(symbol)}, separators=(",", ":")),
                }
            ],
        }

    @staticmethod
    def _unsubscription_payload(*, symbol: str) -> Dict[str, Any]:
        return {
            "action": "unsubscribe",
            "subscriptions": [
                {
                    "topic": "crypto_prices_chainlink",
                    "type": "*",
                    "filters": json.dumps({"symbol": str(symbol)}, separators=(",", ":")),
                }
            ],
        }

    @staticmethod
    def _iter_message_items(parsed: Any) -> List[Dict[str, Any]]:
        if isinstance(parsed, dict):
            return [parsed]
        if isinstance(parsed, list):
            return [item for item in parsed if isinstance(item, dict)]
        return []

    @classmethod
    def _extract_chainlink_price_event(
        cls,
        item: Dict[str, Any],
        *,
        symbol_key: str,
    ) -> Optional[Dict[str, Any]]:
        if not isinstance(item, dict):
            return None
        topic = str(item.get("topic") or "").strip().lower()
        if topic and topic != "crypto_prices_chainlink":
            return None
        event_payload = item.get("payload")
        payload = event_payload if isinstance(event_payload, dict) else item
        payload_symbol = str(payload.get("symbol") or "").strip().lower()
        if payload_symbol != str(symbol_key).strip().lower():
            return None
        price = _as_float(payload.get("value"))
        if price is None or price <= 0:
            return None
        source_ts_ms = _as_int(payload.get("timestamp"))
        if source_ts_ms is None or source_ts_ms <= 0:
            return None
        second_ts_ms = int((int(source_ts_ms) // 1000) * 1000)
        return {
            "symbol": payload_symbol,
            "price_usd": float(price),
            "source_timestamp_ms": int(source_ts_ms),
            "second_timestamp_ms": int(second_ts_ms),
        }

    def _apply_event(self, event: Dict[str, Any], *, recv_ms: int) -> None:
        second_ts_ms = int(event["second_timestamp_ms"])
        price = float(event["price_usd"])
        with self._lock:
            self._second_cache[second_ts_ms] = price
            self._latest_source_timestamp_ms = int(second_ts_ms)
            self._latest_price_usd = float(price)
            self._message_count += 1
            self._last_error = None
            prune_before_ms = int(recv_ms - (self.cache_window_seconds * 1000))
            self._second_cache = {
                ts_ms: value
                for ts_ms, value in self._second_cache.items()
                if int(ts_ms) >= prune_before_ms
            }

    async def _ping_loop(self, ws: Any) -> None:
        while not self._stop_event.is_set():
            await asyncio.sleep(5.0)
            try:
                await ws.send("PING")
            except Exception:
                return

    async def _unsubscribe_and_close_ws(self, ws: Any) -> None:
        try:
            await ws.send(json.dumps(self._unsubscription_payload(symbol=self.symbol)))
        except Exception:
            pass
        try:
            await ws.close(code=1000, reason="client shutdown")
        except Exception:
            pass

    async def _worker_loop(self) -> None:
        if websockets is None:
            with self._lock:
                self._last_error = "Missing websocket dependency."
            return
        attempt = 0
        while not self._stop_event.is_set():
            try:
                with self._lock:
                    self._last_connect_ms = int(_now_ms())
                async with websockets.connect(
                    self.ws_url,
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=10,
                    max_size=8 * 1024 * 1024,
                ) as ws:
                    attempt = 0
                    with self._lock:
                        self._active_loop = asyncio.get_running_loop()
                        self._active_ws = ws
                    await ws.send(json.dumps(self._subscription_payload(symbol=self.symbol)))
                    ping_task = asyncio.create_task(self._ping_loop(ws))
                    try:
                        while not self._stop_event.is_set():
                            try:
                                raw = await asyncio.wait_for(ws.recv(), timeout=self.recv_timeout_seconds)
                            except asyncio.TimeoutError:
                                continue
                            recv_ms = int(_now_ms())
                            if isinstance(raw, str) and raw.strip().upper() == "PING":
                                try:
                                    await ws.send("PONG")
                                except Exception:
                                    pass
                                continue
                            try:
                                parsed = json.loads(raw)
                            except Exception:
                                continue
                            for item in self._iter_message_items(parsed):
                                event = self._extract_chainlink_price_event(item, symbol_key=self._symbol_key)
                                if event is None:
                                    continue
                                self._apply_event(event, recv_ms=recv_ms)
                    finally:
                        ping_task.cancel()
                        await asyncio.gather(ping_task, return_exceptions=True)
                        if self._stop_event.is_set():
                            await self._unsubscribe_and_close_ws(ws)
                        with self._lock:
                            self._active_ws = None
                            self._active_loop = None
            except Exception as exc:
                attempt += 1
                with self._lock:
                    self._last_error = str(exc)
                    self._last_disconnect_ms = int(_now_ms())
                error_text = str(exc).lower()
                if "429" in error_text or "too many requests" in error_text:
                    delay = max(60.0, self.reconnect_max_seconds)
                else:
                    delay = min(
                        self.reconnect_max_seconds,
                        self.reconnect_base_seconds * (2 ** min(attempt - 1, 8)),
                    )
                slept = 0.0
                while slept < delay and not self._stop_event.is_set():
                    chunk = min(0.25, delay - slept)
                    await asyncio.sleep(chunk)
                    slept += chunk

    def _worker_main(self) -> None:
        try:
            asyncio.run(self._worker_loop())
        except Exception as exc:
            with self._lock:
                self._last_error = str(exc)

    def _ensure_worker_started(self, *, at_ms: int) -> None:
        with self._lock:
            if self._started:
                return
            self._started = True
            self._start_at_ms = int(at_ms)
        self._thread = threading.Thread(
            target=self._worker_main,
            name="polymarket-rtds-chainlink-worker",
            daemon=True,
        )
        self._thread.start()

    def start(self, *, at_ms: Optional[int] = None) -> None:
        self._ensure_worker_started(at_ms=int(at_ms if at_ms is not None else _now_ms()))

    @staticmethod
    def _pick_second(
        second_cache: Dict[int, float],
        *,
        target_second_ms: int,
    ) -> Optional[int]:
        if not second_cache:
            return None
        if target_second_ms in second_cache:
            return int(target_second_ms)
        prior = [ts_ms for ts_ms in second_cache.keys() if int(ts_ms) <= int(target_second_ms)]
        if prior:
            return int(max(prior))
        return int(max(second_cache.keys()))

    def read(self, *, at_ms: int) -> UnderlyingPricePoint:
        now_ms = int(at_ms)
        self._ensure_worker_started(at_ms=now_ms)
        target_second_ms = int((now_ms // 1000) * 1000)
        with self._lock:
            selected_second_ms = self._pick_second(self._second_cache, target_second_ms=target_second_ms)
            if selected_second_ms is not None:
                price = float(self._second_cache[selected_second_ms])
                return UnderlyingPricePoint(
                    source="polymarket_rtds_chainlink",
                    price_usd=price,
                    source_timestamp_ms=int(selected_second_ms),
                    max_age_ms=int(self.max_age_ms),
                    metadata={
                        "ws_url": self.ws_url,
                        "symbol": self.symbol,
                        "price_field": "payload.value",
                        "requested_second_ms": int(target_second_ms),
                        "selected_second_ms": int(selected_second_ms),
                        "selected_second_exact": bool(selected_second_ms == target_second_ms),
                        "selected_second_offset_ms": int(target_second_ms - selected_second_ms),
                        "cache_second_count": len(self._second_cache),
                        "message_count": int(self._message_count),
                        "last_error": self._last_error,
                    },
                )

            started_ms = int(self._start_at_ms) if self._start_at_ms is not None else now_ms
            elapsed_ms = int(max(0, now_ms - started_ms))
            raise RuntimeError(
                "Polymarket RTDS has no cached chainlink BTC price yet "
                f"(elapsed_ms={elapsed_ms}, last_error={self._last_error})"
            )

    def close(self) -> None:
        self._stop_event.set()
        active_loop: Optional[asyncio.AbstractEventLoop]
        active_ws: Optional[Any]
        with self._lock:
            active_loop = self._active_loop
            active_ws = self._active_ws
        if active_loop is not None and active_ws is not None:
            try:
                future = asyncio.run_coroutine_threadsafe(
                    self._unsubscribe_and_close_ws(active_ws),
                    active_loop,
                )
                future.result(timeout=2.0)
            except Exception:
                pass
        if self._thread is not None:
            self._thread.join(timeout=2.0)


def build_live_collectors(
    pair: Dict[str, Any],
    runtime: SharePriceRuntime,
    health_config: WsHealthConfig,
) -> Sequence[CollectorRunner]:
    pm_yes = str(pair.get("polymarket_token_yes") or "").strip()
    pm_no = str(pair.get("polymarket_token_no") or "").strip()
    kalshi_ticker = str(pair.get("kalshi_ticker") or "").strip()
    if not pm_yes or not pm_no:
        raise RuntimeError("Live collector setup requires polymarket_token_yes and polymarket_token_no.")
    if not kalshi_ticker:
        raise RuntimeError("Live collector setup requires kalshi_ticker.")

    polymarket_collector = PolymarketWsCollector(
        token_yes=pm_yes,
        token_no=pm_no,
        custom_feature_enabled=True,
        raw_writer=NullWriter(),
        event_writer=NullWriter(),
        health_config=health_config,
        on_event=runtime.apply_polymarket_event,
    )
    kalshi_collector = KalshiWsCollector(
        market_ticker=kalshi_ticker,
        channels=["ticker", "orderbook_delta"],
        headers_factory=resolve_kalshi_ws_headers,
        raw_writer=NullWriter(),
        event_writer=NullWriter(),
        health_config=health_config,
        on_event=runtime.apply_kalshi_event,
    )
    return [kalshi_collector, polymarket_collector]


class MockChainlinkPriceAdapter:
    def __init__(self, *, start_price: float = 65000.0, step: float = 0.25) -> None:
        self._start_price = float(start_price)
        self._step = float(step)
        self._idx = 0

    def read(self, *, at_ms: int) -> UnderlyingPricePoint:
        price = self._start_price + (self._idx * self._step)
        self._idx += 1
        return UnderlyingPricePoint(
            source="chainlink_mock",
            price_usd=round(price, 8),
            source_timestamp_ms=int(at_ms),
        )


class MockKalshiWebsitePriceAdapter:
    def __init__(self, *, start_price: float = 65001.0, step: float = 0.2) -> None:
        self._start_price = float(start_price)
        self._step = float(step)
        self._idx = 0

    def read(self, *, at_ms: int) -> UnderlyingPricePoint:
        price = self._start_price + (self._idx * self._step)
        self._idx += 1
        return UnderlyingPricePoint(
            source="kalshi_site_mock",
            price_usd=round(price, 8),
            source_timestamp_ms=int(at_ms),
        )


class MockBestAskAdapter:
    def __init__(self) -> None:
        self._idx = 0

    def read(self, *, at_ms: int) -> Dict[str, Dict[str, Optional[float]]]:
        del at_ms
        shift = (self._idx % 3) * 0.01
        self._idx += 1
        return {
            "polymarket_yes": {"price": round(0.41 + shift, 6), "size": 500.0},
            "polymarket_no": {"price": round(0.60 - shift, 6), "size": 300.0},
            "kalshi_yes": {"price": round(0.42 + shift, 6), "size": 250.0},
            "kalshi_no": {"price": round(0.59 - shift, 6), "size": 240.0},
        }


def _normalize_pair_metadata(pair: Dict[str, Any]) -> Dict[str, Any]:
    normalized = {
        "kalshi_ticker": pair.get("kalshi_ticker"),
        "polymarket_slug": pair.get("polymarket_slug"),
        "polymarket_market_id": pair.get("polymarket_market_id"),
        "polymarket_token_yes": pair.get("polymarket_token_yes"),
        "polymarket_token_no": pair.get("polymarket_token_no"),
        "window_end": pair.get("window_end"),
    }
    return normalized


def _normalize_best_asks(best_asks: Dict[str, Dict[str, Optional[float]]]) -> Dict[str, Dict[str, Optional[float]]]:
    normalized: Dict[str, Dict[str, Optional[float]]] = {}
    for leg in BEST_ASK_LEGS:
        row = best_asks.get(leg) if isinstance(best_asks, dict) else None
        row_dict = row if isinstance(row, dict) else {}
        normalized[leg] = {
            "price": row_dict.get("price"),
            "size": row_dict.get("size"),
        }
    return normalized


def _quotes_fresh(best_asks: Dict[str, Dict[str, Optional[float]]]) -> bool:
    for leg in BEST_ASK_LEGS:
        row = best_asks.get(leg, {})
        if row.get("price") is None or row.get("size") is None:
            return False
    return True


def _underlying_age_ms(*, recv_ms: int, source_timestamp_ms: Optional[int]) -> Optional[int]:
    if source_timestamp_ms is None:
        return None
    return int(max(0, int(recv_ms) - int(source_timestamp_ms)))


def _underlying_is_stale(
    *,
    recv_ms: int,
    source_timestamp_ms: Optional[int],
    max_age_ms: Optional[int],
) -> bool:
    if source_timestamp_ms is None:
        return True
    if max_age_ms is None:
        return False
    age = _underlying_age_ms(recv_ms=recv_ms, source_timestamp_ms=source_timestamp_ms)
    if age is None:
        return True
    return bool(age > int(max_age_ms))


def _underlying_entry(*, point: UnderlyingPricePoint, recv_ms: int) -> Dict[str, Any]:
    age_ms = _underlying_age_ms(recv_ms=recv_ms, source_timestamp_ms=point.source_timestamp_ms)
    is_stale = _underlying_is_stale(
        recv_ms=recv_ms,
        source_timestamp_ms=point.source_timestamp_ms,
        max_age_ms=point.max_age_ms,
    )
    return {
        "price_usd": point.price_usd,
        "source_timestamp_ms": point.source_timestamp_ms,
        "source_age_ms": age_ms,
        "max_age_ms": point.max_age_ms,
        "is_stale": is_stale,
        "source": point.source,
        "metadata": point.metadata or {},
    }


def _underlying_fresh(*, chainlink_entry: Dict[str, Any], kalshi_entry: Dict[str, Any]) -> bool:
    if chainlink_entry.get("price_usd") is None or kalshi_entry.get("price_usd") is None:
        return False
    if chainlink_entry.get("source_timestamp_ms") is None or kalshi_entry.get("source_timestamp_ms") is None:
        return False
    if bool(chainlink_entry.get("is_stale")) or bool(kalshi_entry.get("is_stale")):
        return False
    return True


def build_snapshot_row(
    *,
    run_id: str,
    recv_ms: int,
    ts: str,
    pair: Dict[str, Any],
    chainlink_price: UnderlyingPricePoint,
    kalshi_price: UnderlyingPricePoint,
    best_asks: Dict[str, Dict[str, Optional[float]]],
    errors: Optional[List[str]] = None,
) -> Dict[str, Any]:
    normalized_best_asks = _normalize_best_asks(best_asks)
    normalized_pair = _normalize_pair_metadata(pair)
    error_list = list(errors or [])
    chainlink_entry = _underlying_entry(point=chainlink_price, recv_ms=recv_ms)
    kalshi_entry = _underlying_entry(point=kalshi_price, recv_ms=recv_ms)
    return {
        "ts": ts,
        "recv_ms": int(recv_ms),
        "run_id": str(run_id),
        "pair": normalized_pair,
        "underlying_prices": {
            "polymarket_chainlink": chainlink_entry,
            "kalshi_site": kalshi_entry,
        },
        "best_asks": normalized_best_asks,
        "health": {
            "quotes_fresh": _quotes_fresh(normalized_best_asks),
            "underlying_fresh": _underlying_fresh(
                chainlink_entry=chainlink_entry,
                kalshi_entry=kalshi_entry,
            ),
            "errors": error_list,
        },
    }


def _parse_iso_datetime(value: Optional[str]) -> Optional[datetime]:
    raw = str(value or "").strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _pair_key(pair: Dict[str, Any]) -> str:
    return "|".join(
        [
            str(pair.get("kalshi_ticker") or ""),
            str(pair.get("polymarket_slug") or ""),
            str(pair.get("polymarket_market_id") or ""),
        ]
    )


def _window_end_epoch_ms(pair: Dict[str, Any]) -> Optional[int]:
    window_end = _parse_iso_datetime(str(pair.get("window_end") or ""))
    if window_end is None:
        return None
    return int(window_end.timestamp() * 1000)


def _next_quarter_boundary_epoch_ms(now_ms: int) -> int:
    bucket_ms = 15 * 60 * 1000
    return (int(now_ms // bucket_ms) + 1) * bucket_ms


def _boundary_target_epoch_ms_for_pair(pair: Dict[str, Any], *, now_ms: int) -> int:
    pair_window_end = _window_end_epoch_ms(pair)
    if pair_window_end is not None and pair_window_end > int(now_ms):
        return int(pair_window_end)
    return _next_quarter_boundary_epoch_ms(now_ms)


async def _start_quote_runtime_session(
    *,
    pair: Dict[str, Any],
    health_config: WsHealthConfig,
    collector_factory: CollectorFactory,
) -> QuoteRuntimeSession:
    runtime = SharePriceRuntime(
        polymarket_token_yes=str(pair.get("polymarket_token_yes") or ""),
        polymarket_token_no=str(pair.get("polymarket_token_no") or ""),
    )
    collectors = list(collector_factory(pair, runtime, health_config))
    stop_event = asyncio.Event()
    tasks = [asyncio.create_task(collector.run(stop_event)) for collector in collectors]
    return QuoteRuntimeSession(
        pair=dict(pair),
        runtime=runtime,
        collectors=collectors,
        stop_event=stop_event,
        tasks=tasks,
    )


async def _stop_quote_runtime_session(session: QuoteRuntimeSession, *, timeout_seconds: float = 10.0) -> None:
    session.stop_event.set()
    if not session.tasks:
        return
    done, pending = await asyncio.wait(session.tasks, timeout=max(0.1, float(timeout_seconds)))
    del done
    for task in pending:
        task.cancel()
    if pending:
        await asyncio.gather(*pending, return_exceptions=True)


def _default_resolved_pairs_runner(
    *,
    config_path: Path,
    pair_log_path: Path,
    summary_path: Path,
    page_size: int,
    max_pages: Optional[int],
    max_markets: Optional[int],
) -> Dict[str, Any]:
    from scripts.diagnostic.compare_resolved_15m_pairs import run as compare_resolved_pairs_run

    return compare_resolved_pairs_run(
        config_path=config_path,
        pair_log_path=pair_log_path,
        summary_path=summary_path,
        page_size=int(page_size),
        max_pages=max_pages,
        max_markets=max_markets,
    )


def _resolved_summary_compact(summary: Dict[str, Any]) -> Dict[str, Any]:
    payload = dict(summary if isinstance(summary, dict) else {})
    totals = payload.get("totals") if isinstance(payload.get("totals"), dict) else {}
    return {
        "generated_at": payload.get("generated_at"),
        "pairs_in_log_total": totals.get("pairs_in_log_total"),
        "new_pairs_appended_to_log": totals.get("new_pairs_appended_to_log"),
        "resolved_same": totals.get("resolved_same"),
        "resolved_different": totals.get("resolved_different"),
    }


async def run_ws_market_surveillance(
    *,
    output_jsonl_path: Path,
    status_path: Path,
    summary_path: Optional[Path] = None,
    config_path: Path,
    discovery_output_path: Path,
    pair_cache_path: Path,
    run_id: Optional[str] = None,
    tick_seconds: float = 1.0,
    duration_seconds: int = 0,
    max_ticks: Optional[int] = None,
    ws_start_stagger_seconds: float = 10.0,
    pair_boundary_retry_seconds: float = 20.0,
    resolved_pairs_enabled: bool = True,
    resolved_pairs_interval_seconds: float = 600.0,
    resolved_pairs_run_on_start: bool = True,
    resolved_pairs_pair_log_path: Path = Path("data/diagnostic/resolved_15m_pairs.log"),
    resolved_pairs_summary_path: Path = Path("data/diagnostic/resolution_comparison_summary.json"),
    resolved_pairs_page_size: int = 500,
    resolved_pairs_max_pages: Optional[int] = None,
    resolved_pairs_max_markets: Optional[int] = None,
    skip_discovery_initial: bool = False,
    chainlink_adapter: Optional[UnderlyingPriceAdapter] = None,
    kalshi_adapter: Optional[UnderlyingPriceAdapter] = None,
    pair_selector: Optional[PairSelector] = None,
    collector_factory: Optional[CollectorFactory] = None,
    resolved_pairs_runner: Optional[ResolvedPairsRunner] = None,
    now_ms_fn: Callable[[], int] = _now_ms,
    now_iso_fn: Callable[[int], str] = _utc_iso_with_millis,
    sleep_async_fn: Optional[Callable[[float], Awaitable[None]]] = None,
) -> Dict[str, Any]:
    jsonl_writer = JsonlFileWriter(output_jsonl_path)
    status_writer = StatusFileWriter(status_path)
    resolved_run_id = str(run_id or _default_run_id())
    resolved_sleep = sleep_async_fn or asyncio.sleep
    resolved_chainlink = chainlink_adapter or PolymarketRtdsChainlinkPriceAdapter()
    resolved_kalshi = kalshi_adapter or KalshiWebsitePriceAdapter()
    resolved_pair_selector = pair_selector or DiscoveryPairSelector(
        config_path=config_path,
        discovery_output=discovery_output_path,
        pair_cache_path=pair_cache_path,
    )
    resolved_collector_factory = collector_factory or build_live_collectors
    health_config = load_health_config_from_run_config(config_path=config_path)
    resolved_pairs_interval_ms = int(max(1.0, float(resolved_pairs_interval_seconds)) * 1000.0)
    resolved_pairs_worker_stop = asyncio.Event()
    resolved_pairs_task: Optional[asyncio.Task[Any]] = None
    resolved_state: Dict[str, Any] = {
        "enabled": bool(resolved_pairs_enabled),
        "interval_seconds": float(max(1.0, float(resolved_pairs_interval_seconds))),
        "run_on_start": bool(resolved_pairs_run_on_start),
        "attempts": 0,
        "successes": 0,
        "failures": 0,
        "last_run_started_ms": None,
        "last_success_ms": None,
        "last_failure_ms": None,
        "last_error": None,
        "last_summary": None,
    }

    if bool(resolved_pairs_enabled):
        effective_resolved_runner = resolved_pairs_runner or (
            lambda: _default_resolved_pairs_runner(
                config_path=config_path,
                pair_log_path=resolved_pairs_pair_log_path,
                summary_path=resolved_pairs_summary_path,
                page_size=int(resolved_pairs_page_size),
                max_pages=resolved_pairs_max_pages,
                max_markets=resolved_pairs_max_markets,
            )
        )
    else:
        effective_resolved_runner = None

    def _resolve_pair(run_discovery_first: bool) -> Dict[str, Any]:
        return dict(resolved_pair_selector.select(run_discovery_first=bool(run_discovery_first)))

    tick_count = 0
    started_at_ms = int(now_ms_fn())
    ended_at_ms = started_at_ms
    pair_refresh_attempts = 0
    pair_refresh_failures = 0
    pair_rotations = 0
    pending_retry_at_ms: Optional[int] = None
    pending_retry_for_boundary_ms: Optional[int] = None
    process_id = int(os.getpid())
    status_heartbeat_seq = 0
    last_errors: List[str] = []
    ws_start_stagger_applied = False

    chainlink_start = getattr(resolved_chainlink, "start", None)
    if callable(chainlink_start):
        try:
            chainlink_start(at_ms=int(now_ms_fn()))
            if float(ws_start_stagger_seconds) > 0:
                ws_start_stagger_applied = True
                await resolved_sleep(float(ws_start_stagger_seconds))
        except Exception:
            # Startup sequencing should not kill the run; first read() will surface adapter errors.
            pass

    current_pair = _resolve_pair(run_discovery_first=not bool(skip_discovery_initial))
    current_session = await _start_quote_runtime_session(
        pair=current_pair,
        health_config=health_config,
        collector_factory=resolved_collector_factory,
    )
    runtime_best_ask_adapter = RuntimeBestAskAdapter(runtime=current_session.runtime)
    boundary_target_ms = _boundary_target_epoch_ms_for_pair(current_pair, now_ms=started_at_ms)

    def _write_status(*, state: str, at_ms: int, at_iso: str, errors: List[str]) -> None:
        nonlocal status_heartbeat_seq
        status_heartbeat_seq += 1
        status_writer.write(
            {
                "kind": "market_surveillance_status",
                "ts": at_iso,
                "run_id": resolved_run_id,
                "watchdog": {
                    "state": str(state),
                    "heartbeat_seq": int(status_heartbeat_seq),
                    "pid": int(process_id),
                    "started_at_ms": int(started_at_ms),
                    "updated_at_ms": int(at_ms),
                    "updated_at": at_iso,
                },
                "ticks_written": tick_count,
                "last_tick_recv_ms": ended_at_ms if tick_count > 0 else None,
                "current_pair_key": _pair_key(current_pair),
                "current_pair": _normalize_pair_metadata(current_pair),
                "pair_refresh_attempts": pair_refresh_attempts,
                "pair_refresh_failures": pair_refresh_failures,
                "pair_rotations": pair_rotations,
                "boundary_target_ms": int(boundary_target_ms),
                "pending_retry_for_boundary_ms": pending_retry_for_boundary_ms,
                "pending_retry_at_ms": pending_retry_at_ms,
                "resolved_pairs": {
                    **resolved_state,
                    "pair_log_path": str(resolved_pairs_pair_log_path),
                    "summary_path": str(resolved_pairs_summary_path),
                },
                "last_error_count": len(errors),
                "last_errors": list(errors),
                "output_jsonl": str(output_jsonl_path),
            }
        )

    async def _resolved_pairs_loop() -> None:
        if effective_resolved_runner is None:
            return
        next_run_ms = int(started_at_ms if bool(resolved_pairs_run_on_start) else started_at_ms + resolved_pairs_interval_ms)
        while not resolved_pairs_worker_stop.is_set():
            at_ms = int(now_ms_fn())
            if at_ms < next_run_ms:
                sleep_s = min(1.0, max(0.0, float(next_run_ms - at_ms) / 1000.0))
                await resolved_sleep(sleep_s if sleep_s > 0 else 0.0)
                continue

            resolved_state["attempts"] = int(resolved_state.get("attempts", 0)) + 1
            resolved_state["last_run_started_ms"] = int(at_ms)
            try:
                run_summary = await asyncio.to_thread(effective_resolved_runner)
                resolved_state["successes"] = int(resolved_state.get("successes", 0)) + 1
                resolved_state["last_success_ms"] = int(now_ms_fn())
                resolved_state["last_error"] = None
                resolved_state["last_summary"] = _resolved_summary_compact(
                    run_summary if isinstance(run_summary, dict) else {}
                )
            except Exception as exc:
                resolved_state["failures"] = int(resolved_state.get("failures", 0)) + 1
                resolved_state["last_failure_ms"] = int(now_ms_fn())
                resolved_state["last_error"] = str(exc)

            next_run_ms = int(at_ms + resolved_pairs_interval_ms)

    if effective_resolved_runner is not None:
        resolved_pairs_task = asyncio.create_task(_resolved_pairs_loop())

    try:
        while True:
            at_ms = int(now_ms_fn())
            at_iso = str(now_iso_fn(at_ms))
            errors: List[str] = []

            boundary_retry_triggered = (
                pending_retry_at_ms is not None and at_ms >= int(pending_retry_at_ms)
            )
            boundary_due_triggered = (
                pending_retry_at_ms is None and at_ms >= int(boundary_target_ms)
            )
            if boundary_retry_triggered or boundary_due_triggered:
                pair_refresh_attempts += 1
                try:
                    refreshed_pair = _resolve_pair(run_discovery_first=True)
                    refreshed_key = _pair_key(refreshed_pair)
                    current_key = _pair_key(current_pair)
                    if refreshed_key != current_key:
                        await _stop_quote_runtime_session(current_session)
                        current_pair = dict(refreshed_pair)
                        current_session = await _start_quote_runtime_session(
                            pair=current_pair,
                            health_config=health_config,
                            collector_factory=resolved_collector_factory,
                        )
                        runtime_best_ask_adapter = RuntimeBestAskAdapter(runtime=current_session.runtime)
                        pair_rotations += 1
                    else:
                        current_pair = dict(refreshed_pair)
                    pending_retry_at_ms = None
                    pending_retry_for_boundary_ms = None
                    boundary_target_ms = _boundary_target_epoch_ms_for_pair(current_pair, now_ms=at_ms)
                except Exception as exc:
                    pair_refresh_failures += 1
                    errors.append(f"pair_refresh_error:{exc}")
                    if boundary_due_triggered:
                        pending_retry_for_boundary_ms = int(boundary_target_ms)
                        pending_retry_at_ms = int(at_ms + (max(0.0, float(pair_boundary_retry_seconds)) * 1000.0))
                    else:
                        pending_retry_for_boundary_ms = None
                        pending_retry_at_ms = None
                        boundary_target_ms = _next_quarter_boundary_epoch_ms(at_ms)

            try:
                chainlink_price = resolved_chainlink.read(at_ms=at_ms)
            except Exception as exc:
                chainlink_price = UnderlyingPricePoint(
                    source="chainlink_error",
                    price_usd=None,
                    source_timestamp_ms=None,
                )
                errors.append(f"chainlink_adapter_error:{exc}")

            try:
                kalshi_price = resolved_kalshi.read(at_ms=at_ms)
            except Exception as exc:
                kalshi_price = UnderlyingPricePoint(
                    source="kalshi_site_error",
                    price_usd=None,
                    source_timestamp_ms=None,
                )
                errors.append(f"kalshi_adapter_error:{exc}")

            try:
                best_asks = runtime_best_ask_adapter.read(at_ms=at_ms)
            except Exception as exc:
                best_asks = {}
                errors.append(f"best_ask_runtime_error:{exc}")

            row = build_snapshot_row(
                run_id=resolved_run_id,
                recv_ms=at_ms,
                ts=at_iso,
                pair=current_pair,
                chainlink_price=chainlink_price,
                kalshi_price=kalshi_price,
                best_asks=best_asks,
                errors=errors,
            )
            jsonl_writer.write(row)
            tick_count += 1
            ended_at_ms = at_ms
            last_errors = list(errors)
            _write_status(state="running", at_ms=at_ms, at_iso=at_iso, errors=last_errors)

            if max_ticks is not None and max_ticks > 0 and tick_count >= int(max_ticks):
                break
            if duration_seconds > 0 and (at_ms - started_at_ms) >= int(duration_seconds * 1000):
                break

            if tick_seconds > 0:
                await resolved_sleep(float(tick_seconds))
            else:
                await resolved_sleep(0.0)
    finally:
        try:
            resolved_pairs_worker_stop.set()
            if resolved_pairs_task is not None:
                try:
                    await asyncio.wait_for(resolved_pairs_task, timeout=5.0)
                except asyncio.TimeoutError:
                    resolved_pairs_task.cancel()
                    await asyncio.gather(resolved_pairs_task, return_exceptions=True)
            await _stop_quote_runtime_session(current_session)
            final_ms = int(now_ms_fn())
            final_iso = str(now_iso_fn(final_ms))
            if tick_count <= 0:
                ended_at_ms = int(final_ms)
            _write_status(state="stopped", at_ms=final_ms, at_iso=final_iso, errors=last_errors)
            chainlink_close = getattr(resolved_chainlink, "close", None)
            if callable(chainlink_close):
                try:
                    chainlink_close()
                except Exception:
                    pass
        finally:
            jsonl_writer.close()

    summary = {
        "run_id": resolved_run_id,
        "mode": "ws",
        "ticks_written": tick_count,
        "started_at_ms": started_at_ms,
        "ended_at_ms": ended_at_ms,
        "started_at": str(now_iso_fn(started_at_ms)),
        "ended_at": str(now_iso_fn(ended_at_ms)),
        "output_jsonl": str(output_jsonl_path),
        "status_path": str(status_path),
        "summary_path": str(summary_path) if summary_path is not None else None,
        "pair_refresh_attempts": pair_refresh_attempts,
        "pair_refresh_failures": pair_refresh_failures,
        "pair_rotations": pair_rotations,
        "ws_start_stagger_seconds": float(max(0.0, float(ws_start_stagger_seconds))),
        "ws_start_stagger_applied": bool(ws_start_stagger_applied),
        "resolved_pairs": {
            **resolved_state,
            "pair_log_path": str(resolved_pairs_pair_log_path),
            "summary_path": str(resolved_pairs_summary_path),
        },
        "watchdog": {
            "pid": int(process_id),
            "heartbeat_seq": int(status_heartbeat_seq),
        },
    }
    if summary_path is not None:
        _write_json(summary_path, summary)
    return summary


def run_surveillance_loop(
    *,
    run_id: str,
    pair: Dict[str, Any],
    output_jsonl_path: Path,
    status_path: Path,
    summary_path: Optional[Path] = None,
    chainlink_adapter: UnderlyingPriceAdapter,
    kalshi_adapter: UnderlyingPriceAdapter,
    best_ask_adapter: BestAskAdapter,
    tick_seconds: float,
    duration_seconds: int,
    max_ticks: Optional[int] = None,
    now_ms_fn: Callable[[], int] = _now_ms,
    now_iso_fn: Callable[[int], str] = _utc_iso_with_millis,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> Dict[str, Any]:
    jsonl_writer = JsonlFileWriter(output_jsonl_path)
    status_writer = StatusFileWriter(status_path)

    tick_count = 0
    started_at_ms = int(now_ms_fn())
    ended_at_ms = started_at_ms
    process_id = int(os.getpid())
    heartbeat_seq = 0
    last_errors: List[str] = []

    def _write_status(*, state: str, at_ms: int, at_iso: str, errors: List[str]) -> None:
        nonlocal heartbeat_seq
        heartbeat_seq += 1
        status_writer.write(
            {
                "kind": "market_surveillance_status",
                "ts": at_iso,
                "run_id": run_id,
                "watchdog": {
                    "state": str(state),
                    "heartbeat_seq": int(heartbeat_seq),
                    "pid": int(process_id),
                    "started_at_ms": int(started_at_ms),
                    "updated_at_ms": int(at_ms),
                    "updated_at": at_iso,
                },
                "ticks_written": tick_count,
                "last_tick_recv_ms": ended_at_ms if tick_count > 0 else None,
                "last_error_count": len(errors),
                "last_errors": list(errors),
                "output_jsonl": str(output_jsonl_path),
            }
        )

    try:
        while True:
            at_ms = int(now_ms_fn())
            at_iso = str(now_iso_fn(at_ms))
            errors: List[str] = []

            try:
                chainlink_price = chainlink_adapter.read(at_ms=at_ms)
            except Exception as exc:
                chainlink_price = UnderlyingPricePoint(
                    source="chainlink_error",
                    price_usd=None,
                    source_timestamp_ms=None,
                )
                errors.append(f"chainlink_adapter_error:{exc}")

            try:
                kalshi_price = kalshi_adapter.read(at_ms=at_ms)
            except Exception as exc:
                kalshi_price = UnderlyingPricePoint(
                    source="kalshi_site_error",
                    price_usd=None,
                    source_timestamp_ms=None,
                )
                errors.append(f"kalshi_adapter_error:{exc}")

            try:
                best_asks = best_ask_adapter.read(at_ms=at_ms)
            except Exception as exc:
                best_asks = {}
                errors.append(f"best_ask_adapter_error:{exc}")

            row = build_snapshot_row(
                run_id=run_id,
                recv_ms=at_ms,
                ts=at_iso,
                pair=pair,
                chainlink_price=chainlink_price,
                kalshi_price=kalshi_price,
                best_asks=best_asks,
                errors=errors,
            )
            jsonl_writer.write(row)
            tick_count += 1
            ended_at_ms = at_ms
            last_errors = list(errors)
            _write_status(state="running", at_ms=at_ms, at_iso=at_iso, errors=last_errors)

            if max_ticks is not None and max_ticks > 0 and tick_count >= int(max_ticks):
                break

            if duration_seconds > 0 and (at_ms - started_at_ms) >= int(duration_seconds * 1000):
                break

            if tick_seconds > 0:
                sleep_fn(float(tick_seconds))
    finally:
        final_ms = int(now_ms_fn())
        final_iso = str(now_iso_fn(final_ms))
        if tick_count <= 0:
            ended_at_ms = int(final_ms)
        _write_status(state="stopped", at_ms=final_ms, at_iso=final_iso, errors=last_errors)
        for adapter in (chainlink_adapter, kalshi_adapter):
            close_fn = getattr(adapter, "close", None)
            if callable(close_fn):
                try:
                    close_fn()
                except Exception:
                    pass
        jsonl_writer.close()

    summary = {
        "run_id": run_id,
        "mode": "mock",
        "ticks_written": tick_count,
        "started_at_ms": started_at_ms,
        "ended_at_ms": ended_at_ms,
        "started_at": str(now_iso_fn(started_at_ms)),
        "ended_at": str(now_iso_fn(ended_at_ms)),
        "output_jsonl": str(output_jsonl_path),
        "status_path": str(status_path),
        "summary_path": str(summary_path) if summary_path is not None else None,
        "watchdog": {
            "pid": int(process_id),
            "heartbeat_seq": int(heartbeat_seq),
        },
    }
    if summary_path is not None:
        _write_json(summary_path, summary)
    return summary


def run_mock_market_surveillance(
    *,
    output_jsonl_path: Path,
    status_path: Path,
    summary_path: Optional[Path] = None,
    run_id: Optional[str] = None,
    pair: Optional[Dict[str, Any]] = None,
    tick_seconds: float = 1.0,
    duration_seconds: int = 0,
    max_ticks: Optional[int] = None,
    now_ms_fn: Callable[[], int] = _now_ms,
    now_iso_fn: Callable[[int], str] = _utc_iso_with_millis,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> Dict[str, Any]:
    resolved_run_id = str(run_id or _default_run_id())
    resolved_pair = dict(
        pair
        or {
            "kalshi_ticker": "KXBTC15M-UNKNOWN",
            "polymarket_slug": "btc-updown-15m-unknown",
            "polymarket_market_id": "unknown",
            "polymarket_token_yes": "unknown",
            "polymarket_token_no": "unknown",
            "window_end": None,
        }
    )
    return run_surveillance_loop(
        run_id=resolved_run_id,
        pair=resolved_pair,
        output_jsonl_path=output_jsonl_path,
        status_path=status_path,
        summary_path=summary_path,
        chainlink_adapter=MockChainlinkPriceAdapter(),
        kalshi_adapter=MockKalshiWebsitePriceAdapter(),
        best_ask_adapter=MockBestAskAdapter(),
        tick_seconds=float(tick_seconds),
        duration_seconds=int(duration_seconds),
        max_ticks=max_ticks,
        now_ms_fn=now_ms_fn,
        now_iso_fn=now_iso_fn,
        sleep_fn=sleep_fn,
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Market surveillance runner (phase-1 mock mode and phase-2 websocket quote mode)."
    )
    parser.add_argument(
        "--runtime-mode",
        choices=["mock", "ws"],
        default="ws",
        help="Run with live websocket quote runtime (`ws`) or phase-1 mocked quote adapters (`mock`).",
    )
    parser.add_argument(
        "--output-jsonl",
        default="data/diagnostic/market_surveillance/btc_15m_per_second.jsonl",
    )
    parser.add_argument(
        "--status-json",
        default="data/diagnostic/market_surveillance/market_surveillance_status.json",
    )
    parser.add_argument(
        "--summary-json",
        default="",
        help=(
            "Optional path for structured run summary JSON. "
            "If omitted, defaults to data/diagnostic/market_surveillance/"
            "market_surveillance_summary__<run_id>.json."
        ),
    )
    parser.add_argument("--duration-seconds", type=int, default=0)
    parser.add_argument("--tick-seconds", type=float, default=1.0)
    parser.add_argument(
        "--ws-start-stagger-seconds",
        type=float,
        default=10.0,
        help="Seconds to wait after starting RTDS before starting market quote websockets (ws mode).",
    )
    parser.add_argument(
        "--max-ticks",
        type=int,
        default=0,
        help="Optional max ticks for bounded runs. 0 means disabled.",
    )
    parser.add_argument("--run-id", default="")
    parser.add_argument("--kalshi-ticker", default="KXBTC15M-UNKNOWN")
    parser.add_argument("--polymarket-slug", default="btc-updown-15m-unknown")
    parser.add_argument("--polymarket-market-id", default="unknown")
    parser.add_argument("--polymarket-token-yes", default="unknown")
    parser.add_argument("--polymarket-token-no", default="unknown")
    parser.add_argument("--window-end", default="")
    parser.add_argument("--config", default="config/run_config.json")
    parser.add_argument("--discovery-output", default="data/market_discovery_latest.json")
    parser.add_argument("--pair-cache", default="data/market_pair_cache.json")
    parser.add_argument("--skip-discovery", action="store_true")
    parser.add_argument("--pair-boundary-retry-seconds", type=float, default=20.0)
    parser.add_argument(
        "--resolved-pairs-enabled",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    parser.add_argument("--resolved-pairs-interval-seconds", type=float, default=600.0)
    parser.add_argument(
        "--resolved-pairs-run-on-start",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    parser.add_argument(
        "--resolved-pairs-log-path",
        default="data/diagnostic/resolved_15m_pairs.log",
    )
    parser.add_argument(
        "--resolved-pairs-summary-path",
        default="data/diagnostic/resolution_comparison_summary.json",
    )
    parser.add_argument("--resolved-pairs-page-size", type=int, default=500)
    parser.add_argument("--resolved-pairs-max-pages", type=int, default=0)
    parser.add_argument("--resolved-pairs-max-markets", type=int, default=0)
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    resolved_run_id = str(args.run_id or _default_run_id())
    resolved_summary_path = (
        Path(args.summary_json)
        if str(args.summary_json or "").strip()
        else _default_summary_path(run_id=resolved_run_id)
    )
    pair = {
        "kalshi_ticker": args.kalshi_ticker,
        "polymarket_slug": args.polymarket_slug,
        "polymarket_market_id": args.polymarket_market_id,
        "polymarket_token_yes": args.polymarket_token_yes,
        "polymarket_token_no": args.polymarket_token_no,
        "window_end": args.window_end or None,
    }
    if str(args.runtime_mode) == "ws":
        summary = asyncio.run(
            run_ws_market_surveillance(
                output_jsonl_path=Path(args.output_jsonl),
                status_path=Path(args.status_json),
                summary_path=resolved_summary_path,
                config_path=Path(args.config),
                discovery_output_path=Path(args.discovery_output),
                pair_cache_path=Path(args.pair_cache),
                run_id=resolved_run_id,
                tick_seconds=float(args.tick_seconds),
                duration_seconds=int(args.duration_seconds),
                max_ticks=(int(args.max_ticks) if int(args.max_ticks) > 0 else None),
                ws_start_stagger_seconds=float(args.ws_start_stagger_seconds),
                pair_boundary_retry_seconds=float(args.pair_boundary_retry_seconds),
                resolved_pairs_enabled=bool(args.resolved_pairs_enabled),
                resolved_pairs_interval_seconds=float(args.resolved_pairs_interval_seconds),
                resolved_pairs_run_on_start=bool(args.resolved_pairs_run_on_start),
                resolved_pairs_pair_log_path=Path(args.resolved_pairs_log_path),
                resolved_pairs_summary_path=Path(args.resolved_pairs_summary_path),
                resolved_pairs_page_size=int(args.resolved_pairs_page_size),
                resolved_pairs_max_pages=(int(args.resolved_pairs_max_pages) if int(args.resolved_pairs_max_pages) > 0 else None),
                resolved_pairs_max_markets=(
                    int(args.resolved_pairs_max_markets) if int(args.resolved_pairs_max_markets) > 0 else None
                ),
                skip_discovery_initial=bool(args.skip_discovery),
            )
        )
        print("Market surveillance phase-2 websocket run complete")
    else:
        summary = run_mock_market_surveillance(
            output_jsonl_path=Path(args.output_jsonl),
            status_path=Path(args.status_json),
            summary_path=resolved_summary_path,
            run_id=resolved_run_id,
            pair=pair,
            tick_seconds=float(args.tick_seconds),
            duration_seconds=int(args.duration_seconds),
            max_ticks=(int(args.max_ticks) if int(args.max_ticks) > 0 else None),
        )
        print("Market surveillance phase-1 run complete")
    print(f"Run ID: {summary['run_id']}")
    print(f"Ticks written: {summary['ticks_written']}")
    print(f"Output JSONL: {summary['output_jsonl']}")
    print(f"Status JSON:  {summary['status_path']}")
    print(f"Summary JSON: {summary.get('summary_path')}")


if __name__ == "__main__":
    main()
