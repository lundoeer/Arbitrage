from __future__ import annotations

import asyncio
import base64
import os
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, List, Optional

from scripts.common.api_transport import ApiTransport, RetryConfig
from scripts.common.buy_execution import (
    _build_polymarket_clob_context_from_env,
    _load_kalshi_private_key_from_env,
    _resolve_kalshi_api_key_from_env,
)
from scripts.common.position_runtime import PositionRuntime
from scripts.common.utils import as_dict, as_non_empty_text, as_float, now_ms


@dataclass(frozen=True)
class PositionPollClientConfig:
    timeout_seconds: int = 10
    retry_max_attempts: int = 3
    retry_base_backoff_seconds: float = 0.5
    retry_jitter_ratio: float = 0.2
    kalshi_page_limit: int = 100
    kalshi_max_pages: int = 20
    kalshi_orders_page_limit: int = 100
    kalshi_orders_max_pages: int = 20

    def build_transport(self) -> ApiTransport:
        retry = RetryConfig(
            max_attempts=max(1, int(self.retry_max_attempts)),
            base_backoff_seconds=max(0.0, float(self.retry_base_backoff_seconds)),
            jitter_ratio=max(0.0, float(self.retry_jitter_ratio)),
        )
        return ApiTransport(timeout_seconds=max(1, int(self.timeout_seconds)), retry_config=retry)


class PolymarketPositionsPollClient:
    """
    Poll current open positions from Polymarket Data API for the selected pair.
    """

    def __init__(
        self,
        *,
        user_address: str,
        condition_id: str,
        token_yes: str,
        token_no: str,
        base_url: str = "https://data-api.polymarket.com",
        transport: Optional[ApiTransport] = None,
        config: Optional[PositionPollClientConfig] = None,
    ) -> None:
        self.user_address = str(user_address or "").strip().lower()
        self.condition_id = str(condition_id or "").strip().lower()
        self.token_yes = str(token_yes or "").strip()
        self.token_no = str(token_no or "").strip()
        self.base_url = str(base_url).rstrip("/")
        cfg = config or PositionPollClientConfig()
        self.transport = transport or cfg.build_transport()
        if not self.user_address:
            raise RuntimeError("Polymarket positions poll client requires user_address")
        if not self.condition_id:
            raise RuntimeError("Polymarket positions poll client requires condition_id")
        if not self.token_yes or not self.token_no:
            raise RuntimeError("Polymarket positions poll client requires token_yes/token_no")

    def fetch_positions(self, *, include_raw_http: bool = False) -> Dict[str, Any]:
        _, payload = self.transport.request_json(
            "GET",
            f"{self.base_url}/positions",
            params={
                "user": self.user_address,
                "market": self.condition_id,
                "sizeThreshold": 0,
            },
            allow_status={200},
        )
        if payload is None:
            raise RuntimeError("Polymarket positions poll returned null payload")
        if not isinstance(payload, list):
            raise RuntimeError(f"Polymarket positions poll returned unexpected payload type: {type(payload).__name__}")
        rows = list(payload)

        yes_total = 0.0
        no_total = 0.0
        yes_exposure_total = 0.0
        no_exposure_total = 0.0
        yes_exposure_seen = False
        no_exposure_seen = False
        filtered_count = 0
        for raw in rows:
            item = as_dict(raw)
            if not item:
                continue
            condition = str(item.get("conditionId") or "").strip().lower()
            asset = str(item.get("asset") or "").strip()
            raw_outcome = as_non_empty_text(item.get("outcome"))
            outcome = raw_outcome.lower() if raw_outcome else None
            size = as_float(item.get("size"))
            if condition and condition != self.condition_id:
                continue
            # Defensive scope filter: only selected pair assets are accepted.
            if asset not in {self.token_yes, self.token_no}:
                continue

            filtered_count += 1
            avg_price = as_float(item.get("avgPrice"))
            initial_value = as_float(item.get("initialValue"))
            if initial_value is None and size is not None and avg_price is not None:
                initial_value = float(size) * float(avg_price)
            if outcome == "yes" or asset == self.token_yes:
                if size is not None:
                    yes_total += float(size)
                if initial_value is not None:
                    yes_exposure_total += float(initial_value)
                    yes_exposure_seen = True
            elif outcome == "no" or asset == self.token_no:
                if size is not None:
                    no_total += float(size)
                if initial_value is not None:
                    no_exposure_total += float(initial_value)
                    no_exposure_seen = True

        yes_avg_entry_price: Optional[float] = None
        if yes_exposure_seen and yes_total > 0.0:
            inferred = float(yes_exposure_total) / float(yes_total)
            if 0.0 < inferred <= 1.0:
                yes_avg_entry_price = inferred
        no_avg_entry_price: Optional[float] = None
        if no_exposure_seen and no_total > 0.0:
            inferred = float(no_exposure_total) / float(no_total)
            if 0.0 < inferred <= 1.0:
                no_avg_entry_price = inferred

        positions = [
            {
                "instrument_id": self.token_yes,
                "outcome_side": "yes",
                "net_contracts": float(yes_total),
                "avg_entry_price": yes_avg_entry_price,
                "position_exposure_usd": float(yes_exposure_total) if yes_exposure_seen else None,
            },
            {
                "instrument_id": self.token_no,
                "outcome_side": "no",
                "net_contracts": float(no_total),
                "avg_entry_price": no_avg_entry_price,
                "position_exposure_usd": float(no_exposure_total) if no_exposure_seen else None,
            },
        ]
        snapshot = {
            "venue": "polymarket",
            "positions": positions,
            "raw_count": len(rows),
            "filtered_count": int(filtered_count),
            "pages": 1,
            "cursor_end": None,
        }
        if bool(include_raw_http):
            snapshot["raw_http_body"] = payload
        return snapshot


def _resolve_polymarket_signature_type_from_env(default: int = 1) -> int:
    raw = str(os.getenv("POLYMARKET_SIGNATURE_TYPE", "") or "").strip()
    if not raw:
        return int(default)
    try:
        parsed = int(raw)
    except Exception:
        return int(default)
    if parsed not in {0, 1, 2}:
        return int(default)
    return int(parsed)


class PolymarketAccountPollClient:
    """
    Poll authenticated Polymarket account balance/allowance state.

    Uses py-clob-client L2 auth path and reports collateral allowance payload.
    """

    def __init__(
        self,
        *,
        base_url: str = "https://clob.polymarket.com",
        clob_client: Optional[Any] = None,
        signature_type: Optional[int] = None,
    ) -> None:
        if clob_client is None:
            context = _build_polymarket_clob_context_from_env(base_url=base_url)
            clob_client = context.clob_client
        self.clob_client = clob_client
        self.signature_type = (
            int(signature_type)
            if signature_type is not None
            else _resolve_polymarket_signature_type_from_env(default=1)
        )

    def fetch_balance_allowance(self) -> Dict[str, Any]:
        params: Any
        try:
            from py_clob_client.clob_types import AssetType, BalanceAllowanceParams
            params = BalanceAllowanceParams(
                asset_type=AssetType.COLLATERAL,
                signature_type=int(self.signature_type),
            )
        except Exception:
            # Fallback supports lightweight stubs in tests.
            params = {
                "asset_type": "COLLATERAL",
                "signature_type": int(self.signature_type),
            }

        allowance = self.clob_client.get_balance_allowance(params)
        return {
            "venue": "polymarket",
            "balance_allowance": as_dict(allowance),
        }


class PolymarketOrdersPollClient:
    """
    Poll Polymarket user orders for the selected pair (including recently closed
    where available from the underlying client method).
    """

    def __init__(
        self,
        *,
        condition_id: str,
        token_yes: str,
        token_no: str,
        clob_client: Optional[Any] = None,
        base_url: str = "https://clob.polymarket.com",
    ) -> None:
        self.condition_id = str(condition_id or "").strip().lower()
        self.token_yes = str(token_yes or "").strip()
        self.token_no = str(token_no or "").strip()
        if not self.condition_id:
            raise RuntimeError("Polymarket orders poll client requires condition_id")
        if not self.token_yes or not self.token_no:
            raise RuntimeError("Polymarket orders poll client requires token_yes/token_no")
        if clob_client is None:
            context = _build_polymarket_clob_context_from_env(base_url=base_url)
            clob_client = context.clob_client
        self.clob_client = clob_client

    @staticmethod
    def _collect_rows(payload: Any) -> List[Dict[str, Any]]:
        if isinstance(payload, list):
            return [as_dict(item) for item in payload if isinstance(item, dict)]
        body = as_dict(payload)
        for key in ("orders", "data", "results"):
            value = body.get(key)
            if isinstance(value, list):
                return [as_dict(item) for item in value if isinstance(item, dict)]
        return []

    def _fetch_raw_orders(self) -> List[Dict[str, Any]]:
        methods = [
            ("get_orders", [{"market": self.condition_id}, {}]),
            ("get_orders_history", [{"market": self.condition_id}, {}]),
            ("get_open_orders", [{"market": self.condition_id}, {}]),
        ]
        for method_name, arg_candidates in methods:
            fn = getattr(self.clob_client, method_name, None)
            if not callable(fn):
                continue
            for args in arg_candidates:
                try:
                    payload = fn(args) if args else fn()
                except TypeError:
                    continue
                except Exception:
                    continue
                rows = self._collect_rows(payload)
                if rows:
                    return rows
        return []

    def fetch_orders(self) -> Dict[str, Any]:
        rows = self._fetch_raw_orders()
        filtered: List[Dict[str, Any]] = []
        for item in rows:
            market = str(item.get("market") or item.get("condition_id") or item.get("conditionId") or "").strip().lower()
            asset = str(item.get("asset_id") or item.get("asset") or item.get("token_id") or "").strip()
            if market and market != self.condition_id:
                continue
            if asset not in {self.token_yes, self.token_no}:
                continue
            status = as_non_empty_text(item.get("status")) or as_non_empty_text(item.get("type")) or None
            action = as_non_empty_text(item.get("side")) or as_non_empty_text(item.get("order_side")) or None
            requested_size = as_float(item.get("original_size"))
            if requested_size is None:
                requested_size = as_float(item.get("size"))
            filled_size = as_float(item.get("size_matched"))
            remaining_size = as_float(item.get("remaining_size"))
            if remaining_size is None and requested_size is not None and filled_size is not None:
                remaining_size = max(0.0, float(requested_size - filled_size))
            filtered.append(
                {
                    "client_order_id": as_non_empty_text(item.get("client_order_id")),
                    "order_id": as_non_empty_text(item.get("id")) or as_non_empty_text(item.get("order_id")),
                    "instrument_id": asset,
                    "outcome_side": "yes" if asset == self.token_yes else "no",
                    "action": str(action or "").strip().lower() or None,
                    "status": str(status or "").strip().lower() or None,
                    "requested_size": None if requested_size is None else float(requested_size),
                    "filled_size": None if filled_size is None else float(filled_size),
                    "remaining_size": None if remaining_size is None else float(remaining_size),
                    "limit_price": as_float(item.get("price")),
                    "raw": item,
                }
            )

        return {
            "venue": "polymarket",
            "orders": filtered,
            "raw_count": int(len(rows)),
            "filtered_count": int(len(filtered)),
            "pages": 1,
            "cursor_end": None,
            "truncated_by_max_pages": False,
        }


class KalshiPositionsPollClient:
    """
    Poll current open positions from Kalshi private REST API for one ticker.
    """

    def __init__(
        self,
        *,
        market_ticker: str,
        base_host: str = "https://api.elections.kalshi.com",
        api_prefix: str = "/trade-api/v2",
        transport: Optional[ApiTransport] = None,
        config: Optional[PositionPollClientConfig] = None,
        api_key: Optional[str] = None,
        private_key: Optional[Any] = None,
        headers_factory: Optional[Callable[[str], Dict[str, str]]] = None,
    ) -> None:
        self.market_ticker = str(market_ticker or "").strip()
        if not self.market_ticker:
            raise RuntimeError("Kalshi positions poll client requires market_ticker")
        self.base_host = str(base_host).rstrip("/")
        self.api_prefix = "/" + str(api_prefix or "").strip("/").strip()
        cfg = config or PositionPollClientConfig()
        self.config = cfg
        self.transport = transport or cfg.build_transport()
        self.api_key = str(api_key or _resolve_kalshi_api_key_from_env()).strip()
        self.private_key = private_key if private_key is not None else _load_kalshi_private_key_from_env()
        self.headers_factory = headers_factory
        if self.headers_factory is None and not self.api_key:
            raise RuntimeError("Kalshi positions poll client requires API key credentials")

    def _sign(self, *, method: str, path: str, timestamp_ms: str) -> str:
        try:
            from cryptography.hazmat.primitives import hashes
            from cryptography.hazmat.primitives.asymmetric import padding
        except Exception as exc:
            raise RuntimeError("Missing cryptography dependency required for Kalshi auth signing") from exc
        message = f"{timestamp_ms}{method.upper()}{path}"
        signature = self.private_key.sign(
            message.encode("utf-8"),
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.DIGEST_LENGTH,
            ),
            hashes.SHA256(),
        )
        return base64.b64encode(signature).decode("utf-8")

    def _headers_for_path(self, *, path: str) -> Dict[str, str]:
        if self.headers_factory is not None:
            return dict(self.headers_factory(path))
        ts = str(now_ms())
        return {
            "KALSHI-ACCESS-KEY": self.api_key,
            "KALSHI-ACCESS-TIMESTAMP": ts,
            "KALSHI-ACCESS-SIGNATURE": self._sign(method="GET", path=path, timestamp_ms=ts),
            "Content-Type": "application/json",
        }

    def fetch_positions(self, *, include_raw_http: bool = False) -> Dict[str, Any]:
        path = f"{self.api_prefix}/portfolio/positions"
        limit = max(1, min(1000, int(self.config.kalshi_page_limit)))
        max_pages = max(1, int(self.config.kalshi_max_pages))
        cursor: Optional[str] = None
        pages = 0
        row_count = 0
        net_position_total = 0.0
        yes_exposure_total = 0.0
        no_exposure_total = 0.0
        yes_exposure_seen = False
        no_exposure_seen = False
        total_fees_paid_usd: Optional[float] = None
        total_realized_pnl_usd: Optional[float] = None
        raw_http_pages: List[Any] = []

        while pages < max_pages:
            pages += 1
            params: Dict[str, Any] = {"ticker": self.market_ticker, "limit": limit}
            if cursor:
                params["cursor"] = cursor
            _, payload = self.transport.request_json(
                "GET",
                f"{self.base_host}{path}",
                headers=self._headers_for_path(path=path),
                params=params,
                allow_status={200},
            )
            if payload is None:
                raise RuntimeError("Kalshi positions poll returned null payload")
            body = as_dict(payload)
            if not body:
                raise RuntimeError("Kalshi positions poll returned empty/non-object payload")
            if bool(include_raw_http):
                raw_http_pages.append(payload)
            market_positions = body.get("market_positions")
            if market_positions is None:
                raise RuntimeError("Kalshi positions poll payload missing market_positions")
            rows = list(market_positions) if isinstance(market_positions, list) else []
            for raw in rows:
                item = as_dict(raw)
                if str(item.get("ticker") or "").strip() != self.market_ticker:
                    continue
                row_count += 1
                value = as_float(item.get("position_fp"))
                if value is None:
                    value = as_float(item.get("position"))
                position = float(value or 0.0)
                net_position_total += position
                side = str(item.get("side") or "").strip().lower()
                market_exposure_dollars = as_float(item.get("market_exposure_dollars"))
                if position > 0.0 or side == "yes":
                    if market_exposure_dollars is not None:
                        yes_exposure_total += abs(float(market_exposure_dollars))
                        yes_exposure_seen = True
                elif position < 0.0 or side == "no":
                    if market_exposure_dollars is not None:
                        no_exposure_total += abs(float(market_exposure_dollars))
                        no_exposure_seen = True

                realized = as_float(item.get("realized_pnl_dollars"))
                if realized is None:
                    cents = as_float(item.get("realized_pnl"))
                    realized = None if cents is None else float(cents) / 10_000.0
                if realized is not None:
                    total_realized_pnl_usd = float((total_realized_pnl_usd or 0.0) + realized)

                fees = as_float(item.get("fees_paid_dollars"))
                if fees is None:
                    cents = as_float(item.get("fees_paid"))
                    fees = None if cents is None else float(cents) / 10_000.0
                if fees is not None:
                    total_fees_paid_usd = float((total_fees_paid_usd or 0.0) + fees)

            cursor_raw = as_non_empty_text(body.get("cursor"))
            if not cursor_raw:
                cursor = None
                break
            cursor = cursor_raw

        yes_position = net_position_total if net_position_total > 0.0 else 0.0
        no_position = abs(net_position_total) if net_position_total < 0.0 else 0.0
        yes_avg_entry_price: Optional[float] = None
        if yes_exposure_seen and yes_position > 0.0:
            inferred = float(yes_exposure_total) / float(yes_position)
            if 0.0 < inferred <= 1.0:
                yes_avg_entry_price = inferred
        no_avg_entry_price: Optional[float] = None
        if no_exposure_seen and no_position > 0.0:
            inferred = float(no_exposure_total) / float(no_position)
            if 0.0 < inferred <= 1.0:
                no_avg_entry_price = inferred

        positions = [
            {
                "instrument_id": self.market_ticker,
                "outcome_side": "yes",
                "net_contracts": float(yes_position),
                "avg_entry_price": yes_avg_entry_price,
                "position_exposure_usd": float(yes_exposure_total) if yes_exposure_seen else None,
                "fees": total_fees_paid_usd,
                "realized_pnl": total_realized_pnl_usd,
            },
            {
                "instrument_id": self.market_ticker,
                "outcome_side": "no",
                "net_contracts": float(no_position),
                "avg_entry_price": no_avg_entry_price,
                "position_exposure_usd": float(no_exposure_total) if no_exposure_seen else None,
                "fees": total_fees_paid_usd,
                "realized_pnl": total_realized_pnl_usd,
            },
        ]
        snapshot = {
            "venue": "kalshi",
            "positions": positions,
            "raw_count": int(row_count),
            "filtered_count": int(row_count),
            "pages": int(pages),
            "cursor_end": cursor,
            "truncated_by_max_pages": bool(cursor is not None),
        }
        if bool(include_raw_http):
            snapshot["raw_http_pages"] = raw_http_pages
        return snapshot

    def fetch_balance(self) -> Dict[str, Any]:
        path = f"{self.api_prefix}/portfolio/balance"
        _, payload = self.transport.request_json(
            "GET",
            f"{self.base_host}{path}",
            headers=self._headers_for_path(path=path),
            allow_status={200},
        )
        return {
            "venue": "kalshi",
            "balance": as_dict(payload),
        }


class KalshiOrdersPollClient:
    """
    Poll Kalshi private REST orders for one ticker, including recently closed
    orders when available from API defaults.
    """

    def __init__(
        self,
        *,
        market_ticker: str,
        base_host: str = "https://api.elections.kalshi.com",
        api_prefix: str = "/trade-api/v2",
        transport: Optional[ApiTransport] = None,
        config: Optional[PositionPollClientConfig] = None,
        api_key: Optional[str] = None,
        private_key: Optional[Any] = None,
        headers_factory: Optional[Callable[[str], Dict[str, str]]] = None,
    ) -> None:
        self.market_ticker = str(market_ticker or "").strip()
        if not self.market_ticker:
            raise RuntimeError("Kalshi orders poll client requires market_ticker")
        self.base_host = str(base_host).rstrip("/")
        self.api_prefix = "/" + str(api_prefix or "").strip("/").strip()
        cfg = config or PositionPollClientConfig()
        self.config = cfg
        self.transport = transport or cfg.build_transport()
        self.api_key = str(api_key or _resolve_kalshi_api_key_from_env()).strip()
        self.private_key = private_key if private_key is not None else _load_kalshi_private_key_from_env()
        self.headers_factory = headers_factory
        if self.headers_factory is None and not self.api_key:
            raise RuntimeError("Kalshi orders poll client requires API key credentials")

    def _sign(self, *, method: str, path: str, timestamp_ms: str) -> str:
        try:
            from cryptography.hazmat.primitives import hashes
            from cryptography.hazmat.primitives.asymmetric import padding
        except Exception as exc:
            raise RuntimeError("Missing cryptography dependency required for Kalshi auth signing") from exc
        message = f"{timestamp_ms}{method.upper()}{path}"
        signature = self.private_key.sign(
            message.encode("utf-8"),
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.DIGEST_LENGTH,
            ),
            hashes.SHA256(),
        )
        return base64.b64encode(signature).decode("utf-8")

    def _headers_for_path(self, *, path: str) -> Dict[str, str]:
        if self.headers_factory is not None:
            return dict(self.headers_factory(path))
        ts = str(now_ms())
        return {
            "KALSHI-ACCESS-KEY": self.api_key,
            "KALSHI-ACCESS-TIMESTAMP": ts,
            "KALSHI-ACCESS-SIGNATURE": self._sign(method="GET", path=path, timestamp_ms=ts),
            "Content-Type": "application/json",
        }

    def fetch_orders(self) -> Dict[str, Any]:
        path = f"{self.api_prefix}/portfolio/orders"
        limit = max(1, min(1000, int(self.config.kalshi_orders_page_limit)))
        max_pages = max(1, int(self.config.kalshi_orders_max_pages))
        cursor: Optional[str] = None
        pages = 0
        row_count = 0
        filtered: List[Dict[str, Any]] = []
        while pages < max_pages:
            pages += 1
            params: Dict[str, Any] = {"ticker": self.market_ticker, "limit": limit}
            if cursor:
                params["cursor"] = cursor
            _, payload = self.transport.request_json(
                "GET",
                f"{self.base_host}{path}",
                headers=self._headers_for_path(path=path),
                params=params,
                allow_status={200},
            )
            body = as_dict(payload)
            rows_raw = body.get("orders")
            rows = list(rows_raw) if isinstance(rows_raw, list) else []
            for raw in rows:
                item = as_dict(raw)
                if str(item.get("ticker") or item.get("market_ticker") or "").strip() != self.market_ticker:
                    continue
                row_count += 1
                side_text = str(item.get("side") or "").strip().lower()
                outcome_side = side_text if side_text in {"yes", "no"} else None
                status = as_non_empty_text(item.get("status")) or as_non_empty_text(item.get("order_status"))
                requested_size = as_float(item.get("initial_count_fp"))
                if requested_size is None:
                    requested_size = as_float(item.get("initial_count"))
                filled_size = as_float(item.get("fill_count_fp"))
                if filled_size is None:
                    filled_size = as_float(item.get("fill_count"))
                remaining_size = as_float(item.get("remaining_count_fp"))
                if remaining_size is None:
                    remaining_size = as_float(item.get("remaining_count"))
                if remaining_size is None and requested_size is not None and filled_size is not None:
                    remaining_size = max(0.0, float(requested_size - filled_size))
                limit_price = as_float(item.get("yes_price_dollars")) if outcome_side == "yes" else as_float(item.get("no_price_dollars"))
                if limit_price is None:
                    limit_price = as_float(item.get("price_dollars"))
                filtered.append(
                    {
                        "client_order_id": as_non_empty_text(item.get("client_order_id")),
                        "order_id": as_non_empty_text(item.get("order_id")) or as_non_empty_text(item.get("id")),
                        "instrument_id": self.market_ticker,
                        "outcome_side": outcome_side,
                        "action": str(as_non_empty_text(item.get("action")) or "").strip().lower() or None,
                        "status": str(status or "").strip().lower() or None,
                        "requested_size": None if requested_size is None else float(requested_size),
                        "filled_size": None if filled_size is None else float(filled_size),
                        "remaining_size": None if remaining_size is None else float(remaining_size),
                        "limit_price": None if limit_price is None else float(limit_price),
                        "raw": item,
                    }
                )

            cursor_raw = as_non_empty_text(body.get("cursor"))
            if not cursor_raw:
                cursor = None
                break
            cursor = cursor_raw

        return {
            "venue": "kalshi",
            "orders": filtered,
            "raw_count": int(row_count),
            "filtered_count": int(len(filtered)),
            "pages": int(pages),
            "cursor_end": cursor,
            "truncated_by_max_pages": bool(cursor is not None),
        }


@dataclass(frozen=True)
class PositionReconcileLoopConfig:
    polymarket_poll_seconds: float = 10.0
    kalshi_poll_seconds: float = 20.0
    polymarket_orders_poll_seconds: float = 10.0
    kalshi_orders_poll_seconds: float = 20.0
    loop_sleep_seconds: float = 0.2


class PositionReconcileLoop:
    """
    Orchestrates periodic REST snapshots and applies them to PositionRuntime.
    """

    def __init__(
        self,
        *,
        runtime: PositionRuntime,
        polymarket_client: Optional[PolymarketPositionsPollClient],
        kalshi_client: Optional[KalshiPositionsPollClient],
        polymarket_orders_client: Optional[PolymarketOrdersPollClient] = None,
        kalshi_orders_client: Optional[KalshiOrdersPollClient] = None,
        config: Optional[PositionReconcileLoopConfig] = None,
        log_raw_http: bool = False,
    ) -> None:
        self.runtime = runtime
        self.polymarket_client = polymarket_client
        self.kalshi_client = kalshi_client
        self.polymarket_orders_client = polymarket_orders_client
        self.kalshi_orders_client = kalshi_orders_client
        self.config = config or PositionReconcileLoopConfig()
        self.log_raw_http = bool(log_raw_http)
        self.last_position_attempt_ms: Dict[str, Optional[int]] = {"polymarket": None, "kalshi": None}
        self.last_order_attempt_ms: Dict[str, Optional[int]] = {"polymarket": None, "kalshi": None}
        self.stats: Dict[str, int] = {
            "polymarket_success": 0,
            "polymarket_failure": 0,
            "kalshi_success": 0,
            "kalshi_failure": 0,
            "polymarket_orders_success": 0,
            "polymarket_orders_failure": 0,
            "kalshi_orders_success": 0,
            "kalshi_orders_failure": 0,
            "poll_iterations": 0,
        }

    def _is_due(self, *, venue: str, now_epoch_ms: int, source: str) -> bool:
        if source == "orders":
            last = self.last_order_attempt_ms.get(venue)
            interval_s = (
                float(self.config.polymarket_orders_poll_seconds)
                if venue == "polymarket"
                else float(self.config.kalshi_orders_poll_seconds)
            )
        else:
            last = self.last_position_attempt_ms.get(venue)
            interval_s = (
                float(self.config.polymarket_poll_seconds)
                if venue == "polymarket"
                else float(self.config.kalshi_poll_seconds)
            )
        interval_ms = int(max(0.2, interval_s) * 1000)
        if last is None:
            return True
        return int(now_epoch_ms - int(last)) >= interval_ms

    def _fetch_positions_snapshot(self, *, client: Any) -> Dict[str, Any]:
        if not bool(self.log_raw_http):
            return as_dict(client.fetch_positions())
        return as_dict(client.fetch_positions(include_raw_http=True))

    def run_once(self, *, now_epoch_ms: Optional[int] = None, force: bool = False) -> Dict[str, Any]:
        ts = int(now_epoch_ms if now_epoch_ms is not None else now_ms())
        results: Dict[str, Any] = {"at_ms": ts, "venues": {}}
        self.stats["poll_iterations"] += 1

        for venue in ("polymarket", "kalshi"):
            client = self.polymarket_client if venue == "polymarket" else self.kalshi_client
            if client is None:
                continue
            if not force and not self._is_due(venue=venue, now_epoch_ms=ts, source="positions"):
                continue
            self.last_position_attempt_ms[venue] = ts
            try:
                snapshot = self._fetch_positions_snapshot(client=client)
                reconcile = self.runtime.reconcile_positions_snapshot(
                    venue=venue,
                    positions=list(snapshot.get("positions") or []),
                    snapshot_id=f"{venue}_poll_{ts}",
                    now_epoch_ms=ts,
                )
                self.stats[f"{venue}_success"] += 1
                results["venues"][venue] = {
                    "status": "ok",
                    "snapshot": snapshot,
                    "reconcile": reconcile,
                }
            except Exception as exc:
                self.runtime.mark_reconcile_failure(
                    venue=venue,
                    error=f"{type(exc).__name__}:{exc}",
                    now_epoch_ms=ts,
                )
                self.stats[f"{venue}_failure"] += 1
                results["venues"][venue] = {
                    "status": "error",
                    "error": f"{type(exc).__name__}:{exc}",
                }

        for venue in ("polymarket", "kalshi"):
            client = self.polymarket_orders_client if venue == "polymarket" else self.kalshi_orders_client
            if client is None:
                continue
            if not force and not self._is_due(venue=venue, now_epoch_ms=ts, source="orders"):
                continue
            self.last_order_attempt_ms[venue] = ts
            venue_result = as_dict(results["venues"].get(venue))
            try:
                snapshot = client.fetch_orders()
                snapshot_is_complete = not bool(as_dict(snapshot).get("truncated_by_max_pages"))
                reconcile = self.runtime.reconcile_orders_snapshot(
                    venue=venue,
                    orders=list(snapshot.get("orders") or []),
                    snapshot_id=f"{venue}_orders_poll_{ts}",
                    snapshot_is_complete=bool(snapshot_is_complete),
                    now_epoch_ms=ts,
                )
                self.stats[f"{venue}_orders_success"] += 1
                venue_result["orders"] = {
                    "status": "ok",
                    "snapshot": snapshot,
                    "reconcile": reconcile,
                }
            except Exception as exc:
                self.stats[f"{venue}_orders_failure"] += 1
                venue_result["orders"] = {
                    "status": "error",
                    "error": f"{type(exc).__name__}:{exc}",
                }
            results["venues"][venue] = venue_result
        return results

    async def run(self, stop_event: asyncio.Event) -> None:
        sleep_s = max(0.05, float(self.config.loop_sleep_seconds))
        while not stop_event.is_set():
            self.run_once()
            await asyncio.sleep(sleep_s)


def capture_account_portfolio_snapshot(
    *,
    polymarket_client: Optional[PolymarketPositionsPollClient],
    polymarket_account_client: Optional[PolymarketAccountPollClient],
    kalshi_client: Optional[KalshiPositionsPollClient],
    now_epoch_ms: Optional[int] = None,
) -> Dict[str, Any]:
    ts = int(now_epoch_ms if now_epoch_ms is not None else now_ms())
    venues: Dict[str, Any] = {}

    polymarket_snapshot: Dict[str, Any] = {}
    polymarket_component_count = 0
    polymarket_ok_count = 0
    if polymarket_client is None:
        polymarket_snapshot["positions"] = {
            "status": "unavailable",
            "reason": "positions_client_not_configured",
        }
    else:
        polymarket_component_count += 1
        try:
            positions = polymarket_client.fetch_positions()
            polymarket_snapshot["positions"] = {
                "status": "ok",
                "data": positions,
            }
            polymarket_ok_count += 1
        except Exception as exc:
            polymarket_snapshot["positions"] = {
                "status": "error",
                "error": f"{type(exc).__name__}:{exc}",
            }

    if polymarket_account_client is None:
        polymarket_snapshot["balance_allowance"] = {
            "status": "unavailable",
            "reason": "account_client_not_configured",
        }
    else:
        polymarket_component_count += 1
        try:
            balance_allowance = polymarket_account_client.fetch_balance_allowance()
            polymarket_snapshot["balance_allowance"] = {
                "status": "ok",
                "data": balance_allowance,
            }
            polymarket_ok_count += 1
        except Exception as exc:
            polymarket_snapshot["balance_allowance"] = {
                "status": "error",
                "error": f"{type(exc).__name__}:{exc}",
            }

    if polymarket_component_count == 0:
        polymarket_snapshot["status"] = "unavailable"
    elif polymarket_ok_count == polymarket_component_count:
        polymarket_snapshot["status"] = "ok"
    elif polymarket_ok_count > 0:
        polymarket_snapshot["status"] = "partial"
    else:
        polymarket_snapshot["status"] = "error"
    venues["polymarket"] = polymarket_snapshot

    if kalshi_client is None:
        venues["kalshi"] = {
            "status": "unavailable",
            "reason": "positions_client_not_configured",
        }
    else:
        try:
            positions = kalshi_client.fetch_positions()
            balance = kalshi_client.fetch_balance()
            venues["kalshi"] = {
                "status": "ok",
                "positions": positions,
                "balance": balance,
            }
        except Exception as exc:
            venues["kalshi"] = {
                "status": "error",
                "error": f"{type(exc).__name__}:{exc}",
            }

    return {
        "at_ms": ts,
        "venues": venues,
    }
