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

    def fetch_positions(self) -> Dict[str, Any]:
        _, payload = self.transport.request_json(
            "GET",
            f"{self.base_url}/positions",
            params={"user": self.user_address},
            allow_status={200},
        )
        rows = list(payload) if isinstance(payload, list) else []

        yes_total = 0.0
        no_total = 0.0
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
            if size is None:
                continue
            if condition and condition != self.condition_id:
                continue
            # Defensive scope filter: only selected pair assets are accepted.
            if asset not in {self.token_yes, self.token_no}:
                continue

            filtered_count += 1
            if outcome == "yes" or asset == self.token_yes:
                yes_total += float(size)
            elif outcome == "no" or asset == self.token_no:
                no_total += float(size)

        positions = [
            {
                "instrument_id": self.token_yes,
                "outcome_side": "yes",
                "net_contracts": float(yes_total),
            },
            {
                "instrument_id": self.token_no,
                "outcome_side": "no",
                "net_contracts": float(no_total),
            },
        ]
        return {
            "venue": "polymarket",
            "positions": positions,
            "raw_count": len(rows),
            "filtered_count": int(filtered_count),
            "pages": 1,
            "cursor_end": None,
        }


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

    def fetch_positions(self) -> Dict[str, Any]:
        path = f"{self.api_prefix}/portfolio/positions"
        limit = max(1, min(1000, int(self.config.kalshi_page_limit)))
        max_pages = max(1, int(self.config.kalshi_max_pages))
        cursor: Optional[str] = None
        pages = 0
        row_count = 0
        net_position_total = 0.0
        total_fees_paid_usd: Optional[float] = None
        total_realized_pnl_usd: Optional[float] = None

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
            market_positions = body.get("market_positions")
            rows = list(market_positions) if isinstance(market_positions, list) else []
            for raw in rows:
                item = as_dict(raw)
                if str(item.get("ticker") or "").strip() != self.market_ticker:
                    continue
                row_count += 1
                value = as_float(item.get("position_fp"))
                if value is None:
                    value = as_float(item.get("position"))
                net_position_total += float(value or 0.0)

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

        yes_position = net_position_total if net_position_total > 0 else 0.0
        no_position = abs(net_position_total) if net_position_total < 0 else 0.0
        positions = [
            {
                "instrument_id": self.market_ticker,
                "outcome_side": "yes",
                "net_contracts": float(yes_position),
                "fees": total_fees_paid_usd,
                "realized_pnl": total_realized_pnl_usd,
            },
            {
                "instrument_id": self.market_ticker,
                "outcome_side": "no",
                "net_contracts": float(no_position),
                "fees": total_fees_paid_usd,
                "realized_pnl": total_realized_pnl_usd,
            },
        ]
        return {
            "venue": "kalshi",
            "positions": positions,
            "raw_count": int(row_count),
            "filtered_count": int(row_count),
            "pages": int(pages),
            "cursor_end": cursor,
            "truncated_by_max_pages": bool(cursor is not None),
        }

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


@dataclass(frozen=True)
class PositionReconcileLoopConfig:
    polymarket_poll_seconds: float = 10.0
    kalshi_poll_seconds: float = 20.0
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
        config: Optional[PositionReconcileLoopConfig] = None,
    ) -> None:
        self.runtime = runtime
        self.polymarket_client = polymarket_client
        self.kalshi_client = kalshi_client
        self.config = config or PositionReconcileLoopConfig()
        self.last_attempt_ms: Dict[str, Optional[int]] = {"polymarket": None, "kalshi": None}
        self.stats: Dict[str, int] = {
            "polymarket_success": 0,
            "polymarket_failure": 0,
            "kalshi_success": 0,
            "kalshi_failure": 0,
            "poll_iterations": 0,
        }

    def _is_due(self, *, venue: str, now_epoch_ms: int) -> bool:
        last = self.last_attempt_ms.get(venue)
        interval_s = (
            float(self.config.polymarket_poll_seconds)
            if venue == "polymarket"
            else float(self.config.kalshi_poll_seconds)
        )
        interval_ms = int(max(0.2, interval_s) * 1000)
        if last is None:
            return True
        return int(now_epoch_ms - int(last)) >= interval_ms

    def run_once(self, *, now_epoch_ms: Optional[int] = None, force: bool = False) -> Dict[str, Any]:
        ts = int(now_epoch_ms if now_epoch_ms is not None else now_ms())
        results: Dict[str, Any] = {"at_ms": ts, "venues": {}}
        self.stats["poll_iterations"] += 1

        for venue in ("polymarket", "kalshi"):
            client = self.polymarket_client if venue == "polymarket" else self.kalshi_client
            if client is None:
                continue
            if not force and not self._is_due(venue=venue, now_epoch_ms=ts):
                continue
            self.last_attempt_ms[venue] = ts
            try:
                snapshot = client.fetch_positions()
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
