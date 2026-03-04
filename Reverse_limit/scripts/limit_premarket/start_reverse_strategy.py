#!/usr/bin/env python3
"""
Reverse premarket strategy (Polymarket-only).

Behavior:
- Monitor the current BTC 15m market.
- During the final observation window, check last-trade price on interval.
- If current market outcome is determined, place exactly one reverse-side
  GTC limit order in the next market.
- If threshold determination fails, try official market resolution.
- If official resolution is unavailable, try pre-close last trade (1s before end by default).
- If pre-close resolution is unavailable, fallback to RTDS start-vs-end
  comparison only when the strategy captured that market's start price.
"""

from __future__ import annotations

import argparse
import asyncio
from collections import deque
import json
import os
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.common.api_transport import ApiTransport
from scripts.common.buy_execution import build_client_order_id, build_polymarket_api_buy_client_from_env
from scripts.common.engine_setup import build_buy_execution_transport
from scripts.common.polymarket_resolution import fetch_closed_positions_resolution_map
from scripts.common.position_polling import PolymarketAccountPollClient
from scripts.common.run_config import load_buy_execution_runtime_config_from_run_config
from scripts.common.utils import as_dict, as_float, as_non_empty_text, now_ms, utc_now_iso
from scripts.common.ws_normalization import (
    normalize_polymarket_market_resolution_event,
    normalize_polymarket_user_event,
)
from scripts.common.ws_transport import JsonlWriter

try:
    import websockets
except Exception:
    websockets = None


POLYMARKET_GAMMA_API = "https://gamma-api.polymarket.com"
POLYMARKET_RTDS_WS_URL = "wss://ws-live-data.polymarket.com"
POLYMARKET_RTDS_WS_ORIGIN = "https://polymarket.com"
POLYMARKET_USER_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/user"
WINDOW_SECONDS = 900
WINDOW_BOUNDARY_MINUTES = {0, 15, 30, 45}


def _to_float(value: Any, *, default: float, min_value: float, max_value: Optional[float] = None) -> float:
    try:
        parsed = float(value)
    except Exception:
        parsed = float(default)
    if parsed < float(min_value):
        parsed = float(min_value)
    if max_value is not None and parsed > float(max_value):
        parsed = float(max_value)
    return float(parsed)


def _to_int(value: Any, *, default: int, min_value: int) -> int:
    try:
        parsed = int(float(value))
    except Exception:
        parsed = int(default)
    if parsed < int(min_value):
        parsed = int(min_value)
    return int(parsed)


def _to_bool(value: Any, *, default: bool) -> bool:
    if value is None:
        return bool(default)
    if isinstance(value, bool):
        return bool(value)
    if isinstance(value, (int, float)):
        return bool(value != 0)
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return bool(default)


def _parse_iso_datetime(value: Any) -> Optional[datetime]:
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
    return dt.astimezone(timezone.utc)


def _parse_json_list(value: Any) -> List[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except Exception:
            return []
        if isinstance(parsed, list):
            return parsed
    return []


def _normalize_share_price(value: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    price = float(value)
    if price > 1.0 and price <= 100.0:
        price = price / 100.0
    if price < 0.0 or price > 1.0:
        return None
    return float(price)


def _normalize_outcome_label(label: Any) -> Optional[str]:
    text = str(label or "").strip().lower()
    if text in {"yes", "up"}:
        return "yes"
    if text in {"no", "down"}:
        return "no"
    return None


def _window_start_epoch_s(now_epoch_ms: int) -> int:
    now_s = int(now_epoch_ms // 1000)
    return int(now_s - (now_s % WINDOW_SECONDS))


def _slug_from_window_start(window_start_s: int) -> str:
    return f"btc-updown-15m-{int(window_start_s)}"


def _resolve_polymarket_user_address_from_env() -> Optional[str]:
    for key in ("POLYMARKET_FUNDER", "POLYMARKET_ADDRESS", "POLYMARKET_SIGNER"):
        value = str(os.getenv(key, "") or "").strip()
        if value:
            return value.lower()
    return None


@dataclass(frozen=True)
class ReverseStrategyConfig:
    enabled: bool = True
    poll_interval_seconds: float = 10.0
    loop_sleep_seconds: float = 1.0
    observation_window_seconds: int = 120
    yes_threshold: float = 0.97
    no_threshold: float = 0.03
    order_limit_price: float = 0.49
    order_size: int = 10
    dry_run: bool = False
    output_dir: str = "data/limit_market"
    state_file: str = "data/limit_market/reverse_strategy_state.json"
    start_price_capture_grace_seconds: int = 20
    resolution_wait_after_end_seconds: int = 45
    rtds_symbol: str = "BTC/USD"
    rtds_max_age_ms: int = 15_000
    rtds_resolution_epsilon_usd: float = 0.0
    preclose_last_trade_seconds_before_end: int = 1
    preclose_yes_upper_threshold: float = 0.70
    preclose_yes_lower_threshold: float = 0.30
    ws_resolution_fallback_after_close_seconds: int = 600
    previous_window_resolution_grace_seconds: int = 180
    log_events: bool = True
    log_decision_polls: bool = True
    log_order_attempts: bool = True


@dataclass(frozen=True)
class MarketWindow:
    slug: str
    market_id: str
    condition_id: str
    window_start_s: int
    window_end_s: int
    token_yes: str
    token_no: str
    yes_label: Optional[str]
    no_label: Optional[str]
    last_trade_yes_price: Optional[float]
    official_resolution: Optional[str]
    raw_market: Dict[str, Any]


def _load_reverse_strategy_config(*, config_path: Path) -> ReverseStrategyConfig:
    payload = json.loads(config_path.read_text(encoding="utf-8"))
    section = as_dict(payload.get("reverse_strategy"))
    return ReverseStrategyConfig(
        enabled=_to_bool(section.get("enabled"), default=True),
        poll_interval_seconds=_to_float(section.get("poll_interval_seconds"), default=10.0, min_value=1.0),
        loop_sleep_seconds=_to_float(section.get("loop_sleep_seconds"), default=1.0, min_value=0.1),
        observation_window_seconds=_to_int(section.get("observation_window_seconds"), default=120, min_value=10),
        yes_threshold=_to_float(section.get("yes_threshold"), default=0.97, min_value=0.5, max_value=1.0),
        no_threshold=_to_float(section.get("no_threshold"), default=0.03, min_value=0.0, max_value=0.5),
        order_limit_price=_to_float(section.get("order_limit_price"), default=0.49, min_value=0.01, max_value=0.99),
        order_size=_to_int(section.get("order_size"), default=10, min_value=1),
        dry_run=_to_bool(section.get("dry_run"), default=False),
        output_dir=str(section.get("output_dir") or "data/limit_market"),
        state_file=str(section.get("state_file") or "data/limit_market/reverse_strategy_state.json"),
        start_price_capture_grace_seconds=_to_int(
            section.get("start_price_capture_grace_seconds"),
            default=20,
            min_value=1,
        ),
        resolution_wait_after_end_seconds=_to_int(
            section.get("resolution_wait_after_end_seconds"),
            default=45,
            min_value=0,
        ),
        rtds_symbol=str(section.get("rtds_symbol") or "BTC/USD"),
        rtds_max_age_ms=_to_int(section.get("rtds_max_age_ms"), default=15_000, min_value=1_000),
        rtds_resolution_epsilon_usd=_to_float(
            section.get("rtds_resolution_epsilon_usd"),
            default=0.0,
            min_value=0.0,
        ),
        preclose_last_trade_seconds_before_end=_to_int(
            section.get("preclose_last_trade_seconds_before_end"),
            default=1,
            min_value=0,
        ),
        preclose_yes_upper_threshold=_to_float(
            section.get("preclose_yes_upper_threshold"),
            default=0.70,
            min_value=0.5,
            max_value=1.0,
        ),
        preclose_yes_lower_threshold=_to_float(
            section.get("preclose_yes_lower_threshold"),
            default=0.30,
            min_value=0.0,
            max_value=0.5,
        ),
        ws_resolution_fallback_after_close_seconds=_to_int(
            section.get("ws_resolution_fallback_after_close_seconds"),
            default=600,
            min_value=0,
        ),
        previous_window_resolution_grace_seconds=_to_int(
            section.get("previous_window_resolution_grace_seconds"),
            default=180,
            min_value=0,
        ),
        log_events=_to_bool(section.get("log_events"), default=True),
        log_decision_polls=_to_bool(section.get("log_decision_polls"), default=True),
        log_order_attempts=_to_bool(section.get("log_order_attempts"), default=True),
    )


class ReverseStateStore:
    def __init__(self, *, path: Path) -> None:
        self.path = path
        self.payload: Dict[str, Any] = {}

    @staticmethod
    def _default_payload() -> Dict[str, Any]:
        return {
            "version": 1,
            "updated_at": utc_now_iso(),
            "start_prices_by_window_start": {},
            "next_market_attempts": {},
        }

    def load(self) -> None:
        if not self.path.exists():
            self.payload = self._default_payload()
            return
        try:
            loaded = json.loads(self.path.read_text(encoding="utf-8"))
        except Exception:
            loaded = {}
        parsed = as_dict(loaded)
        self.payload = self._default_payload()
        self.payload["start_prices_by_window_start"] = as_dict(parsed.get("start_prices_by_window_start"))
        self.payload["next_market_attempts"] = as_dict(parsed.get("next_market_attempts"))
        self.payload["updated_at"] = str(parsed.get("updated_at") or utc_now_iso())

    def save(self) -> None:
        self.payload["updated_at"] = utc_now_iso()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(self.payload, indent=2), encoding="utf-8")

    def get_start_price(self, *, window_start_s: int) -> Optional[Dict[str, Any]]:
        rows = as_dict(self.payload.get("start_prices_by_window_start"))
        return as_dict(rows.get(str(int(window_start_s)))) or None

    def set_start_price(self, *, window_start_s: int, payload: Dict[str, Any]) -> None:
        rows = as_dict(self.payload.get("start_prices_by_window_start"))
        rows[str(int(window_start_s))] = dict(payload)
        self.payload["start_prices_by_window_start"] = rows
        self.save()

    def is_next_market_attempted(self, *, market_key: str) -> bool:
        attempts = as_dict(self.payload.get("next_market_attempts"))
        return str(market_key or "").strip() in attempts

    def mark_next_market_attempt(self, *, market_key: str, payload: Dict[str, Any]) -> None:
        key = str(market_key or "").strip()
        if not key:
            return
        attempts = as_dict(self.payload.get("next_market_attempts"))
        attempts[key] = dict(payload)
        self.payload["next_market_attempts"] = attempts
        self.save()


class ReverseStrategyLogger:
    def __init__(self, *, output_dir: Path, run_id: str, log_events: bool) -> None:
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.run_id = run_id
        self.events_path = self.output_dir / f"reverse_strategy_events__{run_id}.jsonl"
        self.orders_path = self.output_dir / f"reverse_strategy_orders__{run_id}.jsonl"
        self.summary_path = self.output_dir / f"reverse_strategy_summary__{run_id}.json"
        self.events_writer = JsonlWriter(self.events_path) if bool(log_events) else None
        self.orders_writer = JsonlWriter(self.orders_path)

    def write_event(self, *, kind: str, payload: Dict[str, Any]) -> None:
        if self.events_writer is None:
            return
        self.events_writer.write(
            {
                "ts": utc_now_iso(),
                "recv_ms": now_ms(),
                "kind": str(kind),
                **dict(payload or {}),
            }
        )

    def write_order(self, *, kind: str, payload: Dict[str, Any]) -> None:
        self.orders_writer.write(
            {
                "ts": utc_now_iso(),
                "recv_ms": now_ms(),
                "kind": str(kind),
                **dict(payload or {}),
            }
        )

    def close(self) -> None:
        if self.events_writer is not None:
            try:
                self.events_writer.close()
            except Exception:
                pass
        try:
            self.orders_writer.close()
        except Exception:
            pass


@dataclass
class ReverseMarketLogRow:
    run_id: str
    window_start_s: int
    market_start: str
    market_slug: Optional[str] = None
    market_id: Optional[str] = None
    condition_id: Optional[str] = None
    window_end_s: int = 0
    previous_condition_id: Optional[str] = None
    previous_market_resolve: Optional[str] = None
    order_side: Optional[str] = None
    order_price: Optional[float] = None
    order_size: Optional[float] = None
    orders_filled_when_first_seconds: Optional[float] = None
    orders_filled_when_full_seconds: Optional[float] = None
    orders_unfilled: Optional[float] = None
    result_usd: Optional[float] = None
    next_order_decided_by: Optional[str] = None
    account_balance: Optional[float] = None
    account_balance_method: Optional[str] = None
    own_market_resolution: Optional[str] = None
    order_attempted_at_ms: Optional[int] = None
    first_fill_at_ms: Optional[int] = None
    full_fill_at_ms: Optional[int] = None
    fill_total_shares: float = 0.0
    fill_cost_usd: float = 0.0
    close_checkpoint_done: bool = False
    finalized: bool = False
    written: bool = False


class ReverseStrategyPersistentMarketLog:
    def __init__(
        self,
        *,
        path: Path,
        run_id: str,
        ws_resolution_fallback_after_close_seconds: int = 600,
    ) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.writer = JsonlWriter(self.path)
        self.run_id = str(run_id)
        self.rows_by_window_start: Dict[int, ReverseMarketLogRow] = {}
        self.window_start_by_condition: Dict[str, int] = {}
        self.resolution_by_condition: Dict[str, str] = {}
        self.last_closed_positions_refresh_ms: int = 0
        self.closed_positions_refresh_interval_ms: int = 15_000
        self.ws_resolution_fallback_after_close_seconds = max(0, int(ws_resolution_fallback_after_close_seconds))
        self.stats: Dict[str, int] = {
            "rows_final": 0,
            "rows_partial": 0,
            "account_fetch_errors": 0,
            "resolution_fetch_errors": 0,
        }

    @staticmethod
    def _iso_from_epoch_s(epoch_s: int) -> str:
        return datetime.fromtimestamp(float(epoch_s), tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    @staticmethod
    def _normalize_resolution(value: Any) -> Optional[str]:
        raw = str(value or "").strip().lower()
        if raw in {"yes", "up"}:
            return "yes"
        if raw in {"no", "down"}:
            return "no"
        return None

    def _ensure_row(self, *, window_start_s: int) -> ReverseMarketLogRow:
        key = int(window_start_s)
        row = self.rows_by_window_start.get(key)
        if row is not None:
            return row
        row = ReverseMarketLogRow(
            run_id=self.run_id,
            window_start_s=int(key),
            market_start=self._iso_from_epoch_s(int(key)),
            window_end_s=int(key + WINDOW_SECONDS),
        )
        self.rows_by_window_start[key] = row
        return row

    def upsert_market_context(
        self,
        *,
        window_start_s: int,
        current_market: Optional[MarketWindow],
        previous_market: Optional[MarketWindow],
    ) -> None:
        row = self._ensure_row(window_start_s=int(window_start_s))
        if current_market is not None:
            row.market_slug = current_market.slug
            row.market_id = current_market.market_id
            row.condition_id = current_market.condition_id
            row.window_end_s = int(current_market.window_end_s)
            self.window_start_by_condition[str(current_market.condition_id).lower()] = int(window_start_s)
            resolved_current = self._normalize_resolution(self.resolution_by_condition.get(str(current_market.condition_id).lower()))
            if resolved_current in {"yes", "no"}:
                row.own_market_resolution = resolved_current
        if previous_market is not None:
            row.previous_condition_id = previous_market.condition_id
            resolved_prev = self._normalize_resolution(self.resolution_by_condition.get(str(previous_market.condition_id).lower()))
            if resolved_prev in {"yes", "no"}:
                row.previous_market_resolve = resolved_prev

    def record_order_attempt(
        self,
        *,
        target_market: MarketWindow,
        reverse_side: str,
        limit_price: float,
        size: float,
        decision_reason: Optional[str],
        attempted_at_ms: int,
    ) -> None:
        row = self._ensure_row(window_start_s=int(target_market.window_start_s))
        row.market_slug = target_market.slug
        row.market_id = target_market.market_id
        row.condition_id = target_market.condition_id
        row.window_end_s = int(target_market.window_end_s)
        self.window_start_by_condition[str(target_market.condition_id).lower()] = int(target_market.window_start_s)
        side_norm = str(reverse_side or "").strip().lower()
        if side_norm == "yes":
            row.order_side = "Up"
        elif side_norm == "no":
            row.order_side = "Down"
        row.order_price = float(limit_price)
        row.order_size = float(size)
        row.next_order_decided_by = str(decision_reason or "").strip() or None
        row.order_attempted_at_ms = int(attempted_at_ms)
        if row.first_fill_at_ms is not None and row.orders_filled_when_first_seconds is None:
            row.orders_filled_when_first_seconds = (float(row.first_fill_at_ms) - (float(row.window_start_s) * 1000.0)) / 1000.0
        if row.order_size is not None and row.full_fill_at_ms is None:
            if float(row.fill_total_shares) >= (float(row.order_size) - 1e-9):
                row.full_fill_at_ms = int(row.first_fill_at_ms or attempted_at_ms)
                row.orders_filled_when_full_seconds = (float(row.full_fill_at_ms) - (float(row.window_start_s) * 1000.0)) / 1000.0

    def record_fill_event(self, *, fill_event: Dict[str, Any]) -> None:
        condition_id = str(fill_event.get("market") or "").strip().lower()
        if not condition_id:
            return
        window_start_s = self.window_start_by_condition.get(condition_id)
        if window_start_s is None:
            return
        row = self._ensure_row(window_start_s=int(window_start_s))
        delta = as_float(fill_event.get("fill_delta"))
        if delta is None or delta <= 0.0:
            return
        recv_ms = int(as_float(fill_event.get("recv_ms")) or now_ms())
        fill_price = as_float(fill_event.get("fill_price"))
        if fill_price is None:
            fill_price = as_float(row.order_price)
        row.fill_total_shares = float(row.fill_total_shares + float(delta))
        if fill_price is not None:
            row.fill_cost_usd = float(row.fill_cost_usd + (float(delta) * float(fill_price)))
        if row.first_fill_at_ms is None:
            row.first_fill_at_ms = int(recv_ms)
            row.orders_filled_when_first_seconds = (float(recv_ms) - (float(row.window_start_s) * 1000.0)) / 1000.0
        if row.order_size is not None and row.full_fill_at_ms is None:
            if float(row.fill_total_shares) >= (float(row.order_size) - 1e-9):
                row.full_fill_at_ms = int(recv_ms)
                row.orders_filled_when_full_seconds = (float(recv_ms) - (float(row.window_start_s) * 1000.0)) / 1000.0

    def apply_resolution(self, *, condition_id: str, outcome: Any, overwrite: bool = True) -> None:
        condition = str(condition_id or "").strip().lower()
        normalized = self._normalize_resolution(outcome)
        if not condition or normalized not in {"yes", "no"}:
            return
        existing = self._normalize_resolution(self.resolution_by_condition.get(condition))
        if existing in {"yes", "no"} and not bool(overwrite):
            return
        self.resolution_by_condition[condition] = normalized
        window_start_s = self.window_start_by_condition.get(condition)
        if window_start_s is not None:
            row = self._ensure_row(window_start_s=int(window_start_s))
            current_row_resolution = self._normalize_resolution(row.own_market_resolution)
            if current_row_resolution not in {"yes", "no"} or bool(overwrite):
                row.own_market_resolution = normalized
        for row in self.rows_by_window_start.values():
            if str(row.previous_condition_id or "").strip().lower() == condition:
                current_prev_resolution = self._normalize_resolution(row.previous_market_resolve)
                if current_prev_resolution not in {"yes", "no"} or bool(overwrite):
                    row.previous_market_resolve = normalized

    def _refresh_resolutions_from_ws(self, *, last_trade_ws: PolymarketLastTradeAdapter) -> None:
        conditions: List[str] = []
        for row in self.rows_by_window_start.values():
            own = str(row.condition_id or "").strip()
            prev = str(row.previous_condition_id or "").strip()
            if own and row.own_market_resolution is None:
                conditions.append(own)
            if prev and row.previous_market_resolve is None:
                conditions.append(prev)
        for condition_id in conditions:
            resolution_row = last_trade_ws.get_resolution_for_condition(condition_id=condition_id)
            if not resolution_row:
                continue
            self.apply_resolution(
                condition_id=condition_id,
                outcome=resolution_row.get("outcome"),
            )

    def _refresh_resolutions_from_closed_positions(
        self,
        *,
        now_epoch_ms: int,
        api_transport: ApiTransport,
        user_address: Optional[str],
    ) -> None:
        if not user_address:
            return
        eligible_conditions: set[str] = set()
        for row in self.rows_by_window_start.values():
            own_condition = str(row.condition_id or "").strip().lower()
            if own_condition and row.own_market_resolution is None and row.order_size is not None:
                own_eligible_after_ms = int(
                    (int(row.window_end_s) + int(self.ws_resolution_fallback_after_close_seconds)) * 1000
                )
                if int(now_epoch_ms) >= own_eligible_after_ms:
                    eligible_conditions.add(own_condition)

            prev_condition = str(row.previous_condition_id or "").strip().lower()
            if prev_condition and row.previous_market_resolve is None:
                # Previous market closes when this market opens.
                prev_eligible_after_ms = int(
                    (int(row.window_start_s) + int(self.ws_resolution_fallback_after_close_seconds)) * 1000
                )
                if int(now_epoch_ms) >= prev_eligible_after_ms:
                    eligible_conditions.add(prev_condition)

        if not eligible_conditions:
            return
        if now_epoch_ms < (self.last_closed_positions_refresh_ms + self.closed_positions_refresh_interval_ms):
            return
        self.last_closed_positions_refresh_ms = int(now_epoch_ms)
        try:
            resolution_map = fetch_closed_positions_resolution_map(
                transport=api_transport,
                user_address=str(user_address),
            )
            for condition_id, row in resolution_map.items():
                normalized_condition = str(condition_id or "").strip().lower()
                if normalized_condition not in eligible_conditions:
                    continue
                self.apply_resolution(
                    condition_id=str(condition_id),
                    outcome=as_dict(row).get("outcome"),
                )
        except Exception:
            self.stats["resolution_fetch_errors"] += 1

    @staticmethod
    def _extract_cash_balance_usd(account_snapshot: Dict[str, Any]) -> Optional[float]:
        allowance_root = as_dict(account_snapshot.get("balance_allowance"))
        allowance = as_dict(allowance_root.get("balance_allowance")) if "balance_allowance" in allowance_root else allowance_root
        raw_balance = as_float(allowance.get("balance"))
        if raw_balance is None:
            return None
        return float(raw_balance / 1_000_000.0)

    def _set_close_checkpoint(
        self,
        *,
        row: ReverseMarketLogRow,
        account_client: Optional[PolymarketAccountPollClient],
    ) -> None:
        if row.close_checkpoint_done:
            return
        row.close_checkpoint_done = True
        if row.order_size is None:
            row.orders_unfilled = None
        else:
            filled = min(float(row.fill_total_shares), float(row.order_size))
            row.orders_unfilled = float(max(0.0, float(row.order_size - filled)))
        if account_client is None:
            row.account_balance = None
            row.account_balance_method = "unavailable"
            return
        try:
            account_snapshot = account_client.fetch_balance_allowance()
            cash_usd = self._extract_cash_balance_usd(as_dict(account_snapshot))
            if cash_usd is None:
                row.account_balance = None
                row.account_balance_method = "unavailable"
                self.stats["account_fetch_errors"] += 1
            else:
                row.account_balance = float(cash_usd)
                row.account_balance_method = "cash_only"
        except Exception:
            row.account_balance = None
            row.account_balance_method = "unavailable"
            self.stats["account_fetch_errors"] += 1

    @staticmethod
    def _is_order_winner(*, order_side: Optional[str], outcome: Optional[str]) -> Optional[bool]:
        side = str(order_side or "").strip().lower()
        resolved = str(outcome or "").strip().lower()
        if side not in {"up", "down"} or resolved not in {"yes", "no"}:
            return None
        if side == "up":
            return bool(resolved == "yes")
        return bool(resolved == "no")

    def _update_result(self, *, row: ReverseMarketLogRow) -> None:
        if row.order_size is None:
            row.result_usd = None
            return
        if row.own_market_resolution not in {"yes", "no"}:
            return
        winner = self._is_order_winner(order_side=row.order_side, outcome=row.own_market_resolution)
        if winner is None:
            return
        filled = min(float(row.fill_total_shares), float(row.order_size))
        payout = float(filled) if bool(winner) else 0.0
        row.result_usd = float(payout - float(row.fill_cost_usd))

    def _serialize_row(self, *, row: ReverseMarketLogRow, status: str) -> Dict[str, Any]:
        return {
            "run_id": row.run_id,
            "market_start": row.market_start,
            "previous_market_resolve": row.previous_market_resolve,
            "order_side": row.order_side,
            "order_price": row.order_price,
            "order_size": row.order_size,
            "orders_filled_when_first_seconds": row.orders_filled_when_first_seconds,
            "orders_filled_when_full_seconds": row.orders_filled_when_full_seconds,
            "orders_unfilled": row.orders_unfilled,
            "result_usd": row.result_usd,
            "next_order_decided_by": row.next_order_decided_by,
            "account_balance": row.account_balance,
            "market_slug": row.market_slug,
            "market_id": row.market_id,
            "condition_id": row.condition_id,
            "account_balance_method": row.account_balance_method,
            "finalized_at": utc_now_iso(),
            "finalization_status": str(status),
        }

    def _can_finalize(self, *, row: ReverseMarketLogRow) -> bool:
        if row.written:
            return False
        if not row.close_checkpoint_done:
            return False
        if row.previous_condition_id and row.previous_market_resolve is None:
            return False
        if row.account_balance_method is None:
            return False
        if row.order_size is not None and row.result_usd is None:
            return False
        return True

    def _write_final_row(self, *, row: ReverseMarketLogRow, status: str) -> None:
        if row.written:
            return
        self.writer.write(self._serialize_row(row=row, status=status))
        row.written = True
        row.finalized = True
        if status == "final":
            self.stats["rows_final"] += 1
        else:
            self.stats["rows_partial"] += 1

    def on_periodic_tick(
        self,
        *,
        now_epoch_ms: int,
        api_transport: ApiTransport,
        user_address: Optional[str],
        account_client: Optional[PolymarketAccountPollClient],
        last_trade_ws: PolymarketLastTradeAdapter,
    ) -> None:
        self._refresh_resolutions_from_ws(last_trade_ws=last_trade_ws)
        self._refresh_resolutions_from_closed_positions(
            now_epoch_ms=int(now_epoch_ms),
            api_transport=api_transport,
            user_address=user_address,
        )
        for row in self.rows_by_window_start.values():
            close_checkpoint_ms = int((int(row.window_end_s) * 1000) + 10_000)
            if not row.close_checkpoint_done and now_epoch_ms >= close_checkpoint_ms:
                self._set_close_checkpoint(row=row, account_client=account_client)
            self._update_result(row=row)
            if self._can_finalize(row=row):
                self._write_final_row(row=row, status="final")

    def flush_shutdown_partial(self) -> None:
        for row in self.rows_by_window_start.values():
            if row.written:
                continue
            self._write_final_row(row=row, status="partial_on_shutdown")

    def close(self) -> None:
        try:
            self.writer.close()
        except Exception:
            pass


class PolymarketLastTradeAdapter:
    def __init__(
        self,
        *,
        ws_url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market",
        recv_timeout_seconds: float = 2.0,
        reconnect_base_seconds: float = 1.0,
        reconnect_max_seconds: float = 30.0,
    ) -> None:
        self.ws_url = str(ws_url)
        self.recv_timeout_seconds = max(0.5, float(recv_timeout_seconds))
        self.reconnect_base_seconds = max(0.2, float(reconnect_base_seconds))
        self.reconnect_max_seconds = max(self.reconnect_base_seconds, float(reconnect_max_seconds))

        self._lock = threading.Lock()
        self._started = False
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._desired_assets: tuple[str, ...] = ()
        self._last_trade_by_asset: Dict[str, Dict[str, Any]] = {}
        self._trade_history_by_asset: Dict[str, deque[Dict[str, Any]]] = {}
        self._trade_history_max_per_asset = 2_000
        self._resolution_by_condition: Dict[str, Dict[str, Any]] = {}
        self._message_count = 0
        self._resolution_message_count = 0
        self._last_error: Optional[str] = None
        self._active_loop: Optional[asyncio.AbstractEventLoop] = None
        self._active_ws: Optional[Any] = None

    @staticmethod
    def _subscription_payload(*, asset_ids: tuple[str, ...]) -> Dict[str, Any]:
        assets = [str(asset_id) for asset_id in asset_ids]
        return {
            "type": "MARKET",
            "asset_ids": assets,
            "assets_ids": assets,
            "custom_feature_enabled": True,
        }

    @staticmethod
    def _iter_message_items(parsed: Any) -> List[Dict[str, Any]]:
        if isinstance(parsed, dict):
            return [parsed]
        if isinstance(parsed, list):
            return [item for item in parsed if isinstance(item, dict)]
        return []

    def _extract_last_trade_event(self, item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        event_type = str(item.get("event_type") or "").strip().lower()
        if event_type != "last_trade_price":
            return None
        asset_id = str(item.get("asset_id") or "").strip()
        if not asset_id:
            return None
        trade_price = _normalize_share_price(as_float(item.get("price")))
        if trade_price is None:
            return None
        source_timestamp_ms = int(as_float(item.get("timestamp")) or 0)
        if source_timestamp_ms <= 0:
            source_timestamp_ms = int(now_ms())
        trade_size = as_float(item.get("size"))
        trade_side = str(item.get("side") or "").strip().lower() or None
        return {
            "asset_id": asset_id,
            "trade_price": float(trade_price),
            "trade_size": None if trade_size is None else float(trade_size),
            "trade_side": trade_side,
            "source_timestamp_ms": int(source_timestamp_ms),
        }

    def _apply_event(self, event: Dict[str, Any]) -> None:
        asset_id = str(event.get("asset_id") or "").strip()
        if not asset_id:
            return
        with self._lock:
            self._last_trade_by_asset[asset_id] = dict(event)
            history = self._trade_history_by_asset.get(asset_id)
            if history is None:
                history = deque(maxlen=int(self._trade_history_max_per_asset))
                self._trade_history_by_asset[asset_id] = history
            history.append(dict(event))
            self._message_count += 1
            self._last_error = None

    def set_assets(self, *, token_yes: str, token_no: str) -> None:
        assets = tuple(
            asset
            for asset in (str(token_yes or "").strip(), str(token_no or "").strip())
            if asset
        )
        with self._lock:
            self._desired_assets = assets

    def get_last_trade_for_asset(self, *, asset_id: str) -> Optional[Dict[str, Any]]:
        key = str(asset_id or "").strip()
        if not key:
            return None
        with self._lock:
            row = self._last_trade_by_asset.get(key)
            return dict(row) if isinstance(row, dict) else None

    def get_last_trade_for_asset_asof(self, *, asset_id: str, at_or_before_ms: int) -> Optional[Dict[str, Any]]:
        key = str(asset_id or "").strip()
        cutoff_ms = int(at_or_before_ms)
        if not key:
            return None
        with self._lock:
            history = self._trade_history_by_asset.get(key)
            if history is not None:
                for row in reversed(history):
                    source_ms = int(as_float(as_dict(row).get("source_timestamp_ms")) or 0)
                    if source_ms > 0 and source_ms <= cutoff_ms:
                        return dict(row)
            latest = self._last_trade_by_asset.get(key)
            if isinstance(latest, dict):
                source_ms = int(as_float(as_dict(latest).get("source_timestamp_ms")) or 0)
                if source_ms > 0 and source_ms <= cutoff_ms:
                    return dict(latest)
            return None

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "desired_assets": list(self._desired_assets),
                "message_count": int(self._message_count),
                "resolution_message_count": int(self._resolution_message_count),
                "last_error": self._last_error,
            }

    def get_resolution_for_condition(self, *, condition_id: str) -> Optional[Dict[str, Any]]:
        key = str(condition_id or "").strip().lower()
        if not key:
            return None
        with self._lock:
            row = self._resolution_by_condition.get(key)
            return dict(row) if isinstance(row, dict) else None

    def _apply_resolution_event(self, event: Dict[str, Any]) -> None:
        condition_id = str(event.get("condition_id") or "").strip().lower()
        if not condition_id:
            return
        source_ts_ms = int(as_float(event.get("source_timestamp_ms")) or 0)
        with self._lock:
            prior = as_dict(self._resolution_by_condition.get(condition_id))
            prior_ts = int(as_float(prior.get("source_timestamp_ms")) or 0)
            if prior and prior_ts > source_ts_ms > 0:
                return
            self._resolution_by_condition[condition_id] = dict(event)
            self._resolution_message_count += 1

    async def _worker_loop(self) -> None:
        if websockets is None:
            with self._lock:
                self._last_error = "missing_websocket_dependency"
            return

        attempt = 0
        while not self._stop_event.is_set():
            with self._lock:
                desired_assets = tuple(self._desired_assets)
            if not desired_assets:
                await asyncio.sleep(0.2)
                continue
            try:
                async with websockets.connect(
                    self.ws_url,
                    additional_headers={
                        "Origin": POLYMARKET_RTDS_WS_ORIGIN,
                        "User-Agent": "Arbitrage-Reverse-Strategy/1.0",
                    },
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=10,
                    max_size=8 * 1024 * 1024,
                ) as ws:
                    with self._lock:
                        self._active_loop = asyncio.get_running_loop()
                        self._active_ws = ws
                    attempt = 0
                    await ws.send(json.dumps(self._subscription_payload(asset_ids=desired_assets)))
                    while not self._stop_event.is_set():
                        with self._lock:
                            current_assets = tuple(self._desired_assets)
                        if current_assets != desired_assets:
                            break
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=self.recv_timeout_seconds)
                        except asyncio.TimeoutError:
                            continue
                        recv_ms = now_ms()
                        try:
                            parsed = json.loads(raw)
                        except Exception:
                            continue
                        for item in self._iter_message_items(parsed):
                            event = self._extract_last_trade_event(item)
                            if event is not None:
                                self._apply_event(event)
                            for resolution_event in normalize_polymarket_market_resolution_event(item, recv_ms):
                                self._apply_resolution_event(as_dict(resolution_event))
            except Exception as exc:
                attempt += 1
                with self._lock:
                    self._last_error = str(exc)
                delay = min(self.reconnect_max_seconds, self.reconnect_base_seconds * (2 ** min(attempt - 1, 8)))
                slept = 0.0
                while slept < delay and not self._stop_event.is_set():
                    chunk = min(0.25, delay - slept)
                    await asyncio.sleep(chunk)
                    slept += chunk
            finally:
                with self._lock:
                    self._active_ws = None
                    self._active_loop = None

    def _worker_main(self) -> None:
        try:
            asyncio.run(self._worker_loop())
        except Exception as exc:
            with self._lock:
                self._last_error = str(exc)

    def start(self) -> None:
        with self._lock:
            if self._started:
                return
            self._started = True
        self._thread = threading.Thread(
            target=self._worker_main,
            name="reverse-strategy-last-trade-worker",
            daemon=True,
        )
        self._thread.start()

    async def _close_ws(self, ws: Any) -> None:
        try:
            await ws.close(code=1000, reason="client shutdown")
        except Exception:
            pass

    def close(self) -> None:
        self._stop_event.set()
        active_loop: Optional[asyncio.AbstractEventLoop]
        active_ws: Optional[Any]
        with self._lock:
            active_loop = self._active_loop
            active_ws = self._active_ws
        if active_loop is not None and active_ws is not None:
            try:
                future = asyncio.run_coroutine_threadsafe(self._close_ws(active_ws), active_loop)
                future.result(timeout=2.0)
            except Exception:
                pass
        if self._thread is not None:
            self._thread.join(timeout=2.0)


class PolymarketUserFillAdapter:
    def __init__(
        self,
        *,
        ws_url: str = POLYMARKET_USER_WS_URL,
        recv_timeout_seconds: float = 2.0,
        reconnect_base_seconds: float = 1.0,
        reconnect_max_seconds: float = 30.0,
        max_fill_queue_size: int = 1000,
    ) -> None:
        self.ws_url = str(ws_url)
        self.recv_timeout_seconds = max(0.5, float(recv_timeout_seconds))
        self.reconnect_base_seconds = max(0.2, float(reconnect_base_seconds))
        self.reconnect_max_seconds = max(self.reconnect_base_seconds, float(reconnect_max_seconds))
        self.max_fill_queue_size = max(100, int(max_fill_queue_size))
        self.auth = self._resolve_auth_from_env()

        self._lock = threading.Lock()
        self._started = False
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._desired_markets: tuple[str, ...] = ()
        self._tracked_by_order_id: Dict[str, Dict[str, Any]] = {}
        self._tracked_by_client_id: Dict[str, Dict[str, Any]] = {}
        self._fill_events: deque[Dict[str, Any]] = deque()
        self._recent_event_keys: deque[str] = deque(maxlen=2000)
        self._recent_event_key_set: set[str] = set()

        self._message_count = 0
        self._normalized_event_count = 0
        self._fill_event_count = 0
        self._last_error: Optional[str] = None
        self._active_loop: Optional[asyncio.AbstractEventLoop] = None
        self._active_ws: Optional[Any] = None

    @staticmethod
    def _resolve_auth_from_env() -> Dict[str, str]:
        api_key = str(os.getenv("POLYMARKET_L2_API_KEY", "") or "").strip()
        secret = str(os.getenv("POLYMARKET_L2_API_SECRET", "") or "").strip()
        passphrase = str(os.getenv("POLYMARKET_L2_API_PASSPHRASE", "") or "").strip()
        missing: List[str] = []
        if not api_key:
            missing.append("POLYMARKET_L2_API_KEY")
        if not secret:
            missing.append("POLYMARKET_L2_API_SECRET")
        if not passphrase:
            missing.append("POLYMARKET_L2_API_PASSPHRASE")
        if missing:
            raise RuntimeError(f"Missing Polymarket user-channel auth env vars: {', '.join(missing)}")
        return {
            "apiKey": api_key,
            "secret": secret,
            "passphrase": passphrase,
        }

    @staticmethod
    def _subscription_payload(*, auth: Dict[str, str], markets: tuple[str, ...]) -> Dict[str, Any]:
        return {
            "type": "user",
            "auth": dict(auth),
            "markets": [str(market) for market in markets],
        }

    @staticmethod
    def _iter_message_items(parsed: Any) -> List[Dict[str, Any]]:
        if isinstance(parsed, dict):
            return [parsed]
        if isinstance(parsed, list):
            return [item for item in parsed if isinstance(item, dict)]
        return []

    def _find_record_locked(
        self,
        *,
        order_id: Optional[str],
        client_order_id: Optional[str],
    ) -> Optional[Dict[str, Any]]:
        order_key = as_non_empty_text(order_id)
        if order_key and order_key in self._tracked_by_order_id:
            return self._tracked_by_order_id[order_key]
        client_key = as_non_empty_text(client_order_id)
        if client_key and client_key in self._tracked_by_client_id:
            return self._tracked_by_client_id[client_key]
        return None

    def _link_record_locked(
        self,
        *,
        record: Dict[str, Any],
        order_id: Optional[str],
        client_order_id: Optional[str],
    ) -> None:
        order_key = as_non_empty_text(order_id)
        if order_key:
            record["order_id"] = str(order_key)
            self._tracked_by_order_id[str(order_key)] = record
        client_key = as_non_empty_text(client_order_id)
        if client_key:
            record["client_order_id"] = str(client_key)
            self._tracked_by_client_id[str(client_key)] = record

    def _remember_event_key_locked(self, *, event_key: str) -> bool:
        key = str(event_key or "").strip()
        if not key:
            return False
        if key in self._recent_event_key_set:
            return True
        if len(self._recent_event_keys) == self._recent_event_keys.maxlen:
            removed = self._recent_event_keys.popleft()
            self._recent_event_key_set.discard(removed)
        self._recent_event_keys.append(key)
        self._recent_event_key_set.add(key)
        return False

    def _enqueue_fill_locked(self, *, payload: Dict[str, Any]) -> None:
        self._fill_events.append(dict(payload))
        self._fill_event_count += 1
        while len(self._fill_events) > self.max_fill_queue_size:
            self._fill_events.popleft()

    def _apply_filled_total_locked(
        self,
        *,
        record: Dict[str, Any],
        new_total: float,
        recv_ms: int,
        source: str,
        status: Optional[str],
        fill_price: Optional[float],
    ) -> None:
        requested_size = as_float(record.get("requested_size"))
        prev_total = max(0.0, float(as_float(record.get("filled_size")) or 0.0))
        capped_total = max(0.0, float(new_total))
        if requested_size is not None:
            capped_total = min(capped_total, float(requested_size))
        if capped_total <= (prev_total + 1e-9):
            return
        delta = float(capped_total - prev_total)
        record["filled_size"] = float(capped_total)
        record["updated_at_ms"] = int(recv_ms)
        if status:
            record["last_status"] = str(status)
        self._enqueue_fill_locked(
            payload={
                "source": str(source),
                "recv_ms": int(recv_ms),
                "order_id": as_non_empty_text(record.get("order_id")),
                "client_order_id": as_non_empty_text(record.get("client_order_id")),
                "signal_id": as_non_empty_text(record.get("signal_id")),
                "market": as_non_empty_text(record.get("market")),
                "instrument_id": as_non_empty_text(record.get("instrument_id")),
                "outcome_side": as_non_empty_text(record.get("outcome_side")),
                "order_side": as_non_empty_text(record.get("order_side")),
                "requested_size": None if requested_size is None else float(requested_size),
                "fill_delta": float(delta),
                "filled_total": float(capped_total),
                "fill_price": None if fill_price is None else float(fill_price),
                "status": as_non_empty_text(status) or as_non_empty_text(record.get("last_status")),
            }
        )

    @staticmethod
    def _order_event_key(event: Dict[str, Any]) -> str:
        return (
            "order:"
            f"{as_non_empty_text(event.get('order_id')) or as_non_empty_text(event.get('client_order_id')) or 'na'}:"
            f"{as_non_empty_text(event.get('source_timestamp_ms')) or 'na'}:"
            f"{as_non_empty_text(event.get('size_matched')) or 'na'}:"
            f"{as_non_empty_text(event.get('status')) or 'na'}"
        )

    @staticmethod
    def _trade_event_key(event: Dict[str, Any]) -> str:
        return (
            "trade:"
            f"{as_non_empty_text(event.get('event_id')) or as_non_empty_text(event.get('taker_order_id')) or 'na'}:"
            f"{as_non_empty_text(event.get('source_timestamp_ms')) or 'na'}:"
            f"{as_non_empty_text(event.get('size')) or 'na'}:"
            f"{as_non_empty_text(event.get('price')) or 'na'}:"
            f"{as_non_empty_text(event.get('status')) or 'na'}"
        )

    @staticmethod
    def _extract_maker_order_refs(
        *,
        maker_orders: List[Any],
    ) -> List[Dict[str, Any]]:
        refs: List[Dict[str, Any]] = []
        for row in maker_orders:
            item = as_dict(row)
            order_id = as_non_empty_text(item.get("maker_order_id")) or as_non_empty_text(item.get("order_id")) or as_non_empty_text(item.get("id"))
            if not order_id:
                continue
            size = (
                as_float(item.get("matched_size"))
                or as_float(item.get("size"))
                or as_float(item.get("takingAmount"))
                or as_float(item.get("makingAmount"))
            )
            refs.append(
                {
                    "order_id": str(order_id),
                    "size": None if size is None else float(size),
                }
            )
        return refs

    def _handle_normalized_event(self, *, event: Dict[str, Any], recv_ms: int) -> None:
        kind = str(event.get("kind") or "").strip().lower()
        if kind not in {"polymarket_user_order", "polymarket_user_trade"}:
            return

        with self._lock:
            self._normalized_event_count += 1

            if kind == "polymarket_user_order":
                if self._remember_event_key_locked(event_key=self._order_event_key(event)):
                    return
                order_id = as_non_empty_text(event.get("order_id")) or as_non_empty_text(event.get("event_id"))
                client_order_id = as_non_empty_text(event.get("client_order_id"))
                record = self._find_record_locked(order_id=order_id, client_order_id=client_order_id)
                if record is None:
                    return
                self._link_record_locked(record=record, order_id=order_id, client_order_id=client_order_id)
                market = as_non_empty_text(event.get("market"))
                if market:
                    record["market"] = str(market)
                asset_id = as_non_empty_text(event.get("asset_id"))
                if asset_id:
                    record["instrument_id"] = str(asset_id)
                outcome_side = as_non_empty_text(event.get("outcome_side"))
                if outcome_side:
                    record["outcome_side"] = str(outcome_side).lower()
                order_side = as_non_empty_text(event.get("order_side"))
                if order_side:
                    record["order_side"] = str(order_side).lower()
                requested = as_float(event.get("original_size"))
                if requested is not None:
                    record["requested_size"] = float(requested)
                status = as_non_empty_text(event.get("status"))
                if status:
                    record["last_status"] = str(status).lower()
                size_matched = as_float(event.get("size_matched"))
                if size_matched is None:
                    return
                self._apply_filled_total_locked(
                    record=record,
                    new_total=float(size_matched),
                    recv_ms=recv_ms,
                    source="user_ws_order",
                    status=as_non_empty_text(event.get("status")),
                    fill_price=as_float(event.get("price")),
                )
                return

            # Confirmed trades are treated as fallback fills for tracked orders.
            if not bool(event.get("is_confirmed")):
                return
            if self._remember_event_key_locked(event_key=self._trade_event_key(event)):
                return
            trade_size = as_float(event.get("size"))
            if trade_size is None or trade_size <= 0:
                return
            status = as_non_empty_text(event.get("status"))
            taker_order_id = as_non_empty_text(event.get("taker_order_id"))
            taker_record = self._find_record_locked(order_id=taker_order_id, client_order_id=None)
            if taker_record is not None:
                self._link_record_locked(record=taker_record, order_id=taker_order_id, client_order_id=None)
                prev = float(as_float(taker_record.get("filled_size")) or 0.0)
                self._apply_filled_total_locked(
                    record=taker_record,
                    new_total=float(prev + float(trade_size)),
                    recv_ms=recv_ms,
                    source="user_ws_confirmed_trade_taker",
                    status=status,
                    fill_price=as_float(event.get("price")),
                )
                return

            maker_refs = self._extract_maker_order_refs(maker_orders=list(event.get("maker_orders") or []))
            if not maker_refs:
                return

            for ref in maker_refs:
                order_id = as_non_empty_text(ref.get("order_id"))
                if not order_id:
                    continue
                record = self._find_record_locked(order_id=order_id, client_order_id=None)
                if record is None:
                    continue
                fill_size = as_float(ref.get("size"))
                if fill_size is None or fill_size <= 0:
                    if len(maker_refs) == 1:
                        fill_size = float(trade_size)
                    else:
                        continue
                self._link_record_locked(record=record, order_id=order_id, client_order_id=None)
                prev = float(as_float(record.get("filled_size")) or 0.0)
                self._apply_filled_total_locked(
                    record=record,
                    new_total=float(prev + float(fill_size)),
                    recv_ms=recv_ms,
                    source="user_ws_confirmed_trade_maker",
                    status=status,
                    fill_price=as_float(event.get("price")),
                )

    def add_market(self, *, condition_id: str) -> None:
        condition = str(condition_id or "").strip()
        if not condition:
            return
        with self._lock:
            markets = set(self._desired_markets)
            if condition in markets:
                return
            markets.add(condition)
            self._desired_markets = tuple(sorted(markets))

    def register_order(
        self,
        *,
        order_id: Optional[str],
        client_order_id: Optional[str],
        signal_id: Optional[str],
        market: Optional[str],
        instrument_id: Optional[str],
        outcome_side: Optional[str],
        order_side: Optional[str],
        requested_size: Optional[float],
        initial_filled_size: Optional[float] = None,
    ) -> None:
        order_key = as_non_empty_text(order_id)
        client_key = as_non_empty_text(client_order_id)
        with self._lock:
            record = self._find_record_locked(order_id=order_key, client_order_id=client_key)
            if record is None:
                record = {
                    "order_id": order_key,
                    "client_order_id": client_key,
                    "signal_id": as_non_empty_text(signal_id),
                    "market": as_non_empty_text(market),
                    "instrument_id": as_non_empty_text(instrument_id),
                    "outcome_side": as_non_empty_text(outcome_side),
                    "order_side": as_non_empty_text(order_side),
                    "requested_size": None if requested_size is None else float(requested_size),
                    "filled_size": 0.0,
                    "submitted_at_ms": now_ms(),
                    "updated_at_ms": now_ms(),
                    "last_status": None,
                }
            else:
                if signal_id:
                    record["signal_id"] = str(signal_id)
                if market:
                    record["market"] = str(market)
                if instrument_id:
                    record["instrument_id"] = str(instrument_id)
                if outcome_side:
                    record["outcome_side"] = str(outcome_side).lower()
                if order_side:
                    record["order_side"] = str(order_side).lower()
                if requested_size is not None:
                    record["requested_size"] = float(requested_size)
                record["updated_at_ms"] = now_ms()

            if initial_filled_size is not None:
                baseline = max(0.0, float(initial_filled_size))
                prior = max(0.0, float(as_float(record.get("filled_size")) or 0.0))
                record["filled_size"] = float(max(prior, baseline))
            self._link_record_locked(record=record, order_id=order_key, client_order_id=client_key)

    def drain_fill_events(self, *, max_events: int = 200) -> List[Dict[str, Any]]:
        take = max(1, int(max_events))
        rows: List[Dict[str, Any]] = []
        with self._lock:
            while self._fill_events and len(rows) < take:
                rows.append(dict(self._fill_events.popleft()))
        return rows

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "desired_markets": list(self._desired_markets),
                "tracked_order_ids": int(len(self._tracked_by_order_id)),
                "tracked_client_order_ids": int(len(self._tracked_by_client_id)),
                "message_count": int(self._message_count),
                "normalized_event_count": int(self._normalized_event_count),
                "fill_event_count": int(self._fill_event_count),
                "pending_fill_events": int(len(self._fill_events)),
                "last_error": self._last_error,
            }

    async def _worker_loop(self) -> None:
        if websockets is None:
            with self._lock:
                self._last_error = "missing_websocket_dependency"
            return

        attempt = 0
        while not self._stop_event.is_set():
            with self._lock:
                desired_markets = tuple(self._desired_markets)
            if not desired_markets:
                await asyncio.sleep(0.2)
                continue

            try:
                async with websockets.connect(
                    self.ws_url,
                    additional_headers={
                        "Origin": POLYMARKET_RTDS_WS_ORIGIN,
                        "User-Agent": "Arbitrage-Reverse-Strategy/1.0",
                    },
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=10,
                    max_size=8 * 1024 * 1024,
                ) as ws:
                    with self._lock:
                        self._active_loop = asyncio.get_running_loop()
                        self._active_ws = ws
                        self._last_error = None
                    attempt = 0
                    await ws.send(json.dumps(self._subscription_payload(auth=self.auth, markets=desired_markets)))
                    while not self._stop_event.is_set():
                        with self._lock:
                            current_markets = tuple(self._desired_markets)
                        if current_markets != desired_markets:
                            break
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=self.recv_timeout_seconds)
                        except asyncio.TimeoutError:
                            continue
                        recv_ms = now_ms()
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
                        with self._lock:
                            self._message_count += 1
                        for item in self._iter_message_items(parsed):
                            for normalized in normalize_polymarket_user_event(item, recv_ms, condition_id=None):
                                self._handle_normalized_event(event=as_dict(normalized), recv_ms=recv_ms)
            except Exception as exc:
                attempt += 1
                with self._lock:
                    self._last_error = str(exc)
                delay = min(self.reconnect_max_seconds, self.reconnect_base_seconds * (2 ** min(attempt - 1, 8)))
                slept = 0.0
                while slept < delay and not self._stop_event.is_set():
                    chunk = min(0.25, delay - slept)
                    await asyncio.sleep(chunk)
                    slept += chunk
            finally:
                with self._lock:
                    self._active_ws = None
                    self._active_loop = None

    def _worker_main(self) -> None:
        try:
            asyncio.run(self._worker_loop())
        except Exception as exc:
            with self._lock:
                self._last_error = str(exc)

    def start(self) -> None:
        with self._lock:
            if self._started:
                return
            self._started = True
        self._thread = threading.Thread(
            target=self._worker_main,
            name="reverse-strategy-user-fill-worker",
            daemon=True,
        )
        self._thread.start()

    async def _close_ws(self, ws: Any) -> None:
        try:
            await ws.close(code=1000, reason="client shutdown")
        except Exception:
            pass

    def close(self) -> None:
        self._stop_event.set()
        active_loop: Optional[asyncio.AbstractEventLoop]
        active_ws: Optional[Any]
        with self._lock:
            active_loop = self._active_loop
            active_ws = self._active_ws
        if active_loop is not None and active_ws is not None:
            try:
                future = asyncio.run_coroutine_threadsafe(self._close_ws(active_ws), active_loop)
                future.result(timeout=2.0)
            except Exception:
                pass
        if self._thread is not None:
            self._thread.join(timeout=2.0)


class PolymarketRtdsPriceAdapter:
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
        self._message_count = 0
        self._last_error: Optional[str] = None
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

    def _extract_price_event(self, item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        topic = str(item.get("topic") or "").strip().lower()
        if topic and topic != "crypto_prices_chainlink":
            return None
        payload = as_dict(item.get("payload")) or item
        symbol = str(payload.get("symbol") or "").strip().lower()
        if symbol != self._symbol_key:
            return None
        price = as_float(payload.get("value"))
        source_ts_ms = int(as_float(payload.get("timestamp")) or 0)
        if price is None or price <= 0 or source_ts_ms <= 0:
            return None
        second_ts_ms = int((source_ts_ms // 1000) * 1000)
        return {
            "price_usd": float(price),
            "source_timestamp_ms": int(source_ts_ms),
            "second_timestamp_ms": int(second_ts_ms),
        }

    def _apply_event(self, *, event: Dict[str, Any], recv_ms: int) -> None:
        second_ts_ms = int(event["second_timestamp_ms"])
        price = float(event["price_usd"])
        prune_before_ms = int(recv_ms - (self.cache_window_seconds * 1000))
        with self._lock:
            self._second_cache[second_ts_ms] = price
            self._second_cache = {
                ts_ms: value
                for ts_ms, value in self._second_cache.items()
                if int(ts_ms) >= prune_before_ms
            }
            self._message_count += 1
            self._last_error = None

    async def _unsubscribe_and_close_ws(self, ws: Any) -> None:
        try:
            await ws.send(json.dumps(self._unsubscription_payload(symbol=self.symbol)))
        except Exception:
            pass
        try:
            await ws.close(code=1000, reason="client shutdown")
        except Exception:
            pass

    async def _ping_loop(self, ws: Any) -> None:
        while not self._stop_event.is_set():
            await asyncio.sleep(5.0)
            try:
                await ws.send("PING")
            except Exception:
                return

    async def _worker_loop(self) -> None:
        if websockets is None:
            with self._lock:
                self._last_error = "missing_websocket_dependency"
            return

        attempt = 0
        while not self._stop_event.is_set():
            try:
                async with websockets.connect(
                    self.ws_url,
                    additional_headers={
                        "Origin": POLYMARKET_RTDS_WS_ORIGIN,
                        "User-Agent": "Arbitrage-Reverse-Strategy/1.0",
                    },
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=10,
                    max_size=8 * 1024 * 1024,
                ) as ws:
                    with self._lock:
                        self._active_loop = asyncio.get_running_loop()
                        self._active_ws = ws
                    attempt = 0
                    await ws.send(json.dumps(self._subscription_payload(symbol=self.symbol)))
                    ping_task = asyncio.create_task(self._ping_loop(ws))
                    try:
                        while not self._stop_event.is_set():
                            try:
                                raw = await asyncio.wait_for(ws.recv(), timeout=self.recv_timeout_seconds)
                            except asyncio.TimeoutError:
                                continue
                            recv_ms = now_ms()
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
                                event = self._extract_price_event(item)
                                if event is None:
                                    continue
                                self._apply_event(event=event, recv_ms=recv_ms)
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
                text = str(exc).lower()
                if "429" in text or "too many requests" in text:
                    delay = max(60.0, self.reconnect_max_seconds)
                else:
                    delay = min(self.reconnect_max_seconds, self.reconnect_base_seconds * (2 ** min(attempt - 1, 8)))
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

    def _ensure_started(self, *, at_ms: int) -> None:
        with self._lock:
            if self._started:
                return
            self._started = True
            self._start_at_ms = int(at_ms)
        self._thread = threading.Thread(
            target=self._worker_main,
            name="reverse-strategy-rtds-worker",
            daemon=True,
        )
        self._thread.start()

    def start(self) -> None:
        self._ensure_started(at_ms=now_ms())

    @staticmethod
    def _pick_second(second_cache: Dict[int, float], *, target_second_ms: int) -> Optional[int]:
        if not second_cache:
            return None
        if target_second_ms in second_cache:
            return int(target_second_ms)
        prior = [ts for ts in second_cache if int(ts) <= int(target_second_ms)]
        if prior:
            return int(max(prior))
        return int(max(second_cache))

    def read(self, *, at_ms: int) -> Dict[str, Any]:
        self._ensure_started(at_ms=int(at_ms))
        target_second_ms = int((int(at_ms) // 1000) * 1000)
        with self._lock:
            selected_second_ms = self._pick_second(self._second_cache, target_second_ms=target_second_ms)
            if selected_second_ms is None:
                started_ms = int(self._start_at_ms) if self._start_at_ms is not None else int(at_ms)
                raise RuntimeError(
                    "rtds_price_not_ready:"
                    f"elapsed_ms={max(0, int(at_ms) - started_ms)}"
                    f":last_error={self._last_error}"
                )
            price = float(self._second_cache[selected_second_ms])
            age_ms = int(at_ms - selected_second_ms)
            return {
                "price_usd": price,
                "source_timestamp_ms": int(selected_second_ms),
                "age_ms": age_ms,
                "is_stale": bool(age_ms > int(self.max_age_ms)),
                "message_count": int(self._message_count),
                "last_error": self._last_error,
                "selected_second_exact": bool(selected_second_ms == target_second_ms),
            }

    def close(self) -> None:
        self._stop_event.set()
        active_loop: Optional[asyncio.AbstractEventLoop]
        active_ws: Optional[Any]
        with self._lock:
            active_loop = self._active_loop
            active_ws = self._active_ws
        if active_loop is not None and active_ws is not None:
            try:
                future = asyncio.run_coroutine_threadsafe(self._unsubscribe_and_close_ws(active_ws), active_loop)
                future.result(timeout=2.0)
            except Exception:
                pass
        if self._thread is not None:
            self._thread.join(timeout=2.0)


def _resolve_market_yes_no_tokens(market: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    tokens = [str(item) for item in _parse_json_list(market.get("clobTokenIds")) if str(item or "").strip()]
    if len(tokens) != 2:
        return None
    outcomes = [str(item or "").strip() for item in _parse_json_list(market.get("outcomes")) if str(item or "").strip()]

    yes_idx = 0
    no_idx = 1
    yes_label: Optional[str] = outcomes[0] if len(outcomes) > 0 else None
    no_label: Optional[str] = outcomes[1] if len(outcomes) > 1 else None

    if len(outcomes) == 2:
        normalized = [_normalize_outcome_label(label) for label in outcomes]
        if "yes" in normalized and "no" in normalized:
            yes_idx = normalized.index("yes")
            no_idx = normalized.index("no")
            yes_label = outcomes[yes_idx]
            no_label = outcomes[no_idx]

    return {
        "token_yes": tokens[yes_idx],
        "token_no": tokens[no_idx],
        "yes_label": yes_label,
        "no_label": no_label,
    }


def _extract_official_resolution(*, market: Dict[str, Any], yes_label: Optional[str], no_label: Optional[str]) -> Optional[str]:
    outcomes = [str(item or "").strip() for item in _parse_json_list(market.get("outcomes")) if str(item or "").strip()]
    prices_raw = _parse_json_list(market.get("outcomePrices"))
    if not outcomes or not prices_raw or len(outcomes) != len(prices_raw):
        return None

    prices: List[float] = []
    for item in prices_raw:
        value = _normalize_share_price(as_float(item))
        if value is None:
            return None
        prices.append(float(value))
    if not prices:
        return None

    best_idx = max(range(len(prices)), key=lambda idx: prices[idx])
    best_price = float(prices[best_idx])
    if best_price < 0.99:
        return None

    winner_label = outcomes[best_idx]
    normalized = _normalize_outcome_label(winner_label)
    if normalized in {"yes", "no"}:
        return normalized

    if yes_label and winner_label.strip().lower() == yes_label.strip().lower():
        return "yes"
    if no_label and winner_label.strip().lower() == no_label.strip().lower():
        return "no"
    return None


def _extract_last_trade_yes_price(market: Dict[str, Any]) -> Optional[float]:
    return _normalize_share_price(as_float(market.get("lastTradePrice")))


def _fetch_market_window(*, transport: ApiTransport, window_start_s: int) -> Optional[MarketWindow]:
    slug = _slug_from_window_start(window_start_s)
    _, payload = transport.request_json(
        "GET",
        f"{POLYMARKET_GAMMA_API}/events",
        params={"slug": slug},
        allow_status={200},
    )
    events = payload if isinstance(payload, list) else []
    if not events:
        return None
    event = as_dict(events[0])
    markets = event.get("markets") if isinstance(event.get("markets"), list) else []
    target_market: Optional[Dict[str, Any]] = None
    for item in markets:
        market = as_dict(item)
        if str(market.get("slug") or "").strip() == slug:
            target_market = market
            break
    if target_market is None and markets:
        target_market = as_dict(markets[0])
    if target_market is None:
        return None

    tokens = _resolve_market_yes_no_tokens(target_market)
    if tokens is None:
        return None

    market_end_dt = _parse_iso_datetime(target_market.get("endDate") or target_market.get("endDateIso") or event.get("endDate"))
    market_end_s = int(market_end_dt.timestamp()) if market_end_dt is not None else int(window_start_s + WINDOW_SECONDS)
    official_resolution = _extract_official_resolution(
        market=target_market,
        yes_label=str(tokens.get("yes_label") or "") or None,
        no_label=str(tokens.get("no_label") or "") or None,
    )
    return MarketWindow(
        slug=slug,
        market_id=str(target_market.get("id") or "").strip(),
        condition_id=str(target_market.get("conditionId") or "").strip(),
        window_start_s=int(window_start_s),
        window_end_s=int(market_end_s),
        token_yes=str(tokens.get("token_yes") or "").strip(),
        token_no=str(tokens.get("token_no") or "").strip(),
        yes_label=str(tokens.get("yes_label") or "").strip() or None,
        no_label=str(tokens.get("no_label") or "").strip() or None,
        last_trade_yes_price=_extract_last_trade_yes_price(target_market),
        official_resolution=official_resolution,
        raw_market=target_market,
    )


def _resolve_observed_last_trade_yes_price(
    *,
    market: MarketWindow,
    last_trade_adapter: PolymarketLastTradeAdapter,
) -> Optional[float]:
    yes_row = last_trade_adapter.get_last_trade_for_asset(asset_id=market.token_yes)
    yes_price = _normalize_share_price(as_float(as_dict(yes_row).get("trade_price")))
    if yes_price is not None:
        return float(yes_price)

    # If only NO token traded recently, infer YES as complement.
    no_row = last_trade_adapter.get_last_trade_for_asset(asset_id=market.token_no)
    no_price = _normalize_share_price(as_float(as_dict(no_row).get("trade_price")))
    if no_price is not None:
        return float(max(0.0, min(1.0, 1.0 - float(no_price))))
    return None


def _resolve_preclose_last_trade_yes_price(
    *,
    market: MarketWindow,
    last_trade_adapter: PolymarketLastTradeAdapter,
    seconds_before_end: int,
) -> Optional[float]:
    offset_seconds = max(0, int(seconds_before_end))
    cutoff_ms = int((int(market.window_end_s) - offset_seconds) * 1000)

    yes_row = last_trade_adapter.get_last_trade_for_asset_asof(
        asset_id=market.token_yes,
        at_or_before_ms=cutoff_ms,
    )
    yes_price = _normalize_share_price(as_float(as_dict(yes_row).get("trade_price")))
    if yes_price is not None:
        return float(yes_price)

    no_row = last_trade_adapter.get_last_trade_for_asset_asof(
        asset_id=market.token_no,
        at_or_before_ms=cutoff_ms,
    )
    no_price = _normalize_share_price(as_float(as_dict(no_row).get("trade_price")))
    if no_price is not None:
        return float(max(0.0, min(1.0, 1.0 - float(no_price))))
    return None


def _extract_order_response_core(response: Dict[str, Any]) -> Dict[str, Any]:
    root = as_dict(response)
    nested = as_dict(root.get("response"))
    nested_response = as_dict(nested.get("response"))
    nested_request = as_dict(nested.get("request"))
    order = as_dict(nested.get("order")) or as_dict(nested_request.get("order"))
    containers = [root, nested, nested_response, order]

    def _pick_text(keys: List[str]) -> Optional[str]:
        for container in containers:
            for key in keys:
                value = str(container.get(key) or "").strip()
                if value:
                    return value
        return None

    def _pick_float(keys: List[str]) -> Optional[float]:
        for container in containers:
            for key in keys:
                value = as_float(container.get(key))
                if value is not None:
                    return float(value)
        return None

    status = _pick_text(["status", "state", "order_status"])
    filled_size = _pick_float(
        [
            "filled",
            "filled_size",
            "filled_count_fp",
            "filled_count",
            "filled_qty",
            "filled_quantity",
            "matched_size",
            "size_filled",
            "takingAmount",
            "taking_amount",
            "fill_count_fp",
            "fill_count",
        ]
    )
    return {
        "order_id": _pick_text(["order_id", "orderId", "orderID", "id"]),
        "status": status,
        "filled_size": filled_size,
        "remaining_size": _pick_float(
            [
                "remaining",
                "remaining_size",
                "size_left",
                "unfilled_size",
                "size_remaining",
                "open_size",
            ]
        ),
        "limit_price": _pick_float(["price", "limit_price", "avg_price", "average_price"]),
    }


def _is_fill_response(core: Dict[str, Any]) -> bool:
    status = str(core.get("status") or "").strip().lower()
    filled_size = as_float(core.get("filled_size"))
    if status in {"filled", "partially_filled", "partial_fill", "matched"}:
        return True
    return bool(filled_size is not None and filled_size > 0.0)


def _place_reverse_order(
    *,
    client: Any,
    signal_id: str,
    target_market: MarketWindow,
    target_side: str,
    target_token: str,
    config: ReverseStrategyConfig,
    dry_run: bool,
) -> Dict[str, Any]:
    client_order_id = build_client_order_id(
        signal_id=signal_id,
        venue="polymarket",
        instrument_id=target_token,
        side=target_side,
        seed="reverse-limit-premarket",
    )
    request_payload = {
        "signal_id": signal_id,
        "venue": "polymarket",
        "action": "buy",
        "side": str(target_side),
        "instrument_id": str(target_token),
        "order_kind": "limit",
        "time_in_force": "gtc",
        "size": int(config.order_size),
        "limit_price": float(config.order_limit_price),
        "client_order_id": client_order_id,
        "market_id": target_market.market_id,
        "condition_id": target_market.condition_id,
        "slug": target_market.slug,
    }
    if dry_run:
        return {
            "ok": True,
            "dry_run": True,
            "request": request_payload,
            "response": {
                "status": "dry_run_not_submitted",
            },
            "normalized": {
                "order_id": None,
                "status": "dry_run_not_submitted",
                "filled_size": None,
                "remaining_size": None,
                "limit_price": float(config.order_limit_price),
            },
        }

    try:
        response = client.place_buy_order(
            instrument_id=str(target_token),
            side=str(target_side),
            size=float(config.order_size),
            order_kind="limit",
            limit_price=float(config.order_limit_price),
            time_in_force="gtc",
            client_order_id=client_order_id,
        )
        normalized = _extract_order_response_core(as_dict(response))
        return {
            "ok": True,
            "dry_run": False,
            "request": request_payload,
            "response": as_dict(response),
            "normalized": normalized,
        }
    except Exception as exc:
        return {
            "ok": False,
            "dry_run": False,
            "request": request_payload,
            "error": f"{type(exc).__name__}:{exc}",
        }


def _build_api_transport() -> ApiTransport:
    return ApiTransport(
        default_headers={
            "User-Agent": "Arbitrage-Reverse-Strategy/1.0",
            "Accept": "application/json, text/plain, */*",
        }
    )


def _determine_outcome(
    *,
    current_market: MarketWindow,
    now_epoch_ms: int,
    config: ReverseStrategyConfig,
    start_price_entry: Optional[Dict[str, Any]],
    rtds_price_now: Optional[float],
    observed_last_trade_yes_price: Optional[float],
    preclose_last_trade_yes_price: Optional[float],
    allow_threshold: bool,
) -> Dict[str, Any]:
    now_s = int(now_epoch_ms // 1000)
    seconds_to_end = int(current_market.window_end_s - now_s)
    in_observation_window = bool(0 <= seconds_to_end <= int(config.observation_window_seconds))
    observed_last_trade = (
        float(observed_last_trade_yes_price)
        if observed_last_trade_yes_price is not None
        else current_market.last_trade_yes_price
    )

    if allow_threshold and in_observation_window and observed_last_trade is not None:
        if float(observed_last_trade) >= float(config.yes_threshold):
            return {
                "resolved": True,
                "outcome": "yes",
                "reason": "last_trade_threshold_upper",
                "last_trade_yes_price": float(observed_last_trade),
                "seconds_to_end": int(seconds_to_end),
            }
        if float(observed_last_trade) <= float(config.no_threshold):
            return {
                "resolved": True,
                "outcome": "no",
                "reason": "last_trade_threshold_lower",
                "last_trade_yes_price": float(observed_last_trade),
                "seconds_to_end": int(seconds_to_end),
            }

    if now_s < int(current_market.window_end_s + int(config.resolution_wait_after_end_seconds)):
        return {
            "resolved": False,
            "reason": "not_resolved_yet",
            "seconds_to_end": int(seconds_to_end),
            "last_trade_yes_price": observed_last_trade,
            "preclose_last_trade_yes_price": preclose_last_trade_yes_price,
        }

    if current_market.official_resolution in {"yes", "no"}:
        return {
            "resolved": True,
            "outcome": str(current_market.official_resolution),
            "reason": "official_market_resolution",
            "last_trade_yes_price": observed_last_trade,
            "preclose_last_trade_yes_price": preclose_last_trade_yes_price,
            "seconds_to_end": int(seconds_to_end),
        }

    preclose_price = _normalize_share_price(preclose_last_trade_yes_price)
    if preclose_price is not None:
        if float(preclose_price) >= float(config.preclose_yes_upper_threshold):
            return {
                "resolved": True,
                "outcome": "yes",
                "reason": "last_trade_preclose_upper",
                "last_trade_yes_price": observed_last_trade,
                "preclose_last_trade_yes_price": float(preclose_price),
                "seconds_to_end": int(seconds_to_end),
            }
        if float(preclose_price) <= float(config.preclose_yes_lower_threshold):
            return {
                "resolved": True,
                "outcome": "no",
                "reason": "last_trade_preclose_lower",
                "last_trade_yes_price": observed_last_trade,
                "preclose_last_trade_yes_price": float(preclose_price),
                "seconds_to_end": int(seconds_to_end),
            }

    start_price = as_float(as_dict(start_price_entry).get("price_usd")) if start_price_entry else None
    if start_price is not None and rtds_price_now is not None:
        delta = float(rtds_price_now - start_price)
        eps = float(config.rtds_resolution_epsilon_usd)
        if delta > eps:
            return {
                "resolved": True,
                "outcome": "yes",
                "reason": "rtds_start_end_fallback",
                "start_price_usd": float(start_price),
                "end_price_usd": float(rtds_price_now),
                "price_delta_usd": float(delta),
                "preclose_last_trade_yes_price": preclose_last_trade_yes_price,
                "seconds_to_end": int(seconds_to_end),
            }
        if delta < (-eps):
            return {
                "resolved": True,
                "outcome": "no",
                "reason": "rtds_start_end_fallback",
                "start_price_usd": float(start_price),
                "end_price_usd": float(rtds_price_now),
                "price_delta_usd": float(delta),
                "preclose_last_trade_yes_price": preclose_last_trade_yes_price,
                "seconds_to_end": int(seconds_to_end),
            }

    return {
        "resolved": False,
        "reason": "no_resolution_available",
        "seconds_to_end": int(seconds_to_end),
        "last_trade_yes_price": observed_last_trade,
        "preclose_last_trade_yes_price": preclose_last_trade_yes_price,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the Polymarket reverse premarket limit strategy.")
    parser.add_argument("--config", default="config/run_config.json")
    parser.add_argument(
        "--duration-seconds",
        type=int,
        default=0,
        help="Run duration in seconds. 0 means run until interrupted.",
    )
    parser.add_argument(
        "--dry-run",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Override config.reverse_strategy.dry_run.",
    )
    parser.add_argument(
        "--log-events",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Override config.reverse_strategy.log_events.",
    )
    parser.add_argument(
        "--log-decision-polls",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Override config.reverse_strategy.log_decision_polls.",
    )
    parser.add_argument(
        "--log-order-attempts",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Override config.reverse_strategy.log_order_attempts.",
    )
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    load_dotenv(dotenv_path=".env", override=False)

    config_path = Path(args.config)
    if not config_path.exists():
        raise FileNotFoundError(f"Missing config file: {config_path}")

    strategy_config = _load_reverse_strategy_config(config_path=config_path)
    overrides: Dict[str, Any] = {}
    if args.dry_run is not None:
        overrides["dry_run"] = bool(args.dry_run)
    if args.log_events is not None:
        overrides["log_events"] = bool(args.log_events)
    if args.log_decision_polls is not None:
        overrides["log_decision_polls"] = bool(args.log_decision_polls)
    if args.log_order_attempts is not None:
        overrides["log_order_attempts"] = bool(args.log_order_attempts)
    if overrides:
        strategy_config = ReverseStrategyConfig(
            **{
                **strategy_config.__dict__,
                **overrides,
            }
        )

    if not bool(strategy_config.enabled):
        print("reverse_strategy.enabled=false; exiting.")
        return

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_dir = PROJECT_ROOT / Path(strategy_config.output_dir)
    logger = ReverseStrategyLogger(
        output_dir=output_dir,
        run_id=run_id,
        log_events=bool(strategy_config.log_events),
    )
    state_path = PROJECT_ROOT / Path(strategy_config.state_file)
    state = ReverseStateStore(path=state_path)
    state.load()
    persistent_market_log_path = PROJECT_ROOT / "logs" / "log_reverse_strategy.jsonl"
    market_log = ReverseStrategyPersistentMarketLog(
        path=persistent_market_log_path,
        run_id=run_id,
        ws_resolution_fallback_after_close_seconds=int(
            strategy_config.ws_resolution_fallback_after_close_seconds
        ),
    )

    counters: Dict[str, int] = {
        "decision_polls": 0,
        "start_price_capture_success": 0,
        "start_price_capture_fail": 0,
        "resolved_threshold": 0,
        "resolved_preclose": 0,
        "resolved_official": 0,
        "resolved_rtds": 0,
        "orders_attempted": 0,
        "orders_submitted": 0,
        "order_errors": 0,
        "order_fills": 0,
        "order_fills_submit_ack": 0,
        "order_fills_user_ws": 0,
        "market_lookup_failures": 0,
        "reverse_log_rows_final": 0,
        "reverse_log_rows_partial": 0,
        "reverse_log_account_fetch_errors": 0,
        "reverse_log_resolution_fetch_errors": 0,
    }

    api_transport = _build_api_transport()
    rtds = PolymarketRtdsPriceAdapter(
        symbol=strategy_config.rtds_symbol,
        max_age_ms=int(strategy_config.rtds_max_age_ms),
    )
    rtds.start()
    last_trade_ws = PolymarketLastTradeAdapter()
    last_trade_ws.start()
    user_fill_ws: Optional[PolymarketUserFillAdapter] = None
    polymarket_account_client: Optional[PolymarketAccountPollClient] = None
    polymarket_user_address: Optional[str] = _resolve_polymarket_user_address_from_env()

    order_client: Any = None
    if not bool(strategy_config.dry_run):
        buy_exec_cfg = load_buy_execution_runtime_config_from_run_config(config_path=config_path)
        order_transport = build_buy_execution_transport(buy_execution_config=buy_exec_cfg)
        order_client = build_polymarket_api_buy_client_from_env(transport=order_transport)
        try:
            polymarket_account_client = PolymarketAccountPollClient()
        except Exception as exc:
            polymarket_account_client = None
            logger.write_event(
                kind="account_balance_client_error",
                payload={
                    "stage": "startup",
                    "error": f"{type(exc).__name__}:{exc}",
                },
            )
        try:
            user_fill_ws = PolymarketUserFillAdapter()
            user_fill_ws.start()
        except Exception as exc:
            user_fill_ws = None
            logger.write_event(
                kind="user_fill_ws_error",
                payload={
                    "stage": "startup",
                    "error": f"{type(exc).__name__}:{exc}",
                },
            )

    started_at_ms = now_ms()
    run_deadline_ms = None
    if int(args.duration_seconds) > 0:
        run_deadline_ms = int(started_at_ms + (int(args.duration_seconds) * 1000))

    next_poll_ms = int(started_at_ms)
    last_window_start_s: Optional[int] = None
    logger.write_event(
        kind="strategy_start",
        payload={
            "run_id": run_id,
            "config_path": str(config_path),
            "state_path": str(state_path),
            "dry_run": bool(strategy_config.dry_run),
            "config": strategy_config.__dict__,
        },
    )

    try:
        while True:
            now_epoch_ms = now_ms()
            if user_fill_ws is not None:
                for fill_event in user_fill_ws.drain_fill_events(max_events=500):
                    market_log.record_fill_event(fill_event=fill_event)
                    counters["order_fills"] += 1
                    counters["order_fills_user_ws"] += 1
                    logger.write_order(
                        kind="order_fill",
                        payload={
                            "fill_source": "user_ws",
                            **fill_event,
                        },
                    )
            if run_deadline_ms is not None and now_epoch_ms >= int(run_deadline_ms):
                break

            current_window_start_s = _window_start_epoch_s(now_epoch_ms)
            if last_window_start_s != current_window_start_s:
                last_window_start_s = int(current_window_start_s)
                transition_dt = datetime.fromtimestamp(float(current_window_start_s), tz=timezone.utc)
                logger.write_event(
                    kind="window_transition",
                    payload={
                        "window_start_s": int(current_window_start_s),
                        "window_slug": _slug_from_window_start(current_window_start_s),
                        "window_start_utc": transition_dt.isoformat(),
                        "minute_bucket": int(transition_dt.minute),
                        "is_quarter_hour": bool(int(transition_dt.minute) in WINDOW_BOUNDARY_MINUTES),
                    },
                )

            start_price_entry = state.get_start_price(window_start_s=current_window_start_s)
            capture_deadline_ms = int((current_window_start_s * 1000) + (strategy_config.start_price_capture_grace_seconds * 1000))
            if start_price_entry is None and now_epoch_ms <= capture_deadline_ms:
                try:
                    rtds_row = rtds.read(at_ms=now_epoch_ms)
                    state.set_start_price(
                        window_start_s=current_window_start_s,
                        payload={
                            "captured_at_ms": int(now_epoch_ms),
                            "source_timestamp_ms": int(as_float(rtds_row.get("source_timestamp_ms")) or now_epoch_ms),
                            "price_usd": float(as_float(rtds_row.get("price_usd")) or 0.0),
                            "slug": _slug_from_window_start(current_window_start_s),
                            "symbol": str(strategy_config.rtds_symbol),
                        },
                    )
                    counters["start_price_capture_success"] += 1
                    logger.write_event(
                        kind="start_price_captured",
                        payload={
                            "window_start_s": int(current_window_start_s),
                            "slug": _slug_from_window_start(current_window_start_s),
                            "rtds": rtds_row,
                        },
                    )
                except Exception as exc:
                    counters["start_price_capture_fail"] += 1
                    logger.write_event(
                        kind="start_price_capture_pending",
                        payload={
                            "window_start_s": int(current_window_start_s),
                            "slug": _slug_from_window_start(current_window_start_s),
                            "error": f"{type(exc).__name__}:{exc}",
                        },
                    )

            if now_epoch_ms < int(next_poll_ms):
                time.sleep(float(strategy_config.loop_sleep_seconds))
                continue
            next_poll_ms = int(now_epoch_ms + (float(strategy_config.poll_interval_seconds) * 1000.0))
            counters["decision_polls"] += 1

            within_previous_grace = bool(
                now_epoch_ms
                <= int(
                    (current_window_start_s * 1000)
                    + (int(strategy_config.previous_window_resolution_grace_seconds) * 1000)
                )
            )
            market_window_starts = {
                int(current_window_start_s),
                int(current_window_start_s + WINDOW_SECONDS),
            }
            if within_previous_grace:
                market_window_starts.add(int(current_window_start_s - WINDOW_SECONDS))

            markets_by_window: Dict[int, Optional[MarketWindow]] = {}
            market_lookup_errors: Dict[str, str] = {}
            for window_start in sorted(market_window_starts):
                try:
                    markets_by_window[int(window_start)] = _fetch_market_window(
                        transport=api_transport,
                        window_start_s=int(window_start),
                    )
                except Exception as exc:
                    markets_by_window[int(window_start)] = None
                    market_lookup_errors[str(int(window_start))] = f"{type(exc).__name__}:{exc}"

            if market_lookup_errors:
                counters["market_lookup_failures"] += len(market_lookup_errors)
                logger.write_event(
                    kind="market_lookup_error",
                    payload={
                        "window_start_s": int(current_window_start_s),
                        "errors": market_lookup_errors,
                    },
                )

            current_market = markets_by_window.get(int(current_window_start_s))
            next_market = markets_by_window.get(int(current_window_start_s + WINDOW_SECONDS))
            previous_market = (
                markets_by_window.get(int(current_window_start_s - WINDOW_SECONDS))
                if within_previous_grace
                else None
            )

            if current_market is not None:
                last_trade_ws.set_assets(
                    token_yes=current_market.token_yes,
                    token_no=current_market.token_no,
                )
            market_log.upsert_market_context(
                window_start_s=int(current_window_start_s),
                current_market=current_market,
                previous_market=previous_market,
            )
            if user_fill_ws is not None:
                for market_row in (previous_market, current_market, next_market):
                    if market_row is None:
                        continue
                    user_fill_ws.add_market(condition_id=market_row.condition_id)
            market_log.on_periodic_tick(
                now_epoch_ms=int(now_epoch_ms),
                api_transport=api_transport,
                user_address=polymarket_user_address,
                account_client=polymarket_account_client,
                last_trade_ws=last_trade_ws,
            )

            candidate_pairs: List[Dict[str, Any]] = []
            if within_previous_grace and previous_market is not None and current_market is not None:
                candidate_pairs.append(
                    {
                        "candidate_kind": "previous_window_grace",
                        "signal_market": previous_market,
                        "signal_window_start_s": int(previous_market.window_start_s),
                        "target_market": current_market,
                        "allow_threshold": False,
                        "use_ws_last_trade": False,
                    }
                )
            if current_market is not None and next_market is not None:
                candidate_pairs.append(
                    {
                        "candidate_kind": "current_window",
                        "signal_market": current_market,
                        "signal_window_start_s": int(current_market.window_start_s),
                        "target_market": next_market,
                        "allow_threshold": True,
                        "use_ws_last_trade": True,
                    }
                )

            if not candidate_pairs:
                counters["market_lookup_failures"] += 1
                logger.write_event(
                    kind="market_lookup_incomplete",
                    payload={
                        "window_start_s": int(current_window_start_s),
                        "within_previous_grace": bool(within_previous_grace),
                        "current_market_found": bool(current_market is not None),
                        "next_market_found": bool(next_market is not None),
                        "previous_market_found": bool(previous_market is not None),
                    },
                )
                continue

            rtds_now_row: Optional[Dict[str, Any]] = None
            rtds_now_price: Optional[float] = None
            try:
                rtds_now_row = rtds.read(at_ms=now_epoch_ms)
                rtds_now_price = as_float(rtds_now_row.get("price_usd"))
            except Exception:
                rtds_now_row = None
                rtds_now_price = None

            for candidate in candidate_pairs:
                signal_market = candidate["signal_market"]
                target_market = candidate["target_market"]
                signal_window_start_s = int(candidate["signal_window_start_s"])
                allow_threshold = bool(candidate["allow_threshold"])
                use_ws_last_trade = bool(candidate["use_ws_last_trade"])

                start_price_entry = state.get_start_price(window_start_s=signal_window_start_s)
                observed_last_trade_yes_price = (
                    _resolve_observed_last_trade_yes_price(
                        market=signal_market,
                        last_trade_adapter=last_trade_ws,
                    )
                    if use_ws_last_trade
                    else None
                )
                preclose_last_trade_yes_price = _resolve_preclose_last_trade_yes_price(
                    market=signal_market,
                    last_trade_adapter=last_trade_ws,
                    seconds_before_end=int(strategy_config.preclose_last_trade_seconds_before_end),
                )
                determination = _determine_outcome(
                    current_market=signal_market,
                    now_epoch_ms=now_epoch_ms,
                    config=strategy_config,
                    start_price_entry=start_price_entry,
                    rtds_price_now=rtds_now_price,
                    observed_last_trade_yes_price=observed_last_trade_yes_price,
                    preclose_last_trade_yes_price=preclose_last_trade_yes_price,
                    allow_threshold=allow_threshold,
                )

                if bool(strategy_config.log_decision_polls):
                    logger.write_event(
                        kind="decision_poll",
                        payload={
                            "window_start_s": int(current_window_start_s),
                            "candidate_kind": str(candidate["candidate_kind"]),
                            "signal_window_start_s": int(signal_window_start_s),
                            "signal_market": {
                                "slug": signal_market.slug,
                                "market_id": signal_market.market_id,
                                "condition_id": signal_market.condition_id,
                                "window_end_s": int(signal_market.window_end_s),
                                "last_trade_yes_price_api": signal_market.last_trade_yes_price,
                                "last_trade_yes_price_observed": observed_last_trade_yes_price,
                                "last_trade_yes_price_preclose": preclose_last_trade_yes_price,
                                "official_resolution": signal_market.official_resolution,
                            },
                            "target_market": {
                                "slug": target_market.slug,
                                "market_id": target_market.market_id,
                                "condition_id": target_market.condition_id,
                            },
                            "rtds": rtds_now_row,
                            "start_price": start_price_entry,
                            "last_trade_ws": last_trade_ws.snapshot(),
                            "user_fill_ws": (user_fill_ws.snapshot() if user_fill_ws is not None else None),
                            "determination": determination,
                        },
                    )

                if not bool(determination.get("resolved")):
                    continue

                reason = str(determination.get("reason") or "")
                if reason.startswith("last_trade_threshold"):
                    counters["resolved_threshold"] += 1
                elif reason.startswith("last_trade_preclose"):
                    counters["resolved_preclose"] += 1
                elif reason == "official_market_resolution":
                    counters["resolved_official"] += 1
                elif reason == "rtds_start_end_fallback":
                    counters["resolved_rtds"] += 1

                outcome = str(determination.get("outcome") or "").strip().lower()
                if outcome not in {"yes", "no"}:
                    continue
                market_log.apply_resolution(
                    condition_id=signal_market.condition_id,
                    outcome=outcome,
                    overwrite=False,
                )

                target_market_key = str(target_market.market_id or target_market.slug).strip()
                if not target_market_key:
                    continue
                if state.is_next_market_attempted(market_key=target_market_key):
                    continue

                reverse_side = "no" if outcome == "yes" else "yes"
                target_token = target_market.token_no if outcome == "yes" else target_market.token_yes
                signal_id = f"reverse-premarket-{signal_window_start_s}-{target_market_key}"
                attempt_meta = {
                    "attempted_at_ms": int(now_epoch_ms),
                    "run_id": run_id,
                    "window_start_s": int(current_window_start_s),
                    "candidate_kind": str(candidate["candidate_kind"]),
                    "signal_window_start_s": int(signal_window_start_s),
                    "signal_slug": signal_market.slug,
                    "target_market_key": target_market_key,
                    "target_slug": target_market.slug,
                    "resolved_outcome": outcome,
                    "resolved_reason": reason,
                    "reverse_side": reverse_side,
                    "target_token": target_token,
                }
                state.mark_next_market_attempt(
                    market_key=target_market_key,
                    payload={
                        **attempt_meta,
                        "status": "in_flight",
                    },
                )

                counters["orders_attempted"] += 1
                market_log.record_order_attempt(
                    target_market=target_market,
                    reverse_side=reverse_side,
                    limit_price=float(strategy_config.order_limit_price),
                    size=float(strategy_config.order_size),
                    decision_reason=reason,
                    attempted_at_ms=int(now_epoch_ms),
                )
                order_result = _place_reverse_order(
                    client=order_client,
                    signal_id=signal_id,
                    target_market=target_market,
                    target_side=reverse_side,
                    target_token=target_token,
                    config=strategy_config,
                    dry_run=bool(strategy_config.dry_run),
                )
                ok = bool(order_result.get("ok"))
                normalized = as_dict(order_result.get("normalized"))
                request_payload = as_dict(order_result.get("request"))
                order_status = str(normalized.get("status") or "")
                is_fill = _is_fill_response(normalized)

                if ok:
                    counters["orders_submitted"] += 1
                else:
                    counters["order_errors"] += 1
                if is_fill:
                    counters["order_fills"] += 1
                    counters["order_fills_submit_ack"] += 1

                if user_fill_ws is not None and ok and not bool(strategy_config.dry_run):
                    user_fill_ws.register_order(
                        order_id=as_non_empty_text(normalized.get("order_id")),
                        client_order_id=as_non_empty_text(request_payload.get("client_order_id")),
                        signal_id=signal_id,
                        market=target_market.condition_id,
                        instrument_id=target_token,
                        outcome_side=reverse_side,
                        order_side="buy",
                        requested_size=float(strategy_config.order_size),
                        initial_filled_size=as_float(normalized.get("filled_size")),
                    )

                state.mark_next_market_attempt(
                    market_key=target_market_key,
                    payload={
                        **attempt_meta,
                        "status": (
                            "dry_run"
                            if bool(strategy_config.dry_run)
                            else ("submitted" if ok else "failed")
                        ),
                        "order_status": order_status or None,
                        "is_fill": bool(is_fill),
                        "result": order_result,
                    },
                )

                if bool(strategy_config.log_order_attempts):
                    logger.write_order(
                        kind="order_attempt",
                        payload={
                            **attempt_meta,
                            "ok": ok,
                            "result": order_result,
                        },
                    )
                if not ok:
                    logger.write_order(
                        kind="order_error",
                        payload={
                            **attempt_meta,
                            "error": str(order_result.get("error") or "unknown_order_error"),
                            "request": as_dict(order_result.get("request")),
                        },
                    )
                if is_fill:
                    logger.write_order(
                        kind="order_fill",
                        payload={
                            **attempt_meta,
                            "normalized": normalized,
                            "response": as_dict(order_result.get("response")),
                        },
                    )

    except KeyboardInterrupt:
        logger.write_event(kind="strategy_stop", payload={"reason": "keyboard_interrupt"})
    finally:
        if user_fill_ws is not None:
            for fill_event in user_fill_ws.drain_fill_events(max_events=5_000):
                market_log.record_fill_event(fill_event=fill_event)
                counters["order_fills"] += 1
                counters["order_fills_user_ws"] += 1
                logger.write_order(
                    kind="order_fill",
                    payload={
                        "fill_source": "user_ws",
                        **fill_event,
                    },
                )
        try:
            rtds.close()
        except Exception:
            pass
        try:
            last_trade_ws.close()
        except Exception:
            pass
        try:
            if user_fill_ws is not None:
                user_fill_ws.close()
        except Exception:
            pass
        try:
            market_log.on_periodic_tick(
                now_epoch_ms=int(now_ms()),
                api_transport=api_transport,
                user_address=polymarket_user_address,
                account_client=polymarket_account_client,
                last_trade_ws=last_trade_ws,
            )
            market_log.flush_shutdown_partial()
        except Exception:
            pass
        try:
            market_log.close()
        except Exception:
            pass
        counters["reverse_log_rows_final"] = int(market_log.stats.get("rows_final", 0))
        counters["reverse_log_rows_partial"] = int(market_log.stats.get("rows_partial", 0))
        counters["reverse_log_account_fetch_errors"] = int(market_log.stats.get("account_fetch_errors", 0))
        counters["reverse_log_resolution_fetch_errors"] = int(market_log.stats.get("resolution_fetch_errors", 0))
        summary = {
            "run_id": run_id,
            "started_at_ms": int(started_at_ms),
            "started_at": datetime.fromtimestamp(started_at_ms / 1000.0, tz=timezone.utc).isoformat(),
            "ended_at_ms": int(now_ms()),
            "ended_at": datetime.now(timezone.utc).isoformat(),
            "config_path": str(config_path),
            "state_path": str(state_path),
            "dry_run": bool(strategy_config.dry_run),
            "counters": counters,
            "output_files": {
                "events": (
                    str(logger.events_path)
                    if bool(strategy_config.log_events)
                    else None
                ),
                "orders": str(logger.orders_path),
                "summary": str(logger.summary_path),
                "state": str(state_path),
                "persistent_market_log": str(persistent_market_log_path),
            },
            "user_fill_ws": (user_fill_ws.snapshot() if user_fill_ws is not None else None),
            "persistent_market_log": {
                "path": str(persistent_market_log_path),
                "stats": dict(market_log.stats),
            },
            "config": strategy_config.__dict__,
        }
        logger.summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
        logger.close()

    print("Reverse premarket strategy run complete")
    print(f"Run ID: {run_id}")
    print(
        "Events log:  "
        + (str(logger.events_path) if bool(strategy_config.log_events) else "disabled (log_events=false)")
    )
    print(f"Orders log:  {logger.orders_path}")
    print(f"Summary:     {logger.summary_path}")
    print(f"State file:  {state_path}")
    print(f"Persistent market log: {persistent_market_log_path}")


if __name__ == "__main__":
    main()
