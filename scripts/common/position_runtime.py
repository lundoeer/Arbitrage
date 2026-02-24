from __future__ import annotations

from dataclasses import dataclass, asdict as dataclass_asdict
from typing import Any, Deque, Dict, Iterable, List, Optional, Tuple

from collections import deque

from scripts.common.utils import as_dict, as_float, as_non_empty_text, now_ms


def _position_key(*, venue: str, instrument_id: str, outcome_side: str) -> str:
    return f"{str(venue).strip().lower()}|{str(instrument_id).strip()}|{str(outcome_side).strip().lower()}"


def _is_valid_venue(venue: str) -> bool:
    return str(venue).strip().lower() in {"polymarket", "kalshi"}


@dataclass(frozen=True)
class PositionRuntimeConfig:
    require_bootstrap_before_buy: bool = True
    drift_tolerance_contracts: float = 0.0
    max_exposure_per_market_usd: float = 0.0
    include_pending_orders_in_exposure: bool = True
    stale_warning_seconds: int = 60
    stale_error_seconds: int = 180
    dedupe_max_events: int = 10_000


@dataclass
class PositionState:
    venue: str
    instrument_id: str
    outcome_side: str
    net_contracts: float = 0.0
    avg_entry_price: Optional[float] = None
    position_exposure_usd: Optional[float] = None
    realized_pnl: Optional[float] = None
    fees: Optional[float] = None
    last_update_ms: int = 0
    last_source: str = "init"
    is_authoritative: bool = False


@dataclass
class OrderState:
    client_order_id: str
    signal_id: str
    venue: str
    instrument_id: str
    outcome_side: str
    action: Optional[str]
    submitted: bool
    status: str
    error: Optional[str] = None
    order_id: Optional[str] = None
    requested_size: Optional[float] = None
    filled_size: Optional[float] = None
    remaining_size: Optional[float] = None
    limit_price: Optional[float] = None
    first_seen_ms: int = 0
    last_update_ms: int = 0
    last_source: str = "submit_ack"
    source_rank: int = 0


class PositionRuntime:
    """
    In-memory position monitor for the currently selected pair.

    Scope is intentionally narrow for phase 1:
    - tracks only the active Polymarket YES/NO tokens + Kalshi YES/NO sides
    - merges submit-ack order state
    - applies confirmed fill deltas
    - reconciles from authoritative snapshots
    - maintains health gate `buy_execution_allowed`
    - evaluates execution plans against per-market exposure caps
    """

    def __init__(
        self,
        *,
        polymarket_token_yes: str,
        polymarket_token_no: str,
        kalshi_ticker: str,
        config: Optional[PositionRuntimeConfig] = None,
        now_epoch_ms: Optional[int] = None,
    ) -> None:
        self.config = config or PositionRuntimeConfig()
        self._known_events: set[str] = set()
        self._known_event_order: Deque[str] = deque()
        self.positions_by_key: Dict[str, PositionState] = {}
        self.orders_by_client_order_id: Dict[str, OrderState] = {}
        self.client_order_id_by_order_id: Dict[str, str] = {}
        self._counters: Dict[str, int] = {
            "deduped_events": 0,
            "drift_corrections": 0,
            "order_submit_acks": 0,
            "confirmed_fills_applied": 0,
            "order_ws_updates_applied": 0,
            "order_poll_updates_applied": 0,
            "order_closed_removed": 0,
        }
        self._tracked_keys = {
            _position_key(venue="polymarket", instrument_id=polymarket_token_yes, outcome_side="yes"),
            _position_key(venue="polymarket", instrument_id=polymarket_token_no, outcome_side="no"),
            _position_key(venue="kalshi", instrument_id=kalshi_ticker, outcome_side="yes"),
            _position_key(venue="kalshi", instrument_id=kalshi_ticker, outcome_side="no"),
        }
        self._venue_state: Dict[str, Dict[str, Any]] = {
            "polymarket": {
                "bootstrap_ok": False,
                "last_reconcile_success_ms": None,
                "last_reconcile_failure_ms": None,
                "last_error": None,
                "warning_stale": False,
                "hard_stale": False,
            },
            "kalshi": {
                "bootstrap_ok": False,
                "last_reconcile_success_ms": None,
                "last_reconcile_failure_ms": None,
                "last_error": None,
                "warning_stale": False,
                "hard_stale": False,
            },
        }
        ts = int(now_epoch_ms if now_epoch_ms is not None else now_ms())
        self.health: Dict[str, Any] = {}
        for key in self._tracked_keys:
            venue, instrument_id, outcome_side = key.split("|", 2)
            self.positions_by_key[key] = PositionState(
                venue=venue,
                instrument_id=instrument_id,
                outcome_side=outcome_side,
                last_update_ms=ts,
            )
        self._refresh_health(now_epoch_ms=ts)

    def _record_event_id(self, event_id: Optional[str]) -> bool:
        key = str(event_id or "").strip()
        if not key:
            return True
        if key in self._known_events:
            self._counters["deduped_events"] += 1
            return False
        self._known_events.add(key)
        self._known_event_order.append(key)
        limit = max(100, int(self.config.dedupe_max_events))
        while len(self._known_event_order) > limit:
            old = self._known_event_order.popleft()
            self._known_events.discard(old)
        return True

    def _is_tracked(self, *, venue: str, instrument_id: str, outcome_side: str) -> bool:
        return _position_key(venue=venue, instrument_id=instrument_id, outcome_side=outcome_side) in self._tracked_keys

    def _refresh_health(self, *, now_epoch_ms: Optional[int] = None) -> None:
        ts = int(now_epoch_ms if now_epoch_ms is not None else now_ms())
        warn_ms = int(max(1, int(self.config.stale_warning_seconds)) * 1000)
        err_ms = int(max(1, int(self.config.stale_error_seconds)) * 1000)
        for venue, state in self._venue_state.items():
            _ = venue
            last_ok = state.get("last_reconcile_success_ms")
            if isinstance(last_ok, int):
                age_ms = int(ts - last_ok)
                state["warning_stale"] = bool(age_ms > warn_ms)
                state["hard_stale"] = bool(age_ms > err_ms)
            else:
                state["warning_stale"] = False
                state["hard_stale"] = False

        bootstrap_completed = bool(
            self._venue_state["polymarket"]["bootstrap_ok"] and self._venue_state["kalshi"]["bootstrap_ok"]
        )
        hard_stale = bool(self._venue_state["polymarket"]["hard_stale"] or self._venue_state["kalshi"]["hard_stale"])
        position_exposure_by_market = self._position_gross_exposure_by_market_usd()
        open_order_exposure_by_market = self._open_order_reserved_exposure_by_market_usd()
        max_exposure_per_market_usd = max(0.0, float(self.config.max_exposure_per_market_usd))
        exposure_by_market: Dict[str, Dict[str, Any]] = {}
        for market_key in ("polymarket", "kalshi"):
            gross_position_exposure_usd = float(position_exposure_by_market.get(market_key, 0.0))
            gross_open_order_exposure_usd = float(open_order_exposure_by_market.get(market_key, 0.0))
            gross_total_exposure_usd = float(gross_position_exposure_usd + gross_open_order_exposure_usd)
            exposure_limit_exceeded = bool(
                max_exposure_per_market_usd > 0.0
                and gross_total_exposure_usd > (max_exposure_per_market_usd + 1e-9)
            )
            exposure_by_market[market_key] = {
                "gross_position_exposure_usd": round(float(gross_position_exposure_usd), 6),
                "gross_open_order_exposure_usd": round(float(gross_open_order_exposure_usd), 6),
                "gross_total_exposure_usd": round(float(gross_total_exposure_usd), 6),
                "max_exposure_per_market_usd": float(max_exposure_per_market_usd),
                "exposure_limit_exceeded": bool(exposure_limit_exceeded),
                "buy_blocked": bool(exposure_limit_exceeded),
            }
        buy_execution_allowed = True
        buy_execution_block_reasons: List[str] = []
        if bool(self.config.require_bootstrap_before_buy) and not bootstrap_completed:
            buy_execution_allowed = False
            buy_execution_block_reasons.append("bootstrap_not_completed")
        if hard_stale:
            buy_execution_allowed = False
            buy_execution_block_reasons.append("position_health_hard_stale")

        self.health = {
            "buy_execution_allowed": bool(buy_execution_allowed),
            "buy_execution_block_reasons": buy_execution_block_reasons,
            "require_bootstrap_before_buy": bool(self.config.require_bootstrap_before_buy),
            "bootstrap_completed": bool(bootstrap_completed),
            "hard_stale": bool(hard_stale),
            "max_exposure_per_market_usd": float(max_exposure_per_market_usd),
            "include_pending_orders_in_exposure": bool(self.config.include_pending_orders_in_exposure),
            "exposure_by_market": exposure_by_market,
            "drift_corrections": int(self._counters["drift_corrections"]),
            "deduped_events": int(self._counters["deduped_events"]),
            "venues": {
                "polymarket": dict(self._venue_state["polymarket"]),
                "kalshi": dict(self._venue_state["kalshi"]),
            },
        }

    @staticmethod
    def _market_key_from_venue(venue: Any) -> Optional[str]:
        normalized = str(venue or "").strip().lower()
        if normalized in {"polymarket", "kalshi"}:
            return normalized
        return None

    def evaluate_execution_plan(
        self,
        *,
        execution_plan: Dict[str, Any],
        now_epoch_ms: Optional[int] = None,
    ) -> Dict[str, Any]:
        ts = int(now_epoch_ms if now_epoch_ms is not None else now_ms())
        self._refresh_health(now_epoch_ms=ts)
        position_health = dict(self.health)
        global_allowed = bool(position_health.get("buy_execution_allowed"))
        global_reasons = [str(r) for r in list(position_health.get("buy_execution_block_reasons") or [])]
        if not global_allowed:
            return {
                "allowed": False,
                "reasons": global_reasons,
                "blocked_markets": [],
                "blocked_legs": [],
                "global_block_reasons": global_reasons,
                "position_health": position_health,
            }

        exposure_by_market = as_dict(position_health.get("exposure_by_market"))
        blocked_markets = [
            market_key
            for market_key in ("polymarket", "kalshi")
            if bool(as_dict(exposure_by_market.get(market_key)).get("buy_blocked"))
        ]
        legs_raw = execution_plan.get("legs")
        legs = list(legs_raw) if isinstance(legs_raw, list) else []
        blocked_legs: List[Dict[str, Any]] = []
        if blocked_markets:
            # Simplified policy: if any market bucket is blocked, block all buy legs.
            for index, raw_leg in enumerate(legs):
                leg = as_dict(raw_leg)
                action = str(leg.get("action") or "").strip().lower()
                if action and action != "buy":
                    continue
                market_key = self._market_key_from_venue(leg.get("venue"))
                blocked_legs.append(
                    {
                        "index": int(index),
                        "venue": market_key,
                        "side": str(leg.get("side") or "").strip().lower() or None,
                        "instrument_id": as_non_empty_text(leg.get("instrument_id")),
                        "action": action or "buy",
                        "reason": "max_exposure_per_market_exceeded",
                    }
                )

        reasons: List[str] = []
        if blocked_legs:
            reasons.append("max_exposure_per_market_exceeded")
        return {
            "allowed": not bool(blocked_legs),
            "reasons": reasons,
            "blocked_markets": blocked_markets,
            "blocked_legs": blocked_legs,
            "global_block_reasons": global_reasons,
            "position_health": position_health,
        }

    @staticmethod
    def _normalized_probability_price(value: Any) -> Optional[float]:
        price = as_float(value)
        if price is None:
            return None
        if 0.0 < float(price) <= 1.0:
            return float(price)
        return None

    @staticmethod
    def _extract_order_id(response_payload: Dict[str, Any]) -> Optional[str]:
        response = as_dict(response_payload)
        nested = as_dict(response.get("response"))
        order = as_dict(nested.get("order"))
        for container in (order, response, nested):
            for key in ("order_id", "orderId", "id"):
                value = as_non_empty_text(container.get(key))
                if value:
                    return value
        return None

    @staticmethod
    def _extract_filled_size(response_payload: Dict[str, Any]) -> Optional[float]:
        response = as_dict(response_payload)
        nested = as_dict(response.get("response"))
        order = as_dict(nested.get("order"))
        for container in (order, response, nested):
            for key in (
                "filled",
                "filled_size",
                "filled_count_fp",
                "filled_count",
                "filled_qty",
                "filled_quantity",
                "fill_count_fp",
                "fill_count",
                "size_filled",
                "matched_size",
            ):
                value = as_float(container.get(key))
                if value is not None and value > 0:
                    return float(value)
        return None

    @staticmethod
    def _extract_fill_price(response_payload: Dict[str, Any]) -> Optional[float]:
        response = as_dict(response_payload)
        nested = as_dict(response.get("response"))
        order = as_dict(nested.get("order"))
        for container in (order, response, nested):
            for key in (
                "fill_price",
                "filled_price",
                "avg_price",
                "average_price",
                "price",
                "yes_price_dollars",
                "no_price_dollars",
            ):
                price = PositionRuntime._normalized_probability_price(container.get(key))
                if price is not None:
                    return float(price)
        return None

    @staticmethod
    def _extract_remaining_size(response_payload: Dict[str, Any]) -> Optional[float]:
        response = as_dict(response_payload)
        nested = as_dict(response.get("response"))
        order = as_dict(nested.get("order"))
        for container in (order, response, nested):
            for key in (
                "remaining_count_fp",
                "remaining_count",
                "remaining_size",
                "size_remaining",
                "unfilled_size",
                "open_size",
            ):
                value = as_float(container.get(key))
                if value is not None and value >= 0:
                    return float(value)
        return None

    @staticmethod
    def _extract_order_status(response_payload: Dict[str, Any]) -> Optional[str]:
        response = as_dict(response_payload)
        nested = as_dict(response.get("response"))
        order = as_dict(nested.get("order"))
        for container in (order, response, nested):
            for key in ("status", "order_status", "state"):
                value = as_non_empty_text(container.get(key))
                if value:
                    return str(value).strip().lower()
        return None

    @staticmethod
    def _resolve_order_status(
        *,
        submitted: bool,
        response_payload: Dict[str, Any],
        remaining_size: Optional[float],
    ) -> str:
        if not submitted:
            return "submit_error"
        parsed = PositionRuntime._extract_order_status(response_payload)
        if parsed:
            return parsed
        if remaining_size is not None:
            return "open" if float(remaining_size) > 0.0 else "executed"
        return "submitted"

    @staticmethod
    def _status_source_rank(source: str) -> int:
        value = str(source or "").strip().lower()
        if value == "submit_ack":
            return 1
        if value in {"ws", "user_ws"}:
            return 2
        if value in {"poll", "orders_poll"}:
            return 3
        return 0

    @staticmethod
    def _normalize_order_status(
        *,
        venue: str,
        status: Optional[str],
        submitted: bool,
        remaining_size: Optional[float],
        requested_size: Optional[float],
        filled_size: Optional[float],
    ) -> str:
        _ = venue
        raw = str(status or "").strip().lower()
        remaining = as_float(remaining_size)
        requested = as_float(requested_size)
        filled = as_float(filled_size)

        inferred: Optional[str] = None
        if remaining is not None:
            if float(remaining) <= 1e-9:
                inferred = "filled"
            elif filled is not None and float(filled) > 0.0:
                inferred = "partially_filled"
            else:
                inferred = "open"
        elif requested is not None and filled is not None:
            if float(filled) >= (float(requested) - 1e-9):
                inferred = "filled"
            elif float(filled) > 0.0:
                inferred = "partially_filled"

        if raw in {"cancelled", "canceled", "killed"}:
            return "canceled"
        if raw in {"rejected", "reject"}:
            return "rejected"
        if raw in {"expired"}:
            return "expired"
        if raw in {"failed", "error", "submit_error"}:
            return "failed"
        if raw in {"partially_filled", "partial_fill", "partiallyfilled", "partial"}:
            return "partially_filled"
        if raw in {"filled", "executed", "confirmed", "done", "complete", "completed"}:
            if inferred == "partially_filled":
                return "partially_filled"
            return "filled"
        if raw in {
            "open",
            "live",
            "resting",
            "pending",
            "accepted",
            "queued",
            "new",
            "booked",
            "active",
            "unmatched",
            "matched",
            "mined",
            "retrying",
            "update",
            "submitted",
        }:
            if inferred is not None:
                return inferred
            return "open"
        if not submitted:
            return "failed"
        if inferred is not None:
            return inferred
        return "open"

    def _resolve_order_record_key(
        self,
        *,
        venue: str,
        order_id: Optional[str],
        client_order_id: Optional[str],
        instrument_id: Optional[str],
        outcome_side: Optional[str],
    ) -> Optional[str]:
        client_id = as_non_empty_text(client_order_id)
        if client_id:
            return client_id
        order_id_text = as_non_empty_text(order_id)
        if order_id_text:
            mapped = as_non_empty_text(self.client_order_id_by_order_id.get(order_id_text))
            if mapped:
                return mapped
            return f"{str(venue or '').strip().lower()}:oid:{order_id_text}"
        instrument = as_non_empty_text(instrument_id)
        side = as_non_empty_text(outcome_side)
        if instrument and side:
            return f"{str(venue or '').strip().lower()}:pos:{instrument}:{str(side).lower()}"
        return None

    def _drop_order_record(self, *, client_order_id: str) -> None:
        record = self.orders_by_client_order_id.pop(str(client_order_id), None)
        if record is None:
            return
        order_id = as_non_empty_text(record.order_id)
        if order_id and self.client_order_id_by_order_id.get(order_id) == client_order_id:
            self.client_order_id_by_order_id.pop(order_id, None)
        self._counters["order_closed_removed"] += 1

    def _upsert_order_state(
        self,
        *,
        source: str,
        venue: str,
        signal_id: Optional[str],
        client_order_id: Optional[str],
        order_id: Optional[str],
        instrument_id: Optional[str],
        outcome_side: Optional[str],
        action: Optional[str],
        submitted: bool,
        status: Optional[str],
        requested_size: Optional[float],
        filled_size: Optional[float],
        remaining_size: Optional[float],
        limit_price: Optional[float],
        error: Optional[str],
        event_id: Optional[str],
        now_epoch_ms: Optional[int] = None,
    ) -> bool:
        if not self._record_event_id(event_id):
            return False
        ts = int(now_epoch_ms if now_epoch_ms is not None else now_ms())
        source_rank = self._status_source_rank(source)
        key = self._resolve_order_record_key(
            venue=venue,
            order_id=order_id,
            client_order_id=client_order_id,
            instrument_id=instrument_id,
            outcome_side=outcome_side,
        )
        if key is None:
            return False
        prior = self.orders_by_client_order_id.get(key)
        if prior is not None:
            prior_rank = int(prior.source_rank)
            if source_rank < prior_rank:
                return False
            if source_rank == prior_rank and ts < int(prior.last_update_ms):
                return False
        requested = as_float(requested_size)
        filled = as_float(filled_size)
        remaining = as_float(remaining_size)
        status_norm = self._normalize_order_status(
            venue=venue,
            status=status,
            submitted=bool(submitted),
            remaining_size=remaining,
            requested_size=requested,
            filled_size=filled,
        )
        if prior is None:
            first_seen_ms = ts
        else:
            first_seen_ms = int(prior.first_seen_ms)
            if requested is None:
                requested = as_float(prior.requested_size)
            if filled is None:
                filled = as_float(prior.filled_size)
            if remaining is None:
                remaining = as_float(prior.remaining_size)
            if limit_price is None:
                limit_price = as_float(prior.limit_price)
            if not as_non_empty_text(instrument_id):
                instrument_id = str(prior.instrument_id or "")
            if not as_non_empty_text(outcome_side):
                outcome_side = str(prior.outcome_side or "")
            if not as_non_empty_text(action):
                action = prior.action
            if not as_non_empty_text(order_id):
                order_id = prior.order_id
            if not as_non_empty_text(signal_id):
                signal_id = prior.signal_id
            if not as_non_empty_text(error):
                error = prior.error
        if remaining is None and requested is not None and filled is not None:
            remaining = max(0.0, float(requested - filled))

        if self._is_order_closed_status(status_norm):
            self._drop_order_record(client_order_id=key)
            return True

        venue_norm = str(venue or "").strip().lower()
        side_norm = str(outcome_side or "").strip().lower()
        action_norm = str(action or "").strip().lower() or None
        self.orders_by_client_order_id[key] = OrderState(
            client_order_id=str(key),
            signal_id=str(signal_id or ""),
            venue=venue_norm,
            instrument_id=str(instrument_id or "").strip(),
            outcome_side=side_norm,
            action=action_norm,
            submitted=bool(submitted),
            status=str(status_norm),
            error=as_non_empty_text(error),
            order_id=as_non_empty_text(order_id),
            requested_size=None if requested is None else float(requested),
            filled_size=None if filled is None else float(filled),
            remaining_size=None if remaining is None else float(remaining),
            limit_price=None if limit_price is None else float(limit_price),
            first_seen_ms=first_seen_ms,
            last_update_ms=ts,
            last_source=str(source),
            source_rank=int(source_rank),
        )
        order_id_text = as_non_empty_text(order_id)
        if order_id_text:
            self.client_order_id_by_order_id[order_id_text] = str(key)
        return True

    @staticmethod
    def _is_order_closed_status(status: str) -> bool:
        value = str(status or "").strip().lower()
        return value in {
            "filled",
            "canceled",
            "cancelled",
            "rejected",
            "expired",
            "failed",
            # Backward compatibility for pre-normalized states:
            "executed",
            "filled",
            "killed",
            "closed",
            "submit_error",
            "error",
        }

    @staticmethod
    def _is_open_order(record: OrderState) -> bool:
        if not bool(record.submitted):
            return False
        if PositionRuntime._is_order_closed_status(record.status):
            return False
        remaining = as_float(record.remaining_size)
        if remaining is not None:
            return bool(float(remaining) > 0.0)
        requested = as_float(record.requested_size)
        filled = as_float(record.filled_size)
        if requested is not None and filled is not None:
            return bool((float(requested) - float(filled)) > 1e-9)
        return True

    def _position_gross_exposure_by_market_usd(self) -> Dict[str, float]:
        gross_by_market: Dict[str, float] = {
            "polymarket": 0.0,
            "kalshi": 0.0,
        }
        for rec in self.positions_by_key.values():
            contracts = abs(float(rec.net_contracts))
            if contracts <= 0.0:
                continue
            position_exposure = as_float(rec.position_exposure_usd)
            if position_exposure is not None and float(position_exposure) >= 0.0:
                exposure_usd = float(position_exposure)
            else:
                price = self._normalized_probability_price(rec.avg_entry_price)
                if price is None:
                    # Conservative fallback: binary payout max is $1 per contract.
                    price = 1.0
                exposure_usd = float(contracts * price)
            market_key = self._market_key_from_venue(rec.venue)
            if market_key is None:
                continue
            gross_by_market[market_key] = float(gross_by_market.get(market_key, 0.0) + exposure_usd)
        return {
            "polymarket": float(gross_by_market.get("polymarket", 0.0)),
            "kalshi": float(gross_by_market.get("kalshi", 0.0)),
        }

    def _open_order_reserved_exposure_by_market_usd(self) -> Dict[str, float]:
        gross_by_market: Dict[str, float] = {
            "polymarket": 0.0,
            "kalshi": 0.0,
        }
        if not bool(self.config.include_pending_orders_in_exposure):
            return gross_by_market
        for rec in self.orders_by_client_order_id.values():
            if not self._is_open_order(rec):
                continue
            action = str(rec.action or "").strip().lower()
            if action and action != "buy":
                continue
            remaining = as_float(rec.remaining_size)
            requested = as_float(rec.requested_size)
            filled = as_float(rec.filled_size)
            if remaining is None and requested is not None and filled is not None:
                remaining = max(0.0, float(requested - filled))
            if remaining is None:
                remaining = requested
            if remaining is None or float(remaining) <= 0.0:
                continue
            price = self._normalized_probability_price(rec.limit_price)
            if price is None:
                # Missing/invalid price should reserve max payout to fail safely.
                price = 1.0
            market_key = self._market_key_from_venue(rec.venue)
            if market_key is None:
                continue
            gross_by_market[market_key] = float(gross_by_market.get(market_key, 0.0) + (remaining * price))
        return {
            "polymarket": float(gross_by_market.get("polymarket", 0.0)),
            "kalshi": float(gross_by_market.get("kalshi", 0.0)),
        }

    @staticmethod
    def _merged_avg_entry_price(
        *,
        prior_net: float,
        prior_avg: Optional[float],
        delta_contracts: float,
        fill_price: Optional[float],
    ) -> Optional[float]:
        if fill_price is None:
            if abs(prior_net + delta_contracts) <= 0:
                return None
            return prior_avg

        new_net = float(prior_net + delta_contracts)
        if abs(new_net) <= 0:
            return None

        # Same-direction add: weighted average entry.
        if prior_net == 0 or (prior_net > 0 and delta_contracts > 0) or (prior_net < 0 and delta_contracts < 0):
            prior_weight = abs(float(prior_net))
            delta_weight = abs(float(delta_contracts))
            if prior_weight <= 0:
                return float(fill_price)
            if prior_avg is None:
                return float(fill_price)
            return float((prior_avg * prior_weight + float(fill_price) * delta_weight) / (prior_weight + delta_weight))

        # Reducing or flipping: keep prior avg until crossed through zero.
        if (prior_net > 0 > new_net) or (prior_net < 0 < new_net):
            return float(fill_price)
        return prior_avg

    def _apply_delta(
        self,
        *,
        venue: str,
        instrument_id: str,
        outcome_side: str,
        delta_contracts: float,
        fill_price: Optional[float],
        source: str,
        authoritative: bool,
        event_id: Optional[str] = None,
        now_epoch_ms: Optional[int] = None,
    ) -> bool:
        venue_norm = str(venue).strip().lower()
        side_norm = str(outcome_side).strip().lower()
        instrument = str(instrument_id or "").strip()
        if not self._is_tracked(venue=venue_norm, instrument_id=instrument, outcome_side=side_norm):
            return False
        if not self._record_event_id(event_id):
            return False
        ts = int(now_epoch_ms if now_epoch_ms is not None else now_ms())
        key = _position_key(venue=venue_norm, instrument_id=instrument, outcome_side=side_norm)
        rec = self.positions_by_key[key]
        prior_net = float(rec.net_contracts)
        prior_avg = rec.avg_entry_price
        delta = float(delta_contracts)
        rec.net_contracts = float(prior_net + delta)
        rec.avg_entry_price = self._merged_avg_entry_price(
            prior_net=prior_net,
            prior_avg=prior_avg,
            delta_contracts=delta,
            fill_price=fill_price,
        )
        # Delta-based updates carry size/price, so explicit API-reported exposure
        # should be re-derived rather than reused.
        rec.position_exposure_usd = None
        rec.last_update_ms = ts
        rec.last_source = str(source)
        rec.is_authoritative = bool(authoritative)
        self._refresh_health(now_epoch_ms=ts)
        return True

    def apply_polymarket_confirmed_fill(
        self,
        *,
        event_id: Optional[str],
        instrument_id: str,
        outcome_side: str,
        filled_contracts: float,
        fill_price: Optional[float] = None,
        now_epoch_ms: Optional[int] = None,
    ) -> bool:
        size = as_float(filled_contracts)
        if size is None or size <= 0:
            return False
        applied = self._apply_delta(
            venue="polymarket",
            instrument_id=instrument_id,
            outcome_side=outcome_side,
            delta_contracts=float(size),
            fill_price=fill_price,
            source="user_ws_confirmed_trade",
            authoritative=False,
            event_id=event_id,
            now_epoch_ms=now_epoch_ms,
        )
        if applied:
            self._counters["confirmed_fills_applied"] += 1
        return bool(applied)

    def apply_kalshi_market_position(
        self,
        *,
        event_id: Optional[str],
        instrument_id: str,
        outcome_side: str,
        net_contracts: float,
        avg_entry_price: Optional[float] = None,
        position_exposure_usd: Optional[float] = None,
        now_epoch_ms: Optional[int] = None,
    ) -> bool:
        venue = "kalshi"
        instrument = str(instrument_id or "").strip()
        side = str(outcome_side or "").strip().lower()
        if not self._is_tracked(venue=venue, instrument_id=instrument, outcome_side=side):
            return False
        if not self._record_event_id(event_id):
            return False
        ts = int(now_epoch_ms if now_epoch_ms is not None else now_ms())
        key = _position_key(venue=venue, instrument_id=instrument, outcome_side=side)
        rec = self.positions_by_key[key]
        rec.net_contracts = float(as_float(net_contracts) or 0.0)
        rec.avg_entry_price = as_float(avg_entry_price)
        parsed_exposure = as_float(position_exposure_usd)
        if parsed_exposure is not None:
            rec.position_exposure_usd = float(parsed_exposure)
        rec.last_update_ms = ts
        rec.last_source = "positions_ws"
        rec.is_authoritative = True
        self._refresh_health(now_epoch_ms=ts)
        return True

    def apply_polymarket_user_order_event(
        self,
        *,
        event_payload: Dict[str, Any],
        now_epoch_ms: Optional[int] = None,
    ) -> bool:
        ts = int(now_epoch_ms if now_epoch_ms is not None else now_ms())
        event = as_dict(event_payload)
        client_order_id = as_non_empty_text(event.get("client_order_id"))
        order_id = as_non_empty_text(event.get("order_id")) or as_non_empty_text(event.get("event_id"))
        instrument_id = as_non_empty_text(event.get("asset_id"))
        outcome_side = as_non_empty_text(event.get("outcome_side"))
        action = as_non_empty_text(event.get("order_side")) or as_non_empty_text(event.get("action"))
        status = as_non_empty_text(event.get("status"))
        requested_size = as_float(event.get("original_size"))
        filled_size = as_float(event.get("size_matched"))
        remaining_size = as_float(event.get("remaining_size"))
        if remaining_size is None and requested_size is not None and filled_size is not None:
            remaining_size = max(0.0, float(requested_size - filled_size))
        limit_price = self._normalized_probability_price(event.get("price"))
        applied = self._upsert_order_state(
            source="ws",
            venue="polymarket",
            signal_id=None,
            client_order_id=client_order_id,
            order_id=order_id,
            instrument_id=instrument_id,
            outcome_side=outcome_side,
            action=action,
            submitted=True,
            status=status,
            requested_size=requested_size,
            filled_size=filled_size,
            remaining_size=remaining_size,
            limit_price=limit_price,
            error=None,
            event_id=as_non_empty_text(event.get("event_id")) or f"pm_user_order:{order_id or client_order_id}",
            now_epoch_ms=ts,
        )
        if applied:
            self._counters["order_ws_updates_applied"] += 1
            self._refresh_health(now_epoch_ms=ts)
        return bool(applied)

    def apply_kalshi_user_order_event(
        self,
        *,
        event_payload: Dict[str, Any],
        now_epoch_ms: Optional[int] = None,
    ) -> bool:
        ts = int(now_epoch_ms if now_epoch_ms is not None else now_ms())
        event = as_dict(event_payload)
        applied = self._upsert_order_state(
            source="ws",
            venue="kalshi",
            signal_id=None,
            client_order_id=as_non_empty_text(event.get("client_order_id")),
            order_id=as_non_empty_text(event.get("order_id")),
            instrument_id=as_non_empty_text(event.get("market_ticker")),
            outcome_side=as_non_empty_text(event.get("outcome_side")),
            action=as_non_empty_text(event.get("action")),
            submitted=True,
            status=as_non_empty_text(event.get("status")),
            requested_size=as_float(event.get("requested_size")),
            filled_size=as_float(event.get("filled_size")),
            remaining_size=as_float(event.get("remaining_size")),
            limit_price=self._normalized_probability_price(event.get("limit_price")),
            error=None,
            event_id=as_non_empty_text(event.get("event_id")),
            now_epoch_ms=ts,
        )
        if applied:
            self._counters["order_ws_updates_applied"] += 1
            self._refresh_health(now_epoch_ms=ts)
        return bool(applied)

    def apply_buy_execution_result(self, *, result_payload: Dict[str, Any], now_epoch_ms: Optional[int] = None) -> int:
        ts = int(now_epoch_ms if now_epoch_ms is not None else now_ms())
        payload = as_dict(result_payload)
        signal_id = as_non_empty_text(payload.get("signal_id")) or ""
        legs = payload.get("legs")
        if not isinstance(legs, list):
            return 0

        accepted = 0
        for raw_leg in legs:
            leg = as_dict(raw_leg)
            client_order_id = as_non_empty_text(leg.get("client_order_id"))
            if not client_order_id:
                continue
            venue = str(leg.get("venue") or "").strip().lower()
            side = str(leg.get("side") or "").strip().lower()
            instrument_id = str(leg.get("instrument_id") or "").strip()
            response_payload = as_dict(leg.get("response_payload"))
            request_payload = as_dict(leg.get("request_payload"))
            submitted = bool(leg.get("submitted"))
            error = as_non_empty_text(leg.get("error"))
            requested_size = as_float(request_payload.get("size"))
            limit_price = self._normalized_probability_price(request_payload.get("limit_price"))
            action = as_non_empty_text(request_payload.get("action"))
            action_norm = str(action).lower() if action else None
            filled = self._extract_filled_size(response_payload)
            remaining = self._extract_remaining_size(response_payload)
            if remaining is None and requested_size is not None and filled is not None:
                remaining = max(0.0, float(requested_size - filled))
            status = self._resolve_order_status(
                submitted=submitted,
                response_payload=response_payload,
                remaining_size=remaining,
            )
            applied = self._upsert_order_state(
                source="submit_ack",
                venue=venue,
                signal_id=signal_id,
                client_order_id=client_order_id,
                order_id=self._extract_order_id(response_payload),
                instrument_id=instrument_id,
                outcome_side=side,
                action=action_norm,
                submitted=bool(submitted),
                status=status,
                requested_size=requested_size,
                filled_size=filled,
                remaining_size=remaining,
                limit_price=limit_price,
                error=error,
                event_id=f"submit_ack:{client_order_id}:{status}:{filled}:{remaining}",
                now_epoch_ms=ts,
            )
            if applied:
                accepted += 1
                self._counters["order_submit_acks"] += 1

            # Apply only if venue explicitly returns filled size in submit response.
            if submitted and filled is not None and filled > 0 and _is_valid_venue(venue):
                fill_price = self._extract_fill_price(response_payload)
                self._apply_delta(
                    venue=venue,
                    instrument_id=instrument_id,
                    outcome_side=side,
                    delta_contracts=float(filled),
                    fill_price=fill_price,
                    source="submit_ack_with_fill",
                    authoritative=False,
                    event_id=f"submit_fill:{client_order_id}:{filled}:{fill_price}",
                    now_epoch_ms=ts,
                )

        self._refresh_health(now_epoch_ms=ts)
        return accepted

    def reconcile_orders_snapshot(
        self,
        *,
        venue: str,
        orders: Iterable[Dict[str, Any]],
        snapshot_id: Optional[str] = None,
        snapshot_is_complete: bool = False,
        now_epoch_ms: Optional[int] = None,
    ) -> Dict[str, Any]:
        venue_norm = str(venue).strip().lower()
        if venue_norm not in {"polymarket", "kalshi"}:
            raise RuntimeError(f"Unsupported venue for order reconciliation: {venue}")
        if not self._record_event_id(snapshot_id):
            return {"applied": 0, "ignored": 0, "deduped": True}

        ts = int(now_epoch_ms if now_epoch_ms is not None else now_ms())
        applied = 0
        ignored = 0
        present_order_keys: set[str] = set()
        for idx, row in enumerate(orders):
            item = as_dict(row)
            client_order_id = as_non_empty_text(item.get("client_order_id"))
            order_id = as_non_empty_text(item.get("order_id")) or as_non_empty_text(item.get("id"))
            instrument_id = as_non_empty_text(item.get("instrument_id")) or as_non_empty_text(item.get("asset_id"))
            outcome_side = as_non_empty_text(item.get("outcome_side")) or as_non_empty_text(item.get("side"))
            action = as_non_empty_text(item.get("action")) or as_non_empty_text(item.get("order_side"))
            status = as_non_empty_text(item.get("status")) or as_non_empty_text(item.get("order_status"))
            requested_size = as_float(item.get("requested_size"))
            if requested_size is None:
                requested_size = as_float(item.get("original_size"))
            filled_size = as_float(item.get("filled_size"))
            if filled_size is None:
                filled_size = as_float(item.get("size_matched"))
            remaining_size = as_float(item.get("remaining_size"))
            if remaining_size is None and requested_size is not None and filled_size is not None:
                remaining_size = max(0.0, float(requested_size - filled_size))
            limit_price = self._normalized_probability_price(item.get("limit_price"))
            if limit_price is None:
                limit_price = self._normalized_probability_price(item.get("price"))
            order_key = self._resolve_order_record_key(
                venue=venue_norm,
                order_id=order_id,
                client_order_id=client_order_id,
                instrument_id=instrument_id,
                outcome_side=outcome_side,
            )
            if order_key:
                present_order_keys.add(str(order_key))
            ok = self._upsert_order_state(
                source="poll",
                venue=venue_norm,
                signal_id=None,
                client_order_id=client_order_id,
                order_id=order_id,
                instrument_id=instrument_id,
                outcome_side=outcome_side,
                action=action,
                submitted=True,
                status=status,
                requested_size=requested_size,
                filled_size=filled_size,
                remaining_size=remaining_size,
                limit_price=limit_price,
                error=None,
                event_id=f"{snapshot_id}:{venue_norm}:{idx}:{order_id or client_order_id or 'na'}",
                now_epoch_ms=ts,
            )
            if ok:
                applied += 1
            else:
                ignored += 1
        if bool(snapshot_is_complete):
            # Poll snapshots are authoritative for the returned venue when complete.
            # Drop missing local orders so stale pending exposure does not persist.
            prune_grace_ms = 5_000
            for client_order_id, record in list(self.orders_by_client_order_id.items()):
                if str(record.venue).strip().lower() != venue_norm:
                    continue
                if str(client_order_id) in present_order_keys:
                    continue
                last_update_ms = int(record.last_update_ms)
                if (ts - last_update_ms) < prune_grace_ms:
                    continue
                self._drop_order_record(client_order_id=str(client_order_id))
        self._counters["order_poll_updates_applied"] += int(applied)
        self._refresh_health(now_epoch_ms=ts)
        return {"applied": int(applied), "ignored": int(ignored), "deduped": False}

    def reconcile_positions_snapshot(
        self,
        *,
        venue: str,
        positions: Iterable[Dict[str, Any]],
        snapshot_id: Optional[str] = None,
        now_epoch_ms: Optional[int] = None,
    ) -> Dict[str, Any]:
        venue_norm = str(venue).strip().lower()
        if venue_norm not in {"polymarket", "kalshi"}:
            raise RuntimeError(f"Unsupported venue for reconciliation: {venue}")
        if not self._record_event_id(snapshot_id):
            return {"applied": 0, "drift_corrections": 0, "deduped": True}

        ts = int(now_epoch_ms if now_epoch_ms is not None else now_ms())
        parsed_targets: Dict[str, Dict[str, Any]] = {}
        for row in positions:
            item = as_dict(row)
            instrument_id = (
                as_non_empty_text(item.get("instrument_id"))
                or as_non_empty_text(item.get("token_id"))
                or as_non_empty_text(item.get("asset_id"))
                or ""
            )
            side = as_non_empty_text(item.get("outcome_side")) or as_non_empty_text(item.get("side")) or ""
            net_contracts = as_float(item.get("net_contracts"))
            if net_contracts is None:
                net_contracts = as_float(item.get("position"))
            if net_contracts is None:
                net_contracts = as_float(item.get("size"))
            if not instrument_id or not side:
                continue
            side_norm = side.strip().lower()
            if side_norm not in {"yes", "no"}:
                continue
            key = _position_key(venue=venue_norm, instrument_id=instrument_id, outcome_side=side_norm)
            if key not in self._tracked_keys:
                continue
            parsed_targets[key] = {
                "net_contracts": float(net_contracts or 0.0),
                "avg_entry_price": (
                    as_float(item.get("avg_entry_price"))
                    or as_float(item.get("avg_price"))
                ),
                "position_exposure_usd": (
                    as_float(item.get("position_exposure_usd"))
                    or as_float(item.get("market_exposure_dollars"))
                    or as_float(item.get("initial_value"))
                    or as_float(item.get("initialValue"))
                ),
            }

        venue_keys = [key for key in self._tracked_keys if key.startswith(f"{venue_norm}|")]
        applied = 0
        drift_corrections = 0
        tolerance = max(0.0, float(self.config.drift_tolerance_contracts))
        for key in venue_keys:
            rec = self.positions_by_key[key]
            target = parsed_targets.get(
                key,
                {
                    "net_contracts": 0.0,
                    "avg_entry_price": None,
                    "position_exposure_usd": None,
                },
            )
            prior_net = float(rec.net_contracts)
            target_net = float(target.get("net_contracts") or 0.0)
            if abs(prior_net - target_net) > tolerance:
                drift_corrections += 1
                self._counters["drift_corrections"] += 1
            rec.net_contracts = target_net
            target_exposure = as_float(target.get("position_exposure_usd"))
            target_avg = as_float(target.get("avg_entry_price"))
            if target_avg is None and target_exposure is not None and target_net > 0.0:
                inferred_avg = float(target_exposure) / float(target_net)
                if 0.0 < inferred_avg <= 1.0:
                    target_avg = inferred_avg
            rec.avg_entry_price = target_avg
            rec.position_exposure_usd = target_exposure
            rec.last_update_ms = ts
            rec.last_source = "positions_poll"
            rec.is_authoritative = True
            applied += 1

        state = self._venue_state[venue_norm]
        state["bootstrap_ok"] = True
        state["last_reconcile_success_ms"] = ts
        state["last_error"] = None
        self._refresh_health(now_epoch_ms=ts)
        return {"applied": int(applied), "drift_corrections": int(drift_corrections), "deduped": False}

    def mark_reconcile_failure(self, *, venue: str, error: str, now_epoch_ms: Optional[int] = None) -> None:
        venue_norm = str(venue).strip().lower()
        if venue_norm not in self._venue_state:
            return
        ts = int(now_epoch_ms if now_epoch_ms is not None else now_ms())
        state = self._venue_state[venue_norm]
        state["last_reconcile_failure_ms"] = ts
        state["last_error"] = str(error)
        self._refresh_health(now_epoch_ms=ts)

    def refresh_health(self, *, now_epoch_ms: Optional[int] = None) -> Dict[str, Any]:
        self._refresh_health(now_epoch_ms=now_epoch_ms)
        return dict(self.health)

    def position(self, *, venue: str, instrument_id: str, outcome_side: str) -> Dict[str, Any]:
        key = _position_key(venue=venue, instrument_id=instrument_id, outcome_side=outcome_side)
        rec = self.positions_by_key.get(key)
        return dataclass_asdict(rec) if rec else {}

    def snapshot(self, *, now_epoch_ms: Optional[int] = None) -> Dict[str, Any]:
        self._refresh_health(now_epoch_ms=now_epoch_ms)
        return {
            "positions_by_key": {key: dataclass_asdict(value) for key, value in self.positions_by_key.items()},
            "orders_by_client_order_id": {key: dataclass_asdict(value) for key, value in self.orders_by_client_order_id.items()},
            "counters": dict(self._counters),
            "health": dict(self.health),
        }
