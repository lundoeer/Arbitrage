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
    submitted: bool
    status: str
    error: Optional[str] = None
    order_id: Optional[str] = None
    first_seen_ms: int = 0
    last_update_ms: int = 0


class PositionRuntime:
    """
    In-memory position monitor for the currently selected pair.

    Scope is intentionally narrow for phase 1:
    - tracks only the active Polymarket YES/NO tokens + Kalshi YES/NO sides
    - merges submit-ack order state
    - applies confirmed fill deltas
    - reconciles from authoritative snapshots
    - maintains health gate `buy_execution_allowed`
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
        self._counters: Dict[str, int] = {
            "deduped_events": 0,
            "drift_corrections": 0,
            "order_submit_acks": 0,
            "confirmed_fills_applied": 0,
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
        buy_execution_allowed = True
        if bool(self.config.require_bootstrap_before_buy) and not bootstrap_completed:
            buy_execution_allowed = False
        if hard_stale:
            buy_execution_allowed = False

        self.health = {
            "buy_execution_allowed": bool(buy_execution_allowed),
            "require_bootstrap_before_buy": bool(self.config.require_bootstrap_before_buy),
            "bootstrap_completed": bool(bootstrap_completed),
            "hard_stale": bool(hard_stale),
            "drift_corrections": int(self._counters["drift_corrections"]),
            "deduped_events": int(self._counters["deduped_events"]),
            "venues": {
                "polymarket": dict(self._venue_state["polymarket"]),
                "kalshi": dict(self._venue_state["kalshi"]),
            },
        }

    @staticmethod
    def _extract_order_id(response_payload: Dict[str, Any]) -> Optional[str]:
        response = as_dict(response_payload)
        nested = as_dict(response.get("response"))
        for container in (response, nested):
            for key in ("order_id", "orderId", "id"):
                value = as_non_empty_text(container.get(key))
                if value:
                    return value
        return None

    @staticmethod
    def _extract_filled_size(response_payload: Dict[str, Any]) -> Optional[float]:
        response = as_dict(response_payload)
        nested = as_dict(response.get("response"))
        for container in (response, nested):
            for key in (
                "filled",
                "filled_size",
                "filled_count",
                "filled_qty",
                "filled_quantity",
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
        for container in (response, nested):
            for key in ("fill_price", "filled_price", "avg_price", "average_price", "price"):
                value = as_float(container.get(key))
                if value is not None and value > 0:
                    return float(value)
        return None

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
        rec.last_update_ms = ts
        rec.last_source = "positions_ws"
        rec.is_authoritative = True
        self._refresh_health(now_epoch_ms=ts)
        return True

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
            submitted = bool(leg.get("submitted"))
            error = as_non_empty_text(leg.get("error"))

            prior = self.orders_by_client_order_id.get(client_order_id)
            if prior is None:
                first_seen_ms = ts
            else:
                first_seen_ms = int(prior.first_seen_ms)

            status = "submitted" if submitted else "submit_error"
            self.orders_by_client_order_id[client_order_id] = OrderState(
                client_order_id=client_order_id,
                signal_id=signal_id,
                venue=venue,
                instrument_id=instrument_id,
                outcome_side=side,
                submitted=bool(submitted),
                status=status,
                error=error,
                order_id=self._extract_order_id(response_payload),
                first_seen_ms=first_seen_ms,
                last_update_ms=ts,
            )
            accepted += 1
            self._counters["order_submit_acks"] += 1

            # Apply only if venue explicitly returns filled size in submit response.
            filled = self._extract_filled_size(response_payload)
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
                "avg_entry_price": as_float(item.get("avg_entry_price")) or as_float(item.get("avg_price")),
            }

        venue_keys = [key for key in self._tracked_keys if key.startswith(f"{venue_norm}|")]
        applied = 0
        drift_corrections = 0
        tolerance = max(0.0, float(self.config.drift_tolerance_contracts))
        for key in venue_keys:
            rec = self.positions_by_key[key]
            target = parsed_targets.get(key, {"net_contracts": 0.0, "avg_entry_price": None})
            prior_net = float(rec.net_contracts)
            target_net = float(target.get("net_contracts") or 0.0)
            if abs(prior_net - target_net) > tolerance:
                drift_corrections += 1
                self._counters["drift_corrections"] += 1
            rec.net_contracts = target_net
            rec.avg_entry_price = as_float(target.get("avg_entry_price"))
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
