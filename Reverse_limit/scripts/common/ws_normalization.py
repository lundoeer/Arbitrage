from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional


def to_float(raw: Any) -> Optional[float]:
    if raw is None:
        return None
    try:
        return float(raw)
    except Exception:
        return None


def to_epoch_ms(raw: Any) -> Optional[int]:
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None

    try:
        number = float(text)
        value = int(number)
        return value * 1000 if value < 10_000_000_000 else value
    except Exception:
        pass

    iso = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        dt = datetime.fromisoformat(iso)
    except Exception:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def to_text(raw: Any) -> Optional[str]:
    if raw is None:
        return None
    text = str(raw).strip()
    return text if text else None


def to_side(raw: Any) -> Optional[str]:
    text = to_text(raw)
    if not text:
        return None
    lowered = text.lower()
    if lowered in {"buy", "sell"}:
        return lowered
    return None


def to_outcome_side(raw: Any) -> Optional[str]:
    text = to_text(raw)
    if not text:
        return None
    lowered = text.lower()
    if lowered in {"yes", "no"}:
        return lowered
    return None


def _parse_outcomes_list(raw: Any) -> List[str]:
    if isinstance(raw, list):
        return [str(item).strip() for item in raw if str(item).strip()]
    if isinstance(raw, str):
        text = str(raw).strip()
        if not text:
            return []
        try:
            parsed = json.loads(text)
        except Exception:
            return []
        if isinstance(parsed, list):
            return [str(item).strip() for item in parsed if str(item).strip()]
    return []


def _to_index(raw: Any) -> Optional[int]:
    value = to_float(raw)
    if value is None:
        return None
    try:
        idx = int(value)
    except Exception:
        return None
    if idx < 0:
        return None
    return int(idx)


def normalize_polymarket_market_resolution_event(message: Dict[str, Any], recv_ms: int) -> Iterable[Dict[str, Any]]:
    event_type = str(message.get("event_type") or message.get("type") or "").strip().lower()
    source_ts_ms = to_epoch_ms(
        message.get("last_update")
        or message.get("timestamp")
        or message.get("updatedAt")
        or message.get("matchtime")
    )
    lag_ms = recv_ms - source_ts_ms if source_ts_ms is not None else None

    condition_id = to_text(message.get("condition_id")) or to_text(message.get("conditionId")) or to_text(message.get("market"))
    if not condition_id:
        return

    resolution_raw = (
        to_text(message.get("resolved_outcome"))
        or to_text(message.get("winner"))
        or to_text(message.get("resolution"))
        or to_text(message.get("result"))
        or to_text(message.get("outcome"))
    )
    outcome = to_outcome_side(resolution_raw)

    if outcome is None:
        idx = (
            _to_index(message.get("outcomeIndex"))
            or _to_index(message.get("winnerIndex"))
            or _to_index(message.get("resolution_index"))
        )
        outcomes = _parse_outcomes_list(message.get("outcomes"))
        if idx is not None and outcomes and idx < len(outcomes):
            outcome = to_outcome_side(outcomes[idx])

    resolution_status = (
        to_text(message.get("umaResolutionStatus"))
        or to_text(message.get("resolution_status"))
        or to_text(message.get("status"))
    )
    status_norm = str(resolution_status or "").strip().lower()
    is_resolved = bool(
        message.get("resolved") is True
        or status_norm in {"resolved", "finalized", "settled", "closed"}
        or event_type in {"resolution", "market_resolution", "market_resolved", "resolved"}
    )

    if outcome not in {"yes", "no"} and not is_resolved:
        return

    yield {
        "kind": "polymarket_market_resolution",
        "condition_id": str(condition_id),
        "outcome": outcome if outcome in {"yes", "no"} else None,
        "is_resolved": bool(is_resolved),
        "resolution_status": status_norm or None,
        "source_event_type": event_type or "resolution",
        "source_timestamp_ms": source_ts_ms,
        "lag_ms": lag_ms,
        "raw": message,
    }


def normalize_polymarket_user_event(
    message: Dict[str, Any],
    recv_ms: int,
    *,
    condition_id: Optional[str] = None,
) -> Iterable[Dict[str, Any]]:
    event_type = str(message.get("event_type") or message.get("type") or "").strip().lower()
    market = str(message.get("market") or "").strip() or None
    if condition_id:
        expected = str(condition_id).strip()
        if expected and market and market != expected:
            return

    source_ts_ms = to_epoch_ms(message.get("last_update") or message.get("timestamp") or message.get("matchtime"))
    lag_ms = recv_ms - source_ts_ms if source_ts_ms is not None else None

    if event_type == "trade":
        trade_status = to_text(message.get("status"))
        trade_status_upper = str(trade_status or "").upper() if trade_status else None
        yield {
            "kind": "polymarket_user_trade",
            "event_id": to_text(message.get("id")),
            "market": market,
            "asset_id": to_text(message.get("asset_id")),
            "outcome_side": to_outcome_side(message.get("outcome")),
            "order_side": to_side(message.get("side")),
            "price": to_float(message.get("price")),
            "size": to_float(message.get("size")),
            "status": trade_status_upper,
            "is_confirmed": bool(trade_status_upper == "CONFIRMED"),
            "trade_owner": to_text(message.get("trade_owner")) or to_text(message.get("owner")),
            "taker_order_id": to_text(message.get("taker_order_id")),
            "maker_orders": list(message.get("maker_orders") or []) if isinstance(message.get("maker_orders"), list) else [],
            "source_event_type": "trade",
            "source_timestamp_ms": source_ts_ms,
            "lag_ms": lag_ms,
        }
        return

    if event_type == "order":
        order_event_type = to_text(message.get("type"))
        original_size = to_float(message.get("original_size"))
        size_matched = to_float(message.get("size_matched"))
        remaining_size = to_float(message.get("remaining_size"))
        if remaining_size is None and original_size is not None and size_matched is not None:
            remaining_size = max(0.0, float(original_size - size_matched))
        yield {
            "kind": "polymarket_user_order",
            "event_id": to_text(message.get("id")),
            "order_id": to_text(message.get("id")) or to_text(message.get("order_id")),
            "client_order_id": to_text(message.get("client_order_id")),
            "market": market,
            "asset_id": to_text(message.get("asset_id")),
            "outcome_side": to_outcome_side(message.get("outcome")),
            "order_side": to_side(message.get("side")),
            "price": to_float(message.get("price")),
            "original_size": original_size,
            "size_matched": size_matched,
            "remaining_size": remaining_size,
            "status": str(order_event_type or "").upper() or None,
            "order_owner": to_text(message.get("order_owner")) or to_text(message.get("owner")),
            "associate_trades": (
                list(message.get("associate_trades") or [])
                if isinstance(message.get("associate_trades"), list)
                else []
            ),
            "source_event_type": "order",
            "source_timestamp_ms": source_ts_ms,
            "lag_ms": lag_ms,
        }
        return

    yield {
        "kind": "control_or_unknown",
        "source_event_type": event_type or "unknown",
        "market": market,
        "source_timestamp_ms": source_ts_ms,
        "lag_ms": lag_ms,
    }
