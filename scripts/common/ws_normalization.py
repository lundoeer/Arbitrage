from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple


def to_float(raw: Any) -> Optional[float]:
    if raw is None:
        return None
    try:
        return float(raw)
    except Exception:
        return None


def to_prob(raw: Any) -> Optional[float]:
    value = to_float(raw)
    if value is None:
        return None
    if value > 1.0:
        value = value / 100.0
    if value < 0.0:
        return None
    return max(0.0, min(1.0, float(value)))


def to_epoch_ms(raw: Any) -> Optional[int]:
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None

    # Numeric epoch (seconds or milliseconds).
    try:
        number = float(text)
        value = int(number)
        return value * 1000 if value < 10_000_000_000 else value
    except Exception:
        pass

    # ISO datetime (e.g. 2026-02-18T09:01:21.666321Z).
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


def normalize_ladder(levels: Any, *, side: str, max_levels: Optional[int] = None) -> List[List[float]]:
    parsed: List[Tuple[float, float]] = []
    for level in levels or []:
        if isinstance(level, dict):
            raw_price = level.get("price")
            raw_size = level.get("size")
        elif isinstance(level, (list, tuple)) and len(level) >= 2:
            raw_price = level[0]
            raw_size = level[1]
        else:
            continue

        price = to_float(raw_price)
        size = to_float(raw_size)
        if price is None or size is None:
            continue
        if price > 1.0:
            price = price / 100.0
        if price < 0.0 or price > 1.0 or size <= 0:
            continue
        parsed.append((float(price), float(size)))

    parsed.sort(key=lambda x: x[0], reverse=(side == "bid"))
    if max_levels is not None and max_levels > 0:
        parsed = parsed[: int(max_levels)]
    return [[round(price, 6), round(size, 6)] for price, size in parsed]


def normalize_polymarket_event(message: Dict[str, Any], recv_ms: int) -> Iterable[Dict[str, Any]]:
    event_type = str(message.get("event_type") or message.get("type") or "").lower()
    asset_id = str(message.get("asset_id") or "")
    source_ts = to_float(message.get("timestamp"))
    source_ts_ms = int(source_ts) if source_ts is not None else None
    lag_ms = recv_ms - source_ts_ms if source_ts_ms is not None else None

    # Snapshot-like book message.
    if asset_id and ("bids" in message or "asks" in message or "buys" in message or "sells" in message):
        bids = message.get("bids") or message.get("buys") or []
        asks = message.get("asks") or message.get("sells") or []
        normalized_bids = normalize_ladder(bids, side="bid", max_levels=50)
        normalized_asks = normalize_ladder(asks, side="ask", max_levels=50)
        best_bid = normalized_bids[0][0] if normalized_bids else None
        best_bid_size = normalized_bids[0][1] if normalized_bids else None
        best_ask = normalized_asks[0][0] if normalized_asks else None
        best_ask_size = normalized_asks[0][1] if normalized_asks else None
        yield {
            "kind": "book_snapshot",
            "asset_id": asset_id,
            "best_bid": best_bid,
            "best_bid_size": best_bid_size,
            "best_ask": best_ask,
            "best_ask_size": best_ask_size,
            "bids": normalized_bids,
            "asks": normalized_asks,
            "source_event_type": event_type or "book",
            "source_timestamp_ms": source_ts_ms,
            "lag_ms": lag_ms,
        }
        return

    # Best bid/ask top-of-book update (custom feature on Polymarket).
    if event_type == "best_bid_ask":
        if not asset_id:
            return
        yield {
            "kind": "book_best_bid_ask",
            "asset_id": asset_id,
            "best_bid": to_float(message.get("best_bid")),
            "best_ask": to_float(message.get("best_ask")),
            "spread": to_float(message.get("spread")),
            "source_event_type": event_type,
            "source_timestamp_ms": source_ts_ms,
            "lag_ms": lag_ms,
        }
        return

    # Incremental best-price updates.
    if event_type == "price_change":
        for update in message.get("price_changes") or []:
            if not isinstance(update, dict):
                continue
            aid = str(update.get("asset_id") or "")
            if not aid:
                continue
            yield {
                "kind": "book_top_update",
                "asset_id": aid,
                "best_bid": to_float(update.get("best_bid")),
                "best_ask": to_float(update.get("best_ask")),
                "changed_side": "bid" if str(update.get("side") or "").strip().upper() == "BUY" else "ask",
                "changed_price": to_float(update.get("price")),
                "changed_size": to_float(update.get("size")),
                "best_bid_size": None,
                "best_ask_size": None,
                "source_event_type": event_type,
                "source_timestamp_ms": source_ts_ms,
                "lag_ms": lag_ms,
            }
        return

    # Last trade print update.
    if event_type == "last_trade_price":
        side_text = str(message.get("side") or "").strip().upper()
        if side_text == "BUY":
            trade_side = "buy"
        elif side_text == "SELL":
            trade_side = "sell"
        else:
            trade_side = None

        yield {
            "kind": "last_trade_price",
            "asset_id": asset_id or None,
            "trade_side": trade_side,
            "trade_price": to_float(message.get("price")),
            "trade_size": to_float(message.get("size")),
            "fee_rate_bps": to_float(message.get("fee_rate_bps")),
            "source_event_type": event_type,
            "source_timestamp_ms": source_ts_ms,
            "lag_ms": lag_ms,
        }
        return

    yield {
        "kind": "control_or_unknown",
        "source_event_type": event_type or "unknown",
        "asset_id": asset_id or None,
    }


def normalize_kalshi_event(message: Dict[str, Any], recv_ms: int, *, market_ticker: str) -> Iterable[Dict[str, Any]]:
    event_type = str(message.get("type") or message.get("event_type") or "").strip().lower()
    data = message.get("msg") if isinstance(message.get("msg"), dict) else message.get("data")
    if not isinstance(data, dict):
        data = message

    source_ts_ms = to_epoch_ms(data.get("ts") or data.get("timestamp") or data.get("time"))
    lag_ms = recv_ms - source_ts_ms if source_ts_ms is not None else None

    if event_type == "ticker":
        yes_bid = to_prob(data.get("yes_bid_dollars") or data.get("yes_bid"))
        yes_ask = to_prob(data.get("yes_ask_dollars") or data.get("yes_ask"))
        yes_bid_size = to_float(data.get("yes_bid_size_fp") or data.get("yes_bid_size"))
        yes_ask_size = to_float(data.get("yes_ask_size_fp") or data.get("yes_ask_size"))

        no_bid = to_prob(data.get("no_bid_dollars") or data.get("no_bid"))
        no_ask = to_prob(data.get("no_ask_dollars") or data.get("no_ask"))
        no_bid_size = to_float(data.get("no_bid_size_fp") or data.get("no_bid_size"))
        no_ask_size = to_float(data.get("no_ask_size_fp") or data.get("no_ask_size"))

        # Some streams only include YES L1. Infer NO side from YES complement in that case.
        if no_bid is None and yes_ask is not None:
            no_bid = 1.0 - yes_ask
        if no_ask is None and yes_bid is not None:
            no_ask = 1.0 - yes_bid
        if no_bid_size is None and yes_ask_size is not None:
            no_bid_size = yes_ask_size
        if no_ask_size is None and yes_bid_size is not None:
            no_ask_size = yes_bid_size

        yield {
            "kind": "ticker",
            "market_ticker": market_ticker,
            "yes_bid": yes_bid,
            "yes_bid_size": yes_bid_size,
            "yes_ask": yes_ask,
            "yes_ask_size": yes_ask_size,
            "no_bid": no_bid,
            "no_bid_size": no_bid_size,
            "no_ask": no_ask,
            "no_ask_size": no_ask_size,
            "source_timestamp_ms": source_ts_ms,
            "lag_ms": lag_ms,
        }
        return

    if event_type == "trade":
        yield {
            "kind": "kalshi_trade",
            "market_ticker": market_ticker,
            "yes_price": to_prob(data.get("yes_price_dollars") or data.get("yes_price")),
            "no_price": to_prob(data.get("no_price_dollars") or data.get("no_price")),
            "count": to_float(data.get("count")),
            "taker_side": str(data.get("taker_side") or "").strip().lower() or None,
            "source_timestamp_ms": source_ts_ms,
            "lag_ms": lag_ms,
        }
        return

    if "orderbook" in event_type or "delta" in event_type:
        yield {
            "kind": "orderbook_event",
            "market_ticker": market_ticker,
            "source_event_type": event_type,
            "payload": data,
            "source_timestamp_ms": source_ts_ms,
            "lag_ms": lag_ms,
        }
        return

    yield {
        "kind": "control_or_unknown",
        "market_ticker": market_ticker,
        "source_event_type": event_type or "unknown",
        "payload": data,
        "source_timestamp_ms": source_ts_ms,
        "lag_ms": lag_ms,
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


def normalize_kalshi_market_positions_event(
    message: Dict[str, Any],
    recv_ms: int,
    *,
    market_ticker: Optional[str] = None,
) -> Iterable[Dict[str, Any]]:
    event_type = str(message.get("type") or message.get("event_type") or "").strip().lower()
    data = message.get("msg") if isinstance(message.get("msg"), dict) else message.get("data")
    if not isinstance(data, dict):
        data = message

    source_ts_ms = to_epoch_ms(data.get("ts") or data.get("timestamp") or data.get("time"))
    lag_ms = recv_ms - source_ts_ms if source_ts_ms is not None else None

    if event_type != "market_position":
        yield {
            "kind": "control_or_unknown",
            "market_ticker": to_text(data.get("market_ticker")) or market_ticker,
            "source_event_type": event_type or "unknown",
            "payload": data,
            "source_timestamp_ms": source_ts_ms,
            "lag_ms": lag_ms,
        }
        return

    ticker = to_text(data.get("market_ticker")) or to_text(market_ticker)
    if market_ticker and ticker and str(ticker) != str(market_ticker):
        return

    raw_position = to_float(data.get("position_fp"))
    if raw_position is None:
        raw_position = to_float(data.get("position"))
    position = float(raw_position or 0.0)

    # Kalshi position streams encode cost/pnl fields in centi-cents (1/10_000 USD).
    def _to_usd(raw: Any) -> Optional[float]:
        value = to_float(raw)
        if value is None:
            return None
        return float(value) / 10_000.0

    yes_position = position if position > 0 else 0.0
    no_position = abs(position) if position < 0 else 0.0

    yield {
        "kind": "kalshi_market_position",
        "market_ticker": ticker,
        "user_id": to_text(data.get("user_id")),
        "position": position,
        "position_yes": float(yes_position),
        "position_no": float(no_position),
        "position_cost_usd": _to_usd(data.get("position_cost")),
        "position_fee_cost_usd": _to_usd(data.get("position_fee_cost")),
        "realized_pnl_usd": _to_usd(data.get("realized_pnl")),
        "fees_paid_usd": _to_usd(data.get("fees_paid")),
        "volume": to_float(data.get("volume_fp")) if data.get("volume_fp") is not None else to_float(data.get("volume")),
        "source_event_type": "market_position",
        "source_timestamp_ms": source_ts_ms,
        "lag_ms": lag_ms,
    }


def normalize_kalshi_user_orders_event(
    message: Dict[str, Any],
    recv_ms: int,
    *,
    market_ticker: Optional[str] = None,
) -> Iterable[Dict[str, Any]]:
    event_type = str(message.get("type") or message.get("event_type") or "").strip().lower()
    data = message.get("msg") if isinstance(message.get("msg"), dict) else message.get("data")
    if data is None:
        data = message.get("msg")
    if data is None:
        data = message

    source_ts_ms = to_epoch_ms(
        (data.get("ts") if isinstance(data, dict) else None)
        or (data.get("timestamp") if isinstance(data, dict) else None)
        or (data.get("time") if isinstance(data, dict) else None)
    )
    lag_ms = recv_ms - source_ts_ms if source_ts_ms is not None else None

    rows: List[Dict[str, Any]] = []
    if isinstance(data, dict):
        if isinstance(data.get("orders"), list):
            rows = [item for item in data.get("orders") if isinstance(item, dict)]
        elif isinstance(data.get("order"), dict):
            rows = [data.get("order")]  # type: ignore[list-item]
        elif event_type in {"order", "user_order", "user_orders", "order_update"}:
            rows = [data]
    elif isinstance(data, list):
        rows = [item for item in data if isinstance(item, dict)]

    if not rows:
        yield {
            "kind": "control_or_unknown",
            "market_ticker": to_text(data.get("market_ticker")) if isinstance(data, dict) else market_ticker,
            "source_event_type": event_type or "unknown",
            "payload": data,
            "source_timestamp_ms": source_ts_ms,
            "lag_ms": lag_ms,
        }
        return

    for item in rows:
        ticker = to_text(item.get("ticker")) or to_text(item.get("market_ticker")) or to_text(market_ticker)
        if market_ticker and ticker and str(ticker) != str(market_ticker):
            continue

        side_text = to_text(item.get("side"))
        action = to_side(item.get("action"))
        if action is None:
            action = to_side(item.get("order_side"))
        if action is None and side_text is not None and side_text.lower() in {"buy", "sell"}:
            action = side_text.lower()

        outcome_side = to_outcome_side(side_text)
        status = to_text(item.get("status")) or to_text(item.get("state")) or to_text(item.get("order_status"))
        status_upper = str(status or "").upper() if status else None

        requested_size = (
            to_float(item.get("initial_count_fp"))
            if item.get("initial_count_fp") is not None
            else to_float(item.get("initial_count"))
        )
        filled_size = (
            to_float(item.get("fill_count_fp"))
            if item.get("fill_count_fp") is not None
            else to_float(item.get("fill_count"))
        )
        remaining_size = (
            to_float(item.get("remaining_count_fp"))
            if item.get("remaining_count_fp") is not None
            else to_float(item.get("remaining_count"))
        )
        if remaining_size is None and requested_size is not None and filled_size is not None:
            remaining_size = max(0.0, float(requested_size - filled_size))

        limit_price = (
            to_prob(item.get("yes_price_dollars"))
            if outcome_side == "yes"
            else to_prob(item.get("no_price_dollars"))
        )
        if limit_price is None:
            limit_price = to_prob(item.get("price_dollars"))
        if limit_price is None:
            limit_price = to_prob(item.get("price"))

        order_id = to_text(item.get("order_id")) or to_text(item.get("id"))
        client_order_id = to_text(item.get("client_order_id"))
        event_id = (
            to_text(item.get("event_id"))
            or to_text(item.get("id"))
            or f"kx_user_order:{order_id or client_order_id or 'na'}:{source_ts_ms or recv_ms}"
        )

        yield {
            "kind": "kalshi_user_order",
            "event_id": event_id,
            "market_ticker": ticker,
            "order_id": order_id,
            "client_order_id": client_order_id,
            "outcome_side": outcome_side,
            "action": action,
            "status": status_upper,
            "requested_size": requested_size,
            "filled_size": filled_size,
            "remaining_size": remaining_size,
            "limit_price": limit_price,
            "source_event_type": event_type or "user_orders",
            "source_timestamp_ms": source_ts_ms,
            "lag_ms": lag_ms,
            "payload": item,
        }
