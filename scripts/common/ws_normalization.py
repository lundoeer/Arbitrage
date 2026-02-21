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
