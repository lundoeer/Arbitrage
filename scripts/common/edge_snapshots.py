"""Cross-venue gross edge snapshot computation."""

from __future__ import annotations

from typing import Any, Dict, Optional

from scripts.common.utils import as_dict, as_float


def extract_leg_quote(quotes: Dict[str, Any], leg_name: str, field_name: str) -> Optional[float]:
    """Extract a single quote field from a nested legs dict."""
    legs = as_dict(quotes.get("legs"))
    leg = as_dict(legs.get(leg_name))
    return as_float(leg.get(field_name))


def cross_venue_edge(
    *,
    quotes: Dict[str, Any],
    yes_leg: str,
    no_leg: str,
    price_field: str,
) -> Dict[str, Any]:
    """Compute the gross edge for one cross-venue candidate pair."""
    yes_px = extract_leg_quote(quotes, yes_leg, price_field)
    no_px = extract_leg_quote(quotes, no_leg, price_field)
    if yes_px is None or no_px is None:
        return {
            "yes_leg": yes_leg,
            "no_leg": no_leg,
            "yes_price": yes_px,
            "no_price": no_px,
            "total_price": None,
            "gross_edge": None,
        }
    total = float(yes_px + no_px)
    return {
        "yes_leg": yes_leg,
        "no_leg": no_leg,
        "yes_price": yes_px,
        "no_price": no_px,
        "total_price": total,
        "gross_edge": float(1.0 - total),
    }


def build_edge_snapshot(
    *,
    now_epoch_ms: int,
    market_context: Dict[str, Any],
    market_window_end_epoch_ms: Optional[int],
    market_duration_seconds: int,
    min_gross_edge_threshold: float,
    quotes: Dict[str, Any],
) -> Dict[str, Any]:
    """Build a full edge snapshot with ask- and bid-side gross edges."""
    duration_ms = int(max(1, int(market_duration_seconds)) * 1000)
    age_seconds = None
    if market_window_end_epoch_ms is not None:
        market_start_ms = int(market_window_end_epoch_ms) - duration_ms
        age_seconds = max(0, int((int(now_epoch_ms) - int(market_start_ms)) / 1000))

    ask_a = cross_venue_edge(
        quotes=quotes,
        yes_leg="polymarket_yes",
        no_leg="kalshi_no",
        price_field="best_ask",
    )
    ask_b = cross_venue_edge(
        quotes=quotes,
        yes_leg="kalshi_yes",
        no_leg="polymarket_no",
        price_field="best_ask",
    )
    bid_a = cross_venue_edge(
        quotes=quotes,
        yes_leg="polymarket_yes",
        no_leg="kalshi_no",
        price_field="best_bid",
    )
    bid_b = cross_venue_edge(
        quotes=quotes,
        yes_leg="kalshi_yes",
        no_leg="polymarket_no",
        price_field="best_bid",
    )

    def _with_threshold(payload: Dict[str, Any]) -> Dict[str, Any]:
        edge = as_float(payload.get("gross_edge"))
        return {
            **payload,
            "meets_threshold": bool(edge is not None and edge >= float(min_gross_edge_threshold)),
        }

    return {
        "marketpair": {
            "polymarket_event_slug": market_context.get("polymarket_event_slug"),
            "polymarket_market_id": market_context.get("polymarket_market_id"),
            "kalshi_ticker": market_context.get("kalshi_ticker"),
        },
        "marketpair_age_in_seconds": age_seconds,
        "gross_edge_thresholds": {
            "min_gross_edge_threshold": float(min_gross_edge_threshold),
            "buy_polymarket_yes_and_kalshi_no": _with_threshold(ask_a),
            "buy_kalshi_yes_and_polymarket_no": _with_threshold(ask_b),
        },
        "gross_edge_thresholds_bids": {
            "min_gross_edge_threshold": float(min_gross_edge_threshold),
            "buy_polymarket_yes_and_kalshi_no": _with_threshold(bid_a),
            "buy_kalshi_yes_and_polymarket_no": _with_threshold(bid_b),
        },
    }
