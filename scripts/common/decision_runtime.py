from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import math
import time
from typing import Any, Dict, List, Optional

from scripts.common.normalized_books import NormalizedBookRuntime
from scripts.common.run_config import BuyDecisionConfig, DecisionConfig, ExecutionDecisionConfig
from scripts.common.utils import (
    as_dict as _as_dict,
    as_float as _as_float_number,
    as_int as _as_int_number,
    as_non_empty_text as _as_non_empty_text,
)


def _signal_segment(value: str, *, max_len: int = 24) -> str:
    raw = str(value or "").strip().lower()
    cooked = "".join(ch if ch.isalnum() else "-" for ch in raw).strip("-")
    if not cooked:
        return "x"
    return cooked[: max(1, int(max_len))]


_MARKET_EMULATION_OFFSET = 0.02
_MARKET_EMULATION_MAX_PRICE = 0.99


def _execution_leg_from_name(
    *,
    leg_name: str,
    leg_quote: Dict[str, Any],
    market_context: Dict[str, Any],
    size: float,
) -> Optional[Dict[str, Any]]:
    ask_price = _as_float_number(leg_quote.get("best_ask"))
    if ask_price is None or ask_price <= 0.0:
        return None

    name = str(leg_name or "").strip().lower()
    if name == "polymarket_yes":
        instrument_id = _as_non_empty_text(market_context.get("polymarket_token_yes"))
        venue = "polymarket"
        side = "yes"
    elif name == "polymarket_no":
        instrument_id = _as_non_empty_text(market_context.get("polymarket_token_no"))
        venue = "polymarket"
        side = "no"
    elif name == "kalshi_yes":
        instrument_id = _as_non_empty_text(market_context.get("kalshi_ticker"))
        venue = "kalshi"
        side = "yes"
    elif name == "kalshi_no":
        instrument_id = _as_non_empty_text(market_context.get("kalshi_ticker"))
        venue = "kalshi"
        side = "no"
    else:
        return None

    if instrument_id is None:
        return None

    limit_price = None
    metadata: Dict[str, Any] = {
        "planning_reference_best_ask": float(ask_price),
    }
    if venue in {"polymarket", "kalshi"}:
        emulation_limit_price = min(
            _MARKET_EMULATION_MAX_PRICE,
            float(ask_price + _MARKET_EMULATION_OFFSET),
        )
        limit_price = round(float(emulation_limit_price), 6)
        metadata["market_emulation_offset"] = float(_MARKET_EMULATION_OFFSET)
        metadata["market_emulation_limit_price"] = float(limit_price)

    return {
        "venue": venue,
        "side": side,
        "action": "buy",
        "instrument_id": instrument_id,
        "order_kind": "market",
        "limit_price": limit_price,
        "size": float(size),
        "time_in_force": "fak",
        "client_order_id_seed": f"{name}-buy",
        "metadata": metadata,
    }


def _build_signal_id(
    *,
    candidate_name: str,
    market_context: Dict[str, Any],
    market_window_end_epoch_ms: Optional[int],
) -> str:
    window_end = _as_int_number(market_window_end_epoch_ms)
    if window_end is None:
        window_end = _as_int_number(market_context.get("market_window_end_epoch_ms"))

    identity = {
        "candidate": str(candidate_name or ""),
        "polymarket_event_slug": _as_non_empty_text(market_context.get("polymarket_event_slug")),
        "polymarket_market_id": _as_non_empty_text(market_context.get("polymarket_market_id")),
        "polymarket_token_yes": _as_non_empty_text(market_context.get("polymarket_token_yes")),
        "polymarket_token_no": _as_non_empty_text(market_context.get("polymarket_token_no")),
        "kalshi_ticker": _as_non_empty_text(market_context.get("kalshi_ticker")),
        "market_window_end_epoch_ms": window_end,
    }
    digest = hashlib.sha256(
        json.dumps(identity, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()[:12]
    candidate_key = _signal_segment(candidate_name, max_len=22)
    window_key = str(int(window_end)) if window_end is not None else "na"
    return f"sig-{window_key}-{candidate_key}-{digest}"


def _build_execution_plan(
    *,
    buy_signal: Dict[str, Any],
    quotes: Dict[str, Any],
    market_context: Dict[str, Any],
    decision_config: DecisionConfig,
    now_epoch_ms: Optional[int],
    market_window_end_epoch_ms: Optional[int],
) -> tuple[Optional[Dict[str, Any]], List[str]]:
    candidate = _as_dict(buy_signal.get("best_candidate"))
    candidate_name = _as_non_empty_text(candidate.get("name"))
    yes_leg_name = _as_non_empty_text(candidate.get("yes_leg"))
    no_leg_name = _as_non_empty_text(candidate.get("no_leg"))
    if candidate_name is None:
        return None, ["missing_best_candidate_name"]
    if yes_leg_name is None:
        return None, ["missing_best_candidate_yes_leg"]
    if no_leg_name is None:
        return None, ["missing_best_candidate_no_leg"]

    quote_legs = _as_dict(quotes.get("legs"))
    yes_quote = _as_dict(quote_legs.get(yes_leg_name))
    no_quote = _as_dict(quote_legs.get(no_leg_name))
    yes_ask = _as_float_number(yes_quote.get("best_ask"))
    no_ask = _as_float_number(no_quote.get("best_ask"))
    yes_ask_size = _as_float_number(yes_quote.get("best_ask_size"))
    no_ask_size = _as_float_number(no_quote.get("best_ask_size"))
    reasons: List[str] = []
    if yes_ask is None or yes_ask <= 0.0:
        reasons.append(f"missing_or_invalid_best_ask:{yes_leg_name}")
    if no_ask is None or no_ask <= 0.0:
        reasons.append(f"missing_or_invalid_best_ask:{no_leg_name}")
    if yes_ask_size is None or yes_ask_size <= 0.0:
        reasons.append(f"missing_or_invalid_best_ask_size:{yes_leg_name}")
    if no_ask_size is None or no_ask_size <= 0.0:
        reasons.append(f"missing_or_invalid_best_ask_size:{no_leg_name}")
    total_ask = _as_float_number(candidate.get("total_ask"))
    if total_ask is None and yes_ask is not None and no_ask is not None:
        total_ask = float(yes_ask + no_ask)
    if total_ask is None or total_ask <= 0.0:
        reasons.append("missing_or_invalid_total_ask")
    if reasons:
        return None, reasons
    assert total_ask is not None
    assert yes_ask_size is not None
    assert no_ask_size is not None
    total_ask_value = float(total_ask)

    max_spend = float(decision_config.buy.max_spend_per_market_usd)
    if max_spend <= 0.0:
        return None, ["invalid_max_spend_per_market_usd"]
    max_size_cap_per_leg = float(decision_config.buy.max_size_cap_per_leg)
    if max_size_cap_per_leg <= 0.0:
        return None, ["invalid_max_size_cap_per_leg"]
    min_size_per_leg_contracts = float(decision_config.buy.min_size_per_leg_contracts)
    if min_size_per_leg_contracts < 0.0:
        return None, ["invalid_min_size_per_leg_contracts"]
    min_notional_per_leg_usd = float(decision_config.buy.min_notional_per_leg_usd)
    if min_notional_per_leg_usd < 0.0:
        return None, ["invalid_min_notional_per_leg_usd"]
    best_ask_size_safety_factor = float(decision_config.buy.best_ask_size_safety_factor)
    if best_ask_size_safety_factor <= 0.0 or best_ask_size_safety_factor > 1.0:
        return None, ["invalid_best_ask_size_safety_factor"]

    # Buy size is capped by spend, explicit size cap, and top-of-book liquidity
    # after applying a safety factor to visible ask size.
    top_ask_size_min = float(min(float(yes_ask_size), float(no_ask_size)))
    cap_by_spend = float(max_spend / total_ask_value)
    cap_by_config_size = float(max_size_cap_per_leg)
    cap_by_top_ask_after_safety_factor = float(top_ask_size_min * best_ask_size_safety_factor)
    if cap_by_top_ask_after_safety_factor <= 0.0:
        return None, ["insufficient_top_of_book_after_safety_factor"]
    raw_size = min(cap_by_spend, cap_by_config_size, cap_by_top_ask_after_safety_factor)
    if raw_size <= 0.0:
        return None, ["computed_non_positive_size"]
    size_whole = int(math.floor(float(raw_size)))
    if size_whole <= 0:
        if cap_by_top_ask_after_safety_factor < 1.0:
            return None, ["insufficient_top_of_book_after_safety_factor"]
        return None, ["computed_size_below_one_contract"]
    size = float(size_whole)
    if min_size_per_leg_contracts > 0.0 and size < min_size_per_leg_contracts:
        return None, ["computed_size_below_min_size_per_leg_contracts"]
    yes_leg_notional_usd = float(yes_ask * size)
    no_leg_notional_usd = float(no_ask * size)
    notional_reasons: List[str] = []
    if min_notional_per_leg_usd > 0.0:
        if yes_leg_notional_usd < min_notional_per_leg_usd:
            notional_reasons.append(f"computed_notional_below_min_notional_per_leg_usd:{yes_leg_name}")
        if no_leg_notional_usd < min_notional_per_leg_usd:
            notional_reasons.append(f"computed_notional_below_min_notional_per_leg_usd:{no_leg_name}")
    if notional_reasons:
        return None, notional_reasons

    yes_leg = _execution_leg_from_name(
        leg_name=yes_leg_name,
        leg_quote=yes_quote,
        market_context=market_context,
        size=size,
    )
    no_leg = _execution_leg_from_name(
        leg_name=no_leg_name,
        leg_quote=no_quote,
        market_context=market_context,
        size=size,
    )
    if yes_leg is None:
        reasons.append(f"could_not_build_execution_leg:{yes_leg_name}")
    if no_leg is None:
        reasons.append(f"could_not_build_execution_leg:{no_leg_name}")
    if reasons:
        return None, reasons
    assert yes_leg is not None
    assert no_leg is not None
    yes_leg_payload: Dict[str, Any] = yes_leg
    no_leg_payload: Dict[str, Any] = no_leg

    quote_ages: List[int] = []
    for quote in (yes_quote, no_quote):
        age = _as_int_number(quote.get("quote_age_ms"))
        if age is not None and age >= 0:
            quote_ages.append(int(age))
    max_quote_age_ms = max(quote_ages) if quote_ages else None

    created_at_ms = int(now_epoch_ms if now_epoch_ms is not None else int(time.time() * 1000))
    window_end = _as_int_number(market_window_end_epoch_ms)
    if window_end is None:
        window_end = _as_int_number(market_context.get("market_window_end_epoch_ms"))

    plan = {
        "signal_id": _build_signal_id(
            candidate_name=candidate_name,
            market_context=market_context,
            market_window_end_epoch_ms=window_end,
        ),
        "market": {
            "polymarket_event_slug": _as_non_empty_text(market_context.get("polymarket_event_slug")),
            "polymarket_market_id": _as_non_empty_text(market_context.get("polymarket_market_id")),
            "polymarket_token_yes": _as_non_empty_text(market_context.get("polymarket_token_yes")),
            "polymarket_token_no": _as_non_empty_text(market_context.get("polymarket_token_no")),
            "kalshi_ticker": _as_non_empty_text(market_context.get("kalshi_ticker")),
            "market_window_end_epoch_ms": window_end,
            "candidate_name": candidate_name,
        },
        "created_at_ms": created_at_ms,
        "execution_mode": "two_leg_parallel",
        "legs": [yes_leg_payload, no_leg_payload],
        "max_quote_age_ms": max_quote_age_ms,
        "policy": {
            "planner_version": "decision_runtime_v1",
            "sizing_mode": "top_of_book_budget_caps_with_min_thresholds",
            "max_spend_per_market_usd": max_spend,
            "max_size_cap_per_leg": max_size_cap_per_leg,
            "best_ask_size_safety_factor": best_ask_size_safety_factor,
            "min_size_per_leg_contracts": min_size_per_leg_contracts,
            "min_notional_per_leg_usd": min_notional_per_leg_usd,
            "inputs": {
                "yes_best_ask": float(yes_ask),
                "no_best_ask": float(no_ask),
                "yes_best_ask_size": float(yes_ask_size),
                "no_best_ask_size": float(no_ask_size),
                "top_ask_size_min": float(top_ask_size_min),
            },
            "computed_caps": {
                "cap_by_spend": float(cap_by_spend),
                "cap_by_config_size": float(cap_by_config_size),
                "cap_by_top_ask_after_safety_factor": float(cap_by_top_ask_after_safety_factor),
            },
            "size_rounding": "floor_to_whole_contract",
            "raw_size_before_rounding": float(raw_size),
            "final_size": float(size),
            "final_leg_notional_usd": {
                "yes_leg_notional_usd": float(yes_leg_notional_usd),
                "no_leg_notional_usd": float(no_leg_notional_usd),
            },
        },
    }
    return plan, []


def validate_leg_quote(leg: Dict[str, Any]) -> Dict[str, Any]:
    bid = _as_float_number(leg.get("best_bid"))
    ask = _as_float_number(leg.get("best_ask"))
    bid_size = _as_float_number(leg.get("best_bid_size"))
    ask_size = _as_float_number(leg.get("best_ask_size"))
    reasons: List[str] = []

    if bid is None:
        reasons.append("missing_best_bid")
    if ask is None:
        reasons.append("missing_best_ask")
    if bid_size is None:
        reasons.append("missing_best_bid_size")
    if ask_size is None:
        reasons.append("missing_best_ask_size")

    if bid is not None and (bid < 0.0 or bid > 1.0):
        reasons.append("best_bid_out_of_bounds")
    if ask is not None and (ask < 0.0 or ask > 1.0):
        reasons.append("best_ask_out_of_bounds")
    if bid is not None and ask is not None and bid > ask:
        reasons.append("crossed_quote_bid_gt_ask")
    if bid_size is not None and bid_size <= 0:
        reasons.append("non_positive_best_bid_size")
    if ask_size is not None and ask_size <= 0:
        reasons.append("non_positive_best_ask_size")

    return {
        "valid": not reasons,
        "reasons": reasons,
        "best_bid": bid,
        "best_bid_size": bid_size,
        "best_ask": ask,
        "best_ask_size": ask_size,
    }


def build_quote_sanity_and_canonical(quotes: Dict[str, Any]) -> Dict[str, Any]:
    legs = _as_dict(quotes.get("legs"))
    poly_yes = validate_leg_quote(_as_dict(legs.get("polymarket_yes")))
    poly_no = validate_leg_quote(_as_dict(legs.get("polymarket_no")))
    kx_yes = validate_leg_quote(_as_dict(legs.get("kalshi_yes")))
    kx_no = validate_leg_quote(_as_dict(legs.get("kalshi_no")))

    candidates = []
    pm_yes_ask = _as_float_number(poly_yes.get("best_ask"))
    pm_no_ask = _as_float_number(poly_no.get("best_ask"))
    kx_yes_ask = _as_float_number(kx_yes.get("best_ask"))
    kx_no_ask = _as_float_number(kx_no.get("best_ask"))

    for candidate_name, yes_ask, no_ask, yes_leg, no_leg in [
        ("buy_polymarket_yes_and_kalshi_no", pm_yes_ask, kx_no_ask, "polymarket_yes", "kalshi_no"),
        ("buy_kalshi_yes_and_polymarket_no", kx_yes_ask, pm_no_ask, "kalshi_yes", "polymarket_no"),
    ]:
        total_ask = None if yes_ask is None or no_ask is None else float(yes_ask + no_ask)
        gross_edge = None if total_ask is None else float(1.0 - total_ask)
        candidates.append(
            {
                "name": candidate_name,
                "yes_leg": yes_leg,
                "no_leg": no_leg,
                "yes_ask": yes_ask,
                "no_ask": no_ask,
                "total_ask": total_ask,
                "gross_edge": gross_edge,
                "valid": total_ask is not None,
            }
        )

    best_candidate = None
    scored_candidates = [c for c in candidates if isinstance(c.get("gross_edge"), float)]
    if scored_candidates:
        best_candidate = max(scored_candidates, key=lambda item: float(item.get("gross_edge") or -1e9))

    all_legs_valid = (
        bool(poly_yes.get("valid"))
        and bool(poly_no.get("valid"))
        and bool(kx_yes.get("valid"))
        and bool(kx_no.get("valid"))
    )
    decision_ready = all_legs_valid
    return {
        "legs": {
            "polymarket_yes": poly_yes,
            "polymarket_no": poly_no,
            "kalshi_yes": kx_yes,
            "kalshi_no": kx_no,
        },
        "all_legs_valid": all_legs_valid,
        "canonical": {
            "kalshi_side": "yes",
            "polymarket_side": "yes",
            "decision_ready": decision_ready,
            "decision_reject_reasons": (
                list(poly_yes.get("reasons") or [])
                + list(poly_no.get("reasons") or [])
                + list(kx_yes.get("reasons") or [])
                + list(kx_no.get("reasons") or [])
            )
            if not decision_ready
            else [],
            "polymarket_yes": poly_yes,
            "kalshi_yes": kx_yes,
            "cross_venue_buy_candidates": candidates,
            "best_cross_venue_buy_candidate": best_candidate,
        },
    }


def _default_decision_config() -> DecisionConfig:
    return DecisionConfig(
        buy=BuyDecisionConfig(),
        execution=ExecutionDecisionConfig(),
    )


def _evaluate_price_bounds(
    *,
    quote_sanity: Dict[str, Any],
    decision_config: DecisionConfig,
) -> Dict[str, Any]:
    lower = float(decision_config.execution.best_ask_and_bids_at_min)
    upper = float(decision_config.execution.best_ask_and_bids_at_max)
    checks: Dict[str, Dict[str, Any]] = {}
    reasons: List[str] = []
    legs = _as_dict(quote_sanity.get("legs"))

    for leg_name, leg_value in legs.items():
        leg = _as_dict(leg_value)
        for field_name in ("best_bid", "best_ask"):
            path = f"{leg_name}.{field_name}"
            price = _as_float_number(leg.get(field_name))
            in_bounds = bool(price is not None and lower <= float(price) <= upper)
            checks[path] = {
                "price": price,
                "in_bounds": in_bounds,
            }
            if in_bounds:
                continue
            if price is None:
                reasons.append(f"missing_price:{path}")
            elif float(price) < lower:
                reasons.append(f"below_min_price:{path}:{round(float(price), 6)}")
            else:
                reasons.append(f"above_max_price:{path}:{round(float(price), 6)}")

    return {
        "pass": not reasons,
        "price_min": lower,
        "price_max": upper,
        "checks": checks,
        "reasons": reasons,
    }


def _evaluate_time_window(
    *,
    execution_config: ExecutionDecisionConfig,
    now_epoch_ms: Optional[int],
    market_window_end_epoch_ms: Optional[int],
) -> Dict[str, Any]:
    now_ms = int(now_epoch_ms if now_epoch_ms is not None else int(time.time() * 1000))
    start_buffer_s = int(execution_config.market_start_trade_time_seconds)
    end_buffer_s = int(execution_config.market_end_trade_time_seconds)
    market_duration_s = int(execution_config.market_duration_seconds)
    reasons: List[str] = []

    if market_window_end_epoch_ms is None:
        return {
            "pass": False,
            "reasons": ["missing_market_window_end"],
            "market_window_end_epoch_ms": None,
            "market_window_start_epoch_ms": None,
            "seconds_since_start": None,
            "seconds_to_end": None,
            "start_buffer_seconds": start_buffer_s,
            "end_buffer_seconds": end_buffer_s,
            "market_duration_seconds": market_duration_s,
        }

    window_end_ms = int(market_window_end_epoch_ms)
    window_start_ms = int(window_end_ms - (market_duration_s * 1000))
    seconds_since_start = int((now_ms - window_start_ms) / 1000)
    seconds_to_end = int((window_end_ms - now_ms) / 1000)

    if start_buffer_s + end_buffer_s >= market_duration_s:
        reasons.append("time_buffers_block_full_market")

    if now_ms < window_start_ms:
        reasons.append("market_not_open")
    elif seconds_since_start < start_buffer_s:
        reasons.append("market_start_buffer_active")

    if now_ms >= window_end_ms:
        reasons.append("market_closed")
    elif seconds_to_end < end_buffer_s:
        reasons.append("market_end_buffer_active")

    return {
        "pass": not reasons,
        "reasons": reasons,
        "market_window_end_epoch_ms": window_end_ms,
        "market_window_start_epoch_ms": window_start_ms,
        "seconds_since_start": seconds_since_start,
        "seconds_to_end": seconds_to_end,
        "start_buffer_seconds": start_buffer_s,
        "end_buffer_seconds": end_buffer_s,
        "market_duration_seconds": market_duration_s,
    }


def _evaluate_execution_gate(
    *,
    quote_sanity: Dict[str, Any],
    decision_config: DecisionConfig,
    now_epoch_ms: Optional[int],
    market_window_end_epoch_ms: Optional[int],
) -> Dict[str, Any]:
    bounds = _evaluate_price_bounds(quote_sanity=quote_sanity, decision_config=decision_config)
    time_window = _evaluate_time_window(
        execution_config=decision_config.execution,
        now_epoch_ms=now_epoch_ms,
        market_window_end_epoch_ms=market_window_end_epoch_ms,
    )
    reasons = list(bounds.get("reasons") or []) + list(time_window.get("reasons") or [])
    return {
        "pass": bool(bounds.get("pass")) and bool(time_window.get("pass")),
        "reasons": reasons,
        "price_bounds": bounds,
        "time_window": time_window,
    }


def _evaluate_buy_signal_gate(
    *,
    quote_sanity: Dict[str, Any],
    decision_config: DecisionConfig,
) -> Dict[str, Any]:
    threshold = float(decision_config.buy.min_gross_edge_threshold)
    max_spend_per_market_usd = float(decision_config.buy.max_spend_per_market_usd)
    canonical = _as_dict(quote_sanity.get("canonical"))
    candidates_raw = canonical.get("cross_venue_buy_candidates")
    candidates = list(candidates_raw) if isinstance(candidates_raw, list) else []
    scored_candidates = []

    for candidate in candidates:
        item = _as_dict(candidate)
        gross_edge = _as_float_number(item.get("gross_edge"))
        total_ask = _as_float_number(item.get("total_ask"))
        scored_candidates.append(
            {
                **item,
                "gross_edge": gross_edge,
                "total_ask": total_ask,
                "pass": bool(gross_edge is not None and gross_edge >= threshold),
            }
        )

    passing = [item for item in scored_candidates if bool(item.get("pass"))]
    best_candidate = None
    if scored_candidates:
        ranked = [item for item in scored_candidates if item.get("gross_edge") is not None]
        if ranked:
            best_candidate = max(ranked, key=lambda item: float(item.get("gross_edge") or -1e9))

    reasons: List[str] = []
    if not passing:
        reasons.append("min_gross_edge_threshold_not_met")

    return {
        "pass": bool(passing),
        "reasons": reasons,
        "min_gross_edge_threshold": threshold,
        "best_candidate": best_candidate,
        "candidates": scored_candidates,
        "max_spend_per_market_usd": max_spend_per_market_usd,
        "spend_cap_enforced": False,
        "spend_cap_note": "configured_only_not_enforced",
    }


class SharePriceRuntime:
    """
    Starter runtime for decisioning built on top of normalized books.

    This is intentionally minimal and preserves current behavior while we
    harden memory semantics and then evolve a dedicated decision model.
    """

    def __init__(self, *, polymarket_token_yes: str, polymarket_token_no: str) -> None:
        self.book_runtime = NormalizedBookRuntime(
            polymarket_token_yes=polymarket_token_yes,
            polymarket_token_no=polymarket_token_no,
            max_depth_levels=30,
        )

    def apply_polymarket_event(self, event: Dict[str, Any]) -> None:
        self.book_runtime.apply_polymarket_event(event)

    def apply_kalshi_event(self, event: Dict[str, Any]) -> None:
        kind = str(event.get("kind") or "")
        if kind in {"ticker", "orderbook_event"}:
            self.book_runtime.apply_kalshi_event(event)
            return

    def snapshot(self, now_epoch_ms: Optional[int]) -> Dict[str, Any]:
        return {
            "quotes": self.book_runtime.executable_price_feed(now_epoch_ms=now_epoch_ms),
        }


@dataclass
class DecisionSnapshot:
    can_trade: bool
    health_can_trade: bool
    decision_ready: bool
    buy_signal_ready: bool
    hard_gate_state: str
    health_reasons: Dict[str, Dict[str, List[str]]]
    quote_sanity: Dict[str, Any]
    execution_gate: Dict[str, Any]
    buy_signal: Dict[str, Any]
    execution_plan: Optional[Dict[str, Any]]
    execution_plan_reasons: List[str]
    gate_reasons: List[str]


class DecisionRuntime:
    """
    First-pass decision runtime shell.

    Current implementation keeps parity with existing gating:
    - stream health gate AND quote canonical readiness.
    """

    @staticmethod
    def evaluate(
        *,
        kalshi_health: Dict[str, Any],
        polymarket_health: Dict[str, Any],
        quotes: Dict[str, Any],
        market_context: Optional[Dict[str, Any]] = None,
        decision_config: Optional[DecisionConfig] = None,
        now_epoch_ms: Optional[int] = None,
        market_window_end_epoch_ms: Optional[int] = None,
    ) -> DecisionSnapshot:
        cfg = decision_config or _default_decision_config()
        health_can_trade = bool(kalshi_health.get("decision_ok")) and bool(polymarket_health.get("decision_ok"))
        quote_sanity = build_quote_sanity_and_canonical(quotes)
        canonical = _as_dict(quote_sanity.get("canonical"))
        decision_ready = bool(canonical.get("decision_ready"))
        execution_gate = _evaluate_execution_gate(
            quote_sanity=quote_sanity,
            decision_config=cfg,
            now_epoch_ms=now_epoch_ms,
            market_window_end_epoch_ms=market_window_end_epoch_ms,
        )
        buy_signal = _evaluate_buy_signal_gate(
            quote_sanity=quote_sanity,
            decision_config=cfg,
        )
        buy_signal_ready = bool(execution_gate.get("pass")) and bool(buy_signal.get("pass"))
        can_trade = bool(health_can_trade and decision_ready and buy_signal_ready)
        market_ctx = _as_dict(market_context)
        execution_plan: Optional[Dict[str, Any]] = None
        execution_plan_reasons: List[str] = []
        if can_trade:
            execution_plan, execution_plan_reasons = _build_execution_plan(
                buy_signal=buy_signal,
                quotes=quotes,
                market_context=market_ctx,
                decision_config=cfg,
                now_epoch_ms=now_epoch_ms,
                market_window_end_epoch_ms=market_window_end_epoch_ms,
            )
            if execution_plan is None:
                can_trade = False

        gate_reasons: List[str] = []
        if not health_can_trade:
            gate_reasons.append("health_gate_blocked")
        if not decision_ready:
            reject_reasons = canonical.get("decision_reject_reasons")
            if isinstance(reject_reasons, list):
                gate_reasons.extend(str(reason) for reason in reject_reasons)
            else:
                gate_reasons.append("decision_not_ready")
        if not execution_gate.get("pass"):
            gate_reasons.extend(str(reason) for reason in list(execution_gate.get("reasons") or []))
        if not buy_signal.get("pass"):
            gate_reasons.extend(str(reason) for reason in list(buy_signal.get("reasons") or []))
        if execution_plan is None and execution_plan_reasons:
            gate_reasons.append("execution_plan_unavailable")
            gate_reasons.extend(str(reason) for reason in execution_plan_reasons)

        return DecisionSnapshot(
            can_trade=can_trade,
            health_can_trade=health_can_trade,
            decision_ready=decision_ready,
            buy_signal_ready=buy_signal_ready,
            hard_gate_state="open" if can_trade else "blocked",
            health_reasons={
                "kalshi": {
                    "transport_reasons": list(kalshi_health.get("transport_reasons") or []),
                    "market_data_reasons": list(kalshi_health.get("market_data_reasons") or []),
                },
                "polymarket_market": {
                    "transport_reasons": list(polymarket_health.get("transport_reasons") or []),
                    "market_data_reasons": list(polymarket_health.get("market_data_reasons") or []),
                },
            },
            quote_sanity=quote_sanity,
            execution_gate=execution_gate,
            buy_signal=buy_signal,
            execution_plan=execution_plan,
            execution_plan_reasons=execution_plan_reasons,
            gate_reasons=gate_reasons,
        )
