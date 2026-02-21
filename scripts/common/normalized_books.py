from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple


def _to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None


def _price_to_prob(value: Any) -> Optional[float]:
    number = _to_float(value)
    if number is None:
        return None
    if number > 1.0:
        number = number / 100.0
    if number < 0.0 or number > 1.0:
        return None
    return float(number)


def _normalize_side_name(value: str) -> Optional[str]:
    text = str(value or "").strip().lower()
    if text in {"buy", "bid", "bids"}:
        return "bid"
    if text in {"sell", "ask", "asks"}:
        return "ask"
    return None


@dataclass
class OrderBookSide:
    levels: Dict[float, float]

    def __init__(self) -> None:
        self.levels = {}

    def clear(self) -> None:
        self.levels.clear()

    def set_level(self, price: float, size: float) -> None:
        p = round(float(price), 6)
        s = float(size)
        if s <= 0:
            self.levels.pop(p, None)
        else:
            self.levels[p] = s

    def add_delta(self, price: float, delta: float) -> None:
        p = round(float(price), 6)
        current = float(self.levels.get(p, 0.0))
        self.set_level(p, current + float(delta))

    def sorted_levels(self, side: str, max_levels: Optional[int] = None) -> List[Tuple[float, float]]:
        reverse = str(side) == "bid"
        ordered = sorted(self.levels.items(), key=lambda x: x[0], reverse=reverse)
        if max_levels is not None and max_levels > 0:
            return ordered[: int(max_levels)]
        return ordered

    def replace_from_levels(self, levels: List[Tuple[float, float]], max_levels: int, side: str) -> None:
        self.clear()
        for price, size in levels[: max(1, int(max_levels))]:
            self.set_level(price, size)

    def top(self, side: str) -> Optional[Tuple[float, float]]:
        ordered = self.sorted_levels(side=side, max_levels=1)
        return ordered[0] if ordered else None


@dataclass
class OutcomeBook:
    bids: OrderBookSide
    asks: OrderBookSide
    source_timestamp_ms: Optional[int] = None
    recv_timestamp_ms: Optional[int] = None

    def __init__(self) -> None:
        self.bids = OrderBookSide()
        self.asks = OrderBookSide()
        self.source_timestamp_ms = None
        self.recv_timestamp_ms = None

    def mark_update(self, source_ts_ms: Optional[int], recv_ts_ms: Optional[int]) -> None:
        if source_ts_ms is not None:
            self.source_timestamp_ms = int(source_ts_ms)
        if recv_ts_ms is not None:
            self.recv_timestamp_ms = int(recv_ts_ms)

    def top(self) -> Dict[str, Optional[float]]:
        best_bid = self.bids.top(side="bid")
        best_ask = self.asks.top(side="ask")
        return {
            "best_bid": None if best_bid is None else float(best_bid[0]),
            "best_bid_size": None if best_bid is None else float(best_bid[1]),
            "best_ask": None if best_ask is None else float(best_ask[0]),
            "best_ask_size": None if best_ask is None else float(best_ask[1]),
        }

    def depth_summary(self, max_levels: int) -> Dict[str, Any]:
        bids = self.bids.sorted_levels(side="bid", max_levels=max_levels)
        asks = self.asks.sorted_levels(side="ask", max_levels=max_levels)
        return {
            "levels_kept": int(max_levels),
            "bid_levels": len(bids),
            "ask_levels": len(asks),
            "bid_total_size": round(sum(size for _, size in bids), 6),
            "ask_total_size": round(sum(size for _, size in asks), 6),
        }

    def estimate_take(self, *, action: str, size: float, max_levels: int) -> Dict[str, Any]:
        target_size = max(0.0, float(size))
        if target_size <= 0:
            return {
                "requested_size": 0.0,
                "filled_size": 0.0,
                "unfilled_size": 0.0,
                "complete_fill": True,
                "top_price": None,
                "worst_price": None,
                "vwap": None,
                "slippage_bps": None,
            }

        side = str(action).strip().lower()
        if side == "buy":
            ladder = self.asks.sorted_levels(side="ask", max_levels=max_levels)
            favorable = "ask"
        elif side == "sell":
            ladder = self.bids.sorted_levels(side="bid", max_levels=max_levels)
            favorable = "bid"
        else:
            raise RuntimeError(f"Unsupported action for estimate_take: {action}")

        if not ladder:
            return {
                "requested_size": target_size,
                "filled_size": 0.0,
                "unfilled_size": target_size,
                "complete_fill": False,
                "top_price": None,
                "worst_price": None,
                "vwap": None,
                "slippage_bps": None,
            }

        remaining = target_size
        filled = 0.0
        notional = 0.0
        top_price = float(ladder[0][0])
        worst_price = float(top_price)

        for price, avail in ladder:
            if remaining <= 0:
                break
            take = min(remaining, float(avail))
            if take <= 0:
                continue
            notional += float(price) * take
            filled += take
            remaining -= take
            worst_price = float(price)

        vwap = (notional / filled) if filled > 0 else None
        slippage_bps = None
        if vwap is not None and top_price > 0:
            if favorable == "ask":
                slippage_bps = ((vwap - top_price) / top_price) * 10_000.0
            else:
                slippage_bps = ((top_price - vwap) / top_price) * 10_000.0

        return {
            "requested_size": round(target_size, 6),
            "filled_size": round(filled, 6),
            "unfilled_size": round(max(0.0, target_size - filled), 6),
            "complete_fill": bool(abs(target_size - filled) < 1e-9),
            "top_price": None if top_price is None else round(top_price, 6),
            "worst_price": None if worst_price is None else round(worst_price, 6),
            "vwap": None if vwap is None else round(vwap, 6),
            "slippage_bps": None if slippage_bps is None else round(slippage_bps, 3),
        }

    def max_size_within_slippage(
        self,
        *,
        action: str,
        max_slippage_bps: float,
        max_levels: int,
    ) -> Dict[str, Any]:
        side = str(action).strip().lower()
        if side == "buy":
            ladder = self.asks.sorted_levels(side="ask", max_levels=max_levels)
            def adverse(top: float, px: float) -> float:
                return ((px - top) / top) * 10_000.0
        elif side == "sell":
            ladder = self.bids.sorted_levels(side="bid", max_levels=max_levels)
            def adverse(top: float, px: float) -> float:
                return ((top - px) / top) * 10_000.0
        else:
            raise RuntimeError(f"Unsupported action for max_size_within_slippage: {action}")

        if not ladder:
            return {
                "max_slippage_bps": float(max_slippage_bps),
                "max_size": 0.0,
                "top_price": None,
            }

        top = float(ladder[0][0])
        if top <= 0:
            return {
                "max_slippage_bps": float(max_slippage_bps),
                "max_size": 0.0,
                "top_price": round(top, 6),
            }

        max_size = 0.0
        for price, size in ladder:
            if adverse(top, float(price)) > float(max_slippage_bps):
                break
            max_size += float(size)

        return {
            "max_slippage_bps": float(max_slippage_bps),
            "max_size": round(max_size, 6),
            "top_price": round(top, 6),
        }


class NormalizedBookRuntime:
    """
    Normalized in-memory books for decisioning.

    Normalization policy:
    - Prices are probabilities in [0,1].
    - Sizes are floats (shares/contracts).
    - Keep only top-N levels per side/outcome for efficiency.

    Venue mapping:
    - Polymarket:
      - `asset_id` maps directly to YES/NO outcome.
      - Snapshot provides explicit bids+asks.
      - price_change updates are applied as side+price+size level updates.
      - last_trade_price updates consume opposite-side depth at trade price:
        - BUY trade reduces ask depth
        - SELL trade reduces bid depth
    - Kalshi:
      - WS orderbook snapshot/delta are side-specific (`yes` or `no`) bid ladders.
      - We keep those as bid books and derive asks from opposite-side bids:
        - ask_yes(price) = 1 - bid_no(price)
        - ask_no(price) = 1 - bid_yes(price)
    """

    def __init__(
        self,
        *,
        polymarket_token_yes: str,
        polymarket_token_no: str,
        max_depth_levels: int = 30,
    ) -> None:
        self.max_depth_levels = max(1, int(max_depth_levels))
        self.polymarket_token_yes = str(polymarket_token_yes)
        self.polymarket_token_no = str(polymarket_token_no)
        self.polymarket_asset_to_outcome = {
            self.polymarket_token_yes: "yes",
            self.polymarket_token_no: "no",
        }
        self.books: Dict[str, Dict[str, OutcomeBook]] = {
            "polymarket": {"yes": OutcomeBook(), "no": OutcomeBook()},
            "kalshi": {"yes": OutcomeBook(), "no": OutcomeBook()},
        }
        self.kalshi_orderbook_depth_active = False
        self.kalshi_orderbook_snapshot_received = False

    def _book(self, venue: str, outcome: str) -> OutcomeBook:
        return self.books[str(venue)][str(outcome)]

    @staticmethod
    def _to_int_ms(value: Any) -> Optional[int]:
        if isinstance(value, bool):
            return None
        if isinstance(value, (int, float)):
            return int(value)
        return None

    @staticmethod
    def _is_newest_or_tied_event(
        book: OutcomeBook,
        *,
        source_ts_ms: Optional[int],
        recv_ts_ms: Optional[int],
    ) -> bool:
        """
        Per-book monotonicity guard for venue-source timestamps.

        Accept when:
        - first event for this book
        - source timestamp is newer
        - source timestamp ties and recv timestamp is not older

        Reject when source timestamp is missing or older.
        """
        if source_ts_ms is None:
            return False

        last_source = book.source_timestamp_ms
        last_recv = book.recv_timestamp_ms
        if last_source is None:
            return True
        if source_ts_ms > int(last_source):
            return True
        if source_ts_ms < int(last_source):
            return False

        # Same source timestamp: keep stable ordering by recv timestamp when available.
        if recv_ts_ms is None or last_recv is None:
            return True
        return int(recv_ts_ms) >= int(last_recv)

    @staticmethod
    def _normalize_levels(levels: Any, side: str) -> List[Tuple[float, float]]:
        parsed: List[Tuple[float, float]] = []
        for level in levels or []:
            if isinstance(level, dict):
                raw_price = level.get("price")
                raw_size = level.get("size")
            elif isinstance(level, (list, tuple)) and len(level) >= 2:
                raw_price, raw_size = level[0], level[1]
            else:
                continue

            price = _price_to_prob(raw_price)
            size = _to_float(raw_size)
            if price is None or size is None or size <= 0:
                continue
            parsed.append((price, float(size)))

        parsed.sort(key=lambda x: x[0], reverse=(side == "bid"))
        return parsed

    @staticmethod
    def _complement_levels(levels: List[Tuple[float, float]], side: str) -> List[Tuple[float, float]]:
        complemented = []
        for price, size in levels:
            comp = round(1.0 - float(price), 6)
            if comp < 0.0 or comp > 1.0:
                continue
            complemented.append((comp, float(size)))
        complemented.sort(key=lambda x: x[0], reverse=(side == "bid"))
        return complemented

    def _truncate(self, levels: List[Tuple[float, float]]) -> List[Tuple[float, float]]:
        return levels[: self.max_depth_levels]

    def _prune_side_if_needed(self, side_book: OrderBookSide, *, side: str) -> None:
        # Fast no-op path for the common case where depth is already bounded.
        if len(side_book.levels) <= self.max_depth_levels:
            return
        top_levels = side_book.sorted_levels(side=side, max_levels=self.max_depth_levels)
        side_book.replace_from_levels(top_levels, self.max_depth_levels, side=side)

    def _prune_outcome_book(
        self,
        book: OutcomeBook,
        *,
        prune_bids: bool = True,
        prune_asks: bool = True,
    ) -> None:
        """
        Enforce in-memory top-N depth cap for selected sides of one outcome book.

        Side-selective pruning avoids unnecessary sorting/replacement churn.
        """
        if prune_bids:
            self._prune_side_if_needed(book.bids, side="bid")
        if prune_asks:
            self._prune_side_if_needed(book.asks, side="ask")

    @staticmethod
    def _merge_snapshot_depth_preserve_top(
        *,
        book: OutcomeBook,
        bids: List[Tuple[float, float]],
        asks: List[Tuple[float, float]],
    ) -> None:
        """
        Merge snapshot depth without modifying current top-of-book levels.

        Used for stale snapshots so they can still backfill depth while
        preserving current best bid/ask price+size.
        """
        top_bid = book.bids.top(side="bid")
        top_ask = book.asks.top(side="ask")
        top_bid_price = None if top_bid is None else float(top_bid[0])
        top_ask_price = None if top_ask is None else float(top_ask[0])

        for price, size in bids:
            if top_bid_price is not None and float(price) >= float(top_bid_price):
                continue
            book.bids.set_level(price, size)

        for price, size in asks:
            if top_ask_price is not None and float(price) <= float(top_ask_price):
                continue
            book.asks.set_level(price, size)

    def _rebuild_kalshi_asks_from_bids(self) -> None:
        yes_bid_levels = self._book("kalshi", "yes").bids.sorted_levels(side="bid", max_levels=self.max_depth_levels)
        no_bid_levels = self._book("kalshi", "no").bids.sorted_levels(side="bid", max_levels=self.max_depth_levels)

        yes_ask = self._truncate(self._complement_levels(no_bid_levels, side="ask"))
        no_ask = self._truncate(self._complement_levels(yes_bid_levels, side="ask"))

        self._book("kalshi", "yes").asks.replace_from_levels(yes_ask, self.max_depth_levels, side="ask")
        self._book("kalshi", "no").asks.replace_from_levels(no_ask, self.max_depth_levels, side="ask")

    def apply_polymarket_event(self, event: Dict[str, Any]) -> None:
        kind = str(event.get("kind") or "")
        asset_id = str(event.get("asset_id") or "")
        outcome = self.polymarket_asset_to_outcome.get(asset_id)
        if not outcome:
            return
        book = self._book("polymarket", outcome)
        source_ts_ms = self._to_int_ms(event.get("source_timestamp_ms"))
        recv_ts_ms = self._to_int_ms(event.get("recv_ms"))
        is_newest_or_tied = True

        if kind in {"book_snapshot", "book_top_update", "book_best_bid_ask", "last_trade_price"}:
            is_newest_or_tied = self._is_newest_or_tied_event(book, source_ts_ms=source_ts_ms, recv_ts_ms=recv_ts_ms)
            if kind != "book_snapshot" and not is_newest_or_tied:
                return

        if kind == "book_snapshot":
            bids = self._normalize_levels(event.get("bids"), side="bid")
            asks = self._normalize_levels(event.get("asks"), side="ask")
            truncated_bids = self._truncate(bids)
            truncated_asks = self._truncate(asks)
            if is_newest_or_tied:
                book.bids.replace_from_levels(truncated_bids, self.max_depth_levels, side="bid")
                book.asks.replace_from_levels(truncated_asks, self.max_depth_levels, side="ask")
                self._prune_outcome_book(book)
                book.mark_update(source_ts_ms=source_ts_ms, recv_ts_ms=recv_ts_ms)
            else:
                self._merge_snapshot_depth_preserve_top(book=book, bids=truncated_bids, asks=truncated_asks)
                self._prune_outcome_book(book)
            return

        if kind == "book_top_update":
            touched_bids = False
            touched_asks = False
            changed_side = _normalize_side_name(str(event.get("changed_side") or ""))
            changed_price = _price_to_prob(event.get("changed_price"))
            changed_size = _to_float(event.get("changed_size"))
            if changed_side and changed_price is not None and changed_size is not None:
                if changed_side == "bid":
                    book.bids.set_level(changed_price, changed_size)
                    touched_bids = True
                else:
                    book.asks.set_level(changed_price, changed_size)
                    touched_asks = True

            best_bid = _price_to_prob(event.get("best_bid"))
            best_bid_size = _to_float(event.get("best_bid_size"))
            best_ask = _price_to_prob(event.get("best_ask"))
            best_ask_size = _to_float(event.get("best_ask_size"))
            fallback_top_size = 0.1
            if best_bid is not None:
                touched_bids = True
                best_bid_key = round(float(best_bid), 6)
                existing = _to_float(book.bids.levels.get(best_bid_key))
                top = book.bids.top(side="bid")
                inferred_bid_size = existing if existing is not None else (float(top[1]) if top is not None else None)
                if best_bid_size is None or best_bid_size <= 0:
                    best_bid_size = inferred_bid_size
                if best_bid_size is None or best_bid_size <= 0:
                    best_bid_size = fallback_top_size

                # If venue says best bid moved, stale higher bids must be removed.
                for price in list(book.bids.levels.keys()):
                    if float(price) > float(best_bid_key):
                        book.bids.levels.pop(price, None)

                book.bids.set_level(best_bid_key, best_bid_size)
            if best_ask is not None:
                touched_asks = True
                best_ask_key = round(float(best_ask), 6)
                existing = _to_float(book.asks.levels.get(best_ask_key))
                top = book.asks.top(side="ask")
                inferred_ask_size = existing if existing is not None else (float(top[1]) if top is not None else None)
                if best_ask_size is None or best_ask_size <= 0:
                    best_ask_size = inferred_ask_size
                if best_ask_size is None or best_ask_size <= 0:
                    best_ask_size = fallback_top_size

                # If venue says best ask moved, stale lower asks must be removed.
                for price in list(book.asks.levels.keys()):
                    if float(price) < float(best_ask_key):
                        book.asks.levels.pop(price, None)

                book.asks.set_level(best_ask_key, best_ask_size)

            self._prune_outcome_book(book, prune_bids=touched_bids, prune_asks=touched_asks)
            book.mark_update(source_ts_ms=source_ts_ms, recv_ts_ms=recv_ts_ms)
            return

        if kind == "book_best_bid_ask":
            touched_bids = False
            touched_asks = False
            best_bid = _price_to_prob(event.get("best_bid"))
            best_ask = _price_to_prob(event.get("best_ask"))

            if best_bid is not None:
                touched_bids = True
                best_bid_key = round(float(best_bid), 6)
                existing = _to_float(book.bids.levels.get(best_bid_key))
                top = book.bids.top(side="bid")
                inferred_bid_size = existing if existing is not None else (float(top[1]) if top is not None else None)

                # If venue tells us best bid changed, stale prices above it cannot remain top-of-book.
                for price in list(book.bids.levels.keys()):
                    if float(price) > float(best_bid_key):
                        book.bids.levels.pop(price, None)

                if inferred_bid_size is not None and inferred_bid_size > 0:
                    book.bids.set_level(best_bid_key, inferred_bid_size)

            if best_ask is not None:
                touched_asks = True
                best_ask_key = round(float(best_ask), 6)
                existing = _to_float(book.asks.levels.get(best_ask_key))
                top = book.asks.top(side="ask")
                inferred_ask_size = existing if existing is not None else (float(top[1]) if top is not None else None)

                # If venue tells us best ask changed, stale prices below it cannot remain top-of-book.
                for price in list(book.asks.levels.keys()):
                    if float(price) < float(best_ask_key):
                        book.asks.levels.pop(price, None)

                if inferred_ask_size is not None and inferred_ask_size > 0:
                    book.asks.set_level(best_ask_key, inferred_ask_size)

            self._prune_outcome_book(book, prune_bids=touched_bids, prune_asks=touched_asks)
            book.mark_update(source_ts_ms=source_ts_ms, recv_ts_ms=recv_ts_ms)
            return

        if kind == "last_trade_price":
            trade_side = str(event.get("trade_side") or "").strip().lower()
            trade_price = _price_to_prob(event.get("trade_price"))
            trade_size = _to_float(event.get("trade_size"))
            touched_bids = False
            touched_asks = False
            if trade_price is not None and trade_size is not None and trade_size > 0:
                # Taker BUY consumes maker asks. Taker SELL consumes maker bids.
                if trade_side == "buy":
                    book.asks.add_delta(trade_price, -float(trade_size))
                    touched_asks = True
                elif trade_side == "sell":
                    book.bids.add_delta(trade_price, -float(trade_size))
                    touched_bids = True

            self._prune_outcome_book(book, prune_bids=touched_bids, prune_asks=touched_asks)
            book.mark_update(source_ts_ms=source_ts_ms, recv_ts_ms=recv_ts_ms)
            return

    def apply_kalshi_event(self, event: Dict[str, Any]) -> None:
        kind = str(event.get("kind") or "")
        source_type = str(event.get("source_event_type") or "")
        source_ts = event.get("source_timestamp_ms")
        recv_ts = event.get("recv_ms")

        if kind == "orderbook_event" and source_type == "orderbook_snapshot":
            payload_raw = event.get("payload")
            payload = payload_raw if isinstance(payload_raw, dict) else {}
            yes = self._normalize_levels(payload.get("yes"), side="bid")
            no = self._normalize_levels(payload.get("no"), side="bid")
            self._book("kalshi", "yes").bids.replace_from_levels(self._truncate(yes), self.max_depth_levels, side="bid")
            self._book("kalshi", "no").bids.replace_from_levels(self._truncate(no), self.max_depth_levels, side="bid")
            self._rebuild_kalshi_asks_from_bids()
            self.kalshi_orderbook_depth_active = True
            self.kalshi_orderbook_snapshot_received = True

            for outcome in ("yes", "no"):
                self._book("kalshi", outcome).mark_update(
                    source_ts_ms=int(source_ts) if isinstance(source_ts, (int, float)) else None,
                    recv_ts_ms=int(recv_ts) if isinstance(recv_ts, (int, float)) else None,
                )
            return

        if kind == "orderbook_event" and source_type == "orderbook_delta":
            if not self.kalshi_orderbook_snapshot_received:
                return
            payload_raw = event.get("payload")
            payload = payload_raw if isinstance(payload_raw, dict) else {}
            side = str(payload.get("side") or "").strip().lower()
            if side not in {"yes", "no"}:
                return
            price = _price_to_prob(payload.get("price_dollars") or payload.get("price"))
            delta = _to_float(payload.get("delta_fp") or payload.get("delta"))
            if price is None or delta is None:
                return
            side_book = self._book("kalshi", side)
            side_book.bids.add_delta(price, delta)
            self._prune_outcome_book(side_book, prune_bids=True, prune_asks=False)
            self._rebuild_kalshi_asks_from_bids()
            self.kalshi_orderbook_depth_active = True
            self._book("kalshi", side).mark_update(
                source_ts_ms=int(source_ts) if isinstance(source_ts, (int, float)) else None,
                recv_ts_ms=int(recv_ts) if isinstance(recv_ts, (int, float)) else None,
            )
            return

        if kind == "ticker":
            yes_bid = _price_to_prob(event.get("yes_bid"))
            yes_bid_size = _to_float(event.get("yes_bid_size"))
            yes_ask = _price_to_prob(event.get("yes_ask"))
            yes_ask_size = _to_float(event.get("yes_ask_size"))
            no_bid = _price_to_prob(event.get("no_bid"))
            no_bid_size = _to_float(event.get("no_bid_size"))
            no_ask = _price_to_prob(event.get("no_ask"))
            no_ask_size = _to_float(event.get("no_ask_size"))
            yes_book = self._book("kalshi", "yes")
            no_book = self._book("kalshi", "no")

            if self.kalshi_orderbook_depth_active:
                # When orderbook feed is active, ticker should refresh top bids only
                # and never wipe accumulated depth.
                touched_yes_bid = False
                touched_no_bid = False
                if yes_bid is not None:
                    touched_yes_bid = True
                    yes_bid_key = round(float(yes_bid), 6)
                    for price in list(yes_book.bids.levels.keys()):
                        if float(price) > float(yes_bid_key):
                            yes_book.bids.levels.pop(price, None)
                    if yes_bid_size is not None and yes_bid_size > 0:
                        yes_book.bids.set_level(yes_bid_key, yes_bid_size)
                    elif yes_bid_size is not None and yes_bid_size <= 0:
                        yes_book.bids.levels.pop(yes_bid_key, None)

                if no_bid is not None:
                    touched_no_bid = True
                    no_bid_key = round(float(no_bid), 6)
                    for price in list(no_book.bids.levels.keys()):
                        if float(price) > float(no_bid_key):
                            no_book.bids.levels.pop(price, None)
                    if no_bid_size is not None and no_bid_size > 0:
                        no_book.bids.set_level(no_bid_key, no_bid_size)
                    elif no_bid_size is not None and no_bid_size <= 0:
                        no_book.bids.levels.pop(no_bid_key, None)

                if touched_yes_bid:
                    self._prune_outcome_book(yes_book, prune_bids=True, prune_asks=False)
                if touched_no_bid:
                    self._prune_outcome_book(no_book, prune_bids=True, prune_asks=False)
                self._rebuild_kalshi_asks_from_bids()
                for outcome in ("yes", "no"):
                    self._book("kalshi", outcome).mark_update(
                        source_ts_ms=int(source_ts) if isinstance(source_ts, (int, float)) else None,
                        recv_ts_ms=int(recv_ts) if isinstance(recv_ts, (int, float)) else None,
                    )
                return

            # Kalshi ticker is an L1 snapshot. Replace L1 state instead of
            # accumulating prior levels, which can create synthetic crossed books.
            yes_book.bids.clear()
            yes_book.asks.clear()
            no_book.bids.clear()
            no_book.asks.clear()

            if yes_bid is not None and yes_bid_size is not None and yes_bid_size > 0:
                yes_book.bids.set_level(yes_bid, yes_bid_size)
            if yes_ask is not None and yes_ask_size is not None and yes_ask_size > 0:
                yes_book.asks.set_level(yes_ask, yes_ask_size)
            if no_bid is not None and no_bid_size is not None and no_bid_size > 0:
                no_book.bids.set_level(no_bid, no_bid_size)
            if no_ask is not None and no_ask_size is not None and no_ask_size > 0:
                no_book.asks.set_level(no_ask, no_ask_size)

            for outcome in ("yes", "no"):
                self._book("kalshi", outcome).mark_update(
                    source_ts_ms=int(source_ts) if isinstance(source_ts, (int, float)) else None,
                    recv_ts_ms=int(recv_ts) if isinstance(recv_ts, (int, float)) else None,
                )

    def _outcome_snapshot(self, venue: str, outcome: str) -> Dict[str, Any]:
        book = self._book(venue, outcome)
        return {
            "top": book.top(),
            "depth": book.depth_summary(max_levels=self.max_depth_levels),
            "source_timestamp_ms": book.source_timestamp_ms,
            "recv_timestamp_ms": book.recv_timestamp_ms,
        }

    def _outcome_quote(self, venue: str, outcome: str, now_epoch_ms: Optional[int]) -> Dict[str, Any]:
        book = self._book(venue, outcome)
        top = book.top()
        bid = top.get("best_bid")
        ask = top.get("best_ask")
        bid_size = top.get("best_bid_size")
        ask_size = top.get("best_ask_size")

        mid = None
        spread = None
        spread_bps = None
        if isinstance(bid, float) and isinstance(ask, float):
            spread = float(ask - bid)
            mid = float((ask + bid) / 2.0)
            if mid > 0:
                spread_bps = float((spread / mid) * 10_000.0)

        quote_age_ms = None
        if now_epoch_ms is not None and book.recv_timestamp_ms is not None:
            quote_age_ms = int(max(0, int(now_epoch_ms) - int(book.recv_timestamp_ms)))

        return {
            "best_bid": bid,
            "best_bid_size": bid_size,
            "best_ask": ask,
            "best_ask_size": ask_size,
            "mid": None if mid is None else round(mid, 6),
            "spread": None if spread is None else round(spread, 6),
            "spread_bps": None if spread_bps is None else round(spread_bps, 3),
            "source_timestamp_ms": book.source_timestamp_ms,
            "recv_timestamp_ms": book.recv_timestamp_ms,
            "quote_age_ms": quote_age_ms,
        }

    def estimate_take(
        self,
        *,
        venue: str,
        outcome: str,
        action: str,
        size: float,
    ) -> Dict[str, Any]:
        book = self._book(venue, outcome)
        return book.estimate_take(action=action, size=size, max_levels=self.max_depth_levels)

    def max_size_within_slippage(
        self,
        *,
        venue: str,
        outcome: str,
        action: str,
        max_slippage_bps: float,
    ) -> Dict[str, Any]:
        book = self._book(venue, outcome)
        return book.max_size_within_slippage(
            action=action,
            max_slippage_bps=max_slippage_bps,
            max_levels=self.max_depth_levels,
        )

    def snapshot(self) -> Dict[str, Any]:
        return {
            "max_depth_levels": int(self.max_depth_levels),
            "books": {
                "polymarket": {
                    "yes": self._outcome_snapshot("polymarket", "yes"),
                    "no": self._outcome_snapshot("polymarket", "no"),
                },
                "kalshi": {
                    "yes": self._outcome_snapshot("kalshi", "yes"),
                    "no": self._outcome_snapshot("kalshi", "no"),
                },
            },
            "executable_quotes": self.executable_price_feed(),
            "sizing_examples": {
                "slippage_bps_50": {
                    "polymarket_yes_buy": self.max_size_within_slippage(
                        venue="polymarket", outcome="yes", action="buy", max_slippage_bps=50.0
                    ),
                    "kalshi_yes_buy": self.max_size_within_slippage(
                        venue="kalshi", outcome="yes", action="buy", max_slippage_bps=50.0
                    ),
                },
                "take_size_100": {
                    "polymarket_yes_buy": self.estimate_take(
                        venue="polymarket", outcome="yes", action="buy", size=100.0
                    ),
                    "kalshi_yes_buy": self.estimate_take(
                        venue="kalshi", outcome="yes", action="buy", size=100.0
                    ),
                },
            },
        }

    def executable_price_feed(self, now_epoch_ms: Optional[int] = None) -> Dict[str, Any]:
        return {
            "legs": {
                "polymarket_yes": self._outcome_quote("polymarket", "yes", now_epoch_ms),
                "polymarket_no": self._outcome_quote("polymarket", "no", now_epoch_ms),
                "kalshi_yes": self._outcome_quote("kalshi", "yes", now_epoch_ms),
                "kalshi_no": self._outcome_quote("kalshi", "no", now_epoch_ms),
            },
        }
