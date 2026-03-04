from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Optional

from scripts.common.utils import as_float, utc_now_iso
from scripts.common.ws_transport import JsonlWriter

DEFAULT_CLOB_BASE_URL = "https://clob.polymarket.com"


@dataclass(frozen=True)
class FutureMarketShape:
    slug: str
    condition_id: str
    token_yes: str
    token_no: str


class FutureMarketsLoggerRuntime:
    def __init__(
        self,
        *,
        output_dir: Path,
        run_id: str,
        enabled: bool,
        interval_seconds: float = 10.0,
        filename_template: str = "future_markets__{run_id}.jsonl",
        clob_base_url: str = DEFAULT_CLOB_BASE_URL,
        order_book_reader: Optional[Callable[[str], Any]] = None,
    ) -> None:
        self.enabled = bool(enabled)
        self.run_id = str(run_id)
        self.interval_ms = max(1_000, int(max(1.0, float(interval_seconds)) * 1000.0))
        self.next_write_ms = 0
        self.clob_base_url = str(clob_base_url).rstrip("/")
        self._order_book_reader = order_book_reader
        self._clob_client: Any = None
        self._clob_import_error: Optional[str] = None

        self.path: Optional[Path] = None
        self.writer: Optional[JsonlWriter] = None
        if self.enabled:
            filename = str(filename_template or "future_markets__{run_id}.jsonl").format(run_id=self.run_id)
            self.path = output_dir / filename
            self.writer = JsonlWriter(self.path)

        self.stats: Dict[str, Any] = {
            "rows_written": 0,
            "rows_failed": 0,
            "last_error": None,
            "path": (str(self.path) if self.path is not None else None),
        }

    @staticmethod
    def _market_or_none(market: Any) -> Optional[FutureMarketShape]:
        if market is None:
            return None
        slug = str(getattr(market, "slug", "") or "").strip()
        condition_id = str(getattr(market, "condition_id", "") or "").strip()
        token_yes = str(getattr(market, "token_yes", "") or "").strip()
        token_no = str(getattr(market, "token_no", "") or "").strip()
        if not slug:
            return None
        return FutureMarketShape(
            slug=slug,
            condition_id=condition_id,
            token_yes=token_yes,
            token_no=token_no,
        )

    @staticmethod
    def _read_level(level: Any) -> tuple[Optional[float], Optional[float]]:
        if level is None:
            return None, None
        price = as_float(getattr(level, "price", None))
        size = as_float(getattr(level, "size", None))
        if isinstance(level, dict):
            price = as_float(level.get("price")) if price is None else price
            size = as_float(level.get("size")) if size is None else size
        return (float(price) if price is not None else None, float(size) if size is not None else None)

    @staticmethod
    def _top_from_side(side_levels: Any, *, side: str) -> tuple[Optional[float], Optional[float]]:
        levels = list(side_levels or [])
        if not levels:
            return None, None

        side_norm = str(side or "").strip().lower()
        if side_norm not in {"ask", "bid"}:
            raise ValueError(f"invalid_side:{side}")

        best_price: Optional[float] = None
        best_size: Optional[float] = None
        for level in levels:
            price, size = FutureMarketsLoggerRuntime._read_level(level)
            if price is None:
                continue
            if best_price is None:
                best_price, best_size = price, size
                continue
            if side_norm == "ask" and price < best_price:
                best_price, best_size = price, size
            elif side_norm == "bid" and price > best_price:
                best_price, best_size = price, size
        return best_price, best_size

    def _default_order_book_reader(self, token_id: str) -> Any:
        if self._clob_import_error is not None:
            raise RuntimeError(self._clob_import_error)
        if self._clob_client is None:
            try:
                from py_clob_client.client import ClobClient
            except Exception as exc:
                self._clob_import_error = f"py_clob_client_import_error:{type(exc).__name__}:{exc}"
                raise RuntimeError(self._clob_import_error) from exc
            self._clob_client = ClobClient(self.clob_base_url)
        return self._clob_client.get_order_book(str(token_id))

    def _read_token_top(self, *, token_id: str) -> Dict[str, Optional[float]]:
        reader = self._order_book_reader or self._default_order_book_reader
        book = reader(str(token_id))
        asks = getattr(book, "asks", None)
        bids = getattr(book, "bids", None)
        if isinstance(book, dict):
            asks = book.get("asks", asks)
            bids = book.get("bids", bids)
        best_ask_price, best_ask_size = self._top_from_side(asks, side="ask")
        best_bid_price, best_bid_size = self._top_from_side(bids, side="bid")
        return {
            "best_ask_price": best_ask_price,
            "best_ask_size": best_ask_size,
            "best_bid_price": best_bid_price,
            "best_bid_size": best_bid_size,
        }

    @staticmethod
    def _set_market_nulls(*, row: Dict[str, Any], prefix: str) -> None:
        for side in ("yes", "no"):
            row[f"{prefix}_best_ask_price_{side}"] = None
            row[f"{prefix}_best_ask_size_{side}"] = None
            row[f"{prefix}_best_bid_price_{side}"] = None
            row[f"{prefix}_best_bid_size_{side}"] = None

    def _append_market_quotes(
        self,
        *,
        row: Dict[str, Any],
        prefix: str,
        market: Optional[FutureMarketShape],
        errors: list[str],
    ) -> None:
        if market is None:
            self._set_market_nulls(row=row, prefix=prefix)
            errors.append(f"{prefix}_market_missing")
            return

        for outcome_side, token_id in (("yes", market.token_yes), ("no", market.token_no)):
            key_suffix = str(outcome_side)
            if not str(token_id or "").strip():
                row[f"{prefix}_best_ask_price_{key_suffix}"] = None
                row[f"{prefix}_best_ask_size_{key_suffix}"] = None
                row[f"{prefix}_best_bid_price_{key_suffix}"] = None
                row[f"{prefix}_best_bid_size_{key_suffix}"] = None
                errors.append(f"{prefix}_{key_suffix}_token_missing")
                continue
            try:
                top = self._read_token_top(token_id=str(token_id))
            except Exception as exc:
                row[f"{prefix}_best_ask_price_{key_suffix}"] = None
                row[f"{prefix}_best_ask_size_{key_suffix}"] = None
                row[f"{prefix}_best_bid_price_{key_suffix}"] = None
                row[f"{prefix}_best_bid_size_{key_suffix}"] = None
                errors.append(f"{prefix}_{key_suffix}_book_error:{type(exc).__name__}:{exc}")
                continue
            row[f"{prefix}_best_ask_price_{key_suffix}"] = top.get("best_ask_price")
            row[f"{prefix}_best_ask_size_{key_suffix}"] = top.get("best_ask_size")
            row[f"{prefix}_best_bid_price_{key_suffix}"] = top.get("best_bid_price")
            row[f"{prefix}_best_bid_size_{key_suffix}"] = top.get("best_bid_size")

    def maybe_write(
        self,
        *,
        now_epoch_ms: int,
        current_window_start_s: int,
        current_market: Any,
        next_market: Any,
        second_next_market: Any,
        rtds_row: Optional[Dict[str, Any]],
    ) -> None:
        if not self.enabled or self.writer is None:
            return
        now_ms_i = int(now_epoch_ms)
        if now_ms_i < int(self.next_write_ms):
            return
        self.next_write_ms = int(now_ms_i + self.interval_ms)

        current = self._market_or_none(current_market)
        next_row = self._market_or_none(next_market)
        second_next = self._market_or_none(second_next_market)
        errors: list[str] = []
        rtds = dict(rtds_row or {})
        btc_price = as_float(rtds.get("price_usd"))
        if btc_price is None:
            errors.append("btc_price_missing")

        row: Dict[str, Any] = {
            "ts": utc_now_iso(),
            "recv_ms": now_ms_i,
            "run_id": self.run_id,
            "current_market_age_seconds": int(max(0, (now_ms_i // 1000) - int(current_window_start_s))),
            "btc_price_usd": (float(btc_price) if btc_price is not None else None),
        }

        self._append_market_quotes(
            row=row,
            prefix="current_market",
            market=current,
            errors=errors,
        )
        self._append_market_quotes(
            row=row,
            prefix="next_market",
            market=next_row,
            errors=errors,
        )
        self._append_market_quotes(
            row=row,
            prefix="second_next_market",
            market=second_next,
            errors=errors,
        )

        row["errors"] = errors
        row["current_market_slug"] = (current.slug if current is not None else None)
        row["next_market_slug"] = (next_row.slug if next_row is not None else None)
        row["second_next_market_slug"] = (second_next.slug if second_next is not None else None)
        row["current_market_condition_id"] = (current.condition_id if current is not None else None)

        try:
            self.writer.write(row)
            self.stats["rows_written"] = int(self.stats.get("rows_written", 0)) + 1
        except Exception as exc:
            self.stats["rows_failed"] = int(self.stats.get("rows_failed", 0)) + 1
            self.stats["last_error"] = f"{type(exc).__name__}:{exc}"

    def close(self) -> None:
        if self.writer is None:
            return
        try:
            self.writer.close()
        except Exception:
            pass
