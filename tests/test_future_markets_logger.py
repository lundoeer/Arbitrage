from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict

from scripts.limit_premarket.future_markets_logger import FutureMarketsLoggerRuntime


@dataclass(frozen=True)
class _Market:
    slug: str
    condition_id: str
    token_yes: str
    token_no: str


def _read_jsonl(path: Path) -> list[Dict[str, Any]]:
    rows: list[Dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        rows.append(json.loads(line))
    return rows


def test_future_markets_logger_writes_flat_row_and_market_trailers(tmp_path: Path) -> None:
    books = {
        "cur_yes": {"asks": [{"price": 0.51, "size": 101}], "bids": [{"price": 0.49, "size": 99}]},
        "cur_no": {"asks": [{"price": 0.52, "size": 88}], "bids": [{"price": 0.48, "size": 87}]},
        "nxt_yes": {"asks": [{"price": 0.53, "size": 77}], "bids": [{"price": 0.47, "size": 76}]},
        "nxt_no": {"asks": [{"price": 0.54, "size": 66}], "bids": [{"price": 0.46, "size": 65}]},
        "snx_yes": {"asks": [{"price": 0.55, "size": 55}], "bids": [{"price": 0.45, "size": 54}]},
        "snx_no": {"asks": [{"price": 0.56, "size": 44}], "bids": [{"price": 0.44, "size": 43}]},
    }

    runtime = FutureMarketsLoggerRuntime(
        output_dir=tmp_path,
        run_id="run-1",
        enabled=True,
        interval_seconds=10.0,
        order_book_reader=lambda token_id: books[token_id],
    )
    runtime.maybe_write(
        now_epoch_ms=1_770_000_123_000,
        current_window_start_s=1_770_000_000,
        current_market=_Market("cur", "cond-cur", "cur_yes", "cur_no"),
        next_market=_Market("nxt", "cond-nxt", "nxt_yes", "nxt_no"),
        second_next_market=_Market("snx", "cond-snx", "snx_yes", "snx_no"),
        rtds_row={"price_usd": 67_001.25},
    )
    runtime.close()

    assert runtime.path is not None
    rows = _read_jsonl(runtime.path)
    assert len(rows) == 1
    row = rows[0]

    assert row["run_id"] == "run-1"
    assert row["btc_price_usd"] == 67_001.25
    assert row["current_market_age_seconds"] == 123

    assert row["current_market_best_ask_price_yes"] == 0.51
    assert row["next_market_best_bid_size_no"] == 65
    assert row["second_next_market_best_ask_price_no"] == 0.56

    assert row["errors"] == []
    assert row["current_market_slug"] == "cur"
    assert row["next_market_slug"] == "nxt"
    assert row["second_next_market_slug"] == "snx"
    assert row["current_market_condition_id"] == "cond-cur"
    assert list(row.keys())[-4:] == [
        "current_market_slug",
        "next_market_slug",
        "second_next_market_slug",
        "current_market_condition_id",
    ]


def test_future_markets_logger_respects_interval(tmp_path: Path) -> None:
    runtime = FutureMarketsLoggerRuntime(
        output_dir=tmp_path,
        run_id="run-2",
        enabled=True,
        interval_seconds=10.0,
        order_book_reader=lambda token_id: {"asks": [{"price": 0.51, "size": 1}], "bids": [{"price": 0.49, "size": 2}]},
    )
    market = _Market("cur", "cond-cur", "a", "b")
    runtime.maybe_write(
        now_epoch_ms=10_000,
        current_window_start_s=0,
        current_market=market,
        next_market=market,
        second_next_market=market,
        rtds_row={"price_usd": 1.0},
    )
    runtime.maybe_write(
        now_epoch_ms=15_000,
        current_window_start_s=0,
        current_market=market,
        next_market=market,
        second_next_market=market,
        rtds_row={"price_usd": 1.0},
    )
    runtime.maybe_write(
        now_epoch_ms=20_001,
        current_window_start_s=0,
        current_market=market,
        next_market=market,
        second_next_market=market,
        rtds_row={"price_usd": 1.0},
    )
    runtime.close()

    assert runtime.path is not None
    rows = _read_jsonl(runtime.path)
    assert len(rows) == 2


def test_future_markets_logger_writes_nulls_and_errors_when_missing_data(tmp_path: Path) -> None:
    def _reader(token_id: str) -> Dict[str, Any]:
        if token_id == "bad_yes":
            raise RuntimeError("boom")
        return {"asks": [{"price": 0.62, "size": 6}], "bids": [{"price": 0.38, "size": 7}]}

    runtime = FutureMarketsLoggerRuntime(
        output_dir=tmp_path,
        run_id="run-3",
        enabled=True,
        interval_seconds=10.0,
        order_book_reader=_reader,
    )
    runtime.maybe_write(
        now_epoch_ms=25_000,
        current_window_start_s=0,
        current_market=_Market("cur", "cond-cur", "bad_yes", "ok_no"),
        next_market=None,
        second_next_market=_Market("snx", "cond-snx", "snx_yes", ""),
        rtds_row=None,
    )
    runtime.close()

    assert runtime.path is not None
    rows = _read_jsonl(runtime.path)
    assert len(rows) == 1
    row = rows[0]

    assert row["btc_price_usd"] is None
    assert row["current_market_best_ask_price_yes"] is None
    assert row["next_market_best_ask_price_yes"] is None
    assert row["second_next_market_best_bid_price_no"] is None
    assert "btc_price_missing" in row["errors"]
    assert "next_market_market_missing" in row["errors"]
    assert any(err.startswith("current_market_yes_book_error:") for err in row["errors"])
    assert "second_next_market_no_token_missing" in row["errors"]


def test_future_markets_logger_selects_best_prices_from_unsorted_books(tmp_path: Path) -> None:
    books = {
        "cur_yes": {
            "asks": [{"price": 0.99, "size": 3}, {"price": 0.53, "size": 7}, {"price": 0.72, "size": 2}],
            "bids": [{"price": 0.01, "size": 9}, {"price": 0.47, "size": 5}, {"price": 0.35, "size": 1}],
        },
        "cur_no": {
            "asks": [{"price": 0.98, "size": 4}, {"price": 0.41, "size": 8}, {"price": 0.66, "size": 2}],
            "bids": [{"price": 0.02, "size": 6}, {"price": 0.58, "size": 3}, {"price": 0.17, "size": 1}],
        },
    }

    runtime = FutureMarketsLoggerRuntime(
        output_dir=tmp_path,
        run_id="run-4",
        enabled=True,
        interval_seconds=10.0,
        order_book_reader=lambda token_id: books[token_id],
    )
    market = _Market("cur", "cond-cur", "cur_yes", "cur_no")
    runtime.maybe_write(
        now_epoch_ms=30_000,
        current_window_start_s=0,
        current_market=market,
        next_market=market,
        second_next_market=market,
        rtds_row={"price_usd": 1.0},
    )
    runtime.close()

    assert runtime.path is not None
    row = _read_jsonl(runtime.path)[0]
    assert row["current_market_best_ask_price_yes"] == 0.53
    assert row["current_market_best_bid_price_yes"] == 0.47
    assert row["current_market_best_ask_price_no"] == 0.41
    assert row["current_market_best_bid_price_no"] == 0.58
