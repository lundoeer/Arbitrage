from __future__ import annotations

import json
from pathlib import Path
from typing import List

from scripts.surveillance.market_surveillance import (
    JsonlFileWriter,
    StatusFileWriter,
    UnderlyingPricePoint,
    build_snapshot_row,
    run_mock_market_surveillance,
)


def _read_jsonl(path: Path) -> List[dict]:
    rows: List[dict] = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line:
            continue
        rows.append(json.loads(line))
    return rows


def test_jsonl_writer_appends_rows(tmp_path: Path) -> None:
    out_path = tmp_path / "surveillance.jsonl"
    writer = JsonlFileWriter(out_path)
    try:
        writer.write({"a": 1})
        writer.write({"b": "x"})
    finally:
        writer.close()

    rows = _read_jsonl(out_path)
    assert len(rows) == 2
    assert rows[0]["a"] == 1
    assert rows[1]["b"] == "x"


def test_status_writer_overwrites_with_latest_payload(tmp_path: Path) -> None:
    status_path = tmp_path / "status.json"
    writer = StatusFileWriter(status_path)
    writer.write({"state": "starting", "ticks_written": 1})
    writer.write({"state": "running", "ticks_written": 2})

    payload = json.loads(status_path.read_text(encoding="utf-8"))
    assert payload["state"] == "running"
    assert payload["ticks_written"] == 2


def test_build_snapshot_row_schema_shape() -> None:
    row = build_snapshot_row(
        run_id="run-1",
        recv_ms=1772232001123,
        ts="2026-02-27T12:00:01.123+00:00",
        pair={
            "kalshi_ticker": "KXBTC15M-27FEB1200-00",
            "polymarket_slug": "btc-updown-15m-1772231100",
            "polymarket_market_id": "1234",
            "polymarket_token_yes": "yes-token",
            "polymarket_token_no": "no-token",
            "window_end": "2026-02-27T12:15:00+00:00",
        },
        chainlink_price=UnderlyingPricePoint(
            source="chainlink_mock",
            price_usd=66820.12,
            source_timestamp_ms=1772232001000,
        ),
        kalshi_price=UnderlyingPricePoint(
            source="kalshi_site_mock",
            price_usd=66819.88,
            source_timestamp_ms=1772232001000,
        ),
        best_asks={
            "polymarket_yes": {"price": 0.41, "size": 100.0},
            "polymarket_no": {"price": 0.60, "size": 110.0},
            "kalshi_yes": {"price": 0.42, "size": 120.0},
            "kalshi_no": {"price": 0.59, "size": 130.0},
        },
        errors=[],
    )

    assert row["run_id"] == "run-1"
    assert row["underlying_prices"]["polymarket_chainlink"]["price_usd"] == 66820.12
    assert row["underlying_prices"]["kalshi_site"]["price_usd"] == 66819.88
    assert row["best_asks"]["polymarket_yes"]["price"] == 0.41
    assert row["best_asks"]["kalshi_no"]["size"] == 130.0
    assert row["health"]["quotes_fresh"] is True
    assert row["health"]["underlying_fresh"] is True
    assert row["health"]["errors"] == []


def test_run_mock_market_surveillance_writes_rows_and_status(tmp_path: Path) -> None:
    out_path = tmp_path / "btc_15m_per_second.jsonl"
    status_path = tmp_path / "status.json"

    tick_clock = {"value": 1772232000000}

    def _now_ms() -> int:
        current = tick_clock["value"]
        tick_clock["value"] += 1000
        return current

    def _now_iso(epoch_ms: int) -> str:
        return f"iso-{epoch_ms}"

    summary = run_mock_market_surveillance(
        output_jsonl_path=out_path,
        status_path=status_path,
        run_id="fixed-run",
        tick_seconds=0.0,
        duration_seconds=0,
        max_ticks=3,
        now_ms_fn=_now_ms,
        now_iso_fn=_now_iso,
        sleep_fn=lambda _: None,
    )

    assert summary["run_id"] == "fixed-run"
    assert summary["ticks_written"] == 3

    rows = _read_jsonl(out_path)
    assert len(rows) == 3
    assert all(row["run_id"] == "fixed-run" for row in rows)
    assert all("best_asks" in row for row in rows)
    assert all("underlying_prices" in row for row in rows)

    status_payload = json.loads(status_path.read_text(encoding="utf-8"))
    assert status_payload["kind"] == "market_surveillance_status"
    assert status_payload["run_id"] == "fixed-run"
    assert status_payload["ticks_written"] == 3

