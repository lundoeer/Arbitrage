from __future__ import annotations

from scripts.common.position_runtime import PositionRuntime, PositionRuntimeConfig


def _runtime(
    *,
    config: PositionRuntimeConfig | None = None,
    now_epoch_ms: int = 1_000,
) -> PositionRuntime:
    return PositionRuntime(
        polymarket_token_yes="pm_yes_token",
        polymarket_token_no="pm_no_token",
        kalshi_ticker="KXBTC15M-TEST",
        config=config,
        now_epoch_ms=now_epoch_ms,
    )


def test_apply_buy_execution_result_merges_submit_acks_without_fill_change() -> None:
    runtime = _runtime()

    payload = {
        "signal_id": "sig-1",
        "status": "submitted",
        "legs": [
            {
                "venue": "polymarket",
                "side": "yes",
                "instrument_id": "pm_yes_token",
                "client_order_id": "cid-pm-1",
                "response_payload": {"response": {"id": "pm-order-1"}},
                "submitted": True,
                "error": None,
            },
            {
                "venue": "kalshi",
                "side": "no",
                "instrument_id": "KXBTC15M-TEST",
                "client_order_id": "cid-kx-1",
                "response_payload": {"response": {"order_id": "kx-order-1"}},
                "submitted": True,
                "error": None,
            },
        ],
    }
    accepted = runtime.apply_buy_execution_result(result_payload=payload, now_epoch_ms=1_500)
    assert accepted == 2
    assert len(runtime.orders_by_client_order_id) == 2
    assert runtime.orders_by_client_order_id["cid-pm-1"].order_id == "pm-order-1"
    assert runtime.orders_by_client_order_id["cid-kx-1"].order_id == "kx-order-1"
    assert runtime.position(venue="polymarket", instrument_id="pm_yes_token", outcome_side="yes")["net_contracts"] == 0.0
    assert runtime.position(venue="kalshi", instrument_id="KXBTC15M-TEST", outcome_side="no")["net_contracts"] == 0.0


def test_apply_execution_result_sell_fill_reduces_position() -> None:
    runtime = _runtime()
    payload = {
        "signal_id": "sig-sell-1",
        "status": "submitted",
        "legs": [
            {
                "venue": "polymarket",
                "side": "yes",
                "instrument_id": "pm_yes_token",
                "client_order_id": "cid-pm-sell-1",
                "request_payload": {
                    "action": "sell",
                    "size": 2.0,
                    "limit_price": 0.65,
                },
                "response_payload": {
                    "response": {
                        "order": {
                            "status": "filled",
                            "filled_size": "2.0",
                            "remaining_size": "0.0",
                        }
                    }
                },
                "submitted": True,
                "error": None,
            },
        ],
    }
    accepted = runtime.apply_execution_result(result_payload=payload, now_epoch_ms=1_600)
    assert accepted == 1
    assert runtime.position(venue="polymarket", instrument_id="pm_yes_token", outcome_side="yes")["net_contracts"] == -2.0


def test_polymarket_confirmed_fill_is_deduped_by_event_id() -> None:
    runtime = _runtime()

    applied_first = runtime.apply_polymarket_confirmed_fill(
        event_id="pm-trade-1",
        instrument_id="pm_yes_token",
        outcome_side="yes",
        filled_contracts=2.0,
        fill_price=0.44,
        now_epoch_ms=2_000,
    )
    applied_second = runtime.apply_polymarket_confirmed_fill(
        event_id="pm-trade-1",
        instrument_id="pm_yes_token",
        outcome_side="yes",
        filled_contracts=2.0,
        fill_price=0.44,
        now_epoch_ms=2_100,
    )

    assert applied_first is True
    assert applied_second is False
    assert runtime.position(venue="polymarket", instrument_id="pm_yes_token", outcome_side="yes")["net_contracts"] == 2.0
    assert runtime.snapshot()["counters"]["deduped_events"] == 1


def test_reconcile_snapshot_corrects_drift_and_overwrites_position() -> None:
    runtime = _runtime()
    runtime.apply_polymarket_confirmed_fill(
        event_id="pm-trade-2",
        instrument_id="pm_yes_token",
        outcome_side="yes",
        filled_contracts=3.0,
        fill_price=0.5,
        now_epoch_ms=3_000,
    )
    before = runtime.position(venue="polymarket", instrument_id="pm_yes_token", outcome_side="yes")
    assert before["net_contracts"] == 3.0

    result = runtime.reconcile_positions_snapshot(
        venue="polymarket",
        positions=[
            {"instrument_id": "pm_yes_token", "outcome_side": "yes", "net_contracts": 1.0, "avg_entry_price": 0.41},
            {"instrument_id": "pm_no_token", "outcome_side": "no", "net_contracts": 0.0},
        ],
        snapshot_id="poll-1",
        now_epoch_ms=3_500,
    )

    assert result["applied"] == 2
    assert result["drift_corrections"] == 1
    after = runtime.position(venue="polymarket", instrument_id="pm_yes_token", outcome_side="yes")
    assert after["net_contracts"] == 1.0
    assert after["avg_entry_price"] == 0.41
    assert runtime.snapshot()["counters"]["drift_corrections"] == 1


def test_bootstrap_fail_closed_then_auto_reenable_on_successful_reconciliation() -> None:
    runtime = _runtime()
    assert runtime.snapshot(now_epoch_ms=1_000)["health"]["buy_execution_allowed"] is False

    runtime.mark_reconcile_failure(venue="polymarket", error="timeout", now_epoch_ms=4_000)
    assert runtime.snapshot(now_epoch_ms=4_000)["health"]["buy_execution_allowed"] is False

    runtime.reconcile_positions_snapshot(
        venue="polymarket",
        positions=[
            {"instrument_id": "pm_yes_token", "outcome_side": "yes", "net_contracts": 0},
            {"instrument_id": "pm_no_token", "outcome_side": "no", "net_contracts": 0},
        ],
        snapshot_id="poll-pm-success",
        now_epoch_ms=4_100,
    )
    assert runtime.snapshot(now_epoch_ms=4_100)["health"]["buy_execution_allowed"] is False

    runtime.reconcile_positions_snapshot(
        venue="kalshi",
        positions=[
            {"instrument_id": "KXBTC15M-TEST", "outcome_side": "yes", "net_contracts": 0},
            {"instrument_id": "KXBTC15M-TEST", "outcome_side": "no", "net_contracts": 0},
        ],
        snapshot_id="poll-kx-success",
        now_epoch_ms=4_200,
    )
    assert runtime.snapshot(now_epoch_ms=4_200)["health"]["buy_execution_allowed"] is True


def test_hard_stale_blocks_buy_and_recovers_after_fresh_reconcile() -> None:
    runtime = _runtime(
        config=PositionRuntimeConfig(
            require_bootstrap_before_buy=True,
            stale_warning_seconds=1,
            stale_error_seconds=2,
        ),
        now_epoch_ms=10_000,
    )

    runtime.reconcile_positions_snapshot(
        venue="polymarket",
        positions=[
            {"instrument_id": "pm_yes_token", "outcome_side": "yes", "net_contracts": 0},
            {"instrument_id": "pm_no_token", "outcome_side": "no", "net_contracts": 0},
        ],
        snapshot_id="bootstrap-pm",
        now_epoch_ms=10_000,
    )
    runtime.reconcile_positions_snapshot(
        venue="kalshi",
        positions=[
            {"instrument_id": "KXBTC15M-TEST", "outcome_side": "yes", "net_contracts": 0},
            {"instrument_id": "KXBTC15M-TEST", "outcome_side": "no", "net_contracts": 0},
        ],
        snapshot_id="bootstrap-kx",
        now_epoch_ms=10_000,
    )
    assert runtime.snapshot(now_epoch_ms=10_000)["health"]["buy_execution_allowed"] is True

    stale_health = runtime.snapshot(now_epoch_ms=12_200)["health"]
    assert stale_health["hard_stale"] is True
    assert stale_health["buy_execution_allowed"] is False

    runtime.reconcile_positions_snapshot(
        venue="polymarket",
        positions=[
            {"instrument_id": "pm_yes_token", "outcome_side": "yes", "net_contracts": 0},
            {"instrument_id": "pm_no_token", "outcome_side": "no", "net_contracts": 0},
        ],
        snapshot_id="recover-pm",
        now_epoch_ms=12_300,
    )
    runtime.reconcile_positions_snapshot(
        venue="kalshi",
        positions=[
            {"instrument_id": "KXBTC15M-TEST", "outcome_side": "yes", "net_contracts": 0},
            {"instrument_id": "KXBTC15M-TEST", "outcome_side": "no", "net_contracts": 0},
        ],
        snapshot_id="recover-kx",
        now_epoch_ms=12_300,
    )
    recovered = runtime.snapshot(now_epoch_ms=12_300)["health"]
    assert recovered["hard_stale"] is False
    assert recovered["buy_execution_allowed"] is True


def test_max_exposure_blocks_buy_when_filled_position_exceeds_limit() -> None:
    runtime = _runtime(
        config=PositionRuntimeConfig(
            require_bootstrap_before_buy=False,
            max_exposure_per_market_usd=5.0,
            include_pending_orders_in_exposure=True,
        ),
        now_epoch_ms=1_000,
    )

    applied = runtime.apply_polymarket_confirmed_fill(
        event_id="pm-trade-exposure-1",
        instrument_id="pm_yes_token",
        outcome_side="yes",
        filled_contracts=10.0,
        fill_price=0.6,
        now_epoch_ms=1_100,
    )
    assert applied is True
    health = runtime.snapshot(now_epoch_ms=1_100)["health"]
    pm_exposure = health["exposure_by_market"]["polymarket"]
    kx_exposure = health["exposure_by_market"]["kalshi"]
    assert pm_exposure["gross_position_exposure_usd"] == 6.0
    assert pm_exposure["gross_open_order_exposure_usd"] == 0.0
    assert pm_exposure["gross_total_exposure_usd"] == 6.0
    assert pm_exposure["exposure_limit_exceeded"] is True
    assert pm_exposure["buy_blocked"] is True
    assert kx_exposure["gross_total_exposure_usd"] == 0.0
    assert kx_exposure["buy_blocked"] is False
    assert health["buy_execution_allowed"] is True


def test_max_exposure_uses_api_reported_position_exposure_when_present() -> None:
    runtime = _runtime(
        config=PositionRuntimeConfig(
            require_bootstrap_before_buy=False,
            max_exposure_per_market_usd=5.0,
            include_pending_orders_in_exposure=True,
        ),
        now_epoch_ms=1_000,
    )
    runtime.reconcile_positions_snapshot(
        venue="polymarket",
        positions=[
            {
                "instrument_id": "pm_yes_token",
                "outcome_side": "yes",
                "net_contracts": 20.0,
                "position_exposure_usd": 4.0,
            },
            {"instrument_id": "pm_no_token", "outcome_side": "no", "net_contracts": 0.0},
        ],
        snapshot_id="poll-pm-exposure-1",
        now_epoch_ms=1_100,
    )
    health = runtime.snapshot(now_epoch_ms=1_100)["health"]
    pm_exposure = health["exposure_by_market"]["polymarket"]
    assert pm_exposure["gross_position_exposure_usd"] == 4.0
    assert pm_exposure["exposure_limit_exceeded"] is False
    assert pm_exposure["buy_blocked"] is False


def test_max_exposure_counts_open_orders_at_limit_price() -> None:
    runtime = _runtime(
        config=PositionRuntimeConfig(
            require_bootstrap_before_buy=False,
            max_exposure_per_market_usd=5.0,
            include_pending_orders_in_exposure=True,
        ),
        now_epoch_ms=1_000,
    )
    payload = {
        "signal_id": "sig-open-order-exposure-1",
        "status": "submitted",
        "legs": [
            {
                "venue": "polymarket",
                "side": "no",
                "instrument_id": "pm_no_token",
                "client_order_id": "cid-open-1",
                "request_payload": {
                    "action": "buy",
                    "size": 10.0,
                    "limit_price": 0.6,
                },
                "response_payload": {
                    "response": {
                        "order": {
                            "status": "live",
                            "remaining_size": "10.0",
                        }
                    }
                },
                "submitted": True,
                "error": None,
            }
        ],
    }
    accepted = runtime.apply_buy_execution_result(result_payload=payload, now_epoch_ms=1_200)
    assert accepted == 1
    health = runtime.snapshot(now_epoch_ms=1_200)["health"]
    pm_exposure = health["exposure_by_market"]["polymarket"]
    kx_exposure = health["exposure_by_market"]["kalshi"]
    assert pm_exposure["gross_position_exposure_usd"] == 0.0
    assert pm_exposure["gross_open_order_exposure_usd"] == 6.0
    assert pm_exposure["gross_total_exposure_usd"] == 6.0
    assert pm_exposure["exposure_limit_exceeded"] is True
    assert pm_exposure["buy_blocked"] is True
    assert kx_exposure["gross_total_exposure_usd"] == 0.0
    assert health["buy_execution_allowed"] is True


def test_max_exposure_open_order_reserve_can_be_disabled() -> None:
    runtime = _runtime(
        config=PositionRuntimeConfig(
            require_bootstrap_before_buy=False,
            max_exposure_per_market_usd=5.0,
            include_pending_orders_in_exposure=False,
        ),
        now_epoch_ms=1_000,
    )
    payload = {
        "signal_id": "sig-open-order-exposure-2",
        "status": "submitted",
        "legs": [
            {
                "venue": "kalshi",
                "side": "yes",
                "instrument_id": "KXBTC15M-TEST",
                "client_order_id": "cid-open-2",
                "request_payload": {
                    "action": "buy",
                    "size": 10.0,
                    "limit_price": 0.7,
                },
                "response_payload": {
                    "response": {
                        "order": {
                            "status": "open",
                            "remaining_count_fp": "10.0",
                        }
                    }
                },
                "submitted": True,
                "error": None,
            }
        ],
    }
    accepted = runtime.apply_buy_execution_result(result_payload=payload, now_epoch_ms=1_200)
    assert accepted == 1
    health = runtime.snapshot(now_epoch_ms=1_200)["health"]
    kx_exposure = health["exposure_by_market"]["kalshi"]
    assert kx_exposure["gross_open_order_exposure_usd"] == 0.0
    assert kx_exposure["exposure_limit_exceeded"] is False
    assert health["buy_execution_allowed"] is True


def test_evaluate_execution_plan_blocks_all_buy_legs_when_any_market_exceeded() -> None:
    runtime = _runtime(
        config=PositionRuntimeConfig(
            require_bootstrap_before_buy=False,
            max_exposure_per_market_usd=5.0,
            include_pending_orders_in_exposure=True,
        ),
        now_epoch_ms=1_000,
    )
    runtime.apply_polymarket_confirmed_fill(
        event_id="pm-trade-exposure-2",
        instrument_id="pm_yes_token",
        outcome_side="yes",
        filled_contracts=10.0,
        fill_price=0.6,
        now_epoch_ms=1_100,
    )

    buy_plan = {
        "legs": [
            {
                "venue": "polymarket",
                "side": "yes",
                "action": "buy",
                "instrument_id": "pm_yes_token",
            },
            {
                "venue": "kalshi",
                "side": "no",
                "action": "buy",
                "instrument_id": "KXBTC15M-TEST",
            },
        ]
    }
    buy_gate = runtime.evaluate_execution_plan(execution_plan=buy_plan, now_epoch_ms=1_200)
    assert buy_gate["allowed"] is False
    assert buy_gate["blocked_markets"] == ["polymarket"]
    assert len(buy_gate["blocked_legs"]) == 2
    assert buy_gate["blocked_legs"][0]["venue"] == "polymarket"
    assert buy_gate["blocked_legs"][0]["action"] == "buy"
    assert buy_gate["blocked_legs"][1]["venue"] == "kalshi"
    assert buy_gate["blocked_legs"][1]["action"] == "buy"

    hedge_plan = {
        "legs": [
            {
                "venue": "polymarket",
                "side": "yes",
                "action": "sell",
                "instrument_id": "pm_yes_token",
                "size": 1.0,
            },
            {
                "venue": "kalshi",
                "side": "no",
                "action": "buy",
                "instrument_id": "KXBTC15M-TEST",
            },
        ]
    }
    hedge_gate = runtime.evaluate_execution_plan(execution_plan=hedge_plan, now_epoch_ms=1_200)
    assert hedge_gate["allowed"] is False
    assert hedge_gate["blocked_markets"] == ["polymarket"]
    assert len(hedge_gate["blocked_legs"]) == 1
    assert hedge_gate["blocked_legs"][0]["venue"] == "kalshi"
    assert hedge_gate["blocked_legs"][0]["action"] == "buy"

    sell_only_plan = {
        "legs": [
            {
                "venue": "polymarket",
                "side": "yes",
                "action": "sell",
                "instrument_id": "pm_yes_token",
                "size": 1.0,
            },
            {
                "venue": "kalshi",
                "side": "no",
                "action": "sell",
                "instrument_id": "KXBTC15M-TEST",
                "size": 1.0,
            },
        ]
    }
    sell_only_gate = runtime.evaluate_execution_plan(execution_plan=sell_only_plan, now_epoch_ms=1_200)
    assert sell_only_gate["allowed"] is False
    assert sell_only_gate["blocked_markets"] == ["kalshi", "polymarket"]
    assert len(sell_only_gate["blocked_legs"]) == 1
    assert sell_only_gate["blocked_legs"][0]["venue"] == "kalshi"
    assert sell_only_gate["blocked_legs"][0]["action"] == "sell"
    assert sell_only_gate["blocked_legs"][0]["reason"] == "insufficient_available_position_for_sell"


def test_evaluate_execution_plan_blocks_sell_when_available_position_is_insufficient() -> None:
    runtime = _runtime(
        config=PositionRuntimeConfig(
            require_bootstrap_before_buy=False,
            max_exposure_per_market_usd=0.0,
            include_pending_orders_in_exposure=True,
        ),
        now_epoch_ms=1_000,
    )
    runtime.reconcile_positions_snapshot(
        venue="polymarket",
        positions=[
            {"instrument_id": "pm_yes_token", "outcome_side": "yes", "net_contracts": 1.0},
            {"instrument_id": "pm_no_token", "outcome_side": "no", "net_contracts": 0.0},
        ],
        snapshot_id="sell-pos-pm-1",
        now_epoch_ms=1_050,
    )
    runtime.reconcile_positions_snapshot(
        venue="kalshi",
        positions=[
            {"instrument_id": "KXBTC15M-TEST", "outcome_side": "yes", "net_contracts": 0.0},
            {"instrument_id": "KXBTC15M-TEST", "outcome_side": "no", "net_contracts": 0.0},
        ],
        snapshot_id="sell-pos-kx-1",
        now_epoch_ms=1_050,
    )
    plan = {
        "legs": [
            {
                "venue": "polymarket",
                "side": "yes",
                "action": "sell",
                "instrument_id": "pm_yes_token",
                "size": 2.0,
            },
            {
                "venue": "kalshi",
                "side": "no",
                "action": "sell",
                "instrument_id": "KXBTC15M-TEST",
                "size": 1.0,
            },
        ]
    }
    gate = runtime.evaluate_execution_plan(execution_plan=plan, now_epoch_ms=1_100)
    assert gate["allowed"] is False
    assert "insufficient_available_position_for_sell" in gate["reasons"]
    sell_blocks = [item for item in gate["blocked_legs"] if item.get("action") == "sell"]
    assert len(sell_blocks) == 2


def test_evaluate_execution_plan_allows_sell_when_available_position_is_sufficient() -> None:
    runtime = _runtime(
        config=PositionRuntimeConfig(
            require_bootstrap_before_buy=False,
            max_exposure_per_market_usd=0.0,
            include_pending_orders_in_exposure=True,
        ),
        now_epoch_ms=1_000,
    )
    runtime.reconcile_positions_snapshot(
        venue="polymarket",
        positions=[
            {"instrument_id": "pm_yes_token", "outcome_side": "yes", "net_contracts": 3.0},
            {"instrument_id": "pm_no_token", "outcome_side": "no", "net_contracts": 0.0},
        ],
        snapshot_id="sell-pos-pm-2",
        now_epoch_ms=1_050,
    )
    runtime.reconcile_positions_snapshot(
        venue="kalshi",
        positions=[
            {"instrument_id": "KXBTC15M-TEST", "outcome_side": "yes", "net_contracts": 0.0},
            {"instrument_id": "KXBTC15M-TEST", "outcome_side": "no", "net_contracts": 2.0},
        ],
        snapshot_id="sell-pos-kx-2",
        now_epoch_ms=1_050,
    )
    plan = {
        "legs": [
            {
                "venue": "polymarket",
                "side": "yes",
                "action": "sell",
                "instrument_id": "pm_yes_token",
                "size": 2.0,
            },
            {
                "venue": "kalshi",
                "side": "no",
                "action": "sell",
                "instrument_id": "KXBTC15M-TEST",
                "size": 1.0,
            },
        ]
    }
    gate = runtime.evaluate_execution_plan(execution_plan=plan, now_epoch_ms=1_100)
    assert gate["allowed"] is True
    assert gate["blocked_legs"] == []


def test_order_lifecycle_ws_and_poll_remove_closed_orders_immediately() -> None:
    runtime = _runtime(
        config=PositionRuntimeConfig(
            require_bootstrap_before_buy=False,
            include_pending_orders_in_exposure=True,
        ),
        now_epoch_ms=1_000,
    )

    submit_payload = {
        "signal_id": "sig-ord-life-1",
        "status": "submitted",
        "legs": [
            {
                "venue": "kalshi",
                "side": "yes",
                "instrument_id": "KXBTC15M-TEST",
                "client_order_id": "cid-life-1",
                "request_payload": {
                    "action": "buy",
                    "size": 10.0,
                    "limit_price": 0.6,
                },
                "response_payload": {
                    "response": {
                        "order": {
                            "order_id": "oid-life-1",
                            "status": "open",
                            "remaining_count_fp": "10.0",
                        }
                    }
                },
                "submitted": True,
                "error": None,
            }
        ],
    }
    runtime.apply_buy_execution_result(result_payload=submit_payload, now_epoch_ms=1_100)
    assert runtime.orders_by_client_order_id.get("cid-life-1") is not None

    ws_closed = runtime.apply_kalshi_user_order_event(
        event_payload={
            "event_id": "ev-kx-closed-1",
            "client_order_id": "cid-life-1",
            "order_id": "oid-life-1",
            "market_ticker": "KXBTC15M-TEST",
            "outcome_side": "yes",
            "action": "buy",
            "status": "FILLED",
            "requested_size": 10.0,
            "filled_size": 10.0,
            "remaining_size": 0.0,
            "limit_price": 0.6,
        },
        now_epoch_ms=1_200,
    )
    assert ws_closed is True
    assert runtime.orders_by_client_order_id.get("cid-life-1") is None

    reopen = runtime.reconcile_orders_snapshot(
        venue="kalshi",
        orders=[
            {
                "client_order_id": "cid-life-1",
                "order_id": "oid-life-1",
                "instrument_id": "KXBTC15M-TEST",
                "outcome_side": "yes",
                "action": "buy",
                "status": "open",
                "requested_size": 10.0,
                "filled_size": 2.0,
                "remaining_size": 8.0,
                "limit_price": 0.6,
            }
        ],
        snapshot_id="poll-kx-orders-open-1",
        now_epoch_ms=1_300,
    )
    assert reopen["applied"] == 1
    assert runtime.orders_by_client_order_id.get("cid-life-1") is not None

    reconcile = runtime.reconcile_orders_snapshot(
        venue="kalshi",
        orders=[
            {
                "client_order_id": "cid-life-1",
                "order_id": "oid-life-1",
                "instrument_id": "KXBTC15M-TEST",
                "outcome_side": "yes",
                "action": "buy",
                "status": "filled",
                "requested_size": 10.0,
                "filled_size": 10.0,
                "remaining_size": 0.0,
                "limit_price": 0.6,
            }
        ],
        snapshot_id="poll-kx-orders-1",
        now_epoch_ms=1_400,
    )
    assert reconcile["applied"] == 1
    assert runtime.orders_by_client_order_id.get("cid-life-1") is None


def test_open_order_helper_by_venue_tracks_terminal_vs_open() -> None:
    runtime = _runtime(
        config=PositionRuntimeConfig(
            require_bootstrap_before_buy=False,
            include_pending_orders_in_exposure=True,
        ),
        now_epoch_ms=1_000,
    )

    open_payload = {
        "signal_id": "sig-open-helper-1",
        "status": "submitted",
        "legs": [
            {
                "venue": "polymarket",
                "side": "yes",
                "instrument_id": "pm_yes_token",
                "client_order_id": "cid-open-helper-pm",
                "request_payload": {
                    "action": "buy",
                    "size": 5.0,
                    "limit_price": 0.51,
                },
                "response_payload": {
                    "response": {
                        "order": {
                            "status": "open",
                            "remaining_size": "5.0",
                        }
                    }
                },
                "submitted": True,
                "error": None,
            },
            {
                "venue": "kalshi",
                "side": "no",
                "instrument_id": "KXBTC15M-TEST",
                "client_order_id": "cid-open-helper-kx",
                "request_payload": {
                    "action": "buy",
                    "size": 4.0,
                    "limit_price": 0.49,
                },
                "response_payload": {
                    "response": {
                        "order": {
                            "status": "filled",
                            "remaining_count_fp": "0.0",
                        }
                    }
                },
                "submitted": True,
                "error": None,
            },
        ],
    }
    runtime.apply_buy_execution_result(result_payload=open_payload, now_epoch_ms=1_100)

    assert runtime.has_open_orders_for_venue("polymarket") is True
    assert runtime.has_open_orders_for_venue("kalshi") is False
    assert runtime.open_order_counts_by_venue() == {"polymarket": 1, "kalshi": 0}

    runtime.reconcile_orders_snapshot(
        venue="polymarket",
        orders=[
            {
                "client_order_id": "cid-open-helper-pm",
                "instrument_id": "pm_yes_token",
                "outcome_side": "yes",
                "action": "buy",
                "status": "filled",
                "requested_size": 5.0,
                "filled_size": 5.0,
                "remaining_size": 0.0,
                "limit_price": 0.51,
            }
        ],
        snapshot_id="poll-close-pm-open-helper",
        now_epoch_ms=1_200,
    )
    assert runtime.has_open_orders_for_venue("polymarket") is False
    assert runtime.open_order_counts_by_venue() == {"polymarket": 0, "kalshi": 0}
