from __future__ import annotations

from scripts.common.polymarket_redeem import (
    REDEEM_POSITIONS_SELECTOR,
    ZERO_BYTES32,
    build_redeem_positions_calldata,
    group_redeem_targets,
    parse_redeemable_rows,
    redeem_positions,
)


def _bytes32_hex(fill: str) -> str:
    return "0x" + (fill * 64)


def test_parse_redeemable_rows_filters_invalid_and_non_redeemable() -> None:
    rows = [
        {
            "conditionId": _bytes32_hex("a"),
            "outcomeIndex": 1,
            "size": "3.25",
            "redeemable": True,
            "asset": "token_a",
            "proxyWallet": "0x1111111111111111111111111111111111111111",
        },
        {
            "conditionId": _bytes32_hex("b"),
            "outcomeIndex": 0,
            "size": "5",
            "redeemable": False,
            "asset": "token_b",
        },
        {
            "conditionId": "bad",
            "outcomeIndex": 0,
            "size": "5",
            "redeemable": True,
            "asset": "token_c",
        },
    ]

    parsed = parse_redeemable_rows(rows=rows)
    assert len(parsed) == 1
    row = parsed[0]
    assert row.condition_id == _bytes32_hex("a")
    assert row.outcome_index == 1
    assert row.index_set == 2
    assert row.size == 3.25


def test_group_redeem_targets_combines_index_sets_per_condition() -> None:
    parsed = parse_redeemable_rows(
        rows=[
            {"conditionId": _bytes32_hex("a"), "outcomeIndex": 0, "size": 1, "redeemable": True},
            {"conditionId": _bytes32_hex("a"), "outcomeIndex": 1, "size": 2, "redeemable": True},
            {"conditionId": _bytes32_hex("b"), "outcomeIndex": 1, "size": 3, "redeemable": True},
        ]
    )
    targets = group_redeem_targets(
        rows=parsed,
        collateral_token="0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",
        parent_collection_id=ZERO_BYTES32,
    )
    assert len(targets) == 2
    assert targets[0].condition_id == _bytes32_hex("a")
    assert targets[0].index_sets == (1, 2)
    assert targets[1].condition_id == _bytes32_hex("b")
    assert targets[1].index_sets == (2,)


def test_build_redeem_positions_calldata_prefix_and_non_empty_payload() -> None:
    data = build_redeem_positions_calldata(
        collateral_token="0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",
        parent_collection_id=ZERO_BYTES32,
        condition_id=_bytes32_hex("c"),
        index_sets=[2],
    )
    assert data.startswith(REDEEM_POSITIONS_SELECTOR)
    assert len(data) > len(REDEEM_POSITIONS_SELECTOR)


def test_redeem_positions_blocks_sender_mismatch(monkeypatch) -> None:
    import scripts.common.polymarket_redeem as mod

    monkeypatch.setattr(
        mod,
        "resolve_ctf_contracts",
        lambda: {
            "ctf_contract": "0x4d97dcd97ec945f40cf65f87097ace5ea0476045",
            "collateral_token": "0x2791bca1f2de4661ed88a30c99a7a9449aa84174",
        },
    )
    monkeypatch.setattr(
        mod,
        "fetch_polymarket_positions",
        lambda **_: [
            {
                "conditionId": _bytes32_hex("d"),
                "outcomeIndex": 1,
                "size": 1,
                "redeemable": True,
                "proxyWallet": "0x1111111111111111111111111111111111111111",
            }
        ],
    )
    monkeypatch.setattr(
        mod,
        "resolve_tx_sender_address",
        lambda **_: "0x2222222222222222222222222222222222222222",
    )

    class _RpcClient:
        def __init__(self, **_: object) -> None:
            pass

        def call(self, method: str, params: object) -> object:
            raise AssertionError(f"rpc should not be called on mismatch: {method} {params}")

    monkeypatch.setattr(mod, "PolygonRpcClient", _RpcClient)

    try:
        redeem_positions(
            user_address="0x1111111111111111111111111111111111111111",
            private_key="dummy",
            dry_run=False,
            enforce_sender_match=True,
        )
        assert False, "expected mismatch runtime error"
    except RuntimeError as exc:
        assert "tx_sender_mismatch" in str(exc)
