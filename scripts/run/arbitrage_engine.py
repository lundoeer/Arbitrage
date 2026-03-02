from __future__ import annotations

import asyncio
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.common.buy_execution import BuyExecutionClients, BuyIdempotencyState
from scripts.common.decision_runtime import SharePriceRuntime
from scripts.common.engine_logger import EngineLogger
from scripts.common.engine_setup import (
    build_buy_execution_clients,
    build_sell_execution_clients,
    build_position_components,
)
from scripts.common.kalshi_auth import resolve_kalshi_ws_headers
from scripts.common.position_polling import PositionReconcileLoop, capture_account_portfolio_snapshot, PositionReconcileLoopConfig
from scripts.common.position_runtime import PositionRuntime, PositionRuntimeConfig
from scripts.common.run_config import (
    BuyExecutionRuntimeConfig,
    PositionMonitoringRuntimeConfig,
    SellExecutionRuntimeConfig,
    WsHealthConfig,
)
from scripts.common.ws_collectors import KalshiWsCollector, PolymarketWsCollector
from scripts.common.ws_transport import NullWriter, now_ms
from scripts.common.utils import as_dict as _as_dict, as_float as _as_float
from scripts.run.engine_loop import run_core_loop


class ArbitrageEngine:
    def __init__(
        self,
        *,
        logger: EngineLogger,
        health_config: WsHealthConfig,
        decision_config: Any,
        buy_execution_config: BuyExecutionRuntimeConfig,
        sell_execution_config: SellExecutionRuntimeConfig,
        position_monitoring_config: PositionMonitoringRuntimeConfig,
        position_reconcile_config: PositionReconcileLoopConfig,
        buy_execution_requested: bool,
        sell_execution_requested: bool,
        position_monitoring_requested: bool,
        account_snapshot_logging_enabled: bool,
        buy_execution_cooldown_ms: int,
        buy_execution_max_attempts: int,
        buy_execution_parallel_leg_timeout_ms: int,
        sell_execution_parallel_leg_timeout_ms: int,
        polymarket_user_ws_enabled: bool,
        kalshi_market_positions_ws_enabled: bool,
        kalshi_user_orders_ws_enabled: bool,
        kalshi_channels: List[str],
        args: Any,
    ) -> None:
        self.logger = logger
        self.health_config = health_config
        self.decision_config = decision_config
        self.buy_execution_config = buy_execution_config
        self.sell_execution_config = sell_execution_config
        self.position_monitoring_config = position_monitoring_config
        self.position_reconcile_config = position_reconcile_config

        self.buy_execution_requested = buy_execution_requested
        self.sell_execution_requested = sell_execution_requested
        self.position_monitoring_requested = position_monitoring_requested
        self.account_snapshot_logging_enabled = account_snapshot_logging_enabled
        self.buy_execution_cooldown_ms = buy_execution_cooldown_ms
        self.buy_execution_max_attempts = buy_execution_max_attempts
        self.buy_execution_parallel_leg_timeout_ms = buy_execution_parallel_leg_timeout_ms
        self.sell_execution_parallel_leg_timeout_ms = sell_execution_parallel_leg_timeout_ms
        self.polymarket_user_ws_enabled = polymarket_user_ws_enabled
        self.kalshi_market_positions_ws_enabled = kalshi_market_positions_ws_enabled
        self.kalshi_user_orders_ws_enabled = kalshi_user_orders_ws_enabled
        self.kalshi_channels = kalshi_channels
        self.args = args

        self.segment_index = 0
        self.buy_idempotency_state = BuyIdempotencyState()
        self.buy_execution_attempt_state: Dict[str, int] = {"attempts_used": 0}
        
        self.buy_execution_setup_errors: list[str] = []
        self.sell_execution_setup_errors: list[str] = []
        self.position_monitoring_setup_errors: list[str] = []
        self.account_snapshot_setup_errors: list[str] = []

        self.buy_execution_enabled_last = False
        self.sell_execution_enabled_last = False
        self.position_monitoring_enabled_last = False

        self.market_segments: list[Dict[str, Any]] = []
        self.raw_event_segments: list[Dict[str, Any]] = []
        self.stats: Dict[str, Any] = self._initial_stats()

        self.last_kalshi_health: Dict[str, Any] = {}
        self.last_polymarket_health: Dict[str, Any] = {}
        self.first_selection: Dict[str, Any] = {}
        self.last_selection: Dict[str, Any] = {}

        self.run_account_snapshot_start: Optional[Dict[str, Any]] = None
        self.run_account_snapshot_end: Optional[Dict[str, Any]] = None

        self.last_snapshot_polymarket_client = None
        self.last_snapshot_polymarket_account_client = None
        self.last_snapshot_kalshi_client = None
        self.last_market_window_end_epoch_ms: Optional[int] = None
        
        self.total_kalshi_raw_messages = 0
        self.total_kalshi_normalized_events = 0
        self.total_polymarket_raw_messages = 0
        self.total_polymarket_normalized_events = 0

    def _initial_stats(self) -> Dict[str, Any]:
        return {
            "decision_samples": 0,
            "can_trade_true_samples": 0,
            "can_trade_false_samples": 0,
            "decision_ready_true_samples": 0,
            "decision_ready_false_samples": 0,
            "runtime_memory_samples": 0,
            "decision_logged_samples": 0,
            "buy_decision_logged_samples": 0,
            "edge_snapshot_samples": 0,
            "buy_execution_attempts": 0,
            "buy_execution_submitted": 0,
            "buy_execution_partially_submitted": 0,
            "buy_execution_rejected": 0,
            "buy_execution_skipped_idempotent": 0,
            "buy_execution_errors": 0,
            "buy_execution_disabled_signals": 0,
            "buy_execution_blocked_fsm_signals": 0,
            "buy_execution_blocked_max_attempts": 0,
            "buy_execution_blocked_position_health": 0,
            "sell_execution_attempts": 0,
            "sell_execution_submitted": 0,
            "sell_execution_partially_submitted": 0,
            "sell_execution_rejected": 0,
            "sell_execution_skipped_idempotent": 0,
            "sell_execution_errors": 0,
            "sell_execution_disabled_signals": 0,
            "sell_execution_blocked_fsm_signals": 0,
            "sell_execution_blocked_position_health": 0,
            "position_poll_iterations": 0,
            "position_poll_polymarket_success": 0,
            "position_poll_polymarket_failure": 0,
            "position_poll_kalshi_success": 0,
            "position_poll_kalshi_failure": 0,
            "position_poll_polymarket_orders_success": 0,
            "position_poll_polymarket_orders_failure": 0,
            "position_poll_kalshi_orders_success": 0,
            "position_poll_kalshi_orders_failure": 0,
            "position_ws_polymarket_user_events": 0,
            "position_ws_kalshi_market_position_events": 0,
            "position_ws_kalshi_user_order_events": 0,
            "position_health_state_changes": 0,
            "position_health_allowed_samples": 0,
            "position_health_blocked_samples": 0,
            "position_health_bootstrap_completed_transitions": 0,
            "position_health_hard_stale_transitions": 0,
            "last_position_health": None,
            "last_position_poll": None,
            "last_decision": None,
            "last_buy_execution": None,
            "last_sell_execution": None,
            "buy_fsm": None,
        }

    def run_segment(
        self,
        *,
        selection: Dict[str, Any],
        market_window_end_epoch_ms: int,
        segment_duration_seconds: int,
    ) -> None:
        selection_source = str(selection.get("selection_source") or "").strip()
        pm_slug = str(selection["polymarket"]["event_slug"])
        pm_market_id = str(selection["polymarket"]["market_id"])
        pm_condition_id = str(selection["polymarket"].get("condition_id") or "").strip()
        pm_yes = str(selection["polymarket"]["token_yes"])
        pm_no = str(selection["polymarket"]["token_no"])
        kx_ticker = str(selection["kalshi"]["ticker"])
        is_manual_setup = selection_source == "manual_setup_file"
        is_standard_btc15 = str(kx_ticker).upper().startswith("KXBTC15M")
        disable_market_not_open_check = bool(is_manual_setup and not is_standard_btc15)
        if disable_market_not_open_check:
            print(
                "Time gate override enabled: skipping market_not_open check "
                "for manual non-BTC15 market."
            )
        
        # Build Buy Execution Clients
        segment_buy_execution_enabled = False
        segment_buy_execution_clients = BuyExecutionClients()
        segment_buy_execution_setup_errors: list[str] = []
        if self.buy_execution_requested:
            (
                segment_buy_execution_enabled,
                segment_buy_execution_clients,
                segment_buy_execution_setup_errors,
            ) = build_buy_execution_clients(
                enable_buy_execution=True,
                buy_execution_config=self.buy_execution_config,
            )
            self.buy_execution_enabled_last = bool(segment_buy_execution_enabled)
            for entry in segment_buy_execution_setup_errors:
                if entry not in self.buy_execution_setup_errors:
                    self.buy_execution_setup_errors.append(entry)
            if not bool(segment_buy_execution_enabled):
                print(
                    "Buy execution setup failed for discovered market segment; "
                    "execution disabled for this segment:"
                )
                for entry in segment_buy_execution_setup_errors:
                    print(f"  - {entry}")
            else:
                print("Buy execution enabled for current market segment.")
        else:
            self.buy_execution_enabled_last = False

        # Build Sell Execution Clients
        segment_sell_execution_enabled = False
        segment_sell_execution_clients = BuyExecutionClients()
        segment_sell_execution_setup_errors: list[str] = []
        if self.sell_execution_requested:
            (
                segment_sell_execution_enabled,
                segment_sell_execution_clients,
                segment_sell_execution_setup_errors,
            ) = build_sell_execution_clients(
                enable_sell_execution=True,
                sell_execution_config=self.sell_execution_config,
            )
            self.sell_execution_enabled_last = bool(segment_sell_execution_enabled)
            for entry in segment_sell_execution_setup_errors:
                if entry not in self.sell_execution_setup_errors:
                    self.sell_execution_setup_errors.append(entry)
            if not bool(segment_sell_execution_enabled):
                print(
                    "Sell execution setup failed for discovered market segment; "
                    "execution disabled for this segment:"
                )
                for entry in segment_sell_execution_setup_errors:
                    print(f"  - {entry}")
            else:
                print("Sell execution enabled for current market segment.")
        else:
            self.sell_execution_enabled_last = False

        # Build position runtimes and websockets
        segment_position_runtime: Optional[PositionRuntime] = None
        segment_position_reconcile_loop: Optional[PositionReconcileLoop] = None
        segment_position_event_state: Dict[str, int] = {
            "polymarket_user_events": 0,
            "kalshi_market_position_events": 0,
            "kalshi_user_order_events": 0,
        }
        
        pm_raw_writer = NullWriter()
        pm_event_writer = NullWriter()
        kx_raw_writer = NullWriter()
        kx_event_writer = NullWriter()
        pm_user_raw_writer = NullWriter()
        pm_user_event_writer = NullWriter()
        kx_market_positions_raw_writer = NullWriter()
        kx_market_positions_event_writer = NullWriter()
        kx_user_orders_raw_writer = NullWriter()
        kx_user_orders_event_writer = NullWriter()
        
        ws_writers = []
        if bool(getattr(self.args, "log_raw_events", False)):
            data_dir = self.logger.project_root / "data"
            poly_dir = data_dir / "websocket_poly"
            kalshi_dir = data_dir / "websocket_kalshi"
            poly_dir.mkdir(parents=True, exist_ok=True)
            kalshi_dir.mkdir(parents=True, exist_ok=True)
            run_id = self.logger.run_id
            
            from scripts.common.ws_transport import JsonlWriter
            
            pm_raw_writer = JsonlWriter(poly_dir / f"raw_engine__{self.segment_index}_{run_id}.jsonl")
            pm_event_writer = JsonlWriter(poly_dir / f"events_engine__{self.segment_index}_{run_id}.jsonl")
            kx_raw_writer = JsonlWriter(kalshi_dir / f"raw_engine__{self.segment_index}_{run_id}.jsonl")
            kx_event_writer = JsonlWriter(kalshi_dir / f"events_engine__{self.segment_index}_{run_id}.jsonl")
            
            ws_writers.extend([pm_raw_writer, pm_event_writer, kx_raw_writer, kx_event_writer])
            
            if self.polymarket_user_ws_enabled:
                pm_user_raw_writer = JsonlWriter(poly_dir / f"raw_user__{self.segment_index}_{run_id}.jsonl")
                pm_user_event_writer = JsonlWriter(poly_dir / f"events_user__{self.segment_index}_{run_id}.jsonl")
                ws_writers.extend([pm_user_raw_writer, pm_user_event_writer])
                
            if self.kalshi_market_positions_ws_enabled:
                kx_market_positions_raw_writer = JsonlWriter(kalshi_dir / f"raw_market_positions__{self.segment_index}_{run_id}.jsonl")
                kx_market_positions_event_writer = JsonlWriter(kalshi_dir / f"events_market_positions__{self.segment_index}_{run_id}.jsonl")
                ws_writers.extend([kx_market_positions_raw_writer, kx_market_positions_event_writer])
            if self.kalshi_user_orders_ws_enabled:
                kx_user_orders_raw_writer = JsonlWriter(kalshi_dir / f"raw_user_orders__{self.segment_index}_{run_id}.jsonl")
                kx_user_orders_event_writer = JsonlWriter(kalshi_dir / f"events_user_orders__{self.segment_index}_{run_id}.jsonl")
                ws_writers.extend([kx_user_orders_raw_writer, kx_user_orders_event_writer])

        def _on_pm_user_event(event: Dict[str, Any]) -> None:
            if segment_position_runtime is None:
                return
            kind = str(event.get("kind") or "")
            if kind in {"polymarket_user_trade", "polymarket_user_order"}:
                segment_position_event_state["polymarket_user_events"] = (
                    int(segment_position_event_state.get("polymarket_user_events", 0)) + 1
                )
            if kind == "polymarket_user_order":
                segment_position_runtime.apply_polymarket_user_order_event(
                    event_payload=event,
                    now_epoch_ms=now_ms(),
                )
            if kind == "polymarket_user_trade" and bool(event.get("is_confirmed")):
                size = _as_float(event.get("size"))
                price = _as_float(event.get("price"))
                asset_id = str(event.get("asset_id") or "").strip()
                outcome_side = str(event.get("outcome_side") or "").strip().lower()
                if size is not None and size > 0 and asset_id and outcome_side in {"yes", "no"}:
                    segment_position_runtime.apply_polymarket_confirmed_fill(
                        event_id=str(event.get("event_id") or ""),
                        instrument_id=asset_id,
                        outcome_side=outcome_side,
                        filled_contracts=float(size),
                        fill_price=price,
                        action=str(event.get("order_side") or ""),
                        now_epoch_ms=now_ms(),
                    )
            if kind in {"polymarket_user_trade", "polymarket_user_order"}:
                self.logger.write_position_event(
                    recv_ms=now_ms(),
                    sub_kind=kind.replace("position_", ""),
                    payload={"event": event, "position_health": segment_position_runtime.refresh_health(now_epoch_ms=now_ms())}
                )

        def _on_kx_market_position_event(event: Dict[str, Any]) -> None:
            if segment_position_runtime is None:
                return
            kind = str(event.get("kind") or "")
            if kind != "kalshi_market_position":
                return
            segment_position_event_state["kalshi_market_position_events"] = (
                int(segment_position_event_state.get("kalshi_market_position_events", 0)) + 1
            )
            ticker = str(event.get("market_ticker") or "").strip()
            if ticker != kx_ticker:
                return
            yes_size = _as_float(event.get("position_yes"))
            no_size = _as_float(event.get("position_no"))
            event_id = str(event.get("event_id") or event.get("source_timestamp_ms") or now_ms())
            if yes_size is not None:
                segment_position_runtime.apply_kalshi_market_position(
                    event_id=f"{event_id}:yes",
                    instrument_id=ticker,
                    outcome_side="yes",
                    net_contracts=float(yes_size),
                    now_epoch_ms=now_ms(),
                )
            if no_size is not None:
                segment_position_runtime.apply_kalshi_market_position(
                    event_id=f"{event_id}:no",
                    instrument_id=ticker,
                    outcome_side="no",
                    net_contracts=float(no_size),
                    now_epoch_ms=now_ms(),
                )
            self.logger.write_position_event(
                recv_ms=now_ms(),
                sub_kind="kalshi_market_position",
                payload={"event": event, "position_health": segment_position_runtime.refresh_health(now_epoch_ms=now_ms())}
            )

        def _on_kx_user_order_event(event: Dict[str, Any]) -> None:
            if segment_position_runtime is None:
                return
            kind = str(event.get("kind") or "")
            if kind != "kalshi_user_order":
                return
            segment_position_event_state["kalshi_user_order_events"] = (
                int(segment_position_event_state.get("kalshi_user_order_events", 0)) + 1
            )
            segment_position_runtime.apply_kalshi_user_order_event(
                event_payload=event,
                now_epoch_ms=now_ms(),
            )
            self.logger.write_position_event(
                recv_ms=now_ms(),
                sub_kind="kalshi_user_order",
                payload={"event": event, "position_health": segment_position_runtime.refresh_health(now_epoch_ms=now_ms())}
            )

        (
            pm_poll_client,
            pm_account_poll_client,
            kx_poll_client,
            pm_orders_poll_client,
            kx_orders_poll_client,
            segment_polymarket_user_collector,
            segment_kalshi_market_positions_collector,
            segment_kalshi_user_orders_collector,
            segment_position_setup_errors,
        ) = build_position_components(
            enable_position_monitoring=self.position_monitoring_requested,
            enable_account_snapshots=self.account_snapshot_logging_enabled,
            pm_yes=pm_yes,
            pm_no=pm_no,
            pm_condition_id=pm_condition_id,
            kx_ticker=kx_ticker,
            polymarket_user_ws_enabled=self.polymarket_user_ws_enabled,
            kalshi_market_positions_ws_enabled=self.kalshi_market_positions_ws_enabled,
            kalshi_user_orders_ws_enabled=self.kalshi_user_orders_ws_enabled,
            health_config=self.health_config,
            on_pm_user_event=_on_pm_user_event,
            on_kx_market_position_event=_on_kx_market_position_event,
            on_kx_user_order_event=_on_kx_user_order_event,
            pm_user_raw_writer=pm_user_raw_writer,
            pm_user_event_writer=pm_user_event_writer,
            kx_market_positions_raw_writer=kx_market_positions_raw_writer,
            kx_market_positions_event_writer=kx_market_positions_event_writer,
            kx_user_orders_raw_writer=kx_user_orders_raw_writer,
            kx_user_orders_event_writer=kx_user_orders_event_writer,
        )

        segment_position_monitoring_enabled = False
        if self.position_monitoring_requested:
            segment_position_runtime = PositionRuntime(
                polymarket_token_yes=pm_yes,
                polymarket_token_no=pm_no,
                kalshi_ticker=kx_ticker,
                config=PositionRuntimeConfig(
                    require_bootstrap_before_buy=bool(self.position_monitoring_config.require_bootstrap_before_buy),
                    drift_tolerance_contracts=float(self.position_monitoring_config.drift_tolerance_contracts),
                    max_exposure_per_market_usd=float(self.position_monitoring_config.max_exposure_per_market_usd),
                    include_pending_orders_in_exposure=bool(
                        self.position_monitoring_config.include_pending_orders_in_exposure
                    ),
                    stale_warning_seconds=int(self.position_monitoring_config.stale_warning_seconds),
                    stale_error_seconds=int(self.position_monitoring_config.stale_error_seconds),
                ),
                now_epoch_ms=now_ms(),
            )

            segment_position_reconcile_loop = PositionReconcileLoop(
                runtime=segment_position_runtime,
                polymarket_client=pm_poll_client,
                kalshi_client=kx_poll_client,
                polymarket_orders_client=pm_orders_poll_client,
                kalshi_orders_client=kx_orders_poll_client,
                config=self.position_reconcile_config,
                log_raw_http=bool(getattr(self.args, "log_raw_events", False)),
            )
            segment_position_monitoring_enabled = bool(segment_position_reconcile_loop is not None)
            self.position_monitoring_enabled_last = bool(segment_position_monitoring_enabled)

        if segment_position_setup_errors:
            for entry in segment_position_setup_errors:
                if bool(self.position_monitoring_requested) and entry not in self.position_monitoring_setup_errors:
                    self.position_monitoring_setup_errors.append(entry)
                if bool(self.account_snapshot_logging_enabled) and entry not in self.account_snapshot_setup_errors:
                    self.account_snapshot_setup_errors.append(entry)
            if bool(self.position_monitoring_requested):
                print("Position monitoring setup warnings for current market segment:")
            elif bool(self.account_snapshot_logging_enabled):
                print("Account snapshot setup warnings for current market segment:")
            for entry in segment_position_setup_errors:
                print(f"  - {entry}")

        if self.segment_index == 0:
            self.first_selection = selection
        self.last_selection = selection
        self.last_market_window_end_epoch_ms = int(market_window_end_epoch_ms)
        runtime = SharePriceRuntime(polymarket_token_yes=pm_yes, polymarket_token_no=pm_no)
        segment_market_context = {
            "polymarket_event_slug": pm_slug,
            "polymarket_market_id": pm_market_id,
            "polymarket_condition_id": pm_condition_id,
            "polymarket_token_yes": pm_yes,
            "polymarket_token_no": pm_no,
            "kalshi_ticker": kx_ticker,
            "market_window_end_epoch_ms": market_window_end_epoch_ms,
            "selection_source": selection_source,
            "disable_market_not_open_check": bool(disable_market_not_open_check),
        }

        segment_account_snapshot_start: Optional[Dict[str, Any]] = None
        segment_account_snapshot_end: Optional[Dict[str, Any]] = None
        if bool(self.account_snapshot_logging_enabled) or bool(self.position_monitoring_requested):
            self.last_snapshot_polymarket_client = pm_poll_client
            self.last_snapshot_polymarket_account_client = pm_account_poll_client
            self.last_snapshot_kalshi_client = kx_poll_client
            segment_account_snapshot_start = capture_account_portfolio_snapshot(
                polymarket_client=pm_poll_client,
                polymarket_account_client=pm_account_poll_client,
                kalshi_client=kx_poll_client,
                now_epoch_ms=now_ms(),
            )
            self.logger.write_account_snapshot(
                recv_ms=now_ms(),
                scope="market_start",
                snapshot=segment_account_snapshot_start,
                market_context=segment_market_context,
            )
            if self.run_account_snapshot_start is None:
                self.run_account_snapshot_start = dict(segment_account_snapshot_start)
                self.logger.write_account_snapshot(
                    recv_ms=now_ms(),
                    scope="run_start",
                    snapshot=self.run_account_snapshot_start,
                    market_context=segment_market_context,
                )

        segment_started = datetime.now(timezone.utc)
        polymarket_collector: Optional[PolymarketWsCollector] = None
        kalshi_collector: Optional[KalshiWsCollector] = None
        segment_stats: Dict[str, Any] = {}
        try:
            polymarket_collector = PolymarketWsCollector(
                token_yes=pm_yes,
                token_no=pm_no,
                custom_feature_enabled=bool(self.args.custom_feature_enabled),
                raw_writer=pm_raw_writer,
                event_writer=pm_event_writer,
                health_config=self.health_config,
                on_event=runtime.apply_polymarket_event,
            )
            kalshi_collector = KalshiWsCollector(
                market_ticker=kx_ticker,
                channels=self.kalshi_channels,
                headers_factory=resolve_kalshi_ws_headers,
                raw_writer=kx_raw_writer,
                event_writer=kx_event_writer,
                health_config=self.health_config,
                on_event=runtime.apply_kalshi_event,
            )
            segment_stats = asyncio.run(
                run_core_loop(
                    logger=self.logger,
                    kalshi_collector=kalshi_collector,
                    polymarket_collector=polymarket_collector,
                    kalshi_market_positions_collector=segment_kalshi_market_positions_collector,
                    kalshi_user_orders_collector=segment_kalshi_user_orders_collector,
                    polymarket_user_collector=segment_polymarket_user_collector,
                    runtime=runtime,
                    position_runtime=segment_position_runtime,
                    position_reconcile_loop=segment_position_reconcile_loop,
                    position_event_state=segment_position_event_state,
                    duration_seconds=int(segment_duration_seconds),
                    decision_poll_seconds=float(self.args.decision_poll_seconds),
                    decision_config=self.decision_config,
                    market_window_end_epoch_ms=market_window_end_epoch_ms,
                    market_context=segment_market_context,
                    runtime_memory_poll_seconds=float(self.args.runtime_memory_poll_seconds),
                    edge_snapshot_poll_seconds=float(self.args.edge_snapshot_poll_seconds),
                    buy_execution_enabled=bool(segment_buy_execution_enabled),
                    buy_execution_clients=segment_buy_execution_clients,
                    sell_execution_enabled=bool(segment_sell_execution_enabled),
                    sell_execution_clients=segment_sell_execution_clients,
                    buy_idempotency_state=self.buy_idempotency_state,
                    buy_execution_cooldown_ms=self.buy_execution_cooldown_ms,
                    buy_execution_max_attempts=self.buy_execution_max_attempts,
                    buy_execution_parallel_leg_timeout_ms=self.buy_execution_parallel_leg_timeout_ms,
                    sell_execution_parallel_leg_timeout_ms=self.sell_execution_parallel_leg_timeout_ms,
                    buy_execution_attempt_state=self.buy_execution_attempt_state,
                )
            )
        finally:
            for writer in ws_writers:
                try:
                    writer.close()
                except Exception:
                    pass

        segment_ended = datetime.now(timezone.utc)
        if bool(self.account_snapshot_logging_enabled) or bool(self.position_monitoring_requested):
            segment_account_snapshot_end = capture_account_portfolio_snapshot(
                polymarket_client=pm_poll_client,
                polymarket_account_client=pm_account_poll_client,
                kalshi_client=kx_poll_client,
                now_epoch_ms=now_ms(),
            )
            self.logger.write_account_snapshot(
                recv_ms=now_ms(),
                scope="market_end",
                snapshot=segment_account_snapshot_end,
                market_context=segment_market_context,
            )
        self.segment_index += 1

        for key, value in segment_stats.items():
            if isinstance(value, int) and "last" not in key and key != "buy_fsm":
                self.stats[key] = self.stats.get(key, 0) + value

        self.stats["last_decision"] = segment_stats.get("last_decision")
        self.stats["last_buy_execution"] = segment_stats.get("last_buy_execution")
        self.stats["last_sell_execution"] = segment_stats.get("last_sell_execution")
        self.stats["buy_fsm"] = segment_stats.get("buy_fsm")
        self.stats["last_position_health"] = segment_stats.get("last_position_health")
        self.stats["last_position_poll"] = segment_stats.get("last_position_poll")

        self.total_kalshi_raw_messages += int(kalshi_collector.message_count if kalshi_collector else 0)
        self.total_kalshi_normalized_events += int(kalshi_collector.event_count if kalshi_collector else 0)
        self.total_polymarket_raw_messages += int(polymarket_collector.message_count if polymarket_collector else 0)
        self.total_polymarket_normalized_events += int(polymarket_collector.event_count if polymarket_collector else 0)
        self.last_kalshi_health = kalshi_collector.health_snapshot() if kalshi_collector else {}
        self.last_polymarket_health = polymarket_collector.health_snapshot() if polymarket_collector else {}

        self.market_segments.append(
            {
                "segment_index": int(self.segment_index),
                "segment_started_at": segment_started.isoformat(),
                "segment_ended_at": segment_ended.isoformat(),
                "segment_duration_seconds_requested": int(segment_duration_seconds),
                "polymarket_event_slug": pm_slug,
                "polymarket_market_id": pm_market_id,
                "polymarket_condition_id": pm_condition_id,
                "kalshi_ticker": kx_ticker,
                "market_window_end_epoch_ms": int(market_window_end_epoch_ms),
                "account_snapshot_start": segment_account_snapshot_start,
                "account_snapshot_end": segment_account_snapshot_end,
                "counts": {
                    "kalshi_raw_messages": int(kalshi_collector.message_count if kalshi_collector else 0),
                    "kalshi_normalized_events": int(kalshi_collector.event_count if kalshi_collector else 0),
                    "polymarket_raw_messages": int(polymarket_collector.message_count if polymarket_collector else 0),
                    "polymarket_normalized_events": int(
                        polymarket_collector.event_count if polymarket_collector else 0
                    ),
                    **{k: v for k, v in segment_stats.items() if isinstance(v, int)},
                },
            }
        )

    def capture_run_end_snapshot(self) -> None:
        run_end_market_context: Dict[str, Any] = {}
        if self.last_selection:
            run_end_market_context = {
                "polymarket_event_slug": str(_as_dict(self.last_selection.get("polymarket")).get("event_slug") or ""),
                "polymarket_market_id": str(_as_dict(self.last_selection.get("polymarket")).get("market_id") or ""),
                "polymarket_condition_id": str(_as_dict(self.last_selection.get("polymarket")).get("condition_id") or ""),
                "polymarket_token_yes": str(_as_dict(self.last_selection.get("polymarket")).get("token_yes") or ""),
                "polymarket_token_no": str(_as_dict(self.last_selection.get("polymarket")).get("token_no") or ""),
                "kalshi_ticker": str(_as_dict(self.last_selection.get("kalshi")).get("ticker") or ""),
                "market_window_end_epoch_ms": self.last_market_window_end_epoch_ms,
            }
        self.run_account_snapshot_end = capture_account_portfolio_snapshot(
            polymarket_client=self.last_snapshot_polymarket_client,
            polymarket_account_client=self.last_snapshot_polymarket_account_client,
            kalshi_client=self.last_snapshot_kalshi_client,
            now_epoch_ms=now_ms(),
        )
        self.logger.write_account_snapshot(
            recv_ms=now_ms(),
            scope="run_end",
            snapshot=self.run_account_snapshot_end,
            market_context=run_end_market_context,
        )

    def print_summary(self, session_started: datetime, session_ended: datetime, args: Any) -> None:
        print("Arbitrage websocket engine stopped")
        print(f"Started: {session_started.isoformat()}")
        print(f"Ended:   {session_ended.isoformat()}")
        print(f"Market segments: {len(self.market_segments)}")
        if self.market_segments:
            first_segment = self.market_segments[0]
            last_segment = self.market_segments[-1]
            print(
                "First market: "
                f"{first_segment.get('polymarket_event_slug')} ({first_segment.get('polymarket_market_id')})"
            )
            print(f"First Kalshi: {first_segment.get('kalshi_ticker')}")
            if len(self.market_segments) > 1:
                print(
                    "Last market: "
                    f"{last_segment.get('polymarket_event_slug')} ({last_segment.get('polymarket_market_id')})"
                )
                print(f"Last Kalshi: {last_segment.get('kalshi_ticker')}")
        print(
            "Messages: "
            f"kalshi={int(self.total_kalshi_raw_messages)}, polymarket={int(self.total_polymarket_raw_messages)}"
        )
        print(
            "Decision samples: "
            f"{int(self.stats.get('decision_samples', 0))}, "
            f"can_trade_true={int(self.stats.get('can_trade_true_samples', 0))}, "
            f"can_trade_false={int(self.stats.get('can_trade_false_samples', 0))}"
        )
        print(
            "Buy execution: "
            f"enabled={bool(self.buy_execution_enabled_last)}, "
            f"max_attempts={int(self.buy_execution_max_attempts)}, "
            f"attempts_used={int(self.buy_execution_attempt_state.get('attempts_used', 0))}, "
            f"attempts={int(self.stats.get('buy_execution_attempts', 0))}, "
            f"submitted={int(self.stats.get('buy_execution_submitted', 0))}, "
            f"rejected={int(self.stats.get('buy_execution_rejected', 0))}, "
            f"errors={int(self.stats.get('buy_execution_errors', 0))}"
        )
        print(
            "Sell execution: "
            f"enabled={bool(self.sell_execution_enabled_last)}, "
            f"attempts={int(self.stats.get('sell_execution_attempts', 0))}, "
            f"submitted={int(self.stats.get('sell_execution_submitted', 0))}, "
            f"rejected={int(self.stats.get('sell_execution_rejected', 0))}, "
            f"errors={int(self.stats.get('sell_execution_errors', 0))}"
        )
