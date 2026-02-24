from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

from scripts.common.ws_transport import JsonlWriter, utc_now_iso


class EngineLogger:
    def __init__(self, *, run_id: str, project_root: Path) -> None:
        self.run_id = run_id
        self.project_root = project_root
        self.writers: List[JsonlWriter] = []
        self.files: Dict[str, str] = {}
        
        self.decision_writer: Optional[JsonlWriter] = None
        self.buy_decision_writer: Optional[JsonlWriter] = None
        self.buy_execution_writer: Optional[JsonlWriter] = None
        self.positions_writer: Optional[JsonlWriter] = None
        self.position_poll_raw_polymarket_writer: Optional[JsonlWriter] = None
        self.position_poll_raw_kalshi_writer: Optional[JsonlWriter] = None
        self.account_snapshot_writer: Optional[JsonlWriter] = None
        self.edge_snapshot_writer: Optional[JsonlWriter] = None
        self.runtime_memory_writer: Optional[JsonlWriter] = None

    def setup_writers(
        self,
        *,
        log_decisions: bool,
        log_buy_decisions: bool,
        log_buy_execution: bool,
        log_positions: bool,
        log_raw_events: bool,
        log_account_snapshots: bool,
        log_edge_snapshots: bool,
        log_runtime_memory: bool,
    ) -> None:
        data_dir = self.project_root / "data"

        if log_decisions:
            path = data_dir / f"decision_log__{self.run_id}.jsonl"
            self.decision_writer = JsonlWriter(path)
            self.writers.append(self.decision_writer)
            self.files["decision_log"] = str(path)

        if log_buy_decisions:
            path = data_dir / f"buy_decision_log__{self.run_id}.jsonl"
            self.buy_decision_writer = JsonlWriter(path)
            self.writers.append(self.buy_decision_writer)
            self.files["buy_decision_log"] = str(path)

        if log_buy_execution:
            path = data_dir / f"buy_execution_log__{self.run_id}.jsonl"
            self.buy_execution_writer = JsonlWriter(path)
            self.writers.append(self.buy_execution_writer)
            self.files["buy_execution_log"] = str(path)

        if log_positions:
            path = data_dir / f"position_monitoring_log__{self.run_id}.jsonl"
            self.positions_writer = JsonlWriter(path)
            self.writers.append(self.positions_writer)
            self.files["position_monitoring_log"] = str(path)

        if log_raw_events:
            pm_path = data_dir / f"position_poll_raw_http_polymarket__{self.run_id}.jsonl"
            self.position_poll_raw_polymarket_writer = JsonlWriter(pm_path)
            self.writers.append(self.position_poll_raw_polymarket_writer)
            self.files["position_poll_raw_http_polymarket"] = str(pm_path)

            kx_path = data_dir / f"position_poll_raw_http_kalshi__{self.run_id}.jsonl"
            self.position_poll_raw_kalshi_writer = JsonlWriter(kx_path)
            self.writers.append(self.position_poll_raw_kalshi_writer)
            self.files["position_poll_raw_http_kalshi"] = str(kx_path)

        if log_account_snapshots:
            path = data_dir / f"account_portfolio_snapshot_log__{self.run_id}.jsonl"
            self.account_snapshot_writer = JsonlWriter(path)
            self.writers.append(self.account_snapshot_writer)
            self.files["account_portfolio_snapshot_log"] = str(path)

        if log_edge_snapshots:
            path = data_dir / f"gross_edge_snapshot__{self.run_id}.jsonl"
            self.edge_snapshot_writer = JsonlWriter(path)
            self.writers.append(self.edge_snapshot_writer)
            self.files["gross_edge_snapshot"] = str(path)

        if log_runtime_memory:
            path = data_dir / f"websocket_share_price_runtime__{self.run_id}.jsonl"
            self.runtime_memory_writer = JsonlWriter(path)
            self.writers.append(self.runtime_memory_writer)
            self.files["runtime_memory"] = str(path)

    def write_decision(self, *, recv_ms: int, payload: Dict[str, Any]) -> None:
        if self.decision_writer:
            self.decision_writer.write({
                "ts": utc_now_iso(),
                "recv_ms": recv_ms,
                "kind": "decision",
                **payload,
            })

    def write_buy_decision(self, *, recv_ms: int, payload: Dict[str, Any]) -> None:
        if self.buy_decision_writer:
            self.buy_decision_writer.write({
                "ts": utc_now_iso(),
                "recv_ms": recv_ms,
                "kind": "buy_decision",
                **payload,
            })

    def write_buy_execution(self, *, recv_ms: int, payload: Dict[str, Any]) -> None:
        if self.buy_execution_writer:
            self.buy_execution_writer.write({
                "ts": utc_now_iso(),
                "recv_ms": recv_ms,
                "kind": "buy_execution",
                **payload,
            })

    def write_position_event(self, *, recv_ms: int, sub_kind: str, payload: Dict[str, Any]) -> None:
        if self.positions_writer:
            self.positions_writer.write({
                "ts": utc_now_iso(),
                "recv_ms": recv_ms,
                "kind": f"position_{sub_kind}",
                **payload,
            })

    def write_position_poll_raw(self, *, recv_ms: int, venue: str, payload: Dict[str, Any]) -> None:
        venue_norm = str(venue or "").strip().lower()
        if venue_norm == "polymarket":
            writer = self.position_poll_raw_polymarket_writer
        elif venue_norm == "kalshi":
            writer = self.position_poll_raw_kalshi_writer
        else:
            return
        if writer:
            writer.write({
                "ts": utc_now_iso(),
                "recv_ms": recv_ms,
                "kind": f"position_poll_raw_http_{venue_norm}",
                "venue": venue_norm,
                **payload,
            })

    def write_account_snapshot(self, *, recv_ms: int, scope: str, snapshot: Dict[str, Any], market_context: Dict[str, Any]) -> None:
        if self.account_snapshot_writer:
            self.account_snapshot_writer.write({
                "ts": utc_now_iso(),
                "recv_ms": recv_ms,
                "kind": "account_portfolio_snapshot",
                "scope": scope,
                "market_context": market_context,
                "snapshot": snapshot,
            })

    def write_edge_snapshot(self, *, recv_ms: int, payload: Dict[str, Any]) -> None:
        if self.edge_snapshot_writer:
            self.edge_snapshot_writer.write({
                "ts": utc_now_iso(),
                "recv_ms": recv_ms,
                "kind": "gross_edge_snapshot",
                **payload,
            })

    def write_runtime_memory(self, *, recv_ms: int, memory: Dict[str, Any]) -> None:
        if self.runtime_memory_writer:
            self.runtime_memory_writer.write({
                "ts": utc_now_iso(),
                "recv_ms": recv_ms,
                "kind": "share_price_runtime_memory",
                "memory": memory,
            })

    def close(self) -> None:
        for writer in self.writers:
            try:
                writer.close()
            except Exception:
                pass
