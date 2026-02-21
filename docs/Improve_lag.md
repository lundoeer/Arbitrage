# Lag Metric Improvement Suggestions

## Goal
Make lag metrics reflect true stream state and measurement quality, independent of how strict `lag_exceeded` thresholds are.

## Suggestions

1. Use the highest-precision source timestamp, not first non-null.
- Current normalization uses `ts -> timestamp -> time` precedence.
- `ts` is often second-level precision and can inflate measured lag.
- Parse all available source time fields and choose the most precise valid value.
- Emit metadata like `source_ts_field_used` and `source_ts_precision`.

2. Report both raw lag and clock-skew-corrected lag.
- Keep `raw_lag_ms = recv_ms - source_timestamp_ms`.
- Add `corrected_lag_ms = raw_lag_ms - estimated_clock_offset_ms`.
- Refresh per-venue clock offset periodically (simple baseline: HTTP `Date` headers).

3. Move from single-sample lag to rolling lag distributions.
- Do not rely only on `last_lag_ms`.
- Track rolling stats: `count`, `min`, `p50`, `p90`, `p99`, `max`, and optionally EWMA.
- Expose these in health snapshots so lag quality is stable and interpretable.

4. Split lag into separate stages.
- Ingress lag: source timestamp to socket receive.
- Processing lag: socket receive to normalized event emission/runtime apply.
- Decision lag: current decision time to quote source timestamp.
- This avoids mixing transport and internal processing effects.

5. Track lag by event type/channel.
- Maintain separate lag stats for `ticker`, `trade`, `orderbook_snapshot`, `orderbook_delta`, etc.
- Different channels have different timestamp characteristics.

6. Add timestamp quality flags.
- Mark and count events with:
- missing source timestamp
- future source timestamp
- low-precision source timestamp
- Keep these as explicit quality metrics rather than silently polluting lag stats.

7. Add sequence integrity metrics alongside lag.
- For channels with sequence numbers, track gaps, duplicates, and out-of-order events.
- Low lag without sequence integrity can still produce stale or wrong local book state.

8. Add deterministic tests for lag logic.
- Unit test timestamp parsing and selection policy.
- Unit test lag aggregation/rolling stats.
- Replay captured JSONL streams to ensure lag metrics remain stable across refactors.

## Suggested Implementation Order

1. Timestamp selection + metadata (`source_ts_field_used`, precision).
2. Rolling lag stats in health snapshots.
3. Clock offset estimation and `corrected_lag_ms`.
4. Stage-separated lag and per-event-type stats.
5. Sequence integrity metrics and replay tests.

## Relevant Current Code
- `scripts/common/ws_normalization.py`
- `scripts/common/ws_transport.py`
- `scripts/common/normalized_books.py`
