# Recent Accounting Summary

_Generated: 2026-03-28T20:31:35Z._

## Why this exists
- This ledger is the execution checkpoint for keeping source claims recent, attributable, and reviewable.
- `recency_status` is derived from `refresh_cadence` plus `last_published_date`; blank publication dates remain `unknown`.
- Re-running `python bootstrap.py --recent-accounting` preserves analyst-entered fields and refreshes derived status columns.
- Connector, query, and staged external execution surfaces are tracked in `plans/connector_readiness.csv` and surfaced as `EXT-*` rows in `plans/work_queue.csv`, separate from this date-based ledger.

## Snapshot
- Total tracked sources: 30
- Current: 21
- Due now: 0
- Overdue: 0
- Blocked: 0
- Manual review cadence: 3
- Unknown recency: 6
- Unassigned owners: 13
- Pending review rows: 6

## Immediate actions
1. Resolve the remaining tier-1 non-current rows first: seed-33.
2. Treat `unknown` and `overdue` rows as the priority backlog before due-now or manual-review cleanup.
3. Backfill `owner` for the remaining unassigned rows so queue ownership matches the ledger.
