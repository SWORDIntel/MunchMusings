# Recent Accounting Summary

_Generated: 2026-03-26T06:12:59Z._

## Why this exists
- This ledger is the execution checkpoint for keeping source claims recent, attributable, and reviewable.
- `recency_status` is derived from `refresh_cadence` plus `last_published_date`; blank publication dates remain `unknown`.
- Re-running `python bootstrap.py --recent-accounting` preserves analyst-entered fields and refreshes derived status columns.
- Connector/query sources are tracked separately in `plans/connector_readiness.csv` and excluded from this date-based ledger.

## Snapshot
- Total tracked sources: 30
- Current: 12
- Due now: 2
- Overdue: 5
- Blocked: 0
- Manual review cadence: 5
- Unknown recency: 6
- Unassigned owners: 14
- Pending review rows: 6

## Immediate actions
1. Fill `owner` and `last_checked_utc` for every tier-1 source.
2. Capture `last_published_date`, `latest_period_covered`, and `evidence_link` for the Egypt baseline sources first.
3. Treat `unknown` and `overdue` rows as the current work queue for accurate recent accounting.
