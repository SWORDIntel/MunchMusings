# Source Verification Sprint

_Generated: 2026-03-26T05:55:58Z._

## Purpose
- Keep source verification rerunnable and keyed by `source_id`.
- Preserve analyst-entered state while refreshing derived source metadata from `recent_accounting.csv`.
- Track only date-bearing source rows here; connector/query infrastructure lives in `plans/connector_readiness.csv`.
- Sync verification lanes without duplicating milestone tasks.

## Snapshot
- verified: 23

## Lanes
- baseline_refresh: 9
- humanitarian_feed: 3
- macro_price: 4
- market_monitor: 5
- proxy_refresh: 2

## Rerun
- `python bootstrap.py --verification-sprint` refreshes the ledger-backed tracker and `VER-*` queue rows.
