# Source Verification Sprint

_Generated: 2026-03-28T20:45:28Z._

## Purpose
- Keep source verification rerunnable and keyed by `source_id`.
- Preserve analyst-entered state while refreshing derived source metadata from `recent_accounting.csv`.
- Track only date-bearing source rows here; connector, query, and staged external execution infrastructure lives in `plans/connector_readiness.csv` and is actioned through `EXT-*` queue rows.
- Sync verification lanes without duplicating milestone tasks.

## Snapshot
- verified: 2

## Lanes
- humanitarian_feed: 1
- market_monitor: 1

## Rerun
- `python bootstrap.py --verification-sprint` refreshes the ledger-backed tracker plus derived `VER-*` and `EXT-*` queue rows.
- It also refreshes the staged request-contract artifacts for manifest rows still marked `staged_external`.
