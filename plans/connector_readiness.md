# Connector Readiness

_Generated: 2026-03-28T20:23:39Z._

## Purpose
- Track connector, query, and staged external execution infrastructure separately from publication-date verification.
- Keep API/query readiness visible without inflating `unknown` source recency counts.
- Surface actionable external work through `EXT-*` rows in `plans/work_queue.csv`.

## Snapshot
- needs_credentials: 1
- ready: 6

## Credential State
- api_key_required: 1
- public_endpoint: 6

## Queue
- Run `python bootstrap.py --verification-sprint` after connector changes to refresh the derived `EXT-*` work queue rows.
