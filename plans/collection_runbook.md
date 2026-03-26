# Collection Runbook

## Purpose
Define the minimum operating sequence for a lawful public-source collection cycle.

## Inputs
- `plans/recent_accounting.csv`
- `plans/work_queue.csv`
- `artifacts/collection/source-adapter-registry.csv`
- `artifacts/collection/collection-run-manifest.csv`
- `artifacts/collection/district-collection-plan.csv`
- `artifacts/collection/places-query-seeds.csv`
- `artifacts/collection/overpass-query-seeds.csv`

## Run Sequence
1. Check source freshness and ownership.
2. Confirm the current district scope and control pairings.
3. Pull only the `ready` rows from the collection manifest.
4. Capture raw outputs into `artifacts/collection/raw/<source_id>/`.
5. Normalize outputs into `artifacts/collection/normalized/`.
6. Write one evidence row per captured source action.
7. Flag blocked, stale, or ambiguous rows immediately.
8. Feed verified observations into the briefing pack.

## Collection Rules
- Do not collect without a source row and a manifest row.
- Do not normalize without preserving the raw capture path.
- Do not score any anomaly until the source is in the ledger.
- Do not overwrite raw captures.

## Stop Conditions
- A source URL changes materially.
- A source returns ambiguous publication dates.
- A district lacks a defensible control.
- Collection volume starts to outpace review capacity.

## Hand-Off
- Raw captures go to normalization.
- Normalized rows go to the evidence log.
- Evidence rows go to briefing assembly.
