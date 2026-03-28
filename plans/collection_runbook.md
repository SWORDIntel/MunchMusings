# Collection Runbook

## Purpose
Define the minimum operating sequence for a lawful public-source collection cycle.

## Inputs
- `plans/recent_accounting.csv`
- `plans/work_queue.csv`
- `plans/connector_readiness.csv`
- `plans/source_specs/*.json`
- `artifacts/collection/source-adapter-registry.csv`
- `artifacts/collection/collection-run-manifest.csv`
- `artifacts/collection/district-collection-plan.csv`
- `artifacts/collection/places-query-seeds.csv`
- `artifacts/collection/overpass-query-seeds.csv`

## Run Sequence
1. Check source freshness, ownership, and active queue items.
2. Confirm the current district scope and control pairings.
3. Run `python bootstrap.py --collect-ready --max-runs <n>` against only the current `ready` rows.
4. Separate results into `completed` and `staged_external` paths before doing anything else.
5. For `completed` rows, confirm raw outputs in `artifacts/collection/raw/<source_id>/` and normalized outputs in `artifacts/collection/normalized/`.
6. For `staged_external` rows, use the matching `EXT-*` task in `plans/work_queue.csv` as the operator action surface and the staged normalized/raw contracts as the execution surface.
7. Re-run `python bootstrap.py --verification-sprint` after external collection state changes; it refreshes recent accounting, finalizes completed external captures, and rebuilds the queue.
8. Feed verified observations into the briefing pack only after freshness, provenance, and any completed external captures are synced.

## Staged External Procedure
1. Start with the matching `EXT-*` task in `plans/work_queue.csv`.
2. Read the `next_action` on that `EXT-*` row first; it should tell you whether the task is credential-blocked, query-ready, manual-capture only, or browser-export only.
3. Open the staged normalized artifact referenced by that `EXT-*` row in `artifacts/collection/normalized/<source_id>.json`.
4. Use `plans/connector_readiness.csv` to confirm the synced connector state and any supporting notes.
5. Open the staged raw artifact in `artifacts/collection/raw/<source_id>/` when you need the exact request payload or capture surface.
6. Treat the staged raw artifact as the authoritative execution contract for that run.
7. Confirm the matching source spec in `plans/source_specs/` for path templates, extraction targets, quality checks, and operator steps.

## Contract Fields To Read
- Query-driven rows: `district_scope`, `query_seed_file`, `query_seed_path`, `queries`, `execution_contract`, `connector_status`, `credential_state`
- Manual/browser rows: `request_method`, `execution_contract.request_params`, `execution_contract.operator_steps`, `quality_checks`
- Provenance fields: `source_spec_path`, `raw_path`, `secondary_raw_path`, `evidence_link`, `mirror_evidence_link`

## Current Staged External Families
- `seed-11`: Google Places API. Needs credentials; use the staged JSON request bodies and field-mask contract.
- `seed-12`: OpenStreetMap Overpass. Public endpoint; use the rendered `overpass_ql` queries from the staged contract.
- `seed-17`: Overpass Turbo. Analyst-driven Overpass workflow; use the same bounded query contract as Overpass plus the Turbo-specific source spec.
- `seed-13`: Google Maps billing guidance. Manual capture; record the current controls page and retain the raw artifact.
- `seed-18` and `seed-19`: Manual capture; record the visible publication date and claims from the pinned trend pages.
- `seed-20`: Browser export; retain the exact Trends query settings with the exported artifact.

## Collection Rules
- Do not collect without a source row and a manifest row.
- Do not normalize without preserving the raw capture path.
- Do not treat a staged request spec as a completed capture.
- Do not score any anomaly until the source is in the ledger.
- Do not overwrite raw captures.

## Stop Conditions
- A source URL changes materially.
- A source returns ambiguous publication dates.
- A district lacks a defensible control.
- Collection volume starts to outpace review capacity.
- A staged external row lacks enough contract detail to execute safely.

## Hand-Off
- Raw captures go to normalization.
- Normalized rows go to the evidence log.
- Evidence rows go to briefing assembly.
- Staged external rows go to operator execution first, then back through recent accounting and verification sprint refresh.
