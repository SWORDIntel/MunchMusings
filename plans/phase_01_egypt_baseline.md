# Phase 01: Egypt Baseline Accounting

## Objective
Establish a defendable recent-accounting baseline for the Egypt pilot before any market-proxy claim is treated as live.

## Priority Sources
1. UNHCR Egypt data portal
2. IOM DTM Sudan
3. Google Places API
4. OpenStreetMap Overpass

## Inputs
- `plans/recent_accounting.csv`
- `artifacts/v0_1/event-timeline.csv`
- `artifacts/v0_1/district-watchlist.csv`

## Required Outputs
- Egypt baseline source rows updated with owner, access timestamp, latest publication date, and evidence link.
- Event-timeline rows populated with `source_accessed_utc`.
- Cairo/Giza pilot districts confirmed or explicitly deferred.

## Acceptance Criteria
- Tier-1 Egypt baseline sources are no longer `unknown` or `overdue`.
- Each baseline claim has at least one evidence URL.
- Every deferred claim has a written blocker.

## First Tasks
1. Keep `seed-01` and `seed-02` current in `plans/recent_accounting.csv`.
2. Record the baseline event window anchored to 2023-04-15.
3. Treat the remaining Egypt gating work as place-query execution readiness, not baseline freshness cleanup.
