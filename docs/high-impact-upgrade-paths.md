# High-Impact Upgrade Paths

_Prepared: 2026-03-25._

This report stays inside lawful, public-source collection. It does not attempt to replicate HUMINT or SIGINT. The target standard here is the strongest defensible OSINT workflow the current repo can support.

## Six High-Level Paths

### 1) Source Accounting Control Plane
- Objective: make every source claim auditable, fresh, and reviewable before it influences analysis.
- Core components: recent-accounting ledger, freshness policy, owner assignment, evidence links, workflow states, and blocker handling.
- Why it changes the system materially: it converts the repo from planning-first to evidence-first and exposes stale baselines immediately.
- Biggest risks: analyst overhead, partial updates, and false confidence if publication dates are inferred instead of verified.
- First concrete deliverable: drive every tier-1 source out of `unknown` in `plans/recent_accounting.csv`.

### 2) Scheduled Collection Orchestration
- Objective: turn the weekly review loop into a controlled task system instead of a manual checklist.
- Core components: work queue with dependencies, source-specific cadences, due-date generation, collection checklists, and failure escalation rules.
- Why it changes the system materially: it creates repeatable throughput and stops important sources from silently aging out.
- Biggest risks: queue sprawl, noisy alerts, and brittle dependence on sources with irregular publication habits.
- First concrete deliverable: add generated due tasks for every tier-1 source and tie them to `work_queue.csv`.

### 3) Normalized Evidence Store
- Objective: move from scattered CSV starters to a unified evidence model that can support time-series comparison.
- Core components: `source_registry`, `event_baseline`, `place_snapshot`, `menu_item_snapshot`, `sku_snapshot`, `anomaly`, and evidence packet storage.
- Why it changes the system materially: it enables backtests, cross-source joins, and district-level deltas instead of one-off operator reviews.
- Biggest risks: overengineering before collection stabilizes, schema churn, and unlabeled low-quality inputs.
- First concrete deliverable: implement the event-baseline and anomaly tables first, then ingest the Egypt pilot rows.

### 4) District Change-Detection Engine
- Objective: detect district-level movement in establishment density, lexical drift, and assortment change without over-claiming causality.
- Core components: frozen district/control pairs, monthly snapshots, baseline overlays, confound flags, and anomaly thresholds.
- Why it changes the system materially: it gives the repo an actual signal engine instead of just a source registry and templates.
- Biggest risks: false positives from seasonality, chain rollout effects, and geocoding inconsistency.
- First concrete deliverable: score Cairo/Giza districts and generate the first Egypt anomaly candidate list.

### 5) Analyst Evidence Packet Pipeline
- Objective: standardize what a reviewable case looks like and reduce analyst variance.
- Core components: packet template, source excerpts, provenance fields, confound notes, score breakdown, and review gates.
- Why it changes the system materially: it improves quality control and makes outputs portable across reviewers.
- Biggest risks: too much manual burden, inconsistent narrative standards, and weak packet discipline.
- First concrete deliverable: require an evidence packet for every row entering `artifacts/v0_1/anomaly-review-worksheet.csv`.

### 6) Decision-Grade Reporting Layer
- Objective: turn raw accounting and anomaly rows into weekly briefs that highlight readiness, gaps, and emerging signals.
- Core components: source-readiness summary, overdue source list, anomaly deck, district watchlist deltas, and explicit publication labels.
- Why it changes the system materially: it gives operators and stakeholders a bounded, repeatable output instead of ad hoc file inspection.
- Biggest risks: presenting weak cases too early, collapsing caveats, and confusing freshness with truth.
- First concrete deliverable: generate a weekly Egypt readiness brief from the recent-accounting ledger and work queue.

## Chosen Path For Immediate Action

### Path: Source Accounting Control Plane

This is the highest-leverage path because every later capability depends on knowing which baselines are current, stale, or still unverified.

## Current Readiness Report

### Current repo state
- Total tracked sources: 20
- Current sources: 1
- Overdue sources: 2
- Unknown sources: 17
- Completed queue tasks: 2
- In-progress queue tasks: 1

### Verified source status
- `seed-01` UNHCR Egypt: verified on 2026-03-25, latest publication 2026-03-05, latest period covered 2026-02-28, status `overdue`
- `seed-02` IOM DTM Sudan: verified on 2026-03-25, latest publication 2024-09-17, latest period covered 2024-09-10, status `overdue`
- `seed-05` OCHA OPT Gaza updates: verified on 2026-03-25, latest publication 2026-03-19, latest period covered 2026-03-19, status `current`

### Queue state
- `ACC-001`: completed
- `ACC-002`: completed
- `ACC-003`: in progress
- `ACC-004`: pending
- `ACC-005`: pending

### Why this matters
- The repo can now distinguish between a verified but stale source and a source that has never been checked.
- Egypt baseline work is no longer blocked by uncertainty about whether the source rows were ever touched.
- The next bottleneck is not more planning. It is clearing the tier-1 `unknown` rows and freezing district/control choices.

## Recommended Next Move

Use Path 1 as the control layer, then sequence Path 2 and Path 4 behind it:
1. Clear the remaining tier-1 `unknown` rows in `plans/recent_accounting.csv`.
2. Auto-generate due tasks from source cadence into `plans/work_queue.csv`.
3. Freeze Egypt district/control pairs and start anomaly generation only after the baseline rows are defensible.
