# High-Rigor OSINT Evidence-Pack Report

_Prepared: 2026-03-25 (UTC)._

## Boundary

This repository should not attempt HUMINT- or SIGINT-style collection. The lawful upgrade path is a high-rigor public-source evidence-pack system that maximizes provenance, recency control, geospatial coherence, and analyst review.

## Selected Upgrade Path

**Path:** Provenance-first evidence graph plus analyst evidence packets.

## Objective

Turn the repo from a planning-first collection scaffold into a defensible evidence system that can support weekly zone-level market assessments using only public, lawful, reviewable sources.

## Why This Path Matters

- It upgrades the repo from static CSV scaffolds to a queryable evidence chain.
- It makes every claim traceable to a source, publication date, access timestamp, geography, and review decision.
- It is the closest lawful substitute for higher-end intelligence workflows because it emphasizes provenance, time, location, corroboration, and controlled analyst judgment.

## Target Output

Each published weekly assessment should include:
- one zone summary,
- one baseline event table,
- one district change table,
- one anomaly queue with confidence labels,
- one evidence appendix linking each claim to public-source records.

## Required Data Objects

### `source_registry`
- source metadata, cadence, owner, terms notes, refresh class

### `recent_accounting`
- latest publication date, last checked timestamp, evidence link, recency status, next action

### `event_baseline`
- humanitarian or market-collapse events with time window and geography

### `place_snapshot`
- dated place observations from public mapping/place sources

### `menu_item_snapshot`
- dated menu lexicon observations with normalized terms

### `anomaly`
- district-level candidate signals with temporal and spatial linkage to baseline events

### `evidence_packet`
- normalized review packet joining sources, extracts, scores, confounds, and final label

## System Components

1. **Raw landing layer**
Store immutable source pulls or captures with run metadata and checksums.

2. **Normalization layer**
Convert each source family into stable typed records keyed by time and geography.

3. **Evidence graph**
Link source -> observation -> baseline event -> anomaly -> review decision.

4. **Freshness control**
Block or downgrade claims when source recency fails the policy.

5. **Review gate**
Require analyst scoring, confound notes, and explicit publication labels before release.

## Minimum Viable Build

### Phase A
- Move `recent_accounting.csv` into a durable operational ledger.
- Add stable IDs for source, event, district, and anomaly rows.
- Require evidence links for every tier-1 source row.

### Phase B
- Create typed `event_baseline` and `evidence_packet` tables.
- Join the Egypt 2023-04-15 baseline to Cairo/Giza districts.
- Add first reviewable anomaly packets.

### Phase C
- Add repeatable weekly zone report generation.
- Add backtest metrics and reviewer variance tracking.

## First Concrete Deliverable

**Egypt high-rigor baseline pack**

Contents:
- verified source ledger rows for UNHCR Egypt and IOM DTM Sudan,
- Egypt baseline event timeline with access timestamps,
- frozen Cairo/Giza district pack,
- one blank evidence packet template ready for the first anomaly.

## Acceptance Standard

The first pack is acceptable when:
- all baseline claims point to a public evidence link,
- source freshness is visible and enforceable,
- each district-level claim can be traced to a baseline event and a market proxy,
- a second reviewer can reconstruct the logic without verbal handoff.

## Current Repo Fit

The repo already contains the seed elements needed for this path:
- source stack and launcher,
- recent-accounting ledger,
- event timeline,
- district watchlist,
- confidence rubric,
- agent roles and work queue.

The missing step is not more planning. The missing step is joining these artifacts into one durable evidence chain.
