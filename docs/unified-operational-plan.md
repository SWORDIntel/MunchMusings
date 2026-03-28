# Unified Operational Plan

_Last updated: 2026-03-28 (UTC)._  
_Purpose: provide one consolidated plan that combines the launcher workflow, preseeded source registry, compliant collection rules, scoring model, district-selection method, automation path, and v0.1 execution roadmap._

## TIS // Tactical Implementation Spec

### SITREP
- **Current State:** The repository now contains the launcher, a seeded source registry, generated collection/verification artifacts, staged-external execution contracts, and supporting planning documents. Source accounting is mostly closed, the collection manifest is stable, and `seed-33` is the only live tier-1 recency blocker.
- **Objective:** Stand up a current, source-driven, legally compliant scraper + analysis framework that can infer migration-linked food signals across Egypt, UAE, Saudi Arabia, and the Levant without violating platform terms or crossing legal/ethical lines.
- **Threat Assessment:** Some collection patterns are explicitly out of bounds: geo-fencing bypasses, MITM interception, anti-bot circumvention, residential proxy abuse, and authenticated scraping against platform terms. The compliant surface is public APIs, licensed data, public business registries, humanitarian dashboards, investor-relations releases, public retailer catalogues, Google Trends, OSM/Overpass, UN/IOM/UNHCR/OCHA/WFP/IPC/Comtrade, and public merchant pages.

### BATTLE PLAN
- **OBJECTIVE:** Build a public-source intelligence pipeline that can detect migration-linked food signals without platform abuse and without over-claiming what food proxies actually mean.
- **CURRENT_STATE:** The repo now has the bootstrap launcher, seeded source stack, dated operating-cycle wrapper, generated recent-accounting tracker, and staged-external handoff contracts. The operational intent still needs to stay anchored to humanitarian baselines, compliant public collection, and explicit validation rules.
- **ACTIONS:**
  1. Build and maintain the source registry first.
  2. Start with official/public baselines before platform collection.
  3. Use restaurant/bakery density, menu lexicon, import shifts, and grocery assortment as the core signal bundle.
  4. Require at least two-source confirmation before making any demographic inference.
  5. Treat snack preference shifts as soft indicators, not standalone proof.
- **VERIFICATION:** Map openings by cuisine, neighborhood, and month; match shifts against UNHCR/IOM/OCHA/WFP baselines; and test whether signals survive Ramadan, tourism, inflation, and price shocks.
- **CONTINGENCY:** If delivery platforms are unavailable, pivot to Google Places, OSM/Overpass, web search indexing, retailer assortment snapshots, investor-relations releases, public merchant pages, and import/trade statistics.

### QUALITY METRICS & SUCCESS VALIDATION
- **Metric 1:** Percentage of detected food-establishment anomalies confirmed by a second source.
- **Metric 2:** Lag between humanitarian event dates and proxy-signal emergence.
- **Success Validation:** Recover at least three backtest windows: Sudanese arrivals into Egypt after **15 April 2023**, Syria/Lebanon movement dynamics centered on **8 December 2024**, and Gaza market-collapse conditions after **2 March 2025** and **18 March 2025**.

### SECURITY & COMPLIANCE GATES
- **Lint/Static:** Not applicable for planning docs; `bootstrap.py` should stay `py_compile` clean and the stdlib test suite should stay green.
- **CVE Scan:** The runtime is intentionally light but not stdlib-only; `bootstrap.py` uses `requests` and the dashboard uses `rich`.
- **Unsafe patterns avoided:** No MITM guidance, no anti-bot evasion, no proxy abuse, no credential scraping, no person-level inference.

## 1) Single Entry Point and Automation Modes

The repository entry point is:

```bash
python bootstrap.py
```

### Launcher modes
- **TUI mode:** default interactive mode when run in a terminal without extra flags.
- **CLI mode:** direct automation path using flags such as `--verbose`, `--force-version`, `--input`, and `--output-dir`.
- **GUI mode:** `python bootstrap.py --gui` launches a desktop window with field inputs, a progress bar, and JSON output display.
- **Check mode:** `python bootstrap.py --check` validates the seed and prints JSON without writing artifacts.
- **Inspect mode:** `python bootstrap.py --inspect` prints a human-readable source summary for operators.

### Progress reporting
The launcher now supports progress tracking across the bootstrap lifecycle:
1. prepare directories,
2. load seed,
3. validate seed,
4. select version,
5. write JSON,
6. write CSV,
7. complete.

This makes the workflow suitable for:
- direct CLI automation,
- TUI-based operator execution,
- desktop GUI launch with visible progress,
- no-write validation checks,
- source-pack inspection before any write operation.

## 2) Seeded Source Stack

The canonical source of truth is `seed/preseed_sources_v1.json`, which is exported to:
- `artifacts/bootstrap/preseed_sources_vN.json`
- `artifacts/bootstrap/preseed_sources_vN.csv`
- `docs/source-registry.csv`

### Tier 1 priority sources
Use these first for hard baselines and stronger corroboration:
1. UNHCR Egypt data portal
2. IOM DTM Sudan
3. UNHCR Lebanon reporting hub
4. ACAPS Lebanon
5. OCHA OPT Gaza updates
6. IPC Gaza snapshot
7. IPC Lebanon analysis
8. WFP Retail and Markets factsheet
9. UN Comtrade API portal
10. USDA FAS Saudi Retail Foods Annual
11. Google Places API
12. OpenStreetMap Overpass

### Tier 2 supporting sources
Use these as supporting context or weaker corroboration:
13. Google Maps billing guidance
14. Agthia Abu Auf IR releases
15. WFP Lebanon food systems publication
16. UNSD API catalogue
17. Overpass Turbo
18. Deliveroo UAE trend releases
19. Spinneys UAE trends
20. Google Trends

## 3) High-Confidence Regional Readouts

### Egypt
- Strongest near-term pilot.
- Anchor on Sudan-linked movement after **15 April 2023**.
- Best proxies: Sudanese bakeries, East African/Sudanese tea/coffee shops, sorghum/millet/ful patterns, Sudanese menu lexicon.

### Lebanon / Syria corridor
- Monitor both formation and disappearance of low-cost bakery/sweets clusters.
- Treat the period around **8 December 2024** and follow-on 2025 movement reporting as the core volatility window.

### Gaza / OPT
- Use primarily as a market-collapse baseline, not a snack-trend market.
- Focus on functionality collapse, assortment compression, retail inactivity, and price spikes.

### Saudi Arabia
- Best country for scalable SKU and retail-channel monitoring.
- Focus on savory snacks, quick-commerce assortment, and migrant-neighborhood grocery/bakery clustering.

### UAE
- Better interpreted as a premiumization + diaspora-mainstreaming market.
- Separate luxury/viral dessert signals from true diaspora-staple signals.

## 4) Core Signal Bundle

Use four signal families:
- **Density:** new ethnic bakeries, groceries, and dessert shops by district.
- **Lexical drift:** cuisine/dialect terms appearing in new districts.
- **Assortment drift:** retailer shelf or online assortment changes by city/neighborhood.
- **Price-positioning drift:** cuisine moving from low-cost staple format to premiumized format.

### Core analytic controls
Every serious inference must control for:
- Ramadan and Eid,
- tourism and pilgrimage peaks,
- inflation and FX shocks,
- chain-led rollout campaigns,
- platform taxonomy changes,
- supply disruptions.

## 5) Validation Rules

Before any analytic claim:
1. require one hard baseline source from UNHCR, IOM, OCHA, WFP, IPC, or official statistics,
2. require one independent market proxy source,
3. downgrade or block publication if confounds explain the anomaly better.

### Claim labels
Use:
- **Observed**
- **Correlated**
- **Inferred**
- **Unconfirmed**

Three-source confirmation remains the standard for higher-confidence reporting.

## 6) District Selection Method

Score districts on:
- movement relevance,
- source coverage,
- food-retail observability,
- mapping/geocoding quality,
- control comparability.

### Inclusion thresholds
- **Monitoring districts:** score >= 70.
- **Control districts:** score >= 60 with strong comparability and lower movement relevance.

### v0.1 geography focus
- Egypt: Cairo/Giza districts first.
- Lebanon: Akkar, Tripoli, Bekaa, Beirut periphery.
- Saudi Arabia: Riyadh, Jeddah, Dammam.
- UAE: Deira/Bur Dubai, Sharjah, Abu Dhabi middle-market zones.
- Gaza/OPT: baseline areas where observability remains feasible.

## 7) Data and NLP Design

### Data model direction
Use Postgres + PostGIS for:
- `place_snapshot`
- `menu_item_snapshot`
- `sku_snapshot`
- `event_baseline`
- `anomaly`

### NLP stack
Start simple.
- Arabic normalization
- transliteration normalization
- hand-built lexicons for Sudanese, Levantine, Gulf, and Egyptian food terms
- embeddings for clustering
- human review queue

Only consider heavier local-model work after a labeled review set exists.

## 8) v0.1 Execution Sequence

### Phase 0: planning hardening
- confirm source registry,
- confirm scoring workflow,
- freeze districts and controls,
- align the launcher outputs with downstream needs.

### Phase 1: Egypt baseline ingestion
- load UNHCR Egypt and IOM DTM Sudan context,
- freeze the Egypt district watchlist.

### Phase 2: Egypt market-proxy collection
- stage or execute Google Places and OSM collection through the current connector contracts,
- capture public merchant/menu signals,
- produce first anomaly candidates.

### Phase 3: Egypt backtest and analyst queue
- score anomalies,
- review confounds,
- decide whether to expand.

### Phase 4: Lebanon / Syria corridor expansion
- test both signal appearance and disappearance.

### Phase 5: Saudi and UAE support tracks
- use Saudi for assortment/retail monitoring,
- use UAE for premiumization and mainstreaming context.

## 9) Analyst Workflow

1. Run `python bootstrap.py` or `python bootstrap.py --gui`.
2. Confirm the output artifacts were created.
3. Review the seeded source pack and lock the pilot scope.
4. Select monitoring and control districts.
5. Collect only compliant public signals.
6. If collection leaves rows in `staged_external`, start with the matching `EXT-*` row in `plans/work_queue.csv`, open the staged normalized contract it references, then use `plans/connector_readiness.csv`, the staged raw spec under `artifacts/collection/raw/`, and the linked `plans/source_specs/*.json` contract as supporting execution detail.
7. Rerun `python bootstrap.py --verification-sprint` after staged external execution; it now refreshes recent accounting, finalizes completed external captures, and rebuilds the queue.
8. Score anomalies and record confounds.
9. Publish only after the minimum validation rule is satisfied.

## 10) Companion Documents

Use these when more detail is needed:
- `docs/public-source-implementation-plan.md`
- `docs/confidence-rubric.md`
- `docs/district-selection-matrix.md`
- `docs/v0.1-execution-plan.md`
- `docs/source-registry.csv`

## 11) Recommended Immediate Next Actions

1. Use this file as the primary runbook.
2. Run the launcher in CLI, TUI, GUI, check, or inspect mode depending on task.
3. Treat `plans/work_queue.csv` and `plans/collection_runbook.md` as the primary operator surface for staged external work, with `plans/connector_readiness.csv` as the synced status sheet behind it.
4. Use the wrapper resume flow (`python scripts/run_operating_cycle.py --resume-latest` or `python bootstrap.py --operating-cycle --resume-latest`) instead of manually restarting interrupted cycles.
5. Keep the seeded registry current and treat trend/lifestyle sources as supporting evidence only.
6. Run the stdlib test suite before future launcher changes.
7. Keep `seed-33` honest as an access/freshness blocker until the official source surface improves.
