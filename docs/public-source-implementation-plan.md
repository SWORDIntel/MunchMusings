# Public-Source Implementation Plan: FMCG / Snack Proxy Signals for Demographic Shift Detection

_Last updated: 2026-03-28 (UTC)._  
_Scope: Egypt, UAE, Saudi Arabia, Lebanon/Syria corridor, and Gaza/OPT as a market-collapse baseline._

## TIS // Tactical Implementation Spec

### SITREP
- **Current State:** The repository now has a live launcher, seeded source registry, collection manifest, recent-accounting tracker, source-spec contracts, and staged-external operator handoffs. The main remaining recency blocker is Ashdod (`seed-33`), not missing pipeline scaffolding.
- **Objective:** Design a legally compliant scraper + analysis framework that uses food-system proxies to detect migration-linked demographic shifts without bypassing geo-fencing, intercepting traffic, evading anti-bot controls, or abusing proxies.
- **Threat Assessment:** The core risk is analytical overreach, not just collection risk. Cuisine density, menu language, and snack assortment changes are only proxies. They can be distorted by inflation, Ramadan seasonality, tourism, premiumization, platform assortment churn, and chain-expansion strategy.

### BATTLE PLAN
- **OBJECTIVE:** Build a public-source pipeline that fuses displacement baselines with food-market and geospatial proxies, then applies explicit confidence rules before any demographic inference.
- **CURRENT_STATE:** The repo already has the source registry, evidence model, collection pipeline, recent-accounting ledger, and staged-external execution contracts. The next value is in keeping source definitions current, preserving provenance, and using the staged contracts operationally.
- **ACTIONS:**
  1. Stand up a source registry that prioritizes official humanitarian, market, trade, and mapping sources.
  2. Ingest baseline movement data first, then layer food/retail/place signals on top.
  3. Use four signal families: density, lexical drift, assortment drift, and price-positioning drift.
  4. Require at least one humanitarian baseline source plus one market proxy source before labeling any shift as correlated or inferred.
  5. Treat snack preference shifts as weak signals unless confirmed by establishment or displacement evidence.
- **VERIFICATION:** Validate against three historical windows: Sudanese arrivals into Egypt after **15 April 2023**, Syria/Lebanon movement dynamics centered on **8 December 2024**, and Gaza market-collapse indicators after **2 March 2025** and **18 March 2025**.
- **CONTINGENCY:** If delivery platforms or retailer pages become unavailable, pivot to Google Places, OpenStreetMap/Overpass, investor relations releases, public merchant pages, app-store listings, and trade statistics.

### QUALITY METRICS & SUCCESS VALIDATION
- **Metric 1:** Percentage of establishment or assortment anomalies independently confirmed by a second source.
- **Metric 2:** Median lag between humanitarian baseline event date and first proxy-signal detection.
- **Metric 3:** False-positive rate after controlling for Ramadan, tourism peaks, inflation, and chain-led expansion.
- **Success Validation:** At least three historical backtests recover known movement or market-disruption episodes with analyst-reviewable evidence trails.

### SECURITY & COMPLIANCE GATES
- **Lint/Static:** Not applicable yet; planning only.
- **CVE Scan:** Not applicable yet; no dependency changes.
- **Unsafe patterns avoided:** No MITM interception, no anti-bot evasion, no residential proxy abuse, no authenticated scraping against platform terms, no collection of private or sensitive personal data.

## 1) Mission Constraints and Legal/Ethical Guardrails

### Hard boundaries
Do **not** include any of the following in implementation:
- Geo-fencing bypasses.
- MITM interception of mobile or web APIs.
- Anti-bot evasion patterns, browser fingerprint spoofing, or captcha defeat.
- Residential proxy abuse.
- Credential scraping or authenticated account automation.

### Allowed collection classes
Use only:
- Official APIs and official downloadable datasets.
- Public web pages that are accessible without authentication.
- Public business registry or investor-relations material.
- Public merchant pages, menus, and retailer catalogue pages where collection volume remains modest and terms-compatible.
- Explicit staged-external handoffs for query execution, browser export, or manual capture where the repo records the contract and the operator completes the compliant public step.
- Licensed commercial data only where counsel and procurement approve terms.

### Current operator workflow
When a source stages instead of collecting directly:
- check `plans/connector_readiness.csv`,
- inspect the staged raw contract in `artifacts/collection/raw/<source_id>/run-*.json`,
- inspect the normalized staged contract in `artifacts/collection/normalized/<source_id>.json`,
- follow the linked `plans/source_specs/*.json` instructions,
- then rerun `python bootstrap.py --recent-accounting` and `python bootstrap.py --verification-sprint`.

### Analytic discipline
Every claim should be tagged as one of:
- **Observed:** Directly measured in source data.
- **Correlated:** Source A and source B move together in time/space.
- **Inferred:** Analyst conclusion supported by multiple sources but still indirect.
- **Unconfirmed:** Interesting anomaly lacking sufficient corroboration.

Do not infer protected-class membership for identifiable individuals. Work at district, neighborhood, or grid-cell level only.

## 2) Current Source Registry (Authoritative First)

The framework should start with sources that are stable, citable, and defensible in review.

### A. Humanitarian displacement / returns baselines
1. **UNHCR Egypt data portal**  
   https://data.unhcr.org/en/country/egy  
   Use for Egypt refugee counts, country-of-origin splits, monthly factsheets, and Sudan-arrivals updates. As of **28 February 2026**, the portal lists **1,098,750** refugees and asylum-seekers registered with UNHCR Egypt, including **841,477 from Sudan**.

2. **UNHCR regional / Lebanon flash updates and reporting hub**  
   https://data.unhcr.org/en/country/lbn  
   Use for Syria↔Lebanon movement monitoring, returns perceptions, and flash updates tied to the 2024-2026 movement dynamics.

3. **ACAPS Lebanon country analysis**  
   https://www.acaps.org/en/countries/lebanon  
   Use for conflict, displacement, returns, refugee-load, and humanitarian-context baselines. ACAPS notes a **27 November 2024** ceasefire and return of **over 918,000** people to southern Lebanon, with **115,000** still displaced until February 2025.

4. **IOM Displacement Tracking Matrix (DTM) Sudan**  
   https://dtm.iom.int/sudan  
   Use for displacement-location tracking, movement snapshots, and cross-border context that can anchor Egypt and regional spillover analysis.

5. **OCHA OPT Gaza humanitarian response updates**  
   https://www.ochaopt.org/publications/situation-reports  
   Use for Gaza market-functionality collapse baselines. OCHA states that food consumption sharply deteriorated after the total blockade on supplies since **2 March 2025** and renewed hostilities since **18 March 2025**.

6. **IPC Gaza and Lebanon analyses**  
   Gaza example: https://www.ipcinfo.org/fileadmin/user_upload/ipcinfo/docs/IPC_Gaza_Strip_Acute_Food_Insecurity_Malnutrition_Apr_Sept2025_Special_Snapshot.pdf  
   Lebanon example hub: https://www.ipcinfo.org/ipc-country-analysis/details-map/en/c/1159456/  
   Use for food insecurity severity, projection windows, and district-level stress context.

7. **WFP Lebanon market and food-system materials**  
   https://www.wfp.org/publications-WFP-Lebanon-Programme-Factsheets-2025  
   https://www.wfp.org/publications/strengthening-government-institutions-inclusive-food-systems-transformation-lebanon  
   Use for retail/market functionality context and food-systems resilience framing.

### B. Trade, macro-retail, and FMCG baselines
1. **UN Comtrade / UNSD API catalogue**  
   https://comtradeplus.un.org/DataAvailability  
   https://unstats.un.org/unsd/api/  
   Use for monthly/annual import shifts in ingredients and finished goods: sesame, pistachio, dates, confectionery bases, bakery inputs, tea/coffee categories, sweet biscuits, and savory snacks.

2. **USDA FAS Saudi Arabia retail and market fact sheets**  
   https://www.fas.usda.gov/data/saudi-arabia-retail-foods-annual  
   https://apps.fas.usda.gov/newgainapi/api/Report/DownloadReportByFileName?fileName=Retail+Foods+Annual_Riyadh_Saudi+Arabia_SA2025-0015  
   Use for Saudi retail structure, top retailers, channel mix, and growth categories. Public USDA material flags **savory snacks** among top Saudi growth products and places retail food industry size above **$50 billion**.

3. **Agthia / Abu Auf investor-relations signals**  
   https://www.agthia.com/news/agthia-group-consolidates-additional-10-stake-in-egyptian-healthy-snacks-and-coffee-company-abu-auf-group/  
   https://www.agthia.com/wp-content/uploads/2025/05/Agthia-Group-Q1-2025-MDA-EN.pdf  
   Use as an Egypt premium-snacking / healthy-snack formalization proxy. Agthia states Abu Auf opened **100+ net new stores over two years**, and Q1 2025 material says its ownership increased from **70% to 80%**.

### C. Public geospatial and place-search sources
1. **Google Places API**  
   https://developers.google.com/maps/documentation/places/web-service  
   Use for compliant place discovery and attributes. The documented core features are **Nearby Search**, **Text Search**, and **Place Details**.

2. **Google Maps pricing / usage guidance**  
   https://developers.google.com/maps/billing-and-pricing/manage-costs  
   Use to enforce quotas, field masks, key restrictions, and cost controls from day one.

3. **OpenStreetMap / Overpass**  
   https://wiki.openstreetmap.org/wiki/Overpass_API/Overpass_QL  
   https://overpass-turbo.eu/  
   Use for bakeries, groceries, supermarkets, confectionery shops, cafés, and historical/iterative POI extraction where mapping quality is adequate.

4. **Google Trends**  
   https://trends.google.com/explore  
   Use only for relative attention shifts, not counts. Normalize by geography and period and treat as a weak supporting signal.

### D. Public retail / platform / consumer-trend signals
1. **Deliveroo UAE public trend releases**  
   https://deliveroo.ae/more/news-articles/11-dishes-from-uae-restaurants-make-it-to-deliveroo-s-top-100-trending-dishes-of-2024  
   Use as a directional proxy for what is becoming mainstream or premiumized. Deliveroo states that the dessert **“Can’t Get Knafeh of It”** was the top trending dish worldwide for 2024.

2. **Spinneys UAE trend content**  
   https://www.spinneys.com/en-ae/lifestyle/top-trends-2025/  
   Use as soft evidence on premiumization, flavor drift, and merchandising narratives, never as a standalone demographic signal.

3. **Public retailer e-commerce catalogues**  
   Examples to evaluate country-by-country: Carrefour, LuLu, Danube, Panda, Tamimi, Spinneys, Choithrams, Abu Auf.  
   Use only where robots, terms, and collection volume allow. Favor periodic snapshots over high-frequency scraping.

## 3) Country-by-Country Hypotheses to Test

### Egypt
**Highest-value pilot.**  
Anchor on Sudan-linked movement after **15 April 2023** using UNHCR and IOM baselines. Then test for:
- Growth in Sudanese bakeries, tea/coffee shops, and staple-food merchants.
- Menu terms for Sudanese breads, ful variants, millet/sorghum dishes, and Sudanese dialect markers.
- Assortment changes in districts with strong new-arrival concentration.

**Priority geography:** Cairo, Giza, 6th of October, Nasr City, Ain Shams, and other districts surfaced by humanitarian updates plus place-density data.

### UAE
**Premiumization and diaspora-mainstreaming market, not a displacement market first.**  
Test for:
- Viral dessert diffusion and knafeh/kunafa product expansion.
- Pistachio-heavy and fusion dessert SKU growth.
- Grocery-delivery assortment spread by emirate and neighborhood.

**Important caveat:** A premium dessert trend in Dubai can reflect tourism, social media virality, and affluent consumer behavior rather than migration.

### Saudi Arabia
**Best market for scalable retail and SKU monitoring.**  
Test for:
- Savory-snack assortment growth by chain and city.
- Specialty bakery/grocery clustering around labor-heavy or migrant-heavy districts.
- Differences between Riyadh, Jeddah, Dammam, and seasonal pilgrimage demand zones.

### Lebanon / Syria corridor
**Movement-sensitive but noisy.**  
Test for both appearance and disappearance of signals:
- New low-cost Syrian or mixed-Levant bakeries/sweets.
- Menu simplification or closure clusters after return waves.
- Changes in Akkar, Tripoli, Bekaa, and Beirut-adjacent districts following cross-border movement events.

### Gaza / OPT
Use Gaza mainly as a **market-collapse baseline**, not as a snack-preference market. Focus on:
- Stockouts.
- Retail functionality collapse.
- Price spikes and assortment compression.
- Reduced menu diversity where public signals remain available.

## 4) Signal Bundle and Causal Logic

Use four signal families, in descending order of robustness:

### 4.1 Establishment density
Examples:
- New bakeries by cuisine tag or dish lexicon.
- Growth in grocery/specialty stores selling culturally specific staples.
- Open/close velocity by district and month.

**Why it is useful:** Dense physical clustering often survives algorithm changes and is easier to triangulate with maps, business directories, and merchant pages.

### 4.2 Lexical drift
Examples:
- New dish names appearing in public menus outside their previous district footprint.
- Dialect-specific spelling variants in Arabic menus.
- Brand or product naming tied to origin cuisines.

**Why it is useful:** Menu language often changes earlier than store-category taxonomies.

### 4.3 Assortment drift
Examples:
- New SKUs in nuts, dates, sesame products, tahini, tea/coffee, biscuits, savory snacks, syrups, baking inputs.
- Chain assortment changes by store zone or delivery radius.

**Why it is useful:** Captures mainstreaming after the first establishment wave.

### 4.4 Price-positioning drift
Examples:
- A cuisine moving from low-cost staple outlets into premium dessert or packaged-snack format.
- Localized spread from informal or budget merchants into modern retail.

**Why it is useful:** Helps distinguish subsistence-driven community formation from affluent adoption.

## 5) Data Model v0.1

Use **PostgreSQL + PostGIS**. Suggested core tables:

### `source_registry`
- `source_id`
- `source_name`
- `source_family` (humanitarian, trade, retail, place, menu, trend, investor_relations)
- `access_type` (api, csv, pdf, html, manual)
- `terms_notes`
- `robots_notes`
- `refresh_cadence`
- `country_scope`
- `confidence_weight`

### `event_baseline`
- `event_id`
- `source_id`
- `event_type` (arrival, return, displacement, blockade, ceasefire, hostilities)
- `country`
- `admin1`
- `admin2`
- `event_date`
- `population_estimate`
- `confidence`
- `source_url`

### `place_snapshot`
- `place_key`
- `source_id`
- `external_id`
- `name_original`
- `normalized_name`
- `category`
- `cuisine_tag`
- `lat`
- `lon`
- `district`
- `city`
- `country`
- `first_seen_at`
- `last_seen_at`
- `snapshot_date`

### `menu_item_snapshot`
- `menu_item_id`
- `place_key`
- `item_name_original`
- `normalized_text`
- `detected_script`
- `detected_dialect`
- `cuisine_tag`
- `ingredient_tags`
- `price`
- `currency`
- `snapshot_date`
- `source_url`

### `sku_snapshot`
- `sku_id`
- `retailer`
- `store_zone`
- `country`
- `city`
- `sku_name`
- `brand`
- `origin_country`
- `category`
- `ingredients_text`
- `weight`
- `price`
- `promo_flag`
- `snapshot_date`
- `source_url`

### `anomaly`
- `anomaly_id`
- `geography_key`
- `signal_family`
- `metric_name`
- `baseline_window`
- `observation_window`
- `expected_value`
- `observed_value`
- `zscore`
- `changepoint_score`
- `status_label` (observed, correlated, inferred, unconfirmed)
- `confidence`
- `analyst_note`

## 6) Collection Architecture

### Collection order
1. **Official baselines first**  
   UNHCR, IOM DTM, OCHA, IPC, WFP, UN Comtrade.
2. **Places second**  
   Google Places and OSM/Overpass.
3. **Retail/merchant third**  
   Public menus, catalogues, investor-relations pages, public press releases.
4. **Weak signals last**  
   Google Trends, trend articles, public app-store listings.

### Refresh cadence
- Humanitarian baselines: daily to weekly.
- Place datasets: weekly.
- Retail/merchant snapshots: weekly or biweekly.
- Trade statistics: monthly.
- Trend signals: weekly.

### Storage policy
- Save raw HTML/PDF metadata and parsed extracts separately.
- Keep a hash per fetch for evidentiary reproducibility.
- Version normalized outputs by extraction date.
- Store exact source URL and access timestamp for every derived record.

## 7) NLP / Taxonomy Stack

### Phase 1: Lexicon-first
Start simple.
- Arabic normalization.
- Script normalization for Arabic/Latin transliterations.
- Hand-built lexicons for Sudanese, Egyptian, Levantine, Gulf, and pan-Arab food terms.
- Seed synonym tables for variant spellings.
- Embedding-based clustering for unknown menu items.
- Human review queue for unresolved terms.

### Phase 2: Lightweight classifiers
- Dialect classifier.
- Cuisine classifier.
- Novelty detector: “term or cuisine appears in a new district.”

### Phase 3: Model expansion only if justified
Only consider a fine-tuned local model after a strong labeled set exists. Before that point, a lexicon-plus-review workflow will be faster and easier to validate.

## 8) Analytics and Inference Rules

### Core methods
- Seasonal decomposition (STL) for weekly or monthly series.
- Rolling z-scores by district, cuisine, and source family.
- Bayesian or penalized change-point detection.
- Event-study windows around humanitarian baselines.
- Synthetic controls where enough comparison districts exist.

### Required controls
Every scorecard should control for:
- Ramadan and Eid timing.
- Tourism peaks.
- Inflation and FX shocks.
- Platform assortment policy changes.
- Chain expansion campaigns.
- Supply shocks and import restrictions.

### Minimum inference rule
To publish an analytic conclusion, require:
1. **One hard baseline source** from UNHCR, IOM, OCHA, WFP, IPC, or official statistics, and  
2. **One independent market proxy** from places, public merchant pages, retailer assortment, trade data, or investor-relations material.

Three-source confirmation should be the standard for higher-confidence reporting.

## 9) Backtesting Framework

### Backtest A: Egypt / Sudan arrivals
- **Anchor date:** 15 April 2023.
- **Outcome target:** District-level emergence of Sudan-linked bakery/café/grocery signals in Greater Cairo and Giza.
- **Success test:** Detect abnormal growth after the humanitarian baseline shift and confirm with at least one additional place or merchant source.

### Backtest B: Lebanon / Syria corridor
- **Anchor date:** 8 December 2024 stakeholder-specified transition marker, plus 27 November 2024 ceasefire context and subsequent 2025 flash updates.
- **Outcome target:** Appearance/disappearance of bakery and low-cost cuisine clusters in Akkar, Tripoli, Bekaa, and Beirut-adjacent districts.
- **Success test:** Detect whether place density and menu lexicon changes track movement reports rather than only tourism or seasonal demand.

### Backtest C: Gaza market collapse
- **Anchor dates:** 2 March 2025 blockade; 18 March 2025 renewed hostilities.
- **Outcome target:** Assortment compression, place inactivity, menu simplification, or price spikes where public signals remain measurable.
- **Success test:** Show collapse signals rather than preference signals, and match timing to OCHA/IPC deterioration.

## 10) Implementation Roadmap

### Milestone 0: Registry and schema
Deliverables:
- Source registry.
- Terms/compliance checklist.
- Country and district priority list.
- Postgres/PostGIS schema.

### Milestone 1: Egypt pilot
Deliverables:
- Cairo/Giza target district list.
- UNHCR/IOM baseline ingestion and freshness verification.
- Google Places + OSM bakery/grocery staged or direct collection contracts.
- Menu lexicon v1 for Sudan-linked terms.
- First anomaly dashboard or analyst-ready briefing pack.

### Milestone 2: UAE premiumization track
Deliverables:
- Dessert/snack lexicon.
- Retail assortment snapshot workflow.
- Neighborhood-level spread analysis for premiumized Levantine desserts.

### Milestone 3: Lebanon/Syria corridor
Deliverables:
- Flash-update event timeline.
- Bakery/open-close tracking.
- Cross-border event overlays and analyst review process.

### Milestone 4: Saudi retail scaling
Deliverables:
- Retail chain and city monitoring pack.
- SKU-category drift dashboard.
- Labor-heavy district comparison analysis.

## 11) Recommended v0.1 Operating Footprint

Start small.
- **Countries:** Egypt, UAE, Saudi Arabia, Lebanon.
- **Special baseline:** Gaza/OPT for market-collapse reference.
- **Target districts:** 20 total.
- **Source families:** 6.
- **Refresh cadence:** Weekly for operational monitoring, monthly for trade.
- **Review model:** One analyst queue with explicit evidence labels.

### Suggested outputs
- District heatmap of establishment change.
- Cuisine-emergence chart.
- SKU/assortment drift chart.
- Event timeline overlay.
- Analyst evidence card showing source links and confidence labels.

## 12) Final Assessment

### What is robust
- Establishment density plus humanitarian baselines.
- Trade/import shifts in staple or culturally specific inputs.
- Public retailer assortment snapshots where terms allow collection.
- Market-collapse tracking in Gaza using OCHA/IPC/WFP context.

### What is useful but weaker
- Google Trends.
- Retail lifestyle articles.
- Delivery-platform public trend roundups.
- Premium dessert virality.

### What will break first if overused
- Any signal that depends on a single platform.
- High-frequency assortment scraping without legal review.
- Preference-based inference without a displacement baseline.
- District-level claims that ignore Ramadan, tourism, or inflation.

## Companion Operational Documents

- `docs/confidence-rubric.md` operationalizes the evidence labels into a weighted scoring model for analyst review.
- `docs/district-selection-matrix.md` defines how to choose monitoring districts and control districts without cherry-picking.
- `docs/v0.1-execution-plan.md` converts the roadmap into acceptance-tested milestones and go/no-go gates.

## 13) Immediate Next Steps

1. Lock the scoring workflow in `docs/confidence-rubric.md` so every anomaly receives a repeatable confidence label.
2. Finalize the first 20 monitoring districts and their controls using `docs/district-selection-matrix.md`.
3. Keep `plans/connector_readiness.csv` and `plans/collection_runbook.md` aligned with the staged-external operator workflow.
4. Review and expand `docs/source-registry.csv` with term/licensing notes, district coverage, and owner metadata.
5. Stand up PostgreSQL + PostGIS schema for the tables listed here when the evidence loop is stable enough to justify it.
6. Build a lexicon seed file for Sudanese, Levantine, Egyptian, and Gulf food terms.
7. Run **Egypt first** as the primary backtest and pilot.
8. Do not scale platform collection until the two-source confirmation workflow is working.
