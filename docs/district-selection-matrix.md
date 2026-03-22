# District Selection Matrix for v0.1 Monitoring

_Last updated: 2026-03-21 (UTC)._  
_Purpose: standardize how the first monitoring districts and control districts are chosen._

## TIS // Tactical Implementation Spec

### SITREP
- **Current State:** The implementation plan says to define 20 target districts and control districts, but it does not yet define a reproducible selection method.
- **Objective:** Create a fast district-prioritization rubric that improves comparability and limits cherry-picking.
- **Threat Assessment:** Poor district selection will create false positives, inflate signal strength in high-density commercial zones, and weaken backtests.

### BATTLE PLAN
- **OBJECTIVE:** Select districts using comparable, score-based criteria rather than intuition.
- **CURRENT_STATE:** Country-level priorities exist, but district onboarding rules do not.
- **ACTIONS:**
  1. Score candidate districts across displacement relevance, source coverage, retail density, mapping completeness, and control comparability.
  2. Prioritize 20 monitoring districts and 10 control districts.
  3. Keep district onboarding conservative until the first backtest is stable.
- **VERIFICATION:** Two analysts using this matrix should produce materially similar district shortlists.
- **CONTINGENCY:** If local source coverage is too weak, drop the district and replace it with the highest-scoring alternate.

### QUALITY METRICS & SUCCESS VALIDATION
- **Metric 1:** Percentage of selected districts with usable data from at least three source families.
- **Metric 2:** Backtest precision difference between scored districts and ad hoc district selection.
- **Success Validation:** The first 20 districts should support all three required backtests and at least one clean control comparison per country.

### SECURITY & COMPLIANCE GATES
- **Lint/Static:** Not applicable; documentation only.
- **CVE Scan:** Not applicable; no software changes.
- **Unsafe patterns avoided:** No household-level targeting, no person-level inference, no sensitive-personal-data collection.

## 1) District Scoring Criteria

Score each district from **0 to 100**.

### A. Movement relevance (0-30)
- Hard-baseline evidence of arrivals, returns, or disruption in the district or adjacent district.
- Priority goes to places repeatedly named in UNHCR, IOM, OCHA, IPC, or WFP context.

### B. Source coverage (0-20)
- Availability of at least three of the following: Google Places, OSM, public merchant pages, menus, retailer catalogues, local press, investor-relations signals.

### C. Food-retail observability (0-20)
- Sufficient density of bakeries, groceries, cafés, sweets shops, or modern retail to produce measurable change.

### D. Mapping / geocoding quality (0-15)
- District boundaries are stable, names are consistent, and coordinates can be normalized without excessive ambiguity.

### E. Control comparability (0-15)
- Existence of a plausible control district with similar retail density and urban form but lower expected displacement exposure.

## 2) District Acceptance Thresholds

### Monitoring districts
Require:
- total score **>= 70**,
- at least **15/30** on movement relevance,
- at least **10/20** on source coverage.

### Control districts
Require:
- total score **>= 60**,
- strong retail comparability,
- lower movement relevance than the paired monitoring district.

## 3) Suggested v0.1 District Packs

### Egypt monitoring pack
- Nasr City
- 6th of October City
- Ain Shams / Matariya cluster
- Giza / Faisal corridor
- Imbaba
- Shubra El Kheima

### Lebanon monitoring pack
- Akkar
- Tripoli
- Bekaa / Zahle-adjacent districts
- Beirut southern periphery

### Saudi monitoring pack
- Riyadh labor-heavy districts
- Jeddah mixed migrant-commercial districts
- Dammam / Khobar commercial corridors

### UAE monitoring pack
- Dubai Deira / Bur Dubai-adjacent commercial districts
- Sharjah dense middle-market retail districts
- Abu Dhabi mixed grocery-delivery districts

### Gaza / OPT baseline pack
- Use administrative areas only where humanitarian and market-functionality data are consistently available.

## 4) Example District Scorecard

### Example: Giza / Faisal corridor
- Movement relevance: 26/30
- Source coverage: 16/20
- Food-retail observability: 18/20
- Mapping quality: 11/15
- Control comparability: 10/15
- **Total:** 81/100
- **Decision:** Include as monitoring district.

### Example: premium mall district in Dubai
- Movement relevance: 5/30
- Source coverage: 19/20
- Food-retail observability: 20/20
- Mapping quality: 14/15
- Control comparability: 9/15
- **Total:** 67/100
- **Decision:** Exclude from migration-focused v0.1; retain for premiumization side-track only.

## 5) Control-District Method

Pair each monitoring district with a control district that is:
- in the same country,
- economically comparable,
- similar in food-retail density,
- but lower in displacement or return relevance.

Use controls to test whether a spike is likely due to:
- general inflation,
- seasonality,
- platform catalog changes,
- or broad chain expansion.

## 6) Operational Workflow

1. Build a candidate list of 50-60 districts.
2. Score them independently by two analysts.
3. Reconcile any district with a variance greater than 10 points.
4. Select the top 20 monitoring districts.
5. Pair each with a control district where possible.
6. Revisit scores quarterly or after major humanitarian events.
