# Confidence Rubric for Migration-Linked Food Proxy Analysis

_Last updated: 2026-03-21 (UTC)._  
_Purpose: convert the high-level labels in the implementation plan into a repeatable analyst scoring model._

## TIS // Tactical Implementation Spec

### SITREP
- **Current State:** The main implementation plan defines the labels `Observed`, `Correlated`, `Inferred`, and `Unconfirmed`, but it does not specify how analysts should calculate confidence consistently.
- **Objective:** Provide a fast, auditable, and operational scoring rubric that turns mixed-source anomalies into publishable confidence levels.
- **Threat Assessment:** The main failure mode is analyst inconsistency. Without explicit source weighting and confound penalties, different reviewers can reach different conclusions from the same evidence.

### BATTLE PLAN
- **OBJECTIVE:** Standardize analytic confidence scoring for establishment, lexical, assortment, and price-positioning anomalies.
- **CURRENT_STATE:** The source stack is defined, but the scoring logic is not yet encoded.
- **ACTIONS:**
  1. Score each candidate anomaly on a 100-point scale.
  2. Weight humanitarian baselines and market proxies separately.
  3. Apply mandatory penalties for confounds such as Ramadan, tourism, inflation, and chain-led rollout.
  4. Map final scores to a fixed publication label.
- **VERIFICATION:** Two analysts using the same evidence packet should land within ±10 points of one another.
- **CONTINGENCY:** If evidence is incomplete, hold the anomaly at `Unconfirmed` and queue it for another collection cycle.

### QUALITY METRICS & SUCCESS VALIDATION
- **Metric 1:** Inter-analyst scoring variance.
- **Metric 2:** Percentage of anomalies later confirmed by a second independent source.
- **Success Validation:** Historical backtests should produce similar or better precision than ad hoc analyst judgment.

### SECURITY & COMPLIANCE GATES
- **Lint/Static:** Not applicable; documentation only.
- **CVE Scan:** Not applicable; no dependency changes.
- **Unsafe patterns avoided:** No sensitive personal data, no identity attribution, no single-source demographic claims.

## 1) Scoring Overview

Each anomaly starts at **0** and can earn up to **100 points**.

### Component weights
- **Humanitarian baseline evidence:** up to 30 points.
- **Market/food proxy evidence:** up to 30 points.
- **Spatial fit:** up to 15 points.
- **Temporal fit:** up to 15 points.
- **Cross-source consistency:** up to 10 points.

### Mandatory confound penalties
Subtract points when the anomaly could be better explained by:
- **Ramadan / Eid seasonality:** -5 to -20.
- **Tourism or pilgrimage peaks:** -5 to -20.
- **Inflation / FX shock:** -5 to -15.
- **Chain-led rollout / promotion campaign:** -5 to -20.
- **Platform taxonomy or assortment policy change:** -5 to -15.

## 2) Source Family Weights

### A. Humanitarian baseline evidence (0-30)
- **UNHCR / IOM / OCHA / IPC / WFP / official statistics** in the relevant geography and time window: 20-30.
- Same sources but only at country level or with lagged timing: 10-19.
- Narrative context without usable time or location granularity: 1-9.
- No baseline support: 0.

### B. Market proxy evidence (0-30)
- **Places + merchant/catalogue/menu evidence aligned in time and district:** 20-30.
- One strong proxy source with clear district signal: 10-19.
- Only weak consumer-trend or lifestyle coverage: 1-9.
- No relevant proxy evidence: 0.

### C. Spatial fit (0-15)
- Strong district or neighborhood match: 12-15.
- City-level fit only: 6-11.
- Country-level fit only: 1-5.
- No geographic fit: 0.

### D. Temporal fit (0-15)
- Signal appears in the expected lag window after an event: 12-15.
- Plausible but loose timing: 6-11.
- Weak timing alignment: 1-5.
- No timing fit: 0.

### E. Cross-source consistency (0-10)
- Three or more independent sources align: 8-10.
- Two independent sources align: 4-7.
- One source only: 0-3.

## 3) Publication Labels

### 85-100: Observed
Use only when:
- there is strong hard-baseline support,
- at least two independent market signals agree, and
- confounds are controlled or explicitly ruled out.

### 65-84: Correlated
Use when:
- the anomaly lines up well with displacement or market-collapse timing,
- at least one independent market signal supports it,
- but direct attribution is still indirect.

### 45-64: Inferred
Use when:
- evidence is suggestive,
- timing or geography is partially aligned,
- but confounds remain materially plausible.

### 0-44: Unconfirmed
Use when:
- only one source is available,
- weak signals dominate,
- or confounds are stronger than the migration-linked explanation.

## 4) Fast Scoring Worksheet

For each anomaly, answer the following:
1. What is the nearest hard-baseline event and date?
2. What is the lag between the event and the proxy signal?
3. What district or neighborhood is affected?
4. Which source families support the anomaly?
5. Which confounds could explain the same movement?
6. What evidence would move this case up one confidence tier?

## 5) Example Scoring

### Example A: Sudan-linked bakery cluster in Giza
- UNHCR/IOM support in relevant window: 26/30
- Places + menu evidence: 24/30
- Spatial fit: 14/15
- Temporal fit: 13/15
- Cross-source consistency: 8/10
- Confound penalty: -6
- **Final score:** 79 (`Correlated`)

### Example B: Viral knafeh dessert growth in Dubai mall districts
- Humanitarian baseline support: 4/30
- Market proxy evidence: 18/30
- Spatial fit: 8/15
- Temporal fit: 7/15
- Cross-source consistency: 6/10
- Tourism / virality penalty: -18
- **Final score:** 25 (`Unconfirmed`)

### Example C: Gaza assortment collapse after March 2025
- OCHA/IPC support: 29/30
- Market proxy evidence: 21/30
- Spatial fit: 12/15
- Temporal fit: 15/15
- Cross-source consistency: 8/10
- Confound penalty: -2
- **Final score:** 83 (`Correlated`)

## 6) Operational Rule Set

- Never publish `Observed` based on weak consumer-trend content alone.
- Never let Google Trends or lifestyle articles raise a case above `Inferred` without harder support.
- If a chain expansion campaign is documented, cap the case at `Inferred` unless additional district-specific evidence overrides it.
- If a case loses its hard-baseline support after review, downgrade it by at least one tier.

## 7) Analyst Review Output

Each reviewed anomaly should store:
- anomaly ID,
- raw score,
- penalty total,
- final score,
- publication label,
- analyst initials,
- review timestamp,
- evidence links,
- explicit confound notes,
- next collection action.
