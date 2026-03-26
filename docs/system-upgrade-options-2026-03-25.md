# System Upgrade Options

_Date: 2026-03-25 UTC._

## Boundary

This repo should not attempt to reproduce HUMINT or SIGINT collection. The strongest lawful upgrade path is a decision-grade public-source evidence system with explicit provenance, recency, and confidence controls.

## Six High-Level Upgrade Paths

### 1. Provenance Graph and Claim Registry
- Objective: move from scattered CSV artifacts to a claim-centric evidence system.
- Core components: `claim`, `observation`, `source`, `event_baseline`, `confound`, and `review_decision` records linked by stable IDs.
- Why it changes the system materially: analysts stop reasoning from loose notes and start reasoning from auditable claim graphs.
- Biggest risks: schema sprawl, over-modeling before enough real cases exist, and poor join discipline.
- First concrete deliverable: a claim registry schema and one Egypt anomaly recorded end-to-end with linked source observations.

### 2. Spatial-Temporal Warehouse
- Objective: make district-level change measurable over time rather than anecdotal.
- Core components: Postgres/PostGIS, snapshot tables for places/menus/SKUs, normalized district geometries, and event-window overlays.
- Why it changes the system materially: enables backtests, lag analysis, and control-district comparisons at scale.
- Biggest risks: geometry normalization, sparse local coverage, and warehouse buildout before collection quality is stable.
- First concrete deliverable: one Cairo/Giza pilot database with baseline events and place snapshots for a defined window.

### 3. Collection Control Plane and Freshness SLA
- Objective: turn source monitoring into a managed operating function.
- Core components: source ledger, freshness policy, due/overdue queue, owner assignment, and rerunnable refresh commands.
- Why it changes the system materially: makes “recent” a measurable state instead of an assumption.
- Biggest risks: analysts treating missing dates as acceptable and letting stale sources contaminate downstream scoring.
- First concrete deliverable: all tier-1 sources assigned, dated, and classified as `current`, `due_now`, `overdue`, `blocked`, or `unknown`.

### 4. Confound Attribution Lab
- Objective: stop over-attributing market changes to migration when seasonality or commercial rollout explains them better.
- Core components: explicit confound records for Ramadan, tourism, inflation, FX shocks, chain expansion, and taxonomy changes.
- Why it changes the system materially: it raises precision and protects the system from false positives.
- Biggest risks: incomplete confound data and analysts applying penalties inconsistently.
- First concrete deliverable: one backtest worksheet where every Egypt anomaly candidate is scored against a fixed confound checklist.

### 5. Decision-Grade Public-Source Evidence Pack
- Objective: produce a zone-level report that is rigorous enough for operational review without crossing into restricted collection.
- Core components: zone brief, source observation log, event baseline table, anomaly cards, claim register, evidence index, and reviewer signoff.
- Why it changes the system materially: it turns the repo from planning and raw artifacts into a product that can be reviewed, challenged, and reused.
- Biggest risks: users mistaking a strong OSINT pack for HUMINT/SIGINT equivalence, and uneven evidence quality across zones.
- First concrete deliverable: one Egypt pilot evidence pack for Cairo/Giza using only public humanitarian, mapping, and merchant sources.

### 6. Weekly Operating Picture and Dossier Production
- Objective: standardize how weekly decisions are made and documented.
- Core components: fixed refresh cycle, one-page executive brief, anomaly table, source recency appendix, and go/no-go gate.
- Why it changes the system materially: leadership sees trend, confidence, and source health in one repeatable artifact.
- Biggest risks: dashboard theater without evidence depth, and pressure to summarize before source accounting is complete.
- First concrete deliverable: a weekly Egypt review brief backed by the recent-accounting ledger and anomaly worksheet.

## Selected Path

### Path 5: Decision-Grade Public-Source Evidence Pack

This is the strongest lawful path if the goal is a zone-level output that approaches operational usefulness without replicating HUMINT/SIGINT.

#### Objective
- Build a reusable evidence pack for one zone that integrates source freshness, event baselines, market observations, confounds, and analyst judgment into one reviewable dossier.

#### Target Output
- `zone_brief.md`
- `source_observation_log.csv`
- `event_baseline.csv`
- `anomaly_cards.csv`
- `claim_register.csv`
- `evidence_index.csv`
- `review_decision.md`

#### Required Data Layers
1. Source accounting layer
   - source ID
   - last checked time
   - latest publication date
   - evidence link
   - freshness status
2. Baseline layer
   - humanitarian or official event
   - event date
   - geography
   - confidence
3. Observation layer
   - place/menu/SKU/public-report observation
   - capture date
   - district
   - source family
4. Analytic layer
   - anomaly summary
   - score
   - confounds
   - publication label

#### Why this path is the best immediate upgrade
- It reuses the repo's strongest assets: source registry, district watchlist, event timeline, confidence rubric, and recent-accounting ledger.
- It creates an actual operator product rather than another planning document.
- It forces every claim to carry provenance and freshness metadata.

#### Biggest Risks
- Thin district-level evidence will make the pack look more confident than it is.
- Missing or stale baseline dates will contaminate the whole dossier.
- Reviewers may over-read soft consumer or lifestyle signals unless hard-source gates are enforced.

#### Minimum Acceptance Standard
- At least one hard baseline source is verified and recent enough for the review window.
- At least one independent market proxy source exists for the same zone.
- Every anomaly has a confound note.
- Every source used in the dossier appears in the recent-accounting ledger.

#### First Build Sequence
1. Lock one pilot zone: Cairo/Giza only.
2. Verify the Egypt baseline sources in `plans/recent_accounting.csv`.
3. Populate one event-baseline row with access timestamp and note.
4. Capture one public-source market observation set for the same districts.
5. Produce one anomaly card and one claim register row.
6. Ship the first dossier for review.

#### Immediate Recommendation
- Do not expand breadth first.
- Build one Egypt evidence pack that can survive challenge, then clone the pattern to Lebanon or Gaza.
