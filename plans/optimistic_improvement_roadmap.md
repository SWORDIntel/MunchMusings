# Optimistic Improvement Roadmap

_Prepared: 2026-03-27._

This roadmap treats the docs as authoritative and the current codebase as a capable but incomplete implementation. The goal is not to rewrite the repo around a new idea. The goal is to harden the existing source-accounting, collection, and briefing pipeline into a repeatable control plane that can sustain weekly operations.

## Operating Principles
- Keep lawful public-source collection as the boundary.
- Preserve the current bootstrap-led architecture, but make the control plane explicit.
- Optimize for rerunnability, provenance, and reviewability before broader feature growth.
- Use three parallel workstreams for every major planning cycle: research, analysis, and pipeline engineering.
- Prefer one Cairo/Giza or Egypt pilot path that works end to end before cloning the pattern to Israel and then the wider corridor set.

## Priority Order

### 1. Source Accounting Control Plane
This was the first dependency and is now mostly complete. The ledger is trustworthy enough to drive collection and briefing, with only one live tier-1 recency blocker left.

Deliverables:
- Keep `plans/source_verification_sprint.csv` and `plans/work_queue.csv` synchronized.
- Preserve publication dates, evidence links, owners, and next actions as first-class fields.
- Keep the final tier-1 recency blocker (`seed-33`) explicit instead of masking it with weak freshness claims.

Success looks like:
- Every tier-1 source except known blocked/stale cases has a current state.
- The queue reflects due work instead of manual memory.
- Source freshness can be reviewed without opening raw source pages.

### 2. Scheduled Collection Orchestration
The next layer is a dependable operating loop that converts the ledger into actual collection work.

Deliverables:
- Generated due tasks from source cadence.
- Stable collection manifests and evidence-capture logs.
- A repeatable operator flow through `bootstrap.py --collect-ready` and `scripts/run_operating_cycle.py`.
- Honest blocked-public-source fallbacks where live collection is public but intermittently inaccessible.
- Actionable request specs for staged external sources such as place-query collectors.

Success looks like:
- Collection can be rerun without manually reconstructing state.
- Failed or incomplete runs remain visible instead of being overwritten.
- Ready work is derived from the ledger, not improvised by the operator.

### 3. Normalized Evidence Store
Once collection is stable, the repo should elevate outputs from CSV starters into a coherent evidence layer.

Deliverables:
- Standardized normalized rows for baselines, observations, anomalies, and claims.
- Durable linkage from source rows to briefing artifacts.
- Clear evidence packet structure for reviewer handoff.

Success looks like:
- A source observation can be traced into a claim and then into a briefing.
- The same collection inputs produce the same normalized shape.
- The review path is explicit, not inferred from file names.

### 4. District Change Detection
The docs are clear that district control pairs and anomaly thresholds matter, but only after the baseline layer is defensible.

Deliverables:
- Frozen Egypt district/control pairs.
- Monthly or cycle-based observation overlays.
- Confound flags for seasonality, tourism, inflation, and chain rollout.

Success looks like:
- Cairo/Giza anomalies can be compared against control districts.
- The system detects change without over-claiming causality.
- District selection is stable enough to support repeatable backtests.

### 5. Decision-Grade Reporting
The final layer is a reviewable output that packages the ledger, baseline, observations, and decisions into one artifact.

Deliverables:
- Zone briefings with explicit source status and confidence labels.
- Claim registers and evidence indexes with reviewer-ready notes.
- A weekly operating picture that highlights readiness, gaps, and emerging signals.

Success looks like:
- Cairo/Giza becomes the first repeatable evidence pack.
- The pattern can then be cloned to Israel.
- Reporting is a consequence of the pipeline, not a separate manual exercise.

## 3-Agent Split For The Current Cycle

### Agent 1: Research And Source Accounting
Own the ledger and source freshness work.

Immediate tasks:
- Keep `seed-33` current on provenance and blocker notes.
- Refresh verification notes and evidence links where the collector now emits stronger metadata.
- Reconcile source status with the now much smaller current work queue.

### Agent 2: Collection And Recovery Engineering
Own the rerunnable operating loop.

Immediate tasks:
- Harden cycle-state durability.
- Reduce overwrite risk in collection outputs.
- Improve staged external handoffs so query-driven sources carry district-scoped request payloads.
- Make interruptions visible and recoverable.

### Agent 3: Briefing And Control-Plane Synthesis
Own the docs-to-operator translation.

Immediate tasks:
- Keep the roadmap current with actual repo state.
- Ensure the control plane vocabulary is consistent across docs and plans.
- Prepare the first decision-grade Cairo/Giza path now that the ledger is mostly clean.

## Near-Term Milestones

### Milestone A: Control Plane Verified
The source ledger, verification sprint, and work queue align.

### Milestone B: Collection Loop Stable
The operating cycle can be rerun with durable state and readable failure behavior.

### Milestone C: Evidence Pack Ready
One Cairo/Giza briefing can be built from ledger-backed baselines and observations.

### Milestone D: Pattern Reusable
The evidence-pack pattern can be cloned to Israel without changing the underlying operating model.

## Next Actions
1. Keep `seed-33` as the only open recency blocker unless a newer official Ashdod release becomes collectible.
2. Improve the staged external handoff quality for place-query sources and other external collectors.
3. Use Cairo/Giza as the first proof point for the evidence-pack chain.
4. Expand only after the control plane is repeatable and the outputs are auditable.
