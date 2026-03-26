# Agent Registry

## `source_monitor`
- Mission: verify the latest publication or update date for seeded sources.
- Inputs: `docs/source-registry.csv`, `plans/recent_accounting.csv`
- Outputs: `last_checked_utc`, `last_published_date`, `latest_period_covered`, `evidence_link`
- Stop condition: source row is updated and evidence is citable.
- Failure condition: broken URL, ambiguous publication date, or source-method change.

## `baseline_accountant`
- Mission: normalize humanitarian events into dated baseline rows.
- Inputs: official humanitarian sources, `plans/baseline_event_template.csv`
- Outputs: event rows with event date, access timestamp, geography, and notes.
- Stop condition: baseline row is auditable and tied to a specific source.

## `district_tracker`
- Mission: maintain the district shortlist and control pairings.
- Inputs: `docs/district-selection-matrix.md`, `artifacts/v0_1/district-watchlist.csv`
- Outputs: updated scores, control pairs, and rationale notes.
- Stop condition: district decisions are frozen for the current review cycle.

## `anomaly_scorer`
- Mission: score market signals against baseline events and confounds.
- Inputs: anomaly candidates, `docs/confidence-rubric.md`
- Outputs: scored anomaly rows and evidence packet references.
- Stop condition: each anomaly has a label, score, and confound note.

## `review_gatekeeper`
- Mission: block publication-quality claims that lack corroboration or provenance.
- Inputs: anomaly worksheet, evidence packets, recent-accounting ledger
- Outputs: go/no-go decision and remediation notes.
- Stop condition: claim is either approved, downgraded, or returned for collection.

## Handoff Rules
- `source_monitor` must update the ledger before `baseline_accountant` treats a source as current.
- `baseline_accountant` must complete baseline rows before `anomaly_scorer` finalizes a migration-linked claim.
- `review_gatekeeper` can override a confidence label if provenance is incomplete or stale.
