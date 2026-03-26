# Review Decision Template

## Decision Header
- `decision_id`:
- `pack_id`:
- `zone_name`:
- `country`:
- `decision_utc`:
- `reviewer`:
- `approver_role`:

## Decision Summary
- `decision_label`: `approve` / `defer` / `downgrade` / `reject`
- `publication_label`:
- `summary`:
- `one_line_rationale`:

## Basis For Decision
- `source_health_assessment`:
- `baseline_coverage`:
- `observation_quality`:
- `anomaly_quality`:
- `confound_strength`:
- `counterevidence_summary`:

## Required Follow-Up
- `next_collection_action`:
- `next_review_date`:
- `owner`:
- `blocked_by`:

## Decision Rule
- Approve only if the evidence chain is complete enough to survive handoff.
- Defer if the source ledger is stale or the observation layer is incomplete.
- Downgrade if the confounds are stronger than the claimed pattern.
- Reject if the pack is not briefing-grade or if provenance is missing.
- Escalate if the same source issue affects more than one zone or evidence family.
