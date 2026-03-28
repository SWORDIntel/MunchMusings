# Phase 02: Egypt Market-Proxy Collection

## Objective
Only begin proxy collection after the Egypt baseline accounting rows are current or explicitly blocked with notes.

## Entry Gate
- `seed-01` and `seed-02` must have verified publication dates.
- Pilot districts must be frozen for the review cycle.
- A menu/place evidence packet template must exist for the first anomaly candidate.
- `seed-11` / `seed-12` staged request specs must be actionable enough for external execution.

## Inputs
- `plans/recent_accounting.csv`
- `artifacts/v0_1/anomaly-review-worksheet.csv`
- `plans/evidence_packet_template.json`

## Outputs
- First set of place/menu evidence packets.
- First analyst-reviewable anomaly rows.
- Explicit confound notes for each candidate.

## Stop Rules
- Do not score a migration-linked anomaly if the nearest baseline source is `overdue` or `unknown`.
- Do not elevate a consumer-trend signal above supporting evidence without a harder source family.
