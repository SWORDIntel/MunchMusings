# Recent Accounting Schema

## Ledger Goal
Provide one source-level record proving what was checked recently, what the source actually covers, and what remains unresolved.

## Required Fields
- `source_id`: stable row key, derived from the canonical seed.
- `source_name`: source label from the registry.
- `refresh_cadence`: expected update rhythm.
- `last_checked_utc`: when an analyst last verified the source.
- `last_published_date`: date of the newest verified publication or release.
- `latest_period_covered`: period covered by that publication.
- `claim_date_utc`: when the working claim was entered into the ledger.
- `recency_status`: derived freshness state.
- `owner`: accountable operator.
- `status`: workflow state such as `pending_review`, `in_review`, `blocked`, or `complete`.
- `evidence_link`: public URL proving the latest publication.
- `evidence_path`: local file path for captured notes or extracts.
- `next_action`: exact next step, not a generic placeholder.

## Evidence Discipline
- Do not write a date without storing where it came from.
- Do not mark a source `current` if the latest publication date is inferred rather than verified.
- Do not reuse an old publication date after a source changes structure without a new access check.
