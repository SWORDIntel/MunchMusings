# Source Freshness Policy

## Status Rules
- `current`: latest publication date is inside the expected recency window.
- `due_now`: latest publication date is just outside the window and requires prompt re-check.
- `overdue`: latest publication date is materially outside the window.
- `blocked`: source is inaccessible, structurally changed, or non-actionable pending review.
- `manual_review`: cadence is periodic, occasional, or otherwise not suitable for automatic stale detection.
- `unknown`: no verified publication date has been recorded yet.

## Default Windows
- `weekly`: 8 days
- `biweekly`: 16 days
- `monthly`: 35 days
- `quarterly`: 100 days
- `annual`: 400 days
- `periodic`, `as needed`, `occasional`: manual review only

## Required Fields For A Credible Check
- `last_checked_utc`
- `last_published_date`
- `latest_period_covered`
- `evidence_link` or `evidence_path`
- `owner`

## Escalation Rules
- If a source URL changes or a methodology note suggests a structural break, set `status=blocked`.
- If two sources disagree on a count or date, keep both cited and flag the row for reviewer adjudication.
- If a source cannot be refreshed within one cycle, record the gap instead of carrying the prior claim forward silently.
