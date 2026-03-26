# Zone Briefing Format

_Purpose: define a repeatable, public-source briefing structure for one zone-level OSINT assessment._

## Boundary
- Use only public, lawful, reviewable sources.
- Do not imply HUMINT or SIGINT collection.
- Do not elevate a claim without provenance, freshness, and confound notes.

## Brief Package
1. `Header`
2. `Executive Summary`
3. `Key Judgments`
4. `Zone Scope`
5. `Source Health`
6. `Baseline Events`
7. `Observed Signals`
8. `Anomaly Cards`
9. `Claim Register`
10. `Confounds And Limits`
11. `Analyst Assessment`
12. `Review Decision`
13. `Appendix`

## Required Standards
- Every source used in the brief appears in `plans/recent_accounting.csv`.
- Every observation has a timestamp, source link, district or neighborhood, and capture method.
- Every anomaly card states the nearest baseline event, the dominant confound, and the strongest counterpoint.
- Every claim is tagged `Observed`, `Correlated`, `Inferred`, or `Unconfirmed`.

## Briefing Tone
- Use direct, compact language.
- Prefer factual statements over interpretive language.
- Separate confirmed facts from assessed implications.
- State what changed, where it changed, when it changed, and why that matters.

## Page 1 Test
- The first page must tell the reader whether the zone is worth continued attention.
- If the evidence is thin or stale, say so immediately.
- If the zone cannot support a briefing-grade assessment, label it `Unconfirmed` and stop.

## Minimum Acceptable Pack
- One zone summary.
- One source-health table.
- One baseline event table.
- One observation log.
- One anomaly card set.
- One claim register.
- One review decision paragraph.
- One appendix with source IDs and notes.

## Evidence Discipline
- A source row without `last_checked_utc`, `last_published_date`, and `evidence_link` does not count as current.
- A claim without a linked observation and baseline event does not count as briefing-grade.
- A control district must be named whenever the brief asserts a directional change.
