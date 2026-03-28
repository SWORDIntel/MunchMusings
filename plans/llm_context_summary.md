# LLM Context Summary - 2026-03-28

## Status Overview
The docs-first control-plane pass is mostly complete. The source-accounting lane is effectively closed, the verification tracker is narrow, and the codebase now behaves like a rerunnable evidence pipeline rather than a planning stub.

## Current Live State
- `plans/work_queue.csv` is down to one active recent-accounting task: `ACC-RA-033`.
- `plans/source_verification_sprint.csv` is still narrowed to `seed-25` and `seed-33`, but `seed-25` is now a low-priority `manual_review` row rather than hard recency debt.
- `seed-02` now completes collection through a blocked-public-source fallback instead of staying `staged_external`.
- `seed-11`, `seed-12`, and `seed-17` still require external execution, but their raw and normalized staged artifacts now carry district-scoped query payloads, source-spec metadata, and execution contracts instead of thin placeholders.
- Manual/browser staged rows (`seed-13`, `seed-18`, `seed-19`, `seed-20`) now carry explicit operator contracts and appear in `plans/connector_readiness.csv`.

## What Still Matters
- `bootstrap.py` is the control-plane entry point for recent accounting, verification sprinting, collection, and zone briefing.
- `scripts/run_operating_cycle.py` remains the wrapper for resumable collection and briefing cycles.
- `plans/` is still the operator-facing state layer for freshness, queueing, handoffs, and evidence provenance.

## Remaining Gaps
- `seed-33` is still the only real tier-1 recency blocker; the issue is source freshness and access, not parser coverage.
- Staged external execution still depends on operators, even though the request specs are now self-describing.
- The planning docs need to stay synchronized with the now much smaller live backlog.

## Immediate Next Actions
- Keep `seed-33` honest and provenance-rich while waiting for a newer collectible Ashdod publication.
- Keep the staged external handoff layer aligned as operators start using the new contracts, especially for place-query execution and browser/manual captures.
- Keep roadmap and summary docs aligned with the live queue instead of the earlier broad cleanup phase.

## Key Artifacts
- `plans/optimistic_improvement_roadmap.md`
- `plans/recent_accounting.csv`
- `plans/source_verification_sprint.csv`
- `plans/work_queue.csv`
