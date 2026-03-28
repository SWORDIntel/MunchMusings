# MunchMusings

MunchMusings is a planning-first, public-source intelligence workspace for detecting migration-linked food-market signals across Egypt, Israel, the UAE, Saudi Arabia, Lebanon/Syria, and Gaza/OPT.

## Current State

- Source accounting is mostly closed: [plans/work_queue.csv](plans/work_queue.csv) has one active recency task, `ACC-RA-033`.
- External execution is now first-class in the queue: `EXT-011` through `EXT-020` are the live staged-external operator tasks, and each row now carries the staged contract path plus inline execution metadata such as request method, connector state, and query context.
- The collection manifest is stable at 27 completed runs and 7 honest `staged_external` runs in `artifacts/collection/collection-run-manifest.csv`.
- Staged external rows are now explicit operator handoffs, not silent failures: start with the `EXT-*` row in `plans/work_queue.csv`, use its `next_action`, open the staged normalized contract it points to, then inspect `plans/connector_readiness.csv` and the staged raw spec if needed.
- `seed-05` OCHA Gaza is fixed to the live `publications/situation-reports` endpoint.
- `seed-11` and `seed-12` now emit actionable staged request specs with matched district queries and connector readiness metadata.
- `seed-33` Ashdod remains the only real tier-1 recency blocker.

## Start Here

If you only read one planning document, read:
- `docs/unified-operational-plan.md` — single consolidated plan covering the bootstrap launcher, the current seeded source stack, compliant collection rules, scoring model, district-selection method, analyst workflow, automation path, and v0.1 execution sequence.

## Single Entry Point

```bash
python bootstrap.py
```

## Recommended Flow

For normal rerunnable operations, use the wrapper:

```bash
python scripts/run_operating_cycle.py
```

That runs:
- `python bootstrap.py --collect-ready`
- `python bootstrap.py --verification-sprint`
- `python bootstrap.py --brief-zone`

and writes a dated run manifest plus log under `artifacts/operating-cycles/`.

Resume support is built in:
- `python scripts/run_operating_cycle.py --resume-latest`
- `python scripts/run_operating_cycle.py --resume-cycle-dir artifacts/operating-cycles/<cycle_id>`

The wrapper may legitimately leave some sources in `staged_external`. That is expected when a source needs credentials, a browser export, or manual capture. Treat those rows as queued operator work, not broken automation.

### Launcher behavior
- **Interactive terminal, no extra args:** launches the pipeline management console with live ledger, verification, collection, and latest-cycle status.
- **Automation / scripting:** run directly with flags.
- **Forced TUI:** use `--tui`.
- **GUI with progress bar:** use `--gui` on a desktop session with `DISPLAY` or `WAYLAND_DISPLAY` available.
- **Validation-only mode:** use `--check` for JSON output without writing artifacts.
- **Inspection mode:** use `--inspect` for a human-readable source summary.
- **v0.1 scaffolding mode:** use `--scaffold-v0` to generate the operator pack for district scoring, event baselines, analyst review, and pilot execution tracking.
- **Recent-accounting mode:** use `--recent-accounting` to refresh the source-recency ledger and summary in `plans/`.
- **Verification-sprint mode:** use `--verification-sprint` to merge `plans/source_verification_findings.csv` plus normalized collection-derived verification updates into the recency ledger, refresh the rolling source-verification tracker, and sync derived `VER-*` queue rows.
- **Collection scaffolding mode:** use `--scaffold-collection` to generate the collection pipeline pack in `artifacts/collection/`.
- **Collection execution mode:** use `--collect-ready` to execute ready direct-source runs, including HDX metadata collectors, and stage query, browser-export, and manual-capture request specs.
- **Zone briefing mode:** use `--brief-zone` to generate a zone-level public-source briefing pack in `artifacts/briefings/`.
- **Operating-cycle mode:** use `--operating-cycle` to run collection, verification, and briefing through the dated wrapper from inside `bootstrap.py`; wrapper flags are also exposed as `--cycle-root`, `--resume-cycle-dir`, `--resume-latest`, `--dry-run-cycle`, and `--cycle-dashboard`.

## Quick Start

```bash
# Launch the TUI-based bootstrap/launcher
python bootstrap.py
```

Inside the console you can:
- review current recent-accounting, verification, and collection status
- run the full operating cycle as one action
- preflight write actions before execution
- edit seed, plans, collection, briefing, and zone settings without leaving the console

```bash
# Launch the GUI bootstrap window with progress tracking
python bootstrap.py --gui
```

```bash
# Validate the seed without writing artifacts
python bootstrap.py --check --verbose
```

```bash
# Print a human-readable source summary
python bootstrap.py --inspect
```

```bash
# Run directly in CLI mode with verbose logs and a pinned version
python bootstrap.py --verbose --force-version 1
```

```bash
# Generate the v0.1 operator pack
python bootstrap.py --scaffold-v0
```

```bash
# Refresh the recent-accounting ledger in plans/
python bootstrap.py --recent-accounting
```

```bash
# Refresh the rolling source-verification sprint in plans/
python bootstrap.py --verification-sprint
```

```bash
# Generate the collection pipeline pack
python bootstrap.py --scaffold-collection
```

```bash
# Execute ready collection runs
python bootstrap.py --collect-ready
```

```bash
# Generate the default Cairo/Giza zone briefing pack
python bootstrap.py --brief-zone
```

```bash
# Run the dated operating cycle wrapper from bootstrap.py
python bootstrap.py --operating-cycle
```

```bash
# Run the full operating cycle with dated logs
python scripts/run_operating_cycle.py
```

```bash
# Resume the latest interrupted cycle
python scripts/run_operating_cycle.py --resume-latest
```

```bash
# Run the dated wrapper from bootstrap.py with resume support
python bootstrap.py --operating-cycle --resume-latest
```

## Handling Staged External Runs

When `--collect-ready` or the operating-cycle wrapper leaves rows in `staged_external`, use this sequence:

1. Skim `plans/work_queue.md` first for the current active queue summary.
2. Check the matching `EXT-*` row in `plans/work_queue.csv` to see whether the external step is pending or blocked, plus the staged `source_spec_path`, `request_method`, connector state, and query context.
3. Open the staged normalized contract referenced by the `EXT-*` row in `artifacts/collection/normalized/<source_id>.json` when you need the full payload.
4. Check `plans/connector_readiness.csv` to confirm the synced connector state, credential posture, and next action.
5. Open the staged raw spec in `artifacts/collection/raw/<source_id>/run-*.*` when you need the exact request payload or capture surface.
6. Follow the `execution_contract`, `connector_next_action`, and any linked `plans/source_specs/*.json` file.
7. Capture the external result into the staged raw/normalized artifact path expected by that source.
8. Rerun `python bootstrap.py --verification-sprint`.
9. That refresh also updates recent accounting, finalizes completed external captures in the collection logs, and removes resolved `EXT-*` rows.

`plans/collection_runbook.md` is the detailed operator runbook for this handoff.

## Output Contract

After a successful bootstrap, the launcher writes:
- `artifacts/bootstrap/preseed_sources_vN.json`
- `artifacts/bootstrap/preseed_sources_vN.csv`
- `docs/source-registry.csv`

After a successful v0.1 scaffold run, the launcher writes:
- `artifacts/v0_1/source-owner-assignments.csv`
- `artifacts/v0_1/district-watchlist.csv`
- `artifacts/v0_1/event-timeline.csv`
- `artifacts/v0_1/anomaly-review-worksheet.csv`
- `artifacts/v0_1/pilot-execution-summary.md`

After a successful recent-accounting refresh, the launcher writes:
- `plans/recent_accounting.csv`
- `plans/recent_accounting.md`
- `plans/source_verification_findings.csv` if it does not already exist

After a successful verification-sprint refresh, the launcher writes or updates:
- `plans/source_verification_sprint.csv`
- `plans/source_verification_sprint.md`
- `plans/work_queue.csv`
- `plans/work_queue.md`
- `plans/recent_accounting.csv`
- `plans/recent_accounting.md`
- `plans/source_verification_findings.csv`

After a successful collection scaffold run, the launcher writes:
- `artifacts/collection/source-adapter-registry.csv`
- `artifacts/collection/collection-run-manifest.csv`
- `artifacts/collection/district-collection-plan.csv`
- `artifacts/collection/places-query-seeds.csv`
- `artifacts/collection/overpass-query-seeds.csv`
- `artifacts/collection/evidence-capture-log.csv`
- `artifacts/collection/collection-pipeline-summary.md`

After a successful collection execution run, the launcher updates:
- `artifacts/collection/collection-run-manifest.csv`
- `artifacts/collection/evidence-capture-log.csv`
- `artifacts/collection/collection-run-results.csv`
- `artifacts/collection/raw/`
- `artifacts/collection/normalized/`

After a successful zone briefing run, the launcher writes a zone-specific folder under `artifacts/briefings/` containing:
- `zone_brief.md`
- `source_observation_log.csv`
- `aggregated_signals.csv`
- `event_baseline.csv`
- `anomaly_cards.csv`
- `claim_register.csv`
- `evidence_index.csv`
- `review_decision.md`
- `zone_evidence_pack.json`

## Repo Hygiene

- `.gitignore` now ignores Python cache artifacts.
- `tests/test_bootstrap.py` covers validation, source-summary generation, check-only mode, and bootstrap artifact writing.
- `tests/test_bootstrap.py` also covers v0.1 operator-pack generation.
- The runtime is no longer Python-stdlib only: `bootstrap.py` uses `requests`, and `scripts/dashboard.py` uses `rich`.
- `python -m unittest -v` is the canonical test entry point.

## Repository Layout

- `bootstrap.py` — GUI/TUI/CLI bootstrap launcher with progress reporting, validation, and inspection flows.
- `seed/preseed_sources_v1.json` — canonical preseeded source stack, now expanded to 34 sources.
- `artifacts/bootstrap/` — versioned generated JSON and CSV artifacts.
- `artifacts/v0_1/` — generated operator pack for source ownership, district scoring, event baselines, anomaly review, and pilot execution.
- `artifacts/collection/` — generated collection pipeline pack, query seeds, and evidence-capture starter logs.
- `artifacts/briefings/` — generated zone briefing packs and evidence dossiers.
- `artifacts/operating-cycles/` — generated dated cycle manifests and logs for end-to-end collection, verification, and briefing runs.
- `plans/` — agent contracts, work queue, freshness policy, evidence templates, and generated recent-accounting artifacts.
- `scripts/run_operating_cycle.py` — wrapper that runs collection, verification, and briefing in one dated cycle.
- `docs/unified-operational-plan.md` — consolidated operator-facing plan.
- `docs/operating-cycle-wrapper.md` — operator notes for the cycle wrapper.
- `docs/source-registry.csv` — repo-tracked CSV export of the current seeded registry.
- `docs/public-source-implementation-plan.md` — full implementation plan.
- `docs/confidence-rubric.md` — anomaly scoring model.
- `docs/district-selection-matrix.md` — monitoring/control district selection method.
- `docs/v0.1-execution-plan.md` — phased execution plan with acceptance criteria.
- `tests/test_bootstrap.py` — stdlib test coverage for launcher behaviors.
- `tests/test_evidence_loop.py` — fixture coverage for briefing/evidence-loop ingestion.

## Test Command

```bash
python -m unittest -v
```
