# Operating Cycle Wrapper

This repo now has a single repeatable wrapper for the main operating loop:

```bash
python scripts/run_operating_cycle.py
```

The wrapper runs these steps in order:

1. `python bootstrap.py --collect-ready`
2. `python bootstrap.py --verification-sprint`
3. `python bootstrap.py --brief-zone`

It writes a dated cycle folder under `artifacts/operating-cycles/` with two outputs:

1. `run.log`
2. `run-manifest.json`

Why this exists:

- It keeps the collection, verification, and briefing sequence consistent.
- It gives each run its own dated audit trail.
- It avoids overwriting older cycle logs when the workflow is rerun.

What to expect:

- A successful cycle can still leave some rows in `staged_external`.
- Those rows are operator handoffs for query execution, manual capture, browser export, or credential-gated collection.
- The cycle is only considered collector-broken when rows fail unexpectedly, not when they stage honestly.

When a cycle leaves staged work:

1. Start with the matching `EXT-*` row in `plans/work_queue.csv`.
2. Inspect the normalized staged contract in `artifacts/collection/normalized/<source_id>.json`.
3. Check `plans/connector_readiness.csv` for the synced connector state and next action.
4. Inspect the staged raw spec in `artifacts/collection/raw/<source_id>/run-*.json` when you need the exact request payload or capture surface.
5. Follow any linked `plans/source_specs/*.json` execution contract.
6. After the external step is completed, rerun `python bootstrap.py --verification-sprint`.
7. That refresh also updates recent accounting, finalizes completed external captures in the collection logs, and removes resolved `EXT-*` rows.

`plans/collection_runbook.md` is the operator-facing detailed runbook for this process.

Common overrides:

```bash
python scripts/run_operating_cycle.py --max-runs 10
python scripts/run_operating_cycle.py --zone-name "Cairo/Giza pilot" --zone-country Egypt
python scripts/run_operating_cycle.py --python python3
```

Dry-run mode is available when you only want the manifest skeleton and command list:

```bash
python scripts/run_operating_cycle.py --dry-run
```
