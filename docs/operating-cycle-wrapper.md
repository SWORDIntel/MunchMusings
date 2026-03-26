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
