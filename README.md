# MunchMusings

MunchMusings is a planning-first, public-source intelligence workspace for detecting migration-linked food-market signals across Egypt, the UAE, Saudi Arabia, Lebanon/Syria, and Gaza/OPT.

## Start Here

If you only read one planning document, read:
- `docs/unified-operational-plan.md` — single consolidated plan covering the bootstrap launcher, top-20 source stack, compliant collection rules, scoring model, district-selection method, analyst workflow, automation path, and v0.1 execution sequence.

## Single Entry Point

```bash
python bootstrap.py
```

### Launcher behavior
- **Interactive terminal, no extra args:** launches the TUI-style menu.
- **Automation / scripting:** run directly with flags.
- **Forced TUI:** use `--tui`.
- **GUI with progress bar:** use `--gui` on a desktop session with `DISPLAY` or `WAYLAND_DISPLAY` available.
- **Validation-only mode:** use `--check` for JSON output without writing artifacts.
- **Inspection mode:** use `--inspect` for a human-readable source summary.

## Quick Start

```bash
# Launch the TUI-based bootstrap/launcher
python bootstrap.py
```

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

## Output Contract

After a successful bootstrap, the launcher writes:
- `artifacts/bootstrap/preseed_sources_vN.json`
- `artifacts/bootstrap/preseed_sources_vN.csv`
- `docs/source-registry.csv`

## Repo Hygiene

- `.gitignore` now ignores Python cache artifacts.
- `tests/test_bootstrap.py` covers validation, source-summary generation, check-only mode, and bootstrap artifact writing.
- The launcher remains Python-stdlib only.

## Repository Layout

- `bootstrap.py` — GUI/TUI/CLI bootstrap launcher with progress reporting, validation, and inspection flows.
- `seed/preseed_sources_v1.json` — canonical preseeded top-20 source stack.
- `artifacts/bootstrap/` — versioned generated JSON and CSV artifacts.
- `docs/unified-operational-plan.md` — consolidated operator-facing plan.
- `docs/source-registry.csv` — repo-tracked CSV export of the current seeded registry.
- `docs/public-source-implementation-plan.md` — full implementation plan.
- `docs/confidence-rubric.md` — anomaly scoring model.
- `docs/district-selection-matrix.md` — monitoring/control district selection method.
- `docs/v0.1-execution-plan.md` — phased execution plan with acceptance criteria.
- `tests/test_bootstrap.py` — stdlib test coverage for launcher behaviors.
