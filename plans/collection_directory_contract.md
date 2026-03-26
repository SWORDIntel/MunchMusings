# Collection Directory Contract

## Root
- `artifacts/collection/`

## Required Subdirectories
- `raw/`
- `normalized/`

## Required Artifacts
- `source-adapter-registry.csv`
- `collection-run-manifest.csv`
- `district-collection-plan.csv`
- `places-query-seeds.csv`
- `overpass-query-seeds.csv`
- `evidence-capture-log.csv`
- `collection-pipeline-summary.md`

## Raw Layer
- One directory per source ID.
- Store immutable captures only.
- Preserve the original file name whenever possible.

## Normalized Layer
- One normalized artifact per source ID or observation family.
- Use stable IDs and UTC timestamps.
- Avoid lossy field reduction when a source provides structured metadata.

## Evidence Layer
- Evidence rows belong in `evidence-capture-log.csv`.
- Each evidence row should point back to raw and normalized paths.
- Each evidence row should be traceable to a run manifest row.

## Naming Rules
- Use lowercase ASCII names.
- Prefer `seed-XX` source IDs.
- Use `YYYY-MM-DDTHH:MM:SSZ` for timestamps.
- Do not store hidden operator notes in raw filenames.
