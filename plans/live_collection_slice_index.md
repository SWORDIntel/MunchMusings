# Live Collection Slice Index

_Purpose: define the first live collector slice for Cairo/Giza baseline and market-proxy collection._

## Scope
- `seed-01` UNHCR Egypt data portal
- `seed-05` OCHA OPT Gaza updates
- `seed-11` Google Places API
- `seed-12` OpenStreetMap Overpass

## Files
- `plans/source_specs/unhcr_egypt_collection.json`
- `plans/source_specs/ocha_gaza_collection.json`
- `plans/source_specs/google_places_cairo_giza_collection.json`
- `plans/source_specs/overpass_cairo_giza_collection.json`
- `artifacts/collection/live_collection_slice.md`

## Operating Rule
- Use these specs as the source of truth for request shape, extraction targets, and normalization expectations.
- Do not broaden the slice until the first four source contracts have been exercised and their outputs normalized.
