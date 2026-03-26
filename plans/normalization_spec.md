# Normalization Spec

## Purpose
Define the shared record shape for turning raw public-source captures into reusable evidence.

## Record Families

### `source_pull`
- `source_id`
- `source_name`
- `captured_utc`
- `published_date`
- `latest_period_covered`
- `source_url`
- `raw_path`
- `checksum_sha256`
- `capture_status`

### `source_observation`
- `observation_id`
- `source_id`
- `zone_name`
- `district_or_neighborhood`
- `observation_type`
- `capture_utc`
- `signal_direction`
- `normalized_summary`
- `confidence`
- `evidence_path`

### `event_baseline`
- `event_id`
- `event_date`
- `event_type`
- `zone_name`
- `country`
- `geography`
- `source_ref`
- `brief_use`

### `anomaly_card`
- `anomaly_id`
- `pack_id`
- `zone_name`
- `district`
- `signal_type`
- `nearest_baseline_event`
- `score`
- `publication_label`
- `confound_notes`

### `claim_record`
- `claim_id`
- `pack_id`
- `zone_name`
- `claim_text`
- `claim_type`
- `basis_type`
- `evidence_links`
- `decision_label`
- `publication_label`

## Normalization Rules
- Keep raw and normalized artifacts separate.
- Preserve the source URL and access timestamp on every row.
- Use stable IDs so the same observation can be linked across reports.
- If a capture cannot be normalized cleanly, mark it `needs_review` instead of inventing structure.

## Crosswalk
- `source_pull` feeds `source_observation`.
- `source_observation` feeds `anomaly_card`.
- `event_baseline` anchors timing.
- `claim_record` is the final review object.
