# Evidence Loop Crosswalk

## Purpose
Map normalized collection outputs into briefing-grade anomaly cards and claim records without inventing extra structure.

## Flow
1. `source_pull` captures the raw verified state of a source and its freshness metadata.
2. `source_observation` converts that source into a zone-specific observation.
3. `event_baseline` anchors timing and geography.
4. `anomaly_card` combines the observation, baseline, and confounds into a scored signal.
5. `claim_record` converts the anomaly into the final review object.

## Minimum Auto-Population Rules
- If a normalized `source_pull` row lacks `source_id`, `captured_utc`, `source_url`, or `capture_status`, do not promote it.
- If a `source_observation` row lacks `source_id`, `zone_name`, `capture_utc`, or `normalized_summary`, do not link it to an anomaly.
- If an `anomaly_card` row lacks `nearest_baseline_event`, `publication_label`, or `confound_notes`, keep it in draft.
- If a `claim_record` row lacks `evidence_links` or `decision_label`, keep it in review state.

## Recommended Fixture Set
- One `source_pull` record for a humanitarian baseline source.
- One `source_observation` record for a direct source-health observation.
- One `source_observation` record for a place or market-proxy observation.
- One `event_baseline` record for the Egypt arrival anchor.
- One `anomaly_card` record that remains `Unconfirmed` until the market-proxy layer is scored.
- One `claim_record` record that is ready for review but not publication.

## Usage Rule
The first auto-populated briefing should prefer traceability over completeness. A small, consistent chain is better than a broad chain full of guessed fields.
