# Evidence Loop Fixtures

These fixtures mirror normalized collection outputs and the downstream briefing objects they should populate.

## Files
- `source_pull_egypt_unhcr.json`
- `source_observation_egypt.csv`
- `event_baseline_egypt.csv`
- `anomaly_card_egypt.csv`
- `claim_record_egypt.csv`

## Intended Use
- Validate that normalized collection output has enough structure to populate an anomaly card.
- Validate that a claim record can be traced back to observation and baseline IDs.
- Keep the fixture data small and explicit so it can be reused as a smoke-test corpus.
