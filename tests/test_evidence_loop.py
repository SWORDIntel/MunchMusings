import csv
import json
import unittest
from pathlib import Path


FIXTURE_DIR = Path('tests/fixtures/evidence_loop')


class EvidenceLoopFixtureTests(unittest.TestCase):
    def test_source_pull_fixture_has_required_fields(self) -> None:
        payload = json.loads((FIXTURE_DIR / 'source_pull_egypt_unhcr.json').read_text())
        required = {
            'source_id',
            'source_name',
            'captured_utc',
            'published_date',
            'latest_period_covered',
            'source_url',
            'raw_path',
            'checksum_sha256',
            'capture_status',
        }
        self.assertTrue(required.issubset(payload.keys()))
        self.assertEqual(payload['source_id'], 'seed-01')
        self.assertEqual(payload['capture_status'], 'completed')

    def test_source_observation_fixture_can_populate_anomaly_card(self) -> None:
        with (FIXTURE_DIR / 'source_observation_egypt.csv').open(newline='') as handle:
            rows = list(csv.DictReader(handle))
        self.assertEqual(len(rows), 2)
        self.assertTrue(all(row['source_id'] for row in rows))
        self.assertTrue(all(row['zone_name'] == 'Cairo/Giza pilot' for row in rows))
        self.assertTrue(any(row['signal_direction'] == 'upward' for row in rows))

    def test_anomaly_and_claim_fixtures_link_back_to_baseline(self) -> None:
        with (FIXTURE_DIR / 'anomaly_card_egypt.csv').open(newline='') as anomaly_handle:
            anomaly_rows = list(csv.DictReader(anomaly_handle))
        with (FIXTURE_DIR / 'claim_record_egypt.csv').open(newline='') as claim_handle:
            claim_rows = list(csv.DictReader(claim_handle))
        with (FIXTURE_DIR / 'event_baseline_egypt.csv').open(newline='') as baseline_handle:
            baseline_rows = list(csv.DictReader(baseline_handle))

        self.assertEqual(len(anomaly_rows), 1)
        self.assertEqual(len(claim_rows), 1)
        self.assertEqual(len(baseline_rows), 1)
        self.assertEqual(anomaly_rows[0]['nearest_baseline_event'], baseline_rows[0]['event_id'])
        self.assertEqual(claim_rows[0]['publication_label'], 'Unconfirmed')
        self.assertIn('defer', claim_rows[0]['decision_label'])


if __name__ == '__main__':
    unittest.main()
