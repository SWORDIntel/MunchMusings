import argparse
import json
import tempfile
import unittest
from pathlib import Path

import bootstrap


class BootstrapTests(unittest.TestCase):
    def setUp(self) -> None:
        self.seed_path = Path('seed/preseed_sources_v1.json')
        self.records = json.loads(self.seed_path.read_text())

    def test_validate_seed_accepts_canonical_seed(self) -> None:
        bootstrap.validate_seed(self.records)

    def test_source_summary_counts_total_sources(self) -> None:
        summary = bootstrap.build_source_summary(self.records)
        self.assertEqual(summary['total_sources'], 20)
        self.assertIn('humanitarian', summary['family_counts'])
        self.assertIn('tier1', summary['tier_counts'])

    def test_check_action_returns_summary_without_writes(self) -> None:
        args = argparse.Namespace(
            input=str(self.seed_path),
            output_dir='artifacts/bootstrap',
            docs_csv='docs/source-registry.csv',
            version_prefix='preseed_sources_v',
            force_version=1,
            verbose=False,
            launcher_mode='cli',
        )
        result = bootstrap.execute_action(args, action='check')
        self.assertEqual(result['status'], 'ok')
        self.assertEqual(result['action'], 'check')
        self.assertEqual(result['source_summary']['total_sources'], 20)

    def test_bootstrap_action_writes_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            docs_csv = tmp_path / 'source-registry.csv'
            args = argparse.Namespace(
                input=str(self.seed_path),
                output_dir=str(tmp_path / 'artifacts'),
                docs_csv=str(docs_csv),
                version_prefix='preseed_sources_v',
                force_version=1,
                verbose=False,
                launcher_mode='cli',
            )
            result = bootstrap.execute_action(args, action='bootstrap')
            self.assertEqual(result['status'], 'ok')
            self.assertTrue(Path(result['generated_json']).exists())
            self.assertTrue(Path(result['generated_csv']).exists())
            self.assertTrue(docs_csv.exists())


if __name__ == '__main__':
    unittest.main()
