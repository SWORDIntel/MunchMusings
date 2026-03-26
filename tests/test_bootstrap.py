import argparse
import csv
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import bootstrap


class BootstrapTests(unittest.TestCase):
    def setUp(self) -> None:
        self.seed_path = Path('seed/preseed_sources_v1.json')
        self.records = json.loads(self.seed_path.read_text())

    def test_validate_seed_accepts_canonical_seed(self) -> None:
        bootstrap.validate_seed(self.records)

    def test_source_summary_counts_total_sources(self) -> None:
        summary = bootstrap.build_source_summary(self.records)
        self.assertEqual(summary['total_sources'], 34)
        self.assertIn('humanitarian', summary['family_counts'])
        self.assertIn('tier1', summary['tier_counts'])

    def test_check_action_returns_summary_without_writes(self) -> None:
        args = argparse.Namespace(
            input=str(self.seed_path),
            output_dir='artifacts/bootstrap',
            docs_csv='docs/source-registry.csv',
            pack_dir='artifacts/v0_1',
            plans_dir='plans',
            collection_dir='artifacts/collection',
            briefing_dir='artifacts/briefings',
            zone_name='Cairo/Giza pilot',
            zone_country='Egypt',
            analyst='system',
            reviewer='pending_review',
            max_runs=5,
            version_prefix='preseed_sources_v',
            force_version=1,
            verbose=False,
            launcher_mode='cli',
        )
        result = bootstrap.execute_action(args, action='check')
        self.assertEqual(result['status'], 'ok')
        self.assertEqual(result['action'], 'check')
        self.assertEqual(result['source_summary']['total_sources'], 34)

    def test_bootstrap_action_writes_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            docs_csv = tmp_path / 'source-registry.csv'
            args = argparse.Namespace(
                input=str(self.seed_path),
                output_dir=str(tmp_path / 'artifacts'),
                docs_csv=str(docs_csv),
                pack_dir=str(tmp_path / 'v0_1'),
                plans_dir=str(tmp_path / 'plans'),
                collection_dir=str(tmp_path / 'collection'),
                briefing_dir=str(tmp_path / 'briefings'),
                zone_name='Cairo/Giza pilot',
                zone_country='Egypt',
                analyst='system',
                reviewer='pending_review',
                max_runs=5,
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

    def test_scaffold_v0_action_writes_operator_pack(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            args = argparse.Namespace(
                input=str(self.seed_path),
                output_dir=str(tmp_path / 'artifacts'),
                docs_csv=str(tmp_path / 'source-registry.csv'),
                pack_dir=str(tmp_path / 'v0_1'),
                plans_dir=str(tmp_path / 'plans'),
                collection_dir=str(tmp_path / 'collection'),
                briefing_dir=str(tmp_path / 'briefings'),
                zone_name='Cairo/Giza pilot',
                zone_country='Egypt',
                analyst='system',
                reviewer='pending_review',
                max_runs=5,
                version_prefix='preseed_sources_v',
                force_version=1,
                verbose=False,
                launcher_mode='cli',
            )
            result = bootstrap.execute_action(args, action='scaffold_v0')
            self.assertEqual(result['status'], 'ok')
            self.assertTrue(Path(result['source_owner_assignments']).exists())
            self.assertTrue(Path(result['district_watchlist']).exists())
            self.assertTrue(Path(result['event_timeline']).exists())
            self.assertTrue(Path(result['anomaly_review_worksheet']).exists())
            self.assertTrue(Path(result['pilot_execution_summary']).exists())

    def test_recent_accounting_action_writes_and_preserves_ledger_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            plans_dir = tmp_path / 'plans'
            plans_dir.mkdir(parents=True, exist_ok=True)
            existing_path = plans_dir / 'recent_accounting.csv'
            existing_path.write_text(
                '\n'.join(
                    [
                        'source_id,rank,source_name,source_family,priority_tier,region_or_country,refresh_cadence,expected_recency_window,last_checked_utc,last_published_date,latest_period_covered,claim_date_utc,recency_status,owner,evidence_link,evidence_path,status,next_check_due_utc,next_action,notes,url',
                        'seed-01,1,UNHCR Egypt data portal,humanitarian,tier1,Egypt,weekly,8 days,2026-03-20T00:00:00Z,2026-03-21,2026-03,2026-03-25T00:00:00Z,current,ops-a,https://example.test/evidence,/tmp/evidence.md,in_review,2026-03-28T00:00:00Z,Verify Sudan split,Preserve this note,https://data.unhcr.org/en/country/egy',
                    ]
                )
                + '\n'
            )
            args = argparse.Namespace(
                input=str(self.seed_path),
                output_dir=str(tmp_path / 'artifacts'),
                docs_csv=str(tmp_path / 'source-registry.csv'),
                pack_dir=str(tmp_path / 'v0_1'),
                plans_dir=str(plans_dir),
                collection_dir=str(tmp_path / 'collection'),
                briefing_dir=str(tmp_path / 'briefings'),
                zone_name='Cairo/Giza pilot',
                zone_country='Egypt',
                analyst='system',
                reviewer='pending_review',
                max_runs=5,
                version_prefix='preseed_sources_v',
                force_version=1,
                verbose=False,
                launcher_mode='cli',
            )
            result = bootstrap.execute_action(args, action='recent_accounting')
            self.assertEqual(result['status'], 'ok')
            ledger_path = Path(result['recent_accounting_csv'])
            summary_path = Path(result['recent_accounting_summary'])
            self.assertTrue(ledger_path.exists())
            self.assertTrue(summary_path.exists())

            with ledger_path.open(newline='') as handle:
                rows = list(csv.DictReader(handle))

            self.assertEqual(len(rows), 30)
            self.assertEqual(rows[0]['source_id'], 'seed-01')
            self.assertEqual(rows[0]['owner'], 'ops-a')
            self.assertEqual(rows[0]['notes'], 'Preserve this note')
            self.assertEqual(rows[0]['recency_status'], 'current')
            self.assertEqual(rows[1]['recency_status'], 'unknown')

    def test_verification_sprint_action_preserves_state_and_syncs_queue(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            plans_dir = tmp_path / 'plans'
            plans_dir.mkdir(parents=True, exist_ok=True)

            (plans_dir / 'recent_accounting.csv').write_text(
                '\n'.join(
                    [
                        'source_id,rank,source_name,source_family,priority_tier,region_or_country,refresh_cadence,expected_recency_window,last_checked_utc,last_published_date,latest_period_covered,claim_date_utc,recency_status,owner,evidence_link,evidence_path,status,next_check_due_utc,next_action,notes,url',
                        'seed-01,1,UNHCR Egypt data portal,humanitarian,tier1,Egypt,weekly,8 days,2026-03-20T00:00:00Z,2026-03-21,2026-03,2026-03-25T00:00:00Z,current,ops-a,https://example.test/evidence,/tmp/evidence.md,in_review,2026-03-28T00:00:00Z,Verify Sudan split,Current baseline row,https://data.unhcr.org/en/country/egy',
                        'seed-24,24,HDX Humanitarian API,humanitarian_feed,tier1,Global,weekly,8 days,2026-03-25T00:00:00Z,,,2026-03-25T00:00:00Z,unknown,source_monitor,https://example.test/hapi,,in_review,2026-04-02T00:00:00Z,Capture dated product evidence,Existing HAPI note,https://data.humdata.org/hapi',
                    ]
                )
                + '\n'
            )
            (plans_dir / 'source_verification_sprint.csv').write_text(
                '\n'.join(
                    [
                        'sprint_id,status,priority,lane,owner,source_id,source_name,source_family,priority_tier,region_or_country,best_current_page,latest_visible_date,latest_period_covered,recency_status,evidence_link,last_checked_utc,next_action,notes',
                        'SV-024,research_complete,high,humanitarian_feed,humanitarian_feed_monitor,seed-24,HDX Humanitarian API,humanitarian_feed,tier1,Global,https://custom.example/hapi,,,unknown,https://example.test/hapi,2026-03-25T00:00:00Z,Keep this next action,Preserve this sprint note',
                    ]
                )
                + '\n'
            )
            (plans_dir / 'work_queue.csv').write_text(
                '\n'.join(
                    [
                        'task_id,status,priority,agent,region,source_id,artifact,target_date,depends_on,acceptance_criteria',
                        'ACC-001,completed,high,source_monitor,Egypt,seed-01,plans/recent_accounting.csv,2026-03-26,,Keep baseline task',
                        'ACC-900,in_progress,high,humanitarian_feed_monitor,Global,seed-24,plans/recent_accounting.csv,2026-03-26,,Duplicate verification task to remove',
                        'VER-999,pending,high,source_monitor,Global,seed-24,plans/source_verification_sprint.csv,2026-03-26,,Old verification row to replace',
                    ]
                )
                + '\n'
            )

            args = argparse.Namespace(
                input=str(self.seed_path),
                output_dir=str(tmp_path / 'artifacts'),
                docs_csv=str(tmp_path / 'source-registry.csv'),
                pack_dir=str(tmp_path / 'v0_1'),
                plans_dir=str(plans_dir),
                collection_dir=str(tmp_path / 'collection'),
                briefing_dir=str(tmp_path / 'briefings'),
                zone_name='Cairo/Giza pilot',
                zone_country='Egypt',
                analyst='system',
                reviewer='pending_review',
                max_runs=5,
                version_prefix='preseed_sources_v',
                force_version=1,
                verbose=False,
                launcher_mode='cli',
            )
            result = bootstrap.execute_action(args, action='verification_sprint')
            self.assertEqual(result['status'], 'ok')
            self.assertTrue(Path(result['verification_sprint_csv']).exists())
            self.assertTrue(Path(result['verification_sprint_summary']).exists())
            self.assertTrue(Path(result['connector_readiness_csv']).exists())
            self.assertTrue(Path(result['connector_readiness_summary']).exists())
            self.assertTrue(Path(result['work_queue_csv']).exists())

            with Path(result['verification_sprint_csv']).open(newline='') as handle:
                sprint_rows = list(csv.DictReader(handle))
            with Path(result['connector_readiness_csv']).open(newline='') as handle:
                connector_rows = list(csv.DictReader(handle))
            with Path(result['work_queue_csv']).open(newline='') as handle:
                queue_rows = list(csv.DictReader(handle))

            sprint_lookup = {row['source_id']: row for row in sprint_rows}
            connector_lookup = {row['source_id']: row for row in connector_rows}
            queue_ids = {row['task_id'] for row in queue_rows}

            self.assertNotIn('seed-01', sprint_lookup)
            self.assertNotIn('seed-11', sprint_lookup)
            self.assertNotIn('seed-12', sprint_lookup)
            self.assertEqual(sprint_lookup['seed-24']['notes'], 'Preserve this sprint note')
            self.assertEqual(sprint_lookup['seed-24']['best_current_page'], 'https://custom.example/hapi')
            self.assertEqual(connector_lookup['seed-11']['status'], 'needs_credentials')
            self.assertEqual(connector_lookup['seed-12']['status'], 'ready')
            self.assertIn('ACC-001', queue_ids)
            self.assertNotIn('ACC-900', queue_ids)
            self.assertIn('VER-001', queue_ids)
            self.assertIn('VER-003', queue_ids)

    def test_scaffold_collection_action_writes_collection_pack(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            args = argparse.Namespace(
                input=str(self.seed_path),
                output_dir=str(tmp_path / 'artifacts'),
                docs_csv=str(tmp_path / 'source-registry.csv'),
                pack_dir=str(tmp_path / 'v0_1'),
                plans_dir=str(tmp_path / 'plans'),
                collection_dir=str(tmp_path / 'collection'),
                briefing_dir=str(tmp_path / 'briefings'),
                zone_name='Cairo/Giza pilot',
                zone_country='Egypt',
                analyst='system',
                reviewer='pending_review',
                max_runs=5,
                version_prefix='preseed_sources_v',
                force_version=1,
                verbose=False,
                launcher_mode='cli',
            )
            result = bootstrap.execute_action(args, action='scaffold_collection')
            self.assertEqual(result['status'], 'ok')
            self.assertTrue(Path(result['source_adapter_registry']).exists())
            self.assertTrue(Path(result['collection_run_manifest']).exists())
            self.assertTrue(Path(result['district_collection_plan']).exists())
            self.assertTrue(Path(result['places_query_seeds']).exists())
            self.assertTrue(Path(result['overpass_query_seeds']).exists())
            self.assertTrue(Path(result['evidence_capture_log']).exists())
            self.assertTrue(Path(result['collection_pipeline_summary']).exists())

            with Path(result['source_adapter_registry']).open(newline='') as handle:
                adapter_rows = {row['source_id']: row for row in csv.DictReader(handle)}
            self.assertEqual(adapter_rows['seed-25']['adapter_type'], 'hdx_signals_story')
            self.assertEqual(adapter_rows['seed-23']['adapter_type'], 'lebanon_cas_cpi')
            self.assertEqual(adapter_rows['seed-31']['adapter_type'], 'israel_cbs_price_indices')
            self.assertEqual(adapter_rows['seed-32']['adapter_type'], 'israel_cbs_impexp_files')
            self.assertEqual(adapter_rows['seed-34']['adapter_type'], 'israel_iaa_monthly_reports')

    def test_verification_sprint_action_writes_tracker_and_queue_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            plans_dir = tmp_path / 'plans'
            args = argparse.Namespace(
                input=str(self.seed_path),
                output_dir=str(tmp_path / 'artifacts'),
                docs_csv=str(tmp_path / 'source-registry.csv'),
                pack_dir=str(tmp_path / 'v0_1'),
                plans_dir=str(plans_dir),
                collection_dir=str(tmp_path / 'collection'),
                briefing_dir=str(tmp_path / 'briefings'),
                zone_name='Cairo/Giza pilot',
                zone_country='Egypt',
                analyst='system',
                reviewer='pending_review',
                max_runs=5,
                version_prefix='preseed_sources_v',
                force_version=1,
                verbose=False,
                launcher_mode='cli',
            )
            result = bootstrap.execute_action(args, action='verification_sprint')
            self.assertEqual(result['status'], 'ok')
            self.assertTrue(Path(result['verification_sprint_csv']).exists())
            self.assertTrue(Path(result['verification_sprint_summary']).exists())
            self.assertTrue(Path(result['work_queue_csv']).exists())

            with Path(result['verification_sprint_csv']).open(newline='') as handle:
                sprint_rows = list(csv.DictReader(handle))
            with Path(result['work_queue_csv']).open(newline='') as handle:
                queue_rows = list(csv.DictReader(handle))

            sprint_ids = {row['source_id'] for row in sprint_rows}
            self.assertTrue({'seed-21', 'seed-22', 'seed-23', 'seed-24', 'seed-25', 'seed-26', 'seed-27'}.issubset(sprint_ids))
            self.assertTrue(any(row['task_id'] == 'VER-001' for row in queue_rows))
            self.assertTrue(any(row['task_id'] == 'VER-002' for row in queue_rows))
            self.assertTrue(any(row['task_id'] == 'VER-003' for row in queue_rows))
            self.assertTrue(any(row['task_id'] == 'VER-004' for row in queue_rows))

    def test_verification_sprint_action_preserves_analyst_state_and_refreshes_derived_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            plans_dir = tmp_path / 'plans'
            plans_dir.mkdir(parents=True, exist_ok=True)
            (plans_dir / 'recent_accounting.csv').write_text(
                '\n'.join(
                    [
                        'source_id,rank,source_name,source_family,priority_tier,region_or_country,refresh_cadence,expected_recency_window,last_checked_utc,last_published_date,latest_period_covered,claim_date_utc,recency_status,owner,evidence_link,evidence_path,status,next_check_due_utc,next_action,notes,url',
                        'seed-21,21,Old FAO label,macro_price,tier1,Global,monthly,35 days,2026-03-25T00:00:00Z,2026-02-12,2026-02,2026-03-25T00:00:00Z,due_now,source_monitor,https://example.test/fao,,in_review,2026-04-29T00:00:00Z,Ledger next action,Ledger note,https://www.fao.org/giews/food-prices/en/',
                    ]
                )
                + '\n'
            )
            (plans_dir / 'source_verification_sprint.csv').write_text(
                '\n'.join(
                    [
                        'sprint_id,status,priority,lane,owner,source_id,source_name,source_family,priority_tier,region_or_country,best_current_page,latest_visible_date,latest_period_covered,recency_status,evidence_link,last_checked_utc,next_action,notes',
                        'SV-021,research_complete,high,macro_price,custom_owner,seed-21,Stale name,macro_price,tier1,Global,https://custom.test/page,2024-01-01,2024-01,stale,https://custom.test/evidence,2026-03-01T00:00:00Z,Custom next action,Custom analyst note',
                    ]
                )
                + '\n'
            )

            args = argparse.Namespace(
                input=str(self.seed_path),
                output_dir=str(tmp_path / 'artifacts'),
                docs_csv=str(tmp_path / 'source-registry.csv'),
                pack_dir=str(tmp_path / 'v0_1'),
                plans_dir=str(plans_dir),
                collection_dir=str(tmp_path / 'collection'),
                briefing_dir=str(tmp_path / 'briefings'),
                zone_name='Cairo/Giza pilot',
                zone_country='Egypt',
                analyst='system',
                reviewer='pending_review',
                max_runs=5,
                version_prefix='preseed_sources_v',
                force_version=1,
                verbose=False,
                launcher_mode='cli',
            )
            result = bootstrap.execute_action(args, action='verification_sprint')
            with Path(result['verification_sprint_csv']).open(newline='') as handle:
                sprint_rows = {row['source_id']: row for row in csv.DictReader(handle)}

            row = sprint_rows['seed-21']
            self.assertEqual(row['owner'], 'custom_owner')
            self.assertEqual(row['next_action'], 'Ledger next action')
            self.assertEqual(row['notes'], 'Custom analyst note')
            self.assertEqual(row['evidence_link'], 'https://example.test/fao')
            self.assertEqual(row['source_name'], 'FAO FPMA Food Price Monitoring and Analysis')
            self.assertEqual(row['recency_status'], 'due_now')
            self.assertEqual(row['latest_period_covered'], '2026-02')

    def test_verification_sprint_action_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            plans_dir = tmp_path / 'plans'
            args = argparse.Namespace(
                input=str(self.seed_path),
                output_dir=str(tmp_path / 'artifacts'),
                docs_csv=str(tmp_path / 'source-registry.csv'),
                pack_dir=str(tmp_path / 'v0_1'),
                plans_dir=str(plans_dir),
                collection_dir=str(tmp_path / 'collection'),
                briefing_dir=str(tmp_path / 'briefings'),
                zone_name='Cairo/Giza pilot',
                zone_country='Egypt',
                analyst='system',
                reviewer='pending_review',
                max_runs=5,
                version_prefix='preseed_sources_v',
                force_version=1,
                verbose=False,
                launcher_mode='cli',
            )
            first = bootstrap.execute_action(args, action='verification_sprint')
            first_tracker = Path(first['verification_sprint_csv']).read_text()
            first_queue = Path(first['work_queue_csv']).read_text()

            second = bootstrap.execute_action(args, action='verification_sprint')
            second_tracker = Path(second['verification_sprint_csv']).read_text()
            second_queue = Path(second['work_queue_csv']).read_text()

            self.assertEqual(first_tracker, second_tracker)
            self.assertEqual(first_queue, second_queue)

    def test_verification_sprint_action_merges_findings_overlay_into_ledger(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            plans_dir = tmp_path / 'plans'
            plans_dir.mkdir(parents=True, exist_ok=True)
            (plans_dir / 'recent_accounting.csv').write_text(
                '\n'.join(
                    [
                        'source_id,rank,source_name,source_family,priority_tier,region_or_country,refresh_cadence,expected_recency_window,last_checked_utc,last_published_date,latest_period_covered,claim_date_utc,recency_status,owner,evidence_link,evidence_path,status,next_check_due_utc,next_action,notes,url',
                        'seed-25,25,HDX Signals,humanitarian_feed,tier1,Global,weekly,8 days,2026-03-25T00:00:00Z,,,,unknown,source_monitor,https://example.test/old,,in_review,2026-04-02T00:00:00Z,Old next action,Old note,https://data.humdata.org/signals',
                    ]
                )
                + '\n'
            )
            (plans_dir / 'source_verification_findings.csv').write_text(
                '\n'.join(
                    [
                        'source_id,last_checked_utc,last_published_date,latest_period_covered,claim_date_utc,owner,evidence_link,evidence_path,status,next_action,notes',
                        'seed-25,2026-03-25T09:58:17Z,2025-04-03,2025-02,,,https://centre.humdata.org/impact-story-hdx-signals-alerting-humanitarians-to-deteriorating-crises/,,in_review,Refresh against the newest dated Signals page,Impact-story evidence',
                    ]
                )
                + '\n'
            )

            args = argparse.Namespace(
                input=str(self.seed_path),
                output_dir=str(tmp_path / 'artifacts'),
                docs_csv=str(tmp_path / 'source-registry.csv'),
                pack_dir=str(tmp_path / 'v0_1'),
                plans_dir=str(plans_dir),
                collection_dir=str(tmp_path / 'collection'),
                briefing_dir=str(tmp_path / 'briefings'),
                zone_name='Cairo/Giza pilot',
                zone_country='Egypt',
                analyst='system',
                reviewer='pending_review',
                max_runs=5,
                version_prefix='preseed_sources_v',
                force_version=1,
                verbose=False,
                launcher_mode='cli',
            )

            result = bootstrap.execute_action(args, action='verification_sprint')
            with Path(result['recent_accounting_csv']).open(newline='') as handle:
                accounting_rows = {row['source_id']: row for row in csv.DictReader(handle)}
            with Path(result['verification_sprint_csv']).open(newline='') as handle:
                sprint_rows = {row['source_id']: row for row in csv.DictReader(handle)}

            self.assertEqual(accounting_rows['seed-25']['last_published_date'], '2025-04-03')
            self.assertEqual(accounting_rows['seed-25']['latest_period_covered'], '2025-02')
            self.assertEqual(
                accounting_rows['seed-25']['evidence_link'],
                'https://centre.humdata.org/impact-story-hdx-signals-alerting-humanitarians-to-deteriorating-crises/',
            )
            self.assertEqual(accounting_rows['seed-25']['notes'], 'Impact-story evidence')
            self.assertEqual(sprint_rows['seed-25']['status'], 'verified')
            self.assertEqual(sprint_rows['seed-25']['latest_visible_date'], '2025-04-03')

    def test_verification_sprint_refreshes_same_host_best_current_page(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            plans_dir = tmp_path / 'plans'
            plans_dir.mkdir(parents=True, exist_ok=True)
            (plans_dir / 'recent_accounting.csv').write_text(
                '\n'.join(
                    [
                        'source_id,rank,source_name,source_family,priority_tier,region_or_country,refresh_cadence,expected_recency_window,last_checked_utc,last_published_date,latest_period_covered,claim_date_utc,recency_status,owner,evidence_link,evidence_path,status,next_check_due_utc,next_action,notes,url',
                        'seed-07,7,IPC Lebanon analysis,humanitarian,tier1,Lebanon,periodic,manual review,2026-03-25T00:00:00Z,2025-12-23,2025-11/2026-07,2026-03-25T00:00:00Z,manual_review,source_monitor,https://www.ipcinfo.org/ipc-country-analysis/details-map/en/c/1159824/,,in_review,,Watch the updated IPC page,Existing note,https://www.ipcinfo.org/ipc-country-analysis/details-map/en/c/1159824/',
                    ]
                )
                + '\n'
            )
            (plans_dir / 'source_verification_sprint.csv').write_text(
                '\n'.join(
                    [
                        'sprint_id,status,priority,lane,owner,source_id,source_name,source_family,priority_tier,region_or_country,best_current_page,latest_visible_date,latest_period_covered,recency_status,evidence_link,last_checked_utc,next_action,notes',
                        'SV-007,research_complete,high,baseline_refresh,source_monitor,seed-07,IPC Lebanon analysis,humanitarian,tier1,Lebanon,https://www.ipcinfo.org/ipc-country-analysis/details-map/en/c/1159456/,2024-01-01,2024-01,manual_review,https://www.ipcinfo.org/ipc-country-analysis/details-map/en/c/1159456/,2026-03-01T00:00:00Z,Keep this next action,Custom analyst note',
                    ]
                )
                + '\n'
            )

            args = argparse.Namespace(
                input=str(self.seed_path),
                output_dir=str(tmp_path / 'artifacts'),
                docs_csv=str(tmp_path / 'source-registry.csv'),
                pack_dir=str(tmp_path / 'v0_1'),
                plans_dir=str(plans_dir),
                collection_dir=str(tmp_path / 'collection'),
                briefing_dir=str(tmp_path / 'briefings'),
                zone_name='Cairo/Giza pilot',
                zone_country='Egypt',
                analyst='system',
                reviewer='pending_review',
                max_runs=5,
                version_prefix='preseed_sources_v',
                force_version=1,
                verbose=False,
                launcher_mode='cli',
            )

            result = bootstrap.execute_action(args, action='verification_sprint')
            with Path(result['verification_sprint_csv']).open(newline='') as handle:
                sprint_rows = {row['source_id']: row for row in csv.DictReader(handle)}

            self.assertEqual(
                sprint_rows['seed-07']['best_current_page'],
                'https://www.ipcinfo.org/ipc-country-analysis/details-map/en/c/1159824/',
            )
            self.assertEqual(sprint_rows['seed-07']['notes'], 'Custom analyst note')

    def test_verification_sprint_refreshes_legacy_host_best_current_page(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            plans_dir = tmp_path / 'plans'
            plans_dir.mkdir(parents=True, exist_ok=True)
            (plans_dir / 'recent_accounting.csv').write_text(
                '\n'.join(
                    [
                        'source_id,rank,source_name,source_family,priority_tier,region_or_country,refresh_cadence,expected_recency_window,last_checked_utc,last_published_date,latest_period_covered,claim_date_utc,recency_status,owner,evidence_link,evidence_path,status,next_check_due_utc,next_action,notes,url',
                        'seed-03,3,UNHCR Lebanon reporting hub,humanitarian,tier1,Lebanon,weekly,8 days,2026-03-25T00:00:00Z,2026-03-16,2026-03-09/2026-03-15,2026-03-25T00:00:00Z,due_now,source_monitor,https://data.unhcr.org/en/documents/details/121604,,in_review,,Track the UNHCR country page,Existing note,https://data.unhcr.org/en/country/lbn',
                    ]
                )
                + '\n'
            )
            (plans_dir / 'source_verification_sprint.csv').write_text(
                '\n'.join(
                    [
                        'sprint_id,status,priority,lane,owner,source_id,source_name,source_family,priority_tier,region_or_country,best_current_page,latest_visible_date,latest_period_covered,recency_status,evidence_link,last_checked_utc,next_action,notes',
                        'SV-003,research_complete,high,baseline_refresh,source_monitor,seed-03,UNHCR Lebanon reporting hub,humanitarian,tier1,Lebanon,https://reporting.unhcr.org/lebanon-flash-update,2024-01-01,2024-01,unknown,https://reporting.unhcr.org/lebanon-flash-update,2026-03-01T00:00:00Z,Keep this next action,Custom analyst note',
                    ]
                )
                + '\n'
            )

            args = argparse.Namespace(
                input=str(self.seed_path),
                output_dir=str(tmp_path / 'artifacts'),
                docs_csv=str(tmp_path / 'source-registry.csv'),
                pack_dir=str(tmp_path / 'v0_1'),
                plans_dir=str(plans_dir),
                collection_dir=str(tmp_path / 'collection'),
                briefing_dir=str(tmp_path / 'briefings'),
                zone_name='Cairo/Giza pilot',
                zone_country='Egypt',
                analyst='system',
                reviewer='pending_review',
                max_runs=5,
                version_prefix='preseed_sources_v',
                force_version=1,
                verbose=False,
                launcher_mode='cli',
            )

            result = bootstrap.execute_action(args, action='verification_sprint')
            with Path(result['verification_sprint_csv']).open(newline='') as handle:
                sprint_rows = {row['source_id']: row for row in csv.DictReader(handle)}

            self.assertEqual(sprint_rows['seed-03']['best_current_page'], 'https://data.unhcr.org/en/country/lbn')
            self.assertEqual(sprint_rows['seed-03']['notes'], 'Custom analyst note')

    def test_verification_sprint_refreshes_comtrade_portal_host(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            plans_dir = tmp_path / 'plans'
            plans_dir.mkdir(parents=True, exist_ok=True)
            (plans_dir / 'recent_accounting.csv').write_text(
                '\n'.join(
                    [
                        'source_id,rank,source_name,source_family,priority_tier,region_or_country,refresh_cadence,expected_recency_window,last_checked_utc,last_published_date,latest_period_covered,claim_date_utc,recency_status,owner,evidence_link,evidence_path,status,next_check_due_utc,next_action,notes,url',
                        'seed-09,9,UN Comtrade API portal,trade,tier1,Global,monthly,35 days,2026-03-25T00:00:00Z,2026-03-25,,,current,,https://comtradeplus.un.org/api/DataAvailability/getComtradeTrend,,in_review,2026-04-29T00:00:00Z,Track the Comtrade Plus API,Existing note,https://comtradeplus.un.org/DataAvailability',
                    ]
                )
                + '\n'
            )
            (plans_dir / 'source_verification_sprint.csv').write_text(
                '\n'.join(
                    [
                        'sprint_id,status,priority,lane,owner,source_id,source_name,source_family,priority_tier,region_or_country,best_current_page,latest_visible_date,latest_period_covered,recency_status,evidence_link,last_checked_utc,next_action,notes',
                        'SV-009,research_complete,high,baseline_refresh,trade_monitor,seed-09,UN Comtrade API portal,trade,tier1,Global,https://comtrade.un.org/data/dev/portal/,2026-03-01,,unknown,https://comtrade.un.org/data/dev/portal/,2026-03-01T00:00:00Z,Keep this next action,Custom analyst note',
                    ]
                )
                + '\n'
            )

            args = argparse.Namespace(
                input=str(self.seed_path),
                output_dir=str(tmp_path / 'artifacts'),
                docs_csv=str(tmp_path / 'source-registry.csv'),
                pack_dir=str(tmp_path / 'v0_1'),
                plans_dir=str(plans_dir),
                collection_dir=str(tmp_path / 'collection'),
                briefing_dir=str(tmp_path / 'briefings'),
                zone_name='Cairo/Giza pilot',
                zone_country='Egypt',
                analyst='system',
                reviewer='pending_review',
                max_runs=5,
                version_prefix='preseed_sources_v',
                force_version=1,
                verbose=False,
                launcher_mode='cli',
            )

            result = bootstrap.execute_action(args, action='verification_sprint')
            with Path(result['verification_sprint_csv']).open(newline='') as handle:
                sprint_rows = {row['source_id']: row for row in csv.DictReader(handle)}

            self.assertEqual(sprint_rows['seed-09']['best_current_page'], 'https://comtradeplus.un.org/DataAvailability')
            self.assertEqual(sprint_rows['seed-09']['notes'], 'Custom analyst note')

    def test_build_pipeline_snapshot_reports_counts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            plans_dir = tmp_path / 'plans'
            collection_dir = tmp_path / 'collection'
            briefing_dir = tmp_path / 'briefings'
            cycle_manifest = tmp_path / 'artifacts' / 'operating-cycles' / '20260325T000000Z' / 'run-manifest.json'
            zone_brief = briefing_dir / bootstrap.zone_pack_id('Cairo/Giza pilot') / 'zone_brief.md'
            plans_dir.mkdir(parents=True, exist_ok=True)
            collection_dir.mkdir(parents=True, exist_ok=True)
            zone_brief.parent.mkdir(parents=True, exist_ok=True)
            cycle_manifest.parent.mkdir(parents=True, exist_ok=True)

            (plans_dir / 'recent_accounting.csv').write_text(
                '\n'.join(
                    [
                        'source_id,rank,source_name,source_family,priority_tier,region_or_country,refresh_cadence,expected_recency_window,last_checked_utc,last_published_date,latest_period_covered,claim_date_utc,recency_status,owner,evidence_link,evidence_path,status,next_check_due_utc,next_action,notes,url',
                        'seed-01,1,Current row,humanitarian,tier1,Egypt,weekly,8 days,,,,,current,,,,pending_review,,,,https://example.test/1',
                        'seed-02,2,Unknown row,humanitarian,tier1,Egypt,weekly,8 days,,,,,unknown,,,,pending_review,,,,https://example.test/2',
                    ]
                )
                + '\n'
            )
            (plans_dir / 'source_verification_sprint.csv').write_text(
                '\n'.join(
                    [
                        'sprint_id,status,priority,lane,owner,source_id,source_name,source_family,priority_tier,region_or_country,best_current_page,latest_visible_date,latest_period_covered,recency_status,evidence_link,last_checked_utc,next_action,notes',
                        'SV-001,verified,high,baseline_refresh,source_monitor,seed-01,Current row,humanitarian,tier1,Egypt,https://example.test/1,2026-03-01,,current,https://example.test/1,2026-03-25T00:00:00Z,,',
                        'SV-002,pending,high,baseline_refresh,source_monitor,seed-02,Unknown row,humanitarian,tier1,Egypt,https://example.test/2,,,,,https://example.test/2,,,',
                    ]
                )
                + '\n'
            )
            (collection_dir / 'collection-run-results.csv').write_text(
                '\n'.join(
                    [
                        'run_id,source_id,source_name,adapter_type,executed_utc,result_status,raw_path,normalized_path,notes',
                        'run-1,seed-01,Current row,html_snapshot,2026-03-25T00:00:00Z,completed,/tmp/raw,/tmp/normalized,ok',
                        'run-2,seed-02,Unknown row,browser_export,2026-03-25T00:00:00Z,staged_external,/tmp/raw2,/tmp/normalized2,stage',
                    ]
                )
                + '\n'
            )
            zone_brief.write_text('brief\n')
            cycle_manifest.write_text(
                json.dumps(
                    {
                        'cycle_id': '20260325T000000Z',
                        'status': 'ok',
                        'ended_at_utc': '2026-03-25T00:00:10Z',
                    }
                )
                + '\n'
            )

            args = argparse.Namespace(
                input=str(self.seed_path),
                output_dir=str(tmp_path / 'bootstrap'),
                docs_csv=str(tmp_path / 'source-registry.csv'),
                pack_dir=str(tmp_path / 'v0_1'),
                plans_dir=str(plans_dir),
                collection_dir=str(collection_dir),
                briefing_dir=str(briefing_dir),
                zone_name='Cairo/Giza pilot',
                zone_country='Egypt',
                analyst='system',
                reviewer='pending_review',
                max_runs=5,
                version_prefix='preseed_sources_v',
                force_version=1,
                verbose=False,
                launcher_mode='cli',
            )

            with patch.object(bootstrap, '__file__', str(tmp_path / 'bootstrap.py')):
                snapshot = bootstrap.build_pipeline_snapshot(args)

            self.assertEqual(snapshot['recent_counts']['current'], 1)
            self.assertEqual(snapshot['verification_counts']['verified'], 1)
            self.assertEqual(snapshot['collection_counts']['staged_external'], 1)
            self.assertEqual(snapshot['latest_cycle']['status'], 'ok')
            self.assertTrue(snapshot['latest_brief_exists'])

    def test_operating_cycle_action_reads_latest_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            (tmp_path / 'scripts').mkdir(parents=True, exist_ok=True)
            (tmp_path / 'scripts' / 'run_operating_cycle.py').write_text('# fixture\n')
            cycle_manifest = tmp_path / 'artifacts' / 'operating-cycles' / '20260325T000000Z' / 'run-manifest.json'
            cycle_manifest.parent.mkdir(parents=True, exist_ok=True)
            cycle_manifest.write_text(
                json.dumps(
                    {
                        'cycle_id': '20260325T000000Z',
                        'cycle_dir': str(cycle_manifest.parent),
                        'status': 'ok',
                        'planned_commands': ['python bootstrap.py --collect-ready'],
                        'steps': [{'command': 'python bootstrap.py --collect-ready', 'exit_code': 0}],
                    }
                )
                + '\n'
            )

            args = argparse.Namespace(
                input=str(self.seed_path),
                output_dir=str(tmp_path / 'bootstrap'),
                docs_csv=str(tmp_path / 'source-registry.csv'),
                pack_dir=str(tmp_path / 'v0_1'),
                plans_dir=str(tmp_path / 'plans'),
                collection_dir=str(tmp_path / 'collection'),
                briefing_dir=str(tmp_path / 'briefings'),
                zone_name='Cairo/Giza pilot',
                zone_country='Egypt',
                analyst='system',
                reviewer='pending_review',
                max_runs=7,
                version_prefix='preseed_sources_v',
                force_version=1,
                verbose=False,
                launcher_mode='cli',
            )

            class FakeCompletedProcess:
                def __init__(self) -> None:
                    self.returncode = 0
                    self.stdout = 'cycle ok\n'
                    self.stderr = ''

            with patch.object(bootstrap, '__file__', str(tmp_path / 'bootstrap.py')):
                with patch.object(bootstrap.subprocess, 'run', return_value=FakeCompletedProcess()) as mocked_run:
                    result = bootstrap.execute_action(args, action='operating_cycle')

            self.assertEqual(result['status'], 'ok')
            self.assertEqual(result['cycle_id'], '20260325T000000Z')
            self.assertEqual(result['cycle_status'], 'ok')
            invoked = mocked_run.call_args.args[0]
            self.assertIn('--max-runs', invoked)
            self.assertIn('7', invoked)

    def test_brief_zone_action_writes_zone_pack(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            args = argparse.Namespace(
                input=str(self.seed_path),
                output_dir=str(tmp_path / 'artifacts'),
                docs_csv=str(tmp_path / 'source-registry.csv'),
                pack_dir=str(tmp_path / 'v0_1'),
                plans_dir=str(tmp_path / 'plans'),
                collection_dir=str(tmp_path / 'collection'),
                briefing_dir=str(tmp_path / 'briefings'),
                zone_name='Cairo/Giza pilot',
                zone_country='Egypt',
                analyst='system',
                reviewer='pending_review',
                max_runs=5,
                version_prefix='preseed_sources_v',
                force_version=1,
                verbose=False,
                launcher_mode='cli',
            )
            result = bootstrap.execute_action(args, action='brief_zone')
            self.assertEqual(result['status'], 'ok')
            self.assertTrue(Path(result['zone_brief']).exists())
            self.assertTrue(Path(result['source_observation_log']).exists())
            self.assertTrue(Path(result['aggregated_signals']).exists())
            self.assertTrue(Path(result['event_baseline']).exists())
            self.assertTrue(Path(result['anomaly_cards']).exists())
            self.assertTrue(Path(result['claim_register']).exists())
            self.assertTrue(Path(result['evidence_index']).exists())
            self.assertTrue(Path(result['review_decision']).exists())
            self.assertTrue(Path(result['zone_evidence_pack']).exists())

    def test_collect_ready_action_fetches_local_file_source(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            collection_dir = tmp_path / 'collection'
            raw_dir = collection_dir / 'raw' / 'seed-01'
            normalized_dir = collection_dir / 'normalized'
            collection_dir.mkdir(parents=True, exist_ok=True)
            raw_dir.mkdir(parents=True, exist_ok=True)
            normalized_dir.mkdir(parents=True, exist_ok=True)

            local_source = tmp_path / 'source.html'
            local_source.write_text('<html><body>baseline</body></html>\n')

            (collection_dir / 'source-adapter-registry.csv').write_text(
                '\n'.join(
                    [
                        'source_id,rank,source_name,source_family,priority_tier,collection_stage,collection_mode,adapter_type,access_type,region_or_country,refresh_cadence,raw_landing_dir,normalized_output_path,evidence_log_path,query_seed_file,notes,url',
                        f'seed-01,1,Local baseline,humanitarian,tier1,baseline,automatable,html_snapshot,html,Egypt,weekly,{raw_dir},{normalized_dir / "seed-01.json"},{collection_dir / "evidence-capture-log.csv"},,Local file adapter,{local_source.as_uri()}',
                    ]
                )
                + '\n'
            )
            (collection_dir / 'collection-run-manifest.csv').write_text(
                '\n'.join(
                    [
                        'run_id,source_id,source_name,collection_stage,adapter_type,priority,district_scope,scheduled_run_utc,expected_artifact,query_seed_file,status,failure_action,notes',
                        f'run-seed-01,seed-01,Local baseline,baseline,html_snapshot,high,Egypt,2026-03-25T00:00:00Z,{normalized_dir / "seed-01.json"},,ready,log_and_escalate,Test run',
                    ]
                )
                + '\n'
            )
            args = argparse.Namespace(
                input=str(self.seed_path),
                output_dir=str(tmp_path / 'artifacts'),
                docs_csv=str(tmp_path / 'source-registry.csv'),
                pack_dir=str(tmp_path / 'v0_1'),
                plans_dir=str(tmp_path / 'plans'),
                collection_dir=str(collection_dir),
                briefing_dir=str(tmp_path / 'briefings'),
                zone_name='Cairo/Giza pilot',
                zone_country='Egypt',
                analyst='system',
                reviewer='pending_review',
                max_runs=5,
                version_prefix='preseed_sources_v',
                force_version=1,
                verbose=False,
                launcher_mode='cli',
            )
            result = bootstrap.execute_action(args, action='collect_ready')
            self.assertEqual(result['status'], 'ok')
            self.assertEqual(result['processed_runs'], 1)
            self.assertEqual(result['completed_runs'], 1)
            self.assertTrue(Path(result['collection_run_results']).exists())
            self.assertTrue((normalized_dir / 'seed-01.json').exists())

    def test_collect_ready_action_extracts_unhcr_document_index_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            collection_dir = tmp_path / 'collection'
            raw_dir = collection_dir / 'raw' / 'seed-03'
            normalized_dir = collection_dir / 'normalized'
            collection_dir.mkdir(parents=True, exist_ok=True)
            raw_dir.mkdir(parents=True, exist_ok=True)
            normalized_dir.mkdir(parents=True, exist_ok=True)

            index_html = (
                '<html><body>'
                '<li class="searchResultItem -_doc media -table">'
                '<div class="searchResultItem_content media_body">'
                '<h2 class="searchResultItem_title"><a target="_blank" href="https://data.unhcr.org/en/documents/details/121604">'
                'Middle East Situation Lebanon - Flash Update #2</a></h2>'
                '<div class="searchResultItem_download"><a class="searchResultItem_download_link" href="https://data.unhcr.org/en/documents/download/121604" data-title="Middle East Situation Lebanon - Flash Update #2">Download</a></div>'
                '<div class="searchResultItem_body">Middle East Situation Lebanon - Flash Update #2</div>'
                '<span class="searchResultItem_date">Publish date: <b>16 March 2026</b><br>Create date: <b>16 March 2026</b></span>'
                '</div></li>'
                '</body></html>'
            )
            detail_html = '<html><body><p>Upload date: 16 March 2026</p><p>Issue period: 9-15 March 2026</p></body></html>'

            def fake_fetch(url: str, raw_path: Path) -> dict[str, object]:
                if url == 'https://data.unhcr.org/en/country/lbn':
                    body = index_html.encode('utf-8')
                elif url == 'https://data.unhcr.org/en/documents/details/121604':
                    body = detail_html.encode('utf-8')
                else:
                    raise AssertionError(f'unexpected url: {url}')
                raw_path.parent.mkdir(parents=True, exist_ok=True)
                raw_path.write_bytes(body)
                return {
                    'status_code': 200,
                    'content_type': 'text/html; charset=UTF-8',
                    'bytes_written': len(body),
                    'checksum_sha256': 'fixture',
                }

            (collection_dir / 'source-adapter-registry.csv').write_text(
                '\n'.join(
                    [
                        'source_id,rank,source_name,source_family,priority_tier,collection_stage,collection_mode,adapter_type,access_type,region_or_country,refresh_cadence,raw_landing_dir,normalized_output_path,evidence_log_path,query_seed_file,notes,url',
                        f'seed-03,3,UNHCR Lebanon reporting hub,humanitarian,tier1,baseline,automatable,unhcr_document_index,html/pdf,Lebanon,weekly,{raw_dir},{normalized_dir / "seed-03.json"},{collection_dir / "evidence-capture-log.csv"},,UNHCR index adapter,https://data.unhcr.org/en/country/lbn',
                    ]
                )
                + '\n'
            )
            (collection_dir / 'collection-run-manifest.csv').write_text(
                '\n'.join(
                    [
                        'run_id,source_id,source_name,collection_stage,adapter_type,priority,district_scope,scheduled_run_utc,expected_artifact,query_seed_file,status,failure_action,notes',
                        f'run-seed-03,seed-03,UNHCR Lebanon reporting hub,baseline,unhcr_document_index,high,Lebanon,2026-03-25T00:00:00Z,{normalized_dir / "seed-03.json"},,ready,log_and_escalate,UNHCR test run',
                    ]
                )
                + '\n'
            )
            args = argparse.Namespace(
                input=str(self.seed_path),
                output_dir=str(tmp_path / 'artifacts'),
                docs_csv=str(tmp_path / 'source-registry.csv'),
                pack_dir=str(tmp_path / 'v0_1'),
                plans_dir=str(tmp_path / 'plans'),
                collection_dir=str(collection_dir),
                briefing_dir=str(tmp_path / 'briefings'),
                zone_name='Cairo/Giza pilot',
                zone_country='Egypt',
                analyst='system',
                reviewer='pending_review',
                max_runs=5,
                version_prefix='preseed_sources_v',
                force_version=1,
                verbose=False,
                launcher_mode='cli',
            )

            with patch.object(bootstrap, 'fetch_direct_source', side_effect=fake_fetch):
                result = bootstrap.execute_action(args, action='collect_ready')

            self.assertEqual(result['status'], 'ok')
            payload = json.loads((normalized_dir / 'seed-03.json').read_text())
            self.assertEqual(payload['adapter_type'], 'unhcr_document_index')
            self.assertEqual(payload['verification_updates']['last_published_date'], '2026-03-16')
            self.assertEqual(payload['verification_updates']['latest_period_covered'], '2026-03-09/2026-03-15')
            self.assertEqual(payload['verification_updates']['evidence_link'], 'https://data.unhcr.org/en/documents/details/121604')

    def test_collect_ready_action_extracts_ipc_lebanon_analysis_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            collection_dir = tmp_path / 'collection'
            raw_dir = collection_dir / 'raw' / 'seed-07'
            normalized_dir = collection_dir / 'normalized'
            collection_dir.mkdir(parents=True, exist_ok=True)
            raw_dir.mkdir(parents=True, exist_ok=True)
            normalized_dir.mkdir(parents=True, exist_ok=True)

            page_html = (
                '<html><head><title>IPC Lebanon Analysis</title></head><body>'
                'RELEASE DATE 23.12.2025<br />'
                'VALIDITY PERIOD 01.11.2025 - 31.07.2026<br />'
                '<a href="/IPC_Lebanon_Acute_Food_Insecurity_latest.pdf">PDF</a>'
                '</body></html>'
            )
            raw_path = raw_dir / 'run-seed-07.html'
            raw_path.write_text(page_html)

            (collection_dir / 'source-adapter-registry.csv').write_text(
                '\n'.join(
                    [
                        'source_id,rank,source_name,source_family,priority_tier,collection_stage,collection_mode,adapter_type,access_type,region_or_country,refresh_cadence,raw_landing_dir,normalized_output_path,evidence_log_path,query_seed_file,notes,url',
                        f"seed-07,7,IPC Lebanon analysis,humanitarian,tier1,baseline,automatable,ipc_lebanon_analysis,html,Lebanon,periodic,{raw_dir},{normalized_dir / 'seed-07.json'},{collection_dir / 'evidence-capture-log.csv'},,IPC Lebanon analysis adapter,https://example.test/ipc-analysis",
                    ]
                )
                + '\n'
            )
            (collection_dir / 'collection-run-manifest.csv').write_text(
                '\n'.join(
                    [
                        'run_id,source_id,source_name,collection_stage,adapter_type,priority,district_scope,scheduled_run_utc,expected_artifact,query_seed_file,status,failure_action,notes',
                        f"run-seed-07,seed-07,IPC Lebanon analysis,baseline,ipc_lebanon_analysis,high,Lebanon,2026-03-26T00:00:00Z,{normalized_dir / 'seed-07.json'},,ready,log_and_escalate,IPC Lebanon analysis test run",
                    ]
                )
                + '\n'
            )
            args = argparse.Namespace(
                input=str(self.seed_path),
                output_dir=str(tmp_path / 'artifacts'),
                docs_csv=str(tmp_path / 'source-registry.csv'),
                pack_dir=str(tmp_path / 'v0_1'),
                plans_dir=str(tmp_path / 'plans'),
                collection_dir=str(collection_dir),
                briefing_dir=str(tmp_path / 'briefings'),
                zone_name='Cairo/Giza pilot',
                zone_country='Egypt',
                analyst='system',
                reviewer='pending_review',
                max_runs=5,
                version_prefix='preseed_sources_v',
                force_version=1,
                verbose=False,
                launcher_mode='cli',
            )
            result = None
            def fake_fetch(url, raw_path):
                return {
                    'status_code': 200,
                    'content_type': 'text/html',
                    'bytes_written': len(page_html),
                    'checksum_sha256': 'fixture',
                }

            with patch.object(bootstrap, 'fetch_direct_source', side_effect=fake_fetch):
                result = bootstrap.execute_action(args, action='collect_ready')
            self.assertEqual(result['status'], 'ok')
            payload = json.loads((normalized_dir / 'seed-07.json').read_text())
            self.assertEqual(payload['adapter_type'], 'ipc_lebanon_analysis')
            self.assertEqual(payload['verification_updates']['last_published_date'], '2025-12-23')
            self.assertEqual(payload['verification_updates']['latest_period_covered'], '2025-11/2026-07')

    def test_collect_ready_action_extracts_ipc_gaza_snapshot_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            collection_dir = tmp_path / 'collection'
            raw_dir = collection_dir / 'raw' / 'seed-06'
            normalized_dir = collection_dir / 'normalized'
            collection_dir.mkdir(parents=True, exist_ok=True)
            raw_dir.mkdir(parents=True, exist_ok=True)
            normalized_dir.mkdir(parents=True, exist_ok=True)

            fixture = Path('tests/fixtures/ipc-gaza-snapshot.html')
            raw_path = raw_dir / 'run-seed-06.html'
            raw_path.write_bytes(fixture.read_bytes())

            (collection_dir / 'source-adapter-registry.csv').write_text(
                '\n'.join(
                    [
                        'source_id,rank,source_name,source_family,priority_tier,collection_stage,collection_mode,adapter_type,access_type,region_or_country,refresh_cadence,raw_landing_dir,normalized_output_path,evidence_log_path,query_seed_file,notes,url',
                        f"seed-06,6,IPC Gaza snapshot,humanitarian,tier1,baseline,automatable,ipc_gaza_snapshot,html,Gaza/OPT,periodic,{raw_dir},{normalized_dir / 'seed-06.json'},{collection_dir / 'evidence-capture-log.csv'},,IPC Gaza snapshot adapter,{raw_path.as_uri()}",
                    ]
                )
                + '\n'
            )
            (collection_dir / 'collection-run-manifest.csv').write_text(
                '\n'.join(
                    [
                        'run_id,source_id,source_name,collection_stage,adapter_type,priority,district_scope,scheduled_run_utc,expected_artifact,query_seed_file,status,failure_action,notes',
                        f"run-seed-06,seed-06,IPC Gaza snapshot,baseline,ipc_gaza_snapshot,high,Gaza/OPT,2026-03-26T00:00:00Z,{normalized_dir / 'seed-06.json'},,ready,log_and_escalate,IPC Gaza snapshot test run",
                    ]
                )
                + '\n'
            )
            args = argparse.Namespace(
                input=str(self.seed_path),
                output_dir=str(tmp_path / 'artifacts'),
                docs_csv=str(tmp_path / 'source-registry.csv'),
                pack_dir=str(tmp_path / 'v0_1'),
                plans_dir=str(tmp_path / 'plans'),
                collection_dir=str(collection_dir),
                briefing_dir=str(tmp_path / 'briefings'),
                zone_name='Cairo/Giza pilot',
                zone_country='Egypt',
                analyst='system',
                reviewer='pending_review',
                max_runs=5,
                version_prefix='preseed_sources_v',
                force_version=1,
                verbose=False,
                launcher_mode='cli',
            )
            result = bootstrap.execute_action(args, action='collect_ready')
            self.assertEqual(result['status'], 'ok')
            payload = json.loads((normalized_dir / 'seed-06.json').read_text())
            self.assertEqual(payload['adapter_type'], 'ipc_gaza_snapshot')
            self.assertEqual(payload['verification_updates']['last_published_date'], '2025-12-19')
            self.assertEqual(payload['verification_updates']['latest_period_covered'], 'October 2025 - April 2026')

    def test_collect_ready_action_extracts_ashdod_port_financials_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            collection_dir = tmp_path / 'collection'
            raw_dir = collection_dir / 'raw' / 'seed-33'
            normalized_dir = collection_dir / 'normalized'
            collection_dir.mkdir(parents=True, exist_ok=True)
            raw_dir.mkdir(parents=True, exist_ok=True)
            normalized_dir.mkdir(parents=True, exist_ok=True)

            fixture = Path('tests/fixtures/ashdod-port-financial.html')
            raw_path = raw_dir / 'run-seed-33.html'
            raw_path.write_bytes(fixture.read_bytes())

            (collection_dir / 'source-adapter-registry.csv').write_text(
                '\n'.join(
                    [
                        'source_id,rank,source_name,source_family,priority_tier,collection_stage,collection_mode,adapter_type,access_type,region_or_country,refresh_cadence,raw_landing_dir,normalized_output_path,evidence_log_path,query_seed_file,notes,url',
                        f"seed-33,33,Ashdod Port Financial and Operating Updates,market_monitor,tier1,supporting_proxy,automatable,ashdod_port_financials,html,Israel,quarterly,{raw_dir},{normalized_dir / 'seed-33.json'},{collection_dir / 'evidence-capture-log.csv'},,Ashdod financials adapter,{raw_path.as_uri()}",
                    ]
                )
                + '\n'
            )
            (collection_dir / 'collection-run-manifest.csv').write_text(
                '\n'.join(
                    [
                        'run_id,source_id,source_name,collection_stage,adapter_type,priority,district_scope,scheduled_run_utc,expected_artifact,query_seed_file,status,failure_action,notes',
                        f"run-seed-33,seed-33,Ashdod Port Financial and Operating Updates,supporting_proxy,ashdod_port_financials,high,Israel,2026-03-26T00:00:00Z,{normalized_dir / 'seed-33.json'},,ready,log_and_escalate,Ashdod financials test run",
                    ]
                )
                + '\n'
            )
            args = argparse.Namespace(
                input=str(self.seed_path),
                output_dir=str(tmp_path / 'artifacts'),
                docs_csv=str(tmp_path / 'source-registry.csv'),
                pack_dir=str(tmp_path / 'v0_1'),
                plans_dir=str(tmp_path / 'plans'),
                collection_dir=str(collection_dir),
                briefing_dir=str(tmp_path / 'briefings'),
                zone_name='Cairo/Giza pilot',
                zone_country='Egypt',
                analyst='system',
                reviewer='pending_review',
                max_runs=5,
                version_prefix='preseed_sources_v',
                force_version=1,
                verbose=False,
                launcher_mode='cli',
            )
            result = bootstrap.execute_action(args, action='collect_ready')
            self.assertEqual(result['status'], 'ok')
            payload = json.loads((normalized_dir / 'seed-33.json').read_text())
            self.assertEqual(payload['adapter_type'], 'ashdod_port_financials')
            self.assertEqual(payload['verification_updates']['last_published_date'], '2025-11-23')
            self.assertEqual(payload['verification_updates']['latest_period_covered'], '2025-09')

    def test_collect_ready_action_extracts_israel_cbs_price_indices_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            collection_dir = tmp_path / 'collection'
            raw_dir = collection_dir / 'raw' / 'seed-31'
            normalized_dir = collection_dir / 'normalized'
            collection_dir.mkdir(parents=True, exist_ok=True)
            raw_dir.mkdir(parents=True, exist_ok=True)
            normalized_dir.mkdir(parents=True, exist_ok=True)

            price_indices_html = tmp_path / 'price-indices.html'
            price_indices_html.write_text(
                '<html><body><script>'
                "var MadadNewsdataList ='{\"0\":{\"CbsDataPublishDate\":\"15/03/2026\",\"Title\":\"Consumer Price Index,February 2026\",\"ArticleStartDate\":\"15/03/2026\",\"Url\":\"https://www.cbs.gov.il/en/mediarelease/Madad/Pages/2026/Consumer-Price-Index-February-2026.aspx\"}}';"
                '</script></body></html>\n'
            )

            (collection_dir / 'source-adapter-registry.csv').write_text(
                '\n'.join(
                    [
                        'source_id,rank,source_name,source_family,priority_tier,collection_stage,collection_mode,adapter_type,access_type,region_or_country,refresh_cadence,raw_landing_dir,normalized_output_path,evidence_log_path,query_seed_file,notes,url',
                        f'seed-31,31,Israel CBS Main Price Indices,macro_price,tier1,supporting_proxy,automatable,israel_cbs_price_indices,html,Israel,monthly,{raw_dir},{normalized_dir / "seed-31.json"},{collection_dir / "evidence-capture-log.csv"},,Israel CPI adapter,{price_indices_html.as_uri()}',
                    ]
                )
                + '\n'
            )
            (collection_dir / 'collection-run-manifest.csv').write_text(
                '\n'.join(
                    [
                        'run_id,source_id,source_name,collection_stage,adapter_type,priority,district_scope,scheduled_run_utc,expected_artifact,query_seed_file,status,failure_action,notes',
                        f'run-seed-31,seed-31,Israel CBS Main Price Indices,supporting_proxy,israel_cbs_price_indices,high,Israel,2026-03-25T00:00:00Z,{normalized_dir / "seed-31.json"},,ready,log_and_escalate,Israel CPI test run',
                    ]
                )
                + '\n'
            )
            args = argparse.Namespace(
                input=str(self.seed_path),
                output_dir=str(tmp_path / 'artifacts'),
                docs_csv=str(tmp_path / 'source-registry.csv'),
                pack_dir=str(tmp_path / 'v0_1'),
                plans_dir=str(tmp_path / 'plans'),
                collection_dir=str(collection_dir),
                briefing_dir=str(tmp_path / 'briefings'),
                zone_name='Cairo/Giza pilot',
                zone_country='Egypt',
                analyst='system',
                reviewer='pending_review',
                max_runs=5,
                version_prefix='preseed_sources_v',
                force_version=1,
                verbose=False,
                launcher_mode='cli',
            )

            result = bootstrap.execute_action(args, action='collect_ready')
            self.assertEqual(result['status'], 'ok')
            payload = json.loads((normalized_dir / 'seed-31.json').read_text())
            self.assertEqual(payload['adapter_type'], 'israel_cbs_price_indices')
            self.assertEqual(payload['verification_updates']['last_published_date'], '2026-03-15')
            self.assertEqual(payload['verification_updates']['latest_period_covered'], '2026-02')
            self.assertEqual(
                payload['verification_updates']['evidence_link'],
                'https://www.cbs.gov.il/en/mediarelease/Madad/Pages/2026/Consumer-Price-Index-February-2026.aspx',
            )

    def test_collect_ready_action_extracts_israel_cbs_trade_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            collection_dir = tmp_path / 'collection'
            raw_dir = collection_dir / 'raw' / 'seed-32'
            normalized_dir = collection_dir / 'normalized'
            collection_dir.mkdir(parents=True, exist_ok=True)
            raw_dir.mkdir(parents=True, exist_ok=True)
            normalized_dir.mkdir(parents=True, exist_ok=True)

            impexp_xml = tmp_path / 'impexp.xml'
            impexp_xml.write_text(
                '<?xml version="1.0" encoding="utf-8"?>\n'
                '<feed xmlns="http://www.w3.org/2005/Atom" xmlns:d="http://schemas.microsoft.com/ado/2007/08/dataservices" xmlns:m="http://schemas.microsoft.com/ado/2007/08/dataservices/metadata">\n'
                '  <entry><content type="application/xml"><m:properties><d:Name>exp_12_2025.zip</d:Name><d:TimeLastModified>2026-01-29T08:26:53Z</d:TimeLastModified><d:ServerRelativeUrl>/he/publications/DocLib/impexpfiles/exp_12_2025.zip</d:ServerRelativeUrl></m:properties></content></entry>\n'
                '  <entry><content type="application/xml"><m:properties><d:Name>imp_12_2025.zip</d:Name><d:TimeLastModified>2026-01-29T08:29:56Z</d:TimeLastModified><d:ServerRelativeUrl>/he/publications/DocLib/impexpfiles/imp_12_2025.zip</d:ServerRelativeUrl></m:properties></content></entry>\n'
                '  <entry><content type="application/xml"><m:properties><d:Name>exp_1_2026.zip</d:Name><d:TimeLastModified>2026-02-25T12:56:19Z</d:TimeLastModified><d:ServerRelativeUrl>/he/publications/DocLib/impexpfiles/exp_1_2026.zip</d:ServerRelativeUrl></m:properties></content></entry>\n'
                '  <entry><content type="application/xml"><m:properties><d:Name>imp_1_2026.zip</d:Name><d:TimeLastModified>2026-02-25T12:56:32Z</d:TimeLastModified><d:ServerRelativeUrl>/he/publications/DocLib/impexpfiles/imp_1_2026.zip</d:ServerRelativeUrl></m:properties></content></entry>\n'
                '</feed>\n'
            )

            (collection_dir / 'source-adapter-registry.csv').write_text(
                '\n'.join(
                    [
                        'source_id,rank,source_name,source_family,priority_tier,collection_stage,collection_mode,adapter_type,access_type,region_or_country,refresh_cadence,raw_landing_dir,normalized_output_path,evidence_log_path,query_seed_file,notes,url',
                        f'seed-32,32,Israel CBS Exports and Imports Monthly Files,trade,tier1,supporting_proxy,automatable,israel_cbs_impexp_files,html/api,Israel,monthly,{raw_dir},{normalized_dir / "seed-32.json"},{collection_dir / "evidence-capture-log.csv"},,Israel trade adapter,{impexp_xml.as_uri()}',
                    ]
                )
                + '\n'
            )
            (collection_dir / 'collection-run-manifest.csv').write_text(
                '\n'.join(
                    [
                        'run_id,source_id,source_name,collection_stage,adapter_type,priority,district_scope,scheduled_run_utc,expected_artifact,query_seed_file,status,failure_action,notes',
                        f'run-seed-32,seed-32,Israel CBS Exports and Imports Monthly Files,supporting_proxy,israel_cbs_impexp_files,high,Israel,2026-03-25T00:00:00Z,{normalized_dir / "seed-32.json"},,ready,log_and_escalate,Israel trade test run',
                    ]
                )
                + '\n'
            )
            args = argparse.Namespace(
                input=str(self.seed_path),
                output_dir=str(tmp_path / 'artifacts'),
                docs_csv=str(tmp_path / 'source-registry.csv'),
                pack_dir=str(tmp_path / 'v0_1'),
                plans_dir=str(tmp_path / 'plans'),
                collection_dir=str(collection_dir),
                briefing_dir=str(tmp_path / 'briefings'),
                zone_name='Cairo/Giza pilot',
                zone_country='Egypt',
                analyst='system',
                reviewer='pending_review',
                max_runs=5,
                version_prefix='preseed_sources_v',
                force_version=1,
                verbose=False,
                launcher_mode='cli',
            )

            result = bootstrap.execute_action(args, action='collect_ready')
            self.assertEqual(result['status'], 'ok')
            payload = json.loads((normalized_dir / 'seed-32.json').read_text())
            self.assertEqual(payload['adapter_type'], 'israel_cbs_impexp_files')
            self.assertEqual(payload['verification_updates']['last_published_date'], '2026-02-25')
            self.assertEqual(payload['verification_updates']['latest_period_covered'], '2026-01')
            self.assertEqual(len(payload['latest_period_files']), 2)
            self.assertEqual(payload['verification_updates']['evidence_link'], impexp_xml.as_uri())

    def test_collect_ready_action_extracts_israel_iaa_monthly_report_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            collection_dir = tmp_path / 'collection'
            raw_dir = collection_dir / 'raw' / 'seed-34'
            normalized_dir = collection_dir / 'normalized'
            collection_dir.mkdir(parents=True, exist_ok=True)
            raw_dir.mkdir(parents=True, exist_ok=True)
            normalized_dir.mkdir(parents=True, exist_ok=True)

            archive_html = tmp_path / 'iaa-archive.html'
            archive_html.write_text(
                '<html><body>'
                'דו&quot;חות פעילות חודשיים לשנת 2026'
                '<div class="main--content">'
                '<p><a href="https://www.iaa.gov.il/media/geskf5qk/monthly-january-2026.pdf" title="דוח חודשי ינואר 2026 עברית.Cleaned">ינואר</a></p>'
                '<p><a href="https://www.iaa.gov.il/media/ublpjwzu/monthly-february-2026.pdf" title="דוח חודשי פברואר 2026 עברית.Cleaned">פברואר</a></p>'
                '</div>'
                '</body></html>\n'
            )

            (collection_dir / 'source-adapter-registry.csv').write_text(
                '\n'.join(
                    [
                        'source_id,rank,source_name,source_family,priority_tier,collection_stage,collection_mode,adapter_type,access_type,region_or_country,refresh_cadence,raw_landing_dir,normalized_output_path,evidence_log_path,query_seed_file,notes,url',
                        f'seed-34,34,Israel Airports Authority Monthly Report,market_monitor,tier1,supporting_proxy,automatable,israel_iaa_monthly_reports,html,Israel,monthly,{raw_dir},{normalized_dir / "seed-34.json"},{collection_dir / "evidence-capture-log.csv"},,IAA archive adapter,{archive_html.as_uri()}',
                    ]
                )
                + '\n'
            )
            (collection_dir / 'collection-run-manifest.csv').write_text(
                '\n'.join(
                    [
                        'run_id,source_id,source_name,collection_stage,adapter_type,priority,district_scope,scheduled_run_utc,expected_artifact,query_seed_file,status,failure_action,notes',
                        f'run-seed-34,seed-34,Israel Airports Authority Monthly Report,supporting_proxy,israel_iaa_monthly_reports,high,Israel,2026-03-25T00:00:00Z,{normalized_dir / "seed-34.json"},,ready,log_and_escalate,IAA archive test run',
                    ]
                )
                + '\n'
            )
            args = argparse.Namespace(
                input=str(self.seed_path),
                output_dir=str(tmp_path / 'artifacts'),
                docs_csv=str(tmp_path / 'source-registry.csv'),
                pack_dir=str(tmp_path / 'v0_1'),
                plans_dir=str(tmp_path / 'plans'),
                collection_dir=str(collection_dir),
                briefing_dir=str(tmp_path / 'briefings'),
                zone_name='Cairo/Giza pilot',
                zone_country='Egypt',
                analyst='system',
                reviewer='pending_review',
                max_runs=5,
                version_prefix='preseed_sources_v',
                force_version=1,
                verbose=False,
                launcher_mode='cli',
            )

            with patch.object(bootstrap, 'fetch_head_last_modified', return_value='2026-03-17'):
                result = bootstrap.execute_action(args, action='collect_ready')

            self.assertEqual(result['status'], 'ok')
            payload = json.loads((normalized_dir / 'seed-34.json').read_text())
            self.assertEqual(payload['adapter_type'], 'israel_iaa_monthly_reports')
            self.assertEqual(payload['verification_updates']['last_published_date'], '2026-03-17')
            self.assertEqual(payload['verification_updates']['latest_period_covered'], '2026-02')
            self.assertEqual(
                payload['verification_updates']['evidence_link'],
                'https://www.iaa.gov.il/media/ublpjwzu/monthly-february-2026.pdf',
            )

    def test_collect_ready_action_extracts_usda_fas_gain_pdf_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            collection_dir = tmp_path / 'collection'
            raw_dir = collection_dir / 'raw' / 'seed-10'
            normalized_dir = collection_dir / 'normalized'
            collection_dir.mkdir(parents=True, exist_ok=True)
            raw_dir.mkdir(parents=True, exist_ok=True)
            normalized_dir.mkdir(parents=True, exist_ok=True)

            report_pdf = tmp_path / 'report.pdf'
            report_pdf.write_bytes(b'%PDF-1.5\nfixture\n')

            (collection_dir / 'source-adapter-registry.csv').write_text(
                '\n'.join(
                    [
                        'source_id,rank,source_name,source_family,priority_tier,collection_stage,collection_mode,adapter_type,access_type,region_or_country,refresh_cadence,raw_landing_dir,normalized_output_path,evidence_log_path,query_seed_file,notes,url',
                        f'seed-10,10,USDA FAS Saudi Retail Foods Annual,market,tier1,supporting_proxy,automatable,usda_fas_gain_pdf,html/pdf,Saudi Arabia,annual,{raw_dir},{normalized_dir / "seed-10.json"},{collection_dir / "evidence-capture-log.csv"},,USDA GAIN PDF adapter,{report_pdf.as_uri()}',
                    ]
                )
                + '\n'
            )
            (collection_dir / 'collection-run-manifest.csv').write_text(
                '\n'.join(
                    [
                        'run_id,source_id,source_name,collection_stage,adapter_type,priority,district_scope,scheduled_run_utc,expected_artifact,query_seed_file,status,failure_action,notes',
                        f'run-seed-10,seed-10,USDA FAS Saudi Retail Foods Annual,supporting_proxy,usda_fas_gain_pdf,high,Saudi Arabia,2026-03-25T00:00:00Z,{normalized_dir / "seed-10.json"},,ready,log_and_escalate,USDA GAIN test run',
                    ]
                )
                + '\n'
            )
            args = argparse.Namespace(
                input=str(self.seed_path),
                output_dir=str(tmp_path / 'artifacts'),
                docs_csv=str(tmp_path / 'source-registry.csv'),
                pack_dir=str(tmp_path / 'v0_1'),
                plans_dir=str(tmp_path / 'plans'),
                collection_dir=str(collection_dir),
                briefing_dir=str(tmp_path / 'briefings'),
                zone_name='Cairo/Giza pilot',
                zone_country='Egypt',
                analyst='system',
                reviewer='pending_review',
                max_runs=5,
                version_prefix='preseed_sources_v',
                force_version=1,
                verbose=False,
                launcher_mode='cli',
            )

            class FakeCompletedProcess:
                def __init__(self, stdout: str) -> None:
                    self.stdout = stdout

            with patch.object(
                bootstrap.subprocess,
                'run',
                return_value=FakeCompletedProcess(
                    'CreationDate:    Wed Sep 24 20:00:38 2025 BST\n'
                    'ModDate:         Wed Sep 24 20:00:38 2025 BST\n'
                ),
            ):
                result = bootstrap.execute_action(args, action='collect_ready')

            self.assertEqual(result['status'], 'ok')
            payload = json.loads((normalized_dir / 'seed-10.json').read_text())
            self.assertEqual(payload['adapter_type'], 'usda_fas_gain_pdf')
            self.assertEqual(payload['verification_updates']['last_published_date'], '2025-09-24')
            self.assertEqual(payload['verification_updates']['evidence_link'], report_pdf.as_uri())

    def test_collect_ready_action_extracts_lebanon_cas_cpi_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            collection_dir = tmp_path / 'collection'
            raw_dir = collection_dir / 'raw' / 'seed-23'
            normalized_dir = collection_dir / 'normalized'
            collection_dir.mkdir(parents=True, exist_ok=True)
            raw_dir.mkdir(parents=True, exist_ok=True)
            normalized_dir.mkdir(parents=True, exist_ok=True)

            cpi_page = tmp_path / 'cpi.html'
            cpi_json = tmp_path / 'cpi_data.json'
            cpi_page.write_text(
                '<html><body><script>'
                f'var cpiConfig = {{"jsonUrl":"{cpi_json.as_uri()}"}};'
                '</script></body></html>\n'
            )
            cpi_json.write_text(
                json.dumps(
                    {
                        'weights': {},
                        'entries': {
                            '2025': {
                                'October': {'consumer_price_index': 1.0},
                                'November': {'consumer_price_index': 1.1},
                            }
                        },
                    }
                )
                + '\n'
            )

            (collection_dir / 'source-adapter-registry.csv').write_text(
                '\n'.join(
                    [
                        'source_id,rank,source_name,source_family,priority_tier,collection_stage,collection_mode,adapter_type,access_type,region_or_country,refresh_cadence,raw_landing_dir,normalized_output_path,evidence_log_path,query_seed_file,notes,url',
                        f'seed-23,23,Lebanon CAS Consumer Price Index,macro_price,tier1,supporting_proxy,automatable,lebanon_cas_cpi,html/api,Lebanon,monthly,{raw_dir},{normalized_dir / "seed-23.json"},{collection_dir / "evidence-capture-log.csv"},,CAS CPI adapter,{cpi_page.as_uri()}',
                    ]
                )
                + '\n'
            )
            (collection_dir / 'collection-run-manifest.csv').write_text(
                '\n'.join(
                    [
                        'run_id,source_id,source_name,collection_stage,adapter_type,priority,district_scope,scheduled_run_utc,expected_artifact,query_seed_file,status,failure_action,notes',
                        f'run-seed-23,seed-23,Lebanon CAS Consumer Price Index,supporting_proxy,lebanon_cas_cpi,high,Lebanon,2026-03-25T00:00:00Z,{normalized_dir / "seed-23.json"},,ready,log_and_escalate,CAS CPI test run',
                    ]
                )
                + '\n'
            )
            args = argparse.Namespace(
                input=str(self.seed_path),
                output_dir=str(tmp_path / 'artifacts'),
                docs_csv=str(tmp_path / 'source-registry.csv'),
                pack_dir=str(tmp_path / 'v0_1'),
                plans_dir=str(tmp_path / 'plans'),
                collection_dir=str(collection_dir),
                briefing_dir=str(tmp_path / 'briefings'),
                zone_name='Cairo/Giza pilot',
                zone_country='Egypt',
                analyst='system',
                reviewer='pending_review',
                max_runs=5,
                version_prefix='preseed_sources_v',
                force_version=1,
                verbose=False,
                launcher_mode='cli',
            )

            with patch.object(bootstrap, 'requests_head_last_modified', return_value='2026-01-24'):
                result = bootstrap.execute_action(args, action='collect_ready')

            self.assertEqual(result['status'], 'ok')
            payload = json.loads((normalized_dir / 'seed-23.json').read_text())
            self.assertEqual(payload['adapter_type'], 'lebanon_cas_cpi')
            self.assertEqual(payload['verification_updates']['last_published_date'], '2026-01-24')
            self.assertEqual(payload['verification_updates']['latest_period_covered'], '2025-11')
            self.assertEqual(payload['verification_updates']['evidence_link'], cpi_json.as_uri())

    def test_collect_ready_action_continues_after_timeout_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            collection_dir = tmp_path / 'collection'
            raw_dir_bad = collection_dir / 'raw' / 'seed-01'
            raw_dir_good = collection_dir / 'raw' / 'seed-31'
            normalized_dir = collection_dir / 'normalized'
            collection_dir.mkdir(parents=True, exist_ok=True)
            raw_dir_bad.mkdir(parents=True, exist_ok=True)
            raw_dir_good.mkdir(parents=True, exist_ok=True)
            normalized_dir.mkdir(parents=True, exist_ok=True)

            price_indices_html = tmp_path / 'price-indices.html'
            price_indices_html.write_text(
                '<html><body><script>'
                "var MadadNewsdataList ='{\"0\":{\"CbsDataPublishDate\":\"15/03/2026\",\"Title\":\"Consumer Price Index,February 2026\",\"ArticleStartDate\":\"15/03/2026\",\"Url\":\"https://www.cbs.gov.il/en/mediarelease/Madad/Pages/2026/Consumer-Price-Index-February-2026.aspx\"}}';"
                '</script></body></html>\n'
            )

            (collection_dir / 'source-adapter-registry.csv').write_text(
                '\n'.join(
                    [
                        'source_id,rank,source_name,source_family,priority_tier,collection_stage,collection_mode,adapter_type,access_type,region_or_country,refresh_cadence,raw_landing_dir,normalized_output_path,evidence_log_path,query_seed_file,notes,url',
                        f'seed-01,1,UNHCR Egypt data portal,humanitarian,tier1,baseline,automatable,html_snapshot,html,Egypt,weekly,{raw_dir_bad},{normalized_dir / "seed-01.json"},{collection_dir / "evidence-capture-log.csv"},,Timeout fixture,https://example.test/timeout',
                        f'seed-31,31,Israel CBS Main Price Indices,macro_price,tier1,supporting_proxy,automatable,israel_cbs_price_indices,html,Israel,monthly,{raw_dir_good},{normalized_dir / "seed-31.json"},{collection_dir / "evidence-capture-log.csv"},,Success fixture,{price_indices_html.as_uri()}',
                    ]
                )
                + '\n'
            )
            (collection_dir / 'collection-run-manifest.csv').write_text(
                '\n'.join(
                    [
                        'run_id,source_id,source_name,collection_stage,adapter_type,priority,district_scope,scheduled_run_utc,expected_artifact,query_seed_file,status,failure_action,notes',
                        f'run-seed-01,seed-01,UNHCR Egypt data portal,baseline,html_snapshot,high,Egypt,2026-03-25T00:00:00Z,{normalized_dir / "seed-01.json"},,ready,log_and_escalate,Timeout test run',
                        f'run-seed-31,seed-31,Israel CBS Main Price Indices,supporting_proxy,israel_cbs_price_indices,high,Israel,2026-03-25T00:00:00Z,{normalized_dir / "seed-31.json"},,ready,log_and_escalate,Success test run',
                    ]
                )
                + '\n'
            )
            args = argparse.Namespace(
                input=str(self.seed_path),
                output_dir=str(tmp_path / 'artifacts'),
                docs_csv=str(tmp_path / 'source-registry.csv'),
                pack_dir=str(tmp_path / 'v0_1'),
                plans_dir=str(tmp_path / 'plans'),
                collection_dir=str(collection_dir),
                briefing_dir=str(tmp_path / 'briefings'),
                zone_name='Cairo/Giza pilot',
                zone_country='Egypt',
                analyst='system',
                reviewer='pending_review',
                max_runs=5,
                version_prefix='preseed_sources_v',
                force_version=1,
                verbose=False,
                launcher_mode='cli',
            )

            original_fetch = bootstrap.fetch_direct_source

            def fake_fetch(url: str, raw_path: Path) -> dict[str, object]:
                if url == 'https://example.test/timeout':
                    raise TimeoutError('fixture timeout')
                return original_fetch(url, raw_path)

            with patch.object(bootstrap, 'fetch_direct_source', side_effect=fake_fetch):
                result = bootstrap.execute_action(args, action='collect_ready')

            self.assertEqual(result['status'], 'ok')
            self.assertEqual(result['processed_runs'], 2)
            self.assertEqual(result['failed_runs'], 1)
            self.assertEqual(result['completed_runs'], 1)
            self.assertTrue((normalized_dir / 'seed-31.json').exists())

    def test_collect_ready_action_stages_manual_capture_without_query_seed_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            collection_dir = tmp_path / 'collection'
            raw_dir = collection_dir / 'raw' / 'seed-13'
            normalized_dir = collection_dir / 'normalized'
            collection_dir.mkdir(parents=True, exist_ok=True)
            raw_dir.mkdir(parents=True, exist_ok=True)
            normalized_dir.mkdir(parents=True, exist_ok=True)

            (collection_dir / 'source-adapter-registry.csv').write_text(
                '\n'.join(
                    [
                        'source_id,rank,source_name,source_family,priority_tier,collection_stage,collection_mode,adapter_type,access_type,region_or_country,refresh_cadence,raw_landing_dir,normalized_output_path,evidence_log_path,query_seed_file,notes,url',
                        f'seed-13,13,Google Maps billing guidance,place,tier2,supporting_proxy,manual_or_hybrid,manual_capture,html,Regional,as needed,{raw_dir},{normalized_dir / "seed-13.json"},{collection_dir / "evidence-capture-log.csv"},,Manual capture fixture,https://developers.google.com/maps/billing-and-pricing/manage-costs',
                    ]
                )
                + '\n'
            )
            (collection_dir / 'collection-run-manifest.csv').write_text(
                '\n'.join(
                    [
                        'run_id,source_id,source_name,collection_stage,adapter_type,priority,district_scope,scheduled_run_utc,expected_artifact,query_seed_file,status,failure_action,notes',
                        f'run-seed-13,seed-13,Google Maps billing guidance,supporting_proxy,manual_capture,low,Regional,2026-03-25T00:00:00Z,{normalized_dir / "seed-13.json"},,ready,log_and_escalate,Manual staging test run',
                    ]
                )
                + '\n'
            )
            args = argparse.Namespace(
                input=str(self.seed_path),
                output_dir=str(tmp_path / 'artifacts'),
                docs_csv=str(tmp_path / 'source-registry.csv'),
                pack_dir=str(tmp_path / 'v0_1'),
                plans_dir=str(tmp_path / 'plans'),
                collection_dir=str(collection_dir),
                briefing_dir=str(tmp_path / 'briefings'),
                zone_name='Cairo/Giza pilot',
                zone_country='Egypt',
                analyst='system',
                reviewer='pending_review',
                max_runs=5,
                version_prefix='preseed_sources_v',
                force_version=1,
                verbose=False,
                launcher_mode='cli',
            )

            result = bootstrap.execute_action(args, action='collect_ready')
            self.assertEqual(result['status'], 'ok')
            self.assertEqual(result['staged_external_runs'], 1)
            payload = json.loads((normalized_dir / 'seed-13.json').read_text())
            self.assertEqual(payload['capture_status'], 'staged_external')
            self.assertEqual(payload['query_count'], 0)

    def test_recent_accounting_action_syncs_findings_from_normalized_collection(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            plans_dir = tmp_path / 'plans'
            collection_dir = tmp_path / 'collection'
            normalized_dir = collection_dir / 'normalized'
            plans_dir.mkdir(parents=True, exist_ok=True)
            normalized_dir.mkdir(parents=True, exist_ok=True)
            (normalized_dir / 'seed-27.json').write_text(
                json.dumps(
                    {
                        'source_id': 'seed-27',
                        'source_name': 'WFP Global Market Monitor',
                        'adapter_type': 'hdx_dataset_metadata',
                        'captured_utc': '2026-03-25T12:00:00Z',
                        'verification_updates': {
                            'source_id': 'seed-27',
                            'last_checked_utc': '2026-03-25T12:00:00Z',
                            'last_published_date': '2026-03-20',
                            'latest_period_covered': '',
                            'claim_date_utc': '',
                            'owner': '',
                            'evidence_link': 'https://data.humdata.org/api/3/action/package_show?id=global-market-monitor',
                            'evidence_path': '',
                            'status': 'in_review',
                            'next_action': 'Review dataset resources for an explicit coverage-period marker after metadata sync.',
                            'notes': 'Metadata sync fixture.',
                        },
                    }
                )
                + '\n'
            )
            args = argparse.Namespace(
                input=str(self.seed_path),
                output_dir=str(tmp_path / 'artifacts'),
                docs_csv=str(tmp_path / 'source-registry.csv'),
                pack_dir=str(tmp_path / 'v0_1'),
                plans_dir=str(plans_dir),
                collection_dir=str(collection_dir),
                briefing_dir=str(tmp_path / 'briefings'),
                zone_name='Cairo/Giza pilot',
                zone_country='Egypt',
                analyst='system',
                reviewer='pending_review',
                max_runs=5,
                version_prefix='preseed_sources_v',
                force_version=1,
                verbose=False,
                launcher_mode='cli',
            )

            result = bootstrap.execute_action(args, action='recent_accounting')
            findings_path = Path(result['source_verification_findings_csv'])
            self.assertTrue(findings_path.exists())
            with findings_path.open(newline='') as handle:
                findings_rows = {row['source_id']: row for row in csv.DictReader(handle)}
            with Path(result['recent_accounting_csv']).open(newline='') as handle:
                accounting_rows = {row['source_id']: row for row in csv.DictReader(handle)}

            self.assertEqual(findings_rows['seed-27']['last_published_date'], '2026-03-20')
            self.assertEqual(accounting_rows['seed-27']['last_published_date'], '2026-03-20')
            self.assertEqual(accounting_rows['seed-27']['recency_status'], 'current')

    def test_brief_zone_ingests_normalized_collection_observations(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            plans_dir = tmp_path / 'plans'
            collection_dir = tmp_path / 'collection'
            normalized_dir = collection_dir / 'normalized'
            pack_dir = tmp_path / 'v0_1'
            briefing_dir = tmp_path / 'briefings'
            plans_dir.mkdir(parents=True, exist_ok=True)
            normalized_dir.mkdir(parents=True, exist_ok=True)
            pack_dir.mkdir(parents=True, exist_ok=True)

            (plans_dir / 'recent_accounting.csv').write_text(
                '\n'.join(
                    [
                        'source_id,rank,source_name,source_family,priority_tier,region_or_country,refresh_cadence,expected_recency_window,last_checked_utc,last_published_date,latest_period_covered,claim_date_utc,recency_status,owner,evidence_link,evidence_path,status,next_check_due_utc,next_action,notes,url',
                        'seed-01,1,UNHCR Egypt data portal,humanitarian,tier1,Egypt,weekly,8 days,2026-03-25T08:29:05Z,2026-03-05,2026-02-28,2026-03-25T08:29:05Z,overdue,source_monitor,https://data.unhcr.org/en/documents/details/121421,,in_review,2026-04-02T08:29:05Z,Refresh,Verified baseline,https://data.unhcr.org/en/country/egy',
                        'seed-11,11,Google Places API,place,tier1,Regional,weekly,8 days,2026-03-25T08:29:05Z,2026-03-25,2026-03-25,2026-03-25T08:29:05Z,current,collector,https://developers.google.com/maps/documentation/places/web-service,,in_review,2026-04-02T08:29:05Z,Collect places,Ready place source,https://developers.google.com/maps/documentation/places/web-service',
                        'seed-12,12,OpenStreetMap Overpass,place,tier1,Regional,weekly,8 days,2026-03-25T08:29:05Z,2026-03-25,2026-03-25,2026-03-25T08:29:05Z,current,collector,https://wiki.openstreetmap.org/wiki/Overpass_API/Overpass_QL,,in_review,2026-04-02T08:29:05Z,Collect overpass,Ready place source,https://wiki.openstreetmap.org/wiki/Overpass_API/Overpass_QL',
                    ]
                )
                + '\n'
            )
            (pack_dir / 'event-timeline.csv').write_text(
                '\n'.join(
                    [
                        'event_id,event_type,country,admin1,district_focus,event_date,event_window_start,event_window_end,source_name,source_url,source_accessed_utc,confidence,summary,notes',
                        'egypt-sudan-arrivals-2023-04-15,arrival,Egypt,Cairo/Giza,Pilot districts,2023-04-15,2023-04-15,,UNHCR Egypt data portal; IOM DTM Sudan,https://data.unhcr.org/en/country/egy; https://dtm.iom.int/sudan,2026-03-25T08:29:05Z,high,Anchor,Anchor note',
                    ]
                )
                + '\n'
            )
            (pack_dir / 'district-watchlist.csv').write_text(
                '\n'.join(
                    [
                        'country,district_name,district_role,movement_relevance,source_coverage,food_retail_observability,mapping_quality,control_comparability,total_score,decision,paired_control,review_owner,notes',
                        'Egypt,Giza / Faisal corridor,monitoring,,,,,,,include,,,' ,
                    ]
                )
                + '\n'
            )
            (pack_dir / 'anomaly-review-worksheet.csv').write_text(
                'anomaly_id,review_week,country,district,signal_family,signal_summary,nearest_baseline_event,baseline_event_date,proxy_signal_date,humanitarian_baseline_score,market_proxy_score,spatial_fit_score,temporal_fit_score,cross_source_score,confound_penalty,raw_score,final_score,publication_label,analyst_initials,evidence_links,confound_notes,next_collection_action,status\n'
            )

            source_pull_payload = {
                'source_id': 'seed-01',
                'source_name': 'UNHCR Egypt data portal',
                'source_family': 'humanitarian',
                'captured_utc': '2026-03-25T08:29:05Z',
                'source_url': 'https://data.unhcr.org/en/country/egy',
                'capture_status': 'completed',
                'raw_path': 'artifacts/collection/raw/seed-01/run-seed-01.html',
            }
            (normalized_dir / 'seed-01.json').write_text(json.dumps(source_pull_payload) + '\n')
            place_payload = {
                'source_id': 'seed-11',
                'source_name': 'Google Places API',
                'source_family': 'place',
                'captured_utc': '2026-03-25T08:29:05Z',
                'source_url': 'https://developers.google.com/maps/documentation/places/web-service',
                'capture_status': 'completed',
                'observations': [
                    {
                        'district_or_neighborhood': 'Giza / Faisal corridor',
                        'observation_type': 'place_density',
                        'capture_method': 'places_api_search',
                        'analysis_bucket': 'signal_observation',
                        'capture_utc': '2026-03-25T08:29:05Z',
                        'source_url': 'https://developers.google.com/maps/documentation/places/web-service',
                        'source_excerpt': 'Synthetic fixture observation.',
                        'normalized_summary': 'Place-density capture completed for bakery and cafe categories in Giza / Faisal corridor.',
                        'signal_direction': 'upward',
                        'observed_value': '18',
                        'baseline_or_control_value': '11',
                        'confound_notes': 'No broad CPI spike identified in the current fixture.',
                        'confidence': 'medium',
                        'confidence_reason': 'Normalized place observation fixture.',
                        'status': 'completed',
                        'notes': 'fixture-observation',
                    }
                ],
            }
            (normalized_dir / 'seed-11.json').write_text(json.dumps(place_payload) + '\n')
            overpass_payload = {
                'source_id': 'seed-12',
                'source_name': 'OpenStreetMap Overpass',
                'source_family': 'place',
                'captured_utc': '2026-03-25T08:31:05Z',
                'source_url': 'https://wiki.openstreetmap.org/wiki/Overpass_API/Overpass_QL',
                'capture_status': 'completed',
                'observations': [
                    {
                        'district_or_neighborhood': 'Giza / Faisal corridor',
                        'observation_type': 'place_density',
                        'capture_method': 'overpass_query',
                        'analysis_bucket': 'signal_observation',
                        'capture_utc': '2026-03-25T08:31:05Z',
                        'source_url': 'https://wiki.openstreetmap.org/wiki/Overpass_API/Overpass_QL',
                        'source_excerpt': 'Synthetic corroborating observation.',
                        'normalized_summary': 'Overpass place-density capture also shows elevated bakery and cafe tags in Giza / Faisal corridor.',
                        'signal_direction': 'upward',
                        'observed_value': '16',
                        'baseline_or_control_value': '10',
                        'confound_notes': 'No taxonomy drift flagged in the fixture.',
                        'confidence': 'medium',
                        'confidence_reason': 'Normalized overpass observation fixture.',
                        'status': 'completed',
                        'notes': 'fixture-overpass',
                    }
                ],
            }
            (normalized_dir / 'seed-12.json').write_text(json.dumps(overpass_payload) + '\n')

            args = argparse.Namespace(
                input=str(self.seed_path),
                output_dir=str(tmp_path / 'artifacts'),
                docs_csv=str(tmp_path / 'source-registry.csv'),
                pack_dir=str(pack_dir),
                plans_dir=str(plans_dir),
                collection_dir=str(collection_dir),
                briefing_dir=str(briefing_dir),
                zone_name='Cairo/Giza pilot',
                zone_country='Egypt',
                analyst='system',
                reviewer='pending_review',
                max_runs=5,
                version_prefix='preseed_sources_v',
                force_version=1,
                verbose=False,
                launcher_mode='cli',
            )
            result = bootstrap.execute_action(args, action='brief_zone')
            self.assertEqual(result['status'], 'ok')

            with Path(result['source_observation_log']).open(newline='') as handle:
                observation_rows = list(csv.DictReader(handle))
            with Path(result['aggregated_signals']).open(newline='') as handle:
                aggregated_rows = list(csv.DictReader(handle))
            with Path(result['anomaly_cards']).open(newline='') as handle:
                anomaly_rows = list(csv.DictReader(handle))

            self.assertTrue(any(row['observation_type'] == 'place_density' for row in observation_rows))
            self.assertTrue(any(row['promotion_ready'] == 'yes' for row in aggregated_rows))
            self.assertTrue(any('Place-density capture completed' in row['signal_summary'] for row in anomaly_rows))

    def test_brief_zone_blocks_anomaly_promotion_for_incomplete_signal_observations(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            plans_dir = tmp_path / 'plans'
            collection_dir = tmp_path / 'collection'
            normalized_dir = collection_dir / 'normalized'
            pack_dir = tmp_path / 'v0_1'
            briefing_dir = tmp_path / 'briefings'
            plans_dir.mkdir(parents=True, exist_ok=True)
            normalized_dir.mkdir(parents=True, exist_ok=True)
            pack_dir.mkdir(parents=True, exist_ok=True)

            (plans_dir / 'recent_accounting.csv').write_text(
                '\n'.join(
                    [
                        'source_id,rank,source_name,source_family,priority_tier,region_or_country,refresh_cadence,expected_recency_window,last_checked_utc,last_published_date,latest_period_covered,claim_date_utc,recency_status,owner,evidence_link,evidence_path,status,next_check_due_utc,next_action,notes,url',
                        'seed-11,11,Google Places API,place,tier1,Regional,weekly,8 days,2026-03-25T08:29:05Z,2026-03-25,2026-03-25,2026-03-25T08:29:05Z,current,collector,https://developers.google.com/maps/documentation/places/web-service,,in_review,2026-04-02T08:29:05Z,Collect places,Ready place source,https://developers.google.com/maps/documentation/places/web-service',
                    ]
                )
                + '\n'
            )
            (pack_dir / 'event-timeline.csv').write_text(
                '\n'.join(
                    [
                        'event_id,event_type,country,admin1,district_focus,event_date,event_window_start,event_window_end,source_name,source_url,source_accessed_utc,confidence,summary,notes',
                        'egypt-sudan-arrivals-2023-04-15,arrival,Egypt,Cairo/Giza,Pilot districts,2023-04-15,2023-04-15,,UNHCR Egypt data portal; IOM DTM Sudan,https://data.unhcr.org/en/country/egy; https://dtm.iom.int/sudan,2026-03-25T08:29:05Z,high,Anchor,Anchor note',
                    ]
                )
                + '\n'
            )
            (pack_dir / 'district-watchlist.csv').write_text(
                '\n'.join(
                    [
                        'country,district_name,district_role,movement_relevance,source_coverage,food_retail_observability,mapping_quality,control_comparability,total_score,decision,paired_control,review_owner,notes',
                        'Egypt,Giza / Faisal corridor,monitoring,,,,,,,include,,,',
                    ]
                )
                + '\n'
            )
            (pack_dir / 'anomaly-review-worksheet.csv').write_text(
                'anomaly_id,review_week,country,district,signal_family,signal_summary,nearest_baseline_event,baseline_event_date,proxy_signal_date,humanitarian_baseline_score,market_proxy_score,spatial_fit_score,temporal_fit_score,cross_source_score,confound_penalty,raw_score,final_score,publication_label,analyst_initials,evidence_links,confound_notes,next_collection_action,status\n'
            )

            incomplete_payload = {
                'source_id': 'seed-11',
                'source_name': 'Google Places API',
                'source_family': 'place',
                'captured_utc': '2026-03-25T08:29:05Z',
                'source_url': 'https://developers.google.com/maps/documentation/places/web-service',
                'capture_status': 'completed',
                'observations': [
                    {
                        'district_or_neighborhood': 'Giza / Faisal corridor',
                        'observation_type': 'place_density',
                        'capture_method': 'places_api_search',
                        'capture_utc': '2026-03-25T08:29:05Z',
                        'source_url': 'https://developers.google.com/maps/documentation/places/web-service',
                        'source_excerpt': 'Incomplete fixture observation.',
                        'normalized_summary': 'Single-source place-density capture without baseline comparison.',
                        'confidence': 'medium',
                        'confidence_reason': 'Incomplete normalized place observation fixture.',
                        'status': 'completed',
                        'notes': 'fixture-incomplete',
                    }
                ],
            }
            (normalized_dir / 'seed-11.json').write_text(json.dumps(incomplete_payload) + '\n')

            args = argparse.Namespace(
                input=str(self.seed_path),
                output_dir=str(tmp_path / 'artifacts'),
                docs_csv=str(tmp_path / 'source-registry.csv'),
                pack_dir=str(pack_dir),
                plans_dir=str(plans_dir),
                collection_dir=str(collection_dir),
                briefing_dir=str(briefing_dir),
                zone_name='Cairo/Giza pilot',
                zone_country='Egypt',
                analyst='system',
                reviewer='pending_review',
                max_runs=5,
                version_prefix='preseed_sources_v',
                force_version=1,
                verbose=False,
                launcher_mode='cli',
            )
            result = bootstrap.execute_action(args, action='brief_zone')
            self.assertEqual(result['status'], 'ok')

            with Path(result['aggregated_signals']).open(newline='') as handle:
                aggregated_rows = list(csv.DictReader(handle))
            with Path(result['anomaly_cards']).open(newline='') as handle:
                anomaly_rows = list(csv.DictReader(handle))
            with Path(result['claim_register']).open(newline='') as handle:
                claim_rows = list(csv.DictReader(handle))

            self.assertTrue(all(row['promotion_ready'] == 'no' for row in aggregated_rows))
            self.assertEqual(anomaly_rows[0]['signal_type'], 'collection_readiness')
            self.assertEqual(claim_rows[0]['publication_label'], 'Unconfirmed')


if __name__ == '__main__':
    unittest.main()
