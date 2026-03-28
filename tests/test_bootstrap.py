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

    def test_derive_recency_status_honors_manual_review_status(self) -> None:
        self.assertEqual(
            bootstrap.derive_recency_status('weekly', '2025-01-01', 'manual_review'),
            'manual_review',
        )

    def test_derive_recency_status_treats_active_periodic_manual_review_as_current(self) -> None:
        self.assertEqual(
            bootstrap.derive_recency_status('periodic', '2025-01-01', 'manual_review', '2025-11/2099-07'),
            'current',
        )

    def test_soften_nonblocking_recency_downgrades_tier2_humanitarian_feed(self) -> None:
        record = {
            'source_family': 'humanitarian_feed',
            'priority_tier': 'tier2',
        }
        self.assertEqual(
            bootstrap.soften_nonblocking_recency(record, 'due_now'),
            'manual_review',
        )
        self.assertEqual(
            bootstrap.soften_nonblocking_recency(record, 'overdue'),
            'manual_review',
        )
        self.assertEqual(
            bootstrap.soften_nonblocking_recency(record, 'current'),
            'current',
        )

    def test_unhcr_egypt_sudan_emergency_update_matches_arrivals_title(self) -> None:
        self.assertTrue(
            bootstrap.unhcr_document_title_match(
                'UNHCR Egypt Sudan Emergency Update',
                'Egypt: New Arrivals from Sudan as of 19 March 2026',
            )
        )

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
            self.assertIn('mirror_evidence_link', rows[0])
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
                        'task_id,status,priority,agent,region,source_id,artifact,target_date,depends_on,next_action,acceptance_criteria',
                        'ACC-001,completed,high,source_monitor,Egypt,seed-01,plans/recent_accounting.csv,2026-03-26,,,Keep baseline task',
                        'ACC-RA-008,pending,medium,proxy_accountant,Lebanon,seed-08,plans/recent_accounting.csv,2026-03-26,,,Obsolete tier2 accounting task to remove',
                        'ACC-RA-900,in_progress,high,humanitarian_feed_monitor,Global,seed-24,plans/recent_accounting.csv,2026-03-26,,,Duplicate verification task to remove',
                        'EXT-900,pending,high,proxy_accountant,Regional,seed-11,plans/connector_readiness.csv,2026-03-26,,,Old connector task to replace',
                        'VER-999,pending,high,source_monitor,Global,seed-24,plans/source_verification_sprint.csv,2026-03-26,,,Old verification row to replace',
                    ]
                )
                + '\n'
            )
            collection_dir = tmp_path / 'collection'
            collection_dir.mkdir(parents=True, exist_ok=True)
            (collection_dir / 'collection-run-manifest.csv').write_text(
                '\n'.join(
                    [
                        'run_id,source_id,source_name,collection_stage,adapter_type,priority,district_scope,scheduled_run_utc,expected_artifact,query_seed_file,status,failure_action,notes',
                        'run-seed-11,seed-11,Google Places API,market_proxy,places_api_search,high,Regional,2026-03-25T00:00:00Z,/tmp/seed-11.json,places-query-seeds.csv,staged_external,stage,Places contract staged',
                        'run-seed-12,seed-12,OpenStreetMap Overpass,market_proxy,overpass_query,high,Regional,2026-03-25T00:00:00Z,/tmp/seed-12.json,overpass-query-seeds.csv,staged_external,stage,Overpass contract staged',
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
            queue_lookup = {row['task_id']: row for row in queue_rows}
            self.assertIn('ACC-001', queue_ids)
            self.assertIn('ACC-RA-024', queue_ids)
            self.assertNotIn('ACC-RA-008', queue_ids)
            self.assertNotIn('ACC-RA-900', queue_ids)
            self.assertIn('EXT-011', queue_ids)
            self.assertIn('EXT-012', queue_ids)
            self.assertNotIn('EXT-900', queue_ids)
            self.assertIn('VER-001', queue_ids)
            self.assertIn('VER-003', queue_ids)
            self.assertEqual(queue_lookup['EXT-011']['next_action'], 'Configure a bounded API key, quota limits, and field masks before live collection.')

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
            self.assertEqual(adapter_rows['seed-25']['adapter_type'], 'hdx_dataset_metadata')
            self.assertEqual(adapter_rows['seed-22']['adapter_type'], 'saudi_gastat_cpi')
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
            self.assertIn('mirror_evidence_link', sprint_rows[0])
            self.assertTrue(any(row['task_id'] == 'VER-001' for row in queue_rows))
            self.assertTrue(any(row['task_id'] == 'VER-002' for row in queue_rows))
            self.assertTrue(any(row['task_id'] == 'VER-003' for row in queue_rows))
            self.assertTrue(any(row['task_id'] == 'VER-004' for row in queue_rows))

    def test_build_verification_queue_rows_omits_empty_lane_tasks_and_completes_tracker(self) -> None:
        accounting_rows = [
            {
                'source_id': 'seed-33',
                'rank': '33',
                'source_name': 'Ashdod Port Financial and Operating Updates',
                'source_family': 'market_monitor',
                'priority_tier': 'tier1',
                'region_or_country': 'Israel',
                'recency_status': 'due_now',
                'owner': 'source_monitor',
                'next_check_due_utc': '2026-07-06T12:42:47Z',
            }
        ]
        sprint_rows = [
            {
                'source_id': 'seed-25',
                'lane': 'humanitarian_feed',
                'status': 'verified',
                'region_or_country': 'Global',
            },
            {
                'source_id': 'seed-33',
                'lane': 'market_monitor',
                'status': 'verified',
                'region_or_country': 'Israel',
            },
        ]

        connector_rows = [
            {
                'connector_id': 'CON-011',
                'status': 'needs_credentials',
                'priority': 'high',
                'owner': 'proxy_accountant',
                'source_id': 'seed-11',
                'region_or_country': 'Regional',
                'source_name': 'Google Places API',
            }
        ]

        queue_rows = bootstrap.build_verification_queue_rows(
            [],
            accounting_rows,
            sprint_rows,
            connector_rows,
            {'seed-11': {'source_id': 'seed-11', 'expected_artifact': 'artifacts/collection/normalized/seed-11.json'}},
        )
        queue_lookup = {row['task_id']: row for row in queue_rows}

        self.assertEqual(queue_lookup['VER-001']['status'], 'completed')
        self.assertNotIn('VER-002', queue_lookup)
        self.assertIn('VER-003', queue_lookup)
        self.assertIn('VER-004', queue_lookup)
        self.assertEqual(queue_lookup['EXT-011']['status'], 'blocked')

    def test_build_connector_queue_rows_maps_connector_state_into_ext_tasks(self) -> None:
        connector_rows = [
            {
                'connector_id': 'CON-011',
                'status': 'needs_credentials',
                'priority': 'high',
                'owner': 'proxy_accountant',
                'source_id': 'seed-11',
                'region_or_country': 'Regional',
                'source_name': 'Google Places API',
            },
            {
                'connector_id': 'CON-012',
                'status': 'ready',
                'priority': 'high',
                'owner': 'proxy_accountant',
                'source_id': 'seed-12',
                'region_or_country': 'Regional',
                'source_name': 'OpenStreetMap Overpass',
            },
        ]

        queue_rows = bootstrap.build_connector_queue_rows(
            connector_rows,
            {},
            {
                'seed-11': {'source_id': 'seed-11', 'expected_artifact': 'artifacts/collection/normalized/seed-11.json'},
                'seed-12': {'source_id': 'seed-12', 'expected_artifact': 'artifacts/collection/normalized/seed-12.json'},
            },
        )
        queue_lookup = {row['task_id']: row for row in queue_rows}

        self.assertEqual(queue_lookup['EXT-011']['status'], 'blocked')
        self.assertEqual(queue_lookup['EXT-011']['artifact'], 'artifacts/collection/normalized/seed-11.json')
        self.assertEqual(queue_lookup['EXT-012']['status'], 'pending')
        self.assertEqual(queue_lookup['EXT-012']['agent'], 'proxy_accountant')

    def test_build_connector_queue_rows_omits_nonstaged_connector_sources(self) -> None:
        connector_rows = [
            {
                'connector_id': 'CON-011',
                'status': 'needs_credentials',
                'priority': 'high',
                'owner': 'proxy_accountant',
                'source_id': 'seed-11',
                'region_or_country': 'Regional',
                'source_name': 'Google Places API',
            }
        ]

        queue_rows = bootstrap.build_connector_queue_rows(connector_rows, {}, {})

        self.assertEqual(queue_rows, [])

    def test_sync_staged_external_contracts_refreshes_manual_capture_connector_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            plans_dir = tmp_path / 'plans'
            collection_dir = tmp_path / 'collection'
            (plans_dir / 'source_specs').mkdir(parents=True, exist_ok=True)
            (collection_dir / 'raw' / 'seed-18').mkdir(parents=True, exist_ok=True)
            (collection_dir / 'normalized').mkdir(parents=True, exist_ok=True)

            (plans_dir / 'connector_readiness.csv').write_text(
                '\n'.join(
                    [
                        'connector_id,status,priority,owner,source_id,source_name,adapter_type,region_or_country,query_seed_file,credential_state,last_checked_utc,next_action,notes,url',
                        'CON-018,ready,low,source_monitor,seed-18,Deliveroo UAE trend releases,manual_capture,UAE,,public_endpoint,,Capture the pinned page,Soft evidence only,https://deliveroo.example/trends',
                    ]
                )
                + '\n'
            )
            (plans_dir / 'source_specs' / 'deliveroo.json').write_text(
                json.dumps(
                    {
                        'source_id': 'seed-18',
                        'request_method': 'GET',
                        'request_params': {'capture_mode': 'html_snapshot'},
                        'operator_steps': ['Open the pinned page'],
                    }
                )
                + '\n'
            )
            (collection_dir / 'source-adapter-registry.csv').write_text(
                '\n'.join(
                    [
                        'source_id,rank,source_name,source_family,priority_tier,collection_stage,collection_mode,adapter_type,access_type,region_or_country,refresh_cadence,raw_landing_dir,normalized_output_path,evidence_log_path,query_seed_file,notes,url',
                        f'seed-18,18,Deliveroo UAE trend releases,consumer_trends,tier2,supporting_proxy,manual_review,manual_capture,html,UAE,occasional,{collection_dir / "raw" / "seed-18"},{collection_dir / "normalized" / "seed-18.json"},{collection_dir / "evidence-capture-log.csv"},,Soft evidence only,https://deliveroo.example/trends',
                    ]
                )
                + '\n'
            )
            (collection_dir / 'collection-run-manifest.csv').write_text(
                '\n'.join(
                    [
                        'run_id,source_id,source_name,collection_stage,adapter_type,priority,district_scope,scheduled_run_utc,expected_artifact,query_seed_file,status,failure_action,notes',
                        f'run-seed-18,seed-18,Deliveroo UAE trend releases,supporting_proxy,manual_capture,low,UAE,2026-03-28T00:00:00Z,{collection_dir / "normalized" / "seed-18.json"},,staged_external,stage,Manual capture staged',
                    ]
                )
                + '\n'
            )
            (collection_dir / 'normalized' / 'seed-18.json').write_text(
                json.dumps(
                    {
                        'run_id': 'run-seed-18',
                        'source_id': 'seed-18',
                        'source_name': 'Deliveroo UAE trend releases',
                        'adapter_type': 'manual_capture',
                        'captured_utc': '2026-03-27T00:00:00Z',
                        'capture_status': 'staged_external',
                        'connector_status': '',
                        'credential_state': '',
                        'connector_next_action': '',
                    }
                )
                + '\n'
            )

            synced = bootstrap.sync_staged_external_contracts(collection_dir, plans_dir)
            self.assertEqual(synced, 1)

            payload = json.loads((collection_dir / 'normalized' / 'seed-18.json').read_text())
            self.assertEqual(payload['connector_status'], 'ready')
            self.assertEqual(payload['credential_state'], 'public_endpoint')
            self.assertEqual(payload['connector_next_action'], 'Capture the pinned page')
            self.assertEqual(payload['connector_url'], 'https://deliveroo.example/trends')

    def test_refresh_connector_readiness_from_staged_contracts_syncs_capture_time(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            collection_dir = tmp_path / 'collection'
            (collection_dir / 'normalized').mkdir(parents=True, exist_ok=True)
            (collection_dir / 'source-adapter-registry.csv').write_text(
                '\n'.join(
                    [
                        'source_id,rank,source_name,source_family,priority_tier,collection_stage,collection_mode,adapter_type,access_type,region_or_country,refresh_cadence,raw_landing_dir,normalized_output_path,evidence_log_path,query_seed_file,notes,url',
                        f'seed-12,12,OpenStreetMap Overpass,place_query,tier1,market_proxy,api,overpass_query,json,Regional,weekly,{collection_dir / "raw" / "seed-12"},{collection_dir / "normalized" / "seed-12.json"},{collection_dir / "evidence-capture-log.csv"},overpass-query-seeds.csv,Overpass contract,https://overpass.example/query',
                    ]
                )
                + '\n'
            )
            (collection_dir / 'collection-run-manifest.csv').write_text(
                '\n'.join(
                    [
                        'run_id,source_id,source_name,collection_stage,adapter_type,priority,district_scope,scheduled_run_utc,expected_artifact,query_seed_file,status,failure_action,notes',
                        f'run-seed-12,seed-12,OpenStreetMap Overpass,market_proxy,overpass_query,high,Regional,2026-03-28T00:00:00Z,{collection_dir / "normalized" / "seed-12.json"},overpass-query-seeds.csv,staged_external,stage,Overpass contract staged',
                    ]
                )
                + '\n'
            )
            (collection_dir / 'normalized' / 'seed-12.json').write_text(
                json.dumps(
                    {
                        'source_id': 'seed-12',
                        'captured_utc': '2026-03-28T20:03:59Z',
                        'connector_status': 'ready',
                        'credential_state': 'public_endpoint',
                        'connector_next_action': 'Keep Overpass queries bounded.',
                        'connector_notes': 'Use bounded Overpass queries.',
                        'connector_url': 'https://wiki.openstreetmap.org/wiki/Overpass_API/Overpass_QL',
                    }
                )
                + '\n'
            )

            rows = bootstrap.refresh_connector_readiness_from_staged_contracts(
                [
                    {
                        'connector_id': 'CON-012',
                        'status': 'ready',
                        'priority': 'high',
                        'owner': 'proxy_accountant',
                        'source_id': 'seed-12',
                        'source_name': 'OpenStreetMap Overpass',
                        'adapter_type': 'overpass_query',
                        'region_or_country': 'Regional',
                        'query_seed_file': 'overpass-query-seeds.csv',
                        'credential_state': 'public_endpoint',
                        'last_checked_utc': '',
                        'next_action': '',
                        'notes': '',
                        'url': '',
                    }
                ],
                collection_dir,
            )

            self.assertEqual(rows[0]['last_checked_utc'], '2026-03-28T20:03:59Z')
            self.assertEqual(rows[0]['status'], 'ready')
            self.assertEqual(rows[0]['credential_state'], 'public_endpoint')
            self.assertEqual(rows[0]['next_action'], 'Keep Overpass queries bounded.')
            self.assertEqual(rows[0]['notes'], 'Use bounded Overpass queries.')
            self.assertEqual(rows[0]['url'], 'https://wiki.openstreetmap.org/wiki/Overpass_API/Overpass_QL')

    def test_verification_sprint_generates_accounting_tasks_for_noncurrent_tier1_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            plans_dir = tmp_path / 'plans'
            plans_dir.mkdir(parents=True, exist_ok=True)
            (plans_dir / 'recent_accounting.csv').write_text(
                '\n'.join(
                    [
                        'source_id,rank,source_name,source_family,priority_tier,region_or_country,refresh_cadence,expected_recency_window,last_checked_utc,last_published_date,latest_period_covered,claim_date_utc,recency_status,owner,evidence_link,evidence_path,status,next_check_due_utc,next_action,notes,url',
                        'seed-01,1,UNHCR Egypt data portal,humanitarian,tier1,Egypt,weekly,8 days,2026-03-25T00:00:00Z,2026-03-21,2026-03,2026-03-25T00:00:00Z,current,ops-a,https://example.test/evidence,,in_review,2026-04-02T00:00:00Z,Current row,Current note,https://data.unhcr.org/en/country/egy',
                        'seed-24,24,HDX Humanitarian API,humanitarian_feed,tier1,Global,weekly,8 days,2026-03-25T00:00:00Z,,,2026-03-25T00:00:00Z,unknown,source_monitor,https://example.test/hapi,,in_review,2026-04-02T00:00:00Z,Capture dated product evidence,Existing HAPI note,https://data.humdata.org/hapi',
                    ]
                )
                + '\n'
            )
            (plans_dir / 'work_queue.csv').write_text(
                '\n'.join(
                    [
                        'task_id,status,priority,agent,region,source_id,artifact,target_date,depends_on,next_action,acceptance_criteria',
                        'ACC-RA-900,in_progress,high,humanitarian_feed_monitor,Global,seed-24,plans/recent_accounting.csv,2026-03-26,,,Duplicate verification task to remove',
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
            with Path(result['work_queue_csv']).open(newline='') as handle:
                queue_lookup = {row['task_id']: row for row in csv.DictReader(handle)}

            self.assertIn('ACC-RA-024', queue_lookup)
            self.assertEqual(queue_lookup['ACC-RA-024']['source_id'], 'seed-24')
            self.assertNotIn('ACC-RA-900', queue_lookup)

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

    def test_verification_sprint_refreshes_hdx_product_host_best_current_page(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            plans_dir = tmp_path / 'plans'
            plans_dir.mkdir(parents=True, exist_ok=True)
            (plans_dir / 'recent_accounting.csv').write_text(
                '\n'.join(
                    [
                        'source_id,rank,source_name,source_family,priority_tier,region_or_country,refresh_cadence,expected_recency_window,last_checked_utc,last_published_date,latest_period_covered,claim_date_utc,recency_status,owner,evidence_link,evidence_path,status,next_check_due_utc,next_action,notes,url',
                        'seed-25,25,HDX Signals,humanitarian_feed,tier1,Global,weekly,8 days,2026-03-27T00:00:00Z,2026-03-17,2025-02,,due_now,source_monitor,https://data.humdata.org/api/3/action/package_show?id=hdx-signals,,in_review,2026-04-04T00:00:00Z,Review dataset resources,Existing note,https://data.humdata.org/signals',
                    ]
                )
                + '\n'
            )
            (plans_dir / 'source_verification_sprint.csv').write_text(
                '\n'.join(
                    [
                        'sprint_id,status,priority,lane,owner,source_id,source_name,source_family,priority_tier,region_or_country,best_current_page,latest_visible_date,latest_period_covered,recency_status,evidence_link,last_checked_utc,next_action,notes',
                        'SV-025,research_complete,high,humanitarian_feed,humanitarian_feed_monitor,seed-25,HDX Signals,humanitarian_feed,tier1,Global,https://centre.humdata.org/introducing-hdx-signals/,2025-04-03,2025-02,manual_review,https://centre.humdata.org/hdx-signals-alerting-humanitarians-to-deteriorating-crises/,2026-03-01T00:00:00Z,Keep this next action,Custom analyst note',
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

            self.assertEqual(sprint_rows['seed-25']['best_current_page'], 'https://data.humdata.org/signals')
            self.assertEqual(sprint_rows['seed-25']['notes'], 'Custom analyst note')

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

    def test_collect_ready_action_requeues_noncurrent_tier1_manifest_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            plans_dir = tmp_path / 'plans'
            collection_dir = tmp_path / 'collection'
            raw_dir = collection_dir / 'raw' / 'seed-01'
            normalized_dir = collection_dir / 'normalized'
            plans_dir.mkdir(parents=True, exist_ok=True)
            collection_dir.mkdir(parents=True, exist_ok=True)
            raw_dir.mkdir(parents=True, exist_ok=True)
            normalized_dir.mkdir(parents=True, exist_ok=True)

            local_source = tmp_path / 'source.html'
            local_source.write_text('<html><body>requeued baseline</body></html>\n')
            (plans_dir / 'recent_accounting.csv').write_text(
                '\n'.join(
                    [
                        'source_id,rank,source_name,source_family,priority_tier,region_or_country,refresh_cadence,expected_recency_window,last_checked_utc,last_published_date,latest_period_covered,claim_date_utc,recency_status,owner,evidence_link,evidence_path,status,next_check_due_utc,next_action,notes,url',
                        f'seed-01,1,Local baseline,humanitarian,tier1,Egypt,weekly,8 days,2026-03-25T00:00:00Z,2026-03-10,,,due_now,source_monitor,{local_source.as_uri()},,in_review,2026-04-02T00:00:00Z,Refresh the row,Requeue fixture,{local_source.as_uri()}',
                    ]
                )
                + '\n'
            )

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
                        f'run-seed-01,seed-01,Local baseline,baseline,html_snapshot,high,Egypt,2026-03-25T00:00:00Z,{normalized_dir / "seed-01.json"},,completed,log_and_escalate,Completed row should be requeued',
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

    def test_collect_ready_action_extracts_unhcr_period_from_pdf_fallback(self) -> None:
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
            detail_html = '<html><body><p>Upload date: 16 March 2026</p><p>No issue period on the detail page.</p></body></html>'
            pdf_body = b'%PDF-1.4 fixture'

            def fake_fetch(url: str, raw_path: Path) -> dict[str, object]:
                if url == 'https://data.unhcr.org/en/country/lbn':
                    body = index_html.encode('utf-8')
                    content_type = 'text/html; charset=UTF-8'
                elif url == 'https://data.unhcr.org/en/documents/details/121604':
                    body = detail_html.encode('utf-8')
                    content_type = 'text/html; charset=UTF-8'
                elif url == 'https://data.unhcr.org/en/documents/download/121604':
                    body = pdf_body
                    content_type = 'application/pdf'
                else:
                    raise AssertionError(f'unexpected url: {url}')
                raw_path.parent.mkdir(parents=True, exist_ok=True)
                raw_path.write_bytes(body)
                return {
                    'status_code': 200,
                    'content_type': content_type,
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

            with (
                patch.object(bootstrap, 'fetch_direct_source', side_effect=fake_fetch),
                patch.object(bootstrap, 'pdftotext_content', return_value='Flash Update #2 9 – 15 March 2026'),
            ):
                result = bootstrap.execute_action(args, action='collect_ready')

            self.assertEqual(result['status'], 'ok')
            payload = json.loads((normalized_dir / 'seed-03.json').read_text())
            self.assertEqual(payload['verification_updates']['latest_period_covered'], '2026-03-09/2026-03-15')
            self.assertTrue(payload['tertiary_raw_path'].endswith('run-seed-03-document.pdf'))
            self.assertIn('linked PDF fallback', payload['verification_updates']['notes'])

    def test_collect_ready_action_prefers_newer_unhcr_as_of_update(self) -> None:
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
                '<h2 class="searchResultItem_title"><a target="_blank" href="https://data.unhcr.org/en/documents/details/121699">'
                'Middle East Situation - as of 24 March 2026</a></h2>'
                '<div class="searchResultItem_download"><a class="searchResultItem_download_link" href="https://data.unhcr.org/en/documents/download/121699" data-title="Middle East Situation - as of 24 March 2026">Download</a></div>'
                '<div class="searchResultItem_body"></div>'
                '<span class="searchResultItem_date">Publish date: <b>25 March 2026</b><br>Create date: <b>25 March 2026</b></span>'
                '</div></li>'
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
            detail_html = '<html><body><p>Upload date: 25 March 2026</p></body></html>'

            def fake_fetch(url: str, raw_path: Path) -> dict[str, object]:
                if url == 'https://data.unhcr.org/en/country/lbn':
                    body = index_html.encode('utf-8')
                    content_type = 'text/html; charset=UTF-8'
                elif url == 'https://data.unhcr.org/en/documents/details/121699':
                    body = detail_html.encode('utf-8')
                    content_type = 'text/html; charset=UTF-8'
                else:
                    raise AssertionError(f'unexpected url: {url}')
                raw_path.parent.mkdir(parents=True, exist_ok=True)
                raw_path.write_bytes(body)
                return {
                    'status_code': 200,
                    'content_type': content_type,
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
                        f'run-seed-03,seed-03,UNHCR Lebanon reporting hub,baseline,unhcr_document_index,high,Lebanon,2026-03-25T00:00:00Z,{normalized_dir / "seed-03.json"},,ready,log_and_escalate,UNHCR as-of update test run',
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
            self.assertEqual(payload['document_id'], '121699')
            self.assertEqual(payload['verification_updates']['last_published_date'], '2026-03-25')
            self.assertEqual(payload['verification_updates']['latest_period_covered'], '2026-03-24')
            self.assertEqual(payload['verification_updates']['evidence_link'], 'https://data.unhcr.org/en/documents/details/121699')

    def test_collect_ready_action_extracts_unhcr_egypt_arrivals_update(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            collection_dir = tmp_path / 'collection'
            raw_dir = collection_dir / 'raw' / 'seed-01'
            normalized_dir = collection_dir / 'normalized'
            collection_dir.mkdir(parents=True, exist_ok=True)
            raw_dir.mkdir(parents=True, exist_ok=True)
            normalized_dir.mkdir(parents=True, exist_ok=True)

            index_html = (
                '<html><body>'
                '<li class="searchResultItem -_doc media -table">'
                '<div class="searchResultItem_content media_body">'
                '<h2 class="searchResultItem_title"><a target="_blank" href="https://data.unhcr.org/en/documents/details/121681">'
                'Egypt: New Arrivals from Sudan as of 19 March 2026</a></h2>'
                '<div class="searchResultItem_download"><a class="searchResultItem_download_link" href="https://data.unhcr.org/en/documents/download/121681" data-title="Egypt: New Arrivals from Sudan as of 19 March 2026">Download</a></div>'
                '<div class="searchResultItem_body"></div>'
                '<span class="searchResultItem_date">Publish date: <b>23 March 2026</b><br>Create date: <b>24 March 2026</b></span>'
                '</div></li>'
                '</body></html>'
            )
            detail_html = '<html><body><p>Upload date: 24 March 2026</p></body></html>'
            search_url = (
                'https://data.unhcr.org/en/search?direction=desc&geo_id=1&page=1&sector=0&'
                'sector_json=%7B%220%22%3A+%220%22%7D&sort=publishDate&sv_id=0&type%5B0%5D=document'
            )

            def fake_fetch(url: str, raw_path: Path) -> dict[str, object]:
                if url == search_url:
                    body = index_html.encode('utf-8')
                    content_type = 'text/html; charset=UTF-8'
                elif url == 'https://data.unhcr.org/en/documents/details/121681':
                    body = detail_html.encode('utf-8')
                    content_type = 'text/html; charset=UTF-8'
                else:
                    raise AssertionError(f'unexpected url: {url}')
                raw_path.parent.mkdir(parents=True, exist_ok=True)
                raw_path.write_bytes(body)
                return {
                    'status_code': 200,
                    'content_type': content_type,
                    'bytes_written': len(body),
                    'checksum_sha256': 'fixture',
                }

            (collection_dir / 'source-adapter-registry.csv').write_text(
                '\n'.join(
                    [
                        'source_id,rank,source_name,source_family,priority_tier,collection_stage,collection_mode,adapter_type,access_type,region_or_country,refresh_cadence,raw_landing_dir,normalized_output_path,evidence_log_path,query_seed_file,notes,url',
                        f'seed-01,1,UNHCR Egypt data portal,humanitarian,tier1,baseline,automatable,unhcr_document_index,html/pdf,Egypt,weekly,{raw_dir},{normalized_dir / "seed-01.json"},{collection_dir / "evidence-capture-log.csv"},,UNHCR Egypt index adapter,https://data.unhcr.org/en/country/egy',
                    ]
                )
                + '\n'
            )
            (collection_dir / 'collection-run-manifest.csv').write_text(
                '\n'.join(
                    [
                        'run_id,source_id,source_name,collection_stage,adapter_type,priority,district_scope,scheduled_run_utc,expected_artifact,query_seed_file,status,failure_action,notes',
                        f'run-seed-01,seed-01,UNHCR Egypt data portal,baseline,unhcr_document_index,high,Egypt,2026-03-28T00:00:00Z,{normalized_dir / "seed-01.json"},,ready,log_and_escalate,UNHCR Egypt arrivals test run',
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
            payload = json.loads((normalized_dir / 'seed-01.json').read_text())
            self.assertEqual(payload['document_id'], '121681')
            self.assertEqual(payload['verification_updates']['last_published_date'], '2026-03-23')
            self.assertEqual(payload['verification_updates']['latest_period_covered'], '2026-03-19')
            self.assertEqual(payload['verification_updates']['evidence_link'], 'https://data.unhcr.org/en/documents/details/121681')

    def test_collect_ready_action_extracts_hdx_hapi_faq_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            collection_dir = tmp_path / 'collection'
            raw_dir = collection_dir / 'raw' / 'seed-24'
            normalized_dir = collection_dir / 'normalized'
            collection_dir.mkdir(parents=True, exist_ok=True)
            raw_dir.mkdir(parents=True, exist_ok=True)
            normalized_dir.mkdir(parents=True, exist_ok=True)

            faq_url = 'https://centre.humdata.org/ufaqs/how-up-to-date-is-the-data-in-hdx-hapi/'
            changelog_url = 'https://hdx-hapi.readthedocs.io/en/latest/changelog/'
            faq_html = (
                '<html><body>'
                '<p>HDX HAPI is updated daily from the source data. The update frequency of each source dataset varies from daily, weekly, yearly and as needed.</p>'
                '</body></html>'
            )
            changelog_html = '<html><body><h3 id="2025-09-15">2025-09-15</h3></body></html>'

            def fake_fetch(url: str, raw_path: Path) -> dict[str, object]:
                if url == faq_url:
                    body = faq_html.encode('utf-8')
                elif url == changelog_url:
                    body = changelog_html.encode('utf-8')
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
                        f'seed-24,24,HDX Humanitarian API,humanitarian_feed,tier1,supporting_proxy,automatable,hdx_hapi_changelog,html,Global,weekly,{raw_dir},{normalized_dir / "seed-24.json"},{collection_dir / "evidence-capture-log.csv"},,HDX HAPI adapter,https://data.humdata.org/hapi',
                    ]
                )
                + '\n'
            )
            (collection_dir / 'collection-run-manifest.csv').write_text(
                '\n'.join(
                    [
                        'run_id,source_id,source_name,collection_stage,adapter_type,priority,district_scope,scheduled_run_utc,expected_artifact,query_seed_file,status,failure_action,notes',
                        f'run-seed-24,seed-24,HDX Humanitarian API,supporting_proxy,hdx_hapi_changelog,high,Global,2026-03-25T00:00:00Z,{normalized_dir / "seed-24.json"},,ready,log_and_escalate,HDX HAPI test run',
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

            with (
                patch.object(bootstrap, 'fetch_direct_source', side_effect=fake_fetch),
                patch.object(bootstrap, 'requests_head_last_modified', return_value='2026-03-25'),
            ):
                result = bootstrap.execute_action(args, action='collect_ready')

            self.assertEqual(result['status'], 'ok')
            payload = json.loads((normalized_dir / 'seed-24.json').read_text())
            self.assertEqual(payload['adapter_type'], 'hdx_hapi_changelog')
            self.assertEqual(payload['metadata_source_url'], faq_url)
            self.assertEqual(payload['verification_updates']['last_published_date'], '2026-03-25')
            self.assertEqual(payload['verification_updates']['status'], 'in_review')
            self.assertEqual(payload['verification_updates']['evidence_link'], faq_url)
            self.assertTrue(payload['secondary_raw_path'].endswith('run-seed-24-changelog.html'))

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

    def test_collect_ready_action_extracts_ipc_lebanon_analysis_from_fallback_when_blocked(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            collection_dir = tmp_path / 'collection'
            raw_dir = collection_dir / 'raw' / 'seed-07'
            normalized_dir = collection_dir / 'normalized'
            plans_dir = tmp_path / 'plans'
            collection_dir.mkdir(parents=True, exist_ok=True)
            raw_dir.mkdir(parents=True, exist_ok=True)
            normalized_dir.mkdir(parents=True, exist_ok=True)
            plans_dir.mkdir(parents=True, exist_ok=True)

            (plans_dir / 'source_verification_findings.csv').write_text(
                '\n'.join(
                    [
                        'source_id,last_checked_utc,last_published_date,latest_period_covered,claim_date_utc,owner,evidence_link,evidence_path,status,next_action,notes',
                        'seed-07,2026-03-27T00:00:00Z,2025-12-23,2025-11/2026-07,,,,in_review,Watch the IPC page,Verified against the IPC Lebanon analysis page released on 2025-12-23 with validity 2025-11-01 to 2026-07-31 and linked report IPC_Lebanon_Acute_Food_Insecurity_Nov2025_July2026_Report.pdf.',
                    ]
                )
                + '\n'
            )

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
                        f"run-seed-07,seed-07,IPC Lebanon analysis,baseline,ipc_lebanon_analysis,high,Lebanon,2026-03-26T00:00:00Z,{normalized_dir / 'seed-07.json'},,ready,log_and_escalate,IPC Lebanon analysis fallback test run",
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

            def fake_fetch(url: str, destination: Path) -> dict[str, object]:
                raise bootstrap.HTTPError(url, 403, 'Forbidden', None, None)

            with patch.object(bootstrap, 'fetch_direct_source', side_effect=fake_fetch):
                result = bootstrap.execute_action(args, action='collect_ready')

            self.assertEqual(result['status'], 'ok')
            payload = json.loads((normalized_dir / 'seed-07.json').read_text())
            self.assertEqual(payload['capture_status'], 'completed')
            self.assertEqual(payload['metadata_source_url'], 'https://example.test/ipc-analysis')
            self.assertEqual(payload['verification_updates']['last_published_date'], '2025-12-23')
            self.assertEqual(payload['verification_updates']['latest_period_covered'], '2025-11/2026-07')
            self.assertEqual(payload['verification_updates']['status'], 'in_review')

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

    def test_collect_ready_action_extracts_iom_dtm_sudan_from_recent_accounting_when_blocked(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            collection_dir = tmp_path / 'collection'
            raw_dir = collection_dir / 'raw' / 'seed-02'
            normalized_dir = collection_dir / 'normalized'
            plans_dir = tmp_path / 'plans'
            collection_dir.mkdir(parents=True, exist_ok=True)
            raw_dir.mkdir(parents=True, exist_ok=True)
            normalized_dir.mkdir(parents=True, exist_ok=True)
            plans_dir.mkdir(parents=True, exist_ok=True)

            source_url = 'https://dtm.iom.int/sudan'
            (plans_dir / 'recent_accounting.csv').write_text(
                '\n'.join(
                    [
                        'source_id,rank,source_name,source_family,priority_tier,region_or_country,refresh_cadence,expected_recency_window,last_checked_utc,last_published_date,latest_period_covered,claim_date_utc,recency_status,owner,evidence_link,mirror_evidence_link,evidence_path,status,next_check_due_utc,next_action,notes,url',
                        f'seed-02,2,IOM DTM Sudan,humanitarian,tier1,Sudan/Regional,weekly,8 days,2026-03-26T10:00:00Z,2026-03-26,2026-03-26,2026-03-26T10:00:00Z,current,source_monitor,{source_url},,,in_review,2026-04-03T10:00:00Z,Follow the IOM Crisis Response Plan 2026 for returnee tracking (3M+) and IDP baseline update (10M+).,Verified on 2026-03-26. IOM reports 3M+ returnees; total displacement ~15M; Port Sudan is a major IDP hub.,{source_url}',
                    ]
                )
                + '\n'
            )

            (collection_dir / 'source-adapter-registry.csv').write_text(
                '\n'.join(
                    [
                        'source_id,rank,source_name,source_family,priority_tier,collection_stage,collection_mode,adapter_type,access_type,region_or_country,refresh_cadence,raw_landing_dir,normalized_output_path,evidence_log_path,query_seed_file,notes,url',
                        f"seed-02,2,IOM DTM Sudan,humanitarian,tier1,baseline,automatable,iom_dtm_sudan,html/pdf,Sudan/Regional,weekly,{raw_dir},{normalized_dir / 'seed-02.json'},{collection_dir / 'evidence-capture-log.csv'},,IOM DTM Sudan adapter,{source_url}",
                    ]
                )
                + '\n'
            )
            (collection_dir / 'collection-run-manifest.csv').write_text(
                '\n'.join(
                    [
                        'run_id,source_id,source_name,collection_stage,adapter_type,priority,district_scope,scheduled_run_utc,expected_artifact,query_seed_file,status,failure_action,notes',
                        f"run-seed-02,seed-02,IOM DTM Sudan,baseline,iom_dtm_sudan,high,Sudan/Regional,2026-03-28T00:00:00Z,{normalized_dir / 'seed-02.json'},,ready,log_and_escalate,IOM DTM Sudan blocked-source fallback test run",
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

            def fake_fetch(url: str, destination: Path) -> dict[str, object]:
                raise bootstrap.HTTPError(url, 403, 'Forbidden', None, None)

            with patch.object(bootstrap, 'fetch_direct_source', side_effect=fake_fetch):
                result = bootstrap.execute_action(args, action='collect_ready')

            self.assertEqual(result['status'], 'ok')
            self.assertEqual(result['staged_external_runs'], 0)
            payload = json.loads((normalized_dir / 'seed-02.json').read_text())
            self.assertEqual(payload['capture_status'], 'completed')
            self.assertEqual(payload['adapter_type'], 'iom_dtm_sudan')
            self.assertEqual(payload['metadata_source_url'], source_url)
            self.assertEqual(payload['verification_updates']['last_published_date'], '2026-03-26')
            self.assertEqual(payload['verification_updates']['latest_period_covered'], '2026-03-26')
            self.assertEqual(payload['verification_updates']['evidence_link'], source_url)
            self.assertIn('public page was blocked', payload['verification_updates']['notes'])

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

    def test_collect_ready_action_extracts_ashdod_port_financials_from_anyflip_fallback(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            collection_dir = tmp_path / 'collection'
            raw_dir = collection_dir / 'raw' / 'seed-33'
            normalized_dir = collection_dir / 'normalized'
            plans_dir = tmp_path / 'plans'
            collection_dir.mkdir(parents=True, exist_ok=True)
            raw_dir.mkdir(parents=True, exist_ok=True)
            normalized_dir.mkdir(parents=True, exist_ok=True)
            plans_dir.mkdir(parents=True, exist_ok=True)

            fallback_url = 'https://anyflip.com/hvel/gwqa/basic'
            fallback_html = (
                '<html><head>'
                '<title>תוצאות כספיות והתפתחויות תשעת חודשים ראשונים של שנת 2025 - Flip eBook Pages 1-25 | AnyFlip</title>'
                '<meta name="description" content="View flipping ebook version of תוצאות כספיות והתפתחויות תשעת חודשים ראשונים של שנת 2025 published by Ashdod Port on 2025-11-23." />'
                '<script type="application/ld+json">{"datePublished":"2025-11-23","author":{"name":"Ashdod Port"}}</script>'
                '</head><body></body></html>'
            )
            (plans_dir / 'source_verification_findings.csv').write_text(
                '\n'.join(
                    [
                        'source_id,last_checked_utc,last_published_date,latest_period_covered,claim_date_utc,owner,evidence_link,evidence_path,status,next_action,notes',
                        f'seed-33,2026-03-27T00:00:00Z,2025-11-23,2025-09,,,{fallback_url},,in_review,Track fallback,Existing fallback note',
                    ]
                )
                + '\n'
            )

            (collection_dir / 'source-adapter-registry.csv').write_text(
                '\n'.join(
                    [
                        'source_id,rank,source_name,source_family,priority_tier,collection_stage,collection_mode,adapter_type,access_type,region_or_country,refresh_cadence,raw_landing_dir,normalized_output_path,evidence_log_path,query_seed_file,notes,url',
                        f'seed-33,33,Ashdod Port Financial and Operating Updates,market_monitor,tier1,supporting_proxy,automatable,ashdod_port_financials,html,Israel,quarterly,{raw_dir},{normalized_dir / "seed-33.json"},{collection_dir / "evidence-capture-log.csv"},,Ashdod financials adapter,https://www.ashdodport.co.il/about/financial-information/Pages/default.aspx',
                    ]
                )
                + '\n'
            )
            (collection_dir / 'collection-run-manifest.csv').write_text(
                '\n'.join(
                    [
                        'run_id,source_id,source_name,collection_stage,adapter_type,priority,district_scope,scheduled_run_utc,expected_artifact,query_seed_file,status,failure_action,notes',
                        f'run-seed-33,seed-33,Ashdod Port Financial and Operating Updates,supporting_proxy,ashdod_port_financials,high,Israel,2026-03-26T00:00:00Z,{normalized_dir / "seed-33.json"},,ready,log_and_escalate,Ashdod financials fallback test run',
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

            def fake_fetch(url: str, destination: Path) -> dict[str, object]:
                if 'ashdodport.co.il' in url:
                    raise bootstrap.HTTPError(url, 403, 'Forbidden', None, None)
                if url == fallback_url:
                    body = fallback_html.encode('utf-8')
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    destination.write_bytes(body)
                    return {
                        'status_code': 200,
                        'content_type': 'text/html; charset=utf-8',
                        'bytes_written': len(body),
                        'checksum_sha256': 'fixture-fallback',
                    }
                raise AssertionError(f'unexpected url: {url}')

            with patch.object(bootstrap, 'fetch_direct_source', side_effect=fake_fetch):
                result = bootstrap.execute_action(args, action='collect_ready')

            self.assertEqual(result['status'], 'ok')
            payload = json.loads((normalized_dir / 'seed-33.json').read_text())
            self.assertEqual(payload['adapter_type'], 'ashdod_port_financials')
            self.assertEqual(payload['verification_updates']['last_published_date'], '2025-11-23')
            self.assertEqual(payload['verification_updates']['latest_period_covered'], '2025-09')
            self.assertEqual(
                payload['verification_updates']['evidence_link'],
                'https://www.ashdodport.co.il/about/financial-information/Pages/default.aspx',
            )
            self.assertEqual(payload['mirror_evidence_link'], fallback_url)
            self.assertEqual(payload['verification_updates']['mirror_evidence_link'], fallback_url)
            self.assertEqual(payload['verification_updates']['status'], 'manual_review')
            self.assertTrue(payload['secondary_raw_path'].endswith('run-seed-33-fallback.html'))

    def test_collect_ready_action_extracts_ashdod_mirror_from_fallback_notes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            collection_dir = tmp_path / 'collection'
            raw_dir = collection_dir / 'raw' / 'seed-33'
            normalized_dir = collection_dir / 'normalized'
            plans_dir = tmp_path / 'plans'
            collection_dir.mkdir(parents=True, exist_ok=True)
            raw_dir.mkdir(parents=True, exist_ok=True)
            normalized_dir.mkdir(parents=True, exist_ok=True)
            plans_dir.mkdir(parents=True, exist_ok=True)

            source_url = 'https://www.ashdodport.co.il/about/financial-information/Pages/default.aspx'
            fallback_url = 'https://anyflip.com/hvel/gwqa/basic'
            fallback_html = (
                '<html><head>'
                '<title>תוצאות כספיות והתפתחויות תשעת חודשים ראשונים של שנת 2025 - Flip eBook Pages 1-25 | AnyFlip</title>'
                '<meta name="description" content="View flipping ebook version of תוצאות כספיות והתפתחויות תשעת חודשים ראשונים של שנת 2025 published by Ashdod Port on 2025-11-23." />'
                '<script type="application/ld+json">{"datePublished":"2025-11-23","author":{"name":"Ashdod Port"}}</script>'
                '</head><body></body></html>'
            )
            (plans_dir / 'source_verification_findings.csv').write_text(
                '\n'.join(
                    [
                        'source_id,last_checked_utc,last_published_date,latest_period_covered,claim_date_utc,owner,evidence_link,evidence_path,status,next_action,notes',
                        (
                            'seed-33,2026-03-27T00:00:00Z,2025-11-23,2025-09,,,'
                            f'{source_url},,in_review,Track fallback,'
                            f'Ashdod Port Financial and Operating Updates official hub was blocked; metadata was parsed from the AnyFlip mirror fallback at {fallback_url}.'
                        ),
                    ]
                )
                + '\n'
            )

            (collection_dir / 'source-adapter-registry.csv').write_text(
                '\n'.join(
                    [
                        'source_id,rank,source_name,source_family,priority_tier,collection_stage,collection_mode,adapter_type,access_type,region_or_country,refresh_cadence,raw_landing_dir,normalized_output_path,evidence_log_path,query_seed_file,notes,url',
                        f'seed-33,33,Ashdod Port Financial and Operating Updates,market_monitor,tier1,supporting_proxy,automatable,ashdod_port_financials,html,Israel,quarterly,{raw_dir},{normalized_dir / "seed-33.json"},{collection_dir / "evidence-capture-log.csv"},,Ashdod financials adapter,{source_url}',
                    ]
                )
                + '\n'
            )
            (collection_dir / 'collection-run-manifest.csv').write_text(
                '\n'.join(
                    [
                        'run_id,source_id,source_name,collection_stage,adapter_type,priority,district_scope,scheduled_run_utc,expected_artifact,query_seed_file,status,failure_action,notes',
                        f'run-seed-33,seed-33,Ashdod Port Financial and Operating Updates,supporting_proxy,ashdod_port_financials,high,Israel,2026-03-26T00:00:00Z,{normalized_dir / "seed-33.json"},,ready,log_and_escalate,Ashdod financials notes fallback test run',
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

            def fake_fetch(url: str, destination: Path) -> dict[str, object]:
                if url == source_url:
                    raise bootstrap.HTTPError(url, 403, 'Forbidden', None, None)
                if url == fallback_url:
                    body = fallback_html.encode('utf-8')
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    destination.write_bytes(body)
                    return {
                        'status_code': 200,
                        'content_type': 'text/html; charset=utf-8',
                        'bytes_written': len(body),
                        'checksum_sha256': 'fixture-fallback-notes',
                    }
                raise AssertionError(f'unexpected url: {url}')

            with patch.object(bootstrap, 'fetch_direct_source', side_effect=fake_fetch):
                result = bootstrap.execute_action(args, action='collect_ready')

            self.assertEqual(result['status'], 'ok')
            payload = json.loads((normalized_dir / 'seed-33.json').read_text())
            self.assertEqual(payload['mirror_evidence_link'], fallback_url)
            self.assertEqual(payload['verification_updates']['evidence_link'], source_url)
            self.assertEqual(payload['verification_updates']['mirror_evidence_link'], fallback_url)
            self.assertEqual(payload['verification_updates']['status'], 'manual_review')
            self.assertTrue(payload['secondary_raw_path'].endswith('run-seed-33-fallback.html'))

    def test_collect_ready_action_extracts_wfp_factsheet_page_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            collection_dir = tmp_path / 'collection'
            raw_dir = collection_dir / 'raw' / 'seed-08'
            normalized_dir = collection_dir / 'normalized'
            collection_dir.mkdir(parents=True, exist_ok=True)
            raw_dir.mkdir(parents=True, exist_ok=True)
            normalized_dir.mkdir(parents=True, exist_ok=True)

            page_url = 'https://www.wfp.org/publications-WFP-Lebanon-Programme-Factsheets-2025'
            pdf_url = 'https://docs.wfp.org/api/documents/WFP-0000169349/download/'
            page_html = (
                '<html><head>'
                '<title>WFP Lebanon Programme Factsheets - September 2025 | World Food Programme</title>'
                '<script>dataLayer.push({"content_publication_date":"2025-10-08"});</script>'
                '</head><body>'
                '<div class="field field--field-publication-date"><time datetime="2025-10-08T12:00:00Z">8 October 2025</time></div>'
                '<h1>WFP Lebanon Programme Factsheets - September 2025</h1>'
                '<a href="https://docs.wfp.org/api/documents/WFP-0000169349/download/" class="button-new button-new--primary" aria-label="Open in English">English</a>'
                '</body></html>'
            )
            pdf_body = b'%PDF-1.4 fixture'

            def fake_fetch(url: str, destination: Path) -> dict[str, object]:
                if url == page_url:
                    body = page_html.encode('utf-8')
                    content_type = 'text/html; charset=utf-8'
                elif url == pdf_url:
                    body = pdf_body
                    content_type = 'application/pdf'
                else:
                    raise AssertionError(f'unexpected url: {url}')
                destination.parent.mkdir(parents=True, exist_ok=True)
                destination.write_bytes(body)
                return {
                    'status_code': 200,
                    'content_type': content_type,
                    'bytes_written': len(body),
                    'checksum_sha256': 'fixture-wfp',
                }

            (collection_dir / 'source-adapter-registry.csv').write_text(
                '\n'.join(
                    [
                        'source_id,rank,source_name,source_family,priority_tier,collection_stage,collection_mode,adapter_type,access_type,region_or_country,refresh_cadence,raw_landing_dir,normalized_output_path,evidence_log_path,query_seed_file,notes,url',
                        f'seed-08,8,WFP Retail and Markets factsheet,market,tier1,supporting_proxy,automatable,wfp_lebanon_factsheet_pdf,html/pdf,Lebanon,periodic,{raw_dir},{normalized_dir / "seed-08.json"},{collection_dir / "evidence-capture-log.csv"},,WFP factsheet adapter,{page_url}',
                    ]
                )
                + '\n'
            )
            (collection_dir / 'collection-run-manifest.csv').write_text(
                '\n'.join(
                    [
                        'run_id,source_id,source_name,collection_stage,adapter_type,priority,district_scope,scheduled_run_utc,expected_artifact,query_seed_file,status,failure_action,notes',
                        f'run-seed-08,seed-08,WFP Retail and Markets factsheet,supporting_proxy,wfp_lebanon_factsheet_pdf,high,Lebanon,2026-03-26T00:00:00Z,{normalized_dir / "seed-08.json"},,ready,log_and_escalate,WFP factsheet test run',
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

            with (
                patch.object(bootstrap, 'fetch_direct_source', side_effect=fake_fetch),
                patch.object(bootstrap, 'pdfinfo_report_date', return_value='2025-10-06'),
                patch.object(bootstrap, 'pdftotext_content', return_value='WFP Lebanon - September 2025'),
            ):
                result = bootstrap.execute_action(args, action='collect_ready')

            self.assertEqual(result['status'], 'ok')
            payload = json.loads((normalized_dir / 'seed-08.json').read_text())
            self.assertEqual(payload['adapter_type'], 'wfp_lebanon_factsheet_pdf')
            self.assertEqual(payload['metadata_source_url'], page_url)
            self.assertEqual(payload['document_download_url'], pdf_url)
            self.assertEqual(payload['document_title'], 'WFP Lebanon Programme Factsheets - September 2025')
            self.assertEqual(payload['verification_updates']['last_published_date'], '2025-10-08')
            self.assertEqual(payload['verification_updates']['latest_period_covered'], '2025-09')
            self.assertEqual(payload['verification_updates']['evidence_link'], page_url)
            self.assertTrue(payload['secondary_raw_path'].endswith('run-seed-08-page.html'))

    def test_collect_ready_action_extracts_unctad_maritime_insights_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            collection_dir = tmp_path / 'collection'
            raw_dir = collection_dir / 'raw' / 'seed-29'
            normalized_dir = collection_dir / 'normalized'
            collection_dir.mkdir(parents=True, exist_ok=True)
            raw_dir.mkdir(parents=True, exist_ok=True)
            normalized_dir.mkdir(parents=True, exist_ok=True)

            theme_url = 'https://unctadstat.unctad.org/insights/theme/246'
            metadata_url = 'https://unctadstat.unctad.org/datacentre/dataviewer/US.LSCI'
            theme_html = (
                '<html><body><article>'
                '<h3 class="dataviz-heading__title">Mauritania, Sierra Leone, and Cameroon saw the largest annual connectivity increases in the fourth quarter of 2025</h3>'
                '<a href="/datacentre/dataviewer/US.LSCI">Open data centre</a>'
                '<p>In the fourth quarter of 2025 compared to the same period in 2024, connectivity gains were more widespread than losses.</p>'
                '<span class="updatedate__content">28 January 2026</span>'
                '</article></body></html>'
            )
            metadata_html = '<html><head><title>UNCTADstat Data Centre</title></head><body>US.LSCI</body></html>'

            def fake_fetch(url: str, destination: Path) -> dict[str, object]:
                if url == theme_url:
                    body = theme_html.encode('utf-8')
                elif url == metadata_url:
                    body = metadata_html.encode('utf-8')
                else:
                    raise AssertionError(f'unexpected url: {url}')
                destination.parent.mkdir(parents=True, exist_ok=True)
                destination.write_bytes(body)
                return {
                    'status_code': 200,
                    'content_type': 'text/html; charset=utf-8',
                    'bytes_written': len(body),
                    'checksum_sha256': 'fixture-unctad',
                }

            (collection_dir / 'source-adapter-registry.csv').write_text(
                '\n'.join(
                    [
                        'source_id,rank,source_name,source_family,priority_tier,collection_stage,collection_mode,adapter_type,access_type,region_or_country,refresh_cadence,raw_landing_dir,normalized_output_path,evidence_log_path,query_seed_file,notes,url',
                        f'seed-29,29,UNCTAD Maritime Transport Insights,market_monitor,tier1,supporting_proxy,automatable,unctad_maritime_insights,html,Global,periodic,{raw_dir},{normalized_dir / "seed-29.json"},{collection_dir / "evidence-capture-log.csv"},,UNCTAD maritime adapter,{theme_url}',
                    ]
                )
                + '\n'
            )
            (collection_dir / 'collection-run-manifest.csv').write_text(
                '\n'.join(
                    [
                        'run_id,source_id,source_name,collection_stage,adapter_type,priority,district_scope,scheduled_run_utc,expected_artifact,query_seed_file,status,failure_action,notes',
                        f'run-seed-29,seed-29,UNCTAD Maritime Transport Insights,supporting_proxy,unctad_maritime_insights,high,Global,2026-03-26T00:00:00Z,{normalized_dir / "seed-29.json"},,ready,log_and_escalate,UNCTAD maritime test run',
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
            payload = json.loads((normalized_dir / 'seed-29.json').read_text())
            self.assertEqual(payload['adapter_type'], 'unctad_maritime_insights')
            self.assertEqual(payload['verification_updates']['last_published_date'], '2026-01-28')
            self.assertEqual(payload['verification_updates']['latest_period_covered'], '2025-12')
            self.assertEqual(payload['verification_updates']['evidence_link'], metadata_url)
            self.assertTrue(payload['secondary_raw_path'].endswith('run-seed-29-datacentre.html'))

    def test_collect_ready_action_extracts_sca_navigation_news_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            collection_dir = tmp_path / 'collection'
            raw_dir = collection_dir / 'raw' / 'seed-30'
            normalized_dir = collection_dir / 'normalized'
            collection_dir.mkdir(parents=True, exist_ok=True)
            raw_dir.mkdir(parents=True, exist_ok=True)
            normalized_dir.mkdir(parents=True, exist_ok=True)

            index_url = 'https://www.suezcanal.gov.eg/English/MediaCenter/News/Pages/default.aspx'
            detail_url = 'https://www.suezcanal.gov.eg/English/MediaCenter/News/Pages/Sca_3-3-2026.aspx'
            index_html = (
                '<xml>'
                '<Row Title="Traffic through the Canal is flowing normally in both directions" '
                'PublishingStartDate="3 Mar 2026" '
                'NewsCategory_x003a_Title="Navigation News" '
                'ServerUrl="/English/MediaCenter/News/Pages/Sca_3-3-2026.aspx"></Row>'
                '</xml>'
            )
            detail_html = (
                '<html><head><title>SCA - Traffic through the Canal is flowing normally in both directions</title></head>'
                '<body>'
                '<span class="bold" id="spnDate">3 March 2026</span>'
                '<div>Navigation News</div>'
                '<div>The Suez Canal witnesses the transit of 56 vessels today at a total gross tonnage of 2.6 million tons.</div>'
                '<div>The Canal has witnessed today the transit of 56 vessels in both directions at a total gross tonnage of 2.6 million tons; '
                '24 vessels in the nourthern convoy at a total gross tonnage of 1 million tons and 32 vessels in the southern convoy at a total gross tonnage of 1.6 million tons.</div>'
                '<div>It is worth noting that, during the past three days, 100 vessels transited through the Canal, with a total net tonnage of 3.8 million tons.</div>'
                '</body></html>'
            )

            def fake_fetch(url: str, destination: Path) -> dict[str, object]:
                if url == index_url:
                    body = index_html.encode('utf-8')
                elif url == detail_url:
                    body = detail_html.encode('utf-8')
                else:
                    raise AssertionError(f'unexpected url: {url}')
                destination.parent.mkdir(parents=True, exist_ok=True)
                destination.write_bytes(body)
                return {
                    'status_code': 200,
                    'content_type': 'text/html; charset=utf-8',
                    'bytes_written': len(body),
                    'checksum_sha256': 'fixture-sca',
                }

            (collection_dir / 'source-adapter-registry.csv').write_text(
                '\n'.join(
                    [
                        'source_id,rank,source_name,source_family,priority_tier,collection_stage,collection_mode,adapter_type,access_type,region_or_country,refresh_cadence,raw_landing_dir,normalized_output_path,evidence_log_path,query_seed_file,notes,url',
                        f'seed-30,30,Suez Canal Authority Navigation News,market_monitor,tier1,supporting_proxy,automatable,sca_navigation_news,html,Regional,periodic,{raw_dir},{normalized_dir / "seed-30.json"},{collection_dir / "evidence-capture-log.csv"},,SCA navigation adapter,{index_url}',
                    ]
                )
                + '\n'
            )
            (collection_dir / 'collection-run-manifest.csv').write_text(
                '\n'.join(
                    [
                        'run_id,source_id,source_name,collection_stage,adapter_type,priority,district_scope,scheduled_run_utc,expected_artifact,query_seed_file,status,failure_action,notes',
                        f'run-seed-30,seed-30,Suez Canal Authority Navigation News,supporting_proxy,sca_navigation_news,high,Regional,2026-03-26T00:00:00Z,{normalized_dir / "seed-30.json"},,ready,log_and_escalate,SCA navigation test run',
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
            payload = json.loads((normalized_dir / 'seed-30.json').read_text())
            self.assertEqual(payload['adapter_type'], 'sca_navigation_news')
            self.assertEqual(payload['verification_updates']['last_published_date'], '2026-03-03')
            self.assertEqual(payload['verification_updates']['claim_date_utc'], '2026-03-03')
            self.assertEqual(payload['daily_vessel_count'], '56')
            self.assertEqual(payload['daily_gross_tonnage_mtons'], '2.6')
            self.assertEqual(payload['rolling_three_day_vessel_count'], '100')
            self.assertEqual(payload['rolling_three_day_gross_tonnage_mtons'], '3.8')
            self.assertEqual(payload['verification_updates']['evidence_link'], detail_url)

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

    def test_collect_ready_action_extracts_hdx_signals_dataset_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            collection_dir = tmp_path / 'collection'
            raw_dir = collection_dir / 'raw' / 'seed-25'
            normalized_dir = collection_dir / 'normalized'
            collection_dir.mkdir(parents=True, exist_ok=True)
            raw_dir.mkdir(parents=True, exist_ok=True)
            normalized_dir.mkdir(parents=True, exist_ok=True)

            api_url = 'https://data.humdata.org/api/3/action/package_show?id=hdx-signals'
            package_payload = {
                'success': True,
                'result': {
                    'name': 'hdx-signals',
                    'title': 'HDX Signals',
                    'metadata_modified': '2026-03-17T14:50:30.273874',
                    'metadata_created': '2024-06-07T14:04:16.875037',
                    'data_update_frequency': 7,
                    'due_date': '2026-03-20T23:59:59',
                    'update_status': 'needs_update',
                    'resources': [
                        {
                            'name': 'hdx_signals.csv',
                            'format': 'CSV',
                            'last_modified': '2026-03-17T14:50:30.167678',
                            'created': '2024-06-07T14:04:16.881913',
                            'url': 'https://data.humdata.org/dataset/464950af/resource/49056751/download/hdx_signals.csv',
                        },
                    ],
                },
            }

            (collection_dir / 'source-adapter-registry.csv').write_text(
                '\n'.join(
                    [
                        'source_id,rank,source_name,source_family,priority_tier,collection_stage,collection_mode,adapter_type,access_type,region_or_country,refresh_cadence,raw_landing_dir,normalized_output_path,evidence_log_path,query_seed_file,notes,url',
                        f'seed-25,25,HDX Signals,humanitarian_feed,tier1,supporting_proxy,automatable,hdx_dataset_metadata,html,Global,weekly,{raw_dir},{normalized_dir / "seed-25.json"},{collection_dir / "evidence-capture-log.csv"},,HDX Signals adapter,https://data.humdata.org/signals',
                    ]
                )
                + '\n'
            )
            (collection_dir / 'collection-run-manifest.csv').write_text(
                '\n'.join(
                    [
                        'run_id,source_id,source_name,collection_stage,adapter_type,priority,district_scope,scheduled_run_utc,expected_artifact,query_seed_file,status,failure_action,notes',
                        f'run-seed-25,seed-25,HDX Signals,supporting_proxy,hdx_dataset_metadata,high,Global,2026-03-25T00:00:00Z,{normalized_dir / "seed-25.json"},,ready,log_and_escalate,HDX Signals test run',
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

            def fake_fetch(url: str, destination: Path) -> dict[str, object]:
                if url != api_url:
                    raise AssertionError(f'unexpected url: {url}')
                body = json.dumps(package_payload).encode('utf-8')
                destination.parent.mkdir(parents=True, exist_ok=True)
                destination.write_bytes(body)
                return {
                    'status_code': 200,
                    'content_type': 'application/json',
                    'bytes_written': len(body),
                    'checksum_sha256': 'fixture-hdx-signals',
                }

            with patch.object(bootstrap, 'fetch_direct_source', side_effect=fake_fetch):
                result = bootstrap.execute_action(args, action='collect_ready')

            self.assertEqual(result['status'], 'ok')
            payload = json.loads((normalized_dir / 'seed-25.json').read_text())
            self.assertEqual(payload['adapter_type'], 'hdx_dataset_metadata')
            self.assertEqual(payload['dataset_name'], 'hdx-signals')
            self.assertEqual(payload['verification_updates']['last_published_date'], '2026-03-17')
            self.assertEqual(payload['verification_updates']['evidence_link'], api_url)
            self.assertEqual(payload['verification_updates']['status'], 'in_review')
            self.assertEqual(payload['latest_resource_modified_date'], '2026-03-17')
            self.assertEqual(payload['data_update_frequency'], '7')
            self.assertEqual(payload['due_date'], '2026-03-20T23:59:59')
            self.assertEqual(payload['update_status'], 'needs_update')
            self.assertIn('Declared update frequency: every 7 day(s).', payload['verification_updates']['notes'])

    def test_collect_ready_action_extracts_saudi_gastat_cpi_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            collection_dir = tmp_path / 'collection'
            raw_dir = collection_dir / 'raw' / 'seed-22'
            normalized_dir = collection_dir / 'normalized'
            collection_dir.mkdir(parents=True, exist_ok=True)
            raw_dir.mkdir(parents=True, exist_ok=True)
            normalized_dir.mkdir(parents=True, exist_ok=True)

            gastat_page = tmp_path / 'gastat.html'
            gastat_page.write_text(
                '<html><body>'
                '<h1>Consumer Price Index</h1>'
                '<p class="fs-sm text-secondary"> Last Modified: 22/12/2025 12:42:02 PM</p>'
                '</body></html>\n'
            )

            (collection_dir / 'source-adapter-registry.csv').write_text(
                '\n'.join(
                    [
                        'source_id,rank,source_name,source_family,priority_tier,collection_stage,collection_mode,adapter_type,access_type,region_or_country,refresh_cadence,raw_landing_dir,normalized_output_path,evidence_log_path,query_seed_file,notes,url',
                        f'seed-22,22,Saudi GASTAT Consumer Price Index,macro_price,tier1,supporting_proxy,automatable,saudi_gastat_cpi,html,Saudi Arabia,monthly,{raw_dir},{normalized_dir / "seed-22.json"},{collection_dir / "evidence-capture-log.csv"},,GASTAT CPI adapter,{gastat_page.as_uri()}',
                    ]
                )
                + '\n'
            )
            (collection_dir / 'collection-run-manifest.csv').write_text(
                '\n'.join(
                    [
                        'run_id,source_id,source_name,collection_stage,adapter_type,priority,district_scope,scheduled_run_utc,expected_artifact,query_seed_file,status,failure_action,notes',
                        f'run-seed-22,seed-22,Saudi GASTAT Consumer Price Index,supporting_proxy,saudi_gastat_cpi,high,Saudi Arabia,2026-03-25T00:00:00Z,{normalized_dir / "seed-22.json"},,ready,log_and_escalate,GASTAT CPI test run',
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
            payload = json.loads((normalized_dir / 'seed-22.json').read_text())
            self.assertEqual(payload['adapter_type'], 'saudi_gastat_cpi')
            self.assertEqual(payload['verification_updates']['last_published_date'], '2025-12-22')
            self.assertEqual(payload['verification_updates']['latest_period_covered'], '2025-11')
            self.assertEqual(payload['verification_updates']['evidence_link'], gastat_page.as_uri())

    def test_collect_ready_action_extracts_saudi_gastat_cpi_from_listing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            collection_dir = tmp_path / 'collection'
            raw_dir = collection_dir / 'raw' / 'seed-22'
            normalized_dir = collection_dir / 'normalized'
            collection_dir.mkdir(parents=True, exist_ok=True)
            raw_dir.mkdir(parents=True, exist_ok=True)
            normalized_dir.mkdir(parents=True, exist_ok=True)

            listing_url = 'https://www.stats.gov.sa/en/statistics-tabs/-/categories/121421?category=121421&delta=20&start=1&tab=436312'
            pdf_url = 'https://www.stats.gov.sa/documents/20117/2435267/CPI+Feb+2026-EN.pdf/b5bb5491-511c-d979-de0b-f84380966d1a?t=1773476710285'
            listing_html = (
                '<html><body>'
                '<div class="box-body">'
                '<span class="row-header">Consumer Price Index – February 2026</span>'
                '<span class="th">2026</span>'
                '<span class="th">Monthly</span>'
                '<span class="th">February</span>'
                '<div class="d-flex gap-2 align-items-center">'
                '<a class="btn-link btn-white" href="/documents/20117/2435267/CPI+Feb+2026-EN.pdf/b5bb5491-511c-d979-de0b-f84380966d1a?t=1773476710285" target="_blank">PDF</a>'
                '<a class="btn-link btn-white" href="/documents/20117/2435267/CPI+Tables-Feb+2026-AR-EN+%281%29.xlsx/239a9d5c-a963-7194-5e39-ce39f53dd31a?t=1773476597747" target="_blank">XLSX</a>'
                '</div>'
                '<!-- View Details Button -->'
                '</div>'
                '</body></html>\n'
            )

            (collection_dir / 'source-adapter-registry.csv').write_text(
                '\n'.join(
                    [
                        'source_id,rank,source_name,source_family,priority_tier,collection_stage,collection_mode,adapter_type,access_type,region_or_country,refresh_cadence,raw_landing_dir,normalized_output_path,evidence_log_path,query_seed_file,notes,url',
                        f'seed-22,22,Saudi GASTAT Consumer Price Index,macro_price,tier1,supporting_proxy,automatable,saudi_gastat_cpi,html,Saudi Arabia,monthly,{raw_dir},{normalized_dir / "seed-22.json"},{collection_dir / "evidence-capture-log.csv"},,GASTAT CPI adapter,https://stats.gov.sa/en/w/cpi-1',
                    ]
                )
                + '\n'
            )
            (collection_dir / 'collection-run-manifest.csv').write_text(
                '\n'.join(
                    [
                        'run_id,source_id,source_name,collection_stage,adapter_type,priority,district_scope,scheduled_run_utc,expected_artifact,query_seed_file,status,failure_action,notes',
                        f'run-seed-22,seed-22,Saudi GASTAT Consumer Price Index,supporting_proxy,saudi_gastat_cpi,high,Saudi Arabia,2026-03-25T00:00:00Z,{normalized_dir / "seed-22.json"},,ready,log_and_escalate,GASTAT CPI listing test run',
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

            def fake_fetch(url: str, destination: Path) -> dict[str, object]:
                if url != listing_url:
                    raise AssertionError(f'unexpected url: {url}')
                body = listing_html.encode('utf-8')
                destination.parent.mkdir(parents=True, exist_ok=True)
                destination.write_bytes(body)
                return {
                    'status_code': 200,
                    'content_type': 'text/html; charset=utf-8',
                    'bytes_written': len(body),
                    'checksum_sha256': 'fixture-gastat-listing',
                }

            with (
                patch.object(bootstrap, 'fetch_direct_source', side_effect=fake_fetch),
                patch.object(bootstrap, 'requests_head_last_modified', return_value='2026-03-14'),
            ):
                result = bootstrap.execute_action(args, action='collect_ready')

            self.assertEqual(result['status'], 'ok')
            payload = json.loads((normalized_dir / 'seed-22.json').read_text())
            self.assertEqual(payload['adapter_type'], 'saudi_gastat_cpi')
            self.assertEqual(payload['metadata_source_url'], listing_url)
            self.assertEqual(payload['document_title'], 'Consumer Price Index – February 2026')
            self.assertEqual(payload['verification_updates']['last_published_date'], '2026-03-14')
            self.assertEqual(payload['verification_updates']['latest_period_covered'], '2026-02')
            self.assertEqual(payload['verification_updates']['evidence_link'], pdf_url)

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

    def test_collect_ready_action_prefers_newer_official_lebanon_cas_cpi_pdf(self) -> None:
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
            official_pdf = tmp_path / 'CPI' / '2026' / '2-CPI_FEBRUARY2026.pdf'
            official_pdf.parent.mkdir(parents=True, exist_ok=True)
            official_pdf.write_bytes(b'%PDF-1.4 official cpi fixture\n')
            cpi_page.write_text(
                '<html><body><script>'
                f'var cpiConfig = {{"jsonUrl":"{cpi_json.as_uri()}"}};'
                '</script>'
                f'<a href="{official_pdf.as_uri()}">February 2026 PDF</a>'
                '</body></html>\n'
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
                        f'run-seed-23,seed-23,Lebanon CAS Consumer Price Index,supporting_proxy,lebanon_cas_cpi,high,Lebanon,2026-03-25T00:00:00Z,{normalized_dir / "seed-23.json"},,ready,log_and_escalate,CAS CPI official PDF test run',
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

            def fake_last_modified(url: str) -> str:
                if url == cpi_json.as_uri():
                    return '2026-01-24'
                if url == official_pdf.as_uri():
                    return '2026-03-24'
                return ''

            with patch.object(bootstrap, 'requests_head_last_modified', side_effect=fake_last_modified):
                result = bootstrap.execute_action(args, action='collect_ready')

            self.assertEqual(result['status'], 'ok')
            payload = json.loads((normalized_dir / 'seed-23.json').read_text())
            self.assertEqual(payload['verification_updates']['last_published_date'], '2026-03-24')
            self.assertEqual(payload['verification_updates']['latest_period_covered'], '2026-02')
            self.assertEqual(payload['verification_updates']['evidence_link'], official_pdf.as_uri())
            self.assertTrue(payload['tertiary_raw_path'])

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
            plans_dir = tmp_path / 'plans'
            source_specs_dir = plans_dir / 'source_specs'
            collection_dir.mkdir(parents=True, exist_ok=True)
            raw_dir.mkdir(parents=True, exist_ok=True)
            normalized_dir.mkdir(parents=True, exist_ok=True)
            plans_dir.mkdir(parents=True, exist_ok=True)
            source_specs_dir.mkdir(parents=True, exist_ok=True)

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
            (plans_dir / 'connector_readiness.csv').write_text(
                '\n'.join(
                    [
                        'connector_id,status,priority,owner,source_id,source_name,adapter_type,region_or_country,query_seed_file,credential_state,last_checked_utc,next_action,notes,url',
                        'CON-013,ready,low,proxy_accountant,seed-13,Google Maps billing guidance,manual_capture,Regional,,public_endpoint,,Review billing and quota controls before enabling Places collection.,Operational safeguard to keep API use controlled and auditable.,https://developers.google.com/maps/billing-and-pricing/manage-costs',
                    ]
                )
                + '\n'
            )
            (source_specs_dir / 'google_maps_billing_guidance_capture.json').write_text(
                json.dumps(
                    {
                        'source_id': 'seed-13',
                        'source_name': 'Google Maps billing guidance',
                        'adapter_type': 'manual_capture',
                        'request_method': 'GET',
                        'request_params': {
                            'page_url': 'https://developers.google.com/maps/billing-and-pricing/manage-costs',
                            'capture_mode': 'manual_page_review',
                        },
                        'operator_steps': [
                            'Capture the current pricing and quota-control page.',
                            'Record any cost-control recommendations that constrain Places collection.',
                        ],
                        'extraction_targets': ['billing controls', 'quota guidance', 'field-mask guidance'],
                        'raw_path_template': 'artifacts/collection/raw/seed-13/manual_capture/<run_id>.json',
                        'normalized_path_template': 'artifacts/collection/normalized/seed-13.json',
                        'quality_checks': ['Keep this source in a supporting operational role only.'],
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

            result = bootstrap.execute_action(args, action='collect_ready')
            self.assertEqual(result['status'], 'ok')
            self.assertEqual(result['staged_external_runs'], 1)
            payload = json.loads((normalized_dir / 'seed-13.json').read_text())
            self.assertEqual(payload['capture_status'], 'staged_external')
            self.assertEqual(payload['query_count'], 0)
            self.assertEqual(payload['connector_status'], 'ready')
            self.assertEqual(payload['credential_state'], 'public_endpoint')
            self.assertEqual(payload['request_method'], 'GET')
            self.assertTrue(payload['source_spec_path'].endswith('google_maps_billing_guidance_capture.json'))
            self.assertEqual(payload['execution_contract']['request_method'], 'GET')
            self.assertEqual(
                payload['execution_contract']['reference_url'],
                'https://developers.google.com/maps/billing-and-pricing/manage-costs',
            )
            self.assertEqual(
                payload['execution_contract']['request_params']['capture_mode'],
                'manual_page_review',
            )
            raw_payload = json.loads(Path(payload['raw_path']).read_text())
            self.assertEqual(raw_payload['connector_status'], 'ready')
            self.assertEqual(raw_payload['credential_state'], 'public_endpoint')
            self.assertEqual(raw_payload['request_method'], 'GET')
            self.assertTrue(raw_payload['source_spec_path'].endswith('google_maps_billing_guidance_capture.json'))
            self.assertEqual(raw_payload['execution_contract']['request_method'], 'GET')
            with (collection_dir / 'evidence-capture-log.csv').open(newline='') as handle:
                evidence_rows = list(csv.DictReader(handle))
            staged_row = next(row for row in evidence_rows if row['source_id'] == 'seed-13' and row['status'] == 'staged_external')
            self.assertEqual(staged_row['normalized_path'], str(normalized_dir / 'seed-13.json'))
            self.assertEqual(staged_row['evidence_path'], str(normalized_dir / 'seed-13.json'))

    def test_collect_ready_action_matches_place_queries_to_district_scope(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            collection_dir = tmp_path / 'collection'
            raw_dir = collection_dir / 'raw' / 'seed-11'
            normalized_dir = collection_dir / 'normalized'
            plans_dir = tmp_path / 'plans'
            source_specs_dir = plans_dir / 'source_specs'
            collection_dir.mkdir(parents=True, exist_ok=True)
            raw_dir.mkdir(parents=True, exist_ok=True)
            normalized_dir.mkdir(parents=True, exist_ok=True)
            plans_dir.mkdir(parents=True, exist_ok=True)
            source_specs_dir.mkdir(parents=True, exist_ok=True)

            (collection_dir / 'places-query-seeds.csv').write_text(
                '\n'.join(
                    [
                        'query_id,country,district_name,place_source,place_type,search_mode,query_text,priority,status,notes',
                        'gplaces-001,Egypt,Nasr City,Google Places API,bakery,text_search_then_details,bakery in Nasr City,high,ready,Fixture row',
                        'gplaces-002,Egypt,Imbaba,Google Places API,cafe,text_search_then_details,cafe in Imbaba,high,ready,Fixture row',
                        'gplaces-003,Lebanon,Beirut,Google Places API,bakery,text_search_then_details,bakery in Beirut,high,ready,Fixture row',
                    ]
                )
                + '\n'
            )
            (collection_dir / 'source-adapter-registry.csv').write_text(
                '\n'.join(
                    [
                        'source_id,rank,source_name,source_family,priority_tier,collection_stage,collection_mode,adapter_type,access_type,region_or_country,refresh_cadence,raw_landing_dir,normalized_output_path,evidence_log_path,query_seed_file,notes,url',
                        f'seed-11,11,Google Places API,place,tier1,market_proxy,automatable,places_api_search,api,Egypt,weekly,{raw_dir},{normalized_dir / "seed-11.json"},{collection_dir / "evidence-capture-log.csv"},places-query-seeds.csv,Places request spec test,https://example.test/places',
                    ]
                )
                + '\n'
            )
            (collection_dir / 'collection-run-manifest.csv').write_text(
                '\n'.join(
                    [
                        'run_id,source_id,source_name,collection_stage,adapter_type,priority,district_scope,scheduled_run_utc,expected_artifact,query_seed_file,status,failure_action,notes',
                        f'run-seed-11,seed-11,Google Places API,market_proxy,places_api_search,high,Nasr City; Imbaba,2026-03-25T00:00:00Z,{normalized_dir / "seed-11.json"},places-query-seeds.csv,ready,log_and_escalate,Place query matching test run',
                    ]
                )
                + '\n'
            )
            (plans_dir / 'connector_readiness.csv').write_text(
                '\n'.join(
                    [
                        'connector_id,status,priority,owner,source_id,source_name,adapter_type,region_or_country,query_seed_file,credential_state,last_checked_utc,next_action,notes,url',
                        'CON-011,needs_credentials,high,proxy_accountant,seed-11,Google Places API,places_api_search,Regional,places-query-seeds.csv,api_key_required,,Configure an API key,Need credentials before live execution,https://developers.google.com/maps/documentation/places/web-service',
                    ]
                )
                + '\n'
            )
            (source_specs_dir / 'google_places_cairo_giza_collection.json').write_text(
                json.dumps(
                    {
                        'source_id': 'seed-11',
                        'source_name': 'Google Places API',
                        'adapter_type': 'places_api_search',
                        'request_method': 'POST',
                        'request_params': {
                            'fields': ['place_id', 'name', 'formatted_address'],
                            'capture_limit': 50,
                        },
                        'extraction_targets': ['place_id', 'name'],
                        'raw_path_template': 'artifacts/collection/raw/seed-11/google_places/<run_id>.json',
                        'normalized_path_template': 'artifacts/collection/normalized/seed-11.json',
                        'quality_checks': ['Limit fields to the minimum useful set.'],
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

            result = bootstrap.execute_action(args, action='collect_ready')

            self.assertEqual(result['status'], 'ok')
            payload = json.loads((normalized_dir / 'seed-11.json').read_text())
            self.assertEqual(payload['capture_status'], 'staged_external')
            self.assertEqual(payload['query_count'], 3)
            self.assertEqual(payload['matched_queries'], 2)
            self.assertEqual(payload['district_scope'], 'Nasr City; Imbaba')
            self.assertEqual(payload['connector_status'], 'needs_credentials')
            self.assertEqual(payload['credential_state'], 'api_key_required')
            self.assertEqual(payload['connector_owner'], 'proxy_accountant')
            self.assertEqual(payload['request_method'], 'POST')
            self.assertTrue(payload['query_seed_path'].endswith('collection/places-query-seeds.csv'))
            self.assertTrue(payload['source_spec_path'].endswith('google_places_cairo_giza_collection.json'))
            self.assertEqual(payload['execution_contract']['search_endpoint'], 'https://places.googleapis.com/v1/places:searchText')
            self.assertEqual(payload['execution_contract']['credential_state'], 'api_key_required')
            self.assertEqual(len(payload['execution_contract']['per_query_requests']), 2)
            self.assertEqual(len(payload['queries']), 2)
            raw_payload = json.loads((raw_dir / 'run-seed-11.json').read_text())
            self.assertEqual(raw_payload['matched_query_count'], 2)
            self.assertEqual(len(raw_payload['queries']), 2)
            self.assertEqual({row['district_name'] for row in raw_payload['queries']}, {'Nasr City', 'Imbaba'})
            self.assertEqual(raw_payload['connector_status'], 'needs_credentials')
            self.assertEqual(raw_payload['credential_state'], 'api_key_required')
            self.assertEqual(raw_payload['connector_owner'], 'proxy_accountant')
            self.assertEqual(raw_payload['connector_next_action'], 'Configure an API key')
            self.assertEqual(raw_payload['request_method'], 'POST')
            self.assertTrue(raw_payload['source_spec_path'].endswith('google_places_cairo_giza_collection.json'))
            self.assertEqual(raw_payload['execution_contract']['search_endpoint'], 'https://places.googleapis.com/v1/places:searchText')
            self.assertEqual(raw_payload['execution_contract']['credential_state'], 'api_key_required')

    def test_collect_ready_action_stages_overpass_turbo_with_source_spec_contract(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            collection_dir = tmp_path / 'collection'
            raw_dir = collection_dir / 'raw' / 'seed-17'
            normalized_dir = collection_dir / 'normalized'
            plans_dir = tmp_path / 'plans'
            source_specs_dir = plans_dir / 'source_specs'
            collection_dir.mkdir(parents=True, exist_ok=True)
            raw_dir.mkdir(parents=True, exist_ok=True)
            normalized_dir.mkdir(parents=True, exist_ok=True)
            plans_dir.mkdir(parents=True, exist_ok=True)
            source_specs_dir.mkdir(parents=True, exist_ok=True)

            (collection_dir / 'overpass-query-seeds.csv').write_text(
                '\n'.join(
                    [
                        'query_id,country,district_name,place_source,place_type,tag_key,tag_value,query_text,priority,status,notes',
                        'ovp-001,Egypt,Nasr City,Overpass,bakery,shop,bakery,shop=bakery in Nasr City,high,ready,Fixture row',
                        'ovp-002,Egypt,Imbaba,Overpass,cafe,amenity,cafe,amenity=cafe in Imbaba,high,ready,Fixture row',
                        'ovp-003,Lebanon,Beirut,Overpass,bakery,shop,bakery,shop=bakery in Beirut,high,ready,Fixture row',
                    ]
                )
                + '\n'
            )
            (collection_dir / 'source-adapter-registry.csv').write_text(
                '\n'.join(
                    [
                        'source_id,rank,source_name,source_family,priority_tier,collection_stage,collection_mode,adapter_type,access_type,region_or_country,refresh_cadence,raw_landing_dir,normalized_output_path,evidence_log_path,query_seed_file,notes,url',
                        f'seed-17,17,Overpass Turbo,place,tier2,market_proxy,manual_review,overpass_query,web,Regional,as needed,{raw_dir},{normalized_dir / "seed-17.json"},{collection_dir / "evidence-capture-log.csv"},overpass-query-seeds.csv,Overpass Turbo request spec test,https://overpass-turbo.eu/',
                    ]
                )
                + '\n'
            )
            (collection_dir / 'collection-run-manifest.csv').write_text(
                '\n'.join(
                    [
                        'run_id,source_id,source_name,collection_stage,adapter_type,priority,district_scope,scheduled_run_utc,expected_artifact,query_seed_file,status,failure_action,notes',
                        f'run-seed-17,seed-17,Overpass Turbo,market_proxy,overpass_query,medium,Nasr City; Imbaba,2026-03-25T00:00:00Z,{normalized_dir / "seed-17.json"},overpass-query-seeds.csv,ready,log_and_escalate,Overpass Turbo query matching test run',
                    ]
                )
                + '\n'
            )
            (plans_dir / 'connector_readiness.csv').write_text(
                '\n'.join(
                    [
                        'connector_id,status,priority,owner,source_id,source_name,adapter_type,region_or_country,query_seed_file,credential_state,last_checked_utc,next_action,notes,url',
                        'CON-017,ready,low,proxy_accountant,seed-17,Overpass Turbo,overpass_query,Regional,overpass-query-seeds.csv,public_endpoint,,Keep Overpass queries bounded and monitor endpoint policy, latency, and query failures.,Use for analyst iteration before automating query packs.,https://overpass-turbo.eu/',
                    ]
                )
                + '\n'
            )
            (source_specs_dir / 'overpass_turbo_cairo_giza_collection.json').write_text(
                json.dumps(
                    {
                        'source_id': 'seed-17',
                        'source_name': 'Overpass Turbo',
                        'adapter_type': 'overpass_query',
                        'request_method': 'POST',
                        'request_params': {
                            'capture_limit': 200,
                        },
                        'extraction_targets': ['overpass_turbo_query', 'osm_id', 'name'],
                        'raw_path_template': 'artifacts/collection/raw/seed-17/overpass_turbo/<run_id>.json',
                        'normalized_path_template': 'artifacts/collection/normalized/seed-17.json',
                        'quality_checks': ['Preserve the analyst query text and exported Turbo link together.'],
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

            result = bootstrap.execute_action(args, action='collect_ready')

            self.assertEqual(result['status'], 'ok')
            payload = json.loads((normalized_dir / 'seed-17.json').read_text())
            self.assertEqual(payload['capture_status'], 'staged_external')
            self.assertEqual(payload['matched_queries'], 2)
            self.assertEqual(payload['district_scope'], 'Nasr City; Imbaba')
            self.assertEqual(payload['connector_status'], 'ready')
            self.assertEqual(payload['credential_state'], 'public_endpoint')
            self.assertEqual(payload['request_method'], 'POST')
            self.assertTrue(payload['query_seed_path'].endswith('collection/overpass-query-seeds.csv'))
            self.assertTrue(payload['source_spec_path'].endswith('overpass_turbo_cairo_giza_collection.json'))
            self.assertEqual(payload['execution_contract']['interpreter_url'], 'https://overpass-api.de/api/interpreter')
            self.assertEqual(payload['execution_contract']['credential_state'], 'public_endpoint')
            self.assertEqual(len(payload['execution_contract']['per_query_requests']), 2)
            self.assertEqual(len(payload['queries']), 2)
            raw_payload = json.loads((raw_dir / 'run-seed-17.json').read_text())
            self.assertEqual(raw_payload['matched_query_count'], 2)
            self.assertEqual(len(raw_payload['queries']), 2)
            self.assertEqual({row['district_name'] for row in raw_payload['queries']}, {'Nasr City', 'Imbaba'})
            self.assertEqual(raw_payload['connector_status'], 'ready')
            self.assertEqual(raw_payload['credential_state'], 'public_endpoint')
            self.assertEqual(raw_payload['request_method'], 'POST')
            self.assertTrue(raw_payload['source_spec_path'].endswith('overpass_turbo_cairo_giza_collection.json'))
            self.assertEqual(raw_payload['execution_contract']['interpreter_url'], 'https://overpass-api.de/api/interpreter')
            self.assertEqual(raw_payload['execution_contract']['credential_state'], 'public_endpoint')

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
                        'secondary_raw_path': 'artifacts/collection/raw/seed-27/detail.html',
                        'verification_updates': {
                            'source_id': 'seed-27',
                            'last_checked_utc': '2026-03-25T12:00:00Z',
                            'last_published_date': '2026-03-20',
                            'latest_period_covered': '',
                            'claim_date_utc': '',
                            'owner': '',
                            'evidence_link': 'https://data.humdata.org/api/3/action/package_show?id=global-market-monitor',
                            'mirror_evidence_link': 'https://example.test/mirror/global-market-monitor',
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
            self.assertEqual(findings_rows['seed-27']['mirror_evidence_link'], 'https://example.test/mirror/global-market-monitor')
            self.assertEqual(findings_rows['seed-27']['evidence_path'], 'artifacts/collection/raw/seed-27/detail.html')
            self.assertEqual(accounting_rows['seed-27']['last_published_date'], '2026-03-20')
            self.assertEqual(accounting_rows['seed-27']['mirror_evidence_link'], 'https://example.test/mirror/global-market-monitor')
            self.assertEqual(accounting_rows['seed-27']['evidence_path'], 'artifacts/collection/raw/seed-27/detail.html')
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
