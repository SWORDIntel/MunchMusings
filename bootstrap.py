#!/usr/bin/env python3
# Name: bootstrap.py | Version: v1.3.0
# Purpose: GUI/TUI/CLI launcher for the MunchMusings seeded source registry.
# Features: bootstrap, validation, seed inspection, GUI progress, versioned outputs, safe defaults, JSON summaries.

import argparse
import csv
import html as html_lib
import hashlib
import json
import logging
import os
import re
import subprocess
import sys
import xml.etree.ElementTree as ET
from collections import Counter
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime, parsedate_to_datetime
from pathlib import Path
from queue import Empty, Queue
from threading import Thread
from tkinter import BOTH, END, DISABLED, NORMAL, BooleanVar, StringVar, Text, Tk, ttk
from typing import Any, Callable
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urljoin, urlparse, urlunparse
from urllib.request import Request, urlopen

import requests

ProgressCallback = Callable[[int, int, str], None]
RECENT_ACCOUNTING_FINDINGS_FIELDS = [
    'source_id',
    'last_checked_utc',
    'last_published_date',
    'latest_period_covered',
    'claim_date_utc',
    'owner',
    'evidence_link',
    'mirror_evidence_link',
    'evidence_path',
    'status',
    'next_action',
    'notes',
]
MONTH_NAME_TO_NUMBER = {
    'january': 1,
    'february': 2,
    'march': 3,
    'april': 4,
    'may': 5,
    'june': 6,
    'july': 7,
    'august': 8,
    'september': 9,
    'october': 10,
    'november': 11,
    'december': 12,
    'ינואר': 1,
    'פברואר': 2,
    'מרץ': 3,
    'אפריל': 4,
    'מאי': 5,
    'יוני': 6,
    'יולי': 7,
    'אוגוסט': 8,
    'ספטמבר': 9,
    'אוקטובר': 10,
    'נובמבר': 11,
    'דצמבר': 12,
}


def configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format='[%(levelname)s] %(message)s')


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description='Bootstrap MunchMusings source artifacts from the canonical preseed file.'
    )
    parser.add_argument('--input', default='seed/preseed_sources_v1.json', help='Path to the canonical seeded source JSON file.')
    parser.add_argument('--output-dir', default='artifacts/bootstrap', help='Directory where versioned JSON and CSV outputs will be written.')
    parser.add_argument('--docs-csv', default='docs/source-registry.csv', help='Path to the repo-tracked CSV registry that should be refreshed from the seed.')
    parser.add_argument('--pack-dir', default='artifacts/v0_1', help='Directory where the v0.1 operator pack should be written.')
    parser.add_argument('--plans-dir', default='plans', help='Directory where execution plans and recent-accounting artifacts will be written.')
    parser.add_argument('--collection-dir', default='artifacts/collection', help='Directory where collection pipeline scaffolds and run manifests will be written.')
    parser.add_argument('--briefing-dir', default='artifacts/briefings', help='Directory where zone briefing packs will be written.')
    parser.add_argument('--version-prefix', default='preseed_sources_v', help='Filename prefix for generated JSON and CSV artifacts.')
    parser.add_argument('--force-version', type=int, default=None, help='Force a specific output version number instead of auto-incrementing.')
    parser.add_argument('--verbose', action='store_true', help='Enable verbose logging.')
    parser.add_argument('--tui', action='store_true', help='Launch the terminal UI even if arguments are supplied via stdin piping.')
    parser.add_argument('--gui', action='store_true', help='Launch the GUI bootstrap window with progress tracking.')
    parser.add_argument('--check', action='store_true', help='Validate the seed and print a JSON summary without writing artifacts.')
    parser.add_argument('--inspect', action='store_true', help='Print a human-readable summary of the seeded sources without writing artifacts.')
    parser.add_argument('--scaffold-v0', action='store_true', help='Generate the v0.1 operator pack from the canonical seed.')
    parser.add_argument('--recent-accounting', action='store_true', help='Refresh the plans/recent-accounting ledger and summary from the canonical seed.')
    parser.add_argument('--scaffold-collection', action='store_true', help='Generate the collection pipeline pack, including source adapters, query seeds, and evidence-capture logs.')
    parser.add_argument('--verification-sprint', action='store_true', help='Refresh recent-accounting and rebuild the canonical source-verification sprint tracker in plans/.')
    parser.add_argument('--brief-zone', action='store_true', help='Generate a zone-level public-source briefing pack from the current ledgers and manifests.')
    parser.add_argument('--operating-cycle', action='store_true', help='Run collection, verification, and briefing as one dated operating cycle.')
    parser.add_argument('--zone-name', default='Cairo/Giza pilot', help='Zone name for the generated briefing pack.')
    parser.add_argument('--zone-country', default='Egypt', help='Country label for the generated briefing pack.')
    parser.add_argument('--analyst', default='system', help='Analyst name or ID used in generated briefing packs.')
    parser.add_argument('--reviewer', default='pending_review', help='Reviewer name or ID used in generated briefing packs.')
    parser.add_argument('--collect-ready', action='store_true', help='Execute ready collection runs for direct-source adapters and stage request specs for external-query adapters.')
    parser.add_argument('--max-runs', type=int, default=5, help='Maximum number of collection runs to process in one invocation of --collect-ready.')
    return parser.parse_args()


def load_seed(path: Path) -> list[dict[str, Any]]:
    logging.debug('Loading canonical seed file from %s', path)
    data = json.loads(path.read_text())
    if not isinstance(data, list):
        raise ValueError('Seed JSON must contain a top-level list.')
    return data


def validate_seed(records: list[dict[str, Any]]) -> None:
    logging.debug('Validating seeded source records')
    required = {
        'rank',
        'source_name',
        'source_family',
        'priority_tier',
        'region_or_country',
        'access_type',
        'refresh_cadence',
        'output_formats',
        'primary_use',
        'notes',
        'url',
    }
    if len(records) < 20:
        raise ValueError(f'Expected at least 20 seeded sources, found {len(records)}.')

    seen_ranks = set()
    seen_names = set()
    for record in records:
        missing = required - record.keys()
        if missing:
            raise ValueError(f'Missing required fields for {record.get("source_name", "<unknown>")}: {sorted(missing)}')
        rank = record['rank']
        name = record['source_name']
        if rank in seen_ranks:
            raise ValueError(f'Duplicate rank detected: {rank}')
        if name in seen_names:
            raise ValueError(f'Duplicate source_name detected: {name}')
        if record['output_formats'] != ['json', 'csv']:
            raise ValueError(f'Output formats for {name} must be ["json", "csv"].')
        seen_ranks.add(rank)
        seen_names.add(name)

    expected_ranks = set(range(1, len(records) + 1))
    if seen_ranks != expected_ranks:
        raise ValueError(f'Seed ranks must be exactly 1 through {len(records)}.')


def build_source_summary(records: list[dict[str, Any]]) -> dict[str, Any]:
    family_counts: dict[str, int] = {}
    tier_counts: dict[str, int] = {}
    region_counts: dict[str, int] = {}

    for record in records:
        family_counts[record['source_family']] = family_counts.get(record['source_family'], 0) + 1
        tier_counts[record['priority_tier']] = tier_counts.get(record['priority_tier'], 0) + 1
        region_counts[record['region_or_country']] = region_counts.get(record['region_or_country'], 0) + 1

    return {
        'total_sources': len(records),
        'family_counts': dict(sorted(family_counts.items())),
        'tier_counts': dict(sorted(tier_counts.items())),
        'region_counts': dict(sorted(region_counts.items())),
        'top_five_sources': [record['source_name'] for record in sorted(records, key=lambda item: item['rank'])[:5]],
    }


def render_source_summary(records: list[dict[str, Any]]) -> str:
    summary = build_source_summary(records)
    lines = [
        'MunchMusings Seed Summary',
        '=========================',
        f"Total sources : {summary['total_sources']}",
        '',
        'Tier counts:',
    ]
    lines.extend([f"- {key}: {value}" for key, value in summary['tier_counts'].items()])
    lines.append('')
    lines.append('Source family counts:')
    lines.extend([f"- {key}: {value}" for key, value in summary['family_counts'].items()])
    lines.append('')
    lines.append('Top 5 ranked sources:')
    lines.extend([f"- {name}" for name in summary['top_five_sources']])
    return '\n'.join(lines)


def next_version(output_dir: Path, prefix: str, forced: int | None) -> int:
    if forced is not None:
        logging.info('Using forced output version: %s', forced)
        return forced

    existing = sorted(output_dir.glob(f'{prefix}*.json'))
    versions = []
    for path in existing:
        suffix = path.stem.replace(prefix, '')
        if suffix.isdigit():
            versions.append(int(suffix))
    version = (max(versions) + 1) if versions else 1
    logging.info('Auto-selected output version: %s', version)
    return version


def write_json(records: list[dict[str, Any]], path: Path) -> None:
    logging.info('Writing JSON artifact: %s', path)
    path.write_text(json.dumps(records, indent=2, ensure_ascii=False) + '\n')


def write_csv(records: list[dict[str, Any]], path: Path) -> None:
    logging.info('Writing CSV artifact: %s', path)
    fieldnames = [
        'rank',
        'source_name',
        'source_family',
        'priority_tier',
        'region_or_country',
        'access_type',
        'refresh_cadence',
        'output_formats',
        'primary_use',
        'notes',
        'url',
    ]
    with path.open('w', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for record in records:
            row = dict(record)
            row['output_formats'] = ';'.join(record['output_formats'])
            writer.writerow(row)


def write_rows_csv(fieldnames: list[str], rows: list[dict[str, Any]], path: Path) -> None:
    logging.info('Writing CSV artifact: %s', path)
    with path.open('w', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def write_markdown(text: str, path: Path) -> None:
    logging.info('Writing Markdown artifact: %s', path)
    path.write_text(text.rstrip() + '\n')


def collection_mode(access_type: str) -> str:
    if 'api' in access_type or 'json' in access_type or 'csv' in access_type:
        return 'automatable'
    if 'pdf' in access_type:
        return 'manual_or_hybrid'
    return 'manual_review'


def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def expected_recency_window(refresh_cadence: str) -> str:
    windows = {
        'weekly': '8 days',
        'biweekly': '16 days',
        'monthly': '35 days',
        'quarterly': '100 days',
        'annual': '400 days',
        'periodic': 'manual review',
        'as needed': 'manual review',
        'occasional': 'manual review',
    }
    return windows.get(refresh_cadence, 'manual review')


def recency_window_days(refresh_cadence: str) -> int | None:
    windows = {
        'weekly': 8,
        'biweekly': 16,
        'monthly': 35,
        'quarterly': 100,
        'annual': 400,
    }
    return windows.get(refresh_cadence)


def parse_accounting_date(value: str) -> datetime | None:
    value = value.strip()
    if not value:
        return None

    normalized = value.replace('Z', '+00:00')
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        try:
            parsed = datetime.strptime(value, '%Y-%m-%d')
        except ValueError:
            return None
        return parsed.replace(tzinfo=timezone.utc)

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def month_end_date(year: int, month: int) -> datetime:
    if month == 12:
        return datetime(year + 1, 1, 1, tzinfo=timezone.utc) - timedelta(days=1)
    return datetime(year, month + 1, 1, tzinfo=timezone.utc) - timedelta(days=1)


def latest_period_end_date(value: str) -> datetime | None:
    normalized = value.strip()
    if not normalized:
        return None

    candidates: list[datetime] = []
    for part in re.split(r'\s*/\s*|\s+-\s+|\s+to\s+', normalized):
        item = part.strip()
        if not item:
            continue
        parsed = parse_accounting_date(item)
        if parsed is not None:
            candidates.append(parsed)
            continue
        month_match = re.fullmatch(r'(\d{4})-(\d{2})', item)
        if month_match:
            candidates.append(month_end_date(int(month_match.group(1)), int(month_match.group(2))))
            continue
        named_period = extract_named_month_period(item)
        if named_period:
            year, month = named_period.split('-')
            candidates.append(month_end_date(int(year), int(month)))
    return max(candidates) if candidates else None


def derive_recency_status(
    refresh_cadence: str,
    last_published_date: str,
    status: str,
    latest_period_covered: str = '',
) -> str:
    if status == 'blocked':
        return 'blocked'
    if refresh_cadence == 'periodic':
        period_end = latest_period_end_date(latest_period_covered)
        if period_end is not None and period_end.date() >= utc_now().date():
            return 'current'
    if status == 'manual_review':
        return 'manual_review'

    published_at = parse_accounting_date(last_published_date)
    window_days = recency_window_days(refresh_cadence)
    if published_at is None:
        return 'unknown'
    if window_days is None:
        return 'manual_review'

    age_days = (utc_now() - published_at).days
    if age_days <= window_days:
        return 'current'
    if age_days <= int(window_days * 1.5):
        return 'due_now'
    return 'overdue'


def next_check_due_utc(refresh_cadence: str, last_checked_utc: str) -> str:
    checked_at = parse_accounting_date(last_checked_utc)
    window_days = recency_window_days(refresh_cadence)
    if checked_at is None or window_days is None:
        return ''
    return (checked_at + timedelta(days=window_days)).isoformat().replace('+00:00', 'Z')


def load_existing_recent_accounting(path: Path) -> dict[str, dict[str, str]]:
    if not path.exists():
        return {}

    with path.open(newline='') as handle:
        reader = csv.DictReader(handle)
        return {row['source_id']: row for row in reader if row.get('source_id')}


def merge_recent_accounting_findings(
    existing_rows: dict[str, dict[str, str]],
    findings_rows: dict[str, dict[str, str]],
) -> dict[str, dict[str, str]]:
    merged_rows = {source_id: dict(row) for source_id, row in existing_rows.items()}
    for source_id, finding in findings_rows.items():
        target = dict(merged_rows.get(source_id, {}))
        target['source_id'] = source_id
        for field in RECENT_ACCOUNTING_FINDINGS_FIELDS:
            if field == 'source_id':
                continue
            value = finding.get(field, '').strip()
            if value:
                target[field] = value
        if target:
            merged_rows[source_id] = target
    return merged_rows


def build_recent_accounting_rows(
    records: list[dict[str, Any]],
    existing_rows: dict[str, dict[str, str]] | None = None,
) -> list[dict[str, Any]]:
    existing_rows = existing_rows or {}
    rows = []

    for record in sorted(records, key=lambda item: item['rank']):
        if record.get('source_family') == 'place':
            continue
        source_id = f"seed-{record['rank']:02d}"
        existing = existing_rows.get(source_id, {})
        last_checked_utc = existing.get('last_checked_utc', '')
        last_published_date = existing.get('last_published_date', '')
        latest_period_covered = existing.get('latest_period_covered', '')
        claim_date_utc = existing.get('claim_date_utc', '')
        owner = existing.get('owner', '')
        evidence_link = existing.get('evidence_link', '')
        mirror_evidence_link = existing.get('mirror_evidence_link', '')
        evidence_path = existing.get('evidence_path', '')
        status = existing.get('status', 'pending_review') or 'pending_review'
        next_action = existing.get('next_action', 'Verify latest publication date, coverage window, and evidence link.')
        notes = existing.get('notes', record['notes']) or record['notes']

        rows.append(
            {
                'source_id': source_id,
                'rank': record['rank'],
                'source_name': record['source_name'],
                'source_family': record['source_family'],
                'priority_tier': record['priority_tier'],
                'region_or_country': record['region_or_country'],
                'refresh_cadence': record['refresh_cadence'],
                'expected_recency_window': expected_recency_window(record['refresh_cadence']),
                'last_checked_utc': last_checked_utc,
                'last_published_date': last_published_date,
                'latest_period_covered': latest_period_covered,
                'claim_date_utc': claim_date_utc,
                'recency_status': derive_recency_status(
                    record['refresh_cadence'],
                    last_published_date,
                    status,
                    latest_period_covered,
                ),
                'owner': owner,
                'evidence_link': evidence_link,
                'mirror_evidence_link': mirror_evidence_link,
                'evidence_path': evidence_path,
                'status': status,
                'next_check_due_utc': next_check_due_utc(record['refresh_cadence'], last_checked_utc),
                'next_action': next_action,
                'notes': notes,
                'url': record['url'],
            }
        )

    return rows


def render_recent_accounting_summary(rows: list[dict[str, Any]]) -> str:
    recency_buckets = ['current', 'due_now', 'overdue', 'blocked', 'manual_review', 'unknown']
    status_counts = {bucket: 0 for bucket in recency_buckets}
    for row in rows:
        status_counts[row['recency_status']] = status_counts.get(row['recency_status'], 0) + 1

    unassigned = sum(1 for row in rows if not row['owner'])
    pending = sum(1 for row in rows if row['status'] == 'pending_review')
    generated_at = utc_now().isoformat().replace('+00:00', 'Z')
    tier1_noncurrent = [
        row for row in rows if row.get('priority_tier') == 'tier1' and row.get('recency_status') != 'current'
    ]
    tier1_noncurrent_labels = ', '.join(row['source_id'] for row in tier1_noncurrent[:5]) or 'none'
    unknown_or_overdue = [row for row in rows if row.get('recency_status') in {'unknown', 'overdue'}]
    if tier1_noncurrent:
        action_one = f"Resolve the remaining tier-1 non-current rows first: {tier1_noncurrent_labels}."
    else:
        action_one = 'Keep the tier-1 ledger current and only open new accounting tasks when a row rolls out of window.'
    if unknown_or_overdue:
        action_two = 'Treat `unknown` and `overdue` rows as the priority backlog before due-now or manual-review cleanup.'
    else:
        action_two = 'The hard freshness backlog is clear; focus on the remaining `due_now` or `manual_review` rows and their next actions.'
    if unassigned:
        action_three = 'Backfill `owner` for the remaining unassigned rows so queue ownership matches the ledger.'
    else:
        action_three = 'Keep `owner` assignments aligned with the queue and preserve analyst-entered notes on refresh.'

    return """# Recent Accounting Summary

_Generated: {generated_at}._

## Why this exists
- This ledger is the execution checkpoint for keeping source claims recent, attributable, and reviewable.
- `recency_status` is derived from `refresh_cadence` plus `last_published_date`; blank publication dates remain `unknown`.
- Re-running `python bootstrap.py --recent-accounting` preserves analyst-entered fields and refreshes derived status columns.
- Connector/query sources are tracked separately in `plans/connector_readiness.csv` and excluded from this date-based ledger.

## Snapshot
- Total tracked sources: {total_sources}
- Current: {current}
- Due now: {due_now}
- Overdue: {overdue}
- Blocked: {blocked}
- Manual review cadence: {manual_review}
- Unknown recency: {unknown}
- Unassigned owners: {unassigned}
- Pending review rows: {pending}

## Immediate actions
1. {action_one}
2. {action_two}
3. {action_three}
""".format(
        generated_at=generated_at,
        total_sources=len(rows),
        current=status_counts.get('current', 0),
        due_now=status_counts.get('due_now', 0),
        overdue=status_counts.get('overdue', 0),
        blocked=status_counts.get('blocked', 0),
        manual_review=status_counts.get('manual_review', 0),
        unknown=status_counts.get('unknown', 0),
        unassigned=unassigned,
        pending=pending,
        action_one=action_one,
        action_two=action_two,
        action_three=action_three,
    )


def write_recent_accounting_pack(args: argparse.Namespace, records: list[dict[str, Any]]) -> dict[str, Any]:
    plans_dir = Path(args.plans_dir)
    plans_dir.mkdir(parents=True, exist_ok=True)

    accounting_path = plans_dir / 'recent_accounting.csv'
    summary_path = plans_dir / 'recent_accounting.md'
    findings_path = sync_source_verification_findings_from_collection(plans_dir, Path(args.collection_dir))
    existing_rows = load_existing_recent_accounting(accounting_path)
    findings_rows = load_existing_rows_by_key(findings_path, 'source_id')
    accounting_rows = build_recent_accounting_rows(
        records,
        existing_rows=merge_recent_accounting_findings(existing_rows, findings_rows),
    )

    write_rows_csv(
        [
            'source_id',
            'rank',
            'source_name',
            'source_family',
            'priority_tier',
            'region_or_country',
            'refresh_cadence',
            'expected_recency_window',
            'last_checked_utc',
            'last_published_date',
            'latest_period_covered',
            'claim_date_utc',
            'recency_status',
            'owner',
            'evidence_link',
            'mirror_evidence_link',
            'evidence_path',
            'status',
            'next_check_due_utc',
            'next_action',
            'notes',
            'url',
        ],
        accounting_rows,
        accounting_path,
    )
    write_markdown(render_recent_accounting_summary(accounting_rows), summary_path)

    return {
        'status': 'ok',
        'action': 'recent_accounting',
        'input': str(Path(args.input)),
        'plans_dir': str(plans_dir),
        'recent_accounting_csv': str(accounting_path),
        'recent_accounting_summary': str(summary_path),
        'source_verification_findings_csv': str(findings_path),
        'source_summary': build_source_summary(records),
        'unknown_count': sum(1 for row in accounting_rows if row['recency_status'] == 'unknown'),
        'overdue_count': sum(1 for row in accounting_rows if row['recency_status'] == 'overdue'),
        'launcher_mode': getattr(args, 'launcher_mode', 'cli'),
    }


def verification_lane(source_family: str) -> str:
    if source_family == 'macro_price':
        return 'macro_price'
    if source_family == 'humanitarian_feed':
        return 'humanitarian_feed'
    if source_family == 'market_monitor':
        return 'market_monitor'
    if source_family in {'humanitarian', 'trade'}:
        return 'baseline_refresh'
    if source_family in {'place', 'market', 'retail_catalogue'}:
        return 'proxy_refresh'
    return 'supporting_review'


def verification_owner(source_family: str) -> str:
    owners = {
        'macro_price': 'macro_price_monitor',
        'humanitarian_feed': 'humanitarian_feed_monitor',
        'market_monitor': 'market_monitor_monitor',
        'humanitarian': 'source_monitor',
        'trade': 'trade_monitor',
        'place': 'proxy_accountant',
        'market': 'proxy_accountant',
        'retail_catalogue': 'proxy_accountant',
    }
    return owners.get(source_family, 'source_monitor')


def verification_priority(priority_tier: str, recency_status: str) -> str:
    if priority_tier == 'tier1' and recency_status in {'unknown', 'overdue', 'due_now'}:
        return 'high'
    if priority_tier == 'tier1':
        return 'medium'
    return 'low'


def verification_status_for_accounting_row(row: dict[str, str]) -> str:
    if row.get('evidence_link') and row.get('last_published_date'):
        return 'verified'
    if row.get('evidence_link') or row.get('last_checked_utc'):
        return 'research_complete'
    return 'pending'


def preserve_url(existing: dict[str, str], field: str, default: str) -> str:
    value = existing.get(field, '').strip()
    if value.startswith('http://') or value.startswith('https://'):
        return value
    return default


def preserve_cross_host_url(existing: dict[str, str], field: str, default: str) -> str:
    value = existing.get(field, '').strip()
    if not (value.startswith('http://') or value.startswith('https://')):
        return default

    existing_host = urlparse(value).netloc.lower()
    default_host = urlparse(default).netloc.lower()
    legacy_host_map = {
        'reporting.unhcr.org': {'data.unhcr.org'},
        'cas.gov.lb': {'beta.cas.gov.lb', 'www.beta.cas.gov.lb'},
        'comtrade.un.org': {'comtradeplus.un.org'},
        'centre.humdata.org': {'data.humdata.org'},
    }
    if default_host in legacy_host_map.get(existing_host, set()):
        return default
    if existing_host and default_host and existing_host != default_host:
        return value
    return default


def preserve_text(existing: dict[str, str], field: str, default: str) -> str:
    value = existing.get(field, '').strip()
    if not value:
        return default
    if parse_accounting_date(value) is not None:
        return default
    return value


def load_existing_rows_by_key(path: Path, key_field: str) -> dict[str, dict[str, str]]:
    if not path.exists():
        return {}
    with path.open(newline='') as handle:
        return {
            row[key_field]: row
            for row in csv.DictReader(handle)
            if row.get(key_field)
        }


def build_source_verification_rows(
    accounting_rows: list[dict[str, str]],
    existing_rows: dict[str, dict[str, str]] | None = None,
) -> list[dict[str, Any]]:
    existing_rows = existing_rows or {}
    rows = []
    for row in accounting_rows:
        source_id = row.get('source_id', '')
        if row.get('priority_tier') != 'tier1' and row.get('source_family') not in {'macro_price', 'humanitarian_feed', 'market_monitor'}:
            continue
        existing = existing_rows.get(source_id, {})
        if row.get('recency_status') == 'current' and not existing:
            continue
        lane = verification_lane(row.get('source_family', ''))
        derived_status = verification_status_for_accounting_row(row)
        source_state_changed = any(
            [
                existing.get('latest_visible_date', '').strip() != row.get('last_published_date', '').strip(),
                existing.get('latest_period_covered', '').strip() != row.get('latest_period_covered', '').strip(),
                existing.get('recency_status', '').strip() != row.get('recency_status', '').strip(),
                existing.get('evidence_link', '').strip() != row.get('evidence_link', '').strip(),
            ]
        )
        rows.append(
            {
                'sprint_id': existing.get('sprint_id') or f"SV-{row.get('rank', '').zfill(3)}",
                'status': 'blocked' if existing.get('status') == 'blocked' else derived_status,
                'priority': verification_priority(row.get('priority_tier', ''), row.get('recency_status', 'unknown')),
                'lane': lane,
                'owner': preserve_text(existing, 'owner', verification_owner(row.get('source_family', ''))),
                'source_id': source_id,
                'source_name': row.get('source_name', ''),
                'source_family': row.get('source_family', ''),
                'priority_tier': row.get('priority_tier', ''),
                'region_or_country': row.get('region_or_country', ''),
                'best_current_page': preserve_cross_host_url(existing, 'best_current_page', row.get('url', '')),
                'latest_visible_date': row.get('last_published_date', ''),
                'latest_period_covered': row.get('latest_period_covered', ''),
                'recency_status': row.get('recency_status', ''),
                'evidence_link': row.get('evidence_link', '') if source_state_changed else preserve_url(existing, 'evidence_link', row.get('evidence_link', '')),
                'mirror_evidence_link': row.get('mirror_evidence_link', ''),
                'last_checked_utc': row.get('last_checked_utc', ''),
                'next_action': row.get('next_action', '') if source_state_changed else preserve_text(existing, 'next_action', row.get('next_action', '')),
                'notes': preserve_text(existing, 'notes', row.get('notes', '')),
            }
        )
    return rows


def render_source_verification_summary(rows: list[dict[str, Any]]) -> str:
    status_counts: dict[str, int] = {}
    lane_counts: dict[str, int] = {}
    for row in rows:
        status_counts[row['status']] = status_counts.get(row['status'], 0) + 1
        lane_counts[row['lane']] = lane_counts.get(row['lane'], 0) + 1
    lines = [
        '# Source Verification Sprint',
        '',
        f"_Generated: {utc_now().isoformat().replace('+00:00', 'Z')}._",
        '',
        '## Purpose',
        '- Keep source verification rerunnable and keyed by `source_id`.',
        '- Preserve analyst-entered state while refreshing derived source metadata from `recent_accounting.csv`.',
        '- Track only date-bearing source rows here; connector/query infrastructure lives in `plans/connector_readiness.csv`.',
        '- Sync verification lanes without duplicating milestone tasks.',
        '',
        '## Snapshot',
    ]
    lines.extend([f"- {key}: {value}" for key, value in sorted(status_counts.items())])
    lines.append('')
    lines.append('## Lanes')
    lines.extend([f"- {key}: {value}" for key, value in sorted(lane_counts.items())])
    lines.append('')
    lines.append('## Rerun')
    lines.append('- `python bootstrap.py --verification-sprint` refreshes the ledger-backed tracker and `VER-*` queue rows.')
    return '\n'.join(lines)


def accounting_queue_status(existing: dict[str, str], recency_status: str) -> str:
    preserved_status = existing.get('status', '').strip()
    if preserved_status in {'in_progress', 'blocked'}:
        return preserved_status
    if recency_status == 'blocked':
        return 'blocked'
    return 'pending'


def accounting_queue_priority(priority_tier: str, recency_status: str) -> str:
    if priority_tier == 'tier1' and recency_status in {'unknown', 'due_now', 'overdue', 'blocked'}:
        return 'high'
    if priority_tier == 'tier1':
        return 'medium'
    return 'low'


def accounting_queue_acceptance_criteria(row: dict[str, str]) -> str:
    source_name = row.get('source_name', 'Source row')
    return (
        f"{source_name} row captures the latest visible publication date, coverage period, evidence link, "
        "and next action, or records an explicit blocker/manual-review note"
    )


def build_accounting_queue_rows(
    accounting_rows: list[dict[str, str]],
    existing_lookup: dict[str, dict[str, str]],
) -> list[dict[str, Any]]:
    rows = []
    for row in accounting_rows:
        recency_status = row.get('recency_status', '')
        if row.get('priority_tier') != 'tier1' or recency_status not in {'unknown', 'due_now', 'overdue', 'manual_review', 'blocked'}:
            continue
        rank = row.get('rank', '').zfill(3)
        task_id = f"ACC-RA-{rank}" if rank else f"ACC-RA-{row.get('source_id', '')}"
        existing = existing_lookup.get(task_id, {})
        target_date = (row.get('next_check_due_utc', '') or '').split('T', 1)[0] or utc_now().date().isoformat()
        rows.append(
            {
                'task_id': task_id,
                'status': accounting_queue_status(existing, recency_status),
                'priority': accounting_queue_priority(row.get('priority_tier', ''), recency_status),
                'agent': row.get('owner', '') or verification_owner(row.get('source_family', '')),
                'region': row.get('region_or_country', ''),
                'source_id': row.get('source_id', ''),
                'artifact': 'plans/recent_accounting.csv',
                'target_date': existing.get('target_date') or target_date,
                'depends_on': '',
                'acceptance_criteria': accounting_queue_acceptance_criteria(row),
            }
        )
    return rows


def build_verification_queue_rows(
    existing_rows: list[dict[str, str]],
    accounting_rows: list[dict[str, str]],
    sprint_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    existing_lookup = {row.get('task_id', ''): row for row in existing_rows if row.get('task_id')}
    accounting_source_ids = {row.get('source_id', '') for row in accounting_rows if row.get('source_id')}
    preserved_rows = []
    for row in existing_rows:
        task_id = row.get('task_id', '')
        if task_id.startswith('VER-'):
            continue
        if task_id.startswith('ACC-RA-') and row.get('artifact') == 'plans/recent_accounting.csv' and row.get('source_id', '') in accounting_source_ids:
            continue
        preserved_rows.append(row)
    lane_map = {
        'VER-002': ('macro_price', 'Macro-price rows have verified latest publication metadata and official evidence links'),
        'VER-003': ('humanitarian_feed', 'Humanitarian-feed rows have verified activity evidence and latest visible recency signals'),
        'VER-004': ('market_monitor', 'Market-monitor row has verified activity evidence and latest visible recency signal'),
    }

    def lane_status(lane: str) -> str:
        rows = [row for row in sprint_rows if row.get('lane') == lane]
        if not rows:
            return ''
        if all(row.get('status') == 'verified' for row in rows):
            return 'completed'
        if any(row.get('status') in {'verified', 'research_complete'} for row in rows):
            return 'in_progress'
        return 'pending'

    accounting_queue_rows = build_accounting_queue_rows(accounting_rows, existing_lookup)
    ver_rows = []
    tracker_row = existing_lookup.get('VER-001', {})
    ver_rows.append(
        {
            'task_id': 'VER-001',
            'status': (
                'completed'
                if sprint_rows and all(row.get('status') == 'verified' for row in sprint_rows)
                else tracker_row.get('status') or 'in_progress'
            ),
            'priority': 'high',
            'agent': 'source_monitor',
            'region': 'Global',
            'source_id': ';'.join(row.get('source_id', '') for row in sprint_rows),
            'artifact': 'plans/source_verification_sprint.csv',
            'target_date': tracker_row.get('target_date') or utc_now().date().isoformat(),
            'depends_on': '',
            'acceptance_criteria': 'Verification tracker exists and captures latest visible dates, evidence links, and next actions for all scoped sources',
        }
    )

    for task_id, (lane, criteria) in lane_map.items():
        existing = existing_lookup.get(task_id, {})
        lane_rows = [row for row in sprint_rows if row.get('lane') == lane]
        status = lane_status(lane)
        if not lane_rows and not existing:
            continue
        if not lane_rows and existing and existing.get('status') not in {'in_progress', 'blocked'}:
            continue
        ver_rows.append(
            {
                'task_id': task_id,
                'status': status or existing.get('status') or 'pending',
                'priority': 'high',
                'agent': existing.get('agent') or f'{lane}_research',
                'region': '|'.join(sorted({row.get('region_or_country', '') for row in lane_rows if row.get('region_or_country')})) or 'Global',
                'source_id': '|'.join(row.get('source_id', '') for row in lane_rows),
                'artifact': 'plans/source_verification_sprint.csv',
                'target_date': existing.get('target_date') or utc_now().date().isoformat(),
                'depends_on': 'VER-001',
                'acceptance_criteria': criteria,
            }
        )
    return preserved_rows + accounting_queue_rows + ver_rows


def write_verification_sprint_pack(args: argparse.Namespace, records: list[dict[str, Any]]) -> dict[str, Any]:
    accounting_result = write_recent_accounting_pack(args, records)
    accounting_rows = load_csv_rows(Path(accounting_result['recent_accounting_csv']))
    plans_dir = Path(args.plans_dir)
    sprint_path = plans_dir / 'source_verification_sprint.csv'
    sprint_summary_path = plans_dir / 'source_verification_sprint.md'
    connector_path = plans_dir / 'connector_readiness.csv'
    connector_summary_path = plans_dir / 'connector_readiness.md'
    work_queue_path = plans_dir / 'work_queue.csv'

    existing_sprint_rows = load_existing_rows_by_key(sprint_path, 'source_id')
    existing_connector_rows = load_existing_rows_by_key(connector_path, 'source_id')
    sprint_rows = build_source_verification_rows(accounting_rows, existing_sprint_rows)
    connector_rows = build_connector_readiness_rows(records, existing_connector_rows)
    existing_work_queue_rows = load_csv_rows(work_queue_path)
    work_queue_rows = build_verification_queue_rows(existing_work_queue_rows, accounting_rows, sprint_rows)

    write_rows_csv(
        [
            'sprint_id',
            'status',
            'priority',
            'lane',
            'owner',
            'source_id',
            'source_name',
            'source_family',
            'priority_tier',
            'region_or_country',
            'best_current_page',
            'latest_visible_date',
            'latest_period_covered',
            'recency_status',
            'evidence_link',
            'mirror_evidence_link',
            'last_checked_utc',
            'next_action',
            'notes',
        ],
        sprint_rows,
        sprint_path,
    )
    write_markdown(render_source_verification_summary(sprint_rows), sprint_summary_path)
    write_rows_csv(
        [
            'connector_id',
            'status',
            'priority',
            'owner',
            'source_id',
            'source_name',
            'adapter_type',
            'region_or_country',
            'query_seed_file',
            'credential_state',
            'last_checked_utc',
            'next_action',
            'notes',
            'url',
        ],
        connector_rows,
        connector_path,
    )
    write_markdown(render_connector_readiness_summary(connector_rows), connector_summary_path)
    write_rows_csv(
        [
            'task_id',
            'status',
            'priority',
            'agent',
            'region',
            'source_id',
            'artifact',
            'target_date',
            'depends_on',
            'acceptance_criteria',
        ],
        work_queue_rows,
        work_queue_path,
    )

    result = {
        'status': 'ok',
        'action': 'verification_sprint',
        'input': str(Path(args.input)),
        'plans_dir': str(plans_dir),
        'recent_accounting_csv': accounting_result['recent_accounting_csv'],
        'recent_accounting_summary': accounting_result['recent_accounting_summary'],
        'verification_sprint_csv': str(sprint_path),
        'verification_sprint_summary': str(sprint_summary_path),
        'connector_readiness_csv': str(connector_path),
        'connector_readiness_summary': str(connector_summary_path),
        'work_queue_csv': str(work_queue_path),
        'verification_row_count': len(sprint_rows),
        'source_summary': build_source_summary(records),
        'launcher_mode': getattr(args, 'launcher_mode', 'cli'),
    }
    return result


def source_id_for_record(record: dict[str, Any]) -> str:
    return f"seed-{record['rank']:02d}"


def collection_adapter_type(record: dict[str, Any]) -> str:
    source_id = source_id_for_record(record)
    source_name = record['source_name']
    access_type = record['access_type']
    source_family = record['source_family']

    # ID-based overrides for high-priority adapters
    if source_id == 'seed-02':
        return 'iom_dtm_sudan'
    if source_id == 'seed-33':
        return 'ashdod_port_financials'
    if source_id == 'seed-06':
        return 'ipc_gaza_snapshot'
    if source_id == 'seed-07':
        return 'ipc_lebanon_analysis'
    if source_id == 'seed-22':
        return 'saudi_gastat_cpi'

    browser_staged_sources = {
        'IOM DTM Sudan',
        'IPC Gaza snapshot',
        'Ashdod Port Financial and Operating Updates',
    }

    if source_name in {'UNHCR Lebanon reporting hub', 'UNHCR Egypt Sudan Emergency Update', 'UNHCR Egypt data portal'}:
        return 'unhcr_document_index'
    if source_name == 'IPC Lebanon analysis':
        return 'ipc_lebanon_analysis'
    if source_name == 'ACAPS Lebanon':
        return 'acaps_country_page'
    if source_name == 'WFP Retail and Markets factsheet':
        return 'wfp_lebanon_factsheet_pdf'
    if source_name in {'UN Comtrade API portal', 'UN Comtrade TradeFlow'}:
        return 'comtrade_data_availability'
    if source_name == 'UNCTAD Maritime Transport Insights':
        return 'unctad_maritime_insights'
    if source_name == 'Suez Canal Authority Navigation News':
        return 'sca_navigation_news'
    if source_name == 'Israel CBS Main Price Indices':
        return 'israel_cbs_price_indices'
    if source_name == 'Israel CBS Exports and Imports Monthly Files':
        return 'israel_cbs_impexp_files'
    if source_name == 'Israel Airports Authority Monthly Report':
        return 'israel_iaa_monthly_reports'
    if source_name == 'Lebanon CAS Consumer Price Index':
        return 'lebanon_cas_cpi'
    if source_name == 'USDA FAS Saudi Retail Foods Annual':
        return 'usda_fas_gain_pdf'
    if source_name == 'HDX Humanitarian API':
        return 'hdx_hapi_changelog'
    if source_name == 'HDX Signals':
        return 'hdx_dataset_metadata'
    if source_name == 'IPC Gaza snapshot':
        return 'ipc_gaza_snapshot'
    if source_name == 'Ashdod Port Financial and Operating Updates':
        return 'ashdod_port_financials'
    if 'data.humdata.org/dataset/' in record['url']:
        return 'hdx_dataset_metadata'
    if source_name == 'Google Places API':
        return 'places_api_search'
    if source_name in {'OpenStreetMap Overpass', 'Overpass Turbo'}:
        return 'overpass_query'
    if source_name in browser_staged_sources:
        return 'browser_export'
    if 'api' in access_type:
        return 'api_pull'
    if 'pdf' in access_type:
        return 'pdf_capture'
    if 'html' in access_type and source_family in {
        'humanitarian',
        'humanitarian_feed',
        'market',
        'market_monitor',
        'trade',
        'macro_price',
        'geospatial_reference',
        'investor_relations',
    }:
        return 'html_snapshot'
    if access_type == 'web':
        return 'browser_export'
    return 'manual_capture'


def collection_stage(record: dict[str, Any]) -> str:
    if record['source_family'] in {'humanitarian', 'humanitarian_feed', 'geospatial_reference'}:
        return 'baseline'
    if record['source_family'] == 'place':
        return 'market_proxy'
    return 'supporting_proxy'


def collection_query_seed_file(record: dict[str, Any]) -> str:
    adapter_type = collection_adapter_type(record)
    if adapter_type == 'places_api_search':
        return 'places-query-seeds.csv'
    if adapter_type == 'overpass_query':
        return 'overpass-query-seeds.csv'
    return ''


def connector_credential_state(record: dict[str, Any]) -> str:
    if record.get('source_name') == 'Google Places API':
        return 'api_key_required'
    return 'public_endpoint'


def connector_default_status(record: dict[str, Any]) -> str:
    if record.get('source_name') == 'Google Places API':
        return 'needs_credentials'
    return 'ready'


def connector_default_next_action(record: dict[str, Any]) -> str:
    if record.get('source_name') == 'Google Places API':
        return 'Configure a bounded API key, quota limits, and field masks before live collection.'
    return 'Keep Overpass queries bounded and monitor endpoint policy, latency, and query failures.'


def build_connector_readiness_rows(
    records: list[dict[str, Any]],
    existing_rows: dict[str, dict[str, str]] | None = None,
) -> list[dict[str, Any]]:
    existing_rows = existing_rows or {}
    rows = []
    for record in sorted(records, key=lambda item: item['rank']):
        if record.get('source_family') != 'place':
            continue
        source_id = source_id_for_record(record)
        existing = existing_rows.get(source_id, {})
        rows.append(
            {
                'connector_id': existing.get('connector_id') or f"CON-{record['rank']:03d}",
                'status': existing.get('status') or connector_default_status(record),
                'priority': 'high' if record.get('priority_tier') == 'tier1' else 'low',
                'owner': preserve_text(existing, 'owner', verification_owner(record.get('source_family', ''))),
                'source_id': source_id,
                'source_name': record.get('source_name', ''),
                'adapter_type': collection_adapter_type(record),
                'region_or_country': record.get('region_or_country', ''),
                'query_seed_file': collection_query_seed_file(record),
                'credential_state': existing.get('credential_state') or connector_credential_state(record),
                'last_checked_utc': existing.get('last_checked_utc', ''),
                'next_action': preserve_text(existing, 'next_action', connector_default_next_action(record)),
                'notes': preserve_text(existing, 'notes', record.get('notes', '')),
                'url': record.get('url', ''),
            }
        )
    return rows


def render_connector_readiness_summary(rows: list[dict[str, Any]]) -> str:
    status_counts: dict[str, int] = {}
    credential_counts: dict[str, int] = {}
    for row in rows:
        status_counts[row['status']] = status_counts.get(row['status'], 0) + 1
        credential_counts[row['credential_state']] = credential_counts.get(row['credential_state'], 0) + 1
    lines = [
        '# Connector Readiness',
        '',
        f"_Generated: {utc_now().isoformat().replace('+00:00', 'Z')}._",
        '',
        '## Purpose',
        '- Track connector and query infrastructure separately from publication-date verification.',
        '- Keep API/query readiness visible without inflating `unknown` source recency counts.',
        '',
        '## Snapshot',
    ]
    lines.extend([f"- {key}: {value}" for key, value in sorted(status_counts.items())])
    lines.append('')
    lines.append('## Credential State')
    lines.extend([f"- {key}: {value}" for key, value in sorted(credential_counts.items())])
    return '\n'.join(lines)


def initial_run_priority(record: dict[str, Any]) -> str:
    if record['priority_tier'] == 'tier1':
        return 'high'
    if record['source_family'] in {'place', 'market'}:
        return 'medium'
    return 'low'


def scheduled_collection_run_utc(record: dict[str, Any]) -> str:
    now = utc_now()
    if record['priority_tier'] == 'tier1':
        return now.isoformat().replace('+00:00', 'Z')
    return (now + timedelta(days=1)).isoformat().replace('+00:00', 'Z')


def load_collection_district_rows(pack_dir: Path) -> list[dict[str, Any]]:
    district_path = pack_dir / 'district-watchlist.csv'
    if district_path.exists():
        with district_path.open(newline='') as handle:
            rows = list(csv.DictReader(handle))
    else:
        rows = build_district_watchlist_rows()

    return [
        row for row in rows
        if row.get('district_role') in {'monitoring', 'baseline'}
    ]


def build_source_adapter_rows(records: list[dict[str, Any]], collection_dir: Path) -> list[dict[str, Any]]:
    rows = []
    for record in sorted(records, key=lambda item: item['rank']):
        source_id = source_id_for_record(record)
        rows.append(
            {
                'source_id': source_id,
                'rank': record['rank'],
                'source_name': record['source_name'],
                'source_family': record['source_family'],
                'priority_tier': record['priority_tier'],
                'collection_stage': collection_stage(record),
                'collection_mode': collection_mode(record['access_type']),
                'adapter_type': collection_adapter_type(record),
                'access_type': record['access_type'],
                'region_or_country': record['region_or_country'],
                'refresh_cadence': record['refresh_cadence'],
                'raw_landing_dir': str(collection_dir / 'raw' / source_id),
                'normalized_output_path': str(collection_dir / 'normalized' / f'{source_id}.json'),
                'evidence_log_path': str(collection_dir / 'evidence-capture-log.csv'),
                'query_seed_file': collection_query_seed_file(record),
                'notes': record['notes'],
                'url': record['url'],
            }
        )
    return rows


def build_collection_run_manifest_rows(
    records: list[dict[str, Any]],
    district_rows: list[dict[str, Any]],
    collection_dir: Path,
) -> list[dict[str, Any]]:
    district_scope = '; '.join(row['district_name'] for row in district_rows if row['country'] == 'Egypt') or 'pilot_districts'
    rows = []
    for record in sorted(records, key=lambda item: item['rank']):
        source_id = source_id_for_record(record)
        rows.append(
            {
                'run_id': f'run-{source_id}',
                'source_id': source_id,
                'source_name': record['source_name'],
                'collection_stage': collection_stage(record),
                'adapter_type': collection_adapter_type(record),
                'priority': initial_run_priority(record),
                'district_scope': district_scope if record['source_family'] == 'place' else record['region_or_country'],
                'scheduled_run_utc': scheduled_collection_run_utc(record),
                'expected_artifact': str(collection_dir / 'normalized' / f'{source_id}.json'),
                'query_seed_file': collection_query_seed_file(record),
                'status': 'ready',
                'failure_action': 'log_and_escalate',
                'notes': record['primary_use'],
            }
        )
    return rows


def build_district_collection_plan_rows(district_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for row in district_rows:
        rows.append(
            {
                'country': row['country'],
                'district_name': row['district_name'],
                'district_role': row['district_role'],
                'baseline_overlay': 'yes',
                'places_api_collection': 'yes' if row['district_role'] == 'monitoring' else 'no',
                'overpass_collection': 'yes' if row['district_role'] == 'monitoring' else 'no',
                'merchant_page_capture': 'yes' if row['district_role'] == 'monitoring' else 'no',
                'menu_capture': 'yes' if row['district_role'] == 'monitoring' else 'no',
                'control_pair_status': row.get('paired_control', ''),
                'status': 'ready_for_scoring' if row['district_role'] == 'monitoring' else 'baseline_only',
                'notes': row.get('notes', ''),
            }
        )
    return rows


def build_places_query_rows(district_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    place_types = [
        ('bakery', 'bakery'),
        ('grocery', 'grocery_or_supermarket'),
        ('cafe', 'cafe'),
        ('confectionery', 'dessert_or_sweets'),
    ]
    rows = []
    query_number = 1
    for district_row in district_rows:
        if district_row['district_role'] != 'monitoring':
            continue
        for place_type, query_text in place_types:
            rows.append(
                {
                    'query_id': f'gplaces-{query_number:03d}',
                    'country': district_row['country'],
                    'district_name': district_row['district_name'],
                    'place_source': 'Google Places API',
                    'place_type': place_type,
                    'search_mode': 'text_search_then_details',
                    'query_text': f'{query_text} in {district_row["district_name"]}',
                    'priority': 'high' if district_row['country'] == 'Egypt' else 'medium',
                    'status': 'ready',
                    'notes': 'Capture place details and keep field masks tight.',
                }
            )
            query_number += 1
    return rows


def build_overpass_query_rows(district_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    tag_map = [
        ('shop', 'bakery'),
        ('shop', 'convenience'),
        ('shop', 'supermarket'),
        ('amenity', 'cafe'),
        ('shop', 'confectionery'),
    ]
    rows = []
    query_number = 1
    for district_row in district_rows:
        if district_row['district_role'] != 'monitoring':
            continue
        for tag_key, tag_value in tag_map:
            rows.append(
                {
                    'query_id': f'overpass-{query_number:03d}',
                    'country': district_row['country'],
                    'district_name': district_row['district_name'],
                    'tag_key': tag_key,
                    'tag_value': tag_value,
                    'priority': 'high' if district_row['country'] == 'Egypt' else 'medium',
                    'status': 'ready',
                    'notes': 'Export bounded query results and preserve raw JSON snapshots.',
                }
            )
            query_number += 1
    return rows


def build_evidence_capture_rows() -> list[dict[str, Any]]:
    return [
        {
            'capture_id': '',
            'run_id': '',
            'source_id': '',
            'captured_utc': '',
            'capture_type': '',
            'raw_path': '',
            'normalized_path': '',
            'evidence_path': '',
            'checksum_sha256': '',
            'operator': '',
            'status': 'pending_capture',
            'notes': '',
        }
    ]


def render_collection_pipeline_summary(
    records: list[dict[str, Any]],
    district_rows: list[dict[str, Any]],
    places_rows: list[dict[str, Any]],
    overpass_rows: list[dict[str, Any]],
) -> str:
    summary = build_source_summary(records)
    return """# Collection Pipeline Summary

_Generated: {generated_at}._

## Purpose
- Turn the repo from source/accounting scaffolds into a runnable collection pack.
- Keep collection compliant, source-specific, and district-aware from the first run.

## Generated Components
- Source adapter registry
- Collection run manifest
- District collection plan
- Google Places query seeds
- Overpass query seeds
- Evidence capture log

## Snapshot
- Total seeded sources: {total_sources}
- Tier-1 sources: {tier1_count}
- Collection districts in scope: {district_count}
- Google Places query seeds: {places_count}
- Overpass query seeds: {overpass_count}

## Immediate Use
1. Verify the tier-1 baseline rows in `plans/recent_accounting.csv`.
2. Freeze Egypt district/control decisions in `artifacts/v0_1/district-watchlist.csv`.
3. Run the `ready` rows in the collection manifest and record each raw capture in `evidence-capture-log.csv`.
4. Normalize results before anomaly scoring.
""".format(
        generated_at=utc_now().isoformat().replace('+00:00', 'Z'),
        total_sources=summary['total_sources'],
        tier1_count=summary['tier_counts'].get('tier1', 0),
        district_count=len(district_rows),
        places_count=len(places_rows),
        overpass_count=len(overpass_rows),
    )


def scaffold_collection_pack(args: argparse.Namespace, records: list[dict[str, Any]]) -> dict[str, Any]:
    collection_dir = Path(args.collection_dir)
    raw_dir = collection_dir / 'raw'
    normalized_dir = collection_dir / 'normalized'
    raw_dir.mkdir(parents=True, exist_ok=True)
    normalized_dir.mkdir(parents=True, exist_ok=True)

    district_rows = load_collection_district_rows(Path(args.pack_dir))
    adapter_rows = build_source_adapter_rows(records, collection_dir)
    manifest_rows = build_collection_run_manifest_rows(records, district_rows, collection_dir)
    district_plan_rows = build_district_collection_plan_rows(district_rows)
    places_rows = build_places_query_rows(district_rows)
    overpass_rows = build_overpass_query_rows(district_rows)
    evidence_rows = build_evidence_capture_rows()

    adapter_path = collection_dir / 'source-adapter-registry.csv'
    manifest_path = collection_dir / 'collection-run-manifest.csv'
    district_path = collection_dir / 'district-collection-plan.csv'
    places_path = collection_dir / 'places-query-seeds.csv'
    overpass_path = collection_dir / 'overpass-query-seeds.csv'
    evidence_path = collection_dir / 'evidence-capture-log.csv'
    summary_path = collection_dir / 'collection-pipeline-summary.md'

    write_rows_csv(
        [
            'source_id',
            'rank',
            'source_name',
            'source_family',
            'priority_tier',
            'collection_stage',
            'collection_mode',
            'adapter_type',
            'access_type',
            'region_or_country',
            'refresh_cadence',
            'raw_landing_dir',
            'normalized_output_path',
            'evidence_log_path',
            'query_seed_file',
            'notes',
            'url',
        ],
        adapter_rows,
        adapter_path,
    )
    write_rows_csv(
        [
            'run_id',
            'source_id',
            'source_name',
            'collection_stage',
            'adapter_type',
            'priority',
            'district_scope',
            'scheduled_run_utc',
            'expected_artifact',
            'query_seed_file',
            'status',
            'failure_action',
            'notes',
        ],
        manifest_rows,
        manifest_path,
    )
    write_rows_csv(
        [
            'country',
            'district_name',
            'district_role',
            'baseline_overlay',
            'places_api_collection',
            'overpass_collection',
            'merchant_page_capture',
            'menu_capture',
            'control_pair_status',
            'status',
            'notes',
        ],
        district_plan_rows,
        district_path,
    )
    write_rows_csv(
        [
            'query_id',
            'country',
            'district_name',
            'place_source',
            'place_type',
            'search_mode',
            'query_text',
            'priority',
            'status',
            'notes',
        ],
        places_rows,
        places_path,
    )
    write_rows_csv(
        [
            'query_id',
            'country',
            'district_name',
            'tag_key',
            'tag_value',
            'priority',
            'status',
            'notes',
        ],
        overpass_rows,
        overpass_path,
    )
    write_rows_csv(
        [
            'capture_id',
            'run_id',
            'source_id',
            'captured_utc',
            'capture_type',
            'raw_path',
            'normalized_path',
            'evidence_path',
            'checksum_sha256',
            'operator',
            'status',
            'notes',
        ],
        evidence_rows,
        evidence_path,
    )
    write_markdown(render_collection_pipeline_summary(records, district_rows, places_rows, overpass_rows), summary_path)

    return {
        'status': 'ok',
        'action': 'scaffold_collection',
        'input': str(Path(args.input)),
        'collection_dir': str(collection_dir),
        'source_adapter_registry': str(adapter_path),
        'collection_run_manifest': str(manifest_path),
        'district_collection_plan': str(district_path),
        'places_query_seeds': str(places_path),
        'overpass_query_seeds': str(overpass_path),
        'evidence_capture_log': str(evidence_path),
        'collection_pipeline_summary': str(summary_path),
        'source_summary': build_source_summary(records),
        'launcher_mode': getattr(args, 'launcher_mode', 'cli'),
    }


def slugify(value: str) -> str:
    cleaned = ''.join(ch.lower() if ch.isalnum() else '-' for ch in value)
    while '--' in cleaned:
        cleaned = cleaned.replace('--', '-')
    return cleaned.strip('-') or 'zone'


def load_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline='') as handle:
        return list(csv.DictReader(handle))


def zone_review_window() -> tuple[str, str]:
    end_date = utc_now().date().isoformat()
    start_date = (utc_now().date() - timedelta(days=30)).isoformat()
    return start_date, end_date


def zone_pack_id(zone_name: str) -> str:
    return f'{slugify(zone_name)}-{utc_now().date().isoformat()}'


def is_zone_relevant_source(row: dict[str, str], zone_country: str) -> bool:
    region = row.get('region_or_country', '')
    source_family = row.get('source_family', '')
    priority_tier = row.get('priority_tier', '')
    if zone_country in region:
        return True
    if region in {'Regional', 'Global'} and source_family in {
        'place',
        'trade',
        'trends',
        'humanitarian_feed',
        'market_monitor',
        'macro_price',
        'geospatial_reference',
    }:
        return True
    if zone_country == 'Egypt' and region == 'Sudan/Regional':
        return True
    return priority_tier == 'tier1' and source_family == 'place'


def filter_zone_recent_accounting_rows(rows: list[dict[str, str]], zone_country: str) -> list[dict[str, str]]:
    return [row for row in rows if is_zone_relevant_source(row, zone_country)]


def filter_zone_event_rows(rows: list[dict[str, str]], zone_country: str) -> list[dict[str, str]]:
    filtered = []
    for row in rows:
        country = row.get('country', '')
        if zone_country in country:
            filtered.append(row)
    return filtered


def zone_control_districts(district_rows: list[dict[str, str]], zone_country: str) -> tuple[list[str], list[str]]:
    monitoring = []
    controls = []
    for row in district_rows:
        if row.get('country') != zone_country:
            continue
        if row.get('district_role') == 'monitoring':
            monitoring.append(row.get('district_name', ''))
        paired_control = row.get('paired_control', '').strip()
        if paired_control:
            controls.append(paired_control)
    return monitoring, controls


def source_health_counts(rows: list[dict[str, str]]) -> dict[str, int]:
    counts = {
        'current': 0,
        'due_now': 0,
        'overdue': 0,
        'unknown': 0,
        'blocked': 0,
        'manual_review': 0,
    }
    for row in rows:
        status = row.get('recency_status', 'unknown')
        counts[status] = counts.get(status, 0) + 1
    return counts


def reviewed_analytic_anomalies(anomaly_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        row for row in anomaly_rows
        if row.get('signal_type') != 'collection_readiness'
        and row.get('review_state') in {'reviewed', 'approved'}
    ]


def current_source_family_balance(accounting_rows: list[dict[str, str]]) -> tuple[bool, str]:
    current_families = {
        row.get('source_family', '')
        for row in accounting_rows
        if row.get('recency_status') == 'current'
    }
    baseline_families = {'humanitarian', 'humanitarian_feed', 'macro_price', 'trade', 'market_monitor', 'geospatial_reference'}
    proxy_families = {'place', 'market', 'market_monitor', 'retail_catalogue'}
    has_baseline = bool(current_families & baseline_families)
    has_proxy = bool(current_families & proxy_families)
    if has_baseline and has_proxy:
        return True, 'Current source-family balance meets the baseline-plus-proxy minimum.'
    return False, 'Decision gating still lacks one current baseline family and one current proxy family.'


def zone_assessment_label(
    counts: dict[str, int],
    anomaly_rows: list[dict[str, Any]],
    accounting_rows: list[dict[str, str]] | None = None,
) -> str:
    reviewed_rows = reviewed_analytic_anomalies(anomaly_rows)
    if any(row.get('publication_label') == 'Observed' for row in reviewed_rows):
        return 'Observed'
    if any(row.get('publication_label') == 'Correlated' for row in reviewed_rows):
        return 'Correlated'
    if reviewed_rows and counts.get('current', 0) >= 2 and counts.get('overdue', 0) == 0:
        return 'Inferred'
    return 'Unconfirmed'


def zone_decision_label(
    counts: dict[str, int],
    anomaly_rows: list[dict[str, Any]],
    accounting_rows: list[dict[str, str]] | None = None,
) -> str:
    if counts.get('overdue', 0) > 0 or counts.get('unknown', 0) > 0:
        return 'defer'
    reviewed_rows = reviewed_analytic_anomalies(anomaly_rows)
    if not reviewed_rows:
        return 'defer'
    if accounting_rows is None:
        return 'approve'
    balanced, _ = current_source_family_balance(accounting_rows)
    if not balanced:
        return 'defer'
    return 'approve'


def load_zone_template_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {
            'pack_id': '',
            'generated_utc': '',
            'zone_name': '',
            'country': '',
            'review_window_start': '',
            'review_window_end': '',
            'zone_hypothesis': '',
            'briefing_sections': [],
            'source_health': {},
            'baseline_events': [],
            'observations': [],
            'anomaly_cards': [],
            'claim_register': [],
            'confounds': [],
            'review_decision': {},
        }
    return json.loads(path.read_text())


def load_normalized_collection_payloads(collection_dir: Path) -> list[dict[str, Any]]:
    normalized_dir = collection_dir / 'normalized'
    if not normalized_dir.exists():
        return []

    payloads = []
    for path in sorted(normalized_dir.glob('*.json')):
        try:
            payload = json.loads(path.read_text())
        except json.JSONDecodeError:
            continue
        payload['_normalized_path'] = str(path)
        payloads.append(payload)
    return payloads


def sort_source_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    def rank(row: dict[str, Any]) -> tuple[int, str]:
        source_id = row.get('source_id', '')
        if source_id.startswith('seed-'):
            try:
                return int(source_id.split('-', 1)[1]), source_id
            except ValueError:
                pass
        return 9999, source_id

    return sorted(rows, key=rank)


def build_collection_findings_updates(normalized_payloads: list[dict[str, Any]]) -> dict[str, dict[str, str]]:
    updates: dict[str, dict[str, str]] = {}
    for payload in normalized_payloads:
        finding = payload.get('verification_updates')
        if not isinstance(finding, dict):
            continue
        source_id = str(finding.get('source_id', '')).strip()
        if not source_id:
            continue
        updates[source_id] = {field: str(finding.get(field, '')).strip() for field in RECENT_ACCOUNTING_FINDINGS_FIELDS}
        updates[source_id]['source_id'] = source_id
    return updates


def sync_source_verification_findings_from_collection(plans_dir: Path, collection_dir: Path) -> Path:
    findings_path = plans_dir / 'source_verification_findings.csv'
    existing_rows = load_existing_rows_by_key(findings_path, 'source_id')
    collection_updates = build_collection_findings_updates(load_normalized_collection_payloads(collection_dir))
    merged_rows = merge_recent_accounting_findings(existing_rows, collection_updates)
    write_rows_csv(
        RECENT_ACCOUNTING_FINDINGS_FIELDS,
        sort_source_rows(list(merged_rows.values())),
        findings_path,
    )
    return findings_path


def build_collection_observation_rows(
    pack_id: str,
    zone_name: str,
    zone_country: str,
    normalized_payloads: list[dict[str, Any]],
    allowed_source_ids: set[str],
) -> list[dict[str, Any]]:
    rows = []
    observation_index = 1
    for payload in normalized_payloads:
        source_id = payload.get('source_id', '')
        if source_id not in allowed_source_ids:
            continue

        nested_observations = payload.get('observations')
        if isinstance(nested_observations, list) and nested_observations:
            for item in nested_observations:
                rows.append(
                    {
                        'observation_id': f'obs-col-{observation_index:03d}',
                        'pack_id': pack_id,
                        'zone_name': zone_name,
                        'country': zone_country,
                        'source_id': source_id,
                        'source_name': payload.get('source_name', ''),
                        'source_family': item.get('source_family', payload.get('source_family', '')),
                        'district_or_neighborhood': item.get('district_or_neighborhood', zone_name),
                        'observation_type': item.get('observation_type', 'market_observation'),
                        'analysis_bucket': item.get('analysis_bucket', 'signal_observation'),
                        'signal_direction': item.get('signal_direction', ''),
                        'observed_value': item.get('observed_value', ''),
                        'baseline_or_control_value': item.get('baseline_or_control_value', ''),
                        'confound_notes': item.get('confound_notes', ''),
                        'independence_group': item.get('independence_group', source_id),
                        'analysis_eligible': '',
                        'analysis_blockers': '',
                        'capture_method': item.get('capture_method', payload.get('adapter_type', 'normalized_ingest')),
                        'capture_utc': item.get('capture_utc', payload.get('captured_utc', '')),
                        'published_date': item.get('published_date', ''),
                        'latest_period_covered': item.get('latest_period_covered', ''),
                        'source_url': item.get('source_url', payload.get('source_url', '')),
                        'source_excerpt': item.get('source_excerpt', ''),
                        'normalized_summary': item.get('normalized_summary', ''),
                        'geo_lat': item.get('geo_lat', ''),
                        'geo_lon': item.get('geo_lon', ''),
                        'confidence': item.get('confidence', 'medium'),
                        'confidence_reason': item.get('confidence_reason', 'Normalized collection observation.'),
                        'operator': item.get('operator', 'collector'),
                        'status': item.get('status', payload.get('capture_status', 'completed')),
                        'raw_artifact_path': payload.get('raw_path', ''),
                        'normalized_artifact_path': payload.get('_normalized_path', ''),
                        'notes': item.get('notes', ''),
                    }
                )
                observation_index += 1
            continue

        rows.append(
            {
                'observation_id': f'obs-col-{observation_index:03d}',
                'pack_id': pack_id,
                'zone_name': zone_name,
                'country': zone_country,
                'source_id': source_id,
                'source_name': payload.get('source_name', ''),
                'source_family': payload.get('source_family', ''),
                'district_or_neighborhood': payload.get('district_scope', zone_name),
                'observation_type': 'collection_execution',
                'analysis_bucket': 'collection_execution',
                'signal_direction': '',
                'observed_value': '',
                'baseline_or_control_value': '',
                'confound_notes': '',
                'independence_group': source_id,
                'analysis_eligible': 'no',
                'analysis_blockers': 'Execution metadata is not an analytic signal.',
                'capture_method': payload.get('adapter_type', 'normalized_ingest'),
                'capture_utc': payload.get('captured_utc', ''),
                'published_date': '',
                'latest_period_covered': '',
                'source_url': payload.get('source_url', ''),
                'source_excerpt': payload.get('capture_status', ''),
                'normalized_summary': f"{payload.get('source_name', source_id)} collection run status: {payload.get('capture_status', 'unknown')}.",
                'geo_lat': '',
                'geo_lon': '',
                'confidence': 'medium',
                'confidence_reason': 'Collection execution metadata captured.',
                'operator': 'collector',
                'status': payload.get('capture_status', ''),
                'raw_artifact_path': payload.get('raw_path', ''),
                'normalized_artifact_path': payload.get('_normalized_path', ''),
                'notes': payload.get('query_seed_file', ''),
            }
        )
        observation_index += 1
    return rows


def build_zone_source_observation_rows(
    pack_id: str,
    zone_name: str,
    zone_country: str,
    accounting_rows: list[dict[str, str]],
    normalized_payloads: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    allowed_source_ids = {row.get('source_id', '') for row in accounting_rows if row.get('source_id')}
    rows = build_collection_observation_rows(pack_id, zone_name, zone_country, normalized_payloads, allowed_source_ids)
    start_index = len(rows) + 1
    for index, row in enumerate(accounting_rows, start=start_index):
        rows.append(
            {
                'observation_id': f'obs-{index:03d}',
                'pack_id': pack_id,
                'zone_name': zone_name,
                'country': zone_country,
                'source_id': row.get('source_id', ''),
                'source_name': row.get('source_name', ''),
                'source_family': row.get('source_family', ''),
                'district_or_neighborhood': zone_name,
                'observation_type': 'source_health',
                'analysis_bucket': 'source_readiness',
                'signal_direction': '',
                'observed_value': '',
                'baseline_or_control_value': '',
                'confound_notes': '',
                'independence_group': row.get('source_id', ''),
                'analysis_eligible': 'no',
                'analysis_blockers': 'Source-health rows are not analytic signals.',
                'capture_method': 'ledger_verification',
                'capture_utc': row.get('last_checked_utc', ''),
                'published_date': row.get('last_published_date', ''),
                'latest_period_covered': row.get('latest_period_covered', ''),
                'source_url': row.get('url', ''),
                'source_excerpt': row.get('notes', ''),
                'normalized_summary': f"{row.get('source_name', '')} is {row.get('recency_status', 'unknown')} for {zone_name}.",
                'geo_lat': '',
                'geo_lon': '',
                'confidence': 'high' if row.get('evidence_link') else 'medium',
                'confidence_reason': 'Verified ledger row with evidence link.' if row.get('evidence_link') else 'Ledger row exists but evidence chain is incomplete.',
                'operator': row.get('owner', ''),
                'status': row.get('status', ''),
                'raw_artifact_path': '',
                'normalized_artifact_path': '',
                'notes': row.get('evidence_link', ''),
            }
        )
    for row in rows:
        if row.get('analysis_eligible') in {'yes', 'no'}:
            continue
        blockers = []
        for field, label in (
            ('district_or_neighborhood', 'district'),
            ('capture_utc', 'capture_utc'),
            ('signal_direction', 'signal_direction'),
            ('observed_value', 'observed_value'),
            ('baseline_or_control_value', 'baseline_or_control_value'),
            ('confound_notes', 'confound_notes'),
        ):
            if not row.get(field):
                blockers.append(label)
        if row.get('analysis_bucket') != 'signal_observation':
            blockers.append('non_signal_bucket')
        row['analysis_eligible'] = 'no' if blockers else 'yes'
        row['analysis_blockers'] = ';'.join(blockers)
    return rows


def build_aggregated_signal_rows(
    pack_id: str,
    zone_name: str,
    zone_country: str,
    observation_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in observation_rows:
        if row.get('analysis_eligible') != 'yes':
            continue
        capture_key = (row.get('capture_utc', '') or row.get('published_date', '') or row.get('latest_period_covered', ''))[:10]
        key = (
            row.get('district_or_neighborhood', zone_name),
            row.get('observation_type', 'market_observation'),
            capture_key,
        )
        group = grouped.setdefault(
            key,
            {
                'pack_id': pack_id,
                'zone_name': zone_name,
                'country': zone_country,
                'district': key[0],
                'signal_type': key[1],
                'signal_window': key[2],
                'source_ids': set(),
                'source_families': set(),
                'observation_ids': [],
                'evidence_links': set(),
                'signal_directions': set(),
                'summaries': [],
                'observed_values': [],
                'baseline_values': [],
                'confound_notes': set(),
                'latest_capture_utc': '',
            },
        )
        group['source_ids'].add(row.get('source_id', ''))
        group['source_families'].add(row.get('source_family', ''))
        group['observation_ids'].append(row.get('observation_id', ''))
        if row.get('notes'):
            group['evidence_links'].add(row.get('notes', ''))
        if row.get('signal_direction'):
            group['signal_directions'].add(row.get('signal_direction', ''))
        if row.get('normalized_summary'):
            group['summaries'].append(row.get('normalized_summary', ''))
        if row.get('observed_value'):
            group['observed_values'].append(row.get('observed_value', ''))
        if row.get('baseline_or_control_value'):
            group['baseline_values'].append(row.get('baseline_or_control_value', ''))
        if row.get('confound_notes'):
            group['confound_notes'].add(row.get('confound_notes', ''))
        if row.get('capture_utc', '') > group['latest_capture_utc']:
            group['latest_capture_utc'] = row.get('capture_utc', '')

    rows = []
    for index, group in enumerate(grouped.values(), start=1):
        source_ids = {value for value in group['source_ids'] if value}
        source_families = {value for value in group['source_families'] if value}
        promotion_ready = 'no'
        promotion_reason = 'At least two independent corroborating sources are required.'
        if len(source_ids) >= 2:
            has_official_or_baseline = bool(source_families & {'humanitarian', 'humanitarian_feed', 'macro_price', 'trade', 'market_monitor'})
            has_proxy = bool(source_families & {'place', 'market', 'market_monitor', 'retail_catalogue'})
            if has_official_or_baseline and has_proxy:
                promotion_ready = 'yes'
                promotion_reason = 'Minimum corroboration met with baseline and proxy families.'
            else:
                promotion_ready = 'yes'
                promotion_reason = 'Minimum corroboration met with two independent sources.'

        rows.append(
            {
                'aggregated_signal_id': f'signal-{index:03d}',
                'pack_id': group['pack_id'],
                'zone_name': group['zone_name'],
                'country': group['country'],
                'district': group['district'],
                'signal_type': group['signal_type'],
                'signal_window': group['signal_window'],
                'source_ids': ';'.join(sorted(source_ids)),
                'source_family_mix': ';'.join(sorted(source_families)),
                'observation_ids': ';'.join(group['observation_ids']),
                'observation_count': str(len(group['observation_ids'])),
                'latest_capture_utc': group['latest_capture_utc'],
                'signal_direction': ';'.join(sorted(group['signal_directions'])),
                'observed_values': '; '.join(group['observed_values']),
                'baseline_values': '; '.join(group['baseline_values']),
                'signal_summary': group['summaries'][0] if group['summaries'] else '',
                'confound_notes': '; '.join(sorted(group['confound_notes'])),
                'evidence_links': ';'.join(sorted(group['evidence_links'])),
                'promotion_ready': promotion_ready,
                'promotion_reason': promotion_reason,
            }
        )
    return rows


def build_zone_anomaly_rows(
    pack_id: str,
    zone_name: str,
    zone_country: str,
    worksheet_rows: list[dict[str, str]],
    event_rows: list[dict[str, str]],
    observation_rows: list[dict[str, Any]],
    aggregated_signal_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    rows = []
    for row in worksheet_rows:
        if row.get('country') and row.get('country') != zone_country:
            continue
        if row.get('anomaly_id'):
            rows.append(
                {
                    'anomaly_id': row.get('anomaly_id', ''),
                    'pack_id': pack_id,
                    'zone_name': zone_name,
                    'country': zone_country,
                    'district': row.get('district', ''),
                    'signal_type': row.get('signal_family', ''),
                    'signal_summary': row.get('signal_summary', ''),
                    'zone_hypothesis': 'Migration-linked food-market change under review.',
                    'nearest_baseline_event': row.get('nearest_baseline_event', ''),
                    'baseline_event_date': row.get('baseline_event_date', ''),
                    'proxy_signal_date': row.get('proxy_signal_date', ''),
                    'control_district': '',
                    'humanitarian_baseline_score': row.get('humanitarian_baseline_score', ''),
                    'market_proxy_score': row.get('market_proxy_score', ''),
                    'spatial_fit_score': row.get('spatial_fit_score', ''),
                    'temporal_fit_score': row.get('temporal_fit_score', ''),
                    'cross_source_score': row.get('cross_source_score', ''),
                    'confound_penalty': row.get('confound_penalty', ''),
                    'raw_score': row.get('raw_score', ''),
                    'final_score': row.get('final_score', ''),
                    'publication_label': row.get('publication_label', ''),
                    'analyst': row.get('analyst_initials', ''),
                    'review_state': row.get('status', ''),
                    'source_ids': '',
                    'observation_ids': '',
                    'evidence_links': row.get('evidence_links', ''),
                    'confound_notes': row.get('confound_notes', ''),
                    'counterevidence': '',
                    'next_collection_action': row.get('next_collection_action', ''),
                }
            )

    if rows:
        return rows

    promotable_signals = [row for row in aggregated_signal_rows if row.get('promotion_ready') == 'yes']
    if promotable_signals:
        nearest_event = event_rows[0] if event_rows else {}
        for index, row in enumerate(promotable_signals, start=1):
            signal_type = row.get('signal_type', 'market_observation')
            rows.append(
                {
                    'anomaly_id': f'anomaly-auto-{index:03d}',
                    'pack_id': pack_id,
                    'zone_name': zone_name,
                    'country': zone_country,
                    'district': row.get('district', zone_name),
                    'signal_type': signal_type,
                    'signal_summary': row.get('signal_summary', ''),
                    'zone_hypothesis': 'Market-proxy collection output has been ingested and requires analyst scoring against the baseline event chain.',
                    'nearest_baseline_event': nearest_event.get('event_id', ''),
                    'baseline_event_date': nearest_event.get('event_date', ''),
                    'proxy_signal_date': row.get('latest_capture_utc', ''),
                    'control_district': '',
                    'humanitarian_baseline_score': '',
                    'market_proxy_score': '',
                    'spatial_fit_score': '',
                    'temporal_fit_score': '',
                    'cross_source_score': '',
                    'confound_penalty': '',
                    'raw_score': '',
                    'final_score': '',
                    'publication_label': 'Unconfirmed',
                    'analyst': '',
                    'review_state': 'pending_review',
                    'source_ids': row.get('source_ids', ''),
                    'observation_ids': row.get('observation_ids', ''),
                    'evidence_links': row.get('evidence_links', ''),
                    'confound_notes': row.get('confound_notes', '') or 'Confounds captured, but control-district scoring is still required.',
                    'counterevidence': 'No reviewed control-district comparison has been recorded yet.',
                    'next_collection_action': 'Review the fused signal, score confounds, and attach control-district context before claim promotion.',
                }
            )
        return rows

    nearest_event = event_rows[0] if event_rows else {}
    blocked_signals = [row for row in aggregated_signal_rows if row.get('promotion_ready') == 'no']
    blocker_note = blocked_signals[0].get('promotion_reason', '') if blocked_signals else 'No analytic signal observations have passed the promotion gate yet.'
    return [
        {
            'anomaly_id': 'anomaly-starter-001',
            'pack_id': pack_id,
            'zone_name': zone_name,
            'country': zone_country,
            'district': zone_name,
            'signal_type': 'collection_readiness',
            'signal_summary': 'No scored market anomaly is recorded yet; the current pack is baseline- and collection-readiness focused.',
            'zone_hypothesis': 'The zone remains worth monitoring, but the market-proxy layer is not yet briefing-grade.',
            'nearest_baseline_event': nearest_event.get('event_id', ''),
            'baseline_event_date': nearest_event.get('event_date', ''),
            'proxy_signal_date': '',
            'control_district': '',
            'humanitarian_baseline_score': '',
            'market_proxy_score': '',
            'spatial_fit_score': '',
            'temporal_fit_score': '',
            'cross_source_score': '',
            'confound_penalty': '',
            'raw_score': '',
            'final_score': '',
            'publication_label': 'Unconfirmed',
            'analyst': '',
            'review_state': 'pending_collection',
            'source_ids': '',
            'observation_ids': '',
            'evidence_links': '',
            'confound_notes': blocker_note,
            'counterevidence': 'Baseline sources may be present, but corroborated analytic signal promotion is incomplete.',
            'next_collection_action': 'Capture at least two corroborating signal observations with control or baseline comparison fields before anomaly promotion.',
        }
    ]


def build_zone_claim_rows(
    pack_id: str,
    zone_name: str,
    zone_country: str,
    accounting_rows: list[dict[str, str]],
    observation_rows: list[dict[str, Any]],
    event_rows: list[dict[str, str]],
    anomaly_rows: list[dict[str, Any]],
    counts: dict[str, int],
) -> list[dict[str, Any]]:
    nearest_event = event_rows[0] if event_rows else {}
    decision_utc = utc_now().isoformat().replace('+00:00', 'Z')
    decision_label = zone_decision_label(counts, anomaly_rows, accounting_rows)
    reviewed_rows = reviewed_analytic_anomalies(anomaly_rows)
    if reviewed_rows:
        rows = []
        for index, anomaly in enumerate(reviewed_rows, start=1):
            publication_label = anomaly.get('publication_label', 'Unconfirmed')
            if publication_label == 'Observed':
                confidence_score = '88'
            elif publication_label == 'Correlated':
                confidence_score = '76'
            else:
                confidence_score = '66'
            rows.append(
                {
                    'claim_id': f'claim-{index:03d}',
                    'pack_id': pack_id,
                    'zone_name': zone_name,
                    'country': zone_country,
                    'claim_text': anomaly.get('signal_summary', ''),
                    'claim_type': 'analytic_claim',
                    'basis_type': 'reviewed_anomaly',
                    'source_ids': anomaly.get('source_ids', ''),
                    'observation_ids': anomaly.get('observation_ids', ''),
                    'evidence_links': anomaly.get('evidence_links', ''),
                    'nearest_baseline_event': anomaly.get('nearest_baseline_event', nearest_event.get('event_id', '')),
                    'baseline_event_date': anomaly.get('baseline_event_date', nearest_event.get('event_date', '')),
                    'confound_ids': 'requires_review_trace',
                    'confidence_label': publication_label,
                    'confidence_score': confidence_score,
                    'review_state': anomaly.get('review_state', ''),
                    'reviewer': '',
                    'decision_utc': decision_utc,
                    'decision_label': decision_label,
                    'publication_label': publication_label,
                    'notes': 'Derived directly from a reviewed anomaly row.',
                }
            )
        return rows

    source_ids = ';'.join(row.get('source_id', '') for row in accounting_rows[:4] if row.get('source_id'))
    evidence_links = ';'.join(row.get('evidence_link', '') for row in accounting_rows[:4] if row.get('evidence_link'))
    observation_ids = ';'.join(row.get('observation_id', '') for row in observation_rows[:4] if row.get('observation_id'))
    balanced, balance_note = current_source_family_balance(accounting_rows)
    return [
        {
            'claim_id': 'claim-001',
            'pack_id': pack_id,
            'zone_name': zone_name,
            'country': zone_country,
            'claim_text': f'{zone_name} remains collection-worthy, but no reviewed analytic anomaly currently supports a directional claim.',
            'claim_type': 'collection_readiness',
            'basis_type': 'source_health_and_pending_analysis',
            'source_ids': source_ids,
            'observation_ids': observation_ids,
            'evidence_links': evidence_links,
            'nearest_baseline_event': nearest_event.get('event_id', ''),
            'baseline_event_date': nearest_event.get('event_date', ''),
            'confound_ids': 'source_staleness;promotion_gate',
            'confidence_label': 'Unconfirmed',
            'confidence_score': '42' if balanced else '35',
            'review_state': 'draft',
            'reviewer': '',
            'decision_utc': decision_utc,
            'decision_label': decision_label,
            'publication_label': 'Unconfirmed',
            'notes': balance_note,
        }
    ]


def build_zone_evidence_index_rows(
    pack_id: str,
    accounting_rows: list[dict[str, str]],
    normalized_payloads: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    rows = []
    for index, row in enumerate(accounting_rows, start=1):
        rows.append(
            {
                'evidence_id': f'evidence-{index:03d}',
                'pack_id': pack_id,
                'source_id': row.get('source_id', ''),
                'source_name': row.get('source_name', ''),
                'evidence_type': 'source_verification',
                'source_url': row.get('url', ''),
                'evidence_link': row.get('evidence_link', ''),
                'last_checked_utc': row.get('last_checked_utc', ''),
                'last_published_date': row.get('last_published_date', ''),
                'latest_period_covered': row.get('latest_period_covered', ''),
                'recency_status': row.get('recency_status', ''),
                'notes': row.get('notes', ''),
            }
        )
    start_index = len(rows) + 1
    for index, payload in enumerate(normalized_payloads, start=start_index):
        rows.append(
            {
                'evidence_id': f'evidence-{index:03d}',
                'pack_id': pack_id,
                'source_id': payload.get('source_id', ''),
                'source_name': payload.get('source_name', ''),
                'evidence_type': 'normalized_collection_output',
                'source_url': payload.get('source_url', ''),
                'evidence_link': '',
                'last_checked_utc': payload.get('captured_utc', ''),
                'last_published_date': payload.get('published_date', ''),
                'latest_period_covered': payload.get('latest_period_covered', ''),
                'recency_status': payload.get('capture_status', ''),
                'notes': payload.get('_normalized_path', ''),
            }
        )
    return rows


def render_zone_brief(
    pack_id: str,
    zone_name: str,
    zone_country: str,
    analyst: str,
    reviewer: str,
    review_window_start: str,
    review_window_end: str,
    monitoring_districts: list[str],
    control_districts: list[str],
    accounting_rows: list[dict[str, str]],
    event_rows: list[dict[str, str]],
    observation_rows: list[dict[str, Any]],
    anomaly_rows: list[dict[str, Any]],
    counts: dict[str, int],
) -> str:
    assessment_label = zone_assessment_label(counts, anomaly_rows, accounting_rows)
    current_ids = ', '.join(row['source_id'] for row in accounting_rows if row.get('recency_status') == 'current') or 'none'
    overdue_ids = ', '.join(row['source_id'] for row in accounting_rows if row.get('recency_status') == 'overdue') or 'none'
    unknown_ids = ', '.join(row['source_id'] for row in accounting_rows if row.get('recency_status') == 'unknown') or 'none'
    first_event = event_rows[0] if event_rows else {}
    first_anomaly = anomaly_rows[0] if anomaly_rows else {}
    lead_signal = next(
        (
            row for row in observation_rows
            if row.get('analysis_eligible') == 'yes'
        ),
        observation_rows[0] if observation_rows else {},
    )
    balanced, balance_note = current_source_family_balance(accounting_rows)
    return """# Zone Brief

## Header
- `pack_id`: {pack_id}
- `generated_utc`: {generated_utc}
- `zone_name`: {zone_name}
- `country`: {zone_country}
- `review_window_start`: {review_window_start}
- `review_window_end`: {review_window_end}
- `analyst`: {analyst}
- `reviewer`: {reviewer}
- `briefing_label`: {assessment_label}

## Executive Summary
- `assessment_label`: {assessment_label}
- `bottom_line`: {zone_name} remains a valid collection zone, but the current evidence chain is not yet strong enough for a briefing-grade directional assessment.
- `confidence_reason`: Baseline events are established, but relevant source freshness is mixed and the market-proxy layer has not yet produced scored anomalies.
- `main_driver`: Egypt baseline monitoring anchored to the Sudan-linked arrival window beginning 2023-04-15.
- `principal_limit`: Key baseline sources are overdue and the first normalized place snapshots are not yet present in the anomaly queue.
- `one_sentence_warning`: Treat this pack as a readiness and collection briefing, not a validated directional signal product.

## Key Judgments
- `judgment_1`: The zone remains operationally relevant because the baseline event chain is defined and the collection manifest is ready.
- `judgment_2`: Promotion gates are now active; observations do not become anomalies until corroboration, baseline comparison, and confound fields are present.
- `judgment_3`: The next material upgrade is not more parsing. It is executing the place-query runs and capturing at least two corroborating analytic observations.
- `judgment_support`: Current source IDs: {current_ids}; overdue source IDs: {overdue_ids}; unknown source IDs: {unknown_ids}.

## Zone Scope
- `districts_in_scope`: {districts_in_scope}
- `control_districts`: {control_districts}
- `population_or_market_context`: Egypt pilot zone for migration-linked food-market monitoring.
- `operational_relevance`: High for baseline setup and first market-proxy collection cycle.
- `exclusions`: No person-level inference, no restricted collection, and no claim elevation beyond public-source evidence.

## Source Health
- `current_sources`: {current_sources}
- `due_now_sources`: {due_now_sources}
- `overdue_sources`: {overdue_sources}
- `unknown_sources`: {unknown_sources}
- `blocked_sources`: {blocked_sources}
- `source_quality_notes`: {balance_note}

## Baseline Events
- `event_id`: {event_id}
- `event_date`: {event_date}
- `event_type`: {event_type}
- `geography`: {event_geo}
- `source_ref`: {event_source}
- `brief_use`: Anchor event for Egypt baseline and timing checks.

## Observed Signals
- `signal_id`: {signal_id}
- `signal_type`: {signal_type}
- `district`: {zone_name}
- `capture_date`: {signal_capture}
- `source_ref`: {signal_source}
- `signal_summary`: {signal_summary}
- `capture_method`: {signal_capture_method}

## Anomaly Cards
- `anomaly_id`: {anomaly_id}
- `district`: {anomaly_district}
- `nearest_baseline_event`: {anomaly_event}
- `score`: {anomaly_score}
- `label`: {anomaly_label}
- `dominant_confound`: {dominant_confound}
- `counterpoint`: {counterpoint}

## Claim Register
- `claim_id`: claim-001
- `claim_text`: {zone_name} remains collection-worthy, but not yet briefing-grade for a directional conclusion.
- `evidence_links`: {claim_links}
- `support_level`: readiness-driven
- `review_state`: draft
- `publication_label`: Unconfirmed

## Confounds And Limits
- `seasonality`: Not yet assessed in a scored market signal.
- `tourism_or_pilgrimage`: Not yet ruled out in the market-proxy layer.
- `inflation_or_fx`: Remains an open confound until the first normalized place and market snapshots are reviewed.
- `source_family_balance`: {source_family_balance}
- `platform_or_taxonomy_change`: Relevant for future place collection; not yet scored.
- `other_limits`: Sparse current-source coverage and no scored anomaly rows yet.

## Analyst Assessment
- `what_changed`: The repo now supports source accounting, collection manifests, and zone-brief generation from the same artifact chain.
- `why_it_matters`: This creates a repeatable dossier structure that can survive review and be updated cycle by cycle.
- `what_would_change_my_mind`: Two current baseline rows plus the first normalized place snapshots and one scored anomaly card.
- `recommended_next_collection_action`: Run Google Places and Overpass seeds for the Egypt monitoring districts and normalize the first capture set.
 - `recommended_next_collection_action`: Capture at least two corroborating signal observations with explicit baseline or control values and confound notes.

## Review Decision
- `decision_label`: {decision_label}
- `decision_notes`: Defer publication-grade judgment until the baseline freshness gap is closed and the observation layer is populated.
- `approved_by`: {reviewer}
- `decision_utc`: {generated_utc}
- `next_review_date`: {next_review_date}
""".format(
        pack_id=pack_id,
        generated_utc=utc_now().isoformat().replace('+00:00', 'Z'),
        zone_name=zone_name,
        zone_country=zone_country,
        review_window_start=review_window_start,
        review_window_end=review_window_end,
        analyst=analyst,
        reviewer=reviewer,
        assessment_label=assessment_label,
        current_ids=current_ids,
        overdue_ids=overdue_ids,
        unknown_ids=unknown_ids,
        districts_in_scope=', '.join(monitoring_districts) or 'none',
        control_districts=', '.join(control_districts) or 'not yet paired',
        current_sources=counts.get('current', 0),
        due_now_sources=counts.get('due_now', 0),
        overdue_sources=counts.get('overdue', 0),
        unknown_sources=counts.get('unknown', 0),
        blocked_sources=counts.get('blocked', 0),
        event_id=first_event.get('event_id', 'not_available'),
        event_date=first_event.get('event_date', ''),
        event_type=first_event.get('event_type', ''),
        event_geo=first_event.get('admin1', ''),
        event_source=first_event.get('source_name', ''),
        balance_note=balance_note,
        signal_id=lead_signal.get('observation_id', 'obs-000'),
        signal_type=lead_signal.get('observation_type', 'source_health'),
        signal_capture=lead_signal.get('capture_utc', ''),
        signal_source=lead_signal.get('source_id', ''),
        signal_summary=lead_signal.get('normalized_summary', 'No observation rows available.') if observation_rows else 'No observation rows available.',
        signal_capture_method=lead_signal.get('capture_method', 'ledger_verification'),
        anomaly_id=first_anomaly.get('anomaly_id', 'anomaly-none'),
        anomaly_district=first_anomaly.get('district', zone_name),
        anomaly_event=first_anomaly.get('nearest_baseline_event', ''),
        anomaly_score=first_anomaly.get('final_score', '') or 'n/a',
        anomaly_label=first_anomaly.get('publication_label', 'Unconfirmed'),
        dominant_confound=first_anomaly.get('confound_notes', '') or 'collection incompleteness',
        counterpoint=first_anomaly.get('counterevidence', '') or 'No scored market anomaly exists yet.',
        claim_links='; '.join(row.get('evidence_link', '') for row in accounting_rows[:3] if row.get('evidence_link')) or 'none',
        source_family_balance='balanced' if balanced else 'unbalanced',
        decision_label=zone_decision_label(counts, anomaly_rows, accounting_rows),
        next_review_date=(utc_now().date() + timedelta(days=7)).isoformat(),
    )


def render_review_decision(
    pack_id: str,
    zone_name: str,
    zone_country: str,
    reviewer: str,
    accounting_rows: list[dict[str, str]],
    counts: dict[str, int],
    anomaly_rows: list[dict[str, Any]],
) -> str:
    decision_label = zone_decision_label(counts, anomaly_rows, accounting_rows)
    publication_label = zone_assessment_label(counts, anomaly_rows, accounting_rows)
    balanced, balance_note = current_source_family_balance(accounting_rows)
    return """# Review Decision

## Decision Header
- `decision_id`: review-{pack_id}
- `pack_id`: {pack_id}
- `zone_name`: {zone_name}
- `country`: {zone_country}
- `decision_utc`: {decision_utc}
- `reviewer`: {reviewer}
- `approver_role`: review_gatekeeper

## Decision Summary
- `decision_label`: {decision_label}
- `publication_label`: {publication_label}
- `summary`: The pack is suitable for internal collection guidance but not yet for a high-confidence directional briefing.
- `one_line_rationale`: Source freshness and observation completeness remain below briefing-grade threshold.

## Basis For Decision
- `source_health_assessment`: current={current}, overdue={overdue}, unknown={unknown}
- `source_family_balance`: {source_family_balance}
- `baseline_coverage`: Baseline event chain exists for the zone.
- `observation_quality`: Source-health observations are present; market observations are not yet normalized.
- `anomaly_quality`: {anomaly_quality}
- `confound_strength`: Collection incompleteness remains the dominant confound.
- `counterevidence_summary`: {balance_note}

## Required Follow-Up
- `next_collection_action`: Execute the ready Egypt place-query runs and normalize the first capture set.
- `next_review_date`: {next_review_date}
- `owner`: source_monitor and anomaly_scorer
- `blocked_by`: overdue baseline rows and incomplete market-proxy observations
""".format(
        pack_id=pack_id,
        zone_name=zone_name,
        zone_country=zone_country,
        decision_utc=utc_now().isoformat().replace('+00:00', 'Z'),
        reviewer=reviewer,
        decision_label=decision_label,
        publication_label=publication_label,
        current=counts.get('current', 0),
        overdue=counts.get('overdue', 0),
        unknown=counts.get('unknown', 0),
        source_family_balance='balanced' if balanced else 'unbalanced',
        balance_note=balance_note,
        anomaly_quality='starter only' if anomaly_rows and anomaly_rows[0].get('review_state') == 'pending_collection' else 'scored rows available',
        next_review_date=(utc_now().date() + timedelta(days=7)).isoformat(),
    )


def write_zone_briefing_pack(args: argparse.Namespace, records: list[dict[str, Any]]) -> dict[str, Any]:
    zone_name = args.zone_name
    zone_country = args.zone_country
    pack_id = zone_pack_id(zone_name)
    review_window_start, review_window_end = zone_review_window()

    briefing_root = Path(args.briefing_dir)
    pack_dir = briefing_root / pack_id
    pack_dir.mkdir(parents=True, exist_ok=True)

    recent_accounting_path = Path(args.plans_dir) / 'recent_accounting.csv'
    event_timeline_path = Path(args.pack_dir) / 'event-timeline.csv'
    district_watchlist_path = Path(args.pack_dir) / 'district-watchlist.csv'
    anomaly_path = Path(args.pack_dir) / 'anomaly-review-worksheet.csv'
    normalized_payloads = load_normalized_collection_payloads(Path(args.collection_dir))

    accounting_rows = load_csv_rows(recent_accounting_path)
    if not accounting_rows:
        accounting_rows = build_recent_accounting_rows(records)
    accounting_rows = filter_zone_recent_accounting_rows(accounting_rows, zone_country)

    event_rows = load_csv_rows(event_timeline_path)
    if not event_rows:
        event_rows = build_event_timeline_rows()
    event_rows = filter_zone_event_rows(event_rows, zone_country)

    district_rows = load_csv_rows(district_watchlist_path)
    if not district_rows:
        district_rows = build_district_watchlist_rows()
    monitoring_districts, control_districts = zone_control_districts(district_rows, zone_country)

    worksheet_rows = load_csv_rows(anomaly_path)
    observation_rows = build_zone_source_observation_rows(pack_id, zone_name, zone_country, accounting_rows, normalized_payloads)
    aggregated_signal_rows = build_aggregated_signal_rows(pack_id, zone_name, zone_country, observation_rows)
    anomaly_rows = build_zone_anomaly_rows(pack_id, zone_name, zone_country, worksheet_rows, event_rows, observation_rows, aggregated_signal_rows)
    counts = source_health_counts(accounting_rows)
    claim_rows = build_zone_claim_rows(pack_id, zone_name, zone_country, accounting_rows, observation_rows, event_rows, anomaly_rows, counts)
    evidence_rows = build_zone_evidence_index_rows(pack_id, accounting_rows, normalized_payloads)

    zone_brief_path = pack_dir / 'zone_brief.md'
    source_observation_path = pack_dir / 'source_observation_log.csv'
    aggregated_signal_path = pack_dir / 'aggregated_signals.csv'
    event_baseline_path = pack_dir / 'event_baseline.csv'
    anomaly_cards_path = pack_dir / 'anomaly_cards.csv'
    claim_register_path = pack_dir / 'claim_register.csv'
    evidence_index_path = pack_dir / 'evidence_index.csv'
    review_decision_path = pack_dir / 'review_decision.md'
    pack_json_path = pack_dir / 'zone_evidence_pack.json'

    write_markdown(
        render_zone_brief(
            pack_id,
            zone_name,
            zone_country,
            args.analyst,
            args.reviewer,
            review_window_start,
            review_window_end,
            monitoring_districts,
            control_districts,
            accounting_rows,
            event_rows,
            observation_rows,
            anomaly_rows,
            counts,
        ),
        zone_brief_path,
    )
    write_rows_csv(
        [
            'observation_id',
            'pack_id',
            'zone_name',
            'country',
            'source_id',
            'source_name',
            'source_family',
            'district_or_neighborhood',
            'observation_type',
            'analysis_bucket',
            'signal_direction',
            'observed_value',
            'baseline_or_control_value',
            'confound_notes',
            'independence_group',
            'analysis_eligible',
            'analysis_blockers',
            'capture_method',
            'capture_utc',
            'published_date',
            'latest_period_covered',
            'source_url',
            'source_excerpt',
            'normalized_summary',
            'geo_lat',
            'geo_lon',
            'confidence',
            'confidence_reason',
            'operator',
            'status',
            'raw_artifact_path',
            'normalized_artifact_path',
            'notes',
        ],
        observation_rows,
        source_observation_path,
    )
    write_rows_csv(
        [
            'aggregated_signal_id',
            'pack_id',
            'zone_name',
            'country',
            'district',
            'signal_type',
            'signal_window',
            'source_ids',
            'source_family_mix',
            'observation_ids',
            'observation_count',
            'latest_capture_utc',
            'signal_direction',
            'observed_values',
            'baseline_values',
            'signal_summary',
            'confound_notes',
            'evidence_links',
            'promotion_ready',
            'promotion_reason',
        ],
        aggregated_signal_rows,
        aggregated_signal_path,
    )
    write_rows_csv(
        [
            'event_id',
            'event_type',
            'country',
            'admin1',
            'district_focus',
            'event_date',
            'event_window_start',
            'event_window_end',
            'source_name',
            'source_url',
            'source_accessed_utc',
            'confidence',
            'summary',
            'notes',
        ],
        event_rows,
        event_baseline_path,
    )
    write_rows_csv(
        [
            'anomaly_id',
            'pack_id',
            'zone_name',
            'country',
            'district',
            'signal_type',
            'signal_summary',
            'zone_hypothesis',
            'nearest_baseline_event',
            'baseline_event_date',
            'proxy_signal_date',
            'control_district',
            'humanitarian_baseline_score',
            'market_proxy_score',
            'spatial_fit_score',
            'temporal_fit_score',
            'cross_source_score',
            'confound_penalty',
            'raw_score',
            'final_score',
            'publication_label',
            'analyst',
            'review_state',
            'source_ids',
            'observation_ids',
            'evidence_links',
            'confound_notes',
            'counterevidence',
            'next_collection_action',
        ],
        anomaly_rows,
        anomaly_cards_path,
    )
    write_rows_csv(
        [
            'claim_id',
            'pack_id',
            'zone_name',
            'country',
            'claim_text',
            'claim_type',
            'basis_type',
            'source_ids',
            'observation_ids',
            'evidence_links',
            'nearest_baseline_event',
            'baseline_event_date',
            'confound_ids',
            'confidence_label',
            'confidence_score',
            'review_state',
            'reviewer',
            'decision_utc',
            'decision_label',
            'publication_label',
            'notes',
        ],
        claim_rows,
        claim_register_path,
    )
    write_rows_csv(
        [
            'evidence_id',
            'pack_id',
            'source_id',
            'source_name',
            'evidence_type',
            'source_url',
            'evidence_link',
            'last_checked_utc',
            'last_published_date',
            'latest_period_covered',
            'recency_status',
            'notes',
        ],
        evidence_rows,
        evidence_index_path,
    )
    write_markdown(render_review_decision(pack_id, zone_name, zone_country, args.reviewer, accounting_rows, counts, anomaly_rows), review_decision_path)

    pack_json = load_zone_template_json(Path(args.plans_dir) / 'zone_evidence_pack_template.json')
    pack_json['pack_id'] = pack_id
    pack_json['generated_utc'] = utc_now().isoformat().replace('+00:00', 'Z')
    pack_json['zone_name'] = zone_name
    pack_json['country'] = zone_country
    pack_json['review_window_start'] = review_window_start
    pack_json['review_window_end'] = review_window_end
    pack_json['zone_hypothesis'] = 'Collection-readiness and migration-linked food-market monitoring for the selected zone.'
    pack_json['source_health'] = {
        'current': counts.get('current', 0),
        'due_now': counts.get('due_now', 0),
        'overdue': counts.get('overdue', 0),
        'unknown': counts.get('unknown', 0),
    }
    pack_json['baseline_events'] = event_rows
    pack_json['observations'] = observation_rows
    pack_json['aggregated_signals'] = aggregated_signal_rows
    pack_json['anomaly_cards'] = anomaly_rows
    pack_json['claim_register'] = claim_rows
    pack_json['confounds'] = [
        'source_staleness',
        'collection_incompleteness',
        'seasonality_not_yet_scored',
    ]
    pack_json['review_decision'] = {
        'label': zone_decision_label(counts, anomaly_rows),
        'approved_for_briefing': zone_decision_label(counts, anomaly_rows) == 'approve',
        'reviewer': args.reviewer,
        'decision_utc': utc_now().isoformat().replace('+00:00', 'Z'),
        'notes': 'Auto-generated from source health, baseline coverage, and anomaly readiness.',
    }
    logging.info('Writing JSON artifact: %s', pack_json_path)
    pack_json_path.write_text(json.dumps(pack_json, indent=2, ensure_ascii=False) + '\n')

    return {
        'status': 'ok',
        'action': 'brief_zone',
        'input': str(Path(args.input)),
        'briefing_dir': str(pack_dir),
        'zone_brief': str(zone_brief_path),
        'source_observation_log': str(source_observation_path),
        'aggregated_signals': str(aggregated_signal_path),
        'event_baseline': str(event_baseline_path),
        'anomaly_cards': str(anomaly_cards_path),
        'claim_register': str(claim_register_path),
        'evidence_index': str(evidence_index_path),
        'review_decision': str(review_decision_path),
        'zone_evidence_pack': str(pack_json_path),
        'source_summary': build_source_summary(records),
        'launcher_mode': getattr(args, 'launcher_mode', 'cli'),
    }


def collection_pack_paths(collection_dir: Path) -> dict[str, Path]:
    return {
        'adapter_registry': collection_dir / 'source-adapter-registry.csv',
        'run_manifest': collection_dir / 'collection-run-manifest.csv',
        'evidence_log': collection_dir / 'evidence-capture-log.csv',
        'run_results': collection_dir / 'collection-run-results.csv',
    }


def load_adapter_lookup(path: Path) -> dict[str, dict[str, str]]:
    return {row['source_id']: row for row in load_csv_rows(path) if row.get('source_id')}


def ensure_collection_artifacts(args: argparse.Namespace, records: list[dict[str, Any]]) -> dict[str, Path]:
    collection_dir = Path(args.collection_dir)
    paths = collection_pack_paths(collection_dir)
    if not paths['adapter_registry'].exists() or not paths['run_manifest'].exists():
        scaffold_collection_pack(args, records)
    return paths


def raw_extension(adapter_type: str) -> str:
    if adapter_type in {'pdf_capture', 'usda_fas_gain_pdf', 'wfp_lebanon_factsheet_pdf'}:
        return '.pdf'
    if adapter_type == 'israel_cbs_impexp_files':
        return '.xml'
    if adapter_type in {'api_pull', 'hdx_dataset_metadata', 'comtrade_data_availability'}:
        return '.json'
    if adapter_type in {
        'html_snapshot',
        'manual_capture',
        'hdx_hapi_changelog',
        'hdx_signals_story',
        'lebanon_cas_cpi',
        'israel_cbs_price_indices',
        'israel_iaa_monthly_reports',
        'unhcr_document_index',
        'iom_dtm_sudan',
        'ipc_lebanon_analysis',
        'acaps_country_page',
        'unctad_maritime_insights',
        'sca_navigation_news',
        'ipc_gaza_snapshot',
        'ashdod_port_financials',
    }:
        return '.html'
    return '.json'


def fetch_direct_source(url: str, raw_path: Path) -> dict[str, Any]:
    if url.startswith('file://'):
        request = Request(url, headers={'User-Agent': 'MunchMusings/1.0'})
        with urlopen(request, timeout=20) as response:
            body = response.read()
            raw_path.parent.mkdir(parents=True, exist_ok=True)
            raw_path.write_bytes(body)
            return {
                'status_code': getattr(response, 'status', 200),
                'content_type': response.headers.get('Content-Type', ''),
                'bytes_written': len(body),
                'checksum_sha256': hashlib.sha256(body).hexdigest(),
            }

    verify_tls = urlparse(url).netloc not in {'cas.gov.lb', 'beta.cas.gov.lb', 'www.beta.cas.gov.lb'}
    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0 Safari/537.36',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    }
    try:
        response = requests.get(url, timeout=20, headers=headers, allow_redirects=True, verify=verify_tls)
    except requests.exceptions.Timeout as exc:
        raise TimeoutError(str(exc)) from exc
    except requests.exceptions.RequestException as exc:
        raise URLError(str(exc)) from exc

    if response.status_code >= 400:
        raise HTTPError(url, response.status_code, response.reason, response.headers, None)

    body = response.content
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    raw_path.write_bytes(body)
    return {
        'status_code': response.status_code,
        'content_type': response.headers.get('Content-Type', ''),
        'bytes_written': len(body),
        'checksum_sha256': hashlib.sha256(body).hexdigest(),
    }


def fetch_json_source(url: str, raw_path: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    fetch_meta = fetch_direct_source(url, raw_path)
    payload = json.loads(raw_path.read_text())
    return payload, fetch_meta


def hdx_dataset_api_url(url: str) -> str:
    if url.startswith('file://') or url.endswith('.json'):
        return url
    parsed = urlparse(url)
    if parsed.netloc == 'data.humdata.org' and parsed.path.rstrip('/') == '/signals':
        return f'{parsed.scheme}://{parsed.netloc}/api/3/action/package_show?id=hdx-signals'
    marker = '/dataset/'
    if marker not in parsed.path:
        return url
    dataset_id = parsed.path.split(marker, 1)[1].strip('/')
    return f'{parsed.scheme}://{parsed.netloc}/api/3/action/package_show?id={quote(dataset_id)}'


def hdx_hapi_changelog_url(url: str) -> str:
    if url.startswith('file://') or url.endswith('.html'):
        return url
    return 'https://hdx-hapi.readthedocs.io/en/latest/changelog/'


def hdx_hapi_faq_url(url: str) -> str:
    if url.startswith('file://') or url.endswith('.html'):
        return url
    return 'https://centre.humdata.org/ufaqs/how-up-to-date-is-the-data-in-hdx-hapi/'


def unhcr_document_index_url(url: str) -> str:
    if url.startswith('https://data.unhcr.org/en/country/egy'):
        return (
            'https://data.unhcr.org/en/search?direction=desc&geo_id=1&page=1&sector=0&'
            'sector_json=%7B%220%22%3A+%220%22%7D&sort=publishDate&sv_id=0&type%5B0%5D=document'
        )
    return url


def ipc_lebanon_analysis_url(url: str) -> str:
    return url


def acaps_country_page_url(url: str) -> str:
    return url


def wfp_lebanon_programme_factsheet_pdf_url(url: str) -> str:
    if url.startswith('file://') or url.endswith('.pdf'):
        return url
    return 'https://docs.wfp.org/api/documents/WFP-0000169349/download/'


def extract_wfp_lebanon_programme_page_metadata(html_body: str, page_url: str) -> dict[str, str]:
    title = extract_html_title(html_body)
    publication_date = ''
    for pattern in (
        r'content_publication_date":"(20\d{2}-\d{2}-\d{2})"',
        r'<time[^>]+datetime="([^"]+)"',
    ):
        match = re.search(pattern, html_body, flags=re.IGNORECASE)
        if not match:
            continue
        parsed = parse_accounting_date(match.group(1))
        if parsed is None:
            continue
        publication_date = parsed.date().isoformat()
        break

    download_url = ''
    for pattern in (
        r'<a href="(https://docs\.wfp\.org/api/documents/[^"\s]+/download/)" class="button-new button-new--primary" aria-label="Open in English"',
        r'href="(https://docs\.wfp\.org/api/documents/[^"\s]+/download/)"',
    ):
        match = re.search(pattern, html_body, flags=re.IGNORECASE)
        if match:
            download_url = html_lib.unescape(match.group(1))
            break

    return {
        'page_url': page_url,
        'document_title': title,
        'last_published_date': publication_date,
        'latest_period_covered': extract_named_month_period(title),
        'document_download_url': download_url or wfp_lebanon_programme_factsheet_pdf_url(page_url),
    }


def comtrade_data_availability_api_url(url: str) -> str:
    if url.startswith('file://') or url.endswith('.json'):
        return url
    return 'https://comtradeplus.un.org/api/DataAvailability/getComtradeTrend'


def unctad_maritime_insights_url(url: str) -> str:
    return url


def sca_navigation_news_index_url(url: str) -> str:
    return url


def hdx_signals_story_url(url: str) -> str:
    if url.startswith('file://') or url.endswith('.html'):
        return url
    return 'https://centre.humdata.org/hdx-signals-alerting-humanitarians-to-deteriorating-crises/'


def hdx_signals_author_archive_url(url: str) -> str:
    if url.startswith('file://') or url.endswith('.html'):
        return url
    return 'https://centre.humdata.org/author/data-science-team/'


def israel_cbs_price_indices_url(url: str) -> str:
    if url.startswith('file://') or url.endswith('.html'):
        return url
    return 'https://www.cbs.gov.il/en/Pages/Main%20Price%20Indices.aspx'


def israel_cbs_price_series_url() -> str:
    return 'https://api.cbs.gov.il/index/data/price?id=120010&last=1&format=json&download=false&lang=en'


def israel_cbs_price_release_api_url(reference_date: str) -> str:
    release_year = reference_date[:4] if reference_date else str(utc_now().year)
    return (
        "https://www.cbs.gov.il/en/mediarelease/Madad/_api/web/"
        f"GetFolderByServerRelativeUrl('/en/mediarelease/Madad/Pages/{release_year}')/Files"
        "?$select=Name,ServerRelativeUrl,TimeCreated,ListItemAllFields/Title,"
        "ListItemAllFields/ArticleStartDate,ListItemAllFields/CbsDataPublishDate"
        "&$expand=ListItemAllFields"
        "&$filter=startswith(Name,'Consumer-Price-Index-')"
        "&$orderby=TimeCreated%20desc"
        "&$top=3"
    )


def israel_cbs_impexp_api_url(url: str) -> str:
    if url.startswith('file://') or url.endswith('.xml'):
        return url
    return "https://www.cbs.gov.il/he/publications/_api/web/GetFolderByServerRelativeUrl('DocLib/impexpfiles')/Files"


def israel_iaa_monthly_reports_url(url: str) -> str:
    if url.startswith('file://') or url.endswith('.html'):
        return url
    return 'https://www.iaa.gov.il/about/aeronautical-information/annualreport/'


def lebanon_cas_cpi_page_url(url: str) -> str:
    if url.startswith('file://') or url.endswith('.html'):
        return url
    return 'http://cas.gov.lb/index.php/economic-statistics-en/cpi-en/'


def html_to_visible_text(value: str) -> str:
    text = re.sub(r'<script\b.*?</script>', ' ', value, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r'<style\b.*?</style>', ' ', text, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = html_lib.unescape(text)
    return re.sub(r'\s+', ' ', text).strip()


def latest_iso_date(values: list[str]) -> str:
    dated_values = []
    for value in values:
        parsed = parse_accounting_date(value)
        if parsed is None:
            continue
        dated_values.append(parsed.date().isoformat())
    if not dated_values:
        return ''
    return max(dated_values)


def extract_first_month_day_year(text: str) -> str:
    specific_match = re.search(
        r'resourcelibrary \|\s*([A-Z][a-z]+ \d{1,2}, \d{4})\s+HDX Signals: Alerting Humanitarians to Deteriorating Crises',
        text,
        flags=re.IGNORECASE,
    )
    if specific_match:
        try:
            return datetime.strptime(specific_match.group(1), '%B %d, %Y').date().isoformat()
        except ValueError:
            return ''

    match = re.search(r'\b([A-Z][a-z]+ \d{1,2}, \d{4})\b', text)
    if not match:
        return ''
    try:
        return datetime.strptime(match.group(1), '%B %d, %Y').date().isoformat()
    except ValueError:
        return ''


def extract_month_year(text: str, marker: str) -> str:
    pattern = rf'{re.escape(marker)}\s+([A-Z][a-z]+ \d{{4}})'
    match = re.search(pattern, text, flags=re.IGNORECASE)
    if not match:
        return ''
    try:
        return datetime.strptime(match.group(1), '%B %Y').date().strftime('%Y-%m')
    except ValueError:
        return ''


def parse_day_month_year(value: str) -> str:
    value = value.strip()
    if not value:
        return ''
    for fmt in ('%d/%m/%Y', '%d.%m.%Y'):
        try:
            return datetime.strptime(value, fmt).date().isoformat()
        except ValueError:
            continue
    return ''


def parse_written_date(value: str) -> str:
    normalized = re.sub(r'\s+', ' ', html_lib.unescape(value)).strip()
    if not normalized:
        return ''
    for fmt in ('%d %B %Y', '%d %b %Y', '%B %d, %Y', '%b %d, %Y'):
        try:
            return datetime.strptime(normalized, fmt).date().isoformat()
        except ValueError:
            continue
    return ''


def extract_named_month_period(text: str, default_year: str = '') -> str:
    normalized = html_lib.unescape(text).replace('-', ' ')
    year_match = re.search(r'\b(20\d{2})\b', normalized)
    year = year_match.group(1) if year_match else default_year
    if not year:
        return ''

    lowered = normalized.lower()
    for month_name, month_number in MONTH_NAME_TO_NUMBER.items():
        if month_name.isascii():
            if re.search(rf'\b{re.escape(month_name)}\b', lowered):
                return f'{year}-{month_number:02d}'
        elif month_name in normalized:
            return f'{year}-{month_number:02d}'
    return ''


def parse_pdfinfo_iso_date(text: str) -> str:
    match = re.search(r':\s+\w+\s+([A-Z][a-z]{2}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2}\s+\d{4})', text)
    if not match:
        return ''
    try:
        return datetime.strptime(match.group(1), '%b %d %H:%M:%S %Y').date().isoformat()
    except ValueError:
        return ''


def pdfinfo_report_date(pdf_path: Path) -> str:
    try:
        result = subprocess.run(
            ['pdfinfo', str(pdf_path)],
            check=True,
            capture_output=True,
            text=True,
        )
    except (FileNotFoundError, subprocess.CalledProcessError):
        return ''
    mod_match = re.search(r'^ModDate:.*$', result.stdout, flags=re.MULTILINE)
    mod_date = parse_pdfinfo_iso_date(mod_match.group(0)) if mod_match else ''
    if mod_date:
        return mod_date
    creation_match = re.search(r'^CreationDate:.*$', result.stdout, flags=re.MULTILINE)
    return parse_pdfinfo_iso_date(creation_match.group(0)) if creation_match else ''


def pdftotext_content(pdf_path: Path) -> str:
    try:
        result = subprocess.run(
            ['pdftotext', str(pdf_path), '-'],
            check=True,
            capture_output=True,
            text=True,
        )
    except (FileNotFoundError, subprocess.CalledProcessError):
        return ''
    return result.stdout


def extract_wfp_lebanon_factsheet_period(pdf_text: str) -> str:
    for match in re.finditer(r'WFP Lebanon\s*-\s*([A-Z][a-z]+ \d{4})', pdf_text):
        try:
            return datetime.strptime(match.group(1), '%B %Y').date().strftime('%Y-%m')
        except ValueError:
            continue

    for match in re.finditer(r'Programme Factsheets?\s*-\s*([A-Z][a-z]+ \d{4})', pdf_text, flags=re.IGNORECASE):
        try:
            return datetime.strptime(match.group(1), '%B %Y').date().strftime('%Y-%m')
        except ValueError:
            continue
    return ''


def normalize_dotted_period_range(value: str) -> str:
    match = re.search(r'(\d{2}\.\d{2}\.\d{4})\s*[>/-]+\s*(\d{2}\.\d{2}\.\d{4})', value)
    if not match:
        return ''
    start_date = parse_day_month_year(match.group(1))
    end_date = parse_day_month_year(match.group(2))
    if not start_date or not end_date:
        return ''
    return f'{start_date[:7]}/{end_date[:7]}'


def extract_unhcr_issue_period(text: str) -> str:
    range_match = re.search(r'\b(\d{1,2})\s*[-\u2013\u2014]\s*(\d{1,2})\s+([A-Z][a-z]+)\s+(20\d{2})\b', text)
    if range_match:
        start_date = parse_written_date(f"{range_match.group(1)} {range_match.group(3)} {range_match.group(4)}")
        end_date = parse_written_date(f"{range_match.group(2)} {range_match.group(3)} {range_match.group(4)}")
        if start_date and end_date:
            return f'{start_date}/{end_date}'

    daily_match = re.search(r'\bas of\s+([0-9]{1,2}\s+[A-Z][a-z]+\s+20\d{2})\b', text, flags=re.IGNORECASE)
    if daily_match:
        return parse_written_date(daily_match.group(1))

    month_match = re.search(r'\bas of\s+([A-Z][a-z]+ \d{4})\b', text, flags=re.IGNORECASE)
    if not month_match:
        return ''
    try:
        return datetime.strptime(month_match.group(1), '%B %Y').date().strftime('%Y-%m')
    except ValueError:
        return ''


def extract_unhcr_document_candidates(index_html: str) -> list[dict[str, str]]:
    candidates = []
    pattern = re.compile(
        r"<div class=['\"]searchResultItem_content media_body['\"]>.*?"
        r"<h2 class=['\"]searchResultItem_title['\"]>\s*"
        r"<a[^>]+href=['\"](?P<detail>https://data\.unhcr\.org/en/documents/details/(?P<document_id>\d+))['\"][^>]*>\s*(?P<title>.*?)\s*</a>.*?"
        r"<a class=['\"]searchResultItem_download_link['\"] href=['\"](?P<download>https://data\.unhcr\.org/en/documents/download/\d+)['\"][^>]*data-title=['\"](?P<data_title>[^'\"]+)['\"].*?</a>.*?"
        r"<div class=['\"]searchResultItem_body['\"]>\s*(?P<body>.*?)\s*</div>.*?"
        r"Publish date:\s*<b>(?P<publish>[^<]+)</b>.*?"
        r"(?:Create date:\s*<b>(?P<create>[^<]+)</b>)?",
        flags=re.IGNORECASE | re.DOTALL,
    )
    for match in pattern.finditer(index_html):
        title = html_to_visible_text(match.group('title')) or html_lib.unescape(match.group('data_title')).strip()
        candidates.append(
            {
                'document_id': match.group('document_id'),
                'detail_page_url': html_lib.unescape(match.group('detail')),
                'download_url': html_lib.unescape(match.group('download')),
                'title': title,
                'body': html_to_visible_text(match.group('body')),
                'publish_date': parse_written_date(match.group('publish')),
                'upload_date': parse_written_date(match.group('create') or ''),
            }
        )
    return candidates


def unhcr_document_title_match(source_name: str, title: str) -> bool:
    lowered = title.lower()
    if source_name == 'UNHCR Lebanon reporting hub':
        if 'flash update' in lowered and 'lebanon' in lowered:
            return True
        if lowered.startswith('middle east situation') and ('as of' in lowered or 'lebanon' in lowered):
            return True
        return False
    if source_name == 'UNHCR Egypt Sudan Emergency Update':
        return lowered.startswith('egypt:') and (
            'emergency response update' in lowered or 'new arrivals from sudan' in lowered
        )
    if source_name == 'UNHCR Egypt data portal':
        return lowered.startswith('egypt:') and 'new arrivals from sudan' in lowered
    return False


def select_unhcr_document_candidate(source_name: str, candidates: list[dict[str, str]]) -> dict[str, str]:
    ranked = []
    for candidate in candidates:
        if not unhcr_document_title_match(source_name, candidate.get('title', '')):
            continue
        published_at = parse_accounting_date(candidate.get('publish_date', '')) or datetime(1970, 1, 1, tzinfo=timezone.utc)
        ranked.append((published_at, candidate.get('document_id', ''), candidate))
    if not ranked:
        return {}
    ranked.sort(reverse=True)
    return ranked[0][2]


def extract_unhcr_detail_upload_date(detail_text: str) -> str:
    match = re.search(r'Upload date:\s*([0-9]{1,2}\s+[A-Z][a-z]+\s+20\d{2})', detail_text, flags=re.IGNORECASE)
    if not match:
        return ''
    return parse_written_date(match.group(1))


def extract_ipc_lebanon_analysis_metadata(page_html: str, page_url: str) -> dict[str, str]:
    visible_text = html_to_visible_text(page_html)
    release_match = re.search(r'RELEASE DATE\s*(\d{2}\.\d{2}\.\d{4})', visible_text, flags=re.IGNORECASE)
    validity_match = re.search(r'VALIDITY PERIOD\s*(\d{2}\.\d{2}\.\d{4}\s*[>/-]+\s*\d{2}\.\d{2}\.\d{4})', visible_text, flags=re.IGNORECASE)
    pdf_match = re.search(r'href=[\'"]([^\'"]+IPC_Lebanon_Acute_Food_Insecurity_[^\'"]+\.pdf)[\'"]', page_html, flags=re.IGNORECASE)
    return {
        'latest_visible_date': parse_day_month_year(release_match.group(1)) if release_match else '',
        'latest_period_covered': normalize_dotted_period_range(validity_match.group(1)) if validity_match else '',
        'latest_report_url': urljoin(page_url, html_lib.unescape(pdf_match.group(1))) if pdf_match else '',
    }


def extract_acaps_country_page_metadata(page_html: str, page_url: str) -> dict[str, str]:
    items = []
    item_pattern = re.compile(
        r'<div class="rolling-feeds-item">.*?<p class="date">(?P<date>[^<]+)</p>(?P<body>.*?)</div></div>',
        flags=re.IGNORECASE | re.DOTALL,
    )
    for match in item_pattern.finditer(page_html):
        visible_date = parse_written_date(match.group('date'))
        if not visible_date:
            continue
        summary = html_to_visible_text(match.group('body'))
        items.append((visible_date, summary))

    if not items:
        fallback_dates = [parse_written_date(value) for value in re.findall(r'<p class="date">([^<]+)</p>', page_html, flags=re.IGNORECASE)]
        fallback_dates = [value for value in fallback_dates if value]
        return {
            'latest_visible_date': max(fallback_dates) if fallback_dates else '',
            'latest_summary': '',
            'evidence_link': page_url,
        }

    items.sort(reverse=True)
    latest_visible_date, latest_summary = items[0]
    return {
        'latest_visible_date': latest_visible_date,
        'latest_summary': latest_summary,
        'evidence_link': page_url,
    }


def extract_unctad_maritime_insights_metadata(page_html: str, page_url: str) -> dict[str, str]:
    dated_items = []
    for match in re.finditer(r'<article>(?P<body>.*?)</article>', page_html, flags=re.IGNORECASE | re.DOTALL):
        body = match.group('body')
        title_match = re.search(r'<h3[^>]*class="[^"]*dataviz-heading__title[^"]*"[^>]*>\s*(?P<title>.*?)\s*</h3>', body, flags=re.IGNORECASE | re.DOTALL)
        date_match = re.search(r'updatedate__content[^>]*>\s*(?P<date>[^<]+)\s*<', body, flags=re.IGNORECASE | re.DOTALL)
        metadata_match = re.search(
            r'href="(?P<link>(?:https://unctadstat\.unctad\.org)?/datacentre/(?:reportInfo|dataviewer)/[^"\']+)"',
            body,
            flags=re.IGNORECASE,
        )
        title_text = html_to_visible_text(title_match.group('title')) if title_match else ''
        article_text = html_to_visible_text(body)
        visible_date = parse_written_date((date_match.group('date') if date_match else '').replace('.', ''))
        if not visible_date:
            continue
        dated_items.append({
            'latest_visible_date': visible_date,
            'title': title_text,
            'metadata_link': urljoin(page_url, html_lib.unescape(metadata_match.group('link'))) if metadata_match else page_url,
            'latest_period_covered': derive_quarter_period_from_text(f'{title_text} {article_text}'),
        })

    if not dated_items:
        fallback_dates = [parse_written_date(value.replace('.', '')) for value in re.findall(r'updatedate__content[^>]*>\s*([^<]+)\s*<', page_html, flags=re.IGNORECASE)]
        fallback_dates = [value for value in fallback_dates if value]
        return {
            'latest_visible_date': max(fallback_dates) if fallback_dates else '',
            'latest_title': '',
            'metadata_link': page_url,
            'latest_period_covered': '',
        }

    dated_items.sort(key=lambda item: item['latest_visible_date'], reverse=True)
    return {
        'latest_visible_date': dated_items[0]['latest_visible_date'],
        'latest_title': dated_items[0]['title'],
        'metadata_link': dated_items[0]['metadata_link'],
        'latest_period_covered': dated_items[0]['latest_period_covered'],
    }


def parse_sca_publishing_start_date(value: str) -> str:
    normalized = html_lib.unescape(value).replace('.', '').strip()
    if not normalized:
        return ''
    for fmt in ('%d %b %Y', '%d %B %Y'):
        try:
            return datetime.strptime(normalized, fmt).date().isoformat()
        except ValueError:
            continue
    return ''


def extract_sca_navigation_news_candidates(index_html: str, page_url: str) -> list[dict[str, str]]:
    rows = []
    for match in re.finditer(r'<Row\b(?P<attrs>.*?)></Row>', index_html, flags=re.IGNORECASE | re.DOTALL):
        attrs = html_lib.unescape(match.group('attrs'))
        if 'NewsCategory_x003a_Title="Navigation News"' not in attrs:
            continue
        title_match = re.search(r'Title="([^"]+)"', attrs)
        date_match = re.search(r'PublishingStartDate="([^"]+)"', attrs)
        url_match = re.search(r'ServerUrl="([^"]+)"', attrs)
        visible_date = parse_sca_publishing_start_date(date_match.group(1) if date_match else '')
        if not visible_date or not url_match:
            continue
        rows.append({
            'latest_visible_date': visible_date,
            'title': title_match.group(1).strip() if title_match else '',
            'detail_url': urljoin(page_url, url_match.group(1).strip()),
        })
    rows.sort(key=lambda item: item['latest_visible_date'], reverse=True)
    return rows


def extract_sca_navigation_news_detail(detail_html: str) -> dict[str, str]:
    visible_text = html_to_visible_text(detail_html)
    title_match = re.search(r'<title>\s*SCA\s*-\s*(?P<title>.*?)\s*</title>', detail_html, flags=re.IGNORECASE | re.DOTALL)
    category_match = re.search(r'>\s*Navigation News\s*<', detail_html, flags=re.IGNORECASE)
    date_matches = [parse_written_date(value) for value in re.findall(r'\b\d{1,2} [A-Z][a-z]+ 20\d{2}\b', visible_text)]
    date_matches = [value for value in date_matches if value]
    daily_match = re.search(
        r'transit of (\d+) vessels(?: today| in both directions)? at a total gross tonnage of ([0-9.]+) million tons',
        visible_text,
        flags=re.IGNORECASE,
    )
    convoy_match = re.search(
        r'(\d+) vessels in the nourthern convoy.*?(\d+) vessels in the southern convoy',
        visible_text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    rolling_match = re.search(
        r'during the past three days, (\d+) vessels transited through the Canal, with a total net tonnage of ([0-9.]+) million tons',
        visible_text,
        flags=re.IGNORECASE,
    )
    return {
        'title': html_to_visible_text(title_match.group('title')) if title_match else '',
        'category': 'Navigation News' if category_match else '',
        'detail_date': max(date_matches) if date_matches else '',
        'daily_vessel_count': daily_match.group(1) if daily_match else '',
        'daily_gross_tonnage_mtons': daily_match.group(2) if daily_match else '',
        'northbound_vessel_count': convoy_match.group(1) if convoy_match else '',
        'southbound_vessel_count': convoy_match.group(2) if convoy_match else '',
        'rolling_three_day_vessel_count': rolling_match.group(1) if rolling_match else '',
        'rolling_three_day_gross_tonnage_mtons': rolling_match.group(2) if rolling_match else '',
    }


def latest_month_period_from_entries(entries: dict[str, Any]) -> str:
    latest = ''
    for year, months in entries.items():
        if not isinstance(months, dict):
            continue
        for month_name in months.keys():
            period = extract_named_month_period(f'{month_name} {year}', default_year=str(year))
            if period and period_sort_key(period) > period_sort_key(latest):
                latest = period
    return latest


def extract_lebanon_cas_cpi_release_candidates(page_html: str, page_url: str) -> list[dict[str, str]]:
    candidates = []
    seen_urls = set()
    for href in re.findall(r'href=["\']([^"\']+\.pdf)["\']', page_html, flags=re.IGNORECASE):
        pdf_url = urljoin(page_url, html_lib.unescape(href))
        if pdf_url in seen_urls:
            continue
        seen_urls.add(pdf_url)
        match = re.search(r'/CPI/(?P<year>20\d{2})/(?P<month>\d{1,2})-[^/]+\.pdf$', pdf_url, flags=re.IGNORECASE)
        if not match:
            continue
        candidates.append(
            {
                'pdf_url': pdf_url,
                'latest_period_covered': f"{match.group('year')}-{int(match.group('month')):02d}",
            }
        )
    candidates.sort(key=lambda item: period_sort_key(item['latest_period_covered']), reverse=True)
    return candidates


def select_lebanon_cas_cpi_release_candidate(candidates: list[dict[str, str]]) -> dict[str, str]:
    if not candidates:
        return {}
    return candidates[0]


def lebanon_cas_cpi_json_url(page_html: str) -> str:
    match = re.search(r'var\s+cpiConfig\s*=\s*(\{.*?\});', page_html, flags=re.DOTALL)
    if not match:
        return 'https://www.beta.cas.gov.lb/wp-content/uploads/cpi_data.json'
    try:
        payload = json.loads(match.group(1))
    except json.JSONDecodeError:
        return 'https://www.beta.cas.gov.lb/wp-content/uploads/cpi_data.json'
    return str(payload.get('jsonUrl', 'https://www.beta.cas.gov.lb/wp-content/uploads/cpi_data.json')).strip()


def requests_head_last_modified(url: str) -> str:
    verify_tls = urlparse(url).netloc not in {'cas.gov.lb', 'beta.cas.gov.lb', 'www.beta.cas.gov.lb'}
    try:
        response = requests.head(
            url,
            timeout=20,
            headers={'User-Agent': 'Mozilla/5.0', 'Accept-Language': 'en-US,en;q=0.9'},
            allow_redirects=True,
            verify=verify_tls,
        )
    except requests.exceptions.RequestException:
        return ''
    return iso_date_from_http_datetime(response.headers.get('Last-Modified', ''))


def period_sort_key(value: str) -> tuple[int, int]:
    if not re.fullmatch(r'\d{4}-\d{2}', value):
        return (0, 0)
    year, month = value.split('-', 1)
    return int(year), int(month)


def derive_quarter_period_from_text(text: str) -> str:
    normalized = html_lib.unescape(text)
    match = re.search(r'\b(first|second|third|fourth)\s+quarter\s+of\s+(20\d{2})\b', normalized, flags=re.IGNORECASE)
    if not match:
        return ''
    quarter = match.group(1).lower()
    year = match.group(2)
    quarter_to_period = {
        'first': '03',
        'second': '06',
        'third': '09',
        'fourth': '12',
    }
    month = quarter_to_period.get(quarter, '')
    return f'{year}-{month}' if month else ''


def iso_date_from_http_datetime(value: str) -> str:
    value = value.strip()
    if not value:
        return ''
    try:
        return parsedate_to_datetime(value).astimezone(timezone.utc).date().isoformat()
    except (TypeError, ValueError, IndexError):
        return ''


def decode_json_payload(value: Any) -> dict[str, Any]:
    payload = value
    while isinstance(payload, str):
        payload = payload.strip()
        if not payload:
            return {}
        payload = json.loads(payload)
    if isinstance(payload, dict):
        return payload
    return {}


def fetch_head_last_modified(url: str) -> str:
    if url.startswith('file://'):
        path = Path(urlparse(url).path)
        if not path.exists():
            return ''
        return datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).date().isoformat()

    parsed = urlparse(url)
    quoted_url = urlunparse(
        (
            parsed.scheme,
            parsed.netloc,
            quote(parsed.path),
            parsed.params,
            parsed.query,
            parsed.fragment,
        )
    )
    request = Request(quoted_url, headers={'User-Agent': 'MunchMusings/1.0'}, method='HEAD')
    try:
        with urlopen(request, timeout=20) as response:
            return iso_date_from_http_datetime(response.headers.get('Last-Modified', ''))
    except (HTTPError, URLError, ValueError):
        return ''


def extract_cbs_price_indices_rows(html_body: str) -> list[dict[str, Any]]:
    match = re.search(r"var\s+MadadNewsdataList\s*=\s*'(?P<payload>\{.*?\})';", html_body, flags=re.DOTALL)
    if not match:
        return []
    payload = html_lib.unescape(match.group('payload'))
    try:
        data = json.loads(payload)
    except json.JSONDecodeError:
        return []
    if isinstance(data, dict):
        return [row for row in data.values() if isinstance(row, dict)]
    return []


def extract_gastat_last_modified_date(html_body: str) -> str:
    match = re.search(r'Last Modified:\s*(\d{2}/\d{2}/\d{4})', html_body, flags=re.IGNORECASE)
    if not match:
        return ''
    day, month, year = match.group(1).split('/')
    return f'{year}-{month}-{day}'


def saudi_gastat_cpi_listing_url(url: str) -> str:
    if not url or url.startswith('file://') or url.endswith('.html'):
        return url
    if '/statistics-tabs/' in url:
        return url
    return 'https://www.stats.gov.sa/en/statistics-tabs/-/categories/121421?category=121421&delta=20&start=1&tab=436312'


def extract_gastat_cpi_listing_metadata(page_html: str, page_url: str) -> dict[str, str]:
    candidates = []
    pattern = re.compile(
        r'<div class="box-body">(?P<body>.*?)<!-- View Details Button -->',
        flags=re.IGNORECASE | re.DOTALL,
    )
    for match in pattern.finditer(page_html):
        body = match.group('body')
        title_match = re.search(r'<span class="row-header">\s*(?P<title>.*?)\s*</span>', body, flags=re.IGNORECASE | re.DOTALL)
        title = html_to_visible_text(title_match.group('title')) if title_match else ''
        if 'consumer price index' not in title.lower():
            continue
        latest_period_covered = extract_named_month_period(title)
        if not latest_period_covered:
            continue
        links = [urljoin(page_url, html_lib.unescape(link)) for link in re.findall(r'href="([^"]+)"', body, flags=re.IGNORECASE)]
        pdf_link = next((link for link in links if '.pdf' in link.lower()), '')
        xlsx_link = next((link for link in links if '.xlsx' in link.lower()), '')
        evidence_link = pdf_link or xlsx_link or page_url
        candidates.append(
            {
                'document_title': title,
                'latest_period_covered': latest_period_covered,
                'evidence_link': evidence_link,
            }
        )
    if not candidates:
        return {}
    candidates.sort(
        key=lambda item: (period_sort_key(item.get('latest_period_covered', '')), 1 if item.get('evidence_link', '').lower().endswith('.pdf') else 0),
        reverse=True,
    )
    return candidates[0]


def select_cbs_cpi_release(rows: list[dict[str, Any]]) -> dict[str, Any]:
    candidates = []
    for row in rows:
        title = str(row.get('Title', ''))
        url = str(row.get('Url', ''))
        if 'consumer price index' not in title.lower() and 'consumer-price-index' not in url.lower():
            continue
        published_date = parse_day_month_year(str(row.get('CbsDataPublishDate', ''))) or parse_day_month_year(str(row.get('ArticleStartDate', '')))
        covered_period = extract_named_month_period(title)
        candidates.append(
            (
                parse_accounting_date(published_date or '1970-01-01') or datetime(1970, 1, 1, tzinfo=timezone.utc),
                period_sort_key(covered_period),
                row,
            )
        )
    if not candidates:
        return {}
    candidates.sort(reverse=True)
    return candidates[0][2]


def extract_cbs_price_series_period(payload: dict[str, Any]) -> str:
    month_rows = payload.get('month')
    if not isinstance(month_rows, list) or not month_rows:
        return ''
    date_rows = month_rows[0].get('date')
    if not isinstance(date_rows, list) or not date_rows:
        return ''
    year = date_rows[0].get('year')
    month = date_rows[0].get('month')
    if not isinstance(year, int) or not isinstance(month, int):
        return ''
    return f'{year:04d}-{month:02d}'


def fetch_json_headers_source(url: str, raw_path: Path, headers: dict[str, str]) -> tuple[dict[str, Any], dict[str, Any]]:
    request = Request(url, headers=headers)
    with urlopen(request, timeout=20) as response:
        body = response.read()
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        raw_path.write_bytes(body)
        return json.loads(body.decode('utf-8')), {
            'status_code': getattr(response, 'status', 200),
            'content_type': response.headers.get('Content-Type', ''),
            'bytes_written': len(body),
            'checksum_sha256': hashlib.sha256(body).hexdigest(),
        }


def select_cbs_cpi_release_item(items: list[dict[str, Any]], expected_period: str) -> dict[str, Any]:
    candidates = []
    for item in items:
        release_meta = item.get('ListItemAllFields', {})
        if not isinstance(release_meta, dict):
            continue
        title = str(release_meta.get('Title', ''))
        published_date = parse_accounting_date(str(release_meta.get('CbsDataPublishDate', ''))) or parse_accounting_date(str(release_meta.get('ArticleStartDate', '')))
        title_period = extract_named_month_period(title)
        period_bonus = 1 if expected_period and expected_period == title_period else 0
        candidates.append(
            (
                published_date or datetime(1970, 1, 1, tzinfo=timezone.utc),
                period_bonus,
                item,
            )
        )
    if not candidates:
        return {}
    candidates.sort(reverse=True)
    return candidates[0][2]


def build_israel_cbs_price_indices_payload(
    manifest_row: dict[str, str],
    adapter_row: dict[str, str],
    raw_path: Path,
    captured_utc: str,
) -> dict[str, Any]:
    source_url = israel_cbs_price_indices_url(adapter_row.get('url', ''))
    latest_visible_date = ''
    latest_period_covered = ''
    evidence_link = source_url
    title = ''

    if source_url.startswith('file://') or source_url.endswith('.html'):
        fetch_meta = fetch_direct_source(source_url, raw_path)
        html_body = raw_path.read_text(errors='replace')
        row = select_cbs_cpi_release(extract_cbs_price_indices_rows(html_body))
        latest_visible_date = parse_day_month_year(str(row.get('CbsDataPublishDate', ''))) or parse_day_month_year(str(row.get('ArticleStartDate', '')))
        latest_period_covered = extract_named_month_period(str(row.get('Title', '')))
        evidence_link = str(row.get('Url', '')).strip() or source_url
        title = str(row.get('Title', '')).strip()
    else:
        page_meta = fetch_direct_source(source_url, raw_path)
        series_raw_path = raw_path.with_name(f'{raw_path.stem}-series.json')
        release_raw_path = raw_path.with_name(f'{raw_path.stem}-release.json')
        series_payload, series_meta = fetch_json_source(israel_cbs_price_series_url(), series_raw_path)
        latest_period_covered = extract_cbs_price_series_period(series_payload)
        release_payload, release_meta = fetch_json_headers_source(
            israel_cbs_price_release_api_url(latest_period_covered),
            release_raw_path,
            {'User-Agent': 'MunchMusings/1.0', 'Accept': 'application/json;odata=nometadata'},
        )
        items = release_payload.get('value', [])
        release_item = select_cbs_cpi_release_item(items if isinstance(items, list) else [], latest_period_covered)
        release_meta_fields = release_item.get('ListItemAllFields', {}) if isinstance(release_item, dict) else {}
        latest_visible_date = (
            parse_accounting_date(str(release_meta_fields.get('CbsDataPublishDate', ''))) or
            parse_accounting_date(str(release_meta_fields.get('ArticleStartDate', '')))
        )
        latest_visible_date = latest_visible_date.date().isoformat() if latest_visible_date else ''
        title = str(release_meta_fields.get('Title', '')).strip()
        relative_release_url = str(release_item.get('ServerRelativeUrl', '')).strip() if isinstance(release_item, dict) else ''
        evidence_link = urljoin('https://www.cbs.gov.il', relative_release_url) if relative_release_url else source_url
        fetch_meta = {
            'status_code': release_meta['status_code'],
            'content_type': release_meta['content_type'],
            'bytes_written': page_meta['bytes_written'] + series_meta['bytes_written'] + release_meta['bytes_written'],
            'checksum_sha256': hashlib.sha256(raw_path.read_bytes() + series_raw_path.read_bytes() + release_raw_path.read_bytes()).hexdigest(),
        }

    return {
        'capture_status': 'completed',
        'status_code': fetch_meta['status_code'],
        'content_type': fetch_meta['content_type'],
        'bytes_written': fetch_meta['bytes_written'],
        'checksum_sha256': fetch_meta['checksum_sha256'],
        'raw_path': str(raw_path),
        'secondary_raw_path': str(series_raw_path) if not (source_url.startswith('file://') or source_url.endswith('.html')) else '',
        'tertiary_raw_path': str(release_raw_path) if not (source_url.startswith('file://') or source_url.endswith('.html')) else '',
        'metadata_source_url': source_url,
        'release_title': title,
        'latest_visible_date': latest_visible_date,
        'latest_period_covered': latest_period_covered,
        'normalized_summary': f"{manifest_row.get('source_name', '')} release row parsed from the embedded CBS price-index data list.",
        'verification_updates': {
            'source_id': manifest_row.get('source_id', ''),
            'last_checked_utc': captured_utc,
            'last_published_date': latest_visible_date,
            'latest_period_covered': latest_period_covered,
            'claim_date_utc': '',
            'owner': '',
            'evidence_link': evidence_link,
            'evidence_path': '',
            'status': 'in_review',
            'next_action': 'Keep extracting the latest CPI release row from the embedded CBS price-index data list instead of manual page inspection.',
            'notes': f"CBS Main Price Indices capture parsed on {captured_utc}. Latest CPI row: {title or 'not found'}; publication date: {latest_visible_date or 'not found'}; covered period: {latest_period_covered or 'not found'}.",
        },
    }


def parse_cbs_impexp_entries(xml_body: str) -> list[dict[str, str]]:
    root = ET.fromstring(xml_body)
    namespaces = {
        'a': 'http://www.w3.org/2005/Atom',
        'd': 'http://schemas.microsoft.com/ado/2007/08/dataservices',
        'm': 'http://schemas.microsoft.com/ado/2007/08/dataservices/metadata',
    }
    entries = []
    for entry in root.findall('a:entry', namespaces):
        props = entry.find('a:content/m:properties', namespaces)
        if props is None:
            continue
        entries.append(
            {
                'name': props.findtext('d:Name', default='', namespaces=namespaces),
                'modified': props.findtext('d:TimeLastModified', default='', namespaces=namespaces),
                'url': props.findtext('d:ServerRelativeUrl', default='', namespaces=namespaces),
            }
        )
    return entries


def choose_latest_cbs_impexp_period(entries: list[dict[str, str]]) -> tuple[str, list[dict[str, str]]]:
    grouped: dict[str, list[dict[str, str]]] = {}
    for entry in entries:
        match = re.fullmatch(r'(exp|imp)_(\d{1,2})_(\d{4})\.zip', entry.get('name', ''), flags=re.IGNORECASE)
        if not match:
            continue
        period = f"{int(match.group(3)):04d}-{int(match.group(2)):02d}"
        grouped.setdefault(period, []).append(entry)
    if not grouped:
        return '', []

    ranked_periods = []
    for period, rows in grouped.items():
        prefixes = {row.get('name', '').split('_', 1)[0].lower() for row in rows}
        ranked_periods.append((period_sort_key(period), len(prefixes), period, rows))
    ranked_periods.sort(reverse=True)
    _, _, best_period, best_rows = ranked_periods[0]
    return best_period, best_rows


def build_israel_cbs_impexp_payload(
    manifest_row: dict[str, str],
    adapter_row: dict[str, str],
    raw_path: Path,
    captured_utc: str,
) -> dict[str, Any]:
    source_url = israel_cbs_impexp_api_url(adapter_row.get('url', ''))
    fetch_meta = fetch_direct_source(source_url, raw_path)
    entries = parse_cbs_impexp_entries(raw_path.read_text(errors='replace'))
    latest_period_covered, period_entries = choose_latest_cbs_impexp_period(entries)
    latest_visible_date = latest_iso_date([row.get('modified', '') for row in period_entries])
    evidence_links = [urljoin('https://www.cbs.gov.il', row.get('url', '')) for row in sorted(period_entries, key=lambda row: row.get('name', ''))]
    evidence_link = source_url

    return {
        'capture_status': 'completed',
        'status_code': fetch_meta['status_code'],
        'content_type': fetch_meta['content_type'],
        'bytes_written': fetch_meta['bytes_written'],
        'checksum_sha256': fetch_meta['checksum_sha256'],
        'raw_path': str(raw_path),
        'metadata_source_url': source_url,
        'latest_visible_date': latest_visible_date,
        'latest_period_covered': latest_period_covered,
        'latest_period_files': evidence_links,
        'normalized_summary': f"{manifest_row.get('source_name', '')} monthly ZIP inventory parsed from the CBS document-library feed.",
        'verification_updates': {
            'source_id': manifest_row.get('source_id', ''),
            'last_checked_utc': captured_utc,
            'last_published_date': latest_visible_date,
            'latest_period_covered': latest_period_covered,
            'claim_date_utc': '',
            'owner': '',
            'evidence_link': evidence_link,
            'evidence_path': '',
            'status': 'in_review',
            'next_action': 'Keep using the CBS document-library API and watch for the next monthly import and export ZIP pair.',
            'notes': f"CBS imports and exports feed parsed on {captured_utc}. Latest period: {latest_period_covered or 'not found'}; latest file timestamp: {latest_visible_date or 'not found'}; files: {', '.join(Path(link).name for link in evidence_links) or 'not found'}.",
        },
    }


def extract_iaa_archive_candidates(html_body: str, archive_url: str) -> list[dict[str, str]]:
    candidates = []
    pattern = re.compile(
        r'(?:דו&quot;חות פעילות חודשיים לשנת|monthly activity reports(?: for year)?)\s*(\d{4})(.*?)(?=(?:דו&quot;חות פעילות חודשיים לשנת|monthly activity reports(?: for year)?)[^<]*\d{4}|$)',
        flags=re.IGNORECASE | re.DOTALL,
    )
    for match in pattern.finditer(html_body):
        year = match.group(1)
        block = match.group(2)
        for href, title, text in re.findall(r'<a[^>]+href="([^"]+\.pdf)"[^>]*?(?:title="([^"]*)")?[^>]*>(.*?)</a>', block, flags=re.IGNORECASE | re.DOTALL):
            label = html_to_visible_text(f'{title} {text} {href}')
            period = extract_named_month_period(f'{label} {year}', default_year=year)
            if not period:
                continue
            candidates.append(
                {
                    'period': period,
                    'href': urljoin(archive_url, html_lib.unescape(href)),
                    'title': label,
                }
            )
    return candidates


def build_israel_iaa_monthly_reports_payload(
    manifest_row: dict[str, str],
    adapter_row: dict[str, str],
    raw_path: Path,
    captured_utc: str,
) -> dict[str, Any]:
    archive_url = israel_iaa_monthly_reports_url(adapter_row.get('url', ''))
    fetch_meta = fetch_direct_source(archive_url, raw_path)
    html_body = raw_path.read_text(errors='replace')
    candidates = extract_iaa_archive_candidates(html_body, archive_url)
    candidates.sort(key=lambda row: period_sort_key(row.get('period', '')), reverse=True)
    selected = candidates[0] if candidates else {}
    latest_period_covered = selected.get('period', '')
    evidence_link = selected.get('href', archive_url)
    latest_visible_date = fetch_head_last_modified(evidence_link)

    return {
        'capture_status': 'completed',
        'status_code': fetch_meta['status_code'],
        'content_type': fetch_meta['content_type'],
        'bytes_written': fetch_meta['bytes_written'],
        'checksum_sha256': fetch_meta['checksum_sha256'],
        'raw_path': str(raw_path),
        'metadata_source_url': archive_url,
        'latest_visible_date': latest_visible_date,
        'latest_period_covered': latest_period_covered,
        'latest_report_url': evidence_link,
        'normalized_summary': f"{manifest_row.get('source_name', '')} archive page parsed for the latest monthly report PDF.",
        'verification_updates': {
            'source_id': manifest_row.get('source_id', ''),
            'last_checked_utc': captured_utc,
            'last_published_date': latest_visible_date,
            'latest_period_covered': latest_period_covered,
            'claim_date_utc': '',
            'owner': '',
            'evidence_link': evidence_link,
            'evidence_path': '',
            'status': 'in_review',
            'next_action': 'Keep discovering the latest monthly report from the IAA archive and use PDF headers as the release timestamp.',
            'notes': f"IAA monthly archive parsed on {captured_utc}. Latest visible period: {latest_period_covered or 'not found'}; selected report: {Path(urlparse(evidence_link).path).name or 'not found'}; release date: {latest_visible_date or 'not found'}.",
        },
    }


def build_usda_fas_gain_pdf_payload(
    manifest_row: dict[str, str],
    adapter_row: dict[str, str],
    raw_path: Path,
    captured_utc: str,
) -> dict[str, Any]:
    fetch_meta = fetch_direct_source(adapter_row.get('url', ''), raw_path)
    report_date = pdfinfo_report_date(raw_path)
    return {
        'capture_status': 'completed',
        'status_code': fetch_meta['status_code'],
        'content_type': fetch_meta['content_type'],
        'bytes_written': fetch_meta['bytes_written'],
        'checksum_sha256': fetch_meta['checksum_sha256'],
        'raw_path': str(raw_path),
        'metadata_source_url': adapter_row.get('url', ''),
        'latest_visible_date': report_date,
        'normalized_summary': f"{manifest_row.get('source_name', '')} PDF captured from the USDA FAS GAIN API endpoint.",
        'verification_updates': {
            'source_id': manifest_row.get('source_id', ''),
            'last_checked_utc': captured_utc,
            'last_published_date': report_date,
            'latest_period_covered': '',
            'claim_date_utc': '',
            'owner': '',
            'evidence_link': adapter_row.get('url', ''),
            'evidence_path': '',
            'status': 'in_review',
            'next_action': 'Extract the covered year or report title from the PDF text so the annual market baseline carries a structured period marker.',
            'notes': f"USDA FAS GAIN PDF captured on {captured_utc}. PDF metadata date: {report_date or 'not found'}.",
        },
    }


def build_lebanon_cas_cpi_payload(
    manifest_row: dict[str, str],
    adapter_row: dict[str, str],
    raw_path: Path,
    captured_utc: str,
) -> dict[str, Any]:
    page_url = lebanon_cas_cpi_page_url(adapter_row.get('url', ''))
    page_fetch = fetch_direct_source(page_url, raw_path)
    page_html = raw_path.read_text(errors='replace')
    json_url = lebanon_cas_cpi_json_url(page_html)
    json_raw_path = raw_path.with_name(f'{raw_path.stem}-data.json')
    data_payload, json_fetch = fetch_json_source(json_url, json_raw_path)
    beta_period_covered = latest_month_period_from_entries(data_payload.get('entries', {}))
    beta_last_modified = requests_head_last_modified(json_url)
    latest_period_covered = beta_period_covered
    latest_visible_date = beta_last_modified
    evidence_link = json_url
    tertiary_raw_path = ''
    pdf_fetch: dict[str, Any] | None = None

    official_candidate = select_lebanon_cas_cpi_release_candidate(
        extract_lebanon_cas_cpi_release_candidates(page_html, page_url)
    )
    official_period = official_candidate.get('latest_period_covered', '')
    if official_period and period_sort_key(official_period) > period_sort_key(beta_period_covered):
        official_pdf_url = official_candidate.get('pdf_url', '')
        pdf_raw_path = raw_path.with_name(f'{raw_path.stem}-official.pdf')
        pdf_fetch = fetch_direct_source(official_pdf_url, pdf_raw_path)
        tertiary_raw_path = str(pdf_raw_path)
        latest_period_covered = official_period
        latest_visible_date = requests_head_last_modified(official_pdf_url) or beta_last_modified
        evidence_link = official_pdf_url

    checksum_parts = [raw_path.read_bytes(), json_raw_path.read_bytes()]
    bytes_written = page_fetch['bytes_written'] + json_fetch['bytes_written']
    if pdf_fetch:
        checksum_parts.append(Path(tertiary_raw_path).read_bytes())
        bytes_written += pdf_fetch['bytes_written']

    return {
        'capture_status': 'completed',
        'status_code': (pdf_fetch or json_fetch)['status_code'],
        'content_type': (pdf_fetch or json_fetch)['content_type'],
        'bytes_written': bytes_written,
        'checksum_sha256': hashlib.sha256(b''.join(checksum_parts)).hexdigest(),
        'raw_path': str(raw_path),
        'secondary_raw_path': str(json_raw_path),
        'tertiary_raw_path': tertiary_raw_path,
        'metadata_source_url': json_url,
        'latest_visible_date': latest_visible_date,
        'latest_period_covered': latest_period_covered,
        'normalized_summary': (
            f"{manifest_row.get('source_name', '')} CPI dashboard captured from beta.cas.gov.lb."
            if not pdf_fetch
            else f"{manifest_row.get('source_name', '')} CPI dashboard captured with a newer official monthly PDF from cas.gov.lb."
        ),
        'verification_updates': {
            'source_id': manifest_row.get('source_id', ''),
            'last_checked_utc': captured_utc,
            'last_published_date': latest_visible_date,
            'latest_period_covered': latest_period_covered,
            'claim_date_utc': '',
            'owner': '',
            'evidence_link': evidence_link,
            'evidence_path': '',
            'status': 'in_review',
            'next_action': (
                'Track the CAS CPI dashboard JSON feed and the official CAS monthly CPI publication page; promote the official PDF when it is newer than the beta JSON feed.'
            ),
            'notes': (
                f"CAS CPI dashboard captured on {captured_utc}. Beta feed last-modified date: {beta_last_modified or 'not found'}; "
                f"latest visible period in beta JSON: {beta_period_covered or 'not found'}; "
                + (
                    f"newer official CPI PDF selected: {evidence_link} with covered period {latest_period_covered or 'not found'} and last-modified date {latest_visible_date or 'not found'}."
                    if pdf_fetch
                    else f"evidence remains the beta JSON feed at {json_url}."
                )
            ),
        },
    }


def build_saudi_gastat_cpi_payload(
    manifest_row: dict[str, str],
    adapter_row: dict[str, str],
    raw_path: Path,
    captured_utc: str,
) -> dict[str, Any]:
    source_url = adapter_row.get('url', '')
    listing_url = saudi_gastat_cpi_listing_url(source_url)
    fetch_meta = fetch_direct_source(listing_url, raw_path)
    html_body = raw_path.read_text(errors='ignore')
    metadata_source_url = listing_url or source_url
    listing_metadata = extract_gastat_cpi_listing_metadata(html_body, metadata_source_url)
    evidence_link = listing_metadata.get('evidence_link', '')
    last_published_date = requests_head_last_modified(evidence_link) if evidence_link else ''
    latest_period_covered = listing_metadata.get('latest_period_covered', '')
    document_title = listing_metadata.get('document_title', '')

    if not latest_period_covered:
        if listing_url != source_url and source_url:
            fetch_meta = fetch_direct_source(source_url, raw_path)
            html_body = raw_path.read_text(errors='ignore')
            metadata_source_url = source_url
        last_published_date = extract_gastat_last_modified_date(html_body)
        if last_published_date:
            published_dt = parse_accounting_date(last_published_date)
            if published_dt is not None:
                latest_period_covered = (published_dt.replace(day=1) - timedelta(days=1)).strftime('%Y-%m')
        evidence_link = source_url
        document_title = ''

    return {
        'capture_status': 'completed',
        'status_code': fetch_meta['status_code'],
        'content_type': fetch_meta['content_type'],
        'bytes_written': fetch_meta['bytes_written'],
        'checksum_sha256': fetch_meta['checksum_sha256'],
        'raw_path': str(raw_path),
        'metadata_source_url': metadata_source_url,
        'document_title': document_title,
        'latest_visible_date': last_published_date,
        'latest_period_covered': latest_period_covered,
        'normalized_summary': 'Saudi GASTAT CPI listing captured and parsed for the latest monthly publication, with footer fallback if listing metadata is unavailable.',
        'verification_updates': {
            'source_id': manifest_row.get('source_id', ''),
            'last_checked_utc': captured_utc,
            'last_published_date': last_published_date,
            'latest_period_covered': latest_period_covered,
            'claim_date_utc': '',
            'owner': '',
            'evidence_link': evidence_link or source_url,
            'evidence_path': '',
            'status': 'in_review',
            'next_action': 'Check for the next monthly CPI release and capture the exact release page if January 2026 or later is available',
            'notes': (
                f"GASTAT CPI source captured on {captured_utc}. Evidence link: {(evidence_link or source_url) or 'not found'}; "
                f"publication date: {last_published_date or 'not found'}; covered period: {latest_period_covered or 'not found'}"
                f"{f'; title: {document_title}' if document_title else '.'}"
            ),
        },
    }


def build_hdx_dataset_metadata_payload(
    manifest_row: dict[str, str],
    adapter_row: dict[str, str],
    raw_path: Path,
    captured_utc: str,
) -> dict[str, Any]:
    api_url = hdx_dataset_api_url(adapter_row.get('url', ''))
    package_payload, fetch_meta = fetch_json_source(api_url, raw_path)
    package = package_payload.get('result', package_payload)
    resource_dates = []
    for resource in package.get('resources', []):
        if not isinstance(resource, dict):
            continue
        resource_dates.extend(
            [
                resource.get('last_modified', ''),
                resource.get('created', ''),
            ]
        )
    latest_resource_date = latest_iso_date(resource_dates)
    metadata_date = latest_iso_date(
        [
            package.get('metadata_modified', ''),
            package.get('metadata_created', ''),
        ]
    )
    latest_visible_date = metadata_date or latest_resource_date
    data_update_frequency = str(package.get('data_update_frequency', '') or '').strip()
    due_date = str(package.get('due_date', '') or '').strip()
    update_status = str(package.get('update_status', '') or '').strip()
    updated_by_script = str(package.get('updated_by_script', '') or '').strip()
    notes = (
        f"HDX dataset metadata sync captured on {captured_utc}. Latest visible metadata date: "
        f"{latest_visible_date or 'not exposed'}; latest resource date: {latest_resource_date or 'not exposed'}."
    )
    if data_update_frequency:
        notes += f" Declared update frequency: every {data_update_frequency} day(s)."
    if due_date:
        notes += f" Due date: {due_date}."
    if update_status:
        notes += f" Update status: {update_status}."
    return {
        'capture_status': 'completed',
        'status_code': fetch_meta['status_code'],
        'content_type': fetch_meta['content_type'],
        'bytes_written': fetch_meta['bytes_written'],
        'checksum_sha256': fetch_meta['checksum_sha256'],
        'raw_path': str(raw_path),
        'metadata_source_url': api_url,
        'dataset_title': package.get('title', manifest_row.get('source_name', '')),
        'dataset_name': package.get('name', ''),
        'metadata_modified': package.get('metadata_modified', ''),
        'metadata_created': package.get('metadata_created', ''),
        'data_update_frequency': data_update_frequency,
        'due_date': due_date,
        'update_status': update_status,
        'updated_by_script': updated_by_script,
        'resource_count': len(package.get('resources', [])),
        'latest_resource_modified_date': latest_resource_date,
        'normalized_summary': f"{manifest_row.get('source_name', '')} metadata refresh captured from HDX package metadata.",
        'verification_updates': {
            'source_id': manifest_row.get('source_id', ''),
            'last_checked_utc': captured_utc,
            'last_published_date': latest_visible_date,
            'latest_period_covered': '',
            'claim_date_utc': '',
            'owner': '',
            'evidence_link': api_url,
            'evidence_path': '',
            'status': 'in_review',
            'next_action': 'Review dataset resources for an explicit coverage-period marker after metadata sync.',
            'notes': notes,
        },
    }


def build_hdx_hapi_changelog_payload(
    manifest_row: dict[str, str],
    adapter_row: dict[str, str],
    raw_path: Path,
    captured_utc: str,
) -> dict[str, Any]:
    faq_url = hdx_hapi_faq_url(adapter_row.get('url', ''))
    changelog_url = hdx_hapi_changelog_url(adapter_row.get('url', ''))
    fetch_meta = fetch_direct_source(faq_url, raw_path)
    faq_html = raw_path.read_text(errors='replace')
    faq_text = html_to_visible_text(faq_html)
    faq_last_modified = requests_head_last_modified(faq_url)
    daily_update_statement = 'updated daily from the source data' in faq_text.lower()
    secondary_raw_path = ''
    changelog_latest_visible_date = ''
    if changelog_url and changelog_url != faq_url:
        changelog_raw_path = raw_path.with_name(f'{raw_path.stem}-changelog.html')
        changelog_fetch = fetch_direct_source(changelog_url, changelog_raw_path)
        changelog_html = changelog_raw_path.read_text(errors='replace')
        changelog_latest_visible_date = latest_iso_date(re.findall(r'\b20\d{2}-\d{2}-\d{2}\b', changelog_html))
        fetch_meta = {
            'status_code': fetch_meta['status_code'],
            'content_type': fetch_meta['content_type'],
            'bytes_written': fetch_meta['bytes_written'] + changelog_fetch['bytes_written'],
            'checksum_sha256': hashlib.sha256(raw_path.read_bytes() + changelog_raw_path.read_bytes()).hexdigest(),
        }
        secondary_raw_path = str(changelog_raw_path)
    latest_visible_date = faq_last_modified or latest_iso_date(re.findall(r'\b20\d{2}-\d{2}-\d{2}\b', faq_html))
    status = 'in_review' if daily_update_statement and latest_visible_date else 'manual_review'
    next_action = (
        'Use the public HAPI FAQ as the product-level freshness marker and confirm dataset-specific cadence from the underlying source datasets.'
        if status == 'in_review'
        else 'Capture a source-data coverage marker in addition to the product FAQ because per-dataset cadence still varies.'
    )
    notes = (
        f"Public HAPI FAQ captured on {captured_utc}. FAQ last-modified date: {latest_visible_date or 'not found'}; "
        f"daily-update statement present: {'yes' if daily_update_statement else 'no'}"
    )
    if changelog_latest_visible_date:
        notes += f"; changelog latest visible date: {changelog_latest_visible_date}."
    else:
        notes += '.'
    return {
        'capture_status': 'completed',
        'status_code': fetch_meta['status_code'],
        'content_type': fetch_meta['content_type'],
        'bytes_written': fetch_meta['bytes_written'],
        'checksum_sha256': fetch_meta['checksum_sha256'],
        'raw_path': str(raw_path),
        'secondary_raw_path': secondary_raw_path,
        'metadata_source_url': faq_url,
        'latest_visible_date': latest_visible_date,
        'normalized_summary': f"{manifest_row.get('source_name', '')} FAQ refresh captured from the public HAPI product page.",
        'verification_updates': {
            'source_id': manifest_row.get('source_id', ''),
            'last_checked_utc': captured_utc,
            'last_published_date': latest_visible_date,
            'latest_period_covered': '',
            'claim_date_utc': '',
            'owner': '',
            'evidence_link': faq_url,
            'evidence_path': '',
            'status': status,
            'next_action': next_action,
            'notes': notes,
        },
    }


def build_hdx_signals_story_payload(
    manifest_row: dict[str, str],
    adapter_row: dict[str, str],
    raw_path: Path,
    captured_utc: str,
) -> dict[str, Any]:
    story_url = hdx_signals_story_url(adapter_row.get('url', ''))
    author_url = hdx_signals_author_archive_url(adapter_row.get('url', ''))
    author_raw_path = raw_path.with_name(f'{raw_path.stem}-author{raw_path.suffix}')

    story_fetch = fetch_direct_source(story_url, raw_path)
    author_fetch = fetch_direct_source(author_url, author_raw_path)

    story_text = html_to_visible_text(raw_path.read_text(errors='replace'))
    author_text = html_to_visible_text(author_raw_path.read_text(errors='replace'))
    latest_visible_date = extract_first_month_day_year(author_text) or extract_first_month_day_year(story_text)
    latest_period_covered = extract_month_year(story_text, 'As of')

    return {
        'capture_status': 'completed',
        'status_code': story_fetch['status_code'],
        'content_type': story_fetch['content_type'],
        'bytes_written': story_fetch['bytes_written'] + author_fetch['bytes_written'],
        'checksum_sha256': hashlib.sha256((raw_path.read_bytes() + author_raw_path.read_bytes())).hexdigest(),
        'raw_path': str(raw_path),
        'secondary_raw_path': str(author_raw_path),
        'story_source_url': story_url,
        'author_archive_url': author_url,
        'normalized_summary': f"{manifest_row.get('source_name', '')} story capture parsed from the Centre resource page and author archive.",
        'verification_updates': {
            'source_id': manifest_row.get('source_id', ''),
            'last_checked_utc': captured_utc,
            'last_published_date': latest_visible_date,
            'latest_period_covered': latest_period_covered,
            'claim_date_utc': '',
            'owner': '',
            'evidence_link': story_url,
            'evidence_path': '',
            'status': 'manual_review',
            'next_action': 'Use the Signals story page as the coverage source and keep watching for a machine-readable feed or API endpoint.',
            'notes': (
                f"Signals story parsed on {captured_utc}. Story evidence: {story_url}. "
                f"Publication date from the author archive: {latest_visible_date or 'not found'}. "
                f"Coverage marker from the story page: {latest_period_covered or 'not found'}."
            ),
        },
    }


def build_unhcr_document_index_payload(
    manifest_row: dict[str, str],
    adapter_row: dict[str, str],
    raw_path: Path,
    captured_utc: str,
) -> dict[str, Any]:
    index_url = unhcr_document_index_url(adapter_row.get('url', ''))
    index_fetch = fetch_direct_source(index_url, raw_path)
    candidate = select_unhcr_document_candidate(
        manifest_row.get('source_name', ''),
        extract_unhcr_document_candidates(raw_path.read_text(errors='replace')),
    )
    if not candidate:
        raise ValueError(f'No matching UNHCR document found for {manifest_row.get("source_name", "")}.')

    detail_raw_path = raw_path.with_name(f'{raw_path.stem}-detail.html')
    detail_fetch = fetch_direct_source(candidate.get('detail_page_url', ''), detail_raw_path)
    detail_text = html_to_visible_text(detail_raw_path.read_text(errors='replace'))
    latest_period_covered = extract_unhcr_issue_period(' '.join([candidate.get('title', ''), candidate.get('body', ''), detail_text]))
    pdf_raw_path = raw_path.with_name(f'{raw_path.stem}-document.pdf')
    pdf_fetch: dict[str, Any] | None = None
    if not latest_period_covered and candidate.get('download_url', ''):
        pdf_fetch = fetch_direct_source(candidate.get('download_url', ''), pdf_raw_path)
        latest_period_covered = extract_unhcr_issue_period(pdftotext_content(pdf_raw_path))
    upload_date = extract_unhcr_detail_upload_date(detail_text) or candidate.get('upload_date', '')
    checksum_parts = [raw_path.read_bytes(), detail_raw_path.read_bytes()]
    bytes_written = index_fetch['bytes_written'] + detail_fetch['bytes_written']
    tertiary_raw_path = ''
    if pdf_fetch:
        checksum_parts.append(pdf_raw_path.read_bytes())
        bytes_written += pdf_fetch['bytes_written']
        tertiary_raw_path = str(pdf_raw_path)

    return {
        'capture_status': 'completed',
        'status_code': detail_fetch['status_code'],
        'content_type': detail_fetch['content_type'],
        'bytes_written': bytes_written,
        'checksum_sha256': hashlib.sha256(b''.join(checksum_parts)).hexdigest(),
        'raw_path': str(raw_path),
        'secondary_raw_path': str(detail_raw_path),
        'tertiary_raw_path': tertiary_raw_path,
        'metadata_source_url': index_url,
        'detail_page_url': candidate.get('detail_page_url', ''),
        'download_url': candidate.get('download_url', ''),
        'document_id': candidate.get('document_id', ''),
        'document_title': candidate.get('title', ''),
        'latest_visible_date': candidate.get('publish_date', ''),
        'upload_date': upload_date,
        'latest_period_covered': latest_period_covered,
        'normalized_summary': f"{manifest_row.get('source_name', '')} latest UNHCR document resolved from the official index page.",
        'verification_updates': {
            'source_id': manifest_row.get('source_id', ''),
            'last_checked_utc': captured_utc,
            'last_published_date': candidate.get('publish_date', ''),
            'latest_period_covered': latest_period_covered,
            'claim_date_utc': '',
            'owner': '',
            'evidence_link': candidate.get('detail_page_url', ''),
            'evidence_path': '',
            'status': 'in_review',
            'next_action': 'Keep following the UNHCR document index and promote the newest matching detail page when a later publication appears.',
            'notes': (
                f"UNHCR document index captured on {captured_utc}. Latest matching title: {candidate.get('title', 'not found')}; "
                f"publish date: {candidate.get('publish_date', 'not found')}; upload date: {upload_date or 'not found'}; "
                f"covered period: {latest_period_covered or 'not found'}"
                f"{'; extracted from linked PDF fallback' if pdf_fetch and latest_period_covered else '.'}"
            ),
        },
    }


def build_ipc_lebanon_analysis_payload(
    manifest_row: dict[str, str],
    adapter_row: dict[str, str],
    raw_path: Path,
    captured_utc: str,
    plans_dir: Path | None = None,
) -> dict[str, Any]:
    source_id = manifest_row.get('source_id', '')
    page_url = ipc_lebanon_analysis_url(adapter_row.get('url', ''))
    fallback = load_manual_finding(plans_dir, source_id)
    metadata_source_url = fallback.get('evidence_link', '') or page_url
    latest_visible_date = fallback.get('last_published_date', '')
    latest_period_covered = fallback.get('latest_period_covered', '')
    latest_report_url = fallback.get('evidence_link', '') or page_url
    summary = fallback.get('notes', f"{manifest_row.get('source_name', '')} manual fallback evidence.")
    fetch_meta = {'status_code': 0, 'content_type': 'text/plain', 'bytes_written': 0, 'checksum_sha256': ''}
    try:
        fetch_meta = fetch_direct_source(page_url, raw_path)
        metadata = extract_ipc_lebanon_analysis_metadata(raw_path.read_text(errors='replace'), page_url)
        if not metadata.get('latest_visible_date'):
            raise ValueError('IPC Lebanon analysis page did not expose a release date.')
        metadata_source_url = page_url
        latest_visible_date = metadata.get('latest_visible_date', '')
        latest_period_covered = metadata.get('latest_period_covered', '')
        latest_report_url = metadata.get('latest_report_url', '')
        summary = (
            f"{manifest_row.get('source_name', '')} country-analysis page parsed for release date, validity window, and linked report PDF."
        )
    except (HTTPError, URLError, TimeoutError) as exc:
        error_text = f"Collection blocked: {exc}"
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        raw_path.write_text(error_text, encoding='utf-8', errors='replace')
        fetch_meta = {
            'status_code': getattr(exc, 'code', 0) or 0,
            'content_type': 'text/plain',
            'bytes_written': len(error_text.encode()),
            'checksum_sha256': hashlib.sha256(error_text.encode()).hexdigest(),
        }
        if not latest_visible_date:
            raise

    return {
        'capture_status': 'completed',
        'status_code': fetch_meta['status_code'],
        'content_type': fetch_meta['content_type'],
        'bytes_written': fetch_meta['bytes_written'],
        'checksum_sha256': fetch_meta['checksum_sha256'],
        'raw_path': str(raw_path),
        'metadata_source_url': metadata_source_url,
        'latest_visible_date': latest_visible_date,
        'latest_period_covered': latest_period_covered,
        'latest_report_url': latest_report_url,
        'normalized_summary': summary,
        'verification_updates': {
            'source_id': source_id,
            'last_checked_utc': captured_utc,
            'last_published_date': latest_visible_date,
            'latest_period_covered': latest_period_covered,
            'claim_date_utc': '',
            'owner': '',
            'evidence_link': metadata_source_url,
            'evidence_path': '',
            'status': 'in_review',
            'next_action': 'Keep watching the IPC Lebanon country-analysis page for the next release date and validity-window update.',
            'notes': summary,
        },
    }


def load_manual_finding(plans_dir: Path, source_id: str) -> dict[str, str]:
    if not plans_dir:
        return {}
    findings_path = plans_dir / 'source_verification_findings.csv'
    rows = load_existing_rows_by_key(findings_path, 'source_id')
    return rows.get(source_id, {})


def load_recent_accounting_finding(plans_dir: Path, source_id: str) -> dict[str, str]:
    if not plans_dir:
        return {}
    rows = load_existing_recent_accounting(plans_dir / 'recent_accounting.csv')
    return rows.get(source_id, {})


def extract_html_title(text: str) -> str:
    match = re.search(r'<h1[^>]*>(.*?)</h1>', text, flags=re.IGNORECASE | re.DOTALL)
    if match:
        return html_to_visible_text(match.group(1))
    match = re.search(r'<title>(.*?)</title>', text, flags=re.IGNORECASE | re.DOTALL)
    return html_to_visible_text(match.group(1)) if match else ''


def parse_ipc_gaza_publish_date(html: str) -> str:
    match = re.search(r'Published\s*(?:on)?\s*([0-9]{1,2}\s+[A-Za-z]+\s+[0-9]{4})', html, flags=re.IGNORECASE)
    if not match:
        return ''
    return parse_written_date(match.group(1))

def parse_ipc_gaza_period(html: str) -> str:
    match = re.search(r'Period\s*[:\-|–]\s*([A-Za-z0-9\s\-/]+)', html, flags=re.IGNORECASE)
    return match.group(1).strip() if match else ''

def parse_ashdod_published_date(html: str) -> str:
    match = re.search(r'datePublished"\s*:\s*"(\d{4}-\d{2}-\d{2})', html)
    if match:
        parsed = parse_accounting_date(match.group(1))
        return parsed.date().isoformat() if parsed else ''
    match = re.search(r'Published\s*(?:on)?\s*([0-9]{1,2}\s+[A-Za-z]+\s+[0-9]{4})', html, flags=re.IGNORECASE)
    if match:
        parsed = parse_accounting_date(match.group(1))
        return parsed.date().isoformat() if parsed else ''
    return ''

def extract_anyflip_url(text: str) -> str:
    match = re.search(r'https://anyflip\.com/[^\s\'")]+', text)
    return match.group(0).rstrip('.,;:') if match else ''

def derive_ashdod_period_from_text(text: str) -> str:
    normalized = html_lib.unescape(text)
    year_match = re.search(r'(20\d{2})', normalized)
    if not year_match:
        return ''
    year = year_match.group(1)
    if re.search(r'תשעת חודשים ראשונים', normalized):
        return f'{year}-09'
    if re.search(r'מחצית ראשונה', normalized):
        return f'{year}-06'
    if re.search(r'רבעון ראשון', normalized):
        return f'{year}-03'
    if re.search(r'לשנת\s*20\d{2}', normalized) or re.search(r'שנת\s*20\d{2}', normalized):
        return f'{year}-12'
    return ''

def parse_ashdod_period(html: str) -> str:
    match = re.search(r'Period\s*[:\-|–]\s*([0-9]{4}-[0-9]{2}|[0-9]+\s+[A-Za-z]+\s+[0-9]{4})', html, flags=re.IGNORECASE)
    if match:
        return match.group(1).strip()
    title = extract_html_title(html)
    if title:
        derived = derive_ashdod_period_from_text(title)
        if derived:
            return derived
    description_match = re.search(r'<meta\s+name="description"\s+content="([^"]+)"', html, flags=re.IGNORECASE)
    if description_match:
        derived = derive_ashdod_period_from_text(description_match.group(1))
        if derived:
            return derived
    return derive_ashdod_period_from_text(html_to_visible_text(html))

def build_ipc_gaza_snapshot_payload(
    manifest_row: dict[str, str],
    adapter_row: dict[str, str],
    raw_path: Path,
    captured_utc: str,
    plans_dir: Path,
) -> dict[str, Any]:
    source_id = manifest_row.get('source_id', '')
    source_url = adapter_row.get('url', '')
    fallback = load_manual_finding(plans_dir, source_id)
    metadata_source_url = fallback.get('evidence_link', source_url)
    title = fallback.get('notes', manifest_row.get('source_name', 'IPC Gaza snapshot'))
    latest_visible_date = fallback.get('last_published_date', '')
    latest_period_covered = fallback.get('latest_period_covered', '')
    summary = fallback.get('notes', f"{manifest_row.get('source_name', '')} manual fallback evidence.")
    fetch_meta = {'status_code': 0, 'content_type': 'text/plain', 'bytes_written': 0, 'checksum_sha256': ''}
    try:
        fetch_meta = fetch_direct_source(source_url, raw_path)
        page_html = raw_path.read_text(errors='replace')
        parsed_title = extract_html_title(page_html) or title
        parsed_date = parse_ipc_gaza_publish_date(page_html)
        if parsed_date:
            latest_visible_date = parsed_date
        parsed_period = parse_ipc_gaza_period(page_html)
        if parsed_period:
            latest_period_covered = parsed_period
        metadata_source_url = source_url
        title = parsed_title or title
        summary = f"{manifest_row.get('source_name', '')} page parsed for the latest Gaza snapshot update."
    except (HTTPError, URLError, TimeoutError) as exc:
        error_text = f"Collection blocked: {exc}"
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        raw_path.write_text(error_text, encoding='utf-8', errors='replace')
        fetch_meta = {
            'status_code': getattr(exc, 'code', 0) or 0,
            'content_type': 'text/plain',
            'bytes_written': len(error_text.encode()),
            'checksum_sha256': hashlib.sha256(error_text.encode()).hexdigest(),
        }
        metadata_source_url = fallback.get('evidence_link', source_url)
        summary = fallback.get('notes', summary)
    return {
        'capture_status': 'completed',
        'status_code': fetch_meta['status_code'],
        'content_type': fetch_meta['content_type'],
        'bytes_written': fetch_meta['bytes_written'],
        'checksum_sha256': fetch_meta['checksum_sha256'],
        'raw_path': str(raw_path),
        'metadata_source_url': metadata_source_url,
        'document_title': title,
        'latest_visible_date': latest_visible_date,
        'latest_period_covered': latest_period_covered,
        'normalized_summary': summary,
        'verification_updates': {
            'source_id': source_id,
            'last_checked_utc': captured_utc,
            'last_published_date': latest_visible_date,
            'latest_period_covered': latest_period_covered,
            'claim_date_utc': '',
            'owner': '',
            'evidence_link': metadata_source_url,
            'evidence_path': '',
            'status': 'in_review',
            'next_action': 'Keep collecting the IPC Gaza snapshot archive page or fallback PDF when direct requests remain blocked.',
            'notes': summary,
        },
    }


def build_iom_dtm_sudan_payload(
    manifest_row: dict[str, str],
    adapter_row: dict[str, str],
    raw_path: Path,
    captured_utc: str,
    plans_dir: Path,
) -> dict[str, Any]:
    source_id = manifest_row.get('source_id', '')
    source_url = adapter_row.get('url', '')
    fallback = load_recent_accounting_finding(plans_dir, source_id)
    latest_visible_date = fallback.get('last_published_date', '')
    latest_period_covered = fallback.get('latest_period_covered', '')
    claim_date_utc = fallback.get('claim_date_utc', '')
    evidence_link = fallback.get('evidence_link', '') or source_url
    next_action = fallback.get('next_action', 'Follow the IOM Sudan page for the next DTM displacement update.')
    summary = fallback.get('notes', f"{manifest_row.get('source_name', '')} public page metadata preserved from the verified ledger.")
    fetch_meta = {'status_code': 0, 'content_type': 'text/plain', 'bytes_written': 0, 'checksum_sha256': ''}

    try:
        fetch_meta = fetch_direct_source(source_url, raw_path)
        if not latest_visible_date:
            latest_visible_date = requests_head_last_modified(source_url)
        if latest_visible_date:
            summary = (
                f"{manifest_row.get('source_name', '')} page fetched successfully; the verified accounting row remains "
                'the active source for freshness metadata until a stable parser is added.'
            )
    except (HTTPError, URLError, TimeoutError) as exc:
        error_text = f"Collection blocked: {exc}"
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        raw_path.write_text(error_text, encoding='utf-8', errors='replace')
        fetch_meta = {
            'status_code': getattr(exc, 'code', 0) or 0,
            'content_type': 'text/plain',
            'bytes_written': len(error_text.encode()),
            'checksum_sha256': hashlib.sha256(error_text.encode()).hexdigest(),
        }
        if latest_visible_date or latest_period_covered:
            summary = (
                f"{manifest_row.get('source_name', '')} public page was blocked; preserving the latest verified "
                'accounting row as fallback evidence until a collectible public metadata surface is available.'
            )
        else:
            raise

    return {
        'capture_status': 'completed',
        'status_code': fetch_meta['status_code'],
        'content_type': fetch_meta['content_type'],
        'bytes_written': fetch_meta['bytes_written'],
        'checksum_sha256': fetch_meta['checksum_sha256'],
        'raw_path': str(raw_path),
        'metadata_source_url': source_url,
        'latest_visible_date': latest_visible_date,
        'latest_period_covered': latest_period_covered,
        'normalized_summary': summary,
        'verification_updates': {
            'source_id': source_id,
            'last_checked_utc': captured_utc,
            'last_published_date': latest_visible_date,
            'latest_period_covered': latest_period_covered,
            'claim_date_utc': claim_date_utc,
            'owner': '',
            'evidence_link': evidence_link,
            'evidence_path': '',
            'status': fallback.get('status', 'in_review') or 'in_review',
            'next_action': next_action,
            'notes': summary,
        },
    }


def build_ashdod_port_financials_payload(
    manifest_row: dict[str, str],
    adapter_row: dict[str, str],
    raw_path: Path,
    captured_utc: str,
    plans_dir: Path,
) -> dict[str, Any]:
    source_id = manifest_row.get('source_id', '')
    source_url = adapter_row.get('url', '')
    fallback = load_manual_finding(plans_dir, source_id)
    metadata_source_url = source_url
    title = fallback.get('notes', manifest_row.get('source_name', 'Ashdod Port update'))
    latest_visible_date = fallback.get('last_published_date', '')
    latest_period_covered = fallback.get('latest_period_covered', '')
    summary = fallback.get('notes', f"{manifest_row.get('source_name', '')} fallback update capture.")
    fetch_meta = {'status_code': 0, 'content_type': 'text/plain', 'bytes_written': 0, 'checksum_sha256': ''}
    secondary_raw_path = ''
    mirror_evidence_link = ''
    try:
        fetch_meta = fetch_direct_source(source_url, raw_path)
        page_html = raw_path.read_text(errors='replace')
        parsed_title = extract_html_title(page_html) or title
        parsed_date = parse_ashdod_published_date(page_html)
        if parsed_date:
            latest_visible_date = parsed_date
        parsed_period = parse_ashdod_period(page_html)
        if parsed_period:
            latest_period_covered = parsed_period
        metadata_source_url = source_url
        title = parsed_title or title
        summary = f"{manifest_row.get('source_name', '')} financial-information page parsed for the latest release."
    except (HTTPError, URLError, TimeoutError) as exc:
        error_text = f"Collection blocked: {exc}"
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        raw_path.write_text(error_text, encoding='utf-8', errors='replace')
        fetch_meta = {
            'status_code': getattr(exc, 'code', 0) or 0,
            'content_type': 'text/plain',
            'bytes_written': len(error_text.encode()),
            'checksum_sha256': hashlib.sha256(error_text.encode()).hexdigest(),
        }
        summary = fallback.get('notes', summary)
        fallback_url = fallback.get('evidence_link', '').strip()
        if not fallback_url or fallback_url == source_url:
            fallback_url = extract_anyflip_url(fallback.get('notes', ''))
        if fallback_url and fallback_url != source_url:
            fallback_raw_path = raw_path.with_name(f'{raw_path.stem}-fallback{raw_path.suffix}')
            try:
                fallback_fetch = fetch_direct_source(fallback_url, fallback_raw_path)
                fallback_html = fallback_raw_path.read_text(errors='replace')
                parsed_title = extract_html_title(fallback_html) or title
                parsed_date = parse_ashdod_published_date(fallback_html)
                if parsed_date:
                    latest_visible_date = parsed_date
                parsed_period = parse_ashdod_period(fallback_html)
                if parsed_period:
                    latest_period_covered = parsed_period
                mirror_evidence_link = fallback_url
                title = parsed_title or title
                summary = (
                    f"{manifest_row.get('source_name', '')} official hub was blocked; metadata was parsed from the "
                    f"AnyFlip mirror fallback at {fallback_url}."
                )
                secondary_raw_path = str(fallback_raw_path)
                fetch_meta = {
                    'status_code': fallback_fetch['status_code'],
                    'content_type': fallback_fetch['content_type'],
                    'bytes_written': len(error_text.encode()) + fallback_fetch['bytes_written'],
                    'checksum_sha256': hashlib.sha256(raw_path.read_bytes() + fallback_raw_path.read_bytes()).hexdigest(),
                }
            except (HTTPError, URLError, TimeoutError):
                pass
    return {
        'capture_status': 'completed',
        'status_code': fetch_meta['status_code'],
        'content_type': fetch_meta['content_type'],
        'bytes_written': fetch_meta['bytes_written'],
        'checksum_sha256': fetch_meta['checksum_sha256'],
        'raw_path': str(raw_path),
        'secondary_raw_path': secondary_raw_path,
        'metadata_source_url': metadata_source_url,
        'mirror_evidence_link': mirror_evidence_link,
        'document_title': title,
        'latest_visible_date': latest_visible_date,
        'latest_period_covered': latest_period_covered,
        'normalized_summary': summary,
        'verification_updates': {
            'source_id': source_id,
            'last_checked_utc': captured_utc,
            'last_published_date': latest_visible_date,
            'latest_period_covered': latest_period_covered,
            'claim_date_utc': '',
            'owner': '',
            'evidence_link': metadata_source_url,
            'mirror_evidence_link': mirror_evidence_link,
            'evidence_path': '',
            'status': 'in_review',
            'next_action': 'Track the Ashdod financial-information hub and the newest linked presentation to capture future releases.',
            'notes': summary,
        },
    }

def build_acaps_country_page_payload(
    manifest_row: dict[str, str],
    adapter_row: dict[str, str],
    raw_path: Path,
    captured_utc: str,
) -> dict[str, Any]:
    page_url = acaps_country_page_url(adapter_row.get('url', ''))
    fetch_meta = fetch_direct_source(page_url, raw_path)
    metadata = extract_acaps_country_page_metadata(raw_path.read_text(errors='replace'), page_url)
    if not metadata.get('latest_visible_date'):
        raise ValueError('ACAPS country page did not expose a latest visible update date.')

    latest_summary = metadata.get('latest_summary', '')
    return {
        'capture_status': 'completed',
        'status_code': fetch_meta['status_code'],
        'content_type': fetch_meta['content_type'],
        'bytes_written': fetch_meta['bytes_written'],
        'checksum_sha256': fetch_meta['checksum_sha256'],
        'raw_path': str(raw_path),
        'metadata_source_url': page_url,
        'latest_visible_date': metadata.get('latest_visible_date', ''),
        'latest_summary': latest_summary,
        'normalized_summary': f"{manifest_row.get('source_name', '')} country page parsed for the latest dated update item.",
        'verification_updates': {
            'source_id': manifest_row.get('source_id', ''),
            'last_checked_utc': captured_utc,
            'last_published_date': metadata.get('latest_visible_date', ''),
            'latest_period_covered': '',
            'claim_date_utc': '',
            'owner': '',
            'evidence_link': metadata.get('evidence_link', page_url),
            'evidence_path': '',
            'status': 'in_review',
            'next_action': 'Watch the ACAPS Lebanon country page and archive index for the next dated update or crisis note.',
            'notes': (
                f"ACAPS Lebanon country page parsed on {captured_utc}. Latest visible update date: "
                f"{metadata.get('latest_visible_date', 'not found')}. Latest summary excerpt: "
                f"{latest_summary[:180] or 'not found'}."
            ),
        },
    }


def build_wfp_lebanon_factsheet_pdf_payload(
    manifest_row: dict[str, str],
    adapter_row: dict[str, str],
    raw_path: Path,
    captured_utc: str,
) -> dict[str, Any]:
    source_url = adapter_row.get('url', '')
    pdf_url = wfp_lebanon_programme_factsheet_pdf_url(source_url)
    metadata_source_url = pdf_url
    evidence_link = pdf_url
    document_title = ''
    latest_visible_date = ''
    latest_period_covered = ''
    secondary_raw_path = ''
    checksum_parts: list[bytes] = []
    bytes_written = 0

    if source_url.startswith('file://') or source_url.endswith('.pdf'):
        fetch_meta = fetch_direct_source(pdf_url, raw_path)
    else:
        page_raw_path = raw_path.with_name(f'{raw_path.stem}-page.html')
        page_fetch = fetch_direct_source(source_url, page_raw_path)
        page_html = page_raw_path.read_text(errors='replace')
        page_metadata = extract_wfp_lebanon_programme_page_metadata(page_html, source_url)
        pdf_url = page_metadata.get('document_download_url', '') or pdf_url
        document_title = page_metadata.get('document_title', '')
        latest_visible_date = page_metadata.get('last_published_date', '')
        latest_period_covered = page_metadata.get('latest_period_covered', '')
        metadata_source_url = source_url
        evidence_link = source_url
        secondary_raw_path = str(page_raw_path)
        checksum_parts.append(page_raw_path.read_bytes())
        bytes_written += page_fetch['bytes_written']
        fetch_meta = fetch_direct_source(pdf_url, raw_path)

    report_date = pdfinfo_report_date(raw_path)
    pdf_text = pdftotext_content(raw_path)
    latest_visible_date = latest_visible_date or report_date
    latest_period_covered = latest_period_covered or extract_wfp_lebanon_factsheet_period(pdf_text)
    checksum_parts.append(raw_path.read_bytes())
    bytes_written += fetch_meta['bytes_written']

    return {
        'capture_status': 'completed',
        'status_code': fetch_meta['status_code'],
        'content_type': fetch_meta['content_type'],
        'bytes_written': bytes_written,
        'checksum_sha256': hashlib.sha256(b''.join(checksum_parts)).hexdigest(),
        'raw_path': str(raw_path),
        'secondary_raw_path': secondary_raw_path,
        'metadata_source_url': metadata_source_url,
        'document_title': document_title,
        'document_download_url': pdf_url,
        'latest_visible_date': latest_visible_date,
        'latest_period_covered': latest_period_covered,
        'normalized_summary': (
            f"{manifest_row.get('source_name', '')} publication page parsed and linked PDF captured from the current "
            'WFP Lebanon programme factsheet release.'
        ),
        'verification_updates': {
            'source_id': manifest_row.get('source_id', ''),
            'last_checked_utc': captured_utc,
            'last_published_date': latest_visible_date,
            'latest_period_covered': latest_period_covered,
            'claim_date_utc': '',
            'owner': '',
            'evidence_link': evidence_link,
            'evidence_path': '',
            'status': 'in_review',
            'next_action': 'Monitor the current WFP Lebanon programme factsheet page and update the pinned document URL when a newer release appears.',
            'notes': (
                f"WFP Lebanon factsheet capture parsed on {captured_utc}. Evidence page: {evidence_link}; "
                f"selected document: {pdf_url or 'not found'}; publication date: {latest_visible_date or 'not found'}; "
                f"PDF metadata date: {report_date or 'not found'}; latest visible coverage marker: "
                f"{latest_period_covered or 'not found'}."
            ),
        },
    }


def build_comtrade_data_availability_payload(
    manifest_row: dict[str, str],
    adapter_row: dict[str, str],
    raw_path: Path,
    captured_utc: str,
) -> dict[str, Any]:
    api_url = comtrade_data_availability_api_url(adapter_row.get('url', ''))
    payload, fetch_meta = fetch_json_source(api_url, raw_path)
    trend_payload = decode_json_payload(payload)
    latest_visible_date = latest_iso_date([trend_payload.get('lastUpdatedDate', '')])
    results = trend_payload.get('results', [])
    released_total = 0
    upcoming_total = 0
    if isinstance(results, list):
        for row in results:
            if not isinstance(row, dict):
                continue
            released_total += int(row.get('Released', 0) or 0)
            upcoming_total += int(row.get('Upcoming', 0) or 0)

    return {
        'capture_status': 'completed',
        'status_code': fetch_meta['status_code'],
        'content_type': fetch_meta['content_type'],
        'bytes_written': fetch_meta['bytes_written'],
        'checksum_sha256': fetch_meta['checksum_sha256'],
        'raw_path': str(raw_path),
        'metadata_source_url': api_url,
        'latest_visible_date': latest_visible_date,
        'released_total': released_total,
        'upcoming_total': upcoming_total,
        'normalized_summary': f"{manifest_row.get('source_name', '')} freshness timestamp captured from the public Comtrade Plus data-availability API.",
        'verification_updates': {
            'source_id': manifest_row.get('source_id', ''),
            'last_checked_utc': captured_utc,
            'last_published_date': latest_visible_date,
            'latest_period_covered': '',
            'claim_date_utc': '',
            'owner': '',
            'evidence_link': api_url,
            'evidence_path': '',
            'status': 'in_review',
            'next_action': 'Track the Comtrade Plus data-availability API and add file-level publication metadata if a stable public endpoint is exposed.',
            'notes': (
                f"Comtrade data-availability API captured on {captured_utc}. Last updated date: "
                f"{latest_visible_date or 'not found'}; released bucket total: {released_total}; "
                f"upcoming bucket total: {upcoming_total}."
            ),
        },
    }


def build_unctad_maritime_insights_payload(
    manifest_row: dict[str, str],
    adapter_row: dict[str, str],
    raw_path: Path,
    captured_utc: str,
) -> dict[str, Any]:
    page_url = unctad_maritime_insights_url(adapter_row.get('url', ''))
    fetch_meta = fetch_direct_source(page_url, raw_path)
    metadata = extract_unctad_maritime_insights_metadata(raw_path.read_text(errors='replace'), page_url)
    if not metadata.get('latest_visible_date'):
        raise ValueError('UNCTAD maritime insights page did not expose a visible updated date.')
    metadata_link = metadata.get('metadata_link', page_url) or page_url
    secondary_raw_path = ''
    if metadata_link != page_url:
        detail_raw_path = raw_path.with_name(f'{raw_path.stem}-datacentre{raw_path.suffix}')
        try:
            detail_fetch = fetch_direct_source(metadata_link, detail_raw_path)
            secondary_raw_path = str(detail_raw_path)
            fetch_meta = {
                'status_code': fetch_meta['status_code'],
                'content_type': fetch_meta['content_type'],
                'bytes_written': fetch_meta['bytes_written'] + detail_fetch['bytes_written'],
                'checksum_sha256': hashlib.sha256(raw_path.read_bytes() + detail_raw_path.read_bytes()).hexdigest(),
            }
        except (HTTPError, URLError, TimeoutError):
            secondary_raw_path = ''
    return {
        'capture_status': 'completed',
        'status_code': fetch_meta['status_code'],
        'content_type': fetch_meta['content_type'],
        'bytes_written': fetch_meta['bytes_written'],
        'checksum_sha256': fetch_meta['checksum_sha256'],
        'raw_path': str(raw_path),
        'secondary_raw_path': secondary_raw_path,
        'metadata_source_url': page_url,
        'latest_visible_date': metadata.get('latest_visible_date', ''),
        'latest_report_url': metadata_link,
        'latest_period_covered': metadata.get('latest_period_covered', ''),
        'normalized_summary': f"{manifest_row.get('source_name', '')} theme page parsed for the latest 'Data updated on' marker.",
        'verification_updates': {
            'source_id': manifest_row.get('source_id', ''),
            'last_checked_utc': captured_utc,
            'last_published_date': metadata.get('latest_visible_date', ''),
            'latest_period_covered': metadata.get('latest_period_covered', ''),
            'claim_date_utc': '',
            'owner': '',
            'evidence_link': metadata_link,
            'evidence_path': '',
            'status': 'in_review',
            'next_action': 'Monitor the UNCTAD maritime insights theme page and its linked data-centre metadata pages for fresher updates.',
            'notes': (
                f"UNCTAD maritime insights page parsed on {captured_utc}. Latest visible update date: "
                f"{metadata.get('latest_visible_date', 'not found')}; selected insight: {metadata.get('latest_title', 'not found')}; "
                f"covered period: {metadata.get('latest_period_covered', 'not found') or 'not found'}."
            ),
        },
    }


def build_sca_navigation_news_payload(
    manifest_row: dict[str, str],
    adapter_row: dict[str, str],
    raw_path: Path,
    captured_utc: str,
) -> dict[str, Any]:
    index_url = sca_navigation_news_index_url(adapter_row.get('url', ''))
    index_fetch = fetch_direct_source(index_url, raw_path)
    candidates = extract_sca_navigation_news_candidates(raw_path.read_text(errors='replace'), index_url)
    if not candidates:
        raise ValueError('SCA navigation news index did not expose any navigation-news rows.')
    candidate = candidates[0]
    detail_raw_path = raw_path.with_name(f'{raw_path.stem}-detail.html')
    detail_fetch = fetch_direct_source(candidate['detail_url'], detail_raw_path)
    detail = extract_sca_navigation_news_detail(detail_raw_path.read_text(errors='replace'))
    last_published_date = detail.get('detail_date') or candidate.get('latest_visible_date', '')
    daily_summary = ''
    if detail.get('daily_vessel_count') and detail.get('daily_gross_tonnage_mtons'):
        daily_summary = (
            f"same-day transit: {detail.get('daily_vessel_count')} vessels / "
            f"{detail.get('daily_gross_tonnage_mtons')} million tons"
        )
    rolling_summary = ''
    if detail.get('rolling_three_day_vessel_count') and detail.get('rolling_three_day_gross_tonnage_mtons'):
        rolling_summary = (
            f"rolling three-day transit: {detail.get('rolling_three_day_vessel_count')} vessels / "
            f"{detail.get('rolling_three_day_gross_tonnage_mtons')} million tons"
        )
    notes_suffix = '; '.join(part for part in [daily_summary, rolling_summary] if part)
    return {
        'capture_status': 'completed',
        'status_code': detail_fetch['status_code'],
        'content_type': detail_fetch['content_type'],
        'bytes_written': index_fetch['bytes_written'] + detail_fetch['bytes_written'],
        'checksum_sha256': hashlib.sha256(raw_path.read_bytes() + detail_raw_path.read_bytes()).hexdigest(),
        'raw_path': str(raw_path),
        'secondary_raw_path': str(detail_raw_path),
        'metadata_source_url': index_url,
        'detail_page_url': candidate['detail_url'],
        'latest_visible_date': last_published_date,
        'claim_date_utc': detail.get('detail_date', ''),
        'document_title': detail.get('title') or candidate.get('title', ''),
        'daily_vessel_count': detail.get('daily_vessel_count', ''),
        'daily_gross_tonnage_mtons': detail.get('daily_gross_tonnage_mtons', ''),
        'northbound_vessel_count': detail.get('northbound_vessel_count', ''),
        'southbound_vessel_count': detail.get('southbound_vessel_count', ''),
        'rolling_three_day_vessel_count': detail.get('rolling_three_day_vessel_count', ''),
        'rolling_three_day_gross_tonnage_mtons': detail.get('rolling_three_day_gross_tonnage_mtons', ''),
        'normalized_summary': f"{manifest_row.get('source_name', '')} latest navigation-news item resolved from the SCA news index.",
        'verification_updates': {
            'source_id': manifest_row.get('source_id', ''),
            'last_checked_utc': captured_utc,
            'last_published_date': last_published_date,
            'latest_period_covered': '',
            'claim_date_utc': detail.get('detail_date', ''),
            'owner': '',
            'evidence_link': candidate['detail_url'],
            'evidence_path': '',
            'status': 'in_review',
            'next_action': 'Keep following the SCA news index and promote the latest navigation-news detail page on each rerun.',
            'notes': (
                f"SCA navigation news index parsed on {captured_utc}. Latest navigation-news date: "
                f"{last_published_date or 'not found'}; selected title: {detail.get('title') or candidate.get('title', 'not found')}"
                f"{'; ' + notes_suffix if notes_suffix else '.'}"
            ),
        },
    }


def stage_query_request_spec(
    manifest_row: dict[str, str],
    adapter_row: dict[str, str],
    collection_dir: Path,
    plans_dir: Path,
    raw_path: Path,
) -> dict[str, Any]:
    query_seed_name = adapter_row.get('query_seed_file', '')
    query_seed_path = collection_dir / query_seed_name if query_seed_name else None
    query_rows = load_csv_rows(query_seed_path) if query_seed_path and query_seed_path.is_file() else []
    connector_rows = load_existing_rows_by_key(plans_dir / 'connector_readiness.csv', 'source_id')
    connector_row = connector_rows.get(manifest_row.get('source_id', ''), {})
    district_scope = manifest_row.get('district_scope', '')
    district_names = {value.strip() for value in district_scope.split(';') if value.strip()}
    relevant_queries = [
        row
        for row in query_rows
        if row.get('district_name', '').strip() in district_names
        or row.get('country', '').strip() in district_names
    ]
    payload = {
        'run_id': manifest_row.get('run_id', ''),
        'source_id': manifest_row.get('source_id', ''),
        'source_name': manifest_row.get('source_name', ''),
        'adapter_type': manifest_row.get('adapter_type', ''),
        'district_scope': district_scope,
        'query_seed_file': adapter_row.get('query_seed_file', ''),
        'query_count': len(query_rows),
        'matched_query_count': len(relevant_queries),
        'connector_status': connector_row.get('status', ''),
        'credential_state': connector_row.get('credential_state', ''),
        'connector_priority': connector_row.get('priority', ''),
        'connector_owner': connector_row.get('owner', ''),
        'connector_next_action': connector_row.get('next_action', ''),
        'connector_notes': connector_row.get('notes', ''),
        'connector_url': connector_row.get('url', ''),
        'queries': relevant_queries,
        'status': 'awaiting_external_execution',
    }
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    raw_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + '\n')
    return {
        'status_code': '',
        'content_type': 'application/json',
        'bytes_written': len(raw_path.read_bytes()),
        'checksum_sha256': hashlib.sha256(raw_path.read_bytes()).hexdigest(),
        'query_count': len(query_rows),
        'matched_queries': len(relevant_queries),
        'connector_status': connector_row.get('status', ''),
        'credential_state': connector_row.get('credential_state', ''),
        'connector_priority': connector_row.get('priority', ''),
        'connector_owner': connector_row.get('owner', ''),
        'connector_next_action': connector_row.get('next_action', ''),
        'connector_notes': connector_row.get('notes', ''),
        'connector_url': connector_row.get('url', ''),
    }


def write_normalized_collection_record(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + '\n')


def write_collection_rows(
    path: Path,
    fieldnames: list[str],
    rows: list[dict[str, Any]],
) -> None:
    write_rows_csv(fieldnames, rows, path)


def requeue_due_collection_runs(
    manifest_rows: list[dict[str, str]],
    accounting_rows: list[dict[str, str]],
) -> list[dict[str, str]]:
    accounting_lookup = {row.get('source_id', ''): row for row in accounting_rows if row.get('source_id')}
    requeue_statuses = {'unknown', 'due_now', 'overdue', 'manual_review'}
    rerunnable_statuses = {'completed', 'failed', 'staged_external'}
    scheduled_utc = utc_now().isoformat().replace('+00:00', 'Z')

    for manifest_row in manifest_rows:
        accounting_row = accounting_lookup.get(manifest_row.get('source_id', ''))
        if accounting_row is None:
            continue
        if accounting_row.get('priority_tier') != 'tier1':
            continue
        if accounting_row.get('recency_status') not in requeue_statuses:
            continue
        if manifest_row.get('status') not in rerunnable_statuses:
            continue
        manifest_row['status'] = 'ready'
        manifest_row['scheduled_run_utc'] = scheduled_utc
    return manifest_rows


def process_collection_run(
    manifest_row: dict[str, str],
    adapter_row: dict[str, str],
    collection_dir: Path,
    plans_dir: Path,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, str]]:
    source_id = manifest_row.get('source_id', '')
    adapter_type = manifest_row.get('adapter_type', '')
    now = utc_now().isoformat().replace('+00:00', 'Z')
    raw_dir = Path(adapter_row.get('raw_landing_dir', collection_dir / 'raw' / source_id))
    normalized_path = Path(adapter_row.get('normalized_output_path', collection_dir / 'normalized' / f'{source_id}.json'))
    raw_path = raw_dir / f"{manifest_row.get('run_id', source_id)}{raw_extension(adapter_type)}"

    normalized_payload = {
        'run_id': manifest_row.get('run_id', ''),
        'source_id': source_id,
        'source_name': manifest_row.get('source_name', ''),
        'adapter_type': adapter_type,
        'captured_utc': now,
        'source_url': adapter_row.get('url', ''),
    }

    if adapter_type == 'hdx_dataset_metadata':
        normalized_payload.update(build_hdx_dataset_metadata_payload(manifest_row, adapter_row, raw_path, now))
        result_status = 'completed'
        result_notes = 'HDX dataset metadata fetch completed.'
    elif adapter_type == 'unhcr_document_index':
        normalized_payload.update(build_unhcr_document_index_payload(manifest_row, adapter_row, raw_path, now))
        result_status = 'completed'
        result_notes = 'UNHCR document index fetch completed.'
    elif adapter_type == 'ipc_lebanon_analysis':
        normalized_payload.update(build_ipc_lebanon_analysis_payload(manifest_row, adapter_row, raw_path, now, plans_dir))
        result_status = 'completed'
        result_notes = 'IPC Lebanon analysis fetch completed.'
    elif adapter_type == 'ipc_gaza_snapshot':
        normalized_payload.update(build_ipc_gaza_snapshot_payload(manifest_row, adapter_row, raw_path, now, plans_dir))
        result_status = 'completed'
        result_notes = 'IPC Gaza snapshot capture completed.'
    elif adapter_type == 'iom_dtm_sudan':
        normalized_payload.update(build_iom_dtm_sudan_payload(manifest_row, adapter_row, raw_path, now, plans_dir))
        result_status = 'completed'
        result_notes = 'IOM DTM Sudan fallback capture completed.'
    elif adapter_type == 'ashdod_port_financials':
        normalized_payload.update(build_ashdod_port_financials_payload(manifest_row, adapter_row, raw_path, now, plans_dir))
        result_status = 'completed'
        result_notes = 'Ashdod Port financial-information capture completed.'
    elif adapter_type == 'acaps_country_page':
        normalized_payload.update(build_acaps_country_page_payload(manifest_row, adapter_row, raw_path, now))
        result_status = 'completed'
        result_notes = 'ACAPS country page fetch completed.'
    elif adapter_type == 'wfp_lebanon_factsheet_pdf':
        normalized_payload.update(build_wfp_lebanon_factsheet_pdf_payload(manifest_row, adapter_row, raw_path, now))
        result_status = 'completed'
        result_notes = 'WFP Lebanon programme factsheet PDF capture completed.'
    elif adapter_type == 'comtrade_data_availability':
        normalized_payload.update(build_comtrade_data_availability_payload(manifest_row, adapter_row, raw_path, now))
        result_status = 'completed'
        result_notes = 'Comtrade data-availability API fetch completed.'
    elif adapter_type == 'unctad_maritime_insights':
        normalized_payload.update(build_unctad_maritime_insights_payload(manifest_row, adapter_row, raw_path, now))
        result_status = 'completed'
        result_notes = 'UNCTAD maritime insights fetch completed.'
    elif adapter_type == 'sca_navigation_news':
        normalized_payload.update(build_sca_navigation_news_payload(manifest_row, adapter_row, raw_path, now))
        result_status = 'completed'
        result_notes = 'SCA navigation news fetch completed.'
    elif adapter_type == 'lebanon_cas_cpi':
        normalized_payload.update(build_lebanon_cas_cpi_payload(manifest_row, adapter_row, raw_path, now))
        result_status = 'completed'
        result_notes = 'Lebanon CAS CPI feed capture completed.'
    elif adapter_type == 'saudi_gastat_cpi':
        normalized_payload.update(build_saudi_gastat_cpi_payload(manifest_row, adapter_row, raw_path, now))
        result_status = 'completed'
        result_notes = 'Saudi GASTAT CPI page capture completed.'
    elif adapter_type == 'usda_fas_gain_pdf':
        normalized_payload.update(build_usda_fas_gain_pdf_payload(manifest_row, adapter_row, raw_path, now))
        result_status = 'completed'
        result_notes = 'USDA FAS GAIN PDF capture completed.'
    elif adapter_type == 'israel_cbs_price_indices':
        normalized_payload.update(build_israel_cbs_price_indices_payload(manifest_row, adapter_row, raw_path, now))
        result_status = 'completed'
        result_notes = 'Israel CBS price indices metadata fetch completed.'
    elif adapter_type == 'israel_cbs_impexp_files':
        normalized_payload.update(build_israel_cbs_impexp_payload(manifest_row, adapter_row, raw_path, now))
        result_status = 'completed'
        result_notes = 'Israel CBS imports and exports feed fetch completed.'
    elif adapter_type == 'israel_iaa_monthly_reports':
        normalized_payload.update(build_israel_iaa_monthly_reports_payload(manifest_row, adapter_row, raw_path, now))
        result_status = 'completed'
        result_notes = 'Israel Airports Authority monthly archive fetch completed.'
    elif adapter_type == 'hdx_hapi_changelog':
        normalized_payload.update(build_hdx_hapi_changelog_payload(manifest_row, adapter_row, raw_path, now))
        result_status = 'completed'
        result_notes = 'HAPI changelog fetch completed.'
    elif adapter_type == 'hdx_signals_story':
        normalized_payload.update(build_hdx_signals_story_payload(manifest_row, adapter_row, raw_path, now))
        result_status = 'completed'
        result_notes = 'HDX Signals story capture completed.'
    elif adapter_type in {'html_snapshot', 'pdf_capture', 'api_pull'}:
        fetch_meta = fetch_direct_source(adapter_row.get('url', ''), raw_path)
        normalized_payload.update(
            {
                'capture_status': 'completed',
                'status_code': fetch_meta['status_code'],
                'content_type': fetch_meta['content_type'],
                'bytes_written': fetch_meta['bytes_written'],
                'checksum_sha256': fetch_meta['checksum_sha256'],
                'raw_path': str(raw_path),
            }
        )
        result_status = 'completed'
        result_notes = 'Direct source fetch completed.'
    else:
        fetch_meta = stage_query_request_spec(manifest_row, adapter_row, collection_dir, plans_dir, raw_path)
        normalized_payload.update(
            {
                'capture_status': 'staged_external',
                'query_seed_file': adapter_row.get('query_seed_file', ''),
                'query_count': fetch_meta.get('query_count', 0),
                'matched_queries': fetch_meta.get('matched_queries', 0),
                'connector_status': fetch_meta.get('connector_status', ''),
                'credential_state': fetch_meta.get('credential_state', ''),
                'connector_priority': fetch_meta.get('connector_priority', ''),
                'connector_owner': fetch_meta.get('connector_owner', ''),
                'connector_next_action': fetch_meta.get('connector_next_action', ''),
                'connector_notes': fetch_meta.get('connector_notes', ''),
                'connector_url': fetch_meta.get('connector_url', ''),
                'checksum_sha256': fetch_meta['checksum_sha256'],
                'raw_path': str(raw_path),
            }
        )
        result_status = 'staged_external'
        result_notes = 'Request spec written for external query execution.'

    write_normalized_collection_record(normalized_path, normalized_payload)

    evidence_row = {
        'capture_id': f"capture-{source_id}-{slugify(now)}",
        'run_id': manifest_row.get('run_id', ''),
        'source_id': source_id,
        'captured_utc': now,
        'capture_type': adapter_type,
        'raw_path': str(raw_path),
        'normalized_path': str(normalized_path),
        'evidence_path': '',
        'checksum_sha256': normalized_payload.get('checksum_sha256', ''),
        'operator': 'collector',
        'status': result_status,
        'notes': result_notes,
    }

    result_row = {
        'run_id': manifest_row.get('run_id', ''),
        'source_id': source_id,
        'source_name': manifest_row.get('source_name', ''),
        'adapter_type': adapter_type,
        'executed_utc': now,
        'result_status': result_status,
        'raw_path': str(raw_path),
        'normalized_path': str(normalized_path),
        'notes': result_notes,
    }

    manifest_row['status'] = result_status
    return evidence_row, result_row, manifest_row


def run_collection_pack(args: argparse.Namespace, records: list[dict[str, Any]]) -> dict[str, Any]:
    collection_dir = Path(args.collection_dir)
    collection_dir.mkdir(parents=True, exist_ok=True)
    plans_dir = Path(args.plans_dir)
    paths = ensure_collection_artifacts(args, records)

    manifest_rows = load_csv_rows(paths['run_manifest'])
    adapter_lookup = load_adapter_lookup(paths['adapter_registry'])
    evidence_rows = load_csv_rows(paths['evidence_log'])
    result_rows = load_csv_rows(paths['run_results'])
    accounting_rows = load_csv_rows(plans_dir / 'recent_accounting.csv')

    manifest_rows = requeue_due_collection_runs(manifest_rows, accounting_rows)

    ready_rows = [row for row in manifest_rows if row.get('status') == 'ready']
    processed = 0
    completed = 0
    staged = 0
    failed = 0

    for manifest_row in ready_rows[: args.max_runs]:
        adapter_row = adapter_lookup.get(manifest_row.get('source_id', ''))
        if adapter_row is None:
            manifest_row['status'] = 'failed'
            result_rows.append(
                {
                    'run_id': manifest_row.get('run_id', ''),
                    'source_id': manifest_row.get('source_id', ''),
                    'source_name': manifest_row.get('source_name', ''),
                    'adapter_type': manifest_row.get('adapter_type', ''),
                    'executed_utc': utc_now().isoformat().replace('+00:00', 'Z'),
                    'result_status': 'failed',
                    'raw_path': '',
                    'normalized_path': '',
                    'notes': 'Missing adapter registry row.',
                }
            )
            failed += 1
            processed += 1
            continue

        try:
            evidence_row, result_row, updated_manifest = process_collection_run(manifest_row, adapter_row, collection_dir, plans_dir)
            evidence_rows.append(evidence_row)
            result_rows.append(result_row)
            processed += 1
            if updated_manifest['status'] == 'completed':
                completed += 1
            elif updated_manifest['status'] == 'staged_external':
                staged += 1
        except (URLError, TimeoutError, OSError, ValueError, json.JSONDecodeError, ET.ParseError) as exc:
            manifest_row['status'] = 'failed'
            failure_utc = utc_now().isoformat().replace('+00:00', 'Z')
            result_note = f'Collection failed: {exc.__class__.__name__}: {exc}'
            evidence_rows.append(
                {
                    'capture_id': f"capture-{manifest_row.get('source_id', '')}-{slugify(failure_utc)}",
                    'run_id': manifest_row.get('run_id', ''),
                    'source_id': manifest_row.get('source_id', ''),
                    'captured_utc': failure_utc,
                    'capture_type': manifest_row.get('adapter_type', ''),
                    'raw_path': '',
                    'normalized_path': '',
                    'evidence_path': '',
                    'checksum_sha256': '',
                    'operator': 'collector',
                    'status': 'failed',
                    'notes': result_note,
                }
            )
            result_rows.append(
                {
                    'run_id': manifest_row.get('run_id', ''),
                    'source_id': manifest_row.get('source_id', ''),
                    'source_name': manifest_row.get('source_name', ''),
                    'adapter_type': manifest_row.get('adapter_type', ''),
                    'executed_utc': failure_utc,
                    'result_status': 'failed',
                    'raw_path': '',
                    'normalized_path': '',
                    'notes': result_note,
                }
            )
            failed += 1
            processed += 1

    write_collection_rows(
        paths['run_manifest'],
        [
            'run_id',
            'source_id',
            'source_name',
            'collection_stage',
            'adapter_type',
            'priority',
            'district_scope',
            'scheduled_run_utc',
            'expected_artifact',
            'query_seed_file',
            'status',
            'failure_action',
            'notes',
        ],
        manifest_rows,
    )
    write_collection_rows(
        paths['evidence_log'],
        [
            'capture_id',
            'run_id',
            'source_id',
            'captured_utc',
            'capture_type',
            'raw_path',
            'normalized_path',
            'evidence_path',
            'checksum_sha256',
            'operator',
            'status',
            'notes',
        ],
        evidence_rows,
    )
    write_collection_rows(
        paths['run_results'],
        [
            'run_id',
            'source_id',
            'source_name',
            'adapter_type',
            'executed_utc',
            'result_status',
            'raw_path',
            'normalized_path',
            'notes',
        ],
        result_rows,
    )

    return {
        'status': 'ok',
        'action': 'collect_ready',
        'input': str(Path(args.input)),
        'collection_dir': str(collection_dir),
        'run_manifest': str(paths['run_manifest']),
        'evidence_capture_log': str(paths['evidence_log']),
        'collection_run_results': str(paths['run_results']),
        'processed_runs': processed,
        'completed_runs': completed,
        'staged_external_runs': staged,
        'failed_runs': failed,
        'launcher_mode': getattr(args, 'launcher_mode', 'cli'),
    }


def build_source_owner_rows(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for record in sorted(records, key=lambda item: item['rank']):
        rows.append(
            {
                'rank': record['rank'],
                'source_name': record['source_name'],
                'source_family': record['source_family'],
                'priority_tier': record['priority_tier'],
                'region_or_country': record['region_or_country'],
                'refresh_cadence': record['refresh_cadence'],
                'collection_mode': collection_mode(record['access_type']),
                'owner': '',
                'backup_owner': '',
                'status': 'pending_assignment',
                'last_accessed_utc': '',
                'notes': record['notes'],
                'url': record['url'],
            }
        )
    return rows


def build_district_watchlist_rows() -> list[dict[str, Any]]:
    suggested = [
        ('Egypt', 'Nasr City', 'monitoring'),
        ('Egypt', '6th of October City', 'monitoring'),
        ('Egypt', 'Ain Shams / Matariya cluster', 'monitoring'),
        ('Egypt', 'Giza / Faisal corridor', 'monitoring'),
        ('Egypt', 'Imbaba', 'monitoring'),
        ('Egypt', 'Shubra El Kheima', 'monitoring'),
        ('Lebanon', 'Akkar', 'monitoring'),
        ('Lebanon', 'Tripoli', 'monitoring'),
        ('Lebanon', 'Bekaa / Zahle-adjacent districts', 'monitoring'),
        ('Lebanon', 'Beirut southern periphery', 'monitoring'),
        ('Saudi Arabia', 'Riyadh labor-heavy districts', 'monitoring'),
        ('Saudi Arabia', 'Jeddah mixed migrant-commercial districts', 'monitoring'),
        ('Saudi Arabia', 'Dammam / Khobar commercial corridors', 'monitoring'),
        ('UAE', 'Dubai Deira / Bur Dubai-adjacent commercial districts', 'monitoring'),
        ('UAE', 'Sharjah dense middle-market retail districts', 'monitoring'),
        ('UAE', 'Abu Dhabi mixed grocery-delivery districts', 'monitoring'),
        ('Gaza/OPT', 'Administrative areas with usable observability', 'baseline'),
    ]
    rows = []
    for country, district_name, district_role in suggested:
        rows.append(
            {
                'country': country,
                'district_name': district_name,
                'district_role': district_role,
                'movement_relevance': '',
                'source_coverage': '',
                'food_retail_observability': '',
                'mapping_quality': '',
                'control_comparability': '',
                'total_score': '',
                'decision': 'pending_scoring',
                'paired_control': '',
                'review_owner': '',
                'notes': 'Suggested in docs/district-selection-matrix.md',
            }
        )

    rows.append(
        {
            'country': 'Egypt',
            'district_name': 'Giza / Faisal corridor',
            'district_role': 'example_scorecard',
            'movement_relevance': 26,
            'source_coverage': 16,
            'food_retail_observability': 18,
            'mapping_quality': 11,
            'control_comparability': 10,
            'total_score': 81,
            'decision': 'include',
            'paired_control': '',
            'review_owner': '',
            'notes': 'Example scorecard from docs/district-selection-matrix.md',
        }
    )
    return rows


def build_event_timeline_rows() -> list[dict[str, Any]]:
    return [
        {
            'event_id': 'egypt-sudan-arrivals-2023-04-15',
            'event_type': 'arrival',
            'country': 'Egypt',
            'admin1': 'Cairo/Giza',
            'district_focus': 'Pilot districts',
            'event_date': '2023-04-15',
            'event_window_start': '2023-04-15',
            'event_window_end': '',
            'source_name': 'UNHCR Egypt data portal; IOM DTM Sudan',
            'source_url': 'https://data.unhcr.org/en/country/egy; https://dtm.iom.int/sudan',
            'source_accessed_utc': '',
            'confidence': 'high',
            'summary': 'Sudan-linked arrivals used as the Egypt backtest anchor.',
            'notes': 'Freeze Cairo/Giza watchlist against this event window.',
        },
        {
            'event_id': 'lebanon-syria-volatility-2024-12-08',
            'event_type': 'movement_volatility',
            'country': 'Lebanon / Syria corridor',
            'admin1': 'Akkar/Tripoli/Bekaa/Beirut periphery',
            'district_focus': 'Lebanon corridor pack',
            'event_date': '2024-12-08',
            'event_window_start': '2024-12-08',
            'event_window_end': '',
            'source_name': 'UNHCR Lebanon reporting hub; ACAPS Lebanon',
            'source_url': 'https://reporting.unhcr.org/lebanon-flash-update; https://www.acaps.org/en/countries/lebanon',
            'source_accessed_utc': '',
            'confidence': 'high',
            'summary': 'Movement-dynamics anchor for Lebanon/Syria corridor volatility.',
            'notes': 'Use for appearance and disappearance scoring.',
        },
        {
            'event_id': 'gaza-blockade-2025-03-02',
            'event_type': 'blockade',
            'country': 'Gaza/OPT',
            'admin1': 'Gaza Strip',
            'district_focus': 'Baseline administrative areas',
            'event_date': '2025-03-02',
            'event_window_start': '2025-03-02',
            'event_window_end': '',
            'source_name': 'OCHA OPT Gaza updates; IPC Gaza snapshot',
            'source_url': 'https://www.ochaopt.org/publications/situation-reports; https://www.ipcinfo.org/fileadmin/user_upload/ipcinfo/docs/IPC_Gaza_Strip_Acute_Food_Insecurity_Malnutrition_Apr_Sept2025_Special_Snapshot.pdf',
            'source_accessed_utc': '',
            'confidence': 'high',
            'summary': 'Supply blockade date used to anchor Gaza market-collapse analysis.',
            'notes': 'Track assortment compression and retail inactivity after this date.',
        },
        {
            'event_id': 'gaza-renewed-hostilities-2025-03-18',
            'event_type': 'hostilities',
            'country': 'Gaza/OPT',
            'admin1': 'Gaza Strip',
            'district_focus': 'Baseline administrative areas',
            'event_date': '2025-03-18',
            'event_window_start': '2025-03-18',
            'event_window_end': '',
            'source_name': 'OCHA OPT Gaza updates; IPC Gaza snapshot',
            'source_url': 'https://www.ochaopt.org/publications/situation-reports; https://www.ipcinfo.org/fileadmin/user_upload/ipcinfo/docs/IPC_Gaza_Strip_Acute_Food_Insecurity_Malnutrition_Apr_Sept2025_Special_Snapshot.pdf',
            'source_accessed_utc': '',
            'confidence': 'high',
            'summary': 'Renewed hostilities date used to measure post-shock market deterioration.',
            'notes': 'Track pricing and retail-functionality deltas after this date.',
        },
    ]


def build_anomaly_review_rows() -> list[dict[str, Any]]:
    return [
        {
            'anomaly_id': '',
            'review_week': '',
            'country': '',
            'district': '',
            'signal_family': '',
            'signal_summary': '',
            'nearest_baseline_event': '',
            'baseline_event_date': '',
            'proxy_signal_date': '',
            'humanitarian_baseline_score': '',
            'market_proxy_score': '',
            'spatial_fit_score': '',
            'temporal_fit_score': '',
            'cross_source_score': '',
            'confound_penalty': '',
            'raw_score': '',
            'final_score': '',
            'publication_label': '',
            'analyst_initials': '',
            'evidence_links': '',
            'confound_notes': '',
            'next_collection_action': '',
            'status': 'pending_review',
        }
    ]


def render_pilot_execution_summary(records: list[dict[str, Any]]) -> str:
    summary = build_source_summary(records)
    return """# Pilot Execution Summary Template

## Objective
- Egypt-first v0.1 backtest with compliance-safe public sources.

## Generated Inputs
- Source registry: `docs/source-registry.csv`
- Source owner assignments: `source-owner-assignments.csv`
- District watchlist: `district-watchlist.csv`
- Event timeline: `event-timeline.csv`
- Anomaly review worksheet: `anomaly-review-worksheet.csv`

## Source Registry Snapshot
- Total seeded sources: {total_sources}
- Tier 1 sources: {tier1_count}
- Tier 2 sources: {tier2_count}

## Phase 0 Status
- Confidence rubric approved: yes
- District-selection matrix approved: yes
- Top district shortlist seeded: yes
- Source owner assignments completed: no

## Phase 1 Status
- Egypt event baseline starter rows created: yes
- Cairo/Giza district watchlist frozen: no
- Baseline records accessed timestamps captured: no

## Weekly Review Loop
- Refresh baseline event notes.
- Refresh place or merchant snapshots.
- Score anomaly candidates.
- Record confounds and follow-up actions.

## Go / No-Go Gate
- Expand only if at least one Egypt anomaly is supported by one hard baseline and one market proxy.
- Pause if most cases remain `Unconfirmed` or confounds dominate.

## Blockers
- Assign source owners.
- Freeze pilot districts and controls.
- Populate the first weekly anomaly queue.

## Next Actions
1. Assign owners and cadence coverage for the seeded source stack.
2. Score the Egypt district shortlist and pair controls.
3. Populate the Egypt event timeline with access timestamps and notes.
4. Run the first analyst review cycle using the anomaly worksheet.
""".format(
        total_sources=summary['total_sources'],
        tier1_count=summary['tier_counts'].get('tier1', 0),
        tier2_count=summary['tier_counts'].get('tier2', 0),
    )


def scaffold_v0_pack(args: argparse.Namespace, records: list[dict[str, Any]]) -> dict[str, Any]:
    pack_dir = Path(args.pack_dir)
    pack_dir.mkdir(parents=True, exist_ok=True)

    owner_rows = build_source_owner_rows(records)
    district_rows = build_district_watchlist_rows()
    event_rows = build_event_timeline_rows()
    anomaly_rows = build_anomaly_review_rows()

    owner_path = pack_dir / 'source-owner-assignments.csv'
    district_path = pack_dir / 'district-watchlist.csv'
    event_path = pack_dir / 'event-timeline.csv'
    anomaly_path = pack_dir / 'anomaly-review-worksheet.csv'
    summary_path = pack_dir / 'pilot-execution-summary.md'

    write_rows_csv(
        [
            'rank',
            'source_name',
            'source_family',
            'priority_tier',
            'region_or_country',
            'refresh_cadence',
            'collection_mode',
            'owner',
            'backup_owner',
            'status',
            'last_accessed_utc',
            'notes',
            'url',
        ],
        owner_rows,
        owner_path,
    )
    write_rows_csv(
        [
            'country',
            'district_name',
            'district_role',
            'movement_relevance',
            'source_coverage',
            'food_retail_observability',
            'mapping_quality',
            'control_comparability',
            'total_score',
            'decision',
            'paired_control',
            'review_owner',
            'notes',
        ],
        district_rows,
        district_path,
    )
    write_rows_csv(
        [
            'event_id',
            'event_type',
            'country',
            'admin1',
            'district_focus',
            'event_date',
            'event_window_start',
            'event_window_end',
            'source_name',
            'source_url',
            'source_accessed_utc',
            'confidence',
            'summary',
            'notes',
        ],
        event_rows,
        event_path,
    )
    write_rows_csv(
        [
            'anomaly_id',
            'review_week',
            'country',
            'district',
            'signal_family',
            'signal_summary',
            'nearest_baseline_event',
            'baseline_event_date',
            'proxy_signal_date',
            'humanitarian_baseline_score',
            'market_proxy_score',
            'spatial_fit_score',
            'temporal_fit_score',
            'cross_source_score',
            'confound_penalty',
            'raw_score',
            'final_score',
            'publication_label',
            'analyst_initials',
            'evidence_links',
            'confound_notes',
            'next_collection_action',
            'status',
        ],
        anomaly_rows,
        anomaly_path,
    )
    write_markdown(render_pilot_execution_summary(records), summary_path)

    return {
        'status': 'ok',
        'action': 'scaffold_v0',
        'input': str(Path(args.input)),
        'pack_dir': str(pack_dir),
        'source_owner_assignments': str(owner_path),
        'district_watchlist': str(district_path),
        'event_timeline': str(event_path),
        'anomaly_review_worksheet': str(anomaly_path),
        'pilot_execution_summary': str(summary_path),
        'source_summary': build_source_summary(records),
        'launcher_mode': getattr(args, 'launcher_mode', 'cli'),
    }


def emit_progress(callback: ProgressCallback | None, step: int, total: int, message: str) -> None:
    logging.info('%s', message)
    if callback is not None:
        callback(step, total, message)


def execute_action(
    args: argparse.Namespace,
    action: str,
    progress_callback: ProgressCallback | None = None,
) -> dict[str, Any]:
    input_path = Path(args.input)
    records = load_seed(input_path)

    if action == 'inspect':
        validate_seed(records)
        return {
            'status': 'ok',
            'action': 'inspect',
            'input': str(input_path),
            'source_summary': build_source_summary(records),
            'summary_text': render_source_summary(records),
            'launcher_mode': getattr(args, 'launcher_mode', 'cli'),
        }

    if action == 'check':
        total_steps = 3
        emit_progress(progress_callback, 1, total_steps, 'Loading canonical seed file')
        emit_progress(progress_callback, 2, total_steps, 'Validating seeded source records')
        validate_seed(records)
        emit_progress(progress_callback, 3, total_steps, 'Validation complete')
        return {
            'status': 'ok',
            'action': 'check',
            'input': str(input_path),
            'source_summary': build_source_summary(records),
            'launcher_mode': getattr(args, 'launcher_mode', 'cli'),
        }

    if action == 'scaffold_v0':
        total_steps = 8
        emit_progress(progress_callback, 1, total_steps, 'Preparing v0.1 pack directory')
        Path(args.pack_dir).mkdir(parents=True, exist_ok=True)
        emit_progress(progress_callback, 2, total_steps, 'Loading canonical seed file')
        emit_progress(progress_callback, 3, total_steps, 'Validating seeded source records')
        validate_seed(records)
        emit_progress(progress_callback, 4, total_steps, 'Writing source owner assignments')
        emit_progress(progress_callback, 5, total_steps, 'Writing district watchlist')
        emit_progress(progress_callback, 6, total_steps, 'Writing event timeline starter')
        emit_progress(progress_callback, 7, total_steps, 'Writing anomaly review worksheet')
        result = scaffold_v0_pack(args, records)
        emit_progress(progress_callback, 8, total_steps, 'v0.1 operator pack complete')
        return result

    if action == 'recent_accounting':
        total_steps = 6
        emit_progress(progress_callback, 1, total_steps, 'Preparing plans directory')
        Path(args.plans_dir).mkdir(parents=True, exist_ok=True)
        emit_progress(progress_callback, 2, total_steps, 'Loading canonical seed file')
        emit_progress(progress_callback, 3, total_steps, 'Validating seeded source records')
        validate_seed(records)
        emit_progress(progress_callback, 4, total_steps, 'Merging existing recent-accounting ledger state')
        emit_progress(progress_callback, 5, total_steps, 'Writing recent-accounting artifacts')
        result = write_recent_accounting_pack(args, records)
        emit_progress(progress_callback, 6, total_steps, 'Recent-accounting refresh complete')
        return result

    if action == 'verification_sprint':
        total_steps = 7
        emit_progress(progress_callback, 1, total_steps, 'Preparing verification sprint context')
        Path(args.plans_dir).mkdir(parents=True, exist_ok=True)
        emit_progress(progress_callback, 2, total_steps, 'Loading canonical seed file')
        emit_progress(progress_callback, 3, total_steps, 'Validating seeded source records')
        validate_seed(records)
        emit_progress(progress_callback, 4, total_steps, 'Refreshing recent-accounting ledger state')
        emit_progress(progress_callback, 5, total_steps, 'Merging source verification tracker state')
        emit_progress(progress_callback, 6, total_steps, 'Syncing verification queue rows')
        result = write_verification_sprint_pack(args, records)
        emit_progress(progress_callback, 7, total_steps, 'Verification sprint refresh complete')
        return result

    if action == 'scaffold_collection':
        total_steps = 8
        emit_progress(progress_callback, 1, total_steps, 'Preparing collection pipeline directories')
        Path(args.collection_dir).mkdir(parents=True, exist_ok=True)
        emit_progress(progress_callback, 2, total_steps, 'Loading canonical seed file')
        emit_progress(progress_callback, 3, total_steps, 'Validating seeded source records')
        validate_seed(records)
        emit_progress(progress_callback, 4, total_steps, 'Loading district collection scope')
        emit_progress(progress_callback, 5, total_steps, 'Building source adapter registry')
        emit_progress(progress_callback, 6, total_steps, 'Building query seed manifests')
        emit_progress(progress_callback, 7, total_steps, 'Writing collection pipeline artifacts')
        result = scaffold_collection_pack(args, records)
        emit_progress(progress_callback, 8, total_steps, 'Collection pipeline pack complete')
        return result

    if action == 'brief_zone':
        total_steps = 7
        emit_progress(progress_callback, 1, total_steps, 'Preparing zone briefing directory')
        Path(args.briefing_dir).mkdir(parents=True, exist_ok=True)
        emit_progress(progress_callback, 2, total_steps, 'Loading canonical seed file')
        emit_progress(progress_callback, 3, total_steps, 'Validating seeded source records')
        validate_seed(records)
        emit_progress(progress_callback, 4, total_steps, 'Loading zone ledger and collection context')
        emit_progress(progress_callback, 5, total_steps, 'Building zone observations and claim pack')
        emit_progress(progress_callback, 6, total_steps, 'Writing zone briefing artifacts')
        result = write_zone_briefing_pack(args, records)
        emit_progress(progress_callback, 7, total_steps, 'Zone briefing pack complete')
        return result

    if action == 'operating_cycle':
        total_steps = 4
        emit_progress(progress_callback, 1, total_steps, 'Preparing operating cycle wrapper')
        repo_root = Path(__file__).resolve().parent
        script_path = repo_root / 'scripts' / 'run_operating_cycle.py'
        cycle_root = repo_root / 'artifacts' / 'operating-cycles'
        command = [
            sys.executable,
            str(script_path),
            '--plans-dir',
            str(Path(args.plans_dir)),
            '--collection-dir',
            str(Path(args.collection_dir)),
            '--briefing-dir',
            str(Path(args.briefing_dir)),
            '--zone-name',
            args.zone_name,
            '--zone-country',
            args.zone_country,
            '--max-runs',
            str(args.max_runs),
        ]
        emit_progress(progress_callback, 2, total_steps, 'Running collection, verification, and briefing cycle')
        completed = subprocess.run(command, cwd=repo_root, capture_output=True, text=True)
        combined_output = '\n'.join(part for part in [completed.stdout.strip(), completed.stderr.strip()] if part).strip()
        if completed.returncode != 0:
            raise RuntimeError(combined_output or 'Operating cycle failed.')
        emit_progress(progress_callback, 3, total_steps, 'Loading latest operating cycle manifest')
        manifests = sorted(cycle_root.glob('*/run-manifest.json'))
        if not manifests:
            raise RuntimeError('Operating cycle completed but no run manifest was found.')
        manifest_path = manifests[-1]
        manifest = json.loads(manifest_path.read_text())
        emit_progress(progress_callback, 4, total_steps, 'Operating cycle complete')
        return {
            'status': 'ok',
            'action': 'operating_cycle',
            'manifest_path': str(manifest_path),
            'log_path': str(Path(manifest.get('cycle_dir', '')) / 'run.log'),
            'cycle_status': manifest.get('status', ''),
            'cycle_id': manifest.get('cycle_id', ''),
            'planned_commands': manifest.get('planned_commands', []),
            'steps': manifest.get('steps', []),
            'stdout': combined_output,
            'launcher_mode': getattr(args, 'launcher_mode', 'cli'),
        }

    if action == 'collect_ready':
        total_steps = 6
        emit_progress(progress_callback, 1, total_steps, 'Preparing collection execution context')
        Path(args.collection_dir).mkdir(parents=True, exist_ok=True)
        emit_progress(progress_callback, 2, total_steps, 'Loading canonical seed file')
        emit_progress(progress_callback, 3, total_steps, 'Validating seeded source records')
        validate_seed(records)
        emit_progress(progress_callback, 4, total_steps, 'Loading collection manifest and adapter registry')
        emit_progress(progress_callback, 5, total_steps, 'Processing ready collection runs')
        result = run_collection_pack(args, records)
        emit_progress(progress_callback, 6, total_steps, 'Collection execution complete')
        return result

    if action != 'bootstrap':
        raise ValueError(f'Unsupported action: {action}')

    total_steps = 7
    output_dir = Path(args.output_dir)
    docs_csv_path = Path(args.docs_csv)

    emit_progress(progress_callback, 1, total_steps, 'Preparing output directories')
    output_dir.mkdir(parents=True, exist_ok=True)
    docs_csv_path.parent.mkdir(parents=True, exist_ok=True)

    emit_progress(progress_callback, 2, total_steps, 'Loading canonical seed file')
    emit_progress(progress_callback, 3, total_steps, 'Validating seeded source records')
    validate_seed(records)

    emit_progress(progress_callback, 4, total_steps, 'Selecting output version')
    version = next_version(output_dir, args.version_prefix, args.force_version)
    json_path = output_dir / f'{args.version_prefix}{version}.json'
    csv_path = output_dir / f'{args.version_prefix}{version}.csv'

    emit_progress(progress_callback, 5, total_steps, 'Writing JSON artifact')
    write_json(records, json_path)

    emit_progress(progress_callback, 6, total_steps, 'Writing CSV artifacts')
    write_csv(records, csv_path)
    write_csv(records, docs_csv_path)

    emit_progress(progress_callback, 7, total_steps, 'Bootstrap complete')
    return {
        'status': 'ok',
        'action': 'bootstrap',
        'input': str(input_path),
        'output_dir': str(output_dir),
        'generated_json': str(json_path),
        'generated_csv': str(csv_path),
        'docs_csv': str(docs_csv_path),
        'source_count': len(records),
        'version': version,
        'verbose': bool(args.verbose),
        'source_summary': build_source_summary(records),
        'launcher_mode': getattr(args, 'launcher_mode', 'cli'),
    }


def prompt_with_default(label: str, default: str) -> str:
    response = input(f'{label} [{default}]: ').strip()
    return response or default


def prompt_optional_int(label: str) -> int | None:
    response = input(f'{label} [auto]: ').strip()
    if not response:
        return None
    return int(response)


def prompt_yes_no(label: str, default_yes: bool = True) -> bool:
    suffix = 'Y/n' if default_yes else 'y/N'
    response = input(f'{label} [{suffix}]: ').strip().lower()
    if not response:
        return default_yes
    return response in {'y', 'yes'}


def latest_operating_cycle_manifest(cycle_root: Path) -> dict[str, Any]:
    manifests = sorted(cycle_root.glob('*/run-manifest.json'))
    if not manifests:
        return {}
    try:
        payload = json.loads(manifests[-1].read_text())
    except json.JSONDecodeError:
        return {}
    payload['manifest_path'] = str(manifests[-1])
    return payload


def build_pipeline_snapshot(args: argparse.Namespace) -> dict[str, Any]:
    repo_root = Path(__file__).resolve().parent
    recent_rows = load_csv_rows(Path(args.plans_dir) / 'recent_accounting.csv')
    sprint_rows = load_csv_rows(Path(args.plans_dir) / 'source_verification_sprint.csv')
    collection_rows = load_csv_rows(Path(args.collection_dir) / 'collection-run-results.csv')
    latest_brief = Path(args.briefing_dir) / zone_pack_id(args.zone_name) / 'zone_brief.md'
    return {
        'recent_counts': dict(Counter((row.get('recency_status', 'unknown') or 'unknown') for row in recent_rows)),
        'verification_counts': dict(Counter((row.get('status', 'pending') or 'pending') for row in sprint_rows)),
        'collection_counts': dict(Counter((row.get('result_status', 'unknown') or 'unknown') for row in collection_rows)),
        'latest_cycle': latest_operating_cycle_manifest(repo_root / 'artifacts' / 'operating-cycles'),
        'latest_brief_exists': latest_brief.exists(),
        'latest_brief_path': str(latest_brief),
    }


def render_count_line(title: str, counts: dict[str, Any], order: list[str]) -> str:
    parts = [f'{key}={counts.get(key, 0)}' for key in order]
    return f'{title}: ' + ' | '.join(parts)


def render_tui_dashboard(args: argparse.Namespace, snapshot: dict[str, Any], last_summary: dict[str, Any] | None = None) -> str:
    cycle = snapshot.get('latest_cycle', {})
    cycle_line = 'Latest cycle: none'
    if cycle:
        cycle_line = (
            f"Latest cycle: {cycle.get('cycle_id', 'unknown')} | "
            f"status={cycle.get('status', 'unknown')} | ended={cycle.get('ended_at_utc', 'unknown')}"
        )

    lines = [
        '',
        '=== MunchMusings Pipeline Console ===',
        f'Seed: {args.input}',
        f'Zone: {args.zone_name} ({args.zone_country})',
        f'Plans: {args.plans_dir}',
        f'Collection: {args.collection_dir}',
        f'Briefings: {args.briefing_dir}',
        f'Max runs: {args.max_runs} | Verbose: {args.verbose}',
        '',
        render_count_line('Recent accounting', snapshot.get('recent_counts', {}), ['current', 'due_now', 'overdue', 'manual_review', 'unknown', 'blocked']),
        render_count_line('Verification sprint', snapshot.get('verification_counts', {}), ['verified', 'research_complete', 'pending', 'blocked']),
        render_count_line('Collection runs', snapshot.get('collection_counts', {}), ['completed', 'staged_external', 'failed']),
        cycle_line,
        f"Latest brief: {'present' if snapshot.get('latest_brief_exists') else 'missing'} | {snapshot.get('latest_brief_path', '')}",
    ]
    if last_summary:
        lines.append(f"Last action: {last_summary.get('action', 'unknown')} | status={last_summary.get('status', 'unknown')}")
    lines.extend(
        [
            '',
            '1) Run full operating cycle',
            '2) Execute ready collection runs',
            '3) Refresh verification sprint',
            '4) Build zone briefing pack',
            '5) Refresh recent-accounting ledger',
            '6) Build collection pipeline pack',
            '7) Build v0.1 operator pack',
            '8) Bootstrap source registry',
            '9) View source summary',
            '10) Validate seed only',
            '11) Edit settings',
            '12) Refresh dashboard',
            '13) Exit',
        ]
    )
    return '\n'.join(lines)


def render_tui_preflight(action: str, args: argparse.Namespace) -> str:
    updates = {
        'operating_cycle': [Path(args.collection_dir), Path(args.plans_dir), Path(args.briefing_dir), Path('artifacts/operating-cycles')],
        'collect_ready': [Path(args.collection_dir)],
        'verification_sprint': [Path(args.plans_dir)],
        'brief_zone': [Path(args.briefing_dir)],
        'recent_accounting': [Path(args.plans_dir)],
        'scaffold_collection': [Path(args.collection_dir)],
        'scaffold_v0': [Path(args.pack_dir)],
        'bootstrap': [Path(args.output_dir), Path(args.docs_csv)],
    }
    lines = [f'Preflight for `{action}`:', f'- Seed input: {args.input}', f'- Zone: {args.zone_name} ({args.zone_country})']
    if action in {'collect_ready', 'operating_cycle'}:
        lines.append(f'- Max runs: {args.max_runs}')
    lines.append('- Will update:')
    lines.extend(f'  {path}' for path in updates.get(action, []))
    return '\n'.join(lines)


def prompt_tui_settings(args: argparse.Namespace) -> argparse.Namespace:
    print('\n=== Edit Pipeline Settings ===')
    args.input = prompt_with_default('Seed input file', args.input)
    args.output_dir = prompt_with_default('Bootstrap output directory', args.output_dir)
    args.docs_csv = prompt_with_default('Tracked docs CSV', args.docs_csv)
    args.pack_dir = prompt_with_default('v0.1 pack directory', args.pack_dir)
    args.plans_dir = prompt_with_default('Plans directory', args.plans_dir)
    args.collection_dir = prompt_with_default('Collection directory', args.collection_dir)
    args.briefing_dir = prompt_with_default('Briefing directory', args.briefing_dir)
    args.zone_name = prompt_with_default('Zone name', args.zone_name)
    args.zone_country = prompt_with_default('Zone country', args.zone_country)
    args.max_runs = int(prompt_with_default('Max runs per collect-ready', str(args.max_runs)))
    args.verbose = prompt_yes_no('Verbose logging', default_yes=args.verbose)
    if prompt_yes_no('Update forced bootstrap version now', default_yes=False):
        args.force_version = prompt_optional_int('Forced version number')
    return args


def launch_tui(args: argparse.Namespace) -> int:
    args.launcher_mode = 'tui'
    last_summary: dict[str, Any] | None = None
    action_map = {
        '1': 'operating_cycle',
        '2': 'collect_ready',
        '3': 'verification_sprint',
        '4': 'brief_zone',
        '5': 'recent_accounting',
        '6': 'scaffold_collection',
        '7': 'scaffold_v0',
        '8': 'bootstrap',
        '9': 'inspect',
        '10': 'check',
    }
    write_actions = {'operating_cycle', 'collect_ready', 'verification_sprint', 'brief_zone', 'recent_accounting', 'scaffold_collection', 'scaffold_v0', 'bootstrap'}

    while True:
        snapshot = build_pipeline_snapshot(args)
        print(render_tui_dashboard(args, snapshot, last_summary))
        selection = input('Select option [1]: ').strip() or '1'

        if selection == '13':
            print('Exiting console.')
            return 0
        if selection == '11':
            args = prompt_tui_settings(args)
            last_summary = {'action': 'edit_settings', 'status': 'ok'}
            continue
        if selection == '12':
            last_summary = {'action': 'refresh_dashboard', 'status': 'ok'}
            continue
        if selection not in action_map:
            last_summary = {'action': 'menu', 'status': 'failed', 'error': f'Unsupported launcher option: {selection}'}
            print(last_summary['error'])
            continue

        action = action_map[selection]
        if action in write_actions:
            print('\n' + render_tui_preflight(action, args))
            if not prompt_yes_no('Proceed', default_yes=True):
                last_summary = {'action': action, 'status': 'cancelled'}
                continue

        try:
            configure_logging(args.verbose)
            summary = execute_action(args, action=action)
            last_summary = summary
            if action == 'inspect':
                print('\n' + summary['summary_text'])
            else:
                print('\n' + json.dumps(summary, indent=2))
        except Exception as exc:
            last_summary = {'action': action, 'status': 'failed', 'error': str(exc)}
            print(f'\nAction failed: {exc}')

        input('\nPress Enter to return to the console...')


def launch_gui(args: argparse.Namespace) -> int:
    if not (os.environ.get('DISPLAY') or os.environ.get('WAYLAND_DISPLAY')):
        raise RuntimeError('GUI mode requires DISPLAY or WAYLAND_DISPLAY to be set.')

    root = Tk()
    root.title('MunchMusings Bootstrap Launcher')
    root.geometry('820x620')

    input_var = StringVar(value=args.input)
    output_var = StringVar(value=args.output_dir)
    docs_csv_var = StringVar(value=args.docs_csv)
    pack_var = StringVar(value=args.pack_dir)
    plans_var = StringVar(value=args.plans_dir)
    collection_var = StringVar(value=args.collection_dir)
    briefing_var = StringVar(value=args.briefing_dir)
    zone_name_var = StringVar(value=args.zone_name)
    zone_country_var = StringVar(value=args.zone_country)
    force_var = StringVar(value='' if args.force_version is None else str(args.force_version))
    verbose_var = BooleanVar(value=args.verbose)
    status_var = StringVar(value='Ready')
    queue: Queue[tuple[str, Any]] = Queue()

    frame = ttk.Frame(root, padding=12)
    frame.pack(fill=BOTH, expand=True)
    frame.columnconfigure(1, weight=1)
    frame.rowconfigure(13, weight=1)

    ttk.Label(frame, text='Seed input').grid(row=0, column=0, sticky='w')
    ttk.Entry(frame, textvariable=input_var, width=90).grid(row=0, column=1, sticky='ew', pady=4)

    ttk.Label(frame, text='Output directory').grid(row=1, column=0, sticky='w')
    ttk.Entry(frame, textvariable=output_var, width=90).grid(row=1, column=1, sticky='ew', pady=4)

    ttk.Label(frame, text='Tracked docs CSV').grid(row=2, column=0, sticky='w')
    ttk.Entry(frame, textvariable=docs_csv_var, width=90).grid(row=2, column=1, sticky='ew', pady=4)

    ttk.Label(frame, text='v0.1 pack directory').grid(row=3, column=0, sticky='w')
    ttk.Entry(frame, textvariable=pack_var, width=90).grid(row=3, column=1, sticky='ew', pady=4)

    ttk.Label(frame, text='Plans directory').grid(row=4, column=0, sticky='w')
    ttk.Entry(frame, textvariable=plans_var, width=90).grid(row=4, column=1, sticky='ew', pady=4)

    ttk.Label(frame, text='Collection directory').grid(row=5, column=0, sticky='w')
    ttk.Entry(frame, textvariable=collection_var, width=90).grid(row=5, column=1, sticky='ew', pady=4)

    ttk.Label(frame, text='Briefing directory').grid(row=6, column=0, sticky='w')
    ttk.Entry(frame, textvariable=briefing_var, width=90).grid(row=6, column=1, sticky='ew', pady=4)

    ttk.Label(frame, text='Zone name').grid(row=7, column=0, sticky='w')
    ttk.Entry(frame, textvariable=zone_name_var, width=90).grid(row=7, column=1, sticky='ew', pady=4)

    ttk.Label(frame, text='Zone country').grid(row=8, column=0, sticky='w')
    ttk.Entry(frame, textvariable=zone_country_var, width=90).grid(row=8, column=1, sticky='ew', pady=4)

    ttk.Label(frame, text='Forced version').grid(row=9, column=0, sticky='w')
    ttk.Entry(frame, textvariable=force_var, width=20).grid(row=9, column=1, sticky='w', pady=4)

    ttk.Checkbutton(frame, text='Verbose logging', variable=verbose_var).grid(row=10, column=1, sticky='w', pady=4)

    progress = ttk.Progressbar(frame, mode='determinate', maximum=8)
    progress.grid(row=11, column=0, columnspan=2, sticky='ew', pady=8)

    ttk.Label(frame, textvariable=status_var).grid(row=12, column=0, columnspan=2, sticky='w')

    output_box = Text(frame, height=20, wrap='word')
    output_box.grid(row=13, column=0, columnspan=2, sticky='nsew', pady=8)

    button_row = ttk.Frame(frame)
    button_row.grid(row=14, column=0, columnspan=2, sticky='e')
    bootstrap_button = ttk.Button(button_row, text='Run Bootstrap')
    validate_button = ttk.Button(button_row, text='Validate Seed')
    summary_button = ttk.Button(button_row, text='View Summary')
    scaffold_button = ttk.Button(button_row, text='Build v0.1 Pack')
    accounting_button = ttk.Button(button_row, text='Refresh Accounting')
    verification_button = ttk.Button(button_row, text='Refresh Verification')
    collection_button = ttk.Button(button_row, text='Build Collection Pack')
    briefing_button = ttk.Button(button_row, text='Build Zone Brief')
    run_collection_button = ttk.Button(button_row, text='Run Ready Collection')
    close_button = ttk.Button(button_row, text='Close', command=root.destroy)
    bootstrap_button.pack(side='left', padx=4)
    validate_button.pack(side='left', padx=4)
    summary_button.pack(side='left', padx=4)
    scaffold_button.pack(side='left', padx=4)
    accounting_button.pack(side='left', padx=4)
    verification_button.pack(side='left', padx=4)
    collection_button.pack(side='left', padx=4)
    briefing_button.pack(side='left', padx=4)
    run_collection_button.pack(side='left', padx=4)
    close_button.pack(side='left', padx=4)

    def append_output(text: str) -> None:
        output_box.config(state=NORMAL)
        output_box.delete('1.0', END)
        output_box.insert(END, text)
        output_box.config(state=DISABLED)

    def set_buttons(state: str) -> None:
        bootstrap_button.config(state=state)
        validate_button.config(state=state)
        summary_button.config(state=state)
        scaffold_button.config(state=state)
        accounting_button.config(state=state)
        verification_button.config(state=state)
        collection_button.config(state=state)
        briefing_button.config(state=state)
        run_collection_button.config(state=state)

    def worker(action: str) -> None:
        local_args = argparse.Namespace(
            input=input_var.get(),
            output_dir=output_var.get(),
            docs_csv=docs_csv_var.get(),
            pack_dir=pack_var.get(),
            plans_dir=plans_var.get(),
            collection_dir=collection_var.get(),
            briefing_dir=briefing_var.get(),
            zone_name=zone_name_var.get(),
            zone_country=zone_country_var.get(),
            analyst=args.analyst,
            reviewer=args.reviewer,
            max_runs=args.max_runs,
            version_prefix=args.version_prefix,
            force_version=int(force_var.get()) if force_var.get().strip() else None,
            verbose=bool(verbose_var.get()),
            launcher_mode='gui',
        )
        try:
            configure_logging(local_args.verbose)
            summary = execute_action(
                local_args,
                action=action,
                progress_callback=lambda step, total, message: queue.put(('progress', (step, total, message))),
            )
            queue.put(('done', summary))
        except Exception as exc:
            queue.put(('error', str(exc)))

    def poll_queue() -> None:
        try:
            while True:
                event, payload = queue.get_nowait()
                if event == 'progress':
                    step, total, message = payload
                    progress.config(maximum=total, value=step)
                    status_var.set(message)
                elif event == 'done':
                    status_var.set('Completed successfully')
                    if payload.get('action') == 'inspect':
                        append_output(payload['summary_text'])
                    else:
                        append_output(json.dumps(payload, indent=2))
                    set_buttons(NORMAL)
                elif event == 'error':
                    status_var.set('Failed')
                    append_output(payload)
                    set_buttons(NORMAL)
        except Empty:
            pass
        root.after(100, poll_queue)

    def on_run(action: str) -> None:
        set_buttons(DISABLED)
        progress.config(value=0)
        status_var.set(f'Running {action}...')
        append_output('Working...')
        Thread(target=worker, args=(action,), daemon=True).start()

    bootstrap_button.config(command=lambda: on_run('bootstrap'))
    validate_button.config(command=lambda: on_run('check'))
    summary_button.config(command=lambda: on_run('inspect'))
    scaffold_button.config(command=lambda: on_run('scaffold_v0'))
    accounting_button.config(command=lambda: on_run('recent_accounting'))
    verification_button.config(command=lambda: on_run('verification_sprint'))
    collection_button.config(command=lambda: on_run('scaffold_collection'))
    briefing_button.config(command=lambda: on_run('brief_zone'))
    run_collection_button.config(command=lambda: on_run('collect_ready'))

    poll_queue()
    root.mainloop()
    return 0


def should_launch_tui(args: argparse.Namespace) -> bool:
    if args.gui:
        return False
    if args.tui:
        return True
    return len(sys.argv) == 1 and sys.stdin.isatty() and sys.stdout.isatty()


def main() -> int:
    args = parse_args()

    if args.gui:
        return launch_gui(args)

    action = 'bootstrap'
    if args.inspect:
        action = 'inspect'
    elif args.check:
        action = 'check'
    elif args.operating_cycle:
        action = 'operating_cycle'
    elif args.scaffold_v0:
        action = 'scaffold_v0'
    elif args.recent_accounting:
        action = 'recent_accounting'
    elif args.verification_sprint:
        action = 'verification_sprint'
    elif args.scaffold_collection:
        action = 'scaffold_collection'
    elif args.brief_zone:
        action = 'brief_zone'
    elif args.collect_ready:
        action = 'collect_ready'

    if should_launch_tui(args):
        return launch_tui(args)

    args.launcher_mode = 'cli'

    configure_logging(args.verbose)
    summary = execute_action(args, action=action)
    if action == 'inspect':
        print(summary['summary_text'])
    else:
        print(json.dumps(summary, indent=2))
    return 0


if __name__ == '__main__':
    sys.exit(main())
