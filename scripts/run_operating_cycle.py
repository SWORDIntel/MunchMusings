#!/usr/bin/env python3
"""Run collection, verification, and briefing as one dated operating cycle."""

from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


def utc_stamp() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z')


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Run collection, verification, and briefing in one dated cycle.')
    parser.add_argument('--python', default=os.environ.get('PYTHON_BIN', 'python'), help='Python interpreter used to invoke bootstrap.py.')
    parser.add_argument('--plans-dir', default='plans', help='Plans directory passed to bootstrap.py.')
    parser.add_argument('--collection-dir', default='artifacts/collection', help='Collection directory passed to bootstrap.py.')
    parser.add_argument('--briefing-dir', default='artifacts/briefings', help='Briefing directory passed to bootstrap.py.')
    parser.add_argument('--cycle-root', default='artifacts/operating-cycles', help='Directory that will contain dated cycle runs.')
    parser.add_argument('--zone-name', default='Cairo/Giza pilot', help='Zone name passed to bootstrap.py --brief-zone.')
    parser.add_argument('--zone-country', default='Egypt', help='Zone country passed to bootstrap.py --brief-zone.')
    parser.add_argument('--max-runs', type=int, default=5, help='Maximum number of collection runs to process.')
    parser.add_argument('--dry-run', action='store_true', help='Write the manifest and log skeleton without executing bootstrap commands.')
    parser.add_argument('--dashboard', action='store_true', help='Launch the automotive TUI dashboard during the cycle.')
    return parser.parse_args()


def resolve_path(repo_root: Path, value: str) -> Path:
    path = Path(value)
    return path if path.is_absolute() else repo_root / path


def run_command(command: list[str], repo_root: Path, log_handle) -> dict[str, object]:
    started_at = utc_stamp()
    command_line = shlex.join(command)
    print(f'[cycle] starting: {command_line}')
    log_handle.write(f'[cycle] starting: {command_line}\n')
    log_handle.flush()

    process = subprocess.Popen(
        command,
        cwd=repo_root,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    assert process.stdout is not None
    for line in process.stdout:
        sys.stdout.write(line)
        sys.stdout.flush()
        log_handle.write(line)
        log_handle.flush()

    exit_code = process.wait()
    ended_at = utc_stamp()
    log_handle.write(f'[cycle] finished: {command_line} (exit={exit_code})\n')
    log_handle.flush()

    return {
        'command': command_line,
        'started_at_utc': started_at,
        'ended_at_utc': ended_at,
        'exit_code': exit_code,
    }


def write_manifest(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + '\n')


def main() -> int:
    args = parse_args()
    repo_root = Path(__file__).resolve().parents[1]

    cycle_root = resolve_path(repo_root, args.cycle_root)
    cycle_id = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    cycle_dir = cycle_root / cycle_id
    cycle_dir.mkdir(parents=True, exist_ok=False)

    log_path = cycle_dir / 'run.log'
    manifest_path = cycle_dir / 'run-manifest.json'
    started_at = utc_stamp()
    steps: list[dict[str, object]] = []
    exit_code = 0
    failure_reason = ''

    plans_dir = resolve_path(repo_root, args.plans_dir)
    collection_dir = resolve_path(repo_root, args.collection_dir)
    briefing_dir = resolve_path(repo_root, args.briefing_dir)

    commands = [
        [
            args.python,
            'bootstrap.py',
            '--collect-ready',
            '--max-runs',
            str(args.max_runs),
            '--plans-dir',
            str(plans_dir),
            '--collection-dir',
            str(collection_dir),
            '--briefing-dir',
            str(briefing_dir),
            '--zone-name',
            args.zone_name,
            '--zone-country',
            args.zone_country,
        ],
        [
            args.python,
            'bootstrap.py',
            '--verification-sprint',
            '--plans-dir',
            str(plans_dir),
            '--collection-dir',
            str(collection_dir),
            '--briefing-dir',
            str(briefing_dir),
            '--zone-name',
            args.zone_name,
            '--zone-country',
            args.zone_country,
        ],
        [
            args.python,
            'bootstrap.py',
            '--brief-zone',
            '--plans-dir',
            str(plans_dir),
            '--collection-dir',
            str(collection_dir),
            '--briefing-dir',
            str(briefing_dir),
            '--zone-name',
            args.zone_name,
            '--zone-country',
            args.zone_country,
        ],
    ]
    planned_commands = [shlex.join(command) for command in commands]

    if args.dashboard and sys.stdout.isatty():
        print(f"[cycle] Dashboard requested. Launching scripts/dashboard.py...")
        try:
            subprocess.Popen([args.python, str(repo_root / 'scripts/dashboard.py')])
        except Exception as e:
            print(f"[cycle] Failed to launch dashboard: {e}")

    with log_path.open('w', encoding='utf-8') as log_handle:
        log_handle.write(f'[cycle] started: {started_at}\n')
        log_handle.write(f'[cycle] repo_root: {repo_root}\n')
        log_handle.write(f'[cycle] cycle_dir: {cycle_dir}\n')
        log_handle.flush()

        try:
            if args.dry_run:
                for command in commands:
                    command_line = shlex.join(command)
                    print(f'[cycle] dry-run: {command_line}')
                    log_handle.write(f'[cycle] dry-run: {command_line}\n')
                exit_code = 0
            else:
                for command in commands:
                    result = run_command(command, repo_root, log_handle)
                    steps.append(result)
                    if int(result['exit_code']) != 0:
                        exit_code = int(result['exit_code'])
                        failure_reason = f"command failed: {result['command']}"
                        break
        except Exception as exc:  # pragma: no cover - defensive wrapper guard
            exit_code = 1
            failure_reason = f'{type(exc).__name__}: {exc}'
            print(f'[cycle] error: {failure_reason}', file=sys.stderr)
            log_handle.write(f'[cycle] error: {failure_reason}\n')
        finally:
            ended_at = utc_stamp()
            status = 'dry_run' if args.dry_run else ('ok' if exit_code == 0 else 'failed')
            manifest = {
                'cycle_id': cycle_id,
                'repo_root': str(repo_root),
                'cycle_root': str(cycle_root),
                'cycle_dir': str(cycle_dir),
                'started_at_utc': started_at,
                'ended_at_utc': ended_at,
                'status': status,
                'plans_dir': str(plans_dir),
                'collection_dir': str(collection_dir),
                'briefing_dir': str(briefing_dir),
                'zone_name': args.zone_name,
                'zone_country': args.zone_country,
                'max_runs': args.max_runs,
                'planned_commands': planned_commands,
                'steps': steps,
            }
            if failure_reason:
                manifest['failure_reason'] = failure_reason
            write_manifest(manifest_path, manifest)
            log_handle.write(f'[cycle] manifest: {manifest_path}\n')
            log_handle.flush()

    print(f'[cycle] manifest written to {manifest_path}')
    print(f'[cycle] log written to {log_path}')
    return exit_code


if __name__ == '__main__':
    raise SystemExit(main())
