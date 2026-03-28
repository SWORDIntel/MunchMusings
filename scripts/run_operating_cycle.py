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
    parser.add_argument('--resume-cycle-dir', default='', help='Existing cycle directory to resume instead of creating a new one.')
    parser.add_argument('--resume-latest', action='store_true', help='Resume the latest incomplete cycle that matches the current command plan.')
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


def build_commands(args: argparse.Namespace, repo_root: Path, plans_dir: Path, collection_dir: Path, briefing_dir: Path) -> list[list[str]]:
    return [
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


def normalize_steps(commands: list[list[str]], existing_steps: list[dict[str, object]] | None = None) -> list[dict[str, object]]:
    steps: list[dict[str, object]] = []
    existing_by_index = {int(step.get('step_index', index)): step for index, step in enumerate(existing_steps or [])}
    for index, command in enumerate(commands):
        existing = dict(existing_by_index.get(index, {}))
        steps.append(
            {
                'step_index': index,
                'command': shlex.join(command),
                'status': existing.get('status', 'pending'),
                'started_at_utc': existing.get('started_at_utc', ''),
                'ended_at_utc': existing.get('ended_at_utc', ''),
                'exit_code': existing.get('exit_code', ''),
                'notes': existing.get('notes', ''),
            }
        )
    return steps


def summarize_steps(steps: list[dict[str, object]]) -> dict[str, object]:
    completed_indices = [int(step['step_index']) for step in steps if step.get('status') == 'completed']
    failed_indices = [int(step['step_index']) for step in steps if step.get('status') in {'failed', 'interrupted'}]
    running_indices = [int(step['step_index']) for step in steps if step.get('status') == 'running']
    next_step_index = len(completed_indices)
    for index, step in enumerate(steps):
        if step.get('status') != 'completed':
            next_step_index = index
            break
    return {
        'completed_steps': len(completed_indices),
        'failed_steps': failed_indices,
        'running_steps': running_indices,
        'next_step_index': next_step_index,
    }


def load_manifest(path: Path) -> dict[str, object]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except json.JSONDecodeError:
        return {}


def select_cycle_directory(
    cycle_root: Path,
    resume_cycle_dir: str,
    resume_latest: bool,
    planned_commands: list[str],
) -> tuple[Path, dict[str, object], str]:
    if resume_cycle_dir:
        cycle_dir = Path(resume_cycle_dir)
        if not cycle_dir.exists():
            raise FileNotFoundError(f'Resume cycle directory does not exist: {cycle_dir}')
        manifest = load_manifest(cycle_dir / 'run-manifest.json')
        return cycle_dir, manifest, 'explicit'

    if resume_latest and cycle_root.exists():
        manifests = sorted(cycle_root.glob('*/run-manifest.json'))
        for manifest_path in reversed(manifests):
            manifest = load_manifest(manifest_path)
            if not manifest:
                continue
            if manifest.get('status') not in {'running', 'failed', 'interrupted'}:
                continue
            if manifest.get('planned_commands') != planned_commands:
                continue
            return manifest_path.parent, manifest, 'latest'

    cycle_id = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    cycle_dir = cycle_root / cycle_id
    cycle_dir.mkdir(parents=True, exist_ok=False)
    return cycle_dir, {}, 'new'


def main() -> int:
    args = parse_args()
    repo_root = Path(__file__).resolve().parents[1]

    cycle_root = resolve_path(repo_root, args.cycle_root)
    plans_dir = resolve_path(repo_root, args.plans_dir)
    collection_dir = resolve_path(repo_root, args.collection_dir)
    briefing_dir = resolve_path(repo_root, args.briefing_dir)

    commands = build_commands(args, repo_root, plans_dir, collection_dir, briefing_dir)
    planned_commands = [shlex.join(command) for command in commands]
    cycle_dir, existing_manifest, resume_mode = select_cycle_directory(
        cycle_root,
        args.resume_cycle_dir,
        args.resume_latest,
        planned_commands,
    )
    manifest_path = cycle_dir / 'run-manifest.json'
    log_path = cycle_dir / 'run.log'

    if existing_manifest and existing_manifest.get('planned_commands') and existing_manifest.get('planned_commands') != planned_commands:
        raise RuntimeError('Requested resume cycle does not match the current command plan.')

    if existing_manifest:
        steps = normalize_steps(commands, existing_manifest.get('steps', []))
        started_at = str(existing_manifest.get('started_at_utc', utc_stamp()))
        resume_step_index = int(existing_manifest.get('next_step_index', summarize_steps(steps)['next_step_index']))
    else:
        steps = normalize_steps(commands)
        started_at = utc_stamp()
        resume_step_index = 0
    exit_code = 0
    failure_reason = ''
    cycle_id = existing_manifest.get('cycle_id', cycle_dir.name) if existing_manifest else cycle_dir.name

    def snapshot_manifest(status: str, ended_at: str = '', failure_reason: str = '') -> dict[str, object]:
        step_summary = summarize_steps(steps)
        manifest = {
            'cycle_id': cycle_id,
            'repo_root': str(repo_root),
            'cycle_root': str(cycle_root),
            'cycle_dir': str(cycle_dir),
            'started_at_utc': started_at,
            'ended_at_utc': ended_at,
            'last_updated_utc': utc_stamp(),
            'status': status,
            'plans_dir': str(plans_dir),
            'collection_dir': str(collection_dir),
            'briefing_dir': str(briefing_dir),
            'zone_name': args.zone_name,
            'zone_country': args.zone_country,
            'max_runs': args.max_runs,
            'planned_commands': planned_commands,
            'steps': steps,
            'resume_mode': resume_mode,
            'resume_step_index': resume_step_index,
            'next_step_index': step_summary['next_step_index'],
            'completed_steps': step_summary['completed_steps'],
            'failed_steps': step_summary['failed_steps'],
            'running_steps': step_summary['running_steps'],
        }
        if failure_reason:
            manifest['failure_reason'] = failure_reason
        if status in {'running', 'failed', 'interrupted'}:
            manifest['current_step_index'] = step_summary['next_step_index']
        if ended_at:
            manifest['ended_at_utc'] = ended_at
        return manifest

    write_manifest(manifest_path, snapshot_manifest('running'))

    if args.dashboard and sys.stdout.isatty():
        print(f"[cycle] Dashboard requested. Launching scripts/dashboard.py...")
        try:
            subprocess.Popen([args.python, str(repo_root / 'scripts/dashboard.py')])
        except Exception as e:
            print(f"[cycle] Failed to launch dashboard: {e}")

    log_mode = 'a' if log_path.exists() and existing_manifest else 'w'
    with log_path.open(log_mode, encoding='utf-8') as log_handle:
        if not existing_manifest:
            log_handle.write(f'[cycle] started: {started_at}\n')
            log_handle.write(f'[cycle] repo_root: {repo_root}\n')
            log_handle.write(f'[cycle] cycle_dir: {cycle_dir}\n')
        else:
            log_handle.write(f'[cycle] resumed: {utc_stamp()}\n')
            log_handle.write(f'[cycle] cycle_dir: {cycle_dir}\n')
        log_handle.flush()

        try:
            if args.dry_run:
                for command in commands:
                    command_line = shlex.join(command)
                    print(f'[cycle] dry-run: {command_line}')
                    log_handle.write(f'[cycle] dry-run: {command_line}\n')
                exit_code = 0
                write_manifest(manifest_path, snapshot_manifest('dry_run', ended_at=utc_stamp()))
            else:
                for index in range(resume_step_index, len(commands)):
                    command = commands[index]
                    steps[index]['status'] = 'running'
                    steps[index]['started_at_utc'] = utc_stamp()
                    steps[index]['ended_at_utc'] = ''
                    steps[index]['exit_code'] = ''
                    write_manifest(manifest_path, snapshot_manifest('running'))
                    result = run_command(command, repo_root, log_handle)
                    steps[index]['ended_at_utc'] = result['ended_at_utc']
                    steps[index]['exit_code'] = result['exit_code']
                    if int(result['exit_code']) == 0:
                        steps[index]['status'] = 'completed'
                    else:
                        steps[index]['status'] = 'failed'
                        exit_code = int(result['exit_code'])
                        failure_reason = f"command failed: {result['command']}"
                    write_manifest(manifest_path, snapshot_manifest('running' if exit_code == 0 else 'failed'))
                    if exit_code != 0:
                        break
        except KeyboardInterrupt:
            exit_code = 130
            failure_reason = 'Interrupted by user'
            interrupted_index = summarize_steps(steps)['next_step_index']
            if 0 <= interrupted_index < len(steps):
                steps[interrupted_index]['status'] = 'interrupted'
            write_manifest(manifest_path, snapshot_manifest('interrupted', ended_at=utc_stamp()))
            print('[cycle] interrupted by user', file=sys.stderr)
        except Exception as exc:  # pragma: no cover - defensive wrapper guard
            exit_code = 1
            failure_reason = f'{type(exc).__name__}: {exc}'
            print(f'[cycle] error: {failure_reason}', file=sys.stderr)
            log_handle.write(f'[cycle] error: {failure_reason}\n')
            interrupted_index = summarize_steps(steps)['next_step_index']
            if 0 <= interrupted_index < len(steps) and steps[interrupted_index].get('status') == 'running':
                steps[interrupted_index]['status'] = 'failed'
            write_manifest(manifest_path, snapshot_manifest('failed', ended_at=utc_stamp()))
        finally:
            ended_at = utc_stamp()
            if args.dry_run:
                status = 'dry_run'
            elif exit_code == 0:
                status = 'ok'
            elif exit_code == 130:
                status = 'interrupted'
            else:
                status = 'failed'
            write_manifest(manifest_path, snapshot_manifest(status, ended_at=ended_at, failure_reason=failure_reason))
            log_handle.write(f'[cycle] manifest: {manifest_path}\n')
            log_handle.flush()

    print(f'[cycle] manifest written to {manifest_path}')
    print(f'[cycle] log written to {log_path}')
    return exit_code


if __name__ == '__main__':
    raise SystemExit(main())
