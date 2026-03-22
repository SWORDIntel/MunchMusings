#!/usr/bin/env python3
# Name: bootstrap.py | Version: v1.3.0
# Purpose: GUI/TUI/CLI launcher for the MunchMusings seeded source registry.
# Features: bootstrap, validation, seed inspection, GUI progress, versioned outputs, safe defaults, JSON summaries.

import argparse
import csv
import json
import logging
import os
import sys
from pathlib import Path
from queue import Empty, Queue
from threading import Thread
from tkinter import BOTH, END, DISABLED, NORMAL, BooleanVar, StringVar, Text, Tk, ttk
from typing import Any, Callable

ProgressCallback = Callable[[int, int, str], None]


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
    parser.add_argument('--version-prefix', default='preseed_sources_v', help='Filename prefix for generated JSON and CSV artifacts.')
    parser.add_argument('--force-version', type=int, default=None, help='Force a specific output version number instead of auto-incrementing.')
    parser.add_argument('--verbose', action='store_true', help='Enable verbose logging.')
    parser.add_argument('--tui', action='store_true', help='Launch the terminal UI even if arguments are supplied via stdin piping.')
    parser.add_argument('--gui', action='store_true', help='Launch the GUI bootstrap window with progress tracking.')
    parser.add_argument('--check', action='store_true', help='Validate the seed and print a JSON summary without writing artifacts.')
    parser.add_argument('--inspect', action='store_true', help='Print a human-readable summary of the seeded sources without writing artifacts.')
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
    if len(records) != 20:
        raise ValueError(f'Expected exactly 20 seeded sources, found {len(records)}.')

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

    expected_ranks = set(range(1, 21))
    if seen_ranks != expected_ranks:
        raise ValueError('Seed ranks must be exactly 1 through 20.')


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


def launch_tui(args: argparse.Namespace) -> tuple[str, argparse.Namespace] | None:
    print('\n=== MunchMusings Bootstrap Launcher ===')
    print('1) Bootstrap the preseeded top-20 source pack')
    print('2) View source summary')
    print('3) Validate seed only')
    print('4) Exit')
    selection = input('Select option [1]: ').strip() or '1'

    if selection == '4':
        print('Exiting launcher without changes.')
        return None
    if selection not in {'1', '2', '3'}:
        raise ValueError(f'Unsupported launcher option: {selection}')

    args.input = prompt_with_default('Seed input file', args.input)
    args.verbose = prompt_yes_no('Verbose logging', default_yes=True)

    action = 'bootstrap'
    if selection == '1':
        args.output_dir = prompt_with_default('Output directory', args.output_dir)
        args.docs_csv = prompt_with_default('Tracked docs CSV', args.docs_csv)
        args.force_version = prompt_optional_int('Forced version number')
    elif selection == '2':
        action = 'inspect'
    elif selection == '3':
        action = 'check'

    args.launcher_mode = 'tui'
    return action, args


def launch_gui(args: argparse.Namespace) -> int:
    if not (os.environ.get('DISPLAY') or os.environ.get('WAYLAND_DISPLAY')):
        raise RuntimeError('GUI mode requires DISPLAY or WAYLAND_DISPLAY to be set.')

    root = Tk()
    root.title('MunchMusings Bootstrap Launcher')
    root.geometry('820x620')

    input_var = StringVar(value=args.input)
    output_var = StringVar(value=args.output_dir)
    docs_csv_var = StringVar(value=args.docs_csv)
    force_var = StringVar(value='' if args.force_version is None else str(args.force_version))
    verbose_var = BooleanVar(value=args.verbose)
    status_var = StringVar(value='Ready')
    queue: Queue[tuple[str, Any]] = Queue()

    frame = ttk.Frame(root, padding=12)
    frame.pack(fill=BOTH, expand=True)
    frame.columnconfigure(1, weight=1)
    frame.rowconfigure(7, weight=1)

    ttk.Label(frame, text='Seed input').grid(row=0, column=0, sticky='w')
    ttk.Entry(frame, textvariable=input_var, width=90).grid(row=0, column=1, sticky='ew', pady=4)

    ttk.Label(frame, text='Output directory').grid(row=1, column=0, sticky='w')
    ttk.Entry(frame, textvariable=output_var, width=90).grid(row=1, column=1, sticky='ew', pady=4)

    ttk.Label(frame, text='Tracked docs CSV').grid(row=2, column=0, sticky='w')
    ttk.Entry(frame, textvariable=docs_csv_var, width=90).grid(row=2, column=1, sticky='ew', pady=4)

    ttk.Label(frame, text='Forced version').grid(row=3, column=0, sticky='w')
    ttk.Entry(frame, textvariable=force_var, width=20).grid(row=3, column=1, sticky='w', pady=4)

    ttk.Checkbutton(frame, text='Verbose logging', variable=verbose_var).grid(row=4, column=1, sticky='w', pady=4)

    progress = ttk.Progressbar(frame, mode='determinate', maximum=7)
    progress.grid(row=5, column=0, columnspan=2, sticky='ew', pady=8)

    ttk.Label(frame, textvariable=status_var).grid(row=6, column=0, columnspan=2, sticky='w')

    output_box = Text(frame, height=20, wrap='word')
    output_box.grid(row=7, column=0, columnspan=2, sticky='nsew', pady=8)

    button_row = ttk.Frame(frame)
    button_row.grid(row=8, column=0, columnspan=2, sticky='e')
    bootstrap_button = ttk.Button(button_row, text='Run Bootstrap')
    validate_button = ttk.Button(button_row, text='Validate Seed')
    summary_button = ttk.Button(button_row, text='View Summary')
    close_button = ttk.Button(button_row, text='Close', command=root.destroy)
    bootstrap_button.pack(side='left', padx=4)
    validate_button.pack(side='left', padx=4)
    summary_button.pack(side='left', padx=4)
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

    def worker(action: str) -> None:
        local_args = argparse.Namespace(
            input=input_var.get(),
            output_dir=output_var.get(),
            docs_csv=docs_csv_var.get(),
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

    if should_launch_tui(args):
        launched = launch_tui(args)
        if launched is None:
            return 0
        action, args = launched
    else:
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
