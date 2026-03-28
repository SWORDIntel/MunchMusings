import io
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import scripts.run_operating_cycle as operating_cycle


class FakeProcess:
    def __init__(self, stdout_lines: list[str], exit_code: int) -> None:
        payload = ''.join(line if line.endswith('\n') else f'{line}\n' for line in stdout_lines)
        self.stdout = io.StringIO(payload)
        self._exit_code = exit_code

    def wait(self) -> int:
        return self._exit_code


class OperatingCycleWrapperTests(unittest.TestCase):
    def build_args(self, tmp_path: Path, **overrides):
        defaults = dict(
            python='python',
            plans_dir=str(tmp_path / 'plans'),
            collection_dir=str(tmp_path / 'collection'),
            briefing_dir=str(tmp_path / 'briefings'),
            cycle_root=str(tmp_path / 'cycles'),
            resume_cycle_dir='',
            resume_latest=False,
            zone_name='Cairo/Giza pilot',
            zone_country='Egypt',
            max_runs=5,
            dry_run=False,
            dashboard=False,
        )
        defaults.update(overrides)
        return SimpleNamespace(**defaults)

    def test_main_writes_failure_cursor_after_partial_cycle(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            cycle_root = tmp_path / 'cycles'

            args = self.build_args(tmp_path)
            fake_processes = [
                FakeProcess(['collect ok'], 0),
                FakeProcess(['verify failed'], 5),
            ]
            recorded_commands: list[list[str]] = []

            def fake_popen(command, cwd=None, stdout=None, stderr=None, text=None, bufsize=None):  # noqa: ANN001
                recorded_commands.append(command)
                return fake_processes[len(recorded_commands) - 1]

            with patch.object(operating_cycle, 'parse_args', return_value=args), patch.object(
                operating_cycle.subprocess, 'Popen', side_effect=fake_popen
            ):
                exit_code = operating_cycle.main()

            self.assertEqual(exit_code, 5)
            self.assertEqual(len(recorded_commands), 2)

            cycle_dirs = list(cycle_root.iterdir())
            self.assertEqual(len(cycle_dirs), 1)
            manifest_path = cycle_dirs[0] / 'run-manifest.json'
            manifest = json.loads(manifest_path.read_text())

            self.assertEqual(manifest['status'], 'failed')
            self.assertEqual(manifest['resume_mode'], 'new')
            self.assertEqual(manifest['completed_steps'], 1)
            self.assertEqual(manifest['next_step_index'], 1)
            self.assertEqual(manifest['steps'][0]['status'], 'completed')
            self.assertEqual(manifest['steps'][1]['status'], 'failed')
            self.assertEqual(manifest['failure_reason'], f"command failed: {manifest['steps'][1]['command']}")

    def test_main_resumes_existing_cycle_dir_from_next_step(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            cycle_root = tmp_path / 'cycles'
            resume_cycle_dir = cycle_root / '20260327T120000Z'
            resume_cycle_dir.mkdir(parents=True)

            args = self.build_args(tmp_path, resume_cycle_dir=str(resume_cycle_dir))
            commands = operating_cycle.build_commands(
                args,
                Path('.'),
                Path(args.plans_dir),
                Path(args.collection_dir),
                Path(args.briefing_dir),
            )
            planned_commands = [operating_cycle.shlex.join(command) for command in commands]
            manifest_path = resume_cycle_dir / 'run-manifest.json'
            manifest_path.write_text(
                json.dumps(
                    {
                        'cycle_id': resume_cycle_dir.name,
                        'repo_root': str(Path('.').resolve()),
                        'cycle_root': str(cycle_root),
                        'cycle_dir': str(resume_cycle_dir),
                        'started_at_utc': '2026-03-27T12:00:00Z',
                        'ended_at_utc': '',
                        'last_updated_utc': '2026-03-27T12:01:00Z',
                        'status': 'failed',
                        'plans_dir': args.plans_dir,
                        'collection_dir': args.collection_dir,
                        'briefing_dir': args.briefing_dir,
                        'zone_name': args.zone_name,
                        'zone_country': args.zone_country,
                        'max_runs': args.max_runs,
                        'planned_commands': planned_commands,
                        'steps': [
                            {
                                'step_index': 0,
                                'command': planned_commands[0],
                                'status': 'completed',
                                'started_at_utc': '2026-03-27T12:00:10Z',
                                'ended_at_utc': '2026-03-27T12:00:20Z',
                                'exit_code': 0,
                                'notes': '',
                            },
                            {
                                'step_index': 1,
                                'command': planned_commands[1],
                                'status': 'failed',
                                'started_at_utc': '2026-03-27T12:00:30Z',
                                'ended_at_utc': '2026-03-27T12:00:40Z',
                                'exit_code': 5,
                                'notes': '',
                            },
                            {
                                'step_index': 2,
                                'command': planned_commands[2],
                                'status': 'pending',
                                'started_at_utc': '',
                                'ended_at_utc': '',
                                'exit_code': '',
                                'notes': '',
                            },
                        ],
                        'resume_mode': 'new',
                        'resume_step_index': 1,
                        'next_step_index': 1,
                        'completed_steps': 1,
                        'failed_steps': [1],
                        'running_steps': [],
                        'current_step_index': 1,
                        'failure_reason': 'command failed: bootstrap.py --verification-sprint',
                    }
                )
                + '\n'
            )

            fake_processes = [
                FakeProcess(['verify ok'], 0),
                FakeProcess(['brief ok'], 0),
            ]
            recorded_commands: list[list[str]] = []

            def fake_popen(command, cwd=None, stdout=None, stderr=None, text=None, bufsize=None):  # noqa: ANN001
                recorded_commands.append(command)
                return fake_processes[len(recorded_commands) - 1]

            with patch.object(operating_cycle, 'parse_args', return_value=args), patch.object(
                operating_cycle.subprocess, 'Popen', side_effect=fake_popen
            ):
                exit_code = operating_cycle.main()

            self.assertEqual(exit_code, 0)
            self.assertEqual(len(recorded_commands), 2)

            manifest = json.loads(manifest_path.read_text())
            self.assertEqual(manifest['status'], 'ok')
            self.assertEqual(manifest['resume_mode'], 'explicit')
            self.assertEqual(manifest['resume_step_index'], 1)
            self.assertEqual(manifest['completed_steps'], 3)
            self.assertEqual(manifest['next_step_index'], 3)
            self.assertEqual(manifest['steps'][0]['status'], 'completed')
            self.assertEqual(manifest['steps'][1]['status'], 'completed')
            self.assertEqual(manifest['steps'][2]['status'], 'completed')


if __name__ == '__main__':
    unittest.main()
