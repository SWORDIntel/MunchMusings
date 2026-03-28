# Work Queue

_Generated: 2026-03-28T21:29:26Z._

## Purpose
- Keep milestone, recent-accounting, verification, and external-execution tasks in one operator queue.
- Treat `EXT-*` rows as the primary staged-external action surface and `ACC-RA-*` rows as the recency/accounting lane.

## Snapshot
- blocked: 1
- completed: 3
- in_progress: 2
- pending: 9

## Active Tasks
- `ACC-003` | in_progress | Egypt | 
- `ACC-006` | in_progress | Israel | 
- `EXT-011` | blocked | seed-11 | Configure a bounded API key, quota limits, and field masks before live collection.
- `ACC-004` | pending | Egypt | 
- `ACC-005` | pending | Egypt | 
- `ACC-RA-033` | pending | seed-33 | Track the Ashdod financial-information hub and the newest linked presentation to capture future releases.
- `EXT-012` | pending | seed-12 | Keep Overpass queries bounded and monitor endpoint policy, latency, and query failures.
- `EXT-013` | pending | seed-13 | Capture the pinned page or export surface, retain the raw artifact, and record the visible publication date and coverage note.
- `EXT-017` | pending | seed-17 | Keep Overpass queries bounded and monitor endpoint policy, latency, and query failures.
- `EXT-018` | pending | seed-18 | Capture the pinned page or export surface, retain the raw artifact, and record the visible publication date and coverage note.

## Rerun
- `python bootstrap.py --verification-sprint` refreshes queue-derived lanes, staged contract metadata, and summary artifacts.
