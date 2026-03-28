# Plans

This folder turns the repo's strategy documents into accountable execution.

## Purpose
- Define agent roles, handoffs, and stop conditions.
- Keep a machine-readable work queue.
- Maintain a recent-accounting ledger that proves what has been checked, when, and against which source.

## Core Files
- `zone_brief_template.md`: fillable zone briefing structure.
- `review_decision_template.md`: operator decision record for approve/defer/downgrade/reject.
- `source_observation_log_template.csv`: normalized public-source observation log.
- `claim_register_template.csv`: claim registry with provenance and review state.
- `anomaly_card_template.csv`: anomaly scoring row for zone-level assessment.
- `agent_registry.md`: agent contracts and deliverables.
- `work_queue.csv`: execution backlog and dependencies.
- `work_queue.md`: human-readable queue summary for the active operator surface.
- `source_verification_findings.csv`: canonical analyst overlay for publication dates, coverage windows, and evidence links that should survive reruns.
- `source_verification_sprint.csv`: source-by-source verification sprint tracker for publication dates, coverage windows, and evidence links.
- `source_freshness_policy.md`: rules for `current`, `due_now`, `overdue`, `blocked`, and `manual_review`.
- `recent_accounting_schema.md`: field definitions for the recency ledger.
- `evidence_packet_template.json`: normalized review packet shape.
- `baseline_event_template.csv`: normalized humanitarian baseline rows.
- `phase_01_egypt_baseline.md`: first live accounting plan.
- `phase_02_market_proxy.md`: second execution plan after baseline readiness.
- `zone_evidence_pack_template.json`: machine-readable skeleton for the briefing pack.
- `collection_runbook.md`: operating sequence for lawful public-source collection.
- `normalization_spec.md`: shared record shapes for raw-to-normalized conversion.
- `collection_directory_contract.md`: required directories and file naming rules for `artifacts/collection/`.
- `evidence_loop_crosswalk.md`: mapping from normalized outputs to anomaly cards and claims.

## Briefing Templates
- `recent_accounting.csv`
- `recent_accounting.md`
- `zone_brief_template.md`
- `review_decision_template.md`
- `source_observation_log_template.csv`
- `claim_register_template.csv`
- `anomaly_card_template.csv`
- `collection_runbook.md`
- `normalization_spec.md`
- `collection_directory_contract.md`

Refresh those generated files with:

```bash
python bootstrap.py --recent-accounting
```

Refresh the rolling verification tracker with:

```bash
python bootstrap.py --verification-sprint
```

Drop newly researched dates and evidence into `source_verification_findings.csv`, or land machine-collected verification updates under `artifacts/collection/normalized/`, then rerun the sprint command to rebuild the ledger, sprint tracker, `plans/work_queue.csv`, `plans/work_queue.md`, and the derived `VER-*` / `EXT-*` queue rows from the updated findings.

## Operating Rule
If a claim does not have a source row, access timestamp, publication date, and next action in this folder, it is not recent enough to treat as accounted for.
