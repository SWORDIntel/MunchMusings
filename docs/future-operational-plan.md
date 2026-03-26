# Future Operational Plan

**Date:** 2026-03-26 (UTC)

## Background
The workspace is transitioning from planning scaffolds into a repeatable, evidence-grade OSINT pipeline whose rigor rivals lawful HUMINT/SIGINT workflows. The goal is to deliver weekly Egypt-and-Israel-focused briefings while preserving provenance, freshness, and analyst-review discipline for every claim. This plan builds on the Source Accounting Control Plane, Decision-Grade Evidence Pack, and high-rigor evidence pack documents already living in `docs/` and `plans/`.

## Strategic Objectives
- **High-Quality Current Data:** Lock down the strongest public sources (IPC, Ashdod port, Israel CBS, UN/HDX feeds, maritime shipping statistics, etc.) so every fact refers to an auditable, timestamped evidence link.
- **Data Collection & Pipeline Beyond Parsing:** Enhance raw capture, ingestion, and normalization so the sources feed into durable artifacts instead of one-off CSV notes. That includes TUI-driven pipeline management, collection orchestration, normalized evidence layering, and quality-check automation.
- **Multi-Agent Orchestration:** Always coordinate multiple agents (research, analytics, pipeline engineering) running in parallel to reduce latency between discovery, verification, and insight production.
- **Rerunnable Operating Cycle:** Keep the launcher/TUI/pipelines rerunnable; every action (bootstrap, collection, verification, briefing) should succeed on repeated runs without manual intervention or brittle state.
- **Reporting & Evidence Packs:** Synthesize the ledger into zone briefs (Egypt + Israel) that mirror intelligence-cycle outputs: ledger, baseline events, anomalies, claim packets, evidence appendices, and reviewer decisions.

## Phase 1 – Source and Evidence Confirmation
1. Dispatch a **Research Agent** to survey and validate the freshest public data sources: humanitarian baselines (UNHCR, OCHA, IPC), macro-price archives (FAO, CBS, CAS, GASTAT), shipping/chokepoint feeds (Suez Canal Authority, Ashdod Port, UNCTAD, shipping-stat aggregators, Israeli trade stats), and HDX signals. Expand the source ledger with exact publication dates, evidence links, and priority tiers.
2. Use two parallel **Data Analysis Agents** to confirm existing fixtures (IPC/Ashdod) and extract metadata for new sources, feeding updates into `plans/recent_accounting.csv`, `plans/source_verification_sprint.csv`, and `plans/work_queue.csv`. They should also identify additional shipping sites/statistics and Israel angles, using a lightweight crawler (e.g., Katana) only on lawful sites.
3. Log every action in `plans/llm_context_summary.md` (or its successors) so future operators can pick up the chain of custody and the current blockers.

## Phase 2 – Pipeline, TUI, and Evidence Infrastructure
1. Launch a **Pipeline Engineering Agent** to upgrade the management TUI so it can orchestrate the pipeline start-to-finish: queue creation, source assignment, evidence tracking, failure detection, and rerunnable executions (`python scripts/run_operating_cycle.py` or `bootstrap.py --operating-cycle`).
2. Keep collection, normalization, and aggregation pipelines synchronized with the Ledger (freshness statuses, due-now/overdue transitions, evidence links). Ensure the adapters writing to `artifacts/collection/raw/`, `artifacts/collection/normalized/`, and evidence logs are deterministic and idempotent.
3. Add instrumentation (e.g., new fixtures or manifests) to prove the pipeline remains rerunnable even after interruptions like drive remounts or network issues.

## Phase 3 – Reporting & Evidence Packs
1. Compile the decision-grade evidence pack for Cairo/Giza (and then replicate for Israel) by combining:
   - verified ledger rows for tier-1 sources,
   - event-baseline files (humanitarian, market, maritime),
   - district observation snapshots,
   - anomaly cards with confounds and scores,
   - claim register with evidence links,
   - review decision notes with reviewer assignments.
2. Present the pack as an intelligence-style briefing that could not look out of place in HUMINT/SIGINT windows: short factual statements, UTC timestamps, explicit confound scores, and a review decision.
3. Publish the pack artifacts under `artifacts/briefings/` with matching `zone_brief.md`, `source_observation_log.csv`, and `aggregated_signals.csv`.

## Agent Coordination Details
- Always spawn **three** agents per planning cycle:
  1. **Research Agent** – Chrome/HTTP crawler focused on new sources (shipping stats, Israel trade data, humanitarian feeds). Use respectful, law-abiding crawlers; Katana may be used for broad discovery but avoid blocked or login-required endpoints.
  2. **Analysis Agent** – Improves data quality by verifying metadata, refreshing ledger entries, and reconciling evidence links with normalized outputs.
  3. **Pipeline Agent** – Enhances pipelines/TUI, ensures rerunnability, and orchestrates the data capture/aggregration flows.
- Agents must report progress into `plans/llm_context_summary.md` (or future aggregator) and hand off intermediate artifacts for review.
- If an agent fails (e.g., apply_patch issues, network blocks), record the failure, adjust the plan, and retry with a fallback (manual edits, offline fixtures) while keeping the system rerunnable.

## Testing & Validation
- Install `pytest` in the environment and rerun `python -m pytest tests/test_bootstrap.py` to validate the IPC/Ashdod fixtures and adapters. Fix any quoting issues in the ledger and doc CSVs before rerunning.
- After pipeline updates, run `python bootstrap.py --recent-accounting`, `python bootstrap.py --collect-ready`, and the full `scripts/run_operating_cycle.py` wrapper to prove rerunnability.
- Use `artifacts/collection/evidence-capture-log.csv` and `artifacts/briefings/` artifacts to verify that data flows from collection to evidence packs without dropouts.

## References
- `docs/high-impact-upgrade-paths.md` and `docs/system-upgrade-options-2026-03-25.md` for strategic context.
- `docs/high-rigor-osint-evidence-pack-report.md` and `docs/zone-briefing-standards.md` for evidence/reporting expectations.
- `plans/recent_accounting.csv`, `plans/work_queue.csv`, and `plans/source_verification_sprint.csv` for operational inputs.
- `tests/fixtures/ipc-gaza-snapshot.html`, `tests/fixtures/ashdod-port-financial.html` for collector verification.
