# The Ledger: Architectural Design & Tradeoffs

This document explains the architectural choices behind The Ledger and, more importantly, the tradeoffs those choices create. The design is intentionally event-sourced and audit-oriented: the write model protects business invariants, the read model optimizes regulatory and operational queries, and the surrounding infrastructure favors traceability over convenience.

## 1. Data Boundary Decisions
Our system enforces a strict boundary between the read-only **Applicant Registry** and the append-only **Event Store**. Historical data (like 3-year financial histories and past compliance flags) lives in the Applicant Registry because it represents state that existed *prior* to the application lifecycle. Copying this historical data into our event streams would violate the Single Source of Truth principle. Instead, the Event Store focuses purely on the immutability of the *decisioning process*. By treating the external registry as a read-only dependency, our agents can reference historical context without bloating the event streams or risking data duplication.

## 2. Aggregate Boundary Justification
`ComplianceRecord` is intentionally modeled as a separate aggregate from `LoanApplication`.

- **Alternative rejected:** embedding compliance rule execution directly into the `LoanApplication` stream.
- **Tradeoff:** a single aggregate would simplify read-side reconstruction of the full application story, but it would also force compliance rule fan-out, fraud results, and decision transitions to share one optimistic-concurrency cursor.
- **Failure mode:** the `ComplianceAgent` evaluates multiple deterministic rules in rapid succession while `FraudDetectionAgent` and `CreditAnalysisAgent` run concurrently. If all of those writes targeted `loan-{id}`, they would collide constantly, causing `OptimisticConcurrencyError` retries on business-critical lifecycle transitions.
- **Decision:** separating `ComplianceRecord` isolates high-frequency compliance writes into their own stream and prevents that write amplification from degrading decision latency on the main loan stream.

## 3. Week 3 Integration Architecture
The `DocumentProcessingAgent` bridges the filesystem-facing ingestion layer and the event-sourced decisioning pipeline.
- **Handling partial extractions:** when the extraction pipeline cannot recover a field such as EBITDA, the agent records the partial result in `ExtractionCompleted` and marks the gap in `data_quality_caveats` rather than failing the workflow.
- **Downstream impact:** the `CreditAnalysisAgent` consumes those caveats explicitly and lowers confidence instead of pretending the data is clean. That preserves forward progress while still surfacing uncertainty to the orchestrator and, if needed, to a human reviewer.

## 4. LangGraph Prompt Design — CreditAnalysisAgent
- **Initial prompt:** the first version passed financial facts to Claude and requested a credit decision in free form. The result was overly verbose, occasionally hallucinated metrics not present in the extracted facts, and did not reliably stay inside the allowed `LOW` / `MEDIUM` / `HIGH` risk tiers.
- **Refined prompt:** the final version constrains the response to an explicit output schema, requires rationale before the final risk tier, and encodes non-negotiable policy rules such as “if liabilities exceed assets, output `HIGH` risk.” The tradeoff is a more rigid prompt, but the benefit is materially better determinism and fewer parsing failures.

## 5. Agent Failure Modes and Recovery
The system handles transient failures such as LLM timeouts or unexpected pod restarts with the **Gas Town** persistent memory pattern.
- **Recovery behavior:** if an agent crashes mid-session, the replacement worker calls `reconstruct_agent_context()` before doing new work. The session stream is replayed, prior node execution is summarized, the last important events are preserved verbatim, and incomplete work is surfaced as pending reconciliation.
- **Tradeoff:** replaying the session adds recovery overhead, but it prevents duplicate model calls, preserves auditability, and avoids losing partially completed reasoning steps.

## 6. What I Would Do Differently
If given another full day, the single architectural decision I would revisit first is the **async projection daemon**.

The current daemon uses a timer-driven `asyncio` polling loop against the `events` table. That is acceptable for the scope of this exercise, but it creates a standing tradeoff between freshness and database load: lower polling intervals improve lag but waste queries when the system is idle, while higher intervals reduce load but increase stale-read windows.

The preferred redesign would use PostgreSQL `LISTEN/NOTIFY` or a comparable push-based subscription mechanism. Appends would emit a wake-up signal on commit, letting projection workers react immediately instead of polling. The likely outcome would be lower idle overhead, simpler lag tuning, and materially better real-time behavior.

## 7. Phase 1 Schema Contract Justification

### `events`
- `event_id`: immutable event identity for audit, outbox linkage, and external references.
- `stream_id`: aggregate stream partition key; every replay and OCC check begins here.
- `stream_position`: per-stream ordering cursor; required for deterministic aggregate replay.
- `global_position`: monotonic cross-stream ordering for projections and catch-up daemons.
- `event_type`: dispatch key for deserialization, upcasting, and projection routing.
- `event_version`: supports schema evolution and upcasters without rewriting history.
- `payload`: domain facts; immutable business content of the event.
- `metadata`: correlation/causation IDs, tracing, and operational annotations that are not domain facts.
- `recorded_at`: tamper-evident temporal ordering and audit/reporting support.
- `uq_stream_position`: prevents duplicate positions inside one stream and enforces append-only ordering.
- `idx_events_stream_position`: accelerates stream replay.
- `idx_events_global_position`: accelerates projection catch-up and rebuild.
- `idx_events_event_type`: accelerates filtered replays and analytics by event type.
- `idx_events_recorded_at`: supports time-window audits and lag analysis.
- `ck_events_stream_position_nonnegative`: guards invalid cursors before they poison replay.
- `ck_events_event_version_positive`: prevents nonsensical schema version values.

### `event_streams`
- `stream_id`: canonical lifecycle row for each aggregate stream.
- `aggregate_type`: allows stream introspection, diagnostics, and selective maintenance.
- `current_version`: authoritative optimistic-concurrency cursor.
- `created_at`: lifecycle trace for operational support and retention policies.
- `archived_at`: explicit read-only terminal state to prevent future writes.
- `metadata`: stream-level tags such as application identifiers or category labels.
- `ck_event_streams_current_version_nonnegative`: prevents invalid OCC cursors.
- `idx_event_streams_aggregate_type`: supports maintenance and analytics by aggregate family.

### `projection_checkpoints`
- `projection_name`: stable identity for each projection worker.
- `last_position`: replay cursor so read models can resume after crashes.
- `updated_at`: operator visibility into projection freshness and staleness.
- `ck_projection_checkpoints_last_position_nonnegative`: prevents invalid replay resumes.

### `outbox`
- `id`: unique delivery-attempt identity independent of the source event.
- `event_id`: ties external publication back to the immutable event row.
- `destination`: supports multiple downstream sinks without hard-coding routing.
- `payload`: publishable envelope captured in the same transaction as the event.
- `created_at`: supports FIFO-ish dispatching, observability, and stuck-message detection.
- `published_at`: marks successful dispatch and enables unpublished scans.
- `attempts`: retry counter for resilient delivery.
- `idx_outbox_unpublished`: hot-path index for unpublished messages.
- `idx_outbox_destination_published`: supports future destination-partitioned publishers and backlog scans.
- `ck_outbox_attempts_nonnegative`: guards retry-counter corruption.

## 8. Future Schema Validity Hardening
- Consider making `projection_checkpoints.last_position` nullable or defaulting to `-1` if you need to distinguish “never processed” from “processed global position 0”.
- Consider a delivery-status enum or dead-letter columns in `outbox` if external publishing becomes multi-stage.
- Consider explicit retention/partitioning on `events.recorded_at` once the ledger reaches long-lived production volumes.
- Consider `LISTEN/NOTIFY` alongside the outbox to reduce polling on projection consumers.

## 9. Phase 3 Projection Strategy

### Compliance Snapshot Strategy
- `ComplianceAuditView` stores both a current row and an append-only history row for every compliance-affecting event. The current table serves low-latency reads for the MCP server and reporting endpoints, while the history table preserves the exact temporal state needed for “what did we know at time T?” audit queries.
- Each history snapshot carries the effective regulation-set version plus per-rule `rule_version` metadata. That means an auditor can reconstruct not only the verdict, but also which concrete rule set produced the verdict.
- `rebuild_from_scratch()` writes into shadow tables and swaps them in a single transaction. This preserves live reads during rebuilds and avoids exposing partially rebuilt compliance state.

### Projection SLOs
- `ApplicationSummary` targets less than 500 ms lag from the latest committed event to the projected row under normal concurrent command load.
- `AgentPerformanceLedger` targets less than 500 ms lag because it is derived from the same agent-session events that drive orchestration visibility.
- `ComplianceAuditView` allows up to 2 seconds of lag because it writes both current and historical snapshots and therefore does more work per event than the summary projection.
- `ProjectionDaemon` exposes both fleet-wide lag and per-projection lag so these SLOs can be asserted directly in tests and monitored in production.

## 10. Phase 4 Upcasting and Integrity

### Read-Time Upcasting Strategy
- Upcasters run only on the event loading path. Stored rows in `events` remain immutable; callers never invoke migration helpers manually.
- `CreditAnalysisCompleted v1→v2` infers `model_version` and `regulatory_basis` from `recorded_at` cutover dates. This is an explicit historical-compatibility heuristic, not a fabricated claim about hidden data. We use the timestamp because deployment eras and policy-set activation dates are operational facts we can reconstruct deterministically.
- `confidence_score` is intentionally set to `null` for legacy credit events. The older payloads do not store a separate score, and fabricating one from the decision body would create false precision in an audit trail. In this ledger, `null` is more honest than inventing a value.

### Decision Lineage Reconstruction
- `DecisionGenerated v1→v2` reconstructs `model_versions` by loading the referenced agent-session streams and reading each session's `AgentSessionStarted` event after the `AgentContextLoaded` bootstrap. That gives us the model lineage without mutating the original decision event.
- This adds extra store lookups during reads of historical v1 decision events. The tradeoff is acceptable because only old events need the migration, while newer v2 decisions already carry `model_versions` inline.

### Cryptographic Audit Chain
- `run_integrity_check()` replays the prior `AuditIntegrityCheckRun` sequence against the current primary-stream history before appending a new integrity event. That means a later run can detect tampering even if the modified event was already covered by an earlier check.
- Each new integrity event records both the previous chain head and the newly computed chain head, producing a tamper-evident sequence over time rather than a one-off checksum.

## 11. Assessment Rubric Responses

The sections below answer the assessment prompts directly and are intended to be read as the formal tradeoff analysis for the implementation.

### Aggregate Boundary Justification
- `ComplianceRecord` remains separate from `LoanApplication` because compliance emits multiple fine-grained rule events (`ComplianceRulePassed`, `ComplianceRuleFailed`, `ComplianceRuleNoted`) at much higher frequency than the loan aggregate’s lifecycle transitions.
- If both lived in the same stream, the `ComplianceAgent`, `FraudDetectionAgent`, and `DecisionOrchestrator` would all compete for the same `loan-{id}` optimistic-concurrency cursor. The concrete failure mode is a compliance rule event winning the stream race while a fraud or decision transition is appending, causing `OptimisticConcurrencyError` on the losing write and forcing the caller to rehydrate and retry.
- Under concurrent execution, that would couple deterministic compliance rule fan-out to user-visible loan-state latency. The result is that a burst of compliance rule writes could starve the main lifecycle stream and delay `DecisionRequested`, `DecisionGenerated`, or `ApplicationApproved` even though those are logically separate responsibilities.

### Projection Strategy
- `ApplicationSummary`: async projection, not inline. This view is read on every MCP resource call, but computing it inline on each write would couple command latency to query-shape complexity. The SLO commitment is less than 500 ms lag under normal concurrent command load.
- `AgentPerformanceLedger`: async projection, not inline. It is operational telemetry rather than a write-time invariant, so eventual consistency is acceptable. The SLO commitment is less than 500 ms lag because operators use it for near-real-time session visibility.
- `ComplianceAuditView`: async projection with temporal history. It is intentionally not inline because every compliance-affecting event writes both a current row and a historical row, and forcing that into command latency would penalize the write path. The SLO commitment is less than or equal to 2 seconds.
- Temporal query snapshot strategy: this implementation uses per-event historical snapshots rather than an event-count or time-trigger batch snapshot. Every compliance-affecting event appends one immutable history row, so the temporal query can answer “state as of T” without reconstructing on demand.
- Snapshot invalidation logic: there is no cache-style invalidation window. The current row is replaced atomically on every new compliance event, the history table is append-only, and a full rebuild invalidates the materialized state only at the table level by shadow-table swap.
- Inline vs. async tradeoff summary: inline projections would reduce staleness but would also move read-model cost into the write path, which is unacceptable for the compliance and reporting workload. The chosen design pays for eventual consistency in exchange for faster command handling and simpler write-side invariants.

### Concurrency Analysis
- Peak-load assumption: 100 concurrent applications with 4 agents each does not mean 400 writers contend on the same stream. Most high-volume writes land on separate streams (`credit-{id}`, `fraud-{id}`, `compliance-{id}`, `agent-*`). The `loan-{id}` stream mainly receives lifecycle transitions.
- Expected `OptimisticConcurrencyError` rate on `loan-{id}` streams is low-single-digit per minute across the whole fleet, not per stream. With the current aggregate split, the dominant collision window is when two downstream transitions race to advance the loan lifecycle after analysis completes. A reasonable planning estimate is about 5–10 OCC conflicts per minute across 100 active applications, or less than 0.1 OCC/minute per hot loan stream.
- Retry strategy: the LangGraph agent layer uses exponential backoff with `MAX_OCC_RETRIES = 5` in [ledger/agents/base_agent.py](ledger/agents/base_agent.py). Backoff starts at 100 ms and doubles per attempt.
- Maximum retry budget before surfacing failure: 5 total attempts. After the fifth failed compare-and-swap, the caller receives the failure and must reload stream state before retrying at a higher level.

### Upcasting Inference Decisions
- `CreditAnalysisCompleted.model_version`: inferred from `recorded_at` deployment eras. Expected error rate is low but non-zero, mainly around deployment cutover boundaries; the downstream consequence of a bad inference is incorrect attribution in audit or analytics, not a wrong loan decision, because business rules read the already-materialized decision body rather than the inferred model label.
- `CreditAnalysisCompleted.regulatory_basis`: inferred from the rule set active at the event timestamp. Expected error rate is moderate around policy rollout windows if historical deployment metadata is incomplete. The downstream consequence is an audit-trace discrepancy, so the inference is acceptable only because it is explainable and bounded by date ranges.
- `CreditAnalysisCompleted.confidence_score`: set to `null` rather than inferred. The likely error rate of reconstructing it from adjacent fields would be unacceptably high because there is no faithful one-to-one mapping from the legacy payload to a separate score. Here `null` is better than inference because a fabricated score would look precise and could mislead regulators.
- `DecisionGenerated.model_versions`: reconstructed from referenced agent-session streams. Expected error rate is low when the contributing sessions exist and were started correctly; the main failure mode is incomplete lineage if a historical session stream is missing. The downstream consequence is incomplete provenance rather than a mutated business fact, so partial reconstruction is acceptable.
- General rule: choose `null` when the missing field is not derivable from immutable operational facts. Choose inference only when the missing value can be reconstructed from independent evidence such as timestamps, deployment eras, or referenced session streams.

### EventStoreDB Comparison
- PostgreSQL `stream_id` maps directly to EventStoreDB stream IDs.
- `load_all()` maps to EventStoreDB’s `$all` stream subscription: both give globally ordered replay across every stream.
- `ProjectionDaemon` maps to EventStoreDB persistent subscriptions: both maintain a replay cursor and advance read models independently of the write path.
- What EventStoreDB gives natively that this PostgreSQL implementation must build manually: durable catch-up subscriptions, consumer-group semantics, server-managed checkpointing, stream metadata primitives, and a purpose-built append/read engine tuned for event workloads.
- In this repo, those same capabilities are assembled from application code plus tables like `projection_checkpoints`, explicit OCC logic, a polling daemon, and custom upcasting. The tradeoff is portability and inspectability versus more operational work.

### What I Would Do Differently
- The single most significant architectural decision I would reconsider is the polling-based projection daemon.
- It works for this exercise, but it forces me to spend design energy on lag budgets, replay polling cadence, and load-vs-freshness tuning that EventStoreDB or a `LISTEN/NOTIFY` design would reduce substantially.
- With another full day, I would replace timer-driven polling with database-driven wakeups plus a more explicit subscription contract. That would simplify the projection SLO story, reduce idle query load, and make the bonus what-if and examination-package features less dependent on catch-up timing behavior.
- This is the clearest example of the difference between “what was sufficient to build” and “what would be strongest in production.” The current design is correct, but it is not yet the most operationally elegant version of the system.