# The Ledger: Architectural Design & Tradeoffs

## 1. Data Boundary Decisions
Our system enforces a strict boundary between the read-only **Applicant Registry** and the append-only **Event Store**. Historical data (like 3-year financial histories and past compliance flags) lives in the Applicant Registry because it represents state that existed *prior* to the application lifecycle. Copying this historical data into our event streams would violate the Single Source of Truth principle. Instead, the Event Store focuses purely on the immutability of the *decisioning process*. By treating the external registry as a read-only dependency, our agents can reference historical context without bloating the event streams or risking data duplication.

## 2. Aggregate Boundary Justification
I designed `ComplianceRecord` as a separate aggregate from `LoanApplication`. 

* **The Alternative Rejected:** I initially considered embedding compliance rules directly into the `LoanApplication` stream. 
* **The Concurrency Problem:** If embedded, the `ComplianceAgent` (which evaluates 6 deterministic rules in rapid succession) would constantly clash with the `FraudDetectionAgent` and `CreditAnalysisAgent`, which run concurrently. This would result in severe write contention, leading to a high rate of `OptimisticConcurrencyError` exceptions and exhausting our retry budgets. Separating them isolates the high-frequency compliance events into their own stream, completely eliminating write contention on the main loan stream during the analysis phase.

## 3. Week 3 Integration Architecture
The `DocumentProcessingAgent` acts as a bridge between the filesystem and our Week 3 document extraction pipeline. 
* **Handling Partial Extractions:** If the pipeline fails to extract a specific field (e.g., missing EBITDA), the agent does not crash. Instead, it captures the partial data in the `ExtractionCompleted` event and appends a flag to the `data_quality_caveats` array.
* **Downstream Impact:** When the `CreditAnalysisAgent` loads this event, it explicitly reads the `data_quality_caveats`. Rather than failing the process, it uses this information to adjust its `confidence_score` downward, allowing the orchestrator to dynamically route the application for human review if the data quality is too poor.

## 4. LangGraph Prompt Design — CreditAnalysisAgent
* **Initial Prompt:** My first prompt simply passed the financial facts to Claude and asked for a credit decision. *Result:* It was too verbose, occasionally hallucinated metrics that weren't in the JSON, and failed to strictly adhere to the `MEDIUM`/`HIGH`/`LOW` risk tiers.
* **Refined Prompt:** I shifted to a highly structured prompt that explicitly defined the allowed output schema. I added a "Chain of Thought" requirement, forcing the LLM to output its `<rationale>` before declaring the final `<risk_tier>`. I also enforced a hard rule: "If total liabilities exceed assets, you MUST output HIGH risk." This improved determinism significantly and eliminated parsing errors in the output nodes.

## 5. Agent Failure Modes and Recovery
The system handles transient failures (like LLM timeouts or unexpected pod crashes) using the **"Gas Town"** persistent memory pattern.
* **NARR-03 Recovery:** If the `FraudDetectionAgent` crashes mid-session, all progress is not lost. When the retry mechanism spins up a new instance of the agent, the very first thing it does is call `reconstruct_agent_context()`. It replays its specific `AgentSession` stream from the Event Store, rebuilding its memory graph up to the exact node it died on. It then resumes execution without calling the Anthropic API for the steps it has already completed, saving both time and token costs.

## 6. What I Would Do Differently
If I had another week to build this, I would redesign the **Async Projection Daemon**. 
Currently, the daemon relies on a simple `asyncio` loop that aggressively polls the `events` table based on a timer. While effective for this scale, it introduces unnecessary database load and guarantees a base level of projection lag tied to the polling interval. 
* **Tradeoff Analysis:** I would replace the polling loop with PostgreSQL's `LISTEN/NOTIFY` functionality. The `append` function would issue a `NOTIFY` upon committing a transaction, instantly waking up the daemon. This would reduce idle database queries to zero while simultaneously dropping projection lag from ~500ms down to sub-50ms, ensuring our read models are as close to real-time as possible.

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