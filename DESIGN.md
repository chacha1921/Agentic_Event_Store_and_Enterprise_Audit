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