from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any

from fastmcp import FastMCP
from pydantic import ValidationError

from ledger.commands.handlers import (
    ComplianceCheckCompletedCommand,
    CreditAnalysisCompletedCommand,
    DecisionGeneratedCommand,
    FraudScreeningCompletedCommand,
    HumanReviewCompletedCommand,
    SubmitApplicationCommand,
    handle_compliance_check_completed,
    handle_credit_analysis_completed,
    handle_decision_generated,
    handle_fraud_screening_completed,
    handle_human_review_completed,
    handle_submit_application,
)
from ledger.domain.errors import DomainError
from ledger.event_store import EventStore, OptimisticConcurrencyError, assigned_positions_from_new_version
from ledger.integrity import run_integrity_check as run_integrity_check_impl
from ledger.projections import (
    AgentPerformanceLedger,
    ApplicationSummary,
    ComplianceAuditView,
    ProjectionDaemon,
)
from ledger.schema.events import AgentContextLoaded, AgentSessionStarted, AgentType


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if hasattr(value, "hex"):
        return str(value)
    if hasattr(value, "to_dict"):
        return value.to_dict()
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _json_response(payload: Any) -> str:
    return json.dumps(payload, default=_json_default)


def _structured_error(exc: Exception) -> str:
    suggested_action = "inspect_server_logs"
    if isinstance(exc, OptimisticConcurrencyError):
        suggested_action = "reload_stream_and_retry"
    elif isinstance(exc, DomainError):
        suggested_action = "check_domain_preconditions"
    elif isinstance(exc, ValidationError):
        suggested_action = "fix_tool_arguments"
    elif isinstance(exc, ValueError):
        suggested_action = "correct_request_payload"

    return _json_response(
        {
            "error_type": type(exc).__name__,
            "message": str(exc),
            "suggested_action": suggested_action,
        }
    )


def _event_to_dict(event: Any) -> dict[str, Any]:
    if hasattr(event, "to_dict"):
        return event.to_dict()
    if isinstance(event, dict):
        return dict(event)
    return dict(event)


@dataclass
class LedgerMCPContext:
    store: Any
    application_summary: ApplicationSummary
    compliance_audit: ComplianceAuditView
    agent_performance: AgentPerformanceLedger
    daemon: ProjectionDaemon
    _owns_store: bool = False
    _ready: bool = False

    async def ensure_ready(self) -> None:
        if self._ready:
            return
        if self._owns_store and getattr(self.store, "_pool", None) is None:
            await self.store.connect()
        await self.application_summary.setup()
        await self.compliance_audit.setup()
        await self.agent_performance.setup()
        self._ready = True

    async def refresh_projections(self) -> None:
        await self.ensure_ready()
        await self.daemon.run_once()


def build_context(
    *,
    store: Any | None = None,
    application_summary: ApplicationSummary | None = None,
    compliance_audit: ComplianceAuditView | None = None,
    agent_performance: AgentPerformanceLedger | None = None,
    daemon: ProjectionDaemon | None = None,
    db_url: str | None = None,
) -> LedgerMCPContext:
    owns_store = store is None
    resolved_store = store or EventStore(db_url or os.environ.get("DATABASE_URL", "postgresql://localhost/apex_ledger"))
    application_summary = application_summary or ApplicationSummary(resolved_store)
    compliance_audit = compliance_audit or ComplianceAuditView(resolved_store)
    agent_performance = agent_performance or AgentPerformanceLedger(resolved_store)
    daemon = daemon or ProjectionDaemon(
        resolved_store,
        [application_summary, compliance_audit, agent_performance],
    )
    return LedgerMCPContext(
        store=resolved_store,
        application_summary=application_summary,
        compliance_audit=compliance_audit,
        agent_performance=agent_performance,
        daemon=daemon,
        _owns_store=owns_store,
    )


def create_mcp_server(
    *,
    store: Any | None = None,
    application_summary: ApplicationSummary | None = None,
    compliance_audit: ComplianceAuditView | None = None,
    agent_performance: AgentPerformanceLedger | None = None,
    daemon: ProjectionDaemon | None = None,
    db_url: str | None = None,
) -> FastMCP:
    context = build_context(
        store=store,
        application_summary=application_summary,
        compliance_audit=compliance_audit,
        agent_performance=agent_performance,
        daemon=daemon,
        db_url=db_url,
    )

    mcp = FastMCP(
        "Apex Ledger MCP",
        instructions=(
            "Command/query surface for the Apex Ledger event store and CQRS projections. "
            "Write tools return structured JSON strings and recoverable errors as JSON."
        ),
    )

    async def _run_command(command_model: Any, handler, payload: dict[str, Any]) -> str:
        try:
            await context.ensure_ready()
            command = command_model.model_validate(payload)
            positions = await handler(context.store, command)
            await context.refresh_projections()
            return _json_response({"status": "ok", "stream_positions": positions})
        except (OptimisticConcurrencyError, DomainError, ValidationError, ValueError, RuntimeError) as exc:
            return _structured_error(exc)

    @mcp.tool()
    async def submit_application(command: dict[str, Any]) -> str:
        """Submit a new loan application and request the initial document package.

        Preconditions:
        - The `application_id` must not already have been submitted.
        - The payload must include all fields required by the Phase 2 submit-application command.
        - Use this tool before any downstream credit, fraud, compliance, or decision commands.
        """

        return await _run_command(SubmitApplicationCommand, handle_submit_application, command)

    @mcp.tool()
    async def record_credit_analysis(command: dict[str, Any]) -> str:
        """Advance a loan application after credit analysis completes.

        Preconditions:
        - The loan application must already exist and be ready for credit completion handling.
        - If `session_id` is provided, it must reference an active agent session created by `start_agent_session`.
        - The referenced credit-analysis result event must already exist in the credit stream.
        """

        return await _run_command(CreditAnalysisCompletedCommand, handle_credit_analysis_completed, command)

    @mcp.tool()
    async def record_fraud_screening(command: dict[str, Any]) -> str:
        """Advance a loan application after fraud screening completes.

        Preconditions:
        - The loan application must already be awaiting the fraud-screening transition.
        - The `triggered_by_event_id` must refer to the event that completed fraud screening.
        - Call this only after the fraud result has already been appended to its own aggregate stream.
        """

        return await _run_command(FraudScreeningCompletedCommand, handle_fraud_screening_completed, command)

    @mcp.tool()
    async def record_compliance_check(command: dict[str, Any]) -> str:
        """Advance a loan application after compliance evaluation completes.

        Preconditions:
        - The loan application must already be at the compliance-complete transition point.
        - `has_hard_block` must reflect the finalized compliance verdict.
        - If the application was blocked, provide decline/adverse-action details for audit completeness.
        """

        return await _run_command(ComplianceCheckCompletedCommand, handle_compliance_check_completed, command)

    @mcp.tool()
    async def generate_decision(command: dict[str, Any]) -> str:
        """Persist the orchestrator decision for an application.

        Preconditions:
        - The loan application must be pending decision.
        - `orchestrator_session_id` should come from an active session created by `start_agent_session`.
        - Confidence, rationale, and contributing-session metadata should be finalized before calling.
        """

        return await _run_command(DecisionGeneratedCommand, handle_decision_generated, command)

    @mcp.tool()
    async def record_human_review(command: dict[str, Any]) -> str:
        """Complete human review and write the final approval or decline outcome.

        Preconditions:
        - A machine decision must already have been generated for the application.
        - Use this tool only when a human reviewer has finalized an override or confirmation.
        - Approval payloads must include final pricing/term fields; decline payloads must include reasons.
        """

        return await _run_command(HumanReviewCompletedCommand, handle_human_review_completed, command)

    @mcp.tool()
    async def start_agent_session(command: dict[str, Any]) -> str:
        """Create an active agent session stream for later tool-driven workflow steps.

        Preconditions:
        - `session_id` must be globally unique for the given `agent_type`.
        - The target application should already exist in the loan stream.
        - Tools that reference `session_id` assume this session has been started successfully first.
        """

        try:
            await context.ensure_ready()
            loaded_at = command["started_at"]
            if isinstance(loaded_at, str):
                loaded_at = datetime.fromisoformat(loaded_at)
            start_event = AgentSessionStarted.model_validate(command).to_store_dict()
            agent_type = command.get("agent_type")
            if isinstance(agent_type, AgentType):
                agent_type = agent_type.value
            stream_id = f"agent-{agent_type}-{command['session_id']}"
            expected_version = await context.store.stream_version(stream_id)
            if expected_version != -1:
                raise DomainError(f"Agent session '{command['session_id']}' already exists for {agent_type}")
            context_loaded_event = AgentContextLoaded(
                session_id=command["session_id"],
                agent_type=command["agent_type"],
                application_id=command["application_id"],
                context_source=command["context_source"],
                context_token_count=command["context_token_count"],
                loaded_at=loaded_at,
            ).to_store_dict()
            new_version = await context.store.append(stream_id, [context_loaded_event, start_event], expected_version=-1)
            await context.refresh_projections()
            return _json_response(
                {
                    "status": "ok",
                    "stream_id": stream_id,
                    "stream_positions": assigned_positions_from_new_version(new_version, 2),
                }
            )
        except (OptimisticConcurrencyError, DomainError, ValidationError, ValueError, RuntimeError) as exc:
            return _structured_error(exc)

    @mcp.tool(name="run_integrity_check")
    async def run_integrity_check_tool(entity_type: str, entity_id: str) -> str:
        """Run the tamper-evident audit hash-chain verification for one entity.

        Preconditions:
        - The primary stream `{entity_type}-{entity_id}` must already exist.
        - Use this after write-side activity when you need a new integrity checkpoint.
        - The audit stream is append-only; this tool never mutates historical event rows.
        """

        try:
            await context.ensure_ready()
            result = await run_integrity_check_impl(context.store, entity_type, entity_id)
            return _json_response(result)
        except (OptimisticConcurrencyError, DomainError, ValidationError, ValueError, RuntimeError) as exc:
            return _structured_error(exc)

    @mcp.resource("ledger://applications/{id}")
    async def application_resource(id: str) -> str:
        await context.refresh_projections()
        return _json_response(await context.application_summary.get(id))

    @mcp.resource("ledger://applications/{id}/compliance")
    async def application_compliance_resource(id: str) -> str:
        await context.refresh_projections()
        return _json_response(await context.compliance_audit.get_current(id))

    @mcp.resource("ledger://applications/{id}/compliance?as_of={as_of}")
    async def application_compliance_temporal_resource(id: str, as_of: str) -> str:
        await context.refresh_projections()
        return _json_response(await context.compliance_audit.get_compliance_at(id, datetime.fromisoformat(as_of)))

    @mcp.resource("ledger://applications/{id}/audit-trail")
    async def audit_trail_resource(id: str) -> str:
        await context.ensure_ready()
        events = await context.store.load_stream(f"audit-loan-{id}")
        return _json_response([_event_to_dict(event) for event in events])

    @mcp.resource("ledger://agents/{id}/performance")
    async def agent_performance_resource(id: str) -> str:
        await context.refresh_projections()
        return _json_response(await context.agent_performance.list_metrics(id))

    @mcp.resource("ledger://agents/{id}/performance?model_version={model_version}")
    async def agent_performance_by_model_resource(id: str, model_version: str) -> str:
        await context.refresh_projections()
        return _json_response(await context.agent_performance.get_metrics(id, model_version))

    @mcp.resource("ledger://agents/{id}/sessions/{session_id}")
    async def agent_session_resource(id: str, session_id: str) -> str:
        await context.ensure_ready()
        events = await context.store.load_stream(f"agent-{id}-{session_id}")
        return _json_response([_event_to_dict(event) for event in events])

    @mcp.resource("ledger://ledger/health")
    async def ledger_health_resource() -> str:
        await context.ensure_ready()
        return _json_response(await context.daemon.get_all_lags())

    mcp.ledger_context = context
    return mcp


mcp = create_mcp_server()


__all__ = ["LedgerMCPContext", "build_context", "create_mcp_server", "mcp"]