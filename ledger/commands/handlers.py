from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, Field

from ledger.domain.aggregates.agent_session import AgentSessionAggregate
from ledger.domain.aggregates.loan_application import LoanApplicationAggregate
from ledger.domain.errors import DomainError
from ledger.schema.events import (
    AgentType,
    ApplicationApproved,
    ApplicationDeclined,
    ApplicationSubmitted,
    ComplianceCheckRequested,
    CreditAnalysisRequested,
    DecisionGenerated,
    DecisionRequested,
    DocumentType,
    DocumentUploadRequested,
    FraudScreeningRequested,
    HumanReviewCompleted,
    HumanReviewRequested,
    LoanPurpose,
)


class TraceableCommand(BaseModel):
    correlation_id: str | None = None
    causation_id: str | None = None


class SubmitApplicationCommand(TraceableCommand):
    application_id: str
    applicant_id: str
    requested_amount_usd: Decimal
    loan_purpose: LoanPurpose
    loan_term_months: int
    submission_channel: str
    contact_email: str
    contact_name: str
    submitted_at: datetime
    application_reference: str
    required_document_types: list[DocumentType] = Field(
        default_factory=lambda: [
            DocumentType.APPLICATION_PROPOSAL,
            DocumentType.INCOME_STATEMENT,
            DocumentType.BALANCE_SHEET,
        ]
    )
    deadline: datetime
    requested_by: str = "system"


class CreditAnalysisCompletedCommand(TraceableCommand):
    application_id: str
    triggered_by_event_id: str
    requested_at: datetime
    session_id: str | None = None
    agent_type: AgentType = AgentType.CREDIT_ANALYSIS
    model_version: str | None = None


class FraudScreeningCompletedCommand(TraceableCommand):
    application_id: str
    triggered_by_event_id: str
    requested_at: datetime
    regulation_set_version: str = "2026-Q1"
    rules_to_evaluate: list[str] = Field(
        default_factory=lambda: ["REG-001", "REG-002", "REG-003", "REG-004", "REG-005", "REG-006"]
    )


class ComplianceCheckCompletedCommand(TraceableCommand):
    application_id: str
    triggered_by_event_id: str
    requested_at: datetime
    has_hard_block: bool
    decline_reasons: list[str] = Field(default_factory=list)
    adverse_action_codes: list[str] = Field(default_factory=list)


class DecisionGeneratedCommand(TraceableCommand):
    application_id: str
    orchestrator_session_id: str
    recommendation: str
    confidence: float
    executive_summary: str
    generated_at: datetime
    approved_amount_usd: Decimal | None = None
    conditions: list[str] = Field(default_factory=list)
    key_risks: list[str] = Field(default_factory=list)
    contributing_sessions: list[str] = Field(default_factory=list)
    model_versions: dict[str, str] = Field(default_factory=dict)
    assigned_to: str | None = None


class HumanReviewCompletedCommand(TraceableCommand):
    application_id: str
    reviewer_id: str
    override: bool
    original_recommendation: str
    final_decision: str
    reviewed_at: datetime
    override_reason: str | None = None
    approved_amount_usd: Decimal | None = None
    interest_rate_pct: float = 0.0
    term_months: int = 0
    conditions: list[str] = Field(default_factory=list)
    decline_reasons: list[str] = Field(default_factory=list)
    adverse_action_notice_required: bool = True
    adverse_action_codes: list[str] = Field(default_factory=list)


def _trace_kwargs(cmd: TraceableCommand) -> dict:
    return {
        "correlation_id": cmd.correlation_id,
        "causation_id": cmd.causation_id,
    }


async def handle_submit_application(store, cmd: SubmitApplicationCommand) -> list[int]:
    app = await LoanApplicationAggregate.load(store, cmd.application_id)
    app.assert_can_submit()

    submitted = ApplicationSubmitted(
        application_id=cmd.application_id,
        applicant_id=cmd.applicant_id,
        requested_amount_usd=cmd.requested_amount_usd,
        loan_purpose=cmd.loan_purpose,
        loan_term_months=cmd.loan_term_months,
        submission_channel=cmd.submission_channel,
        contact_email=cmd.contact_email,
        contact_name=cmd.contact_name,
        submitted_at=cmd.submitted_at,
        application_reference=cmd.application_reference,
    ).to_store_dict()
    docs_requested = DocumentUploadRequested(
        application_id=cmd.application_id,
        required_document_types=cmd.required_document_types,
        deadline=cmd.deadline,
        requested_by=cmd.requested_by,
    ).to_store_dict()

    return await store.append(
        f"loan-{cmd.application_id}",
        [submitted, docs_requested],
        expected_version=app.version,
        **_trace_kwargs(cmd),
    )


async def handle_credit_analysis_completed(store, cmd: CreditAnalysisCompletedCommand) -> list[int]:
    app = await LoanApplicationAggregate.load(store, cmd.application_id)
    await app.assert_credit_analysis_completion_ready(store)

    if cmd.session_id is not None:
        session = await AgentSessionAggregate.load(store, cmd.agent_type.value, cmd.session_id)
        session.assert_started()
        session.assert_model_version_locked(cmd.model_version)
        if session.application_id is not None and session.application_id != cmd.application_id:
            raise DomainError("Credit analysis session does not belong to this application")

    fraud_requested = FraudScreeningRequested(
        application_id=cmd.application_id,
        requested_at=cmd.requested_at,
        triggered_by_event_id=cmd.triggered_by_event_id,
    ).to_store_dict()
    return await store.append(
        f"loan-{cmd.application_id}",
        [fraud_requested],
        expected_version=app.version,
        **_trace_kwargs(cmd),
    )


async def handle_fraud_screening_completed(store, cmd: FraudScreeningCompletedCommand) -> list[int]:
    app = await LoanApplicationAggregate.load(store, cmd.application_id)
    package_events = await store.load_stream(f"docpkg-{cmd.application_id}")
    if any(event.get("event_type") == "PackageReadyForAnalysis" for event in package_events):
        app.mark_package_ready()
    app.assert_awaiting_credit_analysis()

    compliance_requested = ComplianceCheckRequested(
        application_id=cmd.application_id,
        requested_at=cmd.requested_at,
        triggered_by_event_id=cmd.triggered_by_event_id,
        regulation_set_version=cmd.regulation_set_version,
        rules_to_evaluate=cmd.rules_to_evaluate,
    ).to_store_dict()
    return await store.append(
        f"loan-{cmd.application_id}",
        [compliance_requested],
        expected_version=app.version,
        **_trace_kwargs(cmd),
    )


async def handle_compliance_check_completed(store, cmd: ComplianceCheckCompletedCommand) -> list[int]:
    app = await LoanApplicationAggregate.load(store, cmd.application_id)
    app.mark_compliance_completed(cmd.has_hard_block)

    if cmd.has_hard_block:
        decline = ApplicationDeclined(
            application_id=cmd.application_id,
            decline_reasons=cmd.decline_reasons or ["Compliance hard block"],
            declined_by="compliance-system",
            adverse_action_notice_required=True,
            adverse_action_codes=cmd.adverse_action_codes or ["COMPLIANCE_BLOCK"],
            declined_at=cmd.requested_at,
        ).to_store_dict()
        return await store.append(
            f"loan-{cmd.application_id}",
            [decline],
            expected_version=app.version,
            **_trace_kwargs(cmd),
        )

    decision_requested = DecisionRequested(
        application_id=cmd.application_id,
        requested_at=cmd.requested_at,
        all_analyses_complete=True,
        triggered_by_event_id=cmd.triggered_by_event_id,
    ).to_store_dict()
    return await store.append(
        f"loan-{cmd.application_id}",
        [decision_requested],
        expected_version=app.version,
        **_trace_kwargs(cmd),
    )


async def handle_decision_generated(store, cmd: DecisionGeneratedCommand) -> list[int]:
    app = await LoanApplicationAggregate.load(store, cmd.application_id)
    app.assert_pending_decision()

    recommendation = cmd.recommendation.upper()
    if cmd.confidence < 0.60:
        recommendation = "REFER"

    decision_event_model = DecisionGenerated(
        application_id=cmd.application_id,
        orchestrator_session_id=cmd.orchestrator_session_id,
        recommendation=recommendation,
        confidence=cmd.confidence,
        approved_amount_usd=cmd.approved_amount_usd,
        conditions=cmd.conditions,
        executive_summary=cmd.executive_summary,
        key_risks=cmd.key_risks,
        contributing_sessions=cmd.contributing_sessions,
        model_versions=cmd.model_versions,
        generated_at=cmd.generated_at,
    )
    decision_event = decision_event_model.to_store_dict()

    events = [decision_event]
    if recommendation == "REFER":
        review_requested = HumanReviewRequested(
            application_id=cmd.application_id,
            reason="Low confidence decision requires human review",
            decision_event_id=str(decision_event_model.event_id),
            assigned_to=cmd.assigned_to,
            requested_at=cmd.generated_at,
        ).to_store_dict()
        events.append(review_requested)

    return await store.append(
        f"loan-{cmd.application_id}",
        events,
        expected_version=app.version,
        **_trace_kwargs(cmd),
    )


async def handle_human_review_completed(store, cmd: HumanReviewCompletedCommand) -> list[int]:
    app = await LoanApplicationAggregate.load(store, cmd.application_id)
    if not (app.decision_generated or app.human_review_requested):
        raise DomainError("Human review cannot complete before a decision is generated")

    review_event = HumanReviewCompleted(
        application_id=cmd.application_id,
        reviewer_id=cmd.reviewer_id,
        override=cmd.override,
        original_recommendation=cmd.original_recommendation,
        final_decision=cmd.final_decision,
        override_reason=cmd.override_reason,
        reviewed_at=cmd.reviewed_at,
    ).to_store_dict()

    final_decision = cmd.final_decision.upper()
    if final_decision == "APPROVE":
        final_event = ApplicationApproved(
            application_id=cmd.application_id,
            approved_amount_usd=cmd.approved_amount_usd or Decimal("0"),
            interest_rate_pct=cmd.interest_rate_pct,
            term_months=cmd.term_months,
            conditions=cmd.conditions,
            approved_by=cmd.reviewer_id,
            effective_date=cmd.reviewed_at.date().isoformat(),
            approved_at=cmd.reviewed_at,
        ).to_store_dict()
    elif final_decision == "DECLINE":
        final_event = ApplicationDeclined(
            application_id=cmd.application_id,
            decline_reasons=cmd.decline_reasons or ["Declined by human review"],
            declined_by=cmd.reviewer_id,
            adverse_action_notice_required=cmd.adverse_action_notice_required,
            adverse_action_codes=cmd.adverse_action_codes,
            declined_at=cmd.reviewed_at,
        ).to_store_dict()
    else:
        raise DomainError(f"Unsupported final decision: {final_decision}")

    return await store.append(
        f"loan-{cmd.application_id}",
        [review_event, final_event],
        expected_version=app.version,
    )