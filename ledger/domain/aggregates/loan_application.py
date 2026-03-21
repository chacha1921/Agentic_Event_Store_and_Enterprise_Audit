"""Loan application aggregate with replay-based state reconstruction."""
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Any

from ledger.domain.errors import DomainError
from ledger.schema.events import deserialize_event


class LoanApplicationState(str, Enum):
    SUBMITTED = "Submitted"
    AWAITING_ANALYSIS = "AwaitingAnalysis"
    ANALYSIS_COMPLETE = "AnalysisComplete"
    COMPLIANCE_REVIEW = "ComplianceReview"
    PENDING_DECISION = "PendingDecision"
    APPROVED_PENDING_HUMAN = "ApprovedPendingHuman"
    DECLINED_PENDING_HUMAN = "DeclinedPendingHuman"
    FINAL_APPROVED = "FinalApproved"
    FINAL_DECLINED = "FinalDeclined"


VALID_TRANSITIONS: dict[LoanApplicationState | None, set[LoanApplicationState]] = {
    None: {LoanApplicationState.SUBMITTED},
    LoanApplicationState.SUBMITTED: {LoanApplicationState.AWAITING_ANALYSIS},
    LoanApplicationState.AWAITING_ANALYSIS: {
        LoanApplicationState.ANALYSIS_COMPLETE,
        LoanApplicationState.COMPLIANCE_REVIEW,
    },
    LoanApplicationState.ANALYSIS_COMPLETE: {LoanApplicationState.COMPLIANCE_REVIEW},
    LoanApplicationState.COMPLIANCE_REVIEW: {
        LoanApplicationState.PENDING_DECISION,
        LoanApplicationState.FINAL_DECLINED,
    },
    LoanApplicationState.PENDING_DECISION: {
        LoanApplicationState.APPROVED_PENDING_HUMAN,
        LoanApplicationState.DECLINED_PENDING_HUMAN,
    },
    LoanApplicationState.APPROVED_PENDING_HUMAN: {LoanApplicationState.FINAL_APPROVED, LoanApplicationState.FINAL_DECLINED},
    LoanApplicationState.DECLINED_PENDING_HUMAN: {LoanApplicationState.FINAL_APPROVED, LoanApplicationState.FINAL_DECLINED},
}


@dataclass
class LoanApplicationAggregate:
    application_id: str
    version: int = -1
    state: LoanApplicationState | None = None
    applicant_id: str | None = None
    requested_amount_usd: Decimal | None = None
    loan_purpose: str | None = None
    uploaded_documents: set[str] = field(default_factory=set)
    documents_requested: bool = False
    package_ready: bool = False
    credit_requested: bool = False
    fraud_requested: bool = False
    compliance_requested: bool = False
    compliance_completed: bool = False
    compliance_hard_block: bool = False
    decision_requested: bool = False
    decision_generated: bool = False
    recommendation: str | None = None
    decision_confidence: float | None = None
    human_review_requested: bool = False
    human_review_completed: bool = False
    final_decision: str | None = None
    decline_reasons: list[str] = field(default_factory=list)
    _events: list[dict] = field(default_factory=list)

    @classmethod
    async def load(cls, store, application_id: str) -> "LoanApplicationAggregate":
        aggregate = cls(application_id=application_id)
        stream_events = await store.load_stream(f"loan-{application_id}")
        for event in stream_events:
            aggregate._apply(event)
        return aggregate

    def _apply(self, event: dict) -> None:
        event_type = event.get("event_type")
        handler = getattr(self, f"_on_{event_type}", None)
        if handler is None:
            self.version = event.get("stream_position", self.version + 1)
            self._events.append(event)
            return
        domain_event = deserialize_event(event_type, event.get("payload", {}))
        handler(domain_event, event)
        self.version = event.get("stream_position", self.version + 1)
        self._events.append(event)

    def _transition(self, target: LoanApplicationState) -> None:
        allowed = VALID_TRANSITIONS.get(self.state, set())
        if target != self.state and target not in allowed:
            raise DomainError(f"Invalid transition {self.state} -> {target}")
        self.state = target

    def _ensure(self, condition: bool, message: str) -> None:
        if not condition:
            raise DomainError(message)

    def _on_ApplicationSubmitted(self, event, _: dict) -> None:
        self._transition(LoanApplicationState.SUBMITTED)
        self.applicant_id = event.applicant_id
        self.requested_amount_usd = event.requested_amount_usd
        self.loan_purpose = str(event.loan_purpose)

    def _on_DocumentUploadRequested(self, event, _: dict) -> None:
        self._ensure(self.state == LoanApplicationState.SUBMITTED, "Document upload can only be requested after submission")
        self.documents_requested = True

    def _on_DocumentUploaded(self, event, _: dict) -> None:
        self._ensure(self.documents_requested, "Documents cannot be uploaded before upload is requested")
        self.uploaded_documents.add(str(event.document_type))

    def _on_CreditAnalysisRequested(self, event, _: dict) -> None:
        del event
        self._ensure(self.state == LoanApplicationState.SUBMITTED, "Credit analysis can only start from submitted state")
        self._transition(LoanApplicationState.AWAITING_ANALYSIS)
        self.credit_requested = True

    def _on_FraudScreeningRequested(self, event, _: dict) -> None:
        self._ensure(self.credit_requested, "Fraud screening requires completed credit analysis")
        self._ensure(event.triggered_by_event_id, "Fraud screening must reference a triggering event")
        self.fraud_requested = True

    def _on_ComplianceCheckRequested(self, event, _: dict) -> None:
        self._ensure(self.fraud_requested, "Compliance review requires fraud screening to complete")
        self._ensure(event.triggered_by_event_id, "Compliance check must reference a triggering event")
        if self.state == LoanApplicationState.AWAITING_ANALYSIS:
            self._transition(LoanApplicationState.ANALYSIS_COMPLETE)
        self._transition(LoanApplicationState.COMPLIANCE_REVIEW)
        self.compliance_requested = True

    def _on_DecisionRequested(self, event, _: dict) -> None:
        self._ensure(self.compliance_completed and not self.compliance_hard_block, "Decision cannot be requested before compliance completes cleanly")
        self._ensure(event.triggered_by_event_id, "Decision request must reference a triggering event")
        self._transition(LoanApplicationState.PENDING_DECISION)
        self.decision_requested = True

    def _on_DecisionGenerated(self, event, _: dict) -> None:
        self._ensure(self.state == LoanApplicationState.PENDING_DECISION, "DecisionGenerated is only valid in PendingDecision state")
        recommendation = event.recommendation.upper()
        confidence = float(event.confidence)
        if confidence < 0.60:
            recommendation = "REFER"
        if self.compliance_hard_block and recommendation != "DECLINE":
            raise DomainError("Compliance hard block can only produce DECLINE")
        self.decision_generated = True
        self.decision_confidence = confidence
        self.recommendation = recommendation
        if recommendation == "APPROVE":
            self._transition(LoanApplicationState.APPROVED_PENDING_HUMAN)
        elif recommendation == "DECLINE":
            self._transition(LoanApplicationState.DECLINED_PENDING_HUMAN)
        elif recommendation == "REFER":
            self.human_review_requested = True
        else:
            raise DomainError(f"Unsupported recommendation: {recommendation}")

    def _on_HumanReviewRequested(self, event, _: dict) -> None:
        self._ensure(self.decision_generated, "Human review can only be requested after a decision is generated")
        self._ensure(event.decision_event_id, "Human review must reference a decision event")
        self.human_review_requested = True

    def _on_HumanReviewCompleted(self, event, _: dict) -> None:
        self._ensure(self.human_review_requested or self.decision_generated, "Human review cannot complete before it is requested")
        self.human_review_completed = True
        self.final_decision = event.final_decision.upper()

    def _on_ApplicationApproved(self, event, _: dict) -> None:
        del event
        self._ensure(not self.compliance_hard_block, "Application cannot be approved after compliance hard block")
        self._ensure(self.state in {LoanApplicationState.APPROVED_PENDING_HUMAN, LoanApplicationState.DECLINED_PENDING_HUMAN}, "Approval requires a pending human decision state")
        self._ensure(self.compliance_completed, "Application cannot be approved while compliance is pending")
        self._transition(LoanApplicationState.FINAL_APPROVED)
        self.final_decision = "APPROVE"

    def _on_ApplicationDeclined(self, event, _: dict) -> None:
        self.decline_reasons = list(event.decline_reasons)
        if self.compliance_hard_block and self.state == LoanApplicationState.COMPLIANCE_REVIEW:
            self._transition(LoanApplicationState.FINAL_DECLINED)
        else:
            self._ensure(self.state in {LoanApplicationState.DECLINED_PENDING_HUMAN, LoanApplicationState.APPROVED_PENDING_HUMAN}, "Decline requires a pending human decision state")
            self._transition(LoanApplicationState.FINAL_DECLINED)
        self.final_decision = "DECLINE"

    def mark_package_ready(self) -> None:
        self.package_ready = True

    def mark_compliance_completed(self, has_hard_block: bool) -> None:
        self.compliance_completed = True
        self.compliance_hard_block = has_hard_block

    def assert_can_submit(self) -> None:
        self._ensure(self.version == -1, "Application has already been submitted")

    def assert_awaiting_credit_analysis(self) -> None:
        self._ensure(self.state == LoanApplicationState.AWAITING_ANALYSIS, "Application is not awaiting analysis")
        self._ensure(self.package_ready, "Document package must be ready before credit analysis completes")

    def assert_ready_for_compliance(self) -> None:
        self._ensure(self.state == LoanApplicationState.AWAITING_ANALYSIS, "Application is not ready to enter compliance review")
        self._ensure(self.fraud_requested, "Fraud screening must complete before compliance review")

    def assert_pending_decision(self) -> None:
        self._ensure(self.state == LoanApplicationState.PENDING_DECISION, "Application is not pending a decision")

