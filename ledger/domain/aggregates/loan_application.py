"""
ledger/domain/aggregates/loan_application.py
=============================================
COMPLETION STATUS: STUB — implement apply() for each event, enforce business rules.

The aggregate replays its event stream to rebuild state.
Command handlers validate against current state before appending events.

BUSINESS RULES TO ENFORCE:
  1. State machine: only valid transitions allowed
  2. DocumentFactsExtracted must exist before CreditAnalysisCompleted
  3. All 6 compliance rules must complete before DecisionGenerated (unless hard block)
  4. confidence < 0.60 → recommendation must be REFER (enforced here, not in LLM)
  5. Compliance BLOCKED → only DECLINE allowed, not APPROVE or REFER
  6. Causal chain: every agent event must reference a triggering event_id

See: Section 4 of challenge document for full rule specifications.
"""
from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum

class ApplicationState(str, Enum):
    NEW = "NEW"; SUBMITTED = "SUBMITTED"; DOCUMENTS_PENDING = "DOCUMENTS_PENDING"
    DOCUMENTS_UPLOADED = "DOCUMENTS_UPLOADED"; DOCUMENTS_PROCESSED = "DOCUMENTS_PROCESSED"
    CREDIT_ANALYSIS_REQUESTED = "CREDIT_ANALYSIS_REQUESTED"; CREDIT_ANALYSIS_COMPLETE = "CREDIT_ANALYSIS_COMPLETE"
    FRAUD_SCREENING_REQUESTED = "FRAUD_SCREENING_REQUESTED"; FRAUD_SCREENING_COMPLETE = "FRAUD_SCREENING_COMPLETE"
    COMPLIANCE_CHECK_REQUESTED = "COMPLIANCE_CHECK_REQUESTED"; COMPLIANCE_CHECK_COMPLETE = "COMPLIANCE_CHECK_COMPLETE"
    PENDING_DECISION = "PENDING_DECISION"; PENDING_HUMAN_REVIEW = "PENDING_HUMAN_REVIEW"
    APPROVED = "APPROVED"; DECLINED = "DECLINED"; DECLINED_COMPLIANCE = "DECLINED_COMPLIANCE"
    REFERRED = "REFERRED"

VALID_TRANSITIONS = {
    ApplicationState.NEW: [ApplicationState.SUBMITTED],
    ApplicationState.SUBMITTED: [ApplicationState.DOCUMENTS_PENDING],
    ApplicationState.DOCUMENTS_PENDING: [ApplicationState.DOCUMENTS_UPLOADED],
    ApplicationState.DOCUMENTS_UPLOADED: [ApplicationState.DOCUMENTS_PROCESSED],
    ApplicationState.DOCUMENTS_PROCESSED: [ApplicationState.CREDIT_ANALYSIS_REQUESTED],
    ApplicationState.CREDIT_ANALYSIS_REQUESTED: [ApplicationState.CREDIT_ANALYSIS_COMPLETE],
    ApplicationState.CREDIT_ANALYSIS_COMPLETE: [ApplicationState.FRAUD_SCREENING_REQUESTED],
    ApplicationState.FRAUD_SCREENING_REQUESTED: [ApplicationState.FRAUD_SCREENING_COMPLETE],
    ApplicationState.FRAUD_SCREENING_COMPLETE: [ApplicationState.COMPLIANCE_CHECK_REQUESTED],
    ApplicationState.COMPLIANCE_CHECK_REQUESTED: [ApplicationState.COMPLIANCE_CHECK_COMPLETE],
    ApplicationState.COMPLIANCE_CHECK_COMPLETE: [ApplicationState.PENDING_DECISION, ApplicationState.DECLINED_COMPLIANCE],
    ApplicationState.PENDING_DECISION: [ApplicationState.APPROVED, ApplicationState.DECLINED, ApplicationState.PENDING_HUMAN_REVIEW],
    ApplicationState.PENDING_HUMAN_REVIEW: [ApplicationState.APPROVED, ApplicationState.DECLINED],
}

@dataclass
class LoanApplicationAggregate:
    application_id: str
    state: ApplicationState = ApplicationState.NEW
    applicant_id: str | None = None
    requested_amount_usd: float | None = None
    loan_purpose: str | None = None
    version: int = 0
    events: list[dict] = field(default_factory=list)

    @classmethod
    async def load(cls, store, application_id: str) -> "LoanApplicationAggregate":
        """Load and replay event stream to rebuild aggregate state."""
        agg = cls(application_id=application_id)
        # TODO: stream_events = await store.load_stream(f"loan-{application_id}")
        # TODO: for event in stream_events: agg.apply(event)
        return agg

    def apply(self, event: dict) -> None:
        """Apply one event to update aggregate state. TODO: implement for each event type."""
        et = event.get("event_type"); p = event.get("payload", {})
        self.version += 1
        if et == "ApplicationSubmitted":
            self.state = ApplicationState.SUBMITTED
            self.applicant_id = p.get("applicant_id")
            self.requested_amount_usd = p.get("requested_amount_usd")
            self.loan_purpose = p.get("loan_purpose")
        elif et == "DocumentUploadRequested":
            self.state = ApplicationState.DOCUMENTS_PENDING
        elif et == "DocumentUploaded":
            self.state = ApplicationState.DOCUMENTS_UPLOADED
        # TODO: implement remaining transitions

    def assert_valid_transition(self, target: ApplicationState) -> None:
        allowed = VALID_TRANSITIONS.get(self.state, [])
        if target not in allowed:
            raise ValueError(f"Invalid transition {self.state} → {target}. Allowed: {allowed}")
