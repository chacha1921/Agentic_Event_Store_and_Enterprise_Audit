from __future__ import annotations

from datetime import datetime
from decimal import Decimal

import pytest
import pytest_asyncio

from ledger.domain.aggregates.agent_session import (
    AgentSessionAggregate,
    SessionHealthStatus,
    reconstruct_agent_context,
)
from ledger.domain.aggregates.audit_ledger import AuditLedgerAggregate
from ledger.domain.aggregates.compliance_record import ComplianceRecordAggregate
from ledger.domain.aggregates.loan_application import LoanApplicationAggregate, LoanApplicationState
from ledger.domain.errors import DomainError
from ledger.event_store import InMemoryEventStore
from ledger.schema.events import (
    AgentContextLoaded,
    AgentInputValidated,
    AgentNodeExecuted,
    AgentOutputWritten,
    AgentSessionStarted,
    AgentToolCalled,
    AgentType,
    ApplicationApproved,
    ApplicationSubmitted,
    AuditIntegrityCheckRun,
    ComplianceCheckCompleted,
    ComplianceCheckInitiated,
    ComplianceRulePassed,
    ComplianceVerdict,
    DecisionGenerated,
    LoanPurpose,
)


@pytest_asyncio.fixture
async def store() -> InMemoryEventStore:
    return InMemoryEventStore()


@pytest.mark.asyncio
async def test_reconstruct_agent_context_cold_recovery_flags_reconciliation(store: InMemoryEventStore):
    session_id = "sess-recover-001"
    application_id = "APP-REC-001"
    stream_id = f"agent-{AgentType.DECISION_ORCHESTRATOR.value}-{session_id}"
    now = datetime.now()

    await store.append(
        f"loan-{application_id}",
        [
            ApplicationSubmitted(
                application_id=application_id,
                applicant_id="COMP-REC-001",
                requested_amount_usd=Decimal("250000"),
                loan_purpose=LoanPurpose.WORKING_CAPITAL,
                loan_term_months=24,
                submission_channel="web",
                contact_email="recover@example.com",
                contact_name="Recover User",
                submitted_at=now,
                application_reference=application_id,
            ).to_store_dict()
        ],
        expected_version=-1,
    )

    events = [
        AgentContextLoaded(
            session_id=session_id,
            agent_type=AgentType.DECISION_ORCHESTRATOR,
            application_id=application_id,
            context_source="loan-stream",
            context_token_count=256,
            loaded_at=now,
        ).to_store_dict(),
        AgentSessionStarted(
            session_id=session_id,
            agent_type=AgentType.DECISION_ORCHESTRATOR,
            agent_id="orchestrator-1",
            application_id=application_id,
            model_version="claude-sonnet-4-20250514",
            langgraph_graph_version="graph-v1",
            context_source="loan-stream",
            context_token_count=256,
            started_at=now,
        ).to_store_dict(),
        AgentInputValidated(
            session_id=session_id,
            agent_type=AgentType.DECISION_ORCHESTRATOR,
            application_id=application_id,
            inputs_validated=["application", "credit", "fraud", "compliance"],
            validation_duration_ms=14,
            validated_at=now,
        ).to_store_dict(),
        AgentNodeExecuted(
            session_id=session_id,
            agent_type=AgentType.DECISION_ORCHESTRATOR,
            node_name="synthesize_decision",
            node_sequence=3,
            input_keys=["credit", "fraud", "compliance"],
            output_keys=["recommendation"],
            llm_called=True,
            llm_tokens_input=300,
            llm_tokens_output=90,
            llm_cost_usd=0.01,
            duration_ms=420,
            executed_at=now,
        ).to_store_dict(),
        AgentToolCalled(
            session_id=session_id,
            agent_type=AgentType.DECISION_ORCHESTRATOR,
            tool_name="registry.lookup",
            tool_input_summary="application_id=APP-REC-001",
            tool_output_summary="company profile loaded",
            tool_duration_ms=22,
            called_at=now,
        ).to_store_dict(),
        AgentOutputWritten(
            session_id=session_id,
            agent_type=AgentType.DECISION_ORCHESTRATOR,
            application_id=application_id,
            events_written=[
                DecisionGenerated(
                    application_id=application_id,
                    orchestrator_session_id=session_id,
                    recommendation="REFER",
                    confidence=0.55,
                    approved_amount_usd=Decimal("250000"),
                    conditions=["Quarterly reporting"],
                    executive_summary="Borderline confidence; route to review.",
                    key_risks=["Customer concentration"],
                    contributing_sessions=["sess-crd-001", "sess-frd-001", "sess-cmp-001"],
                    model_versions={"orchestrator": "claude-sonnet-4-20250514"},
                    generated_at=now,
                ).to_store_dict()
            ],
            output_summary="DecisionGenerated emitted but completion not yet recorded.",
            written_at=now,
        ).to_store_dict(),
    ]

    await store.append(stream_id, events, expected_version=-1)

    context = await reconstruct_agent_context(
        store,
        agent_id=AgentType.DECISION_ORCHESTRATOR.value,
        session_id=session_id,
        token_budget=8000,
    )
    compressed_context = await reconstruct_agent_context(
        store,
        agent_id=AgentType.DECISION_ORCHESTRATOR.value,
        session_id=session_id,
        token_budget=80,
    )

    assert context.last_event_position == 5
    assert context.pending_work
    assert context.session_health_status == SessionHealthStatus.NEEDS_RECONCILIATION
    assert context.current_application_state == "Submitted"
    assert "Older session summary:" in context.context_text
    assert "AgentNodeExecuted" in context.context_text
    assert "AgentToolCalled" in context.context_text
    assert "AgentOutputWritten" in context.context_text
    assert len(compressed_context.context_text) < len(context.context_text)
    assert "truncated to respect token budget" in compressed_context.context_text
    assert "AgentOutputWritten" in compressed_context.context_text


@pytest.mark.asyncio
async def test_agent_session_requires_context_loaded_before_start(store: InMemoryEventStore):
    session_id = "sess-missing-context-001"
    await store.append(
        f"agent-{AgentType.CREDIT_ANALYSIS.value}-{session_id}",
        [
            AgentSessionStarted(
                session_id=session_id,
                agent_type=AgentType.CREDIT_ANALYSIS,
                agent_id="credit-agent-1",
                application_id="APP-CTX-001",
                model_version="claude-sonnet-4-20250514",
                langgraph_graph_version="graph-v1",
                context_source="loan-stream",
                context_token_count=128,
                started_at=datetime.now(),
            ).to_store_dict()
        ],
        expected_version=-1,
    )

    with pytest.raises(DomainError):
        await AgentSessionAggregate.load(store, AgentType.CREDIT_ANALYSIS.value, session_id)


@pytest.mark.asyncio
async def test_compliance_record_enforces_rule_uniqueness_and_completion_consistency(store: InMemoryEventStore):
    app_id = "APP-COMP-001"
    aggregate = ComplianceRecordAggregate(application_id=app_id)
    aggregate._apply(
        ComplianceCheckInitiated(
            application_id=app_id,
            session_id="sess-cmp-001",
            regulation_set_version="2026-Q1",
            rules_to_evaluate=["REG-001"],
            initiated_at=datetime.now(),
        ).to_store_dict()
    )
    aggregate._apply(
        ComplianceRulePassed(
            application_id=app_id,
            session_id="sess-cmp-001",
            rule_id="REG-001",
            rule_name="KYC documents complete",
            rule_version="2026-Q1",
            evidence_hash="evidence-001",
            evaluation_notes="ok",
            evaluated_at=datetime.now(),
        ).to_store_dict()
    )

    with pytest.raises(DomainError):
        aggregate._apply(
            ComplianceRulePassed(
                application_id=app_id,
                session_id="sess-cmp-001",
                rule_id="REG-001",
                rule_name="KYC documents complete",
                rule_version="2026-Q1",
                evidence_hash="evidence-002",
                evaluation_notes="duplicate",
                evaluated_at=datetime.now(),
            ).to_store_dict()
        )

    with pytest.raises(DomainError):
        aggregate._apply(
            ComplianceCheckCompleted(
                application_id=app_id,
                session_id="sess-cmp-001",
                rules_evaluated=99,
                rules_passed=1,
                rules_failed=0,
                rules_noted=0,
                has_hard_block=False,
                overall_verdict=ComplianceVerdict.CLEAR,
                completed_at=datetime.now(),
            ).to_store_dict()
        )


@pytest.mark.asyncio
async def test_decision_generated_requires_causal_contributing_sessions():
    app = LoanApplicationAggregate(application_id="APP-CAUSAL-001")
    app.state = LoanApplicationState.PENDING_DECISION
    app.compliance_completed = True

    with pytest.raises(DomainError):
        app._apply(
            DecisionGenerated(
                application_id="APP-CAUSAL-001",
                orchestrator_session_id="sess-orc-001",
                recommendation="APPROVE",
                confidence=0.81,
                approved_amount_usd=Decimal("100000"),
                conditions=[],
                executive_summary="Approved",
                key_risks=[],
                contributing_sessions=[],
                model_versions={"orchestrator": "claude-sonnet-4-20250514"},
                generated_at=datetime.now(),
            ).to_store_dict()
        )


@pytest.mark.asyncio
async def test_approval_requires_all_compliance_rules_satisfied(store: InMemoryEventStore):
    application_id = "APP-APPROVAL-001"
    await store.append(
        f"compliance-{application_id}",
        [
            ComplianceCheckInitiated(
                application_id=application_id,
                session_id="sess-cmp-approve-001",
                regulation_set_version="2026-Q1",
                rules_to_evaluate=["REG-001", "REG-004"],
                initiated_at=datetime.now(),
            ).to_store_dict(),
            ComplianceRulePassed(
                application_id=application_id,
                session_id="sess-cmp-approve-001",
                rule_id="REG-001",
                rule_name="KYC complete",
                rule_version="2026-Q1",
                evidence_hash="ev-approve-001",
                evaluation_notes="passed",
                evaluated_at=datetime.now(),
            ).to_store_dict(),
            ComplianceCheckCompleted(
                application_id=application_id,
                session_id="sess-cmp-approve-001",
                rules_evaluated=1,
                rules_passed=1,
                rules_failed=0,
                rules_noted=0,
                has_hard_block=False,
                overall_verdict=ComplianceVerdict.CLEAR,
                completed_at=datetime.now(),
            ).to_store_dict(),
        ],
        expected_version=-1,
    )

    app = LoanApplicationAggregate(application_id=application_id)

    with pytest.raises(DomainError):
        await app.assert_approval_requirements_satisfied(store)


@pytest.mark.asyncio
async def test_contributing_session_must_have_decision_output_for_application(store: InMemoryEventStore):
    session_id = "sess-no-output-001"
    await store.append(
        f"agent-{AgentType.CREDIT_ANALYSIS.value}-{session_id}",
        [
            AgentContextLoaded(
                session_id=session_id,
                agent_type=AgentType.CREDIT_ANALYSIS,
                application_id="APP-CAUSAL-OUT-001",
                context_source="loan-stream",
                context_token_count=128,
                loaded_at=datetime.now(),
            ).to_store_dict(),
            AgentSessionStarted(
                session_id=session_id,
                agent_type=AgentType.CREDIT_ANALYSIS,
                agent_id="credit-agent-1",
                application_id="APP-CAUSAL-OUT-001",
                model_version="claude-sonnet-4-20250514",
                langgraph_graph_version="graph-v1",
                context_source="loan-stream",
                context_token_count=128,
                started_at=datetime.now(),
            ).to_store_dict(),
        ],
        expected_version=-1,
    )

    session = await AgentSessionAggregate.load(store, AgentType.CREDIT_ANALYSIS.value, session_id)

    with pytest.raises(DomainError):
        session.assert_processed_application_decision()


@pytest.mark.asyncio
async def test_audit_ledger_detects_broken_previous_hash(store: InMemoryEventStore):
    entity_id = "loan-APP-AUD-001"
    await store.append(
        f"audit-{entity_id}",
        [
            AuditIntegrityCheckRun(
                entity_type="loan",
                entity_id="APP-AUD-001",
                check_timestamp=datetime.now(),
                events_verified_count=2,
                integrity_hash="hash-1",
                previous_hash=None,
                chain_valid=True,
                tamper_detected=False,
            ).to_store_dict(),
            AuditIntegrityCheckRun(
                entity_type="loan",
                entity_id="APP-AUD-001",
                check_timestamp=datetime.now(),
                events_verified_count=3,
                integrity_hash="hash-2",
                previous_hash="wrong-hash",
                chain_valid=True,
                tamper_detected=False,
            ).to_store_dict(),
        ],
        expected_version=-1,
    )

    with pytest.raises(DomainError):
        await AuditLedgerAggregate.load(store, entity_id)
