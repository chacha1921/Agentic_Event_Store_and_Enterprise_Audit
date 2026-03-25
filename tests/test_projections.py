from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from decimal import Decimal

import pytest
import pytest_asyncio

from ledger.event_store import InMemoryEventStore
from ledger.projections import (
    AgentPerformanceLedger,
    ApplicationSummary,
    ComplianceAuditView,
    ProjectionDaemon,
)
from ledger.schema.events import (
    AgentOutputWritten,
    AgentSessionCompleted,
    AgentSessionStarted,
    AgentType,
    ApplicationApproved,
    ApplicationSubmitted,
    ComplianceCheckCompleted,
    ComplianceCheckInitiated,
    ComplianceRuleFailed,
    ComplianceVerdict,
    CreditAnalysisCompleted,
    CreditDecision,
    DecisionGenerated,
    LoanPurpose,
    RiskTier,
)


@pytest_asyncio.fixture
async def store() -> InMemoryEventStore:
    return InMemoryEventStore()


async def _append(store: InMemoryEventStore, stream_id: str, event: dict) -> None:
    version = await store.stream_version(stream_id)
    await store.append(stream_id, [event], expected_version=version)


def _submitted(application_id: str) -> dict:
    return ApplicationSubmitted(
        application_id=application_id,
        applicant_id="COMP-001",
        requested_amount_usd=Decimal("500000"),
        loan_purpose=LoanPurpose.WORKING_CAPITAL,
        loan_term_months=36,
        submission_channel="web",
        contact_email="borrower@example.com",
        contact_name="Borrower One",
        submitted_at=datetime.now(),
        application_reference=application_id,
    ).to_store_dict()


@pytest.mark.asyncio
async def test_projection_daemon_updates_application_summary(store: InMemoryEventStore):
    application_id = "APEX-PROJ-001"
    summary = ApplicationSummary(store)
    daemon = ProjectionDaemon(store, [summary], batch_size=10, poll_interval=0.01)

    await store.append(f"loan-{application_id}", [_submitted(application_id)], expected_version=-1)
    await store.append(
        f"credit-{application_id}",
        [
            CreditAnalysisCompleted(
                application_id=application_id,
                session_id="sess-credit-001",
                decision=CreditDecision(
                    risk_tier=RiskTier.MEDIUM,
                    recommended_limit_usd=Decimal("350000"),
                    confidence=0.72,
                    rationale="Healthy operating performance.",
                    key_concerns=[],
                    data_quality_caveats=[],
                    policy_overrides_applied=[],
                ),
                model_version="claude-sonnet-4-20250514",
                model_deployment_id="dep-credit-001",
                input_data_hash="hash-credit-001",
                analysis_duration_ms=1200,
                regulatory_basis=[],
                completed_at=datetime.now(),
            ).to_store_dict()
        ],
        expected_version=-1,
    )
    await store.append(
        f"loan-{application_id}",
        [
            ApplicationApproved(
                application_id=application_id,
                approved_amount_usd=Decimal("400000"),
                interest_rate_pct=7.25,
                term_months=36,
                conditions=["Monthly reporting"],
                approved_by="loan-officer-1",
                effective_date=datetime.now().date().isoformat(),
                approved_at=datetime.now(),
            ).to_store_dict()
        ],
        expected_version=0,
    )

    processed = await daemon.run_once()
    assert processed > 0

    row = await summary.get(application_id)
    assert row is not None
    assert row["applicant_id"] == "COMP-001"
    assert row["risk_tier"] == "MEDIUM"
    assert row["final_decision"] == "APPROVED"
    assert row["state"] == "APPROVED"


@pytest.mark.asyncio
async def test_agent_performance_ledger_aggregates_metrics(store: InMemoryEventStore):
    ledger = AgentPerformanceLedger(store)
    daemon = ProjectionDaemon(store, [ledger], batch_size=10, poll_interval=0.01)
    session_id = "sess-orc-001"

    await _append(
        store,
        f"agent-{AgentType.DECISION_ORCHESTRATOR.value}-{session_id}",
        AgentSessionStarted(
            session_id=session_id,
            agent_type=AgentType.DECISION_ORCHESTRATOR,
            agent_id="orchestrator-1",
            application_id="APEX-PROJ-002",
            model_version="claude-sonnet-4-20250514",
            langgraph_graph_version="graph-v1",
            context_source="loan-stream",
            context_token_count=256,
            started_at=datetime.now(),
        ).to_store_dict(),
    )
    await _append(
        store,
        f"agent-{AgentType.DECISION_ORCHESTRATOR.value}-{session_id}",
        AgentOutputWritten(
            session_id=session_id,
            agent_type=AgentType.DECISION_ORCHESTRATOR,
            application_id="APEX-PROJ-002",
            events_written=[
                DecisionGenerated(
                    application_id="APEX-PROJ-002",
                    orchestrator_session_id=session_id,
                    recommendation="APPROVE",
                    confidence=0.81,
                    approved_amount_usd=Decimal("275000"),
                    conditions=[],
                    executive_summary="Strong candidate.",
                    key_risks=[],
                    contributing_sessions=["sess-credit-001"],
                    model_versions={"orchestrator": "claude-sonnet-4-20250514"},
                    generated_at=datetime.now(),
                ).to_store_dict()
            ],
            output_summary="Approved recommendation generated.",
            written_at=datetime.now(),
        ).to_store_dict(),
    )
    await _append(
        store,
        f"agent-{AgentType.DECISION_ORCHESTRATOR.value}-{session_id}",
        AgentSessionCompleted(
            session_id=session_id,
            agent_type=AgentType.DECISION_ORCHESTRATOR,
            application_id="APEX-PROJ-002",
            total_nodes_executed=6,
            total_llm_calls=1,
            total_tokens_used=500,
            total_cost_usd=0.02,
            total_duration_ms=1800,
            next_agent_triggered=None,
            completed_at=datetime.now(),
        ).to_store_dict(),
    )

    await daemon.run_once()
    metrics = await ledger.get_metrics("orchestrator-1", "claude-sonnet-4-20250514")

    assert metrics is not None
    assert metrics["sessions_started"] == 1
    assert metrics["sessions_completed"] == 1
    assert metrics["decision_count"] == 1
    assert metrics["approve_count"] == 1
    assert metrics["approve_rate"] == pytest.approx(1.0)
    assert metrics["avg_confidence"] == pytest.approx(0.81)
    assert metrics["avg_duration_ms"] == pytest.approx(1800.0)


@pytest.mark.asyncio
async def test_compliance_audit_supports_temporal_queries_and_rebuild(store: InMemoryEventStore):
    audit = ComplianceAuditView(store)
    daemon = ProjectionDaemon(store, [audit], batch_size=10, poll_interval=0.01)
    app_id = "APEX-PROJ-003"
    t1 = datetime.now() - timedelta(minutes=5)
    t2 = datetime.now() - timedelta(minutes=4)
    t3 = datetime.now() - timedelta(minutes=3)

    await store.append(
        f"compliance-{app_id}",
        [
            ComplianceCheckInitiated(
                application_id=app_id,
                session_id="sess-comp-001",
                regulation_set_version="2026-Q1",
                rules_to_evaluate=["REG-001", "REG-003"],
                initiated_at=t1,
            ).to_store_dict(),
            ComplianceRuleFailed(
                application_id=app_id,
                session_id="sess-comp-001",
                rule_id="REG-003",
                rule_name="Montana hard block",
                rule_version="2026-Q1",
                failure_reason="Applicant jurisdiction blocked",
                is_hard_block=True,
                remediation_available=False,
                remediation_description=None,
                evidence_hash="evidence-001",
                evaluated_at=t2,
            ).to_store_dict(),
            ComplianceCheckCompleted(
                application_id=app_id,
                session_id="sess-comp-001",
                rules_evaluated=2,
                rules_passed=1,
                rules_failed=1,
                rules_noted=0,
                has_hard_block=True,
                overall_verdict=ComplianceVerdict.BLOCKED,
                completed_at=t3,
            ).to_store_dict(),
        ],
        expected_version=-1,
    )

    await daemon.run_once()

    before_completion = await audit.get_compliance_at(app_id, t2)
    after_completion = await audit.get_compliance_at(app_id, datetime.now())

    assert before_completion is not None
    assert before_completion["has_hard_block"] is True
    assert before_completion["overall_verdict"] is None

    assert after_completion is not None
    assert after_completion["overall_verdict"] == "BLOCKED"
    assert after_completion["rules_failed"] == 1

    await audit.rebuild_from_scratch()
    rebuilt = await audit.get_current(app_id)
    assert rebuilt is not None
    assert rebuilt["overall_verdict"] == "BLOCKED"


@pytest.mark.asyncio
async def test_projection_daemon_reports_lag_and_tolerates_failures(store: InMemoryEventStore):
    class BrokenProjection:
        checkpoint_name = "projection.broken"

        async def setup(self):
            return None

        async def apply(self, event):
            raise RuntimeError("boom")

    summary = ApplicationSummary(store)
    daemon = ProjectionDaemon(store, [BrokenProjection(), summary], batch_size=10, poll_interval=0.01, max_retries=1)
    app_id = "APEX-PROJ-004"

    await store.append(f"loan-{app_id}", [_submitted(app_id)], expected_version=-1)
    await daemon.run_once()

    row = await summary.get(app_id)
    lag = await daemon.get_lag()

    assert row is not None
    assert lag["latest_global_position"] >= lag["processed_position"]
    assert lag["position_lag"] >= 0