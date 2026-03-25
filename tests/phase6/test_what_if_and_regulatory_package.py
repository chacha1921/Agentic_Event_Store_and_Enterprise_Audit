from __future__ import annotations

import json
from datetime import datetime, timedelta
from decimal import Decimal

import pytest
import pytest_asyncio

from ledger.event_store import InMemoryEventStore
from ledger.projections import AgentPerformanceLedger, ApplicationSummary, ComplianceAuditView
from ledger.regulatory import generate_regulatory_package
from ledger.schema.events import (
    AgentOutputWritten,
    AgentSessionCompleted,
    AgentSessionStarted,
    AgentType,
    ApplicationApproved,
    ApplicationDeclined,
    ApplicationSubmitted,
    ComplianceCheckCompleted,
    ComplianceCheckInitiated,
    ComplianceVerdict,
    CreditAnalysisCompleted,
    CreditDecision,
    DecisionGenerated,
    FraudScreeningCompleted,
    LoanPurpose,
    RiskTier,
)
from ledger.what_if import run_what_if


@pytest_asyncio.fixture
async def store() -> InMemoryEventStore:
    return InMemoryEventStore()


async def _seed_application_history(store: InMemoryEventStore, application_id: str) -> dict[str, datetime]:
    now = datetime.now()
    credit_event = CreditAnalysisCompleted(
        application_id=application_id,
        session_id="sess-crd-whatif-001",
        decision=CreditDecision(
            risk_tier=RiskTier.MEDIUM,
            recommended_limit_usd=Decimal("350000"),
            confidence=0.72,
            rationale="Stable business with manageable leverage.",
            key_concerns=["Customer concentration"],
            data_quality_caveats=[],
            policy_overrides_applied=[],
        ),
        model_version="credit-model-feb",
        model_deployment_id="dep-credit-feb-001",
        input_data_hash="hash-credit-feb-001",
        analysis_duration_ms=1350,
        regulatory_basis=["CREDIT-POLICY-2026-01"],
        completed_at=now + timedelta(minutes=1),
    ).to_store_dict()
    decision_event = DecisionGenerated(
        application_id=application_id,
        orchestrator_session_id="sess-orc-whatif-001",
        recommendation="APPROVE",
        confidence=0.81,
        approved_amount_usd=Decimal("325000"),
        conditions=["Quarterly reporting"],
        executive_summary="Healthy borrower under baseline model.",
        key_risks=["Customer concentration"],
        contributing_sessions=["sess-crd-whatif-001"],
        model_versions={"orchestrator": "orchestrator-model-mar", "credit_analysis": "credit-model-feb"},
        generated_at=now + timedelta(minutes=4),
    ).to_store_dict()

    await store.append(
        f"loan-{application_id}",
        [
            ApplicationSubmitted(
                application_id=application_id,
                applicant_id="COMP-PHASE6-001",
                requested_amount_usd=Decimal("500000"),
                loan_purpose=LoanPurpose.WORKING_CAPITAL,
                loan_term_months=36,
                submission_channel="web",
                contact_email="borrower@example.com",
                contact_name="Borrower One",
                submitted_at=now,
                application_reference=application_id,
            ).to_store_dict(),
        ],
        expected_version=-1,
    )
    await store.append(
        "agent-credit_analysis-sess-crd-whatif-001",
        [
            AgentSessionStarted(
                session_id="sess-crd-whatif-001",
                agent_type=AgentType.CREDIT_ANALYSIS,
                agent_id="credit-agent-1",
                application_id=application_id,
                model_version="credit-model-feb",
                langgraph_graph_version="graph-v1",
                context_source="loan-stream",
                context_token_count=256,
                started_at=now + timedelta(seconds=30),
            ).to_store_dict(),
            AgentOutputWritten(
                session_id="sess-crd-whatif-001",
                agent_type=AgentType.CREDIT_ANALYSIS,
                application_id=application_id,
                events_written=[credit_event],
                output_summary="Credit analysis emitted.",
                written_at=now + timedelta(minutes=1),
            ).to_store_dict(),
            AgentSessionCompleted(
                session_id="sess-crd-whatif-001",
                agent_type=AgentType.CREDIT_ANALYSIS,
                application_id=application_id,
                total_nodes_executed=4,
                total_llm_calls=1,
                total_tokens_used=400,
                total_cost_usd=0.02,
                total_duration_ms=1350,
                next_agent_triggered=None,
                completed_at=now + timedelta(minutes=1, seconds=10),
            ).to_store_dict(),
        ],
        expected_version=-1,
    )
    await store.append(f"credit-{application_id}", [credit_event], expected_version=-1)
    await store.append(
        f"fraud-{application_id}",
        [
            FraudScreeningCompleted(
                application_id=application_id,
                session_id="sess-frd-whatif-001",
                fraud_score=0.18,
                risk_level="LOW",
                anomalies_found=0,
                recommendation="CLEAR",
                screening_model_version="fraud-model-mar",
                input_data_hash="hash-fraud-mar-001",
                completed_at=now + timedelta(minutes=2),
            ).to_store_dict(),
        ],
        expected_version=-1,
    )
    await store.append(
        f"compliance-{application_id}",
        [
            ComplianceCheckInitiated(
                application_id=application_id,
                session_id="sess-cmp-whatif-001",
                regulation_set_version="2026-Q1",
                rules_to_evaluate=["REG-001"],
                initiated_at=now + timedelta(minutes=2, seconds=30),
            ).to_store_dict(),
            ComplianceCheckCompleted(
                application_id=application_id,
                session_id="sess-cmp-whatif-001",
                rules_evaluated=1,
                rules_passed=1,
                rules_failed=0,
                rules_noted=0,
                has_hard_block=False,
                overall_verdict=ComplianceVerdict.CLEAR,
                completed_at=now + timedelta(minutes=3),
            ).to_store_dict(),
        ],
        expected_version=-1,
    )
    await store.append(
        "agent-decision_orchestrator-sess-orc-whatif-001",
        [
            AgentSessionStarted(
                session_id="sess-orc-whatif-001",
                agent_type=AgentType.DECISION_ORCHESTRATOR,
                agent_id="orchestrator-1",
                application_id=application_id,
                model_version="orchestrator-model-mar",
                langgraph_graph_version="graph-v1",
                context_source="loan-stream",
                context_token_count=512,
                started_at=now + timedelta(minutes=3, seconds=30),
            ).to_store_dict(),
            AgentOutputWritten(
                session_id="sess-orc-whatif-001",
                agent_type=AgentType.DECISION_ORCHESTRATOR,
                application_id=application_id,
                events_written=[decision_event],
                output_summary="Decision generated.",
                written_at=now + timedelta(minutes=4),
            ).to_store_dict(),
            AgentSessionCompleted(
                session_id="sess-orc-whatif-001",
                agent_type=AgentType.DECISION_ORCHESTRATOR,
                application_id=application_id,
                total_nodes_executed=5,
                total_llm_calls=1,
                total_tokens_used=620,
                total_cost_usd=0.03,
                total_duration_ms=1600,
                next_agent_triggered=None,
                completed_at=now + timedelta(minutes=4, seconds=10),
            ).to_store_dict(),
        ],
        expected_version=-1,
    )
    await store.append(
        f"loan-{application_id}",
        [
            decision_event,
            ApplicationApproved(
                application_id=application_id,
                approved_amount_usd=Decimal("325000"),
                interest_rate_pct=7.2,
                term_months=36,
                conditions=["Quarterly reporting"],
                approved_by="loan-officer-1",
                effective_date=(now + timedelta(days=1)).date().isoformat(),
                approved_at=now + timedelta(minutes=5),
            ).to_store_dict(),
        ],
        expected_version=0,
    )
    return {"now": now}


@pytest.mark.asyncio
async def test_run_what_if_changes_final_outcome_for_high_risk_counterfactual(store: InMemoryEventStore):
    application_id = "APP-PHASE6-WHATIF-001"
    await _seed_application_history(store, application_id)
    summary = ApplicationSummary(store)
    compliance = ComplianceAuditView(store)

    loan_version_before = await store.stream_version(f"loan-{application_id}")
    credit_version_before = await store.stream_version(f"credit-{application_id}")

    result = await run_what_if(
        store,
        application_id,
        branch_at_event_type="CreditAnalysisCompleted",
        counterfactual_events=[
            CreditAnalysisCompleted(
                application_id=application_id,
                session_id="sess-crd-whatif-001",
                decision=CreditDecision(
                    risk_tier=RiskTier.HIGH,
                    recommended_limit_usd=Decimal("100000"),
                    confidence=0.89,
                    rationale="March model identified materially higher leverage risk.",
                    key_concerns=["Debt service coverage", "Customer concentration"],
                    data_quality_caveats=[],
                    policy_overrides_applied=[],
                ),
                model_version="credit-model-mar",
                model_deployment_id="dep-credit-mar-001",
                input_data_hash="hash-credit-mar-001",
                analysis_duration_ms=1400,
                regulatory_basis=["CREDIT-POLICY-2026-03"],
                completed_at=datetime.now(),
            ),
            DecisionGenerated(
                application_id=application_id,
                orchestrator_session_id="sess-orc-whatif-001",
                recommendation="DECLINE",
                confidence=0.83,
                approved_amount_usd=None,
                conditions=[],
                executive_summary="Counterfactual March model would have declined this application.",
                key_risks=["High leverage"],
                contributing_sessions=["sess-crd-whatif-001"],
                model_versions={"orchestrator": "orchestrator-model-mar", "credit_analysis": "credit-model-mar"},
                generated_at=datetime.now(),
            ),
            ApplicationDeclined(
                application_id=application_id,
                decline_reasons=["High modeled default risk"],
                declined_by="what-if-engine",
                adverse_action_notice_required=True,
                adverse_action_codes=["HIGH_RISK_MODEL"],
                declined_at=datetime.now(),
            ),
        ],
        projections=[summary, compliance],
    )

    real_summary = result["real_outcome"][summary.checkpoint_name]
    counterfactual_summary = result["counterfactual_outcome"][summary.checkpoint_name]

    assert real_summary["final_decision"] == "APPROVED"
    assert real_summary["risk_tier"] == "MEDIUM"
    assert counterfactual_summary["final_decision"] == "DECLINED"
    assert counterfactual_summary["risk_tier"] == "HIGH"
    assert any(item["type"] == "real_event_skipped" for item in result["divergence_events"])
    assert await store.stream_version(f"loan-{application_id}") == loan_version_before
    assert await store.stream_version(f"credit-{application_id}") == credit_version_before


@pytest.mark.asyncio
async def test_generate_regulatory_package_is_self_contained(store: InMemoryEventStore):
    application_id = "APP-PHASE6-PKG-001"
    context = await _seed_application_history(store, application_id)
    package = await generate_regulatory_package(
        store,
        application_id,
        examination_date=context["now"] + timedelta(minutes=10),
    )

    assert package["application_id"] == application_id
    assert len(package["event_stream"]) >= 8
    assert package["event_stream"][0]["event_type"] == "ApplicationSubmitted"
    assert package["projection_states_at_examination"]["projection.application_summary"]["final_decision"] == "APPROVED"
    assert package["projection_states_at_examination"]["projection.compliance_audit"]["overall_verdict"] == "CLEAR"
    assert package["projection_states_at_examination"]["projection.agent_performance"]
    assert package["integrity_verification"]["chain_valid"] is True
    assert package["lifecycle_narrative"]
    assert any("Credit analysis completed" in sentence for sentence in package["lifecycle_narrative"])
    assert any(item["agent_type"] == "credit_analysis" and item["input_data_hash"] == "hash-credit-feb-001" for item in package["agent_provenance"])
    assert any(item["agent_type"] == "orchestrator" and item["confidence_score"] == 0.81 for item in package["agent_provenance"])
    json.dumps(package)
