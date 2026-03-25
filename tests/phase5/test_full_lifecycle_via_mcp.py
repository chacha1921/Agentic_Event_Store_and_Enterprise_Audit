from __future__ import annotations

import json
from datetime import datetime, timedelta
from decimal import Decimal

import pytest
import pytest_asyncio

from ledger.event_store import InMemoryEventStore
from ledger.mcp_server import create_mcp_server
from ledger.projections import (
    AgentPerformanceLedger,
    ApplicationSummary,
    ComplianceAuditView,
    ProjectionDaemon,
)
from ledger.schema.events import (
    ComplianceCheckCompleted,
    ComplianceCheckInitiated,
    ComplianceRuleNoted,
    ComplianceRulePassed,
    ComplianceVerdict,
    CreditAnalysisCompleted,
    CreditAnalysisRequested,
    CreditDecision,
    FraudScreeningCompleted,
    LoanPurpose,
    PackageReadyForAnalysis,
    RiskTier,
)


@pytest_asyncio.fixture
async def mcp_server():
    store = InMemoryEventStore()
    application_summary = ApplicationSummary(store)
    compliance_audit = ComplianceAuditView(store)
    agent_performance = AgentPerformanceLedger(store)
    daemon = ProjectionDaemon(
        store,
        [application_summary, compliance_audit, agent_performance],
        batch_size=100,
        poll_interval=0.01,
    )
    mcp = create_mcp_server(
        store=store,
        application_summary=application_summary,
        compliance_audit=compliance_audit,
        agent_performance=agent_performance,
        daemon=daemon,
    )
    return mcp
async def _call_tool_json(mcp, tool_name: str, **arguments):
    result = await mcp.call_tool(tool_name, arguments)
    payload = json.loads(result.structured_content["result"])
    return payload, result


async def _read_resource_json(mcp, uri: str):
    result = await mcp.read_resource(uri)
    payload = json.loads(result.contents[0].content)
    return payload, result


@pytest.mark.asyncio
async def test_full_lifecycle_via_mcp_tools_and_resources(mcp_server):
    mcp = mcp_server
    application_id = "APEX-MCP-001"
    orchestrator_session_id = "sess-orc-mcp-001"
    now = datetime.now()

    start_payload = {
        "session_id": orchestrator_session_id,
        "agent_type": "decision_orchestrator",
        "agent_id": "orchestrator-1",
        "application_id": application_id,
        "model_version": "claude-sonnet-4-20250514",
        "langgraph_graph_version": "graph-v1",
        "context_source": "loan-stream",
        "context_token_count": 512,
        "started_at": now.isoformat(),
    }
    start_response, _ = await _call_tool_json(mcp, "start_agent_session", command=start_payload)

    submit_payload = {
        "application_id": application_id,
        "applicant_id": "COMP-MCP-001",
        "requested_amount_usd": "500000",
        "loan_purpose": LoanPurpose.WORKING_CAPITAL.value,
        "loan_term_months": 36,
        "submission_channel": "web",
        "contact_email": "founder@example.com",
        "contact_name": "Founder One",
        "submitted_at": now.isoformat(),
        "application_reference": application_id,
        "deadline": (now + timedelta(days=7)).isoformat(),
        "requested_by": "system",
        "required_document_types": [
            "application_proposal",
            "income_statement",
            "balance_sheet",
        ],
    }
    submit_response, _ = await _call_tool_json(mcp, "submit_application", command=submit_payload)

    credit_session_response, _ = await _call_tool_json(
        mcp,
        "start_agent_session",
        command={
            "session_id": "sess-crd-mcp-001",
            "agent_type": "credit_analysis",
            "agent_id": "credit-agent-1",
            "application_id": application_id,
            "model_version": "claude-sonnet-4-20250514",
            "langgraph_graph_version": "graph-v1",
            "context_source": "loan-stream",
            "context_token_count": 384,
            "started_at": (now + timedelta(seconds=30)).isoformat(),
        },
    )
    fraud_session_response, _ = await _call_tool_json(
        mcp,
        "start_agent_session",
        command={
            "session_id": "sess-frd-mcp-001",
            "agent_type": "fraud_detection",
            "agent_id": "fraud-agent-1",
            "application_id": application_id,
            "model_version": "fraud-v2026.03",
            "langgraph_graph_version": "graph-v1",
            "context_source": "loan-stream",
            "context_token_count": 192,
            "started_at": (now + timedelta(seconds=45)).isoformat(),
        },
    )
    compliance_session_response, _ = await _call_tool_json(
        mcp,
        "start_agent_session",
        command={
            "session_id": "sess-cmp-mcp-001",
            "agent_type": "compliance",
            "agent_id": "compliance-agent-1",
            "application_id": application_id,
            "model_version": "rules-v2026.03",
            "langgraph_graph_version": "graph-v1",
            "context_source": "loan-stream",
            "context_token_count": 224,
            "started_at": (now + timedelta(minutes=1)).isoformat(),
        },
    )

    credit_response, _ = await _call_tool_json(
        mcp,
        "record_credit_analysis",
        command={
            "application_id": application_id,
            "session_id": "sess-crd-mcp-001",
            "model_version": "claude-sonnet-4-20250514",
            "triggered_by_event_id": "credit-completed-mcp-001",
            "requested_at": (now + timedelta(minutes=1)).isoformat(),
            "loan_source_events": [
                CreditAnalysisRequested(
                    application_id=application_id,
                    requested_at=now,
                    requested_by="system",
                    priority="NORMAL",
                ).to_store_dict()
            ],
            "document_package_source_events": [
                PackageReadyForAnalysis(
                    package_id=application_id,
                    application_id=application_id,
                    documents_processed=3,
                    has_quality_flags=False,
                    quality_flag_count=0,
                    ready_at=now,
                ).to_store_dict()
            ],
            "source_events": [
                CreditAnalysisCompleted(
                    application_id=application_id,
                    session_id="sess-crd-mcp-001",
                    decision=CreditDecision(
                        risk_tier=RiskTier.MEDIUM,
                        recommended_limit_usd=Decimal("350000"),
                        confidence=0.72,
                        rationale="Stable business with manageable leverage.",
                        key_concerns=["Customer concentration"],
                        data_quality_caveats=[],
                        policy_overrides_applied=[],
                    ),
                    model_version="claude-sonnet-4-20250514",
                    model_deployment_id="dep-credit-mcp-001",
                    input_data_hash="hash-credit-mcp-001",
                    analysis_duration_ms=1350,
                    regulatory_basis=["CREDIT-POLICY-2026-01"],
                    completed_at=now,
                ).to_store_dict()
            ],
        },
    )

    fraud_response, _ = await _call_tool_json(
        mcp,
        "record_fraud_screening",
        command={
            "application_id": application_id,
            "session_id": "sess-frd-mcp-001",
            "triggered_by_event_id": "fraud-completed-mcp-001",
            "requested_at": (now + timedelta(minutes=2)).isoformat(),
            "source_events": [
                FraudScreeningCompleted(
                    application_id=application_id,
                    session_id="sess-frd-mcp-001",
                    fraud_score=0.18,
                    risk_level="LOW",
                    anomalies_found=0,
                    recommendation="CLEAR",
                    screening_model_version="fraud-v2026.03",
                    input_data_hash="hash-fraud-mcp-001",
                    completed_at=now + timedelta(minutes=2),
                ).to_store_dict()
            ],
        },
    )

    compliance_response, _ = await _call_tool_json(
        mcp,
        "record_compliance_check",
        command={
            "application_id": application_id,
            "session_id": "sess-cmp-mcp-001",
            "triggered_by_event_id": "compliance-completed-mcp-001",
            "requested_at": (now + timedelta(minutes=5)).isoformat(),
            "has_hard_block": False,
            "decline_reasons": [],
            "adverse_action_codes": [],
            "source_events": [
                ComplianceCheckInitiated(
                    application_id=application_id,
                    session_id="sess-cmp-mcp-001",
                    regulation_set_version="2026-Q1",
                    rules_to_evaluate=["REG-001", "REG-004"],
                    initiated_at=now + timedelta(minutes=3),
                ).to_store_dict(),
                ComplianceRulePassed(
                    application_id=application_id,
                    session_id="sess-cmp-mcp-001",
                    rule_id="REG-001",
                    rule_name="KYC documents complete",
                    rule_version="2026-Q1",
                    evidence_hash="evidence-pass-001",
                    evaluation_notes="No issues detected.",
                    evaluated_at=now + timedelta(minutes=4),
                ).to_store_dict(),
                ComplianceRuleNoted(
                    application_id=application_id,
                    session_id="sess-cmp-mcp-001",
                    rule_id="REG-004",
                    rule_name="Enhanced monitoring",
                    note_type="INFO",
                    note_text="Quarterly covenant review required.",
                    evaluated_at=now + timedelta(minutes=4),
                ).to_store_dict(),
                ComplianceCheckCompleted(
                    application_id=application_id,
                    session_id="sess-cmp-mcp-001",
                    rules_evaluated=2,
                    rules_passed=1,
                    rules_failed=0,
                    rules_noted=1,
                    has_hard_block=False,
                    overall_verdict=ComplianceVerdict.CLEAR,
                    completed_at=now + timedelta(minutes=5),
                ).to_store_dict(),
            ],
        },
    )

    decision_response, _ = await _call_tool_json(
        mcp,
        "generate_decision",
        command={
            "application_id": application_id,
            "orchestrator_session_id": orchestrator_session_id,
            "recommendation": "REFER",
            "confidence": 0.55,
            "executive_summary": "Borderline confidence; route to human review.",
            "generated_at": (now + timedelta(minutes=6)).isoformat(),
            "approved_amount_usd": "425000",
            "conditions": ["Quarterly reporting"],
            "key_risks": ["Customer concentration"],
            "contributing_sessions": ["sess-crd-mcp-001", "sess-frd-mcp-001", "sess-cmp-mcp-001"],
            "model_versions": {"orchestrator": "claude-sonnet-4-20250514"},
            "assigned_to": "reviewer-1",
        },
    )

    human_review_response, _ = await _call_tool_json(
        mcp,
        "record_human_review",
        command={
            "application_id": application_id,
            "reviewer_id": "reviewer-1",
            "override": True,
            "original_recommendation": "REFER",
            "final_decision": "APPROVE",
            "reviewed_at": (now + timedelta(minutes=7)).isoformat(),
            "override_reason": "Additional collateral package approved manually.",
            "approved_amount_usd": "400000",
            "interest_rate_pct": 7.25,
            "term_months": 36,
            "conditions": ["Quarterly reporting", "Monthly borrowing-base certificate"],
            "decline_reasons": [],
            "adverse_action_notice_required": False,
            "adverse_action_codes": [],
        },
    )

    compliance_view, _ = await _read_resource_json(mcp, f"ledger://applications/{application_id}/compliance")
    application_view, _ = await _read_resource_json(mcp, f"ledger://applications/{application_id}")

    assert start_response["status"] == "ok"
    assert start_response["stream_id"] == f"agent-decision_orchestrator-{orchestrator_session_id}"
    assert start_response["stream_positions"] == [0, 1]
    assert credit_session_response["status"] == "ok"
    assert fraud_session_response["status"] == "ok"
    assert compliance_session_response["status"] == "ok"

    assert submit_response["status"] == "ok"
    assert submit_response["stream_positions"] == [0, 1]

    assert credit_response["status"] == "ok"
    assert credit_response["stream_positions"] == [3]

    assert fraud_response["status"] == "ok"
    assert fraud_response["stream_positions"] == [4]

    assert compliance_response["status"] == "ok"
    assert compliance_response["stream_positions"] == [5]

    assert decision_response["status"] == "ok"
    assert decision_response["stream_positions"] == [6, 7]

    assert human_review_response["status"] == "ok"
    assert human_review_response["stream_positions"] == [8, 9]

    assert compliance_view is not None
    assert compliance_view["application_id"] == application_id
    assert compliance_view["session_id"] == "sess-cmp-mcp-001"
    assert compliance_view["overall_verdict"] == "CLEAR"
    assert compliance_view["has_hard_block"] is False
    assert compliance_view["rules_evaluated"] == 2
    assert len(compliance_view["rule_results"]) == 1
    assert len(compliance_view["note_results"]) == 1
    assert compliance_view["final_decision"] == "APPROVED"

    assert application_view is not None
    assert application_view["application_id"] == application_id
    assert application_view["applicant_id"] == "COMP-MCP-001"
    assert application_view["state"] == "APPROVED"
    assert application_view["final_decision"] == "APPROVED"
    assert application_view["risk_tier"] == "MEDIUM"
    assert application_view["fraud_risk_level"] == "LOW"
    assert application_view["compliance_status"] == "CLEAR"
    assert application_view["compliance_hard_block"] is False
    assert Decimal(str(application_view["approved_amount_usd"])) == Decimal("400000")