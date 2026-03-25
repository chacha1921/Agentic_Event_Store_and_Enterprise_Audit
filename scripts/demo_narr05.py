import asyncio
import json
from datetime import datetime, timedelta
from decimal import Decimal

from ledger.event_store import EventStore
from ledger.mcp_server import create_mcp_server
from ledger.projections import (
    AgentPerformanceLedger,
    ApplicationSummary,
    ComplianceAuditView,
    ProjectionDaemon,
)
from ledger.schema.events import LoanPurpose


async def main():
    print("=== Starting The Ledger: NARR-05 Demo (Human Override) ===")
    
    # 1. Initialize Infrastructure
    store = EventStore() # Assuming you have a default Postgres connection setup here
    summary = ApplicationSummary(store)
    compliance = ComplianceAuditView(store)
    performance = AgentPerformanceLedger(store)
    
    daemon = ProjectionDaemon(store, [summary, compliance, performance])
    mcp = create_mcp_server(store, summary, compliance, performance, daemon)
    
    app_id = "APEX-NARR05"
    now = datetime.now()
    
    print("\n[1] Starting Orchestrator Session...")
    await mcp.call_tool("start_agent_session", {
        "session_id": "sess-orc-005",
        "agent_type": "decision_orchestrator",
        "agent_id": "orchestrator-1",
        "application_id": app_id,
        "model_version": "claude-sonnet-4-20250514",
        "langgraph_graph_version": "graph-v1",
        "context_source": "fresh",
        "context_token_count": 512,
        "started_at": now.isoformat(),
    })

    print("[2] Submitting Loan Application...")
    await mcp.call_tool("submit_application", {
        "application_id": app_id,
        "applicant_id": "COMP-068", # The 15-year bank customer
        "requested_amount_usd": "950000",
        "loan_purpose": LoanPurpose.WORKING_CAPITAL.value,
        "loan_term_months": 36,
        "submission_channel": "web",
        "contact_email": "ceo@comp068.com",
        "contact_name": "Sarah Connor",
        "submitted_at": now.isoformat(),
        "application_reference": app_id,
        "deadline": (now + timedelta(days=7)).isoformat(),
        "requested_by": "system",
        "required_document_types": ["income_statement", "balance_sheet"],
    })

    # Note: In a real demo, you would call the actual LangGraph agents here.
    # For the infrastructure demo, we execute the decision tool directly to show the override.
    print("[3] AI Orchestrator Generates Decision (REFER)...")
    await mcp.call_tool("generate_decision", {
        "application_id": app_id,
        "orchestrator_session_id": "sess-orc-005",
        "recommendation": "REFER",
        "confidence": 0.55,
        "executive_summary": "High risk, low confidence. 15-year customer but declining revenue.",
        "generated_at": (now + timedelta(minutes=5)).isoformat(),
        "approved_amount_usd": None,
        "conditions": [],
        "key_risks": ["Declining revenue (-8% YoY)", "High leverage"],
        "contributing_sessions": [],
        "model_versions": {"orchestrator": "claude-sonnet-4-20250514"},
        "assigned_to": "LO-Sarah-Chen",
    })

    print("[4] Human Loan Officer Overrides to APPROVE...")
    await mcp.call_tool("record_human_review", {
        "application_id": app_id,
        "reviewer_id": "LO-Sarah-Chen",
        "override": True,
        "original_recommendation": "REFER",
        "final_decision": "APPROVE",
        "reviewed_at": (now + timedelta(minutes=10)).isoformat(),
        "override_reason": "15-year customer, prior repayment history, collateral offered",
        "approved_amount_usd": "750000",
        "interest_rate_pct": 7.5,
        "term_months": 36,
        "conditions": ["Monthly revenue reporting for 12 months", "Personal guarantee from CEO"],
        "decline_reasons": [],
        "adverse_action_notice_required": False,
        "adverse_action_codes": [],
    })
    
    print("\n=== Processing Projections ===")
    await daemon.run_once()
    
    print("\n[5] Final Application State (via MCP Resource):")
    app_view = await mcp.read_resource(f"ledger://applications/{app_id}")
    print(json.dumps(json.loads(app_view.contents[0].content), indent=2))
    
    print("\n=== Demo Complete ===")

if __name__ == "__main__":
    asyncio.run(main())