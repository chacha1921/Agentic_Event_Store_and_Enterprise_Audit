from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime
from decimal import Decimal

import pytest
import pytest_asyncio

from ledger.domain.aggregates.agent_session import SessionHealthStatus, reconstruct_agent_context
from ledger.event_store import EventStore, InMemoryEventStore
from ledger.integrity import run_integrity_check
from ledger.upcasters import UpcasterRegistry
from ledger.schema.events import (
    AgentContextLoaded,
    AgentNodeExecuted,
    AgentOutputWritten,
    AgentSessionStarted,
    AgentType,
    DecisionGenerated,
)


@pytest_asyncio.fixture
async def store() -> InMemoryEventStore:
    return InMemoryEventStore(upcaster_registry=UpcasterRegistry())


pytestmark = []


@pytest_asyncio.fixture
async def postgres_store():
    db_url = os.environ.get("DATABASE_URL") or os.environ.get("TEST_DB_URL")
    if not db_url:
        pytest.skip("Requires PostgreSQL via DATABASE_URL or TEST_DB_URL")
    store = EventStore(db_url, upcaster_registry=UpcasterRegistry())
    await store.connect()
    yield store
    await store.close()


def _hash_payload(payload: dict) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    ).hexdigest()


@pytest.mark.asyncio
async def test_credit_analysis_is_upcast_on_stream_reads_without_mutating_stored_event(store: InMemoryEventStore):
    event = {
        "event_type": "CreditAnalysisCompleted",
        "event_version": 1,
        "payload": {
            "application_id": "APP-001",
            "session_id": "sess-001",
            "decision": {
                "risk_tier": "MEDIUM",
                "recommended_limit_usd": "150000",
                "confidence": 0.67,
                "rationale": "Legacy decision",
                "key_concerns": [],
                "data_quality_caveats": [],
                "policy_overrides_applied": [],
            },
            "model_deployment_id": "dep-legacy-001",
            "input_data_hash": "hash-legacy-001",
            "analysis_duration_ms": 800,
            "completed_at": datetime.now().isoformat(),
        },
    }
    await store.append("credit-APP-001", [event], expected_version=-1)
    store._streams["credit-APP-001"][0] = store._streams["credit-APP-001"][0].model_copy(
        update={"recorded_at": datetime.fromisoformat("2025-12-15T00:00:00+00:00")}
    )
    store._global[0] = store._streams["credit-APP-001"][0]

    loaded = await store.load_stream("credit-APP-001")
    raw_stored = store._streams["credit-APP-001"][0]

    assert loaded[0]["event_version"] == 2
    assert loaded[0]["payload"]["model_version"] == "legacy-pre-2026"
    assert "confidence_score" in loaded[0]["payload"]
    assert loaded[0]["payload"]["confidence_score"] is None
    assert loaded[0]["payload"]["regulatory_basis"] == ["CREDIT-POLICY-2025-09"]

    assert raw_stored["event_version"] == 1
    assert "model_version" not in raw_stored["payload"]
    assert "confidence_score" not in raw_stored["payload"]


@pytest.mark.asyncio
async def test_credit_analysis_postgres_raw_payload_remains_unchanged_after_read_time_upcast(postgres_store: EventStore):
    stream_id = f"credit-PG-UPCAST-{datetime.now().timestamp()}"
    legacy_event = {
        "event_type": "CreditAnalysisCompleted",
        "event_version": 1,
        "payload": {
            "application_id": stream_id.removeprefix("credit-"),
            "session_id": "sess-pg-credit-001",
            "decision": {
                "risk_tier": "MEDIUM",
                "recommended_limit_usd": "150000",
                "confidence": 0.67,
                "rationale": "Legacy decision",
                "key_concerns": [],
                "data_quality_caveats": [],
                "policy_overrides_applied": [],
            },
            "model_deployment_id": "dep-legacy-001",
            "input_data_hash": "hash-legacy-001",
            "analysis_duration_ms": 800,
            "completed_at": datetime.now().isoformat(),
        },
    }
    await postgres_store.append(stream_id, [legacy_event], expected_version=-1)

    pool = postgres_store._require_pool()
    async with pool.acquire() as conn:
        raw_before = await conn.fetchrow(
            "SELECT event_version, payload FROM events WHERE stream_id = $1 ORDER BY stream_position ASC LIMIT 1",
            stream_id,
        )

    loaded = await postgres_store.load_stream(stream_id)

    async with pool.acquire() as conn:
        raw_after = await conn.fetchrow(
            "SELECT event_version, payload FROM events WHERE stream_id = $1 ORDER BY stream_position ASC LIMIT 1",
            stream_id,
        )

    assert raw_before is not None
    assert raw_after is not None
    assert raw_before["event_version"] == 1
    assert raw_after["event_version"] == 1
    assert dict(raw_before["payload"]) == dict(raw_after["payload"])
    assert loaded[0]["event_version"] == 2
    assert "model_version" in loaded[0]["payload"]
    assert "confidence_score" in loaded[0]["payload"]
    assert "model_version" not in dict(raw_after["payload"])
    assert "confidence_score" not in dict(raw_after["payload"])


@pytest.mark.asyncio
async def test_decision_generated_is_upcast_on_global_reads(store: InMemoryEventStore):
    await store.append(
        "agent-decision_orchestrator-sess-orc-001",
        [
            AgentContextLoaded(
                session_id="sess-orc-001",
                agent_type=AgentType.DECISION_ORCHESTRATOR,
                application_id="APP-002",
                context_source="loan-stream",
                context_token_count=256,
                loaded_at=datetime.now(),
            ).to_store_dict(),
            AgentSessionStarted(
                session_id="sess-orc-001",
                agent_type=AgentType.DECISION_ORCHESTRATOR,
                agent_id="orchestrator-1",
                application_id="APP-002",
                model_version="claude-sonnet-4-20250514",
                langgraph_graph_version="graph-v1",
                context_source="loan-stream",
                context_token_count=256,
                started_at=datetime.now(),
            ).to_store_dict(),
        ],
        expected_version=-1,
    )
    await store.append(
        "agent-credit_analysis-sess-crd-001",
        [
            AgentContextLoaded(
                session_id="sess-crd-001",
                agent_type=AgentType.CREDIT_ANALYSIS,
                application_id="APP-002",
                context_source="loan-stream",
                context_token_count=128,
                loaded_at=datetime.now(),
            ).to_store_dict(),
            AgentSessionStarted(
                session_id="sess-crd-001",
                agent_type=AgentType.CREDIT_ANALYSIS,
                agent_id="credit-agent-1",
                application_id="APP-002",
                model_version="credit-model-v1",
                langgraph_graph_version="graph-v1",
                context_source="loan-stream",
                context_token_count=128,
                started_at=datetime.now(),
            ).to_store_dict(),
        ],
        expected_version=-1,
    )

    event = {
        "event_type": "DecisionGenerated",
        "event_version": 1,
        "payload": {
            "application_id": "APP-002",
            "orchestrator_session_id": "sess-orc-001",
            "recommendation": "APPROVE",
            "confidence": 0.91,
            "approved_amount_usd": "120000",
            "conditions": [],
            "executive_summary": "Legacy summary",
            "key_risks": [],
            "contributing_sessions": ["sess-crd-001"],
            "generated_at": datetime.now().isoformat(),
        },
    }
    await store.append("loan-APP-002", [event], expected_version=-1)

    loaded = [item async for item in store.load_all()]
    decision_event = next(item for item in loaded if item["event_type"] == "DecisionGenerated")
    raw_decision = next(item for item in store._global if item["event_type"] == "DecisionGenerated")

    assert decision_event["event_version"] == 2
    assert decision_event["payload"]["model_versions"] == {
        "orchestrator": "claude-sonnet-4-20250514",
        "credit_analysis": "credit-model-v1",
    }
    assert raw_decision["event_version"] == 1
    assert "model_versions" not in raw_decision["payload"]


@pytest.mark.asyncio
async def test_run_integrity_check_chains_hashes_across_runs(store: InMemoryEventStore):
    event_one = {
        "event_type": "ApplicationSubmitted",
        "event_version": 1,
        "payload": {
            "application_id": "APP-003",
            "applicant_id": "COMP-003",
            "requested_amount_usd": "100000",
            "loan_purpose": "working_capital",
            "loan_term_months": 12,
            "submission_channel": "web",
            "contact_email": "borrower@example.com",
            "contact_name": "Borrower",
            "submitted_at": datetime.now().isoformat(),
            "application_reference": "APP-003",
        },
    }
    event_two = {
        "event_type": "DecisionRequested",
        "event_version": 1,
        "payload": {
            "application_id": "APP-003",
            "requested_at": datetime.now().isoformat(),
            "all_analyses_complete": True,
            "triggered_by_event_id": "evt-123",
        },
    }

    await store.append("loan-APP-003", [event_one, event_two], expected_version=-1)
    first = await run_integrity_check(store, "loan", "APP-003")

    expected_first_hash = hashlib.sha256(
        ("" + _hash_payload(event_one["payload"]) + _hash_payload(event_two["payload"])).encode("utf-8")
    ).hexdigest()
    assert first["integrity_hash"] == expected_first_hash
    assert first["events_verified_count"] == 2
    assert first["previous_hash"] is None

    event_three = {
        "event_type": "ApplicationApproved",
        "event_version": 1,
        "payload": {
            "application_id": "APP-003",
            "approved_amount_usd": "90000",
            "interest_rate_pct": 7.1,
            "term_months": 12,
            "conditions": [],
            "approved_by": "officer-1",
            "effective_date": "2026-03-25",
            "approved_at": datetime.now().isoformat(),
        },
    }
    await store.append("loan-APP-003", [event_three], expected_version=1)
    second = await run_integrity_check(store, "loan", "APP-003")

    expected_second_hash = hashlib.sha256(
        (first["integrity_hash"] + _hash_payload(event_three["payload"])).encode("utf-8")
    ).hexdigest()
    assert second["previous_hash"] == first["integrity_hash"]
    assert second["integrity_hash"] == expected_second_hash
    assert second["events_verified_count"] == 3

    audit_events = await store.load_stream("audit-loan-APP-003")
    assert len(audit_events) == 2
    assert audit_events[-1]["payload"]["previous_hash"] == first["integrity_hash"]


@pytest.mark.asyncio
async def test_run_integrity_check_detects_tampering_in_previously_verified_history(store: InMemoryEventStore):
    stream_id = "loan-APP-TAMPER-001"
    event = {
        "event_type": "ApplicationSubmitted",
        "event_version": 1,
        "payload": {
            "application_id": "APP-TAMPER-001",
            "applicant_id": "COMP-TAMPER-001",
            "requested_amount_usd": "100000",
            "loan_purpose": "working_capital",
            "loan_term_months": 12,
            "submission_channel": "web",
            "contact_email": "borrower@example.com",
            "contact_name": "Borrower",
            "submitted_at": datetime.now().isoformat(),
            "application_reference": "APP-TAMPER-001",
        },
    }
    await store.append(stream_id, [event], expected_version=-1)
    first = await run_integrity_check(store, "loan", "APP-TAMPER-001")
    assert first["tamper_detected"] is False

    tampered = store._streams[stream_id][0].model_copy(
        update={
            "payload": {
                **store._streams[stream_id][0].payload,
                "contact_name": "Tampered Borrower",
            }
        }
    )
    store._streams[stream_id][0] = tampered
    store._global[0] = tampered

    second = await run_integrity_check(store, "loan", "APP-TAMPER-001")
    assert second["tamper_detected"] is True
    assert second["chain_valid"] is False
    assert second["events_verified"] == 1


@pytest.mark.asyncio
async def test_reconstruct_agent_context_after_simulated_crash_contains_resume_information(store: InMemoryEventStore):
    session_id = "sess-phase4-crash-001"
    application_id = "APP-PHASE4-001"
    stream_id = f"agent-{AgentType.DECISION_ORCHESTRATOR.value}-{session_id}"
    now = datetime.now()

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
        AgentNodeExecuted(
            session_id=session_id,
            agent_type=AgentType.DECISION_ORCHESTRATOR,
            node_name="collect_signals",
            node_sequence=1,
            input_keys=["application"],
            output_keys=["signals"],
            llm_called=False,
            llm_tokens_input=0,
            llm_tokens_output=0,
            llm_cost_usd=0.0,
            duration_ms=20,
            executed_at=now,
        ).to_store_dict(),
        AgentNodeExecuted(
            session_id=session_id,
            agent_type=AgentType.DECISION_ORCHESTRATOR,
            node_name="draft_decision",
            node_sequence=2,
            input_keys=["signals"],
            output_keys=["recommendation"],
            llm_called=True,
            llm_tokens_input=250,
            llm_tokens_output=80,
            llm_cost_usd=0.01,
            duration_ms=410,
            executed_at=now,
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
                    confidence=0.58,
                    approved_amount_usd=Decimal("250000"),
                    conditions=["Quarterly reporting"],
                    executive_summary="Partial decision before crash.",
                    key_risks=["Concentration"],
                    contributing_sessions=[],
                    model_versions={"orchestrator": "claude-sonnet-4-20250514"},
                    generated_at=now,
                ).to_store_dict()
            ],
            output_summary="Decision written before completion event.",
            written_at=now,
        ).to_store_dict(),
    ]
    await store.append(stream_id, events, expected_version=-1)

    recovered = await reconstruct_agent_context(
        store,
        agent_id=AgentType.DECISION_ORCHESTRATOR.value,
        session_id=session_id,
    )

    assert recovered.last_event_position == 4
    assert recovered.session_health_status == SessionHealthStatus.NEEDS_RECONCILIATION
    assert recovered.pending_work
    assert any("Reconcile decision/output written" in item.description for item in recovered.pending_work)
    assert "Older session summary:" in recovered.context_text
    assert "AgentOutputWritten" in recovered.context_text
