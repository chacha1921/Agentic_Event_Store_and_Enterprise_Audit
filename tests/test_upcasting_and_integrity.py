from __future__ import annotations

import hashlib
import json
from datetime import datetime

import pytest
import pytest_asyncio

from ledger.event_store import InMemoryEventStore
from ledger.integrity import run_integrity_check
from ledger.upcasters import UpcasterRegistry


@pytest_asyncio.fixture
async def store() -> InMemoryEventStore:
    return InMemoryEventStore(upcaster_registry=UpcasterRegistry())


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

    loaded = await store.load_stream("credit-APP-001")
    raw_stored = store._streams["credit-APP-001"][0]

    assert loaded[0]["event_version"] == 2
    assert loaded[0]["payload"]["model_version"] == "legacy-pre-2026"
    assert "confidence_score" in loaded[0]["payload"]
    assert loaded[0]["payload"]["confidence_score"] is None
    assert loaded[0]["payload"]["regulatory_basis"] == []

    assert raw_stored["event_version"] == 1
    assert "model_version" not in raw_stored["payload"]
    assert "confidence_score" not in raw_stored["payload"]


@pytest.mark.asyncio
async def test_decision_generated_is_upcast_on_global_reads(store: InMemoryEventStore):
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
            "contributing_sessions": [],
            "generated_at": datetime.now().isoformat(),
        },
    }
    await store.append("loan-APP-002", [event], expected_version=-1)

    loaded = [item async for item in store.load_all()]

    assert loaded[0]["event_version"] == 2
    assert loaded[0]["payload"]["model_versions"] == {}
    assert store._global[0]["event_version"] == 1
    assert "model_versions" not in store._global[0]["payload"]


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