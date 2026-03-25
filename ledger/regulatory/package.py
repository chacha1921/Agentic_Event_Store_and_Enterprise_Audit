from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from ledger.event_store import EventStore, InMemoryEventStore, StoredEvent
from ledger.integrity import run_integrity_check
from ledger.projections import AgentPerformanceLedger, ApplicationSummary, ComplianceAuditView


def _normalize_timestamp(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _jsonable(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: _jsonable(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_jsonable(item) for item in value]
    return value


def _event_to_dict(event: StoredEvent) -> dict[str, Any]:
    return _jsonable(event.to_dict())


def _is_related_event(event: StoredEvent, application_id: str) -> bool:
    payload = event.payload or {}
    stream_id = event.stream_id
    if payload.get("application_id") == application_id:
        return True
    if payload.get("entity_id") == application_id:
        return True
    if payload.get("package_id") == application_id:
        return True
    if stream_id in {
        f"loan-{application_id}",
        f"docpkg-{application_id}",
        f"credit-{application_id}",
        f"fraud-{application_id}",
        f"compliance-{application_id}",
        f"audit-loan-{application_id}",
    }:
        return True
    return False


async def _application_state_at(store: EventStore, application_id: str, examination_date: datetime) -> dict[str, Any] | None:
    temp_store = InMemoryEventStore()
    summary = ApplicationSummary(temp_store)
    await summary.setup()
    normalized_date = _normalize_timestamp(examination_date)

    async for event in store.load_all(from_position=0):
        if not _is_related_event(event, application_id):
            continue
        if _normalize_timestamp(event.recorded_at) > normalized_date:
            continue
        await summary.apply(event)

    state = await summary.get(application_id)
    return _jsonable(state)


async def _projection_states_at(store: EventStore, application_id: str, examination_date: datetime) -> dict[str, Any]:
    temp_store = InMemoryEventStore()
    normalized_date = _normalize_timestamp(examination_date)
    projections = [
        ApplicationSummary(temp_store),
        ComplianceAuditView(temp_store),
        AgentPerformanceLedger(temp_store),
    ]
    for projection in projections:
        setup = getattr(projection, "setup", None)
        if callable(setup):
            await setup()

    related_events: list[StoredEvent] = []
    async for event in store.load_all(from_global_position=0):
        if not _is_related_event(event, application_id):
            continue
        if _normalize_timestamp(event.recorded_at) > normalized_date:
            continue
        related_events.append(event)
        for projection in projections:
            await projection.apply(event)

    agent_projection = projections[2]
    return {
        projections[0].checkpoint_name: _jsonable(await projections[0].get(application_id)),
        projections[1].checkpoint_name: _jsonable(await projections[1].get_current(application_id)),
        agent_projection.checkpoint_name: _jsonable(list(agent_projection._metrics.values())),
        "events_replayed": len(related_events),
    }


def _extract_agent_provenance(events: list[StoredEvent], application_id: str) -> list[dict[str, Any]]:
    provenance_by_key: dict[tuple[str | None, str | None], dict[str, Any]] = {}

    for event in events:
        payload = event.payload or {}
        if payload.get("application_id") != application_id:
            continue

        if event.event_type == "AgentSessionStarted":
            key = (payload.get("session_id"), str(payload.get("agent_type")))
            provenance_by_key[key] = {
                "session_id": payload.get("session_id"),
                "agent_type": payload.get("agent_type"),
                "model_version": payload.get("model_version"),
                "confidence_score": None,
                "input_data_hash": None,
                "source_events": [event.event_type],
            }
        elif event.event_type == "CreditAnalysisCompleted":
            key = (payload.get("session_id"), "credit_analysis")
            record = provenance_by_key.setdefault(
                key,
                {
                    "session_id": payload.get("session_id"),
                    "agent_type": "credit_analysis",
                    "model_version": payload.get("model_version"),
                    "confidence_score": None,
                    "input_data_hash": None,
                    "source_events": [],
                },
            )
            record["model_version"] = payload.get("model_version") or record.get("model_version")
            record["confidence_score"] = (payload.get("decision") or {}).get("confidence")
            record["input_data_hash"] = payload.get("input_data_hash")
            record["source_events"].append(event.event_type)
        elif event.event_type == "FraudScreeningCompleted":
            key = (payload.get("session_id"), "fraud_detection")
            record = provenance_by_key.setdefault(
                key,
                {
                    "session_id": payload.get("session_id"),
                    "agent_type": "fraud_detection",
                    "model_version": payload.get("screening_model_version"),
                    "confidence_score": payload.get("fraud_score"),
                    "input_data_hash": payload.get("input_data_hash"),
                    "source_events": [],
                },
            )
            record["model_version"] = payload.get("screening_model_version") or record.get("model_version")
            record["confidence_score"] = payload.get("fraud_score")
            record["input_data_hash"] = payload.get("input_data_hash")
            record["source_events"].append(event.event_type)
        elif event.event_type == "ComplianceCheckCompleted":
            key = (payload.get("session_id"), "compliance")
            record = provenance_by_key.setdefault(
                key,
                {
                    "session_id": payload.get("session_id"),
                    "agent_type": "compliance",
                    "model_version": None,
                    "confidence_score": None,
                    "input_data_hash": None,
                    "source_events": [],
                },
            )
            record["source_events"].append(event.event_type)
        elif event.event_type == "DecisionGenerated":
            for agent_name, model_version in (payload.get("model_versions") or {}).items():
                key = (payload.get("orchestrator_session_id"), agent_name)
                record = provenance_by_key.setdefault(
                    key,
                    {
                        "session_id": payload.get("orchestrator_session_id"),
                        "agent_type": agent_name,
                        "model_version": model_version,
                        "confidence_score": payload.get("confidence"),
                        "input_data_hash": None,
                        "source_events": [],
                    },
                )
                record["model_version"] = model_version or record.get("model_version")
                record["confidence_score"] = payload.get("confidence")
                record["source_events"].append(event.event_type)

    return [_jsonable(record) for record in provenance_by_key.values()]


def _narrative_sentence(event: StoredEvent) -> str | None:
    payload = event.payload or {}
    if event.event_type == "ApplicationSubmitted":
        return f"Application {payload.get('application_id')} was submitted for ${payload.get('requested_amount_usd')}."
    if event.event_type == "CreditAnalysisCompleted":
        decision = payload.get("decision") or {}
        return f"Credit analysis completed with risk tier {decision.get('risk_tier')} and confidence {decision.get('confidence')}."
    if event.event_type == "FraudScreeningCompleted":
        return f"Fraud screening completed with risk level {payload.get('risk_level')} and score {payload.get('fraud_score')}."
    if event.event_type == "ComplianceCheckCompleted":
        return f"Compliance review completed with verdict {payload.get('overall_verdict')} and hard block={payload.get('has_hard_block')}."
    if event.event_type == "DecisionGenerated":
        return f"The orchestrator generated a {payload.get('recommendation')} recommendation with confidence {payload.get('confidence')}."
    if event.event_type == "HumanReviewCompleted":
        return f"A human reviewer finalized the decision as {payload.get('final_decision')} (override={payload.get('override')})."
    if event.event_type == "ApplicationApproved":
        return f"The application was approved for ${payload.get('approved_amount_usd')}."
    if event.event_type == "ApplicationDeclined":
        reasons = ", ".join(payload.get("decline_reasons") or []) or "unspecified reasons"
        return f"The application was declined for {reasons}."
    return None


def _build_narrative(events: list[StoredEvent]) -> list[str]:
    narrative: list[str] = []
    for event in events:
        sentence = _narrative_sentence(event)
        if sentence:
            narrative.append(sentence)
    return narrative


async def generate_regulatory_package(
    store: EventStore,
    application_id: str,
    examination_date: datetime,
) -> dict[str, Any]:
    """Assemble a JSON-serializable examination package for one application."""
    normalized_examination = _normalize_timestamp(examination_date)
    integrity_proof = await run_integrity_check(store, "loan", application_id)

    related_events: list[StoredEvent] = []
    async for event in store.load_all(from_position=0):
        if _is_related_event(event, application_id):
            related_events.append(event)

    related_events.sort(key=lambda event: (event.global_position, event.stream_position))

    return {
        "application_id": application_id,
        "examination_date": normalized_examination.isoformat(),
        "event_stream": [_event_to_dict(event) for event in related_events],
        "events_by_stream": _jsonable(
            {
                stream_id: [_event_to_dict(event) for event in related_events if event.stream_id == stream_id]
                for stream_id in sorted({event.stream_id for event in related_events})
            }
        ),
        "projection_states_at_examination": await _projection_states_at(store, application_id, normalized_examination),
        "state_at_examination": await _application_state_at(store, application_id, normalized_examination),
        "integrity_verification": _jsonable(integrity_proof),
        "lifecycle_narrative": _build_narrative(related_events),
        "agent_provenance": _extract_agent_provenance(related_events, application_id),
    }