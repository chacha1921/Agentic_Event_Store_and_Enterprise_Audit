from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from ledger.event_store import EventStore, InMemoryEventStore, StoredEvent
from ledger.integrity import run_integrity_check
from ledger.projections import ApplicationSummary


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


def _extract_agent_provenance(events: list[StoredEvent], application_id: str) -> list[dict[str, Any]]:
    provenance: list[dict[str, Any]] = []
    seen: set[tuple[Any, ...]] = set()

    for event in events:
        payload = event.payload or {}
        if payload.get("application_id") != application_id:
            continue

        records: list[dict[str, Any]] = []
        if event.event_type == "AgentSessionStarted":
            records.append(
                {
                    "event_type": event.event_type,
                    "stream_id": event.stream_id,
                    "session_id": payload.get("session_id"),
                    "agent_type": payload.get("agent_type"),
                    "model_version": payload.get("model_version"),
                    "input_data_hash": None,
                }
            )
        elif event.event_type == "CreditAnalysisCompleted":
            records.append(
                {
                    "event_type": event.event_type,
                    "stream_id": event.stream_id,
                    "session_id": payload.get("session_id"),
                    "agent_type": "credit_analysis",
                    "model_version": payload.get("model_version"),
                    "input_data_hash": payload.get("input_data_hash"),
                }
            )
        elif event.event_type == "FraudScreeningCompleted":
            records.append(
                {
                    "event_type": event.event_type,
                    "stream_id": event.stream_id,
                    "session_id": payload.get("session_id"),
                    "agent_type": "fraud_detection",
                    "model_version": payload.get("screening_model_version"),
                    "input_data_hash": payload.get("input_data_hash"),
                }
            )
        elif event.event_type == "DecisionGenerated":
            for agent_name, model_version in (payload.get("model_versions") or {}).items():
                records.append(
                    {
                        "event_type": event.event_type,
                        "stream_id": event.stream_id,
                        "session_id": payload.get("orchestrator_session_id"),
                        "agent_type": agent_name,
                        "model_version": model_version,
                        "input_data_hash": None,
                    }
                )

        for record in records:
            fingerprint = (
                record.get("event_type"),
                record.get("stream_id"),
                record.get("session_id"),
                record.get("agent_type"),
                record.get("model_version"),
                record.get("input_data_hash"),
            )
            if fingerprint in seen:
                continue
            seen.add(fingerprint)
            provenance.append(_jsonable(record))

    return provenance


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
        "events": [_event_to_dict(event) for event in related_events],
        "state_at_examination": await _application_state_at(store, application_id, normalized_examination),
        "integrity_proof": _jsonable(integrity_proof),
        "agent_provenance": _extract_agent_provenance(related_events, application_id),
    }