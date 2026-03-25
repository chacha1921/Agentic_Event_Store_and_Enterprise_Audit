from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Protocol
from uuid import uuid4

from ledger.event_store import EventStore, InMemoryEventStore, StoredEvent
from ledger.projections import AgentPerformanceLedger, ApplicationSummary, ComplianceAuditView
from ledger.schema.events import BaseEvent


class Projection(Protocol):
    checkpoint_name: str

    async def apply(self, event: StoredEvent) -> None: ...


CAUSAL_SKIP_BY_BRANCH: dict[str, set[str]] = {
    "ApplicationSubmitted": {
        "DocumentUploadRequested",
        "CreditAnalysisRequested",
        "FraudScreeningRequested",
        "ComplianceCheckRequested",
        "DecisionRequested",
        "DecisionGenerated",
        "HumanReviewRequested",
        "HumanReviewCompleted",
        "ApplicationApproved",
        "ApplicationDeclined",
    },
    "CreditAnalysisRequested": {
        "CreditAnalysisCompleted",
        "FraudScreeningRequested",
        "FraudScreeningCompleted",
        "ComplianceCheckRequested",
        "ComplianceCheckInitiated",
        "ComplianceCheckCompleted",
        "DecisionRequested",
        "DecisionGenerated",
        "HumanReviewRequested",
        "HumanReviewCompleted",
        "ApplicationApproved",
        "ApplicationDeclined",
    },
    "CreditAnalysisCompleted": {
        "FraudScreeningRequested",
        "FraudScreeningCompleted",
        "ComplianceCheckRequested",
        "ComplianceCheckInitiated",
        "ComplianceCheckCompleted",
        "DecisionRequested",
        "DecisionGenerated",
        "HumanReviewRequested",
        "HumanReviewCompleted",
        "ApplicationApproved",
        "ApplicationDeclined",
    },
    "FraudScreeningRequested": {
        "ComplianceCheckRequested",
        "DecisionRequested",
        "DecisionGenerated",
        "HumanReviewRequested",
        "HumanReviewCompleted",
        "ApplicationApproved",
        "ApplicationDeclined",
    },
    "ComplianceCheckRequested": {
        "DecisionRequested",
        "DecisionGenerated",
        "HumanReviewRequested",
        "HumanReviewCompleted",
        "ApplicationApproved",
        "ApplicationDeclined",
    },
    "DecisionRequested": {
        "DecisionGenerated",
        "HumanReviewRequested",
        "HumanReviewCompleted",
        "ApplicationApproved",
        "ApplicationDeclined",
    },
    "DecisionGenerated": {
        "HumanReviewRequested",
        "HumanReviewCompleted",
        "ApplicationApproved",
        "ApplicationDeclined",
    },
    "HumanReviewRequested": {"HumanReviewCompleted", "ApplicationApproved", "ApplicationDeclined"},
    "HumanReviewCompleted": {"ApplicationApproved", "ApplicationDeclined"},
}


def _is_related_event(event: StoredEvent, application_id: str) -> bool:
    payload = event.payload or {}
    if payload.get("application_id") == application_id:
        return True
    if payload.get("package_id") == application_id:
        return True
    if payload.get("entity_id") == application_id:
        return True
    return event.stream_id in {
        f"loan-{application_id}",
        f"docpkg-{application_id}",
        f"credit-{application_id}",
        f"fraud-{application_id}",
        f"compliance-{application_id}",
        f"audit-loan-{application_id}",
    }


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


def _clone_projection(projection: Projection, store: InMemoryEventStore):
    if isinstance(projection, ApplicationSummary):
        return ApplicationSummary(store, table_name=f"{projection.table_name}_what_if")
    if isinstance(projection, ComplianceAuditView):
        return ComplianceAuditView(
            store,
            current_table=f"{projection.current_table}_what_if",
            history_table=f"{projection.history_table}_what_if",
        )
    if isinstance(projection, AgentPerformanceLedger):
        return AgentPerformanceLedger(
            store,
            metrics_table=f"{projection.metrics_table}_what_if",
            session_table=f"{projection.session_table}_what_if",
        )
    raise TypeError(
        "Unsupported projection type for what-if replay: "
        f"{projection.__class__.__name__}. "
        "Pass ApplicationSummary, ComplianceAuditView, or AgentPerformanceLedger instances."
    )


def _synthetic_from_real(event: StoredEvent, ordinal: int) -> StoredEvent:
    scaled_position = event.global_position * 1000
    scaled_stream_position = event.stream_position * 1000
    return event.model_copy(
        update={
            "global_position": scaled_position + ordinal,
            "stream_position": scaled_stream_position + ordinal,
        }
    )


def _synthetic_from_base_event(
    application_id: str,
    event: BaseEvent,
    global_position: int,
    stream_position: int,
) -> StoredEvent:
    recorded_at = _normalize_timestamp(event.recorded_at or datetime.now(timezone.utc))
    stream_prefix = "loan"
    payload = event.to_payload()
    if event.event_type == "CreditAnalysisCompleted":
        stream_prefix = "credit"
    elif event.event_type.startswith("Fraud"):
        stream_prefix = "fraud"
    elif event.event_type.startswith("Compliance"):
        stream_prefix = "compliance"
    elif event.event_type.startswith("Agent"):
        session_id = payload.get("session_id", uuid4().hex)
        agent_type = payload.get("agent_type", "unknown")
        return StoredEvent(
            event_id=str(event.event_id),
            stream_id=f"agent-{agent_type}-{session_id}",
            stream_position=stream_position,
            global_position=global_position,
            event_type=event.event_type,
            event_version=event.event_version,
            payload=payload,
            metadata={},
            recorded_at=recorded_at,
        )

    return StoredEvent(
        event_id=str(event.event_id),
        stream_id=f"{stream_prefix}-{application_id}",
        stream_position=stream_position,
        global_position=global_position,
        event_type=event.event_type,
        event_version=event.event_version,
        payload=payload,
        metadata={},
        recorded_at=recorded_at,
    )


def _depends_on_branch(event: StoredEvent, branch_event: StoredEvent, injected_events: list[StoredEvent]) -> bool:
    payload = event.payload
    metadata = event.metadata or {}
    linked_ids = {branch_event.event_id, *(injected.event_id for injected in injected_events)}

    if event.event_type in CAUSAL_SKIP_BY_BRANCH.get(branch_event.event_type, set()):
        return True

    for key in ("triggered_by_event_id", "decision_event_id", "causation_id"):
        value = payload.get(key) or metadata.get(key)
        if value in linked_ids:
            return True

    branch_correlation = branch_event.metadata.get("correlation_id")
    if branch_correlation and metadata.get("correlation_id") == branch_correlation:
        return True

    return False


async def _load_related_events(store: EventStore, application_id: str) -> list[StoredEvent]:
    related: list[StoredEvent] = []
    async for event in store.load_all(from_global_position=0):
        if _is_related_event(event, application_id):
            related.append(event)
    related.sort(key=lambda event: (event.global_position, event.stream_position))
    return related


async def _replay_outcome(application_id: str, events: list[StoredEvent], projections: list[Projection]) -> dict[str, Any]:
    scratch_store = InMemoryEventStore()
    projection_clones = [_clone_projection(projection, scratch_store) for projection in projections]
    for projection in projection_clones:
        setup = getattr(projection, "setup", None)
        if callable(setup):
            await setup()
    for event in events:
        for projection in projection_clones:
            await projection.apply(event)
    return {
        projection.checkpoint_name: await _extract_projection_outcome(projection, application_id)
        for projection in projection_clones
    }


async def _extract_projection_outcome(projection: Projection, application_id: str) -> Any:
    if hasattr(projection, "get_current"):
        result = await projection.get_current(application_id)
        return _jsonable(result)
    if hasattr(projection, "get"):
        result = await projection.get(application_id)
        return _jsonable(result)
    if hasattr(projection, "list_metrics"):
        result = await projection.list_metrics(application_id)
        return _jsonable(result)
    if hasattr(projection, "_rows"):
        return _jsonable(getattr(projection, "_rows"))
    if hasattr(projection, "_current"):
        return _jsonable(getattr(projection, "_current"))
    return None


async def run_what_if(
    store: EventStore,
    application_id: str,
    branch_at_event_type: str,
    counterfactual_events: list[BaseEvent],
    projections: list[Projection],
) -> dict[str, Any]:
    """Replay a counterfactual branch entirely in memory without writing to the database."""
    real_events = await _load_related_events(store, application_id)

    branch_index = next((index for index, event in enumerate(real_events) if event.event_type == branch_at_event_type), None)
    if branch_index is None:
        raise ValueError(f"Branch event type '{branch_at_event_type}' was not found for application '{application_id}'")

    branch_event = real_events[branch_index]
    real_outcome = await _replay_outcome(application_id, real_events, projections)

    applied_events: list[StoredEvent] = []
    divergence_events: list[dict[str, Any]] = [
        {
            "type": "branch_replaced",
            "real_event": _jsonable(branch_event.to_dict()),
        }
    ]

    for ordinal, event in enumerate(real_events[:branch_index], start=1):
        synthetic = _synthetic_from_real(event, ordinal)
        applied_events.append(synthetic)

    injected_events: list[StoredEvent] = []
    branch_global_base = branch_event.global_position * 1000
    branch_stream_base = branch_event.stream_position * 1000
    for offset, event in enumerate(counterfactual_events, start=1):
        synthetic = _synthetic_from_base_event(
            application_id,
            event,
            global_position=branch_global_base + offset,
            stream_position=branch_stream_base + offset,
        )
        injected_events.append(synthetic)
        applied_events.append(synthetic)
        divergence_events.append(
            {
                "type": "counterfactual_injected",
                "event": _jsonable(synthetic.to_dict()),
            }
        )

    skipped_events: list[dict[str, Any]] = []
    for ordinal, event in enumerate(real_events[branch_index + 1 :], start=1):
        if _depends_on_branch(event, branch_event, injected_events):
            skipped_event = _jsonable(event.to_dict())
            skipped_events.append(skipped_event)
            divergence_events.append({"type": "real_event_skipped", "event": skipped_event})
            continue
        synthetic = _synthetic_from_real(event, ordinal)
        applied_events.append(synthetic)

    counterfactual_outcome = await _replay_outcome(application_id, applied_events, projections)

    return {
        "application_id": application_id,
        "branch_event": _jsonable(branch_event.to_dict()),
        "real_outcome": real_outcome,
        "counterfactual_events": [_jsonable(event.to_dict()) for event in injected_events],
        "skipped_real_events": skipped_events,
        "divergence_events": divergence_events,
        "applied_event_count": len(applied_events),
        "counterfactual_outcome": counterfactual_outcome,
    }