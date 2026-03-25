"""Read-time schema evolution helpers for immutable event streams."""

from __future__ import annotations

import inspect
from collections.abc import Callable
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from ledger.schema.events import AgentType

if TYPE_CHECKING:
    from ledger.event_store import StoredEvent


def _normalize_recorded_at(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    if isinstance(value, str):
        parsed = datetime.fromisoformat(value)
        return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)
    return datetime.now(UTC)


def _infer_credit_model_version(recorded_at: datetime) -> str:
    if recorded_at < datetime(2026, 1, 1, tzinfo=UTC):
        return "legacy-pre-2026"
    return "claude-sonnet-4-20250514"


def _infer_regulatory_basis(recorded_at: datetime) -> list[str]:
    if recorded_at < datetime(2026, 1, 1, tzinfo=UTC):
        return ["CREDIT-POLICY-2025-09"]
    return ["CREDIT-POLICY-2026-01"]


def upcast_credit_v1_to_v2(payload: dict[str, Any], event: "StoredEvent", store=None) -> dict[str, Any]:
    """Upgrade legacy credit analysis payloads without inventing unknown facts."""
    upgraded = dict(payload)
    recorded_at = _normalize_recorded_at(event.recorded_at)
    upgraded.setdefault("model_version", _infer_credit_model_version(recorded_at))
    upgraded.setdefault("confidence_score", None)
    upgraded.setdefault("regulatory_basis", _infer_regulatory_basis(recorded_at))
    return upgraded


async def upcast_decision_v1_to_v2(payload: dict[str, Any], event: "StoredEvent", store=None) -> dict[str, Any]:
    """Upgrade legacy decision payloads to include model lineage metadata."""
    upgraded = dict(payload)
    if "model_versions" in upgraded:
        return upgraded

    model_versions: dict[str, str] = {}
    if store is not None:
        orchestrator_session_id = upgraded.get("orchestrator_session_id")
        if orchestrator_session_id:
            orchestrator_version = await _load_session_model_version(store, "decision_orchestrator", orchestrator_session_id)
            if orchestrator_version is not None:
                model_versions["orchestrator"] = orchestrator_version
        for session_id in upgraded.get("contributing_sessions") or []:
            resolved = await _load_contributing_session_version(store, session_id)
            if resolved is None:
                continue
            agent_type, model_version = resolved
            model_versions.setdefault(agent_type, model_version)

    upgraded["model_versions"] = model_versions
    return upgraded


async def _load_contributing_session_version(store, session_id: str) -> tuple[str, str] | None:
    for agent_type in AgentType:
        model_version = await _load_session_model_version(store, agent_type.value, session_id)
        if model_version is not None:
            return agent_type.value, model_version
    return None


async def _load_session_model_version(store, agent_type: str, session_id: str) -> str | None:
    stream_id = f"agent-{agent_type}-{session_id}"
    if await store.stream_version(stream_id) < 0:
        return None
    session_events = await store.load_stream(stream_id)
    for session_event in session_events:
        if session_event.event_type == "AgentSessionStarted":
            return session_event.payload.get("model_version")
    return None


class UpcasterRegistry:
    """Apply schema migrations on read only; persisted rows remain unchanged."""

    def __init__(self):
        self._upcasters: dict[tuple[str, int], Callable[..., Any]] = {}
        self.register("CreditAnalysisCompleted", from_version=1)(upcast_credit_v1_to_v2)
        self.register("DecisionGenerated", from_version=1)(upcast_decision_v1_to_v2)

    def register(self, event_type: str, from_version: int):
        def decorator(fn: Callable[..., Any]):
            self._upcasters[(event_type, from_version)] = fn
            return fn

        return decorator

    async def upcast(self, event: "StoredEvent", store=None) -> "StoredEvent":
        current = event
        version = int(event.event_version)

        while (current.event_type, version) in self._upcasters:
            fn = self._upcasters[(current.event_type, version)]
            new_payload = await self._invoke_upcaster(fn, current, store)
            current = current.with_payload(new_payload, version=version + 1)
            version += 1

        return current

    async def _invoke_upcaster(self, fn: Callable[..., Any], event: "StoredEvent", store=None) -> dict[str, Any]:
        parameter_count = len(inspect.signature(fn).parameters)
        if parameter_count == 1:
            result = fn(dict(event.payload))
        else:
            result = fn(dict(event.payload), event, store)
        if inspect.isawaitable(result):
            result = await result
        return dict(result)


__all__ = [
    "UpcasterRegistry",
    "upcast_credit_v1_to_v2",
    "upcast_decision_v1_to_v2",
]
