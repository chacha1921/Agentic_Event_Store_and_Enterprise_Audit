"""Read-time schema evolution helpers for immutable event streams."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any


def upcast_credit_v1_to_v2(payload: dict[str, Any]) -> dict[str, Any]:
    """Upgrade legacy credit analysis payloads without inventing unknown facts."""
    upgraded = dict(payload)
    upgraded.setdefault("model_version", "legacy-pre-2026")
    upgraded.setdefault("confidence_score", None)
    upgraded.setdefault("regulatory_basis", [])
    return upgraded


def upcast_decision_v1_to_v2(payload: dict[str, Any]) -> dict[str, Any]:
    """Upgrade legacy decision payloads to include model lineage metadata."""
    upgraded = dict(payload)
    upgraded.setdefault("model_versions", {})
    return upgraded


class UpcasterRegistry:
    """Apply schema migrations on read only; persisted rows remain unchanged."""

    def __init__(self):
        self._upcasters: dict[str, dict[int, tuple[int, Callable[[dict[str, Any]], dict[str, Any]]]]] = {}
        self.register("CreditAnalysisCompleted", from_version=1, to_version=2)(upcast_credit_v1_to_v2)
        self.register("DecisionGenerated", from_version=1, to_version=2)(upcast_decision_v1_to_v2)

    def register(self, event_type: str, from_version: int, to_version: int):
        def decorator(fn: Callable[[dict[str, Any]], dict[str, Any]]):
            self._upcasters.setdefault(event_type, {})[from_version] = (to_version, fn)
            return fn

        return decorator

    def upcast(self, event: dict[str, Any]) -> dict[str, Any]:
        current = dict(event)
        current["payload"] = dict(current.get("payload") or {})

        event_type = current.get("event_type")
        version = int(current.get("event_version", 1))
        chain = self._upcasters.get(event_type, {})

        while version in chain:
            to_version, fn = chain[version]
            current["payload"] = fn(dict(current["payload"]))
            version = to_version
            current["event_version"] = to_version

        return current


__all__ = [
    "UpcasterRegistry",
    "upcast_credit_v1_to_v2",
    "upcast_decision_v1_to_v2",
]
