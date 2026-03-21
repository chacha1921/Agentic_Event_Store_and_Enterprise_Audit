"""
ledger/upcasters.py — UpcasterRegistry
=======================================
COMPLETION STATUS: STUB — implement upcast() for two event versions.

Upcasters transform old event versions to the current version ON READ.
They NEVER write to the events table. Immutability is non-negotiable.

IMPLEMENT:
  CreditAnalysisCompleted v1 → v2: add model_versions={} if absent
  DecisionGenerated v1 → v2: add model_versions={} if absent

RULE: if event_version == current version, return unchanged.
      if event_version < current version, apply the chain of upcasters.
"""
from __future__ import annotations

class UpcasterRegistry:
    """Apply on load_stream() — never on append()."""
    def upcast(self, event: dict) -> dict:
        et = event.get("event_type"); ver = event.get("event_version", 1)
        if et == "CreditAnalysisCompleted" and ver < 2:
            event = dict(event); event["event_version"] = 2
            p = dict(event.get("payload", {}))
            p.setdefault("regulatory_basis", [])
            event["payload"] = p
        if et == "DecisionGenerated" and ver < 2:
            event = dict(event); event["event_version"] = 2
            p = dict(event.get("payload", {}))
            p.setdefault("model_versions", {})
            event["payload"] = p
        return event
