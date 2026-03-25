from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any

from ledger.domain.aggregates.audit_ledger import AuditLedgerAggregate
from ledger.domain.errors import DomainError
from ledger.event_store import EventStore
from ledger.schema.events import AuditIntegrityCheckRun


def _normalize_for_hash(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _normalize_for_hash(value[key]) for key in sorted(value)}
    if isinstance(value, list):
        return [_normalize_for_hash(item) for item in value]
    return value


def _payload_hash(payload: dict[str, Any]) -> str:
    canonical = json.dumps(_normalize_for_hash(payload), sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _chain_hash(previous_hash: str | None, event_hashes: list[str]) -> str:
    seed = previous_hash or ""
    material = seed + "".join(event_hashes)
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


async def run_integrity_check(store: EventStore, entity_type: str, entity_id: str) -> dict[str, Any]:
    """Compute and append a tamper-evident audit hash for a single entity stream."""
    primary_stream_id = f"{entity_type}-{entity_id}"
    audit_stream_id = f"audit-{entity_type}-{entity_id}"

    primary_events = await store.load_stream(primary_stream_id, from_position=0)
    audit_events = await store.load_stream(audit_stream_id, from_position=0)
    audit_aggregate = await AuditLedgerAggregate.load(store, f"{entity_type}-{entity_id}")

    prior_check = None
    for event in reversed(audit_events):
        if event.event_type == "AuditIntegrityCheckRun":
            prior_check = event
            break

    previously_verified = 0
    previous_hash = None
    chain_valid = True
    if prior_check is not None:
        previous_hash = prior_check.payload.get("integrity_hash")
        previously_verified = int(prior_check.payload.get("events_verified_count", 0))
        chain_valid = bool(prior_check.payload.get("chain_valid", True)) and not bool(
            prior_check.payload.get("tamper_detected", False)
        )

    events_to_verify = primary_events[previously_verified:]
    event_hashes = [_payload_hash(event.payload) for event in events_to_verify]
    new_hash = _chain_hash(previous_hash, event_hashes)
    expected_total = previously_verified + len(events_to_verify)
    tamper_detected = len(primary_events) < previously_verified
    chain_valid = chain_valid and not tamper_detected

    if audit_aggregate.checks_run > 0:
        if audit_aggregate.last_integrity_hash != previous_hash:
            raise DomainError("AuditLedger aggregate chain head does not match the prior integrity event")
        if expected_total < audit_aggregate.last_verified_count:
            raise DomainError("AuditLedger aggregate forbids reducing the verified event count")

    check_event = AuditIntegrityCheckRun(
        entity_type=entity_type,
        entity_id=entity_id,
        check_timestamp=datetime.now(timezone.utc),
        events_verified_count=expected_total,
        integrity_hash=new_hash,
        previous_hash=previous_hash,
        chain_valid=chain_valid,
        tamper_detected=tamper_detected,
    ).to_store_dict()

    expected_version = await store.stream_version(audit_stream_id)
    await store.append(audit_stream_id, [check_event], expected_version=expected_version)

    return {
        "entity_type": entity_type,
        "entity_id": entity_id,
        "stream_id": primary_stream_id,
        "audit_stream_id": audit_stream_id,
        "events_verified_count": expected_total,
        "new_events_hashed": len(events_to_verify),
        "previous_hash": previous_hash,
        "integrity_hash": new_hash,
        "chain_valid": chain_valid,
        "tamper_detected": tamper_detected,
        "event_hashes": event_hashes,
    }