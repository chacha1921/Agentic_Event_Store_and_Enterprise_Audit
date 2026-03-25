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


def _verify_existing_chain(primary_events: list[Any], audit_events: list[Any]) -> tuple[bool, bool, str | None, int]:
    prior_hash: str | None = None
    verified_count = 0
    chain_valid = True
    tamper_detected = False

    integrity_events = [event for event in audit_events if event.event_type == "AuditIntegrityCheckRun"]
    for integrity_event in integrity_events:
        payload = integrity_event.payload
        recorded_count = int(payload.get("events_verified_count", 0))
        if recorded_count < verified_count:
            return False, True, prior_hash, verified_count
        if payload.get("previous_hash") != prior_hash:
            return False, True, prior_hash, verified_count

        segment = primary_events[verified_count:recorded_count]
        if len(segment) != recorded_count - verified_count:
            return False, True, prior_hash, verified_count

        segment_hashes = [_payload_hash(event.payload) for event in segment]
        expected_hash = _chain_hash(prior_hash, segment_hashes)
        if expected_hash != payload.get("integrity_hash"):
            return False, True, prior_hash, verified_count
        if not bool(payload.get("chain_valid", True)) or bool(payload.get("tamper_detected", False)):
            chain_valid = False
            tamper_detected = True

        prior_hash = payload.get("integrity_hash")
        verified_count = recorded_count

    return chain_valid, tamper_detected, prior_hash, verified_count


async def run_integrity_check(store: EventStore, entity_type: str, entity_id: str) -> dict[str, Any]:
    """Compute and append a tamper-evident audit hash for a single entity stream."""
    primary_stream_id = f"{entity_type}-{entity_id}"
    audit_stream_id = f"audit-{entity_type}-{entity_id}"

    primary_events = await store.load_stream(primary_stream_id, from_position=0)
    audit_events = await store.load_stream(audit_stream_id, from_position=0)
    audit_aggregate = await AuditLedgerAggregate.load(store, f"{entity_type}-{entity_id}")

    chain_valid, tamper_detected, previous_hash, previously_verified = _verify_existing_chain(primary_events, audit_events)

    events_to_verify = primary_events[previously_verified:]
    event_hashes = [_payload_hash(event.payload) for event in events_to_verify]
    new_hash = _chain_hash(previous_hash, event_hashes)
    expected_total = previously_verified + len(events_to_verify)
    tamper_detected = tamper_detected or len(primary_events) < previously_verified
    chain_valid = chain_valid and not tamper_detected

    if audit_aggregate.checks_run > 0 and not tamper_detected:
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
        "events_verified": expected_total,
        "events_verified_count": expected_total,
        "new_events_hashed": len(events_to_verify),
        "previous_hash": previous_hash,
        "integrity_hash": new_hash,
        "chain_valid": chain_valid,
        "tamper_detected": tamper_detected,
        "event_hashes": event_hashes,
    }