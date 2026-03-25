from __future__ import annotations

from dataclasses import dataclass

from ledger.domain.errors import DomainError
from ledger.schema.events import deserialize_event


@dataclass
class AuditLedgerAggregate:
    entity_id: str
    version: int = -1
    checks_run: int = 0
    last_integrity_hash: str | None = None
    last_verified_count: int = 0

    @classmethod
    async def load(cls, store, entity_id: str) -> "AuditLedgerAggregate":
        aggregate = cls(entity_id=entity_id)
        events = await store.load_stream(f"audit-{entity_id}")
        for event in events:
            aggregate._apply(event)
        return aggregate

    def _apply(self, event: dict) -> None:
        event_type = event.get("event_type")
        handler = getattr(self, f"_on_{event_type}", None)
        if handler is not None:
            handler(deserialize_event(event_type, event.get("payload", {})))
        self.version = event.get("stream_position", self.version + 1)

    def _on_AuditIntegrityCheckRun(self, event) -> None:
        if self.checks_run > 0:
            if event.previous_hash != self.last_integrity_hash:
                raise DomainError("Audit integrity chain previous_hash does not match the last integrity hash")
            if event.events_verified_count < self.last_verified_count:
                raise DomainError("Audit integrity checks cannot move the verified event count backwards")
        self.checks_run += 1
        self.last_integrity_hash = event.integrity_hash
        self.last_verified_count = event.events_verified_count


__all__ = ["AuditLedgerAggregate"]