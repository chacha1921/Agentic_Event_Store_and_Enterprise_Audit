from __future__ import annotations

from dataclasses import dataclass, field

from ledger.domain.errors import DomainError
from ledger.schema.events import ComplianceVerdict, deserialize_event


@dataclass
class ComplianceRecordAggregate:
    application_id: str
    version: int = -1
    initiated: bool = False
    completed: bool = False
    session_id: str | None = None
    regulation_set_version: str | None = None
    rules_to_evaluate: set[str] = field(default_factory=set)
    passed_rule_ids: set[str] = field(default_factory=set)
    failed_rule_ids: set[str] = field(default_factory=set)
    noted_rule_ids: set[str] = field(default_factory=set)
    has_hard_block: bool = False
    overall_verdict: str | None = None
    final_decision: str | None = None

    @classmethod
    async def load(cls, store, application_id: str) -> "ComplianceRecordAggregate":
        aggregate = cls(application_id=application_id)
        events = await store.load_stream(f"compliance-{application_id}")
        for event in events:
            aggregate._apply(event)
        return aggregate

    def _apply(self, event: dict) -> None:
        event_type = event.get("event_type")
        handler = getattr(self, f"_on_{event_type}", None)
        if handler is not None:
            handler(deserialize_event(event_type, event.get("payload", {})))
        self.version = event.get("stream_position", self.version + 1)

    def _ensure(self, condition: bool, message: str) -> None:
        if not condition:
            raise DomainError(message)

    def _assert_rule_phase(self) -> None:
        self._ensure(self.initiated, "Compliance rules require ComplianceCheckInitiated first")
        self._ensure(not self.completed, "Compliance rules cannot be recorded after completion")

    def _assert_unique_rule(self, rule_id: str) -> None:
        known = self.passed_rule_ids | self.failed_rule_ids | self.noted_rule_ids
        self._ensure(rule_id not in known, f"Compliance rule '{rule_id}' cannot be evaluated twice")

    def _on_ComplianceCheckInitiated(self, event) -> None:
        self._ensure(not self.initiated, "Compliance check cannot be initiated twice")
        self.initiated = True
        self.session_id = event.session_id
        self.regulation_set_version = event.regulation_set_version
        self.rules_to_evaluate = set(event.rules_to_evaluate)

    def _on_ComplianceRulePassed(self, event) -> None:
        self._assert_rule_phase()
        self._assert_unique_rule(event.rule_id)
        self.passed_rule_ids.add(event.rule_id)

    def _on_ComplianceRuleFailed(self, event) -> None:
        self._assert_rule_phase()
        self._assert_unique_rule(event.rule_id)
        self.failed_rule_ids.add(event.rule_id)
        self.has_hard_block = self.has_hard_block or bool(event.is_hard_block)

    def _on_ComplianceRuleNoted(self, event) -> None:
        self._assert_rule_phase()
        self._assert_unique_rule(event.rule_id)
        self.noted_rule_ids.add(event.rule_id)

    def _on_ComplianceCheckCompleted(self, event) -> None:
        self._ensure(self.initiated, "Compliance cannot complete before initiation")
        self._ensure(not self.completed, "Compliance check cannot complete twice")
        self._ensure(event.rules_passed == len(self.passed_rule_ids), "rules_passed does not match observed passed rules")
        self._ensure(event.rules_failed == len(self.failed_rule_ids), "rules_failed does not match observed failed rules")
        self._ensure(event.rules_noted == len(self.noted_rule_ids), "rules_noted does not match observed noted rules")
        total_rules = len(self.passed_rule_ids) + len(self.failed_rule_ids) + len(self.noted_rule_ids)
        self._ensure(event.rules_evaluated == total_rules, "rules_evaluated does not match observed compliance events")
        self._ensure(bool(event.has_hard_block) == self.has_hard_block, "has_hard_block does not match failed rule history")
        if bool(event.has_hard_block):
            self._ensure(event.overall_verdict == ComplianceVerdict.BLOCKED, "Hard block requires BLOCKED verdict")
        if event.overall_verdict == ComplianceVerdict.CLEAR:
            self._ensure(not self.failed_rule_ids and not self.has_hard_block, "CLEAR verdict cannot coexist with failures or hard block")
        self.completed = True
        self.session_id = event.session_id
        self.overall_verdict = event.overall_verdict.value if hasattr(event.overall_verdict, "value") else str(event.overall_verdict)

    def _on_ApplicationApproved(self, event) -> None:
        del event
        self._ensure(self.completed, "Application approval requires completed compliance review")
        self._ensure(not self.has_hard_block, "Application approval is invalid after a compliance hard block")
        self.final_decision = "APPROVED"

    def _on_ApplicationDeclined(self, event) -> None:
        del event
        self.final_decision = "DECLINED"


__all__ = ["ComplianceRecordAggregate"]