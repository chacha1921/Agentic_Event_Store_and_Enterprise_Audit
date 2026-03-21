from __future__ import annotations

from dataclasses import dataclass

from ledger.domain.errors import DomainError
from ledger.schema.events import deserialize_event


@dataclass
class AgentSessionAggregate:
    session_id: str
    agent_type: str
    version: int = -1
    started: bool = False
    completed: bool = False
    model_version: str | None = None
    application_id: str | None = None

    @classmethod
    async def load(cls, store, agent_type: str, session_id: str) -> "AgentSessionAggregate":
        aggregate = cls(session_id=session_id, agent_type=agent_type)
        events = await store.load_stream(f"agent-{agent_type}-{session_id}")
        for index, event in enumerate(events):
            aggregate._apply(event, index)
        return aggregate

    def _apply(self, event: dict, index: int | None = None) -> None:
        event_type = event.get("event_type")
        if index == 0 and event_type not in {"AgentSessionStarted", "AgentContextLoaded"}:
            raise DomainError("Gas Town invariant violated: first session event must start or load context")
        handler = getattr(self, f"_on_{event_type}", None)
        if handler is not None:
            handler(deserialize_event(event_type, event.get("payload", {})))
        self.version = event.get("stream_position", self.version + 1)

    def _on_AgentSessionStarted(self, event) -> None:
        if self.started:
            raise DomainError("Agent session cannot start twice")
        self.started = True
        self.model_version = event.model_version
        self.application_id = event.application_id

    def _on_AgentSessionCompleted(self, event) -> None:
        self.assert_started()
        self.assert_model_version_locked(self.model_version)
        self.completed = True
        self.application_id = event.application_id

    def _on_AgentSessionFailed(self, event) -> None:
        self.assert_started()
        self.application_id = event.application_id

    def _on_AgentSessionRecovered(self, event) -> None:
        self.assert_started()
        self.application_id = event.application_id

    def _on_AgentNodeExecuted(self, event) -> None:
        self.assert_started()
        self.assert_model_version_locked(self.model_version)
        self.application_id = event.session_id and self.application_id

    def _on_AgentToolCalled(self, event) -> None:
        self.assert_started()
        self.application_id = event.session_id and self.application_id

    def _on_AgentOutputWritten(self, event) -> None:
        self.assert_started()
        self.application_id = event.application_id

    def _on_AgentInputValidated(self, event) -> None:
        self.assert_started()
        self.application_id = event.application_id

    def _on_AgentInputValidationFailed(self, event) -> None:
        self.assert_started()
        self.application_id = event.application_id

    def assert_started(self) -> None:
        if not self.started:
            raise DomainError("Agent session decisions require AgentSessionStarted first")

    def assert_model_version_locked(self, model_version: str | None) -> None:
        if self.model_version is None:
            raise DomainError("Model version lock is missing for this session")
        if model_version is not None and self.model_version != model_version:
            raise DomainError(f"Model version lock violated: expected {self.model_version}, got {model_version}")