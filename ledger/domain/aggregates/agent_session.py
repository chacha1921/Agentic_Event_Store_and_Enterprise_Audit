from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import json
from typing import Any

from ledger.domain.errors import DomainError
from ledger.schema.events import deserialize_event


class SessionHealthStatus(str, Enum):
    HEALTHY = "HEALTHY"
    IN_PROGRESS = "IN_PROGRESS"
    FAILED = "FAILED"
    NEEDS_RECONCILIATION = "NEEDS_RECONCILIATION"


@dataclass(frozen=True)
class PendingWorkItem:
    description: str
    source_event_type: str
    source_event_position: int


@dataclass(frozen=True)
class AgentContext:
    context_text: str
    last_event_position: int
    pending_work: list[PendingWorkItem]
    session_health_status: SessionHealthStatus
    current_application_state: str | None = None


@dataclass
class AgentSessionAggregate:
    session_id: str
    agent_type: str
    version: int = -1
    context_loaded: bool = False
    started: bool = False
    completed: bool = False
    model_version: str | None = None
    application_id: str | None = None
    context_source: str | None = None
    failed: bool = False
    recoverable_failure: bool = False
    last_node_name: str | None = None
    last_event_type: str | None = None
    decision_event_types_written: set[str] | None = None

    @classmethod
    async def load(cls, store, agent_type: str, session_id: str) -> "AgentSessionAggregate":
        aggregate = cls(session_id=session_id, agent_type=agent_type)
        events = await store.load_stream(f"agent-{agent_type}-{session_id}")
        for index, event in enumerate(events):
            aggregate._apply(event, index)
        return aggregate

    @classmethod
    async def reconstruct_agent_context(
        cls,
        store,
        agent_id: str,
        session_id: str,
        token_budget: int = 8000,
    ) -> AgentContext:
        return await reconstruct_agent_context(store, agent_id, session_id, token_budget)

    def __post_init__(self) -> None:
        if self.decision_event_types_written is None:
            self.decision_event_types_written = set()

    def _apply(self, event: dict, index: int | None = None) -> None:
        event_type = event.get("event_type")
        if index == 0 and event_type != "AgentContextLoaded":
            raise DomainError("Gas Town invariant violated: first session event must be AgentContextLoaded")
        handler = getattr(self, f"_on_{event_type}", None)
        if handler is not None:
            handler(deserialize_event(event_type, event.get("payload", {})))
        self.version = event.get("stream_position", self.version + 1)
        self.last_event_type = event_type

    def _on_AgentContextLoaded(self, event) -> None:
        if self.context_loaded:
            raise DomainError("Agent context cannot be loaded twice")
        self.context_loaded = True
        self.context_source = event.context_source
        self.application_id = event.application_id

    def _on_AgentSessionStarted(self, event) -> None:
        self.assert_context_loaded()
        if self.started:
            raise DomainError("Agent session cannot start twice")
        self.started = True
        self.model_version = event.model_version
        self.application_id = event.application_id

    def _on_AgentSessionCompleted(self, event) -> None:
        self.assert_started()
        self.assert_model_version_locked(self.model_version)
        self.completed = True
        self.failed = False
        self.application_id = event.application_id

    def _on_AgentSessionFailed(self, event) -> None:
        self.assert_started()
        self.failed = True
        self.recoverable_failure = bool(event.recoverable)
        self.last_node_name = event.last_successful_node
        self.application_id = event.application_id

    def _on_AgentSessionRecovered(self, event) -> None:
        self.assert_started()
        self.failed = False
        self.application_id = event.application_id

    def _on_AgentNodeExecuted(self, event) -> None:
        self.assert_started()
        self.assert_model_version_locked(self.model_version)
        self.last_node_name = event.node_name
        self.application_id = event.session_id and self.application_id

    def _on_AgentToolCalled(self, event) -> None:
        self.assert_started()
        self.application_id = event.session_id and self.application_id

    def _on_AgentOutputWritten(self, event) -> None:
        self.assert_started()
        self.application_id = event.application_id
        for written_event in event.events_written:
            if not isinstance(written_event, dict):
                continue
            payload = written_event.get("payload") or {}
            if payload.get("application_id") != self.application_id:
                continue
            event_type = written_event.get("event_type")
            if event_type in {
                "CreditAnalysisCompleted",
                "FraudScreeningCompleted",
                "ComplianceCheckCompleted",
                "DecisionGenerated",
                "ApplicationApproved",
                "ApplicationDeclined",
            }:
                self.decision_event_types_written.add(event_type)

    def _on_AgentInputValidated(self, event) -> None:
        self.assert_started()
        self.application_id = event.application_id

    def _on_AgentInputValidationFailed(self, event) -> None:
        self.assert_started()
        self.application_id = event.application_id

    def assert_started(self) -> None:
        if not self.started:
            raise DomainError("Agent session decisions require AgentSessionStarted first")

    def assert_context_loaded(self) -> None:
        if not self.context_loaded:
            raise DomainError("Gas Town invariant violated: AgentContextLoaded is required before agent decisions")

    def assert_model_version_locked(self, model_version: str | None) -> None:
        if self.model_version is None:
            raise DomainError("Model version lock is missing for this session")
        if model_version is not None and self.model_version != model_version:
            raise DomainError(f"Model version lock violated: expected {self.model_version}, got {model_version}")

    def assert_model_version_current(self, model_version: str | None) -> None:
        self.assert_model_version_locked(model_version)

    def assert_processed_application_decision(self) -> None:
        self.assert_context_loaded()
        self.assert_started()
        if not self.decision_event_types_written:
            raise DomainError("Contributing session must contain a decision/output event for this application")


def _event_payload_contains_pending_or_error(payload: Any) -> bool:
    if isinstance(payload, dict):
        return any(_event_payload_contains_pending_or_error(value) for value in payload.values())
    if isinstance(payload, list):
        return any(_event_payload_contains_pending_or_error(value) for value in payload)
    if isinstance(payload, str):
        upper = payload.upper()
        return "PENDING" in upper or "ERROR" in upper or "FAILED" in upper
    return False


def _preserve_verbatim(event: Any, last_positions: set[int]) -> bool:
    if event.stream_position in last_positions:
        return True
    if "Failed" in event.event_type or "Error" in event.event_type:
        return True
    return _event_payload_contains_pending_or_error(event.payload)


def _summarize_events(events: list[Any]) -> str:
    if not events:
        return "Older session summary: no older events to summarize."

    counts: dict[str, int] = {}
    node_names: list[str] = []
    tool_names: list[str] = []
    for event in events:
        counts[event.event_type] = counts.get(event.event_type, 0) + 1
        if event.event_type == "AgentNodeExecuted":
            node_name = event.payload.get("node_name")
            if node_name:
                node_names.append(node_name)
        elif event.event_type == "AgentToolCalled":
            tool_name = event.payload.get("tool_name")
            if tool_name:
                tool_names.append(tool_name)

    parts = [
        f"Older session summary: {len(events)} event(s)",
        ", ".join(f"{event_type} x{count}" for event_type, count in sorted(counts.items())),
    ]
    if node_names:
        parts.append(f"nodes={', '.join(node_names)}")
    if tool_names:
        parts.append(f"tools={', '.join(tool_names)}")
    return "; ".join(parts) + "."


def _render_verbatim_events(events: list[Any]) -> str:
    rendered = []
    for event in events:
        rendered.append(
            json.dumps(
                {
                    "stream_position": event.stream_position,
                    "event_type": event.event_type,
                    "payload": event.payload,
                },
                default=str,
                sort_keys=True,
            )
        )
    return "\n".join(rendered)


def _apply_token_budget(summary_text: str, verbatim_text: str, token_budget: int) -> str:
    max_chars = max(token_budget * 4, 256)
    verbatim_header = "\nPreserved verbatim events:\n" if verbatim_text else ""
    full_text = summary_text + verbatim_header + verbatim_text
    if len(full_text) <= max_chars:
        return full_text

    reserved_chars = len(verbatim_header) + len(verbatim_text)
    remaining_summary_chars = max_chars - reserved_chars
    if remaining_summary_chars <= 32:
        if not verbatim_text:
            return full_text[: max_chars - 3] + "..."
        truncated_summary = "Older session summary: truncated to respect token budget."
        return truncated_summary + verbatim_header + verbatim_text

    truncated_summary = summary_text
    if len(summary_text) > remaining_summary_chars:
        truncated_summary = summary_text[: remaining_summary_chars - 3] + "..."
    return truncated_summary + verbatim_header + verbatim_text


def _extract_application_id(events: list[Any]) -> str | None:
    for event in reversed(events):
        application_id = event.payload.get("application_id")
        if application_id:
            return str(application_id)
    return None


async def _load_current_application_state(store, application_id: str | None) -> str | None:
    if not application_id:
        return None
    from ledger.domain.aggregates.loan_application import LoanApplicationAggregate

    aggregate = await LoanApplicationAggregate.load(store, application_id)
    if aggregate.state is None:
        return None
    return aggregate.state.value


def _detect_pending_work(events: list[Any]) -> tuple[list[PendingWorkItem], SessionHealthStatus]:
    if not events:
        return [], SessionHealthStatus.IN_PROGRESS

    pending_work: list[PendingWorkItem] = []
    health = SessionHealthStatus.HEALTHY
    last_event = events[-1]
    completed = any(event.event_type == "AgentSessionCompleted" for event in events)
    last_node_event = next((event for event in reversed(events) if event.event_type == "AgentNodeExecuted"), None)
    last_failed_event = next((event for event in reversed(events) if event.event_type == "AgentSessionFailed"), None)
    last_output_event = next((event for event in reversed(events) if event.event_type == "AgentOutputWritten"), None)

    if last_failed_event is not None:
        description = last_failed_event.payload.get("error_message") or "Recover failed session work"
        pending_work.append(
            PendingWorkItem(
                description=f"Recover from failure: {description}",
                source_event_type=last_failed_event.event_type,
                source_event_position=last_failed_event.stream_position,
            )
        )
        health = SessionHealthStatus.NEEDS_RECONCILIATION if last_failed_event.payload.get("recoverable") else SessionHealthStatus.FAILED

    output_requires_completion = False
    if last_output_event is not None:
        written_events = last_output_event.payload.get("events_written") or []
        output_requires_completion = any(
            isinstance(written_event, dict)
            and written_event.get("event_type") in {"DecisionGenerated", "ApplicationApproved", "ApplicationDeclined"}
            for written_event in written_events
        )

    if output_requires_completion and not completed:
        pending_work.append(
            PendingWorkItem(
                description="Reconcile decision/output written without corresponding session completion event",
                source_event_type=last_output_event.event_type,
                source_event_position=last_output_event.stream_position,
            )
        )
        health = SessionHealthStatus.NEEDS_RECONCILIATION

    if not completed and last_node_event is not None:
        pending_work.append(
            PendingWorkItem(
                description=f"Resume execution after node '{last_node_event.payload.get('node_name', 'unknown')}'",
                source_event_type=last_node_event.event_type,
                source_event_position=last_node_event.stream_position,
            )
        )
        if health == SessionHealthStatus.HEALTHY:
            health = SessionHealthStatus.IN_PROGRESS

    if last_event.event_type == "AgentSessionCompleted":
        health = SessionHealthStatus.HEALTHY

    return pending_work, health


async def reconstruct_agent_context(
    store,
    agent_id: str,
    session_id: str,
    token_budget: int = 8000,
) -> AgentContext:
    events = await store.load_stream(f"agent-{agent_id}-{session_id}")
    if not events:
        raise DomainError(f"Agent session '{session_id}' does not exist for {agent_id}")

    last_event_position = events[-1].stream_position
    last_positions = {event.stream_position for event in events[-3:]}
    preserved = [event for event in events if _preserve_verbatim(event, last_positions)]
    preserved_positions = {event.stream_position for event in preserved}
    summarized = [event for event in events if event.stream_position not in preserved_positions]

    pending_work, health = _detect_pending_work(events)
    summary_text = _summarize_events(summarized)
    verbatim_text = _render_verbatim_events(sorted(preserved, key=lambda event: event.stream_position))
    context_text = _apply_token_budget(summary_text, verbatim_text, token_budget)
    current_application_state = await _load_current_application_state(store, _extract_application_id(events))

    return AgentContext(
        context_text=context_text,
        last_event_position=last_event_position,
        pending_work=pending_work,
        session_health_status=health,
        current_application_state=current_application_state,
    )