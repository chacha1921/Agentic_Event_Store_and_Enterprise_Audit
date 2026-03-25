from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from ledger.event_store import StoredEvent


class AgentPerformanceLedger:
    checkpoint_name = "projection.agent_performance"

    def __init__(self, store, metrics_table: str = "agent_performance_ledger", session_table: str = "agent_session_facts"):
        self.store = store
        self.metrics_table = metrics_table
        self.session_table = session_table
        self._initialized = False
        self._session_facts: dict[str, dict[str, Any]] = {}
        self._metrics: dict[tuple[str, str], dict[str, Any]] = {}

    @property
    def name(self) -> str:
        return self.checkpoint_name

    async def setup(self) -> None:
        if self._initialized:
            return
        pool_getter = getattr(self.store, "_require_pool", None)
        if callable(pool_getter):
            pool = pool_getter()
            async with pool.acquire() as conn:
                await conn.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {self.session_table} (
                        session_id TEXT PRIMARY KEY,
                        agent_id TEXT NOT NULL,
                        agent_type TEXT NOT NULL,
                        model_version TEXT NOT NULL,
                        application_id TEXT,
                        started_at TIMESTAMPTZ,
                        completed_at TIMESTAMPTZ,
                        total_duration_ms BIGINT,
                        failed BOOLEAN NOT NULL DEFAULT FALSE,
                        decision_count INTEGER NOT NULL DEFAULT 0,
                        approve_count INTEGER NOT NULL DEFAULT 0,
                        total_confidence DOUBLE PRECISION NOT NULL DEFAULT 0,
                        confidence_samples INTEGER NOT NULL DEFAULT 0,
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                    """
                )
                await conn.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {self.metrics_table} (
                        agent_id TEXT NOT NULL,
                        model_version TEXT NOT NULL,
                        sessions_started INTEGER NOT NULL DEFAULT 0,
                        sessions_completed INTEGER NOT NULL DEFAULT 0,
                        sessions_failed INTEGER NOT NULL DEFAULT 0,
                        approve_count INTEGER NOT NULL DEFAULT 0,
                        decision_count INTEGER NOT NULL DEFAULT 0,
                        avg_confidence DOUBLE PRECISION,
                        avg_duration_ms DOUBLE PRECISION,
                        approve_rate DOUBLE PRECISION,
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        PRIMARY KEY (agent_id, model_version)
                    )
                    """
                )
        self._initialized = True

    def _default_fact(self, payload: dict[str, Any]) -> dict[str, Any]:
        return {
            "session_id": payload["session_id"],
            "agent_id": payload.get("agent_id", "unknown-agent"),
            "agent_type": payload.get("agent_type", "unknown"),
            "model_version": payload.get("model_version", "unknown-model"),
            "application_id": payload.get("application_id"),
            "started_at": None,
            "completed_at": None,
            "total_duration_ms": None,
            "failed": False,
            "decision_count": 0,
            "approve_count": 0,
            "total_confidence": 0.0,
            "confidence_samples": 0,
            "updated_at": datetime.now(timezone.utc),
        }

    def _extract_decision_stats(self, events_written: list[dict[str, Any]]) -> tuple[int, int, float, int]:
        decision_count = 0
        approve_count = 0
        confidence_total = 0.0
        confidence_samples = 0

        for event in events_written:
            payload = event.get("payload") or {}
            recommendation = payload.get("recommendation") or event.get("recommendation")
            confidence = payload.get("confidence")

            if recommendation is not None:
                decision_count += 1
                if str(recommendation).upper() == "APPROVE":
                    approve_count += 1
            elif event.get("event_type") == "ApplicationApproved":
                decision_count += 1
                approve_count += 1
            elif event.get("event_type") == "ApplicationDeclined":
                decision_count += 1

            if confidence is None and isinstance(payload.get("decision"), dict):
                confidence = payload["decision"].get("confidence")
            if confidence is not None:
                confidence_total += float(confidence)
                confidence_samples += 1

        return decision_count, approve_count, confidence_total, confidence_samples

    async def _persist_session_fact(self, fact: dict[str, Any]) -> None:
        pool_getter = getattr(self.store, "_require_pool", None)
        if not callable(pool_getter):
            return
        pool = pool_getter()
        async with pool.acquire() as conn:
            await conn.execute(
                f"""
                INSERT INTO {self.session_table}(
                    session_id, agent_id, agent_type, model_version, application_id,
                    started_at, completed_at, total_duration_ms, failed,
                    decision_count, approve_count, total_confidence, confidence_samples, updated_at
                )
                VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
                ON CONFLICT (session_id)
                DO UPDATE SET
                    agent_id = EXCLUDED.agent_id,
                    agent_type = EXCLUDED.agent_type,
                    model_version = EXCLUDED.model_version,
                    application_id = EXCLUDED.application_id,
                    started_at = EXCLUDED.started_at,
                    completed_at = EXCLUDED.completed_at,
                    total_duration_ms = EXCLUDED.total_duration_ms,
                    failed = EXCLUDED.failed,
                    decision_count = EXCLUDED.decision_count,
                    approve_count = EXCLUDED.approve_count,
                    total_confidence = EXCLUDED.total_confidence,
                    confidence_samples = EXCLUDED.confidence_samples,
                    updated_at = EXCLUDED.updated_at
                """,
                fact["session_id"], fact["agent_id"], fact["agent_type"], fact["model_version"], fact["application_id"],
                fact["started_at"], fact["completed_at"], fact["total_duration_ms"], fact["failed"],
                fact["decision_count"], fact["approve_count"], fact["total_confidence"], fact["confidence_samples"], fact["updated_at"],
            )

    def _recompute_metrics(self, agent_id: str, model_version: str) -> dict[str, Any]:
        facts = [
            fact for fact in self._session_facts.values()
            if fact["agent_id"] == agent_id and fact["model_version"] == model_version
        ]
        sessions_started = len(facts)
        sessions_completed = sum(1 for fact in facts if fact["completed_at"] is not None)
        sessions_failed = sum(1 for fact in facts if fact["failed"])
        decision_count = sum(fact["decision_count"] for fact in facts)
        approve_count = sum(fact["approve_count"] for fact in facts)
        total_confidence = sum(fact["total_confidence"] for fact in facts)
        confidence_samples = sum(fact["confidence_samples"] for fact in facts)
        duration_values = [fact["total_duration_ms"] for fact in facts if fact["total_duration_ms"] is not None]

        return {
            "agent_id": agent_id,
            "model_version": model_version,
            "sessions_started": sessions_started,
            "sessions_completed": sessions_completed,
            "sessions_failed": sessions_failed,
            "approve_count": approve_count,
            "decision_count": decision_count,
            "avg_confidence": (total_confidence / confidence_samples) if confidence_samples else None,
            "avg_duration_ms": (sum(duration_values) / len(duration_values)) if duration_values else None,
            "approve_rate": (approve_count / decision_count) if decision_count else None,
            "updated_at": datetime.now(timezone.utc),
        }

    async def _persist_metrics(self, metrics: dict[str, Any]) -> None:
        pool_getter = getattr(self.store, "_require_pool", None)
        if not callable(pool_getter):
            return
        pool = pool_getter()
        async with pool.acquire() as conn:
            await conn.execute(
                f"""
                INSERT INTO {self.metrics_table}(
                    agent_id, model_version, sessions_started, sessions_completed,
                    sessions_failed, approve_count, decision_count,
                    avg_confidence, avg_duration_ms, approve_rate, updated_at
                )
                VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
                ON CONFLICT (agent_id, model_version)
                DO UPDATE SET
                    sessions_started = EXCLUDED.sessions_started,
                    sessions_completed = EXCLUDED.sessions_completed,
                    sessions_failed = EXCLUDED.sessions_failed,
                    approve_count = EXCLUDED.approve_count,
                    decision_count = EXCLUDED.decision_count,
                    avg_confidence = EXCLUDED.avg_confidence,
                    avg_duration_ms = EXCLUDED.avg_duration_ms,
                    approve_rate = EXCLUDED.approve_rate,
                    updated_at = EXCLUDED.updated_at
                """,
                metrics["agent_id"], metrics["model_version"], metrics["sessions_started"], metrics["sessions_completed"],
                metrics["sessions_failed"], metrics["approve_count"], metrics["decision_count"],
                metrics["avg_confidence"], metrics["avg_duration_ms"], metrics["approve_rate"], metrics["updated_at"],
            )

    async def apply(self, event: StoredEvent) -> None:
        await self.setup()
        payload = event.payload
        event_type = event.event_type
        session_id = payload.get("session_id")
        if not session_id:
            return

        fact = self._session_facts.get(session_id)
        if fact is None and event_type == "AgentSessionStarted":
            fact = self._default_fact(payload)
        elif fact is None:
            fact = self._default_fact({
                "session_id": session_id,
                "agent_id": "unknown-agent",
                "agent_type": payload.get("agent_type", "unknown"),
                "model_version": "unknown-model",
                "application_id": payload.get("application_id"),
            })

        if event_type == "AgentSessionStarted":
            fact.update(
                {
                    "agent_id": payload.get("agent_id", fact["agent_id"]),
                    "agent_type": payload.get("agent_type", fact["agent_type"]),
                    "model_version": payload.get("model_version", fact["model_version"]),
                    "application_id": payload.get("application_id", fact["application_id"]),
                    "started_at": payload.get("started_at") or event.recorded_at,
                }
            )
        elif event_type == "AgentOutputWritten":
            decisions, approves, confidence_total, confidence_samples = self._extract_decision_stats(
                list(payload.get("events_written") or [])
            )
            fact["decision_count"] += decisions
            fact["approve_count"] += approves
            fact["total_confidence"] += confidence_total
            fact["confidence_samples"] += confidence_samples
        elif event_type == "AgentSessionCompleted":
            fact["completed_at"] = payload.get("completed_at") or event.recorded_at
            fact["total_duration_ms"] = payload.get("total_duration_ms")
        elif event_type == "AgentSessionFailed":
            fact["failed"] = True

        fact["updated_at"] = event.recorded_at
        self._session_facts[session_id] = fact
        await self._persist_session_fact(fact)

        metrics = self._recompute_metrics(fact["agent_id"], fact["model_version"])
        self._metrics[(fact["agent_id"], fact["model_version"])] = metrics
        await self._persist_metrics(metrics)

    async def get_metrics(self, agent_id: str, model_version: str) -> dict[str, Any] | None:
        pool_getter = getattr(self.store, "_require_pool", None)
        if callable(pool_getter) and self._initialized:
            pool = pool_getter()
            async with pool.acquire() as conn:
                row = await conn.fetchrow(
                    f"SELECT * FROM {self.metrics_table} WHERE agent_id = $1 AND model_version = $2",
                    agent_id,
                    model_version,
                )
                if row:
                    return dict(row)
        metrics = self._metrics.get((agent_id, model_version))
        return dict(metrics) if metrics is not None else None