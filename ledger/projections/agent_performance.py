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
        self._application_to_decision_session: dict[str, str] = {}

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
                        decline_count INTEGER NOT NULL DEFAULT 0,
                        refer_count INTEGER NOT NULL DEFAULT 0,
                        override_count INTEGER NOT NULL DEFAULT 0,
                        approve_count INTEGER NOT NULL DEFAULT 0,
                        total_confidence DOUBLE PRECISION NOT NULL DEFAULT 0,
                        confidence_samples INTEGER NOT NULL DEFAULT 0,
                        first_seen_at TIMESTAMPTZ,
                        last_seen_at TIMESTAMPTZ,
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                    """
                )
                await conn.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {self.metrics_table} (
                        agent_id TEXT NOT NULL,
                        model_version TEXT NOT NULL,
                        analyses_completed INTEGER NOT NULL DEFAULT 0,
                        decisions_generated INTEGER NOT NULL DEFAULT 0,
                        avg_confidence_score DOUBLE PRECISION,
                        decline_rate DOUBLE PRECISION,
                        refer_rate DOUBLE PRECISION,
                        human_override_rate DOUBLE PRECISION,
                        first_seen_at TIMESTAMPTZ,
                        last_seen_at TIMESTAMPTZ,
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
                for statement in [
                    f"ALTER TABLE {self.session_table} ADD COLUMN IF NOT EXISTS decline_count INTEGER NOT NULL DEFAULT 0",
                    f"ALTER TABLE {self.session_table} ADD COLUMN IF NOT EXISTS refer_count INTEGER NOT NULL DEFAULT 0",
                    f"ALTER TABLE {self.session_table} ADD COLUMN IF NOT EXISTS override_count INTEGER NOT NULL DEFAULT 0",
                    f"ALTER TABLE {self.session_table} ADD COLUMN IF NOT EXISTS first_seen_at TIMESTAMPTZ",
                    f"ALTER TABLE {self.session_table} ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMPTZ",
                    f"ALTER TABLE {self.metrics_table} ADD COLUMN IF NOT EXISTS analyses_completed INTEGER NOT NULL DEFAULT 0",
                    f"ALTER TABLE {self.metrics_table} ADD COLUMN IF NOT EXISTS decisions_generated INTEGER NOT NULL DEFAULT 0",
                    f"ALTER TABLE {self.metrics_table} ADD COLUMN IF NOT EXISTS avg_confidence_score DOUBLE PRECISION",
                    f"ALTER TABLE {self.metrics_table} ADD COLUMN IF NOT EXISTS decline_rate DOUBLE PRECISION",
                    f"ALTER TABLE {self.metrics_table} ADD COLUMN IF NOT EXISTS refer_rate DOUBLE PRECISION",
                    f"ALTER TABLE {self.metrics_table} ADD COLUMN IF NOT EXISTS human_override_rate DOUBLE PRECISION",
                    f"ALTER TABLE {self.metrics_table} ADD COLUMN IF NOT EXISTS first_seen_at TIMESTAMPTZ",
                    f"ALTER TABLE {self.metrics_table} ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMPTZ",
                ]:
                    await conn.execute(statement)
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
            "decline_count": 0,
            "refer_count": 0,
            "override_count": 0,
            "approve_count": 0,
            "total_confidence": 0.0,
            "confidence_samples": 0,
            "first_seen_at": None,
            "last_seen_at": None,
            "updated_at": datetime.now(timezone.utc),
        }

    def _extract_decision_stats(self, events_written: list[dict[str, Any]]) -> tuple[int, int, int, int, float, int]:
        decision_count = 0
        approve_count = 0
        decline_count = 0
        refer_count = 0
        confidence_total = 0.0
        confidence_samples = 0

        for event in events_written:
            payload = event.get("payload") or {}
            recommendation = payload.get("recommendation") or event.get("recommendation")
            confidence = payload.get("confidence")

            if recommendation is not None:
                decision_count += 1
                normalized_recommendation = str(recommendation).upper()
                if normalized_recommendation == "APPROVE":
                    approve_count += 1
                elif normalized_recommendation == "DECLINE":
                    decline_count += 1
                elif normalized_recommendation == "REFER":
                    refer_count += 1
            elif event.get("event_type") == "ApplicationApproved":
                decision_count += 1
                approve_count += 1
            elif event.get("event_type") == "ApplicationDeclined":
                decision_count += 1
                decline_count += 1

            if confidence is None and isinstance(payload.get("decision"), dict):
                confidence = payload["decision"].get("confidence")
            if confidence is not None:
                confidence_total += float(confidence)
                confidence_samples += 1

        return decision_count, approve_count, decline_count, refer_count, confidence_total, confidence_samples

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
                    decision_count, decline_count, refer_count, override_count,
                    approve_count, total_confidence, confidence_samples, first_seen_at, last_seen_at, updated_at
                )
                VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)
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
                    decline_count = EXCLUDED.decline_count,
                    refer_count = EXCLUDED.refer_count,
                    override_count = EXCLUDED.override_count,
                    approve_count = EXCLUDED.approve_count,
                    total_confidence = EXCLUDED.total_confidence,
                    confidence_samples = EXCLUDED.confidence_samples,
                    first_seen_at = EXCLUDED.first_seen_at,
                    last_seen_at = EXCLUDED.last_seen_at,
                    updated_at = EXCLUDED.updated_at
                """,
                fact["session_id"], fact["agent_id"], fact["agent_type"], fact["model_version"], fact["application_id"],
                fact["started_at"], fact["completed_at"], fact["total_duration_ms"], fact["failed"],
                fact["decision_count"], fact["decline_count"], fact["refer_count"], fact["override_count"],
                fact["approve_count"], fact["total_confidence"], fact["confidence_samples"], fact["first_seen_at"], fact["last_seen_at"], fact["updated_at"],
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
        decline_count = sum(fact["decline_count"] for fact in facts)
        refer_count = sum(fact["refer_count"] for fact in facts)
        override_count = sum(fact["override_count"] for fact in facts)
        total_confidence = sum(fact["total_confidence"] for fact in facts)
        confidence_samples = sum(fact["confidence_samples"] for fact in facts)
        duration_values = [fact["total_duration_ms"] for fact in facts if fact["total_duration_ms"] is not None]
        first_seen_values = [fact["first_seen_at"] or fact["started_at"] or fact["updated_at"] for fact in facts]
        last_seen_values = [fact["last_seen_at"] or fact["updated_at"] for fact in facts]

        return {
            "agent_id": agent_id,
            "model_version": model_version,
            "analyses_completed": sessions_completed,
            "decisions_generated": decision_count,
            "avg_confidence_score": (total_confidence / confidence_samples) if confidence_samples else None,
            "decline_rate": (decline_count / decision_count) if decision_count else None,
            "refer_rate": (refer_count / decision_count) if decision_count else None,
            "human_override_rate": (override_count / decision_count) if decision_count else None,
            "first_seen_at": min(first_seen_values) if first_seen_values else None,
            "last_seen_at": max(last_seen_values) if last_seen_values else None,
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
                    avg_confidence, avg_duration_ms, approve_rate,
                    analyses_completed, decisions_generated, avg_confidence_score,
                    decline_rate, refer_rate, human_override_rate,
                    first_seen_at, last_seen_at, updated_at
                )
                VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)
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
                    analyses_completed = EXCLUDED.analyses_completed,
                    decisions_generated = EXCLUDED.decisions_generated,
                    avg_confidence_score = EXCLUDED.avg_confidence_score,
                    decline_rate = EXCLUDED.decline_rate,
                    refer_rate = EXCLUDED.refer_rate,
                    human_override_rate = EXCLUDED.human_override_rate,
                    first_seen_at = EXCLUDED.first_seen_at,
                    last_seen_at = EXCLUDED.last_seen_at,
                    updated_at = EXCLUDED.updated_at
                """,
                metrics["agent_id"], metrics["model_version"], metrics["sessions_started"], metrics["sessions_completed"],
                metrics["sessions_failed"], metrics["approve_count"], metrics["decision_count"],
                metrics["avg_confidence"], metrics["avg_duration_ms"], metrics["approve_rate"],
                metrics["analyses_completed"], metrics["decisions_generated"], metrics["avg_confidence_score"],
                metrics["decline_rate"], metrics["refer_rate"], metrics["human_override_rate"],
                metrics["first_seen_at"], metrics["last_seen_at"], metrics["updated_at"],
            )

    async def apply(self, event: StoredEvent) -> None:
        await self.setup()
        payload = event.payload
        event_type = event.event_type
        if event_type == "HumanReviewCompleted" and payload.get("override"):
            application_id = payload.get("application_id")
            decision_session_id = self._application_to_decision_session.get(application_id)
            if decision_session_id:
                fact = self._session_facts.get(decision_session_id)
                if fact is not None:
                    fact["override_count"] += 1
                    fact["last_seen_at"] = event.recorded_at
                    fact["updated_at"] = event.recorded_at
                    self._session_facts[decision_session_id] = fact
                    await self._persist_session_fact(fact)
                    metrics = self._recompute_metrics(fact["agent_id"], fact["model_version"])
                    self._metrics[(fact["agent_id"], fact["model_version"])] = metrics
                    await self._persist_metrics(metrics)
            return

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
                    "first_seen_at": fact["first_seen_at"] or payload.get("started_at") or event.recorded_at,
                }
            )
        elif event_type == "AgentOutputWritten":
            decisions, approves, declines, refers, confidence_total, confidence_samples = self._extract_decision_stats(
                list(payload.get("events_written") or [])
            )
            fact["decision_count"] += decisions
            fact["approve_count"] += approves
            fact["decline_count"] += declines
            fact["refer_count"] += refers
            fact["total_confidence"] += confidence_total
            fact["confidence_samples"] += confidence_samples
            if decisions and fact.get("application_id"):
                self._application_to_decision_session[fact["application_id"]] = session_id
        elif event_type == "AgentSessionCompleted":
            fact["completed_at"] = payload.get("completed_at") or event.recorded_at
            fact["total_duration_ms"] = payload.get("total_duration_ms")
        elif event_type == "AgentSessionFailed":
            fact["failed"] = True

        fact["first_seen_at"] = fact["first_seen_at"] or fact.get("started_at") or event.recorded_at
        fact["last_seen_at"] = event.recorded_at
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

    async def list_metrics(self, agent_id: str) -> list[dict[str, Any]]:
        pool_getter = getattr(self.store, "_require_pool", None)
        if callable(pool_getter) and self._initialized:
            pool = pool_getter()
            async with pool.acquire() as conn:
                rows = await conn.fetch(
                    f"SELECT * FROM {self.metrics_table} WHERE agent_id = $1 ORDER BY model_version ASC",
                    agent_id,
                )
                return [dict(row) for row in rows]

        rows = [
            dict(metrics)
            for (stored_agent_id, _), metrics in self._metrics.items()
            if stored_agent_id == agent_id
        ]
        rows.sort(key=lambda row: row.get("model_version") or "")
        return rows

    async def get_projection_lag(self) -> dict[str, Any]:
        checkpoint = await self.store.load_checkpoint(self.checkpoint_name)
        latest_event = None
        async for event in self.store.load_all(from_global_position=max(checkpoint - 1, 0), batch_size=500):
            latest_event = event
        if latest_event is None:
            return {"processed_position": checkpoint - 1, "position_lag": 0, "milliseconds_lag": 0}
        processed_position = checkpoint - 1
        last_seen = max(
            (metrics.get("last_seen_at") or metrics.get("updated_at") for metrics in self._metrics.values()),
            default=None,
        )
        milliseconds_lag = 0 if last_seen is None else max(0, int((latest_event.recorded_at - last_seen).total_seconds() * 1000))
        return {
            "processed_position": processed_position,
            "position_lag": max(0, latest_event.global_position - processed_position),
            "milliseconds_lag": milliseconds_lag,
        }