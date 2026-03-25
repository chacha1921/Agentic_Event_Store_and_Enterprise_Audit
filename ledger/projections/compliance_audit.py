from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from ledger.event_store import StoredEvent


class ComplianceAuditView:
    checkpoint_name = "projection.compliance_audit"

    def __init__(
        self,
        store,
        current_table: str = "compliance_audit_current",
        history_table: str = "compliance_audit_history",
    ):
        self.store = store
        self.current_table = current_table
        self.history_table = history_table
        self._initialized = False
        self._current: dict[str, dict[str, Any]] = {}
        self._history: dict[str, list[dict[str, Any]]] = {}

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
                await self._create_tables(conn, self.current_table, self.history_table)
        self._initialized = True

    async def _create_tables(self, conn, current_table: str, history_table: str) -> None:
        await conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {current_table} (
                application_id TEXT PRIMARY KEY,
                session_id TEXT,
                regulation_set_version TEXT,
                overall_verdict TEXT,
                has_hard_block BOOLEAN NOT NULL DEFAULT FALSE,
                rules_evaluated INTEGER NOT NULL DEFAULT 0,
                rules_passed INTEGER NOT NULL DEFAULT 0,
                rules_failed INTEGER NOT NULL DEFAULT 0,
                rules_noted INTEGER NOT NULL DEFAULT 0,
                rule_results JSONB NOT NULL DEFAULT '[]'::jsonb,
                note_results JSONB NOT NULL DEFAULT '[]'::jsonb,
                failure_reasons JSONB NOT NULL DEFAULT '[]'::jsonb,
                final_decision TEXT,
                as_of_global_position BIGINT NOT NULL DEFAULT -1,
                as_of_recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        await conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {history_table} (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                application_id TEXT NOT NULL,
                session_id TEXT,
                regulation_set_version TEXT,
                overall_verdict TEXT,
                has_hard_block BOOLEAN NOT NULL DEFAULT FALSE,
                rules_evaluated INTEGER NOT NULL DEFAULT 0,
                rules_passed INTEGER NOT NULL DEFAULT 0,
                rules_failed INTEGER NOT NULL DEFAULT 0,
                rules_noted INTEGER NOT NULL DEFAULT 0,
                rule_results JSONB NOT NULL DEFAULT '[]'::jsonb,
                note_results JSONB NOT NULL DEFAULT '[]'::jsonb,
                failure_reasons JSONB NOT NULL DEFAULT '[]'::jsonb,
                final_decision TEXT,
                as_of_global_position BIGINT NOT NULL,
                as_of_recorded_at TIMESTAMPTZ NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        await conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{history_table}_app_time ON {history_table}(application_id, as_of_recorded_at DESC)"
        )

    def _default_snapshot(self, application_id: str) -> dict[str, Any]:
        now = datetime.now(timezone.utc)
        return {
            "application_id": application_id,
            "session_id": None,
            "regulation_set_version": None,
            "overall_verdict": None,
            "has_hard_block": False,
            "rules_evaluated": 0,
            "rules_passed": 0,
            "rules_failed": 0,
            "rules_noted": 0,
            "rule_results": [],
            "note_results": [],
            "failure_reasons": [],
            "final_decision": None,
            "as_of_global_position": -1,
            "as_of_recorded_at": now,
            "updated_at": now,
        }

    def _apply_snapshot(self, snapshot: dict[str, Any], event: StoredEvent) -> dict[str, Any]:
        payload = event.payload
        event_type = event.event_type

        if event_type == "ComplianceCheckInitiated":
            snapshot["session_id"] = payload.get("session_id")
            snapshot["regulation_set_version"] = payload.get("regulation_set_version")
        elif event_type == "ComplianceRulePassed":
            snapshot["rule_results"] = snapshot["rule_results"] + [{
                "rule_id": payload.get("rule_id"),
                "rule_name": payload.get("rule_name"),
                "status": "PASSED",
                "evaluated_at": payload.get("evaluated_at"),
            }]
        elif event_type == "ComplianceRuleFailed":
            snapshot["rule_results"] = snapshot["rule_results"] + [{
                "rule_id": payload.get("rule_id"),
                "rule_name": payload.get("rule_name"),
                "status": "FAILED",
                "is_hard_block": bool(payload.get("is_hard_block", False)),
                "evaluated_at": payload.get("evaluated_at"),
            }]
            snapshot["failure_reasons"] = snapshot["failure_reasons"] + [payload.get("failure_reason")]
            snapshot["has_hard_block"] = snapshot["has_hard_block"] or bool(payload.get("is_hard_block", False))
        elif event_type == "ComplianceRuleNoted":
            snapshot["note_results"] = snapshot["note_results"] + [{
                "rule_id": payload.get("rule_id"),
                "rule_name": payload.get("rule_name"),
                "note_type": payload.get("note_type"),
                "note_text": payload.get("note_text"),
                "evaluated_at": payload.get("evaluated_at"),
            }]
        elif event_type == "ComplianceCheckCompleted":
            snapshot["session_id"] = payload.get("session_id")
            snapshot["rules_evaluated"] = int(payload.get("rules_evaluated", 0))
            snapshot["rules_passed"] = int(payload.get("rules_passed", 0))
            snapshot["rules_failed"] = int(payload.get("rules_failed", 0))
            snapshot["rules_noted"] = int(payload.get("rules_noted", 0))
            snapshot["has_hard_block"] = bool(payload.get("has_hard_block", False))
            snapshot["overall_verdict"] = payload.get("overall_verdict")
        elif event_type == "ApplicationDeclined":
            snapshot["final_decision"] = "DECLINED"
        elif event_type == "ApplicationApproved":
            snapshot["final_decision"] = "APPROVED"

        effective_timestamp = self._event_effective_at(event)
        snapshot["as_of_global_position"] = event.global_position
        snapshot["as_of_recorded_at"] = effective_timestamp
        snapshot["updated_at"] = event.recorded_at
        return snapshot

    def _event_effective_at(self, event: StoredEvent) -> datetime:
        payload = event.payload
        for key in (
            "completed_at",
            "evaluated_at",
            "initiated_at",
            "approved_at",
            "declined_at",
            "reviewed_at",
        ):
            value = payload.get(key)
            if isinstance(value, datetime):
                return self._normalize_timestamp(value)
            if isinstance(value, str):
                try:
                    return self._normalize_timestamp(datetime.fromisoformat(value))
                except ValueError:
                    continue
        return self._normalize_timestamp(event.recorded_at)

    async def _write_snapshot(self, snapshot: dict[str, Any], current_table: str | None = None, history_table: str | None = None) -> None:
        current_table = current_table or self.current_table
        history_table = history_table or self.history_table
        pool_getter = getattr(self.store, "_require_pool", None)
        if not callable(pool_getter):
            return
        pool = pool_getter()
        async with pool.acquire() as conn:
            await conn.execute(
                f"""
                INSERT INTO {history_table}(
                    application_id, session_id, regulation_set_version, overall_verdict,
                    has_hard_block, rules_evaluated, rules_passed, rules_failed, rules_noted,
                    rule_results, note_results, failure_reasons, final_decision,
                    as_of_global_position, as_of_recorded_at, updated_at
                )
                VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,$11::jsonb,$12::jsonb,$13,$14,$15,$16)
                """,
                snapshot["application_id"], snapshot["session_id"], snapshot["regulation_set_version"], snapshot["overall_verdict"],
                snapshot["has_hard_block"], snapshot["rules_evaluated"], snapshot["rules_passed"], snapshot["rules_failed"], snapshot["rules_noted"],
                json.dumps(snapshot["rule_results"]), json.dumps(snapshot["note_results"]), json.dumps(snapshot["failure_reasons"]), snapshot["final_decision"],
                snapshot["as_of_global_position"], snapshot["as_of_recorded_at"], snapshot["updated_at"],
            )
            await conn.execute(
                f"""
                INSERT INTO {current_table}(
                    application_id, session_id, regulation_set_version, overall_verdict,
                    has_hard_block, rules_evaluated, rules_passed, rules_failed, rules_noted,
                    rule_results, note_results, failure_reasons, final_decision,
                    as_of_global_position, as_of_recorded_at, updated_at
                )
                VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,$11::jsonb,$12::jsonb,$13,$14,$15,$16)
                ON CONFLICT (application_id)
                DO UPDATE SET
                    session_id = EXCLUDED.session_id,
                    regulation_set_version = EXCLUDED.regulation_set_version,
                    overall_verdict = EXCLUDED.overall_verdict,
                    has_hard_block = EXCLUDED.has_hard_block,
                    rules_evaluated = EXCLUDED.rules_evaluated,
                    rules_passed = EXCLUDED.rules_passed,
                    rules_failed = EXCLUDED.rules_failed,
                    rules_noted = EXCLUDED.rules_noted,
                    rule_results = EXCLUDED.rule_results,
                    note_results = EXCLUDED.note_results,
                    failure_reasons = EXCLUDED.failure_reasons,
                    final_decision = EXCLUDED.final_decision,
                    as_of_global_position = EXCLUDED.as_of_global_position,
                    as_of_recorded_at = EXCLUDED.as_of_recorded_at,
                    updated_at = EXCLUDED.updated_at
                """,
                snapshot["application_id"], snapshot["session_id"], snapshot["regulation_set_version"], snapshot["overall_verdict"],
                snapshot["has_hard_block"], snapshot["rules_evaluated"], snapshot["rules_passed"], snapshot["rules_failed"], snapshot["rules_noted"],
                json.dumps(snapshot["rule_results"]), json.dumps(snapshot["note_results"]), json.dumps(snapshot["failure_reasons"]), snapshot["final_decision"],
                snapshot["as_of_global_position"], snapshot["as_of_recorded_at"], snapshot["updated_at"],
            )

    async def apply(self, event: StoredEvent) -> None:
        await self.setup()
        payload = event.payload
        application_id = payload.get("application_id")
        if not application_id:
            return
        if event.event_type not in {
            "ComplianceCheckInitiated",
            "ComplianceRulePassed",
            "ComplianceRuleFailed",
            "ComplianceRuleNoted",
            "ComplianceCheckCompleted",
            "ApplicationDeclined",
            "ApplicationApproved",
        }:
            return

        snapshot = await self.get_current(application_id) or self._default_snapshot(application_id)
        snapshot = self._apply_snapshot(snapshot, event)
        self._current[application_id] = snapshot
        self._history.setdefault(application_id, []).append(dict(snapshot))
        await self._write_snapshot(snapshot)

    async def get_current(self, application_id: str) -> dict[str, Any] | None:
        pool_getter = getattr(self.store, "_require_pool", None)
        if callable(pool_getter) and self._initialized:
            pool = pool_getter()
            async with pool.acquire() as conn:
                row = await conn.fetchrow(
                    f"SELECT * FROM {self.current_table} WHERE application_id = $1",
                    application_id,
                )
                if row:
                    data = dict(row)
                    data["rule_results"] = list(data.get("rule_results") or [])
                    data["note_results"] = list(data.get("note_results") or [])
                    data["failure_reasons"] = list(data.get("failure_reasons") or [])
                    return data
        current = self._current.get(application_id)
        return dict(current) if current is not None else None

    def _normalize_timestamp(self, value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    async def get_compliance_at(self, application_id: str, timestamp: datetime) -> dict[str, Any] | None:
        normalized_timestamp = self._normalize_timestamp(timestamp)
        pool_getter = getattr(self.store, "_require_pool", None)
        if callable(pool_getter) and self._initialized:
            pool = pool_getter()
            async with pool.acquire() as conn:
                row = await conn.fetchrow(
                    f"""
                    SELECT * FROM {self.history_table}
                    WHERE application_id = $1 AND as_of_recorded_at <= $2
                    ORDER BY as_of_recorded_at DESC, as_of_global_position DESC
                    LIMIT 1
                    """,
                    application_id,
                    normalized_timestamp,
                )
                if row:
                    data = dict(row)
                    data["rule_results"] = list(data.get("rule_results") or [])
                    data["note_results"] = list(data.get("note_results") or [])
                    data["failure_reasons"] = list(data.get("failure_reasons") or [])
                    return data

        history = self._history.get(application_id, [])
        history = [
            snapshot
            for snapshot in history
            if self._normalize_timestamp(snapshot["as_of_recorded_at"]) <= normalized_timestamp
        ]
        if not history:
            return None
        history.sort(key=lambda snapshot: (snapshot["as_of_recorded_at"], snapshot["as_of_global_position"]), reverse=True)
        return dict(history[0])

    async def rebuild_from_scratch(self) -> None:
        await self.setup()
        pool_getter = getattr(self.store, "_require_pool", None)
        using_postgres = callable(pool_getter)
        shadow_current = f"{self.current_table}_rebuild"
        shadow_history = f"{self.history_table}_rebuild"

        snapshots: dict[str, dict[str, Any]] = {}
        histories: dict[str, list[dict[str, Any]]] = {}

        if using_postgres:
            pool = pool_getter()
            async with pool.acquire() as conn:
                await conn.execute(f"DROP TABLE IF EXISTS {shadow_current}")
                await conn.execute(f"DROP TABLE IF EXISTS {shadow_history}")
                await self._create_tables(conn, shadow_current, shadow_history)

        async for event in self.store.load_all(from_position=0):
            payload = event.payload
            application_id = payload.get("application_id")
            if not application_id:
                continue
            if event.event_type not in {
                "ComplianceCheckInitiated",
                "ComplianceRulePassed",
                "ComplianceRuleFailed",
                "ComplianceRuleNoted",
                "ComplianceCheckCompleted",
                "ApplicationDeclined",
                "ApplicationApproved",
            }:
                continue
            snapshot = snapshots.get(application_id) or self._default_snapshot(application_id)
            snapshot = self._apply_snapshot(snapshot, event)
            snapshots[application_id] = snapshot
            histories.setdefault(application_id, []).append(dict(snapshot))
            if using_postgres:
                await self._write_snapshot(snapshot, current_table=shadow_current, history_table=shadow_history)

        if using_postgres:
            pool = pool_getter()
            backup_current = f"{self.current_table}_old"
            backup_history = f"{self.history_table}_old"
            async with pool.acquire() as conn:
                async with conn.transaction():
                    await conn.execute(f"DROP TABLE IF EXISTS {backup_current}")
                    await conn.execute(f"DROP TABLE IF EXISTS {backup_history}")
                    await conn.execute(f"ALTER TABLE {self.current_table} RENAME TO {backup_current}")
                    await conn.execute(f"ALTER TABLE {self.history_table} RENAME TO {backup_history}")
                    await conn.execute(f"ALTER TABLE {shadow_current} RENAME TO {self.current_table}")
                    await conn.execute(f"ALTER TABLE {shadow_history} RENAME TO {self.history_table}")
                    await conn.execute(f"DROP TABLE {backup_current}")
                    await conn.execute(f"DROP TABLE {backup_history}")

        self._current = snapshots
        self._history = histories