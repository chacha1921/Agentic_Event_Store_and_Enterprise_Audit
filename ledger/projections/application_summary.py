from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from ledger.event_store import StoredEvent


class ApplicationSummary:
    checkpoint_name = "projection.application_summary"

    def __init__(self, store, table_name: str = "application_summary"):
        self.store = store
        self.table_name = table_name
        self._initialized = False
        self._rows: dict[str, dict[str, Any]] = {}

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
                    CREATE TABLE IF NOT EXISTS {self.table_name} (
                        application_id TEXT PRIMARY KEY,
                        applicant_id TEXT,
                        requested_amount_usd NUMERIC,
                        approved_amount_usd NUMERIC,
                        loan_purpose TEXT,
                        state TEXT,
                        decision TEXT,
                        risk_tier TEXT,
                        fraud_score DOUBLE PRECISION,
                        compliance_status TEXT,
                        agent_sessions_completed JSONB NOT NULL DEFAULT '[]'::jsonb,
                        last_event_type TEXT,
                        last_event_at TIMESTAMPTZ,
                        human_reviewer_id TEXT,
                        final_decision_at TIMESTAMPTZ,
                        recommended_limit_usd NUMERIC,
                        credit_confidence DOUBLE PRECISION,
                        fraud_risk_level TEXT,
                        compliance_hard_block BOOLEAN NOT NULL DEFAULT FALSE,
                        final_decision TEXT,
                        decline_reasons JSONB NOT NULL DEFAULT '[]'::jsonb,
                        last_event_id TEXT,
                        last_global_position BIGINT NOT NULL DEFAULT -1,
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                    """
                )
                for statement in [
                    f"ALTER TABLE {self.table_name} ADD COLUMN IF NOT EXISTS approved_amount_usd NUMERIC",
                    f"ALTER TABLE {self.table_name} ADD COLUMN IF NOT EXISTS decision TEXT",
                    f"ALTER TABLE {self.table_name} ADD COLUMN IF NOT EXISTS agent_sessions_completed JSONB NOT NULL DEFAULT '[]'::jsonb",
                    f"ALTER TABLE {self.table_name} ADD COLUMN IF NOT EXISTS last_event_type TEXT",
                    f"ALTER TABLE {self.table_name} ADD COLUMN IF NOT EXISTS last_event_at TIMESTAMPTZ",
                    f"ALTER TABLE {self.table_name} ADD COLUMN IF NOT EXISTS human_reviewer_id TEXT",
                    f"ALTER TABLE {self.table_name} ADD COLUMN IF NOT EXISTS final_decision_at TIMESTAMPTZ",
                ]:
                    await conn.execute(statement)
        self._initialized = True

    def _default_row(self, application_id: str) -> dict[str, Any]:
        return {
            "application_id": application_id,
            "applicant_id": None,
            "requested_amount_usd": None,
            "approved_amount_usd": None,
            "loan_purpose": None,
            "state": None,
            "decision": None,
            "risk_tier": None,
            "fraud_score": None,
            "compliance_status": None,
            "agent_sessions_completed": [],
            "last_event_type": None,
            "last_event_at": None,
            "human_reviewer_id": None,
            "final_decision_at": None,
            "recommended_limit_usd": None,
            "credit_confidence": None,
            "fraud_risk_level": None,
            "compliance_hard_block": False,
            "final_decision": None,
            "decline_reasons": [],
            "last_event_id": None,
            "last_global_position": -1,
            "updated_at": datetime.now(timezone.utc),
        }

    def _application_id_from(self, event: StoredEvent) -> str | None:
        payload = event.payload
        return payload.get("application_id") or payload.get("package_id")

    def _apply_to_row(self, row: dict[str, Any], event: StoredEvent) -> dict[str, Any]:
        payload = event.payload
        event_type = event.event_type

        if event_type == "ApplicationSubmitted":
            row["applicant_id"] = payload.get("applicant_id")
            amount = payload.get("requested_amount_usd")
            row["requested_amount_usd"] = Decimal(str(amount)) if amount is not None else None
            row["loan_purpose"] = payload.get("loan_purpose")
            row["state"] = "Submitted"
        elif event_type == "DocumentUploadRequested":
            row["state"] = row.get("state") or "Submitted"
        elif event_type == "DocumentUploaded":
            row["state"] = row.get("state") or "Submitted"
        elif event_type == "PackageReadyForAnalysis":
            row["state"] = row.get("state") or "Submitted"
        elif event_type == "CreditAnalysisRequested":
            row["state"] = "AwaitingAnalysis"
        elif event_type == "CreditAnalysisCompleted":
            decision = payload.get("decision") or {}
            row["risk_tier"] = decision.get("risk_tier")
            limit_value = decision.get("recommended_limit_usd")
            row["recommended_limit_usd"] = Decimal(str(limit_value)) if limit_value is not None else None
            confidence = decision.get("confidence")
            row["credit_confidence"] = float(confidence) if confidence is not None else None
            row["state"] = row.get("state") or "AwaitingAnalysis"
        elif event_type == "FraudScreeningRequested":
            row["state"] = "AwaitingAnalysis"
        elif event_type == "FraudScreeningCompleted":
            score = payload.get("fraud_score")
            row["fraud_score"] = float(score) if score is not None else None
            row["fraud_risk_level"] = payload.get("risk_level")
            row["state"] = row.get("state") or "AwaitingAnalysis"
        elif event_type in {"ComplianceCheckRequested", "ComplianceCheckInitiated"}:
            row["state"] = "ComplianceReview"
        elif event_type == "ComplianceCheckCompleted":
            row["compliance_status"] = payload.get("overall_verdict")
            row["compliance_hard_block"] = bool(payload.get("has_hard_block", False))
            row["state"] = "DECLINED" if row["compliance_hard_block"] else "PendingDecision"
        elif event_type == "DecisionRequested":
            row["state"] = "PendingDecision"
        elif event_type == "DecisionGenerated":
            recommendation = payload.get("recommendation")
            row["decision"] = recommendation
            if recommendation == "APPROVE":
                row["state"] = "ApprovedPendingHuman"
            elif recommendation == "DECLINE":
                row["state"] = "DeclinedPendingHuman"
            else:
                row["state"] = "PendingDecision"
        elif event_type == "HumanReviewRequested":
            row["state"] = row.get("state") or "PendingDecision"
        elif event_type == "HumanReviewCompleted":
            row["final_decision"] = payload.get("final_decision")
            row["human_reviewer_id"] = payload.get("reviewer_id")
        elif event_type == "ApplicationApproved":
            approved_amount = payload.get("approved_amount_usd")
            row["approved_amount_usd"] = Decimal(str(approved_amount)) if approved_amount is not None else None
            row["final_decision"] = "APPROVED"
            row["final_decision_at"] = payload.get("approved_at") or event.recorded_at
            row["state"] = "APPROVED"
        elif event_type == "ApplicationDeclined":
            row["decline_reasons"] = list(payload.get("decline_reasons") or [])
            row["final_decision"] = "DECLINED"
            row["final_decision_at"] = payload.get("declined_at") or event.recorded_at
            row["state"] = "DECLINED"
        elif event_type == "AgentSessionCompleted":
            session_id = payload.get("session_id")
            if session_id and session_id not in row["agent_sessions_completed"]:
                row["agent_sessions_completed"] = row["agent_sessions_completed"] + [session_id]

        row["last_event_id"] = event.event_id
        row["last_global_position"] = event.global_position
        row["last_event_type"] = event.event_type
        row["last_event_at"] = event.recorded_at
        row["updated_at"] = event.recorded_at
        return row

    async def apply(self, event: StoredEvent) -> None:
        await self.setup()
        application_id = self._application_id_from(event)
        if not application_id:
            return

        row = await self.get(application_id) or self._default_row(application_id)
        row = self._apply_to_row(row, event)

        pool_getter = getattr(self.store, "_require_pool", None)
        if callable(pool_getter):
            pool = pool_getter()
            async with pool.acquire() as conn:
                await conn.execute(
                    f"""
                    INSERT INTO {self.table_name}(
                        application_id, applicant_id, requested_amount_usd, loan_purpose,
                        approved_amount_usd, state, decision, risk_tier, fraud_score,
                        compliance_status, agent_sessions_completed, last_event_type, last_event_at,
                        human_reviewer_id, final_decision_at,
                        recommended_limit_usd, credit_confidence,
                        fraud_risk_level, compliance_hard_block,
                        final_decision, decline_reasons,
                        last_event_id, last_global_position, updated_at
                    )
                    VALUES(
                        $1, $2, $3, $4,
                        $5, $6, $7, $8,
                        $9, $10, $11::jsonb, $12, $13,
                        $14, $15,
                        $16, $17,
                        $18, $19,
                        $20, $21::jsonb,
                        $22, $23, $24
                    )
                    ON CONFLICT (application_id)
                    DO UPDATE SET
                        applicant_id = EXCLUDED.applicant_id,
                        requested_amount_usd = EXCLUDED.requested_amount_usd,
                        loan_purpose = EXCLUDED.loan_purpose,
                        approved_amount_usd = EXCLUDED.approved_amount_usd,
                        state = EXCLUDED.state,
                        decision = EXCLUDED.decision,
                        risk_tier = EXCLUDED.risk_tier,
                        fraud_score = EXCLUDED.fraud_score,
                        compliance_status = EXCLUDED.compliance_status,
                        agent_sessions_completed = EXCLUDED.agent_sessions_completed,
                        last_event_type = EXCLUDED.last_event_type,
                        last_event_at = EXCLUDED.last_event_at,
                        human_reviewer_id = EXCLUDED.human_reviewer_id,
                        final_decision_at = EXCLUDED.final_decision_at,
                        recommended_limit_usd = EXCLUDED.recommended_limit_usd,
                        credit_confidence = EXCLUDED.credit_confidence,
                        fraud_risk_level = EXCLUDED.fraud_risk_level,
                        compliance_hard_block = EXCLUDED.compliance_hard_block,
                        final_decision = EXCLUDED.final_decision,
                        decline_reasons = EXCLUDED.decline_reasons,
                        last_event_id = EXCLUDED.last_event_id,
                        last_global_position = EXCLUDED.last_global_position,
                        updated_at = EXCLUDED.updated_at
                    """,
                    row["application_id"], row["applicant_id"], row["requested_amount_usd"], row["loan_purpose"],
                    row["approved_amount_usd"], row["state"], row["decision"], row["risk_tier"],
                    row["fraud_score"], row["compliance_status"], json.dumps(row["agent_sessions_completed"]), row["last_event_type"], row["last_event_at"],
                    row["human_reviewer_id"], row["final_decision_at"],
                    row["recommended_limit_usd"], row["credit_confidence"],
                    row["fraud_risk_level"], row["compliance_hard_block"],
                    row["final_decision"], json.dumps(row["decline_reasons"]),
                    row["last_event_id"], row["last_global_position"], row["updated_at"],
                )
        self._rows[application_id] = row

    async def get(self, application_id: str) -> dict[str, Any] | None:
        pool_getter = getattr(self.store, "_require_pool", None)
        if callable(pool_getter) and self._initialized:
            pool = pool_getter()
            async with pool.acquire() as conn:
                row = await conn.fetchrow(
                    f"SELECT * FROM {self.table_name} WHERE application_id = $1",
                    application_id,
                )
                if row:
                    data = dict(row)
                    data["agent_sessions_completed"] = list(data.get("agent_sessions_completed") or [])
                    data["decline_reasons"] = list(data.get("decline_reasons") or [])
                    return data
        cached = self._rows.get(application_id)
        return dict(cached) if cached is not None else None

    async def get_projection_lag(self) -> dict[str, Any]:
        checkpoint = await self.store.load_checkpoint(self.checkpoint_name)
        latest_event = None
        async for event in self.store.load_all(from_global_position=max(checkpoint - 1, 0), batch_size=500):
            latest_event = event
        if latest_event is None:
            return {"processed_position": checkpoint - 1, "position_lag": 0, "milliseconds_lag": 0}
        processed_position = checkpoint - 1
        position_lag = max(0, latest_event.global_position - processed_position)
        current_row = None
        if self._rows:
            current_row = max(self._rows.values(), key=lambda row: row.get("last_global_position", -1))
        processed_at = current_row.get("last_event_at") if current_row is not None else latest_event.recorded_at
        if isinstance(processed_at, str):
            processed_at = datetime.fromisoformat(processed_at)
        milliseconds_lag = max(0, int((latest_event.recorded_at - processed_at).total_seconds() * 1000))
        return {
            "processed_position": processed_position,
            "position_lag": position_lag,
            "milliseconds_lag": milliseconds_lag,
        }