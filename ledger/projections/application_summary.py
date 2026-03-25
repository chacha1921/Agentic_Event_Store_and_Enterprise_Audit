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
                        loan_purpose TEXT,
                        state TEXT,
                        risk_tier TEXT,
                        recommended_limit_usd NUMERIC,
                        credit_confidence DOUBLE PRECISION,
                        fraud_score DOUBLE PRECISION,
                        fraud_risk_level TEXT,
                        compliance_status TEXT,
                        compliance_hard_block BOOLEAN NOT NULL DEFAULT FALSE,
                        final_decision TEXT,
                        approved_amount_usd NUMERIC,
                        decline_reasons JSONB NOT NULL DEFAULT '[]'::jsonb,
                        last_event_id TEXT,
                        last_global_position BIGINT NOT NULL DEFAULT -1,
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                    """
                )
        self._initialized = True

    def _default_row(self, application_id: str) -> dict[str, Any]:
        return {
            "application_id": application_id,
            "applicant_id": None,
            "requested_amount_usd": None,
            "loan_purpose": None,
            "state": None,
            "risk_tier": None,
            "recommended_limit_usd": None,
            "credit_confidence": None,
            "fraud_score": None,
            "fraud_risk_level": None,
            "compliance_status": None,
            "compliance_hard_block": False,
            "final_decision": None,
            "approved_amount_usd": None,
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
            row["state"] = "SUBMITTED"
        elif event_type == "DocumentUploadRequested":
            row["state"] = "DOCUMENTS_PENDING"
        elif event_type == "DocumentUploaded":
            row["state"] = "DOCUMENTS_UPLOADED"
        elif event_type == "PackageReadyForAnalysis":
            row["state"] = "DOCUMENTS_PROCESSED"
        elif event_type == "CreditAnalysisRequested":
            row["state"] = "CREDIT_ANALYSIS_REQUESTED"
        elif event_type == "CreditAnalysisCompleted":
            decision = payload.get("decision") or {}
            row["risk_tier"] = decision.get("risk_tier")
            limit_value = decision.get("recommended_limit_usd")
            row["recommended_limit_usd"] = Decimal(str(limit_value)) if limit_value is not None else None
            confidence = decision.get("confidence")
            row["credit_confidence"] = float(confidence) if confidence is not None else None
            row["state"] = "CREDIT_ANALYSIS_COMPLETE"
        elif event_type == "FraudScreeningRequested":
            row["state"] = "FRAUD_SCREENING_REQUESTED"
        elif event_type == "FraudScreeningCompleted":
            score = payload.get("fraud_score")
            row["fraud_score"] = float(score) if score is not None else None
            row["fraud_risk_level"] = payload.get("risk_level")
            row["state"] = "FRAUD_SCREENING_COMPLETE"
        elif event_type in {"ComplianceCheckRequested", "ComplianceCheckInitiated"}:
            row["state"] = "COMPLIANCE_CHECK_REQUESTED"
        elif event_type == "ComplianceCheckCompleted":
            row["compliance_status"] = payload.get("overall_verdict")
            row["compliance_hard_block"] = bool(payload.get("has_hard_block", False))
            row["state"] = "DECLINED_COMPLIANCE" if row["compliance_hard_block"] else "COMPLIANCE_CHECK_COMPLETE"
        elif event_type == "DecisionRequested":
            row["state"] = "PENDING_DECISION"
        elif event_type == "DecisionGenerated":
            recommendation = payload.get("recommendation")
            if recommendation == "REFER":
                row["state"] = "PENDING_HUMAN_REVIEW"
            else:
                row["state"] = "PENDING_DECISION"
        elif event_type == "HumanReviewRequested":
            row["state"] = "PENDING_HUMAN_REVIEW"
        elif event_type == "HumanReviewCompleted":
            row["final_decision"] = payload.get("final_decision")
        elif event_type == "ApplicationApproved":
            approved_amount = payload.get("approved_amount_usd")
            row["approved_amount_usd"] = Decimal(str(approved_amount)) if approved_amount is not None else None
            row["final_decision"] = "APPROVED"
            row["state"] = "APPROVED"
        elif event_type == "ApplicationDeclined":
            row["decline_reasons"] = list(payload.get("decline_reasons") or [])
            row["final_decision"] = "DECLINED"
            row["state"] = "DECLINED_COMPLIANCE" if row.get("compliance_hard_block") else "DECLINED"

        row["last_event_id"] = event.event_id
        row["last_global_position"] = event.global_position
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
                        state, risk_tier, recommended_limit_usd, credit_confidence,
                        fraud_score, fraud_risk_level, compliance_status, compliance_hard_block,
                        final_decision, approved_amount_usd, decline_reasons,
                        last_event_id, last_global_position, updated_at
                    )
                    VALUES(
                        $1, $2, $3, $4,
                        $5, $6, $7, $8,
                        $9, $10, $11, $12,
                        $13, $14, $15::jsonb,
                        $16, $17, $18
                    )
                    ON CONFLICT (application_id)
                    DO UPDATE SET
                        applicant_id = EXCLUDED.applicant_id,
                        requested_amount_usd = EXCLUDED.requested_amount_usd,
                        loan_purpose = EXCLUDED.loan_purpose,
                        state = EXCLUDED.state,
                        risk_tier = EXCLUDED.risk_tier,
                        recommended_limit_usd = EXCLUDED.recommended_limit_usd,
                        credit_confidence = EXCLUDED.credit_confidence,
                        fraud_score = EXCLUDED.fraud_score,
                        fraud_risk_level = EXCLUDED.fraud_risk_level,
                        compliance_status = EXCLUDED.compliance_status,
                        compliance_hard_block = EXCLUDED.compliance_hard_block,
                        final_decision = EXCLUDED.final_decision,
                        approved_amount_usd = EXCLUDED.approved_amount_usd,
                        decline_reasons = EXCLUDED.decline_reasons,
                        last_event_id = EXCLUDED.last_event_id,
                        last_global_position = EXCLUDED.last_global_position,
                        updated_at = EXCLUDED.updated_at
                    """,
                    row["application_id"], row["applicant_id"], row["requested_amount_usd"], row["loan_purpose"],
                    row["state"], row["risk_tier"], row["recommended_limit_usd"], row["credit_confidence"],
                    row["fraud_score"], row["fraud_risk_level"], row["compliance_status"], row["compliance_hard_block"],
                    row["final_decision"], row["approved_amount_usd"], json.dumps(row["decline_reasons"]),
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
                    data["decline_reasons"] = list(data.get("decline_reasons") or [])
                    return data
        cached = self._rows.get(application_id)
        return dict(cached) if cached is not None else None