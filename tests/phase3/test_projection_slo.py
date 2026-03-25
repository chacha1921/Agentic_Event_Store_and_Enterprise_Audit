from __future__ import annotations

import asyncio
from datetime import datetime
from decimal import Decimal

import pytest
import pytest_asyncio

from ledger.commands.handlers import SubmitApplicationCommand, handle_submit_application
from ledger.event_store import InMemoryEventStore
from ledger.projections import AgentPerformanceLedger, ApplicationSummary, ComplianceAuditView, ProjectionDaemon
from ledger.schema.events import (
    ApplicationSubmitted,
    ComplianceCheckCompleted,
    ComplianceCheckInitiated,
    ComplianceVerdict,
    LoanPurpose,
)


@pytest_asyncio.fixture
async def store() -> InMemoryEventStore:
    return InMemoryEventStore()


def _submitted(application_id: str) -> dict:
    return ApplicationSubmitted(
        application_id=application_id,
        applicant_id=f"COMP-{application_id}",
        requested_amount_usd=Decimal("500000"),
        loan_purpose=LoanPurpose.WORKING_CAPITAL,
        loan_term_months=36,
        submission_channel="web",
        contact_email="borrower@example.com",
        contact_name="Borrower One",
        submitted_at=datetime.now(),
        application_reference=application_id,
    ).to_store_dict()


@pytest.mark.asyncio
async def test_projection_daemon_slo_under_concurrent_load_and_rebuild_live_reads(store: InMemoryEventStore):
    summary = ApplicationSummary(store)
    audit = ComplianceAuditView(store)
    performance = AgentPerformanceLedger(store)
    daemon = ProjectionDaemon(store, [summary, audit, performance], batch_size=500, poll_interval=0.01)

    async def append_one(index: int) -> None:
        app_id = f"SLO-{index:03d}"
        await handle_submit_application(
            store,
            SubmitApplicationCommand(
                application_id=app_id,
                applicant_id=f"COMP-{app_id}",
                requested_amount_usd=Decimal("500000"),
                loan_purpose=LoanPurpose.WORKING_CAPITAL,
                loan_term_months=36,
                submission_channel="web",
                contact_email="borrower@example.com",
                contact_name="Borrower One",
                submitted_at=datetime.now(),
                application_reference=app_id,
                deadline=datetime.now(),
            ),
        )

    await asyncio.gather(*(append_one(index) for index in range(50)))
    for index in range(150):
        await store.append(
            f"compliance-SLO-BULK-{index:03d}",
            [
                ComplianceCheckInitiated(
                    application_id=f"SLO-BULK-{index:03d}",
                    session_id=f"sess-cmp-bulk-{index:03d}",
                    regulation_set_version="2026-Q1",
                    rules_to_evaluate=["REG-001"],
                    initiated_at=datetime.now(),
                ).to_store_dict(),
                ComplianceCheckCompleted(
                    application_id=f"SLO-BULK-{index:03d}",
                    session_id=f"sess-cmp-bulk-{index:03d}",
                    rules_evaluated=1,
                    rules_passed=1,
                    rules_failed=0,
                    rules_noted=0,
                    has_hard_block=False,
                    overall_verdict=ComplianceVerdict.CLEAR,
                    completed_at=datetime.now(),
                ).to_store_dict(),
            ],
            expected_version=-1,
        )
    await store.append(
        "compliance-SLO-AUD-001",
        [
            ComplianceCheckInitiated(
                application_id="SLO-AUD-001",
                session_id="sess-cmp-slo-001",
                regulation_set_version="2026-Q1",
                rules_to_evaluate=["REG-001"],
                initiated_at=datetime.now(),
            ).to_store_dict(),
            ComplianceCheckCompleted(
                application_id="SLO-AUD-001",
                session_id="sess-cmp-slo-001",
                rules_evaluated=1,
                rules_passed=1,
                rules_failed=0,
                rules_noted=0,
                has_hard_block=False,
                overall_verdict=ComplianceVerdict.CLEAR,
                completed_at=datetime.now(),
            ).to_store_dict(),
        ],
        expected_version=-1,
    )

    lags_before = await daemon.get_all_lags()
    await daemon.run_once()
    lags_after = await daemon.get_all_lags()

    for projection_lag in lags_before["projections"].values():
        assert projection_lag["position_lag"] >= 0
        assert projection_lag["milliseconds_lag"] >= 0

    assert lags_after["projections"][summary.checkpoint_name]["position_lag"] == 0
    assert lags_after["projections"][summary.checkpoint_name]["milliseconds_lag"] <= 500
    assert lags_after["projections"][audit.checkpoint_name]["position_lag"] == 0
    assert lags_after["projections"][audit.checkpoint_name]["milliseconds_lag"] <= 2000
    assert lags_after["projections"][performance.checkpoint_name]["position_lag"] == 0
    assert lags_after["projections"][performance.checkpoint_name]["milliseconds_lag"] <= 500

    observed_reads: list[dict] = []

    async def live_reader(rebuild_task: asyncio.Task[None]) -> None:
        await asyncio.sleep(0)
        while not rebuild_task.done():
            row = await audit.get_current("SLO-AUD-001")
            if row is not None:
                observed_reads.append(row)
            await asyncio.sleep(0)

    original_rebuild = audit.rebuild_from_scratch

    async def tracked_rebuild() -> None:
        await asyncio.sleep(0.01)
        await original_rebuild()

    rebuild_task = asyncio.create_task(tracked_rebuild())
    reader_task = asyncio.create_task(live_reader(rebuild_task))
    await rebuild_task
    await reader_task

    assert observed_reads
    assert any(row["overall_verdict"] == "CLEAR" for row in observed_reads)
    rebuilt = await audit.get_current("SLO-AUD-001")
    assert rebuilt is not None
    assert rebuilt["overall_verdict"] == "CLEAR"