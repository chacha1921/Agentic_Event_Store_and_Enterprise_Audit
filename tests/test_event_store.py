import os

import pytest
pytestmark = pytest.mark.skipif(not os.environ.get("DATABASE_URL"), reason="Requires PostgreSQL via DATABASE_URL")
"""
tests/test_event_store.py
=========================
Phase 1 tests: EventStore implementation.
These tests FAIL until you implement EventStore. That is expected.
When all pass, your event store is correct.

Run: pytest tests/test_event_store.py -v
"""
import asyncio, pytest, sys
from pathlib import Path; sys.path.insert(0, str(Path(__file__).parent.parent))
from ledger.event_store import EventStore, OptimisticConcurrencyError

DB_URL = os.environ.get("DATABASE_URL", "postgresql://localhost/apex_ledger")

@pytest.fixture
async def store():
    s = EventStore(DB_URL); await s.connect()
    yield s
    await s.close()

def _event(etype, n=1):
    return [{"event_type":etype,"event_version":1,"payload":{"seq":i,"test":True}} for i in range(n)]

@pytest.mark.asyncio
async def test_append_new_stream(store):
    new_version = await store.append("test-new-001", _event("TestEvent"), expected_version=-1)
    assert new_version == 1

@pytest.mark.asyncio
async def test_append_existing_stream(store):
    await store.append("test-exist-001", _event("TestEvent"), expected_version=-1)
    new_version = await store.append("test-exist-001", _event("TestEvent2"), expected_version=1)
    assert new_version == 2

@pytest.mark.asyncio
async def test_occ_wrong_version_raises(store):
    await store.append("test-occ-001", _event("E"), expected_version=-1)
    with pytest.raises(OptimisticConcurrencyError) as exc:
        await store.append("test-occ-001", _event("E"), expected_version=99)
    assert exc.value.expected == 99; assert exc.value.actual == 1

@pytest.mark.asyncio
async def test_concurrent_double_append_exactly_one_succeeds(store):
    """Exact Apex OCC contract: two agents read version 3, one append wins at position 4."""
    stream_id = "credit-APEX-OCC-001"
    await store.append(stream_id, _event("ApplicationSubmitted"), expected_version=-1)
    await store.append(stream_id, _event("DocumentUploadRequested"), expected_version=1)
    await store.append(stream_id, _event("CreditAnalysisRequested"), expected_version=2)

    async def attempt(agent_name: str):
        return await store.append(
            stream_id,
            [
                {
                    "event_type": "CreditAnalysisCompleted",
                    "event_version": 1,
                    "payload": {"agent": agent_name, "score": 0.42},
                }
            ],
            expected_version=3,
        )

    results = await asyncio.gather(attempt("agent-a"), attempt("agent-b"), return_exceptions=True)
    successes = [r for r in results if isinstance(r, int)]
    errors = [r for r in results if isinstance(r, OptimisticConcurrencyError)]
    assert len(successes) == 1, f"Expected exactly 1 success, got {len(successes)}"
    assert len(errors) == 1
    assert successes[0] == 4

    events = await store.load_stream(stream_id)
    completed = [event for event in events if event["event_type"] == "CreditAnalysisCompleted"]
    assert len(events) == 4
    assert len(completed) == 1
    assert completed[0]["stream_position"] == 4
    assert errors[0].expected == 3
    assert errors[0].actual == 4

@pytest.mark.asyncio
async def test_load_stream_ordered(store):
    await store.append("test-load-001", _event("E",3), expected_version=-1)
    events = await store.load_stream("test-load-001")
    assert len(events) == 3
    positions = [e["stream_position"] for e in events]
    assert positions == sorted(positions)

@pytest.mark.asyncio
async def test_stream_version(store):
    await store.append("test-ver-001", _event("E",4), expected_version=-1)
    assert await store.stream_version("test-ver-001") == 4

@pytest.mark.asyncio
async def test_stream_version_nonexistent(store):
    assert await store.stream_version("test-does-not-exist") == -1

@pytest.mark.asyncio
async def test_load_all_yields_in_global_order(store):
    await store.append("test-global-A", _event("E",2), expected_version=-1)
    await store.append("test-global-B", _event("E",2), expected_version=-1)
    all_events = [e async for e in store.load_all(from_global_position=0)]
    positions = [e["global_position"] for e in all_events]
    assert positions == sorted(positions)
