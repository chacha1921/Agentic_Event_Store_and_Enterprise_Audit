"""
tests/phase1/test_event_store.py
=================================
Phase 1 gate tests — EventStore core.
All must pass before starting Phase 2.

Run: pytest tests/phase1/ -v
"""
import asyncio
import pytest
import pytest_asyncio

from ledger.event_store import InMemoryEventStore, OptimisticConcurrencyError


# ─── HELPERS ──────────────────────────────────────────────────────────────────

def _ev(event_type: str, **payload) -> dict:
    return {"event_type": event_type, "event_version": 1, "payload": payload}


# ─── SCHEMA TESTS ─────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_new_stream_version_is_minus_one():
    store = InMemoryEventStore()
    v = await store.stream_version("loan-APEX-0001")
    assert v == -1, "Non-existent stream must return -1"

@pytest.mark.asyncio
async def test_append_new_stream_succeeds():
    store = InMemoryEventStore()
    positions = await store.append(
        "loan-APEX-0001",
        [_ev("ApplicationSubmitted", application_id="APEX-0001")],
        expected_version=-1,
    )
    assert positions == [0]
    assert await store.stream_version("loan-APEX-0001") == 0

@pytest.mark.asyncio
async def test_append_increments_version():
    store = InMemoryEventStore()
    await store.append("s", [_ev("E1")], expected_version=-1)
    await store.append("s", [_ev("E2")], expected_version=0)
    await store.append("s", [_ev("E3")], expected_version=1)
    assert await store.stream_version("s") == 2

@pytest.mark.asyncio
async def test_append_wrong_version_raises():
    store = InMemoryEventStore()
    await store.append("s", [_ev("E1")], expected_version=-1)
    with pytest.raises(OptimisticConcurrencyError) as exc_info:
        await store.append("s", [_ev("E2")], expected_version=-1)  # should be 0
    assert exc_info.value.stream_id == "s"
    assert exc_info.value.expected == -1
    assert exc_info.value.actual == 0

@pytest.mark.asyncio
async def test_concurrent_double_append_exactly_one_succeeds():
    """The critical OCC test: concurrent appends at same expected_version."""
    store = InMemoryEventStore()
    await store.append("s", [_ev("Base")], expected_version=-1)

    results = []
    async def attempt():
        try:
            await store.append("s", [_ev("Concurrent")], expected_version=0)
            results.append("success")
        except OptimisticConcurrencyError:
            results.append("occ")

    await asyncio.gather(attempt(), attempt())
    assert results.count("success") == 1, "Exactly one concurrent append must succeed"
    assert results.count("occ") == 1
    assert await store.stream_version("s") == 1

@pytest.mark.asyncio
async def test_load_stream_returns_events_in_order():
    store = InMemoryEventStore()
    for i in range(5):
        ver = await store.stream_version("s")
        await store.append("s", [_ev(f"Event{i}", seq=i)], expected_version=ver)

    events = await store.load_stream("s")
    assert len(events) == 5
    for i, ev in enumerate(events):
        assert ev["stream_position"] == i
        assert ev["event_type"] == f"Event{i}"

@pytest.mark.asyncio
async def test_load_stream_with_from_position():
    store = InMemoryEventStore()
    for i in range(5):
        ver = await store.stream_version("s")
        await store.append("s", [_ev(f"E{i}")], expected_version=ver)

    events = await store.load_stream("s", from_position=2)
    assert len(events) == 3
    assert events[0]["stream_position"] == 2

@pytest.mark.asyncio
async def test_load_all_yields_all_events_globally():
    store = InMemoryEventStore()
    await store.append("s1", [_ev("E1"), _ev("E2")], expected_version=-1)
    await store.append("s2", [_ev("E3")], expected_version=-1)

    all_events = [e async for e in store.load_all(from_position=0)]
    assert len(all_events) == 3

@pytest.mark.asyncio
async def test_checkpoints_persist():
    store = InMemoryEventStore()
    assert await store.load_checkpoint("proj_a") == 0  # default
    await store.save_checkpoint("proj_a", 42)
    assert await store.load_checkpoint("proj_a") == 42

@pytest.mark.asyncio
async def test_append_multiple_events_in_one_call():
    store = InMemoryEventStore()
    events = [_ev(f"E{i}") for i in range(3)]
    positions = await store.append("s", events, expected_version=-1)
    assert positions == [0, 1, 2]
    assert await store.stream_version("s") == 2


# ─── EVENT SCHEMA CONFORMANCE ─────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_all_seed_event_types_validate():
    """
    Loads seed_events.jsonl (produced by datagen/generate_all.py --validate-only)
    and verifies every event validates against EVENT_REGISTRY.

    Run datagen first:
        python datagen/generate_all.py --skip-db --skip-docs --validate-only
    """
    import json
    from pathlib import Path
    from ledger.schema.events import EVENT_REGISTRY

    seed_file = Path("data/seed_events.jsonl")
    if not seed_file.exists():
        pytest.skip("data/seed_events.jsonl not found — run datagen first")

    errors = []
    validated = 0
    with open(seed_file) as f:
        for line in f:
            rec = json.loads(line)
            event_type = rec["event_type"]
            payload = rec["payload"]
            cls = EVENT_REGISTRY.get(event_type)
            if cls is None:
                errors.append(f"Unknown type: {event_type}")
                continue
            try:
                cls(event_type=event_type, **payload)
                validated += 1
            except Exception as e:
                errors.append(f"{event_type}: {e}")

    assert not errors, f"{len(errors)} schema errors:\n" + "\n".join(errors[:10])
    assert validated > 0, "No events found in seed file"
    print(f"\nValidated {validated} seed events against EVENT_REGISTRY")
