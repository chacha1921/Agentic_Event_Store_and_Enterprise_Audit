"""
ledger/event_store.py — PostgreSQL-backed EventStore
====================================================
Phase 1 core implementation for the append-only event store.
"""
from __future__ import annotations

import asyncio
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import AsyncGenerator
from uuid import UUID, uuid4

import asyncpg


SCHEMA_PATH = Path(__file__).resolve().parent.parent / "src" / "schema.sql"


class OptimisticConcurrencyError(Exception):
    """Raised when expected_version doesn't match current stream version."""

    def __init__(self, stream_id: str, expected: int, actual: int):
        self.stream_id = stream_id
        self.expected = expected
        self.actual = actual
        super().__init__(f"OCC on '{stream_id}': expected v{expected}, actual v{actual}")


class EventStore:
    """
    Append-only PostgreSQL event store.

    Design notes:
    - `events` is immutable and append-only.
    - `event_streams.current_version` is the authoritative OCC cursor.
    - Stream versions are 1-based for PostgreSQL streams in this starter:
      a new stream is `-1`, first appended event gets position `1`.
    - `global_position` is 0-based and monotonic for projection replay.
    """

    def __init__(self, db_url: str, upcaster_registry=None):
        self.db_url = db_url
        self.upcasters = upcaster_registry
        self._pool: asyncpg.Pool | None = None

    async def connect(self) -> None:
        self._pool = await asyncpg.create_pool(self.db_url, min_size=1, max_size=10)
        await self._initialize_schema()

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    async def _initialize_schema(self) -> None:
        schema_sql = SCHEMA_PATH.read_text(encoding="utf-8")
        async with self._pool.acquire() as conn:
            await conn.execute(schema_sql)

    def _require_pool(self) -> asyncpg.Pool:
        if self._pool is None:
            raise RuntimeError("EventStore is not connected. Call connect() first.")
        return self._pool

    @staticmethod
    def _aggregate_type_for(stream_id: str) -> str:
        return stream_id.split("-", 1)[0] if "-" in stream_id else stream_id

    def _deserialize_event(self, row: asyncpg.Record) -> dict:
        event = dict(row)
        payload = event.get("payload") or {}
        metadata = event.get("metadata") or {}
        event["payload"] = dict(payload)
        event["metadata"] = dict(metadata)
        if self.upcasters:
            event = self.upcasters.upcast(event)
        return event

    async def stream_version(self, stream_id: str) -> int:
        """Return current version, or -1 if the stream does not exist."""
        pool = self._require_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT current_version FROM event_streams WHERE stream_id = $1",
                stream_id,
            )
            return row["current_version"] if row else -1

    async def append(
        self,
        stream_id: str,
        events: list[dict],
        expected_version: int,
        correlation_id: str | None = None,
        causation_id: str | None = None,
        metadata: dict | None = None,
    ) -> list[int]:
        """
        Append events atomically with optimistic concurrency control.

        How it works:
        1. Acquire a transaction-scoped advisory lock for the stream.
        2. Read the stream's current version inside the same transaction.
        3. Compare it to `expected_version`; on mismatch raise OCC.
        4. Insert immutable event rows into `events`.
        5. Insert matching outbox rows for downstream delivery.
        6. Advance the stream cursor in `event_streams`.
        """
        if not events:
            return []

        pool = self._require_pool()
        base_metadata = dict(metadata or {})
        if correlation_id is not None:
            base_metadata["correlation_id"] = correlation_id
        if causation_id is not None:
            base_metadata["causation_id"] = causation_id

        async with pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
                    stream_id,
                )

                stream_row = await conn.fetchrow(
                    "SELECT current_version FROM event_streams WHERE stream_id = $1",
                    stream_id,
                )
                current_version = stream_row["current_version"] if stream_row else -1
                if current_version != expected_version:
                    raise OptimisticConcurrencyError(stream_id, expected_version, current_version)

                if stream_row is None:
                    await conn.execute(
                        """
                        INSERT INTO event_streams(stream_id, aggregate_type, current_version)
                        VALUES($1, $2, 0)
                        """,
                        stream_id,
                        self._aggregate_type_for(stream_id),
                    )

                next_position = 1 if current_version == -1 else current_version + 1
                assigned_positions: list[int] = []

                for offset, event in enumerate(events):
                    stream_position = next_position + offset
                    event_payload = dict(event.get("payload") or {})
                    event_metadata = {**base_metadata, **dict(event.get("metadata") or {})}
                    event_version = int(event.get("event_version", 1))

                    inserted = await conn.fetchrow(
                        """
                        INSERT INTO events(
                            stream_id,
                            stream_position,
                            event_type,
                            event_version,
                            payload,
                            metadata,
                            recorded_at
                        )
                        VALUES($1, $2, $3, $4, $5::jsonb, $6::jsonb, $7)
                        RETURNING event_id, global_position, recorded_at
                        """,
                        stream_id,
                        stream_position,
                        event["event_type"],
                        event_version,
                        json.dumps(event_payload),
                        json.dumps(event_metadata),
                        datetime.now(timezone.utc),
                    )

                    outbox_payload = {
                        "event_id": str(inserted["event_id"]),
                        "stream_id": stream_id,
                        "stream_position": stream_position,
                        "global_position": inserted["global_position"],
                        "event_type": event["event_type"],
                        "event_version": event_version,
                        "payload": event_payload,
                        "metadata": event_metadata,
                        "recorded_at": inserted["recorded_at"].isoformat(),
                    }
                    await conn.execute(
                        """
                        INSERT INTO outbox(event_id, destination, payload)
                        VALUES($1, $2, $3::jsonb)
                        """,
                        inserted["event_id"],
                        "event_bus",
                        json.dumps(outbox_payload),
                    )
                    assigned_positions.append(stream_position)

                new_version = assigned_positions[-1]
                await conn.execute(
                    "UPDATE event_streams SET current_version = $1 WHERE stream_id = $2",
                    new_version,
                    stream_id,
                )
                return assigned_positions

    async def load_stream(
        self,
        stream_id: str,
        from_position: int = 0,
        to_position: int | None = None,
    ) -> list[dict]:
        """
        Load a single stream in stream-position order.

        `from_position` and `to_position` are inclusive bounds so callers can
        replay either the whole stream or a deterministic slice.
        """
        pool = self._require_pool()
        query = """
            SELECT
                event_id,
                global_position,
                stream_id,
                stream_position,
                event_type,
                event_version,
                payload,
                metadata,
                recorded_at
            FROM events
            WHERE stream_id = $1 AND stream_position >= $2
        """
        params: list[object] = [stream_id, from_position]
        if to_position is not None:
            query += " AND stream_position <= $3"
            params.append(to_position)
        query += " ORDER BY stream_position ASC"

        async with pool.acquire() as conn:
            rows = await conn.fetch(query, *params)
            return [self._deserialize_event(row) for row in rows]

    async def load_all(
        self,
        from_position: int = 0,
        batch_size: int = 500,
    ) -> AsyncGenerator[dict, None]:
        """
        Yield all events globally ordered by `global_position`.

        This is the replay primitive projections use to catch up from a saved
        checkpoint without touching the write model.
        """
        pool = self._require_pool()
        async with pool.acquire() as conn:
            current_position = from_position
            while True:
                rows = await conn.fetch(
                    """
                    SELECT
                        event_id,
                        global_position,
                        stream_id,
                        stream_position,
                        event_type,
                        event_version,
                        payload,
                        metadata,
                        recorded_at
                    FROM events
                    WHERE global_position >= $1
                    ORDER BY global_position ASC
                    LIMIT $2
                    """,
                    current_position,
                    batch_size,
                )
                if not rows:
                    break

                for row in rows:
                    yield self._deserialize_event(row)

                current_position = rows[-1]["global_position"] + 1
                if len(rows) < batch_size:
                    break

    async def get_event(self, event_id: UUID | str) -> dict | None:
        """Load a single event by id."""
        pool = self._require_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT
                    event_id,
                    global_position,
                    stream_id,
                    stream_position,
                    event_type,
                    event_version,
                    payload,
                    metadata,
                    recorded_at
                FROM events
                WHERE event_id = $1
                """,
                event_id,
            )
            return self._deserialize_event(row) if row else None

    async def save_checkpoint(self, projection_name: str, position: int) -> None:
        """Persist a projection cursor on the read-model side of CQRS."""
        pool = self._require_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO projection_checkpoints(projection_name, last_position)
                VALUES($1, $2)
                ON CONFLICT (projection_name)
                DO UPDATE SET
                    last_position = EXCLUDED.last_position,
                    updated_at = NOW()
                """,
                projection_name,
                position,
            )

    async def load_checkpoint(self, projection_name: str) -> int:
        """Return the last saved projection cursor, defaulting to 0."""
        pool = self._require_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT last_position FROM projection_checkpoints WHERE projection_name = $1",
                projection_name,
            )
            return row["last_position"] if row else 0


class UpcasterRegistry:
    """Transforms old event versions to the current shape when events are read."""

    def __init__(self):
        self._upcasters: dict[str, dict[int, callable]] = {}

    def upcaster(self, event_type: str, from_version: int, to_version: int):
        def decorator(fn):
            self._upcasters.setdefault(event_type, {})[from_version] = fn
            return fn

        return decorator

    def upcast(self, event: dict) -> dict:
        event_type = event["event_type"]
        version = event.get("event_version", 1)
        chain = self._upcasters.get(event_type, {})
        current = dict(event)
        while version in chain:
            current["payload"] = chain[version](dict(current["payload"]))
            version += 1
            current["event_version"] = version
        return current


class InMemoryEventStore:
    """
    Asyncio-safe in-memory store for unit tests.

    This one keeps the repo's Phase 1 test semantics:
    stream positions are 0-based in memory, while the PostgreSQL store above is
    1-based to match the integration test expectations and stream cursor table.
    """

    def __init__(self, upcaster_registry=None):
        self.upcasters = upcaster_registry
        self._streams: dict[str, list[dict]] = defaultdict(list)
        self._versions: dict[str, int] = {}
        self._global: list[dict] = []
        self._checkpoints: dict[str, int] = {}
        self._locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)

    async def stream_version(self, stream_id: str) -> int:
        return self._versions.get(stream_id, -1)

    async def append(
        self,
        stream_id: str,
        events: list[dict],
        expected_version: int,
        correlation_id: str | None = None,
        causation_id: str | None = None,
        metadata: dict | None = None,
    ) -> list[int]:
        async with self._locks[stream_id]:
            current = self._versions.get(stream_id, -1)
            if current != expected_version:
                raise OptimisticConcurrencyError(stream_id, expected_version, current)

            base_metadata = dict(metadata or {})
            if correlation_id is not None:
                base_metadata["correlation_id"] = correlation_id
            if causation_id is not None:
                base_metadata["causation_id"] = causation_id

            assigned_positions: list[int] = []
            for offset, event in enumerate(events):
                stream_position = current + 1 + offset
                stored = {
                    "event_id": str(uuid4()),
                    "stream_id": stream_id,
                    "stream_position": stream_position,
                    "global_position": len(self._global),
                    "event_type": event["event_type"],
                    "event_version": event.get("event_version", 1),
                    "payload": dict(event.get("payload") or {}),
                    "metadata": {**base_metadata, **dict(event.get("metadata") or {})},
                    "recorded_at": datetime.now(timezone.utc),
                }
                self._streams[stream_id].append(stored)
                self._global.append(stored)
                assigned_positions.append(stream_position)

            self._versions[stream_id] = current + len(events)
            return assigned_positions

    async def load_stream(
        self,
        stream_id: str,
        from_position: int = 0,
        to_position: int | None = None,
    ) -> list[dict]:
        events = [
            dict(event)
            for event in self._streams.get(stream_id, [])
            if event["stream_position"] >= from_position
            and (to_position is None or event["stream_position"] <= to_position)
        ]
        events.sort(key=lambda event: event["stream_position"])
        if self.upcasters:
            return [self.upcasters.upcast(event) for event in events]
        return events

    async def load_all(self, from_position: int = 0, batch_size: int = 500):
        del batch_size
        for event in self._global:
            if event["global_position"] >= from_position:
                yield dict(event)

    async def get_event(self, event_id: UUID | str) -> dict | None:
        for event in self._global:
            if event["event_id"] == str(event_id):
                return dict(event)
        return None

    async def save_checkpoint(self, projection_name: str, position: int) -> None:
        self._checkpoints[projection_name] = position

    async def load_checkpoint(self, projection_name: str) -> int:
        return self._checkpoints.get(projection_name, 0)
