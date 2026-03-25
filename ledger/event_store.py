"""
ledger/event_store.py — PostgreSQL-backed EventStore
====================================================
Phase 1 core implementation for the append-only event store.
"""
from __future__ import annotations

import asyncio
import json
from collections import defaultdict
from collections.abc import Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, AsyncGenerator, Iterator
from uuid import UUID, uuid4

import asyncpg
from pydantic import BaseModel, ConfigDict, Field

from ledger.upcasters import UpcasterRegistry


SCHEMA_PATH = Path(__file__).resolve().parent.parent / "src" / "schema.sql"


class OptimisticConcurrencyError(Exception):
    """Raised when expected_version doesn't match current stream version."""

    def __init__(self, stream_id: str, expected: int, actual: int):
        self.stream_id = stream_id
        self.expected = expected
        self.actual = actual
        super().__init__(f"OCC on '{stream_id}': expected v{expected}, actual v{actual}")


class StorageModel(BaseModel, Mapping[str, Any]):
    """Immutable, mapping-compatible base for storage-layer models."""

    model_config = ConfigDict(frozen=True)

    def __getitem__(self, key: str) -> Any:
        return getattr(self, key)

    def __iter__(self) -> Iterator[str]:
        return iter(self.model_dump(mode="python"))

    def __len__(self) -> int:
        return len(self.model_dump(mode="python"))

    def to_dict(self) -> dict[str, Any]:
        return self.model_dump(mode="python")


class StoredEvent(StorageModel):
    """Storage envelope for a persisted domain event."""

    event_id: str
    stream_id: str
    stream_position: int
    global_position: int
    event_type: str
    event_version: int = 1
    payload: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)
    recorded_at: datetime

    @property
    def envelope(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "stream_id": self.stream_id,
            "stream_position": self.stream_position,
            "global_position": self.global_position,
            "event_type": self.event_type,
            "event_version": self.event_version,
            "metadata": dict(self.metadata),
            "recorded_at": self.recorded_at,
        }

    @property
    def domain_data(self) -> dict[str, Any]:
        return dict(self.payload)


class StreamMetadata(StorageModel):
    """Storage metadata for an aggregate stream lifecycle."""

    stream_id: str
    aggregate_type: str
    current_version: int
    created_at: datetime
    archived_at: datetime | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


def assigned_positions_from_new_version(new_version: int, event_count: int) -> list[int]:
    if event_count <= 0:
        return []
    start_position = new_version - event_count + 1
    return list(range(start_position, new_version + 1))


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
        self.upcasters = upcaster_registry or UpcasterRegistry()
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
            await conn.execute(
                "ALTER TABLE event_streams ADD COLUMN IF NOT EXISTS archived_at TIMESTAMPTZ"
            )

    def _require_pool(self) -> asyncpg.Pool:
        if self._pool is None:
            raise RuntimeError("EventStore is not connected. Call connect() first.")
        return self._pool

    @staticmethod
    def _aggregate_type_for(stream_id: str) -> str:
        return stream_id.split("-", 1)[0] if "-" in stream_id else stream_id

    @staticmethod
    def _coerce_json_object(value: Any, field_name: str) -> dict[str, Any]:
        if value is None:
            return {}
        if isinstance(value, Mapping):
            return dict(value)
        if isinstance(value, str):
            parsed = json.loads(value)
            if isinstance(parsed, Mapping):
                return dict(parsed)
            raise ValueError(f"Expected '{field_name}' JSON object but found {type(parsed).__name__}")
        raise ValueError(f"Expected '{field_name}' to be a mapping or JSON string, got {type(value).__name__}")

    def _deserialize_event(self, row: asyncpg.Record) -> StoredEvent:
        event = dict(row)
        event["event_id"] = str(event["event_id"])
        event["payload"] = self._coerce_json_object(event.get("payload"), "payload")
        event["metadata"] = self._coerce_json_object(event.get("metadata"), "metadata")
        if self.upcasters:
            event = self.upcasters.upcast(event)
        return StoredEvent.model_validate(event)

    def _deserialize_stream_metadata(self, row: asyncpg.Record) -> StreamMetadata:
        stream_metadata = dict(row)
        stream_metadata["metadata"] = self._coerce_json_object(stream_metadata.get("metadata"), "metadata")
        return StreamMetadata.model_validate(stream_metadata)

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
        events: list[Any],
        expected_version: int,
        correlation_id: str | None = None,
        causation_id: str | None = None,
        metadata: dict | None = None,
    ) -> int:
        """
        Append events atomically with optimistic concurrency control.

        How it works:
        1. Acquire a transaction-scoped advisory lock for the stream.
        2. Read the stream's current version inside the same transaction.
        3. Compare it to `expected_version`; on mismatch raise OCC.
        4. Insert immutable event rows into `events`.
        5. Insert matching outbox rows for downstream delivery.
        6. Advance the stream cursor in `event_streams`.

        Returns the new stream version after the append succeeds.
        """
        if not events:
            return expected_version

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
                    "SELECT current_version, archived_at FROM event_streams WHERE stream_id = $1",
                    stream_id,
                )
                current_version = stream_row["current_version"] if stream_row else -1
                if current_version != expected_version:
                    raise OptimisticConcurrencyError(stream_id, expected_version, current_version)
                if stream_row and stream_row["archived_at"] is not None:
                    raise RuntimeError(f"Cannot append to archived stream '{stream_id}'")

                if stream_row is None:
                    await conn.execute(
                        """
                        INSERT INTO event_streams(stream_id, aggregate_type, current_version, metadata)
                        VALUES($1, $2, 0, $3::jsonb)
                        """,
                        stream_id,
                        self._aggregate_type_for(stream_id),
                        json.dumps(metadata or {}),
                    )

                next_position = 1 if current_version == -1 else current_version + 1
                normalized_events = [self._normalize_event_input(event) for event in events]

                for offset, event in enumerate(normalized_events):
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
                new_version = next_position + len(normalized_events) - 1
                if metadata:
                    await conn.execute(
                        """
                        UPDATE event_streams
                        SET current_version = $1,
                            metadata = COALESCE(metadata, '{}'::jsonb) || $2::jsonb
                        WHERE stream_id = $3
                        """,
                        new_version,
                        json.dumps(metadata),
                        stream_id,
                    )
                else:
                    await conn.execute(
                        "UPDATE event_streams SET current_version = $1 WHERE stream_id = $2",
                        new_version,
                        stream_id,
                    )
                return new_version

    @staticmethod
    def _normalize_event_input(event: Any) -> dict[str, Any]:
        if hasattr(event, "to_store_dict"):
            return dict(event.to_store_dict())
        if hasattr(event, "model_dump"):
            return dict(event.model_dump(mode="python"))
        if isinstance(event, Mapping):
            return dict(event)
        raise TypeError(f"Unsupported event input type: {type(event).__name__}")

    async def load_stream(
        self,
        stream_id: str,
        from_position: int = 0,
        to_position: int | None = None,
    ) -> list[StoredEvent]:
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
        from_global_position: int = 0,
        event_types: list[str] | None = None,
        batch_size: int = 500,
        **kwargs: Any,
    ) -> AsyncGenerator[StoredEvent, None]:
        """
        Yield all events globally ordered by `global_position`.

        This is the replay primitive projections use to catch up from a saved
        checkpoint without touching the write model.
        """
        pool = self._require_pool()
        if "from_position" in kwargs:
            from_global_position = kwargs.pop("from_position")
        if kwargs:
            unexpected = ", ".join(sorted(kwargs))
            raise TypeError(f"Unexpected load_all keyword argument(s): {unexpected}")

        async with pool.acquire() as conn:
            current_position = from_global_position
            while True:
                if event_types:
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
                          AND event_type = ANY($2::text[])
                        ORDER BY global_position ASC
                        LIMIT $3
                        """,
                        current_position,
                        event_types,
                        batch_size,
                    )
                else:
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

    async def get_event(self, event_id: UUID | str) -> StoredEvent | None:
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

    async def get_stream_metadata(self, stream_id: str) -> StreamMetadata:
        """Return stream lifecycle metadata for an existing stream."""
        pool = self._require_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT
                    stream_id,
                    aggregate_type,
                    current_version,
                    created_at,
                    archived_at,
                    metadata
                FROM event_streams
                WHERE stream_id = $1
                """,
                stream_id,
            )
            if row is None:
                raise ValueError(f"Stream '{stream_id}' does not exist")
            return self._deserialize_stream_metadata(row)

    async def archive_stream(self, stream_id: str, archived_at: datetime | None = None) -> None:
        """Mark a stream as archived so no further writes can occur."""
        pool = self._require_pool()
        archived_timestamp = archived_at or datetime.now(timezone.utc)
        async with pool.acquire() as conn:
            result = await conn.execute(
                """
                UPDATE event_streams
                SET archived_at = COALESCE(archived_at, $2)
                WHERE stream_id = $1
                """,
                stream_id,
                archived_timestamp,
            )
            if result.endswith("0"):
                raise ValueError(f"Stream '{stream_id}' does not exist")

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
class InMemoryEventStore:
    """
    Asyncio-safe in-memory store for unit tests.

    This one keeps the repo's Phase 1 test semantics:
    stream positions are 0-based in memory, while the PostgreSQL store above is
    1-based to match the integration test expectations and stream cursor table.
    """

    def __init__(self, upcaster_registry=None):
        self.upcasters = upcaster_registry or UpcasterRegistry()
        self._streams: dict[str, list[StoredEvent]] = defaultdict(list)
        self._stream_metadata: dict[str, StreamMetadata] = {}
        self._versions: dict[str, int] = {}
        self._global: list[StoredEvent] = []
        self._checkpoints: dict[str, int] = {}
        self._locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)

    async def stream_version(self, stream_id: str) -> int:
        return self._versions.get(stream_id, -1)

    async def append(
        self,
        stream_id: str,
        events: list[Any],
        expected_version: int,
        correlation_id: str | None = None,
        causation_id: str | None = None,
        metadata: dict | None = None,
    ) -> int:
        async with self._locks[stream_id]:
            current = self._versions.get(stream_id, -1)
            if current != expected_version:
                raise OptimisticConcurrencyError(stream_id, expected_version, current)
            stream_metadata = self._stream_metadata.get(stream_id)
            if stream_metadata and stream_metadata.get("archived_at") is not None:
                raise RuntimeError(f"Cannot append to archived stream '{stream_id}'")

            base_metadata = dict(metadata or {})
            if correlation_id is not None:
                base_metadata["correlation_id"] = correlation_id
            if causation_id is not None:
                base_metadata["causation_id"] = causation_id

            if stream_id not in self._stream_metadata:
                self._stream_metadata[stream_id] = StreamMetadata(
                    stream_id=stream_id,
                    aggregate_type=EventStore._aggregate_type_for(stream_id),
                    current_version=current,
                    created_at=datetime.now(timezone.utc),
                    archived_at=None,
                    metadata=dict(metadata or {}),
                )
            elif metadata:
                current_metadata = self._stream_metadata[stream_id]
                self._stream_metadata[stream_id] = current_metadata.model_copy(
                    update={
                        "metadata": {
                            **current_metadata.metadata,
                            **dict(metadata),
                        }
                    }
                )

            normalized_events = [EventStore._normalize_event_input(event) for event in events]
            for offset, event in enumerate(normalized_events):
                stream_position = current + 1 + offset
                stored = StoredEvent(
                    event_id=str(uuid4()),
                    stream_id=stream_id,
                    stream_position=stream_position,
                    global_position=len(self._global),
                    event_type=event["event_type"],
                    event_version=int(event.get("event_version", 1)),
                    payload=dict(event.get("payload") or {}),
                    metadata={**base_metadata, **dict(event.get("metadata") or {})},
                    recorded_at=datetime.now(timezone.utc),
                )
                self._streams[stream_id].append(stored)
                self._global.append(stored)

            self._versions[stream_id] = current + len(normalized_events)
            self._stream_metadata[stream_id] = self._stream_metadata[stream_id].model_copy(
                update={"current_version": self._versions[stream_id]}
            )
            return self._versions[stream_id]

    async def load_stream(
        self,
        stream_id: str,
        from_position: int = 0,
        to_position: int | None = None,
    ) -> list[StoredEvent]:
        events = [
            event.model_copy(deep=True)
            for event in self._streams.get(stream_id, [])
            if event["stream_position"] >= from_position
            and (to_position is None or event["stream_position"] <= to_position)
        ]
        events.sort(key=lambda event: event["stream_position"])
        if self.upcasters:
            return [StoredEvent.model_validate(self.upcasters.upcast(event.to_dict())) for event in events]
        return events

    async def load_all(
        self,
        from_global_position: int = 0,
        event_types: list[str] | None = None,
        batch_size: int = 500,
        **kwargs: Any,
    ):
        del batch_size
        if "from_position" in kwargs:
            from_global_position = kwargs.pop("from_position")
        if kwargs:
            unexpected = ", ".join(sorted(kwargs))
            raise TypeError(f"Unexpected load_all keyword argument(s): {unexpected}")
        for event in self._global:
            if event["global_position"] < from_global_position:
                continue
            if event_types is not None and event["event_type"] not in event_types:
                continue
            loaded = event.model_copy(deep=True)
            if self.upcasters:
                loaded = StoredEvent.model_validate(self.upcasters.upcast(loaded.to_dict()))
            yield loaded

    async def get_event(self, event_id: UUID | str) -> StoredEvent | None:
        for event in self._global:
            if event["event_id"] == str(event_id):
                loaded = event.model_copy(deep=True)
                if self.upcasters:
                    return StoredEvent.model_validate(self.upcasters.upcast(loaded.to_dict()))
                return loaded
        return None

    async def get_stream_metadata(self, stream_id: str) -> StreamMetadata:
        metadata = self._stream_metadata.get(stream_id)
        if metadata is None:
            raise ValueError(f"Stream '{stream_id}' does not exist")
        return metadata.model_copy(deep=True)

    async def archive_stream(self, stream_id: str, archived_at: datetime | None = None) -> None:
        metadata = self._stream_metadata.get(stream_id)
        if metadata is None:
            raise ValueError(f"Stream '{stream_id}' does not exist")
        self._stream_metadata[stream_id] = metadata.model_copy(
            update={"archived_at": archived_at or datetime.now(timezone.utc)}
        )

    async def save_checkpoint(self, projection_name: str, position: int) -> None:
        self._checkpoints[projection_name] = position

    async def load_checkpoint(self, projection_name: str) -> int:
        return self._checkpoints.get(projection_name, 0)
