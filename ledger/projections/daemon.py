from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any


class ProjectionDaemon:
    def __init__(
        self,
        store,
        projections: list | None = None,
        *,
        batch_size: int = 100,
        poll_interval: float = 0.1,
        max_retries: int = 1,
        logger: logging.Logger | None = None,
    ):
        self.store = store
        self.projections = list(projections or [])
        self._projection_map = {projection.checkpoint_name: projection for projection in self.projections}
        self.batch_size = batch_size
        self.poll_interval = poll_interval
        self.max_retries = max_retries
        self.logger = logger or logging.getLogger(__name__)
        self._task: asyncio.Task | None = None
        self._stop_event = asyncio.Event()
        self._running = False
        self._retry_counts: dict[tuple[str, str], int] = defaultdict(int)
        self._projection_last_processed_at: dict[str, datetime] = {}

    def subscribe(self, projection) -> None:
        self.projections.append(projection)
        self._projection_map[projection.checkpoint_name] = projection

    async def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        for projection in self.projections:
            setup = getattr(projection, "setup", None)
            if callable(setup):
                await setup()
        self._stop_event.clear()
        self._running = True
        self._task = asyncio.create_task(self._run(), name="projection-daemon")

    async def stop(self) -> None:
        self._stop_event.set()
        self._running = False
        if self._task is not None:
            await self._task
            self._task = None

    async def _run(self) -> None:
        while not self._stop_event.is_set():
            try:
                await self._process_batch()
            except Exception:
                self.logger.exception("Projection daemon loop failed unexpectedly; continuing")
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=self.poll_interval)
            except asyncio.TimeoutError:
                pass

    async def _load_checkpoints(self) -> dict[str, int]:
        return {
            projection.checkpoint_name: await self.store.load_checkpoint(projection.checkpoint_name)
            for projection in self.projections
        }

    async def _fetch_batch(self, from_position: int) -> list:
        events = []
        async for event in self.store.load_all(from_global_position=from_position, batch_size=self.batch_size):
            events.append(event)
            if len(events) >= self.batch_size:
                break
        return events

    async def run_forever(self, poll_interval_ms: int = 100) -> None:
        self.poll_interval = poll_interval_ms / 1000
        self._running = True
        for projection in self.projections:
            setup = getattr(projection, "setup", None)
            if callable(setup):
                await setup()
        while self._running:
            await self._process_batch()
            await asyncio.sleep(self.poll_interval)

    async def _process_batch(self) -> int:
        return await self.run_once()

    async def run_once(self) -> int:
        if not self.projections:
            return 0

        checkpoints = await self._load_checkpoints()
        from_position = min(checkpoints.values(), default=0)
        events = await self._fetch_batch(from_position)
        if not events:
            return 0

        for event in events:
            for projection in self.projections:
                checkpoint_name = projection.checkpoint_name
                next_position = checkpoints.get(checkpoint_name, 0)
                if event.global_position < next_position:
                    continue

                retry_key = (checkpoint_name, event.event_id)
                try:
                    await projection.apply(event)
                except Exception:
                    self._retry_counts[retry_key] += 1
                    self.logger.exception(
                        "Projection '%s' failed on event %s (%s)",
                        checkpoint_name,
                        event.event_id,
                        event.event_type,
                    )
                    if self._retry_counts[retry_key] >= self.max_retries:
                        self.logger.error(
                            "Projection '%s' skipping event %s after %s attempt(s)",
                            checkpoint_name,
                            event.event_id,
                            self._retry_counts[retry_key],
                        )
                        await self.store.save_checkpoint(checkpoint_name, event.global_position + 1)
                        checkpoints[checkpoint_name] = event.global_position + 1
                        self._projection_last_processed_at[checkpoint_name] = event.recorded_at
                    continue

                self._retry_counts.pop(retry_key, None)
                await self.store.save_checkpoint(checkpoint_name, event.global_position + 1)
                checkpoints[checkpoint_name] = event.global_position + 1
                self._projection_last_processed_at[checkpoint_name] = event.recorded_at

        return len(events)

    async def _latest_event(self):
        pool_getter = getattr(self.store, "_require_pool", None)
        if callable(pool_getter):
            pool = pool_getter()
            async with pool.acquire() as conn:
                row = await conn.fetchrow(
                    """
                    SELECT event_id, global_position, recorded_at
                    FROM events
                    ORDER BY global_position DESC
                    LIMIT 1
                    """
                )
                if row is None:
                    return None
                return {
                    "event_id": str(row["event_id"]),
                    "global_position": row["global_position"],
                    "recorded_at": row["recorded_at"],
                }

        global_events = getattr(self.store, "_global", None)
        if global_events:
            latest = global_events[-1]
            return {
                "event_id": latest["event_id"],
                "global_position": latest["global_position"],
                "recorded_at": latest["recorded_at"],
            }
        return None

    async def get_lag(self) -> dict[str, Any]:
        latest = await self._latest_event()
        checkpoints = await self._load_checkpoints()
        if latest is None:
            return {
                "latest_global_position": -1,
                "processed_position": -1,
                "position_lag": 0,
                "milliseconds_lag": 0,
            }

        next_position = min(checkpoints.values(), default=latest["global_position"] + 1)
        processed_position = next_position - 1
        position_lag = max(0, latest["global_position"] - processed_position)

        if self._projection_last_processed_at:
            processed_at = min(self._projection_last_processed_at.values())
            milliseconds_lag = max(
                0,
                int((latest["recorded_at"] - processed_at).total_seconds() * 1000),
            )
        else:
            milliseconds_lag = max(
                0,
                int((datetime.now(timezone.utc) - latest["recorded_at"]).total_seconds() * 1000),
            )

        return {
            "latest_global_position": latest["global_position"],
            "processed_position": processed_position,
            "position_lag": position_lag,
            "milliseconds_lag": milliseconds_lag,
        }

    async def get_all_lags(self) -> dict[str, Any]:
        latest = await self._latest_event()
        checkpoints = await self._load_checkpoints()
        if latest is None:
            return {
                "latest_global_position": -1,
                "projections": {
                    projection.checkpoint_name: {
                        "processed_position": -1,
                        "position_lag": 0,
                        "milliseconds_lag": 0,
                    }
                    for projection in self.projections
                },
            }

        projection_lags: dict[str, Any] = {}
        for projection in self.projections:
            checkpoint_name = projection.checkpoint_name
            next_position = checkpoints.get(checkpoint_name, 0)
            processed_position = next_position - 1
            position_lag = max(0, latest["global_position"] - processed_position)

            processed_at = self._projection_last_processed_at.get(checkpoint_name)
            if processed_at is None:
                milliseconds_lag = max(
                    0,
                    int((datetime.now(timezone.utc) - latest["recorded_at"]).total_seconds() * 1000),
                )
            else:
                milliseconds_lag = max(
                    0,
                    int((latest["recorded_at"] - processed_at).total_seconds() * 1000),
                )

            projection_lags[checkpoint_name] = {
                "processed_position": processed_position,
                "position_lag": position_lag,
                "milliseconds_lag": milliseconds_lag,
            }

        return {
            "latest_global_position": latest["global_position"],
            "projections": projection_lags,
        }

    async def get_projection_lag(self, checkpoint_name: str) -> dict[str, Any]:
        all_lags = await self.get_all_lags()
        projection_lag = all_lags["projections"].get(checkpoint_name)
        if projection_lag is None:
            raise ValueError(f"Unknown projection checkpoint '{checkpoint_name}'")
        return dict(projection_lag)