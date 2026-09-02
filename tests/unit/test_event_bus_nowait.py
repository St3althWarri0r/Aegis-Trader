"""EventBus.publish_nowait: synchronous fire-and-forget that keeps its task alive."""

from __future__ import annotations

import asyncio

from poseidon.core.events import EventBus, Topics


async def test_publish_nowait_delivers_and_tracks_its_task() -> None:
    bus = EventBus()
    seen: list[tuple[str, object]] = []

    async def handler(topic: str, payload: object) -> None:
        seen.append((topic, payload))

    bus.subscribe(Topics.CIRCUIT_OPENED, handler)
    bus.publish_nowait(Topics.CIRCUIT_OPENED, {"reason": "test"})
    assert bus._tasks  # the scheduling task is held, not left to the GC
    for _ in range(50):
        if seen:
            break
        await asyncio.sleep(0)
    assert seen == [(Topics.CIRCUIT_OPENED, {"reason": "test"})]
    await bus.close()


async def test_publish_nowait_is_a_no_op_after_close() -> None:
    bus = EventBus()
    seen: list[str] = []

    async def handler(topic: str, payload: object) -> None:
        seen.append(topic)

    bus.subscribe(Topics.CIRCUIT_OPENED, handler)
    await bus.close()
    bus.publish_nowait(Topics.CIRCUIT_OPENED, {})
    await asyncio.sleep(0)
    assert seen == [] and not bus._tasks
