"""In-process async event bus.

Subsystems communicate through topics instead of direct references, which
keeps the kernel wiring thin and lets the dashboard, notifier, and audit log
observe everything without being in the call path.

Delivery is at-least-once within the process; handlers must be idempotent.
Handler exceptions are logged and isolated — one bad subscriber can never
break publishers or other subscribers.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections import defaultdict
from collections.abc import Awaitable, Callable
from typing import Any

import structlog

log = structlog.get_logger(__name__)

Handler = Callable[[str, Any], Awaitable[None]]


class Topics:
    """Well-known topic names (subsystems may define more)."""

    QUOTE = "data.quote"
    NEWS = "data.news"
    ACCOUNT_SYNCED = "portfolio.synced"
    ORDER_UPDATED = "order.updated"
    ORDER_FILLED = "order.filled"
    ORDER_REJECTED = "order.rejected"
    DECISION_MADE = "ai.decision"
    APPROVAL_REQUESTED = "ai.approval_requested"
    RISK_VIOLATION = "risk.violation"
    CIRCUIT_OPENED = "risk.circuit_opened"
    CIRCUIT_CLOSED = "risk.circuit_closed"
    HEALTH_CHANGED = "health.changed"
    BROKER_DISCONNECTED = "broker.disconnected"
    BROKER_RECONNECTED = "broker.reconnected"
    NOTIFY = "notify"
    SYSTEM_ERROR = "system.error"


class EventBus:
    def __init__(self) -> None:
        self._subscribers: dict[str, list[Handler]] = defaultdict(list)
        self._tasks: set[asyncio.Task[None]] = set()
        self._closed = False

    def subscribe(self, topic: str, handler: Handler) -> None:
        self._subscribers[topic].append(handler)

    def unsubscribe(self, topic: str, handler: Handler) -> None:
        with contextlib.suppress(ValueError):
            self._subscribers[topic].remove(handler)

    async def publish(self, topic: str, payload: Any = None) -> None:
        """Dispatch to all subscribers concurrently; never raises."""
        if self._closed:
            return
        handlers = list(self._subscribers.get(topic, ()))
        # Wildcard subscribers receive every event (used by audit + dashboard).
        handlers.extend(self._subscribers.get("*", ()))
        for handler in handlers:
            task = asyncio.create_task(self._run(handler, topic, payload))
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)

    def publish_nowait(self, topic: str, payload: Any = None) -> None:
        """Schedule ``publish`` from synchronous code (a risk-engine trip, a
        breaker) without awaiting it. The scheduling task is held in the bus's
        own task set: the event loop keeps only weak references to tasks, so a
        bare ``create_task`` whose result nobody holds can be garbage-collected
        mid-flight — the documented hazard this method exists to avoid."""
        if self._closed:
            return
        task = asyncio.create_task(self.publish(topic, payload))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _run(self, handler: Handler, topic: str, payload: Any) -> None:
        try:
            await handler(topic, payload)
        except Exception:
            log.exception("event handler failed", topic=topic, handler=getattr(handler, "__qualname__", str(handler)))

    async def close(self) -> None:
        """Drain in-flight handler tasks on shutdown."""
        self._closed = True
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
