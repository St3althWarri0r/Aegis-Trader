"""P1 regression pins for src/poseidon/execution/guardian.py (F014).

Self-contained: replicates the minimal kernel/db harness from
``tests/unit/test_guardian.py`` so this sibling file does not collide with or
depend on it. asyncio auto mode — plain ``async def test_...``, no decorator.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace

from poseidon.core.config import GuardianConfig
from poseidon.core.enums import (
    MarketSession,
    OrderSide,
    OrderStatus,
    TradingMode,
)
from poseidon.core.models import Order, Position, Quote
from poseidon.execution.guardian import PositionGuardian
from poseidon.storage.db import Database


class KernelStub:
    """Just the surface the guardian touches (copied from test_guardian.py)."""

    def __init__(self, *, mode: TradingMode, price: str, position_qty: str | None) -> None:
        self.mode = mode
        self._price = Decimal(price)
        self._position_qty = position_qty
        self.executed_decisions: list = []
        self.notifications: list[dict] = []
        self.audit_entries: list[tuple[str, str]] = []

        self.clock = SimpleNamespace(session=lambda: MarketSession.REGULAR)
        self.broker = SimpleNamespace(name="paper")  # plans are broker-scoped
        self.order_manager = SimpleNamespace(
            mode=mode, execute_decision=self._execute_decision
        )
        self.audit = SimpleNamespace(append=self._audit_append)
        self.bus = SimpleNamespace(publish=self._publish)
        self.router = SimpleNamespace(quote=self._quote)
        # synced_at mirrors PortfolioState (kept in sync with test_guardian.py's
        # KernelStub, which this one is a self-contained copy of): the
        # snapshot-grace guard reads it via getattr, so a stub missing this
        # field would silently sit at "no proof" for the postdates-arming
        # check instead of exercising the intended path.
        self.portfolio = SimpleNamespace(position_for=self._position_for,
                                         synced_at=datetime.now(UTC))

    async def _execute_decision(self, decision):
        self.executed_decisions.append(decision)
        return [Order(symbol=decision.trades[0].symbol, side=OrderSide.SELL,
                      quantity=decision.trades[0].quantity, status=OrderStatus.FILLED)]

    async def _audit_append(self, actor: str, action: str, payload=None):
        self.audit_entries.append((actor, action))

    async def _publish(self, topic: str, payload=None):
        if topic == "notify":
            self.notifications.append(payload)

    async def _quote(self, symbol: str, allow_delayed: bool = False) -> Quote:
        return Quote(symbol=symbol, bid=self._price, ask=self._price + Decimal("0.10"),
                     as_of=datetime.now(UTC), source="stub")

    def _position_for(self, symbol: str):
        if self._position_qty is None:
            return None
        return Position(symbol=symbol, quantity=Decimal(self._position_qty),
                        avg_entry_price=Decimal("100"), broker="stub",
                        as_of=datetime.now(UTC))


async def _db_with_decision(tmp_path, *, stop: str | None, target: str | None) -> Database:
    db = Database(tmp_path / "g.db")
    await db.open()
    decision_payload = {
        "rationale": {"exit_plan": {"stop_loss": stop, "take_profit": target}}
    }
    await db.execute(
        "INSERT INTO decisions (id, cycle_id, action, payload, created_at) VALUES (?, ?, ?, ?, ?)",
        ("dec1", "c1", "buy", json.dumps(decision_payload), datetime.now(UTC).isoformat()),
    )
    return db


def filled_buy(symbol: str = "AAPL", qty: str = "10") -> dict:
    order = Order(symbol=symbol, side=OrderSide.BUY, quantity=Decimal(qty),
                  decision_id="dec1", status=OrderStatus.FILLED,
                  filled_quantity=Decimal(qty))
    return {"order": order.model_dump(mode="json")}


def guardian_partial_exit(symbol: str = "AAPL", qty: str = "100",
                          filled: str = "60") -> dict:
    """A guardian exit that partially filled then terminated. The manager's
    poller routes any terminal order with a nonzero fill to ORDER_FILLED (not
    ORDER_UPDATED), so this is what on_order_filled sees for a partial exit."""
    order = Order(symbol=symbol, side=OrderSide.SELL, quantity=Decimal(qty),
                  filled_quantity=Decimal(filled), status=OrderStatus.FILLED,
                  strategy="guardian")
    return {"order": order.model_dump(mode="json")}


# F014: a guardian risk-reducing exit that only PARTIALLY closed (position still
# open) must re-arm the plan + escalate. Pre-fix on_order_filled called
# _maybe_deactivate, which no-ops while the residual is held (quantity > 0), so
# the latched-inactive plan was never re-armed and the remainder ran unprotected.
async def test_f014_partial_guardian_exit_rearms_residual(tmp_path) -> None:
    db = await _db_with_decision(tmp_path, stop="180", target=None)
    try:
        # position_qty="40": the exit closed 60 of 100; 40 shares are still held.
        kernel = KernelStub(mode=TradingMode.AUTONOMOUS, price="179", position_qty="40")
        guardian = PositionGuardian(GuardianConfig(), db, kernel)

        # Arm the plan for the original 100-share long...
        await guardian.on_order_filled("order.filled", filled_buy(symbol="AAPL", qty="100"))
        assert len(await guardian.active_plans()) == 1
        # ...then simulate _trigger_exit having already latched it inactive when the
        # breach fired and the exit was dispatched. This precondition is load-bearing:
        # without it the row stays active=1 from the arm and active_plans() would
        # return 1 on BOTH pre- and post-fix, making the re-arm check meaningless.
        await db.execute(
            "UPDATE exit_plans SET active = 0, triggered_reason = ? WHERE symbol = ?",
            ("stop loss: AAPL at 179 <= stop 180", "AAPL"),
        )
        assert await guardian.active_plans() == []  # latched inactive

        kernel.notifications.clear()

        # The guardian's own exit fills 60/100, the remaining 40 cancels/expires ->
        # poller publishes ORDER_FILLED for a guardian risk-reducing SELL while the
        # position (40) is still open.
        await guardian.on_order_filled("order.filled", guardian_partial_exit())

        # Post-fix: the plan is RE-ARMED for the residual (pre-fix: _maybe_deactivate
        # no-ops on the still-open position and active_plans() stays []).
        plans = await guardian.active_plans()
        assert len(plans) == 1
        assert plans[0]["symbol"] == "AAPL"

        # Post-fix: a warning escalation is published (pre-fix: none).
        assert len(kernel.notifications) == 1
        note = kernel.notifications[0]
        assert note["level"] == "warning"
        assert "partial" in note["title"].lower()
    finally:
        # Close the DB even when an assertion fails (e.g. on pre-fix code), or the
        # lingering aiosqlite connection hangs pytest teardown instead of failing fast.
        await db.close()


# Whether a guardian exit closed IN FULL must be decided from the order's OWN
# fill data (filled_quantity vs quantity), never the portfolio snapshot:
# ORDER_FILLED here and the kernel's fill-triggered portfolio resync
# (app.py's _sync_after_fill) are unordered concurrent handlers of the same
# event, so a snapshot read at this instant can still show the pre-fill
# state. Pre-fix, on_order_filled trusted position_for() to decide
# "still_open"; a stale snapshot showing NO position for a fill that only
# PARTIALLY closed made it skip the re-arm entirely, leaving the residual
# unprotected.
async def test_partial_guardian_exit_rearms_even_when_portfolio_snapshot_is_stale(
        tmp_path) -> None:
    db = await _db_with_decision(tmp_path, stop="180", target=None)
    try:
        # The snapshot is stale/never-synced for this symbol (position_qty=None)
        # even though a real 40-share residual is actually held — exactly the
        # race a not-yet-landed post-fill resync produces.
        kernel = KernelStub(mode=TradingMode.AUTONOMOUS, price="179", position_qty=None)
        guardian = PositionGuardian(GuardianConfig(), db, kernel)

        await guardian.on_order_filled("order.filled", filled_buy(symbol="AAPL", qty="100"))
        await db.execute(
            "UPDATE exit_plans SET active = 0, triggered_reason = ? WHERE symbol = ?",
            ("stop loss: AAPL at 179 <= stop 180", "AAPL"),
        )
        assert await guardian.active_plans() == []  # latched inactive

        await guardian.on_order_filled("order.filled", guardian_partial_exit())

        # Re-armed for the residual despite the stale "no position" snapshot —
        # the decision came from the order's own filled_quantity (60 of 100),
        # not from position_for().
        plans = await guardian.active_plans()
        assert len(plans) == 1
        assert plans[0]["symbol"] == "AAPL"
    finally:
        await db.close()
