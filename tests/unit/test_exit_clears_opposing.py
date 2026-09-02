"""A SELL clears same-symbol resting BUYs before it reaches the broker.

Live failure this pins: a stale GTC buy resting at the broker made every
LINK/USD exit die on alpaca's self-trade block (HTTP 403 "potential wash
trade detected"), leaving the position un-closable. The platform never opens
shorts, so an incoming SELL is always position-closing and a same-symbol
resting BUY is contrary intent: cancel it first. BUYs never clear anything —
a guardian's resting protective take-profit SELL must survive a new entry.
"""
from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import patch

import pytest

from poseidon.brokers.plugins.paper import PaperBroker
from poseidon.core.clock import FreshnessPolicy, MarketClock
from poseidon.core.config import RiskConfig
from poseidon.core.enums import (
    MarketSession,
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
    TradingMode,
)
from poseidon.core.events import EventBus
from poseidon.core.models import Order
from poseidon.data.router import DataRouter
from poseidon.execution.approvals import ApprovalQueue
from poseidon.execution.manager import OrderManager
from poseidon.portfolio.state import PortfolioState
from poseidon.portfolio.sync import PortfolioSyncService
from poseidon.risk.engine import RiskEngine
from poseidon.security.audit import AuditLog
from poseidon.storage.db import Database

from ..conftest import FakeProvider


def _order(symbol: str, side: OrderSide, qty: str = "5", *,
           status: OrderStatus = OrderStatus.SUBMITTED, broker: str = "paper") -> Order:
    return Order(symbol=symbol, side=side, order_type=OrderType.LIMIT,
                 quantity=Decimal(qty), limit_price=Decimal("100"),
                 time_in_force=TimeInForce.GTC, status=status, broker=broker,
                 strategy="test", created_at=datetime.now(UTC))


@pytest.fixture
async def stack(tmp_path):
    bus = EventBus()
    router = DataRouter([(FakeProvider(name="feed"), 10)], FreshnessPolicy())
    broker = PaperBroker(credentials={}, options={
        "starting_cash": "100000", "state_file": str(tmp_path / "paper.json"),
    })
    broker.set_quote_fn(lambda s: router.quote(s, allow_delayed=True))
    await broker.connect()
    db = Database(tmp_path / "test.db")
    await db.open()
    audit = AuditLog(db)
    portfolio = PortfolioState()
    clock = MarketClock()
    sync = PortfolioSyncService(broker, portfolio, bus, db, clock)
    await sync.sync_once()
    risk = RiskEngine(RiskConfig(news_blackout_minutes_before_econ=0),
                      portfolio, router, clock, bus)
    manager = OrderManager(broker, risk, ApprovalQueue(bus), db, audit, bus,
                           mode=TradingMode.AUTONOMOUS)
    session_patch = patch.object(MarketClock, "session",
                                 return_value=MarketSession.REGULAR)
    session_patch.start()
    yield {"manager": manager, "broker": broker, "db": db, "audit": audit, "sync": sync}
    session_patch.stop()
    await bus.close()


async def _persist_resting(db: Database, order: Order) -> None:
    await db.execute(
        "INSERT INTO orders (id, client_order_id, broker, payload, status, created_at, "
        "updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (order.id, order.client_order_id, order.broker,
         json.dumps(order.model_dump(mode="json"), default=str),
         order.status.value, order.created_at.isoformat(), order.created_at.isoformat()),
    )


async def _audit_actions(db: Database) -> list[str]:
    rows = await db.fetch_all("SELECT action FROM audit ORDER BY seq")
    return [r[0] for r in rows]


async def test_sell_cancels_same_symbol_resting_buy(stack) -> None:
    resting_buy = _order("AAPL", OrderSide.BUY)
    canceled: list[str] = []

    async def fake_cancel(order: Order) -> Order:
        canceled.append(order.id)
        order.status = OrderStatus.CANCELED
        return order

    await _persist_resting(stack["db"], resting_buy)
    stack["broker"].cancel_order = fake_cancel  # type: ignore[method-assign]
    sell = _order("AAPL", OrderSide.SELL, broker="")
    sell.status = OrderStatus.APPROVED
    await stack["manager"]._clear_opposing_orders(sell, stack["broker"])
    assert canceled == [resting_buy.id]
    actions = await _audit_actions(stack["db"])
    assert "exit.opposing_order_canceled" in actions
    # The canceled state was persisted over the resting row.
    row = await stack["db"].fetch_all(
        "SELECT status FROM orders WHERE id = ?", (resting_buy.id,))
    assert row[0][0] == OrderStatus.CANCELED.value


async def test_buy_never_cancels_resting_sell(stack) -> None:
    protective_sell = _order("AAPL", OrderSide.SELL)  # e.g. guardian take-profit
    canceled: list[str] = []

    async def fake_cancel(order: Order) -> Order:  # pragma: no cover - must not run
        canceled.append(order.id)
        return order

    await _persist_resting(stack["db"], protective_sell)
    stack["broker"].cancel_order = fake_cancel  # type: ignore[method-assign]
    buy = _order("AAPL", OrderSide.BUY, broker="")
    await stack["manager"]._clear_opposing_orders(buy, stack["broker"])
    assert canceled == []


async def test_other_symbol_and_cross_broker_buys_are_untouched(stack) -> None:
    other_symbol = _order("MSFT", OrderSide.BUY)
    cross_broker = _order("AAPL", OrderSide.BUY, broker="alpaca")
    canceled: list[str] = []

    async def fake_cancel(order: Order) -> Order:
        canceled.append(order.id)
        order.status = OrderStatus.CANCELED
        return order

    for o in (other_symbol, cross_broker):
        await _persist_resting(stack["db"], o)
    stack["broker"].cancel_order = fake_cancel  # type: ignore[method-assign]
    sell = _order("AAPL", OrderSide.SELL, broker="")
    await stack["manager"]._clear_opposing_orders(sell, stack["broker"])
    assert canceled == []


async def test_cancel_failure_is_audited_and_does_not_block(stack) -> None:
    resting_buy = _order("AAPL", OrderSide.BUY)

    async def broken_cancel(order: Order) -> Order:
        raise RuntimeError("broker wedged")

    await _persist_resting(stack["db"], resting_buy)
    stack["broker"].cancel_order = broken_cancel  # type: ignore[method-assign]
    sell = _order("AAPL", OrderSide.SELL, broker="")
    # Must not raise: the exit proceeds even when the cancel fails.
    await stack["manager"]._clear_opposing_orders(sell, stack["broker"])
    actions = await _audit_actions(stack["db"])
    assert "exit.opposing_cancel_failed" in actions


async def test_end_to_end_sell_clears_buy_then_submits(stack) -> None:
    """Through the real _submit path: buy a position, park a resting buy, then a
    full exit sell both cancels the resting buy and fills the exit."""
    manager, broker, db = stack["manager"], stack["broker"], stack["db"]
    # Open a position through the normal path so reduce-only sees it.
    from .test_p1_manager import make_decision  # same-harness factory
    buys = await manager.execute_decision(make_decision("10"))
    assert buys[0].status is OrderStatus.FILLED
    await stack["sync"].sync_once()  # reduce-only must see the live position
    await stack["db"].execute("DELETE FROM orders")  # forget the filled entry row
    resting_buy = _order("AAPL", OrderSide.BUY)
    await _persist_resting(db, resting_buy)
    real_cancel = broker.cancel_order
    canceled: list[str] = []

    async def spy_cancel(order: Order) -> Order:
        canceled.append(order.id)
        return await real_cancel(order)

    broker.cancel_order = spy_cancel  # type: ignore[method-assign]
    from .test_p1_manager import make_exit_decision
    exits = await manager.execute_decision(make_exit_decision("10"))
    assert canceled == [resting_buy.id]
    assert exits[0].status is OrderStatus.FILLED


async def test_sell_cancels_a_live_resting_buy_the_db_does_not_know(stack) -> None:
    """The broker's LIVE book is consulted too: a same-symbol BUY resting at the
    broker that Poseidon's orders table does not list as open (placed from the
    brokerage's own UI, or a row whose status drifted) would trip the same
    wash-trade block. It is canceled and audited, but never persisted as a
    synthetic local row."""
    manager, broker, db = stack["manager"], stack["broker"], stack["db"]
    external = _order("AAPL", OrderSide.BUY, status=OrderStatus.ACCEPTED)
    external.broker_order_id = "ext-1"
    canceled: list[str] = []

    async def fake_open_orders() -> list[Order]:
        return [external]

    async def fake_cancel(order: Order) -> Order:
        canceled.append(order.broker_order_id or order.id)
        order.status = OrderStatus.CANCELED
        return order

    broker.open_orders = fake_open_orders  # type: ignore[method-assign]
    broker.cancel_order = fake_cancel  # type: ignore[method-assign]
    sell = _order("AAPL", OrderSide.SELL, broker="")
    await manager._clear_opposing_orders(sell, broker)
    assert canceled == ["ext-1"]
    assert "exit.opposing_order_canceled" in await _audit_actions(db)
    rows = await db.fetch_all("SELECT COUNT(*) FROM orders")
    assert rows[0][0] == 0  # no synthetic row for an order we never placed


async def test_live_and_db_views_of_one_order_cancel_it_once(stack) -> None:
    manager, broker, db = stack["manager"], stack["broker"], stack["db"]
    resting = _order("AAPL", OrderSide.BUY)
    resting.broker_order_id = "b-1"
    await _persist_resting(db, resting)
    live_view = _order("AAPL", OrderSide.BUY, status=OrderStatus.ACCEPTED)
    live_view.client_order_id = resting.client_order_id
    live_view.broker_order_id = "b-1"
    canceled: list[str] = []

    async def fake_open_orders() -> list[Order]:
        return [live_view]

    async def fake_cancel(order: Order) -> Order:
        canceled.append(order.broker_order_id or order.id)
        order.status = OrderStatus.CANCELED
        return order

    broker.open_orders = fake_open_orders  # type: ignore[method-assign]
    broker.cancel_order = fake_cancel  # type: ignore[method-assign]
    await manager._clear_opposing_orders(_order("AAPL", OrderSide.SELL, broker=""), broker)
    assert canceled == ["b-1"]


async def test_sell_waits_for_an_asynchronous_cancel_to_confirm(stack, monkeypatch) -> None:
    """Alpaca's DELETE only QUEUES the cancel: cancel_order comes back with the
    order still open (pending_cancel -> ACCEPTED). Submitting the SELL that
    instant can still hit the self-trade block, so the clear waits — bounded —
    for the broker to confirm the cancel before the exit goes out."""
    from poseidon.execution import manager as manager_mod

    monkeypatch.setattr(manager_mod, "_CANCEL_CONFIRM_INTERVAL", 0.0)
    manager, broker, db = stack["manager"], stack["broker"], stack["db"]
    resting = _order("AAPL", OrderSide.BUY)
    resting.broker_order_id = "b-2"
    await _persist_resting(db, resting)
    polls: list[str] = []

    async def queued_cancel(order: Order) -> Order:
        order.status = OrderStatus.ACCEPTED  # cancel requested, not yet confirmed
        order.status_reason = "cancel requested — awaiting broker confirmation"
        return order

    async def status_poll(order: Order) -> Order:
        polls.append(order.broker_order_id or "")
        if len(polls) >= 2:
            order.status = OrderStatus.CANCELED
        return order

    broker.cancel_order = queued_cancel  # type: ignore[method-assign]
    broker.order_status = status_poll  # type: ignore[method-assign]
    await manager._clear_opposing_orders(_order("AAPL", OrderSide.SELL, broker=""), broker)
    assert polls == ["b-2", "b-2"]
    row = await db.fetch_all("SELECT status FROM orders WHERE id = ?", (resting.id,))
    assert row[0][0] == OrderStatus.CANCELED.value


async def test_unconfirmed_cancel_is_audited_and_the_exit_still_proceeds(
        stack, monkeypatch) -> None:
    from poseidon.execution import manager as manager_mod

    monkeypatch.setattr(manager_mod, "_CANCEL_CONFIRM_INTERVAL", 0.0)
    monkeypatch.setattr(manager_mod, "_CANCEL_CONFIRM_ATTEMPTS", 2)
    manager, broker, db = stack["manager"], stack["broker"], stack["db"]
    resting = _order("AAPL", OrderSide.BUY)
    resting.broker_order_id = "b-3"
    await _persist_resting(db, resting)

    async def queued_cancel(order: Order) -> Order:
        order.status = OrderStatus.ACCEPTED
        return order

    async def never_confirms(order: Order) -> Order:
        return order

    broker.cancel_order = queued_cancel  # type: ignore[method-assign]
    broker.order_status = never_confirms  # type: ignore[method-assign]
    await manager._clear_opposing_orders(_order("AAPL", OrderSide.SELL, broker=""), broker)
    actions = await _audit_actions(db)
    assert "exit.opposing_cancel_unconfirmed" in actions


async def test_one_transient_poll_error_does_not_end_the_confirm_wait(
        stack, monkeypatch) -> None:
    """A single transient BrokerError on the confirm-poll must be retried
    within the attempt budget, not collapse ~3s/6-attempts down to one
    attempt — that reproduces the exact self-trade 403 this mechanism exists
    to prevent, from one blip rather than a sustained broker failure."""
    from poseidon.core.errors import BrokerError
    from poseidon.execution import manager as manager_mod

    monkeypatch.setattr(manager_mod, "_CANCEL_CONFIRM_INTERVAL", 0.0)
    monkeypatch.setattr(manager_mod, "_CANCEL_CONFIRM_ATTEMPTS", 3)
    manager, broker, db = stack["manager"], stack["broker"], stack["db"]
    resting = _order("AAPL", OrderSide.BUY)
    resting.broker_order_id = "b-flaky"
    await _persist_resting(db, resting)
    polls: list[str] = []

    async def queued_cancel(order: Order) -> Order:
        order.status = OrderStatus.ACCEPTED
        return order

    async def flaky_then_confirms(order: Order) -> Order:
        polls.append("poll")
        if len(polls) == 1:
            raise BrokerError("alpaca", "transient blip", retryable=True)
        order.status = OrderStatus.CANCELED
        return order

    broker.cancel_order = queued_cancel  # type: ignore[method-assign]
    broker.order_status = flaky_then_confirms  # type: ignore[method-assign]
    await manager._clear_opposing_orders(_order("AAPL", OrderSide.SELL, broker=""), broker)

    assert len(polls) == 2, "the blip must be retried, not end the wait"
    actions = await _audit_actions(db)
    assert "exit.opposing_order_canceled" in actions
    assert "exit.opposing_cancel_unconfirmed" not in actions


async def test_opposing_order_that_fills_during_confirm_is_surfaced_as_a_fill(
        stack, monkeypatch) -> None:
    """The opposing BUY fills instead of canceling while the confirm-poll is
    waiting. This is a real fill — the account's position just changed — and
    must be surfaced exactly like any other fill (ORDER_FILLED + an
    order.filled audit fact), never mislabeled "…_canceled"."""
    from poseidon.execution import manager as manager_mod

    monkeypatch.setattr(manager_mod, "_CANCEL_CONFIRM_INTERVAL", 0.0)
    manager, broker, db = stack["manager"], stack["broker"], stack["db"]
    resting = _order("AAPL", OrderSide.BUY, qty="5")
    resting.broker_order_id = "b-fill"
    await _persist_resting(db, resting)
    published: list[dict] = []

    async def on_filled(_topic: str, payload: object) -> None:
        published.append(payload)  # type: ignore[arg-type]

    manager._bus.subscribe("order.filled", on_filled)

    async def queued_cancel(order: Order) -> Order:
        order.status = OrderStatus.ACCEPTED
        return order

    async def status_poll(order: Order) -> Order:
        order.status = OrderStatus.FILLED
        order.filled_quantity = Decimal("5")
        return order

    broker.cancel_order = queued_cancel  # type: ignore[method-assign]
    broker.order_status = status_poll  # type: ignore[method-assign]
    await manager._clear_opposing_orders(_order("AAPL", OrderSide.SELL, broker=""), broker)
    await manager._bus.close()  # drain the fire-and-forget publish before asserting

    actions = await _audit_actions(db)
    assert "order.filled" in actions
    assert "exit.opposing_order_canceled" not in actions
    assert published and published[0]["order"]["status"] == OrderStatus.FILLED.value


async def test_live_book_read_failure_does_not_block_the_clear(stack) -> None:
    from poseidon.core.errors import BrokerError

    manager, broker, db = stack["manager"], stack["broker"], stack["db"]
    resting = _order("AAPL", OrderSide.BUY)
    await _persist_resting(db, resting)
    canceled: list[str] = []

    async def broken_open_orders() -> list[Order]:
        raise BrokerError("paper", "book unreadable")

    async def fake_cancel(order: Order) -> Order:
        canceled.append(order.id)
        order.status = OrderStatus.CANCELED
        return order

    broker.open_orders = broken_open_orders  # type: ignore[method-assign]
    broker.cancel_order = fake_cancel  # type: ignore[method-assign]
    await manager._clear_opposing_orders(_order("AAPL", OrderSide.SELL, broker=""), broker)
    assert canceled == [resting.id]  # the DB view still cleared its row
