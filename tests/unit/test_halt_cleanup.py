"""Halt-cleanup behaviour on ``OrderManager`` (control-hardening spec §3.2).

Task 4 — ``cancel_all_open``: on halt, every resting broker order is canceled
exactly once (never retried), a cancel failure is recorded and the loop
continues, cross-broker rows are skipped (never canceled against the wrong
brokerage), and the pass returns a frozen ``HaltCleanupSummary``.

These pins guard the real-money control plane: a resting order left live during
a halt can fill mid-halt, and a retry loop on a failing cancel could hammer the
broker or wedge the halt. The behaviour under test is deterministic — no LLM,
no risk-rule exemption, Decimal money — and each consequential action audits.
"""

from __future__ import annotations

from dataclasses import FrozenInstanceError, fields, is_dataclass
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

import httpx
import pytest

from poseidon.api.server import build_app
from poseidon.app import ApplicationKernel
from poseidon.core.config import AppConfig
from poseidon.core.enums import (
    AssetClass,
    BrokerCapability,
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
    TradingMode,
)
from poseidon.core.errors import BrokerError, CircuitBreakerOpen, RiskViolation
from poseidon.core.events import EventBus, Topics
from poseidon.core.models import Order, Position
from poseidon.execution.manager import (
    _OPEN_AT_BROKER_STATUSES,
    HaltCleanupSummary,
    OrderManager,
)
from poseidon.risk.rules import reduce_only_breach
from poseidon.security.audit import AuditLog
from poseidon.security.vault import Vault
from poseidon.storage.db import Database


class FakeBroker:
    """A minimal broker whose ``cancel_order`` is fully controllable: it counts
    every call (to prove exactly-once) and can be made to raise for chosen order
    ids (to prove failures are recorded, not retried)."""

    account_scope = "fake:paper"

    def __init__(self, name: str = "fake") -> None:
        self.name = name
        self.cancel_calls: list[str] = []
        self.fail_ids: set[str] = set()

    async def cancel_order(self, order: Order) -> Order:
        self.cancel_calls.append(order.id)
        if order.id in self.fail_ids:
            raise BrokerError(self.name, f"cancel rejected for {order.id}", retryable=True)
        order.status = OrderStatus.CANCELED
        return order

    async def open_orders(self) -> list[Order]:
        # cancel_all_open merges this live view with the orders table so a
        # halt cancels an order the DB doesn't know about too; empty here
        # keeps these DB-only-scoped tests exercising just that view.
        return []


@pytest.fixture
async def harness(tmp_path):
    """Real DB + AuditLog + EventBus with a controllable fake broker. The risk
    engine and approval queue are irrelevant to cancel_all_open (it never
    touches them), so they are stubbed."""
    bus = EventBus()
    db = Database(tmp_path / "halt.db")
    await db.open()
    audit = AuditLog(db)
    broker = FakeBroker()
    manager = OrderManager(broker, MagicMock(), MagicMock(), db, audit, bus,
                           mode=TradingMode.AUTONOMOUS)
    yield {"manager": manager, "broker": broker, "db": db, "audit": audit, "bus": bus}
    await bus.close()
    await db.close()


async def _seed_open(manager: OrderManager, *, symbol: str, broker: str,
                     status: OrderStatus = OrderStatus.SUBMITTED,
                     qty: str = "10") -> Order:
    """Persist an order the DB will report as live at the broker."""
    order = Order(symbol=symbol, side=OrderSide.SELL, order_type=OrderType.LIMIT,
                  quantity=Decimal(qty), limit_price=Decimal("100.00"),
                  strategy="momentum", broker=broker, status=status)
    await manager._persist(order)
    return order


def _actions(audit_calls: list[tuple[str, str, dict]]) -> list[str]:
    return [action for (_actor, action, _payload) in audit_calls]


def _spy_audit(audit: AuditLog) -> list[tuple[str, str, dict]]:
    calls: list[tuple[str, str, dict]] = []
    real = audit.append

    async def spy(actor, action, payload=None):
        calls.append((actor, action, payload or {}))
        return await real(actor, action, payload)

    audit.append = spy
    return calls


# -- test_cancels_each_open_order_once ---------------------------------------------

async def test_cancels_each_open_order_once(harness) -> None:
    manager, broker = harness["manager"], harness["broker"]
    a = await _seed_open(manager, symbol="AAPL", broker="fake",
                         status=OrderStatus.SUBMITTED)
    b = await _seed_open(manager, symbol="MSFT", broker="fake",
                         status=OrderStatus.ACCEPTED)
    c = await _seed_open(manager, symbol="NVDA", broker="fake",
                         status=OrderStatus.PARTIALLY_FILLED)

    summary = await manager.cancel_all_open(reason="operator HALT")

    # Every live row canceled, each exactly once (no retries, no double-cancel).
    assert sorted(broker.cancel_calls) == sorted([a.id, b.id, c.id])
    assert len(broker.cancel_calls) == 3
    assert set(summary.canceled) == {a.id, b.id, c.id}
    assert summary.failed == []
    assert summary.skipped == []
    # The DB rows are now CANCELED (persisted).
    for order in (a, b, c):
        row = await harness["db"].fetch_one("SELECT status FROM orders WHERE id = ?", (order.id,))
        assert row[0] == OrderStatus.CANCELED.value


# -- test_cancels_a_live_order_the_db_does_not_know --------------------------------

async def test_cancels_a_live_order_the_db_does_not_know(harness) -> None:
    """F: an order parked from the brokerage's own UI (or a local row whose
    status drifted) has no row in the orders table, but is still live at the
    broker — an emergency halt must cancel it too, not just the rows this
    platform placed. Mirrors _clear_opposing_orders' live-book merge."""
    manager, broker = harness["manager"], harness["broker"]
    known = await _seed_open(manager, symbol="AAPL", broker="fake")
    external = Order(symbol="MSFT", side=OrderSide.SELL, order_type=OrderType.LIMIT,
                     quantity=Decimal("3"), limit_price=Decimal("50"), broker="fake",
                     status=OrderStatus.ACCEPTED, broker_order_id="ext-1")

    async def fake_open_orders() -> list[Order]:
        return [external]

    broker.open_orders = fake_open_orders  # type: ignore[method-assign]

    summary = await manager.cancel_all_open(reason="operator HALT")

    assert sorted(broker.cancel_calls) == sorted([known.id, external.id])
    assert set(summary.canceled) == {known.id, external.id}
    # The DB-known order is persisted CANCELED; the live-only one gets no
    # synthetic row (mirrors _clear_opposing_orders' known/live-only split).
    row = await harness["db"].fetch_one("SELECT status FROM orders WHERE id = ?", (known.id,))
    assert row[0] == OrderStatus.CANCELED.value
    rows = await harness["db"].fetch_all("SELECT id FROM orders")
    assert {r[0] for r in rows} == {known.id}


# -- test_open_at_broker_statuses_derive_from_enum ---------------------------------

def test_open_at_broker_statuses_derive_from_enum() -> None:
    # cancel_all_open's live-status IN-list is derived from the single source of
    # truth OrderStatus.is_open_at_broker, not a hardcoded literal, so the query and
    # the enum predicate can never drift (spec §3.2). It stays exactly the three
    # broker-live states, and nothing terminal can leak into a cancel sweep.
    assert set(_OPEN_AT_BROKER_STATUSES) == {
        OrderStatus.SUBMITTED.value, OrderStatus.ACCEPTED.value,
        OrderStatus.PARTIALLY_FILLED.value,
    }
    assert all(OrderStatus(s).is_open_at_broker for s in _OPEN_AT_BROKER_STATUSES)
    assert not any(OrderStatus(s).is_terminal for s in _OPEN_AT_BROKER_STATUSES)


# -- test_cancel_failure_recorded_not_retried --------------------------------------

async def test_cancel_failure_recorded_not_retried(harness) -> None:
    manager, broker = harness["manager"], harness["broker"]
    audit_calls = _spy_audit(harness["audit"])
    bad = await _seed_open(manager, symbol="AAPL", broker="fake")
    good = await _seed_open(manager, symbol="MSFT", broker="fake")
    broker.fail_ids = {bad.id}

    summary = await manager.cancel_all_open(reason="operator HALT")

    # The failing cancel was attempted EXACTLY ONCE — no retry loop.
    assert broker.cancel_calls.count(bad.id) == 1
    # It is recorded in the failure list with order_id/symbol/error…
    assert len(summary.failed) == 1
    failure = summary.failed[0]
    assert failure["order_id"] == bad.id
    assert failure["symbol"] == "AAPL"
    assert failure["error"]
    # …and audited as a system halt.cancel_failed fact.
    assert any(
        actor == "system" and action == "halt.cancel_failed"
        and payload.get("order_id") == bad.id
        for (actor, action, payload) in audit_calls
    ), f"expected a halt.cancel_failed audit; got {audit_calls}"
    # The loop CONTINUED: the healthy order was still canceled.
    assert summary.canceled == [good.id]
    assert broker.cancel_calls.count(good.id) == 1


# -- test_cross_broker_rows_skipped_and_recorded -----------------------------------

async def test_cross_broker_rows_skipped_and_recorded(harness) -> None:
    manager, broker = harness["manager"], harness["broker"]
    mine = await _seed_open(manager, symbol="AAPL", broker="fake")
    theirs = await _seed_open(manager, symbol="MSFT", broker="other_brokerage")

    summary = await manager.cancel_all_open(reason="operator HALT")

    # A cross-broker order is NEVER canceled against the active broker…
    assert theirs.id not in broker.cancel_calls
    # …but it IS recorded so the operator knows it survived the halt.
    assert summary.skipped == [theirs.id]
    # The active-broker order was canceled.
    assert broker.cancel_calls == [mine.id]
    assert summary.canceled == [mine.id]


# -- test_returns_summary ----------------------------------------------------------

async def test_returns_summary(harness) -> None:
    manager = harness["manager"]

    summary = await manager.cancel_all_open(reason="operator HALT")

    # A frozen dataclass with exactly canceled / failed / skipped.
    assert isinstance(summary, HaltCleanupSummary)
    assert is_dataclass(summary)
    assert {f.name for f in fields(summary)} == {"canceled", "failed", "skipped"}
    # Frozen: fields cannot be reassigned.
    with pytest.raises(FrozenInstanceError):
        summary.canceled = ["x"]  # type: ignore[misc]
    # Empty book → empty summary (no crash, no cancel calls).
    assert summary.canceled == []
    assert summary.failed == []
    assert summary.skipped == []


# -- switching guard (spec §3.2: "if _switching → abort cleanup, record") ----------

async def test_switching_aborts_cleanup_without_canceling(harness) -> None:
    manager, broker = harness["manager"], harness["broker"]
    audit_calls = _spy_audit(harness["audit"])
    await _seed_open(manager, symbol="AAPL", broker="fake")
    manager._switching = True

    summary = await manager.cancel_all_open(reason="operator HALT")

    # A mid-flight broker swap means the book's broker is indeterminate — cancel
    # nothing rather than cancel against the wrong brokerage.
    assert broker.cancel_calls == []
    assert summary.canceled == []
    assert "halt.cleanup_failed" in _actions(audit_calls)


# ==========================================================================
# Task 5 — flatten_all: reduce-only exits, full rule chain, skip survivors.
# ==========================================================================
#
# The opt-in second cleanup phase of kernel.halt() (control-hardening spec
# §3.2/§3.3). Given the engine-minted, identity-checked halt token, flatten_all
# builds ONE reduce-only MARKET DAY exit per live position (long -> SELL /
# BUY_TO_CLOSE for a short / SELL_TO_CLOSE for a long option), runs each through
# the FULL rule chain (validate_order + the live _guard_reduce_only backstop —
# never exempted by the token), skips any symbol whose resting order survived the
# cancel pass (a resting order must never fill against a closing trade), records
# every denial without retrying, and never consults the approval queue (the
# operator's halt IS the human consent — audited as ``human``).


class FlattenBroker(FakeBroker):
    """Extends the cancel-fake with a controllable book + positions and a
    submit spy, so flatten_all can be driven deterministically: what it flattens,
    what it skips, and the exact orders it builds are all observable."""

    def __init__(self, name: str = "fake") -> None:
        super().__init__(name)
        self.positions_list: list[Position] = []
        self.open_orders_list: list[Order] = []
        self.submit_calls: list[Order] = []
        self.positions_calls = 0
        self.fail_fetch = False

    def capabilities(self) -> frozenset[BrokerCapability]:
        return frozenset({
            BrokerCapability.OPTIONS, BrokerCapability.CRYPTO,
            BrokerCapability.FRACTIONAL_SHARES, BrokerCapability.EXTENDED_HOURS,
        })

    async def positions(self) -> list[Position]:
        self.positions_calls += 1
        if self.fail_fetch:
            raise BrokerError(self.name, "positions feed down", retryable=True)
        return list(self.positions_list)

    async def open_orders(self) -> list[Order]:
        return list(self.open_orders_list)

    async def preflight(self, order: Order) -> str | None:
        return None

    async def submit_order(self, order: Order) -> Order:
        self.submit_calls.append(order)
        order.status = OrderStatus.SUBMITTED
        order.broker = self.name
        order.broker_order_id = f"bx-{order.id[:6]}"
        return order

    async def order_status(self, order: Order) -> Order:
        return order


class FakeRisk:
    """A risk engine faithful to the ONLY two things flatten_all's rule chain
    depends on: the halt-flatten carve-out predicate (identity-checked token) and
    the reduce-only invariant — enforced through the REAL ``reduce_only_breach``
    against a controllable ``held`` snapshot, so ``test_reduce_only_rule_still
    _consulted`` exercises the genuine rule, not a stubbed raise. ``deny_symbols``
    lets a test make any other rule deny a chosen symbol. Everything else _submit
    touches is a recorded no-op."""

    def __init__(self) -> None:
        self.circuit = SimpleNamespace(is_open=True, reason="operator HALT")
        self.token: object | None = None
        self.held: dict[str, Decimal] = {}
        self.reduce_open_orders: list[Order] = []
        self.deny_symbols: dict[str, RiskViolation] = {}
        self.validate_calls: list[Order] = []

    def open_halt_flatten_window(self, *, ttl_seconds: float = 300.0) -> object:
        self.token = object()
        return self.token

    def halt_exit_permitted(self, order: Order, token: object | None) -> bool:
        return (token is not None and token is self.token
                and order.side.is_risk_reducing and not order.legs)

    async def validate_order(self, order: Order, *, halt_token: object | None = None,
                             allow_delayed: bool = False):
        # Semantic pin, not signature convenience: the halt-flatten path must
        # validate strictly — a delayed carve-out there would submit real-book
        # exits on aged quotes. (Without this assert, merely syncing the stub's
        # signature would silently erase the strictness pin.)
        assert allow_delayed is False, "halt-flatten must validate live-only"
        self.validate_calls.append(order)
        if self.circuit.is_open and not (
                halt_token is not None and self.halt_exit_permitted(order, halt_token)):
            raise CircuitBreakerOpen(self.circuit.reason)
        if order.symbol.upper() in self.deny_symbols:
            raise self.deny_symbols[order.symbol.upper()]
        msg = reduce_only_breach(
            order, lambda s: self.held.get(s.upper(), Decimal(0)), self.reduce_open_orders)
        if msg is not None:
            raise RiskViolation("reduce_only", msg)
        return

    def note_order_submitted(self, order: Order) -> None:
        return None

    def release_validated(self, order_id: str) -> None:
        return None

    def note_execution_error(self, reason: str) -> None:
        return None


def _pos(symbol: str, qty: str, *, asset_class: AssetClass = AssetClass.EQUITY) -> Position:
    return Position(symbol=symbol, asset_class=asset_class, quantity=Decimal(qty),
                    avg_entry_price=Decimal("100.00"), as_of=datetime.now(UTC))


@pytest.fixture
async def flat(tmp_path):
    """Real DB + AuditLog + EventBus with a controllable book/positions broker and
    a carve-out-faithful risk engine. The breaker is OPEN (a halt is in progress)
    and a live token is minted, exactly as kernel.halt() will present to
    flatten_all (task 6)."""
    bus = EventBus()
    db = Database(tmp_path / "flat.db")
    await db.open()
    audit = AuditLog(db)
    broker = FlattenBroker()
    risk = FakeRisk()
    manager = OrderManager(broker, risk, MagicMock(), db, audit, bus,
                           mode=TradingMode.AUTONOMOUS)
    token = risk.open_halt_flatten_window()
    yield {"manager": manager, "broker": broker, "risk": risk, "db": db,
           "audit": audit, "bus": bus, "token": token}
    await manager.stop()
    await bus.close()
    await db.close()


# -- test_builds_reduce_only_market_exits ------------------------------------------

async def test_builds_reduce_only_market_exits(flat) -> None:
    manager, broker, risk = flat["manager"], flat["broker"], flat["risk"]
    audit_calls = _spy_audit(flat["audit"])
    # A long equity, a short equity, and a long option — the three sizing cases.
    broker.positions_list = [
        _pos("AAPL", "10"),
        _pos("TSLA", "-5"),
        _pos("AAPL260116C00150000", "3", asset_class=AssetClass.OPTION),
    ]
    risk.held = {"AAPL": Decimal("10"), "TSLA": Decimal("-5"),
                 "AAPL260116C00150000": Decimal("3")}

    summary = await manager.flatten_all(flat["token"], reason="operator HALT")

    built = {o.symbol: o for o in broker.submit_calls}
    assert set(built) == {"AAPL", "TSLA", "AAPL260116C00150000"}
    # Long equity -> SELL; short equity -> BUY_TO_CLOSE; long option -> SELL_TO_CLOSE.
    assert built["AAPL"].side is OrderSide.SELL
    assert built["TSLA"].side is OrderSide.BUY_TO_CLOSE
    assert built["AAPL260116C00150000"].side is OrderSide.SELL_TO_CLOSE
    # Every exit is a reduce-only, leg-free MARKET DAY order sized to abs(qty),
    # strategy halt_flatten — the exact shape halt_exit_permitted admits.
    for order in broker.submit_calls:
        assert order.order_type is OrderType.MARKET
        assert order.time_in_force is TimeInForce.DAY
        assert order.strategy == "halt_flatten"
        assert order.legs == []
        assert order.side.is_risk_reducing
    assert built["AAPL"].quantity == Decimal("10")
    assert built["TSLA"].quantity == Decimal("5")       # abs of the short
    assert built["AAPL260116C00150000"].quantity == Decimal("3")
    # All three submitted; nothing refused.
    assert len(summary.submitted) == 3
    assert summary.refused == []
    # The token rode into every validate_order (the carve-out was consulted).
    assert {o.symbol for o in risk.validate_calls} == {
        "AAPL", "TSLA", "AAPL260116C00150000"}
    # The operator's halt IS the consent: submissions audit as human/halt.flatten_submitted.
    submitted_audits = [(actor, payload) for (actor, action, payload) in audit_calls
                        if action == "halt.flatten_submitted"]
    assert len(submitted_audits) == 3
    assert all(actor == "human" for (actor, _payload) in submitted_audits)


# -- test_skips_symbols_with_surviving_open_orders ---------------------------------

async def test_skips_symbols_with_surviving_open_orders(flat) -> None:
    manager, broker, risk = flat["manager"], flat["broker"], flat["risk"]
    audit_calls = _spy_audit(flat["audit"])
    broker.positions_list = [_pos("AAPL", "10"), _pos("MSFT", "7")]
    risk.held = {"AAPL": Decimal("10"), "MSFT": Decimal("7")}
    # A resting order for MSFT survived the cancel pass (cancel-failed / pending
    # cancel / cross-broker): MSFT must NOT be flattened — a resting order can
    # never be raced against a closing trade.
    survivor = Order(symbol="MSFT", side=OrderSide.SELL, order_type=OrderType.LIMIT,
                     quantity=Decimal("7"), limit_price=Decimal("300.00"),
                     strategy="guardian_exit", broker="fake",
                     status=OrderStatus.SUBMITTED)
    broker.open_orders_list = [survivor]

    summary = await manager.flatten_all(flat["token"], reason="operator HALT")

    # Only AAPL is flattened; MSFT is excluded, not raced.
    assert [o.symbol for o in broker.submit_calls] == ["AAPL"]
    assert summary.submitted and all(  # AAPL got submitted
        True for _ in summary.submitted)
    assert any(r.get("symbol") == "MSFT" and r.get("reason") == "open_order_survives"
               for r in summary.refused), summary.refused
    # …and the exclusion is recorded for the operator.
    assert any(
        action == "halt.flatten_refused" and payload.get("symbol") == "MSFT"
        and payload.get("reason") == "open_order_survives"
        for (_actor, action, payload) in audit_calls
    ), f"expected a halt.flatten_refused for MSFT; got {audit_calls}"
    # MSFT never even reached validate_order (skipped before the rule chain).
    assert "MSFT" not in {o.symbol for o in risk.validate_calls}


# -- test_research_mode_refuses ----------------------------------------------------

async def test_research_mode_refuses(flat) -> None:
    manager, broker = flat["manager"], flat["broker"]
    audit_calls = _spy_audit(flat["audit"])
    manager.set_mode(TradingMode.RESEARCH)
    broker.positions_list = [_pos("AAPL", "10")]

    summary = await manager.flatten_all(flat["token"], reason="operator HALT")

    # Research means no orders from anyone — flatten refuses entirely and never
    # even reads positions.
    assert summary.submitted == []
    assert broker.submit_calls == []
    assert broker.positions_calls == 0
    assert "halt.flatten_refused" in _actions(audit_calls)


# -- test_rule_denial_recorded_not_retried -----------------------------------------

async def test_rule_denial_recorded_not_retried(flat) -> None:
    manager, broker, risk = flat["manager"], flat["broker"], flat["risk"]
    audit_calls = _spy_audit(flat["audit"])
    broker.positions_list = [_pos("AAPL", "10"), _pos("MSFT", "7")]
    risk.held = {"AAPL": Decimal("10"), "MSFT": Decimal("7")}
    # A rule denies the AAPL exit (e.g. a one-sided book -> SlippageProtectionRule).
    risk.deny_symbols = {"AAPL": RiskViolation("slippage", "book too dislocated to exit")}

    summary = await manager.flatten_all(flat["token"], reason="operator HALT")

    # The denied exit was validated EXACTLY ONCE — no retry loop on a rule denial.
    aapl_validations = [o for o in risk.validate_calls if o.symbol == "AAPL"]
    assert len(aapl_validations) == 1
    # It never reached the broker, and it is recorded as refused + audited.
    assert "AAPL" not in {o.symbol for o in broker.submit_calls}
    assert any(r.get("symbol") == "AAPL" for r in summary.refused), summary.refused
    assert any(
        action == "halt.flatten_refused" and payload.get("symbol") == "AAPL"
        for (_actor, action, payload) in audit_calls
    )
    # The loop CONTINUED: the healthy MSFT exit was still submitted.
    assert "MSFT" in {o.symbol for o in broker.submit_calls}
    assert len(summary.submitted) == 1


# -- test_reduce_only_rule_still_consulted -----------------------------------------

async def test_reduce_only_rule_still_consulted(flat) -> None:
    manager, broker, risk = flat["manager"], flat["broker"], flat["risk"]
    audit_calls = _spy_audit(flat["audit"])
    # The broker reports 10 shares (so flatten builds a SELL 10), but the
    # reduce-only snapshot shows only 8 closable — an oversized exit. Even on the
    # sanctioned halt path, carrying a live token, the reduce-only rule is NEVER
    # exempted: the exit must be rejected, not forced past into a short.
    broker.positions_list = [_pos("AAPL", "10")]
    risk.held = {"AAPL": Decimal("8")}

    summary = await manager.flatten_all(flat["token"], reason="operator HALT")

    # Rejected by the real reduce-only invariant, recorded, not submitted.
    assert broker.submit_calls == []
    assert len(summary.submitted) == 0
    assert len(summary.refused) == 1
    assert "reduce_only" in summary.refused[0]["reason"]
    # Consulted exactly once (no retry), and audited as a refusal.
    assert len([o for o in risk.validate_calls if o.symbol == "AAPL"]) == 1
    assert any(
        action == "halt.flatten_refused" and payload.get("symbol") == "AAPL"
        for (_actor, action, payload) in audit_calls
    )


# ==========================================================================
# Task 6 — kernel.halt() orchestration: latch, then cancel-all, then opt-in
# flatten; the latch survives any cleanup failure (control-hardening spec §3.2).
# ==========================================================================
#
# kernel.halt() drives the two OrderManager cleanup phases in a provably safe
# order: (1) it latches the breaker synchronously — trading is dead before any
# broker/DB I/O; (2) it cancels every resting order (always); (3) only when
# ``risk.flatten_on_halt`` is on AND the mode is not RESEARCH does it mint the
# engine's identity-checked flatten token, run ``flatten_all`` through the full
# rule chain, and close the window in ``finally``. Cancel-all ALWAYS completes
# (and the quiet-book check inside flatten_all runs) before any flatten submit,
# so a resting order can never fill against a closing trade. Any exception in
# phases 2-3 is caught: the halt latch always stands. No LLM, Decimal money,
# every consequential action hash-chained into the audit log.


class RecordingBroker(FlattenBroker):
    """A FlattenBroker that also appends each broker call into a shared ordered
    event list, so kernel.halt()'s phase ordering (latch → cancel → flatten) is
    observable end-to-end across the real orchestration."""

    def __init__(self, events: list[str], name: str = "fake") -> None:
        super().__init__(name)
        self._events = events

    async def cancel_order(self, order: Order) -> Order:
        self._events.append(f"cancel:{order.symbol}")
        return await super().cancel_order(order)

    async def positions(self) -> list[Position]:
        self._events.append("positions")
        return await super().positions()

    async def open_orders(self) -> list[Order]:
        self._events.append("open_orders")
        return await super().open_orders()

    async def submit_order(self, order: Order) -> Order:
        self._events.append(f"submit:{order.symbol}")
        return await super().submit_order(order)


class _RecordingCircuit:
    """A breaker faithful to the ONE thing the halt latch needs: ``force_open``
    trips it synchronously (it starts CLOSED, exactly as the real breaker does
    before a halt). Records the trip into the shared event list so a test can
    prove the latch precedes every broker call."""

    def __init__(self, events: list[str]) -> None:
        self._events = events
        self.is_open = False
        self.reason: str | None = None

    def force_open(self, reason: str) -> None:
        self._events.append("force_open")
        self.is_open = True
        self.reason = reason

    def force_close(self, reason: str | None = None) -> None:
        self.is_open = False


class KernelRisk(FakeRisk):
    """FakeRisk wired for the kernel: a recording circuit that starts CLOSED
    (``force_open`` trips it, as the real halt latch does) and a real
    ``close_halt_flatten_window`` so the window's open/close is observable."""

    def __init__(self, events: list[str]) -> None:
        super().__init__()
        self.circuit = _RecordingCircuit(events)  # type: ignore[assignment]
        self.window_opened = False
        self.window_closed = False

    def open_halt_flatten_window(self, *, ttl_seconds: float = 300.0) -> object:
        self.window_opened = True
        return super().open_halt_flatten_window(ttl_seconds=ttl_seconds)

    def close_halt_flatten_window(self) -> None:
        self.window_closed = True
        self.token = None


@pytest.fixture
async def kernel_ctx(tmp_path):
    """A real ApplicationKernel with its heavyweight deps replaced by the
    controllable halt-cleanup fakes (real DB + AuditLog + EventBus, a recording
    broker/risk, a real OrderManager). ``flatten_on_halt`` starts OFF; a test
    flips ``kernel.config.risk.flatten_on_halt`` to exercise the opt-in phase.
    NOTIFY payloads are captured synchronously by wrapping ``bus.publish``."""
    events: list[str] = []
    notifies: list[dict] = []
    bus = EventBus()
    db = Database(tmp_path / "kernel_halt.db")
    await db.open()
    audit = AuditLog(db)
    broker = RecordingBroker(events)
    risk = KernelRisk(events)
    manager = OrderManager(broker, risk, MagicMock(), db, audit, bus,
                           mode=TradingMode.AUTONOMOUS)
    config = AppConfig(data_dir=tmp_path)  # flatten_on_halt default False
    kernel = ApplicationKernel(config, Vault(tmp_path / "v.bin"))
    kernel.bus = bus
    kernel.db = db
    kernel.audit = audit
    kernel.risk = risk  # type: ignore[assignment]
    kernel.broker = broker  # type: ignore[assignment]
    kernel.order_manager = manager

    orig_publish = bus.publish

    async def spy_publish(topic, payload=None):
        if topic == Topics.NOTIFY:
            notifies.append(payload or {})
        return await orig_publish(topic, payload)

    bus.publish = spy_publish  # type: ignore[method-assign]
    yield {"kernel": kernel, "manager": manager, "broker": broker, "risk": risk,
           "db": db, "audit": audit, "bus": bus, "events": events,
           "notifies": notifies, "config": config, "tmp_path": tmp_path}
    await manager.stop()
    await bus.close()
    await db.close()


# -- test_latch_precedes_any_broker_call -------------------------------------------

async def test_latch_precedes_any_broker_call(kernel_ctx) -> None:
    kernel, manager, risk = (kernel_ctx["kernel"], kernel_ctx["manager"],
                             kernel_ctx["risk"])
    events = kernel_ctx["events"]
    audit_calls = _spy_audit(kernel_ctx["audit"])
    await _seed_open(manager, symbol="AAPL", broker="fake")

    await kernel.halt("operator HALT")

    # The breaker is tripped FIRST — trading is dead before any broker I/O.
    assert events, "expected the halt to have driven cleanup"
    assert events[0] == "force_open"
    broker_calls = [i for i, e in enumerate(events)
                    if e.startswith(("cancel:", "submit:")) or e in ("positions", "open_orders")]
    assert broker_calls, "expected at least the cancel pass to touch the broker"
    assert events.index("force_open") < min(broker_calls)
    # The latch is durable three ways: in-memory breaker, kv marker, sentinel file.
    assert risk.circuit.is_open is True
    assert await kernel_ctx["db"].kv_get("circuit.manual_halt") == "operator HALT"
    assert (kernel_ctx["tmp_path"] / "HALT").read_text() == "operator HALT"
    assert "trading.halted" in _actions(audit_calls)


# -- test_cancel_completes_before_first_flatten_submit -----------------------------

async def test_cancel_completes_before_first_flatten_submit(kernel_ctx) -> None:
    kernel, manager, broker, risk = (kernel_ctx["kernel"], kernel_ctx["manager"],
                                     kernel_ctx["broker"], kernel_ctx["risk"])
    events = kernel_ctx["events"]
    kernel.config.risk.flatten_on_halt = True
    # A resting order to cancel, plus a live position to flatten. The book is
    # quiet AFTER the cancel pass (open_orders empty), so AAPL flattens.
    await _seed_open(manager, symbol="AAPL", broker="fake")
    broker.positions_list = [_pos("AAPL", "10")]
    broker.open_orders_list = []
    risk.held = {"AAPL": Decimal("10")}

    await kernel.halt("operator HALT")

    cancels = [i for i, e in enumerate(events) if e.startswith("cancel:")]
    submits = [i for i, e in enumerate(events) if e.startswith("submit:")]
    assert cancels, "expected the resting order to be canceled"
    assert submits, "expected the live position to be flattened"
    # EVERY cancel completes before the FIRST flatten submit — a resting order
    # can never fill against a closing trade.
    assert max(cancels) < min(submits)
    # The flatten window was opened and closed (in finally).
    assert risk.window_opened is True
    assert risk.window_closed is True


# -- test_flatten_off_by_default_no_positions_call ---------------------------------

async def test_flatten_off_by_default_no_positions_call(kernel_ctx) -> None:
    kernel, manager, broker, risk = (kernel_ctx["kernel"], kernel_ctx["manager"],
                                     kernel_ctx["broker"], kernel_ctx["risk"])
    events = kernel_ctx["events"]
    # flatten_on_halt is OFF (default). A live position exists but must NOT be read.
    await _seed_open(manager, symbol="AAPL", broker="fake")
    broker.positions_list = [_pos("AAPL", "10")]

    await kernel.halt("operator HALT")

    # Cancel-all still ran (it is the non-gated fix), but flatten is skipped
    # entirely: no positions() call, no window, no submit.
    assert broker.positions_calls == 0
    assert "positions" not in events
    assert [e for e in events if e.startswith("submit:")] == []
    assert risk.window_opened is False
    assert broker.cancel_calls  # the resting order was still canceled


# -- test_partial_fill_cancel_remainder_then_flatten_filled_position ---------------

async def test_partial_fill_cancel_remainder_then_flatten_filled_position(kernel_ctx) -> None:
    kernel, manager, broker, risk = (kernel_ctx["kernel"], kernel_ctx["manager"],
                                     kernel_ctx["broker"], kernel_ctx["risk"])
    events = kernel_ctx["events"]
    kernel.config.risk.flatten_on_halt = True
    # A PARTIALLY_FILLED resting order: cancel kills the resting remainder; the
    # already-filled portion is a live position that flatten then closes.
    remainder = await _seed_open(manager, symbol="AAPL", broker="fake",
                                 status=OrderStatus.PARTIALLY_FILLED)
    broker.positions_list = [_pos("AAPL", "6")]  # the filled portion, now a position
    broker.open_orders_list = []                 # quiet book after the cancel
    risk.held = {"AAPL": Decimal("6")}

    await kernel.halt("operator HALT")

    # The remainder was canceled exactly once…
    assert broker.cancel_calls.count(remainder.id) == 1
    row = await kernel_ctx["db"].fetch_one(
        "SELECT status FROM orders WHERE id = ?", (remainder.id,))
    assert row[0] == OrderStatus.CANCELED.value
    # …and the filled position was flattened with a reduce-only SELL sized to it.
    exits = [o for o in broker.submit_calls if o.symbol == "AAPL"]
    assert len(exits) == 1
    assert exits[0].side is OrderSide.SELL
    assert exits[0].quantity == Decimal("6")
    assert exits[0].strategy == "halt_flatten"
    # Cancel strictly before the flatten submit.
    assert max(i for i, e in enumerate(events) if e.startswith("cancel:")) < \
        min(i for i, e in enumerate(events) if e.startswith("submit:"))


# -- test_cleanup_failure_keeps_latch ----------------------------------------------

async def test_cleanup_failure_keeps_latch(kernel_ctx) -> None:
    kernel, manager, broker, risk = (kernel_ctx["kernel"], kernel_ctx["manager"],
                                     kernel_ctx["broker"], kernel_ctx["risk"])
    audit_calls = _spy_audit(kernel_ctx["audit"])

    async def boom(*, reason: str):
        raise RuntimeError("DB unavailable during cancel pass")

    manager.cancel_all_open = boom  # type: ignore[method-assign]

    await kernel.halt("operator HALT")

    # The cleanup blew up, but the latch ALWAYS stands: breaker tripped, kv
    # marker and sentinel written, halt audited.
    assert risk.circuit.is_open is True
    assert await kernel_ctx["db"].kv_get("circuit.manual_halt") == "operator HALT"
    assert (kernel_ctx["tmp_path"] / "HALT").read_text() == "operator HALT"
    assert "trading.halted" in _actions(audit_calls)
    # The failure is loud: audited as halt.cleanup_failed and a critical notify.
    assert "halt.cleanup_failed" in _actions(audit_calls)
    assert any(n.get("level") == "critical" for n in kernel_ctx["notifies"])
    # Flatten never attempted after a cancel-phase failure.
    assert risk.window_opened is False
    assert broker.submit_calls == []


# -- test_audit_facts_chain --------------------------------------------------------

async def test_audit_facts_chain(kernel_ctx) -> None:
    kernel, manager, broker, risk = (kernel_ctx["kernel"], kernel_ctx["manager"],
                                     kernel_ctx["broker"], kernel_ctx["risk"])
    audit = kernel_ctx["audit"]
    kernel.config.risk.flatten_on_halt = True
    await _seed_open(manager, symbol="AAPL", broker="fake")
    broker.positions_list = [_pos("AAPL", "10")]
    broker.open_orders_list = []
    risk.held = {"AAPL": Decimal("10")}
    audit_calls = _spy_audit(audit)

    await kernel.halt("operator HALT")

    actions = _actions(audit_calls)
    # The full halt-cleanup story is hash-chained: the latch, the cancel, and
    # the flatten submit are all recorded facts.
    assert "trading.halted" in actions
    assert "halt.order_canceled" in actions
    assert "halt.flatten_submitted" in actions
    # And the audit chain still verifies end-to-end (tamper-evident, intact).
    ok, bad_seq = await audit.verify_chain()
    assert ok, f"audit chain broke at seq {bad_seq}"
    # A single loud critical summary notification was raised.
    assert any(n.get("level") == "critical" for n in kernel_ctx["notifies"])


# -- test_api_halt_runs_cleanup ----------------------------------------------------

async def test_api_halt_runs_cleanup(kernel_ctx, monkeypatch) -> None:
    """POST /api/halt routes through ``kernel.halt`` (spec §8 task 8), so the
    operator's dashboard HALT latches the breaker AND runs the cancel-all cleanup
    — a resting order left live during a halt could fill mid-halt."""
    monkeypatch.delenv("POSEIDON_DASHBOARD_TOKEN", raising=False)
    monkeypatch.delenv("POSEIDON_DASHBOARD_TOKEN_FILE", raising=False)
    kernel, manager, broker, risk = (kernel_ctx["kernel"], kernel_ctx["manager"],
                                     kernel_ctx["broker"], kernel_ctx["risk"])
    order = await _seed_open(manager, symbol="AAPL", broker="fake")
    app = build_app(kernel)

    async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://localhost") as c:
        r = await c.post("/api/halt", json={"reason": "dashboard HALT"})

    assert r.status_code == 200, r.text
    # The latch stands…
    assert risk.circuit.is_open is True
    assert await kernel_ctx["db"].kv_get("circuit.manual_halt") == "dashboard HALT"
    # …and cleanup ran: the resting order was canceled exactly once through the API.
    assert broker.cancel_calls == [order.id]
