"""Position guardian: arming, breach detection, mode-aware exits, latching."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace

import pytest

from poseidon.core.config import GuardianConfig, RiskConfig
from poseidon.core.enums import (
    DecisionAction,
    MarketSession,
    OrderSide,
    OrderStatus,
    OrderType,
    TradingMode,
)
from poseidon.core.models import Order, Position, Quote
from poseidon.execution.guardian import PositionGuardian
from poseidon.storage.db import Database


class KernelStub:
    """Just the surface the guardian touches."""

    def __init__(self, *, mode: TradingMode, price: str, position_qty: str | None) -> None:
        self.mode = mode
        self._price = Decimal(price)
        self._position_qty = position_qty
        self.executed_decisions: list = []
        self.notifications: list[dict] = []
        self.audit_entries: list[tuple[str, str]] = []

        # The REAL RiskConfig, not a hand-rolled stand-in: the guardian prices
        # its stop exits off slippage_limit_pct x exit_slippage_multiple, and a
        # stub with invented defaults would drift from the shipped ones exactly
        # the way tools/ui_verify.py's FakeKernel drifted from set_mode.
        self.config = SimpleNamespace(risk=RiskConfig())
        self.clock = SimpleNamespace(session=lambda: MarketSession.REGULAR)
        self.broker = SimpleNamespace(name="paper")  # plans are broker-scoped
        self.order_manager = SimpleNamespace(
            mode=mode, execute_decision=self._execute_decision
        )
        self.audit = SimpleNamespace(append=self._audit_append)
        self.bus = SimpleNamespace(publish=self._publish)
        self.router = SimpleNamespace(quote=self._quote)
        # synced_at mirrors PortfolioState: the guardian only trusts a "no
        # position" reading from a snapshot taken after the plan was armed.
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


async def test_fill_arms_exit_plan(tmp_path) -> None:
    db = await _db_with_decision(tmp_path, stop="95", target="120")
    kernel = KernelStub(mode=TradingMode.AUTONOMOUS, price="100", position_qty="10")
    guardian = PositionGuardian(GuardianConfig(), db, kernel)
    await guardian.on_order_filled("order.filled", filled_buy())
    plans = await guardian.active_plans()
    assert plans == [pytest.approx(plans[0])]  # exactly one
    assert plans[0]["symbol"] == "AAPL" and plans[0]["stop_loss"] == "95"
    await db.close()


async def test_guardian_stop_is_a_marketable_limit_not_a_raw_market_order(tmp_path) -> None:
    """A breached stop must still exit on a disorderly book.

    It used to go out as a raw MARKET order, which SlippageProtectionRule
    refuses whenever the spread exceeds the band or the book is one-sided —
    i.e. exactly the conditions a stop exists for, so the stop could not
    execute. It now prices *through* the book by the exit band: marketable, so
    it crosses and fills like a market order, but the fill is bounded.
    """
    db = await _db_with_decision(tmp_path, stop="95", target="120")
    kernel = KernelStub(mode=TradingMode.AUTONOMOUS, price="94.50", position_qty="10")
    guardian = PositionGuardian(GuardianConfig(), db, kernel)
    await guardian.on_order_filled("order.filled", filled_buy())
    await guardian.check_all()
    await guardian.drain()

    trade = kernel.executed_decisions[0].trades[0]
    assert trade.order_type is OrderType.LIMIT, "a raw MARKET stop is refused on a wide book"
    assert trade.limit_price is not None
    # priced through the book by slippage_limit_pct x exit_slippage_multiple
    risk = kernel.config.risk
    band = Decimal(str(risk.slippage_limit_pct)) * Decimal(str(risk.exit_slippage_multiple))
    assert trade.limit_price == Decimal("94.50") * (Decimal(1) - band)
    # ...and it is BELOW the market, or it would rest instead of crossing
    assert trade.limit_price < Decimal("94.50")
    await db.close()


async def test_guardian_take_profit_keeps_the_passive_limit(tmp_path) -> None:
    """Only the stop is urgent. A take-profit must not be priced through the
    book — that would sell a spike for less than the target."""
    db = await _db_with_decision(tmp_path, stop="95", target="120")
    kernel = KernelStub(mode=TradingMode.AUTONOMOUS, price="121", position_qty="10")
    guardian = PositionGuardian(GuardianConfig(), db, kernel)
    await guardian.on_order_filled("order.filled", filled_buy())
    await guardian.check_all()
    await guardian.drain()

    trade = kernel.executed_decisions[0].trades[0]
    assert trade.order_type is OrderType.LIMIT
    assert trade.limit_price == Decimal("121"), "take-profit prices at the level, not through it"
    await db.close()


async def test_crypto_stops_are_enforced_while_the_equity_market_is_closed(tmp_path) -> None:
    """Crypto trades 24/7, so its stops must be watched 24/7.

    The guardian used to return at the top of check_all whenever the NYSE
    session was not REGULAR — before reading a single exit_plans row — and
    MarketClock.session() has no crypto awareness. Everything else in the
    system already exempts crypto from the equity session (MarketOpenRule,
    the cycle's crypto ranking), so a position opened at 21:00 with a stop
    armed went unwatched until 09:30 ET: up to 65 hours over a weekend.
    """
    db = await _db_with_decision(tmp_path, stop="95", target="120")
    kernel = KernelStub(mode=TradingMode.AUTONOMOUS, price="94.50", position_qty="10")
    kernel.clock = SimpleNamespace(session=lambda: MarketSession.CLOSED)
    guardian = PositionGuardian(GuardianConfig(), db, kernel)
    await guardian.on_order_filled("order.filled", filled_buy(symbol="BTC/USD"))
    await guardian.check_all()
    await guardian.drain()

    assert len(kernel.executed_decisions) == 1, "a crypto stop must fire outside RTH"
    assert kernel.executed_decisions[0].trades[0].symbol == "BTC/USD"
    await db.close()


async def test_equity_stops_are_still_gated_on_the_session(tmp_path) -> None:
    """The equity gate is physically correct and must survive: an equity exit
    cannot execute when the exchange is closed."""
    db = await _db_with_decision(tmp_path, stop="95", target="120")
    kernel = KernelStub(mode=TradingMode.AUTONOMOUS, price="94.50", position_qty="10")
    kernel.clock = SimpleNamespace(session=lambda: MarketSession.CLOSED)
    guardian = PositionGuardian(GuardianConfig(), db, kernel)
    await guardian.on_order_filled("order.filled", filled_buy())  # AAPL
    await guardian.check_all()
    await guardian.drain()

    assert kernel.executed_decisions == [], "an equity exit must not fire while closed"
    await db.close()


async def test_both_classes_are_enforced_during_regular_hours(tmp_path) -> None:
    db = await _db_with_decision(tmp_path, stop="95", target="120")
    kernel = KernelStub(mode=TradingMode.AUTONOMOUS, price="94.50", position_qty="10")
    guardian = PositionGuardian(GuardianConfig(), db, kernel)  # REGULAR by default
    await guardian.on_order_filled("order.filled", filled_buy(symbol="ETH/USD"))
    await guardian.check_all()
    await guardian.drain()

    assert len(kernel.executed_decisions) == 1
    await db.close()


async def test_no_plan_when_nothing_enforceable(tmp_path) -> None:
    db = await _db_with_decision(tmp_path, stop=None, target=None)
    kernel = KernelStub(mode=TradingMode.AUTONOMOUS, price="100", position_qty="10")
    guardian = PositionGuardian(GuardianConfig(), db, kernel)
    await guardian.on_order_filled("order.filled", filled_buy())
    assert await guardian.active_plans() == []
    await db.close()


async def test_stop_breach_executes_exit_in_autonomous(tmp_path) -> None:
    db = await _db_with_decision(tmp_path, stop="95", target="120")
    kernel = KernelStub(mode=TradingMode.AUTONOMOUS, price="94.50", position_qty="10")
    guardian = PositionGuardian(GuardianConfig(), db, kernel)
    await guardian.on_order_filled("order.filled", filled_buy())
    await guardian.check_all()
    await guardian.drain()  # exit dispatch runs off the detection loop
    assert len(kernel.executed_decisions) == 1
    decision = kernel.executed_decisions[0]
    assert decision.action is DecisionAction.SELL
    assert decision.trades[0].symbol == "AAPL"
    assert decision.trades[0].quantity == Decimal("10")
    assert decision.rationale is not None and "stop loss" in decision.rationale.thesis
    # Latched: plan no longer active, second sweep does nothing.
    assert await guardian.active_plans() == []
    await guardian.check_all()
    assert len(kernel.executed_decisions) == 1
    assert ("guardian", "exit.triggered") in kernel.audit_entries
    await db.close()


async def test_target_breach_triggers(tmp_path) -> None:
    db = await _db_with_decision(tmp_path, stop="95", target="120")
    kernel = KernelStub(mode=TradingMode.AUTONOMOUS, price="121", position_qty="10")
    guardian = PositionGuardian(GuardianConfig(), db, kernel)
    await guardian.on_order_filled("order.filled", filled_buy())
    await guardian.check_all()
    await guardian.drain()  # exit dispatch runs off the detection loop
    assert len(kernel.executed_decisions) == 1
    assert "take profit" in kernel.executed_decisions[0].rationale.thesis
    await db.close()


async def test_no_breach_no_action(tmp_path) -> None:
    db = await _db_with_decision(tmp_path, stop="95", target="120")
    kernel = KernelStub(mode=TradingMode.AUTONOMOUS, price="100", position_qty="10")
    guardian = PositionGuardian(GuardianConfig(), db, kernel)
    await guardian.on_order_filled("order.filled", filled_buy())
    await guardian.check_all()
    assert kernel.executed_decisions == []
    assert len(await guardian.active_plans()) == 1
    await db.close()


async def test_research_mode_notifies_without_order(tmp_path) -> None:
    db = await _db_with_decision(tmp_path, stop="95", target=None)
    kernel = KernelStub(mode=TradingMode.RESEARCH, price="90", position_qty="10")
    guardian = PositionGuardian(GuardianConfig(), db, kernel)
    await guardian.on_order_filled("order.filled", filled_buy())
    await guardian.check_all()
    assert kernel.executed_decisions == []
    assert kernel.notifications and "Research mode" in kernel.notifications[0]["body"]
    await db.close()


async def _db_with_multi_trade_decision(
    tmp_path, *, trades: list[dict], decision_stop: str | None = None,
    decision_target: str | None = None,
) -> Database:
    db = Database(tmp_path / "g.db")
    await db.open()
    payload = {
        "trades": trades,
        "rationale": {"exit_plan": {"stop_loss": decision_stop, "take_profit": decision_target}},
    }
    await db.execute(
        "INSERT INTO decisions (id, cycle_id, action, payload, created_at) VALUES (?, ?, ?, ?, ?)",
        ("dec1", "c1", "buy", json.dumps(payload), datetime.now(UTC).isoformat()),
    )
    return db


async def test_multi_symbol_decision_arms_each_symbols_own_levels(tmp_path) -> None:
    # A1 regression: a decision that opens two names must arm each position
    # with ITS OWN stop/target — never bleed one symbol's stop onto another.
    trades = [
        {"symbol": "AAPL", "side": "buy", "stop_loss": "95", "take_profit": "120"},
        {"symbol": "MSFT", "side": "buy", "stop_loss": "300", "take_profit": "400"},
    ]
    db = await _db_with_multi_trade_decision(tmp_path, trades=trades)
    kernel = KernelStub(mode=TradingMode.AUTONOMOUS, price="100", position_qty="10")
    guardian = PositionGuardian(GuardianConfig(), db, kernel)
    await guardian.on_order_filled("order.filled", filled_buy(symbol="AAPL"))
    plans = await guardian.active_plans()
    assert len(plans) == 1
    assert plans[0]["symbol"] == "AAPL"
    assert plans[0]["stop_loss"] == "95" and plans[0]["take_profit"] == "120"  # not MSFT's 300/400
    await db.close()


async def test_multi_symbol_decision_without_per_trade_levels_arms_nothing(tmp_path) -> None:
    # A1 safety stopgap: when a multi-buy decision has no per-trade levels, the
    # decision-level plan is ambiguous, so the guardian arms nothing rather
    # than risk stopping out the wrong position.
    trades = [
        {"symbol": "AAPL", "side": "buy"},
        {"symbol": "MSFT", "side": "buy"},
    ]
    db = await _db_with_multi_trade_decision(
        tmp_path, trades=trades, decision_stop="95", decision_target="120"
    )
    kernel = KernelStub(mode=TradingMode.AUTONOMOUS, price="100", position_qty="10")
    guardian = PositionGuardian(GuardianConfig(), db, kernel)
    await guardian.on_order_filled("order.filled", filled_buy(symbol="AAPL"))
    assert await guardian.active_plans() == []
    await db.close()


async def test_single_buy_decision_falls_back_to_decision_level_plan(tmp_path) -> None:
    # Unambiguous single-position decision: the decision-level exit plan still
    # arms (backward-compatible with plans that omit per-trade levels).
    trades = [{"symbol": "AAPL", "side": "buy"}]
    db = await _db_with_multi_trade_decision(
        tmp_path, trades=trades, decision_stop="95", decision_target="120"
    )
    kernel = KernelStub(mode=TradingMode.AUTONOMOUS, price="100", position_qty="10")
    guardian = PositionGuardian(GuardianConfig(), db, kernel)
    await guardian.on_order_filled("order.filled", filled_buy(symbol="AAPL"))
    plans = await guardian.active_plans()
    assert len(plans) == 1 and plans[0]["stop_loss"] == "95"
    await db.close()


async def test_plans_are_broker_scoped(tmp_path) -> None:
    # A stop armed while on the paper broker must not fire (or even be
    # visible) after switching to a real brokerage — paper and live state
    # never mix on an order path.
    db = await _db_with_decision(tmp_path, stop="95", target="120")
    kernel = KernelStub(mode=TradingMode.AUTONOMOUS, price="90", position_qty="10")
    guardian = PositionGuardian(GuardianConfig(), db, kernel)
    await guardian.on_order_filled("order.filled", filled_buy())
    assert len(await guardian.active_plans()) == 1
    kernel.broker.name = "alpaca"  # broker switched
    assert await guardian.active_plans() == []
    await guardian.check_all()  # price breaches the old stop — must NOT fire
    assert kernel.executed_decisions == []
    await db.close()


async def _backdate_plan(db: Database, symbol: str, seconds: int) -> None:
    """Make a plan look as if it was armed ``seconds`` ago (its updated_at)."""
    await db.execute(
        "UPDATE exit_plans SET updated_at = ? WHERE symbol = ?",
        ((datetime.now(UTC) - timedelta(seconds=seconds)).isoformat(), symbol),
    )


async def test_plan_deactivates_when_position_gone(tmp_path) -> None:
    db = await _db_with_decision(tmp_path, stop="95", target="120")
    kernel = KernelStub(mode=TradingMode.AUTONOMOUS, price="100", position_qty="10")
    guardian = PositionGuardian(GuardianConfig(), db, kernel)
    await guardian.on_order_filled("order.filled", filled_buy())
    await _backdate_plan(db, "AAPL", 60)  # armed a minute ago ...
    kernel._position_qty = None  # ... position closed externally / by the AI ...
    kernel.portfolio.synced_at = datetime.now(UTC)  # ... and a sync since confirms it
    await guardian.check_all()
    assert await guardian.active_plans() == []
    assert kernel.executed_decisions == []
    await db.close()


async def test_plan_survives_a_snapshot_older_than_its_arming(tmp_path) -> None:
    """Regression: a fill arms a plan, then the guardian ticks BEFORE the periodic
    portfolio sync has seen that fill. position_for() is None only because the
    snapshot is stale — the position is real. Disarming here left every fresh
    scalp entry unprotected until the next decision happened to re-arm it
    (guardian interval 15s vs sync interval 30s: a coin flip per fill, at best).
    """
    db = await _db_with_decision(tmp_path, stop="95", target="120")
    kernel = KernelStub(mode=TradingMode.AUTONOMOUS, price="100", position_qty=None)
    kernel.portfolio.synced_at = datetime.now(UTC) - timedelta(seconds=20)
    guardian = PositionGuardian(GuardianConfig(), db, kernel)
    await guardian.on_order_filled("order.filled", filled_buy())
    await guardian.check_all()
    assert [p["symbol"] for p in await guardian.active_plans()] == ["AAPL"]
    assert kernel.executed_decisions == []
    # A sync taken well AFTER the arming that still shows no position IS proof.
    await _backdate_plan(db, "AAPL", 60)
    kernel.portfolio.synced_at = datetime.now(UTC)
    await guardian.check_all()
    assert await guardian.active_plans() == []
    await db.close()


async def test_plan_survives_a_sync_inside_the_grace_window(tmp_path) -> None:
    """A sync whose fetches straddled the fill can finish after the arming and
    still miss the position; the same 10s grace the risk engine gives an order
    submitted mid-sync-pass applies before a missing position is believed."""
    db = await _db_with_decision(tmp_path, stop="95", target="120")
    kernel = KernelStub(mode=TradingMode.AUTONOMOUS, price="100", position_qty=None)
    guardian = PositionGuardian(GuardianConfig(), db, kernel)
    await guardian.on_order_filled("order.filled", filled_buy())
    kernel.portfolio.synced_at = datetime.now(UTC) + timedelta(seconds=2)  # 2s after arming
    await guardian.check_all()
    assert [p["symbol"] for p in await guardian.active_plans()] == ["AAPL"]
    await db.close()


async def test_check_all_skips_a_stale_exit_when_a_fill_races_the_quote(tmp_path) -> None:
    """A concurrent fill can re-arm a plan's row (new decision_id/quantity)
    while check_all awaits the quote for the OLD arming it already read. The
    compare-and-swap in _trigger_exit must detect the row changed underneath
    it and skip firing an exit keyed off the now-stale decision_id/quantity,
    rather than clobbering the fresh arming and selling against the wrong
    decision."""
    db = await _db_with_decision(tmp_path, stop="95", target="120")
    kernel = KernelStub(mode=TradingMode.AUTONOMOUS, price="94.50", position_qty="10")
    guardian = PositionGuardian(GuardianConfig(), db, kernel)
    await guardian.on_order_filled("order.filled", filled_buy(qty="10"))  # dec1, qty=10

    original_quote = kernel.router.quote

    async def racing_quote(symbol, allow_delayed=False):
        # A fill lands and re-arms this SAME plan while the quote is in
        # flight — exactly the window between check_all's row read and its
        # await on kernel.router.quote.
        await db.execute(
            "UPDATE exit_plans SET decision_id = ?, quantity = ?, updated_at = ? "
            "WHERE symbol = ?",
            ("dec2", "20", datetime.now(UTC).isoformat(), symbol),
        )
        return await original_quote(symbol, allow_delayed=allow_delayed)

    kernel.router.quote = racing_quote
    await guardian.check_all()
    await guardian.drain()

    assert kernel.executed_decisions == [], "must not fire against the now-stale read"
    plans = await guardian.active_plans()
    assert len(plans) == 1
    assert plans[0]["quantity"] == "20", "the fresh arming must survive untouched"
    await db.close()


async def test_maybe_deactivate_skips_when_a_fill_races_the_grace_check(tmp_path) -> None:
    """A concurrent fill can re-arm the plan's row between _maybe_deactivate's
    updated_at SELECT and its deactivating UPDATE. The compare-and-swap must
    detect the row changed and leave the fresh arming alone, instead of
    silently clobbering it back to inactive."""
    db = await _db_with_decision(tmp_path, stop="95", target="120")
    kernel = KernelStub(mode=TradingMode.AUTONOMOUS, price="100", position_qty=None)
    guardian = PositionGuardian(GuardianConfig(), db, kernel)
    await guardian.on_order_filled("order.filled", filled_buy(qty="10"))
    await _backdate_plan(db, "AAPL", 60)  # armed a minute ago
    kernel.portfolio.synced_at = datetime.now(UTC)  # postdates arming -> would deactivate

    real_fetch_one = db.fetch_one

    async def racing_fetch_one(sql, params=()):
        result = await real_fetch_one(sql, params)
        if "exit_plans" in sql and "updated_at" in sql:
            await db.execute(
                "UPDATE exit_plans SET decision_id = ?, quantity = ?, updated_at = ? "
                "WHERE symbol = ?",
                ("dec2", "20", datetime.now(UTC).isoformat(), "AAPL"),
            )
        return result

    db.fetch_one = racing_fetch_one  # type: ignore[method-assign]
    await guardian._maybe_deactivate("AAPL", "position no longer held")

    plans = await guardian.active_plans()
    assert len(plans) == 1
    assert plans[0]["quantity"] == "20", "the fresh arming must survive untouched"
    await db.close()


async def test_maybe_deactivate_captures_synced_at_with_position_not_after(tmp_path) -> None:
    """``position`` and ``synced_at`` must be captured together, from the same
    sync pass. If a full portfolio sync lands during the fetch_one await,
    ``synced_at`` can advance to postdate the arming even though the
    ALREADY-CAPTURED ``position`` reading is the stale pre-sync one — that
    combination must not wrongly validate a "position gone" reading the fresh
    sync actually contradicts."""
    db = await _db_with_decision(tmp_path, stop="95", target="120")
    kernel = KernelStub(mode=TradingMode.AUTONOMOUS, price="100", position_qty=None)
    guardian = PositionGuardian(GuardianConfig(), db, kernel)
    await guardian.on_order_filled("order.filled", filled_buy())
    await _backdate_plan(db, "AAPL", 60)  # armed a minute ago
    # Predates that arming by more than the grace (-90s < -60s + 10s grace).
    kernel.portfolio.synced_at = datetime.now(UTC) - timedelta(seconds=90)

    real_fetch_one = db.fetch_one

    async def sync_lands_during_fetch(sql, params=()):
        # A full sync completes mid-await: the position is real (10 held) and
        # synced_at advances to postdate the arming — but _maybe_deactivate
        # already captured the STALE "gone" position before this call.
        kernel._position_qty = "10"
        kernel.portfolio.synced_at = datetime.now(UTC)
        return await real_fetch_one(sql, params)

    db.fetch_one = sync_lands_during_fetch  # type: ignore[method-assign]
    await guardian._maybe_deactivate("AAPL", "position no longer held")

    assert [p["symbol"] for p in await guardian.active_plans()] == ["AAPL"], (
        "must not deactivate off a position reading that predates the sync which "
        "actually confirmed it"
    )
    await db.close()


async def test_sell_fill_does_not_disarm_on_a_stale_snapshot(tmp_path) -> None:
    """A partial exit fill on a position the snapshot has not seen yet must not
    disarm the remainder's stop."""
    db = await _db_with_decision(tmp_path, stop="95", target="120")
    kernel = KernelStub(mode=TradingMode.AUTONOMOUS, price="100", position_qty=None)
    kernel.portfolio.synced_at = datetime.now(UTC) - timedelta(seconds=20)
    guardian = PositionGuardian(GuardianConfig(), db, kernel)
    await guardian.on_order_filled("order.filled", filled_buy(qty="10"))
    partial = Order(symbol="AAPL", side=OrderSide.SELL, quantity=Decimal("4"),
                    filled_quantity=Decimal("4"), status=OrderStatus.FILLED, strategy="ai")
    await guardian.on_order_filled("order.filled", {"order": partial.model_dump(mode="json")})
    assert [p["symbol"] for p in await guardian.active_plans()] == ["AAPL"]
    await db.close()
