"""Position guardian: enforces exit plans between review cycles.

Every AI decision that opens a position carries an exit plan (stop loss /
take profit). Without enforcement those numbers are just prose — a gap
between review cycles where nothing watches the position. The guardian
closes that gap:

  * when an entry order fills, the decision's exit plan is persisted per
    symbol;
  * on a short interval during market hours, each active plan is checked
    against a live, freshness-graded quote;
  * a breach produces an exit through the normal order-manager path — so
    it obeys the operating mode (research: notify only; approval: queued
    for the human; autonomous: executed) and still passes the risk engine;
  * plans deactivate when the position is gone, and a triggered plan does
    not retrigger.

The guardian enforces numeric stops/targets. ``time_stop`` is free text
by design (e.g. "exit before earnings") and remains the AI's job during
review cycles.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from decimal import Decimal

import structlog

from ..core.clock import synced_after
from ..core.config import GuardianConfig
from ..core.enums import (
    DecisionAction,
    MarketSession,
    OrderSide,
    OrderStatus,
    OrderType,
    TradingMode,
)
from ..core.errors import DataError
from ..core.events import Topics
from ..core.models import Decision, ExitPlan, Order, ProposedTrade, TradeRationale
from ..core.symbols import is_crypto_symbol
from ..storage.db import Database

log = structlog.get_logger(__name__)

# Terminal states meaning "the exit did not protect the position".
# REJECTED_HUMAN is deliberately excluded: an approval-mode decline must not
# loop back to the human every guardian tick.
_EXIT_FAILED = {OrderStatus.REJECTED_RISK, OrderStatus.REJECTED_BROKER,
                OrderStatus.ERROR, OrderStatus.CANCELED, OrderStatus.EXPIRED}

# A "no position" reading from the synced snapshot is only PROOF the position is
# gone when that snapshot was taken after the plan was armed. A fill arms a plan
# the moment the order poller sees it, but the portfolio snapshot is refreshed on
# the sync interval — so the guardian's next tick routinely runs against a
# snapshot older than the fill, sees no position, and (before this guard) disarmed
# the brand-new stop as "position no longer held". ``core.clock.synced_after``
# supplies the grace, shared with the risk engine's ``_reconcile_pending`` (the
# analogous "is this snapshot fresh enough to trust a missing order/position"
# question) so the two windows can never drift out of step with each other.


class PositionGuardian:
    def __init__(self, config: GuardianConfig, db: Database, kernel: object) -> None:
        # `kernel` is the ApplicationKernel; typed loosely to avoid an import
        # cycle. The guardian uses: router, portfolio, order_manager, clock,
        # bus, audit.
        self._config = config
        self._db = db
        self._kernel = kernel
        # Background exit dispatches. In approval mode execute_decision blocks
        # on the human (up to the approval TTL); running it inline in check_all
        # would starve every other position's stop enforcement. We keep strong
        # references so the tasks are not garbage-collected mid-flight.
        self._pending_exits: set[asyncio.Task[None]] = set()

    # -- plan registration (wired to ORDER_FILLED events) ----------------------

    async def on_order_filled(self, _topic: str, payload: object) -> None:
        order_data = (payload or {}).get("order", {}) if isinstance(payload, dict) else {}
        if not order_data:
            return
        try:
            order = Order.model_validate(order_data)
        except Exception:
            return
        if order.side.is_buy and order.decision_id:
            await self._register_plan_for(order)
        elif order.side.is_risk_reducing:
            if order.strategy == "guardian":
                # Whether the guardian's own exit closed the position IN FULL
                # is decided from the order's OWN fill data, never the
                # portfolio snapshot: ORDER_FILLED here and the kernel's
                # fill-triggered portfolio resync are unordered concurrent
                # handlers of the same event (app.py's _sync_after_fill), so a
                # snapshot read right now can still show the pre-fill
                # quantity. filled_quantity/quantity on this order carry no
                # such race — they are exactly what this fill closed.
                remaining = order.quantity - order.filled_quantity
                if remaining > 0:
                    # Only PARTIALLY closed (the rest canceled/expired).
                    # _trigger_exit already latched the plan inactive, and a
                    # partial-then-terminal exit is routed to ORDER_FILLED
                    # (not ORDER_UPDATED), so the on_order_update re-arm never
                    # fires. Re-arm here or the residual has no stop between
                    # review cycles.
                    await self._rearm(order.symbol)
                    await self._kernel.bus.publish(Topics.NOTIFY, {  # type: ignore[attr-defined]
                        "level": "warning", "title": f"Guardian exit partial: {order.symbol}",
                        "body": f"Exit filled {order.filled_quantity}; {remaining} still "
                                "held — stop re-armed for the remainder. Review the position.",
                    })
                # A full close needs no action: _trigger_exit already latched
                # the plan inactive.
            else:
                await self._maybe_deactivate(order.symbol, "position reduced/closed")

    async def _register_plan_for(self, order: Order) -> None:
        row = await self._db.fetch_one(
            "SELECT payload FROM decisions WHERE id = ?", (order.decision_id,)
        )
        if row is None:
            return
        import json

        decision = json.loads(row[0])
        trades = decision.get("trades") or []
        rationale = decision.get("rationale") or {}
        decision_exit = rationale.get("exit_plan") or {}
        symbol_up = order.symbol.upper()

        # Attribute exit levels to THIS symbol only. Prefer the matching
        # trade's own stop/target; fall back to the decision-level exit plan
        # ONLY when the decision opened a single position (so there is no
        # ambiguity about whose stop it is). A multi-symbol decision with no
        # per-trade levels arms nothing rather than risk applying one
        # symbol's stop to another (which would force-sell a fresh position).
        matching = next((t for t in trades if str(t.get("symbol", "")).upper() == symbol_up), None)
        stop = target = None
        if matching is not None and (matching.get("stop_loss") or matching.get("take_profit")):
            stop = matching.get("stop_loss")
            target = matching.get("take_profit")
        elif len([t for t in trades if str(t.get("side", "")).startswith("buy")]) <= 1:
            stop = decision_exit.get("stop_loss")
            target = decision_exit.get("take_profit")
        else:
            log.warning("multi-symbol decision without per-trade exit levels; not arming",
                        symbol=order.symbol, decision_id=order.decision_id)
            return
        if stop is None and target is None:
            return  # nothing enforceable
        exit_plan = decision_exit  # time_stop (free text) is decision-level
        now = datetime.now(UTC).isoformat()
        broker = self._kernel.broker.name  # type: ignore[attr-defined]
        await self._db.execute(
            "INSERT INTO exit_plans (symbol, decision_id, stop_loss, take_profit, time_stop, "
            "quantity, active, triggered_reason, created_at, updated_at, broker) "
            "VALUES (?, ?, ?, ?, ?, ?, 1, NULL, ?, ?, ?) "
            "ON CONFLICT(symbol) DO UPDATE SET decision_id=excluded.decision_id, "
            "stop_loss=excluded.stop_loss, take_profit=excluded.take_profit, "
            "time_stop=excluded.time_stop, quantity=excluded.quantity, active=1, "
            "triggered_reason=NULL, updated_at=excluded.updated_at, broker=excluded.broker",
            (order.symbol.upper(), order.decision_id, stop, target,
             exit_plan.get("time_stop"), str(order.filled_quantity or order.quantity),
             now, now, broker),
        )
        log.info("exit plan armed", symbol=order.symbol, stop=stop, target=target, broker=broker)

    async def _maybe_deactivate(self, symbol: str, reason: str) -> None:
        portfolio = self._kernel.portfolio  # type: ignore[attr-defined]
        position = portfolio.position_for(symbol)
        # Captured together, before the fetch_one await below can yield the
        # event loop to a concurrent full portfolio sync — otherwise a fresh
        # ``synced_at`` read after that await could pair with THIS stale
        # ``position`` read to wrongly validate a "position gone" reading
        # that predates the sync that actually confirmed it (a race distinct
        # from the exit_plans compare-and-swap below: this one lives entirely
        # in the in-memory PortfolioState, no DB row ever changes).
        synced_at = getattr(portfolio, "synced_at", None)
        # A qty-0 row (some brokers report one after a same-day flat) counts
        # as gone — otherwise the plan stays armed with stale levels and can
        # force-sell a later manual re-buy on its first tick.
        if position is not None and position.quantity > 0:
            return
        row = await self._db.fetch_one(
            "SELECT updated_at FROM exit_plans WHERE symbol = ? AND active = 1",
            (symbol.upper(),),
        )
        if row is None:
            return
        armed_at_iso = str(row[0])
        try:
            armed_at = datetime.fromisoformat(armed_at_iso)
        except ValueError:
            armed_at = None  # an unparseable stamp cannot hold a plan armed forever
        if armed_at is not None and not synced_after(synced_at, armed_at):
            # The snapshot predates the arming (or was never taken): "no
            # position" says nothing about the fill that armed this plan. Keep
            # the stop; the next sync will either show the position (and the
            # guardian enforces) or, taken after the grace, prove it gone.
            log.info("exit plan kept armed: portfolio snapshot predates its arming",
                     symbol=symbol, armed_at=armed_at_iso, synced_at=synced_at)
            return
        # Compare-and-swap on updated_at: a concurrent fill can re-arm this
        # exact row (a new decision_id/stop/quantity) between the SELECT above
        # and this UPDATE. Pinning the WHERE to the value just read means a
        # re-arm in that window makes this UPDATE match zero rows instead of
        # silently clobbering the fresh arming back to inactive.
        await self._db.execute(
            "UPDATE exit_plans SET active = 0, triggered_reason = ?, updated_at = ? "
            "WHERE symbol = ? AND active = 1 AND updated_at = ?",
            (reason, datetime.now(UTC).isoformat(), symbol.upper(), armed_at_iso),
        )

    # -- the watch loop (scheduler job) ------------------------------------------

    async def check_all(self) -> None:
        if not self._config.enabled:
            return
        kernel = self._kernel
        # The session gate is PER SYMBOL, not per sweep. It used to return here
        # before reading a single row, and MarketClock.session() is US-equity-
        # only — so crypto stops went unenforced outside 09:30-16:00 ET, up to
        # 65 hours over a weekend, while the rest of the system correctly
        # treats crypto as 24/7 (MarketOpenRule exempts AssetClass.CRYPTO; the
        # review cycle ranks crypto unconditionally). An armed-and-unwatched
        # stop is worse than none, because it substitutes for one.
        equities_tradeable = (
            kernel.clock.session() is MarketSession.REGULAR  # type: ignore[attr-defined]
        )
        # Broker-scoped: a plan armed for another brokerage's position must
        # never fire here. Legacy rows (broker='') still match the active
        # broker so pre-upgrade plans keep protecting their positions.
        rows = await self._db.fetch_all(
            "SELECT symbol, decision_id, stop_loss, take_profit, updated_at FROM exit_plans "
            "WHERE active = 1 AND broker IN (?, '')",
            (kernel.broker.name,),  # type: ignore[attr-defined]
        )
        for symbol, decision_id, stop_raw, target_raw, updated_at in rows:
            if not equities_tradeable and not is_crypto_symbol(symbol):
                continue  # an equity exit genuinely cannot execute while closed
            position = kernel.portfolio.position_for(symbol)  # type: ignore[attr-defined]
            if position is None or position.quantity <= 0:
                await self._maybe_deactivate(symbol, "position no longer held")
                continue
            try:
                # Awaits the network: a concurrent fill can re-arm this plan's
                # row (new decision_id/stop/quantity) while this is in flight.
                # ``updated_at`` (read above, before this await) rides through
                # to ``_trigger_exit`` as the compare-and-swap token, so a
                # re-arm in this window is detected there rather than firing
                # an exit sized/keyed off what is now stale data.
                quote = await kernel.router.quote(symbol, allow_delayed=False)  # type: ignore[attr-defined]
            except DataError as exc:
                log.warning("guardian cannot price position; will retry",
                            symbol=symbol, error=str(exc))
                continue
            price = quote.bid or quote.mid or quote.last
            if price is None:
                continue
            stop = Decimal(stop_raw) if stop_raw else None
            target = Decimal(target_raw) if target_raw else None
            breach: str | None = None
            if stop is not None and price <= stop:
                breach = f"stop loss: {symbol} at {price} <= stop {stop}"
            elif target is not None and price >= target:
                breach = f"take profit: {symbol} at {price} >= target {target}"
            if breach:
                await self._trigger_exit(symbol, decision_id, position.quantity, price, breach,
                                         str(updated_at))

    async def _trigger_exit(self, symbol: str, decision_id: str, quantity: Decimal,
                            price: Decimal, reason: str, expected_updated_at: str) -> None:
        kernel = self._kernel
        now = datetime.now(UTC).isoformat()
        # Latch first so a slow/failed exit cannot fire once per tick forever;
        # the outcome (fill or rejection) is notified and audited either way.
        # Compare-and-swap on updated_at (pinned to the value check_all read
        # before its quote await): if a concurrent fill re-armed this row in
        # that window, this UPDATE matches zero rows rather than clobbering
        # the fresh arming and firing an exit against stale decision_id/
        # stop/quantity.
        affected = await self._db.execute(
            "UPDATE exit_plans SET active = 0, triggered_reason = ?, updated_at = ? "
            "WHERE symbol = ? AND active = 1 AND updated_at = ?",
            (reason, now, symbol.upper(), expected_updated_at),
        )
        if affected == 0:
            log.info("guardian exit skipped: plan changed since it was read",
                     symbol=symbol, reason=reason)
            return
        await kernel.audit.append("guardian", "exit.triggered",  # type: ignore[attr-defined]
                                  {"symbol": symbol, "reason": reason, "price": str(price)})
        log.warning("guardian exit triggered", symbol=symbol, reason=reason)

        mode: TradingMode = kernel.order_manager.mode  # type: ignore[attr-defined]
        if mode is TradingMode.RESEARCH:
            await kernel.bus.publish(Topics.NOTIFY, {  # type: ignore[attr-defined]
                "level": "warning", "title": f"Exit level hit: {symbol}",
                "body": f"{reason}. Research mode — no order placed; review the position.",
            })
            return

        # A breached stop must exit NOW: a DAY limit at the breach-time bid
        # can rest unfilled through a gap and expire, leaving the position
        # unprotected exactly when the stop mattered most. Take-profit keeps
        # the limit (no urgency; never sell a spike for less than the target).
        is_stop = reason.startswith("stop loss")
        # A stop is priced THROUGH the book rather than sent as a raw market
        # order. A raw market order is refused by SlippageProtectionRule
        # whenever the spread exceeds the band or the book is one-sided —
        # exactly the disorderly conditions a stop exists for, so the stop
        # could not execute when it mattered. A limit this far through the
        # market is marketable (it crosses and fills like a market order) while
        # still bounding the fill. Take-profit keeps the passive limit at the
        # level: no urgency, and never sell a spike for less than the target.
        risk = self._kernel.config.risk  # type: ignore[attr-defined]
        exit_band = (Decimal(str(risk.slippage_limit_pct))
                     * Decimal(str(risk.exit_slippage_multiple)))
        limit_price = price * (Decimal(1) - exit_band) if is_stop else price
        decision = Decision(
            action=DecisionAction.SELL,
            trades=[ProposedTrade(
                symbol=symbol, side=OrderSide.SELL,
                order_type=OrderType.LIMIT,
                quantity=quantity,
                limit_price=limit_price,
                strategy="guardian",
            )],
            rationale=TradeRationale(
                thesis=f"Exit-plan enforcement: {reason}.",
                timing="The pre-committed exit level from the original decision was reached.",
                expected_edge="Discipline: realizing the planned exit rather than drifting.",
                risk="Exit executes at the live bid; slippage bounded by the risk engine.",
                reward="Caps the loss / locks in the gain the original plan specified.",
                confidence=1.0,
                supporting_indicators=[reason],
                portfolio_impact=f"Closes the {symbol} position ({quantity} units).",
                exit_plan=ExitPlan(notes="this order IS the exit"),
                max_expected_loss="bounded by the stop level already reached",
            ),
            data_sources=["guardian"],
            model="guardian",
            cycle_id=f"guardian-{decision_id[:8]}",
            created_at=datetime.now(UTC),
        )
        # Dispatch off the detection loop: in approval mode execute_decision
        # blocks on the human, and check_all must keep enforcing every other
        # position's stop meanwhile. The plan is already latched inactive above,
        # so this cannot re-fire while it is in flight.
        task = asyncio.create_task(self._dispatch_exit(symbol, decision, reason),
                                   name=f"guardian-exit-{symbol}")
        self._pending_exits.add(task)
        task.add_done_callback(self._pending_exits.discard)

    async def drain(self) -> None:
        """Await any in-flight exit dispatches. Used at shutdown so a pending
        exit is not abandoned, and by tests to observe the dispatch."""
        if self._pending_exits:
            await asyncio.gather(*self._pending_exits, return_exceptions=True)

    async def _dispatch_exit(self, symbol: str, decision: Decision, reason: str) -> None:
        kernel = self._kernel
        try:
            orders = await kernel.order_manager.execute_decision(decision)  # type: ignore[attr-defined]
        except Exception as exc:  # noqa: BLE001 — a failed exit must always be surfaced, never swallowed
            log.error("guardian exit dispatch failed", symbol=symbol, error=str(exc))
            await kernel.bus.publish(Topics.NOTIFY, {  # type: ignore[attr-defined]
                "level": "critical", "title": f"Guardian exit FAILED: {symbol}",
                "body": f"{reason}\nCould not place the exit order: {exc}. "
                        "The stop is disarmed — review the position immediately.",
            })
            return
        # An exit that ended terminal-with-no-fill did not protect the
        # position: re-arm the plan (the guardian retries next tick, sized
        # from the live position) and escalate to critical.
        failed = [o for o in orders if o.status in _EXIT_FAILED and o.filled_quantity <= 0]
        if failed:
            await self._rearm(symbol)
            await kernel.bus.publish(Topics.NOTIFY, {  # type: ignore[attr-defined]
                "level": "critical", "title": f"Guardian exit FAILED: {symbol}",
                "body": f"{reason}\nExit order ended {failed[0].status.value} with no fill"
                        + (f" — {failed[0].status_reason}" if failed[0].status_reason else "")
                        + ". Plan re-armed; the guardian will retry next tick. "
                          "Review the position.",
            })
            return
        for order in orders:
            await kernel.bus.publish(Topics.NOTIFY, {  # type: ignore[attr-defined]
                "level": "warning", "title": f"Guardian exit: {symbol}",
                "body": f"{reason}\nOrder status: {order.status.value}"
                        + (f" — {order.status_reason}" if order.status_reason else ""),
            })

    async def _rearm(self, symbol: str) -> None:
        await self._db.execute(
            "UPDATE exit_plans SET active = 1, triggered_reason = NULL, updated_at = ? "
            "WHERE symbol = ?",
            (datetime.now(UTC).isoformat(), symbol.upper()),
        )

    async def on_order_update(self, _topic: str, payload: object) -> None:
        """A guardian DAY exit that rests unfilled and expires/cancels at the
        close surfaces later via the manager's poller — re-protect then."""
        order_data = (payload or {}).get("order", {}) if isinstance(payload, dict) else {}
        if not order_data:
            return
        try:
            order = Order.model_validate(order_data)
        except Exception:
            return
        if (order.strategy == "guardian" and order.side.is_risk_reducing
                and order.status in (OrderStatus.CANCELED, OrderStatus.EXPIRED)
                and order.filled_quantity < order.quantity
                and self._kernel.portfolio.position_for(order.symbol) is not None):  # type: ignore[attr-defined]
            await self._rearm(order.symbol)
            await self._kernel.bus.publish(Topics.NOTIFY, {  # type: ignore[attr-defined]
                "level": "critical", "title": f"Guardian exit UNFILLED: {order.symbol}",
                "body": f"Exit order {order.status.value} with "
                        f"{order.filled_quantity}/{order.quantity} filled. The position "
                        "is unprotected; plan re-armed — review immediately.",
            })

    async def active_plans(self) -> list[dict[str, object]]:
        rows = await self._db.fetch_all(
            "SELECT symbol, stop_loss, take_profit, time_stop, quantity, created_at "
            "FROM exit_plans WHERE active = 1 AND broker IN (?, '') ORDER BY symbol",
            (self._kernel.broker.name,),  # type: ignore[attr-defined]
        )
        return [
            {"symbol": r[0], "stop_loss": r[1], "take_profit": r[2],
             "time_stop": r[3], "quantity": r[4], "created_at": r[5]}
            for r in rows
        ]
