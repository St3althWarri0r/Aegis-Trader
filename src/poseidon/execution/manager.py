"""Order manager: the only path from a decision to a broker.

Responsibilities:
  * translate AI decisions into orders and persist them before submission;
    a crash between persist and the broker acknowledging the submit leaves
    the order in an ambiguous state (APPROVED or ERROR), which
    resume_open_orders() reconciles at startup against the broker's live
    open orders (best-effort match on client_order_id/broker_order_id):
    a match is adopted and polled, a non-match is flagged for operator
    verification rather than silently dropped or blindly resubmitted;
  * enforce operating mode (research: never submit; approval: human gate;
    autonomous: submit within risk limits);
  * run the risk engine on every order, no exceptions;
  * submit with bounded retries on retryable broker errors;
  * poll open orders to terminal state and publish fill/reject events;
  * feed execution errors to the circuit breaker.
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import structlog
from pydantic import ValidationError

from ..analytics.execution import slippage_bps
from ..brokers.base import Broker
from ..core.enums import (
    AssetClass,
    BrokerCapability,
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
    TradingMode,
)
from ..core.errors import (
    BrokerError,
    CircuitBreakerOpen,
    ConfigError,
    DataError,
    DuplicateOrderError,
    RiskViolation,
)
from ..core.events import EventBus, Topics
from ..core.models import Decision, Order, Position, ProposedTrade
from ..risk.engine import RiskEngine
from ..risk.rules import reduce_only_breach
from ..security.audit import AuditLog
from ..storage.db import Database
from .approvals import ApprovalQueue

log = structlog.get_logger(__name__)

_SUBMIT_RETRIES = 3
_POLL_INTERVAL = 5.0
_POLL_INTERVAL_MAX = 300.0  # long-lived GTC polls back off to every 5 min
# A DAY order dies at session end, so 8h covers it. A GTC order can rest for
# days; capping its poller at 8h would stop watching a still-open order and
# silently miss a later fill. Watch GTC orders for days; if the process is up
# longer, resume_open_orders() re-attaches a fresh poller on the next restart.
_POLL_TIMEOUT_DAY = 8 * 60 * 60
_POLL_TIMEOUT_GTC = 5 * 24 * 60 * 60
# A broker cancel can be asynchronous (alpaca's DELETE only queues it): after
# clearing an opposing resting BUY, the exit waits — bounded — for the broker
# to confirm the cancel before it submits, or the SELL can still trip the
# self-trade block the clear exists to avoid. ~3s worst case at the defaults.
_CANCEL_CONFIRM_ATTEMPTS = 6
_CANCEL_CONFIRM_INTERVAL = 0.5


@dataclass(frozen=True)
class HaltCleanupSummary:
    """The immutable outcome of ``cancel_all_open`` (control-hardening spec §3.2).

    ``kernel.halt()`` consumes this to (a) decide which symbols still carry a
    live order and must be EXCLUDED from any opt-in flatten (a resting order
    must never fill against a closing trade), and (b) drive the single loud
    summary notification. All three fields are records, never retried actions:

      * ``canceled`` — order ids the active broker confirmed canceled.
      * ``failed``   — ``{order_id, symbol, error}`` per cancel that raised;
                       attempted exactly once, recorded here, never retried.
      * ``skipped``  — order ids left untouched (cross-broker rows, or a
                       broker-switch abort) — never canceled against the wrong
                       brokerage, surfaced so the operator knows they survived.
    """

    canceled: list[str] = field(default_factory=list)
    failed: list[dict[str, str]] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)


# An order that reached the broker (accepted, resting, or already filled) — the
# statuses ``flatten_all`` counts as a submitted exit, versus a rejection/error.
_FLATTEN_LIVE_STATUSES = frozenset({
    OrderStatus.SUBMITTED, OrderStatus.ACCEPTED,
    OrderStatus.FILLED, OrderStatus.PARTIALLY_FILLED,
})

# The live-at-broker statuses ``cancel_all_open`` sweeps, derived from the single
# source of truth ``OrderStatus.is_open_at_broker`` so the cancel query and the
# enum predicate can never drift (control-hardening spec §3.2).
_OPEN_AT_BROKER_STATUSES: tuple[str, ...] = tuple(
    s.value for s in OrderStatus if s.is_open_at_broker
)


@dataclass(frozen=True)
class FlattenSummary:
    """The immutable outcome of ``flatten_all`` (control-hardening spec §3.2/§3.3).

    ``kernel.halt()`` consumes this for the single loud summary notification
    (flattened k / refused j). Both fields are records, never retried actions:

      * ``submitted`` — order ids for reduce-only exits that reached the broker.
      * ``refused``   — ``{symbol[, order_id], reason}`` per position NOT flattened:
                        a surviving open order on the symbol (never raced), a rule
                        denial (full chain runs — reduce-only never exempted), or a
                        submit-time rejection. Recorded here, audited, never retried.
    """

    submitted: list[str] = field(default_factory=list)
    refused: list[dict[str, str]] = field(default_factory=list)


class OrderManager:
    def __init__(self, broker: Broker, risk: RiskEngine, approvals: ApprovalQueue,
                 db: Database, audit: AuditLog, bus: EventBus, *, mode: TradingMode) -> None:
        self._broker = broker
        self._risk = risk
        self._approvals = approvals
        self._db = db
        self._audit = audit
        self._bus = bus
        self._mode = mode
        self._poll_tasks: set[asyncio.Task[None]] = set()
        # Broker-switch coordination: while _switching is set every new order
        # pipeline is refused, and _drained signals when the in-flight ones
        # have finished — the swap may only happen in that quiet window,
        # otherwise an order decided against one account could be submitted
        # to (or polled against) a different brokerage.
        self._inflight = 0
        self._switching = False
        self._drained = asyncio.Event()
        self._drained.set()
        # Serializes the duplicate-guard → submit critical section. Without it
        # two pipelines (guardian exit + review cycle, or a manual ticket)
        # racing on the same symbol can both pass _guard_duplicate before
        # either reaches the broker, and double-submit a real order.
        self._submit_lock = asyncio.Lock()

    @property
    def mode(self) -> TradingMode:
        return self._mode

    def set_mode(self, mode: TradingMode) -> None:
        self._mode = mode

    @property
    def broker_name(self) -> str:
        return self._broker.name

    def set_broker(self, broker: Broker) -> None:
        """Hot-swap the broker (Account view switch). Only valid inside a
        begin_broker_switch()/end_broker_switch() window with no open orders
        — an order created at one broker must never be submitted to, polled,
        or canceled against another."""
        self._broker = broker

    async def begin_broker_switch(self, timeout: float = 20.0) -> None:
        """Refuse new order pipelines and wait for in-flight ones to drain.
        Raises ConfigError (and lifts the refusal) if they do not finish in
        time — e.g. an order is sitting in the human approval queue."""
        self._switching = True
        try:
            await asyncio.wait_for(self._drained.wait(), timeout)
        except TimeoutError:
            self._switching = False
            raise ConfigError(
                f"{self._inflight} order pipeline(s) still in flight — resolve pending "
                "approvals or in-progress orders, then retry the broker switch"
            ) from None

    def end_broker_switch(self) -> None:
        self._switching = False

    def _pipeline_enter(self) -> None:
        self._inflight += 1
        self._drained.clear()

    def _pipeline_exit(self) -> None:
        self._inflight -= 1
        if self._inflight <= 0:
            self._drained.set()

    async def _refuse_for_switch(self, order: Order) -> Order:
        order.status = OrderStatus.REJECTED_RISK
        order.status_reason = "broker switch in progress — order refused; retry in a moment"
        await self._persist(order)
        return order

    async def open_order_count(self) -> int:
        """Orders that are (or may still go) live at the broker — the guard
        the kernel checks before allowing a broker switch."""
        row = await self._db.fetch_one(
            "SELECT COUNT(*) FROM orders WHERE status IN (?, ?, ?, ?, ?)",
            (OrderStatus.SUBMITTED.value, OrderStatus.ACCEPTED.value,
             OrderStatus.PARTIALLY_FILLED.value, OrderStatus.PENDING_APPROVAL.value,
             OrderStatus.APPROVED.value),
        )
        return int(row[0]) if row else 0

    # -- entry point --------------------------------------------------------------

    async def execute_decision(self, decision: Decision) -> list[Order]:
        """Process every proposed trade in a decision. Returns final orders
        (which may be risk-rejected, human-rejected, or submitted)."""
        results: list[Order] = []
        for trade in decision.trades:
            try:
                order = self._trade_to_order(trade, decision)
            except ValidationError as exc:
                # A malformed trade (e.g. non-positive quantity) must not abort
                # the loop after earlier orders were already submitted.
                log.error("invalid proposed trade skipped", symbol=trade.symbol,
                          quantity=str(trade.quantity), error=str(exc))
                await self._audit.append("system", "order.invalid_trade_skipped",
                                         {"decision_id": decision.id, "symbol": trade.symbol,
                                          "reason": str(exc)})
                continue
            results.append(await self._process_order(order, decision))
        return results

    def _trade_to_order(self, trade: ProposedTrade, decision: Decision) -> Order:
        return Order(
            symbol=trade.symbol.upper(),
            asset_class=trade.asset_class,
            side=trade.side,
            order_type=trade.order_type,
            quantity=trade.quantity,
            limit_price=trade.limit_price,
            stop_price=trade.stop_price,
            time_in_force=trade.time_in_force,
            legs=trade.legs,
            strategy=trade.strategy,
            decision_id=decision.id,
            created_at=datetime.now(UTC),
        )

    async def _process_order(self, order: Order, decision: Decision) -> Order:
        await self._persist(order)

        if self._mode is TradingMode.RESEARCH:
            order.status = OrderStatus.REJECTED_HUMAN
            order.status_reason = "research mode: orders are never submitted"
            await self._persist(order)
            return order
        if self._switching:
            return await self._refuse_for_switch(order)
        self._pipeline_enter()
        try:
            return await self._process_order_gated(order, decision)
        finally:
            self._pipeline_exit()

    async def _process_order_gated(self, order: Order, decision: Decision) -> Order:
        # Risk gate — always, in every mode.
        try:
            await self._risk.validate_order(order)
        except (RiskViolation, CircuitBreakerOpen) as exc:
            order.status = OrderStatus.REJECTED_RISK
            order.status_reason = str(exc)
            await self._persist(order)
            await self._audit.append("risk", "order.rejected",
                                     {"order_id": order.id, "reason": str(exc)})
            await self._bus.publish(Topics.ORDER_REJECTED,
                                    {"order": order.model_dump(mode="json"), "reason": str(exc)})
            return order
        except DataError as exc:
            order.status = OrderStatus.REJECTED_RISK
            order.status_reason = f"required live data unavailable: {exc}"
            await self._persist(order)
            await self._audit.append("risk", "order.rejected",
                                     {"order_id": order.id, "reason": str(exc),
                                      "cause": "data_unavailable"})
            await self._bus.publish(Topics.ORDER_REJECTED,
                                    {"order": order.model_dump(mode="json"), "reason": str(exc)})
            return order

        # Human gate in approval mode.
        if self._mode is TradingMode.APPROVAL:
            order.status = OrderStatus.PENDING_APPROVAL
            await self._persist(order)
            entry = await self._approvals.request(order, decision)
            approved = await self._approvals.wait(entry)
            if not approved:
                order.status = OrderStatus.REJECTED_HUMAN
                order.status_reason = f"not approved ({entry.resolver})"
                # Validated (staged) before the approval gate; a human decline or a
                # TTL expiry must drop that reservation, or it phantom-counts as
                # in-flight exposure and wrongly blocks later orders until the 15-min
                # prune (F021 — mirrors the seven sibling pre-submit reject paths).
                self._risk.release_validated(order.id)
                await self._persist(order)
                await self._audit.append("human", "order.rejected",
                                         {"order_id": order.id, "resolver": entry.resolver})
                await self._bus.publish(Topics.ORDER_REJECTED,
                                        {"order": order.model_dump(mode="json"),
                                         "reason": order.status_reason})
                return order
            await self._audit.append("human", "order.approved", {"order_id": order.id})
            # Re-validate: conditions may have moved while the human decided.
            try:
                await self._risk.validate_order(order)
            except (RiskViolation, CircuitBreakerOpen, DataError) as exc:
                order.status = OrderStatus.REJECTED_RISK
                order.status_reason = f"post-approval re-check failed: {exc}"
                # Staged at the first gate; drop that reservation now it will not
                # submit, or it over-counts exposure against later orders (F021).
                self._risk.release_validated(order.id)
                await self._persist(order)
                # The approval was just audited; the rejection of that same
                # human-approved order must also enter the chain (and reach the
                # dashboard), or the log shows an approved order that vanishes.
                await self._audit.append("risk", "order.rejected",
                                         {"order_id": order.id, "reason": str(exc),
                                          "stage": "post_approval"})
                await self._bus.publish(Topics.ORDER_REJECTED,
                                        {"order": order.model_dump(mode="json"),
                                         "reason": order.status_reason})
                return order

        order.status = OrderStatus.APPROVED
        await self._persist(order)
        return await self._submit(order)

    async def submit_manual(self, order: Order) -> Order:
        """A trade the operator entered on the dashboard. The human IS the
        approver, so the approval queue is skipped — but the risk engine is
        not: manual orders pass every rule, exactly like AI orders, with ONE
        deliberate difference — the reference quote may be DELAYED (the
        operator is watching the market; after-hours free feeds age past the
        live bound). STALE quotes still refuse. Research mode still refuses
        (research means no orders, from anyone)."""
        order.strategy = order.strategy or "manual"
        await self._persist(order)
        if self._mode is TradingMode.RESEARCH:
            order.status = OrderStatus.REJECTED_HUMAN
            order.status_reason = "research mode: orders are never submitted (switch modes to trade)"
            await self._persist(order)
            return order
        if self._switching:
            return await self._refuse_for_switch(order)
        self._pipeline_enter()
        try:
            return await self._submit_manual_gated(order)
        finally:
            self._pipeline_exit()

    async def _submit_manual_gated(self, order: Order) -> Order:
        try:
            quote = await self._risk.validate_order(order, allow_delayed=True)
        except (RiskViolation, CircuitBreakerOpen, DataError) as exc:
            order.status = OrderStatus.REJECTED_RISK
            order.status_reason = str(exc)
            await self._persist(order)
            await self._audit.append("risk", "order.rejected",
                                     {"order_id": order.id, "reason": str(exc), "origin": "manual"})
            await self._bus.publish(Topics.ORDER_REJECTED,
                                    {"order": order.model_dump(mode="json"), "reason": str(exc)})
            return order
        order.status = OrderStatus.APPROVED
        await self._persist(order)
        await self._audit.append("human", "order.manual_submitted", {
            "order_id": order.id, "symbol": order.symbol,
            "side": order.side, "qty": str(order.quantity),
            # The carve-out makes the reference-quote grade variable for manual
            # orders: record it, or post-incident review cannot distinguish a
            # delayed-reference trade from a live one (and TCA misreads market
            # drift since an aged arrival price as execution slippage).
            "quote_freshness": str(quote.freshness),
            "quote_source": quote.source,
            "quote_as_of": quote.as_of.isoformat(),
        })
        return await self._submit(order)

    # -- submission ------------------------------------------------------------------

    async def _submit(self, order: Order, *, halt_token: object | None = None) -> Order:
        # ``halt_token`` is the halt-flatten carve-out (§3.4): ONLY
        # kernel.halt() -> flatten_all presents one (an engine-minted, identity-
        # checked object()). Every other caller — execute_decision, submit_manual
        # — passes nothing, so the pre-submit breaker re-check below rejects them
        # while the breaker is open. The token never rides on the Order model, a
        # schema, the DB, or any /api/chat payload; it cannot be forged or replayed.
        # Pin the broker for this submission AND its lifecycle poller: even
        # if a hot swap lands later, this order stays with the brokerage that
        # actually holds it.
        broker = self._broker
        # Capability gate: never hand a broker an order it cannot handle
        # (e.g. options to the paper simulator, which would book the cost
        # without the contract multiplier).
        reason = self._missing_capability(order, broker)
        if reason is not None:
            order.status = OrderStatus.REJECTED_BROKER
            order.status_reason = reason
            self._risk.release_validated(order.id)  # validated but never submitted (F021)
            await self._persist(order)
            await self._audit.append("system", "order.capability_rejected",
                                     {"order_id": order.id, "reason": reason})
            await self._bus.publish(Topics.ORDER_REJECTED,
                                    {"order": order.model_dump(mode="json"), "reason": reason})
            return order
        # An incoming SELL is always position-closing (the platform never opens
        # shorts), and a same-symbol BUY still resting at the broker is doubly
        # wrong against it: it trips the broker's self-trade block (alpaca 403
        # "potential wash trade detected" — observed live: a stale GTC buy at
        # 9.609 made every LINK/USD exit unfillable), and if it later filled it
        # would re-open the position this exit is closing. Clear them first.
        await self._clear_opposing_orders(order, broker)
        # The duplicate guard and the actual broker submission must be atomic
        # against other concurrent pipelines, or two of them can both clear
        # the guard and double-submit. Held across retries too (retries are
        # rare and this is a single-user platform, so serializing submissions
        # is acceptable and safer than a narrow window).
        async with self._submit_lock:
            # Re-check the emergency HALT / circuit breaker: it may have tripped
            # during the validate->submit window (an operator HALT / filesystem
            # sentinel, or the error-rate breaker opening from a sibling order's
            # failures, or a long approval wait). validate_order checked it, but
            # that was before the approval gate and the guard/preflight
            # round-trips — the HALT must block a real order right up to submit.
            # The ONLY exception is the halt-flatten carve-out (§3.4): a live,
            # engine-minted token for a leg-free risk-reducing exit. The
            # ``halt_token is not None`` short-circuit runs first, so every
            # default-arg caller keeps the unconditional reject (no forged/None
            # token ever consults the window). ReduceOnlyRule and the live
            # _guard_reduce_only backstop below still run — never exempted.
            if self._risk.circuit.is_open and not (
                    halt_token is not None
                    and self._risk.halt_exit_permitted(order, halt_token)):
                order.status = OrderStatus.REJECTED_RISK
                order.status_reason = f"halted before submit: {self._risk.circuit.reason}"
                self._risk.release_validated(order.id)  # (F021)
                await self._persist(order)
                await self._audit.append("risk", "order.rejected",
                                         {"order_id": order.id, "reason": order.status_reason})
                await self._bus.publish(Topics.ORDER_REJECTED,
                                        {"order": order.model_dump(mode="json"),
                                         "reason": order.status_reason})
                return order
            try:
                live_open = await self._guard_duplicate(order)
                await self._guard_reduce_only(order, live_open)
            except DuplicateOrderError as exc:
                # Terminal rejection, not an escape: an escaping exception
                # would leave the row APPROVED forever (blocking broker
                # switches, tripping restart reconciliation) and abort the
                # decision's remaining trades.
                order.status = OrderStatus.REJECTED_RISK
                order.status_reason = str(exc)
                self._risk.release_validated(order.id)  # (F021)
                await self._persist(order)
                await self._audit.append("system", "order.duplicate_rejected",
                                         {"order_id": order.id, "reason": str(exc)})
                await self._bus.publish(Topics.ORDER_REJECTED,
                                        {"order": order.model_dump(mode="json"),
                                         "reason": str(exc)})
                return order
            except RiskViolation as exc:
                # Live-state reduce-only breach caught at submit (F022): a racing
                # exit reduced the true position after this order validated against
                # a staler snapshot. Terminal reject, mirroring the duplicate path.
                order.status = OrderStatus.REJECTED_RISK
                order.status_reason = str(exc)
                self._risk.release_validated(order.id)
                await self._persist(order)
                await self._audit.append("risk", "order.reduce_only_rejected",
                                         {"order_id": order.id, "reason": str(exc)})
                await self._bus.publish(Topics.ORDER_REJECTED,
                                        {"order": order.model_dump(mode="json"),
                                         "reason": str(exc)})
                return order
            # Broker-side preflight (where supported): a definitive broker
            # rejection is cheaper and cleaner caught here than as a failed
            # submission. A None result (unsupported/unavailable) changes nothing.
            preflight_reason = await broker.preflight(order)
            if preflight_reason is not None:
                order.status = OrderStatus.REJECTED_BROKER
                order.status_reason = preflight_reason
                self._risk.release_validated(order.id)  # (F021)
                await self._persist(order)
                await self._audit.append("system", "order.preflight_rejected",
                                         {"order_id": order.id, "reason": preflight_reason})
                await self._bus.publish(Topics.ORDER_REJECTED,
                                        {"order": order.model_dump(mode="json"),
                                         "reason": preflight_reason})
                return order
            last_error: Exception | None = None
            for attempt in range(1, _SUBMIT_RETRIES + 1):
                try:
                    order = await broker.submit_order(order)
                    break
                except BrokerError as exc:
                    last_error = exc
                    self._risk.note_execution_error(str(exc))
                    ambiguous = getattr(exc, "ambiguous", False)
                    if ambiguous or not exc.retryable or attempt == _SUBMIT_RETRIES:
                        if ambiguous:
                            # Outcome unknown: do NOT resubmit (could double-fill).
                            # Mark ERROR so startup reconciliation checks the broker.
                            order.status = OrderStatus.ERROR
                            order.status_reason = (
                                f"submit outcome unknown ({exc}); not resubmitted — will be "
                                "reconciled against the broker, or verify at the brokerage"
                            )
                            # Possibly live and consuming buying power: reserve its
                            # validated notional as in-flight exposure (promote
                            # _validated_notional -> _pending) so the next opening
                            # trade in this decision cannot stack past the caps as if
                            # it did not exist; _reconcile_pending releases it once a
                            # sync proves it gone (F018).
                            self._risk.note_order_submitted(order)
                        else:
                            order.status = OrderStatus.ERROR if exc.retryable else OrderStatus.REJECTED_BROKER
                            order.status_reason = str(exc)
                            # Definitive non-submit (broker rejected, or retries
                            # exhausted on a retryable error = never reached the
                            # matching engine per brokers/base): drop the stash so it
                            # does not over-count exposure against later orders (F021).
                            self._risk.release_validated(order.id)
                        await self._persist(order)
                        await self._audit.append("system", "order.submit_failed",
                                                 {"order_id": order.id, "error": str(exc),
                                                  "ambiguous": ambiguous})
                        await self._bus.publish(Topics.ORDER_REJECTED,
                                                {"order": order.model_dump(mode="json"),
                                                 "reason": order.status_reason})
                        return order
                    await asyncio.sleep(2 ** attempt)
            else:  # pragma: no cover — loop always breaks or returns
                raise AssertionError(str(last_error))

        self._risk.note_order_submitted(order)
        await self._persist(order)
        await self._audit.append("system", "order.submitted", {
            "order_id": order.id, "broker": order.broker,
            "broker_order_id": order.broker_order_id,
            "symbol": order.symbol, "side": order.side, "qty": str(order.quantity),
        })
        await self._bus.publish(Topics.ORDER_UPDATED, {"order": order.model_dump(mode="json")})
        self._spawn_poller(order, broker)
        return order

    async def _guard_duplicate(self, order: Order) -> list[Order]:
        row = await self._db.fetch_one(
            "SELECT status FROM orders WHERE client_order_id = ? AND id != ?",
            (order.client_order_id, order.id),
        )
        if row is not None:
            raise DuplicateOrderError(f"client_order_id {order.client_order_id} already used")
        # Same-cycle guard: identical open order (symbol+side+qty) at the broker.
        # Return the live open orders so the reduce-only backstop reuses this one
        # broker round-trip (a single consistent snapshot for both checks).
        open_orders = await self._safe_open_orders()
        for open_order in open_orders:
            if (open_order.symbol == order.symbol and open_order.side == order.side
                    and open_order.quantity == order.quantity):
                raise DuplicateOrderError(
                    f"an identical open order for {order.symbol} already exists at the broker"
                )
        return open_orders

    async def _guard_reduce_only(self, order: Order, open_orders: list[Order]) -> None:
        """Live-state reduce-only backstop, run under ``_submit_lock``. The engine's
        ``ReduceOnlyRule`` validates against the synced portfolio snapshot, which
        can be up to 120s stale, so two exits that race between syncs can both pass
        it and oversell a long into a short (F022). Because ``_submit_lock``
        serializes submissions, by the time a second exit reaches here the first is
        already broker-observable — filled (reflected in ``positions()``), resting
        (in ``open_orders``), or rejected — so re-checking the invariant against
        LIVE broker state closes the window. Held comes from a live
        ``positions()`` fetch, NOT the synced snapshot, because a synchronous
        market-exit fill is gone from ``open_orders`` yet reflected in
        ``positions()``.

        Residuals (documented, not fully closed): (1) on a real broker whose feeds
        lag, this reduces to broker eventual-consistency latency by two paths —
        (1a) ``positions()`` lagging a just-filled MARKET exit still shows the
        pre-exit quantity, and (1b) ``open_orders()`` lagging a just-submitted
        RESTING exit under-counts the pending close — either shrinks the oversell
        window to propagation latency rather than eliminating it (airtight only for
        the synchronous PaperBroker). (2) on a ``positions()`` error it fails CLOSED
        (rejects the exit), which during a broker position-feed outage is in tension
        with never-trap-an-exit; the alternative (fall back to the synced held)
        would reopen the window during that outage."""
        if not (order.side.is_risk_reducing
                or any(leg.side.is_risk_reducing for leg in order.legs)):
            return  # opening order: reduce-only does not apply; skip the positions() fetch
        try:
            positions = await self._broker.positions()
        except Exception as exc:  # noqa: BLE001 — this backstop IS the safety net; it
            # must fail CLOSED (reject the close) on ANY positions() failure, never let
            # an unhandled non-BrokerError escape up the submit path and disarm the exit.
            raise RiskViolation(
                "reduce_only", f"cannot verify live position to bound this close: {exc}"
            ) from exc
        held = {p.symbol.upper(): p.quantity for p in positions}
        msg = reduce_only_breach(order, lambda s: held.get(s.upper(), Decimal(0)), open_orders)
        if msg is not None:
            raise RiskViolation("reduce_only", f"live reduce-only breach at submit: {msg}")

    async def _safe_open_orders(self) -> list[Order]:
        try:
            return await self._broker.open_orders()
        except BrokerError as exc:
            # Can't verify → don't trade. Duplicate prevention must not be skipped.
            raise DuplicateOrderError(f"cannot verify open orders at broker: {exc}") from exc

    def _missing_capability(self, order: Order, broker: Broker) -> str | None:
        caps = broker.capabilities()
        if order.asset_class is AssetClass.OPTION and BrokerCapability.OPTIONS not in caps:
            return f"broker '{broker.name}' does not support options orders"
        if order.asset_class is AssetClass.CRYPTO and BrokerCapability.CRYPTO not in caps:
            return f"broker '{broker.name}' does not support crypto orders"
        if (order.asset_class is AssetClass.CRYPTO and not broker.is_paper
                and not self._risk.config.allow_live_crypto):
            # Not a capability question — a deliberate real-money gate. Both live
            # brokers advertise CRYPTO, so without this the documented
            # "crypto is paper-only" was a claim with nothing behind it.
            return (f"crypto on the LIVE broker '{broker.name}' is disabled — set "
                    "risk.allow_live_crypto: true to trade crypto with real money")
        if order.quantity % 1 != 0 and BrokerCapability.FRACTIONAL_SHARES not in caps:
            return f"broker '{broker.name}' does not support fractional quantities"
        if order.extended_hours and BrokerCapability.EXTENDED_HOURS not in caps:
            return f"broker '{broker.name}' does not support extended-hours orders"
        return None

    # -- lifecycle polling ---------------------------------------------------------------

    def _spawn_poller(self, order: Order, broker: Broker) -> None:
        # The poller is bound to the broker that holds the order — a later
        # hot swap must not redirect status polls to a different brokerage.
        task = asyncio.create_task(self._poll_to_terminal(order, broker),
                                   name=f"order-poll-{order.id[:8]}")
        self._poll_tasks.add(task)
        task.add_done_callback(self._poll_tasks.discard)

    async def _poll_to_terminal(self, order: Order, broker: Broker) -> None:
        is_gtc = order.time_in_force is TimeInForce.GTC
        timeout = _POLL_TIMEOUT_GTC if is_gtc else _POLL_TIMEOUT_DAY
        deadline = asyncio.get_running_loop().time() + timeout
        interval = _POLL_INTERVAL
        while asyncio.get_running_loop().time() < deadline:
            await asyncio.sleep(interval)
            if is_gtc:  # a resting order rarely fills in the first seconds; ease off
                interval = min(interval * 1.5, _POLL_INTERVAL_MAX)
            try:
                order = await broker.order_status(order)
            except BrokerError as exc:
                log.warning("order status poll failed", order_id=order.id, error=str(exc))
                continue
            if (order.status is OrderStatus.FILLED and order.slippage_bps is None
                    and order.arrival_price is not None and order.avg_fill_price is not None):
                order.slippage_bps = slippage_bps(order.side, order.arrival_price,
                                                  order.avg_fill_price)
            await self._persist(order)
            if order.status.is_terminal:
                # A partial fill that ends CANCELED/EXPIRED is still a fill: a
                # real position exists, so the guardian must arm its exit plan
                # (it already arms with filled_quantity) and the operator must
                # hear about the fill.
                filled_any = order.status is OrderStatus.FILLED or order.filled_quantity > 0
                topic = Topics.ORDER_FILLED if filled_any else Topics.ORDER_UPDATED
                await self._audit.append("system", f"order.{order.status.value}", {
                    "order_id": order.id,
                    "filled_qty": str(order.filled_quantity),
                    "avg_price": str(order.avg_fill_price) if order.avg_fill_price else None,
                    "slippage_bps": order.slippage_bps,
                })
                await self._bus.publish(topic, {"order": order.model_dump(mode="json")})
                return
        log.warning("order poll timed out; leaving order open", order_id=order.id)

    async def stop(self) -> None:
        """Cancel outstanding order-status pollers at shutdown so they stop
        calling the broker and writing to the DB before those are closed.
        Safe: any order left non-terminal gets a fresh poller from
        resume_open_orders() on the next boot."""
        tasks = list(self._poll_tasks)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    async def resume_open_orders(self) -> int:
        """Crash recovery, in three phases.

        0. Orders still PENDING_APPROVAL are expired: the approval queue is
           in-memory, so nothing can resolve them after a restart.
        1. Orders already known open at the broker (SUBMITTED/ACCEPTED/
           PARTIALLY_FILLED) get a fresh poller.
        2. Orders left in an ambiguous state (APPROVED, persisted just before
           submit; ERROR, a submit whose transport outcome is unknown) are
           reconciled against the broker's live open orders so an order that
           actually reached the broker before the crash is adopted and polled
           (its fill then arms the guardian, records slippage, and audits),
           instead of becoming an untracked live order.
        """
        # Phase 0: orders that were awaiting human approval when the process
        # stopped. The approval queue is purely in-memory, so after a restart
        # nothing can ever resolve these rows — and they were never submitted
        # to the broker. Expire them (a stale approval must never execute) so
        # they don't count as "open" forever and block broker switches.
        stale = await self._db.fetch_all(
            "SELECT payload FROM orders WHERE status = ?",
            (OrderStatus.PENDING_APPROVAL.value,),
        )
        for (payload,) in stale:
            order = Order.model_validate(json.loads(payload))
            order.status = OrderStatus.REJECTED_HUMAN
            order.status_reason = "approval request lost in restart — never submitted"
            await self._persist(order)
            await self._audit.append("system", "order.approval_lost",
                                     {"order_id": order.id, "symbol": order.symbol})
            log.warning("expired stale pending-approval order from before restart",
                        order_id=order.id, symbol=order.symbol)
        rows = await self._db.fetch_all(
            "SELECT payload FROM orders WHERE status IN (?, ?, ?)",
            (OrderStatus.SUBMITTED.value, OrderStatus.ACCEPTED.value,
             OrderStatus.PARTIALLY_FILLED.value),
        )
        count = 0
        for (payload,) in rows:
            order = Order.model_validate(json.loads(payload))
            # Never poll an order against a different broker than the one it
            # was submitted to (e.g. the operator switched brokers between
            # runs): the ids mean nothing there and a cancel could misfire.
            if order.broker and order.broker != self._broker.name:
                order.status = OrderStatus.ERROR
                order.status_reason = (
                    f"order was open at '{order.broker}' but the active broker is now "
                    f"'{self._broker.name}' — verify it at the brokerage directly"
                )
                await self._persist(order)
                log.warning("open order orphaned by broker switch",
                            order_id=order.id, was=order.broker, now=self._broker.name)
                continue
            self._spawn_poller(order, self._broker)
            count += 1

        count += await self._reconcile_ambiguous_orders()
        if count:
            log.info("resumed polling for open orders", count=count)
        return count

    async def _reconcile_ambiguous_orders(self) -> int:
        """Match APPROVED/ERROR orders (whose submit outcome is unknown after a
        crash) against the broker's live open orders. A live match is adopted
        and polled; a non-match is left flagged for manual verification —
        never assumed filled, never blindly resubmitted."""
        rows = await self._db.fetch_all(
            "SELECT payload FROM orders WHERE status IN (?, ?)",
            (OrderStatus.APPROVED.value, OrderStatus.ERROR.value),
        )
        pending = [Order.model_validate(json.loads(p)) for (p,) in rows]
        mine = [o for o in pending if not o.broker or o.broker == self._broker.name]
        if not mine:
            return 0
        try:
            live = await self._broker.open_orders()
        except BrokerError as exc:
            # Can't verify: leave every ambiguous order flagged so the operator
            # checks the brokerage rather than trusting a possibly-live order.
            for order in mine:
                order.status = OrderStatus.ERROR
                order.status_reason = (
                    f"submit state unknown after restart and broker query failed ({exc}) "
                    "— verify this order at the brokerage directly"
                )
                await self._persist(order)
            return 0
        by_coid = {o.client_order_id: o for o in live if o.client_order_id}
        by_boid = {o.broker_order_id: o for o in live if o.broker_order_id}
        count = 0
        for order in mine:
            match = by_coid.get(order.client_order_id)
            if match is None and order.broker_order_id:
                match = by_boid.get(order.broker_order_id)
            if match is not None:
                # Adopt the broker's view but keep our internal id/decision
                # linkage so the guardian and attribution still resolve.
                match.id = order.id
                match.decision_id = order.decision_id or match.decision_id
                match.strategy = order.strategy or match.strategy
                await self._persist(match)
                self._spawn_poller(match, self._broker)
                count += 1
                log.warning("reconciled ambiguous order: found live at broker",
                            order_id=order.id, broker_order_id=match.broker_order_id)
            else:
                order.status = OrderStatus.ERROR
                order.status_reason = (
                    "not found among the broker's open orders after restart — it was "
                    "never submitted or is already terminal; verify at the brokerage"
                )
                await self._persist(order)
                log.warning("ambiguous order not live at broker; flagged", order_id=order.id)
        return count

    async def _clear_opposing_orders(self, order: Order, broker: Broker) -> None:
        """Cancel same-symbol BUY orders still resting at the broker before this
        SELL is submitted.

        Scope is deliberately narrow — plain SELLs only (options flows and
        multi-leg orders are untouched), and BUYs never cancel anything, so a
        guardian's resting protective take-profit SELL can never be cleared by
        an incoming entry. Mirrors ``cancel_all_open``'s per-order contract:
        cancel exactly once, any failure is audited and skipped (the exit then
        proceeds and may still be rejected broker-side — honest, not wedged),
        and cross-broker rows are never touched.

        Two views of the book are merged. The orders table finds the BUYs this
        platform placed; the broker's LIVE open orders find the rest — a buy
        parked from the brokerage's own UI, or a local row whose status drifted
        — because the broker's self-trade block does not care who placed it.
        A live-only order is canceled and audited but never persisted as a
        synthetic local row. Then each cancel is CONFIRMED (bounded polling)
        because a broker may only queue it: submitting the SELL before the
        buy is actually gone reproduces the very 403 this exists to prevent.
        An unconfirmed cancel is audited and the exit still proceeds."""
        if order.side is not OrderSide.SELL or order.legs:
            return
        symbol = order.symbol.upper()
        placeholders = ", ".join("?" * len(_OPEN_AT_BROKER_STATUSES))
        rows = await self._db.fetch_all(
            f"SELECT payload FROM orders WHERE status IN ({placeholders})",
            _OPEN_AT_BROKER_STATUSES,
        )
        # (order, known_locally) — a local row is persisted after the cancel,
        # a live-only order is not (we hold no row for it).
        targets: list[tuple[Order, bool]] = []
        seen: set[str] = set()
        for (payload,) in rows:
            resting = Order.model_validate(json.loads(payload))
            if (resting.id == order.id
                    or resting.symbol.upper() != symbol
                    or resting.side is not OrderSide.BUY
                    or (resting.broker and resting.broker != broker.name)):
                continue
            targets.append((resting, True))
            seen.update(k for k in (resting.client_order_id, resting.broker_order_id) if k)
        try:
            live = await broker.open_orders()
        except Exception as exc:  # noqa: BLE001 — the live view is additive; a failed
            # read must not stall the exit (the duplicate guard under _submit_lock
            # fails closed on its own read a moment later).
            log.warning("could not read the live book to clear opposing orders",
                        symbol=symbol, error=str(exc))
            live = []
        for resting in live:
            if (resting.symbol.upper() != symbol or resting.side is not OrderSide.BUY
                    or not resting.status.is_open_at_broker):
                continue
            keys = [k for k in (resting.client_order_id, resting.broker_order_id) if k]
            if any(k in seen for k in keys):
                continue  # the DB view already holds this order
            seen.update(keys)
            targets.append((resting, False))
        for resting, known in targets:
            try:
                canceled = await broker.cancel_order(resting)
            except Exception as exc:  # noqa: BLE001 — recorded, never retried:
                # one wedged cancel must not stall the exit path.
                await self._audit.append("system", "exit.opposing_cancel_failed",
                                         {"order_id": resting.id, "symbol": resting.symbol,
                                          "for_order": order.id, "error": str(exc)})
                continue
            canceled = await self._confirm_cancel(canceled, broker)
            if known:
                await self._persist(canceled)
            record = {"order_id": resting.id, "symbol": resting.symbol,
                      "for_order": order.id, "known": known,
                      "broker_order_id": canceled.broker_order_id}
            if canceled.status.is_terminal:
                await self._audit.append("system", "exit.opposing_order_canceled", record)
            else:
                await self._audit.append("system", "exit.opposing_cancel_unconfirmed",
                                         {**record, "status": canceled.status.value})

    @staticmethod
    async def _confirm_cancel(order: Order, broker: Broker) -> Order:
        """Poll a queued cancel to its terminal state, bounded by
        ``_CANCEL_CONFIRM_ATTEMPTS`` x ``_CANCEL_CONFIRM_INTERVAL``. Returns the
        latest broker view either way; a status-poll failure ends the wait."""
        for _ in range(_CANCEL_CONFIRM_ATTEMPTS):
            if order.status.is_terminal:
                break
            await asyncio.sleep(_CANCEL_CONFIRM_INTERVAL)
            try:
                order = await broker.order_status(order)
            except BrokerError as exc:
                log.warning("cancel confirmation poll failed", order_id=order.id,
                            error=str(exc))
                break
        return order

    async def cancel_all_open(self, *, reason: str) -> HaltCleanupSummary:
        """Cancel every order the broker may still hold live — the first cleanup
        phase of ``kernel.halt()`` (control-hardening spec §3.2). Deterministic,
        no LLM, no risk-rule involvement: it only cancels resting orders so none
        can fill mid-halt.

        Each live order (``SUBMITTED``/``ACCEPTED``/``PARTIALLY_FILLED``) is
        canceled **exactly once**: success → persist + audit
        (``human``/``halt.order_canceled``); any exception → audit
        (``system``/``halt.cancel_failed``) + append to ``failed`` and CONTINUE
        the loop — never retried, so one wedged order cannot stall the halt or
        hammer the broker. Cross-broker rows are skipped and recorded (mirroring
        ``cancel()``'s guard — never cancel against the wrong brokerage). A
        ``PARTIALLY_FILLED`` cancel kills only the resting remainder; the filled
        portion is a live position handled by flatten sizing (spec §3.3).

        If a broker swap is in flight the book's broker is indeterminate, so the
        whole pass aborts (recorded) rather than risk a cross-broker cancel."""
        summary = HaltCleanupSummary()
        if self._switching:
            await self._audit.append("system", "halt.cleanup_failed",
                                     {"phase": "cancel_all_open",
                                      "error": "broker switch in progress — cleanup aborted"})
            return summary
        placeholders = ", ".join("?" * len(_OPEN_AT_BROKER_STATUSES))
        rows = await self._db.fetch_all(
            f"SELECT payload FROM orders WHERE status IN ({placeholders})",
            _OPEN_AT_BROKER_STATUSES,
        )
        for (payload,) in rows:
            order = Order.model_validate(json.loads(payload))
            # Never cancel an order that belongs to another brokerage: its ids
            # mean nothing here and a cancel could misfire (mirrors cancel()).
            if order.broker and order.broker != self._broker.name:
                summary.skipped.append(order.id)
                continue
            try:
                canceled = await self._broker.cancel_order(order)
            except Exception as exc:  # noqa: BLE001 — ANY failure is recorded, never
                # retried: a retry loop could double-cancel, hammer a struggling
                # broker, or wedge the halt. The halt latch already stands.
                summary.failed.append({"order_id": order.id, "symbol": order.symbol,
                                       "error": str(exc)})
                await self._audit.append("system", "halt.cancel_failed",
                                         {"order_id": order.id, "symbol": order.symbol,
                                          "error": str(exc)})
                continue
            await self._persist(canceled)
            summary.canceled.append(order.id)
            await self._audit.append("human", "halt.order_canceled",
                                     {"order_id": order.id, "symbol": order.symbol,
                                      "reason": reason})
        return summary

    def _build_flatten_exit(self, position: Position) -> Order:
        """One reduce-only MARKET DAY exit sized to the whole live position
        (spec §3.3): a long equity/ETF/crypto SELLs; a short BUYs_TO_CLOSE; a long
        option SELLs_TO_CLOSE, a short option BUYs_TO_CLOSE. Always leg-free and
        risk-reducing — the exact shape ``halt_exit_permitted`` admits — and never
        multi-leg. ``quantity`` is ``abs`` (a short's live quantity is negative)."""
        is_long = position.quantity > 0
        if position.asset_class is AssetClass.OPTION:
            side = OrderSide.SELL_TO_CLOSE if is_long else OrderSide.BUY_TO_CLOSE
        else:
            side = OrderSide.SELL if is_long else OrderSide.BUY_TO_CLOSE
        return Order(
            symbol=position.symbol.upper(),
            asset_class=position.asset_class,
            side=side,
            order_type=OrderType.MARKET,
            quantity=abs(position.quantity),
            time_in_force=TimeInForce.DAY,
            strategy="halt_flatten",
            created_at=datetime.now(UTC),
        )

    async def flatten_all(self, token: object, *, reason: str) -> FlattenSummary:
        """Opt-in second halt-cleanup phase (control-hardening spec §3.2/§3.3):
        close every live position with a reduce-only MARKET exit, through the FULL
        risk chain. ``token`` is the engine-minted, identity-checked halt-flatten
        capability — the ONLY thing that carries a leg-free reduce-only exit past
        the tripped breaker (``validate_order``'s gate + ``_submit``'s pre-submit
        re-check). No rule is exempted by it: ``ReduceOnlyRule`` (in
        ``validate_order``) and the live ``_guard_reduce_only`` submit backstop
        (F022) both still run, so an oversized or otherwise-denied exit is rejected,
        not forced. The approval queue is NEVER consulted — the operator's halt IS
        the human consent (submissions audit as ``human``/``halt.flatten_submitted``).

        Ordering guarantee: a position whose symbol still carries an open order at
        the broker (a cancel that failed / is pending / is cross-broker) is EXCLUDED
        and recorded — a resting order must never fill against a closing trade. If
        the live book/positions cannot be read, nothing is flattened (cannot verify
        → do not trade). Every denial is recorded and audited; nothing is retried.
        """
        summary = FlattenSummary()
        if self._mode is TradingMode.RESEARCH:
            # Research means no orders, from anyone — refuse before any broker read.
            await self._audit.append("system", "halt.flatten_refused",
                                     {"reason": "research mode: orders are never submitted"})
            return summary
        try:
            positions = await self._broker.positions()
            open_orders = await self._broker.open_orders()
        except Exception as exc:  # noqa: BLE001 — cannot verify the live book/positions:
            # a resting order might survive or a position size be unknown, so do NOT
            # trade blind. Recorded; kernel.halt() raises the critical notification.
            await self._audit.append("system", "halt.flatten_refused",
                                     {"reason": f"cannot verify live book to flatten safely: {exc}"})
            return summary
        surviving = {o.symbol.upper() for o in open_orders}
        for position in positions:
            if position.quantity == 0:
                continue
            symbol = position.symbol.upper()
            if symbol in surviving:
                # A resting order on this symbol survived the cancel pass — excluding
                # it is the guarantee that a resting order cannot fill against a
                # closing trade. Never flattened, always recorded.
                summary.refused.append({"symbol": symbol, "reason": "open_order_survives"})
                await self._audit.append("system", "halt.flatten_refused",
                                         {"symbol": symbol, "reason": "open_order_survives"})
                continue
            order = self._build_flatten_exit(position)
            await self._persist(order)
            try:
                await self._risk.validate_order(order, halt_token=token)
            except (RiskViolation, CircuitBreakerOpen, DataError) as exc:
                # A rule denied the exit (reduce-only, slippage, session, daily
                # budget, …). Recorded + audited, NEVER retried — a best-effort
                # flatten leaves loud partials, it never forces an order past a rule.
                order.status = OrderStatus.REJECTED_RISK
                order.status_reason = str(exc)
                await self._persist(order)
                summary.refused.append({"order_id": order.id, "symbol": symbol,
                                        "reason": str(exc)})
                await self._audit.append("risk", "halt.flatten_refused",
                                         {"order_id": order.id, "symbol": symbol,
                                          "reason": str(exc)})
                continue
            order.status = OrderStatus.APPROVED
            await self._persist(order)
            # The token rides into _submit's pre-submit breaker re-check AND the
            # live _guard_reduce_only backstop still runs there — the full chain.
            result = await self._submit(order, halt_token=token)
            if result.status in _FLATTEN_LIVE_STATUSES:
                summary.submitted.append(result.id)
                await self._audit.append("human", "halt.flatten_submitted",
                                         {"order_id": result.id, "symbol": symbol,
                                          "side": result.side, "qty": str(result.quantity),
                                          "reason": reason})
            else:
                # _submit already persisted + audited the rejection reason; record
                # it as a flatten refusal too so the halt summary is complete.
                summary.refused.append({"order_id": result.id, "symbol": symbol,
                                        "reason": result.status_reason or "rejected at submit"})
                await self._audit.append("risk", "halt.flatten_refused",
                                         {"order_id": result.id, "symbol": symbol,
                                          "reason": result.status_reason or "rejected at submit"})
        return summary

    async def cancel(self, order_id: str) -> Order:
        row = await self._db.fetch_one("SELECT payload FROM orders WHERE id = ?", (order_id,))
        if row is None:
            raise KeyError(f"unknown order {order_id}")
        order = Order.model_validate(json.loads(row[0]))
        if order.broker and order.broker != self._broker.name:
            raise ConfigError(
                f"order {order_id[:8]} belongs to '{order.broker}' but the active broker is "
                f"'{self._broker.name}' — cancel it at that brokerage directly"
            )
        if self._switching:
            raise ConfigError("broker switch in progress — retry the cancel in a moment")
        order = await self._broker.cancel_order(order)
        await self._persist(order)
        await self._audit.append("human", "order.canceled", {"order_id": order.id})
        await self._bus.publish(Topics.ORDER_UPDATED, {"order": order.model_dump(mode="json")})
        return order

    # -- persistence -----------------------------------------------------------------------

    async def _persist(self, order: Order) -> None:
        order.updated_at = datetime.now(UTC)
        if order.created_at is None:
            order.created_at = order.updated_at
        payload = json.dumps(order.model_dump(mode="json"))
        # account_scope is stamped at creation and never updated (like broker),
        # so a poller persisting after a hot swap can't re-scope an order.
        await self._db.execute(
            "INSERT INTO orders (id, client_order_id, broker, broker_order_id, account_scope, "
            "payload, status, decision_id, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(id) DO UPDATE SET payload=excluded.payload, status=excluded.status, "
            "broker_order_id=excluded.broker_order_id, updated_at=excluded.updated_at",
            (order.id, order.client_order_id, order.broker, order.broker_order_id,
             self._broker.account_scope, payload,
             order.status.value, order.decision_id,
             order.created_at.isoformat(), order.updated_at.isoformat()),
        )

    async def recent_orders(self, limit: int = 50) -> list[dict[str, Any]]:
        rows = await self._db.fetch_all(
            "SELECT payload FROM orders ORDER BY created_at DESC LIMIT ?", (limit,)
        )
        return [json.loads(r[0]) for r in rows]

    async def orders_today_count(self, since_iso: str | None = None) -> int:
        """Count orders that consumed the daily budget since ``since_iso``
        (the day boundary; the caller supplies the Eastern day-start in UTC
        so this aligns with the risk engine's Eastern-midnight counter reset).
        """
        since = since_iso or datetime.now(UTC).date().isoformat()
        row = await self._db.fetch_one(
            "SELECT COUNT(*) FROM orders WHERE created_at >= ? AND status NOT IN (?, ?, ?)",
            (since, OrderStatus.REJECTED_RISK.value, OrderStatus.REJECTED_HUMAN.value,
             OrderStatus.PROPOSED.value),
        )
        return int(row[0]) if row else 0
