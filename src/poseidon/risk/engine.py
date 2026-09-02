"""Risk engine: the mandatory gate between any decision and any order.

``validate_order`` gathers the live context (fresh quote, volume history,
economic calendar) and runs every rule. There is deliberately no way to
submit an order that bypasses this method — the order manager owns the only
broker reference used for submission and calls the engine first.
"""

from __future__ import annotations

import asyncio
import time
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import structlog

from ..core.clock import MarketClock
from ..core.config import RiskConfig
from ..core.enums import AssetClass
from ..core.errors import CircuitBreakerOpen, DataError, RiskViolation
from ..core.events import EventBus, Topics
from ..core.models import Bar, EconomicEvent, Order, Quote
from ..data.router import DataRouter
from ..portfolio.state import PortfolioState
from .circuit import CircuitBreaker, TradeCooldowns
from .rules import ALL_RULES, RiskContext, RiskRule

log = structlog.get_logger(__name__)


class RiskEngine:
    def __init__(self, config: RiskConfig, portfolio: PortfolioState, router: DataRouter,
                 clock: MarketClock, bus: EventBus, *, rules: list[RiskRule] | None = None,
                 halt_file: Path | None = None) -> None:
        self._config = config
        self._portfolio = portfolio
        self._router = router
        self._clock = clock
        self._bus = bus
        self._rules = rules if rules is not None else list(ALL_RULES)
        self.circuit = CircuitBreaker(
            error_threshold=config.circuit_breaker_error_threshold,
            window_seconds=config.circuit_breaker_window_seconds,
            cooldown_seconds=config.circuit_breaker_cooldown_seconds,
            halt_file=halt_file,
        )
        self.cooldowns = TradeCooldowns(per_symbol_seconds=config.trade_cooldown_seconds)
        # Dedicated sleeves: strategy name -> fraction of equity its
        # positions may occupy (overrides max_position_pct for that
        # strategy only). Maintained by the algorithm workshop.
        self.sleeve_caps: dict[str, float] = {}
        # strategy name -> symbols it signalled this cycle (trusted attribution
        # for sleeve caps; see PositionSizeRule).
        self.sleeve_attribution: dict[str, set[str]] = {}
        self._orders_today = 0
        self._orders_today_date: str = ""
        # In-flight exposure: orders validated+submitted this cycle but not yet
        # reflected in a portfolio sync. Without this, multiple orders in one
        # decision each validate against the same pre-cycle snapshot and can
        # stack past the gross/leverage/options/position/sector caps.
        # order_id -> (client_order_id, SYMBOL, notional, is_option, submitted_at)
        self._pending: dict[str, tuple[str, str, Decimal, bool, datetime]] = {}
        # order_id -> (SYMBOL, notional, is_option, validated_at); staged at
        # validation, promoted to _pending on submit, released on a pre-submit
        # rejection, or pruned after 15 min if neither happens.
        self._validated_notional: dict[str, tuple[str, Decimal, bool, datetime]] = {}
        # Halt-flatten carve-out (§3.4): an unforgeable capability token whose
        # IDENTITY is the capability. Only kernel.halt() opens the window (and
        # closes it in a finally); only order_manager.flatten_all holds the
        # token. A tripped breaker rejects every order that does not present the
        # live token for a leg-free risk-reducing exit within the deadline. Never
        # rides on the Order model, any schema, the DB, or any /api/chat payload.
        self._halt_flatten_token: object | None = None
        self._halt_flatten_deadline = 0.0

    # -- accounting ----------------------------------------------------------


    @property
    def config(self) -> RiskConfig:
        """Read-only view of the validated risk config.

        The order manager needs a couple of policy flags (e.g.
        ``allow_live_crypto``) at submit time; exposing the model here beats
        threading individual flags through, and it is immutable in practice —
        callers must not mutate it.
        """
        return self._config
    def set_cycle_attribution(self, signals: list[object]) -> None:
        """Record which strategy signalled which symbols this cycle, so a
        sleeve cap can only apply to symbols its strategy actually surfaced."""
        attribution: dict[str, set[str]] = {}
        for sig in signals:
            name = getattr(sig, "strategy", None)
            symbol = getattr(sig, "symbol", None)
            if name and symbol:
                attribution.setdefault(str(name), set()).add(str(symbol).upper())
        self.sleeve_attribution = attribution

    def seed_orders_today(self, count: int, date: str) -> None:
        """Rehydrate the daily order counter from persisted history so a
        restart cannot silently reset max_orders_per_day."""
        self._orders_today_date = date
        self._orders_today = count

    def note_order_submitted(self, order: Order) -> None:
        self._roll_daily_counter()
        self._orders_today += 1
        self.cooldowns.record_trade(order.symbol)
        # Reserve this order's validated notional as in-flight exposure until a
        # later sync reflects it. Risk-reducing orders never increase exposure.
        validated = self._validated_notional.pop(order.id, None)
        if validated is not None and not order.side.is_risk_reducing:
            self._pending[order.id] = (
                order.client_order_id, order.symbol.upper(),
                validated[1], validated[2], datetime.now(UTC),
            )

    def release_validated(self, order_id: str) -> None:
        """Drop a staged validation reservation for an order validated but then
        rejected BEFORE submission (capability / duplicate / preflight /
        circuit-halt / post-approval re-check / broker reject). Without this the
        stash keeps counting as in-flight exposure against later orders until the
        15-min prune. An ambiguous submit (possibly live) is the one exception —
        it is promoted to _pending via note_order_submitted instead (F018)."""
        self._validated_notional.pop(order_id, None)

    def _reconcile_pending(self) -> None:
        """Release a reservation once a portfolio sync taken after submission
        shows the order is no longer open at the broker (filled -> now in
        positions; canceled/rejected -> void). Orders still resting in
        portfolio.open_orders keep their reservation."""
        synced_at = self._portfolio.synced_at
        if synced_at is None or not self._pending:
            return
        open_coids = {o.client_order_id for o in self._portfolio.open_orders if o.client_order_id}
        for oid, (coid, _s, _n, _o, submitted_at) in list(self._pending.items()):
            # 10s grace covers an order submitted mid-sync-pass.
            if coid not in open_coids and synced_at > submitted_at + timedelta(seconds=10):
                del self._pending[oid]

    def note_execution_error(self, reason: str) -> None:
        if self.circuit.record_error(reason):
            # publish is fire-and-forget; engine methods stay sync-friendly.
            # The kernel subscribes CIRCUIT_OPENED to the audit log (app.py).
            # publish_nowait holds the task reference (a bare create_task can
            # be garbage-collected before it runs).
            self._bus.publish_nowait(Topics.CIRCUIT_OPENED, {"reason": reason})

    def _roll_daily_counter(self) -> None:
        today = self._clock.now_eastern().date().isoformat()
        if today != self._orders_today_date:
            self._orders_today_date = today
            self._orders_today = 0

    # -- halt-flatten carve-out (§3.4) ------------------------------------------

    def open_halt_flatten_window(self, *, ttl_seconds: float = 300.0) -> object:
        """Mint a fresh capability token and arm the carve-out for ``ttl_seconds``.

        The returned bare ``object()`` cannot be forged (identity IS the
        capability), serialized, persisted, or replayed. The ONLY caller is
        ``kernel.halt()``, which passes it straight to ``flatten_all`` and closes
        the window in a ``finally``. Rewriting the token invalidates any prior one.
        """
        self._halt_flatten_token = object()
        self._halt_flatten_deadline = time.monotonic() + ttl_seconds
        return self._halt_flatten_token

    def close_halt_flatten_window(self) -> None:
        """Disarm the carve-out. ``kernel.halt()`` calls this in a ``finally`` so
        a crashed flatten still leaves the breaker rejecting every order."""
        self._halt_flatten_token = None

    def halt_exit_permitted(self, order: Order, token: object | None) -> bool:
        """True only for the sanctioned halt-flatten exit: the live token, an
        open window before its deadline, and a leg-free risk-reducing order.
        Even a stolen live token admits nothing else."""
        return (token is not None and token is self._halt_flatten_token
                and time.monotonic() < self._halt_flatten_deadline
                and order.side.is_risk_reducing and not order.legs)

    # -- validation -------------------------------------------------------------

    async def validate_order(self, order: Order, *, halt_token: object | None = None,
                             allow_delayed: bool = False) -> Quote:
        """Run every risk rule against live data.

        Returns the fresh quote used, so the caller can reuse it (e.g. for
        limit-price sanity in the ticket). Raises RiskViolation or
        CircuitBreakerOpen on any breach; raises DataError when the live
        context cannot be assembled (in which case the order must not go
        out — no data, no trade).
        """
        # Breaker gate with the halt-flatten carve-out (§3.4). The
        # ``halt_token is not None`` short-circuit means every existing caller
        # (all pass nothing) never consults the window — a tripped breaker still
        # rejects EVERY normal order, including risk-reducing guardian exits.
        if self.circuit.is_open and not (
                halt_token is not None and self.halt_exit_permitted(order, halt_token)):
            raise CircuitBreakerOpen(self.circuit.reason or "open")
        self._roll_daily_counter()
        self._reconcile_pending()

        # Live inputs. Any failure here aborts the order — deliberately no
        # fallbacks to cached or assumed values.
        # allow_delayed is the manual-ticket carve-out: the operator may accept
        # a DELAYED reference quote; STALE always refuses, and automated
        # callers (AI cycle, guardian, approvals) keep the default False.
        quote = await self._router.quote(order.symbol, allow_delayed=allow_delayed)
        order.arrival_price = quote.mid or quote.last  # TCA benchmark price
        bars: list[Bar] = []
        try:
            bars = await self._router.bars(order.symbol, timeframe="1d", limit=30)
        except DataError:
            bars = []  # VolumeRule treats missing history as a violation for buys
        econ: list[EconomicEvent] = []
        if self._config.news_blackout_minutes_before_econ > 0:
            try:
                econ = await self._router.economic_calendar(days_ahead=1)
            except DataError:
                log.warning("economic calendar unavailable; blackout rule will pass empty")
        order_sector, position_sectors = await self._gather_sectors(order)

        # In-flight exposure = submitted-but-unsynced (_pending) PLUS validated-
        # but-not-yet-submitted (_validated_notional). Summing BOTH closes the
        # window where two genuinely concurrent pipelines (a manual dashboard
        # order and a review-cycle order) each validate before either submits and
        # so each sees zero pending. Staging at validation is what makes this
        # correct: the read->rules->stage span below is await-free, so two
        # validators cannot interleave within it. The order's OWN stash is
        # excluded, or approval-mode re-validation would count it against itself.
        # LOAD-BEARING: this await-free mutual-exclusion (and _submit_lock, an
        # asyncio.Lock) both assume the dashboard and review-cycle pipelines share ONE
        # event loop — true today (a single asyncio.run in cli.py; uvicorn started via
        # create_task on that loop). Moving the dashboard to a thread / second loop
        # would silently reopen this window; keep the pipelines co-loop'd.
        pending_gross = Decimal(0)
        pending_options = Decimal(0)
        pending_by_symbol: dict[str, Decimal] = {}
        for _c, sym, n, is_opt, _t in self._pending.values():
            pending_gross += n
            if is_opt:
                pending_options += n
            pending_by_symbol[sym] = pending_by_symbol.get(sym, Decimal(0)) + n
        for oid, (sym, n, is_opt, _t) in self._validated_notional.items():
            if oid == order.id:
                continue
            pending_gross += n
            if is_opt:
                pending_options += n
            pending_by_symbol[sym] = pending_by_symbol.get(sym, Decimal(0)) + n

        ctx = RiskContext(
            order=order,
            quote=quote,
            portfolio=self._portfolio,
            config=self._config,
            clock=self._clock,
            recent_bars=bars,
            upcoming_econ=econ,
            orders_today=self._orders_today,
            cooldown_remaining=self.cooldowns.remaining(order.symbol),
            order_sector=order_sector,
            position_sectors=position_sectors,
            sleeve_caps=dict(self.sleeve_caps),
            sleeve_attribution={k: set(v) for k, v in self.sleeve_attribution.items()},
            pending_gross=pending_gross,
            pending_options=pending_options,
            pending_by_symbol=pending_by_symbol,
        )
        for rule in self._rules:
            try:
                rule.check(ctx)
            except RiskViolation as violation:
                log.warning("risk violation", rule=violation.rule, order=order.symbol,
                            side=order.side, detail=str(violation))
                await self._bus.publish(
                    Topics.RISK_VIOLATION,
                    {"rule": violation.rule, "order_id": order.id, "symbol": order.symbol,
                     "detail": str(violation), "at": datetime.now(UTC).isoformat()},
                )
                raise
        # All rules passed: stage this order's notional (with its symbol) so it
        # counts as in-flight exposure from validation time — the pending sums
        # above already fold in _validated_notional, so a concurrent validator
        # sees it before it submits. note_order_submitted promotes it to
        # _pending; a pre-submit rejection calls release_validated.
        now = datetime.now(UTC)
        # Only risk-INCREASING orders reserve exposure: a risk-reducing exit
        # shrinks the book, so it must not count toward pending_gross (and
        # note_order_submitted never promotes it to _pending either). The
        # oversell concern for concurrent exits is handled separately by the
        # manager's live-state reduce-only backstop (F022), not this reservation.
        # NOTE (latent, unreachable today): this keys on the ORDER-level side, while
        # ctx.notional and reduce_only_breach key on per-LEG sides. A multi-leg order
        # with a risk-reducing order side but SELL_TO_OPEN legs would be sized but not
        # reserved — harmless while nothing populates order.legs (no production path
        # does), but key this on "opens exposure" before enabling a leg-submitting
        # broker plugin.
        if not order.side.is_risk_reducing:
            self._validated_notional[order.id] = (
                order.symbol.upper(), ctx.notional,
                order.asset_class is AssetClass.OPTION, now,
            )
        # Prune stashes validated but never submitted (belt-and-suspenders behind
        # release_validated: e.g. a pipeline that died before it could reject).
        cutoff = now - timedelta(minutes=15)
        for oid in [o for o, (_s, _n, _is, at) in self._validated_notional.items() if at < cutoff]:
            del self._validated_notional[oid]
        return quote

    async def _gather_sectors(self, order: Order) -> tuple[str | None, dict[str, str]]:
        """Sector classifications for the concentration rule. Router results
        are week-cached, so steady-state cost is zero API calls. Only
        gathered for risk-increasing equity orders — the ones the rule
        constrains. Position lookups run concurrently to bound the cold-path
        latency to one round-trip instead of N."""
        if order.side.is_risk_reducing or order.asset_class is not AssetClass.EQUITY:
            return None, {}
        order_sector = await self._router.sector(order.symbol)
        if order_sector is None:
            return None, {}
        equity_positions = [p for p in self._portfolio.positions
                            if p.asset_class is AssetClass.EQUITY]
        sectors = await asyncio.gather(
            *(self._router.sector(p.symbol) for p in equity_positions)
        )
        position_sectors = {
            p.symbol.upper(): s
            for p, s in zip(equity_positions, sectors, strict=True) if s is not None
        }
        return order_sector, position_sectors

    def status(self) -> dict[str, object]:
        self._roll_daily_counter()
        return {
            "circuit_open": self.circuit.is_open,
            "circuit_reason": self.circuit.reason,
            "orders_today": self._orders_today,
            "max_orders_per_day": self._config.max_orders_per_day,
            "day_loss_pct": self._portfolio.day_loss_pct(),
            "week_loss_pct": self._portfolio.week_loss_pct(),
            "drawdown_pct": self._portfolio.drawdown_pct(),
            "limits": {
                "max_daily_loss_pct": self._config.max_daily_loss_pct,
                "max_weekly_loss_pct": self._config.max_weekly_loss_pct,
                "max_drawdown_pct": self._config.max_drawdown_pct,
                "max_position_pct": self._config.max_position_pct,
                "max_leverage": self._config.max_leverage,
            },
        }
