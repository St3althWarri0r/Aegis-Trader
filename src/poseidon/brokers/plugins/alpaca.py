"""Alpaca broker plugin (official Trading API, https://docs.alpaca.markets).

Credentials (vault JSON): {"key_id": "...", "secret_key": "..."}.
``paper: true`` targets https://paper-api.alpaca.markets.

Supports equities (incl. fractional), options (level permitting), extended
hours, client order IDs (idempotency), and streaming account activity via
polling (websocket streaming is handled by the sync service's poll cadence).
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from ...core.enums import (
    AssetClass,
    BrokerCapability,
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
)
from ...core.errors import BrokerAuthError, BrokerError
from ...core.models import AccountSnapshot, Fill, Order, Position
from ...core.symbols import canonical_crypto_pair
from ..base import Broker

_LIVE = "https://api.alpaca.markets"
_PAPER = "https://paper-api.alpaca.markets"

_STATUS_MAP = {
    "new": OrderStatus.ACCEPTED,
    "accepted": OrderStatus.ACCEPTED,
    "pending_new": OrderStatus.SUBMITTED,
    "accepted_for_bidding": OrderStatus.ACCEPTED,
    "partially_filled": OrderStatus.PARTIALLY_FILLED,
    "filled": OrderStatus.FILLED,
    "done_for_day": OrderStatus.EXPIRED,
    "canceled": OrderStatus.CANCELED,
    "expired": OrderStatus.EXPIRED,
    "replaced": OrderStatus.CANCELED,
    "pending_cancel": OrderStatus.ACCEPTED,
    "pending_replace": OrderStatus.ACCEPTED,
    "rejected": OrderStatus.REJECTED_BROKER,
    "suspended": OrderStatus.REJECTED_BROKER,
    "stopped": OrderStatus.ACCEPTED,
    "held": OrderStatus.ACCEPTED,
}

_SIDE_MAP = {
    OrderSide.BUY: "buy", OrderSide.SELL: "sell",
    OrderSide.BUY_TO_OPEN: "buy", OrderSide.BUY_TO_CLOSE: "buy",
    OrderSide.SELL_TO_OPEN: "sell", OrderSide.SELL_TO_CLOSE: "sell",
}

# Alpaca crypto accepts only these time-in-force values; the equity TIFs
# (day/opg/cls) are rejected with HTTP 422 42210000 "invalid crypto
# time_in_force". A valid crypto TIF is preserved; the rest are remapped to gtc.
_CRYPTO_TIF = frozenset({TimeInForce.GTC, TimeInForce.IOC, TimeInForce.FOK})

# Alpaca refuses any single crypto order above this notional with HTTP 403
# code 40310000 "order notional ... exceeds max notional per order 200000"
# (observed live 2026-07-20). A platform policy, not an account setting. No
# equity-side cap is hardcoded — none has been verified.
_CRYPTO_MAX_ORDER_NOTIONAL = Decimal("200000")


def _parse_ts(value: str | None) -> datetime:
    if not value:
        return datetime.now(UTC)
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


class AlpacaBroker(Broker):
    name = "alpaca"
    display_name = "Alpaca"

    def __init__(self, *, credentials: dict[str, str], paper: bool = True,
                 timeout: float = 15.0, options: dict[str, Any] | None = None) -> None:
        super().__init__(credentials=credentials, paper=paper, timeout=timeout, options=options)
        try:
            self._headers = {
                "APCA-API-KEY-ID": credentials["key_id"],
                "APCA-API-SECRET-KEY": credentials["secret_key"],
            }
        except KeyError as exc:
            raise BrokerAuthError(self.name, f"credential missing field {exc}") from exc
        self._base = _PAPER if paper else _LIVE

    def capabilities(self) -> frozenset[BrokerCapability]:
        return frozenset(
            {
                BrokerCapability.EQUITIES,
                BrokerCapability.OPTIONS,
                BrokerCapability.CRYPTO,
                BrokerCapability.FRACTIONAL_SHARES,
                BrokerCapability.EXTENDED_HOURS,
                BrokerCapability.PAPER_TRADING,
                BrokerCapability.STREAMING,
                BrokerCapability.MARGIN,
            }
        )

    async def _get(self, path: str, **params: Any) -> Any:
        return await self._request("GET", f"{self._base}{path}", headers=self._headers, params=params)

    async def connect(self) -> None:
        account = await self._get("/v2/account")
        if account.get("status") not in ("ACTIVE", "PAPER_ONLY"):
            raise BrokerAuthError(self.name, f"account status is {account.get('status')}")
        self._connected = True

    async def account(self) -> AccountSnapshot:
        a = await self._get("/v2/account")
        equity = Decimal(a["equity"])
        last_equity = Decimal(a.get("last_equity", a["equity"]))
        return AccountSnapshot(
            broker=self.name, account_id=a["account_number"],
            equity=equity,
            cash=Decimal(a["cash"]),
            buying_power=Decimal(a["buying_power"]),
            maintenance_margin=Decimal(a["maintenance_margin"]) if a.get("maintenance_margin") else None,
            margin_used=Decimal(a["initial_margin"]) if a.get("initial_margin") else None,
            options_buying_power=Decimal(a["options_buying_power"]) if a.get("options_buying_power") else None,
            day_pnl=equity - last_equity,
            as_of=datetime.now(UTC),
        )

    async def positions(self) -> list[Position]:
        rows = await self._get("/v2/positions")
        result: list[Position] = []
        now = datetime.now(UTC)
        for p in rows or []:
            asset_class = {
                "us_equity": AssetClass.EQUITY,
                "us_option": AssetClass.OPTION,
                "crypto": AssetClass.CRYPTO,
            }.get(p.get("asset_class", "us_equity"), AssetClass.EQUITY)
            symbol = p["symbol"]
            if asset_class is AssetClass.CRYPTO:
                # /v2/positions returns crypto pairs slashless ("USDTUSD");
                # orders, quotes, and risk matching all use the canonical
                # "USDT/USD" — map at the seam so one position has one key.
                symbol = canonical_crypto_pair(symbol)
            result.append(
                Position(
                    symbol=symbol, asset_class=asset_class,
                    quantity=Decimal(p["qty"]),
                    avg_entry_price=Decimal(p["avg_entry_price"]),
                    market_value=Decimal(p["market_value"]) if p.get("market_value") else None,
                    unrealized_pnl=Decimal(p["unrealized_pl"]) if p.get("unrealized_pl") else None,
                    broker=self.name, as_of=now,
                )
            )
        return result

    def order_limits(self) -> dict[str, Any]:
        return {
            "max_order_notional": {"crypto": str(_CRYPTO_MAX_ORDER_NOTIONAL)},
            "note": ("alpaca refuses any single crypto order (buy OR sell) above "
                     "$200,000 notional; size each crypto order within the cap. A "
                     "position larger than the cap must also be EXITED in slices "
                     "across cycles — plan entries and exits accordingly"),
        }

    async def preflight(self, order: Order) -> str | None:
        # Alpaca's crypto per-order notional cap is policy, not transport, so
        # exceeding it is a DEFINITE refusal — reject pre-submit with the
        # remedy instead of a late 403. Price the check exactly as alpaca
        # does: for a limit/stop-limit order the cap is computed off the
        # LIMIT price (proven from live 403 bodies — notional == qty x limit
        # to the cent, even when the arrival mid differed), a stop order off
        # its trigger, a market order off the arrival mid (the closest bound
        # available). Pricing it conservatively at max(limit, arrival) would
        # falsely refuse placeable orders — including risk-reducing exits —
        # which the base contract forbids. An unpriceable order stays None.
        if order.asset_class is AssetClass.CRYPTO:
            price = order.limit_price or order.stop_price or order.arrival_price
            if price is not None:
                notional = order.quantity * price
                if notional > _CRYPTO_MAX_ORDER_NOTIONAL:
                    return (
                        f"alpaca refuses crypto orders above "
                        f"${_CRYPTO_MAX_ORDER_NOTIONAL:,.0f} notional per order "
                        f"(this one is ~${notional:,.0f}); size at or under the cap "
                        f"and build or unwind the position across cycles"
                    )
        return None

    @staticmethod
    def _order_body(order: Order) -> dict[str, Any]:
        """Build the /v2/orders body.

        Price fields follow the order TYPE, not merely what the order carries.
        Attaching them by presence sent a limit order with a stop price, which
        Alpaca refuses outright:

            {"code":40010001,"message":"limit orders require no stop price"}

        Observed live on the first order the AI placed after the sizing fix
        (ADA/USD, rejected_broker). Alpaca's contract: market -> neither,
        limit -> limit_price, stop -> stop_price, stop_limit -> both,
        trailing_stop -> neither (it uses trail_price/trail_percent).
        """
        # Crypto rejects the equity order fields: the day/opg/cls TIFs return
        # HTTP 422 42210000, and extended_hours is equity-only. Remap a
        # non-crypto TIF to gtc and drop extended_hours; equities are unchanged.
        is_crypto = order.asset_class is AssetClass.CRYPTO
        if is_crypto and order.time_in_force not in _CRYPTO_TIF:
            time_in_force = "gtc"
        else:
            time_in_force = order.time_in_force.value
        body: dict[str, Any] = {
            "symbol": order.symbol.upper(),
            "side": _SIDE_MAP[order.side],
            "time_in_force": time_in_force,
            "client_order_id": order.client_order_id,
        }
        if not is_crypto:
            body["extended_hours"] = order.extended_hours
        # Alpaca uses "trailing_stop" / "stop_limit" verbatim; map enum values.
        body["type"] = {
            OrderType.MARKET: "market", OrderType.LIMIT: "limit", OrderType.STOP: "stop",
            OrderType.STOP_LIMIT: "stop_limit", OrderType.TRAILING_STOP: "trailing_stop",
        }[order.order_type]
        body["qty"] = str(order.quantity)  # fractional supported for market/day
        wants_limit = order.order_type in (OrderType.LIMIT, OrderType.STOP_LIMIT)
        wants_stop = order.order_type in (OrderType.STOP, OrderType.STOP_LIMIT)
        if wants_limit and order.limit_price is not None:
            body["limit_price"] = str(order.limit_price)
        if wants_stop and order.stop_price is not None:
            body["stop_price"] = str(order.stop_price)
        return body

    async def submit_order(self, order: Order) -> Order:
        body = self._order_body(order)
        # idempotent=False: client_order_id LOOKS like a server-enforced
        # idempotency key, but it is not one. A true idempotency key returns the
        # ORIGINAL resource on replay; Alpaca rejects the replay:
        #     HTTP 422 {"code":42210000,"message":"client_order_id must be unique"}
        # (verified against the live paper API, 2026-08). So a timeout or 5xx on
        # submit has an UNKNOWN outcome and must be raised ambiguous rather than
        # auto-retried — a retry resubmits the same client_order_id, earns that
        # 422, and the manager then marks the order REJECTED_BROKER and releases
        # its reservation while submit #1 is LIVE at the broker: no poller, no
        # ORDER_FILLED, so the guardian never arms a stop, and
        # _reconcile_ambiguous_orders (APPROVED/ERROR only) never repairs it.
        response = await self._request(
            "POST", f"{self._base}/v2/orders", headers=self._headers, json_body=body,
            idempotent=False,
        )
        order.broker = self.name
        order.broker_order_id = response["id"]
        order.status = _STATUS_MAP.get(response.get("status", ""), OrderStatus.SUBMITTED)
        order.updated_at = datetime.now(UTC)
        return order

    async def cancel_order(self, order: Order) -> Order:
        if not order.broker_order_id:
            raise BrokerError(self.name, "order has no broker_order_id", retryable=False)
        await self._request(
            "DELETE", f"{self._base}/v2/orders/{order.broker_order_id}", headers=self._headers
        )
        # Cancel is asynchronous at the broker: the DELETE only queues the
        # request and in-flight fills can still occur. Adopt the broker's
        # authoritative state (pending-cancel maps to a non-terminal status)
        # so the lifecycle poller carries the order to its true terminal
        # state with any last-moment fills attached.
        try:
            return await self.order_status(order)
        except BrokerError:
            order.status = OrderStatus.ACCEPTED
            order.status_reason = "cancel requested — awaiting broker confirmation"
            order.updated_at = datetime.now(UTC)
            return order

    async def order_status(self, order: Order) -> Order:
        if not order.broker_order_id:
            raise BrokerError(self.name, "order has no broker_order_id", retryable=False)
        response = await self._get(f"/v2/orders/{order.broker_order_id}")
        return self._merge_order(order, response)

    async def open_orders(self) -> list[Order]:
        rows = await self._get("/v2/orders", status="open", limit=100)
        return [self._row_to_order(r) for r in rows or []]

    async def recent_fills(self, *, limit: int = 50) -> list[Fill]:
        rows = await self._get("/v2/account/activities/FILL", page_size=min(limit, 100))
        fills: list[Fill] = []
        for r in rows or []:
            side = OrderSide.BUY if r.get("side") == "buy" else OrderSide.SELL
            fills.append(
                Fill(
                    order_id=r.get("order_id", ""), broker_order_id=r.get("order_id"),
                    symbol=r.get("symbol", ""), side=side,
                    quantity=Decimal(str(r.get("qty", "0"))),
                    price=Decimal(str(r.get("price", "0"))),
                    filled_at=_parse_ts(r.get("transaction_time")),
                    broker=self.name,
                )
            )
        return fills

    def _merge_order(self, order: Order, response: dict[str, Any]) -> Order:
        order.status = _STATUS_MAP.get(response.get("status", ""), order.status)
        try:
            if response.get("filled_qty"):
                order.filled_quantity = Decimal(response["filled_qty"])
            if response.get("filled_avg_price"):
                order.avg_fill_price = Decimal(response["filled_avg_price"])
        except (InvalidOperation, TypeError, ValueError) as exc:
            # A boundary failure (malformed/unexpected field in the broker's
            # own response), not a status-poll transport error — raised as
            # BrokerError so callers (_confirm_cancel, _poll_to_terminal) hit
            # their existing except BrokerError degrade instead of an
            # unhandled exception abandoning the caller's whole operation.
            raise BrokerError(
                self.name, f"malformed fill data in order response: {exc}", retryable=False
            ) from exc
        order.updated_at = datetime.now(UTC)
        return order

    def _row_to_order(self, r: dict[str, Any]) -> Order:
        side = OrderSide.BUY if r.get("side") == "buy" else OrderSide.SELL
        order_type = {
            "market": OrderType.MARKET, "limit": OrderType.LIMIT, "stop": OrderType.STOP,
            "stop_limit": OrderType.STOP_LIMIT, "trailing_stop": OrderType.TRAILING_STOP,
        }.get(r.get("type", "limit"), OrderType.LIMIT)
        order = Order(
            client_order_id=r.get("client_order_id", ""),
            broker=self.name, broker_order_id=r.get("id"),
            symbol=r.get("symbol", ""), side=side, order_type=order_type,
            quantity=Decimal(r.get("qty") or r.get("filled_qty") or "0") or Decimal("1"),
            limit_price=Decimal(r["limit_price"]) if r.get("limit_price") else None,
            stop_price=Decimal(r["stop_price"]) if r.get("stop_price") else None,
            time_in_force=TimeInForce(r.get("time_in_force", "day")),
            status=_STATUS_MAP.get(r.get("status", ""), OrderStatus.ACCEPTED),
            created_at=_parse_ts(r.get("created_at")),
            updated_at=_parse_ts(r.get("updated_at")),
        )
        if r.get("filled_qty"):
            order.filled_quantity = Decimal(r["filled_qty"])
        if r.get("filled_avg_price"):
            order.avg_fill_price = Decimal(r["filled_avg_price"])
        return order
