"""Alpaca ``submit_order`` request-body shaping.

Pins the crypto time-in-force / extended-hours fix. Alpaca rejects equity-style
order fields on crypto: the equity TIFs (``day``/``opg``/``cls``) return
HTTP 422 ``42210000`` "invalid crypto time_in_force", and ``extended_hours`` is
equity-only. So a crypto order must send a crypto-valid TIF and must NOT send
``extended_hours``; equities/options are unchanged.
"""

from __future__ import annotations

import json
from decimal import Decimal
from typing import Any

import httpx
import pytest

from poseidon.brokers.plugins.alpaca import AlpacaBroker
from poseidon.core.enums import AssetClass, OrderSide, OrderStatus, OrderType, TimeInForce
from poseidon.core.errors import BrokerError
from poseidon.core.models import Order

_CREDS = {"key_id": "k", "secret_key": "s"}


def _broker_capturing(captured: dict[str, Any]) -> AlpacaBroker:
    """An ``AlpacaBroker`` whose HTTP client records the POSTed order body into
    ``captured["body"]`` and returns a minimal accepted-order response. No
    network: the transport is an in-process ``httpx.MockTransport``."""

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content)
        return httpx.Response(
            200,
            json={"id": "broker-order-1", "status": "accepted"},
            headers={"content-type": "application/json"},
        )

    broker = AlpacaBroker(credentials=_CREDS)
    broker._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return broker


async def test_crypto_order_uses_gtc_and_omits_extended_hours() -> None:
    # BTC/USD market order carrying the equity default TIF (day). Alpaca rejects
    # `day` for crypto, so it must be remapped to a crypto-valid `gtc`, and the
    # equity-only `extended_hours` field must be absent from the body entirely.
    captured: dict[str, Any] = {}
    broker = _broker_capturing(captured)
    order = Order(
        symbol="BTC/USD",
        asset_class=AssetClass.CRYPTO,
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=Decimal("0.01"),
        time_in_force=TimeInForce.DAY,
    )
    await broker.submit_order(order)
    body = captured["body"]
    assert body["time_in_force"] == "gtc"
    assert "extended_hours" not in body
    assert body["symbol"] == "BTC/USD"


async def test_equity_order_keeps_tif_and_extended_hours() -> None:
    # Equity path is unchanged: the order's own TIF is sent verbatim and the
    # equity-only extended_hours flag is included.
    captured: dict[str, Any] = {}
    broker = _broker_capturing(captured)
    order = Order(
        symbol="AAPL",
        asset_class=AssetClass.EQUITY,
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=Decimal("1"),
        time_in_force=TimeInForce.DAY,
        extended_hours=True,
    )
    await broker.submit_order(order)
    body = captured["body"]
    assert body["time_in_force"] == "day"
    assert body["extended_hours"] is True


async def test_crypto_order_preserves_already_valid_tif() -> None:
    # gtc/ioc/fok are valid crypto TIFs: only the non-crypto day/opg/cls are
    # remapped. A deliberate immediate-or-cancel must NOT be silently converted
    # to gtc (which would rest on the book). extended_hours is still omitted.
    captured: dict[str, Any] = {}
    broker = _broker_capturing(captured)
    order = Order(
        symbol="ETH/USD",
        asset_class=AssetClass.CRYPTO,
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=Decimal("0.5"),
        time_in_force=TimeInForce.IOC,
    )
    await broker.submit_order(order)
    body = captured["body"]
    assert body["time_in_force"] == "ioc"
    assert "extended_hours" not in body


async def test_positions_canonicalize_slashless_crypto_symbols() -> None:
    # Alpaca /v2/positions returns crypto pairs SLASHLESS ("USDTUSD") while its
    # own trading/data APIs — and this whole platform — use "USDT/USD". Mapped
    # raw, the ledger key never matches an exit order or a crypto quote route.
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path.endswith("/v2/positions")
        return httpx.Response(200, json=[
            {"symbol": "USDTUSD", "asset_class": "crypto", "qty": "9.985",
             "avg_entry_price": "1", "market_value": "9.97", "unrealized_pl": "-0.01"},
            {"symbol": "AAPL", "asset_class": "us_equity", "qty": "10",
             "avg_entry_price": "100", "market_value": "1000", "unrealized_pl": "0"},
            {"symbol": "BTCUSD", "asset_class": "us_equity", "qty": "5",
             "avg_entry_price": "10", "market_value": "50", "unrealized_pl": "0"},
        ], headers={"content-type": "application/json"})

    broker = AlpacaBroker(credentials=_CREDS)
    broker._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    positions = await broker.positions()

    assert positions[0].symbol == "USDT/USD"
    assert positions[0].asset_class is AssetClass.CRYPTO
    assert positions[1].symbol == "AAPL"  # equities untouched
    # An equity that merely LOOKS like a pair must stay raw: the guard
    # keys off the broker's asset_class, never off symbol shape.
    assert positions[2].symbol == "BTCUSD"
    assert positions[2].asset_class is AssetClass.EQUITY


async def test_order_status_raises_broker_error_on_malformed_fill_data() -> None:
    # A boundary failure — Alpaca's own response carries an unparseable
    # filled_qty — must surface as BrokerError, the exception every caller
    # (_confirm_cancel, _poll_to_terminal) already knows how to degrade on,
    # never as an unhandled decimal.InvalidOperation that abandons whatever
    # operation was polling (e.g. clearing an opposing order before an exit).
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path.endswith("/v2/orders/broker-order-1")
        return httpx.Response(200, json={
            "id": "broker-order-1", "status": "filled", "filled_qty": "not-a-number",
        }, headers={"content-type": "application/json"})

    broker = AlpacaBroker(credentials=_CREDS)
    broker._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    order = Order(symbol="AAPL", side=OrderSide.BUY, quantity=Decimal("1"),
                  status=OrderStatus.SUBMITTED, broker_order_id="broker-order-1")

    with pytest.raises(BrokerError):
        await broker.order_status(order)
