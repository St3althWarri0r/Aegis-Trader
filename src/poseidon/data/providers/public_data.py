"""Public.com market data provider (official API, https://public.com/api).

Free with a Public brokerage account: real-time quotes (equities, options,
crypto), OHLCV bars, and full option chains with greeks, using the same API
secret as the ``public`` broker plugin. This is the platform's zero-cost
data path — no separate market-data subscription required.

Endpoints (same contract as Public's own SDK/MCP server):

  * auth:   POST /userapiauthservice/personal/access-tokens
  * quotes: POST /userapigateway/marketdata/{accountId}/quotes
  * chain:  POST /userapigateway/marketdata/{accountId}/option-chain
            POST /userapigateway/marketdata/{accountId}/option-expirations
  * bars:   GET  /userapigateway/historicdata/{type}/{symbol}/{period}/{agg}

Credential: the Public API secret (plain string), or JSON
``{"api_key": "<secret>", "account_id": "..."}`` — ``account_id`` is
resolved automatically from the key when omitted. Options: set
``options: {crypto_symbols: [BTC, ETH]}`` to quote those symbols as crypto
instruments instead of equities.
"""

from __future__ import annotations

import time
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from ...core.clock import ensure_aware
from ...core.enums import OptionRight
from ...core.errors import ProviderAuthError, ProviderError
from ...core.models import Bar, Greeks, OptionChain, OptionContract, Quote
from ..base import DataCapability, MarketDataProvider

_BASE = "https://api.public.com"
_TOKEN_VALIDITY_MINUTES = 1440

# timeframe -> (period, aggregation) for the historicdata endpoint.
_TIMEFRAME_MAP: dict[str, tuple[str, str]] = {
    "1m": ("DAY", "ONE_MINUTE"),
    "5m": ("DAY", "FIVE_MINUTES"),
    "15m": ("DAY", "FIFTEEN_MINUTES"),
    "1h": ("WEEK", "ONE_HOUR"),
    "1d": ("YEAR", "ONE_DAY"),
}

# Per-timeframe (period, approx bars that window holds) ladders. bars() picks
# the smallest window whose capacity covers the requested limit so a large
# intraday request is not silently truncated to one day/week of data. Counts
# assume a ~390-minute regular session.
_WINDOW_LADDER: dict[str, list[tuple[str, int]]] = {
    "1m": [("DAY", 390), ("WEEK", 1950), ("MONTH", 8000)],
    "5m": [("DAY", 78), ("WEEK", 390), ("MONTH", 1640), ("QUARTER", 4900)],
    "15m": [("DAY", 26), ("WEEK", 130), ("MONTH", 550), ("QUARTER", 1650)],
    "1h": [("WEEK", 35), ("MONTH", 147), ("QUARTER", 440), ("HALF_YEAR", 880), ("YEAR", 1750)],
    "1d": [("YEAR", 252), ("FIVE_YEARS", 1260), ("TEN_YEARS", 2520)],
}


def _dec(value: Any) -> Decimal | None:
    if value is None:
        return None
    return Decimal(str(value))


def _num(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _ts(value: Any) -> datetime | None:
    """Parse the API's ISO-8601 timestamps (with or without a Z suffix)."""
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return ensure_aware(parsed)


class PublicDataProvider(MarketDataProvider):
    name = "public_data"

    def __init__(self, *, api_key: str, timeout: float = 10.0,
                 options: dict[str, Any] | None = None) -> None:
        super().__init__(api_key=api_key, timeout=timeout, options=options)
        if not api_key:
            raise ProviderAuthError(self.name)
        self._account_id: str = str(self._options.get("account_id", ""))
        self._crypto = {str(s).upper() for s in self._options.get("crypto_symbols", [])}
        self._access_token: str | None = None
        self._token_expiry = 0.0

    def capabilities(self) -> frozenset[DataCapability]:
        return frozenset({DataCapability.QUOTES, DataCapability.BARS, DataCapability.OPTIONS})

    # -- auth / account --------------------------------------------------------

    async def _ensure_token(self) -> str:
        if self._access_token and time.monotonic() < self._token_expiry - 300:
            return self._access_token
        payload = await self._post_json(
            f"{_BASE}/userapiauthservice/personal/access-tokens",
            json_body={"validityInMinutes": _TOKEN_VALIDITY_MINUTES, "secret": self._api_key},
        )
        token = (payload or {}).get("accessToken")
        if not token:
            raise ProviderAuthError(self.name)
        self._access_token = str(token)
        self._token_expiry = time.monotonic() + _TOKEN_VALIDITY_MINUTES * 60
        return self._access_token

    async def _headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {await self._ensure_token()}"}

    async def _ensure_account(self) -> str:
        if self._account_id:
            return self._account_id
        payload = await self._get_json(
            f"{_BASE}/userapigateway/trading/account", headers=await self._headers()
        )
        accounts = (payload or {}).get("accounts") or []
        if not accounts:
            raise ProviderError(self.name, "no accounts on this API key", retryable=False)
        self._account_id = str(accounts[0]["accountId"])
        return self._account_id

    def _instrument(self, symbol: str) -> dict[str, str]:
        symbol = symbol.upper()
        return {"symbol": symbol, "type": "CRYPTO" if symbol in self._crypto else "EQUITY"}

    # -- quotes ----------------------------------------------------------------

    async def quote(self, symbol: str) -> Quote:
        account_id = await self._ensure_account()
        payload = await self._post_json(
            f"{_BASE}/userapigateway/marketdata/{account_id}/quotes",
            json_body={"instruments": [self._instrument(symbol)]},
            headers=await self._headers(),
        )
        rows = (payload or {}).get("quotes") or []
        if not rows or rows[0].get("outcome") != "SUCCESS":
            raise ProviderError(self.name, f"no quote for {symbol}")
        q = rows[0]
        # Never manufacture a timestamp: if the provider gives no time for
        # any field, it cannot vouch for freshness, so fail rather than stamp
        # possibly-stale data as "now" (which would slip past the router's
        # staleness gate). The router then fails over or refuses to trade.
        as_of = (
            _ts(q.get("lastTimestamp")) or _ts(q.get("askTimestamp"))
            or _ts(q.get("bidTimestamp"))
        )
        if as_of is None:
            raise ProviderError(self.name, f"quote for {symbol} has no timestamp")
        return Quote(
            symbol=symbol.upper(),
            bid=_dec(q.get("bid")), ask=_dec(q.get("ask")), last=_dec(q.get("last")),
            bid_size=q.get("bidSize"), ask_size=q.get("askSize"),
            volume=q.get("volume"),
            as_of=as_of, source=self.name,
        )

    # -- bars ------------------------------------------------------------------

    async def bars(self, symbol: str, *, timeframe: str, limit: int) -> list[Bar]:
        try:
            period, aggregation = _TIMEFRAME_MAP[timeframe]
        except KeyError:
            raise ProviderError(
                self.name, f"unsupported timeframe {timeframe}", retryable=False
            ) from None
        # Widen the fetch window to the smallest that can hold `limit` bars,
        # so intraday and deep-history requests are not silently truncated.
        for candidate_period, capacity in _WINDOW_LADDER.get(timeframe, []):
            period = candidate_period
            if capacity >= limit:
                break
        instrument_type = self._instrument(symbol)["type"]
        payload = await self._get_json(
            f"{_BASE}/userapigateway/historicdata/{instrument_type}"
            f"/{symbol.upper()}/{period}/{aggregation}",
            headers=await self._headers(),
        ) or {}
        rows = ((payload.get("regularMarket") or {}).get("bars")) or []
        bars: list[Bar] = []
        for row in rows[-limit:]:
            start = _ts(row.get("timestamp"))
            if start is None:
                continue
            try:
                bars.append(
                    Bar(
                        symbol=symbol.upper(),
                        open=Decimal(str(row["open"])), high=Decimal(str(row["high"])),
                        low=Decimal(str(row["low"])), close=Decimal(str(row["close"])),
                        volume=int(float(row.get("volume", 0) or 0)),  # explicit null -> 0
                        # Public reports the session-close instant (20:00Z for
                        # daily bars), so start already marks the bar end.
                        start=start, end=start, source=self.name,
                    )
                )
            except (KeyError, ValueError, InvalidOperation, TypeError):
                continue
        if not bars:
            raise ProviderError(self.name, f"no bars for {symbol} ({timeframe})")
        return bars

    # -- options ---------------------------------------------------------------

    async def option_chain(self, underlying: str, *, expiration: date | None = None) -> OptionChain:
        account_id = await self._ensure_account()
        headers = await self._headers()
        instrument = {"symbol": underlying.upper(), "type": "EQUITY"}
        if expiration is None:
            exp_payload = await self._post_json(
                f"{_BASE}/userapigateway/marketdata/{account_id}/option-expirations",
                json_body={"instrument": instrument}, headers=headers,
            )
            dates = (exp_payload or {}).get("expirations") or []
            if not dates:
                raise ProviderError(self.name, f"no option expirations for {underlying}")
            expiration = date.fromisoformat(str(dates[0]))
        payload = await self._post_json(
            f"{_BASE}/userapigateway/marketdata/{account_id}/option-chain",
            json_body={"instrument": instrument, "expirationDate": expiration.isoformat()},
            headers=headers,
        ) or {}
        contracts: list[OptionContract] = []
        for right, key in ((OptionRight.CALL, "calls"), (OptionRight.PUT, "puts")):
            for q in payload.get(key) or []:
                contract = self._contract(q, underlying, right, expiration)
                if contract is not None:
                    contracts.append(contract)
        if not contracts:
            raise ProviderError(self.name, f"empty option chain for {underlying} {expiration}")
        # Contracts carry real per-quote timestamps; grade the chain by its
        # oldest strike so a frozen feed is rejected upstream.
        chain_as_of = min(c.as_of for c in contracts)
        return OptionChain(
            underlying=underlying.upper(), expirations=[expiration],
            contracts=contracts, as_of=chain_as_of, source=self.name,
        )

    def _contract(self, q: dict[str, Any], underlying: str, right: OptionRight,
                  expiration: date) -> OptionContract | None:
        details = q.get("optionDetails") or {}
        strike = _dec(details.get("strikePrice"))
        symbol = (q.get("instrument") or {}).get("symbol")
        if not symbol or strike is None or q.get("outcome") != "SUCCESS":
            return None
        # No verifiable quote time -> no contract; a fabricated as_of would
        # let a frozen chain pass the router's staleness gate.
        ts = _ts(q.get("lastTimestamp"))
        if ts is None:
            return None
        greeks_block = details.get("greeks") or {}
        greeks = Greeks(
            delta=_num(greeks_block.get("delta")), gamma=_num(greeks_block.get("gamma")),
            theta=_num(greeks_block.get("theta")), vega=_num(greeks_block.get("vega")),
            rho=_num(greeks_block.get("rho")),
            implied_volatility=_num(greeks_block.get("impliedVolatility")),
        ) if greeks_block else None
        return OptionContract(
            symbol=str(symbol), underlying=underlying.upper(),
            right=right, strike=strike, expiration=expiration,
            bid=_dec(q.get("bid")), ask=_dec(q.get("ask")), last=_dec(q.get("last")),
            volume=q.get("volume"), open_interest=q.get("openInterest"),
            greeks=greeks,
            as_of=ts, source=self.name,
        )
