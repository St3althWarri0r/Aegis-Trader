"""Coinbase Exchange public data provider (https://docs.cdp.coinbase.com/exchange).

Capabilities: spot crypto quotes and bars for ``BASE/USD`` pairs. The public
REST API (``api.exchange.coinbase.com``) needs no API key or account — the
ticker and candles endpoints are open — so this is a zero-cost, crypto-only
data path that complements the equity providers at the router.

Endpoints:

  * ticker:  GET /products/{BASE}-{QUOTE}/ticker
             -> {"price","bid","ask","size","volume","time","trade_id"}
             (``time`` is the last-trade instant, ISO-8601)
  * candles: GET /products/{BASE}-{QUOTE}/candles?granularity=<seconds>
             -> [[time, low, high, open, close, volume], ...], newest-first,
             ``time`` in unix seconds (bucket start)

Poseidon's internal ``BASE/QUOTE`` form (e.g. ``BTC/USD``) is converted to
Coinbase's ``BASE-QUOTE`` product id (``BTC-USD``). Only USD spot pairs are
supported (``normalize_crypto_symbol`` rejects anything else).
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from ...core.clock import ensure_aware
from ...core.errors import ProviderError
from ...core.models import Bar, Quote
from ...core.symbols import is_crypto_symbol, normalize_crypto_symbol
from ..base import DataCapability, MarketDataProvider, bar_end

_BASE = "https://api.exchange.coinbase.com"

# Poseidon timeframe -> Coinbase candle granularity in seconds. Coinbase serves
# 60/300/900/3600/21600/86400 only; there is no weekly granularity, so "1w" is
# unsupported (raises, never silently downgraded).
_GRANULARITY: dict[str, int] = {
    "1m": 60,
    "5m": 300,
    "15m": 900,
    "1h": 3600,
    "1d": 86400,
}


def _dec(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _product_id(symbol: str) -> str:
    """``BTC/USD`` -> ``BTC-USD`` (Coinbase product id), validating the pair."""
    return normalize_crypto_symbol(symbol).replace("/", "-")


class CoinbaseDataProvider(MarketDataProvider):
    name = "coinbase"

    def capabilities(self) -> frozenset[DataCapability]:
        # Crypto-only: the router uses CRYPTO to gate routing and QUOTES/BARS to
        # pick the method; no equity, options, or news capability is advertised.
        return frozenset({DataCapability.CRYPTO, DataCapability.QUOTES, DataCapability.BARS})

    async def _get(self, path: str, **params: Any) -> Any:
        return await self._get_json(f"{_BASE}{path}", params=params or None)

    def _require_crypto(self, symbol: str) -> str:
        """Reject non-crypto symbols before any HTTP call; never return equity
        data. Returns the Coinbase product id for a valid ``BASE/USD`` pair."""
        if not is_crypto_symbol(symbol):
            raise ProviderError(
                self.name, f"coinbase serves crypto pairs only, not {symbol!r}",
                retryable=False,
            )
        return _product_id(symbol)

    async def quote(self, symbol: str) -> Quote:
        product = self._require_crypto(symbol)
        payload = await self._get(f"/products/{product}/ticker")
        if not isinstance(payload, dict) or payload.get("price") is None:
            raise ProviderError(self.name, f"no quote for {symbol}")
        # as_of is the last-trade time the exchange stamped — never receipt time.
        # A fabricated "now" would let a frozen feed pass the router's staleness
        # gate; absence of a real timestamp is a hard failure instead.
        as_of = self._parse_ts(payload.get("time"))
        if as_of is None:
            raise ProviderError(self.name, f"quote for {symbol} has no timestamp")
        return Quote(
            symbol=normalize_crypto_symbol(symbol),
            bid=_dec(payload.get("bid")),
            ask=_dec(payload.get("ask")),
            last=_dec(payload.get("price")),
            # book sizes are fractional coins, not integer share counts, so they
            # are left unset (SpreadRule uses bid/ask); mirrors alpaca_data.
            as_of=as_of,
            source=self.name,
        )

    async def bars(self, symbol: str, *, timeframe: str, limit: int) -> list[Bar]:
        product = self._require_crypto(symbol)
        granularity = _GRANULARITY.get(timeframe)
        if granularity is None:
            raise ProviderError(
                self.name, f"unsupported timeframe {timeframe}", retryable=False
            )
        payload = await self._get(f"/products/{product}/candles", granularity=granularity)
        if not isinstance(payload, list):
            raise ProviderError(self.name, f"no bars for {symbol} ({timeframe})")
        sym = normalize_crypto_symbol(symbol)
        bars: list[Bar] = []
        # Coinbase returns rows newest-first; take the newest `limit` then flip
        # to chronological order (consumers expect ascending time).
        for row in payload[:limit]:
            try:
                start = datetime.fromtimestamp(float(row[0]), tz=UTC)
                low, high, open_, close = row[1], row[2], row[3], row[4]
                volume = row[5] if len(row) > 5 else 0
                bars.append(
                    Bar(
                        symbol=sym,
                        open=Decimal(str(open_)), high=Decimal(str(high)),
                        low=Decimal(str(low)), close=Decimal(str(close)),
                        # crypto volume is fractional coins; Bar.volume is an int
                        # coin count (VolumeRule is exempt for crypto).
                        volume=int(float(volume)),
                        start=start, end=bar_end(start, timeframe), source=self.name,
                    )
                )
            except (IndexError, ValueError, TypeError, InvalidOperation):
                continue
        bars.reverse()  # newest-first upstream -> chronological
        if not bars:
            raise ProviderError(self.name, f"no bars for {symbol} ({timeframe})")
        return bars

    @staticmethod
    def _parse_ts(value: Any) -> datetime | None:
        if not value:
            return None
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            return None
        return ensure_aware(parsed)
