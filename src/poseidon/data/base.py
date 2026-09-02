"""Market data provider interface.

Each provider implements only the capabilities its upstream API actually
serves and advertises them via :meth:`capabilities`; the failover router
skips providers that cannot serve a request instead of letting them fail.

Contract for implementers:
  * Return timestamped models (``as_of`` from the provider's own timestamps
    where available, otherwise receipt time).
  * Raise :class:`ProviderError` subclasses — never return fabricated data.
  * Be stateless per request; shared state (HTTP client) is owned per
    provider instance and cleaned up in :meth:`close`.
"""

from __future__ import annotations

import abc
from datetime import UTC, date, datetime, timedelta
from enum import StrEnum
from typing import Any

import httpx

from ..core.clock import ensure_aware
from ..core.errors import ProviderAuthError, ProviderError, ProviderRateLimitError
from ..core.models import (
    Bar,
    EarningsEvent,
    EconomicEvent,
    Filing,
    FundamentalsReport,
    InsiderTransaction,
    InstrumentProfile,
    NewsArticle,
    OptionChain,
    Quote,
)

_BAR_DURATIONS: dict[str, timedelta] = {
    "1m": timedelta(minutes=1),
    "5m": timedelta(minutes=5),
    "15m": timedelta(minutes=15),
    "1h": timedelta(hours=1),
    "1d": timedelta(days=1),
    "1w": timedelta(weeks=1),
}


def bar_end(start: datetime, timeframe: str) -> datetime:
    """Nominal end of a bar opening at ``start``. Upstream APIs report only
    the bar-open timestamp; providers derive ``Bar.end`` from it."""
    return start + _BAR_DURATIONS.get(timeframe, timedelta(0))


class DataCapability(StrEnum):
    QUOTES = "quotes"
    BARS = "bars"
    OPTIONS = "options"
    NEWS = "news"
    EARNINGS = "earnings"
    ECONOMIC_CALENDAR = "economic_calendar"
    SECTOR = "sector"  # company sector/industry taxonomy
    CRYPTO = "crypto"  # spot crypto pairs (BASE/USD); gates crypto quote/bars routing
    PROFILE = "profile"  # instrument identity (company name/exchange/currency)
    FUNDAMENTALS = "fundamentals"  # filed/reported statements + company overview
    FILINGS = "filings"  # SEC filing metadata + document links (never document text)
    INSIDER = "insider"  # reported insider (Form-4-style) transactions


class MarketDataProvider(abc.ABC):
    """Base class for market data providers."""

    #: unique provider name, matches config ``providers[].name``
    name: str = ""

    def __init__(self, *, api_key: str, timeout: float = 10.0, options: dict[str, Any] | None = None) -> None:
        self._api_key = api_key
        self._options = options or {}
        self._client = httpx.AsyncClient(timeout=timeout)

    @abc.abstractmethod
    def capabilities(self) -> frozenset[DataCapability]: ...

    # Capability methods raise NotImplementedError unless overridden; the
    # router never calls a method the provider does not advertise.

    async def quote(self, symbol: str) -> Quote:
        raise NotImplementedError

    async def bars(self, symbol: str, *, timeframe: str, limit: int) -> list[Bar]:
        raise NotImplementedError

    async def bars_multi(self, symbols: list[str], *, timeframe: str,
                         limit: int) -> dict[str, list[Bar]]:
        """Batched daily bars for many symbols in one (paginated) round-trip.

        Returns ``{symbol: chronological_bars}``; a symbol the feed cannot serve
        is simply absent (never fabricated). Providers whose upstream has no
        multi-symbol bars endpoint leave this at the default ``NotImplementedError``
        so :class:`DataRouter` degrades to bounded single-symbol ``bars`` calls.
        """
        raise NotImplementedError

    async def option_chain(self, underlying: str, *, expiration: date | None = None) -> OptionChain:
        raise NotImplementedError

    async def news(self, symbols: list[str] | None = None, *, limit: int = 25) -> list[NewsArticle]:
        raise NotImplementedError

    async def earnings(self, *, days_ahead: int = 14, symbols: list[str] | None = None) -> list[EarningsEvent]:
        raise NotImplementedError

    async def economic_calendar(self, *, days_ahead: int = 7) -> list[EconomicEvent]:
        raise NotImplementedError

    async def profile(self, symbol: str) -> InstrumentProfile:
        """Resolved instrument identity. Raise ProviderError when the
        provider cannot resolve the symbol to a listed instrument."""
        raise NotImplementedError

    async def sector(self, symbol: str) -> str:
        """Company sector/industry classification. Raise ProviderError when
        the provider has no classification for the symbol (e.g. ETFs)."""
        raise NotImplementedError

    async def fundamentals(self, symbol: str) -> FundamentalsReport:
        """Filed/reported fundamentals: company overview + statement periods.
        Values are served exactly as reported (Decimal), never derived. Raise
        ProviderError(retryable=False) when the provider has no fundamentals
        for the symbol."""
        raise NotImplementedError

    async def filings(self, symbol: str, *, limit: int = 10) -> list[Filing]:
        """Recent filing metadata, newest first, bounded by ``limit``.
        Metadata + document links only — never fetched document text."""
        raise NotImplementedError

    async def insider_transactions(self, symbol: str, *,
                                   limit: int = 20) -> list[InsiderTransaction]:
        """Recent reported insider transactions, newest first, bounded by
        ``limit``. An empty list means none reported — a real answer, never
        an error."""
        raise NotImplementedError

    async def close(self) -> None:
        await self._client.aclose()

    # -- shared HTTP helpers ---------------------------------------------------

    async def _get_json(self, url: str, *, params: dict[str, Any] | None = None,
                        headers: dict[str, str] | None = None) -> Any:
        try:
            response = await self._client.get(url, params=params, headers=headers)
        except httpx.TimeoutException as exc:
            raise ProviderError(self.name, f"timeout: {exc}") from exc
        except httpx.HTTPError as exc:
            raise ProviderError(self.name, f"transport error: {exc}") from exc
        return self._decode(response)

    async def _post_json(self, url: str, *, json_body: dict[str, Any],
                         headers: dict[str, str] | None = None) -> Any:
        """POST helper for providers whose read endpoints take JSON bodies."""
        try:
            response = await self._client.post(url, json=json_body, headers=headers)
        except httpx.TimeoutException as exc:
            raise ProviderError(self.name, f"timeout: {exc}") from exc
        except httpx.HTTPError as exc:
            raise ProviderError(self.name, f"transport error: {exc}") from exc
        return self._decode(response)

    def _decode(self, response: httpx.Response) -> Any:
        if response.status_code in (401, 403):
            raise ProviderAuthError(self.name)
        if response.status_code == 429:
            raise ProviderRateLimitError(
                self.name, self._parse_retry_after(response.headers.get("Retry-After"))
            )
        if response.status_code in (404, 410):
            # A missing/gone RESOURCE is a permanent fact about this symbol, not
            # an outage of the provider. DataRouter._route skips a provider on a
            # non-retryable error but calls record_failure on a retryable one —
            # and the penalty box is keyed on the provider, NOT on
            # (provider, capability). So a delisted crypto pair classified as
            # retryable penalises that provider for every capability it serves,
            # including equity QUOTES.
            raise ProviderError(
                self.name, f"HTTP {response.status_code}: {response.text[:300]}",
                retryable=False,
            )
        if response.status_code >= 400:
            raise ProviderError(self.name, f"HTTP {response.status_code}: {response.text[:300]}")
        try:
            return response.json()
        except ValueError as exc:
            raise ProviderError(self.name, "invalid JSON in response") from exc

    @staticmethod
    def _parse_retry_after(value: str | None) -> float | None:
        """Retry-After is either delta-seconds or an HTTP-date (RFC 9110).
        Returns seconds-to-wait, or None if absent/unparseable — guaranteeing
        the 429 path only ever raises a ProviderError (so the router fails
        over) instead of a raw ValueError from float() on a date string."""
        if not value:
            return None
        try:
            return float(value)
        except ValueError:
            pass
        try:
            from email.utils import parsedate_to_datetime
            dt = parsedate_to_datetime(value)
        except (TypeError, ValueError):
            return None
        if dt is None:
            return None
        return max(0.0, (ensure_aware(dt) - datetime.now(UTC)).total_seconds())

    @staticmethod
    def _now() -> datetime:
        return datetime.now(UTC)

    @staticmethod
    def _ts_from_epoch(value: float | int | None, *, millis: bool = False,
                       nanos: bool = False) -> datetime | None:
        if value is None:
            return None
        seconds = float(value)
        if nanos:
            seconds /= 1_000_000_000
        elif millis:
            seconds /= 1_000
        return datetime.fromtimestamp(seconds, tz=UTC)
