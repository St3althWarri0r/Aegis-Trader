"""Finnhub provider (https://finnhub.io/docs/api).

Capabilities: quotes, company & general news, earnings calendar, economic
calendar, sector/profile reference data, and insider transactions.
Authentication: token query parameter.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any

from ...core.clock import ensure_aware
from ...core.errors import ProviderError
from ...core.models import (
    EarningsEvent,
    EconomicEvent,
    InsiderTransaction,
    InstrumentProfile,
    NewsArticle,
    Quote,
)
from ..base import DataCapability, MarketDataProvider

_BASE = "https://finnhub.io/api/v1"


def _parse_date(value: Any) -> date | None:
    try:
        return date.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None


class FinnhubProvider(MarketDataProvider):
    name = "finnhub"

    def capabilities(self) -> frozenset[DataCapability]:
        return frozenset(
            {
                DataCapability.QUOTES,
                DataCapability.NEWS,
                DataCapability.EARNINGS,
                DataCapability.ECONOMIC_CALENDAR,
                DataCapability.SECTOR,
                DataCapability.PROFILE,
                DataCapability.INSIDER,
            }
        )

    async def _get(self, path: str, **params: Any) -> Any:
        params["token"] = self._api_key
        return await self._get_json(f"{_BASE}{path}", params=params)

    async def quote(self, symbol: str) -> Quote:
        payload = await self._get("/quote", symbol=symbol.upper())
        current = payload.get("c")
        if current in (None, 0):
            raise ProviderError(self.name, f"no quote for {symbol}")
        as_of = self._ts_from_epoch(payload.get("t"))
        if as_of is None:
            raise ProviderError(self.name, f"quote for {symbol} has no timestamp")
        return Quote(
            symbol=symbol,
            last=Decimal(str(current)),
            as_of=as_of,
            source=self.name,
        )

    async def profile(self, symbol: str) -> InstrumentProfile:
        """Instrument identity from the company profile (free tier). profile2
        has no security-type field and only resolves listed companies (ETFs/
        crypto/indices return {}), so asset_type="equity" when resolved is a
        fact; anything else raises non-retryable rather than guessing."""
        payload = await self._get("/stock/profile2", symbol=symbol.upper())
        name = (payload or {}).get("name")
        if not name:
            raise ProviderError(self.name, f"no company profile for {symbol}",
                                retryable=False)
        return InstrumentProfile(
            symbol=symbol,
            name=str(name),
            exchange=(payload.get("exchange") or None),
            currency=(payload.get("currency") or None),
            asset_type="equity",
            as_of=self._now(),
            source=self.name,
        )

    async def sector(self, symbol: str) -> str:
        """GICS-style classification from the company profile (free tier).
        ETFs and unlisted symbols have no profile — that raises, it is not
        guessed."""
        payload = await self._get("/stock/profile2", symbol=symbol.upper())
        industry = (payload or {}).get("finnhubIndustry")
        if not industry:
            raise ProviderError(self.name, f"no sector classification for {symbol}",
                                retryable=False)
        return str(industry)

    async def insider_transactions(self, symbol: str, *,
                                   limit: int = 20) -> list[InsiderTransaction]:
        """Reported insider transactions, newest first. An empty ``data`` list
        means none reported — a real answer, never an error. Malformed rows are
        skipped (news/earnings row-skip precedent), never guessed at."""
        payload = await self._get("/stock/insider-transactions", symbol=symbol.upper())
        rows = payload.get("data") if isinstance(payload, dict) else None
        now = self._now()
        out: list[InsiderTransaction] = []
        for row in (rows or [])[: max(0, limit)]:
            if not isinstance(row, dict):
                continue
            try:
                change = row.get("change")
                shares = Decimal(str(change)) if change is not None else None
                raw_price = row.get("transactionPrice")
                price = Decimal(str(raw_price)) if raw_price is not None else None
            except (InvalidOperation, ValueError):
                continue
            if price is not None and price <= 0:
                price = None  # 0 means no market price (e.g. grants)
            out.append(InsiderTransaction(
                symbol=symbol,
                name=str(row.get("name") or "unknown"),
                title=None,  # not served on this endpoint
                transaction_date=_parse_date(row.get("transactionDate")),
                filing_date=_parse_date(row.get("filingDate")),
                code=str(row["transactionCode"]) if row.get("transactionCode") else None,
                shares_changed=shares,
                price=price,
                as_of=now,
                source=self.name,
            ))
        return out

    async def news(self, symbols: list[str] | None = None, *, limit: int = 25) -> list[NewsArticle]:
        rows: list[dict[str, Any]]
        if symbols:
            today = datetime.now(UTC).date()
            rows = await self._get(
                "/company-news", symbol=symbols[0].upper(),
                **{"from": (today - timedelta(days=5)).isoformat(), "to": today.isoformat()},
            )
        else:
            rows = await self._get("/news", category="general")
        articles: list[NewsArticle] = []
        for row in (rows or [])[:limit]:
            published = self._ts_from_epoch(row.get("datetime"))
            if published is None:
                continue
            articles.append(
                NewsArticle(
                    headline=row.get("headline", ""),
                    summary=row.get("summary") or None,
                    url=row.get("url"),
                    symbols=[row["related"].upper()] if row.get("related") else [],
                    published_at=published,
                    source=f"{self.name}:{row.get('source', 'unknown')}",
                )
            )
        return articles

    async def earnings(self, *, days_ahead: int = 14,
                       symbols: list[str] | None = None) -> list[EarningsEvent]:
        today = datetime.now(UTC).date()
        payload = await self._get(
            "/calendar/earnings",
            **{"from": today.isoformat(), "to": (today + timedelta(days=days_ahead)).isoformat()},
        )
        wanted = {s.upper() for s in symbols} if symbols else None
        events: list[EarningsEvent] = []
        now = self._now()
        for row in payload.get("earningsCalendar", []) or []:
            sym = (row.get("symbol") or "").upper()
            if not sym or (wanted is not None and sym not in wanted):
                continue
            try:
                report = date.fromisoformat(row["date"])
            except (KeyError, ValueError):
                continue
            events.append(
                EarningsEvent(
                    symbol=sym,
                    report_date=report,
                    time_hint=row.get("hour") or None,
                    eps_estimate=row.get("epsEstimate"),
                    eps_actual=row.get("epsActual"),
                    revenue_estimate=row.get("revenueEstimate"),
                    revenue_actual=row.get("revenueActual"),
                    as_of=now,
                    source=self.name,
                )
            )
        return events

    async def economic_calendar(self, *, days_ahead: int = 7) -> list[EconomicEvent]:
        payload = await self._get("/calendar/economic")
        horizon = datetime.now(UTC) + timedelta(days=days_ahead)
        events: list[EconomicEvent] = []
        now = self._now()
        for row in payload.get("economicCalendar", []) or []:
            raw_time = row.get("time")
            if not raw_time:
                continue
            try:
                scheduled = datetime.fromisoformat(str(raw_time).replace(" ", "T"))
            except ValueError:
                continue
            scheduled = ensure_aware(scheduled)
            if scheduled > horizon:
                continue
            events.append(
                EconomicEvent(
                    name=row.get("event", ""),
                    country=row.get("country", ""),
                    scheduled_at=scheduled,
                    importance=str(row.get("impact")) if row.get("impact") is not None else None,
                    actual=str(row.get("actual")) if row.get("actual") is not None else None,
                    forecast=str(row.get("estimate")) if row.get("estimate") is not None else None,
                    previous=str(row.get("prev")) if row.get("prev") is not None else None,
                    as_of=now,
                    source=self.name,
                )
            )
        return events
