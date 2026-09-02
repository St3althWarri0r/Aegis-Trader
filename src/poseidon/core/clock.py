"""Market clock and data-freshness policy.

Two safety-critical jobs live here:

1. Knowing whether US equity markets are open (regular / extended / closed),
   including weekends, full holidays, and half days, so the scheduler and risk
   engine can gate trading windows.
2. Enforcing the "live data only" contract: every timestamped datum is graded
   REAL_TIME / DELAYED / STALE, and STALE data is rejected upstream of the AI.

The holiday table is shipped for the current and next calendar year and is
validated at startup; if the table does not cover 'today', the market is
treated as CLOSED (fail-safe) and a critical notification is raised by the
watchdog rather than guessing.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from .enums import DataFreshness, MarketSession

EASTERN = ZoneInfo("America/New_York")

# NYSE full-day holidays. Source: NYSE published calendar. Kept two years deep;
# the watchdog raises a config alert when coverage drops below ~60 days.
FULL_HOLIDAYS: frozenset[date] = frozenset(
    {
        # 2026
        date(2026, 1, 1), date(2026, 1, 19), date(2026, 2, 16), date(2026, 4, 3),
        date(2026, 5, 25), date(2026, 6, 19), date(2026, 7, 3), date(2026, 9, 7),
        date(2026, 11, 26), date(2026, 12, 25),
        # 2027
        date(2027, 1, 1), date(2027, 1, 18), date(2027, 2, 15), date(2027, 3, 26),
        date(2027, 5, 31), date(2027, 6, 18), date(2027, 7, 5), date(2027, 9, 6),
        date(2027, 11, 25), date(2027, 12, 24),
    }
)

# Early closes (13:00 ET).
HALF_DAYS: frozenset[date] = frozenset(
    {
        date(2026, 11, 27), date(2026, 12, 24),
        date(2027, 11, 26),
    }
)

_CALENDAR_YEARS: frozenset[int] = frozenset(d.year for d in FULL_HOLIDAYS)

REGULAR_OPEN = time(9, 30)
REGULAR_CLOSE = time(16, 0)
HALF_DAY_CLOSE = time(13, 0)
PRE_MARKET_OPEN = time(4, 0)
AFTER_HOURS_CLOSE = time(20, 0)
HALF_DAY_AFTER_HOURS_CLOSE = time(17, 0)  # post-market ends 17:00 ET on early-close days


def utc_now() -> datetime:
    return datetime.now(UTC)


def ensure_aware(dt: datetime) -> datetime:
    """Treat a naive timestamp as UTC — never as a different, unstated zone."""
    return dt if dt.tzinfo is not None else dt.replace(tzinfo=UTC)


def calendar_covers(day: date) -> bool:
    return day.year in _CALENDAR_YEARS


# Grace given to a portfolio snapshot before the ABSENCE of an order/position
# in it is believed: a sync pass whose broker fetches straddled a submission
# or a fill can stamp ``synced_at`` after that event yet still miss it (its
# fetches started before the event landed). Shared by the risk engine's
# pending-exposure reconciliation (``RiskEngine._reconcile_pending``) and the
# guardian's stale-snapshot disarm guard so the two grace windows can never
# drift out of step with each other.
SYNC_GRACE = timedelta(seconds=10)


def synced_after(synced_at: datetime | None, reference_at: datetime,
                 *, grace: timedelta = SYNC_GRACE) -> bool:
    """True only when ``synced_at`` was taken at least ``grace`` after
    ``reference_at`` — the only snapshot whose missing order/position can be
    trusted. A snapshot never taken (``None``) proves nothing."""
    if synced_at is None:
        return False
    return ensure_aware(synced_at) >= ensure_aware(reference_at) + grace


@dataclass(frozen=True)
class FreshnessPolicy:
    """Thresholds (seconds) for grading data age. Configurable per deployment.

    Crypto trades 24/7 and its quotes arrive over a public REST cadence looser
    than a co-located equity feed, so crypto has its OWN real-time window
    (``crypto_real_time_max_age``, default 60s) selected via ``grade(...,
    is_crypto=True)``. Equities keep the strict default window — this is a
    real-money safety mechanism and is never weakened for the equity path.
    """

    real_time_max_age: float = 5.0
    crypto_real_time_max_age: float = 60.0  # looser 24/7 REST-cadence window for crypto
    delayed_max_age: float = 900.0  # 15 minutes — typical delayed-feed window

    def grade(self, as_of: datetime, *, is_crypto: bool = False,
              now: datetime | None = None) -> DataFreshness:
        now = now or utc_now()
        if as_of.tzinfo is None:
            # Naive timestamps are untrustworthy: treat as stale, never assume.
            return DataFreshness.STALE
        real_time_max = self.crypto_real_time_max_age if is_crypto else self.real_time_max_age
        age = (now - as_of).total_seconds()
        if age < 0:
            # Clock skew from a provider; small negative ages are tolerated.
            return DataFreshness.REAL_TIME if age > -5 else DataFreshness.STALE
        if age <= real_time_max:
            return DataFreshness.REAL_TIME
        if age <= self.delayed_max_age:
            return DataFreshness.DELAYED
        return DataFreshness.STALE


class MarketClock:
    def __init__(self, *, tz: ZoneInfo = EASTERN) -> None:
        self._tz = tz

    def now_eastern(self) -> datetime:
        return datetime.now(self._tz)

    def session(self, at: datetime | None = None) -> MarketSession:
        moment = (at or utc_now()).astimezone(self._tz)
        day = moment.date()
        if not calendar_covers(day):
            return MarketSession.CLOSED  # fail-safe: unknown calendar year
        if moment.weekday() >= 5 or day in FULL_HOLIDAYS:
            return MarketSession.CLOSED
        close = HALF_DAY_CLOSE if day in HALF_DAYS else REGULAR_CLOSE
        t = moment.time()
        if REGULAR_OPEN <= t < close:
            return MarketSession.REGULAR
        if PRE_MARKET_OPEN <= t < REGULAR_OPEN:
            return MarketSession.PRE_MARKET
        ah_close = HALF_DAY_AFTER_HOURS_CLOSE if day in HALF_DAYS else AFTER_HOURS_CLOSE
        if close <= t < ah_close:
            return MarketSession.AFTER_HOURS
        return MarketSession.CLOSED

    def is_trading_day(self, day: date | None = None) -> bool:
        day = day or self.now_eastern().date()
        return calendar_covers(day) and day.weekday() < 5 and day not in FULL_HOLIDAYS

    def next_open(self, at: datetime | None = None) -> datetime:
        """Next regular-session open, in UTC."""
        moment = (at or utc_now()).astimezone(self._tz)
        candidate = moment.date()
        for _ in range(30):
            open_dt = datetime.combine(candidate, REGULAR_OPEN, tzinfo=self._tz)
            if self.is_trading_day(candidate) and open_dt > moment:
                return open_dt.astimezone(UTC)
            candidate += timedelta(days=1)
        raise RuntimeError("no trading day found within 30 days — check holiday calendar")
