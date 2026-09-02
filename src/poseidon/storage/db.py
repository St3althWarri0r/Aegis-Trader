"""Async SQLite store.

One database file under the data directory holds durable runtime state:
orders, fills, AI decisions, approvals, audit chain, equity marks, and a
small key-value table for crash recovery. All in-process access (trading
and dashboard alike) is serialized through the single aiosqlite
connection's worker thread; WAL mode is kept for its faster commits and so
external readers (e.g. the ``poseidon audit``/``doctor`` CLI commands, which
open their own connections) don't block the trading path's writes.

Filesystem-level encryption of the data directory is documented in
docs/security.md (the vault covers credentials; position/order history is
protected by directory permissions plus optional fscrypt/LUKS).
"""

from __future__ import annotations

import asyncio
import json
import os
from collections.abc import AsyncIterator, Iterable
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import aiosqlite

from ..core.models import AnalysisPacket, StrategyHealth, TradeLesson

# Kept separate from _SCHEMA because the account_scope migration rebuilds the
# table (SQLite cannot add a column to a PRIMARY KEY in place) and must create
# the exact same shape a fresh database gets.
_STRATEGY_HEALTH_DDL = """
CREATE TABLE IF NOT EXISTS strategy_health (
    strategy TEXT NOT NULL,
    account_scope TEXT NOT NULL DEFAULT '',  -- matches orders: paper verdicts never leak into live
    state TEXT NOT NULL,
    payload TEXT NOT NULL,
    last_trade_at TEXT,                      -- newest closed trade the assessment saw (evidence watermark)
    updated_at TEXT NOT NULL,
    PRIMARY KEY (strategy, account_scope)
);
"""

_SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS kv (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS orders (
    id TEXT PRIMARY KEY,
    client_order_id TEXT NOT NULL UNIQUE,
    broker TEXT NOT NULL,
    broker_order_id TEXT,
    account_scope TEXT NOT NULL DEFAULT '',  -- broker:paper|live; keeps paper and live fills from mixing in reports
    payload TEXT NOT NULL,          -- full Order JSON
    status TEXT NOT NULL,
    decision_id TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
CREATE INDEX IF NOT EXISTS idx_orders_created ON orders(created_at);
-- idx_orders_scope_status_updated is created in Database.open(): it covers
-- account_scope, which pre-v2.4.0 databases only gain from the additive
-- migration that runs after this script.

CREATE TABLE IF NOT EXISTS fills (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id TEXT NOT NULL,
    payload TEXT NOT NULL,
    filled_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS decisions (
    id TEXT PRIMARY KEY,
    cycle_id TEXT NOT NULL,
    action TEXT NOT NULL,
    payload TEXT NOT NULL,          -- full Decision JSON incl. rationale
    created_at TEXT NOT NULL,
    resolved_at TEXT,               -- outcome-resolution marker (NULL = pending)
    resolution TEXT                 -- small status JSON: status + rounded numbers, never prose
);
CREATE INDEX IF NOT EXISTS idx_decisions_created ON decisions(created_at);
-- No resolution index: decisions grow one row per review cycle, and the
-- outcome sweep's created_at range scan is already served by
-- idx_decisions_created.

CREATE TABLE IF NOT EXISTS equity_marks (
    at TEXT PRIMARY KEY,
    equity TEXT NOT NULL,
    cash TEXT NOT NULL,
    day_pnl TEXT,
    broker TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS exit_plans (
    symbol TEXT PRIMARY KEY,
    decision_id TEXT NOT NULL,
    stop_loss TEXT,
    take_profit TEXT,
    time_stop TEXT,
    quantity TEXT NOT NULL,
    active INTEGER NOT NULL DEFAULT 1,
    triggered_reason TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    broker TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS ai_usage (
    cycle_id TEXT PRIMARY KEY,
    at TEXT NOT NULL,
    input_tokens INTEGER NOT NULL DEFAULT 0,
    output_tokens INTEGER NOT NULL DEFAULT 0,
    cache_read_tokens INTEGER NOT NULL DEFAULT 0,
    cache_write_tokens INTEGER NOT NULL DEFAULT 0,
    api_calls INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_ai_usage_at ON ai_usage(at);

CREATE TABLE IF NOT EXISTS algorithms (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    description TEXT NOT NULL DEFAULT '',
    source TEXT NOT NULL,
    symbols TEXT NOT NULL DEFAULT '[]',   -- JSON list; empty = watchlist
    params TEXT NOT NULL DEFAULT '{}',    -- JSON dict passed as ctx.params
    status TEXT NOT NULL DEFAULT 'draft', -- draft | active | archived
    created_by TEXT NOT NULL DEFAULT 'user',  -- user | claude
    review_notes TEXT NOT NULL DEFAULT '',
    sleeve_pct REAL NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS audit (
    seq INTEGER PRIMARY KEY,
    at TEXT NOT NULL,
    actor TEXT NOT NULL,
    action TEXT NOT NULL,
    payload TEXT NOT NULL,
    prev_hash TEXT NOT NULL,
    hash TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS chat_messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    role TEXT NOT NULL,        -- user | assistant
    content TEXT NOT NULL,
    created_at TEXT NOT NULL
);

-- Advisory post-trade lessons (reflection memory). NOT the tamper-evident
-- audit chain: this is retrospective prose fed back to the AI as context.
CREATE TABLE IF NOT EXISTS trade_lessons (
    id TEXT PRIMARY KEY,
    symbol TEXT NOT NULL,
    strategy TEXT NOT NULL DEFAULT '',
    decision_id TEXT,
    entered_at TEXT NOT NULL,
    exited_at TEXT NOT NULL,
    realized_return REAL NOT NULL,
    alpha REAL,
    holding_days REAL NOT NULL,
    lesson TEXT NOT NULL,
    model TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    kind TEXT NOT NULL DEFAULT 'trade'  -- MUST stay the LAST column: read positionally (r[12])
);
CREATE INDEX IF NOT EXISTS idx_trade_lessons_symbol ON trade_lessons(symbol, created_at);
-- Backs the cross-ticker arm of recent_lessons (no symbol equality, so the
-- (symbol, created_at) index above cannot serve it).
CREATE INDEX IF NOT EXISTS idx_trade_lessons_created ON trade_lessons(created_at);

CREATE TABLE IF NOT EXISTS analysis_packets (
    id TEXT PRIMARY KEY,
    symbol TEXT NOT NULL,
    as_of TEXT NOT NULL,
    model TEXT NOT NULL DEFAULT '',
    payload TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_analysis_packets_symbol ON analysis_packets(symbol, as_of);

""" + _STRATEGY_HEALTH_DDL


def _row_to_lesson(r: tuple[Any, ...]) -> TradeLesson:
    return TradeLesson(
        id=r[0], symbol=r[1], strategy=r[2], decision_id=r[3],
        entered_at=datetime.fromisoformat(r[4]), exited_at=datetime.fromisoformat(r[5]),
        realized_return=r[6], alpha=r[7], holding_days=r[8], lesson=r[9],
        model=r[10], created_at=datetime.fromisoformat(r[11]), kind=r[12])


def _row_to_packet(row: Any) -> AnalysisPacket:
    # columns: id, symbol, as_of, model, payload, created_at
    return AnalysisPacket.model_validate_json(row[4])


def _row_to_health(row: Any) -> StrategyHealth:
    # columns: payload
    return StrategyHealth.model_validate_json(row[0])


class Database:
    def __init__(self, path: Path) -> None:
        self._path = path
        self._conn: aiosqlite.Connection | None = None
        # Serializes writers against each other AND against multi-statement
        # transactions, so an unrelated execute()/commit() can't interleave
        # with (and prematurely commit / roll back) an open transaction.
        self._write_lock = asyncio.Lock()

    async def open(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        # Create the db 0600 up front (the mode only applies on create): SQLite
        # copies the db file's mode onto the -wal/-shm sidecars it creates, so
        # this also keeps them from being briefly world-readable.
        os.close(os.open(self._path, os.O_RDWR | os.O_CREAT, 0o600))
        self._conn = await aiosqlite.connect(self._path)
        await self._conn.executescript(_SCHEMA)
        # Additive migrations for databases created before a column existed.
        # Checked explicitly (rather than suppressing OperationalError) so a
        # transient failure such as 'database is locked' cannot silently skip
        # a migration the broker-scoping safety logic depends on.
        if not await self._column_exists("algorithms", "sleeve_pct"):
            await self._conn.execute(
                "ALTER TABLE algorithms ADD COLUMN sleeve_pct REAL NOT NULL DEFAULT 0"
            )
        if not await self._column_exists("equity_marks", "broker"):
            # Equity marks are broker-scoped so a paper account's history can
            # never leak into a real account's drawdown/performance after a
            # broker switch. Legacy rows keep broker='' and are excluded.
            await self._conn.execute(
                "ALTER TABLE equity_marks ADD COLUMN broker TEXT NOT NULL DEFAULT ''"
            )
        if not await self._column_exists("exit_plans", "broker"):
            # Guardian exit plans are broker-scoped for the same reason: a
            # paper-era stop must never fire against a real account. Legacy
            # rows ('') still match the active broker until re-armed.
            await self._conn.execute(
                "ALTER TABLE exit_plans ADD COLUMN broker TEXT NOT NULL DEFAULT ''"
            )
        if not await self._column_exists("orders", "account_scope"):
            # Orders are account-scoped like equity marks: the same plugin's
            # paper and live fills must never FIFO-match in the performance
            # report. Legacy rows keep '' and drop out of scoped reports.
            await self._conn.execute(
                "ALTER TABLE orders ADD COLUMN account_scope TEXT NOT NULL DEFAULT ''"
            )
        # Outcome-resolution columns. Appended in the exact fresh-DDL order so
        # migrated and fresh databases agree column-for-column: recent_lessons
        # reads trade_lessons positionally through SELECT * / _row_to_lesson.
        if not await self._column_exists("decisions", "resolved_at"):
            await self._conn.execute(
                "ALTER TABLE decisions ADD COLUMN resolved_at TEXT"
            )
        if not await self._column_exists("decisions", "resolution"):
            await self._conn.execute(
                "ALTER TABLE decisions ADD COLUMN resolution TEXT"
            )
        if not await self._column_exists("trade_lessons", "kind"):
            # Pre-taxonomy lessons are all executed-trade reflections.
            await self._conn.execute(
                "ALTER TABLE trade_lessons ADD COLUMN kind TEXT NOT NULL DEFAULT 'trade'"
            )
        if not await self._column_exists("strategy_health", "account_scope"):
            # Strategy-health verdicts are account-scoped like the fills that
            # feed them (orders.account_scope): a paper-era retire_recommended
            # must not survive a broker switch or seed a live account's
            # hysteresis streaks. account_scope joins the PRIMARY KEY, which
            # SQLite only allows via a table rebuild; legacy rows keep
            # scope='' and drop out of scoped reads, matching the orders
            # convention. Statement-by-statement inside the surrounding
            # transaction so a failure cannot leave the table half-rebuilt.
            await self._conn.execute(
                "ALTER TABLE strategy_health RENAME TO strategy_health_legacy"
            )
            await self._conn.execute(_STRATEGY_HEALTH_DDL)
            await self._conn.execute(
                "INSERT INTO strategy_health "
                "(strategy, account_scope, state, payload, updated_at) "
                "SELECT strategy, '', state, payload, updated_at "
                "FROM strategy_health_legacy"
            )
            await self._conn.execute("DROP TABLE strategy_health_legacy")
        # Indexes over migrated columns must be created after the migrations:
        # in _SCHEMA they would fail on a pre-migration database (IF NOT EXISTS
        # only guards the index name, not missing columns). Makes the
        # reflection sweep's "newer than watermark" bound efficient.
        await self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_orders_scope_status_updated "
            "ON orders(account_scope, status, updated_at)"
        )
        await self._conn.commit()
        # Databases (and sidecars) created by older versions must not stay
        # world readable; os.open's mode does not apply to existing files.
        self._path.chmod(0o600)
        for suffix in ("-wal", "-shm"):
            sidecar = self._path.with_name(self._path.name + suffix)
            if sidecar.exists():
                sidecar.chmod(0o600)

    async def _column_exists(self, table: str, column: str) -> bool:
        cursor = await self.conn.execute(f"PRAGMA table_info({table})")
        rows = await cursor.fetchall()
        return any(row[1] == column for row in rows)

    async def close(self) -> None:
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    @property
    def conn(self) -> aiosqlite.Connection:
        if self._conn is None:
            raise RuntimeError("database is not open")
        return self._conn

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[aiosqlite.Connection]:
        # Held for the whole block so no other writer commits inside it.
        # Callers must use only raw ``conn.execute`` here — never db.execute()
        # or db.transaction() again — or they self-deadlock on this lock.
        async with self._write_lock:
            conn = self.conn
            try:
                yield conn
                await conn.commit()
            except BaseException:
                await conn.rollback()
                raise

    async def execute(self, sql: str, params: Iterable[Any] = ()) -> int:
        """Returns the affected row count — callers doing a compare-and-swap
        UPDATE (a WHERE clause pinned to a previously-read value) use this to
        detect a concurrent write and abandon a decision made against stale
        state, rather than blindly trusting their own read."""
        async with self._write_lock:
            cursor = await self.conn.execute(sql, tuple(params))
            await self.conn.commit()
            return cursor.rowcount

    async def fetch_all(self, sql: str, params: Iterable[Any] = ()) -> list[tuple[Any, ...]]:
        cursor = await self.conn.execute(sql, tuple(params))
        rows = await cursor.fetchall()
        return [tuple(r) for r in rows]

    async def fetch_one(self, sql: str, params: Iterable[Any] = ()) -> tuple[Any, ...] | None:
        cursor = await self.conn.execute(sql, tuple(params))
        row = await cursor.fetchone()
        return tuple(row) if row else None

    # -- kv helpers (crash recovery / small state) ---------------------------

    async def kv_set(self, key: str, value: Any) -> None:
        await self.execute(
            "INSERT INTO kv (key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, json.dumps(value, default=str)),
        )

    async def kv_get(self, key: str, default: Any = None) -> Any:
        row = await self.fetch_one("SELECT value FROM kv WHERE key = ?", (key,))
        return json.loads(row[0]) if row else default

    # -- trade lessons (advisory reflection memory; NOT the audit chain) -------

    async def add_trade_lesson(self, lesson: TradeLesson) -> None:
        await self.execute(
            "INSERT OR REPLACE INTO trade_lessons (id, symbol, strategy, decision_id, "
            "entered_at, exited_at, realized_return, alpha, holding_days, lesson, model, "
            "created_at, kind) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (lesson.id, lesson.symbol, lesson.strategy, lesson.decision_id,
             lesson.entered_at.isoformat(), lesson.exited_at.isoformat(),
             lesson.realized_return, lesson.alpha, lesson.holding_days,
             lesson.lesson, lesson.model, lesson.created_at.isoformat(), lesson.kind),
        )

    async def lesson_exists(self, symbol: str, entered_at: datetime,
                            exited_at: datetime) -> bool:
        row = await self.fetch_one(
            "SELECT 1 FROM trade_lessons WHERE symbol = ? AND entered_at = ? "
            "AND exited_at = ? LIMIT 1",
            (symbol, entered_at.isoformat(), exited_at.isoformat()),
        )
        return row is not None

    async def recent_lessons(self, symbols: list[str], *, per_symbol: int,
                             global_n: int, lookback_days: int, limit: int,
                             now: datetime) -> list[TradeLesson]:
        cutoff = (now - timedelta(days=lookback_days)).isoformat()
        picked: dict[str, TradeLesson] = {}
        # Up to `per_symbol` newest lessons for each requested symbol.
        for symbol in symbols:
            rows = await self.fetch_all(
                "SELECT * FROM trade_lessons WHERE symbol = ? AND created_at >= ? "
                "ORDER BY created_at DESC LIMIT ?",
                (symbol, cutoff, per_symbol),
            )
            for r in rows:
                picked[r[0]] = _row_to_lesson(r)
        # Plus up to `global_n` newest lessons overall (cross-ticker).
        rows = await self.fetch_all(
            "SELECT * FROM trade_lessons WHERE created_at >= ? "
            "ORDER BY created_at DESC LIMIT ?",
            (cutoff, global_n),
        )
        for r in rows:
            picked[r[0]] = _row_to_lesson(r)
        ordered = sorted(picked.values(), key=lambda lsn: lsn.exited_at, reverse=True)
        return ordered[:limit]

    # -- decision outcome resolution (advisory markers; NOT the audit chain) ---

    async def unresolved_decisions(self, *, after: str, before: str,
                                   limit: int) -> list[tuple[str, str, str, str]]:
        """Pending decisions in ``(after, before]`` created-order, oldest first.

        ``after`` is the outcome watermark (a static floor: pre-feature history
        is never scanned) and ``before`` the calendar pre-filter for decisions
        whose forward bars could exist. Served by ``idx_decisions_created``.
        """
        rows = await self.fetch_all(
            "SELECT id, action, payload, created_at FROM decisions "
            "WHERE resolved_at IS NULL AND created_at > ? AND created_at <= ? "
            "ORDER BY created_at ASC LIMIT ?",
            (after, before, limit),
        )
        return [(str(r[0]), str(r[1]), str(r[2]), str(r[3])) for r in rows]

    async def mark_decision_resolved(self, decision_id: str, *, resolved_at_iso: str,
                                     resolution_json: str) -> None:
        """Stamp a decision's outcome marker. ``resolution_json`` is a small
        status JSON (status string + rounded numbers) — never model or thesis
        prose. A single ``execute`` on purpose: ``db.transaction()`` would
        self-deadlock on the write lock (see ``transaction``)."""
        await self.execute(
            "UPDATE decisions SET resolved_at = ?, resolution = ? WHERE id = ?",
            (resolved_at_iso, resolution_json, decision_id),
        )

    # -- analysis packets (advisory debate packet; NOT the audit chain) --------

    async def add_analysis_packet(self, packet: AnalysisPacket) -> None:
        await self.execute(
            "INSERT OR REPLACE INTO analysis_packets "
            "(id, symbol, as_of, model, payload, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (packet.id, packet.symbol, packet.as_of.isoformat(), packet.model,
             packet.model_dump_json(), datetime.now(UTC).isoformat()),
        )

    async def packet_fresh(self, symbol: str, *, refresh_hours: int,
                           now: datetime) -> bool:
        cutoff = (now - timedelta(hours=refresh_hours)).isoformat()
        row = await self.fetch_one(
            "SELECT 1 FROM analysis_packets WHERE symbol = ? AND as_of >= ? LIMIT 1",
            (symbol, cutoff))
        return row is not None

    async def recent_packets(self, symbols: list[str], *, refresh_hours: int,
                             limit: int, now: datetime) -> list[AnalysisPacket]:
        cutoff = (now - timedelta(hours=refresh_hours)).isoformat()
        picked: dict[str, AnalysisPacket] = {}
        for symbol in symbols:                       # freshest packet per symbol
            row = await self.fetch_one(
                "SELECT * FROM analysis_packets WHERE symbol = ? AND as_of >= ? "
                "ORDER BY as_of DESC LIMIT 1", (symbol, cutoff))
            if row is not None:
                picked[symbol] = _row_to_packet(row)
        ordered = sorted(picked.values(), key=lambda p: p.as_of, reverse=True)
        return ordered[:limit]

    async def prune_advisory(self, *, lesson_lookback_days: int,
                             packet_refresh_hours: int, now: datetime) -> tuple[int, int]:
        """Delete advisory rows no reader can return anymore; never the audit chain.

        Retention derives from the reader windows with a 2x safety margin:
        lessons older than twice the reflection lookback (recent_lessons filters
        on ``created_at >= now - lookback_days``) and packets whose ``as_of`` is
        older than twice the analysis refresh window (packet_fresh/recent_packets
        filter on ``as_of >= now - refresh_hours``). Returns
        (lessons_deleted, packets_deleted) so the caller can log the sweep.
        """
        lesson_cutoff = (now - timedelta(days=2 * lesson_lookback_days)).isoformat()
        packet_cutoff = (now - timedelta(hours=2 * packet_refresh_hours)).isoformat()
        async with self.transaction() as conn:
            cur = await conn.execute(
                "DELETE FROM trade_lessons WHERE created_at < ?", (lesson_cutoff,))
            lessons_deleted = cur.rowcount
            cur = await conn.execute(
                "DELETE FROM analysis_packets WHERE as_of < ?", (packet_cutoff,))
            packets_deleted = cur.rowcount
        return lessons_deleted, packets_deleted

    # -- strategy health (advisory decay state; NOT the audit chain) -----------

    async def upsert_strategy_health(self, h: StrategyHealth, *, account_scope: str = "",
                                     last_trade_at: datetime | None = None) -> None:
        """`last_trade_at` is the newest closed trade the assessment saw — the
        decay watchdog's new-evidence watermark. It lives on the row (not a
        side channel) so it commits atomically with the state it justifies."""
        await self.execute(
            "INSERT OR REPLACE INTO strategy_health "
            "(strategy, account_scope, state, payload, last_trade_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (h.strategy, account_scope, h.state, h.model_dump_json(),
             last_trade_at.isoformat() if last_trade_at is not None else None,
             h.updated_at.isoformat()))

    async def get_strategy_health(self, strategy: str, *,
                                  account_scope: str = "") -> StrategyHealth | None:
        row = await self.fetch_one(
            "SELECT payload FROM strategy_health WHERE strategy = ? AND account_scope = ?",
            (strategy, account_scope))
        return _row_to_health(row) if row else None

    async def strategy_health_last_trade_at(self, strategy: str, *,
                                            account_scope: str = "") -> datetime | None:
        row = await self.fetch_one(
            "SELECT last_trade_at FROM strategy_health "
            "WHERE strategy = ? AND account_scope = ?",
            (strategy, account_scope))
        return datetime.fromisoformat(row[0]) if row and row[0] else None

    async def list_strategy_health(self, *, account_scope: str = "") -> list[StrategyHealth]:
        rows = await self.fetch_all(
            "SELECT payload FROM strategy_health WHERE account_scope = ? ORDER BY strategy",
            (account_scope,))
        return [_row_to_health(r) for r in rows]
