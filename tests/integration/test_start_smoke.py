"""Actually execute ApplicationKernel.start(). Nothing else does.

This is the gap that let a guaranteed startup crash through a green gate: the
boot-time mode clamp read ``self.order_manager`` fifteen lines before it was
assigned, and 1624 tests on two interpreters plus CI all passed, because no
test drives ``start()`` far enough to build a broker. mypy could not see it
either — ``ApplicationKernel`` declares those attributes with a bare annotation
and binds them only inside ``start()``, so an annotated-but-unassigned
attribute type-checks fine at every read.

``test_start_attribute_ordering.py`` guards that bug class statically. This
guards it dynamically, which is strictly stronger: it runs the real bootstrap
end to end — DB open, audit-chain verify, router, broker, risk engine, order
manager, portfolio sync, scheduler, dashboard — and then shuts it down.

Deliberately hermetic: temp data dir, temp vault, the built-in paper broker (no
credentials, no network), research mode (orders can never be submitted), and an
ephemeral dashboard port.
"""

from __future__ import annotations

import asyncio
import socket
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from poseidon.app import ApplicationKernel
from poseidon.core.config import (
    AppConfig,
    BrokerConfig,
    DashboardConfig,
    DataConfig,
    ProviderConfig,
)
from poseidon.core.enums import OrderSide, OrderStatus, TradingMode
from poseidon.core.events import Topics
from poseidon.core.models import Order
from poseidon.security.vault import Vault


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def _config(tmp_path: Path) -> AppConfig:
    return AppConfig(
        mode=TradingMode.RESEARCH,          # orders are structurally impossible
        data_dir=tmp_path / "data",
        config_path=tmp_path / "poseidon.yaml",
        brokers=[BrokerConfig(name="paper", enabled=True, primary=True,
                              options={"starting_cash": 100_000})],
        watchlists=[],
        # coinbase is keyless and is only CONSTRUCTED here, never called —
        # _build_router refuses to start with no providers at all.
        data=DataConfig(providers=[ProviderConfig(name="coinbase", priority=10)]),
        dashboard=DashboardConfig(host="127.0.0.1", port=_free_port()),
    )


@pytest.fixture
async def kernel(tmp_path: Path):
    cfg = _config(tmp_path)
    cfg.data_dir.mkdir(parents=True, exist_ok=True)
    vault = Vault(cfg.data_dir / "vault.bin")
    vault.create("smoke-test-passphrase")   # temp vault, throwaway secret
    # The default ai.backend is anthropic, which resolves this at wiring time.
    # Never called: research mode runs no cycle in this test.
    vault.set("anthropic_api_key", "sk-ant-not-a-real-key-smoke-test")
    k = ApplicationKernel(cfg, vault)
    await k.start()          # deliberately NOT inside try: a failure here must
    try:                     # surface as itself, not be masked by stop()'s own
        yield k              # AttributeError on attributes start() never bound
    finally:
        await k.stop()


async def test_start_completes_and_builds_the_whole_object_graph(kernel) -> None:
    """The regression was an AttributeError partway through start(). Assert the
    late-bound attributes all exist — reaching this line at all is most of the
    value."""
    for attr in ("broker", "risk", "approvals", "order_manager", "router",
                 "portfolio", "scheduler", "notifier", "dashboard"):
        assert getattr(kernel, attr, None) is not None, f"start() never bound self.{attr}"


async def test_boot_mode_is_the_configured_mode_on_a_paper_broker(kernel) -> None:
    """The clamp must not demote when the broker is paper — that would silently
    take the operator out of the mode they chose."""
    assert kernel.broker.is_paper
    assert kernel.order_manager.mode is TradingMode.RESEARCH


async def test_started_kernel_can_report_status(kernel) -> None:
    """A trivial end-to-end read: the dashboard's own status path must work on a
    freshly started kernel."""
    report = await kernel.status_report()
    assert report.get("mode") == TradingMode.RESEARCH.value


async def test_a_fill_triggers_an_immediate_portfolio_sync(kernel) -> None:
    """The snapshot the guardian and risk engine read must not lag a fill by a
    whole sync interval: an ORDER_FILLED event re-syncs the portfolio at once."""
    before = kernel.portfolio.synced_at
    assert before is not None  # start() ran the first sync
    await asyncio.sleep(0.01)  # a distinguishable later timestamp
    filled = Order(symbol="AAPL", side=OrderSide.BUY, quantity=Decimal("1"),
                   filled_quantity=Decimal("1"), status=OrderStatus.FILLED,
                   created_at=datetime.now(UTC))
    await kernel.bus.publish(Topics.ORDER_FILLED, {"order": filled.model_dump(mode="json")})
    for _ in range(200):  # the bus dispatches handlers as tasks; wait for the pass
        if kernel.portfolio.synced_at != before:
            break
        await asyncio.sleep(0.01)
    assert kernel.portfolio.synced_at is not None and kernel.portfolio.synced_at > before


async def test_a_burst_of_fills_coalesces_into_at_most_two_syncs(kernel) -> None:
    """Three fills published back to back must not queue three full sync passes
    (six broker calls each): the first runs, the rest fold into one rerun."""
    passes: list[int] = []
    real_sync = kernel.sync.sync_once

    async def counting_sync() -> None:
        passes.append(1)
        await asyncio.sleep(0.02)  # long enough for the other fills to land mid-pass
        await real_sync()

    kernel.sync.sync_once = counting_sync  # type: ignore[method-assign]
    payload = {"order": Order(symbol="AAPL", side=OrderSide.BUY, quantity=Decimal("1"),
                              filled_quantity=Decimal("1"), status=OrderStatus.FILLED,
                              created_at=datetime.now(UTC)).model_dump(mode="json")}
    for _ in range(3):
        await kernel.bus.publish(Topics.ORDER_FILLED, payload)
    for _ in range(200):
        await asyncio.sleep(0.01)
        if not kernel._fill_sync_running and passes:
            break
    await asyncio.sleep(0.05)
    assert 1 <= len(passes) <= 2
