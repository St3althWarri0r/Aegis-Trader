# Developer guide

## Setup

```bash
git clone https://github.com/St3althWarri0r/Poseidon && cd Poseidon
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
pytest            # ~1614 tests, a few seconds
ruff check src tests
mypy src          # strict mode
```

## Codebase tour

Start at `app.py` (`ApplicationKernel`) — the composition root — and follow
`run_review_cycle()`: it touches every major subsystem in order
(strategies → agent → persistence → order manager → risk → broker).
`docs/architecture.md` is the map; each package's `__init__.py`/module
docstrings state its contract.

Key invariants to preserve when changing code:

1. Market-data models always carry `as_of` + `source`; anything that can
   feed an order goes through `DataRouter` so staleness is enforced.
2. `OrderManager._process_order` is the only call path to
   `Broker.submit_order`. Do not add another.
3. Money is `Decimal` end to end.
4. New consequential actions get an `audit.append(...)`.
5. Exceptions: subclass `PoseidonError`, set `retryable` honestly.

## Testing

- `tests/unit/` — pure/isolated tests (vault, audit chain, clock, config,
  bus, router failover, every risk rule, circuit breaker, paper broker,
  agent decision parsing, strategy math, backtest suite).
- `tests/integration/` — real components wired together over the fake
  data provider: full order flow in all three modes, approval round-trips,
  duplicate prevention, risk rejection, baseline persistence.
- `tests/conftest.py` provides `FakeProvider` (configurable quotes/bars/
  failures/staleness) — use it rather than mocking HTTP.
- Conventions: pytest-asyncio auto mode (plain `async def` tests); no
  network in tests; freeze/patch the market clock via
  `patch.object(MarketClock, "session", ...)` when a test needs the market
  "open".

Broker simulation: `PaperBroker` *is* the broker simulator and is used by
the integration suite. Backtests: `BacktestEngine` + `monte_carlo` /
`walk_forward` / `stress_test` in `poseidon.backtest`.

```python
from poseidon.backtest.engine import BacktestEngine, BacktestConfig
from poseidon.backtest.analysis import monte_carlo, walk_forward, stress_test
result = await BacktestEngine(BacktestConfig()).run(strategy, history)  # history: dict[symbol, list[Bar]]
print(result.summary(), monte_carlo(result, runs=1000).median_return)
```

## Style

- Python 3.11+, `from __future__ import annotations`, full type hints
  (mypy strict).
- ruff config in `pyproject.toml` (line length 100); run `ruff check
  --fix` before committing.
- structlog everywhere; never log secrets (the redactor is a backstop, not
  permission).
- Docstrings explain *contracts and why*, not what the next line does.

## Dashboard palette

The UI uses a validated dark-surface palette: page `#0d0d0d`, surface
`#1a1a19`, ink `#ffffff`/`#c3c2b7`/muted `#898781`, gridline `#2c2c2a`,
single-series blue `#3987e5`, status good `#0ca30c` / warning `#fab219` /
serious `#ec835a` / critical `#d03b3b`. Rules baked into the CSS: text
always wears ink tokens (color chips carry state, never text color alone
except the paired P&L values), one axis per chart, thin marks, hairline
grid, tabular numerals in tables/axes.

## Release / CI

GitHub Actions (`.github/workflows/ci.yml`) runs ruff, mypy (strict), and
pytest on 3.11, 3.12 and 3.14 and builds the Docker image on every push. Version
lives in
`poseidon/__init__.py` + `pyproject.toml`; the PKGBUILD derives
`pkgver` from `pyproject.toml`.
