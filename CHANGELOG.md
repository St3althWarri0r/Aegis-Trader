# Changelog

All notable, user-facing changes to Poseidon. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); releases are also
published as GitHub release notes.

## [2.19.0] — 2026-09-01

A full-codebase audit release. Every safety-critical path — the order
manager, risk rules and engine, the position guardian, the AI agent loop and
tool dispatcher, both model backends, the Alpaca and paper brokers, portfolio
sync, the kernel's control paths, the dashboard API, config, vault, audit
chain, data router, SSRF guard, launcher and updater — was re-read end to
end, the dependency stack brought current, and every defect the reading
surfaced fixed with a regression test. No new data sources; the engine had
been idle since 2026-08-16, so everything here came from the code, not from
an incident.

### Safety

- **A freshly armed stop can no longer be disarmed by a stale portfolio
  snapshot.** A fill arms a guardian plan the instant the order poller sees
  it, but the snapshot the guardian checks positions against refreshes only
  on the sync interval. The guardian's next tick (15s in the scalping config)
  therefore routinely ran against a snapshot older than the fill, found "no
  position", and deactivated the brand-new stop as *position no longer held*
  — leaving each fresh entry unprotected until a later decision happened to
  re-arm it. A missing position is now believed only from a snapshot taken at
  least 10s after the plan was armed (the same grace the risk engine gives an
  order that lands mid-sync-pass); an older snapshot keeps the plan armed and
  the guardian retries next tick.
- **The portfolio re-syncs on every fill.** An `ORDER_FILLED` event now
  triggers an immediate sync pass, so a new position is visible to the
  guardian, the risk engine's pending-exposure reconciliation and the
  dashboard within about a second instead of up to a full sync interval.
- **Clearing an opposing resting BUY now consults the broker's live book and
  waits for the cancel to confirm.** The v2.18.0 wash-trade fix canceled only
  the BUYs Poseidon's own orders table listed as open, and submitted the SELL
  the instant the broker acknowledged the cancel request. Alpaca's DELETE
  merely *queues* a cancel, and its self-trade block does not care who placed
  the resting buy, so both gaps could reproduce the very 403 the fix exists to
  prevent. The clear now merges the live open-order book with the local rows
  (a live-only order is canceled and audited, never persisted as a synthetic
  row) and polls each cancel to a terminal state — bounded to ~3s — before the
  exit goes out; an unconfirmed cancel is audited as
  `exit.opposing_cancel_unconfirmed` and the exit still proceeds.

### Changed

- **Anthropic SDK 1.x.** `anthropic>=1,<2` (1.0.0 shipped 2026-08-20 with an
  unbounded `>=0.49` pin in place; the whole suite and strict mypy were
  verified against 1.3.0 before pinning). The SDK's HTTP layer is now
  `httpx2` — the maintained fork of `httpx` by its original author, published
  by Pydantic (github.com/pydantic/httpx2) — which arrives transitively; the
  platform's own `httpx` clients (providers, brokers, the local-model backend)
  are unchanged and the two coexist. The deployment's updater installs the
  new packages on the next launch.
- **Default Claude model is `claude-opus-5`** (same $5 / $25 per MTok metering
  defaults as `claude-opus-4-8`). The dashboard's curated menu is now
  `claude-opus-5`, `claude-opus-4-8`, `claude-sonnet-5`. Haiku 4.5 is removed
  from the menu: it rejects the adaptive-thinking + effort request shape the
  Anthropic backend sends on every completion, so selecting it — as the
  primary or as `ai.utility_model` — produced a 400 on the first call. The
  documented cheap utility tier is now `claude-sonnet-5`.
- **`cryptography>=50`** — PYSEC-2026-3552 / CVE-2026-69247 (a Bleichenbacher
  oracle in PKCS#7 EnvelopedData decryption). Poseidon's vault uses Fernet +
  scrypt only and never touched the affected API, so nothing was exposed; the
  floor moves so a dependency scan of a deployment comes back clean.
- CI now also runs on Python 3.14 (the interpreter the deployment runs on;
  float-summation and other CPython-version-specific behavior has bitten
  before).
- `submit_decision`'s trade schema accepts `asset_class: "crypto"` so a
  BASE/USD proposal is labelled honestly (a slash-form symbol was, and still
  is, tagged crypto regardless of the label).
- `EventBus.publish_nowait` for synchronous fire-and-forget publishes; the
  risk engine's circuit-breaker trip uses it instead of a bare
  `create_task` whose result nothing held.

## [2.18.0] — 2026-08-15

The constant-scalping release, built the same night v2.17.0 shipped, from
live failures watched in real time on the paper account.

### Added

- **`ai.mandate`** — a bounded standing directive rendered as an OPERATOR
  MANDATE block in every cycle's user turn: the operator's word on HOW to run
  the book (posture, holding horizon, exit discipline). Advisory to the PM's
  process only; it states explicitly that it never authorizes exceeding risk
  limits. Collapsed to one printable line so a config edit cannot forge a
  platform prompt block. First use: the quick in/out scalping directive.

### Fixed

- **A SELL clears same-symbol resting BUYs before it reaches the broker.**
  Observed live: a stale GTC buy resting at Alpaca made every LINK/USD exit
  die on their self-trade block ("potential wash trade detected") — from the
  AI loop and the operator's manual ticket alike, leaving a $200k+ position
  un-closable. The platform never opens shorts, so an incoming SELL is always
  position-closing; the opposing resting buy is now canceled first (once,
  failures audited and skipped, cross-broker rows untouched). BUYs never
  clear anything, so a guardian's protective take-profit sell always
  survives a new entry.

## [2.17.0] — 2026-08-15

A hardening release: everything here came out of running v2.16.0 unattended
against the live paper account and fixing what the deployment's own logs and
audit sweeps surfaced. No new data sources; the platform's behavior under a
weak local model and a real broker's edge cases is the whole story.

### Safety

- **Autonomous mode no longer survives a restart onto a LIVE broker.** On
  boot, `autonomous` + a live (non-paper) broker clamps back to `approval`;
  crypto trading is additionally gated to paper brokers — enforced in code,
  not advisory docs.
- **Crypto stops are enforced 24/7.** The `PositionGuardian` used to return
  whenever the NYSE session wasn't REGULAR — a crypto position opened Friday
  evening went unwatched until Monday 09:30 ET. Crypto exit plans are now
  checked on every guardian tick; the equity session gate survives only for
  equities (an exchange that is closed cannot execute an exit).
- **A risk cap can never trap the operator in a position.** Exit orders that
  *reduce* exposure are exempt from the caps that would have blocked them
  (over-max positions were previously un-closable); short-option strike basis
  is pinned so the cap math cannot misprice the risk being reduced.
- **Alpaca order submission is treated as non-idempotent** (verified against
  the live paper API): failed submits are never blindly retried into a
  possible double fill.
- The algorithm-workshop sandbox escape via a crafted `__subclasses__` walk is
  closed, and a provider 404 is no longer classified as an outage (it
  penalty-boxed healthy providers).

### Added

- **Broker-side per-order caps, end to end.** `Broker.order_limits()` is a
  first-class interface: Alpaca's $200k-per-order crypto notional cap is
  preflighted broker-side (priced exactly as Alpaca prices it), surfaced to
  the AI through `get_risk_status` *and* deterministically in the cycle
  prompt, so the PM sizes within caps instead of discovering them as
  rejections.
- **Vault passphrase rotation** (`poseidon vault rekey`) with an operator
  runbook (`docs/security.md`).
- **A dashboard Settings view generated from the config schema** — every
  field, grouped and typed, with restart-required detection; plus fixes so it
  shows the saved value (not the stale running one) and never falsely flags
  `Decimal` settings as pending-restart.
- **Health escalation.** Component health transitions escalate silent→loud
  (desktop + log), and the health prober now exercises the model backend
  itself; `poseidon doctor` no longer reports a false green for a server that
  answers TCP but cannot serve the configured model.

### Fixed — the local-model brain

- **Stalled tool loops abort instead of burning the cycle.** A weak model
  alternating `get_portfolio`/`get_risk_status` burned all 24 tool iterations
  on data it already held (10 cycles in one overnight session). The dispatcher
  now annotates byte-identical repeat calls with an explicit `repeat_note`
  (real live data still returned), and after `ai.max_stalled_iterations`
  (default 3) consecutive all-repeat iterations the cycle aborts to an honest
  no-action decision.
- **Chat data tools no longer die of old age.** The AI Desk's per-cycle tool
  budget was never reset, so after ~64k chars of cumulative chat tool output
  every chat data tool returned "budget reached" until the engine restarted.
  Each operator message now starts a fresh budget.
- **Model-supplied symbols are validated at the tool boundary.** Crypto pairs
  can't reach the options provider (there are no listed crypto options —
  previously 100+ futile provider calls per session), the model is told the
  right symbol shape instead of a bare "unavailable", and malformed symbols
  are rejected before any provider sees them.
- **Transient generation failures are retried once, resampled.** LM Studio
  intermittently fails to parse its own model's output (~6% of cycles
  measured); the retry nudges temperature so an identical bad generation
  isn't replayed. Context overflow and auth stay fatal-fast, and the overflow
  error now names the remedy (reload the model with a larger context length).
- **The read timeout fits a partially-offloaded model** (600s; connect stays
  10s so a down server still fails fast), sizing works at any account size,
  strict tools are opt-in (`ai.strict_tools`) because not every server/model
  pair honors grammar-constrained decoding, and schema enforcement was
  restored on the local path so `submit_decision` keeps its structural
  guarantees.
- Three blockers that kept the trader idle: the regime gate, missing order
  fields, and retry resampling.

### Fixed — everything else

- The screener no longer nominates candidates the PM cannot quote.
- Startup crashed when the boot clamp read `order_manager` before it existed.
- A desktop alert whose body starts with "-" was silently dropped
  (notify-send parsed it as a flag).
- Dashboard asset cache busts on content hash, not release version.
- Guardian stop exits are marketable limits (never raw market orders);
  take-profits keep the passive limit price.

## [2.16.0] — 2026-08-13

Round 3 of the starred-repo cross-pollination. The sources this round —
**OpenBB** (AGPL-3.0) and **awesome-systematic-trading** (unlicensed) — could
not be copied from at all; everything here is implemented from each upstream
data source's own public API, with OpenBB used only as a map of what exists.

**Every new feature is off by default.**

### Fixed — Sharpe and Sortino assumed a zero risk-free rate

- `backtest/stats.py` hardcoded **rf = 0**, so a portfolio merely matching
  T-bills scored a healthy positive Sharpe instead of the honest zero — and the
  PM reads these numbers to choose trades. Both ratios now take
  `risk_free_annual`, and Sortino measures shortfall against the rate rather
  than against zero: a positive day that still loses to cash *is* a shortfall.
- The **live** performance report was fixed alongside the backtest one.
  `analytics/performance.py` had always accepted the parameter; its caller
  passed nothing. The report now also states the rate it used, because a Sharpe
  without its risk-free rate is an unfalsifiable number.
- The bootstrap confidence interval and every walk-forward fold are measured on
  the *same* rate as the headline. Left mixed, a report could print a point
  estimate outside its own confidence interval.
- **Three separate inline copies of the Sharpe formula** were consolidated onto
  `stats.sharpe_ratio`.
- A degenerate-benchmark guard (`sxx <= 0`) was interpreter-dependent: CPython
  3.12+ compensates float summation and yields exact zero for a constant
  series, while earlier versions leave ~1e-35 — and beta then became a division
  *by* that residue, turning rounding noise into a plausible-looking
  coefficient. Now gated on magnitude relative to the series' own scale.

### Added — market state, which the platform never had

- **`data/treasury.py`** — the US Treasury par yield curve, keyless. Serves the
  risk-free rate and the 10Y-3M term spread. (Ken French's `RF` column was
  measured and rejected for the rate: two decimals of a *daily* figure quantizes
  to 2.52%/yr steps — 2023, 2024 and 2025 all report exactly 5.04%.)
- **`data/macro.py`** — VIX plus the curve as one regime snapshot, behind
  `ai.pm_tools.macro_context`. CBOE's feed is delayed and is labelled delayed
  everywhere it appears. The two legs degrade independently: one source down
  still reports the other, both down still returns a snapshot with named gaps
  rather than costing a cycle its decision. A missing leg is a gap, never a zero.

### Added — alpha net of the known factors

- **`backtest/factor_model.py`** with `stats.multi_ols`: Fama-French three-factor
  attribution behind `backtest.factor_attribution`. A strategy holding a size
  tilt is now attributed to SMB instead of scoring "alpha" against a single
  benchmark. Stdlib-only (normal equations via Gauss-Jordan with partial
  pivoting), so collinear regressors are detected rather than silently fitted.

### Added — a screened strategy menu

- **`research/menu.py`** — 51 published systematic strategies with each source
  paper's own reported Sharpe, volatility and cadence, screened against what
  Poseidon can actually trade: 23 runnable, 6 partial, 22 blocked with each
  blocker named. Reported figures are labelled the papers', never Poseidon
  backtests.

## [2.15.0] — 2026-08-13

Research-port wave 2 of the Vibe-Trading / TradingAgents cross-pollination
(ranks 4–7). **Every feature here is off by default** — with no config change
the review cycle behaves exactly as it did in 2.14.0.

### Added — fundamentals, filings and insider data (rank 4)

- Two keyless providers: **SEC EDGAR** (`companyfacts` fundamentals + filing
  metadata, identified User-Agent and polite pacing per SEC fair-access
  policy) and **Yahoo fundamentals**. New `FUNDAMENTALS`, `FILINGS` and
  `INSIDER` data capabilities route through `DataRouter` with reference
  caches, so a provider that cannot serve a class degrades instead of failing
  the cycle.
- Filing metadata only — never document text. Statements are curated to
  10-K/10-Q, USD-family units, newest-first, and carried as `Decimal(str(v))`.
- The PM gains fundamentals tools and a fundamentals analyst context; both are
  config-gated and absent from the catalog when disabled.

### Added — PM research tool breadth (rank 6)

- **`read_url`**: an SSRF-guarded web fetch. Scheme allowlist, no userinfo,
  ports 80/443 only, and no request may reach a private, loopback,
  link-local, multicast, reserved, unspecified or RFC6598 CGNAT address —
  literally, via any resolved A/AAAA record, or behind a redirect (each hop
  is re-validated). Page text is quarantined: scanned for prompt injection
  and annotated, never rewritten, and flagged to the model as data that is
  never a price source.
- **`screen_market`** and an NxN date-aligned **`correlation`** matrix, plus a
  `GET /api/correlation` operator endpoint.

### Added — outcome resolution and behavioural diagnostics (rank 5)

- Decisions carry resolution markers and a lesson `kind`; reflection grades
  the realized `forward_return` against a benchmark and writes
  counterfactual, kind-aware lessons into the cycle prompt.
- `analytics/behavior.py` sweeps closed round-trips for behavioural biases
  (entry-after-runup and friends) — advisory only, never a trade veto.

### Added — backtest evaluation depth (rank 7)

- A stdlib-only stats toolkit: safe CAGR, OLS alpha/beta, bootstrap
  confidence intervals, and randomization significance — **sign-flip for
  Sharpe** (which is order-invariant) and order-permutation for max drawdown.
- Engine summaries gain CAGR, Sortino, Calmar, profit factor and dual
  turnover; rebalance mode gains walk-forward folds, benchmark/OLS
  attribution and a wipeout guard. The strategy workshop reports a backtest
  run card with a hash-pure audit entry.

### Security

- `read_url`'s injection scan now covers the page `<title>`, which reaches the
  model verbatim in its own payload field and was previously unscanned.
- The SSRF guard blocks RFC6598 CGNAT (100.64.0.0/10). Python's `ipaddress`
  reports `is_private`, `is_reserved` and `is_multicast` all false for that
  block, so it evaded every predicate the guard used.
- SEC EDGAR's ticker→CIK cache uses double-checked locking. The check and the
  fill straddled an await, so concurrent cold-start callers each downloaded
  the multi-megabyte ticker map and burst through the fair-access ceiling.

## [2.14.0] — 2026-07-20

### Fixed — manual trading unblocked (#27)

- **Crypto position canonicalization.** Alpaca's positions feed returns crypto
  pairs slashless (`USDTUSD`) while orders, quotes, and risk matching use the
  canonical `USDT/USD` — one real position split across two ledger keys, so
  reduce-only refused its exit as a would-be short and its quote misrouted as
  an equity ticker and failed on every provider. Positions are now
  canonicalized at the broker seam, gated on the broker's own asset class
  (an equity that merely looks like a pair stays raw).
- **Manual delayed-quote carve-out.** The operator's dashboard ticket may
  validate against a DELAYED (≤ `delayed_max_age_seconds`, 15 min) reference
  quote — the after-hours reality on free feeds. STALE always refuses, every
  other risk rule runs unchanged, and the AI cycle, approval re-check, and
  guardian halt-flatten keep the strict live-only gate (each pinned by tests).
  The accepted quote's freshness/source/as_of are recorded in the
  `order.manual_submitted` audit entry.

### Added — per-trade risk case (#26)

- `submit_decision`'s rationale gains a required **invalidation** field: the
  observable condition that proves the thesis wrong, mechanized by the armed
  stop-loss when it is a price. Advisory — missing/malformed values degrade to
  empty and can never void trades.
- **Conviction-scaled sizing** in the PM system prompt: size from the
  volatility-equalized `suggest_position_size` baseline scaled by confidence;
  a high-risk play is licensed exactly when the reward case is a multiple of
  the risk case and the max expected loss stays survivable.
- **Reflection scores conviction**: closed positions carry their entry
  confidence and stated invalidation back to the lesson writer (junk-tolerant
  threading from stored decisions), which judges whether the outcome earned
  the conviction — overconfident losers and underconfident winners both
  become lessons.

### Fixed — actionable local-backend errors (#25)

- HTTP 4xx/5xx from the OpenAI-compatible backend now surface the server's own
  diagnosis (bounded, single-line) in the component-error notification, and
  known context-overflow signatures (LM Studio/llama.cpp/OpenAI/vLLM
  phrasings) append a copy-pasteable remedy naming the configured model.

### Added — release automation

- Pushing a version bump to `main` now auto-tags and publishes the GitHub
  release, using the matching changelog section as the release notes
  (`.github/workflows/release.yml`).

## [2.13.0] — 2026-07-18

### Added — market screener (widen the live trading universe; OFF by default)

The autonomous PM can now trade a broad index (S&P 500), not just the fixed
watchlist. Each review cycle cheaply screens ~500 names with **batched daily
bars**, ranks them by blended momentum behind a dollar-volume floor, and hands
the AI the **top-N** candidates to deep-analyze alongside the watchlist
(classic screen-then-analyze). **OFF by default** (`screener.enabled=false`):
zero behavior change until deliberately enabled — the disabled cycle path is
byte-identical to today.

- **Advisory selection only — no risk bypass.** The screener picks *which*
  symbols the AI evaluates, never whether to trade. Every screened candidate
  still flows through the full AI → RiskEngine → broker chain unchanged (every
  rule, including universe allow/deny and volume floors). No new order path.
- **Degrade to the watchlist, never crash the cycle.** Any screen
  failure/timeout returns the last good cache (or `[]`), so the cycle proceeds
  on the watchlist alone. Partial data (short history, below the liquidity
  floor, frozen feed) is silently skipped and the rest ranked.
- **Batched daily bars** (`DataRouter.bars_multi` + `AlpacaDataProvider`
  multi-symbol `/v2/stocks/bars`): a full screen is ~4–8 HTTP requests instead
  of ~500 single-symbol calls, chunked and page-followed. On a stack without a
  batched provider it degrades to bounded-concurrency single-symbol fetches.
- **Blended-momentum ranking** `0.6·r_1m + 0.4·r_3m` behind a median 20-day
  dollar-volume floor (default $20M), reusing the pure `strategy.indicators`
  helpers; a 15-minute cache amortizes the screen across cycles.
- **`ScreenerConfig`** on `AppConfig` (`screener:`): `enabled`, `universe`
  (`sp500`), `top_n`, `min_dollar_volume` (Decimal), `refresh_minutes`,
  `bars_limit`, `max_batch_symbols` — all bounds-validated. A commented
  `screener:` block is documented in `config/poseidon.example.yaml`.
- **`research/` stays severed.** The live screener ships its own S&P 500 copy
  (`data/universe/sp500.txt`) behind a pure `data.universe.load_universe`
  loader and never imports `research/` (drift-guard + no-import tests).

### Added — factor-bench rigor (`poseidon research factors`)

Offline, deterministic factor diagnostics gain a random-control null gate and
quantile-layering, ported pure-stdlib from Vibe-Trading's strict bench. The
`research/` package stays pure/offline/stdlib-only, seeded-deterministic,
anti-lookahead, sample-stdev (n−1), and non-overlapping-`n_eff`; no live-trading
code imports it.

- **Within-date random-control null.** Per rebalance date, factor scores are
  permuted `null_seeds` times (default 5) with a string-seeded
  `random.Random(f"{base_seed+k}:{date}")` — never wall-clock — and the mean
  random IC is subtracted from the real IC to form a paired `alpha_IC` series.
  `alpha_t` uses the same non-overlapping `n_eff` as the base t-stat (the alpha
  pairs inherit the base overlap).
- **Honest verdicts** on each factor: `insufficient_data`, `reversed`, `noise`,
  `train_only`, `confirmed_alive`. Gate = loose IC/hit/t gate **and** null
  survival (`alpha_t >= alpha_t_threshold`, default 2.0).
- **Optional chronological OOS split** (`--train-frac`, default off) with a
  stride-based embargo so no test window overlaps a train window; too-thin
  segments fall back to full-sample with a labeled "split didn't run" note.
- **Quantile-bucket NAV layering** (`n_groups`, default 5): equal-weight buckets
  by score, hold-to-next-rebalance returns (contiguous, non-overlapping),
  long/short spread and monotonicity ρ; thin data yields a labeled
  `insufficient data (<reason>)` readout, never a confident number.
- **Bundled `--universe sp500`** snapshot (`research/data/sp500.txt`, ~500
  current constituents, dot-class tickers kept) resolved at the CLI edge via
  `importlib.resources`; adds a labeled survivorship caveat to the report.
  Config gains `null_seeds`, `null_base_seed`, `train_frac`, `alpha_t_threshold`,
  `verdict_min_n_eff`, `n_groups` (all bounds-validated).
- Report footer prints the Harvey-Liu-Zhu (2016) note: whole-library scans
  should clear `|t| >= 3.5`; treat `confirmed_alive` below that as provisional.

None-sentinel discipline (finding 14) extends to every new field: never-computed
values render `-`, distinct from a measured `0.0`.
