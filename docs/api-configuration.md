# API configuration

## Anthropic (the decision engine)

1. Create an API key at <https://console.anthropic.com>.
2. `poseidon vault set anthropic_api_key`
3. `ai.model` defaults to `claude-opus-5`. `ai.effort` trades depth for
   cost/latency per review cycle (`high` recommended; `xhigh`/`max` for
   the most careful reasoning).

Cost control: the review cadence (`ai.review_interval_seconds`, default 5
minutes during market hours) is the dominant cost lever, followed by
`effort` and the watchlist size. The system prompt is cache-controlled, so
frequent cycles benefit from prompt caching automatically.

The agent's system prompt bans acting on remembered or estimated market
data; every tool it can call is wired to the live data router, and its
final decision must arrive through a strict-schema tool call.

## Local model backend (no API credit)

To run the portfolio manager for free on a local, OpenAI-compatible endpoint
(e.g. [LM Studio](https://lmstudio.ai)) instead of the Anthropic API, set the
`ai` backend:

```yaml
ai:
  backend: openai_compatible
  base_url: http://localhost:1234/v1
  model: devstral-small-2-24b-instruct-2512   # any tool-use-capable local model
  temperature: 0.2
  input_price_per_mtok: 0     # local = free
  output_price_per_mtok: 0
```

The default (`backend: anthropic`) is unchanged, and it stays a one-line switch
back. Everything below the model — the audited tool loop, the strict
`submit_decision` schema, the risk engine — is identical; only what generates
the trade ideas changes. Local models make **weaker** decisions than Opus:
fine for paper experimentation, not for real money.

A local brain still needs **real-time quotes** to trade (orders require fresh
data no matter which model decides them), so enable the Alpaca IEX feed below —
the free finnhub/twelvedata/alphavantage tiers are too delayed to clear
`data.real_time_max_age_seconds`.

## Reflection → lesson-memory loop

When a position closes, Poseidon can distill a short **advisory lesson** (was the
call right, the realized return and alpha vs SPY, one actionable takeaway) and
re-inject the relevant recent lessons into future review cycles — a learning
loop on top of the audited decision record.

```yaml
ai:
  reflection:
    enabled: true        # write a lesson when a position closes
    inject: true         # feed relevant lessons into future cycle prompts
    max_injected: 8      # hard cap on lessons per cycle prompt
    per_symbol: 2        # newest lessons per relevant ticker
    global_n: 3          # newest lessons overall (cross-ticker)
    lookback_days: 120   # ignore lessons older than this
```

Lessons are **advisory context only**: they never gate or bypass the risk
engine, never enter the order path, and are kept out of the tamper-evident audit
chain (their own `trade_lessons` table). `inject: false` keeps writing lessons
but stops feeding them to the model (a reviewable ledger); `enabled: false`
turns the loop off. Use `inject: false` if you want to eyeball a weaker local
model's lesson quality before it influences decisions.

## Model tiering (a utility model for the auxiliary roles)

The **trading decision** always runs on the primary `ai.model`. The tolerant
auxiliary roles — operator chat and the reflection lessons — can optionally run
on a cheaper/faster **utility** model:

```yaml
ai:
  model: claude-opus-5                       # the money decision + reviewer
  utility_model: claude-sonnet-5             # operator chat + reflection lessons
```

The utility model uses the **same backend and endpoint** as the primary with only
the model swapped (Anthropic Opus→Sonnet on one account, or a smaller local model
served at the same LM Studio endpoint). Leave `utility_model` unset — the default
— and every role shares the primary backend exactly as before.

The trading agent and the algorithm reviewer are **never** handed the utility
backend; the money decision runs on the strong model, always. Tiering is a
cost/latency optimization for the advisory roles (and the seam a future
multi-analyst step can build on). On a local-only setup it is near-zero immediate
value — a smaller local model is still $0 — so its real payoff is the Anthropic
path.

## Advisory analyst firm (debate packet)

A background "research firm" precomputes an explainable analysis packet per
watchlist symbol: four analysts (fundamentals, technical, news, sentiment) each
write a structured report, a bull and a bear debate them into a facilitator
verdict, and a three-voice advisory risk lens (risk-seeking / balanced /
risk-averse, plus a synthesis) adds a final read. The freshest packet for each
symbol is fed into the next review-cycle prompt so the portfolio manager gets
one more explainable, citable input.

```yaml
ai:
  analysis:
    enabled: false            # opt-in; off by default
    inject: true               # feed the freshest packet into review cycles
    debate_rounds: 2           # bull/bear exchanges before the facilitator verdict
    risk_rounds: 1              # risk-lens exchanges before its synthesis
    refresh_hours: 24           # reuse a packet younger than this instead of rerunning the firm
    max_injected: 3             # hard cap on packets per cycle prompt
    max_render_chars: 1200      # per-packet truncation bound in the prompt
    max_symbols_per_sweep: 8    # symbols swept per tick — keep low on a local endpoint
```

**Advisory only — the risk lens is not the risk engine.** The three risk
voices produce commentary, never an approval, a size, or a gate; Poseidon's
deterministic `RiskEngine` (caps, VaR, drawdown halt, reduce-only, circuit
breaker) stays the sole pre-trade check. Nothing the firm produces — packet or
risk lens — ever reaches `RiskEngine`, `OrderManager`, the `submit_decision`
tool schema, or the chat dispatcher; it can only shift what the portfolio
manager proposes, never approve or size a trade itself. Only a packet's *id*
lands on a decision's explainability trace, never its prose, and the prose
itself stays out of the tamper-evident audit chain — packets live in their own
`analysis_packets` table, and the audit chain gets only a one-line
`analysis_packet_written` marker, the same treatment as trade lessons.

**Off the execution hot path.** A full run is roughly 12–25 model calls per
symbol (four analysts, a multi-round bull/bear debate, then a multi-round risk
lens), so it runs on a scheduled background sweep (the `analysis_sweep` job),
never inside the review cycle. Turning on `ai.analysis.enabled` adds a default
daily pre-market sweep schedule unless you define your own for that job. The
review cycle itself only reads whatever packet is already cached and still
fresh (`refresh_hours`); a slow, failing, or unavailable model just degrades
the sweep — packets stop refreshing, symbols fall back to no packet — and
never blocks or slows a fill, an exit, or a review cycle.

**OFF by default.** `ai.analysis.enabled` defaults to `false` — call-heavy
infrastructure that is only worth enabling deliberately.

**Local-serialization caveat.** The firm runs on the utility model/backend
(the same tiering as chat and reflection, above), which is $0 on a local
setup — but most local OpenAI-compatible servers (e.g. LM Studio) serve one
generation at a time, so the sweep's concurrent per-symbol calls still queue
up server-side. A sweep is effectively its ~12–25 calls **times** the number
of symbols, back-to-back; a high `max_symbols_per_sweep` on a local endpoint
can take hours and leave packets perpetually stale. This fails open (no fresh
packet just means the PM proceeds without one), so it's a value problem, not a
safety one — hence the low shipped default. Raise `max_symbols_per_sweep`
only on the faster Anthropic utility path.

**Honest framing.** This ships the full firm's *structure* — four analyst
roles, a multi-round debate, a risk lens — but v1's analysts reason only over
the pinned live snapshot (quote + 30-day bars, cited verbatim to reduce
hallucinated numbers) and their own priors. External per-role data (real news,
fundamentals) and live social sentiment are **not** wired in yet: the "news"
and "sentiment" analysts currently read that same price/volume snapshot as the
other two, with no outside feed. Per-role retrieval is a deferred fast-follow —
don't advertise this as a firm with live news or social coverage. Packet
quality tracks the utility model: a weak local model produces a weaker packet,
but it stays advisory, so the portfolio manager can discount it. The real
payoff today is explainability plus a $0 local path — not live intel.

## Market data providers

Configure several — failover is automatic and free. Priorities decide the
order; a failing provider is penalized (exponential backoff) and traffic
shifts to the next one.

| Provider | Capabilities in Poseidon | Key from | Vault value |
| --- | --- | --- | --- |
| `public_data` | real-time quotes, bars, option chains + greeks, crypto | public.com (API secret) | secret, or `{"secret": "...", "account_id": "..."}` |
| `polygon` | quotes (NBBO), bars, option chains + greeks, news | polygon.io | plain API key |
| `finnhub` | quotes, news, earnings calendar, economic calendar, sector taxonomy | finnhub.io | plain API key |
| `twelvedata` | quotes, bars | twelvedata.com | plain API key |
| `alphavantage` | EOD quotes, news+sentiment (bars not offered: free series is split-unadjusted) | alphavantage.co | plain API key |
| `alpaca` | quotes, bars, option chains, news, **crypto** (spot `BASE/USD`) | alpaca.markets | `{"key_id": "...", "secret_key": "..."}` |
| `coinbase` | **crypto** quotes + bars (spot `BASE/USD`), crypto-only | none — public Coinbase Exchange endpoint | *(no credential; free)* |
| `tradier_data` | quotes, daily bars, option chains + greeks | tradier.com | access token (options: `{sandbox: true}`) |

### Running on $0 of API subscriptions

The platform is designed so the only thing you pay for is Claude:

- **`public_data` is free** with a Public brokerage account and serves
  real-time quotes, bars, and full option chains with greeks — the same
  API secret as the `public` broker. If you trade through Public, this is
  your primary source and costs nothing.
- **`finnhub`**, **`twelvedata`**, and **`alphavantage`** free tiers cover
  news, both calendars, and backup quotes (plus bars from `twelvedata`;
  Alpha Vantage bars are not offered — its free daily series is
  split-unadjusted and would change the price basis on failover).
- **`alpaca`** (IEX feed) and **`tradier_data`** (sandbox) are also free
  with their respective accounts.
- **`coinbase`** is free with *no account or key at all* — it hits the public
  Coinbase Exchange REST API for real-time spot crypto (`BASE/USD`) quotes and
  bars. It is enabled by default in the sample config at priority 8 (crypto-only,
  so it never touches equity requests) and is the recommended crypto source.

Recommended free stack: `public_data` (priority 10) for quotes/options/
bars + `finnhub` (priority 20) for news and both calendars, with
`twelvedata`/`alphavantage` as failover. Paid sources (e.g. `polygon`)
are optional upgrades, never requirements.

Notes:

- Free tiers are rate-limited; Poseidon honors `Retry-After` and backs off,
  and the failover router shifts traffic automatically when a provider
  is penalized.
- `public_data` options: `{crypto_symbols: [BTC, ETH]}` marks watchlist
  symbols that should be quoted as crypto instruments.
- **Crypto quotes (`BASE/USD`).** Spot crypto pairs — written with a slash,
  e.g. `BTC/USD`, `ETH/USD` — are routed to whichever configured provider
  advertises the `crypto` capability, and *never* to an equity-only provider.
  `coinbase` is enabled by default and serves crypto free with no key at all, so
  crypto quotes work out of the box; `alpaca` also serves crypto free on the same
  `alpaca_keys` credential (no extra entitlement) as a failover. Only `BASE/USD`
  pairs are supported — stablecoin-quoted pairs such as `BTC/USDT` are rejected
  with a clear error, never a 404. If no crypto-capable provider is configured, a
  crypto quote fails cleanly (`no data, no trade`) rather than falling back to a
  stocks endpoint. Because crypto trades 24/7 over a looser REST cadence, its
  real-time freshness gate is its own knob — `data.crypto_real_time_max_age_seconds`
  (default 60s) — while equities stay strict at `data.real_time_max_age_seconds`
  (default 5s). Crypto trading is **paper-only by default, and enforced**: a
  crypto order on a real-money broker is refused unless `risk.allow_live_crypto`
  is set true. See docs/broker-setup.md for the live follow-on.
- Alpha Vantage quotes are end-of-day: the freshness policy grades them
  DELAYED/STALE, so they can inform research but never orders — that is by
  design, not a bug.
- Capability gaps are fine: the router only asks a provider for what it
  advertises. If *no* configured provider covers a capability the AI asks
  for, the tool returns an explicit "unavailable" error and the AI must
  record it in `data_gaps` instead of guessing.

## Factor research lab

`poseidon research factors` ranks a library of alpha factors (`src/poseidon/research/factors.py`
— momentum, reversal, volatility, trend, volume, drawdown) by point-in-time
predictive power over historical bars, loaded through the same `DataRouter`/
provider stack as everything else in this file. It has its own top-level
config block:

```yaml
# Offline factor research (poseidon research factors). Pure analysis — never
# trades. Point-in-time IC/IR; give it a BROAD --symbols universe (hundreds of
# names), not just your watchlist, or the cross-sectional IC is noisy.
research:
  horizon: 5            # forward-return horizon (trading days) for the headline IC
  rebalance_every: 5    # evaluate every N days; keep >= horizon to avoid overlap inflation
  horizons: [1, 5, 10, 20]   # decay curve
  min_cross: 5          # minimum symbols per cross-section
  lookback_days: 400    # bars to load per symbol
```

**What IC/IR means.** On each rebalance date, Poseidon computes the **IC**
(Information Coefficient) — the Spearman rank correlation between every
symbol's factor score that day and its realized forward return over the next
`horizon` trading days. Across all sampled dates it reports `ic_mean` (does
the ranking predict forward returns, on average?), `ic_std` (how much that
varies date to date), the **IR** (Information Ratio) = `ic_mean / ic_std`
(signal strength adjusted for consistency — a modest but stable IC can beat a
larger but erratic one), a `hit_rate` (fraction of dates with positive IC),
and a decay curve — mean IC recomputed at each horizon in `horizons`, showing
whether the signal fades or strengthens further out.

**Point-in-time, by construction.** A factor is evaluated at date `t` using
only bars whose period has closed on or before `t` (`visible_bars` in
`research/ic.py`); the forward-return label for that same date deliberately
reads bars *after* `t`, but only to score the factor after the fact — it is
never passed to the factor function. Look-ahead leakage is structurally
impossible at the factor boundary, not just a convention to remember.

**The t-stat corrects for overlap, but `rebalance_every >= horizon` is still
the setting to use.** When `rebalance_every` is shorter than `horizon`,
consecutive rebalance dates score overlapping forward-return windows and the
IC series autocorrelates — counting every sampled date as independent would
overstate significance. Poseidon already guards against this: the t-stat is
`IR * sqrt(n_eff)`, where `n_eff` is the count of *non-overlapping* windows,
not the raw number of sampled dates (`_effective_n` in `research/ic.py`), so
tight rebalancing doesn't naively inflate it. Keep `rebalance_every >=
horizon` anyway — that's the regime where `n_eff` equals the raw period count
exactly, every IC sample comes from a genuinely independent forward-return
window, and the t-stat isn't leaning on the downsampling approximation at all.

**Give it a broad universe, not your watchlist.** `--watchlist` is
convenient but reuses your trading watchlist (a handful of names) — exactly
the thin universe that makes cross-sectional IC noisy, since a rank
correlation over a few symbols swings wildly between rebalance dates.
`min_cross` (default 5) is a hard floor: a rebalance date with fewer than
that many symbols carrying both a valid factor score and a valid forward
return is dropped entirely, and the report itself flags any run under 20
total symbols as `[THIN: results are noisy/unreliable]`. Neither floor is a
target — aim for hundreds of names (e.g. the S&P 500 via `--symbols-file`,
one ticker per line) so each cross-section is wide enough for the ranking to
mean something.

**Pure offline research — no live-trading surface.** The command builds only
a `DataRouter` to read historical bars (`ApplicationKernel._build_router()`);
it never calls `ApplicationKernel.start()`, so it never opens the database,
touches the audit chain, connects a broker, or constructs the `RiskEngine` or
`OrderManager`. Factor evaluation itself is pure — no I/O. It prints a ranked
report to stdout and exits; nothing it computes is persisted, injected into a
review cycle, or reachable from `OrderManager.execute_decision`. Safe to run
anytime, in any operating mode — including a live or autonomous config —
with zero chance of influencing a trade.

**Example:**

```bash
poseidon research factors --symbols AAPL,MSFT,GOOGL,AMZN,NVDA,... --days 400 --horizon 5
```

In practice, keep a broad universe in a file instead of typing hundreds of
tickers inline:

```bash
poseidon research factors --symbols-file sp500.txt --days 400 --horizon 5
```

`--days`, `--horizon`, and `--rebalance-every` fall back to the `research:`
config block (`lookback_days`, `horizon`, `rebalance_every`) when omitted or
passed as `0`; `min_cross` and the `horizons` decay list are config-only,
with no CLI override.

## Strategy-decay watchdog

A scheduled sweep (`strategy_health_sweep`, added automatically on a daily
pre-market cron once `strategy_health.enabled` is true) watches each
strategy's **rolling realized edge** — the mean return of its last
`window_trades` closed round-trips, attributed by strategy (fills carrying no
strategy tag are pooled as `unattributed` and tracked the same way) — against
that same strategy's own longer-run baseline, and raises a state when the
edge has genuinely died rather than merely cooled off.

```yaml
strategy_health:
  enabled: true
  auto_retire: false      # opt-in: auto-deactivate a decayed custom strategy
  window_trades: 20
  min_trades: 8           # below this the window is "insufficient" (never flagged decaying)
  baseline_min_trades: 20
  decay_t: 2.0            # t-stat threshold for a significantly-<=0 recent edge
  decay_streak: 2         # consecutive dying sweeps -> decaying
  retire_streak: 4        # consecutive dying sweeps -> retire_recommended
  recover_streak: 2       # consecutive ok sweeps to step back toward healthy
```

**States and hysteresis.** Every strategy sits in one of four states —
`healthy` → `watch` → `decaying` → `retire_recommended` — and a single sweep
moves it at most one rung. A `dying` sweep (the recent window's mean return is
significantly ≤ 0: a one-sample t-stat at or below `-decay_t`) advances a
decline streak: `decay_streak` *consecutive* dying sweeps are needed to cross
from `healthy`/`watch` into `decaying`, and `retire_streak` consecutive dying
sweeps before `decaying` escalates to `retire_recommended`. Recovery is the
mirror image and just as gradual: `recover_streak` consecutive `ok` sweeps
step the state back down exactly one rung (e.g. `decaying` → `watch`), so
climbing all the way back from `retire_recommended` to `healthy` takes three
separate `recover_streak`-length runs. This hysteresis is deliberate — one
bad sweep never flags a strategy and one good sweep never clears a flag; only
a sustained run in either direction moves the needle.

**Decay, not normalization — only a genuinely-unprofitable edge escalates.**
The assessment separates two different kinds of "worse than before." A window
whose mean return is *statistically significantly negative* is `dying`, and
`dying` is the only signal that ever advances the decline streak. A window
that is still net-**profitable** but significantly below the strategy's own
baseline is `softening` instead — read as normalization, not death — and it
caps out at `watch`: it resets any decline streak already in progress but can
never itself reach `decaying` or `retire_recommended`. A strategy that simply
makes less than it used to, while still net-positive, is never a candidate
for retirement.

**Conservative on few trades.** Below `min_trades` closed round-trips in the
recent window, or below `baseline_min_trades` in the baseline that precedes
it, the sweep reports `insufficient` and holds the strategy's current state
and streaks unchanged rather than drawing a conclusion from a noisy sample.
An infrequently-traded strategy can sit at `insufficient` indefinitely — by
design, not a gap.

**Advisory and reduce-only by default.** The watchdog can only ever narrow
what a strategy does next, never expand it: flag a state change, write an
audit entry (`strategy.health_changed`), and send a `warning` notification on
a downgrade into `decaying` or `retire_recommended`. It never places, sizes,
or approves an order, and it holds no reference to `RiskEngine`,
`OrderManager`, `submit_decision`, or any broker — the reduce-only guarantee
is structural, not just a config default. Its one and only mutation, gated
behind `auto_retire`, is deactivating a decayed strategy so it stops
proposing *new* signals; with the shipped default `auto_retire: false`, the
sweep never mutates anything and only flags/audits/notifies, no matter how
long a strategy stays at `retire_recommended`.

**`auto_retire` only ever touches a custom strategy.** When enabled, the
sweep where a strategy's state *transitions into* `retire_recommended`
deactivates (not deletes — it reverts to a `draft`, still editable and
re-activatable) the matching active strategy in the algorithm workshop, and
separately audits `strategy.auto_retired`. Built-in strategies (`momentum`,
`mean_reversion`, etc., configured in the top-level `strategies:` list) have
no workshop entry to deactivate, so `auto_retire` is a no-op for them by
construction — the watchdog still flags, audits, and notifies on a decayed
built-in strategy, but only you can turn one off
(`strategies: [{name: ..., enabled: false}]`); it is never auto-disabled.

Watch it via your configured `notifications:` channel (the `warning` alert on
every downgrade) and the audit trail (`poseidon audit tail`), which records
`strategy.health_changed` for every transition and `strategy.auto_retired`
whenever auto-retire fires.

## Where keys live

All keys go in the encrypted vault (`poseidon vault set NAME`), referenced
from config by name. Keys are never written to config, logs (a redaction
processor scrubs anything key/token/secret-shaped), or the database.
