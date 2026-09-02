# Configuration reference

File: `~/.config/poseidon/poseidon.yaml` (override with `poseidon
--config`). Validated strictly at startup — unknown keys and invalid values
refuse to boot. Environment variables `POSEIDON_SECTION__FIELD=value` override
file values (e.g. `POSEIDON_AI__MODEL=claude-opus-5`,
`POSEIDON_DASHBOARD__HOST=0.0.0.0`).

Secrets never appear in this file: fields named `credential` hold the
*name* of a vault entry (`poseidon vault set <name>`).

## Top level

| Field | Default | Notes |
| --- | --- | --- |
| `mode` | `research` | `research` / `approval` / `autonomous`; switchable live from the dashboard |
| `data_dir` | `~/.local/share/poseidon` | DB, logs, vault, paper state |
| `log_level` | `INFO` | console + rotating JSON file logs |

## `ai`

| Field | Default | Notes |
| --- | --- | --- |
| `model` | `claude-opus-5` | any current Claude model ID |
| `effort` | `high` | `low`–`max`; higher = deeper reasoning per cycle |
| `max_tokens` | 16000 | per API call |
| `api_key_credential` | `anthropic_api_key` | vault entry name |
| `input_price_per_mtok` / `output_price_per_mtok` | 5.0 / 25.0 | USD per Mtok for spend estimation |
| `monthly_budget_usd` | 0 | hard ceiling; review cycles pause when the month's estimated spend reaches it (0 = off) |
| `max_tool_iterations` | 24 | hard cap on tool round-trips per cycle |
| `max_stalled_iterations` | 3 | abort the cycle (no action) after this many consecutive iterations that only repeat identical tool calls (0 = off) |
| `mandate` | `""` | standing operator directive rendered into every cycle prompt (posture, horizon, exit discipline); advisory only, max 1200 chars |
| `review_interval_seconds` | 300 | default market-hours cycle cadence |

## `data`

| Field | Default | Notes |
| --- | --- | --- |
| `providers[]` | — | `name`, `credential`, `priority` (lower = first), `enabled`, `options` |
| `real_time_max_age_seconds` | 5 | freshness gate for REAL_TIME (equities) |
| `crypto_real_time_max_age_seconds` | 60 | REAL_TIME gate for crypto (24/7 REST cadence); equities stay strict |
| `delayed_max_age_seconds` | 900 | gate for DELAYED |
| `allow_delayed_for_research` | true | delayed data may inform research; orders always need fresh quotes |
| `request_timeout_seconds` | 10 | per HTTP call |

Provider names: `public_data` (free with a Public account; credential =
API secret or JSON with `secret`/`account_id`), `polygon`, `finnhub`,
`twelvedata`, `alphavantage`, `alpaca` (credential = JSON with
`key_id`/`secret_key`), `tradier_data` (options: `{sandbox: true}`).
Every provider has a $0 tier — see docs/api-configuration.md.

## `brokers[]`

`name` (plugin), `enabled`, `primary` (exactly one outside research mode),
`paper` (sandbox where supported), `credential` (vault JSON), `options`
(plugin-specific). See docs/broker-setup.md.

## `risk`

Every limit is enforced pre-trade by the risk engine
(docs/risk-controls.md explains each rule):

`max_position_pct`, `max_portfolio_exposure_pct`, `max_daily_loss_pct`,
`max_weekly_loss_pct`, `max_drawdown_pct`, `max_leverage`,
`max_options_exposure_pct`, `max_sector_concentration_pct`,
`max_order_notional`, `min_order_notional`, `max_spread_pct`,
`min_avg_volume`, `max_orders_per_day`, `trade_cooldown_seconds`,
`news_blackout_minutes_before_econ`, `volatility_halt_daily_move_pct`,
`circuit_breaker_error_threshold`, `circuit_breaker_window_seconds`,
`circuit_breaker_cooldown_seconds`, `slippage_limit_pct`,
`max_portfolio_var_pct` (0 disables the VaR halt; enabling it requires
fresh risk metrics before new risk), `benchmark_symbol` (beta/correlation
/regime benchmark, default SPY), `position_risk_budget_pct` (per-position
daily risk budget for the AI's vol-targeted sizing tool, default 0.5%).

Every rule is documented with its rationale in docs/risk-controls.md.

## `guardian`

| Field | Default | Notes |
| --- | --- | --- |
| `enabled` | true | position guardian (exit-plan enforcement, docs/risk-controls.md#position-guardian) |
| `interval_seconds` | 60 | watch cadence; runs 24/7 — the equity session gate is applied per symbol, so crypto stops are enforced around the clock |

## `reports`

| Field | Default | Notes |
| --- | --- | --- |
| `daily_summary` | true | end-of-day digest through the notification channels |
| `daily_summary_cron` | `15 16 * * 1-5` | when to send it (America/New_York) |

## `strategies[]`

`name` (one of the 16 built-ins or a plugin), `enabled`, `symbols`
(defaults to all watchlist symbols), `options` (strategy-specific, e.g.
`min_20d_return`, `pairs`, `min_open_interest`). Each strategy toggles
independently; disabled strategies never run.

## `schedules[]`

`name`, `job` (`review_cycle`, `portfolio_sync`, `update_check`,
`audit_verify`, `position_guardian`, `daily_report`, `risk_metrics`), and
exactly one of:

- `every_seconds: N` — fixed interval, 1 s and up;
- `cron: "m h dom mon dow"` — standard cron, evaluated in America/New_York.

`only_market_hours: true` gates the trigger on the regular session.
Defaults added automatically when absent: a market-hours `review_cycle`
every `ai.review_interval_seconds`, a nightly `audit_verify` at 02:15,
and 24/7 `position_guardian` (60 s, per-symbol session gate) and market-hours `risk_metrics` (15 min)
refreshes.

## `notifications[]`

`kind` (`desktop`, `email`, `discord`, `telegram`, `webhook`),
`min_level` (`info`/`warning`/`critical`), `credential` (vault JSON),
`options`. See the example config for the exact credential shapes.

Events routed automatically: fills, rejections, risk violations, circuit
breaker, broker disconnect/reconnect, approval requests, component errors,
update availability. Repeats are deduplicated for 5 minutes.

## `watchlists[]`, `dashboard`, `updates`

- `watchlists[]`: named symbol lists; the union feeds the AI and default
  strategy universes.
- `dashboard`: `host` (default `127.0.0.1`), `port` (8321),
  `auth_token_credential` — a vault entry holding a bearer token. **A token
  is required by validation whenever `host` is non-loopback**: set the vault
  entry, or supply one via `POSEIDON_DASHBOARD_TOKEN` /
  `POSEIDON_DASHBOARD_TOKEN_FILE` (used by the Docker deployment; the vault
  entry takes precedence when both are set). Clients send
  `Authorization: Bearer <token>` or open `/?token=<token>`.
- `updates`: `enabled`, `check_interval_hours`, `auto_apply`.

## Validation

`poseidon config validate` checks the file without starting anything;
`poseidon doctor` additionally checks the vault, credentials, calendar
coverage, and database.
