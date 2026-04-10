# Polymarket Copy Bot

Rust copy-trading bot for Polymarket with:

- real-time market and orderbook ingestion
- public and authenticated wallet activity ingestion
- hot-path / cold-path runtime separation for latency-sensitive copying
- dynamic wallet registry and wallet scoring
- price-aware risk gating
- live CLOB execution and realistic paper execution
- persistent portfolio, position lifecycle, and Telegram reporting

This README reflects the current architecture in `src/` and the current config surface in `.env.example`.

## Current Architecture

The bot is no longer a simple "watch one wallet and market-buy immediately" flow. The runtime is now built around registries, managed exits, realistic execution semantics, and bounded JSON persistence.

### 1. Startup and live readiness

`src/main.rs` bootstraps the runtime:

- loads `.env` into `Settings`
- supports `cargo run --release -- check-wallet`
- in `live` mode, runs `src/wallet/readiness.rs` before building the executor
- validates signer, profile, funder, Polygon chain id, balances, allowance, and optional approval bootstrap
- skips readiness RPC work entirely in `paper` mode

### 2. Market data path

The market side stays WebSocket-first:

- `src/websocket/market_stream.rs` subscribes to the Polymarket market stream
- `src/websocket/stream_router.rs` preserves ordering and routes decoded events through bounded parser workers
- `src/orderbook/orderbook_state.rs` and `src/orderbook/orderbook_levels.rs` maintain in-memory books
- `src/detection/liquidity_sweep_detector.rs` and `src/detection/trade_inference.rs` convert orderbook changes into confirmed trade signals

The hot parser path now records stage timestamps at WebSocket receive, parse completion, attribution completion, fast-risk completion, submit start, submit end, and confirmation/fill.

### 3. Hot path vs cold path

The runtime is now split explicitly:

- Hot path
  WebSocket ingest, minimal parse, fast attribution, fast in-memory risk checks, direct source-follow exits, and order submission.
- Cold path
  JSON persistence, raw-event archiving, attribution diagnostics, wallet rescanning, analytics rollups, and Telegram updates.

The split is enforced with bounded `tokio` queues and strict exit priority:

- source-follow exits preempt entries
- direct tracked-wallet entries can fall back to inline execution if the hot queue is saturated
- predicted entries may be dropped under hot-path congestion instead of delaying exits
- cold-path logging can shed diagnostics, but it cannot stall trading

### 4. Wallet activity and attribution

Wallet attribution now has multiple inputs:

- `src/wallet/activity_stream.rs` consumes the authenticated user stream when available
- the same module also consumes the public activity stream
- parsed activity is matched back to market-side signals and persisted for later analysis
- `src/attribution_fast.rs` keeps a bounded low-latency index by source wallet, market, outcome, side, and recent timestamp bucket
- `src/raw_activity_logger.rs` records raw and parsed public-activity diagnostics
- `src/attribution_logger.rs` records runtime, stream, signal, and matching events

Direct tracked-wallet entries and source-follow exits now resolve through the fast attribution index first. Rich attribution logging still runs, but it no longer sits in front of the copy decision.

### 5. Dynamic wallet tracking

Tracked wallets are now registry-backed instead of being a static one-wallet assumption:

- `src/wallet_registry.rs` maintains the active wallet set in memory with O(1) lookup
- the registry persists to `data/wallets_active.json`
- `src/wallet_scanner.rs` rescans recent activity every 3 minutes, scores wallets, updates the registry, and marks inactive wallets without deleting them
- `src/wallet_score.rs` prefers early-entry wallets over late chasers

`TARGET_WALLETS` still seeds the registry, but runtime tracking can evolve as the scanner refreshes activity.

The rescanner remains enabled, but it is now firmly off the hot path. Active-wallet updates are atomic, and owned copied-position exits still resolve from the position registry even after a wallet becomes inactive.

### 6. Prediction and wallet quality

`src/prediction.rs` ranks candidate wallets and markets before risk evaluation:

- combines observed wallet behavior, market behavior, and wallet quality
- uses `MIN_WALLET_SCORE` to reject low-quality wallets
- supports multi-wallet prioritization instead of a single-wallet path
- can still execute without a prediction upgrade if a direct matched trade arrives

### 7. Price-aware risk engine

`src/risk.rs` is now price-banded and position-aware:

- `PriceBand::{Low, Mid, High}` changes slippage, spread, and size rules by source price
- classifies markets as `UltraShort`, `Short`, or `Medium` from title heuristics
- blocks very high-price entries above `0.92`
- replaces the old hard entry ceiling with a remaining-edge check
- rejects late copies via the strict `MAX_COPY_DELAY_MS` gate
- treats disabled ultra-short markets as exceptional-only instead of absolute-disabled: they must clear higher quality, liquidity, and slippage requirements
- rejects thin books via visible-liquidity and spread checks
- rejects price chasing, market cooldown churn, conflicting wallet signals, and hyperactive scalp wallets
- downweights source wallets with negative recent realized or open PnL before sizing new entries
- computes a weighted trade-quality score before submit
- sizes entries as the minimum of risk-percent-of-equity, absolute size cap, remaining total exposure, remaining market exposure, and available cash
- switches to adaptive drawdown mode instead of binary drawdown lockout; hard-stop, loss-streak cooldown, total exposure limit, and per-market exposure limit still block entries
- rejects duplicate positions, stale positions, and opposite-side exposure unless `ALLOW_HEDGING=true`

The design goal is "copy where edge still exists", not "copy everything late".

Trading mode is adaptive:

- `NORMAL`
  Uses configured risk sizing and filters.
- `DRAWDOWN`
  Starts at `MAX_DRAWDOWN_PCT`; entries remain allowed but size is multiplied by `DRAWDOWN_SIZE_MULTIPLIER`, trade quality is stricter, and liquidity/spread/slippage thresholds relax by `DRAWDOWN_RELAXATION_FACTOR` within safety caps.
- `HARD_STOP`
  Starts at `HARD_STOP_DRAWDOWN_PCT`; new entries are blocked and exits remain actionable.

If BUY signals are seen but no entry survives for `NO_TRADE_TIMEOUT_MS`, the risk engine temporarily applies the same bounded relaxation so the bot does not deadlock into a zero-trade state.

Risk evaluation is now two-stage:

- hot-path fast gate
  O(1) in-memory checks only: copy delay, duplicate/open position checks, opposite-side exposure, top-of-book liquidity, spread, and high-price / remaining-edge screens
- cold-path audit trail
  richer explanations, analytics, and serialized skip detail

The hard-stop guard remains asymmetric: new entries are blocked only in `HARD_STOP`, but source-follow and managed exits stay actionable. If `FORCE_CLOSE_ON_HARD_STOP=true`, the exit watcher submits emergency-exit closes for open positions after the hard-stop threshold is active.

### 8. Unified execution contract

`src/execution/order_executor.rs` now exposes the same fill contract in both modes:

- `ExecutionResult` always includes requested size/price, filled size/price, and `ExecutionStatus`
- statuses are `Filled`, `PartiallyFilled`, `NoFill`, or `Rejected`
- `success=true` does not imply a fill

Live mode:

- uses the official Rust CLOB SDK
- submits FAK limit orders
- maps exchange fill results into the shared execution status model

Paper mode:

- uses the real market data path
- simulates top-of-book execution using best bid/ask plus visible top-level liquidity
- can return partial fills and no-fills
- does not make blockchain calls

This keeps paper behavior much closer to live behavior than the old "always fully filled" simulation.

### 9. Portfolio, position identity, and lifecycle

The portfolio system now uses a stronger identity model:

- `src/models.rs` defines `PositionKey { condition_id, outcome, source_wallet }`
- `src/portfolio.rs` uses that key to match buys and sells
- positions carry `PositionState::{Open, Closing, Closed, Stale}`
- stale positions are isolated instead of deleted
- `PortfolioSnapshot` tracks `realized_pnl`, `unrealized_pnl`, `total_exposure`, cash, current/starting/peak equity, drawdown, rolling drawdown, open exposure, open-position count, loss streak, loss cooldown, hard-stop status, and risk utilization

This fixes the earlier asset-only matching problems that caused orphan exits and misleading PnL.

### 10. Position resolver and ownership

`src/position_resolver.rs` now keeps a low-latency in-memory ownership index in front of the portfolio refresh cycle:

- entries register a `PendingOpen` record as soon as execution is committed
- the resolver can answer whether a copied position is `PendingOpen`, `Open`, or already `Closing`
- source-follow exits bind directly to pending opens instead of spinning in a retry loop
- when a pending position is promoted to open, any bound exit is released immediately back onto the hot path
- pending-open resolver state persists to `data/position-resolver.json`

This is the key fix for the old `owned_position_not_resolved_yet` bottleneck: exit matching no longer depends on the portfolio snapshot winning a race against the source SELL.

### 11. Position registry and persistence

`src/position_registry.rs` persists copied-position ownership independently of wallet activity:

- persists to `data/positions.json`
- records lifecycle transitions to `data/position-lifecycle.jsonl`
- tracks open/closed status by copied position

This is important because exit eligibility is now driven by owned positions, not just by whether a wallet is still active in the wallet registry.

### 12. Exit management

Exits are now first-class and mandatory.

The runtime in `src/main.rs` applies three exit layers:

1. Source-follow exits
   Source SELL events attempt to close the matching copied position immediately.

2. Price-based exits
   The exit watcher can trigger `TAKE_PROFIT`, `PROFIT_PROTECTION`, and `STOP_LOSS` exits from live quotes.

3. Time-based exits
   Positions older than the configured hold limit are only exited when source-follow is not pending and the position is stagnant or no longer trending favorably.

Important protections:

- exits have priority over entries
- exits are consumed from the hot queue before entries when `EXIT_PRIORITY_STRICT=true`
- duplicate exits are blocked with a shared closing set
- raw source SELL activity is counted separately from actionable copied-position exits
- source SELLs must normalize to a known condition/outcome and map to a plausible owned position before entering the retry buffer
- source SELL resolution always tries exact `PositionKey { condition_id, outcome, source_wallet }` ownership first
- source SELL resolution checks both `PendingOpen` and fully open copied positions
- pending-open exits bind locally and are released on the position-open event instead of polling
- managed exits consult pending unresolved source exits before firing time-based closes
- stop-loss exits yield to pending source exits unless the move is catastrophic
- safe fallback resolution is limited to the same source wallet and same market/condition, and broad condition-only fallback is used only when there is exactly one active candidate
- unresolved source exits are persisted to a short retry buffer in `data/unresolved-exits.json`, but only after local ownership candidates exist
- UNKNOWN or unresolved outcomes are treated as attribution misses instead of actionable retry work unless a deterministic single-candidate fallback resolves them
- unresolved retry now uses stepped backoff (`UNRESOLVED_EXIT_INITIAL_RETRY_MS`, `UNRESOLVED_EXIT_MAX_RETRY_MS`, `UNRESOLVED_EXIT_TOTAL_WINDOW_MS`) and runs off the hot path
- duplicate public/authenticated source SELLs are suppressed for `SOURCE_EXIT_DEDUPE_WINDOW_MS`
- `Closing` positions are revisited until they fill, escalate through mandatory/emergency unwind, or record an explicit failed-close reason
- exit logic uses the position registry, so wallet removal does not strand positions

### 13. Observability and health

Operational visibility is spread across:

- `src/notifier.rs`
  Telegram trade alerts and pinned daily summary
- `src/latency.rs`
  processed/skip latency JSONL logging
- `src/latency_monitor.rs`
  pause-and-reconnect fail-safe when the hot path slows down
- `src/health.rs`
  `GET /health` with readiness, counts, latency, queue depth, degradation state, pause state, and last error

The health surface and JSONL latency logs now expose per-stage timing for:

- entry path
- source-follow exit path
- managed exit path

and backpressure counters for:

- hot entry queue depth
- hot exit queue depth
- cold-path queue depth
- dropped diagnostics
- degradation mode

The Telegram summary now reports:

- realized PnL
- unrealized PnL
- total PnL
- open positions
- exposure

and warns when exits have not executed yet.

The analytics summary now separates:

- `raw_source_sell_seen`
- `actionable_source_exit_seen`
- `source_exit_normalization_failed`
- `source_exit_not_owned`
- `source_exit_duplicate_suppressed`
- `position_pending_registered`
- `position_promoted_to_open`
- `exit_resolved_against_pending`
- `exit_resolved_against_open`
- `source_exit_resolved_against_pending`
- `source_exit_resolved_against_open`
- `exit_bound_to_pending`
- `deferred_exit_released`
- `deferred_exit_expired`
- `resolver_not_found`
- `resolver_ambiguous`
- `source_exit_retry_queued`
- `source_exit_retry_resolved`
- `source_exit_unresolved_expired`
- `exit_noise_filtered`
- `exit_resolved_success`
- `exit_resolved_failed`
- `time_exit_triggered`
- `premature_time_exit`
- `managed_exit_profit_protection`

### 14. Persistence boundaries

Latency-sensitive state updates now happen in memory first and flush on background intervals:

- `data/portfolio-summary.json`
- `data/state.json`
- `data/execution-analytics-summary.json`
- `data/position-resolver.json`

Critical copied-position ownership transitions still persist through the position registry and lifecycle log so entry/exit ownership is not lost.

The runtime now stores the projected portfolio layout in memory immediately after fills, so source-follow exits do not have to wait for a later JSON flush or live refresh just to discover a copied position.

### 15. Log rotation and retention

There are now two JSONL logging patterns:

Rotating logs:

- `data/attribution-events_*.jsonl`
- `data/raw-activity_*.jsonl`

These use `src/log_rotation.rs` and can rotate by:

- line count via `LOG_MAX_LINES`
- elapsed time via `LOG_ROTATE_HOURS`
- optional hard clearing via `ENABLE_LOG_CLEARING`

Rolling logs:

- `data/wallet-activity.jsonl` and timestamp-suffixed rotated files
- `data/wallet-scores.jsonl` and timestamp-suffixed rotated files
- `data/position-lifecycle.jsonl` and timestamp-suffixed rotated files

These use `src/rolling_jsonl.rs` and rotate when the active file reaches 30,000 lines.

## Runtime Flow

The latency-sensitive flow is now:

1. Read market deltas from the market WebSocket.
2. Parse the minimal market/activity fields needed for an immediate decision.
3. Resolve direct tracked-wallet entries and normalize source SELLs through fast attribution.
4. Register `PendingOpen` ownership before entry submission so later exits can resolve locally.
5. For source SELLs, require a known condition/outcome and a pending/open/closing owned-position candidate before treating the event as actionable.
6. Apply the fast in-memory risk gate.
7. Submit an order through the live or paper executor.
8. Project the fill into the in-memory portfolio and promote the resolver entry immediately.
9. Release any exit that was bound to the pending position.
10. Flush diagnostics, summaries, and non-critical persistence on the cold path.
11. Continue managing the position until source exit, TP/SL, or time exit closes it.

## Execution Modes

### `EXECUTION_MODE=paper`

Paper mode is now a realistic dry run:

- uses the real market stream
- uses the real wallet activity path
- uses the same prediction and risk logic
- simulates fills with partial/no-fill outcomes
- updates the local portfolio snapshot and Telegram summary
- skips wallet-readiness RPC and allowance checks

### `EXECUTION_MODE=live`

Live mode keeps the existing executor path intact and adds safety around it:

- runs wallet readiness before startup
- requires signer/profile config
- validates Polygon RPC connectivity, balances, and allowance
- can optionally bootstrap USDC approval in EOA flow

## Important Commands

Build:

```powershell
cargo build --release
```

Run:

```powershell
cargo run --release
```

Wallet readiness check:

```powershell
cargo run --release -- check-wallet
```

If you prefer the release binary directly:

```powershell
.\target\release\polymarket-copy-bot.exe
```

## Configuration Notes

`.env.example` is the canonical config reference. The most important groups are:

- Execution mode: `EXECUTION_MODE=paper|live`
- Wallet readiness: `POLYGON_RPC_URL`, `POLYGON_RPC_FALLBACK_URLS`, `MIN_REQUIRED_MATIC`, `MIN_REQUIRED_USDC`
- Live wallet identity: `POLYMARKET_PRIVATE_KEY`, `POLYMARKET_PROFILE_ADDRESS`, optional `POLYMARKET_FUNDER_ADDRESS`
- EOA allowance flow: `POLYMARKET_USDC_ADDRESS`, `POLYMARKET_SPENDER_ADDRESS`, `AUTO_APPROVE_USDC_ALLOWANCE`, `USDC_APPROVAL_AMOUNT`
- Wallet tracking: `TARGET_WALLETS`, optional authenticated activity credentials
- Risk: `ENABLE_PRICE_BANDS`, `MIN_EDGE_THRESHOLD`, `MAX_COPY_DELAY_MS`, `ENABLE_ULTRA_SHORT_MARKETS`, `MIN_VISIBLE_LIQUIDITY`, `MAX_SPREAD_BPS`, `MAX_ENTRY_SLIPPAGE`, `MIN_WALLET_AVG_HOLD_MS`, `MAX_WALLET_TRADES_PER_MIN`, `MARKET_COOLDOWN_MS`, `MIN_TRADE_QUALITY_SCORE`, `MIN_LIQUIDITY`, `MIN_WALLET_SCORE`, `ALLOW_HEDGING`
- Position risk: `MAX_RISK_PER_TRADE_PCT`, `MAX_POSITION_SIZE_ABS`, `MAX_TOTAL_EXPOSURE_PCT`, `MAX_EXPOSURE_PER_MARKET_PCT`, `MAX_DRAWDOWN_PCT`, `DRAWDOWN_SIZE_MULTIPLIER`, `DRAWDOWN_RELAXATION_FACTOR`, `HARD_STOP_DRAWDOWN_PCT`, `NO_TRADE_TIMEOUT_MS`, `FORCE_CLOSE_ON_HARD_STOP`, `MAX_CONSECUTIVE_LOSSES`, `LOSS_COOLDOWN_MS`
- Lifecycle and exits: `MAX_POSITION_AGE_HOURS`, `MAX_HOLD_TIME_SECONDS`, `ENABLE_EXIT_RETRY`, `EXIT_RETRY_WINDOW_MS`, `EXIT_RETRY_INTERVAL_MS`, `CLOSING_MAX_AGE_MS`, `FORCE_EXIT_ON_CLOSING_TIMEOUT`
- Pending-open exit resolution: `UNRESOLVED_EXIT_INITIAL_RETRY_MS`, `UNRESOLVED_EXIT_MAX_RETRY_MS`, `UNRESOLVED_EXIT_TOTAL_WINDOW_MS`, `POSITION_PENDING_OPEN_TTL_MS`
- Latency-first hot path: `HOT_PATH_MODE`, `HOT_PATH_QUEUE_CAPACITY`, `COLD_PATH_QUEUE_CAPACITY`, `ATTRIBUTION_FAST_CACHE_CAPACITY`, `PARSE_TASKS_MARKET`, `PARSE_TASKS_WALLET`, `EXIT_PRIORITY_STRICT`
- Deferred cold-path work: `PERSISTENCE_FLUSH_INTERVAL_MS`, `ANALYTICS_FLUSH_INTERVAL_MS`, `TELEGRAM_ASYNC_ONLY`
- Logging: `ENABLE_LOG_ROTATION`, `LOG_MAX_LINES`, `ENABLE_TIME_ROTATION`, `LOG_ROTATE_HOURS`, `ENABLE_LOG_CLEARING`
- Notifications and health: `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`, `HEALTH_PORT`

## Files Written By The Bot

Core state:

- `data/state.json`
- `data/portfolio-summary.json`
- `data/execution-analytics-summary.json`
- `data/telegram-daily-summary.json`
- `data/wallets_active.json`
- `data/positions.json`
- `data/position-resolver.json`
- `data/unresolved-exits.json`

Operational logs:

- `data/latency-events.jsonl`
- `data/attribution-events_*.jsonl`
- `data/raw-activity_*.jsonl`
- `data/wallet-activity*.jsonl`
- `data/wallet-scores*.jsonl`
- `data/position-lifecycle*.jsonl`

## Deployment Notes

- The bot still persists to local JSON and does not require a database.
- `ops/systemd/polymarket-copy-bot.service` remains the reference service unit for Linux VPS deployment.
- `/health` is exposed on `HEALTH_PORT` for liveness and runtime inspection.

## Recommended Workflow

1. Start in paper mode.
2. Watch Telegram and `/health`.
3. Review `portfolio-summary.json`, `execution-analytics-summary.json`, and rotated attribution logs.
4. Run `cargo run --release -- check-wallet` before switching to live mode.
5. Only then enable `EXECUTION_MODE=live`.

## References

- Polymarket API docs: https://docs.polymarket.com/api-reference
- Market WebSocket docs: https://docs.polymarket.com/market-data/websocket/overview
- Trading quickstart: https://docs.polymarket.com/trading/quickstart
- Official Rust SDK: https://github.com/Polymarket/rs-clob-client
