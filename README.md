# Polymarket Copy Bot

Rust copy-trading bot for Polymarket with:

- real-time market and orderbook ingestion
- public and authenticated wallet activity ingestion
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
- `src/websocket/stream_router.rs` preserves ordering and routes decoded events
- `src/orderbook/orderbook_state.rs` and `src/orderbook/orderbook_levels.rs` maintain in-memory books
- `src/detection/liquidity_sweep_detector.rs` and `src/detection/trade_inference.rs` convert orderbook changes into confirmed trade signals

### 3. Wallet activity and attribution

Wallet attribution now has multiple inputs:

- `src/wallet/activity_stream.rs` consumes the authenticated user stream when available
- the same module also consumes the public activity stream
- parsed activity is matched back to market-side signals and persisted for later analysis
- `src/raw_activity_logger.rs` records raw and parsed public-activity diagnostics
- `src/attribution_logger.rs` records runtime, stream, signal, and matching events

### 4. Dynamic wallet tracking

Tracked wallets are now registry-backed instead of being a static one-wallet assumption:

- `src/wallet_registry.rs` maintains the active wallet set in memory with O(1) lookup
- the registry persists to `data/wallets_active.json`
- `src/wallet_scanner.rs` rescans recent activity every 3 minutes, scores wallets, updates the registry, and marks inactive wallets without deleting them
- `src/wallet_score.rs` prefers early-entry wallets over late chasers

`TARGET_WALLETS` still seeds the registry, but runtime tracking can evolve as the scanner refreshes activity.

### 5. Prediction and wallet quality

`src/prediction.rs` ranks candidate wallets and markets before risk evaluation:

- combines observed wallet behavior, market behavior, and wallet quality
- uses `MIN_WALLET_SCORE` to reject low-quality wallets
- supports multi-wallet prioritization instead of a single-wallet path
- can still execute without a prediction upgrade if a direct matched trade arrives

### 6. Price-aware risk engine

`src/risk.rs` is now price-banded and position-aware:

- `PriceBand::{Low, Mid, High}` changes slippage, spread, and size rules by source price
- blocks very high-price entries above `0.92`
- replaces the old hard entry ceiling with a remaining-edge check
- rejects late copies via `MAX_COPY_DELAY_MS`
- rejects thin books via spread and liquidity checks
- rejects duplicate positions, stale positions, and opposite-side exposure unless `ALLOW_HEDGING=true`

The design goal is "copy where edge still exists", not "copy everything late".

### 7. Unified execution contract

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

### 8. Portfolio, position identity, and lifecycle

The portfolio system now uses a stronger identity model:

- `src/models.rs` defines `PositionKey { condition_id, outcome, source_wallet }`
- `src/portfolio.rs` uses that key to match buys and sells
- positions carry `PositionState::{Open, Closing, Closed, Stale}`
- stale positions are isolated instead of deleted
- `PortfolioSnapshot` tracks `realized_pnl`, `unrealized_pnl`, `total_exposure`, and cash

This fixes the earlier asset-only matching problems that caused orphan exits and misleading PnL.

### 9. Position registry and ownership

`src/position_registry.rs` persists copied-position ownership independently of wallet activity:

- persists to `data/positions.json`
- records lifecycle transitions to `data/position-lifecycle.jsonl`
- tracks open/closed status by copied position

This is important because exit eligibility is now driven by owned positions, not just by whether a wallet is still active in the wallet registry.

### 10. Exit management

Exits are now first-class and mandatory.

The runtime in `src/main.rs` applies three exit layers:

1. Source-follow exits
   Source SELL events attempt to close the matching copied position immediately.

2. Price-based exits
   The exit watcher can trigger `TAKE_PROFIT` and `STOP_LOSS` exits from live quotes.

3. Time-based exits
   Positions older than the configured hold limit are force-exited.

Important protections:

- exits have priority over entries
- duplicate exits are blocked with a shared closing set
- orphan exits use fallback resolution plus a short retry buffer
- exit logic uses the position registry, so wallet removal does not strand positions

### 11. Observability and health

Operational visibility is spread across:

- `src/notifier.rs`
  Telegram trade alerts and pinned daily summary
- `src/latency.rs`
  processed/skip latency JSONL logging
- `src/latency_monitor.rs`
  pause-and-reconnect fail-safe when the hot path slows down
- `src/health.rs`
  `GET /health` with readiness, counts, latency, pause state, and last error

The Telegram summary now reports:

- realized PnL
- unrealized PnL
- total PnL
- open positions
- exposure

and warns when exits have not executed yet.

### 12. Log rotation and retention

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

The end-to-end trading loop is:

1. Read market deltas from the market WebSocket.
2. Update the local orderbook and infer a probable trade.
3. Correlate that trade against wallet activity.
4. Rank the wallet and market opportunity.
5. Apply price-band, timing, liquidity, and exposure checks.
6. Submit an order through the live or paper executor.
7. Update the portfolio and position registry using filled size only.
8. Continue managing the position until source exit, TP/SL, or time exit closes it.

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
- Risk: `ENABLE_PRICE_BANDS`, `MIN_EDGE_THRESHOLD`, `MAX_COPY_DELAY_MS`, `MIN_LIQUIDITY`, `MIN_WALLET_SCORE`, `ALLOW_HEDGING`
- Lifecycle and exits: `MAX_POSITION_AGE_HOURS`, `MAX_HOLD_TIME_SECONDS`, `ENABLE_EXIT_RETRY`
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
