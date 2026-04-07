# Latency Checklist

Use this checklist before risking real capital.

## Placement

- Use an eligible VPS region close to Polymarket infrastructure.
- Benchmark several regions with repeated requests to `https://clob.polymarket.com/time`.
- Keep the VPS with the lowest median and p95 round-trip time.

## Wallet setup

- Fund only the bot wallet you intend to use.
- Complete token approvals before going live.
- Confirm your `POLYMARKET_PROFILE_ADDRESS` matches the account that actually holds positions.

## Bot tuning

- Start with `EXECUTION_MODE=paper` before enabling live trading.
- Keep `POLL_INTERVAL_MS=100` and `POLL_BURST_INTERVAL_MS=50` as the starting point.
- Only lower the poll interval if you stay within Polymarket Data API limits.
- Keep `MAX_SLIPPAGE_BPS` conservative while validating fills.

## Validation

- Watch `GET /health` for `last_detection_ms` and `last_execution_ms`.
- Confirm `/health` reports `execution_mode` as `paper` while testing.
- Compare Telegram alerts with actual Polymarket fills.
- Run the bot with very small size first and inspect rejection or partial-fill rates.
