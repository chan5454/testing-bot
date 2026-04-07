use rust_decimal::Decimal;
use rust_decimal::RoundingStrategy;
use rust_decimal_macros::dec;
use std::fmt::{Display, Formatter};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::config::Settings;
use crate::execution::ExecutionSide;
use crate::models::{ActivityEntry, PortfolioSnapshot, PositionKey};

#[derive(Clone, Debug)]
pub struct CopyDecision {
    pub token_id: String,
    pub side: ExecutionSide,
    pub notional: Decimal,
    pub size: Decimal,
    pub position_key: Option<PositionKey>,
    pub price_band: PriceBand,
    pub max_market_spread_bps: u32,
    pub min_top_of_book_ratio: Decimal,
    pub min_visible_liquidity_usd: Decimal,
    pub max_slippage_bps: u32,
    pub max_source_price_slippage_bps: u32,
    pub min_edge_threshold: Decimal,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PriceBand {
    Low,
    Mid,
    High,
}

impl PriceBand {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Low => "LOW",
            Self::Mid => "MID",
            Self::High => "HIGH",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SkipReason {
    pub code: &'static str,
    pub detail: String,
}

impl SkipReason {
    pub fn new(code: &'static str, detail: impl Into<String>) -> Self {
        Self {
            code,
            detail: detail.into(),
        }
    }
}

impl Display for SkipReason {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.detail)
    }
}

#[derive(Clone)]
pub struct RiskEngine {
    settings: Settings,
}

impl RiskEngine {
    pub fn new(settings: Settings) -> Self {
        Self { settings }
    }

    pub fn should_log_skips(&self) -> bool {
        self.settings.log_skipped_trades
    }

    pub fn enable_exit_retry(&self) -> bool {
        self.settings.enable_exit_retry
    }

    fn price_band_rules(
        &self,
        source_price: Decimal,
        price_band: PriceBand,
    ) -> Result<PriceBandRules, SkipReason> {
        if !self.settings.enable_price_bands {
            return Ok(PriceBandRules {
                max_slippage_bps: self.settings.max_slippage_bps,
                max_spread_bps: self.settings.max_market_spread_bps,
                size_multiplier: size_multiplier_for_price(source_price),
            });
        }

        match price_band {
            PriceBand::Low => Ok(PriceBandRules {
                max_slippage_bps: 400,
                max_spread_bps: 800,
                size_multiplier: dec!(1.2),
            }),
            PriceBand::Mid => Ok(PriceBandRules {
                max_slippage_bps: 300,
                max_spread_bps: 600,
                size_multiplier: Decimal::ONE,
            }),
            PriceBand::High => {
                if source_price > dec!(0.92) {
                    tracing::info!(
                        reason_code = "high_price_blocked",
                        source_price = %source_price.round_dp(4),
                        "high_price_blocked"
                    );
                    return Err(SkipReason::new(
                        "high_price_blocked",
                        format!(
                            "source price {} is above the high-band cutoff 0.92",
                            source_price.round_dp(4)
                        ),
                    ));
                }

                Ok(PriceBandRules {
                    max_slippage_bps: 150,
                    max_spread_bps: 300,
                    size_multiplier: dec!(0.3),
                })
            }
        }
    }

    pub fn evaluate(
        &self,
        entry: &ActivityEntry,
        portfolio: &PortfolioSnapshot,
    ) -> Result<CopyDecision, SkipReason> {
        let source_notional = entry.usdc_decimal().map_err(|error| {
            SkipReason::new(
                "invalid_source_notional",
                format!("failed to parse usdc size: {error}"),
            )
        })?;
        if self.settings.min_source_trade_usdc > Decimal::ZERO
            && source_notional < self.settings.min_source_trade_usdc
        {
            return Err(SkipReason::new(
                "source_trade_below_minimum",
                format!(
                    "source trade {} is below minimum source trade filter {}",
                    source_notional.round_dp(4),
                    self.settings.min_source_trade_usdc.round_dp(4)
                ),
            ));
        }
        let source_price = entry.price_decimal().map_err(|error| {
            SkipReason::new(
                "invalid_source_price",
                format!("failed to parse source price: {error}"),
            )
        })?;
        if source_price <= Decimal::ZERO {
            return Err(SkipReason::new(
                "source_price_zero",
                "source trade price is zero",
            ));
        }
        let source_size = entry.size_decimal().map_err(|error| {
            SkipReason::new(
                "invalid_source_size",
                format!("failed to parse source size: {error}"),
            )
        })?;
        let price_band = get_price_band(source_price);
        let base_scaled_notional =
            scaled_notional(source_notional, self.settings.copy_scale_above_five_usd);

        let market_exposure = portfolio.market_exposure(&entry.condition_id);
        let hard_cap = self.total_exposure_cap(portfolio);
        let remaining_total = (hard_cap - portfolio.active_total_exposure()).max(Decimal::ZERO);
        let remaining_market =
            (self.settings.max_market_exposure_usd - market_exposure).max(Decimal::ZERO);

        match entry.side.as_str() {
            "BUY" if self.settings.allow_buy => {
                let price_band_rules = self.price_band_rules(source_price, price_band)?;
                tracing::info!(
                    price_band = price_band.as_str(),
                    source_price = %source_price.round_dp(4),
                    source_wallet = %entry.proxy_wallet,
                    condition_id = %entry.condition_id,
                    "price_band_selected"
                );
                if let Some(delay_ms) = copy_delay_ms(entry.timestamp)
                    && delay_ms > self.settings.max_copy_delay_ms
                {
                    tracing::info!(
                        reason_code = "late_entry_rejected",
                        delay_ms,
                        max_copy_delay_ms = self.settings.max_copy_delay_ms,
                        source_wallet = %entry.proxy_wallet,
                        condition_id = %entry.condition_id,
                        "late_entry_rejected"
                    );
                    return Err(SkipReason::new(
                        "too_late",
                        format!(
                            "source trade delay {}ms exceeded max {}ms",
                            delay_ms, self.settings.max_copy_delay_ms
                        ),
                    ));
                }
                let requested_key = entry.position_key();
                if portfolio.has_stale_position_key(&requested_key)
                    || portfolio.has_stale_position_for_condition(&entry.condition_id)
                {
                    tracing::info!(
                        reason_code = "stale_position_ignored",
                        condition_id = %entry.condition_id,
                        outcome = %entry.outcome,
                        source_wallet = %entry.proxy_wallet,
                        "stale_position_ignored"
                    );
                    return Err(SkipReason::new(
                        "stale_position_ignored",
                        format!(
                            "stale_position_ignored: condition {} is isolated from new entries",
                            entry.condition_id
                        ),
                    ));
                }
                if portfolio.has_position_key(&requested_key) {
                    return Err(SkipReason::new(
                        "position_exists",
                        format!(
                            "position already exists for condition {} outcome {} wallet {}",
                            entry.condition_id, entry.outcome, entry.proxy_wallet
                        ),
                    ));
                }
                if !self.settings.allow_hedging
                    && let Some(existing) =
                        portfolio.opposite_side_position(&entry.condition_id, &entry.outcome)
                {
                    tracing::info!(
                        reason_code = "opposite_side_blocked",
                        condition_id = %entry.condition_id,
                        requested_outcome = %entry.outcome,
                        existing_outcome = %existing.outcome,
                        "opposite_side_blocked"
                    );
                    return Err(SkipReason::new(
                        "opposite_side_blocked",
                        format!(
                            "opposite_side_blocked: existing outcome {} for condition {} prevents {}",
                            existing.outcome, entry.condition_id, entry.outcome
                        ),
                    ));
                }
                let scaled_notional = (base_scaled_notional
                    * price_band_rules.size_multiplier
                    * wallet_performance_multiplier(portfolio, &entry.proxy_wallet))
                .min(self.settings.max_copy_notional_usd)
                .max(Decimal::ZERO);
                let allowed_notional = scaled_notional
                    .min(remaining_total)
                    .min(remaining_market)
                    .min(portfolio.cash_balance.max(Decimal::ZERO));
                if allowed_notional < self.settings.min_copy_notional_usd {
                    return Err(SkipReason::new(
                        "buy_notional_below_minimum",
                        format!(
                            "allowed copy notional {} is below minimum {} after exposure and cash caps",
                            allowed_notional.round_dp(4),
                            self.settings.min_copy_notional_usd.round_dp(4)
                        ),
                    ));
                }
                Ok(CopyDecision {
                    token_id: entry.asset.clone(),
                    side: ExecutionSide::Buy,
                    notional: allowed_notional,
                    size: Decimal::ZERO,
                    position_key: Some(requested_key),
                    price_band,
                    max_market_spread_bps: price_band_rules.max_spread_bps,
                    min_top_of_book_ratio: self.settings.min_top_of_book_ratio,
                    min_visible_liquidity_usd: self.settings.min_liquidity.max(Decimal::ZERO),
                    max_slippage_bps: price_band_rules.max_slippage_bps,
                    max_source_price_slippage_bps: price_band_rules.max_slippage_bps,
                    min_edge_threshold: self.settings.min_edge_threshold.max(Decimal::ZERO),
                })
            }
            "SELL" if self.settings.allow_sell => {
                let Some(resolved_position) = portfolio.resolve_position_to_sell(entry) else {
                    tracing::info!(
                        reason_code = "no_position_to_sell",
                        condition_id = %entry.condition_id,
                        outcome = %entry.outcome,
                        source_wallet = %entry.proxy_wallet,
                        asset = %entry.asset,
                        "no_position_to_sell"
                    );
                    return Err(SkipReason::new(
                        "no_position_to_sell",
                        format!(
                            "portfolio does not hold asset {} for wallet {}",
                            entry.asset, entry.proxy_wallet
                        ),
                    ));
                };
                if resolved_position.used_fallback {
                    tracing::info!(
                        event = "fallback_exit_resolution",
                        reason_code = "fallback_exit_used",
                        requested_condition_id = %entry.condition_id,
                        requested_outcome = %entry.outcome,
                        requested_wallet = %entry.proxy_wallet,
                        resolved_condition_id = %resolved_position.key.condition_id,
                        resolved_outcome = %resolved_position.outcome,
                        resolved_wallet = %resolved_position.source_wallet,
                        resolved_asset = %resolved_position.asset,
                        "fallback_exit_resolution"
                    );
                }
                let scaled_size = scaled_size(
                    source_notional,
                    source_size,
                    self.settings.copy_scale_above_five_usd,
                )
                .min(resolved_position.size)
                .round_dp_with_strategy(6, RoundingStrategy::ToZero);
                if scaled_size <= dec!(0) {
                    return Err(SkipReason::new(
                        "sell_size_rounded_to_zero",
                        format!(
                            "scaled sell size {} rounded to zero against held size {}",
                            source_size.round_dp(6),
                            resolved_position.size.round_dp(6)
                        ),
                    ));
                }
                Ok(CopyDecision {
                    token_id: resolved_position.asset.clone(),
                    side: ExecutionSide::Sell,
                    notional: source_notional.min(resolved_position.current_value),
                    size: scaled_size,
                    position_key: Some(resolved_position.key),
                    price_band,
                    max_market_spread_bps: self.settings.max_market_spread_bps,
                    min_top_of_book_ratio: self.settings.min_top_of_book_ratio,
                    min_visible_liquidity_usd: self.settings.min_liquidity.max(Decimal::ZERO),
                    max_slippage_bps: self.settings.max_slippage_bps,
                    max_source_price_slippage_bps: self.settings.max_source_price_slippage_bps,
                    min_edge_threshold: self.settings.min_edge_threshold.max(Decimal::ZERO),
                })
            }
            "BUY" => Err(SkipReason::new(
                "buy_disabled",
                "buy copying is disabled by ALLOW_BUY",
            )),
            "SELL" => Err(SkipReason::new(
                "sell_disabled",
                "sell copying is disabled by ALLOW_SELL",
            )),
            other => Err(SkipReason::new(
                "unsupported_side",
                format!("unsupported trade side {other}"),
            )),
        }
    }

    fn total_exposure_cap(&self, portfolio: &PortfolioSnapshot) -> Decimal {
        let configured_cap = self.settings.max_total_exposure_usd.max(Decimal::ZERO);
        let starting_capital = self.settings.start_capital_usd;
        if configured_cap.is_zero() || starting_capital <= Decimal::ZERO {
            return configured_cap;
        }

        let realized_equity = (starting_capital + portfolio.realized_pnl).max(Decimal::ZERO);
        let exposure_ratio = configured_cap / starting_capital;
        (realized_equity * exposure_ratio).max(Decimal::ZERO)
    }
}

#[derive(Clone, Copy, Debug)]
struct PriceBandRules {
    max_slippage_bps: u32,
    max_spread_bps: u32,
    size_multiplier: Decimal,
}

pub fn get_price_band(price: Decimal) -> PriceBand {
    if price < dec!(0.4) {
        PriceBand::Low
    } else if price < dec!(0.8) {
        PriceBand::Mid
    } else {
        PriceBand::High
    }
}

pub fn bounded_buy_price(best_ask: Decimal, bps: u32, tick_size: Decimal) -> Decimal {
    let multiplier = Decimal::ONE + Decimal::from(bps) / dec!(10000);
    round_to_tick((best_ask * multiplier).min(dec!(0.9999)), tick_size, true)
}

pub fn bounded_sell_price(best_bid: Decimal, bps: u32, tick_size: Decimal) -> Decimal {
    let multiplier = Decimal::ONE - Decimal::from(bps) / dec!(10000);
    round_to_tick((best_bid * multiplier).max(dec!(0.0001)), tick_size, false)
}

pub fn max_buy_price_from_source(source_price: Decimal, bps: u32, tick_size: Decimal) -> Decimal {
    let multiplier = Decimal::ONE + Decimal::from(bps) / dec!(10000);
    round_to_tick(
        (source_price * multiplier).min(dec!(0.9999)),
        tick_size,
        false,
    )
}

pub fn min_sell_price_from_source(source_price: Decimal, bps: u32, tick_size: Decimal) -> Decimal {
    let multiplier = Decimal::ONE - Decimal::from(bps) / dec!(10000);
    round_to_tick(
        (source_price * multiplier).max(dec!(0.0001)),
        tick_size,
        true,
    )
}

pub fn market_spread_bps(best_bid: Decimal, best_ask: Decimal) -> Option<Decimal> {
    if best_bid <= Decimal::ZERO || best_ask <= Decimal::ZERO || best_ask < best_bid {
        return None;
    }

    let midpoint = (best_bid + best_ask) / dec!(2);
    if midpoint <= Decimal::ZERO {
        return None;
    }

    Some(((best_ask - best_bid) / midpoint) * dec!(10000))
}

fn scaled_notional(source_notional: Decimal, scale_above_five: Decimal) -> Decimal {
    if source_notional > dec!(5) {
        source_notional * scale_above_five
    } else {
        source_notional
    }
}

fn scaled_size(
    source_notional: Decimal,
    source_size: Decimal,
    scale_above_five: Decimal,
) -> Decimal {
    if source_notional > dec!(5) {
        source_size * scale_above_five
    } else {
        source_size
    }
}

fn size_multiplier_for_price(price: Decimal) -> Decimal {
    if price > dec!(0.97) {
        dec!(0.25)
    } else if price > dec!(0.90) {
        dec!(0.5)
    } else {
        Decimal::ONE
    }
}

fn wallet_performance_multiplier(portfolio: &PortfolioSnapshot, wallet: &str) -> Decimal {
    if portfolio.wallet_unrealized_pnl(wallet) < Decimal::ZERO {
        dec!(0.5)
    } else {
        Decimal::ONE
    }
}

fn copy_delay_ms(timestamp: i64) -> Option<u64> {
    if timestamp <= 0 {
        return None;
    }

    let normalized_timestamp_ms = if timestamp >= 10_000_000_000 {
        timestamp
    } else {
        timestamp.saturating_mul(1000)
    };
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()?
        .as_millis() as i64;

    Some(now_ms.saturating_sub(normalized_timestamp_ms).max(0) as u64)
}

fn round_to_tick(value: Decimal, tick_size: Decimal, round_up: bool) -> Decimal {
    if tick_size.is_zero() {
        return value;
    }
    let ticks = value / tick_size;
    let rounded_ticks = if round_up {
        ticks.ceil()
    } else {
        ticks.floor()
    };
    (rounded_ticks * tick_size).round_dp_with_strategy(4, RoundingStrategy::MidpointAwayFromZero)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::time::Duration;

    use chrono::Utc;

    use super::*;
    use crate::config::{ExecutionMode, Settings};
    use crate::models::{PortfolioPosition, PortfolioSnapshot};

    #[test]
    fn scales_only_after_five_dollars() {
        assert_eq!(scaled_notional(dec!(5), dec!(0.25)), dec!(5));
        assert_eq!(scaled_notional(dec!(12), dec!(0.25)), dec!(3));
    }

    #[test]
    fn buy_price_rounds_up_to_tick() {
        assert_eq!(bounded_buy_price(dec!(0.501), 100, dec!(0.01)), dec!(0.51));
    }

    #[test]
    fn sell_price_rounds_down_to_tick() {
        assert_eq!(bounded_sell_price(dec!(0.501), 100, dec!(0.01)), dec!(0.49));
    }

    #[test]
    fn source_buy_cap_rounds_down_to_stay_within_limit() {
        assert_eq!(
            max_buy_price_from_source(dec!(0.5), 100, dec!(0.01)),
            dec!(0.5)
        );
    }

    #[test]
    fn source_sell_floor_rounds_up_to_stay_within_limit() {
        assert_eq!(
            min_sell_price_from_source(dec!(0.5), 100, dec!(0.01)),
            dec!(0.5)
        );
    }

    #[test]
    fn market_spread_uses_midpoint_basis() {
        assert_eq!(
            market_spread_bps(dec!(0.48), dec!(0.52)).expect("spread"),
            dec!(800)
        );
    }

    #[test]
    fn price_band_switches_at_expected_boundaries() {
        assert_eq!(get_price_band(dec!(0.39)), PriceBand::Low);
        assert_eq!(get_price_band(dec!(0.40)), PriceBand::Mid);
        assert_eq!(get_price_band(dec!(0.79)), PriceBand::Mid);
        assert_eq!(get_price_band(dec!(0.80)), PriceBand::High);
    }

    fn sample_settings() -> Settings {
        Settings {
            execution_mode: ExecutionMode::Paper,
            polymarket_host: "https://clob.polymarket.com".to_owned(),
            polymarket_data_api: "https://data-api.polymarket.com".to_owned(),
            polymarket_gamma_api: "https://gamma-api.polymarket.com".to_owned(),
            polymarket_market_ws: "wss://example.com/ws/market".to_owned(),
            polymarket_user_ws: "wss://example.com/ws/user".to_owned(),
            polymarket_activity_ws: "wss://example.com/ws/activity".to_owned(),
            polymarket_chain_id: 137,
            polymarket_signature_type: 0,
            polymarket_private_key: None,
            polymarket_funder_address: None,
            polymarket_profile_address: None,
            polygon_rpc_url: "https://polygon-rpc.com".to_owned(),
            polygon_rpc_fallback_urls: Vec::new(),
            rpc_latency_threshold: Duration::from_millis(300),
            rpc_confirmation_timeout: Duration::from_secs(10),
            min_required_matic: dec!(0.1),
            min_required_usdc: dec!(25),
            polymarket_usdc_address: "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174".to_owned(),
            polymarket_spender_address: "0x0000000000000000000000000000000000000001".to_owned(),
            auto_approve_usdc_allowance: false,
            usdc_approval_amount: dec!(1000),
            target_activity_ws_api_key: None,
            target_activity_ws_secret: None,
            target_activity_ws_passphrase: None,
            target_profile_addresses: vec!["0xabc".to_owned()],
            start_capital_usd: dec!(200),
            paper_execution_delay: Duration::ZERO,
            copy_only_new_trades: true,
            source_trades_limit: 50,
            http_timeout: Duration::from_secs(2),
            market_cache_ttl: Duration::from_secs(10),
            market_raw_ring_capacity: 8192,
            market_parser_workers: 1,
            market_subscription_batch_size: 100,
            market_subscription_delay: Duration::from_millis(40),
            wallet_ring_capacity: 1024,
            wallet_parser_workers: 1,
            wallet_subscription_batch_size: 250,
            wallet_subscription_delay: Duration::from_millis(20),
            liquidity_sweep_threshold: dec!(1),
            imbalance_threshold: dec!(2),
            delta_price_move_bps: 40,
            delta_size_drop_ratio: dec!(0.25),
            delta_min_size_drop: dec!(1),
            inference_confirmation_window: Duration::from_millis(400),
            activity_stream_enabled: true,
            activity_match_window: Duration::from_millis(200),
            activity_price_tolerance: dec!(0.01),
            activity_size_tolerance_ratio: dec!(0.5),
            activity_cache_ttl: Duration::from_millis(1500),
            fallback_market_request_interval: Duration::from_millis(1000),
            fallback_global_requests_per_minute: 30,
            activity_correlation_window: Duration::from_millis(400),
            attribution_lookback: Duration::from_millis(2500),
            attribution_trades_limit: 100,
            copy_scale_above_five_usd: dec!(0.25),
            min_copy_notional_usd: dec!(1),
            max_copy_notional_usd: dec!(25),
            max_total_exposure_usd: dec!(150),
            max_market_exposure_usd: dec!(40),
            min_source_trade_usdc: dec!(0),
            max_market_spread_bps: 500,
            min_top_of_book_ratio: dec!(1.25),
            max_slippage_bps: 300,
            max_source_price_slippage_bps: 200,
            latency_fail_safe_enabled: true,
            max_latency: Duration::from_millis(500),
            average_latency_threshold: Duration::from_millis(350),
            latency_monitor_interval: Duration::from_millis(50),
            latency_reconnect_settle: Duration::from_millis(750),
            prediction_validation_timeout: Duration::from_millis(1500),
            log_attribution_events: true,
            activity_parser_debug: false,
            log_latency_events: true,
            log_skipped_trades: true,
            enable_log_rotation: true,
            log_max_lines: 30_000,
            enable_time_rotation: true,
            log_rotate_hours: 6,
            enable_log_clearing: false,
            allow_buy: true,
            allow_sell: true,
            allow_hedging: false,
            enable_price_bands: true,
            enable_realistic_paper: true,
            min_edge_threshold: dec!(0.05),
            max_copy_delay_ms: u64::MAX,
            min_liquidity: dec!(50),
            min_wallet_score: dec!(0.6),
            max_position_age_hours: 6,
            max_hold_time_seconds: 1_800,
            enable_exit_retry: true,
            telegram_bot_token: "token".to_owned(),
            telegram_chat_id: "chat".to_owned(),
            health_port: 3000,
            data_dir: PathBuf::from("./data"),
        }
    }

    fn sample_entry(
        side: &str,
        asset: &str,
        size: f64,
        usdc_size: f64,
        wallet: &str,
    ) -> ActivityEntry {
        ActivityEntry {
            proxy_wallet: wallet.to_owned(),
            timestamp: 1,
            condition_id: "condition-1".to_owned(),
            type_name: "TRADE".to_owned(),
            size,
            usdc_size,
            transaction_hash: "0xhash".to_owned(),
            price: if size > 0.0 { usdc_size / size } else { 0.0 },
            asset: asset.to_owned(),
            side: side.to_owned(),
            outcome_index: 0,
            title: "Sample market".to_owned(),
            slug: "sample-market".to_owned(),
            event_slug: "sample-event".to_owned(),
            outcome: "YES".to_owned(),
        }
    }

    fn sample_portfolio() -> PortfolioSnapshot {
        PortfolioSnapshot {
            fetched_at: Utc::now(),
            total_value: dec!(205),
            total_exposure: dec!(5),
            cash_balance: dec!(200),
            realized_pnl: Decimal::ZERO,
            unrealized_pnl: Decimal::ZERO,
            positions: vec![PortfolioPosition {
                asset: "asset-1".to_owned(),
                condition_id: "condition-1".to_owned(),
                title: "Sample market".to_owned(),
                outcome: "YES".to_owned(),
                source_wallet: "0xsource".to_owned(),
                state: crate::models::PositionState::Open,
                size: dec!(10),
                current_value: dec!(5),
                average_entry_price: dec!(0.4),
                current_price: dec!(0.5),
                cost_basis: dec!(4),
                unrealized_pnl: dec!(1),
                opened_at: Some(Utc::now()),
                source_trade_timestamp_unix: 1,
                closing_started_at: None,
            }],
        }
    }

    #[test]
    fn sell_trade_is_copyable_when_asset_is_held() {
        let engine = RiskEngine::new(sample_settings());
        let entry = sample_entry("SELL", "asset-1", 8.0, 4.0, "0xsource");
        let decision = engine
            .evaluate(&entry, &sample_portfolio())
            .expect("sell should be copyable");

        assert_eq!(decision.side, ExecutionSide::Sell);
        assert_eq!(decision.token_id, "asset-1");
        assert_eq!(decision.size, dec!(8));
    }

    #[test]
    fn sell_trade_uses_fallback_resolution_when_wallet_mismatch() {
        let engine = RiskEngine::new(sample_settings());
        let entry = sample_entry("SELL", "asset-1", 8.0, 4.0, "0xother");
        let decision = engine
            .evaluate(&entry, &sample_portfolio())
            .expect("sell should resolve through fallback");

        assert_eq!(decision.side, ExecutionSide::Sell);
        assert_eq!(decision.token_id, "asset-1");
        assert_eq!(decision.size, dec!(8));
        assert_eq!(
            decision.position_key.expect("position key").source_wallet,
            "0xsource"
        );
    }

    #[test]
    fn buy_cap_compounds_with_realized_profit() {
        let mut settings = sample_settings();
        settings.copy_scale_above_five_usd = Decimal::ONE;
        settings.max_copy_notional_usd = dec!(100);
        let engine = RiskEngine::new(settings);
        let entry = sample_entry("BUY", "asset-2", 100.0, 50.0, "0xsource");
        let mut portfolio = sample_portfolio();
        portfolio.realized_pnl = dec!(40);
        portfolio.total_exposure = dec!(150);
        portfolio.cash_balance = dec!(100);
        portfolio.total_value = dec!(240);
        portfolio.positions = vec![PortfolioPosition {
            asset: "asset-held".to_owned(),
            condition_id: "condition-held".to_owned(),
            title: "Held market".to_owned(),
            outcome: "YES".to_owned(),
            source_wallet: "0xsource".to_owned(),
            state: crate::models::PositionState::Open,
            size: dec!(150),
            current_value: dec!(150),
            average_entry_price: dec!(1),
            current_price: dec!(1),
            cost_basis: dec!(150),
            unrealized_pnl: Decimal::ZERO,
            opened_at: Some(Utc::now()),
            source_trade_timestamp_unix: 1,
            closing_started_at: None,
        }];

        let decision = engine
            .evaluate(&entry, &portfolio)
            .expect("compounded cap should allow an additional buy");

        assert_eq!(decision.notional, dec!(30));
    }

    #[test]
    fn buy_cap_shrinks_with_realized_loss() {
        let mut settings = sample_settings();
        settings.copy_scale_above_five_usd = Decimal::ONE;
        settings.max_copy_notional_usd = dec!(100);
        let engine = RiskEngine::new(settings);
        let entry = sample_entry("BUY", "asset-2", 20.0, 10.0, "0xsource");
        let mut portfolio = sample_portfolio();
        portfolio.realized_pnl = dec!(-20);
        portfolio.total_exposure = dec!(140);
        portfolio.cash_balance = dec!(100);
        portfolio.total_value = dec!(180);
        portfolio.positions = vec![PortfolioPosition {
            asset: "asset-held".to_owned(),
            condition_id: "condition-held".to_owned(),
            title: "Held market".to_owned(),
            outcome: "YES".to_owned(),
            source_wallet: "0xsource".to_owned(),
            state: crate::models::PositionState::Open,
            size: dec!(140),
            current_value: dec!(140),
            average_entry_price: dec!(1),
            current_price: dec!(1),
            cost_basis: dec!(140),
            unrealized_pnl: Decimal::ZERO,
            opened_at: Some(Utc::now()),
            source_trade_timestamp_unix: 1,
            closing_started_at: None,
        }];

        let reason = engine
            .evaluate(&entry, &portfolio)
            .expect_err("realized losses should reduce the compounded exposure cap");

        assert_eq!(reason.code, "buy_notional_below_minimum");
        assert!(reason.detail.contains("below minimum"));
    }

    #[test]
    fn buy_notional_scales_down_in_high_price_band() {
        let mut settings = sample_settings();
        settings.copy_scale_above_five_usd = Decimal::ONE;
        let engine = RiskEngine::new(settings);
        let entry = sample_entry("BUY", "asset-2", 20.0, 18.0, "0xsource");
        let mut portfolio = sample_portfolio();
        portfolio.positions.clear();

        let decision = engine
            .evaluate(&entry, &portfolio)
            .expect("buy should remain eligible");

        assert_eq!(decision.price_band, PriceBand::High);
        assert_eq!(decision.notional.round_dp(4), dec!(5.4));
    }

    #[test]
    fn rejects_high_price_buy_above_cutoff() {
        let mut settings = sample_settings();
        settings.copy_scale_above_five_usd = Decimal::ONE;
        let engine = RiskEngine::new(settings);
        let entry = sample_entry("BUY", "asset-2", 20.0, 19.6, "0xsource");
        let mut portfolio = sample_portfolio();
        portfolio.positions.clear();

        let reason = engine
            .evaluate(&entry, &portfolio)
            .expect_err("late high-price buys should be rejected");

        assert_eq!(reason.code, "high_price_blocked");
    }

    #[test]
    fn buy_notional_is_reduced_when_wallet_open_pnl_is_negative() {
        let mut settings = sample_settings();
        settings.copy_scale_above_five_usd = Decimal::ONE;
        let engine = RiskEngine::new(settings);
        let entry = sample_entry("BUY", "asset-2", 20.0, 10.0, "0xsource");
        let mut portfolio = sample_portfolio();
        portfolio.positions[0].unrealized_pnl = dec!(-2);
        portfolio.positions.clear();
        portfolio.positions.push(PortfolioPosition {
            asset: "asset-held".to_owned(),
            condition_id: "condition-held".to_owned(),
            title: "Held market".to_owned(),
            outcome: "YES".to_owned(),
            source_wallet: "0xsource".to_owned(),
            state: crate::models::PositionState::Open,
            size: dec!(5),
            current_value: dec!(4),
            average_entry_price: dec!(1),
            current_price: dec!(0.8),
            cost_basis: dec!(5),
            unrealized_pnl: dec!(-1),
            opened_at: Some(Utc::now()),
            source_trade_timestamp_unix: 1,
            closing_started_at: None,
        });

        let decision = engine
            .evaluate(&entry, &portfolio)
            .expect("buy should remain eligible");

        assert_eq!(decision.notional, dec!(5));
    }
}
