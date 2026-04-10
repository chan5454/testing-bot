use rust_decimal::Decimal;
use rust_decimal::RoundingStrategy;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal_macros::dec;
use std::collections::{HashMap, VecDeque};
use std::fmt::{Display, Formatter};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

use chrono::Utc;

use crate::config::Settings;
use crate::execution::ExecutionSide;
use crate::models::{
    ActivityEntry, BestQuote, MarketType, PortfolioSnapshot, PositionKey, ResolvedPosition,
    classify_market,
};

#[derive(Clone, Debug)]
pub struct CopyDecision {
    pub token_id: String,
    pub side: ExecutionSide,
    pub notional: Decimal,
    pub size: Decimal,
    pub size_was_scaled: bool,
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
pub enum TradingMode {
    Normal,
    Drawdown,
    HardStop,
}

impl TradingMode {
    pub fn metric_name(self) -> &'static str {
        match self {
            Self::Normal => "trading_mode_normal",
            Self::Drawdown => "trading_mode_drawdown",
            Self::HardStop => "trading_mode_hard_stop",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AdaptiveRiskContext {
    pub mode: TradingMode,
    pub no_trade_relaxation_active: bool,
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

#[derive(Clone, Debug)]
pub struct TradeQualityScore {
    pub liquidity_score: Decimal,
    pub spread_score: Decimal,
    pub slippage_score: Decimal,
    pub wallet_score: Decimal,
    pub latency_score: Decimal,
    pub total_score: Decimal,
}

#[derive(Clone, Debug)]
pub struct MarketQualityObservation {
    pub market_type: MarketType,
    pub signal_age_ms: Option<u64>,
    pub wallet_trades_per_minute: u32,
    pub wallet_avg_hold_ms: Option<u64>,
    pub wallet_hold_samples: u64,
    pub market_cooldown_active: bool,
    pub conflicting_signal: bool,
}

#[derive(Default)]
struct MarketQualityState {
    wallets: HashMap<String, WalletBehaviorStats>,
    markets: HashMap<String, MarketBehaviorStats>,
    first_entry_signal_after_last_allow_ms: Option<i64>,
    last_entry_signal_seen_at_ms: Option<i64>,
    entry_signals_seen_since_last_allow: u32,
    last_entry_allowed_at_ms: Option<i64>,
}

#[derive(Default)]
struct WalletBehaviorStats {
    recent_trade_timestamps_ms: VecDeque<i64>,
    open_entries_by_key: HashMap<PositionKey, i64>,
    total_hold_ms: u128,
    hold_samples: u64,
}

#[derive(Default)]
struct MarketBehaviorStats {
    recent_signals: VecDeque<RecentMarketSignal>,
    last_entry_signal_at_ms: Option<i64>,
}

#[derive(Clone)]
struct RecentMarketSignal {
    timestamp_ms: i64,
    side: ExecutionSide,
    wallet: String,
}

impl Display for SkipReason {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.detail)
    }
}

#[derive(Clone)]
pub struct RiskEngine {
    settings: Settings,
    market_quality_state: Arc<RwLock<MarketQualityState>>,
}

impl RiskEngine {
    pub fn new(settings: Settings) -> Self {
        Self {
            settings,
            market_quality_state: Arc::new(RwLock::new(MarketQualityState::default())),
        }
    }

    pub fn should_log_skips(&self) -> bool {
        self.settings.log_skipped_trades
    }

    pub fn exit_retry_interval(&self) -> Duration {
        self.settings.exit_retry_interval
    }

    pub fn observe_source_trade(&self, entry: &ActivityEntry) -> MarketQualityObservation {
        let now_ms = current_time_ms().unwrap_or_default();
        let event_timestamp_ms = normalized_timestamp_ms(entry.timestamp).unwrap_or(now_ms as i64);
        let market_type = classify_market(&entry.title);
        let signal_age_ms = Some(now_ms.saturating_sub(event_timestamp_ms.max(0) as u64));
        let wallet = normalize_wallet(&entry.proxy_wallet);
        let side = normalized_execution_side(entry);
        let market_window_ms = self.settings.market_cooldown.as_millis().max(60_000) as i64;

        let mut state = self
            .market_quality_state
            .write()
            .expect("market quality write lock");
        let (wallet_trades_per_minute, wallet_avg_hold_ms, wallet_hold_samples) = {
            let wallet_stats = state.wallets.entry(wallet.clone()).or_default();
            while wallet_stats
                .recent_trade_timestamps_ms
                .front()
                .is_some_and(|timestamp_ms| {
                    event_timestamp_ms.saturating_sub(*timestamp_ms) >= 60_000
                })
            {
                wallet_stats.recent_trade_timestamps_ms.pop_front();
            }

            if matches!(side, ExecutionSide::Sell)
                && let Some(open_timestamp_ms) = wallet_stats
                    .open_entries_by_key
                    .remove(&entry.position_key())
                && event_timestamp_ms > open_timestamp_ms
            {
                wallet_stats.total_hold_ms = wallet_stats
                    .total_hold_ms
                    .saturating_add(event_timestamp_ms.saturating_sub(open_timestamp_ms) as u128);
                wallet_stats.hold_samples = wallet_stats.hold_samples.saturating_add(1);
            }

            wallet_stats
                .recent_trade_timestamps_ms
                .push_back(event_timestamp_ms);
            if matches!(side, ExecutionSide::Buy) {
                wallet_stats
                    .open_entries_by_key
                    .insert(entry.position_key(), event_timestamp_ms);
            }

            (
                wallet_stats.recent_trade_timestamps_ms.len() as u32,
                average_hold_ms(wallet_stats),
                wallet_stats.hold_samples,
            )
        };

        if matches!(side, ExecutionSide::Buy) {
            state.entry_signals_seen_since_last_allow =
                state.entry_signals_seen_since_last_allow.saturating_add(1);
            state.last_entry_signal_seen_at_ms = Some(event_timestamp_ms);
            if state.first_entry_signal_after_last_allow_ms.is_none() {
                state.first_entry_signal_after_last_allow_ms = Some(event_timestamp_ms);
            }
        }

        let market_stats = state.markets.entry(entry.condition_id.clone()).or_default();
        while market_stats.recent_signals.front().is_some_and(|recent| {
            event_timestamp_ms.saturating_sub(recent.timestamp_ms) >= market_window_ms
        }) {
            market_stats.recent_signals.pop_front();
        }

        let market_cooldown_active = matches!(side, ExecutionSide::Buy)
            && self.settings.market_cooldown > Duration::ZERO
            && market_stats
                .last_entry_signal_at_ms
                .is_some_and(|last_entry_ms| {
                    event_timestamp_ms.saturating_sub(last_entry_ms)
                        < self.settings.market_cooldown.as_millis() as i64
                });
        let conflicting_signal = matches!(side, ExecutionSide::Buy)
            && market_stats.recent_signals.iter().any(|recent| {
                recent.side != side
                    && recent.wallet != wallet
                    && event_timestamp_ms.saturating_sub(recent.timestamp_ms) < market_window_ms
            });
        market_stats.recent_signals.push_back(RecentMarketSignal {
            timestamp_ms: event_timestamp_ms,
            side,
            wallet,
        });
        if matches!(side, ExecutionSide::Buy) {
            market_stats.last_entry_signal_at_ms = Some(event_timestamp_ms);
        }

        MarketQualityObservation {
            market_type,
            signal_age_ms,
            wallet_trades_per_minute,
            wallet_avg_hold_ms,
            wallet_hold_samples,
            market_cooldown_active,
            conflicting_signal,
        }
    }

    pub fn entry_context(&self, portfolio: &PortfolioSnapshot) -> AdaptiveRiskContext {
        AdaptiveRiskContext {
            mode: self.trading_mode(portfolio),
            no_trade_relaxation_active: self.no_trade_relaxation_active(),
        }
    }

    pub fn trading_mode(&self, portfolio: &PortfolioSnapshot) -> TradingMode {
        let current_drawdown = portfolio.current_drawdown_pct.max(Decimal::ZERO);
        if portfolio.hard_stop_active
            || (self.settings.hard_stop_drawdown_pct > Decimal::ZERO
                && current_drawdown >= self.settings.hard_stop_drawdown_pct)
        {
            return TradingMode::HardStop;
        }

        if portfolio.drawdown_guard_active
            || (self.settings.max_drawdown_pct > Decimal::ZERO
                && current_drawdown >= self.settings.max_drawdown_pct)
        {
            return TradingMode::Drawdown;
        }

        TradingMode::Normal
    }

    pub fn record_entry_allowed(&self) {
        let now_ms = current_time_ms().unwrap_or_default() as i64;
        let mut state = self
            .market_quality_state
            .write()
            .expect("market quality write lock");
        state.last_entry_allowed_at_ms = Some(now_ms);
        state.entry_signals_seen_since_last_allow = 0;
        state.first_entry_signal_after_last_allow_ms = None;
        state.last_entry_signal_seen_at_ms = None;
    }

    fn no_trade_relaxation_active(&self) -> bool {
        if self.settings.no_trade_timeout.is_zero() {
            return false;
        }

        let now_ms = current_time_ms().unwrap_or_default() as i64;
        let timeout_ms = self
            .settings
            .no_trade_timeout
            .as_millis()
            .min(i64::MAX as u128) as i64;
        let state = self
            .market_quality_state
            .read()
            .expect("market quality read lock");
        if state.entry_signals_seen_since_last_allow == 0 {
            return false;
        }

        let Some(first_signal_ms) = state.first_entry_signal_after_last_allow_ms else {
            return false;
        };
        let Some(last_signal_ms) = state.last_entry_signal_seen_at_ms else {
            return false;
        };

        now_ms.saturating_sub(first_signal_ms) >= timeout_ms
            && now_ms.saturating_sub(last_signal_ms) <= timeout_ms
    }

    pub fn enforce_entry_quality_pre_quote(
        &self,
        entry: &ActivityEntry,
        observation: &MarketQualityObservation,
        context: AdaptiveRiskContext,
    ) -> Result<(), SkipReason> {
        if let Some(delay_ms) = observation.signal_age_ms
            && delay_ms > self.settings.max_copy_delay_ms
        {
            return Err(SkipReason::new(
                "too_late_strict",
                format!(
                    "source trade delay {}ms exceeded strict max {}ms",
                    delay_ms, self.settings.max_copy_delay_ms
                ),
            ));
        }

        let max_wallet_trades_per_min = effective_wallet_trades_per_minute(&self.settings, context);
        if max_wallet_trades_per_min > 0
            && observation.wallet_trades_per_minute > max_wallet_trades_per_min
        {
            return Err(SkipReason::new(
                "wallet_too_fast",
                format!(
                    "wallet {} traded {} times in the last minute above max {}",
                    entry.proxy_wallet,
                    observation.wallet_trades_per_minute,
                    max_wallet_trades_per_min
                ),
            ));
        }

        let min_wallet_avg_hold_ms = effective_min_wallet_avg_hold_ms(&self.settings, context);
        if min_wallet_avg_hold_ms > 0
            && observation.wallet_hold_samples > 0
            && observation
                .wallet_avg_hold_ms
                .is_some_and(|hold_ms| hold_ms < min_wallet_avg_hold_ms)
        {
            return Err(SkipReason::new(
                "wallet_too_fast",
                format!(
                    "wallet {} average hold {}ms is below minimum {}ms",
                    entry.proxy_wallet,
                    observation.wallet_avg_hold_ms.unwrap_or_default(),
                    min_wallet_avg_hold_ms
                ),
            ));
        }

        if observation.market_cooldown_active {
            return Err(SkipReason::new(
                "market_cooldown",
                format!(
                    "condition {} is within the {}ms market cooldown window",
                    entry.condition_id,
                    self.settings.market_cooldown.as_millis()
                ),
            ));
        }

        if observation.conflicting_signal {
            return Err(SkipReason::new(
                "conflicting_wallet_signal",
                format!(
                    "condition {} has a recent conflicting source signal",
                    entry.condition_id
                ),
            ));
        }

        Ok(())
    }

    pub fn enforce_entry_quality_post_quote(
        &self,
        entry: &ActivityEntry,
        quote: &BestQuote,
        observation: &MarketQualityObservation,
        context: AdaptiveRiskContext,
    ) -> Result<TradeQualityScore, SkipReason> {
        let best_bid_size = quote.best_bid_size.unwrap_or(Decimal::ZERO);
        let best_ask_size = quote.best_ask_size.unwrap_or(Decimal::ZERO);
        let min_visible_liquidity = effective_min_visible_liquidity(&self.settings, context);
        if best_bid_size < min_visible_liquidity || best_ask_size < min_visible_liquidity {
            return Err(SkipReason::new(
                "low_visible_liquidity",
                format!(
                    "visible top-of-book liquidity bid={} ask={} is below minimum {}",
                    best_bid_size.round_dp(4),
                    best_ask_size.round_dp(4),
                    min_visible_liquidity.round_dp(4)
                ),
            ));
        }

        let Some(best_bid) = quote.best_bid else {
            return Err(SkipReason::new(
                "low_visible_liquidity",
                format!("missing best bid for asset {}", quote.asset_id),
            ));
        };
        let Some(best_ask) = quote.best_ask else {
            return Err(SkipReason::new(
                "low_visible_liquidity",
                format!("missing best ask for asset {}", quote.asset_id),
            ));
        };

        let Some(spread_bps) = market_spread_bps(best_bid, best_ask) else {
            return Err(SkipReason::new(
                "spread_too_wide",
                format!(
                    "could not compute spread from best bid {} and best ask {}",
                    best_bid.round_dp(4),
                    best_ask.round_dp(4)
                ),
            ));
        };
        let max_spread_bps = effective_max_spread_bps(&self.settings, context);
        if max_spread_bps > 0 && spread_bps > Decimal::from(max_spread_bps) {
            return Err(SkipReason::new(
                "spread_too_wide",
                format!(
                    "spread {} bps is above max {} bps",
                    spread_bps.round_dp(2),
                    max_spread_bps
                ),
            ));
        }

        let source_price = entry.price_decimal().map_err(|error| {
            SkipReason::new(
                "invalid_source_price",
                format!("failed to parse source price: {error}"),
            )
        })?;
        let slippage = (best_ask - source_price).abs();
        let max_entry_slippage = effective_max_entry_slippage(&self.settings, context);
        if max_entry_slippage > Decimal::ZERO && slippage > max_entry_slippage {
            return Err(SkipReason::new(
                "price_chased_strict",
                format!(
                    "entry price moved {} above source price {} with max {}",
                    slippage.round_dp(4),
                    source_price.round_dp(4),
                    max_entry_slippage.round_dp(4)
                ),
            ));
        }

        let quality = TradeQualityScore {
            liquidity_score: positive_ratio_score(
                best_bid_size.min(best_ask_size),
                min_visible_liquidity,
            ),
            spread_score: inverse_ratio_score(spread_bps, Decimal::from(max_spread_bps.max(1))),
            slippage_score: inverse_ratio_score(slippage, max_entry_slippage.max(dec!(0.0001))),
            wallet_score: wallet_quality_score(&self.settings, observation),
            latency_score: latency_quality_score(self.settings.max_copy_delay_ms, observation),
            total_score: Decimal::ZERO,
        };
        let total_score = ((quality.liquidity_score * dec!(0.25))
            + (quality.spread_score * dec!(0.20))
            + (quality.slippage_score * dec!(0.20))
            + (quality.wallet_score * dec!(0.20))
            + (quality.latency_score * dec!(0.15)))
        .round_dp(4);
        let quality = TradeQualityScore {
            total_score,
            ..quality
        };

        let min_quality = effective_min_trade_quality_score(
            &self.settings,
            observation.market_type == MarketType::UltraShort,
            context,
        );
        if observation.market_type == MarketType::UltraShort
            && !self.settings.enable_ultra_short_markets
        {
            let ultra_liquidity_min = (min_visible_liquidity * dec!(1.5)).round_dp(4);
            let ultra_slippage_max = (max_entry_slippage * dec!(0.5)).round_dp(4);
            if best_bid_size < ultra_liquidity_min
                || best_ask_size < ultra_liquidity_min
                || slippage > ultra_slippage_max
                || quality.total_score < min_quality
            {
                return Err(SkipReason::new(
                    "market_ultra_short_filtered",
                    format!(
                        "ultra-short market requires quality >= {}, liquidity >= {}, slippage <= {}; got quality {}, bid {}, ask {}, slippage {}",
                        min_quality.round_dp(4),
                        ultra_liquidity_min,
                        ultra_slippage_max,
                        quality.total_score.round_dp(4),
                        best_bid_size.round_dp(4),
                        best_ask_size.round_dp(4),
                        slippage.round_dp(4)
                    ),
                ));
            }
        }

        if quality.total_score < min_quality {
            return Err(SkipReason::new(
                "trade_quality_rejected",
                format!(
                    "trade quality {} is below minimum {} (liq={} spread={} slip={} wallet={} latency={})",
                    quality.total_score.round_dp(4),
                    min_quality.round_dp(4),
                    quality.liquidity_score.round_dp(4),
                    quality.spread_score.round_dp(4),
                    quality.slippage_score.round_dp(4),
                    quality.wallet_score.round_dp(4),
                    quality.latency_score.round_dp(4)
                ),
            ));
        }

        Ok(quality)
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

    #[cfg_attr(not(test), allow(dead_code))]
    pub fn evaluate(
        &self,
        entry: &ActivityEntry,
        portfolio: &PortfolioSnapshot,
    ) -> Result<CopyDecision, SkipReason> {
        let context = self.entry_context(portfolio);
        self.evaluate_with_context(entry, portfolio, context)
    }

    pub fn evaluate_with_context(
        &self,
        entry: &ActivityEntry,
        portfolio: &PortfolioSnapshot,
        context: AdaptiveRiskContext,
    ) -> Result<CopyDecision, SkipReason> {
        match entry.side.as_str() {
            "BUY" if self.settings.allow_buy => self.evaluate_buy(entry, portfolio, context),
            "SELL" if self.settings.allow_sell => Err(SkipReason::new(
                "sell_requires_resolved_position",
                "sell risk evaluation requires a resolver/portfolio-owned position; use evaluate_sell_from_resolved_position",
            )),
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

    fn evaluate_buy(
        &self,
        entry: &ActivityEntry,
        portfolio: &PortfolioSnapshot,
        context: AdaptiveRiskContext,
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
        let price_band = get_price_band(source_price);
        let base_scaled_notional =
            scaled_notional(source_notional, self.settings.copy_scale_above_five_usd);

        let market_exposure = portfolio.market_exposure(&entry.condition_id);
        let hard_cap = self.total_exposure_cap(portfolio);
        let remaining_total = (hard_cap - portfolio.active_total_exposure()).max(Decimal::ZERO);
        let remaining_market =
            (self.market_exposure_cap(portfolio) - market_exposure).max(Decimal::ZERO);

        self.enforce_entry_risk_limits(entry, portfolio, context)?;
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
                "too_late_strict",
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
            * mode_size_multiplier(&self.settings, context.mode)
            * wallet_performance_multiplier(portfolio, &entry.proxy_wallet))
        .min(self.settings.max_copy_notional_usd)
        .max(Decimal::ZERO);
        let allowed_notional = scaled_notional
            .min(self.per_trade_notional_cap(portfolio))
            .min(self.settings.max_position_size_abs.max(Decimal::ZERO))
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
            size_was_scaled: allowed_notional < scaled_notional,
            position_key: Some(requested_key),
            price_band,
            max_market_spread_bps: relax_bps(
                price_band_rules.max_spread_bps,
                &self.settings,
                context,
            ),
            min_top_of_book_ratio: self.settings.min_top_of_book_ratio,
            min_visible_liquidity_usd: effective_execution_min_liquidity(&self.settings, context),
            max_slippage_bps: relax_bps(price_band_rules.max_slippage_bps, &self.settings, context),
            max_source_price_slippage_bps: relax_bps(
                price_band_rules.max_slippage_bps,
                &self.settings,
                context,
            ),
            min_edge_threshold: self.settings.min_edge_threshold.max(Decimal::ZERO),
        })
    }

    pub fn evaluate_sell_from_resolved_position(
        &self,
        entry: &ActivityEntry,
        resolved_position: ResolvedPosition,
    ) -> Result<CopyDecision, SkipReason> {
        let source_notional = entry.usdc_decimal().map_err(|error| {
            SkipReason::new(
                "invalid_source_notional",
                format!("failed to parse usdc size: {error}"),
            )
        })?;
        let source_size = entry.size_decimal().map_err(|error| {
            SkipReason::new(
                "invalid_source_size",
                format!("failed to parse source size: {error}"),
            )
        })?;
        let source_price = entry.price_decimal().map_err(|error| {
            SkipReason::new(
                "invalid_source_price",
                format!("failed to parse source price: {error}"),
            )
        })?;
        self.evaluate_sell_from_resolved_position_internal(
            entry,
            resolved_position,
            source_notional,
            source_size,
            get_price_band(source_price),
        )
    }

    fn total_exposure_cap(&self, portfolio: &PortfolioSnapshot) -> Decimal {
        let configured_cap = self.settings.max_total_exposure_usd.max(Decimal::ZERO);
        let starting_capital = self.settings.start_capital_usd;
        if configured_cap.is_zero() || starting_capital <= Decimal::ZERO {
            return self.total_exposure_cap_pct(portfolio).max(configured_cap);
        }

        let realized_equity = (starting_capital + portfolio.realized_pnl).max(Decimal::ZERO);
        let exposure_ratio = configured_cap / starting_capital;
        let usd_cap = (realized_equity * exposure_ratio).max(Decimal::ZERO);
        let pct_cap = self.total_exposure_cap_pct(portfolio);
        if pct_cap.is_zero() {
            usd_cap
        } else {
            usd_cap.min(pct_cap)
        }
    }

    fn total_exposure_cap_pct(&self, portfolio: &PortfolioSnapshot) -> Decimal {
        if self.settings.max_total_exposure_pct <= Decimal::ZERO {
            return Decimal::ZERO;
        }
        (portfolio.equity() * self.settings.max_total_exposure_pct.max(Decimal::ZERO))
            .max(Decimal::ZERO)
    }

    fn market_exposure_cap(&self, portfolio: &PortfolioSnapshot) -> Decimal {
        let usd_cap = self.settings.max_market_exposure_usd.max(Decimal::ZERO);
        let pct_cap = if self.settings.max_exposure_per_market_pct > Decimal::ZERO {
            (portfolio.equity() * self.settings.max_exposure_per_market_pct.max(Decimal::ZERO))
                .max(Decimal::ZERO)
        } else {
            Decimal::ZERO
        };
        if usd_cap.is_zero() {
            pct_cap
        } else if pct_cap.is_zero() {
            usd_cap
        } else {
            usd_cap.min(pct_cap)
        }
    }

    fn per_trade_notional_cap(&self, portfolio: &PortfolioSnapshot) -> Decimal {
        let pct_cap = if self.settings.max_risk_per_trade_pct > Decimal::ZERO {
            portfolio.equity() * self.settings.max_risk_per_trade_pct.max(Decimal::ZERO)
        } else {
            Decimal::ZERO
        };
        let abs_cap = self.settings.max_position_size_abs.max(Decimal::ZERO);
        if pct_cap.is_zero() {
            abs_cap
        } else if abs_cap.is_zero() {
            pct_cap
        } else {
            pct_cap.min(abs_cap)
        }
    }

    fn enforce_entry_risk_limits(
        &self,
        entry: &ActivityEntry,
        portfolio: &PortfolioSnapshot,
        context: AdaptiveRiskContext,
    ) -> Result<(), SkipReason> {
        let now = Utc::now();
        let current_drawdown = portfolio.current_drawdown_pct.max(Decimal::ZERO);

        if context.mode == TradingMode::HardStop {
            return Err(SkipReason::new(
                "hard_stop_triggered",
                format!(
                    "hard stop active at drawdown {} with threshold {}",
                    current_drawdown.round_dp(4),
                    self.settings.hard_stop_drawdown_pct.round_dp(4)
                ),
            ));
        }

        if portfolio.loss_cooldown_active(now) {
            return Err(SkipReason::new(
                "loss_streak_guard_triggered",
                format!(
                    "loss streak cooldown remains active until {}",
                    portfolio
                        .loss_cooldown_until
                        .map(|until| until.to_rfc3339())
                        .unwrap_or_else(|| "unknown".to_owned())
                ),
            ));
        }

        if self.settings.max_total_exposure_pct > Decimal::ZERO
            && portfolio.total_exposure_pct() >= self.settings.max_total_exposure_pct
        {
            return Err(SkipReason::new(
                "exposure_limit_reached",
                format!(
                    "open exposure {} reached limit {}",
                    portfolio.total_exposure_pct().round_dp(4),
                    self.settings.max_total_exposure_pct.round_dp(4)
                ),
            ));
        }

        if self.settings.max_exposure_per_market_pct > Decimal::ZERO
            && portfolio.market_exposure_pct(&entry.condition_id)
                >= self.settings.max_exposure_per_market_pct
        {
            return Err(SkipReason::new(
                "market_exposure_limit",
                format!(
                    "market exposure {} for condition {} reached limit {}",
                    portfolio
                        .market_exposure_pct(&entry.condition_id)
                        .round_dp(4),
                    entry.condition_id,
                    self.settings.max_exposure_per_market_pct.round_dp(4)
                ),
            ));
        }

        Ok(())
    }
}

impl RiskEngine {
    fn evaluate_sell_from_resolved_position_internal(
        &self,
        entry: &ActivityEntry,
        resolved_position: ResolvedPosition,
        source_notional: Decimal,
        source_size: Decimal,
        price_band: PriceBand,
    ) -> Result<CopyDecision, SkipReason> {
        let _ = price_band;
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
                fallback_reason = %resolved_position
                    .fallback_reason
                    .as_deref()
                    .unwrap_or("same_wallet_same_condition"),
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
            size_was_scaled: false,
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
    let realized_pnl = portfolio.wallet_realized_pnl(wallet);
    let unrealized_pnl = portfolio.wallet_unrealized_pnl(wallet);
    let mut multiplier = Decimal::ONE;
    if realized_pnl < Decimal::ZERO {
        multiplier *= dec!(0.75);
    }
    if unrealized_pnl < Decimal::ZERO {
        multiplier *= dec!(0.5);
    }
    multiplier
}

fn mode_size_multiplier(settings: &Settings, mode: TradingMode) -> Decimal {
    match mode {
        TradingMode::Normal => Decimal::ONE,
        TradingMode::Drawdown => settings
            .drawdown_size_multiplier
            .max(dec!(0.05))
            .min(Decimal::ONE),
        TradingMode::HardStop => Decimal::ZERO,
    }
}

fn adaptive_relaxation_factor(settings: &Settings, context: AdaptiveRiskContext) -> Decimal {
    let configured = settings
        .drawdown_relaxation_factor
        .max(Decimal::ONE)
        .min(dec!(1.5));
    let mut factor = Decimal::ONE;
    if context.mode == TradingMode::Drawdown {
        factor = configured;
    }
    if context.no_trade_relaxation_active {
        factor = factor.max(configured);
    }
    factor
}

fn effective_min_visible_liquidity(settings: &Settings, context: AdaptiveRiskContext) -> Decimal {
    let base = settings.min_visible_liquidity.max(Decimal::ZERO);
    if base.is_zero() {
        return Decimal::ZERO;
    }

    (base / adaptive_relaxation_factor(settings, context))
        .max(base * dec!(0.5))
        .round_dp(4)
}

fn effective_execution_min_liquidity(settings: &Settings, context: AdaptiveRiskContext) -> Decimal {
    let base = settings.min_liquidity.max(Decimal::ZERO);
    if base.is_zero() {
        return Decimal::ZERO;
    }

    (base / adaptive_relaxation_factor(settings, context))
        .max(base * dec!(0.5))
        .round_dp(4)
}

fn effective_max_entry_slippage(settings: &Settings, context: AdaptiveRiskContext) -> Decimal {
    let base = settings.max_entry_slippage.max(Decimal::ZERO);
    if base.is_zero() {
        return Decimal::ZERO;
    }

    (base * adaptive_relaxation_factor(settings, context)).round_dp(4)
}

fn effective_max_spread_bps(settings: &Settings, context: AdaptiveRiskContext) -> u32 {
    relax_bps(settings.max_spread_bps, settings, context)
}

fn effective_wallet_trades_per_minute(settings: &Settings, context: AdaptiveRiskContext) -> u32 {
    relax_bps(settings.max_wallet_trades_per_min, settings, context)
}

fn effective_min_wallet_avg_hold_ms(settings: &Settings, context: AdaptiveRiskContext) -> u64 {
    let base = settings.min_wallet_avg_hold_ms;
    if base == 0 {
        return 0;
    }

    let relaxed = Decimal::from(base) / adaptive_relaxation_factor(settings, context);
    relaxed.floor().to_u64().unwrap_or(u64::MAX).max(1)
}

fn effective_min_trade_quality_score(
    settings: &Settings,
    ultra_short: bool,
    context: AdaptiveRiskContext,
) -> Decimal {
    let mut minimum = settings.min_trade_quality_score.max(Decimal::ZERO);
    if context.mode == TradingMode::Drawdown {
        minimum = (minimum + dec!(0.10)).min(dec!(0.95));
    }
    if context.no_trade_relaxation_active {
        minimum = (minimum - dec!(0.05)).max(dec!(0.50));
    }
    if ultra_short {
        minimum = minimum.max(dec!(0.85));
    }
    minimum.min(Decimal::ONE).round_dp(4)
}

fn relax_bps(base: u32, settings: &Settings, context: AdaptiveRiskContext) -> u32 {
    if base == 0 {
        return 0;
    }

    (Decimal::from(base) * adaptive_relaxation_factor(settings, context))
        .ceil()
        .to_u32()
        .unwrap_or(u32::MAX)
}

fn average_hold_ms(stats: &WalletBehaviorStats) -> Option<u64> {
    if stats.hold_samples == 0 {
        return None;
    }

    Some((stats.total_hold_ms / u128::from(stats.hold_samples)) as u64)
}

fn current_time_ms() -> Option<u64> {
    Some(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()?
            .as_millis() as u64,
    )
}

fn normalized_timestamp_ms(timestamp: i64) -> Option<i64> {
    if timestamp <= 0 {
        return None;
    }

    Some(if timestamp >= 10_000_000_000 {
        timestamp
    } else {
        timestamp.saturating_mul(1000)
    })
}

fn normalize_wallet(wallet: &str) -> String {
    wallet.trim().to_ascii_lowercase()
}

fn normalized_execution_side(entry: &ActivityEntry) -> ExecutionSide {
    if entry.side.eq_ignore_ascii_case("SELL") {
        ExecutionSide::Sell
    } else {
        ExecutionSide::Buy
    }
}

fn positive_ratio_score(actual: Decimal, minimum: Decimal) -> Decimal {
    if minimum <= Decimal::ZERO {
        return Decimal::ONE;
    }

    (actual / minimum).min(Decimal::ONE).max(Decimal::ZERO)
}

fn inverse_ratio_score(actual: Decimal, maximum: Decimal) -> Decimal {
    if maximum <= Decimal::ZERO {
        return Decimal::ONE;
    }

    (Decimal::ONE - (actual / maximum))
        .min(Decimal::ONE)
        .max(Decimal::ZERO)
}

fn wallet_quality_score(settings: &Settings, observation: &MarketQualityObservation) -> Decimal {
    let trade_rate_score = if settings.max_wallet_trades_per_min == 0 {
        Decimal::ONE
    } else {
        inverse_ratio_score(
            Decimal::from(observation.wallet_trades_per_minute),
            Decimal::from(settings.max_wallet_trades_per_min.max(1)),
        )
    };

    let hold_score = match (
        settings.min_wallet_avg_hold_ms,
        observation.wallet_avg_hold_ms,
        observation.wallet_hold_samples,
    ) {
        (0, _, _) | (_, None, _) | (_, _, 0) => Decimal::ONE,
        (minimum, Some(avg_hold_ms), _) => {
            positive_ratio_score(Decimal::from(avg_hold_ms), Decimal::from(minimum.max(1)))
        }
    };

    ((trade_rate_score + hold_score) / dec!(2))
        .min(Decimal::ONE)
        .max(Decimal::ZERO)
}

fn latency_quality_score(
    max_copy_delay_ms: u64,
    observation: &MarketQualityObservation,
) -> Decimal {
    match (max_copy_delay_ms, observation.signal_age_ms) {
        (0, _) | (_, None) => Decimal::ONE,
        (maximum, Some(delay_ms)) => {
            inverse_ratio_score(Decimal::from(delay_ms), Decimal::from(maximum.max(1)))
        }
    }
}

fn copy_delay_ms(timestamp: i64) -> Option<u64> {
    let normalized_timestamp_ms = normalized_timestamp_ms(timestamp)?;
    let now_ms = current_time_ms()? as i64;

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
    use crate::config::Settings;
    use crate::models::{PortfolioPosition, PortfolioSnapshot, RealizedTradePoint};

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
        let mut settings = Settings::default_for_tests(PathBuf::from("./data"));
        settings.max_copy_delay_ms = u64::MAX;
        settings.max_risk_per_trade_pct = dec!(1);
        settings.max_position_size_abs = dec!(1000);
        settings.max_total_exposure_pct = dec!(10);
        settings.max_exposure_per_market_pct = dec!(10);
        settings.max_drawdown_pct = dec!(1);
        settings.hard_stop_drawdown_pct = dec!(1);
        settings
    }

    fn normal_context() -> AdaptiveRiskContext {
        AdaptiveRiskContext {
            mode: TradingMode::Normal,
            no_trade_relaxation_active: false,
        }
    }

    fn drawdown_context() -> AdaptiveRiskContext {
        AdaptiveRiskContext {
            mode: TradingMode::Drawdown,
            no_trade_relaxation_active: false,
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

    fn recent_timestamp_ms() -> i64 {
        current_time_ms().expect("current time") as i64
    }

    fn sample_quote(
        best_bid: Decimal,
        best_bid_size: Decimal,
        best_ask: Decimal,
        best_ask_size: Decimal,
    ) -> BestQuote {
        BestQuote {
            asset_id: "asset-1".to_owned(),
            best_bid: Some(best_bid),
            best_bid_size: Some(best_bid_size),
            best_ask: Some(best_ask),
            best_ask_size: Some(best_ask_size),
            tick_size: dec!(0.01),
            min_order_size: dec!(1),
            neg_risk: false,
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
                closing_reason: None,
                last_close_attempt_at: None,
                close_attempts: 0,
                close_failure_reason: None,
                closing_escalation_level: 0,
                stale_reason: None,
            }],
            ..PortfolioSnapshot::default()
        }
    }

    fn flat_portfolio(equity: Decimal) -> PortfolioSnapshot {
        PortfolioSnapshot {
            fetched_at: Utc::now(),
            total_value: equity,
            total_exposure: Decimal::ZERO,
            cash_balance: equity,
            realized_pnl: Decimal::ZERO,
            unrealized_pnl: Decimal::ZERO,
            current_equity: equity,
            starting_equity: equity,
            peak_equity: equity,
            positions: Vec::new(),
            ..PortfolioSnapshot::default()
        }
    }

    #[test]
    fn sell_trade_is_copyable_when_asset_is_held() {
        let engine = RiskEngine::new(sample_settings());
        let entry = sample_entry("SELL", "asset-1", 8.0, 4.0, "0xsource");
        let resolved_position = sample_portfolio()
            .resolve_position_to_sell(&entry)
            .expect("resolved position");
        let decision = engine
            .evaluate_sell_from_resolved_position(&entry, resolved_position)
            .expect("sell should be copyable");

        assert_eq!(decision.side, ExecutionSide::Sell);
        assert_eq!(decision.token_id, "asset-1");
        assert_eq!(decision.size, dec!(8));
    }

    #[test]
    fn sell_trade_uses_safe_same_wallet_fallback_when_asset_differs() {
        let engine = RiskEngine::new(sample_settings());
        let entry = sample_entry("SELL", "asset-other", 8.0, 4.0, "0xsource");
        let resolved_position = sample_portfolio()
            .resolve_position_to_sell(&entry)
            .expect("resolved fallback position");
        let decision = engine
            .evaluate_sell_from_resolved_position(&entry, resolved_position)
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
    fn sell_trade_rejects_wallet_mismatch_even_if_market_matches() {
        let engine = RiskEngine::new(sample_settings());
        let entry = sample_entry("SELL", "asset-1", 8.0, 4.0, "0xother");
        assert!(
            sample_portfolio()
                .resolve_position_to_sell(&entry)
                .is_none()
        );
        let reason = engine
            .evaluate(&entry, &sample_portfolio())
            .expect_err("sell should not resolve across source-wallet ownership");

        assert_eq!(reason.code, "sell_requires_resolved_position");
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
            closing_reason: None,
            last_close_attempt_at: None,
            close_attempts: 0,
            close_failure_reason: None,
            closing_escalation_level: 0,
            stale_reason: None,
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
            closing_reason: None,
            last_close_attempt_at: None,
            close_attempts: 0,
            close_failure_reason: None,
            closing_escalation_level: 0,
            stale_reason: None,
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
            closing_reason: None,
            last_close_attempt_at: None,
            close_attempts: 0,
            close_failure_reason: None,
            closing_escalation_level: 0,
            stale_reason: None,
        });

        let decision = engine
            .evaluate(&entry, &portfolio)
            .expect("buy should remain eligible");

        assert_eq!(decision.notional, dec!(5));
    }

    #[test]
    fn buy_notional_is_reduced_when_wallet_realized_pnl_is_negative() {
        let mut settings = sample_settings();
        settings.copy_scale_above_five_usd = Decimal::ONE;
        let engine = RiskEngine::new(settings);
        let entry = sample_entry("BUY", "asset-2", 20.0, 10.0, "0xsource");
        let mut portfolio = sample_portfolio();
        portfolio.positions.clear();
        portfolio
            .recent_realized_trade_points
            .push(RealizedTradePoint {
                observed_at: Utc::now(),
                pnl: dec!(-3),
                source_wallet: "0xsource".to_owned(),
            });

        let decision = engine
            .evaluate(&entry, &portfolio)
            .expect("buy should remain eligible");

        assert_eq!(decision.notional, dec!(7.5));
    }

    #[test]
    fn ultra_short_market_requires_high_quality_when_disabled() {
        let engine = RiskEngine::new(sample_settings());
        let mut entry = sample_entry("BUY", "asset-2", 10.0, 5.0, "0xsource");
        entry.title = "BTC Up/Down 5m".to_owned();
        entry.timestamp = recent_timestamp_ms();

        let observation = engine.observe_source_trade(&entry);
        let reason = engine
            .enforce_entry_quality_post_quote(
                &entry,
                &sample_quote(dec!(0.49), dec!(60), dec!(0.51), dec!(60)),
                &observation,
                normal_context(),
            )
            .expect_err("ultra-short markets should require exceptional quality");

        assert_eq!(reason.code, "market_ultra_short_filtered");
    }

    #[test]
    fn ultra_short_market_can_pass_with_exceptional_conditions() {
        let mut settings = sample_settings();
        settings.min_trade_quality_score = dec!(0.50);
        let engine = RiskEngine::new(settings);
        let mut entry = sample_entry("BUY", "asset-2", 10.0, 5.0, "0xsource");
        entry.title = "BTC Up/Down 5m".to_owned();
        entry.timestamp = recent_timestamp_ms();

        let observation = engine.observe_source_trade(&entry);
        let quality = engine
            .enforce_entry_quality_post_quote(
                &entry,
                &sample_quote(dec!(0.499), dec!(500), dec!(0.501), dec!(500)),
                &observation,
                normal_context(),
            )
            .expect("exceptional ultra-short setup should pass");

        assert!(quality.total_score >= dec!(0.85));
    }

    #[test]
    fn drawdown_context_relaxes_market_thresholds_without_removing_quality_gate() {
        let mut settings = sample_settings();
        settings.enable_ultra_short_markets = true;
        settings.min_trade_quality_score = dec!(0.40);
        settings.min_visible_liquidity = dec!(50);
        settings.max_spread_bps = 500;
        settings.max_entry_slippage = dec!(0.03);
        let engine = RiskEngine::new(settings);
        let mut entry = sample_entry("BUY", "asset-2", 10.0, 5.0, "0xsource");
        entry.timestamp = recent_timestamp_ms();
        let observation = engine.observe_source_trade(&entry);
        let quote = sample_quote(dec!(0.49), dec!(40), dec!(0.52), dec!(40));

        assert_eq!(
            engine
                .enforce_entry_quality_post_quote(&entry, &quote, &observation, normal_context())
                .expect_err("normal mode should keep strict thresholds")
                .code,
            "low_visible_liquidity"
        );

        let quality = engine
            .enforce_entry_quality_post_quote(&entry, &quote, &observation, drawdown_context())
            .expect("drawdown mode should relax liquidity/spread/slippage thresholds");
        assert!(quality.total_score >= dec!(0.50));
    }

    #[test]
    fn no_trade_relaxation_activates_after_signals_without_allowed_entries() {
        let mut settings = sample_settings();
        settings.no_trade_timeout = Duration::from_millis(1_000);
        let engine = RiskEngine::new(settings);
        let mut old_entry = sample_entry("BUY", "asset-old", 10.0, 5.0, "0xsource");
        old_entry.timestamp = recent_timestamp_ms() - 2_000;
        let _ = engine.observe_source_trade(&old_entry);

        let mut fresh_entry = sample_entry("BUY", "asset-fresh", 10.0, 5.0, "0xsource");
        fresh_entry.timestamp = recent_timestamp_ms();
        let _ = engine.observe_source_trade(&fresh_entry);

        let context = engine.entry_context(&flat_portfolio(dec!(100)));
        assert!(context.no_trade_relaxation_active);

        engine.record_entry_allowed();
        let context = engine.entry_context(&flat_portfolio(dec!(100)));
        assert!(!context.no_trade_relaxation_active);
    }

    #[test]
    fn low_liquidity_is_rejected() {
        let mut settings = sample_settings();
        settings.enable_ultra_short_markets = true;
        let engine = RiskEngine::new(settings);
        let mut entry = sample_entry("BUY", "asset-2", 10.0, 5.0, "0xsource");
        entry.timestamp = recent_timestamp_ms();

        let observation = engine.observe_source_trade(&entry);
        engine
            .enforce_entry_quality_pre_quote(&entry, &observation, normal_context())
            .expect("pre-quote checks should pass");
        let reason = engine
            .enforce_entry_quality_post_quote(
                &entry,
                &sample_quote(dec!(0.49), dec!(10), dec!(0.51), dec!(12)),
                &observation,
                normal_context(),
            )
            .expect_err("low liquidity should be rejected");

        assert_eq!(reason.code, "low_visible_liquidity");
    }

    #[test]
    fn high_slippage_is_rejected() {
        let mut settings = sample_settings();
        settings.enable_ultra_short_markets = true;
        let engine = RiskEngine::new(settings);
        let mut entry = sample_entry("BUY", "asset-2", 10.0, 5.0, "0xsource");
        entry.timestamp = recent_timestamp_ms();

        let observation = engine.observe_source_trade(&entry);
        let reason = engine
            .enforce_entry_quality_post_quote(
                &entry,
                &sample_quote(dec!(0.58), dec!(80), dec!(0.60), dec!(80)),
                &observation,
                normal_context(),
            )
            .expect_err("slippage should be rejected");

        assert_eq!(reason.code, "price_chased_strict");
    }

    #[test]
    fn fast_wallet_is_rejected() {
        let mut settings = sample_settings();
        settings.enable_ultra_short_markets = true;
        settings.max_wallet_trades_per_min = 2;
        let engine = RiskEngine::new(settings);

        for offset in 0..2 {
            let mut entry = sample_entry("BUY", "asset-2", 10.0, 5.0, "0xsource");
            entry.timestamp = recent_timestamp_ms() - (offset * 1_000);
            let _ = engine.observe_source_trade(&entry);
        }

        let mut third = sample_entry("BUY", "asset-2", 10.0, 5.0, "0xsource");
        third.timestamp = recent_timestamp_ms();
        let observation = engine.observe_source_trade(&third);
        let reason = engine
            .enforce_entry_quality_pre_quote(&third, &observation, normal_context())
            .expect_err("fast wallets should be rejected");

        assert_eq!(reason.code, "wallet_too_fast");
    }

    #[test]
    fn cooldown_prevents_reentry() {
        let mut settings = sample_settings();
        settings.enable_ultra_short_markets = true;
        settings.market_cooldown = Duration::from_secs(30);
        let engine = RiskEngine::new(settings);

        let mut first = sample_entry("BUY", "asset-2", 10.0, 5.0, "0xsource");
        first.timestamp = recent_timestamp_ms();
        let first_observation = engine.observe_source_trade(&first);
        engine
            .enforce_entry_quality_pre_quote(&first, &first_observation, normal_context())
            .expect("first entry should pass");

        let mut second = sample_entry("BUY", "asset-2", 10.0, 5.0, "0xother");
        second.timestamp = recent_timestamp_ms() + 1;
        let second_observation = engine.observe_source_trade(&second);
        let reason = engine
            .enforce_entry_quality_pre_quote(&second, &second_observation, normal_context())
            .expect_err("cooldown should block immediate re-entry");

        assert_eq!(reason.code, "market_cooldown");
    }

    #[test]
    fn valid_trade_passes_quality_filters() {
        let mut settings = sample_settings();
        settings.enable_ultra_short_markets = true;
        settings.max_wallet_trades_per_min = 10;
        settings.min_wallet_avg_hold_ms = 1;
        settings.market_cooldown = Duration::ZERO;
        settings.min_trade_quality_score = dec!(0.50);
        let engine = RiskEngine::new(settings);

        let mut seed_buy = sample_entry("BUY", "asset-seed", 10.0, 5.0, "0xsource");
        seed_buy.condition_id = "condition-seed".to_owned();
        seed_buy.asset = "asset-seed".to_owned();
        seed_buy.timestamp = recent_timestamp_ms() - 30_000;
        let _ = engine.observe_source_trade(&seed_buy);

        let mut seed_sell = sample_entry("SELL", "asset-seed", 10.0, 5.2, "0xsource");
        seed_sell.condition_id = "condition-seed".to_owned();
        seed_sell.asset = "asset-seed".to_owned();
        seed_sell.timestamp = recent_timestamp_ms() - 10_000;
        let _ = engine.observe_source_trade(&seed_sell);

        let mut entry = sample_entry("BUY", "asset-2", 10.0, 5.0, "0xsource");
        entry.timestamp = recent_timestamp_ms();
        let observation = engine.observe_source_trade(&entry);
        engine
            .enforce_entry_quality_pre_quote(&entry, &observation, normal_context())
            .expect("pre-quote filters should pass");
        let quality = engine
            .enforce_entry_quality_post_quote(
                &entry,
                &sample_quote(dec!(0.49), dec!(80), dec!(0.51), dec!(90)),
                &observation,
                normal_context(),
            )
            .expect("valid trade should pass");

        assert!(quality.total_score >= dec!(0.50));
    }

    #[test]
    fn position_size_scales_down_with_equity_drop() {
        let mut settings = sample_settings();
        settings.copy_scale_above_five_usd = Decimal::ONE;
        settings.max_copy_notional_usd = dec!(100);
        settings.max_risk_per_trade_pct = dec!(0.02);
        settings.max_position_size_abs = dec!(100);
        settings.min_copy_notional_usd = dec!(1);
        let engine = RiskEngine::new(settings);
        let entry = sample_entry("BUY", "asset-size", 100.0, 50.0, "0xsource");

        let full_equity = engine
            .evaluate(&entry, &flat_portfolio(dec!(100)))
            .expect("risk-sized buy at full equity");
        let reduced_equity = engine
            .evaluate(&entry, &flat_portfolio(dec!(50)))
            .expect("risk-sized buy at reduced equity");

        assert_eq!(full_equity.notional, dec!(2.00));
        assert_eq!(reduced_equity.notional, dec!(1.00));
        assert!(reduced_equity.size_was_scaled);
    }

    #[test]
    fn exposure_cap_blocks_new_entries() {
        let mut settings = sample_settings();
        settings.max_total_exposure_pct = dec!(0.30);
        settings.max_total_exposure_usd = dec!(1000);
        let engine = RiskEngine::new(settings);
        let mut portfolio = flat_portfolio(dec!(100));
        portfolio.positions.push(PortfolioPosition {
            asset: "asset-held".to_owned(),
            condition_id: "condition-held".to_owned(),
            title: "Held".to_owned(),
            outcome: "YES".to_owned(),
            source_wallet: "0xsource".to_owned(),
            state: crate::models::PositionState::Open,
            size: dec!(31),
            current_value: dec!(31),
            average_entry_price: dec!(1),
            current_price: dec!(1),
            cost_basis: dec!(31),
            unrealized_pnl: Decimal::ZERO,
            opened_at: Some(Utc::now()),
            source_trade_timestamp_unix: 1,
            closing_started_at: None,
            closing_reason: None,
            last_close_attempt_at: None,
            close_attempts: 0,
            close_failure_reason: None,
            closing_escalation_level: 0,
            stale_reason: None,
        });
        let entry = sample_entry("BUY", "asset-new", 10.0, 5.0, "0xsource");

        let reason = engine
            .evaluate(&entry, &portfolio)
            .expect_err("total exposure should block entries");

        assert_eq!(reason.code, "exposure_limit_reached");
    }

    #[test]
    fn drawdown_mode_allows_reduced_entries_and_sells() {
        let mut settings = sample_settings();
        settings.max_drawdown_pct = dec!(0.10);
        settings.hard_stop_drawdown_pct = dec!(0.50);
        settings.drawdown_size_multiplier = dec!(0.5);
        let engine = RiskEngine::new(settings);
        let mut portfolio = sample_portfolio();
        portfolio.current_equity = dec!(89);
        portfolio.peak_equity = dec!(100);
        portfolio.current_drawdown_pct = dec!(0.11);
        portfolio.drawdown_guard_active = true;

        let mut buy = sample_entry("BUY", "asset-new", 10.0, 5.0, "0xsource");
        buy.condition_id = "condition-new".to_owned();
        let sell = sample_entry("SELL", "asset-1", 8.0, 4.0, "0xsource");

        let decision = engine
            .evaluate(&buy, &portfolio)
            .expect("drawdown mode should reduce, not block");
        assert_eq!(engine.trading_mode(&portfolio), TradingMode::Drawdown);
        assert_eq!(decision.notional, dec!(2.5));
        let resolved_position = portfolio
            .resolve_position_to_sell(&sell)
            .expect("sell resolves against owned position");
        assert!(
            engine
                .evaluate_sell_from_resolved_position(&sell, resolved_position)
                .is_ok()
        );
    }

    #[test]
    fn hard_stop_disables_new_entries() {
        let mut settings = sample_settings();
        settings.hard_stop_drawdown_pct = dec!(0.20);
        let engine = RiskEngine::new(settings);
        let mut portfolio = flat_portfolio(dec!(80));
        portfolio.peak_equity = dec!(100);
        portfolio.current_drawdown_pct = dec!(0.20);
        portfolio.hard_stop_active = true;
        let entry = sample_entry("BUY", "asset-new", 10.0, 5.0, "0xsource");

        let reason = engine
            .evaluate(&entry, &portfolio)
            .expect_err("hard stop should block entries");

        assert_eq!(reason.code, "hard_stop_triggered");
    }

    #[test]
    fn loss_streak_triggers_cooldown_gate() {
        let engine = RiskEngine::new(sample_settings());
        let mut portfolio = flat_portfolio(dec!(100));
        portfolio.loss_cooldown_until = Some(Utc::now() + chrono::TimeDelta::seconds(60));
        let entry = sample_entry("BUY", "asset-new", 10.0, 5.0, "0xsource");

        let reason = engine
            .evaluate(&entry, &portfolio)
            .expect_err("loss cooldown should block entries");

        assert_eq!(reason.code, "loss_streak_guard_triggered");
    }

    #[test]
    fn per_market_exposure_cap_blocks_entries() {
        let mut settings = sample_settings();
        settings.max_exposure_per_market_pct = dec!(0.10);
        settings.max_market_exposure_usd = dec!(1000);
        let engine = RiskEngine::new(settings);
        let mut portfolio = flat_portfolio(dec!(100));
        portfolio.positions.push(PortfolioPosition {
            asset: "asset-held".to_owned(),
            condition_id: "condition-1".to_owned(),
            title: "Held".to_owned(),
            outcome: "NO".to_owned(),
            source_wallet: "0xother".to_owned(),
            state: crate::models::PositionState::Open,
            size: dec!(11),
            current_value: dec!(11),
            average_entry_price: dec!(1),
            current_price: dec!(1),
            cost_basis: dec!(11),
            unrealized_pnl: Decimal::ZERO,
            opened_at: Some(Utc::now()),
            source_trade_timestamp_unix: 1,
            closing_started_at: None,
            closing_reason: None,
            last_close_attempt_at: None,
            close_attempts: 0,
            close_failure_reason: None,
            closing_escalation_level: 0,
            stale_reason: None,
        });
        let entry = sample_entry("BUY", "asset-new", 10.0, 5.0, "0xsource");

        let reason = engine
            .evaluate(&entry, &portfolio)
            .expect_err("market exposure should block entries");

        assert_eq!(reason.code, "market_exposure_limit");
    }
}
