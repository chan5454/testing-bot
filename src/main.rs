mod attribution_fast;
mod attribution_logger;
mod config;
mod detection;
mod execution;
mod exits;
mod health;
mod latency;
mod latency_monitor;
mod log_retention;
mod log_rotation;
mod models;
mod notifier;
mod orderbook;
mod portfolio;
mod position_registry;
mod position_resolver;
mod prediction;
mod raw_activity_logger;
mod risk;
mod rolling_jsonl;
mod runtime;
mod storage;
mod wallet;
mod wallet_registry;
mod wallet_scanner;
mod wallet_score;
mod websocket;

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Result, anyhow};
use attribution_logger::AttributionLogger;
use chrono::{DateTime, TimeDelta, TimeZone, Utc};
use detection::trade_inference::ConfirmedTradeSignal;
use execution::{
    ExecutionRequest, ExecutionSide, ExecutionStatus, ExecutionSuccess, TradeExecutor,
};
use exits::resolution::{ExitResolutionBuffer, SourceExitResolution};
use latency::LatencyLogger;
use latency_monitor::LatencyMonitor;
use models::{
    ActivityEntry, ExecutionAnalyticsState, PositionKey, SourceTradeClass, TradeCohort,
    TradeCohortStatus,
};
use notifier::{SUMMARY_REFRESH_INTERVAL, TelegramNotifier};
use orderbook::orderbook_state::OrderBookState;
use portfolio::PortfolioService;
use position_registry::PositionRegistry;
use position_resolver::PositionResolver;
use prediction::{PredictionEngine, build_predicted_trade, signal_cache_key};
use raw_activity_logger::RawActivityLogger;
use risk::{CopyDecision, RiskEngine, SkipReason, TradingMode};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::Deserialize;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::{Mutex, Notify, mpsc};
use tokio::time::{MissedTickBehavior, interval, sleep, timeout};
use tracing::{error, info, warn};
use wallet::activity_stream::{ActivityCommand, WalletActivityStream};
use wallet::wallet_filter::normalize_wallet;
use wallet::wallet_matching::MatchedTrackedTrade;
use wallet_registry::WalletRegistry;
use wallet_scanner::{WalletActivityLogger, WalletScoreLogger, spawn_wallet_scanner};
use websocket::market_stream::{MarketStreamHandle, load_active_asset_catalog};
use websocket::stream_router::StreamRouterHandle;

use crate::config::Settings;
use crate::health::{HealthState, spawn_health_server};
use crate::runtime::backpressure::RuntimeBackpressure;
use crate::runtime::control::RuntimeControl;
use crate::runtime::hot_path::{HotPathEnqueue, HotPathQueue, HotPathTaskKind};
use crate::storage::StateStore;

const MAX_EXECUTION_SIGNAL_KEYS: usize = 4096;
const PREDICTION_VALIDATION_SWEEP_INTERVAL: Duration = Duration::from_millis(25);
const MAX_EXECUTIONS_PER_MINUTE: usize = 3;
const QUOTE_RETRY_DELAY: Duration = Duration::from_millis(50);
const QUOTE_RETRY_ATTEMPTS: usize = 2;
const QUOTE_SNAPSHOT_TIMEOUT: Duration = Duration::from_millis(250);
const HIGH_CONFIDENCE_QUOTE_FALLBACK_THRESHOLD: f64 = 0.90;
const LIVE_POST_TRADE_REFRESH_DELAY: Duration = Duration::from_millis(350);
const EXIT_MANAGER_SWEEP_INTERVAL: Duration = Duration::from_millis(200);
const EMERGENCY_EXIT_STAGE_TWO_AFTER: Duration = Duration::from_secs(3);
const EMERGENCY_EXIT_STAGE_THREE_AFTER: Duration = Duration::from_secs(6);
const TIME_EXIT_MAX_HOLD_SECONDS: i64 = 15 * 60;
const TIME_EXIT_STAGNATION_BPS: Decimal = rust_decimal_macros::dec!(0.01);
const TIME_EXIT_FAVORABLE_TREND_BPS: Decimal = rust_decimal_macros::dec!(0.02);
const TIME_EXIT_MILD_LOSS_BPS: Decimal = rust_decimal_macros::dec!(0.02);
const TIME_EXIT_SMALL_PNL_ABS: Decimal = rust_decimal_macros::dec!(0.50);
const TIME_EXIT_EXTENSION_MULTIPLIER: i64 = 2;
const PREDICTION_HISTORY_SEED_LIMIT: usize = 512;
const LOG_RETENTION_WINDOW: Duration = Duration::from_secs(6 * 60 * 60);
const LOG_RETENTION_SWEEP_INTERVAL: Duration = Duration::from_secs(60 * 60);
const PERIODIC_SUMMARY_RETRY_DELAY: Duration = Duration::from_secs(60);
const LATE_PREDICTION_CONFIRMATION_RETENTION: Duration = Duration::from_secs(5);

type ClosingPositions = Arc<Mutex<HashSet<PositionKey>>>;

#[derive(Clone)]
struct ExecutionAnalyticsTracker {
    path: PathBuf,
    start_capital_usd: Decimal,
    state: Arc<Mutex<ExecutionAnalyticsState>>,
    dirty: Arc<AtomicBool>,
    flush_notify: Arc<Notify>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ManagedExitReason {
    SourceExit,
    TakeProfit,
    ProfitProtection,
    StopLoss,
    TimeExit,
    HardStop,
}

impl ManagedExitReason {
    fn as_str(self) -> &'static str {
        match self {
            Self::SourceExit => "SOURCE_EXIT",
            Self::TakeProfit => "TAKE_PROFIT",
            Self::ProfitProtection => "PROFIT_PROTECTION",
            Self::StopLoss => "STOP_LOSS",
            Self::TimeExit => "TIME_EXIT",
            Self::HardStop => "HARD_STOP",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ManagedExitEvaluation {
    Trigger(ManagedExitReason),
    SuppressTimeExit(&'static str),
    None,
}

#[derive(Clone, Debug, Default)]
struct EntryExecutionMetadata {
    conviction_score: Decimal,
    wallet_alpha_score: Decimal,
    sizing_bucket: String,
}

impl ExecutionAnalyticsTracker {
    async fn load_or_new(settings: &Settings) -> Result<Self> {
        let path = settings.data_dir.join("execution-analytics-summary.json");
        let today = Utc::now().date_naive().to_string();
        let state = match tokio::fs::read_to_string(&path).await {
            Ok(contents) => match serde_json::from_str::<ExecutionAnalyticsState>(&contents) {
                Ok(state) if state.trade_day == today => state,
                Ok(_) | Err(_) => ExecutionAnalyticsState::default(),
            },
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                ExecutionAnalyticsState::default()
            }
            Err(error) => return Err(error.into()),
        };

        let tracker = Self {
            path,
            start_capital_usd: settings.start_capital_usd,
            state: Arc::new(Mutex::new(state)),
            dirty: Arc::new(AtomicBool::new(false)),
            flush_notify: Arc::new(Notify::new()),
        };
        tracker.spawn_flusher(settings.analytics_flush_interval);
        Ok(tracker)
    }

    async fn record_skip(
        &self,
        class: SourceTradeClass,
        reason: &SkipReason,
        portfolio: Option<&models::PortfolioSnapshot>,
    ) {
        let snapshot = {
            let mut state = self.state.lock().await;
            state.summary_generated_at = Utc::now();
            state.trade_day = Utc::now().date_naive().to_string();
            state.total_source_events_seen += 1;
            match class {
                SourceTradeClass::EligibleEntry => {
                    state.eligible_entry_events += 1;
                    state.skipped_entry_events += 1;
                    *state
                        .skipped_entry_by_reason
                        .entry(reason.code.to_owned())
                        .or_insert(0) += 1;
                }
                SourceTradeClass::EligibleExit => {
                    state.eligible_exit_events += 1;
                    state.skipped_exit_events += 1;
                    *state
                        .skipped_exit_by_reason
                        .entry(reason.code.to_owned())
                        .or_insert(0) += 1;
                }
                SourceTradeClass::OrphanExit => {
                    state.orphan_exit_events += 1;
                }
                SourceTradeClass::EntryRejected => {
                    state.skipped_entry_events += 1;
                    *state
                        .skipped_entry_by_reason
                        .entry(reason.code.to_owned())
                        .or_insert(0) += 1;
                }
                SourceTradeClass::ExitRejected => {
                    state.skipped_exit_events += 1;
                    *state
                        .skipped_exit_by_reason
                        .entry(reason.code.to_owned())
                        .or_insert(0) += 1;
                }
            }
            recompute_analytics_rates(&mut state);
            if let Some(portfolio) = portfolio {
                refresh_analytics_from_portfolio(&mut state, portfolio, self.start_capital_usd);
            }
            state.clone()
        };
        self.persist_snapshot(snapshot);
    }

    async fn record_processed(
        &self,
        class: SourceTradeClass,
        entry: &ActivityEntry,
        result: &ExecutionSuccess,
        portfolio: Option<&models::PortfolioSnapshot>,
        position_key: Option<&PositionKey>,
        entry_metadata: Option<&EntryExecutionMetadata>,
    ) {
        let snapshot = {
            let mut state = self.state.lock().await;
            state.summary_generated_at = Utc::now();
            state.trade_day = Utc::now().date_naive().to_string();
            state.total_source_events_seen += 1;
            match class {
                SourceTradeClass::EligibleEntry => {
                    state.eligible_entry_events += 1;
                    state.processed_entry_events += 1;
                    state.cohorts.push(TradeCohort {
                        source_wallet: normalize_wallet(&entry.proxy_wallet),
                        asset: entry.asset.clone(),
                        condition_id: entry.condition_id.clone(),
                        outcome: entry.outcome.clone(),
                        market_type: models::classify_market(&entry.title),
                        side: entry.side.clone(),
                        source_trade_timestamp_unix: entry.timestamp,
                        source_price: decimal_from_f64(entry.price),
                        filled_price: result.filled_price,
                        entry_slippage_pct: entry_slippage_pct(
                            decimal_from_f64(entry.price),
                            result.filled_price,
                        ),
                        conviction_score: entry_metadata
                            .map(|metadata| metadata.conviction_score)
                            .unwrap_or(Decimal::ZERO),
                        wallet_alpha_score: entry_metadata
                            .map(|metadata| metadata.wallet_alpha_score)
                            .unwrap_or(Decimal::ZERO),
                        entry_notional: result.filled_notional,
                        sizing_bucket: entry_metadata
                            .map(|metadata| metadata.sizing_bucket.clone())
                            .unwrap_or_else(|| "standard".to_owned()),
                        filled_size: result.filled_size,
                        execution_mode: result.mode.as_str().to_owned(),
                        open_time: source_trade_time(entry.timestamp),
                        close_time: None,
                        close_reason: None,
                        realized_pnl: Decimal::ZERO,
                        unrealized_pnl: Decimal::ZERO,
                        status: TradeCohortStatus::Open,
                        remaining_size: result.filled_size,
                        cost_basis: result.filled_notional,
                    });
                }
                SourceTradeClass::EligibleExit => {
                    state.eligible_exit_events += 1;
                    state.processed_exit_events += 1;
                    apply_exit_to_trade_cohorts(
                        &mut state.cohorts,
                        position_key.unwrap_or(&entry.position_key()),
                        entry,
                        result,
                    );
                }
                SourceTradeClass::OrphanExit
                | SourceTradeClass::EntryRejected
                | SourceTradeClass::ExitRejected => {}
            }
            recompute_analytics_rates(&mut state);
            if let Some(portfolio) = portfolio {
                refresh_analytics_from_portfolio(&mut state, portfolio, self.start_capital_usd);
            }
            state.clone()
        };
        self.persist_snapshot(snapshot);
    }

    async fn record_exit_event(
        &self,
        event_name: &str,
        portfolio: Option<&models::PortfolioSnapshot>,
    ) {
        let snapshot = {
            let mut state = self.state.lock().await;
            state.summary_generated_at = Utc::now();
            state.trade_day = Utc::now().date_naive().to_string();
            increment_string_counter(&mut state.exit_event_counts, event_name);
            if let Some(portfolio) = portfolio {
                refresh_analytics_from_portfolio(&mut state, portfolio, self.start_capital_usd);
            }
            warn_if_exit_noise_suspicious(&state, event_name);
            state.clone()
        };
        self.persist_snapshot(snapshot);
    }

    async fn record_close_failure_reason(
        &self,
        reason: &str,
        portfolio: Option<&models::PortfolioSnapshot>,
    ) {
        let snapshot = {
            let mut state = self.state.lock().await;
            state.summary_generated_at = Utc::now();
            state.trade_day = Utc::now().date_naive().to_string();
            increment_string_counter(&mut state.close_failed_by_reason, reason);
            if let Some(portfolio) = portfolio {
                refresh_analytics_from_portfolio(&mut state, portfolio, self.start_capital_usd);
            }
            state.clone()
        };
        self.persist_snapshot(snapshot);
    }

    async fn record_risk_event(
        &self,
        event_name: impl AsRef<str>,
        portfolio: Option<&models::PortfolioSnapshot>,
    ) {
        let snapshot = {
            let mut state = self.state.lock().await;
            state.summary_generated_at = Utc::now();
            state.trade_day = Utc::now().date_naive().to_string();
            increment_string_counter(&mut state.risk_event_counts, event_name.as_ref());
            if let Some(portfolio) = portfolio {
                refresh_analytics_from_portfolio(&mut state, portfolio, self.start_capital_usd);
            }
            state.clone()
        };
        self.persist_snapshot(snapshot);
    }

    async fn sync_with_portfolio(&self, portfolio: &models::PortfolioSnapshot) {
        let snapshot = {
            let mut state = self.state.lock().await;
            state.summary_generated_at = Utc::now();
            state.trade_day = Utc::now().date_naive().to_string();
            refresh_analytics_from_portfolio(&mut state, portfolio, self.start_capital_usd);
            recompute_analytics_rates(&mut state);
            state.clone()
        };
        self.persist_snapshot(snapshot);
    }

    fn persist_snapshot(&self, snapshot: ExecutionAnalyticsState) {
        drop(snapshot);
        self.dirty.store(true, Ordering::Relaxed);
        self.flush_notify.notify_one();
    }

    fn spawn_flusher(&self, flush_interval: Duration) {
        let path = self.path.clone();
        let state = self.state.clone();
        let dirty = self.dirty.clone();
        let flush_notify = self.flush_notify.clone();
        tokio::spawn(async move {
            let mut ticker = interval(flush_interval.max(Duration::from_millis(50)));
            ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

            loop {
                tokio::select! {
                    _ = ticker.tick() => {}
                    _ = flush_notify.notified() => {}
                }

                if !dirty.swap(false, Ordering::Relaxed) {
                    continue;
                }

                let snapshot = state.lock().await.clone();
                match serde_json::to_string_pretty(&snapshot) {
                    Ok(body) => {
                        if let Err(error) = tokio::fs::write(&path, body).await {
                            warn!(?error, path = %path.display(), "failed to persist execution analytics summary");
                        }
                    }
                    Err(error) => warn!(?error, "failed to encode execution analytics summary"),
                }
            }
        });
    }
}

fn recompute_analytics_rates(state: &mut ExecutionAnalyticsState) {
    let processed_total = state.processed_entry_events + state.processed_exit_events;
    let eligible_total = state.eligible_entry_events + state.eligible_exit_events;
    state.raw_copy_rate = ratio_decimal(processed_total, state.total_source_events_seen);
    state.executable_entry_copy_rate =
        ratio_decimal(state.processed_entry_events, state.eligible_entry_events);
    state.executable_total_copy_rate = ratio_decimal(processed_total, eligible_total);
}

fn increment_string_counter(map: &mut BTreeMap<String, u64>, key: impl Into<String>) {
    *map.entry(key.into()).or_insert(0) += 1;
}

fn exit_event_count(state: &ExecutionAnalyticsState, event_name: &str) -> u64 {
    state
        .exit_event_counts
        .get(event_name)
        .copied()
        .unwrap_or_default()
}

fn warn_if_exit_noise_suspicious(state: &ExecutionAnalyticsState, last_event: &str) {
    let copied_position_reference = (state.open_positions_count as u64)
        .max(state.processed_entry_events)
        .max(1);

    if last_event == "actionable_source_exit_seen" {
        let actionable = exit_event_count(state, "actionable_source_exit_seen");
        let threshold = copied_position_reference.saturating_mul(20).max(100);
        if should_emit_exit_noise_warning(actionable, threshold) {
            warn!(
                event = "exit_noise_actionable_ratio_high",
                actionable_source_exit_seen = actionable,
                copied_position_reference,
                threshold,
                "actionable source exits exceed copied-position scale; inspect attribution normalization"
            );
        }
    }

    if last_event == "source_exit_retry_queued" {
        let queued = exit_event_count(state, "source_exit_retry_queued");
        let threshold = copied_position_reference.saturating_mul(10).max(100);
        if should_emit_exit_noise_warning(queued, threshold) {
            warn!(
                event = "exit_noise_retry_queue_high",
                source_exit_retry_queued = queued,
                copied_position_reference,
                threshold,
                "unresolved source-exit retry volume is high relative to copied positions"
            );
        }
    }

    if last_event == "source_exit_normalization_failed" {
        let failed = exit_event_count(state, "source_exit_normalization_failed");
        let raw = exit_event_count(state, "raw_source_sell_seen").max(1);
        if failed >= 1_000 && failed.saturating_mul(2) >= raw && failed.is_multiple_of(1_000) {
            warn!(
                event = "exit_noise_normalization_failures_high",
                source_exit_normalization_failed = failed,
                raw_source_sell_seen = raw,
                "UNKNOWN or incomplete source sells dominate raw exit activity"
            );
        }
    }

    if last_event == "resolver_not_found" {
        let resolver_not_found = exit_event_count(state, "resolver_not_found");
        let threshold = copied_position_reference.saturating_mul(20).max(100);
        if should_emit_exit_noise_warning(resolver_not_found, threshold) {
            warn!(
                event = "exit_noise_resolver_not_found_high",
                resolver_not_found,
                copied_position_reference,
                threshold,
                "resolver_not_found volume dwarfs copied-position count"
            );
        }
    }

    if last_event == "managed_exit_time_limit" {
        let actionable = exit_event_count(state, "actionable_source_exit_seen");
        let time_exits = exit_event_count(state, "managed_exit_time_limit");
        if copied_position_reference >= 10
            && actionable.saturating_mul(4) < copied_position_reference
            && should_emit_exit_noise_warning(time_exits, copied_position_reference.max(10))
        {
            warn!(
                event = "EXIT_PIPELINE_FAILURE",
                actionable_source_exit_seen = actionable,
                open_position_reference = copied_position_reference,
                managed_exit_time_limit = time_exits,
                "EXIT PIPELINE FAILURE: actionable source exits are far below open-position scale while time exits dominate"
            );
        }
    }
}

fn should_emit_exit_noise_warning(count: u64, threshold: u64) -> bool {
    threshold > 0 && count >= threshold && count.is_multiple_of(threshold)
}

fn refresh_analytics_from_portfolio(
    state: &mut ExecutionAnalyticsState,
    portfolio: &models::PortfolioSnapshot,
    start_capital_usd: Decimal,
) {
    let today = Utc::now().date_naive();
    let split = portfolio.window_split(today);
    state.current_window_positions = split.current_window_positions;
    state.legacy_positions = split.legacy_positions;
    state.current_window_unrealized_pnl = split.current_window_unrealized_pnl;
    state.legacy_unrealized_pnl = split.legacy_unrealized_pnl;
    state.account_level_portfolio_pnl = portfolio.total_value - start_capital_usd;
    state.current_equity = portfolio.current_equity;
    state.starting_equity = portfolio.starting_equity;
    state.peak_equity = portfolio.peak_equity;
    state.current_drawdown_pct = portfolio.current_drawdown_pct;
    state.rolling_drawdown_pct = portfolio.rolling_drawdown_pct;
    state.open_exposure_total = portfolio.open_exposure_total;
    state.open_positions_count = portfolio.open_positions_count;
    state.risk_utilization_pct = portfolio.risk_utilization_pct;
    state.consecutive_losses = portfolio.consecutive_losses;
    state.rolling_loss_count = portfolio.rolling_loss_count;
    state.loss_cooldown_active = portfolio.loss_cooldown_active(Utc::now());
    state.drawdown_guard_active = portfolio.drawdown_guard_active;
    state.hard_stop_active = portfolio.hard_stop_active;

    refresh_open_cohort_marks(&mut state.cohorts, portfolio);

    state.pnl_current_window = Decimal::ZERO;
    state.pnl_all_open_positions = Decimal::ZERO;
    state.pnl_by_source_wallet = BTreeMap::new();
    state.win_rate_by_wallet = BTreeMap::new();
    state.wallet_alpha_scores = BTreeMap::new();
    state.expectancy_by_wallet = BTreeMap::new();
    state.pnl_by_trade_day = BTreeMap::new();
    state.win_rate_by_market_type = BTreeMap::new();
    state.expectancy_by_market_type = BTreeMap::new();
    state.expectancy_by_conviction_bucket = BTreeMap::new();
    state.pnl_by_sizing_bucket = BTreeMap::new();
    state.average_size_by_conviction_bucket = BTreeMap::new();
    state.stale_position_counts = BTreeMap::new();
    state.current_window_trade_count = 0;
    state.winning_current_window_trades = 0;
    state.losing_current_window_trades = 0;
    state.close_type_counts = BTreeMap::new();
    state.close_type_share = BTreeMap::new();
    state.pnl_by_close_type = BTreeMap::new();
    state.profit_protection_exit_pnl = Decimal::ZERO;
    state.filtered_entry_pct = Decimal::ZERO;
    state.entry_slippage_p50_pct = Decimal::ZERO;
    state.entry_slippage_p90_pct = Decimal::ZERO;
    state.win_rate_by_close_type = BTreeMap::new();
    state.median_hold_ms_by_close_type = BTreeMap::new();
    state.average_winner = Decimal::ZERO;
    state.average_loser = Decimal::ZERO;
    state.payoff_ratio = Decimal::ZERO;
    state.expectancy_per_trade = Decimal::ZERO;

    let mut current_window_open_trades = 0_u64;
    let mut current_window_open_winning = 0_u64;
    let current_trade_day = today.to_string();
    let mut close_type_wins = BTreeMap::<String, u64>::new();
    let mut close_type_holds = BTreeMap::<String, Vec<u64>>::new();
    let mut closed_cohort_count = 0_u64;
    let mut wallet_counts = BTreeMap::<String, u64>::new();
    let mut wallet_wins = BTreeMap::<String, u64>::new();
    let mut market_type_counts = BTreeMap::<String, u64>::new();
    let mut market_type_wins = BTreeMap::<String, u64>::new();
    let mut slippage_values = Vec::<Decimal>::new();
    let mut wallet_alpha_inputs =
        HashMap::<String, (u64, u64, u64, u64, u64, Decimal, u128, Decimal)>::new();
    let mut wallet_expectancy_inputs = BTreeMap::<String, (Decimal, u64)>::new();
    let mut market_type_expectancy_inputs = BTreeMap::<String, (Decimal, u64)>::new();
    let mut conviction_expectancy_inputs = BTreeMap::<String, (Decimal, u64)>::new();
    let mut conviction_size_inputs = BTreeMap::<String, (Decimal, u64)>::new();
    let mut closed_total_pnl = Decimal::ZERO;
    let mut winner_total = Decimal::ZERO;
    let mut winner_count = 0_u64;
    let mut loser_total = Decimal::ZERO;
    let mut loser_count = 0_u64;

    for cohort in &state.cohorts {
        let cohort_pnl = cohort.realized_pnl + cohort.unrealized_pnl;
        if cohort.status == TradeCohortStatus::Open {
            state.pnl_all_open_positions += cohort.unrealized_pnl;
        }

        *state
            .pnl_by_source_wallet
            .entry(cohort.source_wallet.clone())
            .or_insert(Decimal::ZERO) += cohort_pnl;
        slippage_values.push(cohort.entry_slippage_pct.max(Decimal::ZERO));
        *state
            .pnl_by_trade_day
            .entry(cohort.open_time.date_naive().to_string())
            .or_insert(Decimal::ZERO) += cohort_pnl;

        if cohort.open_time.date_naive().to_string() == current_trade_day {
            state.current_window_trade_count += 1;
            state.pnl_current_window += cohort_pnl;
            if cohort.status == TradeCohortStatus::Open {
                current_window_open_trades += 1;
                if cohort.unrealized_pnl > Decimal::ZERO {
                    current_window_open_winning += 1;
                }
            }
            if cohort.realized_pnl > Decimal::ZERO
                || (cohort.status == TradeCohortStatus::Open
                    && cohort.unrealized_pnl > Decimal::ZERO)
            {
                state.winning_current_window_trades += 1;
            } else if cohort.realized_pnl < Decimal::ZERO
                || (cohort.status == TradeCohortStatus::Open
                    && cohort.unrealized_pnl < Decimal::ZERO)
            {
                state.losing_current_window_trades += 1;
            }
        }

        if cohort.status == TradeCohortStatus::Closed {
            closed_cohort_count = closed_cohort_count.saturating_add(1);
            *wallet_counts
                .entry(cohort.source_wallet.clone())
                .or_insert(0) += 1;
            let market_type_key = market_type_label(cohort.market_type).to_owned();
            *market_type_counts
                .entry(market_type_key.clone())
                .or_insert(0) += 1;
            let close_type = cohort
                .close_reason
                .clone()
                .unwrap_or_else(|| "UNKNOWN".to_owned());
            let conviction_bucket =
                conviction_bucket_label(cohort.conviction_score.max(Decimal::ZERO)).to_owned();
            let sizing_bucket = if cohort.sizing_bucket.trim().is_empty() {
                "standard".to_owned()
            } else {
                cohort.sizing_bucket.clone()
            };
            increment_string_counter(&mut state.close_type_counts, close_type.clone());
            *state
                .pnl_by_close_type
                .entry(close_type.clone())
                .or_insert(Decimal::ZERO) += cohort.realized_pnl;
            *state
                .pnl_by_sizing_bucket
                .entry(sizing_bucket)
                .or_insert(Decimal::ZERO) += cohort.realized_pnl;
            let wallet_expectancy = wallet_expectancy_inputs
                .entry(cohort.source_wallet.clone())
                .or_insert((Decimal::ZERO, 0));
            wallet_expectancy.0 += cohort.realized_pnl;
            wallet_expectancy.1 = wallet_expectancy.1.saturating_add(1);
            let market_expectancy = market_type_expectancy_inputs
                .entry(market_type_key.clone())
                .or_insert((Decimal::ZERO, 0));
            market_expectancy.0 += cohort.realized_pnl;
            market_expectancy.1 = market_expectancy.1.saturating_add(1);
            let conviction_expectancy = conviction_expectancy_inputs
                .entry(conviction_bucket.clone())
                .or_insert((Decimal::ZERO, 0));
            conviction_expectancy.0 += cohort.realized_pnl;
            conviction_expectancy.1 = conviction_expectancy.1.saturating_add(1);
            let conviction_size = conviction_size_inputs
                .entry(conviction_bucket)
                .or_insert((Decimal::ZERO, 0));
            conviction_size.0 += cohort
                .entry_notional
                .max(cohort.filled_price * cohort.filled_size);
            conviction_size.1 = conviction_size.1.saturating_add(1);
            closed_total_pnl += cohort.realized_pnl;
            if cohort.realized_pnl > Decimal::ZERO {
                winner_total += cohort.realized_pnl;
                winner_count = winner_count.saturating_add(1);
            } else if cohort.realized_pnl < Decimal::ZERO {
                loser_total += cohort.realized_pnl.abs();
                loser_count = loser_count.saturating_add(1);
            }
            if cohort.realized_pnl > Decimal::ZERO {
                *close_type_wins.entry(close_type.clone()).or_insert(0) += 1;
                *wallet_wins.entry(cohort.source_wallet.clone()).or_insert(0) += 1;
                *market_type_wins.entry(market_type_key).or_insert(0) += 1;
            }
            if let Some(close_time) = cohort.close_time {
                let hold_ms = close_time
                    .signed_duration_since(cohort.open_time)
                    .num_milliseconds()
                    .max(0) as u64;
                close_type_holds
                    .entry(close_type.clone())
                    .or_default()
                    .push(hold_ms);
                let alpha_input = wallet_alpha_inputs
                    .entry(cohort.source_wallet.clone())
                    .or_insert((0, 0, 0, 0, 0, Decimal::ZERO, 0, Decimal::ZERO));
                alpha_input.6 = alpha_input.6.saturating_add(u128::from(hold_ms));
            }
            let alpha_input = wallet_alpha_inputs
                .entry(cohort.source_wallet.clone())
                .or_insert((0, 0, 0, 0, 0, Decimal::ZERO, 0, Decimal::ZERO));
            alpha_input.0 = alpha_input.0.saturating_add(1);
            if cohort.realized_pnl > Decimal::ZERO {
                alpha_input.1 = alpha_input.1.saturating_add(1);
            }
            match close_type.as_str() {
                "SOURCE_EXIT" | "TAKE_PROFIT" | "PROFIT_PROTECTION" => {
                    alpha_input.2 = alpha_input.2.saturating_add(1);
                }
                "TIME_EXIT" => {
                    alpha_input.3 = alpha_input.3.saturating_add(1);
                }
                "STOP_LOSS" | "HARD_STOP" => {
                    alpha_input.4 = alpha_input.4.saturating_add(1);
                }
                _ => {}
            }
            alpha_input.5 += cohort.realized_pnl;
            alpha_input.7 += cohort.entry_slippage_pct.max(Decimal::ZERO);
        }
    }

    for position in portfolio
        .positions
        .iter()
        .filter(|position| position.state == models::PositionState::Stale)
    {
        increment_string_counter(
            &mut state.stale_position_counts,
            position
                .stale_reason
                .clone()
                .unwrap_or_else(|| "stale_without_seen_exit".to_owned()),
        );
    }

    state.current_window_win_rate = ratio_decimal(
        state.winning_current_window_trades,
        state.current_window_trade_count,
    );
    state.current_window_open_mtm_win_rate =
        ratio_decimal(current_window_open_winning, current_window_open_trades);
    state.filtered_entry_pct = ratio_decimal(
        state.skipped_entry_events,
        state.skipped_entry_events + state.processed_entry_events,
    );
    for (close_type, count) in &state.close_type_counts {
        state.close_type_share.insert(
            close_type.clone(),
            ratio_decimal(*count, closed_cohort_count),
        );
        state.win_rate_by_close_type.insert(
            close_type.clone(),
            ratio_decimal(
                close_type_wins.get(close_type).copied().unwrap_or_default(),
                *count,
            ),
        );
        if let Some(holds) = close_type_holds.get_mut(close_type) {
            holds.sort_unstable();
            if let Some(median) = holds.get(holds.len() / 2).copied() {
                state
                    .median_hold_ms_by_close_type
                    .insert(close_type.clone(), median);
            }
        }
    }
    for (wallet, count) in &wallet_counts {
        state.win_rate_by_wallet.insert(
            wallet.clone(),
            ratio_decimal(wallet_wins.get(wallet).copied().unwrap_or_default(), *count),
        );
    }
    for (market_type, count) in &market_type_counts {
        state.win_rate_by_market_type.insert(
            market_type.clone(),
            ratio_decimal(
                market_type_wins
                    .get(market_type)
                    .copied()
                    .unwrap_or_default(),
                *count,
            ),
        );
    }
    slippage_values.sort();
    if let Some(p50) = slippage_values.get(slippage_values.len().saturating_sub(1) / 2) {
        state.entry_slippage_p50_pct = *p50;
    }
    if let Some(p90) = slippage_values
        .get(((slippage_values.len().saturating_sub(1) as f64) * 0.9).round() as usize)
    {
        state.entry_slippage_p90_pct = *p90;
    }
    state.profit_protection_exit_pnl = state
        .pnl_by_close_type
        .get("PROFIT_PROTECTION")
        .copied()
        .unwrap_or(Decimal::ZERO);
    state.average_winner = mean_decimal(winner_total, winner_count);
    state.average_loser = if loser_count == 0 {
        Decimal::ZERO
    } else {
        -(loser_total / Decimal::from(loser_count)).round_dp(4)
    };
    state.payoff_ratio = if loser_total > Decimal::ZERO && loser_count > 0 {
        (state.average_winner / state.average_loser.abs())
            .max(Decimal::ZERO)
            .round_dp(4)
    } else {
        Decimal::ZERO
    };
    state.expectancy_per_trade = mean_decimal(closed_total_pnl, closed_cohort_count);
    for (
        wallet,
        (
            count,
            wins,
            good_exits,
            time_exits,
            stop_losses,
            pnl_total,
            hold_total_ms,
            slippage_total,
        ),
    ) in wallet_alpha_inputs
    {
        let count_decimal = Decimal::from(count.max(1));
        let win_rate = Decimal::from(wins) / count_decimal;
        let good_exit_share = Decimal::from(good_exits) / count_decimal;
        let time_exit_share = Decimal::from(time_exits) / count_decimal;
        let pnl_score = if pnl_total >= Decimal::ZERO {
            Decimal::ONE
        } else {
            (Decimal::ONE - (pnl_total.abs() / Decimal::from(10_u64)))
                .max(Decimal::ZERO)
                .min(Decimal::ONE)
        };
        let hold_score = if count == 0 {
            Decimal::ONE
        } else {
            (Decimal::from(hold_total_ms as u64) / count_decimal / Decimal::from(15_000_u64))
                .max(Decimal::ZERO)
                .min(Decimal::ONE)
        };
        let stop_loss_share = Decimal::from(stop_losses) / count_decimal;
        let slippage_score = (Decimal::ONE - ((slippage_total / count_decimal) / dec!(0.05)))
            .max(Decimal::ZERO)
            .min(Decimal::ONE);
        let score = ((win_rate * dec!(0.30))
            + (good_exit_share * dec!(0.20))
            + ((Decimal::ONE - time_exit_share) * dec!(0.10))
            + ((Decimal::ONE - stop_loss_share) * dec!(0.10))
            + (pnl_score * dec!(0.10))
            + (hold_score * dec!(0.10))
            + (slippage_score * dec!(0.10)))
        .round_dp(4);
        state.wallet_alpha_scores.insert(wallet, score);
    }
    for (wallet, (pnl_sum, count)) in wallet_expectancy_inputs {
        state
            .expectancy_by_wallet
            .insert(wallet, mean_decimal(pnl_sum, count));
    }
    for (market_type, (pnl_sum, count)) in market_type_expectancy_inputs {
        state
            .expectancy_by_market_type
            .insert(market_type, mean_decimal(pnl_sum, count));
    }
    for (bucket, (pnl_sum, count)) in conviction_expectancy_inputs {
        state
            .expectancy_by_conviction_bucket
            .insert(bucket, mean_decimal(pnl_sum, count));
    }
    for (bucket, (size_sum, count)) in conviction_size_inputs {
        state
            .average_size_by_conviction_bucket
            .insert(bucket, mean_decimal(size_sum, count));
    }
}

fn refresh_open_cohort_marks(cohorts: &mut [TradeCohort], portfolio: &models::PortfolioSnapshot) {
    let mut remaining_positions = portfolio
        .positions
        .iter()
        .filter(|position| position.is_active())
        .map(|position| {
            (
                position.position_key(),
                (position.size, position.current_price),
            )
        })
        .collect::<HashMap<_, _>>();

    for cohort in cohorts.iter_mut() {
        cohort.unrealized_pnl = Decimal::ZERO;
    }

    for cohort in cohorts
        .iter_mut()
        .filter(|cohort| cohort.status == TradeCohortStatus::Open)
    {
        let key = cohort_position_key(cohort);
        if let Some((remaining_size, current_price)) = remaining_positions.get_mut(&key) {
            if *remaining_size <= Decimal::ZERO {
                continue;
            }
            let marked_size = cohort.remaining_size.min(*remaining_size);
            if marked_size <= Decimal::ZERO {
                continue;
            }
            cohort.unrealized_pnl = (current_price.to_owned() - cohort.filled_price) * marked_size;
            *remaining_size -= marked_size;
        }
    }
}

fn apply_exit_to_trade_cohorts(
    cohorts: &mut [TradeCohort],
    position_key: &PositionKey,
    entry: &ActivityEntry,
    result: &ExecutionSuccess,
) {
    let mut remaining_size = result.filled_size;
    let close_reason = cohort_close_reason(entry);
    for cohort in cohorts.iter_mut().filter(|cohort| {
        cohort.status == TradeCohortStatus::Open && cohort_position_key(cohort) == *position_key
    }) {
        if remaining_size <= Decimal::ZERO {
            break;
        }
        let closed_size = cohort.remaining_size.min(remaining_size);
        if closed_size <= Decimal::ZERO {
            continue;
        }
        cohort.realized_pnl += (result.filled_price - cohort.filled_price) * closed_size;
        cohort.remaining_size -= closed_size;
        cohort.cost_basis = cohort.filled_price * cohort.remaining_size;
        cohort.unrealized_pnl = Decimal::ZERO;
        cohort.close_reason = Some(close_reason.clone());
        if cohort.remaining_size <= Decimal::ZERO {
            cohort.status = TradeCohortStatus::Closed;
            cohort.close_time = Some(Utc::now());
        }
        remaining_size -= closed_size;
    }
}

fn cohort_close_reason(entry: &ActivityEntry) -> String {
    match entry.type_name.as_str() {
        "TIME_EXIT" => "TIME_EXIT".to_owned(),
        "STOP_LOSS" => "STOP_LOSS".to_owned(),
        "TAKE_PROFIT" => "TAKE_PROFIT".to_owned(),
        "PROFIT_PROTECTION" => "PROFIT_PROTECTION".to_owned(),
        "HARD_STOP" => "HARD_STOP".to_owned(),
        _ if entry.side.eq_ignore_ascii_case("SELL") => "SOURCE_EXIT".to_owned(),
        _ => "UNKNOWN".to_owned(),
    }
}

fn market_type_label(market_type: models::MarketType) -> &'static str {
    match market_type {
        models::MarketType::UltraShort => "ultra_short",
        models::MarketType::Short => "short",
        models::MarketType::Medium => "medium",
    }
}

fn conviction_bucket_label(score: Decimal) -> &'static str {
    risk::conviction_bucket_label(score)
}

fn ratio_decimal(numerator: u64, denominator: u64) -> Decimal {
    if denominator == 0 {
        Decimal::ZERO
    } else {
        Decimal::from(numerator) / Decimal::from(denominator)
    }
}

fn mean_decimal(sum: Decimal, count: u64) -> Decimal {
    if count == 0 {
        Decimal::ZERO
    } else {
        (sum / Decimal::from(count)).round_dp(4)
    }
}

fn decimal_from_f64(value: f64) -> Decimal {
    Decimal::from_f64_retain(value).unwrap_or(Decimal::ZERO)
}

fn entry_slippage_pct(source_price: Decimal, filled_price: Decimal) -> Decimal {
    if source_price <= Decimal::ZERO {
        return Decimal::ZERO;
    }

    ((filled_price - source_price) / source_price)
        .abs()
        .max(Decimal::ZERO)
}

fn source_trade_time(timestamp: i64) -> DateTime<Utc> {
    if timestamp >= 10_000_000_000 {
        Utc.timestamp_millis_opt(timestamp)
            .single()
            .unwrap_or_else(Utc::now)
    } else {
        Utc.timestamp_opt(timestamp, 0)
            .single()
            .unwrap_or_else(Utc::now)
    }
}

fn cohort_position_key(cohort: &TradeCohort) -> PositionKey {
    PositionKey::new(&cohort.condition_id, &cohort.outcome, &cohort.source_wallet)
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    install_rustls_provider();
    report_dotenv_load();
    init_tracing();

    let settings = Settings::from_env()?;
    if matches!(std::env::args().nth(1).as_deref(), Some("check-wallet")) {
        let snapshot = wallet::readiness::inspect_wallet_readiness(&settings).await?;
        wallet::readiness::print_wallet_readiness_report(&snapshot);
        if snapshot.report.ready_for_live_trading {
            return Ok(());
        }
        return Err(anyhow!(
            "live wallet is not ready: {:?}",
            snapshot.report.notes
        ));
    }
    if settings.execution_mode == crate::config::ExecutionMode::Live {
        let readiness = wallet::readiness::check_wallet_readiness(&settings).await?;
        let wallet_flow = if settings.polymarket_funder_address.is_some() {
            "proxy"
        } else {
            "eoa"
        };
        let signer_address = if readiness.signer_address.is_empty() {
            "<unavailable>"
        } else {
            readiness.signer_address.as_str()
        };
        info!(
            signer_address = %signer_address,
            execution_mode = %settings.execution_mode,
            wallet_flow,
            readiness_passed = readiness.ready_for_live_trading,
            "live wallet readiness completed"
        );
        if !readiness.ready_for_live_trading {
            return Err(anyhow!("live wallet is not ready: {:?}", readiness.notes));
        }
    }
    tokio::fs::create_dir_all(&settings.data_dir).await?;
    let user_stream_enabled = settings.target_activity_ws_api_key.is_some()
        && settings.target_activity_ws_secret.is_some()
        && settings.target_activity_ws_passphrase.is_some();

    let state_store = Arc::new(
        StateStore::load_with_flush(&settings.data_dir, settings.persistence_flush_interval)
            .await?,
    );
    if settings.copy_only_new_trades {
        info!("websocket-only activity mode skips REST startup priming");
    }
    info!("websocket-only activity mode skips REST prediction warm start");
    let wallet_registry = WalletRegistry::load(&settings)?;
    let position_registry = PositionRegistry::load(&settings)?;
    let position_resolver = PositionResolver::load(&settings)?;
    let mut prediction_engine = PredictionEngine::with_registries(
        &settings,
        wallet_registry.clone(),
        position_registry.clone(),
    );
    let health = Arc::new(HealthState::new(settings.execution_mode));
    health.mark_ready(false).await;
    let runtime_control = Arc::new(RuntimeControl::new(false));
    let backpressure = RuntimeBackpressure::new(
        settings.hot_path_queue_capacity,
        settings.cold_path_queue_capacity,
    );

    let notifier = Arc::new(TelegramNotifier::new(settings.clone()));
    let attribution_logger = Arc::new(AttributionLogger::with_backpressure(
        settings.clone(),
        backpressure.clone(),
    ));
    let latency_logger = Arc::new(LatencyLogger::with_backpressure(
        settings.clone(),
        backpressure.clone(),
    ));
    let raw_activity_logger = Arc::new(RawActivityLogger::with_backpressure(
        &settings,
        backpressure.clone(),
    ));
    let wallet_activity_logger = WalletActivityLogger::new(&settings);
    let wallet_score_logger = WalletScoreLogger::new(&settings);
    let latency_monitor = Arc::new(LatencyMonitor::new(&settings));
    let analytics = Arc::new(ExecutionAnalyticsTracker::load_or_new(&settings).await?);
    let exit_resolution = Arc::new(ExitResolutionBuffer::load(&settings)?);
    attribution_logger
        .record_runtime_started(&settings, user_stream_enabled)
        .await;
    let catalog = load_active_asset_catalog(&settings).await?;
    let orderbooks = Arc::new(OrderBookState::new(&settings, catalog.clone())?);
    let executor: Arc<dyn TradeExecutor> = match settings.execution_mode {
        crate::config::ExecutionMode::Live => {
            Arc::new(execution::PolymarketExecutor::new(settings.clone()).await?)
        }
        crate::config::ExecutionMode::Paper => Arc::new(execution::PaperExecutor::new(
            settings.clone(),
            Some(orderbooks.clone()),
        )),
    };
    let risk = RiskEngine::new(settings.clone());
    match load_prediction_seed_entries(&settings).await {
        Ok(entries) if !entries.is_empty() => {
            prediction_engine.seed_from_history(&entries);
            info!(
                seeded_confirmations = entries.len(),
                "seeded prediction engine from local wallet confirmation history"
            );
        }
        Ok(_) => {}
        Err(error) => warn!(
            ?error,
            "failed to seed prediction engine from attribution history"
        ),
    }
    let portfolio = Arc::new(PortfolioService::new(
        settings.clone(),
        Some(orderbooks.clone()),
    ));
    let (hot_path_queue, hot_path_rx) = HotPathQueue::new(
        settings.hot_path_queue_capacity,
        settings.exit_priority_strict,
        backpressure.clone(),
    );
    let (hot_path_result_tx, mut hot_path_result_rx) = mpsc::unbounded_channel();
    let (released_hot_task_tx, mut released_hot_task_rx) = mpsc::unbounded_channel();
    let (signal_tx, mut signal_rx) = mpsc::unbounded_channel();
    let (matched_trade_tx, mut matched_trade_rx) = mpsc::unbounded_channel();
    let activity_stream = WalletActivityStream::spawn(
        settings.clone(),
        catalog.clone(),
        state_store.clone(),
        health.clone(),
        runtime_control.clone(),
        attribution_logger.clone(),
        raw_activity_logger.as_ref().clone(),
        matched_trade_tx,
        wallet_registry.clone(),
        position_registry.clone(),
        wallet_activity_logger.clone(),
    );
    let router = StreamRouterHandle::spawn(
        settings.clone(),
        orderbooks.clone(),
        signal_tx,
        health.clone(),
        runtime_control.clone(),
    );
    let _market_stream = MarketStreamHandle::spawn_with_catalog(
        settings.clone(),
        catalog.clone(),
        router.input(),
        health.clone(),
        runtime_control.clone(),
        attribution_logger.clone(),
    )
    .await?;
    latency_monitor.spawn_supervisor(runtime_control.clone(), orderbooks.clone(), health.clone());
    spawn_wallet_scanner(
        settings.clone(),
        wallet_registry.clone(),
        wallet_score_logger,
        health.clone(),
    );

    spawn_health_server(settings.health_port, health.clone()).await?;
    let closing_positions: ClosingPositions = Arc::new(Mutex::new(HashSet::new()));
    warm_start(&portfolio, &health, &position_registry, &position_resolver).await;
    if let Some(snapshot) = portfolio.snapshot().await {
        if let Ok(released_exits) = position_resolver.sync_from_portfolio(&snapshot) {
            for released_exit in released_exits {
                let _ = released_hot_task_tx.send(source_exit_hot_task(released_exit));
            }
        }
        analytics.sync_with_portfolio(&snapshot).await;
        if notifier.async_only() {
            let notifier = notifier.clone();
            let snapshot = snapshot.clone();
            tokio::spawn(async move {
                if let Err(error) = notifier.send_startup_summary(&snapshot).await {
                    warn!(?error, "failed to send startup daily portfolio summary");
                }
            });
        } else if let Err(error) = notifier.send_startup_summary(&snapshot).await {
            warn!(?error, "failed to send startup daily portfolio summary");
        }
    }
    tokio::spawn(spawn_portfolio_refresher(
        portfolio.clone(),
        health.clone(),
        analytics.clone(),
        position_registry.clone(),
        position_resolver.clone(),
        released_hot_task_tx.clone(),
    ));
    tokio::spawn(spawn_periodic_portfolio_summary(
        portfolio.clone(),
        notifier.clone(),
    ));
    tokio::spawn(spawn_force_exit_watcher(
        settings.clone(),
        portfolio.clone(),
        position_registry.clone(),
        position_resolver.clone(),
        exit_resolution.clone(),
        executor.clone(),
        orderbooks.clone(),
        closing_positions.clone(),
        released_hot_task_tx.clone(),
        notifier.clone(),
        health.clone(),
        analytics.clone(),
    ));
    tokio::spawn(spawn_unresolved_exit_retry_worker(
        portfolio.clone(),
        position_registry.clone(),
        position_resolver.clone(),
        exit_resolution.clone(),
        analytics.clone(),
        released_hot_task_tx.clone(),
    ));
    tokio::spawn(spawn_log_retention_maintainer(
        attribution_logger.clone(),
        latency_logger.clone(),
        raw_activity_logger.clone(),
    ));
    tokio::spawn(spawn_backpressure_sampler(
        backpressure.clone(),
        health.clone(),
        Duration::from_millis(100),
    ));
    if settings.hot_path_mode {
        tokio::spawn(run_hot_path_executor(
            hot_path_rx,
            hot_path_result_tx,
            risk.clone(),
            portfolio.clone(),
            position_registry.clone(),
            position_resolver.clone(),
            exit_resolution.clone(),
            executor.clone(),
            orderbooks.clone(),
            latency_logger.clone(),
            latency_monitor.clone(),
            notifier.clone(),
            health.clone(),
            runtime_control.clone(),
            analytics.clone(),
            closing_positions.clone(),
            released_hot_task_tx.clone(),
        ));
    }

    health.mark_ready(true).await;
    info!(mode = %settings.execution_mode, "copy bot started");
    let activity_commands = activity_stream.command_tx();
    let mut pending_signal_deduper = ExecutionSignalDeduper::default();
    let mut matched_trade_deduper = MatchedTradeDeduper::default();
    let mut pending_validations = PendingPredictionTracker::default();
    let mut inflight_predictions = HashSet::<String>::new();
    let mut deferred_direct_matches = HashMap::<String, MatchedTrackedTrade>::new();
    let mut trade_rate_limiter = TradeRateLimiter::default();
    let mut validation_ticker = interval(PREDICTION_VALIDATION_SWEEP_INTERVAL);
    validation_ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = validation_ticker.tick() => {
                handle_expired_prediction_validations(
                    &mut pending_validations,
                    executor.clone(),
                    orderbooks.clone(),
                    portfolio.clone(),
                    position_resolver.clone(),
                    closing_positions.clone(),
                    released_hot_task_tx.clone(),
                    notifier.clone(),
                    attribution_logger.clone(),
                    health.clone(),
                    analytics.clone(),
                )
                .await;
            }
            Some(first_signal) = signal_rx.recv() => {
                let mut signals = vec![first_signal];
                while let Ok(signal) = signal_rx.try_recv() {
                    signals.push(signal);
                }
                signals.sort_by_key(|signal| (matches!(signal.side, ExecutionSide::Buy), signal.confirmed_at));

                for signal in signals {
                    if !pending_signal_deduper.claim(&signal) {
                        info!(
                            asset_id = %signal.asset_id,
                            condition_id = %signal.condition_id,
                            side = signal_side_label(signal.side),
                            confirmed_at = %signal.confirmed_at,
                            "skipping duplicate confirmed market signal before prediction"
                        );
                        continue;
                    }

                    if activity_commands
                        .send(ActivityCommand::ObserveMarketSignal(signal.clone()))
                        .is_err()
                    {
                        pending_signal_deduper.release(&signal);
                        return Err(anyhow!("market validation observer channel closed"));
                    }

                    let market_snapshot = orderbooks.market_snapshot(&signal.asset_id).await;
                    let prediction = prediction_engine.predict(&signal, market_snapshot.as_ref());
                    attribution_logger
                        .record_signal_event(
                            "prediction_evaluated",
                            &signal,
                            pending_validations.len(),
                            0,
                            Some(format_prediction_detail(&prediction)),
                        )
                        .await;

                    if !prediction.tier.should_execute() {
                        attribution_logger
                            .record_signal_event(
                                "prediction_rejected",
                                &signal,
                                pending_validations.len(),
                                0,
                                Some(format_prediction_detail(&prediction)),
                            )
                            .await;
                        continue;
                    }

                    let Some(predicted_trade) = build_predicted_trade(&signal, &catalog, &prediction) else {
                        attribution_logger
                            .record_signal_event(
                                "prediction_missing_trade_shape",
                                &signal,
                                pending_validations.len(),
                                0,
                                Some("failed to synthesize executable trade from prediction".to_owned()),
                            )
                            .await;
                        continue;
                    };

                    let Some(claim_id) = trade_rate_limiter.try_claim(Instant::now()) else {
                        attribution_logger
                            .record_signal_event(
                                "prediction_rate_limited",
                                &signal,
                                pending_validations.len(),
                                0,
                                Some(format!(
                                    "max_trades_per_minute={} {}",
                                    MAX_EXECUTIONS_PER_MINUTE,
                                    format_prediction_detail(&prediction)
                                )),
                            )
                            .await;
                        continue;
                    };

                    let signal_key = signal_cache_key(&signal);
                    let prediction_detail = format_prediction_detail(&prediction);
                    let hot_task = HotPathTradeTask {
                        matched_trade: predicted_trade,
                        execution_confidence: prediction.confidence,
                        quote_policy: QuotePolicy::CacheOnly,
                        source: HotPathTradeSource::Predicted {
                            signal_key: signal_key.clone(),
                            predicted_wallet: prediction
                                .predicted_wallet
                                .clone()
                                .unwrap_or_else(|| "unknown".to_owned()),
                            confidence: prediction.confidence,
                            claim_id,
                            prediction_detail: prediction_detail.clone(),
                        },
                    };
                    inflight_predictions.insert(signal_key.clone());

                    let enqueue_result = if settings.hot_path_mode {
                        hot_path_queue.enqueue(hot_task, HotPathTaskKind::PredictedEntry)
                    } else {
                        HotPathEnqueue::ProcessInline(hot_task)
                    };

                    match enqueue_result {
                        HotPathEnqueue::Queued => {
                            attribution_logger
                                .record_signal_event(
                                    "prediction_queued_hot_path",
                                    &signal,
                                    pending_validations.len(),
                                    0,
                                    Some(prediction_detail),
                                )
                                .await;
                        }
                        HotPathEnqueue::ProcessInline(task) => {
                            let result = execute_hot_path_task(
                                task,
                                &risk,
                                &portfolio,
                                &position_registry,
                                &position_resolver,
                                exit_resolution.clone(),
                                executor.clone(),
                                orderbooks.clone(),
                                latency_logger.clone(),
                                latency_monitor.clone(),
                                notifier.clone(),
                                health.clone(),
                                runtime_control.clone(),
                                analytics.clone(),
                                closing_positions.clone(),
                                released_hot_task_tx.clone(),
                            )
                            .await;
                            handle_hot_path_result(
                                result,
                                &settings,
                                &mut pending_validations,
                                &mut inflight_predictions,
                                &mut deferred_direct_matches,
                                &mut trade_rate_limiter,
                                &hot_path_queue,
                                &risk,
                                &portfolio,
                                &position_registry,
                                &position_resolver,
                                exit_resolution.clone(),
                                executor.clone(),
                                orderbooks.clone(),
                                latency_logger.clone(),
                                latency_monitor.clone(),
                                notifier.clone(),
                                health.clone(),
                                runtime_control.clone(),
                                analytics.clone(),
                                closing_positions.clone(),
                                released_hot_task_tx.clone(),
                                attribution_logger.clone(),
                            )
                            .await;
                        }
                        HotPathEnqueue::Dropped(task) => {
                            if let HotPathTradeSource::Predicted { claim_id, signal_key, .. } = &task.source {
                                trade_rate_limiter.release(*claim_id);
                                inflight_predictions.remove(signal_key);
                            }
                            attribution_logger
                                .record_signal_event(
                                    "prediction_dropped_hot_path_congestion",
                                    &signal,
                                    pending_validations.len(),
                                    0,
                                    Some("hot path queue saturated".to_owned()),
                                )
                                .await;
                        }
                    }
                }
            }
            Some(first_trade) = matched_trade_rx.recv() => {
                let mut matched_trades = vec![first_trade];
                while let Ok(matched_trade) = matched_trade_rx.try_recv() {
                    matched_trades.push(matched_trade);
                }
                matched_trades.sort_by_key(|matched_trade| matched_trade.signal.confirmed_at);

                for matched_trade in matched_trades {
                    if !matched_trade_deduper.claim(&matched_trade) {
                        info!(
                            proxy_wallet = %matched_trade.entry.proxy_wallet,
                            transaction_hash = %matched_trade.entry.transaction_hash,
                            condition_id = %matched_trade.entry.condition_id,
                            "skipping duplicate tracked-wallet match"
                        );
                        continue;
                    }

                    prediction_engine.record_confirmed_trade(&matched_trade.entry);
                    let validation_signal = matched_trade
                        .validation_signal
                        .as_ref()
                        .unwrap_or(&matched_trade.signal);
                    let signal_key = signal_cache_key(validation_signal);
                    let confirmation_detail = format!(
                        "wallet={} tx_hash={} source={} validated={} correlation={} match_window={} tx_hash_matched={}",
                        matched_trade.entry.proxy_wallet,
                        matched_trade.entry.transaction_hash,
                        matched_trade.source,
                        matched_trade.validation_correlation_kind.is_some(),
                        matched_trade
                            .validation_correlation_kind
                            .map(|kind| kind.as_str())
                            .unwrap_or("none"),
                        matched_trade
                            .validation_match_window
                            .map(|window| window.as_str())
                            .unwrap_or("none"),
                        matched_trade.tx_hash_matched,
                    );

                    if let Some(pending_execution) = pending_validations.remove(&signal_key) {
                        if let Err(error) = latency_logger
                            .record_prediction_validation(
                                &pending_execution.signal,
                                &pending_execution.source_entry,
                                &matched_trade.entry,
                                &pending_execution.predicted_wallet,
                                pending_execution.confidence,
                                pending_execution.submitted_at_utc,
                                validation_signal.confirmed_at,
                            )
                            .await
                        {
                            warn!(?error, "failed to persist prediction validation latency event");
                        }
                        attribution_logger
                            .record_signal_event(
                                "prediction_validated",
                                validation_signal,
                                pending_validations.len(),
                                0,
                                Some(format!(
                                    "predicted_wallet={} confidence={:.3} confirmation_delay_ms={} {}",
                                    pending_execution.predicted_wallet,
                                    pending_execution.confidence,
                                    pending_execution.submitted_at.elapsed().as_millis(),
                                    confirmation_detail
                                )),
                            )
                            .await;
                        persist_confirmed_trade_seen(&state_store, &matched_trade.entry, &health)
                            .await;
                    } else if let Some(expired_execution) =
                        pending_validations.take_recent(&signal_key, Instant::now())
                    {
                        if let Err(error) = latency_logger
                            .record_prediction_validation(
                                &expired_execution.signal,
                                &expired_execution.source_entry,
                                &matched_trade.entry,
                                &expired_execution.predicted_wallet,
                                expired_execution.confidence,
                                expired_execution.submitted_at_utc,
                                validation_signal.confirmed_at,
                            )
                            .await
                        {
                            warn!(?error, "failed to persist late prediction validation latency event");
                        }
                        attribution_logger
                            .record_signal_event(
                                "prediction_confirmation_late",
                                validation_signal,
                                pending_validations.len(),
                                0,
                                Some(format!(
                                    "predicted_wallet={} confidence={:.3} post_timeout=true late_confirmation_ms={} {}",
                                    expired_execution.predicted_wallet,
                                    expired_execution.confidence,
                                    expired_execution.submitted_at.elapsed().as_millis(),
                                    confirmation_detail
                                )),
                            )
                            .await;
                        persist_confirmed_trade_seen(&state_store, &matched_trade.entry, &health)
                            .await;
                    } else if matched_trade.entry.side.eq_ignore_ascii_case("BUY")
                        && inflight_predictions.contains(&signal_key)
                    {
                        deferred_direct_matches.insert(signal_key.clone(), matched_trade.clone());
                        attribution_logger
                            .record_signal_event(
                                "wallet_confirmation_waiting_for_inflight_prediction",
                                validation_signal,
                                pending_validations.len(),
                                0,
                                Some(format!(
                                    "{} action=deferred_until_prediction_result",
                                    confirmation_detail
                                )),
                            )
                            .await;
                        persist_confirmed_trade_seen(&state_store, &matched_trade.entry, &health)
                            .await;
                    } else {
                        let hot_task = HotPathTradeTask {
                            matched_trade: matched_trade.clone(),
                            execution_confidence: 1.0,
                            quote_policy: QuotePolicy::CacheOnly,
                            source: HotPathTradeSource::DirectTrackedTrade,
                        };
                        let kind = if matched_trade.entry.side.eq_ignore_ascii_case("SELL") {
                            HotPathTaskKind::SourceExit
                        } else {
                            HotPathTaskKind::DirectEntry
                        };
                        let enqueue_result = if settings.hot_path_mode {
                            hot_path_queue.enqueue(hot_task, kind)
                        } else {
                            HotPathEnqueue::ProcessInline(hot_task)
                        };

                        attribution_logger
                            .record_signal_event(
                                "wallet_confirmation_observed",
                                validation_signal,
                                pending_validations.len(),
                                0,
                                Some(format!(
                                    "{} action=hot_path_execution",
                                    confirmation_detail
                                )),
                            )
                            .await;
                        persist_confirmed_trade_seen(&state_store, &matched_trade.entry, &health)
                            .await;

                        match enqueue_result {
                            HotPathEnqueue::Queued => {}
                            HotPathEnqueue::ProcessInline(task) | HotPathEnqueue::Dropped(task) => {
                                let result = execute_hot_path_task(
                                    task,
                                    &risk,
                                    &portfolio,
                                    &position_registry,
                                    &position_resolver,
                                    exit_resolution.clone(),
                                    executor.clone(),
                                    orderbooks.clone(),
                                    latency_logger.clone(),
                                    latency_monitor.clone(),
                                    notifier.clone(),
                                    health.clone(),
                                    runtime_control.clone(),
                                    analytics.clone(),
                                    closing_positions.clone(),
                                    released_hot_task_tx.clone(),
                                )
                                .await;
                                handle_hot_path_result(
                                    result,
                                    &settings,
                                    &mut pending_validations,
                                    &mut inflight_predictions,
                                    &mut deferred_direct_matches,
                                    &mut trade_rate_limiter,
                                    &hot_path_queue,
                                    &risk,
                                    &portfolio,
                                    &position_registry,
                                    &position_resolver,
                                    exit_resolution.clone(),
                                    executor.clone(),
                                    orderbooks.clone(),
                                    latency_logger.clone(),
                                    latency_monitor.clone(),
                                    notifier.clone(),
                                    health.clone(),
                                    runtime_control.clone(),
                                    analytics.clone(),
                                    closing_positions.clone(),
                                    released_hot_task_tx.clone(),
                                    attribution_logger.clone(),
                                )
                                .await;
                            }
                        }
                    }
                }
            }
            Some(task) = released_hot_task_rx.recv() => {
                let enqueue_result = if settings.hot_path_mode {
                    hot_path_queue.enqueue(task, HotPathTaskKind::SourceExit)
                } else {
                    HotPathEnqueue::ProcessInline(task)
                };

                match enqueue_result {
                    HotPathEnqueue::Queued => {}
                    HotPathEnqueue::ProcessInline(task) | HotPathEnqueue::Dropped(task) => {
                        let result = execute_hot_path_task(
                            task,
                            &risk,
                            &portfolio,
                            &position_registry,
                            &position_resolver,
                            exit_resolution.clone(),
                            executor.clone(),
                            orderbooks.clone(),
                            latency_logger.clone(),
                            latency_monitor.clone(),
                            notifier.clone(),
                            health.clone(),
                            runtime_control.clone(),
                            analytics.clone(),
                            closing_positions.clone(),
                            released_hot_task_tx.clone(),
                        )
                        .await;
                        handle_hot_path_result(
                            result,
                            &settings,
                            &mut pending_validations,
                            &mut inflight_predictions,
                            &mut deferred_direct_matches,
                            &mut trade_rate_limiter,
                            &hot_path_queue,
                            &risk,
                            &portfolio,
                            &position_registry,
                            &position_resolver,
                            exit_resolution.clone(),
                            executor.clone(),
                            orderbooks.clone(),
                            latency_logger.clone(),
                            latency_monitor.clone(),
                            notifier.clone(),
                            health.clone(),
                            runtime_control.clone(),
                            analytics.clone(),
                            closing_positions.clone(),
                            released_hot_task_tx.clone(),
                            attribution_logger.clone(),
                        )
                        .await;
                    }
                }
            }
            Some(result) = hot_path_result_rx.recv() => {
                handle_hot_path_result(
                    result,
                    &settings,
                    &mut pending_validations,
                    &mut inflight_predictions,
                    &mut deferred_direct_matches,
                    &mut trade_rate_limiter,
                    &hot_path_queue,
                    &risk,
                    &portfolio,
                    &position_registry,
                    &position_resolver,
                    exit_resolution.clone(),
                    executor.clone(),
                    orderbooks.clone(),
                    latency_logger.clone(),
                    latency_monitor.clone(),
                    notifier.clone(),
                    health.clone(),
                    runtime_control.clone(),
                    analytics.clone(),
                    closing_positions.clone(),
                    released_hot_task_tx.clone(),
                    attribution_logger.clone(),
                )
                .await;
            }
            else => {
                return Err(anyhow!("signal routing channels closed"));
            }
        }
    }
}

fn report_dotenv_load() {
    match dotenvy::dotenv() {
        Ok(path) => eprintln!("loaded .env from {}", path.display()),
        Err(error) => eprintln!("failed to load .env: {error}"),
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_hot_path_executor(
    mut hot_path_rx: crate::runtime::hot_path::HotPathReceiver<HotPathTradeTask>,
    hot_path_result_tx: mpsc::UnboundedSender<HotPathExecutionResult>,
    risk: RiskEngine,
    portfolio: Arc<PortfolioService>,
    position_registry: PositionRegistry,
    position_resolver: PositionResolver,
    exit_resolution: Arc<ExitResolutionBuffer>,
    executor: Arc<dyn TradeExecutor>,
    orderbooks: Arc<OrderBookState>,
    latency_logger: Arc<LatencyLogger>,
    latency_monitor: Arc<LatencyMonitor>,
    notifier: Arc<TelegramNotifier>,
    health: Arc<HealthState>,
    runtime_control: Arc<RuntimeControl>,
    analytics: Arc<ExecutionAnalyticsTracker>,
    closing_positions: ClosingPositions,
    released_hot_task_tx: mpsc::UnboundedSender<HotPathTradeTask>,
) {
    while let Some(task) = hot_path_rx.recv().await {
        let result = execute_hot_path_task(
            task,
            &risk,
            &portfolio,
            &position_registry,
            &position_resolver,
            exit_resolution.clone(),
            executor.clone(),
            orderbooks.clone(),
            latency_logger.clone(),
            latency_monitor.clone(),
            notifier.clone(),
            health.clone(),
            runtime_control.clone(),
            analytics.clone(),
            closing_positions.clone(),
            released_hot_task_tx.clone(),
        )
        .await;
        if hot_path_result_tx.send(result).is_err() {
            break;
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn execute_hot_path_task(
    task: HotPathTradeTask,
    risk: &RiskEngine,
    portfolio: &Arc<PortfolioService>,
    position_registry: &PositionRegistry,
    position_resolver: &PositionResolver,
    exit_resolution: Arc<ExitResolutionBuffer>,
    executor: Arc<dyn TradeExecutor>,
    orderbooks: Arc<OrderBookState>,
    latency_logger: Arc<LatencyLogger>,
    latency_monitor: Arc<LatencyMonitor>,
    notifier: Arc<TelegramNotifier>,
    health: Arc<HealthState>,
    runtime_control: Arc<RuntimeControl>,
    analytics: Arc<ExecutionAnalyticsTracker>,
    closing_positions: ClosingPositions,
    released_hot_task_tx: mpsc::UnboundedSender<HotPathTradeTask>,
) -> HotPathExecutionResult {
    let outcome = process_trade(
        &task,
        risk,
        portfolio,
        position_registry,
        position_resolver,
        exit_resolution,
        executor,
        orderbooks,
        latency_logger,
        latency_monitor,
        notifier,
        health,
        runtime_control,
        analytics,
        closing_positions,
        released_hot_task_tx,
    )
    .await;
    HotPathExecutionResult { task, outcome }
}

#[allow(clippy::too_many_arguments)]
async fn handle_hot_path_result(
    result: HotPathExecutionResult,
    settings: &Settings,
    pending_validations: &mut PendingPredictionTracker,
    inflight_predictions: &mut HashSet<String>,
    deferred_direct_matches: &mut HashMap<String, MatchedTrackedTrade>,
    trade_rate_limiter: &mut TradeRateLimiter,
    hot_path_queue: &HotPathQueue<HotPathTradeTask>,
    risk: &RiskEngine,
    portfolio: &Arc<PortfolioService>,
    position_registry: &PositionRegistry,
    position_resolver: &PositionResolver,
    exit_resolution: Arc<ExitResolutionBuffer>,
    executor: Arc<dyn TradeExecutor>,
    orderbooks: Arc<OrderBookState>,
    latency_logger: Arc<LatencyLogger>,
    latency_monitor: Arc<LatencyMonitor>,
    notifier: Arc<TelegramNotifier>,
    health: Arc<HealthState>,
    runtime_control: Arc<RuntimeControl>,
    analytics: Arc<ExecutionAnalyticsTracker>,
    closing_positions: ClosingPositions,
    released_hot_task_tx: mpsc::UnboundedSender<HotPathTradeTask>,
    attribution_logger: Arc<AttributionLogger>,
) {
    match &result.task.source {
        HotPathTradeSource::Predicted {
            signal_key,
            predicted_wallet,
            confidence,
            claim_id,
            prediction_detail,
        } => {
            inflight_predictions.remove(signal_key);
            match result.outcome {
                Ok(TradeProcessingOutcome::Skipped(reason)) => {
                    let _ = reason.code;
                    trade_rate_limiter.release(*claim_id);
                }
                Ok(TradeProcessingOutcome::Deferred(deferred)) => {
                    let _ = deferred.code;
                    trade_rate_limiter.release(*claim_id);
                }
                Ok(TradeProcessingOutcome::Executed(executed_trade)) => {
                    let executed_trade = *executed_trade;
                    pending_validations.insert(PendingPredictionExecution {
                        signal_key: signal_key.clone(),
                        signal: result.task.matched_trade.signal.clone(),
                        source_entry: executed_trade.source_entry,
                        decision: executed_trade.decision,
                        order_request: executed_trade.order_request,
                        execution_result: executed_trade.execution_result,
                        predicted_wallet: predicted_wallet.clone(),
                        confidence: *confidence,
                        submitted_at: executed_trade.submitted_at,
                        submitted_at_utc: executed_trade.submission_completed_at,
                        validation_deadline: executed_trade.submitted_at
                            + settings.effective_prediction_validation_timeout(),
                    });
                    attribution_logger
                        .record_signal_event(
                            "prediction_executed",
                            &result.task.matched_trade.signal,
                            pending_validations.len(),
                            0,
                            Some(prediction_detail.clone()),
                        )
                        .await;
                }
                Err(error) => {
                    trade_rate_limiter.release(*claim_id);
                    error!(?error, "failed to process predicted market signal");
                    health.set_last_error(format!("{error:#}")).await;
                }
            }

            if let Some(deferred_trade) = deferred_direct_matches.remove(signal_key) {
                let deferred_task = HotPathTradeTask {
                    matched_trade: deferred_trade.clone(),
                    execution_confidence: 1.0,
                    quote_policy: QuotePolicy::CacheOnly,
                    source: HotPathTradeSource::DirectTrackedTrade,
                };
                let enqueue_result = if settings.hot_path_mode {
                    hot_path_queue.enqueue(deferred_task, HotPathTaskKind::DirectEntry)
                } else {
                    HotPathEnqueue::ProcessInline(deferred_task)
                };
                match enqueue_result {
                    HotPathEnqueue::Queued => {}
                    HotPathEnqueue::ProcessInline(task) | HotPathEnqueue::Dropped(task) => {
                        let deferred_result = execute_hot_path_task(
                            task,
                            risk,
                            portfolio,
                            position_registry,
                            position_resolver,
                            exit_resolution.clone(),
                            executor.clone(),
                            orderbooks.clone(),
                            latency_logger.clone(),
                            latency_monitor.clone(),
                            notifier.clone(),
                            health.clone(),
                            runtime_control.clone(),
                            analytics.clone(),
                            closing_positions.clone(),
                            released_hot_task_tx.clone(),
                        )
                        .await;
                        Box::pin(handle_hot_path_result(
                            deferred_result,
                            settings,
                            pending_validations,
                            inflight_predictions,
                            deferred_direct_matches,
                            trade_rate_limiter,
                            hot_path_queue,
                            risk,
                            portfolio,
                            position_registry,
                            position_resolver,
                            exit_resolution.clone(),
                            executor.clone(),
                            orderbooks.clone(),
                            latency_logger.clone(),
                            latency_monitor.clone(),
                            notifier.clone(),
                            health,
                            runtime_control.clone(),
                            analytics.clone(),
                            closing_positions.clone(),
                            released_hot_task_tx.clone(),
                            attribution_logger.clone(),
                        ))
                        .await;
                    }
                }
            }
        }
        HotPathTradeSource::DirectTrackedTrade => match result.outcome {
            Ok(TradeProcessingOutcome::Deferred(deferred)) => {
                let _ = deferred.detail;
            }
            Ok(_) => {}
            Err(error) => {
                error!(?error, "failed to process direct tracked-wallet trade");
                health.set_last_error(format!("{error:#}")).await;
            }
        },
    }
}

async fn spawn_backpressure_sampler(
    backpressure: RuntimeBackpressure,
    health: Arc<HealthState>,
    sample_interval: Duration,
) {
    let mut ticker = interval(sample_interval.max(Duration::from_millis(50)));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        health.set_backpressure(backpressure.snapshot()).await;
    }
}

async fn warm_start(
    portfolio: &PortfolioService,
    health: &HealthState,
    position_registry: &PositionRegistry,
    position_resolver: &PositionResolver,
) {
    match portfolio.refresh_snapshot().await {
        Ok(snapshot) => {
            health.set_portfolio_value(snapshot.total_value).await;
            if let Err(error) = position_registry.sync_from_portfolio(&snapshot) {
                warn!(?error, "failed to sync position registry during warm start");
            }
            if let Err(error) = position_resolver.sync_from_portfolio(&snapshot) {
                warn!(?error, "failed to sync position resolver during warm start");
            }
        }
        Err(error) => warn!(?error, "initial portfolio fetch failed"),
    }
}

#[derive(Clone, Copy)]
enum QuotePolicy {
    CacheOnly,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum QuoteResolutionSource {
    Cache,
    Retry,
    SnapshotFetch,
    LastValidFallback,
    SyntheticFallback,
}

impl QuoteResolutionSource {
    fn as_str(self) -> &'static str {
        match self {
            Self::Cache => "cache",
            Self::Retry => "retry",
            Self::SnapshotFetch => "snapshot_fetch",
            Self::LastValidFallback => "last_valid_fallback",
            Self::SyntheticFallback => "synthetic_fallback",
        }
    }
}

struct ResolvedQuote {
    quote: models::BestQuote,
    source: QuoteResolutionSource,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ExecutionMode {
    Normal,
    MandatoryExit { reason: ManagedExitReason },
    EmergencyExit { stage: u8 },
}

#[derive(Clone, Copy)]
struct ExecutionGuardrails {
    max_market_spread_bps: Option<u32>,
    min_top_of_book_ratio: rust_decimal::Decimal,
    max_slippage_bps: u32,
    enforce_source_price_slippage: bool,
    enforce_remaining_edge: bool,
    min_visible_liquidity_usd: rust_decimal::Decimal,
    min_edge_threshold: rust_decimal::Decimal,
}

struct ExecutedTrade {
    source_entry: ActivityEntry,
    decision: CopyDecision,
    order_request: ExecutionRequest,
    execution_result: ExecutionSuccess,
    submitted_at: Instant,
    submission_completed_at: DateTime<Utc>,
}

#[derive(Clone)]
struct HotPathTradeTask {
    matched_trade: MatchedTrackedTrade,
    execution_confidence: f64,
    quote_policy: QuotePolicy,
    source: HotPathTradeSource,
}

#[derive(Clone)]
enum HotPathTradeSource {
    Predicted {
        signal_key: String,
        predicted_wallet: String,
        confidence: f64,
        claim_id: u64,
        prediction_detail: String,
    },
    DirectTrackedTrade,
}

struct HotPathExecutionResult {
    task: HotPathTradeTask,
    outcome: Result<TradeProcessingOutcome>,
}

enum TradeProcessingOutcome {
    Skipped(SkipReason),
    Deferred(DeferredTrade),
    Executed(Box<ExecutedTrade>),
}

struct DeferredTrade {
    code: &'static str,
    detail: String,
}

#[derive(Clone)]
struct PendingPredictionExecution {
    signal_key: String,
    signal: ConfirmedTradeSignal,
    source_entry: ActivityEntry,
    decision: CopyDecision,
    order_request: ExecutionRequest,
    execution_result: ExecutionSuccess,
    predicted_wallet: String,
    confidence: f64,
    submitted_at: Instant,
    submitted_at_utc: DateTime<Utc>,
    validation_deadline: Instant,
}

struct RecentlyExpiredPrediction {
    execution: PendingPredictionExecution,
    retain_until: Instant,
}

#[derive(Default)]
struct PendingPredictionTracker {
    by_signal_key: HashMap<String, PendingPredictionExecution>,
    recently_expired: HashMap<String, RecentlyExpiredPrediction>,
}

impl PendingPredictionTracker {
    fn insert(&mut self, execution: PendingPredictionExecution) {
        self.by_signal_key
            .insert(execution.signal_key.clone(), execution);
    }

    fn remove(&mut self, signal_key: &str) -> Option<PendingPredictionExecution> {
        self.by_signal_key.remove(signal_key)
    }

    fn len(&self) -> usize {
        self.by_signal_key.len()
    }

    fn take_expired(&mut self, now: Instant) -> Vec<PendingPredictionExecution> {
        self.evict_recently_expired(now);
        let expired_keys = self
            .by_signal_key
            .iter()
            .filter(|(_, execution)| execution.validation_deadline <= now)
            .map(|(signal_key, _)| signal_key.clone())
            .collect::<Vec<_>>();
        let mut expired = Vec::with_capacity(expired_keys.len());
        for signal_key in expired_keys {
            if let Some(execution) = self.by_signal_key.remove(&signal_key) {
                self.recently_expired.insert(
                    signal_key,
                    RecentlyExpiredPrediction {
                        execution: execution.clone(),
                        retain_until: now + LATE_PREDICTION_CONFIRMATION_RETENTION,
                    },
                );
                expired.push(execution);
            }
        }
        expired
    }

    fn take_recent(
        &mut self,
        signal_key: &str,
        now: Instant,
    ) -> Option<PendingPredictionExecution> {
        self.evict_recently_expired(now);
        self.recently_expired
            .remove(signal_key)
            .map(|recent| recent.execution)
    }

    fn evict_recently_expired(&mut self, now: Instant) {
        self.recently_expired
            .retain(|_, execution| execution.retain_until > now);
    }
}

#[derive(Default)]
struct TradeRateLimiter {
    next_claim_id: u64,
    recent_claims: VecDeque<(u64, Instant)>,
}

impl TradeRateLimiter {
    fn try_claim(&mut self, now: Instant) -> Option<u64> {
        self.evict_expired(now);
        if self.recent_claims.len() >= MAX_EXECUTIONS_PER_MINUTE {
            return None;
        }
        self.next_claim_id = self.next_claim_id.saturating_add(1);
        let claim_id = self.next_claim_id;
        self.recent_claims.push_back((claim_id, now));
        Some(claim_id)
    }

    fn release(&mut self, claim_id: u64) {
        if let Some(index) = self
            .recent_claims
            .iter()
            .position(|(candidate_id, _)| *candidate_id == claim_id)
        {
            self.recent_claims.remove(index);
        }
    }

    fn evict_expired(&mut self, now: Instant) {
        while self.recent_claims.front().is_some_and(|(_, claimed_at)| {
            now.duration_since(*claimed_at) >= Duration::from_secs(60)
        }) {
            self.recent_claims.pop_front();
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn process_trade(
    task: &HotPathTradeTask,
    risk: &RiskEngine,
    portfolio: &PortfolioService,
    position_registry: &PositionRegistry,
    position_resolver: &PositionResolver,
    exit_resolution: Arc<ExitResolutionBuffer>,
    executor: Arc<dyn TradeExecutor>,
    orderbooks: Arc<OrderBookState>,
    latency_logger: Arc<LatencyLogger>,
    latency_monitor: Arc<LatencyMonitor>,
    notifier: Arc<TelegramNotifier>,
    health: Arc<HealthState>,
    runtime_control: Arc<RuntimeControl>,
    analytics: Arc<ExecutionAnalyticsTracker>,
    closing_positions: ClosingPositions,
    released_hot_task_tx: mpsc::UnboundedSender<HotPathTradeTask>,
) -> Result<TradeProcessingOutcome> {
    let entry = &task.matched_trade.entry;
    let signal = &task.matched_trade.signal;
    let execution_confidence = task.execution_confidence;
    let quote_policy = task.quote_policy;
    let side = normalized_execution_side(entry);
    let requested_key = entry.position_key();
    let mut stage_timestamps = signal.stage_timestamps.clone();
    if runtime_control.is_paused() {
        let reason = SkipReason::new(
            "latency_fail_safe_paused",
            "latency fail-safe is pausing order execution during reconnect",
        );
        record_trade_skip(
            entry,
            signal,
            risk,
            health,
            latency_logger,
            analytics,
            None,
            None,
            classify_rejected_trade(side, false),
            &reason,
        )
        .await;
        return Ok(TradeProcessingOutcome::Skipped(reason));
    }

    let mut current_portfolio = match portfolio.snapshot().await {
        Some(snapshot) => snapshot,
        None => portfolio.refresh_snapshot().await?,
    };
    health
        .set_portfolio_value(current_portfolio.total_value)
        .await;
    let market_quality_observation = risk.observe_source_trade(entry);
    let risk_context = risk.entry_context(&current_portfolio);
    let mut resolved_exit_position = if matches!(side, ExecutionSide::Sell) {
        current_portfolio.resolve_position_to_sell(entry)
    } else {
        None
    };
    let mut has_copied_inventory = match side {
        ExecutionSide::Sell => {
            resolved_exit_position.is_some()
                || position_registry.has_matching_open_position(entry)
                || position_resolver.has_resolvable_position(&requested_key)
        }
        ExecutionSide::Buy => {
            current_portfolio.has_position_key(&requested_key)
                || position_resolver.has_resolvable_position(&requested_key)
        }
    };
    if matches!(side, ExecutionSide::Buy)
        && position_resolver.has_resolvable_position(&requested_key)
    {
        let reason = SkipReason::new(
            "position_exists",
            format!(
                "position already pending/open for condition {} outcome {} wallet {}",
                requested_key.condition_id, requested_key.outcome, requested_key.source_wallet
            ),
        );
        record_trade_skip(
            entry,
            signal,
            risk,
            health,
            latency_logger,
            analytics,
            Some(&current_portfolio),
            None,
            classify_rejected_trade(side, has_copied_inventory),
            &reason,
        )
        .await;
        return Ok(TradeProcessingOutcome::Skipped(reason));
    }
    if matches!(side, ExecutionSide::Sell) {
        let now = Utc::now();
        analytics
            .record_exit_event("raw_source_sell_seen", Some(&current_portfolio))
            .await;
        if !exit_resolution.claim_source_exit(entry, now).await {
            analytics
                .record_exit_event("source_exit_duplicate_suppressed", Some(&current_portfolio))
                .await;
            analytics
                .record_exit_event("exit_noise_filtered", Some(&current_portfolio))
                .await;
            info!(
                event = "source_exit_duplicate_suppressed",
                condition_id = %entry.condition_id,
                outcome = %entry.outcome,
                source_wallet = %entry.proxy_wallet,
                transaction_hash = %entry.transaction_hash,
                "suppressed duplicate source sell before exit resolution"
            );
            return Ok(TradeProcessingOutcome::Deferred(DeferredTrade {
                code: "source_exit_duplicate_suppressed",
                detail: "duplicate source sell suppressed before exit resolution".to_owned(),
            }));
        }

        let source_exit_resolution = exit_resolution
            .resolve_source_exit(
                &current_portfolio,
                position_registry,
                position_resolver,
                &task.matched_trade,
                now,
            )
            .await?;
        if let SourceExitResolution::NotActionable(non_actionable) = source_exit_resolution {
            analytics
                .record_exit_event(non_actionable.reason, Some(&current_portfolio))
                .await;
            analytics
                .record_exit_event("exit_noise_filtered", Some(&current_portfolio))
                .await;
            info!(
                event = non_actionable.reason,
                detail = %non_actionable.detail,
                condition_id = %entry.condition_id,
                outcome = %entry.outcome,
                source_wallet = %entry.proxy_wallet,
                "ignored non-actionable source sell before retry queue"
            );
            return Ok(TradeProcessingOutcome::Deferred(DeferredTrade {
                code: non_actionable.reason,
                detail: non_actionable.detail,
            }));
        }

        analytics
            .record_exit_event("source_exit_seen", Some(&current_portfolio))
            .await;
        analytics
            .record_exit_event("actionable_source_exit_seen", Some(&current_portfolio))
            .await;
        analytics
            .record_exit_event("actionable_source_exit", Some(&current_portfolio))
            .await;

        match source_exit_resolution {
            SourceExitResolution::Matched(position_key) => {
                has_copied_inventory = true;
                resolved_exit_position = resolved_exit_position
                    .or_else(|| position_resolver.resolved_position(&position_key));
                analytics
                    .record_exit_event("source_exit_matched", Some(&current_portfolio))
                    .await;
                analytics
                    .record_exit_event("exit_resolved_against_open", Some(&current_portfolio))
                    .await;
                analytics
                    .record_exit_event(
                        "source_exit_resolved_against_open",
                        Some(&current_portfolio),
                    )
                    .await;
                analytics
                    .record_exit_event("exit_resolved_success", Some(&current_portfolio))
                    .await;
                info!(
                    event = "source_exit_matched",
                    condition_id = %position_key.condition_id,
                    outcome = %position_key.outcome,
                    source_wallet = %position_key.source_wallet,
                    "resolved source exit against owned copied position"
                );
            }
            SourceExitResolution::MatchedByFallback(position_key, fallback_reason) => {
                has_copied_inventory = true;
                resolved_exit_position = resolved_exit_position.or_else(|| {
                    position_resolver
                        .resolved_position(&position_key)
                        .map(|mut resolved| {
                            resolved.used_fallback = true;
                            resolved.fallback_reason = Some(fallback_reason.clone());
                            resolved
                        })
                });
                analytics
                    .record_exit_event("source_exit_matched_fallback", Some(&current_portfolio))
                    .await;
                analytics
                    .record_exit_event("exit_resolved_against_open", Some(&current_portfolio))
                    .await;
                analytics
                    .record_exit_event(
                        "source_exit_resolved_against_open",
                        Some(&current_portfolio),
                    )
                    .await;
                analytics
                    .record_exit_event("exit_resolved_success", Some(&current_portfolio))
                    .await;
                info!(
                    event = "source_exit_matched_fallback",
                    condition_id = %position_key.condition_id,
                    outcome = %position_key.outcome,
                    source_wallet = %position_key.source_wallet,
                    fallback_reason = %fallback_reason,
                    "resolved source exit through safe same-owner fallback"
                );
            }
            SourceExitResolution::BoundToPending(bound) => {
                analytics
                    .record_exit_event("exit_resolved_against_pending", Some(&current_portfolio))
                    .await;
                analytics
                    .record_exit_event(
                        "source_exit_resolved_against_pending",
                        Some(&current_portfolio),
                    )
                    .await;
                analytics
                    .record_exit_event("exit_bound_to_pending", Some(&current_portfolio))
                    .await;
                analytics
                    .record_exit_event("retry_attempts_saved_by_binding", Some(&current_portfolio))
                    .await;
                analytics
                    .record_exit_event("exit_resolved_success", Some(&current_portfolio))
                    .await;
                info!(
                    event = "exit_bound_to_pending",
                    condition_id = %bound.position_key.condition_id,
                    outcome = %bound.position_key.outcome,
                    source_wallet = %bound.position_key.source_wallet,
                    already_bound = bound.already_bound,
                    bound_exit_count = bound.bound_exit_count,
                    "bound source exit to pending copied position"
                );
                return Ok(TradeProcessingOutcome::Deferred(DeferredTrade {
                    code: "exit_bound_to_pending",
                    detail: format!(
                        "bound source exit to pending position {}:{}:{}",
                        bound.position_key.condition_id,
                        bound.position_key.outcome,
                        bound.position_key.source_wallet
                    ),
                }));
            }
            SourceExitResolution::DeferredRetry(retry) => {
                if retry.reason.contains("not_resolved") {
                    analytics
                        .record_exit_event("resolver_not_found", Some(&current_portfolio))
                        .await;
                }
                if !retry.already_queued {
                    analytics
                        .record_exit_event("source_exit_retry_queued", Some(&current_portfolio))
                        .await;
                }
                info!(
                    event = "source_exit_retry_queued",
                    retry_key = %retry.retry_key,
                    reason = %retry.reason,
                    condition_id = %entry.condition_id,
                    outcome = %entry.outcome,
                    source_wallet = %entry.proxy_wallet,
                    "queued unresolved source exit for retry"
                );
                return Ok(TradeProcessingOutcome::Deferred(DeferredTrade {
                    code: "source_exit_retry_queued",
                    detail: format!(
                        "queued unresolved source exit retry {}: {}",
                        retry.retry_key, retry.reason
                    ),
                }));
            }
            SourceExitResolution::Failed(reason_detail) => {
                if reason_detail.contains("ambiguous") {
                    analytics
                        .record_exit_event("resolver_ambiguous", Some(&current_portfolio))
                        .await;
                } else {
                    analytics
                        .record_exit_event(
                            "source_exit_unresolved_expired",
                            Some(&current_portfolio),
                        )
                        .await;
                }
                analytics
                    .record_close_failure_reason("exit_unresolved", Some(&current_portfolio))
                    .await;
                analytics
                    .record_exit_event("exit_resolved_failed", Some(&current_portfolio))
                    .await;
                let reason = SkipReason::new("source_exit_unresolved", reason_detail);
                record_trade_skip(
                    entry,
                    signal,
                    risk,
                    health,
                    latency_logger,
                    analytics,
                    Some(&current_portfolio),
                    None,
                    classify_rejected_trade(side, has_copied_inventory),
                    &reason,
                )
                .await;
                return Ok(TradeProcessingOutcome::Skipped(reason));
            }
            SourceExitResolution::NotActionable(_) => {
                unreachable!("handled before actionable exit metrics")
            }
        }
    }

    let evaluation = match side {
        ExecutionSide::Sell => match resolved_exit_position.clone() {
            Some(resolved_position) => {
                risk.evaluate_sell_from_resolved_position(entry, resolved_position)
            }
            None => Err(SkipReason::new(
                "resolved_exit_position_missing",
                "source exit was resolved, but no local owned-position metadata was available",
            )),
        },
        ExecutionSide::Buy => risk.evaluate_with_context(entry, &current_portfolio, risk_context),
    };
    let mut decision = match evaluation {
        Ok(decision) => decision,
        Err(reason) => {
            record_trade_skip(
                entry,
                signal,
                risk,
                health,
                latency_logger,
                analytics,
                Some(&current_portfolio),
                None,
                classify_rejected_trade(side, has_copied_inventory),
                &reason,
            )
            .await;
            return Ok(TradeProcessingOutcome::Skipped(reason));
        }
    };
    if matches!(side, ExecutionSide::Buy)
        && let Err(reason) =
            risk.enforce_entry_quality_pre_quote(entry, &market_quality_observation, risk_context)
    {
        record_trade_skip(
            entry,
            signal,
            risk,
            health,
            latency_logger,
            analytics,
            Some(&current_portfolio),
            None,
            classify_rejected_trade(side, has_copied_inventory),
            &reason,
        )
        .await;
        return Ok(TradeProcessingOutcome::Skipped(reason));
    }
    stage_timestamps.fast_risk_completed_at = Some(Instant::now());
    stage_timestamps.fast_risk_completed_at_utc = Some(Utc::now());
    let eligible_class = eligible_class_for_side(side);
    let mut pending_open_key = None;
    let mut entry_metadata = None;

    if matches!(side, ExecutionSide::Buy)
        && let Some(position_key) = decision.position_key.as_ref()
    {
        if !position_resolver.register_pending_open(position_key.clone(), entry)? {
            let reason = SkipReason::new(
                "position_exists",
                format!(
                    "position already registered for condition {} outcome {} wallet {}",
                    position_key.condition_id, position_key.outcome, position_key.source_wallet
                ),
            );
            record_trade_skip(
                entry,
                signal,
                risk,
                health,
                latency_logger,
                analytics,
                Some(&current_portfolio),
                None,
                classify_rejected_trade(side, true),
                &reason,
            )
            .await;
            return Ok(TradeProcessingOutcome::Skipped(reason));
        }
        analytics
            .record_exit_event("position_pending_registered", Some(&current_portfolio))
            .await;
        pending_open_key = Some(position_key.clone());
    }

    if matches!(side, ExecutionSide::Sell) {
        let Some(position_key) = decision.position_key.as_ref() else {
            let reason = SkipReason::new(
                "missing_position_key",
                "resolved sell decision did not include a position key",
            );
            record_trade_skip(
                entry,
                signal,
                risk,
                health,
                latency_logger,
                analytics,
                Some(&current_portfolio),
                None,
                eligible_class,
                &reason,
            )
            .await;
            return Ok(TradeProcessingOutcome::Skipped(reason));
        };

        let close_retry_interval =
            TimeDelta::from_std(risk.exit_retry_interval()).unwrap_or_else(|_| TimeDelta::zero());
        if (current_portfolio.position_is_closing(position_key)
            || position_resolver.is_closing(position_key))
            && !current_portfolio.close_retry_due(position_key, Utc::now(), close_retry_interval)
        {
            let reason = SkipReason::new(
                "already_closing",
                format!(
                    "position {} {} for wallet {} is already closing",
                    position_key.condition_id, position_key.outcome, position_key.source_wallet
                ),
            );
            record_trade_skip(
                entry,
                signal,
                risk,
                health,
                latency_logger,
                analytics,
                Some(&current_portfolio),
                None,
                eligible_class,
                &reason,
            )
            .await;
            return Ok(TradeProcessingOutcome::Skipped(reason));
        }

        if !claim_closing_position(&closing_positions, position_key).await {
            let reason = SkipReason::new(
                "already_closing",
                format!(
                    "position {} {} for wallet {} is already closing",
                    position_key.condition_id, position_key.outcome, position_key.source_wallet
                ),
            );
            record_trade_skip(
                entry,
                signal,
                risk,
                health,
                latency_logger,
                analytics,
                Some(&current_portfolio),
                None,
                eligible_class,
                &reason,
            )
            .await;
            return Ok(TradeProcessingOutcome::Skipped(reason));
        }

        current_portfolio
            .mark_position_closing(position_key, ManagedExitReason::SourceExit.as_str());
        current_portfolio.note_close_attempt(position_key);
        let _ =
            position_resolver.mark_closing(position_key, ManagedExitReason::SourceExit.as_str());
        if let Err(error) = portfolio.store_snapshot(current_portfolio.clone()).await {
            warn!(
                ?error,
                "failed to persist closing state for source-follow exit"
            );
        }
    }

    let execution_mode = match side {
        ExecutionSide::Buy => ExecutionMode::Normal,
        ExecutionSide::Sell => ExecutionMode::MandatoryExit {
            reason: ManagedExitReason::SourceExit,
        },
    };

    let resolved_quote = match resolve_quote(
        &orderbooks,
        entry,
        &decision,
        quote_policy,
        execution_confidence,
    )
    .await
    {
        Ok(quote) => quote,
        Err(reason) => {
            if matches!(side, ExecutionSide::Sell) {
                analytics
                    .record_close_failure_reason(reason.code, Some(&current_portfolio))
                    .await;
                persist_failed_close_attempt_state(
                    portfolio,
                    &closing_positions,
                    &mut current_portfolio,
                    decision.position_key.as_ref(),
                    reason.code,
                )
                .await;
            } else {
                finalize_failed_pending_open(
                    position_resolver,
                    pending_open_key.as_ref(),
                    &analytics,
                    "quote_resolution_failed",
                )
                .await;
            }
            record_trade_skip(
                entry,
                signal,
                risk,
                health,
                latency_logger,
                analytics,
                Some(&current_portfolio),
                None,
                classify_rejected_trade(side, has_copied_inventory),
                &reason,
            )
            .await;
            return Ok(TradeProcessingOutcome::Skipped(reason));
        }
    };
    if resolved_quote.source != QuoteResolutionSource::Cache {
        info!(
            quote_source = resolved_quote.source.as_str(),
            confidence = execution_confidence,
            asset = %decision.token_id,
            side = %entry.side,
            "resolved execution quote through resilient fallback path"
        );
    }
    if matches!(execution_mode, ExecutionMode::Normal) && matches!(side, ExecutionSide::Buy) {
        let quality = match risk.enforce_entry_quality_post_quote(
            entry,
            &current_portfolio,
            &resolved_quote.quote,
            &market_quality_observation,
            risk_context,
        ) {
            Ok(quality) => quality,
            Err(reason) => {
                finalize_failed_pending_open(
                    position_resolver,
                    pending_open_key.as_ref(),
                    &analytics,
                    "market_quality_rejected",
                )
                .await;
                record_trade_skip(
                    entry,
                    signal,
                    risk,
                    health,
                    latency_logger,
                    analytics,
                    Some(&current_portfolio),
                    None,
                    classify_rejected_trade(side, has_copied_inventory),
                    &reason,
                )
                .await;
                return Ok(TradeProcessingOutcome::Skipped(reason));
            }
        };
        let sizing = match risk.finalize_entry_sizing(
            entry,
            &current_portfolio,
            &market_quality_observation,
            &quality,
            risk_context,
            &mut decision,
        ) {
            Ok(sizing) => sizing,
            Err(reason) => {
                finalize_failed_pending_open(
                    position_resolver,
                    pending_open_key.as_ref(),
                    &analytics,
                    reason.code,
                )
                .await;
                record_trade_skip(
                    entry,
                    signal,
                    risk,
                    health,
                    latency_logger,
                    analytics,
                    Some(&current_portfolio),
                    None,
                    classify_rejected_trade(side, has_copied_inventory),
                    &reason,
                )
                .await;
                return Ok(TradeProcessingOutcome::Skipped(reason));
            }
        };
        entry_metadata = Some(EntryExecutionMetadata {
            conviction_score: sizing.conviction_score,
            wallet_alpha_score: sizing.wallet_alpha_score,
            sizing_bucket: sizing.sizing_bucket.to_owned(),
        });
    }
    if matches!(side, ExecutionSide::Buy) && decision.size_was_scaled {
        analytics
            .record_risk_event("position_size_scaled", Some(&current_portfolio))
            .await;
    }
    if matches!(execution_mode, ExecutionMode::Normal)
        && let Err(reason) = enforce_signal_price_deviation(entry, &decision, &resolved_quote.quote)
    {
        record_trade_skip(
            entry,
            signal,
            risk,
            health,
            latency_logger,
            analytics.clone(),
            Some(&current_portfolio),
            Some(&resolved_quote.quote),
            classify_rejected_trade(side, has_copied_inventory),
            &reason,
        )
        .await;
        if matches!(side, ExecutionSide::Buy) {
            finalize_failed_pending_open(
                position_resolver,
                pending_open_key.as_ref(),
                &analytics,
                "price_deviation_rejected",
            )
            .await;
        }
        return Ok(TradeProcessingOutcome::Skipped(reason));
    }
    let request = match build_execution_request_with_mode(
        entry,
        &decision,
        &resolved_quote.quote,
        execution_mode,
    ) {
        Ok(request) => request,
        Err(reason) => {
            if matches!(side, ExecutionSide::Sell) {
                analytics
                    .record_close_failure_reason(reason.code, Some(&current_portfolio))
                    .await;
                persist_failed_close_attempt_state(
                    portfolio,
                    &closing_positions,
                    &mut current_portfolio,
                    decision.position_key.as_ref(),
                    reason.code,
                )
                .await;
            } else {
                finalize_failed_pending_open(
                    position_resolver,
                    pending_open_key.as_ref(),
                    &analytics,
                    reason.code,
                )
                .await;
            }
            record_trade_skip(
                entry,
                signal,
                risk,
                health,
                latency_logger,
                analytics,
                Some(&current_portfolio),
                Some(&resolved_quote.quote),
                classify_rejected_trade(side, has_copied_inventory),
                &reason,
            )
            .await;
            return Ok(TradeProcessingOutcome::Skipped(reason));
        }
    };

    if let Some(elapsed_total_ms) = latency_monitor.should_pause_before_submit(&stage_timestamps) {
        let reason = SkipReason::new(
            "latency_fail_safe_pre_submit",
            format!(
                "current in-flight latency {} ms exceeded fail-safe max before submission",
                elapsed_total_ms
            ),
        );
        record_trade_skip(
            entry,
            signal,
            risk,
            health,
            latency_logger,
            analytics.clone(),
            Some(&current_portfolio),
            Some(&resolved_quote.quote),
            eligible_class,
            &reason,
        )
        .await;
        if matches!(side, ExecutionSide::Sell) {
            analytics
                .record_close_failure_reason(reason.code, Some(&current_portfolio))
                .await;
            persist_failed_close_attempt_state(
                portfolio,
                &closing_positions,
                &mut current_portfolio,
                decision.position_key.as_ref(),
                reason.code,
            )
            .await;
        } else {
            finalize_failed_pending_open(
                position_resolver,
                pending_open_key.as_ref(),
                &analytics,
                reason.code,
            )
            .await;
        }
        return Ok(TradeProcessingOutcome::Skipped(reason));
    }

    if matches!(side, ExecutionSide::Buy) {
        risk.record_entry_allowed();
        analytics
            .record_risk_event(risk_context.mode.metric_name(), Some(&current_portfolio))
            .await;
        if risk_context.mode == TradingMode::Drawdown {
            analytics
                .record_risk_event("trades_allowed_in_drawdown", Some(&current_portfolio))
                .await;
        }
        if risk_context.no_trade_relaxation_active {
            analytics
                .record_risk_event("no_trade_relaxation_active", Some(&current_portfolio))
                .await;
        }
    }

    let execution_started = Instant::now();
    let result = match executor.submit_order(request.clone()).await {
        Ok(result) => result,
        Err(error) => {
            if matches!(side, ExecutionSide::Sell) {
                analytics
                    .record_close_failure_reason("close_submission_error", Some(&current_portfolio))
                    .await;
                persist_failed_close_attempt_state(
                    portfolio,
                    &closing_positions,
                    &mut current_portfolio,
                    decision.position_key.as_ref(),
                    "close_submission_error",
                )
                .await;
                let reason = SkipReason::new(
                    "close_submission_error",
                    format!("close submission failed: {error:#}"),
                );
                record_trade_skip(
                    entry,
                    signal,
                    risk,
                    health,
                    latency_logger,
                    analytics,
                    Some(&current_portfolio),
                    Some(&resolved_quote.quote),
                    eligible_class,
                    &reason,
                )
                .await;
                return Ok(TradeProcessingOutcome::Skipped(reason));
            }
            finalize_failed_pending_open(
                position_resolver,
                pending_open_key.as_ref(),
                &analytics,
                "entry_submission_error",
            )
            .await;
            return Err(error);
        }
    };
    if matches!(side, ExecutionSide::Sell) {
        analytics
            .record_exit_event("close_submitted", Some(&current_portfolio))
            .await;
    }
    info!(
        order_id = %result.order_id,
        asset = %request.token_id,
        side = %entry.side,
        execution_status = %result.status,
        requested_size = %result.requested_size,
        filled_size = %result.filled_size,
        filled_price = %result.filled_price,
        "execution result received"
    );
    if matches!(result.status, ExecutionStatus::Rejected) || !result.success {
        if matches!(side, ExecutionSide::Sell) {
            analytics
                .record_exit_event("close_rejected", Some(&current_portfolio))
                .await;
            analytics
                .record_close_failure_reason("close_rejected", Some(&current_portfolio))
                .await;
            persist_failed_close_attempt_state(
                portfolio,
                &closing_positions,
                &mut current_portfolio,
                decision.position_key.as_ref(),
                "close_rejected",
            )
            .await;
        }
        let reason = SkipReason::new(
            "order_submission_unsuccessful",
            format!(
                "executor returned success={} order_id={} execution_status={}",
                result.success, result.order_id, result.status
            ),
        );
        record_trade_skip(
            entry,
            signal,
            risk,
            health,
            latency_logger,
            analytics.clone(),
            Some(&current_portfolio),
            Some(&resolved_quote.quote),
            eligible_class,
            &reason,
        )
        .await;
        if matches!(side, ExecutionSide::Buy) {
            finalize_failed_pending_open(
                position_resolver,
                pending_open_key.as_ref(),
                &analytics,
                "close_rejected",
            )
            .await;
        }
        return Ok(TradeProcessingOutcome::Skipped(reason));
    }
    if matches!(result.status, ExecutionStatus::NoFill) {
        if matches!(side, ExecutionSide::Sell) {
            analytics
                .record_exit_event("close_nofill", Some(&current_portfolio))
                .await;
            analytics
                .record_close_failure_reason("close_nofill", Some(&current_portfolio))
                .await;
            persist_failed_close_attempt_state(
                portfolio,
                &closing_positions,
                &mut current_portfolio,
                decision.position_key.as_ref(),
                "close_nofill",
            )
            .await;
        }
        let reason = SkipReason::new(
            "no_fill_skip",
            format!(
                "executor accepted order_id={} execution_status={} but reported zero fill",
                result.order_id, result.status
            ),
        );
        record_trade_skip(
            entry,
            signal,
            risk,
            health,
            latency_logger,
            analytics.clone(),
            Some(&current_portfolio),
            Some(&resolved_quote.quote),
            eligible_class,
            &reason,
        )
        .await;
        if matches!(side, ExecutionSide::Buy) {
            finalize_failed_pending_open(
                position_resolver,
                pending_open_key.as_ref(),
                &analytics,
                "entry_nofill",
            )
            .await;
        }
        return Ok(TradeProcessingOutcome::Skipped(reason));
    }
    let close_is_partial = matches!(side, ExecutionSide::Sell)
        && (result.filled_size < request.size
            || matches!(result.status, ExecutionStatus::PartiallyFilled));
    if matches!(side, ExecutionSide::Sell) {
        analytics
            .record_exit_event(
                if close_is_partial {
                    "close_partial"
                } else {
                    "close_filled"
                },
                Some(&current_portfolio),
            )
            .await;
    }

    let submission_completed_at = Utc::now();
    let submitted_at = Instant::now();
    let submit_order_ms = execution_started.elapsed().as_millis() as u64;
    let measurement = latency_monitor.measure_submission(&stage_timestamps, submitted_at);
    health
        .record_latency(
            measurement.detection_latency_ms,
            measurement.execution_latency_ms,
            measurement.total_latency_ms,
        )
        .await;
    if let Err(error) = latency_logger
        .record_processed_trade(
            entry,
            &request,
            &result,
            &stage_timestamps,
            submission_completed_at,
            measurement,
            submit_order_ms,
        )
        .await
    {
        warn!(?error, "failed to persist latency event");
    }

    let pre_execution_size = if matches!(side, ExecutionSide::Sell) {
        resolved_exit_position
            .as_ref()
            .map(|position| position.size)
    } else {
        None
    };
    current_portfolio = project_execution_state_into_cache(
        portfolio,
        position_resolver,
        &current_portfolio,
        entry,
        &result,
        decision.position_key.as_ref(),
        &analytics,
        &released_hot_task_tx,
    )
    .await;
    if matches!(side, ExecutionSide::Buy) && decision.position_key.is_some() {
        analytics
            .record_exit_event("position_promoted_to_open", Some(&current_portfolio))
            .await;
    }

    let close_fully_closed = pre_execution_size
        .map(|size| size <= result.filled_size)
        .unwrap_or(false);
    if let Err(error) = position_registry
        .record_execution(
            entry,
            &result,
            decision.position_key.as_ref(),
            close_fully_closed,
        )
        .await
    {
        warn!(?error, "failed to update position registry after execution");
        resync_position_registry_after_persist_error(
            position_registry,
            &current_portfolio,
            "hot_path_execution",
        );
    }
    analytics
        .record_processed(
            eligible_class,
            entry,
            &result,
            Some(&current_portfolio),
            decision.position_key.as_ref(),
            entry_metadata.as_ref(),
        )
        .await;
    health.increment_processed().await;
    spawn_post_trade_side_effects(
        entry.clone(),
        decision.clone(),
        request.clone(),
        result.clone(),
        current_portfolio,
        portfolio.clone(),
        (*position_resolver).clone(),
        closing_positions,
        decision.position_key.clone(),
        entry_metadata,
        released_hot_task_tx,
        notifier,
        health,
        analytics,
    );
    Ok(TradeProcessingOutcome::Executed(Box::new(ExecutedTrade {
        source_entry: entry.clone(),
        decision: decision.clone(),
        order_request: request,
        execution_result: result,
        submitted_at,
        submission_completed_at,
    })))
}

async fn finalize_failed_pending_open(
    position_resolver: &PositionResolver,
    position_key: Option<&PositionKey>,
    analytics: &ExecutionAnalyticsTracker,
    reason: &str,
) {
    let Some(position_key) = position_key else {
        return;
    };
    match position_resolver.remove_position(position_key) {
        Ok(removed) if !removed.bound_exits.is_empty() => {
            for _ in removed.bound_exits {
                analytics
                    .record_exit_event("deferred_exit_expired", None)
                    .await;
                analytics.record_close_failure_reason(reason, None).await;
            }
        }
        Ok(_) => {}
        Err(error) => warn!(?error, "failed to clear pending-open resolver state"),
    }
}

#[allow(clippy::too_many_arguments)]
async fn project_execution_state_into_cache(
    portfolio: &PortfolioService,
    position_resolver: &PositionResolver,
    current_portfolio: &models::PortfolioSnapshot,
    entry: &ActivityEntry,
    result: &ExecutionSuccess,
    position_key: Option<&PositionKey>,
    analytics: &ExecutionAnalyticsTracker,
    released_hot_task_tx: &mpsc::UnboundedSender<HotPathTradeTask>,
) -> models::PortfolioSnapshot {
    let projected =
        match portfolio.project_fill_on_snapshot(current_portfolio, entry, result, position_key) {
            Ok(snapshot) => snapshot,
            Err(error) => {
                warn!(
                    ?error,
                    "failed to project execution result onto in-memory portfolio"
                );
                return current_portfolio.clone();
            }
        };

    let stored = match portfolio.store_snapshot(projected.clone()).await {
        Ok(snapshot) => snapshot,
        Err(error) => {
            warn!(?error, "failed to store projected execution snapshot");
            projected
        }
    };

    match position_resolver.sync_from_portfolio(&stored) {
        Ok(released_exits) => {
            for released_exit in released_exits {
                analytics
                    .record_exit_event("deferred_exit_released", Some(&stored))
                    .await;
                if released_hot_task_tx
                    .send(source_exit_hot_task(released_exit))
                    .is_err()
                {
                    warn!("failed to enqueue released deferred source exit");
                }
            }
        }
        Err(error) => warn!(
            ?error,
            "failed to sync position resolver from projected snapshot"
        ),
    }

    stored
}

async fn load_current_portfolio_snapshot(
    portfolio: &PortfolioService,
) -> Result<models::PortfolioSnapshot> {
    match portfolio.snapshot().await {
        Some(snapshot) => Ok(snapshot),
        None => portfolio.refresh_snapshot().await,
    }
}

fn source_exit_hot_task(matched_trade: MatchedTrackedTrade) -> HotPathTradeTask {
    HotPathTradeTask {
        matched_trade,
        execution_confidence: 1.0,
        quote_policy: QuotePolicy::CacheOnly,
        source: HotPathTradeSource::DirectTrackedTrade,
    }
}

fn deferred_source_exit_hot_task(entry: ActivityEntry) -> HotPathTradeTask {
    source_exit_hot_task(MatchedTrackedTrade {
        entry: entry.clone(),
        signal: synthetic_confirmed_signal(&entry),
        source: "deferred_exit",
        validation_correlation_kind: None,
        validation_match_window: None,
        tx_hash_matched: true,
        validation_signal: None,
    })
}

fn synthetic_confirmed_signal(entry: &ActivityEntry) -> ConfirmedTradeSignal {
    let now = Utc::now();
    let instant = Instant::now();
    ConfirmedTradeSignal {
        asset_id: entry.asset.clone(),
        condition_id: entry.condition_id.clone(),
        transaction_hash: Some(entry.transaction_hash.clone()),
        side: normalized_execution_side(entry),
        price: entry.price,
        estimated_size: entry.size,
        stage_timestamps: models::TradeStageTimestamps {
            websocket_event_received_at: instant,
            websocket_event_received_at_utc: now,
            parse_completed_at: instant,
            parse_completed_at_utc: now,
            detection_triggered_at: instant,
            detection_triggered_at_utc: now,
            attribution_completed_at: None,
            attribution_completed_at_utc: None,
            fast_risk_completed_at: None,
            fast_risk_completed_at_utc: None,
        },
        confirmed_at: now,
        generation: 0,
    }
}

async fn persist_failed_close_attempt_state(
    portfolio: &PortfolioService,
    closing_positions: &ClosingPositions,
    current_portfolio: &mut models::PortfolioSnapshot,
    position_key: Option<&PositionKey>,
    failure_reason: &str,
) {
    let Some(position_key) = position_key else {
        return;
    };

    current_portfolio.note_close_failure(position_key, failure_reason);
    release_closing_position(closing_positions, Some(position_key)).await;

    if let Err(error) = portfolio.store_snapshot(current_portfolio.clone()).await {
        warn!(?error, "failed to persist failed closing state");
    }
}

fn resync_position_registry_after_persist_error(
    position_registry: &PositionRegistry,
    snapshot: &models::PortfolioSnapshot,
    context: &'static str,
) {
    if let Err(sync_error) = position_registry.sync_from_portfolio(snapshot) {
        warn!(
            ?sync_error,
            context, "position registry reconciliation failed after write error"
        );
    } else {
        warn!(
            context,
            "position registry reconciled from portfolio after write error"
        );
    }
}

async fn claim_closing_position(
    closing_positions: &ClosingPositions,
    position_key: &PositionKey,
) -> bool {
    let mut closing_positions = closing_positions.lock().await;
    closing_positions.insert(position_key.clone())
}

async fn release_closing_position(
    closing_positions: &ClosingPositions,
    position_key: Option<&PositionKey>,
) {
    let Some(position_key) = position_key else {
        return;
    };
    let mut closing_positions = closing_positions.lock().await;
    closing_positions.remove(position_key);
}

#[allow(clippy::too_many_arguments)]
fn spawn_post_trade_side_effects(
    entry: ActivityEntry,
    decision: CopyDecision,
    request: ExecutionRequest,
    result: ExecutionSuccess,
    current_portfolio: models::PortfolioSnapshot,
    portfolio: PortfolioService,
    position_resolver: PositionResolver,
    closing_positions: ClosingPositions,
    closing_position_key: Option<PositionKey>,
    entry_metadata: Option<EntryExecutionMetadata>,
    released_hot_task_tx: mpsc::UnboundedSender<HotPathTradeTask>,
    notifier: Arc<TelegramNotifier>,
    health: Arc<HealthState>,
    analytics: Arc<ExecutionAnalyticsTracker>,
) {
    tokio::spawn(async move {
        let mut refreshed = if result.mode.is_paper() {
            match portfolio
                .apply_paper_fill(&entry, &result, decision.position_key.as_ref())
                .await
            {
                Ok(snapshot) => snapshot,
                Err(error) => {
                    warn!(?error, "failed to apply paper fill to portfolio snapshot");
                    current_portfolio.clone()
                }
            }
        } else {
            sleep(LIVE_POST_TRADE_REFRESH_DELAY).await;
            let projected_layout = match portfolio.project_fill_on_snapshot(
                &current_portfolio,
                &entry,
                &result,
                decision.position_key.as_ref(),
            ) {
                Ok(snapshot) => Some(snapshot),
                Err(error) => {
                    warn!(
                        ?error,
                        "failed to project wallet-attributed layout for live fill"
                    );
                    None
                }
            };
            match portfolio
                .refresh_and_persist_with_layout_hint(projected_layout.as_ref())
                .await
            {
                Ok(snapshot) => snapshot,
                Err(error) => {
                    warn!(?error, "failed to refresh portfolio after live execution");
                    if let Some(projected_snapshot) = projected_layout {
                        match portfolio.store_snapshot(projected_snapshot.clone()).await {
                            Ok(snapshot) => snapshot,
                            Err(store_error) => {
                                warn!(
                                    ?store_error,
                                    "failed to persist projected live snapshot after refresh error"
                                );
                                projected_snapshot
                            }
                        }
                    } else {
                        current_portfolio.clone()
                    }
                }
            }
        };
        if matches!(decision.side, ExecutionSide::Buy)
            && let (Some(position_key), Some(metadata)) =
                (decision.position_key.as_ref(), entry_metadata.as_ref())
            && let Some(position) = refreshed.position_mut_by_key(position_key)
        {
            position.entry_conviction_score = metadata.conviction_score;
            position.peak_price_since_open =
                position.peak_price_since_open.max(position.current_price);
            if let Ok(stored_snapshot) = portfolio.store_snapshot(refreshed.clone()).await {
                refreshed = stored_snapshot;
            }
        }
        match position_resolver.sync_from_portfolio(&refreshed) {
            Ok(released_exits) => {
                for released_exit in released_exits {
                    analytics
                        .record_exit_event("deferred_exit_released", Some(&refreshed))
                        .await;
                    if released_hot_task_tx
                        .send(source_exit_hot_task(released_exit))
                        .is_err()
                    {
                        warn!("failed to enqueue deferred exit released after post-trade refresh");
                    }
                }
            }
            Err(error) => warn!(
                ?error,
                "failed to sync position resolver after post-trade refresh"
            ),
        }
        health.set_portfolio_value(refreshed.total_value).await;
        analytics.sync_with_portfolio(&refreshed).await;

        if let Err(error) = notifier
            .send_trade_notification(
                &entry,
                &decision,
                &request,
                &result,
                &current_portfolio,
                &refreshed,
            )
            .await
        {
            warn!(?error, "failed to send trade notification");
        }

        release_closing_position(&closing_positions, closing_position_key.as_ref()).await;
    });
}

#[allow(clippy::too_many_arguments)]
async fn record_trade_skip(
    entry: &ActivityEntry,
    signal: &ConfirmedTradeSignal,
    risk: &RiskEngine,
    health: Arc<HealthState>,
    latency_logger: Arc<LatencyLogger>,
    analytics: Arc<ExecutionAnalyticsTracker>,
    portfolio: Option<&models::PortfolioSnapshot>,
    quote: Option<&models::BestQuote>,
    class: SourceTradeClass,
    reason: &SkipReason,
) {
    let skip_processing_ms = signal
        .stage_timestamps
        .detection_triggered_at
        .elapsed()
        .as_millis() as u64;
    let skipped_at = Utc::now();
    if entry.side.eq_ignore_ascii_case("BUY") {
        log_entry_rejection_diagnostics(entry, quote, class, reason);
    }
    log_skipped_trade(
        risk.should_log_skips(),
        entry,
        signal,
        skip_processing_ms,
        class,
        reason,
    );
    analytics.record_skip(class, reason, portfolio).await;
    if matches!(
        reason.code,
        "drawdown_guard_triggered"
            | "hard_stop_triggered"
            | "exposure_limit_reached"
            | "wallet_exposure_limit"
            | "market_exposure_limit"
            | "loss_streak_guard_triggered"
    ) {
        analytics.record_risk_event(reason.code, portfolio).await;
    }
    if reason.code == "hard_stop_triggered" {
        analytics
            .record_risk_event("trades_blocked_by_drawdown", portfolio)
            .await;
    }
    if entry.side.eq_ignore_ascii_case("BUY") && is_filter_rejection_reason(reason.code) {
        analytics
            .record_risk_event(
                format!("filter_rejections_by_reason:{}", reason.code),
                portfolio,
            )
            .await;
    }
    health
        .record_skip_latency(skip_processing_ms, reason.code)
        .await;
    if let Err(error) = latency_logger
        .record_skipped_trade(
            entry,
            &signal.stage_timestamps,
            skipped_at,
            skip_processing_ms,
            reason,
        )
        .await
    {
        warn!(?error, "failed to persist skipped trade latency event");
    }
    health.increment_skipped().await;
}

fn is_filter_rejection_reason(reason_code: &str) -> bool {
    matches!(
        reason_code,
        "market_ultra_short_filtered"
            | "low_visible_liquidity"
            | "spread_too_wide"
            | "price_chased_strict"
            | "too_late_strict"
            | "wallet_too_fast"
            | "market_cooldown"
            | "conflicting_wallet_signal"
            | "trade_quality_rejected"
    )
}

fn normalized_execution_side(entry: &ActivityEntry) -> ExecutionSide {
    match entry.side.as_str() {
        "SELL" => ExecutionSide::Sell,
        _ => ExecutionSide::Buy,
    }
}

fn eligible_class_for_side(side: ExecutionSide) -> SourceTradeClass {
    match side {
        ExecutionSide::Buy => SourceTradeClass::EligibleEntry,
        ExecutionSide::Sell => SourceTradeClass::EligibleExit,
    }
}

fn classify_rejected_trade(side: ExecutionSide, has_copied_inventory: bool) -> SourceTradeClass {
    match side {
        ExecutionSide::Buy => SourceTradeClass::EntryRejected,
        ExecutionSide::Sell if has_copied_inventory => SourceTradeClass::ExitRejected,
        ExecutionSide::Sell => SourceTradeClass::OrphanExit,
    }
}

fn log_entry_rejection_diagnostics(
    entry: &ActivityEntry,
    quote: Option<&models::BestQuote>,
    class: SourceTradeClass,
    reason: &SkipReason,
) {
    let source_price = decimal_from_f64(entry.price);
    let best_ask = quote.and_then(|quote| quote.best_ask);
    let visible_liquidity = quote.and_then(|quote| {
        quote
            .best_ask
            .zip(quote.best_ask_size)
            .map(|(price, size)| price * size)
    });
    let remaining_edge = best_ask.map(|price| (Decimal::ONE - price).max(Decimal::ZERO));

    info!(
        event = "buy_rejection_diagnostic",
        trade_class = class.as_str(),
        source_wallet = %entry.proxy_wallet,
        title = %entry.title,
        asset = %entry.asset,
        source_price = entry.price,
        best_ask = ?best_ask.map(|value| value.round_dp(6)),
        remaining_edge = ?remaining_edge.map(|value| value.round_dp(6)),
        visible_liquidity = ?visible_liquidity.map(|value| value.round_dp(6)),
        skip_reason_code = reason.code,
        price_band = risk::get_price_band(source_price).as_str(),
        price_bucket = price_bucket_label(source_price),
        "captured buy rejection diagnostics"
    );
}

fn price_bucket_label(price: Decimal) -> &'static str {
    if price < Decimal::new(10, 2) {
        "<0.10"
    } else if price < Decimal::new(30, 2) {
        "0.10-0.30"
    } else if price < Decimal::new(60, 2) {
        "0.30-0.60"
    } else if price < Decimal::new(80, 2) {
        "0.60-0.80"
    } else if price <= Decimal::new(95, 2) {
        "0.80-0.95"
    } else {
        ">0.95"
    }
}

async fn persist_confirmed_trade_seen(
    state_store: &Arc<StateStore>,
    entry: &ActivityEntry,
    health: &Arc<HealthState>,
) {
    if let Err(error) = state_store.mark_seen(entry).await {
        warn!(?error, "failed to persist matched tracked-wallet trade");
        health.set_last_error(format!("{error:#}")).await;
    }
}

async fn resolve_quote(
    orderbooks: &OrderBookState,
    entry: &ActivityEntry,
    decision: &CopyDecision,
    quote_policy: QuotePolicy,
    execution_confidence: f64,
) -> std::result::Result<ResolvedQuote, SkipReason> {
    match quote_policy {
        QuotePolicy::CacheOnly => {
            if let Some(quote) = orderbooks
                .best_quote(&decision.token_id)
                .await
                .filter(|quote| is_executable_quote(quote, decision.side))
            {
                return Ok(ResolvedQuote {
                    quote,
                    source: QuoteResolutionSource::Cache,
                });
            }

            let asset_id = decision.token_id.clone();
            let orderbooks_for_fetch = orderbooks.clone();
            let snapshot_fetch =
                tokio::spawn(
                    async move { orderbooks_for_fetch.refresh_book_snapshot(&asset_id).await },
                );

            for _ in 0..QUOTE_RETRY_ATTEMPTS {
                sleep(QUOTE_RETRY_DELAY).await;
                if let Some(quote) = orderbooks
                    .best_quote(&decision.token_id)
                    .await
                    .filter(|quote| is_executable_quote(quote, decision.side))
                {
                    return Ok(ResolvedQuote {
                        quote,
                        source: QuoteResolutionSource::Retry,
                    });
                }
            }

            match timeout(QUOTE_SNAPSHOT_TIMEOUT, snapshot_fetch).await {
                Ok(Ok(Ok(Some(quote)))) if is_executable_quote(&quote, decision.side) => {
                    return Ok(ResolvedQuote {
                        quote,
                        source: QuoteResolutionSource::SnapshotFetch,
                    });
                }
                Ok(Ok(Ok(Some(_)))) | Ok(Ok(Ok(None))) => {}
                Ok(Ok(Err(error))) => {
                    warn!(
                        ?error,
                        asset = %decision.token_id,
                        "order book snapshot refresh failed during quote resolution"
                    );
                }
                Ok(Err(error)) => {
                    warn!(
                        ?error,
                        asset = %decision.token_id,
                        "order book snapshot task failed during quote resolution"
                    );
                }
                Err(_) => {
                    warn!(
                        asset = %decision.token_id,
                        timeout_ms = QUOTE_SNAPSHOT_TIMEOUT.as_millis() as u64,
                        "order book snapshot refresh timed out during quote resolution"
                    );
                }
            }

            if execution_confidence >= HIGH_CONFIDENCE_QUOTE_FALLBACK_THRESHOLD {
                let has_last_valid_quote = orderbooks
                    .last_valid_quote(&decision.token_id)
                    .await
                    .is_some();
                if let Some(quote) = orderbooks
                    .fallback_quote(&decision.token_id, decision.side)
                    .await
                    .filter(|quote| is_executable_quote(quote, decision.side))
                {
                    return Ok(ResolvedQuote {
                        quote,
                        source: if has_last_valid_quote {
                            QuoteResolutionSource::LastValidFallback
                        } else {
                            QuoteResolutionSource::SyntheticFallback
                        },
                    });
                }
            }

            let diagnostic = orderbooks.quote_debug_summary(&decision.token_id).await;
            Err(SkipReason::new(
                "market_quote_not_cached",
                format!(
                    "no executable quote available for asset {} after retries={} snapshot_fetch=true source_price={} detail={diagnostic}",
                    decision.token_id, QUOTE_RETRY_ATTEMPTS, entry.price
                ),
            ))
        }
    }
}

fn is_executable_quote(quote: &models::BestQuote, side: ExecutionSide) -> bool {
    if quote.tick_size.is_zero() || quote.min_order_size.is_zero() {
        return false;
    }

    match side {
        ExecutionSide::Buy => quote.best_ask.is_some() && quote.best_ask_size.is_some(),
        ExecutionSide::Sell => quote.best_bid.is_some() && quote.best_bid_size.is_some(),
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_expired_prediction_validations(
    pending_validations: &mut PendingPredictionTracker,
    executor: Arc<dyn TradeExecutor>,
    orderbooks: Arc<OrderBookState>,
    portfolio: Arc<PortfolioService>,
    position_resolver: PositionResolver,
    closing_positions: ClosingPositions,
    released_hot_task_tx: mpsc::UnboundedSender<HotPathTradeTask>,
    notifier: Arc<TelegramNotifier>,
    attribution_logger: Arc<AttributionLogger>,
    health: Arc<HealthState>,
    analytics: Arc<ExecutionAnalyticsTracker>,
) {
    for pending_execution in pending_validations.take_expired(Instant::now()) {
        let confirmation_wait_ms = pending_execution.submitted_at.elapsed().as_millis();
        if pending_execution.execution_result.mode.is_paper() {
            attribution_logger
                .record_signal_event(
                    "prediction_unconfirmed_fail_safe",
                    &pending_execution.signal,
                    pending_validations.len(),
                    0,
                    Some(format!(
                        "paper_mode=true confirmation_wait_ms={} predicted_wallet={} confidence={:.3} action=log_only",
                        confirmation_wait_ms,
                        pending_execution.predicted_wallet,
                        pending_execution.confidence,
                    )),
                )
                .await;
            continue;
        }

        match executor
            .cancel_order(&pending_execution.execution_result.order_id)
            .await
        {
            Ok(cancel_result) if cancel_result.canceled => {
                attribution_logger
                    .record_signal_event(
                        "prediction_unconfirmed_fail_safe",
                        &pending_execution.signal,
                        pending_validations.len(),
                        0,
                        Some(format!(
                            "paper_mode=false confirmation_wait_ms={} predicted_wallet={} confidence={:.3} action=cancel detail={}",
                            confirmation_wait_ms,
                            pending_execution.predicted_wallet,
                            pending_execution.confidence,
                            cancel_result.detail,
                        )),
                    )
                    .await;
            }
            Ok(cancel_result) => {
                attribution_logger
                    .record_signal_event(
                        "prediction_fail_safe_cancel_missed",
                        &pending_execution.signal,
                        pending_validations.len(),
                        0,
                        Some(format!(
                            "confirmation_wait_ms={} predicted_wallet={} confidence={:.3} detail={}",
                            confirmation_wait_ms,
                            pending_execution.predicted_wallet,
                            pending_execution.confidence,
                            cancel_result.detail,
                        )),
                    )
                    .await;
                attempt_fail_safe_hedge(
                    pending_execution,
                    executor.clone(),
                    orderbooks.clone(),
                    portfolio.clone(),
                    position_resolver.clone(),
                    closing_positions.clone(),
                    released_hot_task_tx.clone(),
                    notifier.clone(),
                    attribution_logger.clone(),
                    health.clone(),
                    analytics.clone(),
                    pending_validations.len(),
                )
                .await;
            }
            Err(error) => {
                attribution_logger
                    .record_signal_event(
                        "prediction_fail_safe_cancel_error",
                        &pending_execution.signal,
                        pending_validations.len(),
                        0,
                        Some(format!(
                            "confirmation_wait_ms={} predicted_wallet={} confidence={:.3} error={error:#}",
                            confirmation_wait_ms,
                            pending_execution.predicted_wallet,
                            pending_execution.confidence,
                        )),
                    )
                    .await;
                health.set_last_error(format!("{error:#}")).await;
                attempt_fail_safe_hedge(
                    pending_execution,
                    executor.clone(),
                    orderbooks.clone(),
                    portfolio.clone(),
                    position_resolver.clone(),
                    closing_positions.clone(),
                    released_hot_task_tx.clone(),
                    notifier.clone(),
                    attribution_logger.clone(),
                    health.clone(),
                    analytics.clone(),
                    pending_validations.len(),
                )
                .await;
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn attempt_fail_safe_hedge(
    pending_execution: PendingPredictionExecution,
    executor: Arc<dyn TradeExecutor>,
    orderbooks: Arc<OrderBookState>,
    portfolio: Arc<PortfolioService>,
    position_resolver: PositionResolver,
    closing_positions: ClosingPositions,
    released_hot_task_tx: mpsc::UnboundedSender<HotPathTradeTask>,
    notifier: Arc<TelegramNotifier>,
    attribution_logger: Arc<AttributionLogger>,
    health: Arc<HealthState>,
    analytics: Arc<ExecutionAnalyticsTracker>,
    pending_count: usize,
) {
    let quote = match orderbooks
        .best_quote(&pending_execution.order_request.token_id)
        .await
    {
        Some(quote) => quote,
        None => {
            attribution_logger
                .record_signal_event(
                    "prediction_fail_safe_hedge_skipped",
                    &pending_execution.signal,
                    pending_count,
                    0,
                    Some("no cached quote available to hedge unconfirmed execution".to_owned()),
                )
                .await;
            return;
        }
    };

    let mode = ExecutionMode::EmergencyExit {
        stage: emergency_exit_stage(&pending_execution),
    };
    let hedge_request = match build_fail_safe_hedge_request(&pending_execution, &quote, mode) {
        Ok(request) => request,
        Err(reason) => {
            attribution_logger
                .record_signal_event(
                    "prediction_fail_safe_hedge_skipped",
                    &pending_execution.signal,
                    pending_count,
                    0,
                    Some(reason.detail),
                )
                .await;
            return;
        }
    };

    match executor.submit_order(hedge_request).await {
        Ok(result) => {
            let hedge_filled = result.success && result.has_fill();
            attribution_logger
                .record_signal_event(
                    "prediction_fail_safe_hedged",
                    &pending_execution.signal,
                    pending_count,
                    0,
                    Some(format!(
                        "hedge_order_id={} hedge_success={} hedge_filled={} source_trade_id={}",
                        result.order_id,
                        result.success,
                        hedge_filled,
                        pending_execution.source_entry.dedupe_key(),
                    )),
                )
                .await;
            if hedge_filled {
                let current_portfolio = match portfolio.snapshot().await {
                    Some(snapshot) => snapshot,
                    None => match portfolio.refresh_snapshot().await {
                        Ok(snapshot) => snapshot,
                        Err(error) => {
                            warn!(
                                ?error,
                                "failed to load portfolio snapshot for fail-safe hedge side effects"
                            );
                            health.set_last_error(format!("{error:#}")).await;
                            return;
                        }
                    },
                };
                let hedge_entry = build_fail_safe_hedge_entry(&pending_execution, &result);
                let hedge_decision = fail_safe_hedge_decision(&result);
                spawn_post_trade_side_effects(
                    hedge_entry,
                    hedge_decision,
                    result.order_request.clone(),
                    result,
                    current_portfolio,
                    portfolio.as_ref().clone(),
                    position_resolver,
                    closing_positions.clone(),
                    None,
                    None,
                    released_hot_task_tx,
                    notifier,
                    health,
                    analytics.clone(),
                );
            }
        }
        Err(error) => {
            attribution_logger
                .record_signal_event(
                    "prediction_fail_safe_hedge_error",
                    &pending_execution.signal,
                    pending_count,
                    0,
                    Some(format!("error={error:#}")),
                )
                .await;
            health.set_last_error(format!("{error:#}")).await;
        }
    }
}

fn build_fail_safe_hedge_request(
    pending_execution: &PendingPredictionExecution,
    quote: &models::BestQuote,
    mode: ExecutionMode,
) -> std::result::Result<ExecutionRequest, SkipReason> {
    let entry = build_fail_safe_hedge_request_entry(pending_execution);
    let decision = fail_safe_hedge_request_decision(pending_execution);
    build_execution_request_with_mode(&entry, &decision, quote, mode)
}

fn build_fail_safe_hedge_entry(
    pending_execution: &PendingPredictionExecution,
    result: &ExecutionSuccess,
) -> ActivityEntry {
    ActivityEntry {
        proxy_wallet: pending_execution.source_entry.proxy_wallet.clone(),
        timestamp: Utc::now().timestamp_millis(),
        condition_id: pending_execution.source_entry.condition_id.clone(),
        type_name: "FAIL_SAFE_HEDGE".to_owned(),
        size: decimal_to_f64(result.filled_size),
        usdc_size: decimal_to_f64(result.filled_notional),
        transaction_hash: result
            .transaction_hashes
            .first()
            .cloned()
            .unwrap_or_else(|| format!("hedge:{}", pending_execution.source_entry.dedupe_key())),
        price: decimal_to_f64(result.filled_price),
        asset: result.order_request.token_id.clone(),
        side: signal_side_label(result.order_request.side).to_owned(),
        outcome_index: pending_execution.source_entry.outcome_index,
        title: format!("Fail-safe hedge | {}", pending_execution.source_entry.title),
        slug: pending_execution.source_entry.slug.clone(),
        event_slug: pending_execution.source_entry.event_slug.clone(),
        outcome: pending_execution.source_entry.outcome.clone(),
    }
}

fn fail_safe_hedge_decision(result: &ExecutionSuccess) -> CopyDecision {
    CopyDecision {
        token_id: result.order_request.token_id.clone(),
        side: result.order_request.side,
        notional: result.filled_notional,
        size: result.filled_size,
        size_was_scaled: false,
        position_key: None,
        price_band: risk::get_price_band(result.filled_price),
        max_market_spread_bps: 0,
        min_top_of_book_ratio: rust_decimal::Decimal::ZERO,
        min_visible_liquidity_usd: rust_decimal::Decimal::ZERO,
        max_slippage_bps: 0,
        max_source_price_slippage_bps: 0,
        min_edge_threshold: rust_decimal::Decimal::ZERO,
    }
}

fn decimal_to_f64(value: rust_decimal::Decimal) -> f64 {
    value.to_string().parse::<f64>().unwrap_or(0.0)
}

fn build_fail_safe_hedge_request_entry(
    pending_execution: &PendingPredictionExecution,
) -> ActivityEntry {
    ActivityEntry {
        proxy_wallet: pending_execution.source_entry.proxy_wallet.clone(),
        timestamp: Utc::now().timestamp_millis(),
        condition_id: pending_execution.source_entry.condition_id.clone(),
        type_name: "FAIL_SAFE_HEDGE_REQUEST".to_owned(),
        size: decimal_to_f64(pending_execution.execution_result.filled_size),
        usdc_size: decimal_to_f64(pending_execution.execution_result.filled_notional),
        transaction_hash: format!("hedge:{}", pending_execution.source_entry.dedupe_key()),
        price: decimal_to_f64(pending_execution.execution_result.filled_price),
        asset: pending_execution.order_request.token_id.clone(),
        side: signal_side_label(opposite_side(pending_execution.order_request.side)).to_owned(),
        outcome_index: pending_execution.source_entry.outcome_index,
        title: format!(
            "Fail-safe hedge request | {}",
            pending_execution.source_entry.title
        ),
        slug: pending_execution.source_entry.slug.clone(),
        event_slug: pending_execution.source_entry.event_slug.clone(),
        outcome: pending_execution.source_entry.outcome.clone(),
    }
}

fn fail_safe_hedge_request_decision(
    pending_execution: &PendingPredictionExecution,
) -> CopyDecision {
    CopyDecision {
        token_id: pending_execution.order_request.token_id.clone(),
        side: opposite_side(pending_execution.order_request.side),
        notional: pending_execution.execution_result.filled_notional,
        size: pending_execution.execution_result.filled_size,
        size_was_scaled: false,
        position_key: None,
        price_band: pending_execution.decision.price_band,
        max_market_spread_bps: pending_execution.decision.max_market_spread_bps,
        min_top_of_book_ratio: pending_execution.decision.min_top_of_book_ratio,
        min_visible_liquidity_usd: pending_execution.decision.min_visible_liquidity_usd,
        max_slippage_bps: pending_execution.decision.max_slippage_bps,
        max_source_price_slippage_bps: pending_execution.decision.max_source_price_slippage_bps,
        min_edge_threshold: pending_execution.decision.min_edge_threshold,
    }
}

fn emergency_exit_stage(pending_execution: &PendingPredictionExecution) -> u8 {
    let elapsed = pending_execution.submitted_at.elapsed();
    if elapsed >= EMERGENCY_EXIT_STAGE_THREE_AFTER {
        3
    } else if elapsed >= EMERGENCY_EXIT_STAGE_TWO_AFTER {
        2
    } else {
        1
    }
}

fn opposite_side(side: ExecutionSide) -> ExecutionSide {
    match side {
        ExecutionSide::Buy => ExecutionSide::Sell,
        ExecutionSide::Sell => ExecutionSide::Buy,
    }
}

fn format_prediction_detail(prediction: &prediction::PredictionDecision) -> String {
    format!(
        "confidence={:.3} tier={} size_multiplier={:.2} wallet={} reasons={}",
        prediction.confidence,
        prediction.tier.as_str(),
        prediction.size_multiplier(),
        prediction.predicted_wallet.as_deref().unwrap_or("unknown"),
        prediction.reasons.join("|")
    )
}

fn enforce_signal_price_deviation(
    entry: &ActivityEntry,
    decision: &CopyDecision,
    quote: &models::BestQuote,
) -> std::result::Result<(), SkipReason> {
    let signal_price = entry.price_decimal().map_err(|error| {
        SkipReason::new(
            "invalid_source_price",
            format!("failed to parse source price: {error}"),
        )
    })?;
    if signal_price <= rust_decimal::Decimal::ZERO {
        return Err(SkipReason::new(
            "source_price_zero",
            "source trade price is zero",
        ));
    }

    let market_price = match decision.side {
        ExecutionSide::Buy => quote.best_ask,
        ExecutionSide::Sell => quote.best_bid,
    }
    .ok_or_else(|| {
        SkipReason::new(
            "missing_market_price_for_signal_deviation",
            format!(
                "missing executable market price for asset {}",
                decision.token_id
            ),
        )
    })?;
    let deviation_ratio = ((market_price - signal_price) / signal_price).abs();
    let deviation_bps = deviation_ratio * rust_decimal::Decimal::new(10_000, 0);
    let max_deviation_bps = rust_decimal::Decimal::from(decision.max_source_price_slippage_bps);
    if deviation_bps > max_deviation_bps {
        return Err(SkipReason::new(
            "price_chased",
            format!(
                "market price {} deviates {} bps from signal price {} (max {} bps)",
                market_price.round_dp(4),
                deviation_bps.round_dp(2),
                signal_price.round_dp(4),
                max_deviation_bps
            ),
        ));
    }

    Ok(())
}

async fn spawn_portfolio_refresher(
    portfolio: Arc<PortfolioService>,
    health: Arc<HealthState>,
    analytics: Arc<ExecutionAnalyticsTracker>,
    position_registry: PositionRegistry,
    position_resolver: PositionResolver,
    released_hot_task_tx: mpsc::UnboundedSender<HotPathTradeTask>,
) {
    loop {
        sleep(Duration::from_secs(15)).await;
        match portfolio.refresh_and_persist().await {
            Ok(snapshot) => {
                health.set_portfolio_value(snapshot.total_value).await;
                analytics.sync_with_portfolio(&snapshot).await;
                if let Err(error) = position_registry.sync_from_portfolio(&snapshot) {
                    warn!(
                        ?error,
                        "failed to sync position registry from refreshed portfolio"
                    );
                }
                match position_resolver.sync_from_portfolio(&snapshot) {
                    Ok(released_exits) => {
                        for released_exit in released_exits {
                            analytics
                                .record_exit_event("deferred_exit_released", Some(&snapshot))
                                .await;
                            if released_hot_task_tx
                                .send(source_exit_hot_task(released_exit))
                                .is_err()
                            {
                                warn!(
                                    "failed to enqueue deferred exit released by portfolio refresh"
                                );
                            }
                        }
                    }
                    Err(error) => warn!(
                        ?error,
                        "failed to sync position resolver from refreshed portfolio"
                    ),
                }
            }
            Err(error) => warn!(?error, "background portfolio refresh failed"),
        }
    }
}

async fn spawn_unresolved_exit_retry_worker(
    portfolio: Arc<PortfolioService>,
    position_registry: PositionRegistry,
    position_resolver: PositionResolver,
    exit_resolution: Arc<ExitResolutionBuffer>,
    analytics: Arc<ExecutionAnalyticsTracker>,
    released_hot_task_tx: mpsc::UnboundedSender<HotPathTradeTask>,
) {
    let mut ticker = interval(Duration::from_millis(100));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        ticker.tick().await;
        let now = Utc::now();

        match position_resolver.take_expired_pending(now) {
            Ok(expired_pending) => {
                for expired in expired_pending {
                    for bound_exit in expired.bound_exits {
                        analytics
                            .record_exit_event("deferred_exit_expired", None)
                            .await;
                        if let Err(error) = exit_resolution
                            .enqueue_unresolved_exit(
                                &bound_exit.entry,
                                "pending_open_ttl_expired",
                                now,
                            )
                            .await
                        {
                            warn!(
                                ?error,
                                condition_id = %expired.key.condition_id,
                                outcome = %expired.key.outcome,
                                source_wallet = %expired.key.source_wallet,
                                "failed to queue deferred exit after pending-open expiry"
                            );
                        }
                    }
                }
            }
            Err(error) => warn!(?error, "failed to expire pending resolver positions"),
        }

        let retry_keys = exit_resolution.due_retry_keys(now).await;
        if retry_keys.is_empty() {
            continue;
        }

        let Some(snapshot) = portfolio.snapshot().await else {
            continue;
        };

        for retry_key in retry_keys {
            let Some(event) = exit_resolution.retry_event(&retry_key).await else {
                continue;
            };

            match exit_resolution
                .retry_unresolved_exit(
                    &retry_key,
                    &snapshot,
                    &position_registry,
                    &position_resolver,
                    now,
                )
                .await
            {
                Ok(SourceExitResolution::Matched(position_key)) => {
                    analytics
                        .record_exit_event("source_exit_retry_resolved", Some(&snapshot))
                        .await;
                    analytics
                        .record_exit_event("exit_resolved_success", Some(&snapshot))
                        .await;
                    info!(
                        event = "source_exit_retry_resolved",
                        retry_key = %retry_key,
                        condition_id = %position_key.condition_id,
                        outcome = %position_key.outcome,
                        source_wallet = %position_key.source_wallet,
                        "resolved queued source exit through local state"
                    );
                    if released_hot_task_tx
                        .send(deferred_source_exit_hot_task(event))
                        .is_err()
                    {
                        warn!("failed to enqueue retry-resolved source exit");
                    }
                }
                Ok(SourceExitResolution::MatchedByFallback(position_key, fallback_reason)) => {
                    analytics
                        .record_exit_event("source_exit_retry_resolved", Some(&snapshot))
                        .await;
                    analytics
                        .record_exit_event("exit_resolved_success", Some(&snapshot))
                        .await;
                    info!(
                        event = "source_exit_retry_resolved",
                        retry_key = %retry_key,
                        condition_id = %position_key.condition_id,
                        outcome = %position_key.outcome,
                        source_wallet = %position_key.source_wallet,
                        fallback_reason = %fallback_reason,
                        "resolved queued source exit through safe fallback"
                    );
                    if released_hot_task_tx
                        .send(deferred_source_exit_hot_task(event))
                        .is_err()
                    {
                        warn!("failed to enqueue retry-resolved fallback source exit");
                    }
                }
                Ok(SourceExitResolution::BoundToPending(_)) => {
                    analytics
                        .record_exit_event("exit_bound_to_pending", Some(&snapshot))
                        .await;
                }
                Ok(SourceExitResolution::NotActionable(non_actionable)) => {
                    analytics
                        .record_exit_event(non_actionable.reason, Some(&snapshot))
                        .await;
                    analytics
                        .record_exit_event("exit_noise_filtered", Some(&snapshot))
                        .await;
                    info!(
                        retry_key = %retry_key,
                        reason = non_actionable.reason,
                        detail = %non_actionable.detail,
                        "dropped non-actionable unresolved source exit during retry"
                    );
                }
                Ok(SourceExitResolution::DeferredRetry(_)) => {}
                Ok(SourceExitResolution::Failed(reason_detail)) => {
                    analytics
                        .record_exit_event("source_exit_unresolved_expired", Some(&snapshot))
                        .await;
                    analytics
                        .record_exit_event("exit_resolved_failed", Some(&snapshot))
                        .await;
                    analytics
                        .record_close_failure_reason("exit_unresolved", Some(&snapshot))
                        .await;
                    warn!(
                        retry_key = %retry_key,
                        reason = %reason_detail,
                        "unresolved source exit expired"
                    );
                }
                Err(error) => warn!(
                    ?error,
                    retry_key = %retry_key,
                    "failed to retry unresolved source exit"
                ),
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn spawn_force_exit_watcher(
    settings: Settings,
    portfolio: Arc<PortfolioService>,
    position_registry: PositionRegistry,
    position_resolver: PositionResolver,
    exit_resolution: Arc<ExitResolutionBuffer>,
    executor: Arc<dyn TradeExecutor>,
    orderbooks: Arc<OrderBookState>,
    closing_positions: ClosingPositions,
    released_hot_task_tx: mpsc::UnboundedSender<HotPathTradeTask>,
    notifier: Arc<TelegramNotifier>,
    health: Arc<HealthState>,
    analytics: Arc<ExecutionAnalyticsTracker>,
) {
    let mut ticker = interval(EXIT_MANAGER_SWEEP_INTERVAL);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        ticker.tick().await;

        let snapshot = match load_current_portfolio_snapshot(portfolio.as_ref()).await {
            Ok(snapshot) => snapshot,
            Err(error) => {
                warn!(
                    ?error,
                    "failed to load portfolio snapshot for force-exit watcher"
                );
                continue;
            }
        };

        let now = Utc::now();
        let positions = snapshot
            .positions
            .iter()
            .filter(|position| {
                position.state == models::PositionState::Open && position.is_active()
            })
            .cloned()
            .collect::<Vec<_>>();

        if snapshot.hard_stop_active && portfolio.force_close_on_hard_stop() {
            for position in positions.clone() {
                let Some(quote) = orderbooks.best_quote(&position.asset).await else {
                    continue;
                };
                if quote.best_bid.is_none() {
                    continue;
                }
                if let Err(error) = attempt_managed_exit_position(
                    &settings,
                    ManagedExitReason::HardStop,
                    position,
                    quote,
                    ExecutionMode::EmergencyExit { stage: 3 },
                    false,
                    portfolio.clone(),
                    position_registry.clone(),
                    position_resolver.clone(),
                    executor.clone(),
                    closing_positions.clone(),
                    released_hot_task_tx.clone(),
                    notifier.clone(),
                    health.clone(),
                    analytics.clone(),
                )
                .await
                {
                    warn!(?error, "hard-stop emergency exit attempt failed");
                }
            }
        }

        for position in positions {
            let Some(quote) = orderbooks.best_quote(&position.asset).await else {
                continue;
            };
            let Some(best_bid) = quote.best_bid else {
                continue;
            };
            let source_exit_pending = exit_resolution
                .has_pending_source_exit_for_position(&position.position_key())
                .await;
            let exit_evaluation = managed_exit_reason(
                &settings,
                &position,
                now,
                best_bid,
                effective_time_exit_seconds(settings.max_hold_time_seconds),
                source_exit_pending,
            );
            let reason = match exit_evaluation {
                ManagedExitEvaluation::Trigger(reason) => {
                    if reason == ManagedExitReason::ProfitProtection {
                        analytics
                            .record_exit_event(
                                "time_exit_blocked_due_to_profit_protection",
                                Some(&snapshot),
                            )
                            .await;
                    }
                    reason
                }
                ManagedExitEvaluation::SuppressTimeExit(suppressed_reason) => {
                    analytics
                        .record_exit_event("premature_time_exit", Some(&snapshot))
                        .await;
                    match suppressed_reason {
                        "source_exit_pending" => {
                            analytics
                                .record_exit_event(
                                    "time_exit_blocked_due_to_pending_source_exit",
                                    Some(&snapshot),
                                )
                                .await;
                        }
                        "mild_loss_extension" => {
                            analytics
                                .record_exit_event("adaptive_time_extension_used", Some(&snapshot))
                                .await;
                        }
                        "profit_protection_active" => {
                            analytics
                                .record_exit_event(
                                    "time_exit_blocked_due_to_profit_protection",
                                    Some(&snapshot),
                                )
                                .await;
                        }
                        _ => {}
                    }
                    info!(
                        event = "time_exit_suppressed",
                        reason = suppressed_reason,
                        condition_id = %position.condition_id,
                        outcome = %position.outcome,
                        source_wallet = %position.source_wallet,
                        "suppressed premature time exit"
                    );
                    continue;
                }
                ManagedExitEvaluation::None => {
                    continue;
                }
            };

            if let Err(error) = attempt_managed_exit_position(
                &settings,
                reason,
                position,
                quote,
                ExecutionMode::MandatoryExit { reason },
                true,
                portfolio.clone(),
                position_registry.clone(),
                position_resolver.clone(),
                executor.clone(),
                closing_positions.clone(),
                released_hot_task_tx.clone(),
                notifier.clone(),
                health.clone(),
                analytics.clone(),
            )
            .await
            {
                warn!(
                    ?error,
                    exit_reason = reason.as_str(),
                    "managed exit attempt failed"
                );
            }
        }

        let closing_positions_due = snapshot
            .positions
            .iter()
            .filter(|position| {
                position.state == models::PositionState::Closing && position.is_active()
            })
            .cloned()
            .collect::<Vec<_>>();

        for position in closing_positions_due {
            let Some(action) = closing_action_plan(&position, &settings, now) else {
                continue;
            };

            match action {
                ClosingActionPlan::Retry {
                    mode,
                    escalation_level,
                } => {
                    if escalation_level > position.closing_escalation_level
                        && let Err(error) = persist_closing_escalation(
                            portfolio.as_ref(),
                            &analytics,
                            &position.position_key(),
                            escalation_level,
                        )
                        .await
                    {
                        warn!(
                            ?error,
                            condition_id = %position.condition_id,
                            outcome = %position.outcome,
                            source_wallet = %position.source_wallet,
                            "failed to persist closing escalation level"
                        );
                    }

                    let Some(quote) = orderbooks.best_quote(&position.asset).await else {
                        continue;
                    };
                    if quote.best_bid.is_none() {
                        continue;
                    }
                    let reason = closing_reason_from_position(&position);
                    if let Err(error) = attempt_managed_exit_position(
                        &settings,
                        reason,
                        position,
                        quote,
                        mode,
                        false,
                        portfolio.clone(),
                        position_registry.clone(),
                        position_resolver.clone(),
                        executor.clone(),
                        closing_positions.clone(),
                        released_hot_task_tx.clone(),
                        notifier.clone(),
                        health.clone(),
                        analytics.clone(),
                    )
                    .await
                    {
                        warn!(
                            ?error,
                            exit_reason = reason.as_str(),
                            "closing retry attempt failed"
                        );
                    }
                }
                ClosingActionPlan::Fail { reason } => {
                    if let Err(error) = mark_failed_closing_position_stale(
                        portfolio.as_ref(),
                        &closing_positions,
                        &position,
                        &position_resolver,
                        reason,
                        analytics.clone(),
                    )
                    .await
                    {
                        warn!(
                            ?error,
                            condition_id = %position.condition_id,
                            outcome = %position.outcome,
                            source_wallet = %position.source_wallet,
                            "failed to finalize stale closing position"
                        );
                    }
                }
            }
        }
    }
}

async fn spawn_periodic_portfolio_summary(
    portfolio: Arc<PortfolioService>,
    notifier: Arc<TelegramNotifier>,
) {
    loop {
        let wait_time = match notifier.time_until_next_periodic_summary().await {
            Ok(delay) => delay,
            Err(error) => {
                warn!(
                    ?error,
                    "failed to load periodic summary state; retrying from default interval"
                );
                SUMMARY_REFRESH_INTERVAL
            }
        };
        sleep(wait_time).await;

        let snapshot = match portfolio.snapshot().await {
            Some(snapshot) => snapshot,
            None => match portfolio.refresh_and_persist().await {
                Ok(snapshot) => snapshot,
                Err(error) => {
                    warn!(?error, "periodic portfolio summary refresh failed");
                    continue;
                }
            },
        };

        if let Err(error) = notifier.refresh_periodic_summary(&snapshot).await {
            warn!(?error, "failed to refresh periodic portfolio summary");
            sleep(PERIODIC_SUMMARY_RETRY_DELAY).await;
        }
    }
}

async fn spawn_log_retention_maintainer(
    attribution_logger: Arc<AttributionLogger>,
    latency_logger: Arc<LatencyLogger>,
    raw_activity_logger: Arc<RawActivityLogger>,
) {
    let mut ticker = interval(LOG_RETENTION_SWEEP_INTERVAL);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        ticker.tick().await;

        if let Err(error) = attribution_logger
            .cull_old_entries(LOG_RETENTION_WINDOW)
            .await
        {
            warn!(?error, "failed to cull attribution log retention window");
        }
        if let Err(error) = latency_logger.cull_old_entries(LOG_RETENTION_WINDOW).await {
            warn!(?error, "failed to cull latency log retention window");
        }
        if let Err(error) = raw_activity_logger
            .cull_old_entries(LOG_RETENTION_WINDOW)
            .await
        {
            warn!(?error, "failed to cull raw activity log retention window");
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn attempt_managed_exit_position(
    settings: &Settings,
    reason: ManagedExitReason,
    position: models::PortfolioPosition,
    quote: models::BestQuote,
    execution_mode: ExecutionMode,
    record_managed_metric: bool,
    portfolio: Arc<PortfolioService>,
    position_registry: PositionRegistry,
    position_resolver: PositionResolver,
    executor: Arc<dyn TradeExecutor>,
    closing_positions: ClosingPositions,
    released_hot_task_tx: mpsc::UnboundedSender<HotPathTradeTask>,
    notifier: Arc<TelegramNotifier>,
    health: Arc<HealthState>,
    analytics: Arc<ExecutionAnalyticsTracker>,
) -> Result<()> {
    let current_portfolio = load_current_portfolio_snapshot(portfolio.as_ref()).await?;
    let position_key = position.position_key();
    if !claim_closing_position(&closing_positions, &position_key).await {
        info!(
            reason_code = "already_closing",
            exit_reason = reason.as_str(),
            condition_id = %position.condition_id,
            outcome = %position.outcome,
            source_wallet = %position.source_wallet,
            "skipping duplicate exit for position already closing"
        );
        return Ok(());
    }
    let mut closing_snapshot = current_portfolio.clone();
    if !closing_snapshot.mark_position_closing(&position_key, reason.as_str()) {
        release_closing_position(&closing_positions, Some(&position_key)).await;
        return Ok(());
    }
    closing_snapshot.note_close_attempt(&position_key);
    let _ = position_resolver.mark_closing(&position_key, reason.as_str());
    if let Err(error) = portfolio.store_snapshot(closing_snapshot.clone()).await {
        release_closing_position(&closing_positions, Some(&position_key)).await;
        return Err(error);
    }
    if record_managed_metric && let Some(event_name) = managed_exit_event_name(reason) {
        analytics
            .record_exit_event(event_name, Some(&closing_snapshot))
            .await;
        if reason == ManagedExitReason::TimeExit {
            analytics
                .record_exit_event("managed_exit_time_limit", Some(&closing_snapshot))
                .await;
        }
        if reason == ManagedExitReason::ProfitProtection {
            analytics
                .record_exit_event("profit_protection_exit_triggered", Some(&closing_snapshot))
                .await;
        }
    }

    info!(
        event = "managed_exit_triggered",
        exit_reason = reason.as_str(),
        condition_id = %position.condition_id,
        outcome = %position.outcome,
        source_wallet = %position.source_wallet,
        asset = %position.asset,
        hold_seconds = position
            .age(Utc::now())
            .map(|age| age.num_seconds())
            .unwrap_or_default(),
        "triggering managed exit"
    );

    let entry = build_managed_exit_entry(&position, &quote, reason);
    let decision = build_managed_exit_decision(&position, settings);
    let request = match build_execution_request_with_mode(&entry, &decision, &quote, execution_mode)
    {
        Ok(request) => request,
        Err(reason) => {
            analytics
                .record_close_failure_reason(reason.code, Some(&closing_snapshot))
                .await;
            persist_failed_close_attempt_state(
                portfolio.as_ref(),
                &closing_positions,
                &mut closing_snapshot,
                Some(&position_key),
                reason.code,
            )
            .await;
            return Err(anyhow!(reason.to_string()));
        }
    };
    let result = match executor.submit_order(request.clone()).await {
        Ok(result) => result,
        Err(error) => {
            analytics
                .record_close_failure_reason("close_submission_error", Some(&closing_snapshot))
                .await;
            persist_failed_close_attempt_state(
                portfolio.as_ref(),
                &closing_positions,
                &mut closing_snapshot,
                Some(&position_key),
                "close_submission_error",
            )
            .await;
            return Err(error);
        }
    };
    analytics
        .record_exit_event("close_submitted", Some(&closing_snapshot))
        .await;
    if !result.success || matches!(result.status, ExecutionStatus::Rejected) {
        analytics
            .record_exit_event("close_rejected", Some(&closing_snapshot))
            .await;
        analytics
            .record_close_failure_reason("close_rejected", Some(&closing_snapshot))
            .await;
        persist_failed_close_attempt_state(
            portfolio.as_ref(),
            &closing_positions,
            &mut closing_snapshot,
            Some(&position_key),
            "close_rejected",
        )
        .await;
        return Err(anyhow!(
            "managed exit order incomplete: success={} filled={} order_id={} status={}",
            result.success,
            result.has_fill(),
            result.order_id,
            result.status
        ));
    }
    if matches!(result.status, ExecutionStatus::NoFill) || !result.has_fill() {
        analytics
            .record_exit_event("close_nofill", Some(&closing_snapshot))
            .await;
        analytics
            .record_close_failure_reason("close_nofill", Some(&closing_snapshot))
            .await;
        persist_failed_close_attempt_state(
            portfolio.as_ref(),
            &closing_positions,
            &mut closing_snapshot,
            Some(&position_key),
            "close_nofill",
        )
        .await;
        return Err(anyhow!(
            "managed exit order incomplete: success={} filled={} order_id={} status={}",
            result.success,
            result.has_fill(),
            result.order_id,
            result.status
        ));
    }
    let close_is_partial = result.filled_size < request.size
        || matches!(result.status, ExecutionStatus::PartiallyFilled);
    analytics
        .record_exit_event(
            if close_is_partial {
                "close_partial"
            } else {
                "close_filled"
            },
            Some(&closing_snapshot),
        )
        .await;
    let close_fully_closed = closing_snapshot
        .position_by_key(&position_key)
        .map(|tracked_position| tracked_position.size <= result.filled_size)
        .unwrap_or(false);
    if let Err(error) = position_registry
        .record_execution(&entry, &result, Some(&position_key), close_fully_closed)
        .await
    {
        warn!(
            ?error,
            "failed to update position registry after managed exit"
        );
        resync_position_registry_after_persist_error(
            &position_registry,
            &closing_snapshot,
            "managed_exit_execution",
        );
    }
    analytics
        .record_processed(
            SourceTradeClass::EligibleExit,
            &entry,
            &result,
            Some(&closing_snapshot),
            Some(&position_key),
            None,
        )
        .await;
    health.increment_processed().await;

    spawn_post_trade_side_effects(
        entry,
        decision,
        request,
        result,
        closing_snapshot,
        portfolio.as_ref().clone(),
        position_resolver,
        closing_positions,
        Some(position_key),
        None,
        released_hot_task_tx,
        notifier,
        health,
        analytics,
    );
    Ok(())
}

fn build_managed_exit_entry(
    position: &models::PortfolioPosition,
    quote: &models::BestQuote,
    reason: ManagedExitReason,
) -> ActivityEntry {
    let best_bid = quote
        .best_bid
        .unwrap_or_else(|| position.current_price.max(position.average_entry_price));
    ActivityEntry {
        proxy_wallet: position.source_wallet.clone(),
        timestamp: Utc::now().timestamp_millis(),
        condition_id: position.condition_id.clone(),
        type_name: reason.as_str().to_owned(),
        size: decimal_to_f64(position.size),
        usdc_size: decimal_to_f64(position.current_value),
        transaction_hash: format!(
            "{}:{}",
            reason.as_str().to_ascii_lowercase(),
            position.asset
        ),
        price: decimal_to_f64(best_bid.max(position.average_entry_price)),
        asset: position.asset.clone(),
        side: "SELL".to_owned(),
        outcome_index: 0,
        title: format!("{} | {}", reason.as_str(), position.title),
        slug: position.title.clone(),
        event_slug: position.title.clone(),
        outcome: position.outcome.clone(),
    }
}

fn build_managed_exit_decision(
    position: &models::PortfolioPosition,
    settings: &Settings,
) -> CopyDecision {
    CopyDecision {
        token_id: position.asset.clone(),
        side: ExecutionSide::Sell,
        notional: position.current_value,
        size: position.size,
        size_was_scaled: false,
        position_key: Some(position.position_key()),
        price_band: risk::get_price_band(position.current_price),
        max_market_spread_bps: settings.max_market_spread_bps,
        min_top_of_book_ratio: settings.min_top_of_book_ratio,
        min_visible_liquidity_usd: settings.min_liquidity.max(rust_decimal::Decimal::ZERO),
        max_slippage_bps: settings.max_slippage_bps,
        max_source_price_slippage_bps: settings.max_source_price_slippage_bps,
        min_edge_threshold: settings.min_edge_threshold.max(rust_decimal::Decimal::ZERO),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TimeExitEvaluation {
    Trigger,
    Suppress(&'static str),
    None,
}

fn profit_protection_triggered(
    settings: &Settings,
    position: &models::PortfolioPosition,
    best_bid: rust_decimal::Decimal,
) -> bool {
    if !settings.enable_trailing_profit_exit {
        return false;
    }
    if position.average_entry_price <= rust_decimal::Decimal::ZERO
        || position.current_price <= rust_decimal::Decimal::ZERO
    {
        return false;
    }

    let conviction = position_conviction_score(position);
    let min_profit_pct = settings.profit_protect_min_pnl_pct.max(Decimal::ZERO)
        * if conviction >= dec!(0.85) {
            dec!(1.10)
        } else if conviction <= dec!(0.60) {
            dec!(0.85)
        } else {
            Decimal::ONE
        };
    let reversal_bps = Decimal::from(settings.profit_protect_reversal_bps.max(1))
        * if conviction >= dec!(0.85) {
            dec!(1.15)
        } else if conviction <= dec!(0.60) {
            dec!(0.75)
        } else {
            Decimal::ONE
        };
    let peak_price = position
        .peak_price_since_open
        .max(position.current_price)
        .max(best_bid);
    let profitable_peak = peak_price
        >= position.average_entry_price
            * (rust_decimal::Decimal::ONE + min_profit_pct.max(Decimal::ZERO));
    if !profitable_peak {
        return false;
    }

    let ratchet_floor = peak_price
        * (rust_decimal::Decimal::ONE
            - (reversal_bps.max(Decimal::ONE) / rust_decimal_macros::dec!(10000)));
    let locked_profit_floor = position.average_entry_price
        * (rust_decimal::Decimal::ONE
            + (min_profit_pct
                * if conviction >= dec!(0.70) {
                    dec!(0.60)
                } else {
                    dec!(0.40)
                }));
    best_bid <= ratchet_floor.max(locked_profit_floor)
}

fn evaluate_time_exit(
    position: &models::PortfolioPosition,
    now: DateTime<Utc>,
    best_bid: rust_decimal::Decimal,
    max_hold_time_seconds: i64,
    source_exit_pending: bool,
) -> TimeExitEvaluation {
    if !position.should_force_exit(now, max_hold_time_seconds) {
        return TimeExitEvaluation::None;
    }
    if source_exit_pending {
        return TimeExitEvaluation::Suppress("source_exit_pending");
    }

    let conviction = position_conviction_score(position);
    let reference_price = position
        .current_price
        .max(position.average_entry_price)
        .max(rust_decimal::Decimal::ZERO);
    let movement_ratio = if reference_price > rust_decimal::Decimal::ZERO {
        ((best_bid - position.current_price).abs() / reference_price).abs()
    } else {
        rust_decimal::Decimal::ZERO
    };
    let stagnant = movement_ratio <= TIME_EXIT_STAGNATION_BPS;
    let favorable_trend = best_bid
        >= position.current_price * (rust_decimal::Decimal::ONE + TIME_EXIT_FAVORABLE_TREND_BPS)
        || position.unrealized_pnl > TIME_EXIT_SMALL_PNL_ABS;
    let mild_loss = position.cost_basis > rust_decimal::Decimal::ZERO
        && ((-position.unrealized_pnl) / position.cost_basis) <= TIME_EXIT_MILD_LOSS_BPS
        && position.unrealized_pnl < rust_decimal::Decimal::ZERO;
    let strong_reversal = best_bid
        <= position.current_price * (rust_decimal::Decimal::ONE - TIME_EXIT_FAVORABLE_TREND_BPS);
    let hold_extension_multiplier = if conviction >= dec!(0.85) {
        3
    } else if conviction >= dec!(0.70) {
        2
    } else {
        1
    };
    let extended_hold = position.age(now).is_some_and(|age| {
        age >= TimeDelta::seconds(
            max_hold_time_seconds
                .saturating_mul(TIME_EXIT_EXTENSION_MULTIPLIER.max(hold_extension_multiplier)),
        )
    });

    if conviction < dec!(0.60) && (position.unrealized_pnl < Decimal::ZERO || strong_reversal) {
        return TimeExitEvaluation::Trigger;
    }
    if mild_loss && conviction >= dec!(0.60) && !strong_reversal && !extended_hold {
        return TimeExitEvaluation::Suppress("mild_loss_extension");
    }
    if favorable_trend && conviction >= dec!(0.65) && !extended_hold {
        return TimeExitEvaluation::Suppress("favorable_trend");
    }
    if stagnant || position.unrealized_pnl <= TIME_EXIT_SMALL_PNL_ABS || extended_hold {
        return TimeExitEvaluation::Trigger;
    }

    TimeExitEvaluation::Suppress("still_moving")
}

fn managed_exit_reason(
    settings: &Settings,
    position: &models::PortfolioPosition,
    now: DateTime<Utc>,
    best_bid: rust_decimal::Decimal,
    max_hold_time_seconds: i64,
    source_exit_pending: bool,
) -> ManagedExitEvaluation {
    if position.average_entry_price > rust_decimal::Decimal::ZERO {
        let conviction = position_conviction_score(position);
        if profit_protection_triggered(settings, position, best_bid) {
            return ManagedExitEvaluation::Trigger(ManagedExitReason::ProfitProtection);
        }
        let take_profit_multiple = if conviction >= dec!(0.85) {
            rust_decimal::Decimal::new(122, 2)
        } else if conviction >= dec!(0.70) {
            rust_decimal::Decimal::new(118, 2)
        } else {
            rust_decimal::Decimal::new(112, 2)
        };
        if !source_exit_pending && best_bid >= position.average_entry_price * take_profit_multiple {
            return ManagedExitEvaluation::Trigger(ManagedExitReason::TakeProfit);
        }
        let catastrophic_stop =
            best_bid <= position.average_entry_price * rust_decimal::Decimal::new(80, 2);
        let stop_loss_multiple = if conviction >= dec!(0.85) {
            rust_decimal::Decimal::new(88, 2)
        } else if conviction >= dec!(0.70) {
            rust_decimal::Decimal::new(90, 2)
        } else {
            rust_decimal::Decimal::new(94, 2)
        };
        if best_bid <= position.average_entry_price * stop_loss_multiple
            && (!source_exit_pending || catastrophic_stop)
        {
            return ManagedExitEvaluation::Trigger(ManagedExitReason::StopLoss);
        }
    }

    match evaluate_time_exit(
        position,
        now,
        best_bid,
        max_hold_time_seconds,
        source_exit_pending,
    ) {
        TimeExitEvaluation::Trigger => ManagedExitEvaluation::Trigger(ManagedExitReason::TimeExit),
        TimeExitEvaluation::Suppress(reason) => ManagedExitEvaluation::SuppressTimeExit(reason),
        TimeExitEvaluation::None => ManagedExitEvaluation::None,
    }
}

fn position_conviction_score(position: &models::PortfolioPosition) -> Decimal {
    if position.entry_conviction_score > Decimal::ZERO {
        return position
            .entry_conviction_score
            .max(dec!(0.30))
            .min(Decimal::ONE);
    }

    match models::classify_market(&position.title) {
        models::MarketType::UltraShort => dec!(0.50),
        models::MarketType::Short => dec!(0.65),
        models::MarketType::Medium => dec!(0.75),
    }
}

fn effective_time_exit_seconds(configured_seconds: u64) -> i64 {
    let configured_seconds = configured_seconds as i64;
    if configured_seconds <= 0 {
        TIME_EXIT_MAX_HOLD_SECONDS
    } else {
        configured_seconds.min(TIME_EXIT_MAX_HOLD_SECONDS)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ClosingActionPlan {
    Retry {
        mode: ExecutionMode,
        escalation_level: u8,
    },
    Fail {
        reason: &'static str,
    },
}

fn managed_exit_event_name(reason: ManagedExitReason) -> Option<&'static str> {
    match reason {
        ManagedExitReason::TakeProfit => Some("managed_exit_take_profit"),
        ManagedExitReason::ProfitProtection => Some("managed_exit_profit_protection"),
        ManagedExitReason::StopLoss => Some("managed_exit_stop_loss"),
        ManagedExitReason::TimeExit => Some("time_exit_triggered"),
        ManagedExitReason::SourceExit | ManagedExitReason::HardStop => None,
    }
}

fn closing_reason_from_position(position: &models::PortfolioPosition) -> ManagedExitReason {
    match position.closing_reason.as_deref().unwrap_or("SOURCE_EXIT") {
        "TAKE_PROFIT" => ManagedExitReason::TakeProfit,
        "PROFIT_PROTECTION" => ManagedExitReason::ProfitProtection,
        "STOP_LOSS" => ManagedExitReason::StopLoss,
        "TIME_EXIT" => ManagedExitReason::TimeExit,
        "HARD_STOP" => ManagedExitReason::HardStop,
        _ => ManagedExitReason::SourceExit,
    }
}

fn closing_action_plan(
    position: &models::PortfolioPosition,
    settings: &Settings,
    now: DateTime<Utc>,
) -> Option<ClosingActionPlan> {
    if position.state != models::PositionState::Closing {
        return None;
    }

    let retry_interval =
        TimeDelta::from_std(settings.exit_retry_interval).unwrap_or_else(|_| TimeDelta::zero());
    if !position.close_retry_due(now, retry_interval) {
        return None;
    }

    let closing_age = position.closing_age(now).unwrap_or_else(TimeDelta::zero);
    let closing_max_age =
        TimeDelta::from_std(settings.closing_max_age).unwrap_or_else(|_| TimeDelta::zero());
    if closing_age < closing_max_age {
        return Some(ClosingActionPlan::Retry {
            mode: ExecutionMode::MandatoryExit {
                reason: closing_reason_from_position(position),
            },
            escalation_level: 0,
        });
    }

    if !settings.force_exit_on_closing_timeout {
        return Some(ClosingActionPlan::Fail {
            reason: "closing_timeout_force_exit_disabled",
        });
    }

    let closing_retry_window =
        TimeDelta::from_std(settings.exit_retry_window).unwrap_or_else(|_| TimeDelta::zero());
    if closing_retry_window > TimeDelta::zero()
        && closing_age >= closing_max_age + closing_retry_window
    {
        return Some(ClosingActionPlan::Fail {
            reason: "closing_timeout_exhausted",
        });
    }

    let overdue = closing_age - closing_max_age;
    let stage_three_after =
        TimeDelta::from_std(EMERGENCY_EXIT_STAGE_THREE_AFTER).unwrap_or_else(|_| TimeDelta::zero());
    let stage_two_after =
        TimeDelta::from_std(EMERGENCY_EXIT_STAGE_TWO_AFTER).unwrap_or_else(|_| TimeDelta::zero());
    let stage = if overdue >= stage_three_after {
        3
    } else if overdue >= stage_two_after {
        2
    } else {
        1
    };

    Some(ClosingActionPlan::Retry {
        mode: ExecutionMode::EmergencyExit { stage },
        escalation_level: stage,
    })
}

async fn persist_closing_escalation(
    portfolio: &PortfolioService,
    analytics: &ExecutionAnalyticsTracker,
    position_key: &PositionKey,
    escalation_level: u8,
) -> Result<()> {
    let mut snapshot = load_current_portfolio_snapshot(portfolio).await?;
    let Some(previous_level) =
        snapshot.set_closing_escalation_level(position_key, escalation_level)
    else {
        return Ok(());
    };
    if escalation_level <= previous_level {
        return Ok(());
    }

    if previous_level < 1 && escalation_level >= 1 {
        analytics
            .record_exit_event("close_escalated_mandatory", Some(&snapshot))
            .await;
    }
    if previous_level < 2 && escalation_level >= 2 {
        analytics
            .record_exit_event("close_escalated_emergency", Some(&snapshot))
            .await;
    }

    portfolio.store_snapshot(snapshot).await?;
    Ok(())
}

async fn mark_failed_closing_position_stale(
    portfolio: &PortfolioService,
    closing_positions: &ClosingPositions,
    position: &models::PortfolioPosition,
    position_resolver: &PositionResolver,
    failure_reason: &str,
    analytics: Arc<ExecutionAnalyticsTracker>,
) -> Result<()> {
    let position_key = position.position_key();
    release_closing_position(closing_positions, Some(&position_key)).await;
    let mut snapshot = load_current_portfolio_snapshot(portfolio).await?;
    snapshot.note_close_failure(&position_key, failure_reason);
    if !snapshot.mark_position_stale(&position_key, "stale_after_failed_exit") {
        return Ok(());
    }
    analytics
        .record_close_failure_reason(failure_reason, Some(&snapshot))
        .await;
    analytics.sync_with_portfolio(&snapshot).await;
    let _ = position_resolver.remove_position(&position_key);
    portfolio.store_snapshot(snapshot).await?;
    Ok(())
}

async fn load_prediction_seed_entries(settings: &Settings) -> Result<Vec<ActivityEntry>> {
    let path = settings.data_dir.join("attribution-events.jsonl");
    let file = match tokio::fs::File::open(&path).await {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => {
            return Err(anyhow!(
                "opening prediction seed history {}: {error}",
                path.display()
            ));
        }
    };

    let mut lines = BufReader::new(file).lines();
    let mut entries = VecDeque::with_capacity(PREDICTION_HISTORY_SEED_LIMIT);
    while let Some(line) = lines.next_line().await? {
        if line.trim().is_empty() {
            continue;
        }

        let Ok(event) = serde_json::from_str::<PredictionSeedEvent>(&line) else {
            continue;
        };
        if event.event_type != "wallet_confirmation_forwarded" {
            continue;
        }

        let (
            Some(proxy_wallet),
            Some(asset_id),
            Some(condition_id),
            Some(side),
            Some(transaction_hash),
            Some(price),
            Some(size),
        ) = (
            event.proxy_wallet,
            event.asset_id,
            event.condition_id,
            event.side,
            event.transaction_hash,
            event.price,
            event.size,
        )
        else {
            continue;
        };

        entries.push_back(ActivityEntry {
            proxy_wallet,
            timestamp: event.logged_at.timestamp_millis(),
            condition_id,
            type_name: "TRADE".to_owned(),
            size,
            usdc_size: round_seed_notional(size, price),
            transaction_hash,
            price,
            asset: asset_id,
            side,
            outcome_index: 0,
            title: "seeded-confirmation".to_owned(),
            slug: "seeded-confirmation".to_owned(),
            event_slug: "seeded-confirmation".to_owned(),
            outcome: "UNKNOWN".to_owned(),
        });

        while entries.len() > PREDICTION_HISTORY_SEED_LIMIT {
            entries.pop_front();
        }
    }

    Ok(entries.into_iter().collect())
}

fn round_seed_notional(size: f64, price: f64) -> f64 {
    ((size * price) * 1_000_000.0).round() / 1_000_000.0
}

#[derive(Deserialize)]
struct PredictionSeedEvent {
    event_type: String,
    logged_at: DateTime<Utc>,
    proxy_wallet: Option<String>,
    asset_id: Option<String>,
    condition_id: Option<String>,
    side: Option<String>,
    transaction_hash: Option<String>,
    price: Option<f64>,
    size: Option<f64>,
}

#[cfg_attr(not(test), allow(dead_code))]
fn build_execution_request(
    entry: &ActivityEntry,
    decision: &CopyDecision,
    quote: &models::BestQuote,
) -> std::result::Result<ExecutionRequest, SkipReason> {
    build_execution_request_with_mode(entry, decision, quote, ExecutionMode::Normal)
}

fn build_execution_request_with_mode(
    entry: &ActivityEntry,
    decision: &CopyDecision,
    quote: &models::BestQuote,
    mode: ExecutionMode,
) -> std::result::Result<ExecutionRequest, SkipReason> {
    let source_price = match entry.price_decimal() {
        Ok(price) if !price.is_zero() => price,
        Ok(_) => {
            return Err(SkipReason::new(
                "source_price_zero",
                "source trade price is zero",
            ));
        }
        Err(error) => {
            return Err(SkipReason::new(
                "invalid_source_price",
                format!("failed to parse source price: {error}"),
            ));
        }
    };
    let guardrails = execution_guardrails(mode, decision);

    match decision.side {
        ExecutionSide::Buy => {
            let Some(best_ask) = quote.best_ask else {
                return Err(SkipReason::new(
                    "missing_best_ask",
                    format!("no best ask available for asset {}", decision.token_id),
                ));
            };
            if best_ask.is_zero() {
                return Err(SkipReason::new(
                    "best_ask_zero",
                    format!("best ask is zero for asset {}", decision.token_id),
                ));
            }
            if let Some(max_market_spread_bps) = guardrails.max_market_spread_bps {
                enforce_market_spread(max_market_spread_bps, quote, &decision.token_id)?;
            }
            let effective_slippage_bps = match mode {
                ExecutionMode::Normal => guardrails.max_slippage_bps,
                ExecutionMode::MandatoryExit { .. } => guardrails.max_slippage_bps,
                ExecutionMode::EmergencyExit { .. } => {
                    slippage_bps_for_mode(best_ask, guardrails.max_slippage_bps, mode)
                }
            };
            let mut limit_price =
                risk::bounded_buy_price(best_ask, effective_slippage_bps, quote.tick_size);
            if guardrails.enforce_source_price_slippage {
                let source_price_cap = risk::max_buy_price_from_source(
                    source_price,
                    decision.max_source_price_slippage_bps,
                    quote.tick_size,
                );
                if best_ask > source_price_cap {
                    return Err(SkipReason::new(
                        "buy_source_slippage_exceeded",
                        format!(
                            "best ask {} is above source price cap {}",
                            best_ask.round_dp(4),
                            source_price_cap.round_dp(4)
                        ),
                    ));
                }
                limit_price = limit_price.min(source_price_cap);
                if limit_price < best_ask {
                    return Err(SkipReason::new(
                        "buy_limit_below_best_ask",
                        format!(
                            "computed limit price {} fell below best ask {}",
                            limit_price.round_dp(4),
                            best_ask.round_dp(4)
                        ),
                    ));
                }
            }
            let size = (decision.notional / limit_price)
                .round_dp_with_strategy(6, rust_decimal::RoundingStrategy::ToZero);
            if size < quote.min_order_size {
                return Err(SkipReason::new(
                    "buy_size_below_min_order",
                    format!(
                        "computed buy size {} is below min order size {}",
                        size.round_dp(6),
                        quote.min_order_size.round_dp(6)
                    ),
                ));
            }
            enforce_top_of_book_depth(
                ExecutionSide::Buy,
                size,
                guardrails.min_top_of_book_ratio,
                quote,
                &decision.token_id,
            )?;
            enforce_visible_liquidity(
                ExecutionSide::Buy,
                best_ask,
                quote.best_ask_size,
                guardrails.min_visible_liquidity_usd,
                &decision.token_id,
            )?;
            if guardrails.enforce_remaining_edge {
                enforce_remaining_edge(
                    source_price,
                    limit_price,
                    guardrails.min_edge_threshold,
                    &decision.token_id,
                )?;
            }
            Ok(ExecutionRequest {
                token_id: decision.token_id.clone(),
                side: ExecutionSide::Buy,
                size,
                limit_price,
                requested_notional: decision.notional,
                source_trade_id: entry.dedupe_key(),
            })
        }
        ExecutionSide::Sell => {
            let Some(best_bid) = quote.best_bid else {
                return Err(SkipReason::new(
                    "missing_best_bid",
                    format!("no best bid available for asset {}", decision.token_id),
                ));
            };
            if best_bid.is_zero() {
                return Err(SkipReason::new(
                    "best_bid_zero",
                    format!("best bid is zero for asset {}", decision.token_id),
                ));
            }
            if let Some(max_market_spread_bps) = guardrails.max_market_spread_bps {
                enforce_market_spread(max_market_spread_bps, quote, &decision.token_id)?;
            }
            let effective_slippage_bps =
                slippage_bps_for_mode(best_bid, guardrails.max_slippage_bps, mode);
            if decision.size < quote.min_order_size {
                return Err(SkipReason::new(
                    "sell_size_below_min_order",
                    format!(
                        "sell size {} is below min order size {}",
                        decision.size.round_dp(6),
                        quote.min_order_size.round_dp(6)
                    ),
                ));
            }
            enforce_top_of_book_depth(
                ExecutionSide::Sell,
                decision.size,
                guardrails.min_top_of_book_ratio,
                quote,
                &decision.token_id,
            )?;
            enforce_visible_liquidity(
                ExecutionSide::Sell,
                best_bid,
                quote.best_bid_size,
                guardrails.min_visible_liquidity_usd,
                &decision.token_id,
            )?;
            let mut limit_price = match mode {
                ExecutionMode::MandatoryExit { .. } => {
                    mandatory_exit_limit_price(best_bid, quote.tick_size)
                }
                _ => risk::bounded_sell_price(best_bid, effective_slippage_bps, quote.tick_size),
            };
            if guardrails.enforce_source_price_slippage {
                let source_price_floor = risk::min_sell_price_from_source(
                    source_price,
                    decision.max_source_price_slippage_bps,
                    quote.tick_size,
                );
                if best_bid < source_price_floor {
                    return Err(SkipReason::new(
                        "sell_source_slippage_exceeded",
                        format!(
                            "best bid {} is below source price floor {}",
                            best_bid.round_dp(4),
                            source_price_floor.round_dp(4)
                        ),
                    ));
                }
                limit_price = limit_price.max(source_price_floor);
            }
            Ok(ExecutionRequest {
                token_id: decision.token_id.clone(),
                side: ExecutionSide::Sell,
                size: decision.size,
                limit_price,
                requested_notional: decision.notional,
                source_trade_id: entry.dedupe_key(),
            })
        }
    }
}

fn execution_guardrails(mode: ExecutionMode, decision: &CopyDecision) -> ExecutionGuardrails {
    match mode {
        ExecutionMode::Normal => ExecutionGuardrails {
            max_market_spread_bps: Some(decision.max_market_spread_bps),
            min_top_of_book_ratio: decision.min_top_of_book_ratio,
            max_slippage_bps: decision.max_slippage_bps,
            enforce_source_price_slippage: true,
            enforce_remaining_edge: true,
            min_visible_liquidity_usd: decision.min_visible_liquidity_usd,
            min_edge_threshold: decision.min_edge_threshold,
        },
        ExecutionMode::MandatoryExit { .. } => ExecutionGuardrails {
            max_market_spread_bps: None,
            min_top_of_book_ratio: rust_decimal::Decimal::ZERO,
            max_slippage_bps: 50,
            enforce_source_price_slippage: false,
            enforce_remaining_edge: false,
            min_visible_liquidity_usd: decision.min_visible_liquidity_usd,
            min_edge_threshold: rust_decimal::Decimal::ZERO,
        },
        ExecutionMode::EmergencyExit { stage } => match stage {
            0 | 1 => ExecutionGuardrails {
                max_market_spread_bps: Some(scale_bps_threshold(
                    decision.max_market_spread_bps,
                    3,
                    2,
                )),
                min_top_of_book_ratio: decision.min_top_of_book_ratio,
                max_slippage_bps: decision.max_slippage_bps,
                enforce_source_price_slippage: true,
                enforce_remaining_edge: false,
                min_visible_liquidity_usd: decision.min_visible_liquidity_usd,
                min_edge_threshold: rust_decimal::Decimal::ZERO,
            },
            2 => ExecutionGuardrails {
                max_market_spread_bps: Some(scale_bps_threshold(
                    decision.max_market_spread_bps,
                    3,
                    1,
                )),
                min_top_of_book_ratio: decision.min_top_of_book_ratio
                    / rust_decimal::Decimal::new(2, 0),
                max_slippage_bps: decision.max_slippage_bps.max(500),
                enforce_source_price_slippage: true,
                enforce_remaining_edge: false,
                min_visible_liquidity_usd: decision.min_visible_liquidity_usd
                    / rust_decimal::Decimal::new(2, 0),
                min_edge_threshold: rust_decimal::Decimal::ZERO,
            },
            _ => ExecutionGuardrails {
                max_market_spread_bps: None,
                min_top_of_book_ratio: rust_decimal::Decimal::ZERO,
                max_slippage_bps: decision.max_slippage_bps.max(1_000),
                enforce_source_price_slippage: false,
                enforce_remaining_edge: false,
                min_visible_liquidity_usd: rust_decimal::Decimal::ZERO,
                min_edge_threshold: rust_decimal::Decimal::ZERO,
            },
        },
    }
}

fn scale_bps_threshold(value: u32, numerator: u32, denominator: u32) -> u32 {
    if value == 0 {
        return 0;
    }
    (value as u64 * numerator as u64).div_ceil(denominator as u64) as u32
}

fn slippage_bps_for_mode(
    reference_price: rust_decimal::Decimal,
    configured_bps: u32,
    mode: ExecutionMode,
) -> u32 {
    let dynamic_bps = dynamic_slippage_bps(reference_price);
    match mode {
        ExecutionMode::Normal => configured_bps.min(dynamic_bps),
        ExecutionMode::MandatoryExit { .. } => configured_bps,
        ExecutionMode::EmergencyExit { .. } => configured_bps.max(dynamic_bps),
    }
}

fn dynamic_slippage_bps(price: rust_decimal::Decimal) -> u32 {
    if price > rust_decimal::Decimal::new(985, 3) {
        10
    } else if price > rust_decimal::Decimal::new(95, 2) {
        25
    } else if price > rust_decimal::Decimal::new(80, 2) {
        75
    } else {
        150
    }
}

fn mandatory_exit_limit_price(
    best_bid: rust_decimal::Decimal,
    tick_size: rust_decimal::Decimal,
) -> rust_decimal::Decimal {
    risk::bounded_sell_price(best_bid, 50, tick_size)
}

fn enforce_remaining_edge(
    source_price: rust_decimal::Decimal,
    entry_price: rust_decimal::Decimal,
    min_edge_threshold: rust_decimal::Decimal,
    token_id: &str,
) -> std::result::Result<(), SkipReason> {
    let remaining_edge =
        (rust_decimal::Decimal::ONE - entry_price).max(rust_decimal::Decimal::ZERO);
    if remaining_edge <= min_edge_threshold {
        info!(
            reason_code = "edge_rejected",
            token_id,
            source_price = %source_price.round_dp(4),
            current_price = %entry_price.round_dp(4),
            remaining_edge = %remaining_edge.round_dp(4),
            min_edge_threshold = %min_edge_threshold.round_dp(4),
            "edge_rejected"
        );
        return Err(SkipReason::new(
            "edge_rejected",
            format!(
                "entry price {} leaves remaining edge {} below minimum {} for asset {}",
                entry_price.round_dp(4),
                remaining_edge.round_dp(4),
                min_edge_threshold.round_dp(4),
                token_id
            ),
        ));
    }
    Ok(())
}

fn enforce_visible_liquidity(
    side: ExecutionSide,
    best_price: rust_decimal::Decimal,
    best_size: Option<rust_decimal::Decimal>,
    min_visible_liquidity_usd: rust_decimal::Decimal,
    token_id: &str,
) -> std::result::Result<(), SkipReason> {
    if min_visible_liquidity_usd <= rust_decimal::Decimal::ZERO {
        return Ok(());
    }

    let available_size = best_size.unwrap_or(rust_decimal::Decimal::ZERO);
    let visible_liquidity = (best_price * available_size).max(rust_decimal::Decimal::ZERO);
    if visible_liquidity < min_visible_liquidity_usd {
        info!(
            reason_code = "liquidity_rejected",
            side = signal_side_label(side),
            token_id,
            best_price = %best_price.round_dp(4),
            visible_liquidity = %visible_liquidity.round_dp(4),
            min_visible_liquidity_usd = %min_visible_liquidity_usd.round_dp(4),
            "liquidity_rejected"
        );
        return Err(SkipReason::new(
            "low_visible_liquidity",
            format!(
                "top-of-book {} liquidity {} is below minimum {} for asset {}",
                signal_side_label(side).to_ascii_lowercase(),
                visible_liquidity.round_dp(4),
                min_visible_liquidity_usd.round_dp(4),
                token_id
            ),
        ));
    }

    Ok(())
}

fn enforce_market_spread(
    max_market_spread_bps: u32,
    quote: &models::BestQuote,
    token_id: &str,
) -> std::result::Result<(), SkipReason> {
    if max_market_spread_bps == 0 {
        return Ok(());
    }

    let Some(best_bid) = quote.best_bid else {
        return Err(SkipReason::new(
            "missing_best_bid_for_spread",
            format!("missing best bid needed for spread filter on asset {token_id}"),
        ));
    };
    let Some(best_ask) = quote.best_ask else {
        return Err(SkipReason::new(
            "missing_best_ask_for_spread",
            format!("missing best ask needed for spread filter on asset {token_id}"),
        ));
    };
    let Some(spread_bps) = risk::market_spread_bps(best_bid, best_ask) else {
        return Err(SkipReason::new(
            "invalid_market_spread",
            format!(
                "could not compute spread from best bid {} and best ask {} for asset {}",
                best_bid.round_dp(4),
                best_ask.round_dp(4),
                token_id
            ),
        ));
    };
    if spread_bps > rust_decimal::Decimal::from(max_market_spread_bps) {
        info!(
            reason_code = "liquidity_rejected",
            token_id,
            spread_bps = %spread_bps.round_dp(2),
            max_market_spread_bps,
            "liquidity_rejected"
        );
        return Err(SkipReason::new(
            "market_spread_exceeded",
            format!(
                "market spread {} bps is above max configured {} bps",
                spread_bps.round_dp(2),
                max_market_spread_bps
            ),
        ));
    }

    Ok(())
}

fn enforce_top_of_book_depth(
    side: ExecutionSide,
    intended_size: rust_decimal::Decimal,
    min_top_of_book_ratio: rust_decimal::Decimal,
    quote: &models::BestQuote,
    token_id: &str,
) -> std::result::Result<(), SkipReason> {
    if min_top_of_book_ratio <= rust_decimal::Decimal::ZERO {
        return Ok(());
    }

    let (available_size, reason_code) = match side {
        ExecutionSide::Buy => (quote.best_ask_size, "missing_best_ask_size"),
        ExecutionSide::Sell => (quote.best_bid_size, "missing_best_bid_size"),
    };
    let Some(available_size) = available_size else {
        return Err(SkipReason::new(
            reason_code,
            format!(
                "missing top-of-book size needed for liquidity filter on asset {}",
                token_id
            ),
        ));
    };

    let required_size = intended_size * min_top_of_book_ratio;
    if available_size < required_size {
        let code = match side {
            ExecutionSide::Buy => "buy_top_of_book_too_thin",
            ExecutionSide::Sell => "sell_top_of_book_too_thin",
        };
        return Err(SkipReason::new(
            code,
            format!(
                "top-of-book size {} is below required size {} using ratio {}",
                available_size.round_dp(6),
                required_size.round_dp(6),
                min_top_of_book_ratio.round_dp(4)
            ),
        ));
    }

    Ok(())
}

fn log_skipped_trade(
    enabled: bool,
    entry: &ActivityEntry,
    signal: &ConfirmedTradeSignal,
    skip_processing_ms: u64,
    class: SourceTradeClass,
    reason: &SkipReason,
) {
    if !enabled {
        return;
    }

    info!(
        trade_class = class.as_str(),
        reason_code = reason.code,
        reason = %reason.detail,
        side = %entry.side,
        asset = %entry.asset,
        condition_id = %entry.condition_id,
        source_price = entry.price,
        source_usdc = entry.usdc_size,
        detection_to_skip_ms = skip_processing_ms,
        websocket_to_skip_ms = signal.stage_timestamps.websocket_event_received_at.elapsed().as_millis() as u64,
        tx_hash = %entry.transaction_hash,
        "skipping target trade"
    );
}

fn execution_signal_key(signal: &ConfirmedTradeSignal) -> String {
    signal_cache_key(signal)
}

fn signal_side_label(side: ExecutionSide) -> &'static str {
    match side {
        ExecutionSide::Buy => "BUY",
        ExecutionSide::Sell => "SELL",
    }
}

#[derive(Default)]
struct ExecutionSignalDeduper {
    seen: HashSet<String>,
    order: VecDeque<String>,
}

impl ExecutionSignalDeduper {
    fn claim(&mut self, signal: &ConfirmedTradeSignal) -> bool {
        let key = execution_signal_key(signal);
        if !self.seen.insert(key.clone()) {
            return false;
        }
        self.order.push_front(key);
        while self.order.len() > MAX_EXECUTION_SIGNAL_KEYS {
            if let Some(expired) = self.order.pop_back() {
                self.seen.remove(&expired);
            }
        }
        true
    }

    fn release(&mut self, signal: &ConfirmedTradeSignal) {
        self.seen.remove(&execution_signal_key(signal));
    }
}

#[derive(Default)]
struct MatchedTradeDeduper {
    seen: HashSet<String>,
    order: VecDeque<String>,
}

impl MatchedTradeDeduper {
    fn claim(&mut self, matched_trade: &MatchedTrackedTrade) -> bool {
        let key = matched_trade.entry.dedupe_key();
        if !self.seen.insert(key.clone()) {
            return false;
        }
        self.order.push_front(key);
        while self.order.len() > MAX_EXECUTION_SIGNAL_KEYS {
            if let Some(expired) = self.order.pop_back() {
                self.seen.remove(&expired);
            }
        }
        true
    }
}

fn install_rustls_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

fn init_tracing() {
    let filter = std::env::var("RUST_LOG").unwrap_or_else(|_| "info".to_owned());
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .compact()
        .init();
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use rust_decimal_macros::dec;

    use super::*;

    fn sample_settings(data_dir: std::path::PathBuf) -> Settings {
        let mut settings = Settings::default_for_tests(data_dir);
        settings.polymarket_host = "https://example.com".to_owned();
        settings.polymarket_data_api = "https://example.com".to_owned();
        settings.polymarket_gamma_api = "https://example.com".to_owned();
        settings.polymarket_market_ws = "wss://example.com".to_owned();
        settings.polymarket_user_ws = "wss://example.com".to_owned();
        settings.polymarket_activity_ws = "wss://example.com".to_owned();
        settings.target_profile_addresses = vec!["0xsource".to_owned()];
        settings.market_subscription_batch_size = 1;
        settings.market_subscription_delay = Duration::from_millis(1);
        settings.wallet_subscription_batch_size = 1;
        settings.wallet_subscription_delay = Duration::from_millis(1);
        settings.hot_path_queue_capacity = 32;
        settings.cold_path_queue_capacity = 128;
        settings.attribution_fast_cache_capacity = 64;
        settings.min_top_of_book_ratio = dec!(1);
        settings.prediction_validation_timeout = Duration::from_millis(500);
        settings
    }

    fn sample_closing_position(
        started_ago: chrono::TimeDelta,
        last_attempt_ago: chrono::TimeDelta,
    ) -> models::PortfolioPosition {
        models::PortfolioPosition {
            asset: "asset-1".to_owned(),
            condition_id: "condition-1".to_owned(),
            title: "Sample market".to_owned(),
            outcome: "YES".to_owned(),
            source_wallet: "0xsource".to_owned(),
            state: models::PositionState::Closing,
            size: dec!(10),
            current_value: dec!(5),
            source_entry_price: dec!(0.5),
            average_entry_price: dec!(0.5),
            entry_conviction_score: Decimal::ZERO,
            peak_price_since_open: dec!(0.5),
            current_price: dec!(0.5),
            cost_basis: dec!(5),
            unrealized_pnl: dec!(0),
            opened_at: Some(Utc::now() - chrono::TimeDelta::minutes(20)),
            source_trade_timestamp_unix: Utc::now().timestamp_millis(),
            closing_started_at: Some(Utc::now() - started_ago),
            closing_reason: Some("SOURCE_EXIT".to_owned()),
            last_close_attempt_at: Some(Utc::now() - last_attempt_ago),
            close_attempts: 1,
            close_failure_reason: Some("close_nofill".to_owned()),
            closing_escalation_level: 0,
            stale_reason: None,
        }
    }

    fn sample_entry(side: &str, price: f64) -> ActivityEntry {
        ActivityEntry {
            proxy_wallet: "0xsource".to_owned(),
            timestamp: Utc::now().timestamp(),
            condition_id: "condition-1".to_owned(),
            type_name: "TRADE".to_owned(),
            size: 10.0,
            usdc_size: 5.0,
            transaction_hash: "0xhash".to_owned(),
            price,
            asset: "asset-1".to_owned(),
            side: side.to_owned(),
            outcome_index: 0,
            title: "Market".to_owned(),
            slug: "market".to_owned(),
            event_slug: "event".to_owned(),
            outcome: "Yes".to_owned(),
        }
    }

    fn sample_quote(
        best_bid: Option<rust_decimal::Decimal>,
        best_ask: Option<rust_decimal::Decimal>,
    ) -> models::BestQuote {
        models::BestQuote {
            asset_id: "asset-1".to_owned(),
            best_bid,
            best_bid_size: Some(dec!(200)),
            best_ask,
            best_ask_size: Some(dec!(200)),
            tick_size: dec!(0.01),
            min_order_size: dec!(1),
            neg_risk: false,
        }
    }

    fn sample_signal(side: ExecutionSide) -> ConfirmedTradeSignal {
        let now = Utc::now();
        let instant = Instant::now();
        ConfirmedTradeSignal {
            asset_id: "asset-1".to_owned(),
            condition_id: "condition-1".to_owned(),
            transaction_hash: None,
            side,
            price: 0.5,
            estimated_size: 10.0,
            stage_timestamps: models::TradeStageTimestamps {
                websocket_event_received_at: instant,
                websocket_event_received_at_utc: now,
                parse_completed_at: instant,
                parse_completed_at_utc: now,
                detection_triggered_at: instant,
                detection_triggered_at_utc: now,
                attribution_completed_at: Some(instant),
                attribution_completed_at_utc: Some(now),
                fast_risk_completed_at: Some(instant),
                fast_risk_completed_at_utc: Some(now),
            },
            confirmed_at: now,
            generation: 1,
        }
    }

    fn sample_execution_success(side: ExecutionSide) -> ExecutionSuccess {
        ExecutionSuccess {
            mode: crate::config::ExecutionMode::Live,
            order_request: ExecutionRequest {
                token_id: "asset-1".to_owned(),
                side,
                size: dec!(4),
                limit_price: dec!(0.48),
                requested_notional: dec!(1.92),
                source_trade_id: "hedge:source".to_owned(),
            },
            order_id: "hedge-order-1".to_owned(),
            success: true,
            transaction_hashes: vec!["0xhedge".to_owned()],
            filled_price: dec!(0.48),
            filled_size: dec!(4),
            requested_size: dec!(4),
            requested_price: dec!(0.48),
            status: ExecutionStatus::Filled,
            filled_notional: dec!(1.92),
        }
    }

    fn sample_pending_prediction_execution() -> PendingPredictionExecution {
        PendingPredictionExecution {
            signal_key: "signal-key".to_owned(),
            signal: sample_signal(ExecutionSide::Buy),
            source_entry: sample_entry("BUY", 0.5),
            decision: CopyDecision {
                token_id: "asset-1".to_owned(),
                side: ExecutionSide::Buy,
                notional: dec!(2),
                size: dec!(4),
                size_was_scaled: false,
                position_key: None,
                price_band: risk::PriceBand::Mid,
                max_market_spread_bps: 300,
                min_top_of_book_ratio: dec!(1),
                min_visible_liquidity_usd: dec!(50),
                max_slippage_bps: 300,
                max_source_price_slippage_bps: 200,
                min_edge_threshold: dec!(0.05),
            },
            order_request: ExecutionRequest {
                token_id: "asset-1".to_owned(),
                side: ExecutionSide::Buy,
                size: dec!(4),
                limit_price: dec!(0.5),
                requested_notional: dec!(2),
                source_trade_id: "predicted-trade".to_owned(),
            },
            execution_result: sample_execution_success(ExecutionSide::Buy),
            predicted_wallet: "0xsource".to_owned(),
            confidence: 0.91,
            submitted_at: Instant::now(),
            submitted_at_utc: Utc::now(),
            validation_deadline: Instant::now() + Duration::from_secs(1),
        }
    }

    #[test]
    fn skips_buy_when_best_ask_exceeds_source_slippage_cap() {
        let entry = sample_entry("BUY", 0.50);
        let decision = CopyDecision {
            token_id: "asset-1".to_owned(),
            side: ExecutionSide::Buy,
            notional: dec!(5),
            size: dec!(0),
            size_was_scaled: false,
            position_key: None,
            price_band: risk::PriceBand::Mid,
            max_market_spread_bps: 0,
            min_top_of_book_ratio: dec!(0),
            min_visible_liquidity_usd: dec!(0),
            max_slippage_bps: 300,
            max_source_price_slippage_bps: 200,
            min_edge_threshold: dec!(0.05),
        };
        let quote = sample_quote(Some(dec!(0.51)), Some(dec!(0.52)));

        let request = build_execution_request(&entry, &decision, &quote);

        assert_eq!(
            request.expect_err("skip reason").code,
            "buy_source_slippage_exceeded"
        );
    }

    #[test]
    fn no_longer_uses_hard_entry_ceiling_for_buy() {
        let entry = sample_entry("BUY", 0.91);
        let decision = CopyDecision {
            token_id: "asset-1".to_owned(),
            side: ExecutionSide::Buy,
            notional: dec!(5),
            size: dec!(0),
            size_was_scaled: false,
            position_key: None,
            price_band: risk::PriceBand::High,
            max_market_spread_bps: 0,
            min_top_of_book_ratio: dec!(0),
            min_visible_liquidity_usd: dec!(0),
            max_slippage_bps: 300,
            max_source_price_slippage_bps: 10_000,
            min_edge_threshold: dec!(0.05),
        };
        let quote = sample_quote(Some(dec!(0.90)), Some(dec!(0.91)));

        let request = build_execution_request(&entry, &decision, &quote).expect("request");

        assert!(request.limit_price >= dec!(0.91));
    }

    #[test]
    fn skips_buy_when_remaining_edge_is_insufficient() {
        let entry = sample_entry("BUY", 0.97);
        let decision = CopyDecision {
            token_id: "asset-1".to_owned(),
            side: ExecutionSide::Buy,
            notional: dec!(5),
            size: dec!(0),
            size_was_scaled: false,
            position_key: None,
            price_band: risk::PriceBand::High,
            max_market_spread_bps: 0,
            min_top_of_book_ratio: dec!(0),
            min_visible_liquidity_usd: dec!(0),
            max_slippage_bps: 300,
            max_source_price_slippage_bps: 10_000,
            min_edge_threshold: dec!(0.05),
        };
        let quote = sample_quote(Some(dec!(0.96)), Some(dec!(0.97)));

        let request = build_execution_request(&entry, &decision, &quote);

        assert_eq!(request.expect_err("skip reason").code, "edge_rejected");
    }

    #[test]
    fn clamps_buy_limit_price_to_source_slippage_cap() {
        let entry = sample_entry("BUY", 0.50);
        let decision = CopyDecision {
            token_id: "asset-1".to_owned(),
            side: ExecutionSide::Buy,
            notional: dec!(5.10),
            size: dec!(0),
            size_was_scaled: false,
            position_key: None,
            price_band: risk::PriceBand::Mid,
            max_market_spread_bps: 0,
            min_top_of_book_ratio: dec!(0),
            min_visible_liquidity_usd: dec!(0),
            max_slippage_bps: 300,
            max_source_price_slippage_bps: 200,
            min_edge_threshold: dec!(0.05),
        };
        let quote = sample_quote(Some(dec!(0.50)), Some(dec!(0.51)));

        let request = build_execution_request(&entry, &decision, &quote).expect("request");

        assert_eq!(request.limit_price, dec!(0.51));
    }

    #[test]
    fn skips_sell_when_best_bid_drops_below_source_slippage_floor() {
        let entry = sample_entry("SELL", 0.50);
        let decision = CopyDecision {
            token_id: "asset-1".to_owned(),
            side: ExecutionSide::Sell,
            notional: dec!(5),
            size: dec!(10),
            size_was_scaled: false,
            position_key: None,
            price_band: risk::PriceBand::Mid,
            max_market_spread_bps: 0,
            min_top_of_book_ratio: dec!(0),
            min_visible_liquidity_usd: dec!(0),
            max_slippage_bps: 300,
            max_source_price_slippage_bps: 200,
            min_edge_threshold: dec!(0.05),
        };
        let quote = sample_quote(Some(dec!(0.47)), Some(dec!(0.48)));

        let request = build_execution_request(&entry, &decision, &quote);

        assert_eq!(
            request.expect_err("skip reason").code,
            "sell_source_slippage_exceeded"
        );
    }

    #[test]
    fn mandatory_exit_sell_bypasses_source_slippage_floor() {
        let entry = sample_entry("SELL", 0.50);
        let decision = CopyDecision {
            token_id: "asset-1".to_owned(),
            side: ExecutionSide::Sell,
            notional: dec!(5),
            size: dec!(10),
            size_was_scaled: false,
            position_key: None,
            price_band: risk::PriceBand::Mid,
            max_market_spread_bps: 300,
            min_top_of_book_ratio: dec!(1.5),
            min_visible_liquidity_usd: dec!(0),
            max_slippage_bps: 300,
            max_source_price_slippage_bps: 200,
            min_edge_threshold: dec!(0.05),
        };
        let quote = sample_quote(Some(dec!(0.47)), Some(dec!(0.60)));

        let request = build_execution_request_with_mode(
            &entry,
            &decision,
            &quote,
            ExecutionMode::MandatoryExit {
                reason: ManagedExitReason::SourceExit,
            },
        )
        .expect("mandatory exit request");

        assert_eq!(request.side, ExecutionSide::Sell);
        assert_eq!(request.limit_price, dec!(0.46));
    }

    #[test]
    fn skips_buy_when_market_spread_is_too_wide() {
        let entry = sample_entry("BUY", 0.50);
        let decision = CopyDecision {
            token_id: "asset-1".to_owned(),
            side: ExecutionSide::Buy,
            notional: dec!(5),
            size: dec!(0),
            size_was_scaled: false,
            position_key: None,
            price_band: risk::PriceBand::Mid,
            max_market_spread_bps: 300,
            min_top_of_book_ratio: dec!(0),
            min_visible_liquidity_usd: dec!(0),
            max_slippage_bps: 300,
            max_source_price_slippage_bps: 10000,
            min_edge_threshold: dec!(0.05),
        };
        let quote = sample_quote(Some(dec!(0.40)), Some(dec!(0.60)));

        let request = build_execution_request(&entry, &decision, &quote);

        assert_eq!(
            request.expect_err("skip reason").code,
            "market_spread_exceeded"
        );
    }

    #[test]
    fn skips_buy_when_top_of_book_is_too_thin() {
        let entry = sample_entry("BUY", 0.50);
        let decision = CopyDecision {
            token_id: "asset-1".to_owned(),
            side: ExecutionSide::Buy,
            notional: dec!(5),
            size: dec!(0),
            size_was_scaled: false,
            position_key: None,
            price_band: risk::PriceBand::Mid,
            max_market_spread_bps: 0,
            min_top_of_book_ratio: dec!(1.5),
            min_visible_liquidity_usd: dec!(0),
            max_slippage_bps: 300,
            max_source_price_slippage_bps: 10000,
            min_edge_threshold: dec!(0.05),
        };
        let mut quote = sample_quote(Some(dec!(0.50)), Some(dec!(0.50)));
        quote.best_ask_size = Some(dec!(10));

        let request = build_execution_request(&entry, &decision, &quote);

        assert_eq!(
            request.expect_err("skip reason").code,
            "buy_top_of_book_too_thin"
        );
    }

    #[test]
    fn emergency_exit_stage_two_relaxes_buy_top_of_book_requirement() {
        let entry = sample_entry("BUY", 0.50);
        let decision = CopyDecision {
            token_id: "asset-1".to_owned(),
            side: ExecutionSide::Buy,
            notional: dec!(5),
            size: dec!(0),
            size_was_scaled: false,
            position_key: None,
            price_band: risk::PriceBand::Mid,
            max_market_spread_bps: 0,
            min_top_of_book_ratio: dec!(1.5),
            min_visible_liquidity_usd: dec!(0),
            max_slippage_bps: 300,
            max_source_price_slippage_bps: 10000,
            min_edge_threshold: dec!(0.05),
        };
        let mut quote = sample_quote(Some(dec!(0.50)), Some(dec!(0.50)));
        quote.best_ask_size = Some(dec!(8));

        let normal =
            build_execution_request_with_mode(&entry, &decision, &quote, ExecutionMode::Normal);
        let stage_one = build_execution_request_with_mode(
            &entry,
            &decision,
            &quote,
            ExecutionMode::EmergencyExit { stage: 1 },
        );
        let stage_two = build_execution_request_with_mode(
            &entry,
            &decision,
            &quote,
            ExecutionMode::EmergencyExit { stage: 2 },
        )
        .expect("stage two request");
        let expected_limit_price = risk::bounded_buy_price(dec!(0.50), 500, dec!(0.01));
        let expected_size = (decision.notional / expected_limit_price)
            .round_dp_with_strategy(6, rust_decimal::RoundingStrategy::ToZero);

        assert_eq!(
            normal.expect_err("normal skip").code,
            "buy_top_of_book_too_thin"
        );
        assert_eq!(
            stage_one.expect_err("stage one skip").code,
            "buy_top_of_book_too_thin"
        );
        assert_eq!(stage_two.side, ExecutionSide::Buy);
        assert_eq!(stage_two.limit_price, expected_limit_price);
        assert_eq!(stage_two.size, expected_size);
    }

    #[test]
    fn emergency_exit_stage_three_bypasses_worst_case_buy_thin_liquidity() {
        let entry = sample_entry("BUY", 0.50);
        let decision = CopyDecision {
            token_id: "asset-1".to_owned(),
            side: ExecutionSide::Buy,
            notional: dec!(5),
            size: dec!(0),
            size_was_scaled: false,
            position_key: None,
            price_band: risk::PriceBand::Mid,
            max_market_spread_bps: 0,
            min_top_of_book_ratio: dec!(1.5),
            min_visible_liquidity_usd: dec!(0),
            max_slippage_bps: 300,
            max_source_price_slippage_bps: 10000,
            min_edge_threshold: dec!(0.05),
        };
        let mut quote = sample_quote(Some(dec!(0.50)), Some(dec!(0.50)));
        quote.best_ask_size = Some(dec!(1));

        let normal =
            build_execution_request_with_mode(&entry, &decision, &quote, ExecutionMode::Normal);
        let stage_one = build_execution_request_with_mode(
            &entry,
            &decision,
            &quote,
            ExecutionMode::EmergencyExit { stage: 1 },
        );
        let stage_two = build_execution_request_with_mode(
            &entry,
            &decision,
            &quote,
            ExecutionMode::EmergencyExit { stage: 2 },
        );
        let stage_three = build_execution_request_with_mode(
            &entry,
            &decision,
            &quote,
            ExecutionMode::EmergencyExit { stage: 3 },
        )
        .expect("stage three request");
        let expected_limit_price = risk::bounded_buy_price(dec!(0.50), 1000, dec!(0.01));
        let expected_size = (decision.notional / expected_limit_price)
            .round_dp_with_strategy(6, rust_decimal::RoundingStrategy::ToZero);

        assert_eq!(
            normal.expect_err("normal skip").code,
            "buy_top_of_book_too_thin"
        );
        assert_eq!(
            stage_one.expect_err("stage one skip").code,
            "buy_top_of_book_too_thin"
        );
        assert_eq!(
            stage_two.expect_err("stage two skip").code,
            "buy_top_of_book_too_thin"
        );
        assert_eq!(stage_three.side, ExecutionSide::Buy);
        assert_eq!(stage_three.limit_price, expected_limit_price);
        assert_eq!(stage_three.size, expected_size);
    }

    #[test]
    fn emergency_exit_stage_three_bypasses_worst_case_sell_thin_liquidity() {
        let entry = sample_entry("SELL", 0.50);
        let decision = CopyDecision {
            token_id: "asset-1".to_owned(),
            side: ExecutionSide::Sell,
            notional: dec!(5),
            size: dec!(10),
            size_was_scaled: false,
            position_key: None,
            price_band: risk::PriceBand::Mid,
            max_market_spread_bps: 0,
            min_top_of_book_ratio: dec!(1.5),
            min_visible_liquidity_usd: dec!(0),
            max_slippage_bps: 300,
            max_source_price_slippage_bps: 10000,
            min_edge_threshold: dec!(0.05),
        };
        let mut quote = sample_quote(Some(dec!(0.50)), Some(dec!(0.50)));
        quote.best_bid_size = Some(dec!(1));

        let normal =
            build_execution_request_with_mode(&entry, &decision, &quote, ExecutionMode::Normal);
        let stage_one = build_execution_request_with_mode(
            &entry,
            &decision,
            &quote,
            ExecutionMode::EmergencyExit { stage: 1 },
        );
        let stage_two = build_execution_request_with_mode(
            &entry,
            &decision,
            &quote,
            ExecutionMode::EmergencyExit { stage: 2 },
        );
        let stage_three = build_execution_request_with_mode(
            &entry,
            &decision,
            &quote,
            ExecutionMode::EmergencyExit { stage: 3 },
        )
        .expect("stage three request");
        let expected_limit_price = risk::bounded_sell_price(dec!(0.50), 1000, dec!(0.01));

        assert_eq!(
            normal.expect_err("normal skip").code,
            "sell_top_of_book_too_thin"
        );
        assert_eq!(
            stage_one.expect_err("stage one skip").code,
            "sell_top_of_book_too_thin"
        );
        assert_eq!(
            stage_two.expect_err("stage two skip").code,
            "sell_top_of_book_too_thin"
        );
        assert_eq!(stage_three.side, ExecutionSide::Sell);
        assert_eq!(stage_three.limit_price, expected_limit_price);
        assert_eq!(stage_three.size, dec!(10));
    }

    #[test]
    fn skips_when_signal_price_is_chased_beyond_bucket_limit() {
        let entry = sample_entry("BUY", 0.50);
        let decision = CopyDecision {
            token_id: "asset-1".to_owned(),
            side: ExecutionSide::Buy,
            notional: dec!(5),
            size: dec!(0),
            size_was_scaled: false,
            position_key: None,
            price_band: risk::PriceBand::Mid,
            max_market_spread_bps: 0,
            min_top_of_book_ratio: dec!(0),
            min_visible_liquidity_usd: dec!(0),
            max_slippage_bps: 300,
            max_source_price_slippage_bps: 300,
            min_edge_threshold: dec!(0.05),
        };
        let quote = sample_quote(Some(dec!(0.50)), Some(dec!(0.52)));

        let reason =
            enforce_signal_price_deviation(&entry, &decision, &quote).expect_err("skip reason");

        assert_eq!(reason.code, "price_chased");
    }

    #[test]
    fn high_price_sell_uses_tighter_normal_slippage() {
        let entry = sample_entry("SELL", 0.97);
        let decision = CopyDecision {
            token_id: "asset-1".to_owned(),
            side: ExecutionSide::Sell,
            notional: dec!(5),
            size: dec!(10),
            size_was_scaled: false,
            position_key: None,
            price_band: risk::PriceBand::High,
            max_market_spread_bps: 0,
            min_top_of_book_ratio: dec!(0),
            min_visible_liquidity_usd: dec!(0),
            max_slippage_bps: 300,
            max_source_price_slippage_bps: 10_000,
            min_edge_threshold: dec!(0.05),
        };
        let quote = sample_quote(Some(dec!(0.97)), Some(dec!(0.98)));

        let request = build_execution_request(&entry, &decision, &quote).expect("request");

        assert_eq!(request.limit_price, dec!(0.96));
    }

    #[test]
    fn managed_exit_reason_selects_profit_protection_loss_and_conditional_time_exit() {
        let settings = sample_settings(std::env::temp_dir().join("managed-exit-reason-test"));
        let position = models::PortfolioPosition {
            asset: "asset-1".to_owned(),
            condition_id: "condition-1".to_owned(),
            title: "Sample market".to_owned(),
            outcome: "YES".to_owned(),
            source_wallet: "0xsource".to_owned(),
            state: models::PositionState::Open,
            size: dec!(10),
            current_value: dec!(5),
            source_entry_price: dec!(0.5),
            average_entry_price: dec!(0.5),
            entry_conviction_score: dec!(0.75),
            peak_price_since_open: dec!(0.58),
            current_price: dec!(0.5),
            cost_basis: dec!(5),
            unrealized_pnl: dec!(0),
            opened_at: Some(Utc::now() - chrono::TimeDelta::minutes(20)),
            source_trade_timestamp_unix: 0,
            closing_started_at: None,
            closing_reason: None,
            last_close_attempt_at: None,
            close_attempts: 0,
            close_failure_reason: None,
            closing_escalation_level: 0,
            stale_reason: None,
        };

        assert_eq!(
            managed_exit_reason(
                &settings,
                &position,
                Utc::now(),
                dec!(0.60),
                TIME_EXIT_MAX_HOLD_SECONDS,
                false,
            ),
            ManagedExitEvaluation::Trigger(ManagedExitReason::TakeProfit)
        );
        assert_eq!(
            managed_exit_reason(
                &settings,
                &models::PortfolioPosition {
                    current_price: dec!(0.56),
                    unrealized_pnl: dec!(0.6),
                    ..position.clone()
                },
                Utc::now(),
                dec!(0.53),
                TIME_EXIT_MAX_HOLD_SECONDS,
                false,
            ),
            ManagedExitEvaluation::Trigger(ManagedExitReason::ProfitProtection)
        );
        assert_eq!(
            managed_exit_reason(
                &settings,
                &models::PortfolioPosition {
                    peak_price_since_open: dec!(0.50),
                    ..position.clone()
                },
                Utc::now(),
                dec!(0.44),
                TIME_EXIT_MAX_HOLD_SECONDS,
                false,
            ),
            ManagedExitEvaluation::Trigger(ManagedExitReason::StopLoss)
        );
        assert_eq!(
            managed_exit_reason(
                &settings,
                &models::PortfolioPosition {
                    peak_price_since_open: dec!(0.50),
                    ..position.clone()
                },
                Utc::now(),
                dec!(0.50),
                TIME_EXIT_MAX_HOLD_SECONDS,
                false,
            ),
            ManagedExitEvaluation::Trigger(ManagedExitReason::TimeExit)
        );
        assert_eq!(
            managed_exit_reason(
                &settings,
                &models::PortfolioPosition {
                    peak_price_since_open: dec!(0.50),
                    ..position
                },
                Utc::now(),
                dec!(0.50),
                TIME_EXIT_MAX_HOLD_SECONDS,
                true,
            ),
            ManagedExitEvaluation::SuppressTimeExit("source_exit_pending")
        );
    }

    #[test]
    fn time_exit_requires_stagnation_or_small_pnl() {
        let settings = sample_settings(std::env::temp_dir().join("time-exit-stagnation-test"));
        let position = models::PortfolioPosition {
            asset: "asset-1".to_owned(),
            condition_id: "condition-1".to_owned(),
            title: "Sample market".to_owned(),
            outcome: "YES".to_owned(),
            source_wallet: "0xsource".to_owned(),
            state: models::PositionState::Open,
            size: dec!(10),
            current_value: dec!(5.8),
            source_entry_price: dec!(0.5),
            average_entry_price: dec!(0.5),
            entry_conviction_score: dec!(0.80),
            peak_price_since_open: dec!(0.56),
            current_price: dec!(0.55),
            cost_basis: dec!(5),
            unrealized_pnl: dec!(0.8),
            opened_at: Some(Utc::now() - chrono::TimeDelta::minutes(20)),
            source_trade_timestamp_unix: 0,
            closing_started_at: None,
            closing_reason: None,
            last_close_attempt_at: None,
            close_attempts: 0,
            close_failure_reason: None,
            closing_escalation_level: 0,
            stale_reason: None,
        };

        assert_eq!(
            managed_exit_reason(
                &settings,
                &position,
                Utc::now(),
                dec!(0.56),
                TIME_EXIT_MAX_HOLD_SECONDS,
                false,
            ),
            ManagedExitEvaluation::SuppressTimeExit("favorable_trend")
        );
        assert_eq!(
            managed_exit_reason(
                &settings,
                &models::PortfolioPosition {
                    current_price: dec!(0.50),
                    current_value: dec!(5),
                    unrealized_pnl: dec!(0),
                    peak_price_since_open: dec!(0.50),
                    ..position
                },
                Utc::now(),
                dec!(0.50),
                TIME_EXIT_MAX_HOLD_SECONDS,
                false,
            ),
            ManagedExitEvaluation::Trigger(ManagedExitReason::TimeExit)
        );
    }

    #[test]
    fn time_exit_extends_mild_losers_without_reversal() {
        let settings = sample_settings(std::env::temp_dir().join("time-exit-extension-test"));
        let position = models::PortfolioPosition {
            asset: "asset-1".to_owned(),
            condition_id: "condition-1".to_owned(),
            title: "Sample market".to_owned(),
            outcome: "YES".to_owned(),
            source_wallet: "0xsource".to_owned(),
            state: models::PositionState::Open,
            size: dec!(10),
            current_value: dec!(4.95),
            source_entry_price: dec!(0.5),
            average_entry_price: dec!(0.5),
            entry_conviction_score: dec!(0.70),
            peak_price_since_open: dec!(0.50),
            current_price: dec!(0.50),
            cost_basis: dec!(5),
            unrealized_pnl: dec!(-0.05),
            opened_at: Some(Utc::now() - chrono::TimeDelta::minutes(20)),
            source_trade_timestamp_unix: 0,
            closing_started_at: None,
            closing_reason: None,
            last_close_attempt_at: None,
            close_attempts: 0,
            close_failure_reason: None,
            closing_escalation_level: 0,
            stale_reason: None,
        };

        assert_eq!(
            managed_exit_reason(
                &settings,
                &position,
                Utc::now(),
                dec!(0.499),
                TIME_EXIT_MAX_HOLD_SECONDS,
                false,
            ),
            ManagedExitEvaluation::SuppressTimeExit("mild_loss_extension")
        );
    }

    #[test]
    fn low_conviction_losers_compress_faster_than_high_conviction_trades() {
        let settings = sample_settings(std::env::temp_dir().join("conviction-time-exit-test"));
        let base_position = models::PortfolioPosition {
            asset: "asset-1".to_owned(),
            condition_id: "condition-1".to_owned(),
            title: "Sample market".to_owned(),
            outcome: "YES".to_owned(),
            source_wallet: "0xsource".to_owned(),
            state: models::PositionState::Open,
            size: dec!(10),
            current_value: dec!(4.95),
            source_entry_price: dec!(0.5),
            average_entry_price: dec!(0.5),
            entry_conviction_score: dec!(0.85),
            peak_price_since_open: dec!(0.50),
            current_price: dec!(0.50),
            cost_basis: dec!(5),
            unrealized_pnl: dec!(-0.05),
            opened_at: Some(Utc::now() - chrono::TimeDelta::minutes(20)),
            source_trade_timestamp_unix: 0,
            closing_started_at: None,
            closing_reason: None,
            last_close_attempt_at: None,
            close_attempts: 0,
            close_failure_reason: None,
            closing_escalation_level: 0,
            stale_reason: None,
        };

        assert_eq!(
            managed_exit_reason(
                &settings,
                &base_position,
                Utc::now(),
                dec!(0.499),
                TIME_EXIT_MAX_HOLD_SECONDS,
                false,
            ),
            ManagedExitEvaluation::SuppressTimeExit("mild_loss_extension")
        );
        assert_eq!(
            managed_exit_reason(
                &settings,
                &models::PortfolioPosition {
                    entry_conviction_score: dec!(0.45),
                    ..base_position
                },
                Utc::now(),
                dec!(0.499),
                TIME_EXIT_MAX_HOLD_SECONDS,
                false,
            ),
            ManagedExitEvaluation::Trigger(ManagedExitReason::TimeExit)
        );
    }

    #[test]
    fn expectancy_analytics_update_correctly() {
        let now = Utc::now();
        let mut state = ExecutionAnalyticsState {
            cohorts: vec![
            TradeCohort {
                source_wallet: "0xwallet-a".to_owned(),
                asset: "asset-a".to_owned(),
                condition_id: "condition-a".to_owned(),
                outcome: "YES".to_owned(),
                market_type: models::MarketType::Medium,
                side: "BUY".to_owned(),
                source_trade_timestamp_unix: now.timestamp_millis(),
                source_price: dec!(0.5),
                filled_price: dec!(0.5),
                entry_slippage_pct: dec!(0.01),
                conviction_score: dec!(0.90),
                wallet_alpha_score: dec!(0.82),
                entry_notional: dec!(8),
                sizing_bucket: "high_conviction".to_owned(),
                filled_size: dec!(16),
                execution_mode: "paper".to_owned(),
                open_time: now - chrono::TimeDelta::minutes(30),
                close_time: Some(now),
                close_reason: Some("PROFIT_PROTECTION".to_owned()),
                realized_pnl: dec!(2),
                unrealized_pnl: Decimal::ZERO,
                status: TradeCohortStatus::Closed,
                remaining_size: Decimal::ZERO,
                cost_basis: Decimal::ZERO,
            },
            TradeCohort {
                source_wallet: "0xwallet-b".to_owned(),
                asset: "asset-b".to_owned(),
                condition_id: "condition-b".to_owned(),
                outcome: "NO".to_owned(),
                market_type: models::MarketType::Short,
                side: "BUY".to_owned(),
                source_trade_timestamp_unix: now.timestamp_millis(),
                source_price: dec!(0.5),
                filled_price: dec!(0.5),
                entry_slippage_pct: dec!(0.02),
                conviction_score: dec!(0.55),
                wallet_alpha_score: dec!(0.50),
                entry_notional: dec!(5),
                sizing_bucket: "reduced".to_owned(),
                filled_size: dec!(10),
                execution_mode: "paper".to_owned(),
                open_time: now - chrono::TimeDelta::minutes(20),
                close_time: Some(now),
                close_reason: Some("TIME_EXIT".to_owned()),
                realized_pnl: dec!(-1),
                unrealized_pnl: Decimal::ZERO,
                status: TradeCohortStatus::Closed,
                remaining_size: Decimal::ZERO,
                cost_basis: Decimal::ZERO,
            },
            TradeCohort {
                source_wallet: "0xwallet-b".to_owned(),
                asset: "asset-c".to_owned(),
                condition_id: "condition-c".to_owned(),
                outcome: "YES".to_owned(),
                market_type: models::MarketType::UltraShort,
                side: "BUY".to_owned(),
                source_trade_timestamp_unix: now.timestamp_millis(),
                source_price: dec!(0.5),
                filled_price: dec!(0.5),
                entry_slippage_pct: dec!(0.02),
                conviction_score: dec!(0.58),
                wallet_alpha_score: dec!(0.48),
                entry_notional: dec!(4),
                sizing_bucket: "micro".to_owned(),
                filled_size: dec!(8),
                execution_mode: "paper".to_owned(),
                open_time: now - chrono::TimeDelta::minutes(10),
                close_time: Some(now),
                close_reason: Some("STOP_LOSS".to_owned()),
                realized_pnl: dec!(-0.5),
                unrealized_pnl: Decimal::ZERO,
                status: TradeCohortStatus::Closed,
                remaining_size: Decimal::ZERO,
                cost_basis: Decimal::ZERO,
            },
            ],
            ..ExecutionAnalyticsState::default()
        };
        let portfolio = models::PortfolioSnapshot {
            fetched_at: now,
            total_value: dec!(200.5),
            current_equity: dec!(200.5),
            starting_equity: dec!(200),
            peak_equity: dec!(200.5),
            ..models::PortfolioSnapshot::default()
        };

        refresh_analytics_from_portfolio(&mut state, &portfolio, dec!(200));

        assert_eq!(state.average_winner.round_dp(4), dec!(2));
        assert_eq!(state.average_loser.round_dp(4), dec!(-0.75));
        assert_eq!(state.payoff_ratio.round_dp(4), dec!(2.6667));
        assert_eq!(state.expectancy_per_trade.round_dp(4), dec!(0.1667));
        assert_eq!(
            state
                .expectancy_by_conviction_bucket
                .get("high")
                .copied()
                .unwrap_or(Decimal::ZERO)
                .round_dp(4),
            dec!(2)
        );
        assert_eq!(
            state
                .expectancy_by_conviction_bucket
                .get("low")
                .copied()
                .unwrap_or(Decimal::ZERO)
                .round_dp(4),
            dec!(-0.75)
        );
        assert_eq!(
            state
                .pnl_by_sizing_bucket
                .get("high_conviction")
                .copied()
                .unwrap_or(Decimal::ZERO)
                .round_dp(4),
            dec!(2)
        );
        assert_eq!(
            state
                .average_size_by_conviction_bucket
                .get("low")
                .copied()
                .unwrap_or(Decimal::ZERO)
                .round_dp(4),
            dec!(4.5)
        );
    }

    #[test]
    fn trade_rate_limiter_caps_three_claims_per_minute() {
        let mut limiter = TradeRateLimiter::default();
        let now = Instant::now();

        assert!(limiter.try_claim(now).is_some());
        assert!(limiter.try_claim(now + Duration::from_secs(1)).is_some());
        assert!(limiter.try_claim(now + Duration::from_secs(2)).is_some());
        assert!(limiter.try_claim(now + Duration::from_secs(3)).is_none());
        assert!(limiter.try_claim(now + Duration::from_secs(61)).is_some());
    }

    #[test]
    fn fail_safe_hedge_entry_preserves_wallet_and_execution_fill() {
        let pending = sample_pending_prediction_execution();
        let result = sample_execution_success(ExecutionSide::Sell);

        let entry = build_fail_safe_hedge_entry(&pending, &result);
        let decision = fail_safe_hedge_decision(&result);

        assert_eq!(entry.proxy_wallet, pending.source_entry.proxy_wallet);
        assert_eq!(entry.asset, "asset-1");
        assert_eq!(entry.side, "SELL");
        assert_eq!(entry.transaction_hash, "0xhedge");
        assert!(entry.title.contains("Fail-safe hedge"));
        assert_eq!(entry.size, 4.0);
        assert_eq!(entry.usdc_size, 1.92);

        assert_eq!(decision.token_id, "asset-1");
        assert_eq!(decision.side, ExecutionSide::Sell);
        assert_eq!(decision.size, dec!(4));
        assert_eq!(decision.notional, dec!(1.92));
    }

    #[test]
    fn live_pending_validation_is_retained_briefly_for_late_confirmation() {
        let mut tracker = PendingPredictionTracker::default();
        let mut pending = sample_pending_prediction_execution();
        pending.signal_key = "late-live-signal".to_owned();
        pending.validation_deadline = Instant::now() - Duration::from_millis(1);

        tracker.insert(pending.clone());
        let expired = tracker.take_expired(Instant::now());

        assert_eq!(expired.len(), 1);
        let recovered = tracker
            .take_recent("late-live-signal", Instant::now())
            .expect("late validation retained");
        assert!(recovered.execution_result.mode == crate::config::ExecutionMode::Live);
        assert_eq!(recovered.signal_key, "late-live-signal");
    }

    #[test]
    fn closing_timeout_escalates_instead_of_becoming_stale() {
        let settings = sample_settings(std::env::temp_dir().join("copytrade-main-test"));
        let position = sample_closing_position(
            chrono::TimeDelta::seconds(31),
            chrono::TimeDelta::seconds(2),
        );

        let action = closing_action_plan(&position, &settings, Utc::now()).expect("action");
        assert!(matches!(
            action,
            ClosingActionPlan::Retry {
                mode: ExecutionMode::EmergencyExit { stage: 1 },
                escalation_level: 1
            }
        ));

        let mut snapshot = models::PortfolioSnapshot {
            fetched_at: Utc::now(),
            total_value: dec!(200),
            total_exposure: dec!(5),
            cash_balance: dec!(195),
            realized_pnl: dec!(0),
            unrealized_pnl: dec!(0),
            positions: vec![position],
            ..models::PortfolioSnapshot::default()
        };
        snapshot.cleanup_stale_positions(settings.max_position_age_hours);
        assert_eq!(snapshot.positions[0].state, models::PositionState::Closing);
    }

    #[tokio::test]
    async fn analytics_counters_distinguish_source_and_managed_exits() {
        let tracker = ExecutionAnalyticsTracker {
            path: std::env::temp_dir().join(format!(
                "copytrade-analytics-{}.json",
                Utc::now().timestamp_nanos_opt().unwrap_or_default()
            )),
            start_capital_usd: dec!(200),
            state: Arc::new(Mutex::new(ExecutionAnalyticsState::default())),
            dirty: Arc::new(AtomicBool::new(false)),
            flush_notify: Arc::new(Notify::new()),
        };

        tracker.record_exit_event("source_exit_matched", None).await;
        tracker.record_exit_event("close_filled", None).await;
        tracker
            .record_exit_event("managed_exit_time_limit", None)
            .await;

        let state = tracker.state.lock().await.clone();
        assert_eq!(state.exit_event_counts.get("source_exit_matched"), Some(&1));
        assert_eq!(
            state.exit_event_counts.get("managed_exit_time_limit"),
            Some(&1)
        );
        assert_eq!(state.exit_event_counts.get("close_filled"), Some(&1));
    }

    #[tokio::test]
    async fn closing_claims_block_duplicate_close_submissions() {
        let closing_positions: ClosingPositions =
            Arc::new(Mutex::new(std::collections::HashSet::new()));
        let key = PositionKey::new("condition-1", "YES", "0xsource");

        assert!(claim_closing_position(&closing_positions, &key).await);
        assert!(!claim_closing_position(&closing_positions, &key).await);
        release_closing_position(&closing_positions, Some(&key)).await;
        assert!(claim_closing_position(&closing_positions, &key).await);
    }
}
