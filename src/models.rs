use std::collections::BTreeMap;
use std::time::Instant;

use anyhow::{Result, anyhow};
use chrono::{DateTime, NaiveDate, TimeDelta, TimeZone, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ActivityEntry {
    #[serde(rename = "proxyWallet")]
    pub proxy_wallet: String,
    pub timestamp: i64,
    #[serde(rename = "conditionId")]
    pub condition_id: String,
    #[serde(rename = "type")]
    pub type_name: String,
    pub size: f64,
    #[serde(rename = "usdcSize")]
    pub usdc_size: f64,
    #[serde(rename = "transactionHash")]
    pub transaction_hash: String,
    pub price: f64,
    pub asset: String,
    pub side: String,
    #[serde(rename = "outcomeIndex")]
    pub outcome_index: i64,
    pub title: String,
    pub slug: String,
    #[serde(rename = "eventSlug")]
    pub event_slug: String,
    pub outcome: String,
}

impl ActivityEntry {
    pub fn dedupe_key(&self) -> String {
        format!(
            "{}:{}:{}:{}:{}",
            self.transaction_hash, self.asset, self.side, self.timestamp, self.usdc_size
        )
    }

    pub fn usdc_decimal(&self) -> Result<Decimal> {
        Decimal::from_f64_retain(self.usdc_size)
            .ok_or_else(|| anyhow!("invalid usdc size {}", self.usdc_size))
    }

    pub fn size_decimal(&self) -> Result<Decimal> {
        Decimal::from_f64_retain(self.size).ok_or_else(|| anyhow!("invalid size {}", self.size))
    }

    pub fn price_decimal(&self) -> Result<Decimal> {
        Decimal::from_f64_retain(self.price).ok_or_else(|| anyhow!("invalid price {}", self.price))
    }
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct PositionKey {
    pub condition_id: String,
    pub outcome: String,
    pub source_wallet: String,
}

impl PositionKey {
    pub fn new(
        condition_id: impl Into<String>,
        outcome: impl AsRef<str>,
        source_wallet: impl AsRef<str>,
    ) -> Self {
        Self {
            condition_id: condition_id.into(),
            outcome: normalize_position_outcome(outcome.as_ref()),
            source_wallet: normalize_portfolio_wallet(source_wallet.as_ref()),
        }
    }
}

impl ActivityEntry {
    pub fn position_key(&self) -> PositionKey {
        PositionKey::new(&self.condition_id, &self.outcome, &self.proxy_wallet)
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub enum PositionState {
    #[default]
    Open,
    Closing,
    Closed,
    Stale,
}

#[derive(Clone, Debug)]
pub struct TradeStageTimestamps {
    pub websocket_event_received_at: Instant,
    pub websocket_event_received_at_utc: DateTime<Utc>,
    pub detection_triggered_at: Instant,
    pub detection_triggered_at_utc: DateTime<Utc>,
}

impl TradeStageTimestamps {
    pub fn detection_latency_ms(&self) -> u64 {
        self.detection_triggered_at
            .duration_since(self.websocket_event_received_at)
            .as_millis() as u64
    }

    pub fn execution_latency_ms(&self, submitted_at: Instant) -> u64 {
        submitted_at
            .duration_since(self.detection_triggered_at)
            .as_millis() as u64
    }

    pub fn total_latency_ms(&self, submitted_at: Instant) -> u64 {
        submitted_at
            .duration_since(self.websocket_event_received_at)
            .as_millis() as u64
    }

    pub fn elapsed_total_ms(&self) -> u64 {
        self.websocket_event_received_at.elapsed().as_millis() as u64
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PositionEntry {
    pub asset: String,
    #[serde(rename = "conditionId")]
    pub condition_id: String,
    pub size: f64,
    #[serde(rename = "currentValue")]
    pub current_value: f64,
    pub title: String,
    pub outcome: String,
}

impl PositionEntry {
    pub fn current_value_decimal(&self) -> Result<Decimal> {
        Decimal::from_f64_retain(self.current_value)
            .ok_or_else(|| anyhow!("invalid current value {}", self.current_value))
    }

    pub fn size_decimal(&self) -> Result<Decimal> {
        Decimal::from_f64_retain(self.size).ok_or_else(|| anyhow!("invalid size {}", self.size))
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct BookLevel {
    pub price: String,
    pub size: String,
}

impl BookLevel {
    pub fn price_decimal(&self) -> Result<Decimal> {
        self.price.parse::<Decimal>().map_err(Into::into)
    }

    pub fn size_decimal(&self) -> Result<Decimal> {
        self.size.parse::<Decimal>().map_err(Into::into)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct OrderBookResponse {
    pub market: String,
    #[serde(rename = "asset_id")]
    pub asset_id: String,
    pub bids: Vec<BookLevel>,
    pub asks: Vec<BookLevel>,
    #[serde(rename = "min_order_size")]
    pub min_order_size: String,
    #[serde(rename = "tick_size")]
    pub tick_size: String,
    #[serde(rename = "neg_risk")]
    pub neg_risk: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct BestQuote {
    pub asset_id: String,
    pub best_bid: Option<Decimal>,
    pub best_bid_size: Option<Decimal>,
    pub best_ask: Option<Decimal>,
    pub best_ask_size: Option<Decimal>,
    pub tick_size: Decimal,
    pub min_order_size: Decimal,
    pub neg_risk: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PortfolioSnapshot {
    pub fetched_at: DateTime<Utc>,
    pub total_value: Decimal,
    pub total_exposure: Decimal,
    pub cash_balance: Decimal,
    #[serde(default)]
    pub realized_pnl: Decimal,
    #[serde(default)]
    pub unrealized_pnl: Decimal,
    pub positions: Vec<PortfolioPosition>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PortfolioPosition {
    pub asset: String,
    pub condition_id: String,
    pub title: String,
    pub outcome: String,
    #[serde(default)]
    pub source_wallet: String,
    #[serde(default)]
    pub state: PositionState,
    pub size: Decimal,
    pub current_value: Decimal,
    #[serde(default)]
    pub average_entry_price: Decimal,
    #[serde(default)]
    pub current_price: Decimal,
    #[serde(default)]
    pub cost_basis: Decimal,
    #[serde(default)]
    pub unrealized_pnl: Decimal,
    #[serde(default)]
    pub opened_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub source_trade_timestamp_unix: i64,
    #[serde(default)]
    pub closing_started_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug)]
pub struct ResolvedPosition {
    pub key: PositionKey,
    pub asset: String,
    pub outcome: String,
    pub source_wallet: String,
    pub size: Decimal,
    pub current_value: Decimal,
    pub used_fallback: bool,
}

impl PortfolioPosition {
    pub fn position_key(&self) -> PositionKey {
        PositionKey::new(&self.condition_id, &self.outcome, &self.source_wallet)
    }

    pub fn is_stale(&self) -> bool {
        self.state == PositionState::Stale
    }

    pub fn is_active(&self) -> bool {
        self.size > Decimal::ZERO
            && !matches!(self.state, PositionState::Closed | PositionState::Stale)
    }

    pub fn reference_time(&self) -> Option<DateTime<Utc>> {
        self.opened_at
            .or_else(|| portfolio_timestamp_to_datetime(self.source_trade_timestamp_unix))
    }

    pub fn age(&self, now: DateTime<Utc>) -> Option<TimeDelta> {
        self.reference_time()
            .map(|opened_at| now.signed_duration_since(opened_at))
    }

    pub fn should_force_exit(&self, now: DateTime<Utc>, max_hold_time_seconds: i64) -> bool {
        self.state == PositionState::Open
            && max_hold_time_seconds > 0
            && self
                .age(now)
                .is_some_and(|age| age >= TimeDelta::seconds(max_hold_time_seconds))
    }
}

impl PortfolioSnapshot {
    pub fn market_exposure(&self, condition_id: &str) -> Decimal {
        self.active_positions()
            .iter()
            .filter(|position| position.condition_id == condition_id)
            .map(|position| position.current_value)
            .sum()
    }

    pub fn active_total_exposure(&self) -> Decimal {
        self.active_positions()
            .iter()
            .map(|position| position.current_value)
            .sum()
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub fn asset_size(&self, asset: &str) -> Decimal {
        self.active_positions()
            .iter()
            .filter(|position| position.asset == asset)
            .map(|position| position.size)
            .sum()
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub fn asset_size_for_wallet(&self, asset: &str, wallet: &str) -> Decimal {
        let wallet = normalize_portfolio_wallet(wallet);
        if wallet.is_empty() {
            return Decimal::ZERO;
        }

        self.active_positions()
            .iter()
            .filter(|position| {
                position.asset == asset
                    && normalize_portfolio_wallet(&position.source_wallet) == wallet
            })
            .map(|position| position.size)
            .sum()
    }

    #[allow(dead_code)]
    pub fn asset_exposure_for_wallet(&self, asset: &str, wallet: &str) -> Decimal {
        let wallet = normalize_portfolio_wallet(wallet);
        if wallet.is_empty() {
            return Decimal::ZERO;
        }

        self.active_positions()
            .iter()
            .filter(|position| {
                position.asset == asset
                    && normalize_portfolio_wallet(&position.source_wallet) == wallet
            })
            .map(|position| position.current_value)
            .sum()
    }

    #[allow(dead_code)]
    pub fn copied_position_size(&self, asset: &str, condition_id: &str, wallet: &str) -> Decimal {
        let wallet = normalize_portfolio_wallet(wallet);
        if wallet.is_empty() {
            return Decimal::ZERO;
        }

        self.active_positions()
            .iter()
            .filter(|position| {
                position.asset == asset
                    && (condition_id.is_empty() || position.condition_id == condition_id)
                    && normalize_portfolio_wallet(&position.source_wallet) == wallet
            })
            .map(|position| position.size)
            .sum()
    }

    #[allow(dead_code)]
    pub fn has_copied_position(&self, asset: &str, condition_id: &str, wallet: &str) -> bool {
        !self
            .copied_position_size(asset, condition_id, wallet)
            .is_zero()
    }

    pub fn has_position_key(&self, key: &PositionKey) -> bool {
        self.position_by_key(key).is_some()
    }

    pub fn has_stale_position_key(&self, key: &PositionKey) -> bool {
        self.positions
            .iter()
            .any(|position| position.position_key() == *key && position.is_stale())
    }

    pub fn has_stale_position_for_condition(&self, condition_id: &str) -> bool {
        self.positions
            .iter()
            .any(|position| position.condition_id == condition_id && position.is_stale())
    }

    pub fn position_by_key(&self, key: &PositionKey) -> Option<&PortfolioPosition> {
        self.active_positions()
            .into_iter()
            .find(|position| position.position_key() == *key)
    }

    #[allow(dead_code)]
    pub fn condition_position(&self, condition_id: &str) -> Option<&PortfolioPosition> {
        self.active_positions()
            .into_iter()
            .find(|position| position.condition_id == condition_id)
    }

    pub fn opposite_side_position(
        &self,
        condition_id: &str,
        outcome: &str,
    ) -> Option<&PortfolioPosition> {
        let requested_outcome = normalize_position_outcome(outcome);
        self.active_positions().into_iter().find(|position| {
            position.condition_id == condition_id
                && normalize_position_outcome(&position.outcome) != requested_outcome
        })
    }

    pub fn resolve_position_to_sell(&self, entry: &ActivityEntry) -> Option<ResolvedPosition> {
        self.resolve_position_to_sell_with_hint(
            &entry.position_key(),
            Some(entry.asset.as_str()),
            Some(entry.proxy_wallet.as_str()),
        )
    }

    pub fn can_resolve_position_to_sell(&self, entry: &ActivityEntry) -> bool {
        self.resolve_position_to_sell(entry).is_some()
    }

    pub fn resolve_position_to_sell_with_hint(
        &self,
        requested_key: &PositionKey,
        asset_hint: Option<&str>,
        wallet_hint: Option<&str>,
    ) -> Option<ResolvedPosition> {
        if let Some(position) = self.position_by_key(requested_key) {
            return Some(ResolvedPosition {
                key: position.position_key(),
                asset: position.asset.clone(),
                outcome: position.outcome.clone(),
                source_wallet: position.source_wallet.clone(),
                size: position.size,
                current_value: position.current_value,
                used_fallback: false,
            });
        }

        self.active_positions()
            .into_iter()
            .filter(|position| position.condition_id == requested_key.condition_id)
            .min_by_key(|position| {
                let position_outcome = normalize_position_outcome(&position.outcome);
                let same_outcome = position_outcome == requested_key.outcome;
                let complementary_outcome =
                    is_complementary_outcome(&position_outcome, &requested_key.outcome);
                let same_asset = asset_hint.is_some_and(|asset| position.asset == asset);
                let same_wallet = wallet_hint.is_some_and(|wallet| {
                    normalize_portfolio_wallet(&position.source_wallet)
                        == normalize_portfolio_wallet(wallet)
                });
                (
                    if same_outcome {
                        0_u8
                    } else if complementary_outcome {
                        1
                    } else {
                        2
                    },
                    if same_asset { 0_u8 } else { 1 },
                    if same_wallet { 0_u8 } else { 1 },
                )
            })
            .map(|position| ResolvedPosition {
                key: position.position_key(),
                asset: position.asset.clone(),
                outcome: position.outcome.clone(),
                source_wallet: position.source_wallet.clone(),
                size: position.size,
                current_value: position.current_value,
                used_fallback: true,
            })
    }

    pub fn mark_position_closing(&mut self, key: &PositionKey) -> bool {
        let now = Utc::now();
        if let Some(position) = self
            .positions
            .iter_mut()
            .find(|position| position.position_key() == *key && position.is_active())
        {
            position.state = PositionState::Closing;
            position.closing_started_at = Some(now);
            return true;
        }
        false
    }

    pub fn restore_position_open(&mut self, key: &PositionKey) -> bool {
        if let Some(position) = self.positions.iter_mut().find(|position| {
            position.position_key() == *key && position.state == PositionState::Closing
        }) {
            position.state = PositionState::Open;
            position.closing_started_at = None;
            return true;
        }
        false
    }

    pub fn cleanup_stale_positions(&mut self, max_position_age_hours: u64) {
        let now = Utc::now();
        let max_age = TimeDelta::hours(max_position_age_hours.min(i64::MAX as u64) as i64);
        let closing_timeout = TimeDelta::seconds(3);

        for position in &mut self.positions {
            let timed_out_while_closing = position.state == PositionState::Closing
                && position.closing_started_at.is_some_and(|started_at| {
                    now.signed_duration_since(started_at) >= closing_timeout
                });
            let legacy_or_unknown_position = position.source_trade_timestamp_unix == 0;
            let aged_out = position.age(now).is_some_and(|age| age >= max_age);

            if legacy_or_unknown_position || aged_out || timed_out_while_closing {
                position.state = PositionState::Stale;
            }
        }
    }

    pub fn wallet_unrealized_pnl(&self, wallet: &str) -> Decimal {
        let wallet = normalize_portfolio_wallet(wallet);
        if wallet.is_empty() {
            return Decimal::ZERO;
        }

        self.active_positions()
            .iter()
            .filter(|position| normalize_portfolio_wallet(&position.source_wallet) == wallet)
            .map(|position| position.unrealized_pnl)
            .sum()
    }

    pub fn window_split(&self, window_day: NaiveDate) -> PortfolioWindowSummary {
        let mut summary = PortfolioWindowSummary::default();
        for position in self.active_positions() {
            let in_current_window = position
                .opened_at
                .map(|opened_at| opened_at.date_naive() == window_day)
                .unwrap_or(false);
            if in_current_window {
                summary.current_window_positions += 1;
                summary.current_window_unrealized_pnl += position.unrealized_pnl;
            } else {
                summary.legacy_positions += 1;
                summary.legacy_unrealized_pnl += position.unrealized_pnl;
            }
        }
        summary
    }

    fn active_positions(&self) -> Vec<&PortfolioPosition> {
        self.positions
            .iter()
            .filter(|position| position.is_active())
            .collect()
    }
}

fn normalize_portfolio_wallet(wallet: &str) -> String {
    wallet.trim().to_ascii_lowercase()
}

fn normalize_position_outcome(outcome: &str) -> String {
    outcome.trim().to_ascii_uppercase()
}

fn is_complementary_outcome(left: &str, right: &str) -> bool {
    matches!(
        (
            normalize_position_outcome(left).as_str(),
            normalize_position_outcome(right).as_str()
        ),
        ("YES", "NO") | ("NO", "YES")
    )
}

fn portfolio_timestamp_to_datetime(timestamp: i64) -> Option<DateTime<Utc>> {
    if timestamp <= 0 {
        return None;
    }

    if timestamp >= 10_000_000_000 {
        Utc.timestamp_millis_opt(timestamp).single()
    } else {
        Utc.timestamp_opt(timestamp, 0).single()
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum SourceTradeClass {
    EligibleEntry,
    EligibleExit,
    OrphanExit,
    EntryRejected,
    ExitRejected,
}

impl SourceTradeClass {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::EligibleEntry => "eligible_entry",
            Self::EligibleExit => "eligible_exit",
            Self::OrphanExit => "orphan_exit",
            Self::EntryRejected => "entry_rejected",
            Self::ExitRejected => "exit_rejected",
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum TradeCohortStatus {
    Open,
    Closed,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct PortfolioWindowSummary {
    pub current_window_positions: usize,
    pub legacy_positions: usize,
    pub current_window_unrealized_pnl: Decimal,
    pub legacy_unrealized_pnl: Decimal,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TradeCohort {
    pub source_wallet: String,
    pub asset: String,
    pub condition_id: String,
    #[serde(default)]
    pub outcome: String,
    pub side: String,
    pub source_trade_timestamp_unix: i64,
    pub source_price: Decimal,
    pub filled_price: Decimal,
    pub filled_size: Decimal,
    pub execution_mode: String,
    pub open_time: DateTime<Utc>,
    pub close_time: Option<DateTime<Utc>>,
    pub realized_pnl: Decimal,
    pub unrealized_pnl: Decimal,
    pub status: TradeCohortStatus,
    #[serde(default)]
    pub remaining_size: Decimal,
    #[serde(default)]
    pub cost_basis: Decimal,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ExecutionAnalyticsState {
    pub summary_generated_at: DateTime<Utc>,
    pub run_started_at: DateTime<Utc>,
    pub trade_day: String,
    pub total_source_events_seen: u64,
    pub eligible_entry_events: u64,
    pub eligible_exit_events: u64,
    pub orphan_exit_events: u64,
    pub processed_entry_events: u64,
    pub processed_exit_events: u64,
    pub skipped_entry_events: u64,
    pub skipped_exit_events: u64,
    #[serde(default)]
    pub skipped_entry_by_reason: BTreeMap<String, u64>,
    #[serde(default)]
    pub skipped_exit_by_reason: BTreeMap<String, u64>,
    pub raw_copy_rate: Decimal,
    pub executable_entry_copy_rate: Decimal,
    pub executable_total_copy_rate: Decimal,
    pub pnl_current_window: Decimal,
    pub pnl_all_open_positions: Decimal,
    #[serde(default)]
    pub pnl_by_source_wallet: BTreeMap<String, Decimal>,
    #[serde(default)]
    pub pnl_by_trade_day: BTreeMap<String, Decimal>,
    pub current_window_positions: usize,
    pub legacy_positions: usize,
    pub current_window_unrealized_pnl: Decimal,
    pub legacy_unrealized_pnl: Decimal,
    pub current_window_trade_count: u64,
    pub winning_current_window_trades: u64,
    pub losing_current_window_trades: u64,
    pub current_window_win_rate: Decimal,
    pub current_window_open_mtm_win_rate: Decimal,
    pub account_level_portfolio_pnl: Decimal,
    #[serde(default)]
    pub cohorts: Vec<TradeCohort>,
}

impl Default for ExecutionAnalyticsState {
    fn default() -> Self {
        let now = Utc::now();
        Self {
            summary_generated_at: now,
            run_started_at: now,
            trade_day: now.date_naive().to_string(),
            total_source_events_seen: 0,
            eligible_entry_events: 0,
            eligible_exit_events: 0,
            orphan_exit_events: 0,
            processed_entry_events: 0,
            processed_exit_events: 0,
            skipped_entry_events: 0,
            skipped_exit_events: 0,
            skipped_entry_by_reason: BTreeMap::new(),
            skipped_exit_by_reason: BTreeMap::new(),
            raw_copy_rate: Decimal::ZERO,
            executable_entry_copy_rate: Decimal::ZERO,
            executable_total_copy_rate: Decimal::ZERO,
            pnl_current_window: Decimal::ZERO,
            pnl_all_open_positions: Decimal::ZERO,
            pnl_by_source_wallet: BTreeMap::new(),
            pnl_by_trade_day: BTreeMap::new(),
            current_window_positions: 0,
            legacy_positions: 0,
            current_window_unrealized_pnl: Decimal::ZERO,
            legacy_unrealized_pnl: Decimal::ZERO,
            current_window_trade_count: 0,
            winning_current_window_trades: 0,
            losing_current_window_trades: 0,
            current_window_win_rate: Decimal::ZERO,
            current_window_open_mtm_win_rate: Decimal::ZERO,
            account_level_portfolio_pnl: Decimal::ZERO,
            cohorts: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct HealthSnapshot {
    pub ready: bool,
    pub execution_mode: String,
    pub processed_trades: u64,
    pub skipped_trades: u64,
    pub poll_failures: u64,
    pub last_detection_ms: u64,
    pub last_execution_ms: u64,
    pub last_total_latency_ms: u64,
    pub average_latency_ms: u64,
    pub last_skip_processing_ms: u64,
    pub last_skip_reason_code: Option<String>,
    pub trading_paused: bool,
    pub last_latency_pause_reason: Option<String>,
    pub latency_fail_safe_trips: u64,
    pub last_error: Option<String>,
    pub last_portfolio_value: Option<Decimal>,
    pub last_update_iso: Option<DateTime<Utc>>,
}
