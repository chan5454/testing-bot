use std::collections::BTreeMap;
#[cfg(debug_assertions)]
use std::collections::BTreeSet;
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

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub enum MarketType {
    UltraShort,
    Short,
    #[default]
    Medium,
}

pub fn classify_market(title: &str) -> MarketType {
    let normalized = title.trim().to_ascii_lowercase();
    if normalized.contains("up/down")
        || normalized.contains("up down")
        || normalized.contains("5m")
        || normalized.contains("5 min")
        || normalized.contains("5 minute")
    {
        MarketType::UltraShort
    } else if normalized.contains("1h") || normalized.contains("1 h") || normalized.contains("hour")
    {
        MarketType::Short
    } else {
        MarketType::Medium
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
    pub parse_completed_at: Instant,
    pub parse_completed_at_utc: DateTime<Utc>,
    pub detection_triggered_at: Instant,
    pub detection_triggered_at_utc: DateTime<Utc>,
    pub attribution_completed_at: Option<Instant>,
    pub attribution_completed_at_utc: Option<DateTime<Utc>>,
    pub fast_risk_completed_at: Option<Instant>,
    pub fast_risk_completed_at_utc: Option<DateTime<Utc>>,
}

impl TradeStageTimestamps {
    pub fn parse_latency_ms(&self) -> u64 {
        self.parse_completed_at
            .duration_since(self.websocket_event_received_at)
            .as_millis() as u64
    }

    pub fn attribution_latency_ms(&self) -> Option<u64> {
        self.attribution_completed_at
            .map(|attribution_completed_at| {
                attribution_completed_at
                    .duration_since(self.parse_completed_at)
                    .as_millis() as u64
            })
    }

    pub fn detection_latency_ms(&self) -> u64 {
        self.detection_triggered_at
            .duration_since(self.websocket_event_received_at)
            .as_millis() as u64
    }

    pub fn fast_risk_latency_ms(&self) -> Option<u64> {
        let baseline = self
            .attribution_completed_at
            .unwrap_or(self.detection_triggered_at);
        self.fast_risk_completed_at.map(|fast_risk_completed_at| {
            fast_risk_completed_at.duration_since(baseline).as_millis() as u64
        })
    }

    pub fn execution_latency_ms(&self, submitted_at: Instant) -> u64 {
        let baseline = self
            .fast_risk_completed_at
            .unwrap_or(self.detection_triggered_at);
        submitted_at.duration_since(baseline).as_millis() as u64
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
pub struct EquityPoint {
    pub observed_at: DateTime<Utc>,
    pub equity: Decimal,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RealizedTradePoint {
    pub observed_at: DateTime<Utc>,
    pub pnl: Decimal,
    #[serde(default)]
    pub source_wallet: String,
    #[serde(default)]
    pub close_reason: String,
    #[serde(default)]
    pub hold_ms: u64,
    #[serde(default)]
    pub entry_slippage_pct: Decimal,
    #[serde(default)]
    pub market_type: MarketType,
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
    #[serde(default)]
    pub starting_equity: Decimal,
    #[serde(default)]
    pub current_equity: Decimal,
    #[serde(default)]
    pub peak_equity: Decimal,
    #[serde(default)]
    pub current_drawdown_pct: Decimal,
    #[serde(default)]
    pub rolling_drawdown_pct: Decimal,
    #[serde(default)]
    pub open_exposure_total: Decimal,
    #[serde(default)]
    pub open_positions_count: usize,
    #[serde(default)]
    pub consecutive_losses: u32,
    #[serde(default)]
    pub rolling_loss_count: u32,
    #[serde(default)]
    pub loss_cooldown_until: Option<DateTime<Utc>>,
    #[serde(default)]
    pub drawdown_guard_active: bool,
    #[serde(default)]
    pub hard_stop_active: bool,
    #[serde(default)]
    pub risk_utilization_pct: Decimal,
    #[serde(default)]
    pub recent_equity_samples: Vec<EquityPoint>,
    #[serde(default)]
    pub recent_realized_trade_points: Vec<RealizedTradePoint>,
    pub positions: Vec<PortfolioPosition>,
}

impl Default for PortfolioSnapshot {
    fn default() -> Self {
        Self {
            fetched_at: Utc::now(),
            total_value: Decimal::ZERO,
            total_exposure: Decimal::ZERO,
            cash_balance: Decimal::ZERO,
            realized_pnl: Decimal::ZERO,
            unrealized_pnl: Decimal::ZERO,
            starting_equity: Decimal::ZERO,
            current_equity: Decimal::ZERO,
            peak_equity: Decimal::ZERO,
            current_drawdown_pct: Decimal::ZERO,
            rolling_drawdown_pct: Decimal::ZERO,
            open_exposure_total: Decimal::ZERO,
            open_positions_count: 0,
            consecutive_losses: 0,
            rolling_loss_count: 0,
            loss_cooldown_until: None,
            drawdown_guard_active: false,
            hard_stop_active: false,
            risk_utilization_pct: Decimal::ZERO,
            recent_equity_samples: Vec::new(),
            recent_realized_trade_points: Vec::new(),
            positions: Vec::new(),
        }
    }
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
    pub source_entry_price: Decimal,
    #[serde(default)]
    pub average_entry_price: Decimal,
    #[serde(default)]
    pub entry_conviction_score: Decimal,
    #[serde(default)]
    pub peak_price_since_open: Decimal,
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
    #[serde(default)]
    pub closing_reason: Option<String>,
    #[serde(default)]
    pub last_close_attempt_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub close_attempts: u32,
    #[serde(default)]
    pub close_failure_reason: Option<String>,
    #[serde(default)]
    pub closing_escalation_level: u8,
    #[serde(default)]
    pub stale_reason: Option<String>,
}

#[derive(Clone, Debug)]
pub struct ResolvedPosition {
    pub key: PositionKey,
    pub asset: String,
    pub outcome: String,
    pub source_wallet: String,
    pub size: Decimal,
    pub current_value: Decimal,
    pub average_entry_price: Decimal,
    pub entry_conviction_score: Decimal,
    pub used_fallback: bool,
    pub fallback_reason: Option<String>,
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

    pub fn closing_age(&self, now: DateTime<Utc>) -> Option<TimeDelta> {
        self.closing_started_at
            .map(|started_at| now.signed_duration_since(started_at))
    }

    pub fn close_retry_due(&self, now: DateTime<Utc>, retry_interval: TimeDelta) -> bool {
        self.state == PositionState::Closing
            && self
                .last_close_attempt_at
                .map(|last_attempt_at| now.signed_duration_since(last_attempt_at) >= retry_interval)
                .unwrap_or(true)
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
    #[track_caller]
    pub fn debug_assert_invariants(&self) {
        #[cfg(debug_assertions)]
        {
            let mut active_keys = BTreeSet::new();
            for position in &self.positions {
                debug_assert!(
                    position.size >= Decimal::ZERO,
                    "portfolio position has negative size: {:?}",
                    position.position_key()
                );
                debug_assert!(
                    position.current_value >= Decimal::ZERO,
                    "portfolio position has negative exposure value: {:?}",
                    position.position_key()
                );

                match position.state {
                    PositionState::Open => {
                        debug_assert!(
                            position.closing_started_at.is_none(),
                            "open position retained closing_started_at: {:?}",
                            position.position_key()
                        );
                    }
                    PositionState::Closing => {
                        debug_assert!(
                            position.closing_started_at.is_some(),
                            "closing position is missing closing_started_at: {:?}",
                            position.position_key()
                        );
                    }
                    PositionState::Closed => {
                        debug_assert!(
                            position.size.is_zero(),
                            "closed position still has non-zero size: {:?}",
                            position.position_key()
                        );
                    }
                    PositionState::Stale => {
                        debug_assert!(
                            position.stale_reason.is_some(),
                            "stale position is missing stale_reason: {:?}",
                            position.position_key()
                        );
                    }
                }

                if position.is_active() {
                    let key = position.position_key();
                    debug_assert!(
                        !key.condition_id.is_empty()
                            && !key.outcome.is_empty()
                            && !key.source_wallet.is_empty(),
                        "active copied position has an incomplete PositionKey: {:?}",
                        key
                    );
                    debug_assert!(
                        active_keys.insert(key.clone()),
                        "duplicate active copied-position key in portfolio: {:?}",
                        key
                    );
                }
            }

            debug_assert!(
                self.active_total_exposure() >= Decimal::ZERO,
                "portfolio total exposure is negative"
            );
        }
    }

    pub fn equity(&self) -> Decimal {
        if self.current_equity > Decimal::ZERO {
            self.current_equity
        } else {
            self.total_value
        }
    }

    pub fn market_exposure(&self, condition_id: &str) -> Decimal {
        self.active_positions()
            .iter()
            .filter(|position| position.condition_id == condition_id)
            .map(|position| position.current_value)
            .sum()
    }

    pub fn market_exposure_pct(&self, condition_id: &str) -> Decimal {
        let equity = self.equity();
        if equity <= Decimal::ZERO {
            return Decimal::ZERO;
        }

        (self.market_exposure(condition_id) / equity)
            .max(Decimal::ZERO)
            .min(Decimal::ONE)
    }

    pub fn wallet_exposure(&self, wallet: &str) -> Decimal {
        let wallet = normalize_portfolio_wallet(wallet);
        if wallet.is_empty() {
            return Decimal::ZERO;
        }

        self.active_positions()
            .iter()
            .filter(|position| normalize_portfolio_wallet(&position.source_wallet) == wallet)
            .map(|position| position.current_value)
            .sum()
    }

    pub fn wallet_exposure_pct(&self, wallet: &str) -> Decimal {
        let equity = self.equity();
        if equity <= Decimal::ZERO {
            return Decimal::ZERO;
        }

        (self.wallet_exposure(wallet) / equity)
            .max(Decimal::ZERO)
            .min(Decimal::ONE)
    }

    pub fn market_type_exposure(&self, market_type: MarketType) -> Decimal {
        self.active_positions()
            .iter()
            .filter(|position| classify_market(&position.title) == market_type)
            .map(|position| position.current_value)
            .sum()
    }

    pub fn market_type_exposure_pct(&self, market_type: MarketType) -> Decimal {
        let equity = self.equity();
        if equity <= Decimal::ZERO {
            return Decimal::ZERO;
        }

        (self.market_type_exposure(market_type) / equity)
            .max(Decimal::ZERO)
            .min(Decimal::ONE)
    }

    pub fn active_total_exposure(&self) -> Decimal {
        self.active_positions()
            .iter()
            .map(|position| position.current_value)
            .sum()
    }

    pub fn total_exposure_pct(&self) -> Decimal {
        let equity = self.equity();
        if equity <= Decimal::ZERO {
            return Decimal::ZERO;
        }

        (self.active_total_exposure() / equity)
            .max(Decimal::ZERO)
            .min(Decimal::ONE)
    }

    pub fn loss_cooldown_active(&self, now: DateTime<Utc>) -> bool {
        self.loss_cooldown_until
            .is_some_and(|cooldown_until| cooldown_until > now)
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

    pub fn active_position_count_for_wallet_condition(
        &self,
        wallet: &str,
        condition_id: &str,
    ) -> usize {
        let wallet = normalize_portfolio_wallet(wallet);
        if wallet.is_empty() || condition_id.trim().is_empty() {
            return 0;
        }

        self.active_positions()
            .iter()
            .filter(|position| {
                position.condition_id == condition_id
                    && normalize_portfolio_wallet(&position.source_wallet) == wallet
            })
            .count()
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

    pub fn position_mut_by_key(&mut self, key: &PositionKey) -> Option<&mut PortfolioPosition> {
        self.positions
            .iter_mut()
            .find(|position| position.position_key() == *key && position.size > Decimal::ZERO)
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
                average_entry_price: position.average_entry_price,
                entry_conviction_score: position.entry_conviction_score,
                used_fallback: false,
                fallback_reason: None,
            });
        }

        let requested_wallet = wallet_hint
            .map(normalize_portfolio_wallet)
            .unwrap_or_else(|| requested_key.source_wallet.clone());
        let candidates = self
            .active_positions()
            .into_iter()
            .filter(|position| {
                position.condition_id == requested_key.condition_id
                    && normalize_portfolio_wallet(&position.source_wallet) == requested_wallet
            })
            .collect::<Vec<_>>();
        let _ = asset_hint;
        select_safe_exit_fallback(candidates).map(|(position, fallback_reason)| ResolvedPosition {
            key: position.position_key(),
            asset: position.asset.clone(),
            outcome: position.outcome.clone(),
            source_wallet: position.source_wallet.clone(),
            size: position.size,
            current_value: position.current_value,
            average_entry_price: position.average_entry_price,
            entry_conviction_score: position.entry_conviction_score,
            used_fallback: true,
            fallback_reason: Some(fallback_reason),
        })
    }

    pub fn position_is_closing(&self, key: &PositionKey) -> bool {
        self.positions.iter().any(|position| {
            position.position_key() == *key && position.state == PositionState::Closing
        })
    }

    pub fn close_retry_due(
        &self,
        key: &PositionKey,
        now: DateTime<Utc>,
        retry_interval: TimeDelta,
    ) -> bool {
        self.position_by_key(key)
            .map(|position| position.close_retry_due(now, retry_interval))
            .unwrap_or(true)
    }

    pub fn mark_position_closing(&mut self, key: &PositionKey, reason: &str) -> bool {
        let now = Utc::now();
        if let Some(position) = self.position_mut_by_key(key) {
            position.state = PositionState::Closing;
            position.closing_started_at = position.closing_started_at.or(Some(now));
            position.closing_reason = Some(reason.to_owned());
            position.stale_reason = None;
            return true;
        }
        false
    }

    pub fn note_close_attempt(&mut self, key: &PositionKey) -> bool {
        let now = Utc::now();
        if let Some(position) = self.position_mut_by_key(key) {
            position.state = PositionState::Closing;
            position.closing_started_at = position.closing_started_at.or(Some(now));
            position.last_close_attempt_at = Some(now);
            position.close_attempts = position.close_attempts.saturating_add(1);
            position.close_failure_reason = None;
            position.stale_reason = None;
            return true;
        }
        false
    }

    pub fn note_close_failure(&mut self, key: &PositionKey, reason: &str) -> bool {
        if let Some(position) = self.position_mut_by_key(key) {
            position.state = PositionState::Closing;
            position.close_failure_reason = Some(reason.to_owned());
            return true;
        }
        false
    }

    pub fn set_closing_escalation_level(&mut self, key: &PositionKey, level: u8) -> Option<u8> {
        let position = self.position_mut_by_key(key)?;
        let previous = position.closing_escalation_level;
        if level > previous {
            position.closing_escalation_level = level;
        }
        Some(previous)
    }

    pub fn mark_position_stale(&mut self, key: &PositionKey, reason: &str) -> bool {
        if let Some(position) = self.position_mut_by_key(key) {
            position.state = PositionState::Stale;
            position.stale_reason = Some(reason.to_owned());
            return true;
        }
        false
    }

    pub fn cleanup_stale_positions(&mut self, max_position_age_hours: u64) {
        let now = Utc::now();
        let max_age = TimeDelta::hours(max_position_age_hours.min(i64::MAX as u64) as i64);

        for position in &mut self.positions {
            if position.state == PositionState::Closing {
                continue;
            }
            let legacy_or_unknown_position = position.source_trade_timestamp_unix == 0;
            let aged_out = position.age(now).is_some_and(|age| age >= max_age);

            if legacy_or_unknown_position || aged_out {
                position.state = PositionState::Stale;
                if position.stale_reason.is_none() {
                    position.stale_reason = Some("stale_without_seen_exit".to_owned());
                }
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

    pub fn wallet_realized_pnl(&self, wallet: &str) -> Decimal {
        let wallet = normalize_portfolio_wallet(wallet);
        if wallet.is_empty() {
            return Decimal::ZERO;
        }

        self.recent_realized_trade_points
            .iter()
            .filter(|point| normalize_portfolio_wallet(&point.source_wallet) == wallet)
            .map(|point| point.pnl)
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

pub fn normalize_position_outcome(outcome: &str) -> String {
    outcome.trim().to_ascii_uppercase()
}

pub fn position_outcome_is_unknown(outcome: &str) -> bool {
    let outcome = normalize_position_outcome(outcome);
    outcome.is_empty()
        || matches!(
            outcome.as_str(),
            "UNKNOWN" | "UNK" | "N/A" | "NA" | "NULL" | "NONE" | "UNRESOLVED"
        )
}

fn select_safe_exit_fallback(
    candidates: Vec<&PortfolioPosition>,
) -> Option<(&PortfolioPosition, String)> {
    if candidates.len() == 1 {
        return Some((
            candidates[0],
            "same_wallet_same_condition_single_candidate".to_owned(),
        ));
    }

    None
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
    #[serde(default)]
    pub market_type: MarketType,
    pub side: String,
    pub source_trade_timestamp_unix: i64,
    pub source_price: Decimal,
    pub filled_price: Decimal,
    #[serde(default)]
    pub entry_slippage_pct: Decimal,
    #[serde(default)]
    pub conviction_score: Decimal,
    #[serde(default)]
    pub wallet_alpha_score: Decimal,
    #[serde(default)]
    pub entry_notional: Decimal,
    #[serde(default)]
    pub sizing_bucket: String,
    pub filled_size: Decimal,
    pub execution_mode: String,
    pub open_time: DateTime<Utc>,
    pub close_time: Option<DateTime<Utc>>,
    #[serde(default)]
    pub close_reason: Option<String>,
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
    #[serde(default)]
    pub exit_event_counts: BTreeMap<String, u64>,
    #[serde(default)]
    pub stale_position_counts: BTreeMap<String, u64>,
    #[serde(default)]
    pub close_failed_by_reason: BTreeMap<String, u64>,
    pub raw_copy_rate: Decimal,
    pub executable_entry_copy_rate: Decimal,
    pub executable_total_copy_rate: Decimal,
    pub pnl_current_window: Decimal,
    pub pnl_all_open_positions: Decimal,
    #[serde(default)]
    pub pnl_by_source_wallet: BTreeMap<String, Decimal>,
    #[serde(default)]
    pub win_rate_by_wallet: BTreeMap<String, Decimal>,
    #[serde(default)]
    pub wallet_alpha_scores: BTreeMap<String, Decimal>,
    #[serde(default)]
    pub expectancy_by_wallet: BTreeMap<String, Decimal>,
    #[serde(default)]
    pub pnl_by_trade_day: BTreeMap<String, Decimal>,
    #[serde(default)]
    pub win_rate_by_market_type: BTreeMap<String, Decimal>,
    #[serde(default)]
    pub expectancy_by_market_type: BTreeMap<String, Decimal>,
    #[serde(default)]
    pub expectancy_by_conviction_bucket: BTreeMap<String, Decimal>,
    #[serde(default)]
    pub pnl_by_sizing_bucket: BTreeMap<String, Decimal>,
    #[serde(default)]
    pub average_size_by_conviction_bucket: BTreeMap<String, Decimal>,
    #[serde(default)]
    pub average_winner: Decimal,
    #[serde(default)]
    pub average_loser: Decimal,
    #[serde(default)]
    pub payoff_ratio: Decimal,
    #[serde(default)]
    pub expectancy_per_trade: Decimal,
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
    pub current_equity: Decimal,
    #[serde(default)]
    pub starting_equity: Decimal,
    #[serde(default)]
    pub peak_equity: Decimal,
    #[serde(default)]
    pub current_drawdown_pct: Decimal,
    #[serde(default)]
    pub rolling_drawdown_pct: Decimal,
    #[serde(default)]
    pub open_exposure_total: Decimal,
    #[serde(default)]
    pub open_positions_count: usize,
    #[serde(default)]
    pub risk_utilization_pct: Decimal,
    #[serde(default)]
    pub consecutive_losses: u32,
    #[serde(default)]
    pub rolling_loss_count: u32,
    #[serde(default)]
    pub loss_cooldown_active: bool,
    #[serde(default)]
    pub drawdown_guard_active: bool,
    #[serde(default)]
    pub hard_stop_active: bool,
    #[serde(default)]
    pub risk_event_counts: BTreeMap<String, u64>,
    #[serde(default)]
    pub close_type_counts: BTreeMap<String, u64>,
    #[serde(default)]
    pub close_type_share: BTreeMap<String, Decimal>,
    #[serde(default)]
    pub pnl_by_close_type: BTreeMap<String, Decimal>,
    #[serde(default)]
    pub profit_protection_exit_pnl: Decimal,
    #[serde(default)]
    pub filtered_entry_pct: Decimal,
    #[serde(default)]
    pub entry_slippage_p50_pct: Decimal,
    #[serde(default)]
    pub entry_slippage_p90_pct: Decimal,
    #[serde(default)]
    pub win_rate_by_close_type: BTreeMap<String, Decimal>,
    #[serde(default)]
    pub median_hold_ms_by_close_type: BTreeMap<String, u64>,
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
            exit_event_counts: BTreeMap::new(),
            stale_position_counts: BTreeMap::new(),
            close_failed_by_reason: BTreeMap::new(),
            raw_copy_rate: Decimal::ZERO,
            executable_entry_copy_rate: Decimal::ZERO,
            executable_total_copy_rate: Decimal::ZERO,
            pnl_current_window: Decimal::ZERO,
            pnl_all_open_positions: Decimal::ZERO,
            pnl_by_source_wallet: BTreeMap::new(),
            win_rate_by_wallet: BTreeMap::new(),
            pnl_by_trade_day: BTreeMap::new(),
            win_rate_by_market_type: BTreeMap::new(),
            expectancy_by_wallet: BTreeMap::new(),
            expectancy_by_market_type: BTreeMap::new(),
            expectancy_by_conviction_bucket: BTreeMap::new(),
            pnl_by_sizing_bucket: BTreeMap::new(),
            average_size_by_conviction_bucket: BTreeMap::new(),
            average_winner: Decimal::ZERO,
            average_loser: Decimal::ZERO,
            payoff_ratio: Decimal::ZERO,
            expectancy_per_trade: Decimal::ZERO,
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
            current_equity: Decimal::ZERO,
            starting_equity: Decimal::ZERO,
            peak_equity: Decimal::ZERO,
            current_drawdown_pct: Decimal::ZERO,
            rolling_drawdown_pct: Decimal::ZERO,
            open_exposure_total: Decimal::ZERO,
            open_positions_count: 0,
            risk_utilization_pct: Decimal::ZERO,
            consecutive_losses: 0,
            rolling_loss_count: 0,
            loss_cooldown_active: false,
            drawdown_guard_active: false,
            hard_stop_active: false,
            risk_event_counts: BTreeMap::new(),
            close_type_counts: BTreeMap::new(),
            close_type_share: BTreeMap::new(),
            pnl_by_close_type: BTreeMap::new(),
            profit_protection_exit_pnl: Decimal::ZERO,
            filtered_entry_pct: Decimal::ZERO,
            entry_slippage_p50_pct: Decimal::ZERO,
            entry_slippage_p90_pct: Decimal::ZERO,
            win_rate_by_close_type: BTreeMap::new(),
            median_hold_ms_by_close_type: BTreeMap::new(),
            wallet_alpha_scores: BTreeMap::new(),
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
    pub hot_entry_queue_depth: usize,
    pub hot_exit_queue_depth: usize,
    pub cold_path_queue_depth: usize,
    pub dropped_diagnostics: u64,
    pub degradation_mode: bool,
    pub last_skip_processing_ms: u64,
    pub last_skip_reason_code: Option<String>,
    pub trading_paused: bool,
    pub last_latency_pause_reason: Option<String>,
    pub latency_fail_safe_trips: u64,
    pub last_error: Option<String>,
    pub last_portfolio_value: Option<Decimal>,
    pub last_update_iso: Option<DateTime<Utc>>,
}
