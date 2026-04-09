use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::RwLock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::{DateTime, TimeDelta, Utc};
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;
use tokio::time::{MissedTickBehavior, interval};
use tracing::warn;

use crate::config::Settings;
use crate::models::{
    ActivityEntry, PortfolioPosition, PortfolioSnapshot, PositionKey, PositionState,
    ResolvedPosition,
};
use crate::wallet::wallet_matching::MatchedTrackedTrade;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum ResolverPositionState {
    PendingOpen,
    Open,
    Closing,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ResolverPositionRecord {
    pub key: PositionKey,
    pub asset: String,
    pub title: String,
    pub state: ResolverPositionState,
    pub registered_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub pending_expires_at: Option<DateTime<Utc>>,
    pub opened_at: Option<DateTime<Utc>>,
    pub source_trade_timestamp_unix: i64,
    pub size: rust_decimal::Decimal,
    pub current_value: rust_decimal::Decimal,
    pub average_entry_price: rust_decimal::Decimal,
    pub closing_reason: Option<String>,
}

#[derive(Clone, Debug)]
pub struct ResolvedResolverPosition {
    pub key: PositionKey,
    pub state: ResolverPositionState,
    pub fallback_reason: Option<String>,
}

#[derive(Clone, Debug)]
pub enum ResolveResult {
    FoundPendingOpen(ResolvedResolverPosition),
    FoundOpen(ResolvedResolverPosition),
    FoundClosing(ResolvedResolverPosition),
    NotFound,
    Ambiguous,
}

#[derive(Clone, Debug, Default)]
pub struct PendingBindResult {
    pub already_bound: bool,
    pub bound_exit_count: usize,
}

#[derive(Clone, Debug, Default)]
pub struct RemovedResolverPosition {
    pub bound_exits: Vec<MatchedTrackedTrade>,
}

#[derive(Clone, Debug)]
pub struct ExpiredPendingPosition {
    pub key: PositionKey,
    pub bound_exits: Vec<MatchedTrackedTrade>,
}

#[derive(Default)]
struct ResolverState {
    positions: HashMap<PositionKey, ResolverPositionRecord>,
    bound_exits: HashMap<PositionKey, BTreeMap<String, MatchedTrackedTrade>>,
}

#[derive(Clone)]
pub struct PositionResolver {
    state: Arc<RwLock<ResolverState>>,
    path: Arc<PathBuf>,
    pending_open_ttl: Duration,
    dirty: Arc<AtomicBool>,
    flush_notify: Arc<Notify>,
}

impl PositionResolver {
    pub fn load(settings: &Settings) -> Result<Self> {
        let path = settings.data_dir.join("position-resolver.json");
        let now = Utc::now();
        let positions = load_positions(&path)?
            .into_iter()
            .filter(|record| !pending_record_expired(record, now))
            .map(|record| (record.key.clone(), record))
            .collect::<HashMap<_, _>>();

        let resolver = Self {
            state: Arc::new(RwLock::new(ResolverState {
                positions,
                bound_exits: HashMap::new(),
            })),
            path: Arc::new(path),
            pending_open_ttl: settings.position_pending_open_ttl,
            dirty: Arc::new(AtomicBool::new(false)),
            flush_notify: Arc::new(Notify::new()),
        };
        resolver.spawn_flusher(settings.persistence_flush_interval);
        Ok(resolver)
    }

    pub fn register_pending_open(
        &self,
        position_key: PositionKey,
        entry: &ActivityEntry,
    ) -> Result<bool> {
        let now = Utc::now();
        let pending_expires_at =
            now + TimeDelta::from_std(self.pending_open_ttl).unwrap_or_else(|_| TimeDelta::zero());
        let record = ResolverPositionRecord {
            key: position_key.clone(),
            asset: entry.asset.clone(),
            title: entry.title.clone(),
            state: ResolverPositionState::PendingOpen,
            registered_at: now,
            updated_at: now,
            pending_expires_at: Some(pending_expires_at),
            opened_at: None,
            source_trade_timestamp_unix: entry.timestamp,
            size: rust_decimal::Decimal::ZERO,
            current_value: rust_decimal::Decimal::ZERO,
            average_entry_price: rust_decimal::Decimal::ZERO,
            closing_reason: None,
        };

        let mut state = self.state.write().expect("position resolver write lock");
        let newly_registered = !state.positions.contains_key(&position_key)
            || matches!(
                state
                    .positions
                    .get(&position_key)
                    .map(|record| record.state),
                None | Some(ResolverPositionState::PendingOpen)
            );
        state.positions.insert(position_key, record);
        drop(state);
        self.schedule_persist();
        Ok(newly_registered)
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub fn promote_to_open(
        &self,
        position_key: &PositionKey,
        position: &PortfolioPosition,
    ) -> Result<Vec<MatchedTrackedTrade>> {
        let mut state = self.state.write().expect("position resolver write lock");
        let now = Utc::now();
        let existing = state.positions.get(position_key).cloned();
        state.positions.insert(
            position_key.clone(),
            ResolverPositionRecord {
                key: position_key.clone(),
                asset: position.asset.clone(),
                title: position.title.clone(),
                state: portfolio_position_state(position),
                registered_at: existing
                    .as_ref()
                    .map(|record| record.registered_at)
                    .unwrap_or(now),
                updated_at: now,
                pending_expires_at: None,
                opened_at: position.opened_at,
                source_trade_timestamp_unix: position.source_trade_timestamp_unix,
                size: position.size,
                current_value: position.current_value,
                average_entry_price: position.average_entry_price,
                closing_reason: position.closing_reason.clone(),
            },
        );
        let released = state
            .bound_exits
            .remove(position_key)
            .map(|bound| bound.into_values().collect())
            .unwrap_or_default();
        drop(state);
        self.schedule_persist();
        Ok(released)
    }

    pub fn sync_from_portfolio(
        &self,
        snapshot: &PortfolioSnapshot,
    ) -> Result<Vec<MatchedTrackedTrade>> {
        let now = Utc::now();
        let mut released = Vec::new();
        let mut state = self.state.write().expect("position resolver write lock");
        let mut active_keys = HashSet::new();

        for position in snapshot
            .positions
            .iter()
            .filter(|position| position.is_active())
        {
            let key = position.position_key();
            active_keys.insert(key.clone());
            let existing = state.positions.get(&key).cloned();
            let next_state = portfolio_position_state(position);
            state.positions.insert(
                key.clone(),
                ResolverPositionRecord {
                    key: key.clone(),
                    asset: position.asset.clone(),
                    title: position.title.clone(),
                    state: next_state,
                    registered_at: existing
                        .as_ref()
                        .map(|record| record.registered_at)
                        .unwrap_or(now),
                    updated_at: now,
                    pending_expires_at: None,
                    opened_at: position.opened_at,
                    source_trade_timestamp_unix: position.source_trade_timestamp_unix,
                    size: position.size,
                    current_value: position.current_value,
                    average_entry_price: position.average_entry_price,
                    closing_reason: position.closing_reason.clone(),
                },
            );

            if existing
                .as_ref()
                .is_some_and(|record| record.state == ResolverPositionState::PendingOpen)
                && next_state == ResolverPositionState::Open
                && let Some(bound) = state.bound_exits.remove(&key)
            {
                released.extend(bound.into_values());
            }
        }

        state.positions.retain(|key, record| {
            active_keys.contains(key) || record.state == ResolverPositionState::PendingOpen
        });
        let retained_keys = state.positions.keys().cloned().collect::<HashSet<_>>();
        state
            .bound_exits
            .retain(|key, _| retained_keys.contains(key));
        drop(state);

        self.schedule_persist();
        Ok(released)
    }

    pub fn mark_closing(&self, position_key: &PositionKey, reason: &str) -> Result<bool> {
        let mut state = self.state.write().expect("position resolver write lock");
        let Some(record) = state.positions.get_mut(position_key) else {
            return Ok(false);
        };
        record.state = ResolverPositionState::Closing;
        record.updated_at = Utc::now();
        record.pending_expires_at = None;
        record.closing_reason = Some(reason.to_owned());
        drop(state);
        self.schedule_persist();
        Ok(true)
    }

    pub fn resolve_owned_position(
        &self,
        entry: &ActivityEntry,
        now: DateTime<Utc>,
    ) -> ResolveResult {
        let state = self.state.read().expect("position resolver read lock");
        let requested_key = entry.position_key();

        if let Some(record) = state
            .positions
            .get(&requested_key)
            .filter(|record| !pending_record_expired(record, now))
        {
            return resolve_result_from_record(record.clone(), None);
        }

        let source_wallet = requested_key.source_wallet.clone();
        let condition_id = requested_key.condition_id.clone();
        let mut candidates = state
            .positions
            .values()
            .filter(|record| {
                !pending_record_expired(record, now)
                    && record.key.source_wallet == source_wallet
                    && record.key.condition_id == condition_id
            })
            .cloned()
            .collect::<Vec<_>>();

        if candidates.is_empty() {
            return ResolveResult::NotFound;
        }

        if candidates.len() > 1 {
            return ResolveResult::Ambiguous;
        }

        resolve_result_from_record(
            candidates.remove(0),
            Some("same_wallet_same_condition_single_candidate".to_owned()),
        )
    }

    pub fn bind_exit_to_pending(
        &self,
        position_key: &PositionKey,
        matched_trade: MatchedTrackedTrade,
    ) -> Result<PendingBindResult> {
        let mut state = self.state.write().expect("position resolver write lock");
        let Some(record) = state.positions.get(position_key) else {
            return Ok(PendingBindResult::default());
        };
        if record.state != ResolverPositionState::PendingOpen {
            return Ok(PendingBindResult::default());
        }

        let bound = state.bound_exits.entry(position_key.clone()).or_default();
        let already_bound = bound
            .insert(matched_trade.entry.dedupe_key(), matched_trade)
            .is_some();
        Ok(PendingBindResult {
            already_bound,
            bound_exit_count: bound.len(),
        })
    }

    pub fn has_resolvable_position(&self, key: &PositionKey) -> bool {
        let now = Utc::now();
        self.state
            .read()
            .expect("position resolver read lock")
            .positions
            .get(key)
            .is_some_and(|record| !pending_record_expired(record, now))
    }

    pub fn is_closing(&self, key: &PositionKey) -> bool {
        self.state
            .read()
            .expect("position resolver read lock")
            .positions
            .get(key)
            .is_some_and(|record| record.state == ResolverPositionState::Closing)
    }

    pub fn resolved_position(&self, key: &PositionKey) -> Option<ResolvedPosition> {
        let record = self
            .state
            .read()
            .expect("position resolver read lock")
            .positions
            .get(key)
            .cloned()?;
        if matches!(record.state, ResolverPositionState::PendingOpen) || record.size.is_zero() {
            return None;
        }
        Some(ResolvedPosition {
            key: record.key.clone(),
            asset: record.asset,
            outcome: record.key.outcome.clone(),
            source_wallet: record.key.source_wallet.clone(),
            size: record.size,
            current_value: record.current_value,
            used_fallback: false,
            fallback_reason: None,
        })
    }

    pub fn remove_position(&self, position_key: &PositionKey) -> Result<RemovedResolverPosition> {
        let mut state = self.state.write().expect("position resolver write lock");
        let removed = state.positions.remove(position_key);
        let bound_exits: Vec<MatchedTrackedTrade> = state
            .bound_exits
            .remove(position_key)
            .map(|bound| bound.into_values().collect())
            .unwrap_or_default();
        drop(state);
        if removed.is_some() || !bound_exits.is_empty() {
            self.schedule_persist();
        }
        Ok(RemovedResolverPosition { bound_exits })
    }

    pub fn take_expired_pending(&self, now: DateTime<Utc>) -> Result<Vec<ExpiredPendingPosition>> {
        let mut state = self.state.write().expect("position resolver write lock");
        let expired_keys = state
            .positions
            .iter()
            .filter(|(_, record)| pending_record_expired(record, now))
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();

        let mut expired = Vec::with_capacity(expired_keys.len());
        for key in expired_keys {
            state.positions.remove(&key);
            expired.push(ExpiredPendingPosition {
                key: key.clone(),
                bound_exits: state
                    .bound_exits
                    .remove(&key)
                    .map(|bound| bound.into_values().collect())
                    .unwrap_or_default(),
            });
        }
        drop(state);
        if !expired.is_empty() {
            self.schedule_persist();
        }
        Ok(expired)
    }

    fn schedule_persist(&self) {
        self.dirty.store(true, Ordering::Relaxed);
        self.flush_notify.notify_one();
    }

    fn spawn_flusher(&self, flush_interval: Duration) {
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let state = self.state.clone();
        let path = self.path.clone();
        let dirty = self.dirty.clone();
        let flush_notify = self.flush_notify.clone();
        runtime.spawn(async move {
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

                let positions = state
                    .read()
                    .expect("position resolver read lock")
                    .positions
                    .values()
                    .cloned()
                    .collect::<Vec<_>>();
                if let Err(error) = persist_positions(&path, &positions).await {
                    warn!(
                        ?error,
                        path = %path.display(),
                        "failed to persist position resolver state"
                    );
                    dirty.store(true, Ordering::Relaxed);
                }
            }
        });
    }
}

fn portfolio_position_state(position: &PortfolioPosition) -> ResolverPositionState {
    if position.state == PositionState::Closing {
        ResolverPositionState::Closing
    } else {
        ResolverPositionState::Open
    }
}

fn resolve_result_from_record(
    record: ResolverPositionRecord,
    fallback_reason: Option<String>,
) -> ResolveResult {
    let resolved = ResolvedResolverPosition {
        key: record.key.clone(),
        state: record.state,
        fallback_reason,
    };
    match resolved.state {
        ResolverPositionState::PendingOpen => ResolveResult::FoundPendingOpen(resolved),
        ResolverPositionState::Open => ResolveResult::FoundOpen(resolved),
        ResolverPositionState::Closing => ResolveResult::FoundClosing(resolved),
    }
}

fn pending_record_expired(record: &ResolverPositionRecord, now: DateTime<Utc>) -> bool {
    record.state == ResolverPositionState::PendingOpen
        && record
            .pending_expires_at
            .is_some_and(|expires_at| expires_at <= now)
}

fn load_positions(path: &PathBuf) -> Result<Vec<ResolverPositionRecord>> {
    match std::fs::read_to_string(path) {
        Ok(contents) => serde_json::from_str::<Vec<ResolverPositionRecord>>(&contents)
            .with_context(|| format!("parsing {}", path.display())),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(error) => Err(error).with_context(|| format!("reading {}", path.display())),
    }
}

async fn persist_positions(path: &PathBuf, positions: &[ResolverPositionRecord]) -> Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("creating {}", parent.display()))?;
    }

    let mut sorted = positions.to_vec();
    sorted.sort_by(|left, right| left.key.cmp(&right.key));
    tokio::fs::write(path, serde_json::to_string_pretty(&sorted)?)
        .await
        .with_context(|| format!("writing {}", path.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use rust_decimal_macros::dec;

    use super::*;
    use crate::config::{ExecutionMode, Settings};
    use crate::detection::trade_inference::ConfirmedTradeSignal;
    use crate::execution::ExecutionSide;
    use crate::models::TradeStageTimestamps;

    fn sample_settings(data_dir: std::path::PathBuf) -> Settings {
        Settings {
            execution_mode: ExecutionMode::Paper,
            polymarket_host: "https://example.com".to_owned(),
            polymarket_data_api: "https://example.com".to_owned(),
            polymarket_gamma_api: "https://example.com".to_owned(),
            polymarket_market_ws: "wss://example.com".to_owned(),
            polymarket_user_ws: "wss://example.com".to_owned(),
            polymarket_activity_ws: "wss://example.com".to_owned(),
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
            polymarket_usdc_address: "0x1".to_owned(),
            polymarket_spender_address: "0x2".to_owned(),
            auto_approve_usdc_allowance: false,
            usdc_approval_amount: dec!(1000),
            target_activity_ws_api_key: None,
            target_activity_ws_secret: None,
            target_activity_ws_passphrase: None,
            target_profile_addresses: vec!["0xsource".to_owned()],
            start_capital_usd: dec!(200),
            paper_execution_delay: Duration::ZERO,
            copy_only_new_trades: true,
            source_trades_limit: 50,
            http_timeout: Duration::from_secs(2),
            market_cache_ttl: Duration::from_secs(10),
            market_raw_ring_capacity: 128,
            market_parser_workers: 1,
            market_subscription_batch_size: 1,
            market_subscription_delay: Duration::from_millis(1),
            wallet_ring_capacity: 128,
            wallet_parser_workers: 1,
            wallet_subscription_batch_size: 1,
            wallet_subscription_delay: Duration::from_millis(1),
            hot_path_mode: true,
            hot_path_queue_capacity: 32,
            cold_path_queue_capacity: 128,
            attribution_fast_cache_capacity: 64,
            persistence_flush_interval: Duration::from_millis(250),
            analytics_flush_interval: Duration::from_millis(500),
            telegram_async_only: true,
            fast_risk_only_on_hot_path: true,
            exit_priority_strict: true,
            parse_tasks_market: 1,
            parse_tasks_wallet: 1,
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
            min_source_trade_usdc: dec!(1),
            max_market_spread_bps: 500,
            enable_ultra_short_markets: false,
            min_visible_liquidity: dec!(50),
            max_spread_bps: 500,
            max_entry_slippage: dec!(0.03),
            min_top_of_book_ratio: dec!(1),
            max_slippage_bps: 300,
            max_source_price_slippage_bps: 200,
            latency_fail_safe_enabled: true,
            max_latency: Duration::from_millis(500),
            average_latency_threshold: Duration::from_millis(350),
            latency_monitor_interval: Duration::from_millis(50),
            latency_reconnect_settle: Duration::from_millis(750),
            prediction_validation_timeout: Duration::from_millis(500),
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
            max_copy_delay_ms: 1_500,
            min_liquidity: dec!(50),
            min_wallet_score: dec!(0.6),
            min_wallet_avg_hold_ms: 15_000,
            max_wallet_trades_per_min: 6,
            market_cooldown: Duration::from_secs(15),
            min_trade_quality_score: dec!(0.65),
            max_position_age_hours: 6,
            max_hold_time_seconds: 1_800,
            enable_exit_retry: true,
            exit_retry_window: Duration::from_secs(30),
            exit_retry_interval: Duration::from_millis(500),
            unresolved_exit_initial_retry: Duration::from_millis(250),
            unresolved_exit_total_window: Duration::from_secs(30),
            unresolved_exit_max_retry: Duration::from_secs(4),
            position_pending_open_ttl: Duration::from_secs(20),
            rpc_global_rate_limit_per_second: 10,
            rpc_per_market_rate_limit_per_second: 3,
            closing_max_age: Duration::from_secs(30),
            force_exit_on_closing_timeout: true,
            telegram_bot_token: "token".to_owned(),
            telegram_chat_id: "chat".to_owned(),
            health_port: 3000,
            data_dir,
        }
    }

    fn sample_entry(side: &str, outcome: &str) -> ActivityEntry {
        ActivityEntry {
            proxy_wallet: "0xsource".to_owned(),
            timestamp: Utc::now().timestamp_millis(),
            condition_id: "condition-1".to_owned(),
            type_name: "TRADE".to_owned(),
            size: 10.0,
            usdc_size: 5.0,
            transaction_hash: format!("0xhash-{side}-{outcome}"),
            price: 0.5,
            asset: "asset-1".to_owned(),
            side: side.to_owned(),
            outcome_index: 0,
            title: "Sample".to_owned(),
            slug: "sample".to_owned(),
            event_slug: "sample".to_owned(),
            outcome: outcome.to_owned(),
        }
    }

    fn sample_signal(side: ExecutionSide) -> ConfirmedTradeSignal {
        let now = Utc::now();
        let instant = std::time::Instant::now();
        ConfirmedTradeSignal {
            asset_id: "asset-1".to_owned(),
            condition_id: "condition-1".to_owned(),
            transaction_hash: Some("0xsignal".to_owned()),
            side,
            price: 0.5,
            estimated_size: 10.0,
            stage_timestamps: TradeStageTimestamps {
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
            generation: 1,
        }
    }

    fn sample_position() -> PortfolioPosition {
        PortfolioPosition {
            asset: "asset-1".to_owned(),
            condition_id: "condition-1".to_owned(),
            title: "Sample".to_owned(),
            outcome: "YES".to_owned(),
            source_wallet: "0xsource".to_owned(),
            state: PositionState::Open,
            size: dec!(10),
            current_value: dec!(5),
            average_entry_price: dec!(0.5),
            current_price: dec!(0.5),
            cost_basis: dec!(5),
            unrealized_pnl: dec!(0),
            opened_at: Some(Utc::now()),
            source_trade_timestamp_unix: Utc::now().timestamp_millis(),
            closing_started_at: None,
            closing_reason: None,
            last_close_attempt_at: None,
            close_attempts: 0,
            close_failure_reason: None,
            closing_escalation_level: 0,
            stale_reason: None,
        }
    }

    #[test]
    fn exit_binds_to_pending_and_releases_when_position_opens() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let resolver = PositionResolver::load(&sample_settings(temp_dir.path().to_path_buf()))
            .expect("resolver");
        let entry = sample_entry("BUY", "YES");
        let key = entry.position_key();
        resolver
            .register_pending_open(key.clone(), &entry)
            .expect("pending");

        let exit_trade = MatchedTrackedTrade {
            entry: sample_entry("SELL", "YES"),
            signal: sample_signal(ExecutionSide::Sell),
            source: "test",
            validation_correlation_kind: None,
            validation_match_window: None,
            tx_hash_matched: true,
            validation_signal: None,
        };

        assert!(matches!(
            resolver.resolve_owned_position(&exit_trade.entry, Utc::now()),
            ResolveResult::FoundPendingOpen(_)
        ));
        let bind = resolver
            .bind_exit_to_pending(&key, exit_trade.clone())
            .expect("bind exit");
        assert!(!bind.already_bound);
        assert_eq!(bind.bound_exit_count, 1);

        let released = resolver
            .promote_to_open(&key, &sample_position())
            .expect("promote");
        assert_eq!(released.len(), 1);
        assert_eq!(
            released[0].entry.transaction_hash,
            exit_trade.entry.transaction_hash
        );
        assert!(matches!(
            resolver.resolve_owned_position(&exit_trade.entry, Utc::now()),
            ResolveResult::FoundOpen(_)
        ));
    }

    #[test]
    fn same_wallet_same_condition_fallback_resolves_single_candidate() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let resolver = PositionResolver::load(&sample_settings(temp_dir.path().to_path_buf()))
            .expect("resolver");
        let key = PositionKey::new("condition-1", "YES", "0xsource");
        resolver
            .promote_to_open(&key, &sample_position())
            .expect("promote");

        let mismatched_exit = sample_entry("SELL", "NO");
        let resolution = resolver.resolve_owned_position(&mismatched_exit, Utc::now());
        match resolution {
            ResolveResult::FoundOpen(resolved) => {
                assert_eq!(resolved.key, key);
                assert_eq!(
                    resolved.fallback_reason.as_deref(),
                    Some("same_wallet_same_condition_single_candidate")
                );
            }
            other => panic!("expected fallback match, got {other:?}"),
        }
    }

    #[test]
    fn duplicate_bound_exit_is_deduplicated() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let resolver = PositionResolver::load(&sample_settings(temp_dir.path().to_path_buf()))
            .expect("resolver");
        let entry = sample_entry("BUY", "YES");
        let key = entry.position_key();
        resolver
            .register_pending_open(key.clone(), &entry)
            .expect("pending");

        let exit_trade = MatchedTrackedTrade {
            entry: sample_entry("SELL", "YES"),
            signal: sample_signal(ExecutionSide::Sell),
            source: "test",
            validation_correlation_kind: None,
            validation_match_window: None,
            tx_hash_matched: true,
            validation_signal: None,
        };

        let first = resolver
            .bind_exit_to_pending(&key, exit_trade.clone())
            .expect("first bind");
        let second = resolver
            .bind_exit_to_pending(&key, exit_trade)
            .expect("second bind");

        assert!(!first.already_bound);
        assert!(second.already_bound);
        assert_eq!(second.bound_exit_count, 1);
    }
}
