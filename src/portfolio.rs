use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, TimeZone, Utc};
use reqwest::Client;
use rust_decimal::Decimal;
use serde_json::Value;
use tokio::sync::{Notify, RwLock};
use tokio::time::{MissedTickBehavior, interval};
use tracing::warn;

use crate::config::{ExecutionMode, Settings};
use crate::execution::{ExecutionSide, ExecutionStatus, ExecutionSuccess};
use crate::models::{
    ActivityEntry, EquityPoint, PortfolioPosition, PortfolioSnapshot, PositionEntry, PositionKey,
    PositionState, RealizedTradePoint,
};
use crate::orderbook::orderbook_state::{MarketSnapshot, OrderBookState};

const ROLLING_DRAWDOWN_WINDOW: Duration = Duration::from_secs(5 * 60);
const EQUITY_SAMPLE_MIN_INTERVAL: Duration = Duration::from_secs(1);
const RECENT_REALIZED_TRADE_LIMIT: usize = 20;

#[derive(Clone)]
struct PortfolioRiskConfig {
    start_capital_usd: Decimal,
    max_drawdown_pct: Decimal,
    hard_stop_drawdown_pct: Decimal,
    max_consecutive_losses: u32,
    loss_cooldown: Duration,
    force_close_on_hard_stop: bool,
}

#[derive(Clone)]
pub struct PortfolioService {
    mode: ExecutionMode,
    client: Client,
    data_api: String,
    profile_address: Option<String>,
    snapshot_path: PathBuf,
    cache: Arc<RwLock<Option<PortfolioSnapshot>>>,
    persistence_dirty: Arc<AtomicBool>,
    flush_notify: Arc<Notify>,
    start_capital_usd: Decimal,
    risk_config: PortfolioRiskConfig,
    paper_orderbooks: Option<Arc<OrderBookState>>,
    max_position_age_hours: u64,
}

impl PortfolioService {
    pub fn new(settings: Settings, paper_orderbooks: Option<Arc<OrderBookState>>) -> Self {
        let client = Client::builder()
            .connect_timeout(settings.http_timeout / 2)
            .timeout(settings.http_timeout)
            .pool_max_idle_per_host(4)
            .pool_idle_timeout(Duration::from_secs(30))
            .tcp_keepalive(Duration::from_secs(30))
            .http2_adaptive_window(true)
            .user_agent("polymarket-copy-bot/0.1.0")
            .build()
            .expect("reqwest client");

        let service = Self {
            mode: settings.execution_mode,
            client,
            data_api: settings.polymarket_data_api,
            profile_address: settings.polymarket_profile_address,
            snapshot_path: settings.data_dir.join("portfolio-summary.json"),
            cache: Arc::new(RwLock::new(None)),
            persistence_dirty: Arc::new(AtomicBool::new(false)),
            flush_notify: Arc::new(Notify::new()),
            start_capital_usd: settings.start_capital_usd,
            risk_config: PortfolioRiskConfig {
                start_capital_usd: settings.start_capital_usd,
                max_drawdown_pct: settings.max_drawdown_pct.max(Decimal::ZERO),
                hard_stop_drawdown_pct: settings.hard_stop_drawdown_pct.max(Decimal::ZERO),
                max_consecutive_losses: settings.max_consecutive_losses,
                loss_cooldown: settings.loss_cooldown,
                force_close_on_hard_stop: settings.force_close_on_hard_stop,
            },
            paper_orderbooks,
            max_position_age_hours: settings.max_position_age_hours,
        };
        service.spawn_flusher(settings.persistence_flush_interval);
        service
    }

    pub async fn snapshot(&self) -> Option<PortfolioSnapshot> {
        self.cache.read().await.clone()
    }

    pub async fn refresh_snapshot(&self) -> Result<PortfolioSnapshot> {
        self.refresh_snapshot_with_layout_hint(None).await
    }

    pub async fn refresh_snapshot_with_layout_hint(
        &self,
        layout_hint: Option<&PortfolioSnapshot>,
    ) -> Result<PortfolioSnapshot> {
        let mut snapshot = match self.mode {
            ExecutionMode::Live => {
                let mut fetched = self.fetch_live_positions().await?;
                let layout = match layout_hint {
                    Some(layout) => Some(layout.clone()),
                    None => self.load_wallet_layout_snapshot().await?,
                };
                if let Some(layout) = layout.as_ref() {
                    rehydrate_live_wallet_positions(&mut fetched, layout);
                    restore_live_accounting_from_layout(&mut fetched, layout);
                    if layout_hint.is_some()
                        && preserve_projected_layout_if_live_refresh_stale(&mut fetched, layout)
                    {
                        warn!(
                            "live portfolio refresh returned stale tracked-position sizes; preserving projected layout"
                        );
                    }
                }
                fetched
            }
            ExecutionMode::Paper => self.load_or_initialize_paper_snapshot().await?,
        };
        self.revalue_snapshot(&mut snapshot).await;
        snapshot.cleanup_stale_positions(self.max_position_age_hours);
        apply_risk_state(&mut snapshot, &self.risk_config);
        *self.cache.write().await = Some(snapshot.clone());
        Ok(snapshot)
    }

    pub async fn refresh_and_persist(&self) -> Result<PortfolioSnapshot> {
        self.refresh_and_persist_with_layout_hint(None).await
    }

    pub async fn refresh_and_persist_with_layout_hint(
        &self,
        layout_hint: Option<&PortfolioSnapshot>,
    ) -> Result<PortfolioSnapshot> {
        let snapshot = self.refresh_snapshot_with_layout_hint(layout_hint).await?;
        self.store_snapshot(snapshot.clone()).await?;
        Ok(snapshot)
    }

    pub async fn store_snapshot(
        &self,
        mut snapshot: PortfolioSnapshot,
    ) -> Result<PortfolioSnapshot> {
        normalize_snapshot(&mut snapshot);
        snapshot.cleanup_stale_positions(self.max_position_age_hours);
        apply_risk_state(&mut snapshot, &self.risk_config);
        *self.cache.write().await = Some(snapshot.clone());
        self.schedule_persist();
        Ok(snapshot)
    }

    pub fn project_fill_on_snapshot(
        &self,
        snapshot: &PortfolioSnapshot,
        source: &ActivityEntry,
        result: &ExecutionSuccess,
        position_key_hint: Option<&PositionKey>,
    ) -> Result<PortfolioSnapshot> {
        let mut projected = snapshot.clone();
        apply_position_fill(&mut projected, source, result, false, position_key_hint)?;
        projected.cleanup_stale_positions(self.max_position_age_hours);
        apply_risk_state(&mut projected, &self.risk_config);
        Ok(projected)
    }

    pub async fn apply_paper_fill(
        &self,
        source: &ActivityEntry,
        result: &ExecutionSuccess,
        position_key_hint: Option<&PositionKey>,
    ) -> Result<PortfolioSnapshot> {
        if self.mode != ExecutionMode::Paper {
            return Err(anyhow!("paper fill requested while running in live mode"));
        }

        let mut snapshot = match self.snapshot().await {
            Some(snapshot) => snapshot,
            None => self.load_or_initialize_paper_snapshot().await?,
        };
        apply_paper_trade(&mut snapshot, source, result, position_key_hint)?;
        self.revalue_snapshot(&mut snapshot).await;
        snapshot.cleanup_stale_positions(self.max_position_age_hours);
        apply_risk_state(&mut snapshot, &self.risk_config);
        *self.cache.write().await = Some(snapshot.clone());
        self.schedule_persist();
        Ok(snapshot)
    }

    pub fn force_close_on_hard_stop(&self) -> bool {
        self.risk_config.force_close_on_hard_stop
    }

    async fn fetch_live_positions(&self) -> Result<PortfolioSnapshot> {
        let profile_address = self
            .profile_address
            .as_deref()
            .ok_or_else(|| anyhow!("POLYMARKET_PROFILE_ADDRESS is required in live mode"))?;
        let value_response = self
            .client
            .get(format!("{}/value", self.data_api))
            .query(&[("user", profile_address)])
            .send()
            .await
            .context("requesting value endpoint")?
            .error_for_status()
            .context("value endpoint returned error")?
            .json::<Value>()
            .await
            .context("decoding value response")?;

        let total_value = match value_response {
            Value::Array(items) => items
                .first()
                .and_then(|entry| entry.get("value"))
                .and_then(|field| field.as_f64())
                .and_then(Decimal::from_f64_retain)
                .unwrap_or(Decimal::ZERO),
            Value::Object(object) => object
                .get("value")
                .and_then(|field| field.as_f64())
                .and_then(Decimal::from_f64_retain)
                .unwrap_or(Decimal::ZERO),
            _ => Decimal::ZERO,
        };

        let positions = self
            .client
            .get(format!("{}/positions", self.data_api))
            .query(&[
                ("user", profile_address),
                ("limit", "500"),
                ("sortBy", "CURRENT"),
                ("sortDirection", "DESC"),
                ("sizeThreshold", "0"),
            ])
            .send()
            .await
            .context("requesting positions endpoint")?
            .error_for_status()
            .context("positions endpoint returned error")?
            .json::<Vec<PositionEntry>>()
            .await
            .context("decoding positions response")?;

        let mut exposure = Decimal::ZERO;
        let mut normalized = Vec::with_capacity(positions.len());
        for position in positions {
            let current_value = position.current_value_decimal()?;
            let size = position.size_decimal()?;
            exposure += current_value;
            let current_price = decimal_price_from_value(size, current_value);
            normalized.push(PortfolioPosition {
                asset: position.asset,
                condition_id: position.condition_id,
                title: position.title,
                outcome: position.outcome,
                source_wallet: String::new(),
                state: PositionState::Open,
                size,
                current_value,
                average_entry_price: current_price,
                current_price,
                cost_basis: current_value,
                unrealized_pnl: Decimal::ZERO,
                opened_at: None,
                source_trade_timestamp_unix: 0,
                closing_started_at: None,
                closing_reason: None,
                last_close_attempt_at: None,
                close_attempts: 0,
                close_failure_reason: None,
                closing_escalation_level: 0,
                stale_reason: None,
            });
        }

        Ok(PortfolioSnapshot {
            fetched_at: Utc::now(),
            total_value,
            total_exposure: exposure,
            cash_balance: (total_value - exposure).max(Decimal::ZERO),
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
            positions: normalized,
        })
    }

    async fn load_or_initialize_paper_snapshot(&self) -> Result<PortfolioSnapshot> {
        match tokio::fs::read_to_string(&self.snapshot_path).await {
            Ok(contents) => serde_json::from_str::<PortfolioSnapshot>(&contents)
                .with_context(|| format!("parsing {}", self.snapshot_path.display())),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                Ok(initial_paper_snapshot(self.start_capital_usd))
            }
            Err(error) => {
                Err(error).with_context(|| format!("reading {}", self.snapshot_path.display()))
            }
        }
    }

    pub async fn persist_snapshot(&self, snapshot: &PortfolioSnapshot) -> Result<()> {
        if let Some(parent) = self.snapshot_path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .with_context(|| format!("creating {}", parent.display()))?;
        }
        let body = serde_json::to_string_pretty(snapshot)?;
        tokio::fs::write(&self.snapshot_path, body)
            .await
            .with_context(|| format!("writing {}", self.snapshot_path.display()))?;
        Ok(())
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub async fn flush_persistence(&self) -> Result<()> {
        let snapshot = self.cache.read().await.clone();
        if let Some(snapshot) = snapshot {
            self.persist_snapshot(&snapshot).await?;
        }
        self.persistence_dirty.store(false, Ordering::Relaxed);
        Ok(())
    }

    async fn load_wallet_layout_snapshot(&self) -> Result<Option<PortfolioSnapshot>> {
        if let Some(snapshot) = self.snapshot().await {
            return Ok(Some(snapshot));
        }

        match tokio::fs::read_to_string(&self.snapshot_path).await {
            Ok(contents) => serde_json::from_str::<PortfolioSnapshot>(&contents)
                .map(Some)
                .with_context(|| format!("parsing {}", self.snapshot_path.display())),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => {
                Err(error).with_context(|| format!("reading {}", self.snapshot_path.display()))
            }
        }
    }

    fn schedule_persist(&self) {
        self.persistence_dirty.store(true, Ordering::Relaxed);
        self.flush_notify.notify_one();
    }

    fn spawn_flusher(&self, flush_interval: Duration) {
        let cache = self.cache.clone();
        let snapshot_path = self.snapshot_path.clone();
        let dirty = self.persistence_dirty.clone();
        let flush_notify = self.flush_notify.clone();
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
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
                let snapshot = cache.read().await.clone();
                let Some(snapshot) = snapshot else {
                    continue;
                };
                if let Err(error) = persist_portfolio_snapshot(&snapshot_path, &snapshot).await {
                    warn!(
                        ?error,
                        path = %snapshot_path.display(),
                        "failed to persist portfolio snapshot"
                    );
                    dirty.store(true, Ordering::Relaxed);
                }
            }
        });
    }
}

async fn persist_portfolio_snapshot(path: &PathBuf, snapshot: &PortfolioSnapshot) -> Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("creating {}", parent.display()))?;
    }
    let body = serde_json::to_string_pretty(snapshot)?;
    tokio::fs::write(path, body)
        .await
        .with_context(|| format!("writing {}", path.display()))?;
    Ok(())
}

fn initial_paper_snapshot(start_capital_usd: Decimal) -> PortfolioSnapshot {
    PortfolioSnapshot {
        fetched_at: Utc::now(),
        total_value: start_capital_usd,
        total_exposure: Decimal::ZERO,
        cash_balance: start_capital_usd,
        realized_pnl: Decimal::ZERO,
        unrealized_pnl: Decimal::ZERO,
        starting_equity: start_capital_usd,
        current_equity: start_capital_usd,
        peak_equity: start_capital_usd,
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
        recent_equity_samples: vec![EquityPoint {
            observed_at: Utc::now(),
            equity: start_capital_usd,
        }],
        recent_realized_trade_points: Vec::new(),
        positions: Vec::new(),
    }
}

fn apply_paper_trade(
    snapshot: &mut PortfolioSnapshot,
    source: &ActivityEntry,
    result: &ExecutionSuccess,
    position_key_hint: Option<&PositionKey>,
) -> Result<()> {
    apply_position_fill(snapshot, source, result, true, position_key_hint)
}

fn apply_position_fill(
    snapshot: &mut PortfolioSnapshot,
    source: &ActivityEntry,
    result: &ExecutionSuccess,
    enforce_cash_balance: bool,
    position_key_hint: Option<&PositionKey>,
) -> Result<()> {
    if matches!(result.status, ExecutionStatus::NoFill) || result.filled_size <= Decimal::ZERO {
        return Ok(());
    }

    if matches!(result.status, ExecutionStatus::Rejected) {
        return Err(anyhow!(
            "cannot apply rejected execution result order_id={} status={}",
            result.order_id,
            result.status
        ));
    }

    match result.order_request.side {
        ExecutionSide::Buy => {
            if enforce_cash_balance && snapshot.cash_balance < result.filled_notional {
                return Err(anyhow!(
                    "insufficient paper cash: need {}, have {}",
                    result.filled_notional,
                    snapshot.cash_balance
                ));
            }

            snapshot.cash_balance -= result.filled_notional;
            let position_key = source.position_key();
            if let Some(position) = snapshot
                .positions
                .iter_mut()
                .find(|position| position.position_key() == position_key)
            {
                let existing_cost_basis = position.average_entry_price * position.size;
                let new_size = position.size + result.filled_size;
                let new_cost_basis = existing_cost_basis + result.filled_notional;
                let opened_at = source_trade_opened_at(source);
                position.size = new_size;
                position.average_entry_price = if new_size.is_zero() {
                    Decimal::ZERO
                } else {
                    new_cost_basis / new_size
                };
                position.current_price = result.filled_price;
                position.cost_basis = new_cost_basis;
                position.current_value = position.size * position.current_price;
                position.unrealized_pnl = position.current_value - position.cost_basis;
                position.state = PositionState::Open;
                position.closing_started_at = None;
                position.opened_at = match (position.opened_at, opened_at) {
                    (Some(existing), Some(candidate)) => Some(existing.min(candidate)),
                    (Some(existing), None) => Some(existing),
                    (None, candidate) => candidate,
                };
                if position.source_trade_timestamp_unix == 0 {
                    position.source_trade_timestamp_unix = source.timestamp;
                }
            } else {
                snapshot.positions.push(PortfolioPosition {
                    asset: result.order_request.token_id.clone(),
                    condition_id: source.condition_id.clone(),
                    title: source.title.clone(),
                    outcome: source.outcome.clone(),
                    source_wallet: normalize_wallet(&source.proxy_wallet),
                    state: PositionState::Open,
                    size: result.filled_size,
                    average_entry_price: result.filled_price,
                    current_price: result.filled_price,
                    cost_basis: result.filled_notional,
                    current_value: result.filled_size * result.filled_price,
                    unrealized_pnl: Decimal::ZERO,
                    opened_at: source_trade_opened_at(source),
                    source_trade_timestamp_unix: source.timestamp,
                    closing_started_at: None,
                    closing_reason: None,
                    last_close_attempt_at: None,
                    close_attempts: 0,
                    close_failure_reason: None,
                    closing_escalation_level: 0,
                    stale_reason: None,
                });
            }
        }
        ExecutionSide::Sell => {
            let requested_key = position_key_hint
                .cloned()
                .unwrap_or_else(|| source.position_key());
            let (index, _used_fallback) = resolve_exit_index(
                snapshot,
                &requested_key,
                Some(&result.order_request.token_id),
            )
            .ok_or_else(|| {
                anyhow!(
                    "cannot sell asset {} for wallet {} without a matching position",
                    result.order_request.token_id,
                    requested_key.source_wallet
                )
            })?;
            if snapshot.positions[index].size < result.filled_size {
                return Err(anyhow!(
                    "paper sell size {} exceeds held size {}",
                    result.filled_size,
                    snapshot.positions[index].size
                ));
            }

            let average_entry_price = snapshot.positions[index].average_entry_price;
            let realized_pnl = (result.filled_price - average_entry_price) * result.filled_size;
            snapshot.realized_pnl += realized_pnl;
            record_realized_trade(snapshot, realized_pnl);
            let position = &mut snapshot.positions[index];
            position.state = PositionState::Closing;
            position.closing_started_at = Some(Utc::now());
            position.size -= result.filled_size;
            position.current_price = result.filled_price;
            position.cost_basis = position.size * position.average_entry_price;
            position.current_value = position.size * position.current_price;
            position.unrealized_pnl = position.current_value - position.cost_basis;
            snapshot.cash_balance += result.filled_notional;

            if position.size.is_zero() {
                position.state = PositionState::Closed;
                snapshot.positions.remove(index);
            } else {
                let has_remaining_close_intent = result.filled_size < result.order_request.size
                    || matches!(result.status, ExecutionStatus::PartiallyFilled);
                if has_remaining_close_intent {
                    position.state = PositionState::Closing;
                    position.close_failure_reason = Some("partial_fill_remaining".to_owned());
                } else {
                    position.state = PositionState::Open;
                    position.closing_started_at = None;
                    position.closing_reason = None;
                    position.last_close_attempt_at = None;
                    position.close_attempts = 0;
                    position.close_failure_reason = None;
                    position.closing_escalation_level = 0;
                    position.stale_reason = None;
                }
            }
        }
    }

    recalculate_snapshot_totals(snapshot);
    Ok(())
}

impl PortfolioService {
    async fn revalue_snapshot(&self, snapshot: &mut PortfolioSnapshot) {
        normalize_snapshot(snapshot);
        if self.mode == ExecutionMode::Paper {
            if let Some(orderbooks) = &self.paper_orderbooks {
                for position in &mut snapshot.positions {
                    if let Some(mark_price) = position_mark_price(
                        orderbooks.market_snapshot(&position.asset).await,
                        position.current_price,
                    ) {
                        position.current_price = mark_price;
                    }
                    position.current_value = position.size * position.current_price;
                    position.cost_basis = position.size * position.average_entry_price;
                    position.unrealized_pnl = position.current_value - position.cost_basis;
                }
            }
        }
        recalculate_snapshot_totals(snapshot);
        apply_risk_state(snapshot, &self.risk_config);
    }
}

fn rehydrate_live_wallet_positions(snapshot: &mut PortfolioSnapshot, layout: &PortfolioSnapshot) {
    let mut layout_by_contract: HashMap<(String, String), Vec<&PortfolioPosition>> = HashMap::new();
    for position in &layout.positions {
        layout_by_contract
            .entry(position_contract_key(
                &position.condition_id,
                &position.outcome,
            ))
            .or_default()
            .push(position);
    }

    let mut merged = Vec::with_capacity(snapshot.positions.len());
    for live_position in snapshot.positions.drain(..) {
        let contract_key =
            position_contract_key(&live_position.condition_id, &live_position.outcome);
        let Some(previous_positions) = layout_by_contract.get(&contract_key) else {
            merged.push(live_position);
            continue;
        };

        let mut remaining_size = live_position.size;
        for previous_position in previous_positions {
            if remaining_size <= Decimal::ZERO {
                break;
            }

            let allocated_size = previous_position.size.min(remaining_size);
            if allocated_size <= Decimal::ZERO {
                continue;
            }

            let mut merged_position = (*previous_position).clone();
            merged_position.condition_id = live_position.condition_id.clone();
            merged_position.title = live_position.title.clone();
            merged_position.outcome = live_position.outcome.clone();
            merged_position.state = previous_position.state;
            merged_position.size = allocated_size;
            merged_position.current_price = live_position.current_price;
            merged_position.current_value = allocated_size * merged_position.current_price;
            merged_position.cost_basis = allocated_size * merged_position.average_entry_price;
            merged_position.unrealized_pnl =
                merged_position.current_value - merged_position.cost_basis;
            merged.push(merged_position);
            remaining_size -= allocated_size;
        }

        if remaining_size > Decimal::ZERO {
            let mut anonymous_position = live_position.clone();
            anonymous_position.size = remaining_size;
            anonymous_position.source_wallet = String::new();
            anonymous_position.current_value = remaining_size * anonymous_position.current_price;
            anonymous_position.average_entry_price = anonymous_position.current_price;
            anonymous_position.cost_basis = anonymous_position.current_value;
            anonymous_position.unrealized_pnl = Decimal::ZERO;
            anonymous_position.opened_at = None;
            anonymous_position.source_trade_timestamp_unix = 0;
            anonymous_position.state = PositionState::Stale;
            anonymous_position.closing_started_at = None;
            anonymous_position.closing_reason = None;
            anonymous_position.last_close_attempt_at = None;
            anonymous_position.close_attempts = 0;
            anonymous_position.close_failure_reason = None;
            anonymous_position.closing_escalation_level = 0;
            anonymous_position.stale_reason = Some("stale_without_seen_exit".to_owned());
            merged.push(anonymous_position);
        }
    }

    snapshot.positions = merged;
}

fn restore_live_accounting_from_layout(
    snapshot: &mut PortfolioSnapshot,
    layout: &PortfolioSnapshot,
) {
    snapshot.realized_pnl = layout.realized_pnl;
}

fn preserve_projected_layout_if_live_refresh_stale(
    fetched: &mut PortfolioSnapshot,
    layout: &PortfolioSnapshot,
) -> bool {
    if live_refresh_size_mismatch_count(fetched, layout) == 0 {
        return false;
    }

    let latest_prices = fetched
        .positions
        .iter()
        .filter(|position| !position.current_price.is_zero())
        .map(|position| (position.asset.clone(), position.current_price))
        .collect::<HashMap<_, _>>();

    *fetched = layout.clone();
    for position in &mut fetched.positions {
        if let Some(price) = latest_prices.get(&position.asset) {
            position.current_price = *price;
            position.current_value = position.size * position.current_price;
            position.cost_basis = position.size * position.average_entry_price;
            position.unrealized_pnl = position.current_value - position.cost_basis;
        }
    }
    fetched.fetched_at = Utc::now();
    true
}

fn live_refresh_size_mismatch_count(
    fetched: &PortfolioSnapshot,
    layout: &PortfolioSnapshot,
) -> usize {
    let tolerance = Decimal::new(1, 6);
    let fetched_sizes = position_sizes_by_key(fetched);
    let layout_sizes = position_sizes_by_key(layout);

    layout_sizes
        .iter()
        .filter(|(key, expected_size)| {
            let observed_size = fetched_sizes.get(*key).copied().unwrap_or(Decimal::ZERO);
            (observed_size - **expected_size).abs() > tolerance
        })
        .count()
}

fn position_sizes_by_key(snapshot: &PortfolioSnapshot) -> HashMap<PositionKey, Decimal> {
    let mut sizes = HashMap::new();
    for position in &snapshot.positions {
        let key = position.position_key();
        *sizes.entry(key).or_insert(Decimal::ZERO) += position.size;
    }
    sizes
}

fn normalize_snapshot(snapshot: &mut PortfolioSnapshot) {
    for position in &mut snapshot.positions {
        position.source_wallet = normalize_wallet(&position.source_wallet);
        if position.current_price.is_zero() {
            position.current_price =
                decimal_price_from_value(position.size, position.current_value);
        }
        if position.average_entry_price.is_zero() {
            position.average_entry_price =
                if !position.cost_basis.is_zero() && !position.size.is_zero() {
                    position.cost_basis / position.size
                } else {
                    position.current_price
                };
        }
        if position.cost_basis.is_zero() {
            position.cost_basis = position.average_entry_price * position.size;
        }
        if position.current_value.is_zero() && !position.current_price.is_zero() {
            position.current_value = position.size * position.current_price;
        }
        if position.size.is_zero() && position.state != PositionState::Closed {
            position.state = PositionState::Closed;
        }
        position.unrealized_pnl = position.current_value - position.cost_basis;
    }
    if snapshot.starting_equity.is_zero() {
        snapshot.starting_equity = snapshot.total_value.max(snapshot.cash_balance);
    }
}

fn recalculate_snapshot_totals(snapshot: &mut PortfolioSnapshot) {
    snapshot.fetched_at = Utc::now();
    snapshot.total_exposure = snapshot
        .positions
        .iter()
        .map(|position| position.current_value)
        .sum();
    snapshot.unrealized_pnl = snapshot
        .positions
        .iter()
        .map(|position| position.unrealized_pnl)
        .sum();
    snapshot.total_value = snapshot.cash_balance + snapshot.total_exposure;
}

fn record_realized_trade(snapshot: &mut PortfolioSnapshot, realized_pnl: Decimal) {
    let now = Utc::now();
    snapshot
        .recent_realized_trade_points
        .push(RealizedTradePoint {
            observed_at: now,
            pnl: realized_pnl,
        });
    if snapshot.recent_realized_trade_points.len() > RECENT_REALIZED_TRADE_LIMIT {
        let drop_count = snapshot.recent_realized_trade_points.len() - RECENT_REALIZED_TRADE_LIMIT;
        snapshot.recent_realized_trade_points.drain(0..drop_count);
    }
    snapshot.rolling_loss_count = snapshot
        .recent_realized_trade_points
        .iter()
        .filter(|point| point.pnl < Decimal::ZERO)
        .count() as u32;
    if realized_pnl < Decimal::ZERO {
        snapshot.consecutive_losses = snapshot.consecutive_losses.saturating_add(1);
    } else if realized_pnl > Decimal::ZERO {
        snapshot.consecutive_losses = 0;
        snapshot.loss_cooldown_until = None;
    }
}

fn apply_risk_state(snapshot: &mut PortfolioSnapshot, config: &PortfolioRiskConfig) {
    let now = Utc::now();
    let starting_equity = if snapshot.starting_equity > Decimal::ZERO {
        snapshot.starting_equity
    } else {
        config
            .start_capital_usd
            .max(snapshot.total_value)
            .max(Decimal::ZERO)
    };
    snapshot.starting_equity = starting_equity;
    snapshot.current_equity = snapshot.total_value.max(Decimal::ZERO);
    snapshot.peak_equity = snapshot
        .peak_equity
        .max(starting_equity)
        .max(snapshot.current_equity);
    snapshot.open_exposure_total = snapshot.active_total_exposure();
    snapshot.open_positions_count = snapshot
        .positions
        .iter()
        .filter(|position| position.is_active())
        .count();
    snapshot.current_drawdown_pct = drawdown_pct(snapshot.peak_equity, snapshot.current_equity);
    snapshot.risk_utilization_pct =
        ratio_pct(snapshot.open_exposure_total, snapshot.current_equity);
    update_recent_equity_samples(snapshot, now);
    snapshot.rolling_drawdown_pct = rolling_drawdown(snapshot, now);
    snapshot.rolling_loss_count = snapshot
        .recent_realized_trade_points
        .iter()
        .filter(|point| point.pnl < Decimal::ZERO)
        .count() as u32;

    if snapshot
        .loss_cooldown_until
        .is_some_and(|cooldown_until| cooldown_until <= now)
    {
        snapshot.loss_cooldown_until = None;
        snapshot.consecutive_losses = 0;
    } else if config.max_consecutive_losses > 0
        && snapshot.consecutive_losses >= config.max_consecutive_losses
        && config.loss_cooldown > Duration::ZERO
        && snapshot.loss_cooldown_until.is_none()
    {
        snapshot.loss_cooldown_until = Some(
            now + chrono::TimeDelta::from_std(config.loss_cooldown)
                .unwrap_or_else(|_| chrono::TimeDelta::zero()),
        );
    }

    snapshot.drawdown_guard_active = config.max_drawdown_pct > Decimal::ZERO
        && snapshot.current_drawdown_pct >= config.max_drawdown_pct;
    if config.hard_stop_drawdown_pct > Decimal::ZERO
        && snapshot.current_drawdown_pct >= config.hard_stop_drawdown_pct
    {
        snapshot.hard_stop_active = true;
    }
}

fn update_recent_equity_samples(snapshot: &mut PortfolioSnapshot, now: DateTime<Utc>) {
    let should_replace_latest = snapshot
        .recent_equity_samples
        .last()
        .and_then(|point| {
            (now.signed_duration_since(point.observed_at)
                < chrono::TimeDelta::from_std(EQUITY_SAMPLE_MIN_INTERVAL)
                    .unwrap_or_else(|_| chrono::TimeDelta::zero()))
            .then_some(())
        })
        .is_some();

    if should_replace_latest {
        if let Some(last) = snapshot.recent_equity_samples.last_mut() {
            last.observed_at = now;
            last.equity = snapshot.current_equity;
        }
    } else {
        snapshot.recent_equity_samples.push(EquityPoint {
            observed_at: now,
            equity: snapshot.current_equity,
        });
    }

    let window = chrono::TimeDelta::from_std(ROLLING_DRAWDOWN_WINDOW)
        .unwrap_or_else(|_| chrono::TimeDelta::zero());
    snapshot
        .recent_equity_samples
        .retain(|point| now.signed_duration_since(point.observed_at) <= window);
}

fn rolling_drawdown(snapshot: &PortfolioSnapshot, now: DateTime<Utc>) -> Decimal {
    let window = chrono::TimeDelta::from_std(ROLLING_DRAWDOWN_WINDOW)
        .unwrap_or_else(|_| chrono::TimeDelta::zero());
    let rolling_peak = snapshot
        .recent_equity_samples
        .iter()
        .filter(|point| now.signed_duration_since(point.observed_at) <= window)
        .map(|point| point.equity)
        .max()
        .unwrap_or(snapshot.current_equity);
    drawdown_pct(rolling_peak, snapshot.current_equity)
}

fn drawdown_pct(peak_equity: Decimal, current_equity: Decimal) -> Decimal {
    if peak_equity <= Decimal::ZERO {
        return Decimal::ZERO;
    }
    ((peak_equity - current_equity).max(Decimal::ZERO) / peak_equity)
        .max(Decimal::ZERO)
        .min(Decimal::ONE)
}

fn ratio_pct(numerator: Decimal, denominator: Decimal) -> Decimal {
    if denominator <= Decimal::ZERO {
        return Decimal::ZERO;
    }
    (numerator / denominator).max(Decimal::ZERO)
}

fn decimal_price_from_value(size: Decimal, value: Decimal) -> Decimal {
    if size.is_zero() {
        Decimal::ZERO
    } else {
        value / size
    }
}

fn source_trade_opened_at(source: &ActivityEntry) -> Option<DateTime<Utc>> {
    if source.timestamp <= 0 {
        return None;
    }

    if source.timestamp >= 10_000_000_000 {
        Utc.timestamp_millis_opt(source.timestamp).single()
    } else {
        Utc.timestamp_opt(source.timestamp, 0).single()
    }
}

fn position_mark_price(snapshot: Option<MarketSnapshot>, fallback: Decimal) -> Option<Decimal> {
    let snapshot = snapshot?;
    snapshot
        .mid_price
        .or(snapshot.last_trade_price)
        .or(snapshot.best_bid)
        .or(snapshot.best_ask)
        .and_then(Decimal::from_f64_retain)
        .map(|price| price.round_dp(6))
        .or_else(|| (!fallback.is_zero()).then_some(fallback))
}

fn normalize_wallet(wallet: &str) -> String {
    wallet.trim().to_ascii_lowercase()
}

fn position_contract_key(condition_id: &str, outcome: &str) -> (String, String) {
    (condition_id.to_owned(), outcome.trim().to_ascii_uppercase())
}

fn resolve_exit_index(
    snapshot: &PortfolioSnapshot,
    requested_key: &PositionKey,
    asset_hint: Option<&str>,
) -> Option<(usize, bool)> {
    if let Some(index) = snapshot
        .positions
        .iter()
        .position(|position| position.is_active() && position.position_key() == *requested_key)
    {
        return Some((index, false));
    }

    let candidates = snapshot
        .positions
        .iter()
        .enumerate()
        .filter(|(_, position)| {
            position.is_active()
                && position.condition_id == requested_key.condition_id
                && normalize_wallet(&position.source_wallet) == requested_key.source_wallet
        })
        .collect::<Vec<_>>();
    if candidates.len() == 1 {
        return Some((candidates[0].0, true));
    }

    asset_hint.and_then(|asset| {
        let asset_matches = candidates
            .into_iter()
            .filter(|(_, position)| position.asset == asset)
            .collect::<Vec<_>>();
        (asset_matches.len() == 1).then_some((asset_matches[0].0, true))
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use chrono::Utc;

    use super::*;
    use crate::config::ExecutionMode;
    use crate::execution::{ExecutionRequest, ExecutionSuccess};
    use crate::models::{BookLevel, OrderBookResponse};
    use crate::orderbook::orderbook_state::{AssetCatalog, AssetMetadata, OrderBookState};
    use rust_decimal_macros::dec;

    fn sample_activity(side: &str) -> ActivityEntry {
        sample_activity_with_wallet(side, "0xsource")
    }

    fn sample_activity_with_wallet(side: &str, wallet: &str) -> ActivityEntry {
        ActivityEntry {
            proxy_wallet: wallet.to_owned(),
            timestamp: 1,
            condition_id: "condition-1".to_owned(),
            type_name: "TRADE".to_owned(),
            size: 10.0,
            usdc_size: 5.0,
            transaction_hash: "0xhash".to_owned(),
            price: 0.5,
            asset: "asset-1".to_owned(),
            side: side.to_owned(),
            outcome_index: 0,
            title: "Sample market".to_owned(),
            slug: "sample-market".to_owned(),
            event_slug: "sample-event".to_owned(),
            outcome: "YES".to_owned(),
        }
    }

    fn sample_result(side: ExecutionSide, size: Decimal, price: Decimal) -> ExecutionSuccess {
        ExecutionSuccess {
            mode: ExecutionMode::Paper,
            order_request: ExecutionRequest {
                token_id: "asset-1".to_owned(),
                side,
                size,
                limit_price: price,
                requested_notional: size * price,
                source_trade_id: "paper-trade".to_owned(),
            },
            order_id: "paper-order".to_owned(),
            success: true,
            transaction_hashes: Vec::new(),
            filled_price: price,
            filled_size: size,
            requested_size: size,
            requested_price: price,
            status: crate::execution::ExecutionStatus::Filled,
            filled_notional: size * price,
        }
    }

    fn sample_settings(data_dir: std::path::PathBuf) -> Settings {
        Settings::default_for_tests(data_dir)
    }

    #[test]
    fn paper_buy_updates_cash_and_position() {
        let mut snapshot = initial_paper_snapshot(dec!(200));
        apply_paper_trade(
            &mut snapshot,
            &sample_activity("BUY"),
            &sample_result(ExecutionSide::Buy, dec!(10), dec!(0.4)),
            None,
        )
        .expect("paper buy should succeed");

        assert_eq!(snapshot.cash_balance, dec!(196));
        assert_eq!(snapshot.total_exposure, dec!(4));
        assert_eq!(snapshot.total_value, dec!(200));
        assert_eq!(snapshot.realized_pnl, Decimal::ZERO);
        assert_eq!(snapshot.unrealized_pnl, Decimal::ZERO);
        assert_eq!(snapshot.positions.len(), 1);
        assert_eq!(snapshot.positions[0].size, dec!(10));
        assert_eq!(snapshot.positions[0].source_wallet, "0xsource");
        assert_eq!(snapshot.positions[0].average_entry_price, dec!(0.4));
        assert_eq!(snapshot.positions[0].cost_basis, dec!(4));
    }

    #[test]
    fn paper_sell_reduces_position_and_restores_cash() {
        let mut snapshot = initial_paper_snapshot(dec!(200));
        apply_paper_trade(
            &mut snapshot,
            &sample_activity("BUY"),
            &sample_result(ExecutionSide::Buy, dec!(10), dec!(0.4)),
            None,
        )
        .expect("paper buy should succeed");
        apply_paper_trade(
            &mut snapshot,
            &sample_activity("SELL"),
            &sample_result(ExecutionSide::Sell, dec!(4), dec!(0.6)),
            None,
        )
        .expect("paper sell should succeed");

        assert_eq!(snapshot.cash_balance, dec!(198.4));
        assert_eq!(snapshot.total_exposure, dec!(3.6));
        assert_eq!(snapshot.total_value, dec!(202.0));
        assert_eq!(snapshot.realized_pnl, dec!(0.8));
        assert_eq!(snapshot.positions[0].size, dec!(6));
        assert_eq!(snapshot.positions[0].source_wallet, "0xsource");
        assert_eq!(snapshot.positions[0].average_entry_price, dec!(0.4));
    }

    #[test]
    fn paper_positions_split_by_wallet_and_sell_uses_matching_lot() {
        let mut snapshot = initial_paper_snapshot(dec!(200));
        apply_paper_trade(
            &mut snapshot,
            &sample_activity_with_wallet("BUY", "0xwallet-a"),
            &sample_result(ExecutionSide::Buy, dec!(10), dec!(0.4)),
            None,
        )
        .expect("first buy should succeed");
        apply_paper_trade(
            &mut snapshot,
            &sample_activity_with_wallet("BUY", "0xwallet-b"),
            &sample_result(ExecutionSide::Buy, dec!(8), dec!(0.5)),
            None,
        )
        .expect("second buy should succeed");
        apply_paper_trade(
            &mut snapshot,
            &sample_activity_with_wallet("SELL", "0xwallet-a"),
            &sample_result(ExecutionSide::Sell, dec!(4), dec!(0.6)),
            None,
        )
        .expect("wallet-specific sell should succeed");

        assert_eq!(snapshot.positions.len(), 2);
        assert_eq!(snapshot.asset_size("asset-1"), dec!(14));
        assert_eq!(
            snapshot.asset_size_for_wallet("asset-1", "0xwallet-a"),
            dec!(6)
        );
        assert_eq!(
            snapshot.asset_size_for_wallet("asset-1", "0xwallet-b"),
            dec!(8)
        );
        assert_eq!(snapshot.positions[0].source_wallet, "0xwallet-a");
        assert_eq!(snapshot.positions[0].size, dec!(6));
        assert_eq!(snapshot.positions[1].source_wallet, "0xwallet-b");
        assert_eq!(snapshot.positions[1].size, dec!(8));
    }

    #[test]
    fn live_rehydration_preserves_wallet_layout_and_realized_pnl() {
        let mut fetched = PortfolioSnapshot {
            fetched_at: Utc::now(),
            total_value: dec!(200),
            total_exposure: dec!(6.4),
            cash_balance: dec!(193.6),
            realized_pnl: Decimal::ZERO,
            unrealized_pnl: Decimal::ZERO,
            positions: vec![PortfolioPosition {
                asset: "asset-1".to_owned(),
                condition_id: "condition-1".to_owned(),
                title: "Live market".to_owned(),
                outcome: "YES".to_owned(),
                source_wallet: String::new(),
                state: PositionState::Open,
                size: dec!(8),
                current_value: dec!(6.4),
                average_entry_price: dec!(0.8),
                current_price: dec!(0.8),
                cost_basis: dec!(6.4),
                unrealized_pnl: Decimal::ZERO,
                opened_at: None,
                source_trade_timestamp_unix: 0,
                closing_started_at: None,
                closing_reason: None,
                last_close_attempt_at: None,
                close_attempts: 0,
                close_failure_reason: None,
                closing_escalation_level: 0,
                stale_reason: None,
            }],
            ..PortfolioSnapshot::default()
        };
        let layout = PortfolioSnapshot {
            fetched_at: Utc::now(),
            total_value: dec!(207.5),
            total_exposure: dec!(4.4),
            cash_balance: dec!(203.1),
            realized_pnl: dec!(7.5),
            unrealized_pnl: Decimal::ZERO,
            positions: vec![
                PortfolioPosition {
                    asset: "asset-1".to_owned(),
                    condition_id: "condition-1".to_owned(),
                    title: "Tracked lot A".to_owned(),
                    outcome: "YES".to_owned(),
                    source_wallet: "0xwallet-a".to_owned(),
                    state: PositionState::Open,
                    size: dec!(6),
                    current_value: dec!(2.4),
                    average_entry_price: dec!(0.4),
                    current_price: dec!(0.4),
                    cost_basis: dec!(2.4),
                    unrealized_pnl: Decimal::ZERO,
                    opened_at: None,
                    source_trade_timestamp_unix: 0,
                    closing_started_at: None,
                    closing_reason: None,
                    last_close_attempt_at: None,
                    close_attempts: 0,
                    close_failure_reason: None,
                    closing_escalation_level: 0,
                    stale_reason: None,
                },
                PortfolioPosition {
                    asset: "asset-1".to_owned(),
                    condition_id: "condition-1".to_owned(),
                    title: "Tracked lot B".to_owned(),
                    outcome: "YES".to_owned(),
                    source_wallet: "0xwallet-b".to_owned(),
                    state: PositionState::Open,
                    size: dec!(4),
                    current_value: dec!(2),
                    average_entry_price: dec!(0.5),
                    current_price: dec!(0.5),
                    cost_basis: dec!(2),
                    unrealized_pnl: Decimal::ZERO,
                    opened_at: None,
                    source_trade_timestamp_unix: 0,
                    closing_started_at: None,
                    closing_reason: None,
                    last_close_attempt_at: None,
                    close_attempts: 0,
                    close_failure_reason: None,
                    closing_escalation_level: 0,
                    stale_reason: None,
                },
            ],
            ..PortfolioSnapshot::default()
        };

        rehydrate_live_wallet_positions(&mut fetched, &layout);
        restore_live_accounting_from_layout(&mut fetched, &layout);
        recalculate_snapshot_totals(&mut fetched);

        assert_eq!(fetched.realized_pnl, dec!(7.5));
        assert_eq!(fetched.positions.len(), 2);
        assert_eq!(fetched.positions[0].source_wallet, "0xwallet-a");
        assert_eq!(fetched.positions[0].size, dec!(6));
        assert_eq!(fetched.positions[0].average_entry_price, dec!(0.4));
        assert_eq!(fetched.positions[0].current_price, dec!(0.8));
        assert_eq!(fetched.positions[1].source_wallet, "0xwallet-b");
        assert_eq!(fetched.positions[1].size, dec!(2));
        assert_eq!(fetched.positions[1].average_entry_price, dec!(0.5));
        assert_eq!(fetched.total_exposure, dec!(6.4));
        assert_eq!(fetched.unrealized_pnl, dec!(3.0));
        assert_eq!(fetched.total_value, dec!(200.0));
    }

    #[test]
    fn stale_live_refresh_preserves_projected_layout_sizes() {
        let mut fetched = PortfolioSnapshot {
            fetched_at: Utc::now(),
            total_value: dec!(200),
            total_exposure: dec!(4),
            cash_balance: dec!(196),
            realized_pnl: Decimal::ZERO,
            unrealized_pnl: Decimal::ZERO,
            positions: vec![PortfolioPosition {
                asset: "asset-1".to_owned(),
                condition_id: "condition-1".to_owned(),
                title: "Live market".to_owned(),
                outcome: "YES".to_owned(),
                source_wallet: String::new(),
                state: PositionState::Open,
                size: dec!(5),
                current_value: dec!(4),
                average_entry_price: dec!(0.8),
                current_price: dec!(0.8),
                cost_basis: dec!(4),
                unrealized_pnl: Decimal::ZERO,
                opened_at: None,
                source_trade_timestamp_unix: 0,
                closing_started_at: None,
                closing_reason: None,
                last_close_attempt_at: None,
                close_attempts: 0,
                close_failure_reason: None,
                closing_escalation_level: 0,
                stale_reason: None,
            }],
            ..PortfolioSnapshot::default()
        };
        let layout = PortfolioSnapshot {
            fetched_at: Utc::now(),
            total_value: dec!(200),
            total_exposure: dec!(8),
            cash_balance: dec!(192),
            realized_pnl: dec!(1.2),
            unrealized_pnl: Decimal::ZERO,
            positions: vec![PortfolioPosition {
                asset: "asset-1".to_owned(),
                condition_id: "condition-1".to_owned(),
                title: "Projected lot".to_owned(),
                outcome: "YES".to_owned(),
                source_wallet: "0xwallet-a".to_owned(),
                state: PositionState::Open,
                size: dec!(10),
                current_value: dec!(5),
                average_entry_price: dec!(0.5),
                current_price: dec!(0.5),
                cost_basis: dec!(5),
                unrealized_pnl: Decimal::ZERO,
                opened_at: None,
                source_trade_timestamp_unix: 0,
                closing_started_at: None,
                closing_reason: None,
                last_close_attempt_at: None,
                close_attempts: 0,
                close_failure_reason: None,
                closing_escalation_level: 0,
                stale_reason: None,
            }],
            ..PortfolioSnapshot::default()
        };

        rehydrate_live_wallet_positions(&mut fetched, &layout);
        restore_live_accounting_from_layout(&mut fetched, &layout);

        assert!(preserve_projected_layout_if_live_refresh_stale(
            &mut fetched,
            &layout
        ));
        recalculate_snapshot_totals(&mut fetched);

        assert_eq!(fetched.realized_pnl, dec!(1.2));
        assert_eq!(fetched.positions.len(), 1);
        assert_eq!(fetched.positions[0].source_wallet, "0xwallet-a");
        assert_eq!(fetched.positions[0].size, dec!(10));
        assert_eq!(fetched.positions[0].average_entry_price, dec!(0.5));
        assert_eq!(fetched.positions[0].current_price, dec!(0.8));
        assert_eq!(fetched.positions[0].current_value, dec!(8));
        assert_eq!(fetched.positions[0].unrealized_pnl, dec!(3));
        assert_eq!(fetched.cash_balance, dec!(192));
        assert_eq!(fetched.total_value, dec!(200));
    }

    #[test]
    fn position_mark_price_prefers_mid_price_then_fallbacks() {
        let midpoint_snapshot = MarketSnapshot {
            spread_ratio: Some(0.02),
            visible_total_volume: 100.0,
            mid_price: Some(0.53),
            best_bid: Some(0.52),
            best_ask: Some(0.54),
            last_trade_price: Some(0.51),
        };
        assert_eq!(
            position_mark_price(Some(midpoint_snapshot), dec!(0.4)).expect("mark"),
            dec!(0.53)
        );

        let fallback_snapshot = MarketSnapshot {
            spread_ratio: None,
            visible_total_volume: 0.0,
            mid_price: None,
            best_bid: None,
            best_ask: None,
            last_trade_price: None,
        };
        assert_eq!(
            position_mark_price(Some(fallback_snapshot), dec!(0.4)).expect("fallback"),
            dec!(0.4)
        );
    }

    #[tokio::test]
    async fn paper_snapshot_marks_to_market_with_orderbook_price() {
        let temp_dir = std::env::temp_dir().join(format!(
            "copytrade-portfolio-test-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let settings = sample_settings(temp_dir.clone());
        let catalog = AssetCatalog::new(vec![AssetMetadata {
            asset_id: "asset-1".to_owned(),
            condition_id: "condition-1".to_owned(),
            title: "Sample market".to_owned(),
            slug: "sample-market".to_owned(),
            event_slug: "sample-event".to_owned(),
            outcome: "YES".to_owned(),
            outcome_index: 0,
        }]);
        let orderbooks = Arc::new(OrderBookState::new(&settings, catalog).expect("orderbooks"));
        orderbooks
            .apply_book_snapshot(
                OrderBookResponse {
                    market: "condition-1".to_owned(),
                    asset_id: "asset-1".to_owned(),
                    bids: vec![BookLevel {
                        price: "0.60".to_owned(),
                        size: "100".to_owned(),
                    }],
                    asks: vec![BookLevel {
                        price: "0.62".to_owned(),
                        size: "100".to_owned(),
                    }],
                    min_order_size: "1".to_owned(),
                    tick_size: "0.01".to_owned(),
                    neg_risk: false,
                },
                Instant::now(),
                Utc::now(),
                Instant::now(),
                Utc::now(),
            )
            .await
            .expect("snapshot");
        let portfolio = PortfolioService::new(settings, Some(orderbooks));
        let refreshed = portfolio
            .apply_paper_fill(
                &sample_activity("BUY"),
                &sample_result(ExecutionSide::Buy, dec!(10), dec!(0.4)),
                None,
            )
            .await
            .expect("paper fill");

        assert_eq!(refreshed.positions[0].average_entry_price, dec!(0.4));
        assert_eq!(refreshed.positions[0].current_price, dec!(0.61));
        assert_eq!(refreshed.positions[0].cost_basis, dec!(4));
        assert_eq!(refreshed.positions[0].current_value, dec!(6.1));
        assert_eq!(refreshed.positions[0].unrealized_pnl, dec!(2.1));
        assert_eq!(refreshed.unrealized_pnl, dec!(2.1));
        assert_eq!(refreshed.total_value, dec!(202.1));
    }

    #[tokio::test]
    async fn store_snapshot_flushes_in_background_without_blocking_cache_updates() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let mut settings = sample_settings(temp_dir.path().to_path_buf());
        settings.persistence_flush_interval = Duration::from_millis(250);
        let portfolio = PortfolioService::new(settings, None);
        let snapshot = initial_paper_snapshot(dec!(200));

        let stored = portfolio
            .store_snapshot(snapshot.clone())
            .await
            .expect("store snapshot");

        assert_eq!(stored.total_value, dec!(200));
        assert_eq!(
            portfolio
                .snapshot()
                .await
                .expect("cached snapshot")
                .total_value,
            dec!(200)
        );
        assert!(!temp_dir.path().join("portfolio-summary.json").exists());

        portfolio
            .flush_persistence()
            .await
            .expect("flush persistence");
        assert!(temp_dir.path().join("portfolio-summary.json").exists());
    }
}
