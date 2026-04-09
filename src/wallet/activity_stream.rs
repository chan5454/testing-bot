use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt, future::BoxFuture};
use reqwest::Client;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::mpsc;
use tokio::time::{MissedTickBehavior, interval, sleep};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::{Error as WsError, Message};
use tracing::{debug, error, info, warn};

use crate::attribution_fast::FastAttributionIndex;
use crate::attribution_logger::AttributionLogger;
use crate::config::Settings;
use crate::detection::trade_inference::ConfirmedTradeSignal;
use crate::execution::ExecutionSide;
use crate::health::HealthState;
use crate::models::ActivityEntry;
use crate::orderbook::orderbook_state::AssetCatalog;
use crate::position_registry::PositionRegistry;
use crate::raw_activity_logger::RawActivityLogger;
use crate::runtime::control::RuntimeControl;
use crate::runtime::ring_buffer::RingBuffer;
use crate::runtime::websocket_reconnect::{
    WS_HEARTBEAT_INTERVAL, WebSocketReconnectState, is_expected_disconnect, log_websocket_error,
};
use crate::runtime::worker_pool::spawn_worker_pool;
use crate::storage::StateStore;
use crate::wallet::pending_signal_buffer::{PendingMatchOutcome, PendingSignalBuffer};
use crate::wallet::wallet_filter::normalize_wallet;
use crate::wallet::wallet_matching::{
    ActivitySource, ActivityTradeEvent, MatchedTrackedTrade, TradeCorrelationKind,
    build_wallet_triggered_trade, signal_reference_timestamps, sizes_are_similar,
};
#[cfg(test)]
use crate::wallet::wallet_matching::{OUTCOME_MISMATCH_WINDOW, is_complementary};
use crate::wallet_registry::WalletRegistry;
use crate::wallet_scanner::WalletActivityLogger;

const FALLBACK_TICK_INTERVAL: Duration = Duration::from_millis(50);
const ATTRIBUTION_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);
const RTDS_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
const RECENT_KEY_CAPACITY: usize = 4_096;
const WALLET_CORRELATION_WINDOW: Duration = Duration::from_secs(1);
const COORDINATOR_COMMAND_BATCH_LIMIT: usize = 128;
const COORDINATOR_ACTIVITY_BATCH_LIMIT: usize = 512;
const UNTRACKED_WALLET_LOG_WINDOW: Duration = Duration::from_secs(1);
const UNTRACKED_WALLET_LOG_BURST: usize = 4;
const ACTIVITY_DEBUG_STREAM: &str = "activity_ws";
const ACTIVITY_FIELD_KEYS: &[&str] = &[
    "data",
    "payload",
    "event",
    "events",
    "activity",
    "activities",
    "trade",
    "trades",
    "items",
    "message",
    "result",
    "body",
];
const ACTIVITY_TOPIC_FIELDS: &[&str] = &["topic", "channel"];
const ACTIVITY_TYPE_FIELDS: &[&str] = &["type", "event_type", "eventType", "msgType"];
const ACTIVITY_WALLET_FIELDS: &[&str] = &[
    "proxyWallet",
    "proxy_wallet",
    "wallet",
    "walletAddress",
    "wallet_address",
    "user",
    "userAddress",
    "user_address",
    "owner",
    "ownerAddress",
    "owner_address",
    "address",
    "maker",
    "makerAddress",
    "maker_address",
    "taker",
    "takerAddress",
    "taker_address",
];
const ACTIVITY_MARKET_FIELDS: &[&str] = &[
    "market",
    "marketId",
    "market_id",
    "conditionId",
    "condition_id",
    "condition",
];
const ACTIVITY_PRICE_FIELDS: &[&str] = &[
    "price",
    "tradePrice",
    "trade_price",
    "lastPrice",
    "last_trade_price",
    "executedPrice",
    "executed_price",
    "matchPrice",
    "match_price",
    "rate",
];
const ACTIVITY_SIZE_FIELDS: &[&str] = &[
    "size",
    "amount",
    "tradeSize",
    "trade_size",
    "base_size",
    "quantity",
    "shares",
    "shareAmount",
    "filledSize",
    "filled_size",
];
const ACTIVITY_TIMESTAMP_FIELDS: &[&str] = &[
    "timestamp",
    "matchtime",
    "time",
    "createdAt",
    "created_at",
    "updatedAt",
    "updated_at",
    "matchedAt",
    "matched_at",
    "eventTime",
    "event_time",
];
const ACTIVITY_TX_HASH_FIELDS: &[&str] = &[
    "transactionHash",
    "transaction_hash",
    "txHash",
    "tx_hash",
    "hash",
    "tradeHash",
    "trade_hash",
];
const ACTIVITY_ASSET_FIELDS: &[&str] = &[
    "asset_id",
    "assetId",
    "asset",
    "tokenId",
    "token_id",
    "clobTokenId",
    "clob_token_id",
];
const ACTIVITY_SIDE_FIELDS: &[&str] = &[
    "side",
    "takerSide",
    "taker_side",
    "orderSide",
    "order_side",
    "action",
    "direction",
];

#[derive(Clone, Debug)]
pub enum ActivityCommand {
    ObserveMarketSignal(ConfirmedTradeSignal),
}

#[derive(Clone)]
pub struct WalletActivityStream {
    command_tx: mpsc::UnboundedSender<ActivityCommand>,
}

#[derive(Clone, Debug)]
struct RawActivityMessage {
    generation: u64,
    message_id: Box<str>,
    payload: Box<str>,
    observed_at: Instant,
    observed_at_utc: DateTime<Utc>,
}

#[derive(Clone, Debug, Default)]
struct ActivityEnvelopeContext {
    topic: Option<String>,
    event_type: Option<String>,
}

#[derive(Clone, Debug, Default)]
pub struct PublicActivityParseReport {
    pub topic: Option<String>,
    pub event_type: Option<String>,
    pub events: Vec<ActivityTradeEvent>,
    pub failure_detail: Option<String>,
}

#[derive(Default)]
struct RecentKeyStore {
    seen: HashSet<String>,
    order: VecDeque<(Instant, String)>,
}

#[derive(Default)]
struct UntrackedWalletLogState {
    window_started_at: Option<Instant>,
    generation: u64,
    emitted_in_window: usize,
    suppressed_in_window: usize,
    sample_wallet: Option<String>,
    sample_market_id: Option<String>,
}

impl UntrackedWalletLogState {
    async fn record(
        &mut self,
        attribution_logger: &AttributionLogger,
        event: &ActivityTradeEvent,
        pending_signals: usize,
    ) {
        self.flush_if_due(attribution_logger, event.observed_at)
            .await;
        if self.window_started_at.is_none() {
            self.start_window(event.observed_at, event.generation);
        }

        if self.emitted_in_window < UNTRACKED_WALLET_LOG_BURST {
            self.emitted_in_window += 1;
            attribution_logger
                .record_activity_event(
                    "skipped_untracked_wallet",
                    event.source.as_str(),
                    event.generation,
                    &event.event_id,
                    &event.wallet,
                    &event.market_id,
                    &event.transaction_hash,
                    event.price,
                    event.size,
                    event.timestamp_ms,
                    pending_signals,
                    Some(format!(
                        "wallet_candidates={}",
                        event.wallet_candidates.join(",")
                    )),
                )
                .await;
        } else {
            self.suppressed_in_window += 1;
            if self.sample_wallet.is_none() {
                self.sample_wallet = Some(event.wallet.clone());
            }
            if self.sample_market_id.is_none() {
                self.sample_market_id = Some(event.market_id.clone());
            }
        }
    }

    async fn flush_if_due(&mut self, attribution_logger: &AttributionLogger, now: Instant) {
        let Some(started_at) = self.window_started_at else {
            return;
        };
        if now.duration_since(started_at) < UNTRACKED_WALLET_LOG_WINDOW {
            return;
        }
        self.flush(attribution_logger).await;
        self.start_window(now, self.generation);
    }

    async fn flush(&mut self, attribution_logger: &AttributionLogger) {
        if self.suppressed_in_window > 0 {
            let sample_wallet = self.sample_wallet.as_deref().unwrap_or("<unknown>");
            let sample_market = self.sample_market_id.as_deref().unwrap_or("<unknown>");
            attribution_logger
                .record_stream_event(
                    "skipped_untracked_wallet_suppressed",
                    ACTIVITY_DEBUG_STREAM,
                    self.generation,
                    Some(format!(
                        "suppressed={} logged={} window_ms={} sample_wallet={} sample_market={}",
                        self.suppressed_in_window,
                        self.emitted_in_window,
                        UNTRACKED_WALLET_LOG_WINDOW.as_millis(),
                        sample_wallet,
                        sample_market
                    )),
                )
                .await;
        }
        self.window_started_at = None;
        self.emitted_in_window = 0;
        self.suppressed_in_window = 0;
        self.sample_wallet = None;
        self.sample_market_id = None;
    }

    fn start_window(&mut self, started_at: Instant, generation: u64) {
        self.window_started_at = Some(started_at);
        self.generation = generation;
        self.emitted_in_window = 0;
        self.suppressed_in_window = 0;
        self.sample_wallet = None;
        self.sample_market_id = None;
    }
}

struct TrackedWalletHint {
    wallet: String,
    market_id: String,
    transaction_hash: String,
    size: f64,
    timestamp_ms: i64,
    observed_at: Instant,
}

struct WalletlessActivityEvent {
    event: ActivityTradeEvent,
    observed_at: Instant,
}

#[derive(Default)]
struct ActivityCorrelationBuffer {
    tracked_hints: VecDeque<TrackedWalletHint>,
    walletless_events: VecDeque<WalletlessActivityEvent>,
}

impl WalletActivityStream {
    pub fn spawn(
        settings: Settings,
        catalog: AssetCatalog,
        state_store: Arc<StateStore>,
        health: Arc<HealthState>,
        runtime_control: Arc<RuntimeControl>,
        attribution_logger: Arc<AttributionLogger>,
        raw_activity_logger: RawActivityLogger,
        matched_trade_tx: mpsc::UnboundedSender<MatchedTrackedTrade>,
        wallet_registry: WalletRegistry,
        position_registry: PositionRegistry,
        wallet_activity_logger: WalletActivityLogger,
    ) -> Self {
        let (command_tx, command_rx) = mpsc::unbounded_channel();
        let (parsed_trade_tx, parsed_trade_rx) = mpsc::unbounded_channel();
        if settings.activity_stream_enabled && settings.activity_parser_debug {
            let logger = raw_activity_logger.clone();
            tokio::spawn(async move {
                logger.record_logger_ready(ACTIVITY_DEBUG_STREAM).await;
            });
        }
        let primary_wallet = settings
            .target_profile_addresses
            .first()
            .cloned()
            .unwrap_or_default();
        let mut websocket_sources = 0_usize;

        if can_use_user_stream(&settings) {
            websocket_sources += 1;
            let raw_ring = RingBuffer::new(settings.wallet_ring_capacity);
            spawn_user_activity_stream(
                settings.clone(),
                catalog.clone(),
                raw_ring.clone(),
                health.clone(),
                runtime_control.clone(),
                attribution_logger.clone(),
            );

            let user_catalog = catalog.clone();
            let user_parsed_trade_tx = parsed_trade_tx.clone();
            let handler = Arc::new(move |message: RawActivityMessage| {
                let catalog = user_catalog.clone();
                let parsed_trade_tx = user_parsed_trade_tx.clone();
                let primary_wallet = primary_wallet.clone();
                Box::pin(async move {
                    if let Some(trade) = parse_user_trade_message(
                        &message.payload,
                        &catalog,
                        &primary_wallet,
                        message.generation,
                        message.observed_at,
                        message.observed_at_utc,
                    ) {
                        let _ = parsed_trade_tx.send(trade);
                    }
                }) as BoxFuture<'static, ()>
            });
            spawn_worker_pool(
                "wallet-user-parser",
                settings
                    .parse_tasks_wallet
                    .max(settings.wallet_parser_workers),
                raw_ring,
                handler,
            );
        }

        if settings.activity_stream_enabled {
            websocket_sources += 1;
            let raw_ring = RingBuffer::new(settings.wallet_ring_capacity);
            spawn_public_activity_stream(
                settings.clone(),
                raw_ring.clone(),
                raw_activity_logger.clone(),
                health.clone(),
                runtime_control.clone(),
                attribution_logger.clone(),
            );

            let activity_parsed_trade_tx = parsed_trade_tx.clone();
            let activity_raw_logger = raw_activity_logger.clone();
            let activity_attribution_logger = attribution_logger.clone();
            let handler = Arc::new(move |message: RawActivityMessage| {
                let parsed_trade_tx = activity_parsed_trade_tx.clone();
                let raw_logger = activity_raw_logger.clone();
                let attribution_logger = activity_attribution_logger.clone();
                Box::pin(async move {
                    let report = parse_public_activity_trade_messages(
                        &message.payload,
                        message.generation,
                        message.observed_at,
                        message.observed_at_utc,
                    );
                    raw_logger
                        .record_parse_report(&message.message_id, message.generation, &report)
                        .await;
                    if let Some(detail) = report.failure_detail.as_ref() {
                        attribution_logger
                            .record_stream_event(
                                "payload_unparsed",
                                ACTIVITY_DEBUG_STREAM,
                                message.generation,
                                Some(format!(
                                    "message_id={} topic={:?} type={:?} detail={detail}",
                                    message.message_id, report.topic, report.event_type
                                )),
                            )
                            .await;
                    }
                    for event in report.events {
                        let _ = parsed_trade_tx.send(event);
                    }
                }) as BoxFuture<'static, ()>
            });
            spawn_worker_pool(
                "wallet-activity-parser",
                settings
                    .parse_tasks_wallet
                    .max(settings.wallet_parser_workers),
                raw_ring,
                handler,
            );
        }

        if websocket_sources == 0 {
            info!("wallet activity websockets disabled; wallet-first execution cannot start");
            let logger = attribution_logger.clone();
            let generation = runtime_control.current_generation();
            tokio::spawn(async move {
                logger
                    .record_stream_event(
                        "stream_disabled",
                        "wallet_attribution_ws",
                        generation,
                        Some(
                            "both wallet activity websockets are disabled; wallet-first execution is unavailable"
                                .to_owned(),
                        ),
                    )
                    .await;
            });
        } else {
            info!(
                websocket_sources,
                "wallet-first execution configured from tracked wallet activity"
            );
        }

        if !can_use_user_stream(&settings) {
            let logger = attribution_logger.clone();
            let generation = runtime_control.current_generation();
            tokio::spawn(async move {
                logger
                    .record_stream_event(
                        "stream_disabled",
                        "user_ws",
                        generation,
                        Some("missing TARGET_ACTIVITY_WS_* credentials".to_owned()),
                    )
                    .await;
            });
        }
        if !settings.activity_stream_enabled {
            let logger = attribution_logger.clone();
            let generation = runtime_control.current_generation();
            tokio::spawn(async move {
                logger
                    .record_stream_event(
                        "stream_disabled",
                        "activity_ws",
                        generation,
                        Some("ACTIVITY_STREAM_ENABLED=false".to_owned()),
                    )
                    .await;
            });
        }

        tokio::spawn(run_activity_coordinator(
            build_client(&settings),
            settings,
            catalog,
            state_store,
            health,
            runtime_control,
            attribution_logger,
            matched_trade_tx,
            parsed_trade_rx,
            command_rx,
            wallet_registry,
            position_registry,
            wallet_activity_logger,
        ));

        Self { command_tx }
    }

    pub fn command_tx(&self) -> mpsc::UnboundedSender<ActivityCommand> {
        self.command_tx.clone()
    }
}

async fn run_activity_coordinator(
    _client: Client,
    settings: Settings,
    catalog: AssetCatalog,
    state_store: Arc<StateStore>,
    health: Arc<HealthState>,
    runtime_control: Arc<RuntimeControl>,
    attribution_logger: Arc<AttributionLogger>,
    matched_trade_tx: mpsc::UnboundedSender<MatchedTrackedTrade>,
    mut parsed_trade_rx: mpsc::UnboundedReceiver<ActivityTradeEvent>,
    mut command_rx: mpsc::UnboundedReceiver<ActivityCommand>,
    wallet_registry: WalletRegistry,
    position_registry: PositionRegistry,
    wallet_activity_logger: WalletActivityLogger,
) {
    let mut pending_buffer = PendingSignalBuffer::new(&settings);
    let mut fast_attribution = FastAttributionIndex::new(&settings);
    let mut recent_event_keys = RecentKeyStore::default();
    let mut activity_correlation = ActivityCorrelationBuffer::default();
    let mut untracked_wallet_logs = UntrackedWalletLogState::default();
    let reconnect_notify = runtime_control.reconnect_notify();
    let mut ticker = interval(FALLBACK_TICK_INTERVAL);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut heartbeat = interval(ATTRIBUTION_HEARTBEAT_INTERVAL);
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut current_generation = runtime_control.current_generation();

    loop {
        tokio::select! {
            _ = reconnect_notify.notified() => {
                untracked_wallet_logs.flush(&attribution_logger).await;
                let previous_generation = current_generation;
                let next_generation = runtime_control.current_generation();
                record_coordinator_reset(
                    &attribution_logger,
                    "coordinator_reconnect_reset",
                    previous_generation,
                    next_generation,
                    &pending_buffer,
                )
                .await;
                pending_buffer.clear();
                recent_event_keys.clear();
                activity_correlation.clear();
                current_generation = next_generation;
            }
            _ = ticker.tick() => {
                recent_event_keys.evict(settings.activity_cache_ttl.max(settings.activity_correlation_window));
                activity_correlation.evict_expired();
                evict_expired_pending(&mut pending_buffer, &attribution_logger).await;
                untracked_wallet_logs
                    .flush_if_due(&attribution_logger, Instant::now())
                    .await;
            }
            _ = heartbeat.tick() => {
                untracked_wallet_logs
                    .flush_if_due(&attribution_logger, Instant::now())
                    .await;
                let snapshot = health.snapshot().await;
                attribution_logger
                    .record_heartbeat(
                        &snapshot,
                        current_generation,
                        runtime_control.streams_ready(current_generation),
                        pending_buffer.pending_signal_count(),
                        pending_buffer.recent_activity_market_count(),
                        pending_buffer.recent_activity_event_count(),
                        0,
                        0,
                        0,
                        None,
                    )
                    .await;
            }
            Some(first_command) = command_rx.recv() => {
                let mut commands = vec![first_command];
                while commands.len() < COORDINATOR_COMMAND_BATCH_LIMIT {
                    let Ok(command) = command_rx.try_recv() else {
                        break;
                    };
                    commands.push(command);
                }

                for command in commands {
                    let signal = match command {
                        ActivityCommand::ObserveMarketSignal(signal) => signal,
                    };
                    if signal.generation < current_generation {
                        attribution_logger
                            .record_signal_event(
                                "signal_dropped_stale_generation",
                                &signal,
                                pending_buffer.pending_signal_count(),
                                0,
                                Some(format!("current_generation={current_generation}")),
                            )
                            .await;
                        continue;
                    }
                    if signal.generation > current_generation {
                        untracked_wallet_logs.flush(&attribution_logger).await;
                        record_coordinator_reset(
                            &attribution_logger,
                            "coordinator_generation_advanced_from_signal",
                            current_generation,
                            signal.generation,
                            &pending_buffer,
                        )
                        .await;
                        pending_buffer.clear();
                        recent_event_keys.clear();
                        activity_correlation.clear();
                        current_generation = signal.generation;
                    }

                    let pending_signal_ttl_ms = pending_buffer.pending_signal_ttl().as_millis() as u64;
                    let late_validation_ttl_ms =
                        pending_buffer.late_validation_ttl().as_millis() as u64;
                    let pending_count = pending_buffer.push_signal(signal.clone());
                    fast_attribution.record_signal(&signal);
                    attribution_logger
                        .record_signal_event(
                            "market_signal_cached_for_validation",
                            &signal,
                            pending_count,
                            0,
                            Some(format!(
                                "market_first=true confirmation_window_ms={pending_signal_ttl_ms} late_validation_window_ms={late_validation_ttl_ms}"
                            )),
                        )
                        .await;
                }
            }
            Some(first_event) = parsed_trade_rx.recv() => {
                let mut events = vec![first_event];
                while events.len() < COORDINATOR_ACTIVITY_BATCH_LIMIT {
                    let Ok(event) = parsed_trade_rx.try_recv() else {
                        break;
                    };
                    events.push(event);
                }

                for event in events {
                    let mut event = event;
                    debug!(
                        source = event.source.as_str(),
                        wallet = %event.wallet,
                        wallet_candidates = %event.wallet_candidates.join(","),
                        market_id = %event.market_id,
                        transaction_hash = %event.transaction_hash,
                        "parsed activity trade event"
                    );
                    if event.generation < current_generation {
                        attribution_logger
                            .record_activity_event(
                                "wallet_activity_dropped_stale_generation",
                                event.source.as_str(),
                                event.generation,
                                &event.event_id,
                                &event.wallet,
                                &event.market_id,
                                &event.transaction_hash,
                                event.price,
                                event.size,
                                event.timestamp_ms,
                                pending_buffer.pending_signal_count(),
                                Some(format!("current_generation={current_generation}")),
                            )
                            .await;
                        continue;
                    }
                    if event.generation > current_generation {
                        untracked_wallet_logs.flush(&attribution_logger).await;
                        record_coordinator_reset(
                            &attribution_logger,
                            "coordinator_generation_advanced_from_activity",
                            current_generation,
                            event.generation,
                            &pending_buffer,
                        )
                        .await;
                        pending_buffer.clear();
                        recent_event_keys.clear();
                        activity_correlation.clear();
                        current_generation = event.generation;
                    }

                    if event.wallet_candidates.is_empty() {
                        if let Some((wallet, correlation_kind)) =
                            activity_correlation.resolve_wallet(&event, &settings)
                        {
                            event.wallet = wallet.to_owned();
                            event.wallet_candidates = vec![wallet.to_owned()];
                            attribution_logger
                                .record_activity_event(
                                    "wallet_detected",
                                    event.source.as_str(),
                                    event.generation,
                                    &event.event_id,
                                    &event.wallet,
                                    &event.market_id,
                                    &event.transaction_hash,
                                    event.price,
                                    event.size,
                                    event.timestamp_ms,
                                    pending_buffer.pending_signal_count(),
                                    Some(format!(
                                        "tracked_wallet=true validated=false wallet_recovered=true correlation={correlation_kind}"
                                    )),
                                )
                                .await;
                        } else {
                            activity_correlation.store_walletless_event(&event);
                            attribution_logger
                                .record_activity_event(
                                    "wallet_not_found",
                                    event.source.as_str(),
                                    event.generation,
                                    &event.event_id,
                                    "",
                                    &event.market_id,
                                    &event.transaction_hash,
                                    event.price,
                                    event.size,
                                    event.timestamp_ms,
                                    pending_buffer.pending_signal_count(),
                                    Some("wallet fields missing; waiting for tx-hash/size-time correlation".to_owned()),
                                )
                                .await;
                            continue;
                        }
                    }

                    let exit_eligible = event.wallet_candidates.iter().any(|wallet| {
                        position_registry
                            .has_open_position_for_wallet_market(wallet, &event.market_id)
                    });
                    let tracked_wallet = event
                        .wallet_candidates
                        .iter()
                        .find(|wallet| {
                            wallet_registry.is_tracked(wallet)
                                || position_registry
                                    .has_open_position_for_wallet_market(wallet, &event.market_id)
                        })
                        .cloned();
                    wallet_activity_logger
                        .record_event(&event, tracked_wallet.is_some(), exit_eligible)
                        .await;
                    let Some(tracked_wallet) = tracked_wallet else {
                        debug!(
                            source = event.source.as_str(),
                            wallet_candidates = %event.wallet_candidates.join(","),
                            market_id = %event.market_id,
                            transaction_hash = %event.transaction_hash,
                            "activity trade did not match a tracked wallet"
                        );
                        untracked_wallet_logs
                            .record(
                                &attribution_logger,
                                &event,
                                pending_buffer.pending_signal_count(),
                            )
                            .await;
                        continue;
                    };
                    debug!(
                        source = event.source.as_str(),
                        tracked_wallet = %tracked_wallet,
                        wallet_candidates = %event.wallet_candidates.join(","),
                        market_id = %event.market_id,
                        transaction_hash = %event.transaction_hash,
                        "activity trade matched tracked wallet"
                    );
                    event.wallet = tracked_wallet;

                    if let Some(fast_trade) = fast_attribution.resolve_direct_trade(
                        &event,
                        &catalog,
                        &wallet_registry,
                        &position_registry,
                    ) {
                        forward_matched_trade(&matched_trade_tx, fast_trade);
                    }

                    if let Some((walletless_event, correlation_kind)) =
                        activity_correlation.take_walletless_match(&event, &settings)
                    {
                        event = merge_activity_events(event, walletless_event);
                        attribution_logger
                            .record_activity_event(
                                "wallet_detected",
                                event.source.as_str(),
                                event.generation,
                                &event.event_id,
                                &event.wallet,
                                &event.market_id,
                                &event.transaction_hash,
                                event.price,
                                event.size,
                                event.timestamp_ms,
                                pending_buffer.pending_signal_count(),
                                Some(format!(
                                    "tracked_wallet=true validated=false merged_walletless_event=true correlation={correlation_kind}"
                                )),
                            )
                            .await;
                    }
                    activity_correlation.store_tracked_hint(&event);

                    if !event.transaction_hash.is_empty()
                        && state_store.has_seen_tx_hash(&event.transaction_hash).await
                    {
                        attribution_logger
                            .record_activity_event(
                                "wallet_activity_duplicate_tx_hash",
                                event.source.as_str(),
                                event.generation,
                                &event.event_id,
                                &event.wallet,
                                &event.market_id,
                                &event.transaction_hash,
                                event.price,
                                event.size,
                                event.timestamp_ms,
                                pending_buffer.pending_signal_count(),
                                None,
                            )
                            .await;
                        continue;
                    }
                    if let Err(error) = state_store
                        .record_activity_observation(
                            event.timestamp_ms,
                            (!event.transaction_hash.is_empty()).then_some(event.transaction_hash.as_str()),
                        )
                        .await
                    {
                        warn!(?error, "failed to persist wallet activity cursor update");
                        health.set_last_error(format!("{error:#}")).await;
                    }

                    if !recent_event_keys.insert(event.event_id.clone(), settings.activity_cache_ttl) {
                        debug!(
                            event_id = %event.event_id,
                            source = event.source.as_str(),
                            "ignoring duplicate wallet activity event"
                        );
                        attribution_logger
                            .record_activity_event(
                                "wallet_activity_duplicate",
                                event.source.as_str(),
                                event.generation,
                                &event.event_id,
                                &event.wallet,
                                &event.market_id,
                                &event.transaction_hash,
                                event.price,
                                event.size,
                                event.timestamp_ms,
                                pending_buffer.pending_signal_count(),
                                None,
                            )
                            .await;
                        continue;
                    }

                    let validation = match pending_buffer.match_pending_signal(&event, &settings, &catalog) {
                        PendingMatchOutcome::Matched(validation) => Some(validation),
                        PendingMatchOutcome::OutcomeMismatch(mismatched_signal) => {
                            attribution_logger
                                .record_activity_event(
                                    "missed_due_to_outcome_mismatch",
                                    event.source.as_str(),
                                    event.generation,
                                    &event.event_id,
                                    &event.wallet,
                                    &event.market_id,
                                    &event.transaction_hash,
                                    event.price,
                                    event.size,
                                    event.timestamp_ms,
                                    pending_buffer.pending_signal_count(),
                                    Some(format!(
                                        "signal_asset={} signal_price={} wallet_price={}",
                                        mismatched_signal.asset_id,
                                        mismatched_signal.price,
                                        event.price
                                    )),
                                )
                                .await;
                            None
                        }
                        PendingMatchOutcome::NoMatch => None,
                    };

                    let Some(matched_trade) =
                        build_wallet_triggered_trade(&event, &catalog, validation.as_ref())
                    else {
                        attribution_logger
                            .record_activity_event(
                                "wallet_detected_missing_execution_fields",
                                event.source.as_str(),
                                event.generation,
                                &event.event_id,
                                &event.wallet,
                                &event.market_id,
                                &event.transaction_hash,
                                event.price,
                                event.size,
                                event.timestamp_ms,
                                pending_buffer.pending_signal_count(),
                                Some("missing asset_id or side and no market validation could enrich the trade".to_owned()),
                            )
                            .await;
                        continue;
                    };

                    attribution_logger
                        .record_activity_event(
                            "wallet_detected",
                            event.source.as_str(),
                            event.generation,
                            &event.event_id,
                            &event.wallet,
                            &event.market_id,
                            &event.transaction_hash,
                            event.price,
                            event.size,
                            event.timestamp_ms,
                            pending_buffer.pending_signal_count(),
                            Some(format!(
                                "tracked_wallet=true validated={} wallet_candidates={}",
                                validation.is_some(),
                                event.wallet_candidates.join(",")
                            )),
                        )
                        .await;

                    if let Some(validation) = validation.as_ref() {
                        log_match(
                            &attribution_logger,
                            event.source.as_str(),
                            &matched_trade.entry,
                            &validation.signal,
                            0,
                            Some(validation.activity_match.correlation_kind),
                        )
                        .await;
                    }

                    dispatch_matched_trade(
                        &attribution_logger,
                        &matched_trade_tx,
                        matched_trade,
                    )
                    .await;
                }
            }
        }
    }
}

async fn record_coordinator_reset(
    attribution_logger: &AttributionLogger,
    event_type: &'static str,
    previous_generation: u64,
    new_generation: u64,
    pending_buffer: &PendingSignalBuffer,
) {
    attribution_logger
        .record_generation_reset(
            event_type,
            previous_generation,
            new_generation,
            pending_buffer.pending_signal_count(),
            pending_buffer.recent_activity_market_count(),
            pending_buffer.recent_activity_event_count(),
            0,
            0,
        )
        .await;
}

async fn evict_expired_pending(
    pending_buffer: &mut PendingSignalBuffer,
    attribution_logger: &AttributionLogger,
) {
    let pending_signal_ttl_ms = pending_buffer.pending_signal_ttl().as_millis() as u64;
    for signal in pending_buffer.evict_expired_signals() {
        debug!(
            asset_id = %signal.asset_id,
            condition_id = %signal.condition_id,
            side = side_label(signal.side),
            validation_window_ms = pending_signal_ttl_ms,
            "short confirmation window expired without tracked-wallet validation"
        );
        attribution_logger
            .record_signal_event(
                "signal_expired",
                &signal,
                pending_buffer.pending_signal_count(),
                0,
                Some(format!(
                    "short confirmation window expired window_ms={pending_signal_ttl_ms}"
                )),
            )
            .await;
    }
}

async fn dispatch_matched_trade(
    attribution_logger: &AttributionLogger,
    matched_trade_tx: &mpsc::UnboundedSender<MatchedTrackedTrade>,
    matched_trade: MatchedTrackedTrade,
) {
    let log_trade = matched_trade.clone();
    if matched_trade_tx.send(matched_trade).is_err() {
        warn!("tracked-wallet confirmation channel is unavailable");
        return;
    }

    let detail = format!(
        "source={} proxy_wallet={} validated={} validation_correlation={} validation_window={} tx_hash_matched={}",
        log_trade.source,
        log_trade.entry.proxy_wallet,
        log_trade.validation_correlation_kind.is_some(),
        log_trade
            .validation_correlation_kind
            .map(|kind| kind.as_str())
            .unwrap_or("none"),
        log_trade
            .validation_match_window
            .map(|window| window.as_str())
            .unwrap_or("none"),
        log_trade.tx_hash_matched,
    );
    if log_trade.validation_correlation_kind == Some(TradeCorrelationKind::Complementary) {
        attribution_logger
            .record_signal_event(
                "complementary_match_detected",
                &log_trade.signal,
                0,
                0,
                Some(detail.clone()),
            )
            .await;
    }
    if log_trade.validation_match_window
        == Some(crate::wallet::wallet_matching::MatchWindow::Fallback)
    {
        attribution_logger
            .record_signal_event(
                "fallback_match_used",
                &log_trade.signal,
                0,
                0,
                Some(detail.clone()),
            )
            .await;
    }
    attribution_logger
        .record_delivery_event(
            "wallet_confirmation_forwarded",
            log_trade.signal.generation,
            &log_trade.entry,
            Some(detail),
        )
        .await;
}

fn forward_matched_trade(
    matched_trade_tx: &mpsc::UnboundedSender<MatchedTrackedTrade>,
    matched_trade: MatchedTrackedTrade,
) {
    if matched_trade_tx.send(matched_trade).is_err() {
        warn!("tracked-wallet confirmation channel is unavailable");
    }
}

#[cfg(test)]
fn wallet_activity_matches_signal(
    entry: &ActivityEntry,
    signal: &ConfirmedTradeSignal,
    settings: &Settings,
) -> bool {
    if entry.condition_id != signal.condition_id {
        return false;
    }

    let entry_timestamp_ms = normalize_timestamp_ms(entry.timestamp);
    let timestamp_delta_ms = signal_reference_timestamps(signal)
        .into_iter()
        .map(|reference| (entry_timestamp_ms - reference).abs())
        .min()
        .unwrap_or(i64::MAX);
    if timestamp_delta_ms > OUTCOME_MISMATCH_WINDOW.as_millis() as i64 {
        return false;
    }

    if !sizes_are_similar(
        entry.size,
        signal.estimated_size,
        decimal_to_f64(settings.activity_size_tolerance_ratio, 0.5),
    ) {
        return false;
    }

    let direct_match = entry.asset == signal.asset_id
        && (entry.price - signal.price).abs()
            <= decimal_to_f64(settings.activity_price_tolerance, 0.01);
    let complementary_match =
        entry.asset != signal.asset_id && is_complementary(entry.price, signal.price);

    direct_match || complementary_match
}

#[cfg(test)]
fn trade_correlation_score(
    wallet_entry: &ActivityEntry,
    market_entry: &ActivityEntry,
    settings: &Settings,
) -> Option<(TradeCorrelationKind, i64, i64, i64)> {
    if wallet_entry.condition_id != market_entry.condition_id {
        return None;
    }

    let wallet_timestamp_ms = normalize_timestamp_ms(wallet_entry.timestamp);
    let market_timestamp_ms = normalize_timestamp_ms(market_entry.timestamp);
    let timestamp_delta_ms = (wallet_timestamp_ms - market_timestamp_ms).abs();
    if timestamp_delta_ms > OUTCOME_MISMATCH_WINDOW.as_millis() as i64 {
        return None;
    }

    let size_tolerance = decimal_to_f64(settings.activity_size_tolerance_ratio, 0.5);
    if !sizes_are_similar(wallet_entry.size, market_entry.size, size_tolerance) {
        return None;
    }

    let direct_price_delta = (market_entry.price - wallet_entry.price).abs();
    let complementary_price_delta =
        complementary_price_delta(market_entry.price, wallet_entry.price);
    let price_tolerance = decimal_to_f64(settings.activity_price_tolerance, 0.01);
    let size_delta_scaled =
        ((market_entry.size - wallet_entry.size).abs() * 1_000_000.0).round() as i64;
    let same_outcome = same_outcome(wallet_entry, market_entry);
    let opposite_outcome = opposite_outcome(wallet_entry, market_entry);

    if !wallet_entry.transaction_hash.is_empty()
        && !market_entry.transaction_hash.is_empty()
        && wallet_entry
            .transaction_hash
            .eq_ignore_ascii_case(&market_entry.transaction_hash)
    {
        return Some((
            if same_outcome {
                TradeCorrelationKind::Direct
            } else {
                TradeCorrelationKind::Complementary
            },
            0,
            0,
            size_delta_scaled,
        ));
    }

    if same_outcome && direct_price_delta <= price_tolerance {
        return Some((
            TradeCorrelationKind::Direct,
            timestamp_delta_ms,
            (direct_price_delta * 1_000_000.0).round() as i64,
            size_delta_scaled,
        ));
    }
    if opposite_outcome && is_complementary(market_entry.price, wallet_entry.price) {
        return Some((
            TradeCorrelationKind::Complementary,
            timestamp_delta_ms,
            (complementary_price_delta * 1_000_000.0).round() as i64,
            size_delta_scaled,
        ));
    }

    None
}

fn trade_correlation_kind_label(kind: TradeCorrelationKind) -> &'static str {
    match kind {
        TradeCorrelationKind::Direct => "direct",
        TradeCorrelationKind::Complementary => "complementary",
    }
}

#[cfg(test)]
fn same_outcome(left: &ActivityEntry, right: &ActivityEntry) -> bool {
    left.asset == right.asset
        || left.outcome_index == right.outcome_index
        || left.outcome.eq_ignore_ascii_case(&right.outcome)
}

#[cfg(test)]
fn opposite_outcome(left: &ActivityEntry, right: &ActivityEntry) -> bool {
    left.asset != right.asset
        || left.outcome_index != right.outcome_index
        || !left.outcome.eq_ignore_ascii_case(&right.outcome)
}

async fn log_match(
    attribution_logger: &AttributionLogger,
    source: &str,
    entry: &ActivityEntry,
    signal: &ConfirmedTradeSignal,
    fallback_attempts: u32,
    correlation_kind: Option<TradeCorrelationKind>,
) {
    let signal_delta_ms = signal_reference_timestamps(signal)
        .into_iter()
        .map(|reference| (normalize_timestamp_ms(entry.timestamp) - reference).abs())
        .min()
        .unwrap_or(0);
    info!(
        source,
        proxy_wallet = %entry.proxy_wallet,
        asset_id = %signal.asset_id,
        condition_id = %signal.condition_id,
        side = side_label(signal.side),
        timestamp_delta_ms = signal_delta_ms,
        price = entry.price,
        size = entry.size,
        fallback_attempts,
        correlation = correlation_kind.map(trade_correlation_kind_label),
        "matched confirmed market signal to wallet activity"
    );
    attribution_logger
        .record_match_event(
            source,
            entry,
            signal,
            fallback_attempts,
            correlation_kind.map(trade_correlation_kind_label),
        )
        .await;
}

fn can_use_user_stream(settings: &Settings) -> bool {
    settings.target_activity_ws_api_key.is_some()
        && settings.target_activity_ws_secret.is_some()
        && settings.target_activity_ws_passphrase.is_some()
}

fn spawn_user_activity_stream(
    settings: Settings,
    catalog: AssetCatalog,
    raw_ring: RingBuffer<RawActivityMessage>,
    health: Arc<HealthState>,
    runtime_control: Arc<RuntimeControl>,
    attribution_logger: Arc<AttributionLogger>,
) {
    tokio::spawn(async move {
        let mut reconnect_state = WebSocketReconnectState::default();
        loop {
            let generation = runtime_control.current_generation();
            if let Some(attempt) = reconnect_state.next_attempt() {
                attribution_logger
                    .record_stream_event(
                        "reconnect_attempt",
                        "user_ws",
                        generation,
                        Some(format!(
                            "attempt={} backoff_ms={}",
                            attempt.attempt,
                            attempt.delay.as_millis()
                        )),
                    )
                    .await;
                sleep(attempt.delay).await;
            }

            let generation = runtime_control.current_generation();
            match connect_async(&settings.polymarket_user_ws).await {
                Ok((stream, _)) => {
                    attribution_logger
                        .record_stream_event("stream_connected", "user_ws", generation, None)
                        .await;
                    let (mut sink, mut source) = stream.split();
                    if let Err(error) = subscribe_user_stream(&mut sink, &settings, &catalog).await
                    {
                        log_websocket_error(&error, "failed to subscribe user activity stream");
                        attribution_logger
                            .record_stream_event(
                                "stream_subscribe_failed",
                                "user_ws",
                                generation,
                                Some(error.to_string()),
                            )
                            .await;
                        if !is_expected_disconnect(&error) {
                            health
                                .set_last_error(format!(
                                    "wallet user stream subscribe failed: {error}"
                                ))
                                .await;
                        }
                        reconnect_state.note_disconnect();
                        continue;
                    } else {
                        runtime_control.mark_wallet_ready(generation);
                        attribution_logger
                            .record_stream_event(
                                "stream_subscribed",
                                "user_ws",
                                generation,
                                Some(format!(
                                    "assets={} conditions={}",
                                    catalog.asset_ids().len(),
                                    catalog.condition_ids().len()
                                )),
                            )
                            .await;
                        if let Some(success) = reconnect_state.mark_recovered() {
                            attribution_logger
                                .record_stream_event(
                                    "reconnect_success",
                                    "user_ws",
                                    generation,
                                    Some(format!(
                                        "attempts={} reconnect_latency_ms={}",
                                        success.attempts,
                                        success.latency.as_millis()
                                    )),
                                )
                                .await;
                        }
                    }

                    let mut heartbeat = interval(RTDS_HEARTBEAT_INTERVAL);
                    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
                    let reconnect_notify = runtime_control.reconnect_notify();

                    loop {
                        tokio::select! {
                            _ = reconnect_notify.notified() => break,
                            _ = heartbeat.tick() => {
                                if runtime_control.should_reconnect(generation) {
                                    attribution_logger
                                        .record_stream_event(
                                            "stream_generation_replaced",
                                            "user_ws",
                                            generation,
                                            Some("runtime requested reconnect".to_owned()),
                                        )
                                        .await;
                                    break;
                                }
                                if let Err(error) = sink.send(Message::Ping(Vec::new().into())).await {
                                    log_websocket_error(&error, "wallet user stream heartbeat failed");
                                    attribution_logger
                                        .record_stream_event(
                                            "stream_heartbeat_failed",
                                            "user_ws",
                                            generation,
                                            Some(error.to_string()),
                                        )
                                        .await;
                                    reconnect_state.note_disconnect();
                                    break;
                                }
                            }
                            message = source.next() => {
                                match message {
                                    Some(Ok(Message::Text(text))) => {
                                        if text.eq_ignore_ascii_case("PONG") {
                                            continue;
                                        }
                                        if text.eq_ignore_ascii_case("PING") {
                                            if let Err(error) = sink.send(Message::Text("PONG".into())).await {
                                                log_websocket_error(&error, "failed to answer wallet text ping");
                                                attribution_logger
                                                    .record_stream_event(
                                                        "stream_ping_response_failed",
                                                        "user_ws",
                                                        generation,
                                                        Some(error.to_string()),
                                                    )
                                                    .await;
                                                reconnect_state.note_disconnect();
                                                break;
                                            }
                                            continue;
                                        }

                                        let observed_at = Instant::now();
                                        let observed_at_utc = Utc::now();
                                        let message_id = format!(
                                            "{}:{}:user_text:{}",
                                            generation,
                                            observed_at_utc.timestamp_micros(),
                                            text.len()
                                        );
                                        raw_ring.push(RawActivityMessage {
                                            generation,
                                            message_id: message_id.into_boxed_str(),
                                            payload: text.to_string().into_boxed_str(),
                                            observed_at,
                                            observed_at_utc,
                                        });
                                    }
                                    Some(Ok(Message::Ping(payload))) => {
                                        if let Err(error) = sink.send(Message::Pong(payload)).await {
                                            log_websocket_error(&error, "failed to answer wallet ping");
                                            attribution_logger
                                                .record_stream_event(
                                                    "stream_ping_response_failed",
                                                    "user_ws",
                                                    generation,
                                                    Some(error.to_string()),
                                                )
                                                .await;
                                            reconnect_state.note_disconnect();
                                            break;
                                        }
                                    }
                                    Some(Ok(Message::Pong(_))) => {}
                                    Some(Ok(Message::Close(frame))) => {
                                        attribution_logger
                                            .record_stream_event(
                                                "stream_closed",
                                                "user_ws",
                                                generation,
                                                Some(format!("{frame:?}")),
                                            )
                                            .await;
                                        reconnect_state.note_disconnect();
                                        break;
                                    }
                                    Some(Err(error)) => {
                                        log_websocket_error(&error, "wallet user stream read failed");
                                        attribution_logger
                                            .record_stream_event(
                                                "stream_read_failed",
                                                "user_ws",
                                                generation,
                                                Some(error.to_string()),
                                            )
                                            .await;
                                        reconnect_state.note_disconnect();
                                        break;
                                    }
                                    None => {
                                        attribution_logger
                                            .record_stream_event(
                                                "stream_ended",
                                                "user_ws",
                                                generation,
                                                Some("wallet user websocket returned EOF".to_owned()),
                                            )
                                            .await;
                                        reconnect_state.note_disconnect();
                                        break;
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                }
                Err(error) => {
                    if is_expected_disconnect(&error) {
                        info!(
                            ?error,
                            "wallet user stream connect interrupted; reconnecting"
                        );
                    } else {
                        error!(?error, "wallet user stream connect failed");
                    }
                    attribution_logger
                        .record_stream_event(
                            "stream_connect_failed",
                            "user_ws",
                            generation,
                            Some(error.to_string()),
                        )
                        .await;
                    if !is_expected_disconnect(&error) {
                        health
                            .set_last_error(format!("wallet user stream connect failed: {error}"))
                            .await;
                    }
                    reconnect_state.note_disconnect();
                }
            }
        }
    });
}

fn spawn_public_activity_stream(
    settings: Settings,
    raw_ring: RingBuffer<RawActivityMessage>,
    raw_activity_logger: RawActivityLogger,
    health: Arc<HealthState>,
    runtime_control: Arc<RuntimeControl>,
    attribution_logger: Arc<AttributionLogger>,
) {
    tokio::spawn(async move {
        let mut reconnect_state = WebSocketReconnectState::default();
        loop {
            let generation = runtime_control.current_generation();
            if let Some(attempt) = reconnect_state.next_attempt() {
                attribution_logger
                    .record_stream_event(
                        "reconnect_attempt",
                        "activity_ws",
                        generation,
                        Some(format!(
                            "attempt={} backoff_ms={}",
                            attempt.attempt,
                            attempt.delay.as_millis()
                        )),
                    )
                    .await;
                sleep(attempt.delay).await;
            }

            let generation = runtime_control.current_generation();
            match connect_async(&settings.polymarket_activity_ws).await {
                Ok((stream, _)) => {
                    attribution_logger
                        .record_stream_event("stream_connected", "activity_ws", generation, None)
                        .await;
                    let (mut sink, mut source) = stream.split();
                    if let Err(error) = subscribe_public_activity_stream(
                        &mut sink,
                        &settings,
                        &raw_activity_logger,
                        generation,
                    )
                    .await
                    {
                        log_websocket_error(&error, "failed to subscribe activity trade stream");
                        attribution_logger
                            .record_stream_event(
                                "stream_subscribe_failed",
                                "activity_ws",
                                generation,
                                Some(error.to_string()),
                            )
                            .await;
                        if !is_expected_disconnect(&error) {
                            health
                                .set_last_error(format!(
                                    "activity trade stream subscribe failed: {error}"
                                ))
                                .await;
                        }
                        reconnect_state.note_disconnect();
                        continue;
                    } else {
                        runtime_control.mark_wallet_ready(generation);
                        attribution_logger
                            .record_stream_event(
                                "stream_subscribed",
                                "activity_ws",
                                generation,
                                None,
                            )
                            .await;
                        if let Some(success) = reconnect_state.mark_recovered() {
                            attribution_logger
                                .record_stream_event(
                                    "reconnect_success",
                                    "activity_ws",
                                    generation,
                                    Some(format!(
                                        "attempts={} reconnect_latency_ms={}",
                                        success.attempts,
                                        success.latency.as_millis()
                                    )),
                                )
                                .await;
                        }
                    }

                    let mut heartbeat = interval(WS_HEARTBEAT_INTERVAL);
                    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
                    let reconnect_notify = runtime_control.reconnect_notify();

                    loop {
                        tokio::select! {
                            _ = reconnect_notify.notified() => break,
                            _ = heartbeat.tick() => {
                                if runtime_control.should_reconnect(generation) {
                                    attribution_logger
                                        .record_stream_event(
                                            "stream_generation_replaced",
                                            "activity_ws",
                                            generation,
                                            Some("runtime requested reconnect".to_owned()),
                                        )
                                        .await;
                                    break;
                                }
                                if let Err(error) = sink.send(Message::Text("PING".into())).await {
                                    log_websocket_error(&error, "activity trade stream heartbeat failed");
                                    attribution_logger
                                        .record_stream_event(
                                            "stream_heartbeat_failed",
                                            "activity_ws",
                                            generation,
                                            Some(error.to_string()),
                                        )
                                        .await;
                                    reconnect_state.note_disconnect();
                                    break;
                                }
                            }
                            message = source.next() => {
                                match message {
                                    Some(Ok(Message::Text(text))) => {
                                        if text.eq_ignore_ascii_case("PONG") {
                                            continue;
                                        }
                                        if text.eq_ignore_ascii_case("PING") {
                                            if let Err(error) = sink.send(Message::Text("PONG".into())).await {
                                                log_websocket_error(&error, "failed to answer activity stream text ping");
                                                attribution_logger
                                                    .record_stream_event(
                                                        "stream_ping_response_failed",
                                                        "activity_ws",
                                                        generation,
                                                        Some(error.to_string()),
                                                    )
                                                    .await;
                                                reconnect_state.note_disconnect();
                                                break;
                                            }
                                            continue;
                                        }

                                        let observed_at = Instant::now();
                                        let observed_at_utc = Utc::now();
                                        let message_id = format!(
                                            "{}:{}:text:{}",
                                            generation,
                                            observed_at_utc.timestamp_micros(),
                                            text.len()
                                        );
                                        raw_activity_logger
                                            .record_raw_message(
                                                &message_id,
                                                generation,
                                                ACTIVITY_DEBUG_STREAM,
                                                "text",
                                                &text,
                                            )
                                            .await;
                                        raw_ring.push(RawActivityMessage {
                                            generation,
                                            message_id: message_id.into_boxed_str(),
                                            payload: text.to_string().into_boxed_str(),
                                            observed_at,
                                            observed_at_utc,
                                        });
                                    }
                                    Some(Ok(Message::Binary(payload))) => {
                                        let decoded = String::from_utf8_lossy(&payload).into_owned();
                                        let observed_at = Instant::now();
                                        let observed_at_utc = Utc::now();
                                        let message_id = format!(
                                            "{}:{}:binary:{}",
                                            generation,
                                            observed_at_utc.timestamp_micros(),
                                            decoded.len()
                                        );
                                        raw_activity_logger
                                            .record_raw_message(
                                                &message_id,
                                                generation,
                                                ACTIVITY_DEBUG_STREAM,
                                                "binary_lossy_utf8",
                                                &decoded,
                                            )
                                            .await;
                                        raw_ring.push(RawActivityMessage {
                                            generation,
                                            message_id: message_id.into_boxed_str(),
                                            payload: decoded.into_boxed_str(),
                                            observed_at,
                                            observed_at_utc,
                                        });
                                    }
                                    Some(Ok(Message::Ping(payload))) => {
                                        if let Err(error) = sink.send(Message::Pong(payload)).await {
                                            log_websocket_error(&error, "failed to answer activity stream ping");
                                            attribution_logger
                                                .record_stream_event(
                                                    "stream_ping_response_failed",
                                                    "activity_ws",
                                                    generation,
                                                    Some(error.to_string()),
                                                )
                                                .await;
                                            reconnect_state.note_disconnect();
                                            break;
                                        }
                                    }
                                    Some(Ok(Message::Pong(_))) => {}
                                    Some(Ok(Message::Close(frame))) => {
                                        attribution_logger
                                            .record_stream_event(
                                                "stream_closed",
                                                "activity_ws",
                                                generation,
                                                Some(format!("{frame:?}")),
                                            )
                                            .await;
                                        reconnect_state.note_disconnect();
                                        break;
                                    }
                                    Some(Err(error)) => {
                                        log_websocket_error(&error, "activity trade stream read failed");
                                        attribution_logger
                                            .record_stream_event(
                                                "stream_read_failed",
                                                "activity_ws",
                                                generation,
                                                Some(error.to_string()),
                                            )
                                            .await;
                                        reconnect_state.note_disconnect();
                                        break;
                                    }
                                    None => {
                                        attribution_logger
                                            .record_stream_event(
                                                "stream_ended",
                                                "activity_ws",
                                                generation,
                                                Some("activity websocket returned EOF".to_owned()),
                                            )
                                            .await;
                                        reconnect_state.note_disconnect();
                                        break;
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                }
                Err(error) => {
                    if is_expected_disconnect(&error) {
                        info!(
                            ?error,
                            "activity trade stream connect interrupted; reconnecting"
                        );
                    } else {
                        error!(?error, "activity trade stream connect failed");
                    }
                    attribution_logger
                        .record_stream_event(
                            "stream_connect_failed",
                            "activity_ws",
                            generation,
                            Some(error.to_string()),
                        )
                        .await;
                    if !is_expected_disconnect(&error) {
                        health
                            .set_last_error(format!(
                                "activity trade stream connect failed: {error}"
                            ))
                            .await;
                    }
                    reconnect_state.note_disconnect();
                }
            }
        }
    });
}

async fn subscribe_user_stream<S>(
    sink: &mut S,
    settings: &Settings,
    catalog: &AssetCatalog,
) -> std::result::Result<(), WsError>
where
    S: futures_util::Sink<Message, Error = WsError> + Unpin,
{
    let subscription_delay = effective_wallet_subscription_delay(settings);
    let auth = json!({
        "auth": {
            "apiKey": settings.target_activity_ws_api_key.as_deref().unwrap_or_default(),
            "secret": settings.target_activity_ws_secret.as_deref().unwrap_or_default(),
            "passphrase": settings.target_activity_ws_passphrase.as_deref().unwrap_or_default(),
        },
        "type": "user"
    });
    sink.send(Message::Text(auth.to_string().into())).await?;

    for chunk in catalog
        .condition_ids()
        .chunks(settings.wallet_subscription_batch_size.max(1))
    {
        let payload = json!({
            "operation": "subscribe",
            "markets": chunk,
        });
        sink.send(Message::Text(payload.to_string().into())).await?;
        if subscription_delay > Duration::ZERO {
            sleep(subscription_delay).await;
        }
    }
    Ok(())
}

async fn subscribe_public_activity_stream<S>(
    sink: &mut S,
    settings: &Settings,
    raw_activity_logger: &RawActivityLogger,
    generation: u64,
) -> std::result::Result<(), WsError>
where
    S: futures_util::Sink<Message, Error = WsError> + Unpin,
{
    let subscription_delay = effective_wallet_subscription_delay(settings);
    let payload = json!({
        "action": "subscribe",
        "subscriptions": [
            {
                "topic": "activity",
                "type": "trades"
            }
        ]
    });
    let payload_text = payload.to_string();
    let message_id = format!("{generation}:0:outbound_subscribe:1");
    raw_activity_logger
        .record_raw_message(
            &message_id,
            generation,
            ACTIVITY_DEBUG_STREAM,
            "outbound_subscribe",
            &payload_text,
        )
        .await;
    sink.send(Message::Text(payload_text.into())).await?;
    if subscription_delay > Duration::ZERO {
        sleep(subscription_delay).await;
    }
    Ok(())
}

fn effective_wallet_subscription_delay(settings: &Settings) -> Duration {
    if settings.hot_path_mode {
        settings
            .wallet_subscription_delay
            .min(Duration::from_millis(5))
    } else {
        settings.wallet_subscription_delay
    }
}

fn parse_user_trade_message(
    payload: &str,
    catalog: &AssetCatalog,
    primary_wallet: &str,
    generation: u64,
    observed_at: Instant,
    observed_at_utc: DateTime<Utc>,
) -> Option<ActivityTradeEvent> {
    let event = serde_json::from_str::<UserTradeEvent>(payload).ok()?;
    if !event.event_type.eq_ignore_ascii_case("trade")
        || !event.status.eq_ignore_ascii_case("MATCHED")
    {
        return None;
    }

    let metadata = catalog.metadata(&event.asset_id)?;
    let timestamp_ms = event
        .matchtime
        .as_deref()
        .or(event.timestamp.as_deref())
        .and_then(parse_raw_timestamp_ms)
        .unwrap_or_else(|| Utc::now().timestamp_millis());
    let size = event.size.parse::<f64>().ok()?;
    let price = event.price.parse::<f64>().ok()?;
    let transaction_hash = if event.transaction_hash.is_empty() {
        event.id.clone()
    } else {
        event.transaction_hash.clone()
    };
    let parse_completed_at = Instant::now();
    let parse_completed_at_utc = Utc::now();
    let wallet = normalize_wallet(primary_wallet);
    let event_id = build_event_id(
        Some(&transaction_hash),
        &wallet,
        &metadata.condition_id,
        timestamp_ms,
        price,
        size,
    );

    Some(ActivityTradeEvent {
        event_id,
        wallet: wallet.clone(),
        wallet_candidates: vec![wallet.clone()],
        market_id: metadata.condition_id.clone(),
        price,
        size,
        timestamp_ms,
        transaction_hash,
        asset_id: Some(metadata.asset_id.clone()),
        side: Some(normalize_side(&event.side)),
        generation,
        source: ActivitySource::UserStream,
        observed_at,
        observed_at_utc,
        parse_completed_at,
        parse_completed_at_utc,
    })
}

fn parse_public_activity_trade_messages(
    payload: &str,
    generation: u64,
    observed_at: Instant,
    observed_at_utc: DateTime<Utc>,
) -> PublicActivityParseReport {
    let parse_completed_at = Instant::now();
    let parse_completed_at_utc = Utc::now();
    let Ok(value) = serde_json::from_str::<Value>(payload) else {
        return PublicActivityParseReport {
            failure_detail: Some("invalid_json".to_owned()),
            ..PublicActivityParseReport::default()
        };
    };

    let context = ActivityEnvelopeContext {
        topic: read_string_field(&value, ACTIVITY_TOPIC_FIELDS),
        event_type: read_string_field(&value, ACTIVITY_TYPE_FIELDS),
    };
    let mut events = Vec::new();
    let mut seen_event_ids = HashSet::new();
    collect_public_activity_trade_events(
        &value,
        generation,
        observed_at,
        observed_at_utc,
        parse_completed_at,
        parse_completed_at_utc,
        &context,
        &mut seen_event_ids,
        &mut events,
    );

    let failure_detail = if events.is_empty() && looks_like_activity_payload(&value, &context) {
        Some(format!(
            "unknown_structure shape={} candidates={}",
            summarize_shape(&value, 0),
            summarize_candidates(&value)
        ))
    } else {
        None
    };

    PublicActivityParseReport {
        topic: context.topic,
        event_type: context.event_type,
        events,
        failure_detail,
    }
}

fn collect_public_activity_trade_events(
    value: &Value,
    generation: u64,
    observed_at: Instant,
    observed_at_utc: DateTime<Utc>,
    parse_completed_at: Instant,
    parse_completed_at_utc: DateTime<Utc>,
    inherited: &ActivityEnvelopeContext,
    seen_event_ids: &mut HashSet<String>,
    output: &mut Vec<ActivityTradeEvent>,
) {
    match value {
        Value::Array(items) => {
            for item in items {
                collect_public_activity_trade_events(
                    item,
                    generation,
                    observed_at,
                    observed_at_utc,
                    parse_completed_at,
                    parse_completed_at_utc,
                    inherited,
                    seen_event_ids,
                    output,
                );
            }
        }
        Value::Object(map) => {
            let context = ActivityEnvelopeContext {
                topic: read_string_field(value, ACTIVITY_TOPIC_FIELDS)
                    .or_else(|| inherited.topic.clone()),
                event_type: read_string_field(value, ACTIVITY_TYPE_FIELDS)
                    .or_else(|| inherited.event_type.clone()),
            };
            if let Some(event) = parse_public_activity_trade_event(
                value,
                generation,
                observed_at,
                observed_at_utc,
                parse_completed_at,
                parse_completed_at_utc,
                &context,
            ) {
                if seen_event_ids.insert(event.event_id.clone()) {
                    output.push(event);
                }
            }

            for key in ACTIVITY_FIELD_KEYS {
                if let Some(nested) = map.get(*key) {
                    collect_public_activity_trade_events(
                        nested,
                        generation,
                        observed_at,
                        observed_at_utc,
                        parse_completed_at,
                        parse_completed_at_utc,
                        &context,
                        seen_event_ids,
                        output,
                    );
                }
            }
        }
        _ => {}
    }
}

fn parse_public_activity_trade_event(
    value: &Value,
    generation: u64,
    observed_at: Instant,
    observed_at_utc: DateTime<Utc>,
    parse_completed_at: Instant,
    parse_completed_at_utc: DateTime<Utc>,
    context: &ActivityEnvelopeContext,
) -> Option<ActivityTradeEvent> {
    if context
        .topic
        .as_deref()
        .is_some_and(|topic| !topic.eq_ignore_ascii_case("activity"))
    {
        return None;
    }
    if context.event_type.as_deref().is_some_and(|value| {
        !value.eq_ignore_ascii_case("trade") && !value.eq_ignore_ascii_case("trades")
    }) && !has_activity_trade_fields(value)
    {
        return None;
    }

    let wallet_candidates = extract_wallet_candidates(value);
    let wallet = wallet_candidates.first().cloned().unwrap_or_default();
    let market_id = read_string_field_deep(value, ACTIVITY_MARKET_FIELDS)?;
    let price = read_f64_field_deep(value, ACTIVITY_PRICE_FIELDS)?;
    let size = read_f64_field_deep(value, ACTIVITY_SIZE_FIELDS)?;
    let timestamp_ms = read_timestamp_ms_field_deep(value, ACTIVITY_TIMESTAMP_FIELDS)?;
    let transaction_hash =
        read_string_field_deep(value, ACTIVITY_TX_HASH_FIELDS).unwrap_or_default();
    let asset_id = read_string_field_deep(value, ACTIVITY_ASSET_FIELDS);
    let side =
        read_string_field_deep(value, ACTIVITY_SIDE_FIELDS).map(|value| normalize_side(&value));
    let event_id = build_event_id(
        (!transaction_hash.is_empty()).then_some(transaction_hash.as_str()),
        &wallet,
        &market_id,
        timestamp_ms,
        price,
        size,
    );

    Some(ActivityTradeEvent {
        event_id,
        wallet,
        wallet_candidates,
        market_id,
        price,
        size,
        timestamp_ms,
        transaction_hash,
        asset_id,
        side,
        generation,
        source: ActivitySource::ActivityStream,
        observed_at,
        observed_at_utc,
        parse_completed_at,
        parse_completed_at_utc,
    })
}

fn has_activity_trade_fields(value: &Value) -> bool {
    read_string_field_deep(value, ACTIVITY_MARKET_FIELDS).is_some()
        && read_f64_field_deep(value, ACTIVITY_PRICE_FIELDS).is_some()
        && read_f64_field_deep(value, ACTIVITY_SIZE_FIELDS).is_some()
        && read_timestamp_ms_field_deep(value, ACTIVITY_TIMESTAMP_FIELDS).is_some()
}

fn build_client(settings: &Settings) -> Client {
    Client::builder()
        .connect_timeout(settings.http_timeout / 2)
        .timeout(settings.http_timeout)
        .pool_max_idle_per_host(8)
        .pool_idle_timeout(Duration::from_secs(30))
        .tcp_keepalive(Duration::from_secs(30))
        .tcp_nodelay(true)
        .http2_adaptive_window(true)
        .user_agent("polymarket-copy-bot/0.1.0")
        .build()
        .expect("reqwest client")
}

fn read_string_field(value: &Value, fields: &[&str]) -> Option<String> {
    fields.iter().find_map(|field| {
        value.get(*field).and_then(|value| match value {
            Value::String(raw) => Some(raw.clone()),
            Value::Number(number) => Some(number.to_string()),
            Value::Bool(boolean) => Some(boolean.to_string()),
            _ => None,
        })
    })
}

fn read_string_field_deep(value: &Value, fields: &[&str]) -> Option<String> {
    read_deep_field(value, fields, &|candidate| match candidate {
        Value::String(raw) => Some(raw.clone()),
        Value::Number(number) => Some(number.to_string()),
        Value::Bool(boolean) => Some(boolean.to_string()),
        _ => None,
    })
}

fn read_string_fields_deep(value: &Value, fields: &[&str]) -> Vec<String> {
    let mut values = Vec::new();
    collect_deep_fields(
        value,
        fields,
        &|candidate| match candidate {
            Value::String(raw) => Some(raw.clone()),
            Value::Number(number) => Some(number.to_string()),
            Value::Bool(boolean) => Some(boolean.to_string()),
            _ => None,
        },
        &mut values,
    );
    values
}

fn read_f64_field_deep(value: &Value, fields: &[&str]) -> Option<f64> {
    read_deep_field(value, fields, &|candidate| match candidate {
        Value::String(raw) => raw.parse::<f64>().ok(),
        Value::Number(number) => number.as_f64(),
        _ => None,
    })
}

fn read_timestamp_ms_field_deep(value: &Value, fields: &[&str]) -> Option<i64> {
    read_deep_field(value, fields, &|candidate| match candidate {
        Value::String(raw) => parse_raw_timestamp_ms(raw),
        Value::Number(number) => number.as_i64().map(normalize_timestamp_ms),
        _ => None,
    })
}

fn read_deep_field<T, F>(value: &Value, fields: &[&str], decode: &F) -> Option<T>
where
    F: Fn(&Value) -> Option<T>,
{
    if let Some(found) = fields
        .iter()
        .find_map(|field| value.get(*field).and_then(decode))
    {
        return Some(found);
    }

    match value {
        Value::Array(items) => items
            .iter()
            .find_map(|item| read_deep_field(item, fields, decode)),
        Value::Object(map) => map
            .values()
            .find_map(|item| read_deep_field(item, fields, decode)),
        _ => None,
    }
}

fn collect_deep_fields<T, F>(value: &Value, fields: &[&str], decode: &F, output: &mut Vec<T>)
where
    F: Fn(&Value) -> Option<T>,
{
    if let Value::Object(map) = value {
        for field in fields {
            if let Some(decoded) = map.get(*field).and_then(decode) {
                output.push(decoded);
            }
        }
    }

    match value {
        Value::Array(items) => {
            for item in items {
                collect_deep_fields(item, fields, decode, output);
            }
        }
        Value::Object(map) => {
            for item in map.values() {
                collect_deep_fields(item, fields, decode, output);
            }
        }
        _ => {}
    }
}

fn extract_wallet_candidates(value: &Value) -> Vec<String> {
    let mut seen = HashSet::new();
    read_string_fields_deep(value, ACTIVITY_WALLET_FIELDS)
        .into_iter()
        .map(|wallet| normalize_wallet(&wallet))
        .filter(|wallet| !wallet.is_empty())
        .filter(|wallet| seen.insert(wallet.clone()))
        .collect()
}

fn looks_like_activity_payload(value: &Value, context: &ActivityEnvelopeContext) -> bool {
    context
        .topic
        .as_deref()
        .is_some_and(|topic| topic.eq_ignore_ascii_case("activity"))
        || context.event_type.as_deref().is_some_and(|kind| {
            kind.eq_ignore_ascii_case("trade") || kind.eq_ignore_ascii_case("trades")
        })
        || has_activity_trade_fields(value)
        || read_string_field_deep(value, ACTIVITY_TX_HASH_FIELDS).is_some()
}

fn summarize_candidates(value: &Value) -> String {
    format!(
        "wallet={} market={} price={} size={} tx_hash={} asset={} side={} timestamp={}",
        {
            let wallets = extract_wallet_candidates(value);
            if wallets.is_empty() {
                "<missing>".to_owned()
            } else {
                wallets.join("|")
            }
        },
        read_string_field_deep(value, ACTIVITY_MARKET_FIELDS)
            .unwrap_or_else(|| "<missing>".to_owned()),
        read_f64_field_deep(value, ACTIVITY_PRICE_FIELDS)
            .map(|value| format!("{value}"))
            .unwrap_or_else(|| "<missing>".to_owned()),
        read_f64_field_deep(value, ACTIVITY_SIZE_FIELDS)
            .map(|value| format!("{value}"))
            .unwrap_or_else(|| "<missing>".to_owned()),
        read_string_field_deep(value, ACTIVITY_TX_HASH_FIELDS)
            .unwrap_or_else(|| "<missing>".to_owned()),
        read_string_field_deep(value, ACTIVITY_ASSET_FIELDS)
            .unwrap_or_else(|| "<missing>".to_owned()),
        read_string_field_deep(value, ACTIVITY_SIDE_FIELDS)
            .unwrap_or_else(|| "<missing>".to_owned()),
        read_timestamp_ms_field_deep(value, ACTIVITY_TIMESTAMP_FIELDS)
            .map(|value| value.to_string())
            .unwrap_or_else(|| "<missing>".to_owned()),
    )
}

fn summarize_shape(value: &Value, depth: usize) -> String {
    if depth >= 2 {
        return match value {
            Value::Object(map) => format!("object({})", map.len()),
            Value::Array(items) => format!("array({})", items.len()),
            Value::String(_) => "string".to_owned(),
            Value::Number(_) => "number".to_owned(),
            Value::Bool(_) => "bool".to_owned(),
            Value::Null => "null".to_owned(),
        };
    }

    match value {
        Value::Object(map) => {
            let mut entries = map
                .iter()
                .take(8)
                .map(|(key, child)| format!("{key}:{}", summarize_shape(child, depth + 1)))
                .collect::<Vec<_>>();
            if map.len() > 8 {
                entries.push("...".to_owned());
            }
            format!("{{{}}}", entries.join(","))
        }
        Value::Array(items) => {
            let preview = items
                .iter()
                .take(3)
                .map(|child| summarize_shape(child, depth + 1))
                .collect::<Vec<_>>();
            if items.len() > 3 {
                format!("[{}...]", preview.join(","))
            } else {
                format!("[{}]", preview.join(","))
            }
        }
        Value::String(_) => "string".to_owned(),
        Value::Number(_) => "number".to_owned(),
        Value::Bool(_) => "bool".to_owned(),
        Value::Null => "null".to_owned(),
    }
}

fn correlate_walletless_event(
    market_id: &str,
    transaction_hash: &str,
    size: f64,
    timestamp_ms: i64,
    event: &ActivityTradeEvent,
    settings: &Settings,
) -> Option<&'static str> {
    if market_id != event.market_id {
        return None;
    }
    if !transaction_hash.is_empty()
        && !event.transaction_hash.is_empty()
        && transaction_hash.eq_ignore_ascii_case(&event.transaction_hash)
    {
        return Some("tx_hash");
    }
    if (timestamp_ms - event.timestamp_ms).abs() > WALLET_CORRELATION_WINDOW.as_millis() as i64 {
        return None;
    }
    if sizes_are_similar(
        size,
        event.size,
        decimal_to_f64(settings.activity_size_tolerance_ratio, 0.5),
    ) {
        return Some("size_timestamp");
    }
    None
}

fn merge_activity_events(
    tracked_event: ActivityTradeEvent,
    walletless_event: ActivityTradeEvent,
) -> ActivityTradeEvent {
    let mut wallet_candidates = tracked_event.wallet_candidates.clone();
    for wallet in walletless_event.wallet_candidates {
        if !wallet_candidates
            .iter()
            .any(|candidate| candidate.eq_ignore_ascii_case(&wallet))
        {
            wallet_candidates.push(wallet);
        }
    }

    let (observed_at, observed_at_utc) = if walletless_event.observed_at < tracked_event.observed_at
    {
        (
            walletless_event.observed_at,
            walletless_event.observed_at_utc,
        )
    } else {
        (tracked_event.observed_at, tracked_event.observed_at_utc)
    };
    let (parse_completed_at, parse_completed_at_utc) =
        if walletless_event.parse_completed_at < tracked_event.parse_completed_at {
            (
                walletless_event.parse_completed_at,
                walletless_event.parse_completed_at_utc,
            )
        } else {
            (
                tracked_event.parse_completed_at,
                tracked_event.parse_completed_at_utc,
            )
        };

    ActivityTradeEvent {
        event_id: tracked_event.event_id,
        wallet: tracked_event.wallet,
        wallet_candidates,
        market_id: tracked_event.market_id,
        price: tracked_event.price,
        size: tracked_event.size,
        timestamp_ms: tracked_event.timestamp_ms,
        transaction_hash: if tracked_event.transaction_hash.is_empty() {
            walletless_event.transaction_hash
        } else {
            tracked_event.transaction_hash
        },
        asset_id: tracked_event.asset_id.or(walletless_event.asset_id),
        side: tracked_event.side.or(walletless_event.side),
        generation: tracked_event.generation,
        source: tracked_event.source,
        observed_at,
        observed_at_utc,
        parse_completed_at,
        parse_completed_at_utc,
    }
}

fn parse_raw_timestamp_ms(raw: &str) -> Option<i64> {
    raw.parse::<i64>()
        .ok()
        .map(normalize_timestamp_ms)
        .or_else(|| {
            DateTime::parse_from_rfc3339(raw)
                .ok()
                .map(|timestamp| timestamp.timestamp_millis())
        })
}

fn build_event_id(
    transaction_hash: Option<&str>,
    wallet: &str,
    market_id: &str,
    timestamp_ms: i64,
    price: f64,
    size: f64,
) -> String {
    match transaction_hash.filter(|hash| !hash.is_empty()) {
        Some(hash) => format!("{}:{}", normalize_wallet(hash), market_id),
        None => format!(
            "{wallet}:{market_id}:{timestamp_ms}:{:.6}:{:.6}",
            price, size
        ),
    }
}

fn normalize_side(side: &str) -> String {
    side.trim().to_ascii_uppercase()
}

fn side_label(side: ExecutionSide) -> &'static str {
    match side {
        ExecutionSide::Buy => "BUY",
        ExecutionSide::Sell => "SELL",
    }
}

fn normalize_timestamp_ms(timestamp: i64) -> i64 {
    if timestamp >= 1_000_000_000_000 {
        timestamp
    } else {
        timestamp * 1000
    }
}

fn decimal_to_f64(value: rust_decimal::Decimal, fallback: f64) -> f64 {
    value.to_string().parse::<f64>().unwrap_or(fallback)
}

#[cfg(test)]
fn complementary_price_delta(entry_price: f64, signal_price: f64) -> f64 {
    (1.0 - entry_price - signal_price).abs()
}

impl RecentKeyStore {
    fn clear(&mut self) {
        self.seen.clear();
        self.order.clear();
    }

    fn evict(&mut self, ttl: Duration) {
        while let Some((observed_at, key)) = self.order.front() {
            if observed_at.elapsed() > ttl {
                let key = key.clone();
                self.order.pop_front();
                self.seen.remove(&key);
            } else {
                break;
            }
        }
        while self.order.len() > RECENT_KEY_CAPACITY {
            if let Some((_, key)) = self.order.pop_front() {
                self.seen.remove(&key);
            }
        }
    }

    fn insert(&mut self, key: String, ttl: Duration) -> bool {
        self.evict(ttl);
        if !self.seen.insert(key.clone()) {
            return false;
        }
        self.order.push_back((Instant::now(), key));
        true
    }
}

impl ActivityCorrelationBuffer {
    fn clear(&mut self) {
        self.tracked_hints.clear();
        self.walletless_events.clear();
    }

    fn evict_expired(&mut self) {
        while self
            .tracked_hints
            .front()
            .is_some_and(|hint| hint.observed_at.elapsed() > WALLET_CORRELATION_WINDOW)
        {
            self.tracked_hints.pop_front();
        }
        while self
            .walletless_events
            .front()
            .is_some_and(|event| event.observed_at.elapsed() > WALLET_CORRELATION_WINDOW)
        {
            self.walletless_events.pop_front();
        }
    }

    fn store_tracked_hint(&mut self, event: &ActivityTradeEvent) {
        self.evict_expired();
        if self.tracked_hints.iter().any(|hint| {
            hint.wallet == event.wallet
                && hint.market_id == event.market_id
                && hint
                    .transaction_hash
                    .eq_ignore_ascii_case(&event.transaction_hash)
                && hint.timestamp_ms == event.timestamp_ms
                && (hint.size - event.size).abs() < 0.000_001
        }) {
            return;
        }
        self.tracked_hints.push_back(TrackedWalletHint {
            wallet: event.wallet.clone(),
            market_id: event.market_id.clone(),
            transaction_hash: event.transaction_hash.clone(),
            size: event.size,
            timestamp_ms: event.timestamp_ms,
            observed_at: Instant::now(),
        });
    }

    fn store_walletless_event(&mut self, event: &ActivityTradeEvent) {
        self.evict_expired();
        if self
            .walletless_events
            .iter()
            .any(|candidate| candidate.event.event_id == event.event_id)
        {
            return;
        }
        self.walletless_events.push_back(WalletlessActivityEvent {
            event: event.clone(),
            observed_at: Instant::now(),
        });
    }

    fn resolve_wallet(
        &mut self,
        event: &ActivityTradeEvent,
        settings: &Settings,
    ) -> Option<(&str, &'static str)> {
        self.evict_expired();
        self.tracked_hints
            .iter()
            .filter_map(|hint| {
                correlate_walletless_event(
                    &hint.market_id,
                    &hint.transaction_hash,
                    hint.size,
                    hint.timestamp_ms,
                    event,
                    settings,
                )
                .map(|kind| (hint.wallet.as_str(), kind))
            })
            .next()
    }

    fn take_walletless_match(
        &mut self,
        event: &ActivityTradeEvent,
        settings: &Settings,
    ) -> Option<(ActivityTradeEvent, &'static str)> {
        self.evict_expired();
        let index = self.walletless_events.iter().position(|candidate| {
            correlate_walletless_event(
                &candidate.event.market_id,
                &candidate.event.transaction_hash,
                candidate.event.size,
                candidate.event.timestamp_ms,
                event,
                settings,
            )
            .is_some()
        })?;
        let candidate = self.walletless_events.remove(index)?;
        let correlation_kind = correlate_walletless_event(
            &candidate.event.market_id,
            &candidate.event.transaction_hash,
            candidate.event.size,
            candidate.event.timestamp_ms,
            event,
            settings,
        )?;
        Some((candidate.event, correlation_kind))
    }
}

#[derive(Debug, Deserialize)]
struct UserTradeEvent {
    event_type: String,
    id: String,
    #[serde(rename = "market")]
    _market: String,
    asset_id: String,
    side: String,
    size: String,
    price: String,
    status: String,
    #[serde(default)]
    matchtime: Option<String>,
    #[serde(default)]
    timestamp: Option<String>,
    #[serde(default)]
    transaction_hash: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Settings;
    use crate::models::TradeStageTimestamps;

    fn test_settings() -> Settings {
        let mut settings = Settings::default_for_tests(std::path::PathBuf::from("./data"));
        settings.target_profile_addresses = vec!["0xtarget".to_owned()];
        settings.market_cache_ttl = Duration::from_secs(3);
        settings.min_source_trade_usdc = rust_decimal_macros::dec!(3);
        settings
    }

    fn sample_signal() -> ConfirmedTradeSignal {
        let now = Utc::now();
        let instant = Instant::now();
        ConfirmedTradeSignal {
            asset_id: "asset-1".to_owned(),
            condition_id: "condition-1".to_owned(),
            transaction_hash: None,
            side: ExecutionSide::Buy,
            price: 0.51,
            estimated_size: 10.0,
            stage_timestamps: TradeStageTimestamps {
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

    fn sample_activity_entry(
        asset: &str,
        outcome_index: i64,
        outcome: &str,
        price: f64,
        timestamp: i64,
    ) -> ActivityEntry {
        ActivityEntry {
            proxy_wallet: "0xtarget".to_owned(),
            timestamp,
            condition_id: "condition-1".to_owned(),
            type_name: "TRADE".to_owned(),
            size: 10.0,
            usdc_size: 10.0 * price,
            transaction_hash: "0xhash".to_owned(),
            price,
            asset: asset.to_owned(),
            side: "BUY".to_owned(),
            outcome_index,
            title: "Market".to_owned(),
            slug: "market".to_owned(),
            event_slug: "event".to_owned(),
            outcome: outcome.to_owned(),
        }
    }

    #[test]
    fn activity_event_matches_signal_by_market_time_price_and_size() {
        let signal = sample_signal();
        let observed_at = Instant::now();
        let observed_at_utc = Utc::now();
        let event = ActivityTradeEvent {
            event_id: "event-1".to_owned(),
            wallet: "0xtarget".to_owned(),
            wallet_candidates: vec!["0xtarget".to_owned()],
            market_id: "condition-1".to_owned(),
            price: 0.515,
            size: 10.2,
            timestamp_ms: signal.confirmed_at.timestamp_millis() + 120,
            transaction_hash: "0xhash".to_owned(),
            asset_id: None,
            side: None,
            generation: 1,
            source: ActivitySource::ActivityStream,
            observed_at,
            observed_at_utc,
            parse_completed_at: observed_at,
            parse_completed_at_utc: observed_at_utc,
        };

        assert!(
            crate::wallet::wallet_matching::activity_match_kind(&event, &signal, &test_settings())
                .is_some()
        );
    }

    #[test]
    fn wallet_activity_fallback_matches_complementary_outcome_trade() {
        let signal = sample_signal();
        let entry = sample_activity_entry(
            "complementary-asset",
            1,
            "NO",
            0.49,
            signal.confirmed_at.timestamp(),
        );

        assert!(wallet_activity_matches_signal(
            &entry,
            &signal,
            &test_settings()
        ));
    }

    #[test]
    fn trade_correlation_accepts_direct_same_outcome_match() {
        let settings = test_settings();
        let wallet = sample_activity_entry("asset-1", 0, "YES", 0.43, 1_773_845_995);
        let market = sample_activity_entry("asset-1", 0, "YES", 0.43, 1_773_845_995);

        let correlation = trade_correlation_score(&wallet, &market, &settings).expect("match");

        assert_eq!(correlation.0, TradeCorrelationKind::Direct);
    }

    #[test]
    fn trade_correlation_accepts_complementary_opposite_outcome_match() {
        let settings = test_settings();
        let wallet = sample_activity_entry("asset-1", 0, "YES", 0.43, 1_773_845_995);
        let market = sample_activity_entry("asset-2", 1, "NO", 0.57, 1_773_845_995);

        let correlation = trade_correlation_score(&wallet, &market, &settings).expect("match");

        assert_eq!(correlation.0, TradeCorrelationKind::Complementary);
    }

    #[test]
    fn parses_nested_activity_payload_envelope() {
        let payload = serde_json::json!({
            "topic": "activity",
            "type": "trades",
            "payload": {
                "proxyWallet": "0xTarget",
                "transactionHash": "0xhash",
                "conditionId": "condition-1",
                "price": "0.43",
                "size": "21.05",
                "timestamp": "2026-03-20T10:15:30Z",
                "assetId": "asset-1",
                "side": "buy"
            }
        })
        .to_string();

        let report = parse_public_activity_trade_messages(&payload, 7, Instant::now(), Utc::now());

        assert_eq!(report.failure_detail, None);
        assert_eq!(report.events.len(), 1);
        let event = &report.events[0];
        assert_eq!(event.wallet, "0xtarget");
        assert_eq!(event.market_id, "condition-1");
        assert_eq!(event.transaction_hash, "0xhash");
        assert_eq!(event.asset_id.as_deref(), Some("asset-1"));
        assert_eq!(event.side.as_deref(), Some("BUY"));
        assert_eq!(event.generation, 7);
    }

    #[test]
    fn parses_alternative_activity_field_names() {
        let payload = serde_json::json!({
            "channel": "activity",
            "eventType": "trade",
            "data": {
                "walletAddress": "0xTarget",
                "tx_hash": "0xhash",
                "marketId": "condition-1",
                "executedPrice": 0.57,
                "shares": "12.5",
                "matchedAt": 1774000000123_i64,
                "clobTokenId": "asset-2",
                "action": "sell"
            }
        })
        .to_string();

        let report = parse_public_activity_trade_messages(&payload, 3, Instant::now(), Utc::now());

        assert_eq!(report.failure_detail, None);
        assert_eq!(report.events.len(), 1);
        let event = &report.events[0];
        assert_eq!(event.wallet, "0xtarget");
        assert_eq!(event.market_id, "condition-1");
        assert_eq!(event.price, 0.57);
        assert_eq!(event.size, 12.5);
        assert_eq!(event.asset_id.as_deref(), Some("asset-2"));
        assert_eq!(event.side.as_deref(), Some("SELL"));
    }

    #[test]
    fn parses_nested_wallet_candidates_from_multiple_fields() {
        let payload = serde_json::json!({
            "topic": "activity",
            "type": "trades",
            "payload": {
                "maker_address": "0xMaker",
                "taker_address": "0x03e8A544E97EEff5753BC1e90d46E5EF22AF1697",
                "profile": {
                    "proxyWallet": "0xProfile"
                },
                "market": "condition-1",
                "price": "0.43",
                "size": "21.05",
                "timestamp": "2026-03-20T10:15:30Z",
                "assetId": "asset-1",
                "side": "buy"
            }
        })
        .to_string();

        let report = parse_public_activity_trade_messages(&payload, 5, Instant::now(), Utc::now());

        assert_eq!(report.failure_detail, None);
        assert_eq!(report.events.len(), 1);
        let event = &report.events[0];
        assert_eq!(
            event.wallet_candidates,
            vec![
                "0xmaker".to_owned(),
                "0x03e8a544e97eeff5753bc1e90d46e5ef22af1697".to_owned(),
                "0xprofile".to_owned()
            ]
        );
    }

    #[test]
    fn parses_trade_payload_without_wallet_for_safe_correlation() {
        let payload = serde_json::json!({
            "topic": "activity",
            "type": "trades",
            "payload": {
                "transactionHash": "0xhash",
                "conditionId": "condition-1",
                "price": "0.43",
                "size": "21.05",
                "timestamp": "2026-03-20T10:15:30Z",
                "assetId": "asset-1",
                "side": "buy"
            }
        })
        .to_string();

        let report = parse_public_activity_trade_messages(&payload, 6, Instant::now(), Utc::now());

        assert_eq!(report.failure_detail, None);
        assert_eq!(report.events.len(), 1);
        let event = &report.events[0];
        assert!(event.wallet.is_empty());
        assert!(event.wallet_candidates.is_empty());
        assert_eq!(event.transaction_hash, "0xhash");
    }

    #[test]
    fn reports_unknown_activity_structure_for_debugging() {
        let payload = serde_json::json!({
            "topic": "activity",
            "type": "trades",
            "payload": {
                "proxyWallet": "0xTarget",
                "transactionHash": "0xhash",
                "price": "0.43"
            }
        })
        .to_string();

        let report = parse_public_activity_trade_messages(&payload, 1, Instant::now(), Utc::now());

        assert!(report.events.is_empty());
        assert!(
            report
                .failure_detail
                .as_deref()
                .is_some_and(|detail| detail.contains("unknown_structure"))
        );
    }

    #[test]
    fn recent_key_store_dedupes_recent_events() {
        let mut store = RecentKeyStore::default();

        assert!(store.insert("one".to_owned(), Duration::from_secs(1)));
        assert!(!store.insert("one".to_owned(), Duration::from_secs(1)));
    }

    #[test]
    fn hot_path_wallet_subscriptions_cap_stagger_delay() {
        let settings = test_settings();

        assert_eq!(
            effective_wallet_subscription_delay(&settings),
            Duration::from_millis(5)
        );
    }
}
