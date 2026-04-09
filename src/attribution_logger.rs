use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::Serialize;
use tokio::io::AsyncWriteExt;
use tokio::sync::{Mutex, mpsc};
use tracing::warn;

use crate::config::Settings;
use crate::detection::trade_inference::ConfirmedTradeSignal;
use crate::log_retention::{RetentionOutcome, enforce_jsonl_retention};
use crate::log_rotation::RotatingLogger;
use crate::models::{ActivityEntry, HealthSnapshot};
use crate::runtime::backpressure::RuntimeBackpressure;

#[derive(Clone)]
pub struct AttributionLogger {
    enabled: bool,
    path: PathBuf,
    rotating_logger: Option<Arc<Mutex<RotatingLogger>>>,
    write_lock: Arc<Mutex<()>>,
    line_tx: Option<mpsc::Sender<Vec<u8>>>,
    backpressure: RuntimeBackpressure,
}

impl AttributionLogger {
    #[allow(dead_code)]
    pub fn new(settings: Settings) -> Self {
        let backpressure = RuntimeBackpressure::new(
            settings.hot_path_queue_capacity,
            settings.cold_path_queue_capacity,
        );
        Self::with_backpressure(settings, backpressure)
    }

    pub fn with_backpressure(settings: Settings, backpressure: RuntimeBackpressure) -> Self {
        let rotating_logger = if settings.enable_log_rotation
            || settings.enable_time_rotation
            || settings.enable_log_clearing
        {
            let base_path = settings.data_dir.join("attribution-events");
            let max_lines = if settings.enable_log_rotation {
                settings.log_max_lines
            } else {
                u32::MAX
            };
            let max_duration = if settings.enable_time_rotation {
                Duration::from_secs(settings.log_rotate_hours.saturating_mul(60 * 60))
            } else {
                Duration::MAX
            };
            match RotatingLogger::new(base_path.display().to_string(), max_lines, max_duration)
                .map(|logger| logger.with_time_clearing(settings.enable_log_clearing))
            {
                Ok(logger) => Some(Arc::new(Mutex::new(logger))),
                Err(error) => {
                    warn!(
                        ?error,
                        "failed to initialize rotating attribution logger, using single-file fallback"
                    );
                    None
                }
            }
        } else {
            None
        };

        let path = settings.data_dir.join("attribution-events.jsonl");
        let write_lock = Arc::new(Mutex::new(()));
        let line_tx = if settings.log_attribution_events {
            let (line_tx, line_rx) = mpsc::channel(settings.cold_path_queue_capacity.max(1));
            spawn_writer(
                line_rx,
                path.clone(),
                rotating_logger.clone(),
                write_lock.clone(),
                backpressure.clone(),
            );
            Some(line_tx)
        } else {
            None
        };

        Self {
            enabled: settings.log_attribution_events,
            path,
            rotating_logger,
            write_lock,
            line_tx,
            backpressure,
        }
    }

    pub async fn record_runtime_started(&self, settings: &Settings, user_stream_enabled: bool) {
        self.try_write(
            &RuntimeStartedEvent {
                event_type: "runtime_started",
                logged_at: Utc::now(),
                execution_mode: settings.execution_mode.as_str(),
                activity_stream_enabled: settings.activity_stream_enabled,
                user_stream_enabled,
                target_wallet_count: settings.target_profile_addresses.len(),
                data_dir: settings.data_dir.display().to_string(),
            },
            "runtime_started",
        )
        .await;
    }

    pub async fn record_stream_event(
        &self,
        event_type: &'static str,
        stream: &'static str,
        generation: u64,
        detail: impl Into<Option<String>>,
    ) {
        self.try_write(
            &StreamEvent {
                event_type,
                logged_at: Utc::now(),
                stream,
                generation,
                detail: detail.into(),
            },
            event_type,
        )
        .await;
    }

    pub async fn record_generation_reset(
        &self,
        event_type: &'static str,
        previous_generation: u64,
        new_generation: u64,
        cleared_pending_signals: usize,
        cleared_recent_activity_markets: usize,
        cleared_recent_activity_events: usize,
        cached_markets: usize,
        cached_wallets: usize,
    ) {
        self.try_write(
            &GenerationResetEvent {
                event_type,
                logged_at: Utc::now(),
                previous_generation,
                new_generation,
                cleared_pending_signals,
                cleared_recent_activity_markets,
                cleared_recent_activity_events,
                cached_markets,
                cached_wallets,
            },
            event_type,
        )
        .await;
    }

    pub async fn record_signal_event(
        &self,
        event_type: &'static str,
        signal: &ConfirmedTradeSignal,
        pending_signals: usize,
        fallback_attempts: u32,
        detail: impl Into<Option<String>>,
    ) {
        self.try_write(
            &SignalEvent {
                event_type,
                logged_at: Utc::now(),
                generation: signal.generation,
                asset_id: signal.asset_id.clone(),
                condition_id: signal.condition_id.clone(),
                side: signal_side_label(signal),
                price: signal.price,
                estimated_size: signal.estimated_size,
                confirmed_at: signal.confirmed_at,
                pending_signals,
                fallback_attempts,
                detail: detail.into(),
            },
            event_type,
        )
        .await;
    }

    pub async fn record_activity_event(
        &self,
        event_type: &'static str,
        source: &str,
        generation: u64,
        event_id: &str,
        wallet: &str,
        market_id: &str,
        transaction_hash: &str,
        price: f64,
        size: f64,
        timestamp_ms: i64,
        pending_signals: usize,
        detail: impl Into<Option<String>>,
    ) {
        self.try_write(
            &ActivityEvent {
                event_type,
                logged_at: Utc::now(),
                source: source.to_owned(),
                generation,
                event_id: event_id.to_owned(),
                wallet: wallet.to_owned(),
                market_id: market_id.to_owned(),
                transaction_hash: transaction_hash.to_owned(),
                price,
                size,
                timestamp_ms,
                pending_signals,
                detail: detail.into(),
            },
            event_type,
        )
        .await;
    }

    pub async fn cull_old_entries(&self, retention: Duration) -> Result<RetentionOutcome> {
        if let Some(logger) = &self.rotating_logger {
            let mut logger = logger.lock().await;
            return logger.cull_old_entries(retention);
        }

        let _guard = self.write_lock.lock().await;
        enforce_jsonl_retention(&self.path, retention).await
    }

    pub async fn record_match_event(
        &self,
        source: &str,
        entry: &ActivityEntry,
        signal: &ConfirmedTradeSignal,
        fallback_attempts: u32,
        correlation_kind: Option<&'static str>,
    ) {
        let timestamp_delta_ms = signal_reference_timestamps(signal)
            .into_iter()
            .map(|reference| (normalize_timestamp_ms(entry.timestamp) - reference).abs())
            .min()
            .unwrap_or(0);
        self.try_write(
            &MatchEvent {
                event_type: "signal_matched",
                logged_at: Utc::now(),
                source: source.to_owned(),
                generation: signal.generation,
                proxy_wallet: entry.proxy_wallet.clone(),
                asset_id: signal.asset_id.clone(),
                condition_id: signal.condition_id.clone(),
                side: signal_side_label(signal),
                transaction_hash: entry.transaction_hash.clone(),
                signal_price: signal.price,
                matched_price: entry.price,
                matched_size: entry.size,
                timestamp_delta_ms,
                fallback_attempts,
                correlation: correlation_kind.map(str::to_owned),
            },
            "signal_matched",
        )
        .await;
    }

    #[allow(dead_code)]
    pub async fn record_delivery_event(
        &self,
        event_type: &'static str,
        generation: u64,
        entry: &ActivityEntry,
        detail: impl Into<Option<String>>,
    ) {
        self.try_write(
            &DeliveryEvent {
                event_type,
                logged_at: Utc::now(),
                generation,
                proxy_wallet: entry.proxy_wallet.clone(),
                asset_id: entry.asset.clone(),
                condition_id: entry.condition_id.clone(),
                side: entry.side.clone(),
                transaction_hash: entry.transaction_hash.clone(),
                price: entry.price,
                size: entry.size,
                detail: detail.into(),
            },
            event_type,
        )
        .await;
    }

    pub async fn record_heartbeat(
        &self,
        health: &HealthSnapshot,
        generation: u64,
        streams_ready: bool,
        pending_signals: usize,
        recent_activity_markets: usize,
        recent_activity_events: usize,
        cached_markets: usize,
        cached_wallets: usize,
        global_requests_in_window: usize,
        cooldown_remaining_ms: Option<u64>,
    ) {
        self.try_write(
            &HeartbeatEvent {
                event_type: "attribution_heartbeat",
                logged_at: Utc::now(),
                generation,
                ready: health.ready,
                streams_ready,
                trading_paused: health.trading_paused,
                processed_trades: health.processed_trades,
                skipped_trades: health.skipped_trades,
                poll_failures: health.poll_failures,
                last_error: health.last_error.clone(),
                pending_signals,
                recent_activity_markets,
                recent_activity_events,
                cached_markets,
                cached_wallets,
                global_requests_in_window,
                cooldown_remaining_ms,
            },
            "attribution_heartbeat",
        )
        .await;
    }

    async fn try_write<T>(&self, event: &T, label: &'static str)
    where
        T: Serialize,
    {
        if !self.enabled {
            return;
        }
        if self.backpressure.should_shed_diagnostics() {
            self.backpressure.note_diagnostic_drop();
            return;
        }
        match serde_json::to_vec(event) {
            Ok(line) => self.enqueue_line(line, label).await,
            Err(error) => warn!(?error, event = label, "failed to encode attribution event"),
        }
    }

    async fn enqueue_line(&self, line: Vec<u8>, label: &'static str) {
        let Some(line_tx) = &self.line_tx else {
            return;
        };
        self.backpressure.increment_cold_path_depth();
        match line_tx.try_send(line) {
            Ok(()) => {}
            Err(tokio::sync::mpsc::error::TrySendError::Full(_))
            | Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                self.backpressure.decrement_cold_path_depth();
                self.backpressure.note_diagnostic_drop();
                warn!(
                    event = label,
                    "dropping attribution event due to cold-path backpressure"
                );
            }
        }
    }
}

fn spawn_writer(
    mut line_rx: mpsc::Receiver<Vec<u8>>,
    path: PathBuf,
    rotating_logger: Option<Arc<Mutex<RotatingLogger>>>,
    write_lock: Arc<Mutex<()>>,
    backpressure: RuntimeBackpressure,
) {
    tokio::spawn(async move {
        while let Some(line) = line_rx.recv().await {
            if let Err(error) =
                write_line(&path, rotating_logger.as_ref(), &write_lock, &line).await
            {
                warn!(?error, "failed to persist attribution event");
            }
            backpressure.decrement_cold_path_depth();
        }
    });
}

async fn write_line(
    path: &PathBuf,
    rotating_logger: Option<&Arc<Mutex<RotatingLogger>>>,
    write_lock: &Arc<Mutex<()>>,
    line: &[u8],
) -> Result<()> {
    if let Some(logger) = rotating_logger {
        let mut logger = logger.lock().await;
        logger.write_line(&String::from_utf8_lossy(line))?;
        return Ok(());
    }

    let _guard = write_lock.lock().await;
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("creating {}", parent.display()))?;
    }
    let mut file = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await
        .with_context(|| format!("opening {}", path.display()))?;
    file.write_all(line)
        .await
        .with_context(|| format!("writing {}", path.display()))?;
    file.write_all(b"\n")
        .await
        .with_context(|| format!("writing newline to {}", path.display()))?;
    Ok(())
}

#[derive(Debug, Serialize)]
struct RuntimeStartedEvent {
    event_type: &'static str,
    logged_at: DateTime<Utc>,
    execution_mode: &'static str,
    activity_stream_enabled: bool,
    user_stream_enabled: bool,
    target_wallet_count: usize,
    data_dir: String,
}

#[derive(Debug, Serialize)]
struct StreamEvent {
    event_type: &'static str,
    logged_at: DateTime<Utc>,
    stream: &'static str,
    generation: u64,
    detail: Option<String>,
}

#[derive(Debug, Serialize)]
struct GenerationResetEvent {
    event_type: &'static str,
    logged_at: DateTime<Utc>,
    previous_generation: u64,
    new_generation: u64,
    cleared_pending_signals: usize,
    cleared_recent_activity_markets: usize,
    cleared_recent_activity_events: usize,
    cached_markets: usize,
    cached_wallets: usize,
}

#[derive(Debug, Serialize)]
struct SignalEvent {
    event_type: &'static str,
    logged_at: DateTime<Utc>,
    generation: u64,
    asset_id: String,
    condition_id: String,
    side: &'static str,
    price: f64,
    estimated_size: f64,
    confirmed_at: DateTime<Utc>,
    pending_signals: usize,
    fallback_attempts: u32,
    detail: Option<String>,
}

#[derive(Debug, Serialize)]
struct ActivityEvent {
    event_type: &'static str,
    logged_at: DateTime<Utc>,
    source: String,
    generation: u64,
    event_id: String,
    wallet: String,
    market_id: String,
    transaction_hash: String,
    price: f64,
    size: f64,
    timestamp_ms: i64,
    pending_signals: usize,
    detail: Option<String>,
}

#[derive(Debug, Serialize)]
struct MatchEvent {
    event_type: &'static str,
    logged_at: DateTime<Utc>,
    source: String,
    generation: u64,
    proxy_wallet: String,
    asset_id: String,
    condition_id: String,
    side: &'static str,
    transaction_hash: String,
    signal_price: f64,
    matched_price: f64,
    matched_size: f64,
    timestamp_delta_ms: i64,
    fallback_attempts: u32,
    correlation: Option<String>,
}

#[derive(Debug, Serialize)]
#[allow(dead_code)]
struct DeliveryEvent {
    event_type: &'static str,
    logged_at: DateTime<Utc>,
    generation: u64,
    proxy_wallet: String,
    asset_id: String,
    condition_id: String,
    side: String,
    transaction_hash: String,
    price: f64,
    size: f64,
    detail: Option<String>,
}

#[derive(Debug, Serialize)]
struct HeartbeatEvent {
    event_type: &'static str,
    logged_at: DateTime<Utc>,
    generation: u64,
    ready: bool,
    streams_ready: bool,
    trading_paused: bool,
    processed_trades: u64,
    skipped_trades: u64,
    poll_failures: u64,
    last_error: Option<String>,
    pending_signals: usize,
    recent_activity_markets: usize,
    recent_activity_events: usize,
    cached_markets: usize,
    cached_wallets: usize,
    global_requests_in_window: usize,
    cooldown_remaining_ms: Option<u64>,
}

fn signal_side_label(signal: &ConfirmedTradeSignal) -> &'static str {
    match signal.side {
        crate::execution::ExecutionSide::Buy => "BUY",
        crate::execution::ExecutionSide::Sell => "SELL",
    }
}

fn signal_reference_timestamps(signal: &ConfirmedTradeSignal) -> [i64; 2] {
    [
        signal
            .stage_timestamps
            .detection_triggered_at_utc
            .timestamp_millis(),
        signal.confirmed_at.timestamp_millis(),
    ]
}

fn normalize_timestamp_ms(timestamp: i64) -> i64 {
    if timestamp >= 1_000_000_000_000 {
        timestamp
    } else {
        timestamp * 1000
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ExecutionMode;
    use crate::execution::ExecutionSide;
    use crate::models::TradeStageTimestamps;
    use std::time::Instant;

    fn test_settings(data_dir: &std::path::Path) -> Settings {
        use rust_decimal_macros::dec;

        Settings {
            execution_mode: ExecutionMode::Paper,
            polymarket_host: "https://clob.polymarket.com".to_owned(),
            polymarket_data_api: "https://data-api.polymarket.com".to_owned(),
            polymarket_gamma_api: "https://gamma-api.polymarket.com".to_owned(),
            polymarket_market_ws: "wss://ws-subscriptions-clob.polymarket.com/ws/market".to_owned(),
            polymarket_user_ws: "wss://ws-subscriptions-clob.polymarket.com/ws/user".to_owned(),
            polymarket_activity_ws: "wss://ws-live-data.polymarket.com".to_owned(),
            polymarket_chain_id: 137,
            polymarket_signature_type: 2,
            polymarket_private_key: None,
            polymarket_funder_address: None,
            polymarket_profile_address: None,
            polygon_rpc_url: "https://polygon-rpc.com".to_owned(),
            polygon_rpc_fallback_urls: Vec::new(),
            rpc_latency_threshold: std::time::Duration::from_millis(300),
            rpc_confirmation_timeout: std::time::Duration::from_secs(10),
            min_required_matic: dec!(0.1),
            min_required_usdc: dec!(25),
            polymarket_usdc_address: "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174".to_owned(),
            polymarket_spender_address: "0x0000000000000000000000000000000000000001".to_owned(),
            auto_approve_usdc_allowance: false,
            usdc_approval_amount: dec!(1000),
            target_activity_ws_api_key: None,
            target_activity_ws_secret: None,
            target_activity_ws_passphrase: None,
            target_profile_addresses: vec!["0xtarget".to_owned()],
            start_capital_usd: dec!(200),
            paper_execution_delay: std::time::Duration::ZERO,
            copy_only_new_trades: true,
            source_trades_limit: 50,
            http_timeout: std::time::Duration::from_secs(2),
            market_cache_ttl: std::time::Duration::from_secs(3),
            market_raw_ring_capacity: 8192,
            market_parser_workers: 1,
            market_subscription_batch_size: 250,
            market_subscription_delay: std::time::Duration::from_millis(20),
            wallet_ring_capacity: 1024,
            wallet_parser_workers: 1,
            wallet_subscription_batch_size: 250,
            wallet_subscription_delay: std::time::Duration::from_millis(20),
            hot_path_mode: true,
            hot_path_queue_capacity: 128,
            cold_path_queue_capacity: 512,
            attribution_fast_cache_capacity: 256,
            persistence_flush_interval: std::time::Duration::from_millis(250),
            analytics_flush_interval: std::time::Duration::from_millis(500),
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
            inference_confirmation_window: std::time::Duration::from_millis(400),
            activity_stream_enabled: true,
            activity_match_window: std::time::Duration::from_millis(200),
            activity_price_tolerance: dec!(0.01),
            activity_size_tolerance_ratio: dec!(0.5),
            activity_cache_ttl: std::time::Duration::from_millis(1500),
            fallback_market_request_interval: std::time::Duration::from_millis(1000),
            fallback_global_requests_per_minute: 30,
            activity_correlation_window: std::time::Duration::from_millis(400),
            attribution_lookback: std::time::Duration::from_millis(2500),
            attribution_trades_limit: 100,
            copy_scale_above_five_usd: dec!(0.25),
            min_copy_notional_usd: dec!(1),
            max_copy_notional_usd: dec!(25),
            max_total_exposure_usd: dec!(150),
            max_market_exposure_usd: dec!(40),
            min_source_trade_usdc: dec!(3),
            max_market_spread_bps: 500,
            enable_ultra_short_markets: false,
            min_visible_liquidity: dec!(50),
            max_spread_bps: 500,
            max_entry_slippage: dec!(0.03),
            min_top_of_book_ratio: dec!(1.25),
            max_slippage_bps: 300,
            max_source_price_slippage_bps: 200,
            latency_fail_safe_enabled: true,
            max_latency: std::time::Duration::from_millis(500),
            average_latency_threshold: std::time::Duration::from_millis(350),
            latency_monitor_interval: std::time::Duration::from_millis(50),
            latency_reconnect_settle: std::time::Duration::from_millis(750),
            prediction_validation_timeout: std::time::Duration::from_millis(1500),
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
            data_dir: data_dir.to_path_buf(),
        }
    }

    fn sample_signal() -> ConfirmedTradeSignal {
        let now = Utc::now();
        let instant = Instant::now();
        ConfirmedTradeSignal {
            asset_id: "asset-1".to_owned(),
            condition_id: "condition-1".to_owned(),
            transaction_hash: None,
            side: ExecutionSide::Buy,
            price: 0.43,
            estimated_size: 21.05,
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
            generation: 7,
        }
    }

    fn sample_entry() -> ActivityEntry {
        ActivityEntry {
            proxy_wallet: "0xtarget".to_owned(),
            timestamp: 1_773_845_995,
            condition_id: "condition-1".to_owned(),
            type_name: "TRADE".to_owned(),
            size: 21.05,
            usdc_size: 9.05,
            transaction_hash: "0xhash".to_owned(),
            price: 0.43,
            asset: "asset-1".to_owned(),
            side: "BUY".to_owned(),
            outcome_index: 0,
            title: "Market".to_owned(),
            slug: "market".to_owned(),
            event_slug: "event".to_owned(),
            outcome: "YES".to_owned(),
        }
    }

    #[tokio::test]
    async fn writes_runtime_and_match_events_when_enabled() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let settings = test_settings(temp_dir.path());
        let logger = AttributionLogger::new(settings.clone());

        logger.record_runtime_started(&settings, false).await;
        logger
            .record_match_event(
                "activity_ws",
                &sample_entry(),
                &sample_signal(),
                1,
                Some("direct"),
            )
            .await;
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        let contents =
            tokio::fs::read_to_string(temp_dir.path().join("attribution-events_0.jsonl"))
                .await
                .expect("read attribution log");
        assert!(contents.contains("\"event_type\":\"runtime_started\""));
        assert!(contents.contains("\"event_type\":\"signal_matched\""));
        assert!(contents.contains("\"correlation\":\"direct\""));
    }

    #[tokio::test]
    async fn writes_heartbeat_event_when_enabled() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let logger = AttributionLogger::new(test_settings(temp_dir.path()));

        logger
            .record_heartbeat(
                &HealthSnapshot {
                    ready: true,
                    execution_mode: "paper".to_owned(),
                    processed_trades: 3,
                    skipped_trades: 1,
                    poll_failures: 0,
                    last_detection_ms: 42,
                    last_execution_ms: 99,
                    last_total_latency_ms: 141,
                    average_latency_ms: 140,
                    hot_entry_queue_depth: 0,
                    hot_exit_queue_depth: 0,
                    cold_path_queue_depth: 0,
                    dropped_diagnostics: 0,
                    degradation_mode: false,
                    last_skip_processing_ms: 17,
                    last_skip_reason_code: None,
                    trading_paused: false,
                    last_latency_pause_reason: None,
                    latency_fail_safe_trips: 0,
                    last_error: Some("none".to_owned()),
                    last_portfolio_value: None,
                    last_update_iso: Some(Utc::now()),
                },
                4,
                true,
                2,
                1,
                3,
                1,
                0,
                5,
                Some(1200),
            )
            .await;
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        let contents =
            tokio::fs::read_to_string(temp_dir.path().join("attribution-events_0.jsonl"))
                .await
                .expect("read attribution log");
        assert!(contents.contains("\"event_type\":\"attribution_heartbeat\""));
        assert!(contents.contains("\"pending_signals\":2"));
        assert!(contents.contains("\"streams_ready\":true"));
    }
}
