use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, TimeZone, Utc};
use serde::Serialize;
use tokio::io::AsyncWriteExt;
use tokio::sync::{Mutex, mpsc};

use crate::config::Settings;
use crate::execution::{ExecutionRequest, ExecutionSuccess};
use crate::latency_monitor::LatencyMeasurement;
use crate::log_retention::{RetentionOutcome, enforce_jsonl_retention};
use crate::models::{ActivityEntry, TradeStageTimestamps};
use crate::risk::SkipReason;
use crate::runtime::backpressure::RuntimeBackpressure;

const TIMESTAMP_MILLIS_THRESHOLD: i64 = 10_000_000_000;

#[derive(Clone)]
pub struct LatencyLogger {
    enabled: bool,
    execution_mode: String,
    path: PathBuf,
    write_lock: Arc<Mutex<()>>,
    line_tx: Option<mpsc::Sender<Vec<u8>>>,
    backpressure: RuntimeBackpressure,
}

impl LatencyLogger {
    #[allow(dead_code)]
    pub fn new(settings: Settings) -> Self {
        let backpressure = RuntimeBackpressure::new(
            settings.hot_path_queue_capacity,
            settings.cold_path_queue_capacity,
        );
        Self::with_backpressure(settings, backpressure)
    }

    pub fn with_backpressure(settings: Settings, backpressure: RuntimeBackpressure) -> Self {
        let path = settings.data_dir.join("latency-events.jsonl");
        let write_lock = Arc::new(Mutex::new(()));
        let line_tx = if settings.log_latency_events {
            let (line_tx, line_rx) = mpsc::channel(settings.cold_path_queue_capacity.max(1));
            spawn_writer(
                line_rx,
                path.clone(),
                write_lock.clone(),
                backpressure.clone(),
            );
            Some(line_tx)
        } else {
            None
        };
        Self {
            enabled: settings.log_latency_events,
            execution_mode: settings.execution_mode.as_str().to_owned(),
            path,
            write_lock,
            line_tx,
            backpressure,
        }
    }

    pub async fn record_processed_trade(
        &self,
        source: &ActivityEntry,
        request: &ExecutionRequest,
        result: &ExecutionSuccess,
        stage_timestamps: &TradeStageTimestamps,
        submission_completed_at: DateTime<Utc>,
        measurement: LatencyMeasurement,
        submit_order_ms: u64,
    ) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }

        let source_timestamp = source_timestamp_metadata(source.timestamp)?;
        let order_submit_started_at =
            submission_completed_at - chrono::TimeDelta::milliseconds(submit_order_ms as i64);
        let event = ProcessedTradeLatencyEvent {
            event_type: "processed_trade",
            execution_mode: result.mode.as_str().to_owned(),
            source_trade_at: source_timestamp.at,
            source_trade_timestamp_unix: source_timestamp.value,
            source_timestamp_resolution: source_timestamp.resolution,
            websocket_event_received_at: stage_timestamps.websocket_event_received_at_utc,
            parse_completed_at: stage_timestamps.parse_completed_at_utc,
            trade_detected_at: stage_timestamps.detection_triggered_at_utc,
            attribution_completed_at: stage_timestamps.attribution_completed_at_utc,
            fast_risk_completed_at: stage_timestamps.fast_risk_completed_at_utc,
            order_submit_started_at,
            submission_completed_at,
            websocket_to_parse_ms: stage_timestamps.parse_latency_ms(),
            parse_to_attribution_ms: stage_timestamps.attribution_latency_ms(),
            attribution_to_fast_risk_ms: stage_timestamps.fast_risk_latency_ms(),
            source_to_detection_ms: stage_timestamps
                .detection_triggered_at_utc
                .signed_duration_since(source_timestamp.at)
                .num_milliseconds(),
            websocket_to_detection_ms: measurement.detection_latency_ms,
            detection_to_submission_ms: measurement.execution_latency_ms,
            submit_order_ms,
            websocket_to_submission_ms: measurement.total_latency_ms,
            source_to_submission_ms: submission_completed_at
                .signed_duration_since(source_timestamp.at)
                .num_milliseconds(),
            market_to_execution_ms: submission_completed_at
                .signed_duration_since(source_timestamp.at)
                .num_milliseconds(),
            source_tx_hash: source.transaction_hash.clone(),
            asset: source.asset.clone(),
            condition_id: source.condition_id.clone(),
            side: source.side.clone(),
            source_price: source.price,
            source_usdc: source.usdc_size,
            source_size: source.size,
            order_id: result.order_id.clone(),
            request_limit_price: request.limit_price.to_string(),
            request_size: request.size.to_string(),
            requested_notional: request.requested_notional.to_string(),
            filled_price: result.filled_price.to_string(),
            filled_size: result.filled_size.to_string(),
            filled_notional: result.filled_notional.to_string(),
            success: result.success,
            status: result.status.to_string(),
        };

        self.write_event(&event).await
    }

    pub async fn record_skipped_trade(
        &self,
        source: &ActivityEntry,
        stage_timestamps: &TradeStageTimestamps,
        skipped_at: DateTime<Utc>,
        skip_processing_ms: u64,
        reason: &SkipReason,
    ) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }

        let source_timestamp = source_timestamp_metadata(source.timestamp)?;
        let event = SkippedTradeLatencyEvent {
            event_type: "skipped_trade",
            execution_mode: self.execution_mode.clone(),
            source_trade_at: source_timestamp.at,
            source_trade_timestamp_unix: source_timestamp.value,
            source_timestamp_resolution: source_timestamp.resolution,
            websocket_event_received_at: stage_timestamps.websocket_event_received_at_utc,
            parse_completed_at: stage_timestamps.parse_completed_at_utc,
            trade_detected_at: stage_timestamps.detection_triggered_at_utc,
            attribution_completed_at: stage_timestamps.attribution_completed_at_utc,
            fast_risk_completed_at: stage_timestamps.fast_risk_completed_at_utc,
            skipped_at,
            websocket_to_parse_ms: stage_timestamps.parse_latency_ms(),
            parse_to_attribution_ms: stage_timestamps.attribution_latency_ms(),
            attribution_to_fast_risk_ms: stage_timestamps.fast_risk_latency_ms(),
            source_to_detection_ms: stage_timestamps
                .detection_triggered_at_utc
                .signed_duration_since(source_timestamp.at)
                .num_milliseconds(),
            websocket_to_detection_ms: stage_timestamps.detection_latency_ms(),
            detection_to_skip_ms: skip_processing_ms,
            websocket_to_skip_ms: skipped_at
                .signed_duration_since(stage_timestamps.websocket_event_received_at_utc)
                .num_milliseconds(),
            source_to_skip_ms: skipped_at
                .signed_duration_since(source_timestamp.at)
                .num_milliseconds(),
            market_to_skip_ms: skipped_at
                .signed_duration_since(source_timestamp.at)
                .num_milliseconds(),
            source_tx_hash: source.transaction_hash.clone(),
            asset: source.asset.clone(),
            condition_id: source.condition_id.clone(),
            side: source.side.clone(),
            source_price: source.price,
            source_usdc: source.usdc_size,
            source_size: source.size,
            skip_reason_code: reason.code.to_owned(),
            skip_reason_detail: reason.detail.clone(),
        };

        self.write_event(&event).await
    }

    pub async fn record_prediction_validation(
        &self,
        signal: &crate::detection::trade_inference::ConfirmedTradeSignal,
        predicted_entry: &ActivityEntry,
        confirmed_entry: &ActivityEntry,
        predicted_wallet: &str,
        confidence: f64,
        submission_completed_at: DateTime<Utc>,
        confirmation_received_at: DateTime<Utc>,
    ) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }

        let market_event_at = signal.stage_timestamps.websocket_event_received_at_utc;
        let event = PredictionValidationLatencyEvent {
            event_type: "prediction_validation",
            execution_mode: self.execution_mode.clone(),
            market_event_at,
            parse_completed_at: signal.stage_timestamps.parse_completed_at_utc,
            trade_detected_at: signal.stage_timestamps.detection_triggered_at_utc,
            attribution_completed_at: signal.stage_timestamps.attribution_completed_at_utc,
            fast_risk_completed_at: signal.stage_timestamps.fast_risk_completed_at_utc,
            submission_completed_at,
            confirmation_received_at,
            market_to_parse_ms: signal.stage_timestamps.parse_latency_ms() as i64,
            parse_to_attribution_ms: signal
                .stage_timestamps
                .attribution_latency_ms()
                .map(|value| value as i64),
            attribution_to_fast_risk_ms: signal
                .stage_timestamps
                .fast_risk_latency_ms()
                .map(|value| value as i64),
            market_to_detection_ms: signal
                .stage_timestamps
                .detection_triggered_at_utc
                .signed_duration_since(market_event_at)
                .num_milliseconds(),
            market_to_execution_ms: submission_completed_at
                .signed_duration_since(market_event_at)
                .num_milliseconds(),
            execution_to_confirmation_ms: confirmation_received_at
                .signed_duration_since(submission_completed_at)
                .num_milliseconds(),
            total_latency_ms: confirmation_received_at
                .signed_duration_since(market_event_at)
                .num_milliseconds(),
            predicted_wallet: predicted_wallet.to_owned(),
            confirmed_wallet: confirmed_entry.proxy_wallet.clone(),
            confidence,
            predicted_tx_hash: predicted_entry.transaction_hash.clone(),
            confirmed_tx_hash: confirmed_entry.transaction_hash.clone(),
            asset: signal.asset_id.clone(),
            condition_id: signal.condition_id.clone(),
            side: match signal.side {
                crate::execution::ExecutionSide::Buy => "BUY",
                crate::execution::ExecutionSide::Sell => "SELL",
            }
            .to_owned(),
        };

        self.write_event(&event).await
    }

    pub async fn cull_old_entries(&self, retention: Duration) -> Result<RetentionOutcome> {
        let _guard = self.write_lock.lock().await;
        enforce_jsonl_retention(&self.path, retention).await
    }

    async fn write_event<T>(&self, event: &T) -> Result<()>
    where
        T: Serialize,
    {
        if !self.enabled {
            return Ok(());
        }
        if self.backpressure.should_shed_diagnostics() {
            self.backpressure.note_diagnostic_drop();
            return Ok(());
        }

        let line = serde_json::to_vec(event)?;
        let Some(line_tx) = &self.line_tx else {
            return write_line(&self.path, &self.write_lock, &line).await;
        };
        self.backpressure.increment_cold_path_depth();
        match line_tx.try_send(line) {
            Ok(()) => Ok(()),
            Err(tokio::sync::mpsc::error::TrySendError::Full(_))
            | Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                self.backpressure.decrement_cold_path_depth();
                self.backpressure.note_diagnostic_drop();
                Ok(())
            }
        }
    }
}

fn spawn_writer(
    mut line_rx: mpsc::Receiver<Vec<u8>>,
    path: PathBuf,
    write_lock: Arc<Mutex<()>>,
    backpressure: RuntimeBackpressure,
) {
    tokio::spawn(async move {
        while let Some(line) = line_rx.recv().await {
            if let Err(error) = write_line(&path, &write_lock, &line).await {
                tracing::warn!(?error, "failed to persist latency event");
            }
            backpressure.decrement_cold_path_depth();
        }
    });
}

async fn write_line(path: &PathBuf, write_lock: &Arc<Mutex<()>>, line: &[u8]) -> Result<()> {
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
struct ProcessedTradeLatencyEvent {
    event_type: &'static str,
    execution_mode: String,
    source_trade_at: DateTime<Utc>,
    source_trade_timestamp_unix: i64,
    source_timestamp_resolution: &'static str,
    websocket_event_received_at: DateTime<Utc>,
    parse_completed_at: DateTime<Utc>,
    trade_detected_at: DateTime<Utc>,
    attribution_completed_at: Option<DateTime<Utc>>,
    fast_risk_completed_at: Option<DateTime<Utc>>,
    order_submit_started_at: DateTime<Utc>,
    submission_completed_at: DateTime<Utc>,
    websocket_to_parse_ms: u64,
    parse_to_attribution_ms: Option<u64>,
    attribution_to_fast_risk_ms: Option<u64>,
    source_to_detection_ms: i64,
    websocket_to_detection_ms: u64,
    detection_to_submission_ms: u64,
    submit_order_ms: u64,
    websocket_to_submission_ms: u64,
    source_to_submission_ms: i64,
    market_to_execution_ms: i64,
    source_tx_hash: String,
    asset: String,
    condition_id: String,
    side: String,
    source_price: f64,
    source_usdc: f64,
    source_size: f64,
    order_id: String,
    request_limit_price: String,
    request_size: String,
    requested_notional: String,
    filled_price: String,
    filled_size: String,
    filled_notional: String,
    success: bool,
    status: String,
}

#[derive(Debug, Serialize)]
struct SkippedTradeLatencyEvent {
    event_type: &'static str,
    execution_mode: String,
    source_trade_at: DateTime<Utc>,
    source_trade_timestamp_unix: i64,
    source_timestamp_resolution: &'static str,
    websocket_event_received_at: DateTime<Utc>,
    parse_completed_at: DateTime<Utc>,
    trade_detected_at: DateTime<Utc>,
    attribution_completed_at: Option<DateTime<Utc>>,
    fast_risk_completed_at: Option<DateTime<Utc>>,
    skipped_at: DateTime<Utc>,
    websocket_to_parse_ms: u64,
    parse_to_attribution_ms: Option<u64>,
    attribution_to_fast_risk_ms: Option<u64>,
    source_to_detection_ms: i64,
    websocket_to_detection_ms: u64,
    detection_to_skip_ms: u64,
    websocket_to_skip_ms: i64,
    source_to_skip_ms: i64,
    market_to_skip_ms: i64,
    source_tx_hash: String,
    asset: String,
    condition_id: String,
    side: String,
    source_price: f64,
    source_usdc: f64,
    source_size: f64,
    skip_reason_code: String,
    skip_reason_detail: String,
}

#[derive(Debug)]
struct SourceTimestampMetadata {
    at: DateTime<Utc>,
    value: i64,
    resolution: &'static str,
}

#[derive(Debug, Serialize)]
struct PredictionValidationLatencyEvent {
    event_type: &'static str,
    execution_mode: String,
    market_event_at: DateTime<Utc>,
    parse_completed_at: DateTime<Utc>,
    trade_detected_at: DateTime<Utc>,
    attribution_completed_at: Option<DateTime<Utc>>,
    fast_risk_completed_at: Option<DateTime<Utc>>,
    submission_completed_at: DateTime<Utc>,
    confirmation_received_at: DateTime<Utc>,
    market_to_parse_ms: i64,
    parse_to_attribution_ms: Option<i64>,
    attribution_to_fast_risk_ms: Option<i64>,
    market_to_detection_ms: i64,
    market_to_execution_ms: i64,
    execution_to_confirmation_ms: i64,
    total_latency_ms: i64,
    predicted_wallet: String,
    confirmed_wallet: String,
    confidence: f64,
    predicted_tx_hash: String,
    confirmed_tx_hash: String,
    asset: String,
    condition_id: String,
    side: String,
}

fn source_timestamp_metadata(timestamp: i64) -> Result<SourceTimestampMetadata> {
    if timestamp >= TIMESTAMP_MILLIS_THRESHOLD {
        let at = Utc
            .timestamp_millis_opt(timestamp)
            .single()
            .ok_or_else(|| anyhow!("invalid source trade timestamp {timestamp}"))?;
        Ok(SourceTimestampMetadata {
            at,
            value: timestamp,
            resolution: "milliseconds",
        })
    } else {
        let at = Utc
            .timestamp_opt(timestamp, 0)
            .single()
            .ok_or_else(|| anyhow!("invalid source trade timestamp {timestamp}"))?;
        Ok(SourceTimestampMetadata {
            at,
            value: timestamp,
            resolution: "seconds",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ExecutionMode;
    use crate::execution::{ExecutionRequest, ExecutionSide};
    use crate::risk::SkipReason;
    use rust_decimal_macros::dec;
    use std::time::Instant;

    fn sample_source() -> ActivityEntry {
        ActivityEntry {
            proxy_wallet: "0xsource".to_owned(),
            timestamp: 1_773_673_025,
            condition_id: "condition-1".to_owned(),
            type_name: "TRADE".to_owned(),
            size: 10.0,
            usdc_size: 5.0,
            transaction_hash: "0xhash".to_owned(),
            price: 0.5,
            asset: "asset-1".to_owned(),
            side: "BUY".to_owned(),
            outcome_index: 0,
            title: "Sample market".to_owned(),
            slug: "sample-market".to_owned(),
            event_slug: "sample-event".to_owned(),
            outcome: "YES".to_owned(),
        }
    }

    fn sample_request() -> ExecutionRequest {
        ExecutionRequest {
            token_id: "asset-1".to_owned(),
            side: ExecutionSide::Buy,
            size: dec!(10),
            limit_price: dec!(0.5),
            requested_notional: dec!(5),
            source_trade_id: "trade-1".to_owned(),
        }
    }

    fn sample_result() -> ExecutionSuccess {
        ExecutionSuccess {
            mode: ExecutionMode::Paper,
            order_request: sample_request(),
            order_id: "paper-order-1".to_owned(),
            success: true,
            transaction_hashes: Vec::new(),
            filled_price: dec!(0.5),
            filled_size: dec!(10),
            requested_size: dec!(10),
            requested_price: dec!(0.5),
            status: crate::execution::ExecutionStatus::Filled,
            filled_notional: dec!(5),
        }
    }

    fn sample_stage_timestamps() -> TradeStageTimestamps {
        let websocket_event_received_at = Instant::now();
        let parse_completed_at = websocket_event_received_at + Duration::from_millis(25);
        let detection_triggered_at = websocket_event_received_at + Duration::from_millis(50);
        let attribution_completed_at = parse_completed_at;
        let fast_risk_completed_at = websocket_event_received_at + Duration::from_millis(100);
        let parse_completed_at_utc = Utc
            .timestamp_opt(1_773_673_035, 975_000_000)
            .single()
            .expect("parse time");
        let fast_risk_completed_at_utc = Utc
            .timestamp_opt(1_773_673_036, 50_000_000)
            .single()
            .expect("fast risk time");
        TradeStageTimestamps {
            websocket_event_received_at,
            websocket_event_received_at_utc: Utc
                .timestamp_opt(1_773_673_035, 950_000_000)
                .single()
                .expect("ws time"),
            parse_completed_at,
            parse_completed_at_utc,
            detection_triggered_at,
            detection_triggered_at_utc: Utc
                .timestamp_opt(1_773_673_036, 0)
                .single()
                .expect("detection time"),
            attribution_completed_at: Some(attribution_completed_at),
            attribution_completed_at_utc: Some(parse_completed_at_utc),
            fast_risk_completed_at: Some(fast_risk_completed_at),
            fast_risk_completed_at_utc: Some(fast_risk_completed_at_utc),
        }
    }

    #[tokio::test]
    async fn writes_latency_event_when_enabled() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let logger = LatencyLogger {
            enabled: true,
            execution_mode: ExecutionMode::Paper.as_str().to_owned(),
            path: temp_dir.path().join("latency-events.jsonl"),
            write_lock: Arc::new(Mutex::new(())),
            line_tx: None,
            backpressure: RuntimeBackpressure::default(),
        };

        logger
            .record_processed_trade(
                &sample_source(),
                &sample_request(),
                &sample_result(),
                &sample_stage_timestamps(),
                Utc.timestamp_opt(1_773_673_036, 200_000_000)
                    .single()
                    .expect("completed time"),
                LatencyMeasurement {
                    detection_latency_ms: 50,
                    execution_latency_ms: 181,
                    total_latency_ms: 231,
                },
                0,
            )
            .await
            .expect("write latency event");

        let contents = tokio::fs::read_to_string(temp_dir.path().join("latency-events.jsonl"))
            .await
            .expect("read latency log");
        assert!(contents.contains("\"source_to_submission_ms\":11200"));
        assert!(contents.contains("\"websocket_to_parse_ms\":25"));
        assert!(contents.contains("\"parse_to_attribution_ms\":0"));
        assert!(contents.contains("\"order_submit_started_at\""));
        assert!(contents.contains("\"websocket_to_detection_ms\":50"));
        assert!(contents.contains("\"detection_to_submission_ms\":181"));
    }

    #[tokio::test]
    async fn writes_skipped_trade_event_when_enabled() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let logger = LatencyLogger {
            enabled: true,
            execution_mode: ExecutionMode::Paper.as_str().to_owned(),
            path: temp_dir.path().join("latency-events.jsonl"),
            write_lock: Arc::new(Mutex::new(())),
            line_tx: None,
            backpressure: RuntimeBackpressure::default(),
        };

        logger
            .record_skipped_trade(
                &sample_source(),
                &sample_stage_timestamps(),
                Utc.timestamp_opt(1_773_673_036, 120_000_000)
                    .single()
                    .expect("skipped time"),
                120,
                &SkipReason::new("market_spread_exceeded", "spread too wide"),
            )
            .await
            .expect("write skipped latency event");

        let contents = tokio::fs::read_to_string(temp_dir.path().join("latency-events.jsonl"))
            .await
            .expect("read latency log");
        assert!(contents.contains("\"event_type\":\"skipped_trade\""));
        assert!(contents.contains("\"parse_completed_at\""));
        assert!(contents.contains("\"detection_to_skip_ms\":120"));
        assert!(contents.contains("\"skip_reason_code\":\"market_spread_exceeded\""));
    }

    #[tokio::test]
    async fn preserves_millisecond_source_timestamps() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let logger = LatencyLogger {
            enabled: true,
            execution_mode: ExecutionMode::Paper.as_str().to_owned(),
            path: temp_dir.path().join("latency-events.jsonl"),
            write_lock: Arc::new(Mutex::new(())),
            line_tx: None,
            backpressure: RuntimeBackpressure::default(),
        };
        let mut source = sample_source();
        source.timestamp = 1_773_673_025_950;

        logger
            .record_skipped_trade(
                &source,
                &sample_stage_timestamps(),
                Utc.timestamp_opt(1_773_673_036, 120_000_000)
                    .single()
                    .expect("skipped time"),
                120,
                &SkipReason::new("market_spread_exceeded", "spread too wide"),
            )
            .await
            .expect("write skipped latency event");

        let contents = tokio::fs::read_to_string(temp_dir.path().join("latency-events.jsonl"))
            .await
            .expect("read latency log");
        assert!(contents.contains("\"source_timestamp_resolution\":\"milliseconds\""));
        assert!(contents.contains("\"source_trade_timestamp_unix\":1773673025950"));
    }

    #[tokio::test]
    async fn writes_prediction_validation_event() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let logger = LatencyLogger {
            enabled: true,
            execution_mode: ExecutionMode::Paper.as_str().to_owned(),
            path: temp_dir.path().join("latency-events.jsonl"),
            write_lock: Arc::new(Mutex::new(())),
            line_tx: None,
            backpressure: RuntimeBackpressure::default(),
        };
        let stage_timestamps = sample_stage_timestamps();
        let signal = crate::detection::trade_inference::ConfirmedTradeSignal {
            asset_id: "asset-1".to_owned(),
            condition_id: "condition-1".to_owned(),
            transaction_hash: None,
            side: ExecutionSide::Buy,
            price: 0.5,
            estimated_size: 10.0,
            stage_timestamps,
            confirmed_at: Utc
                .timestamp_opt(1_773_673_036, 0)
                .single()
                .expect("confirmed"),
            generation: 1,
        };

        logger
            .record_prediction_validation(
                &signal,
                &sample_source(),
                &sample_source(),
                "0xpredicted",
                0.91,
                Utc.timestamp_opt(1_773_673_036, 200_000_000)
                    .single()
                    .expect("submitted"),
                Utc.timestamp_opt(1_773_673_036, 320_000_000)
                    .single()
                    .expect("confirmed"),
            )
            .await
            .expect("write prediction validation event");

        let contents = tokio::fs::read_to_string(temp_dir.path().join("latency-events.jsonl"))
            .await
            .expect("read latency log");
        assert!(contents.contains("\"market_to_parse_ms\":25"));
        assert!(contents.contains("\"event_type\":\"prediction_validation\""));
        assert!(contents.contains("\"market_to_execution_ms\":250"));
        assert!(contents.contains("\"execution_to_confirmation_ms\":120"));
    }

    #[tokio::test]
    async fn cold_path_overflow_drops_diagnostics_without_blocking() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let (tx, _held_rx) = mpsc::channel(1);
        let backpressure = RuntimeBackpressure::new(8, 1);
        let logger = LatencyLogger {
            enabled: true,
            execution_mode: ExecutionMode::Paper.as_str().to_owned(),
            path: temp_dir.path().join("latency-events.jsonl"),
            write_lock: Arc::new(Mutex::new(())),
            line_tx: Some(tx),
            backpressure: backpressure.clone(),
        };

        logger
            .record_skipped_trade(
                &sample_source(),
                &sample_stage_timestamps(),
                Utc.timestamp_opt(1_773_673_036, 120_000_000)
                    .single()
                    .expect("skipped time"),
                120,
                &SkipReason::new("market_spread_exceeded", "spread too wide"),
            )
            .await
            .expect("first queueing succeeds");
        logger
            .record_skipped_trade(
                &sample_source(),
                &sample_stage_timestamps(),
                Utc.timestamp_opt(1_773_673_036, 120_000_000)
                    .single()
                    .expect("skipped time"),
                120,
                &SkipReason::new("market_spread_exceeded", "spread too wide"),
            )
            .await
            .expect("overflow is dropped");

        let snapshot = backpressure.snapshot();
        assert_eq!(snapshot.dropped_diagnostics, 1);
        assert_eq!(snapshot.cold_path_queue_depth, 1);
    }
}
