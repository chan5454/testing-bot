use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use chrono::Utc;
use serde::Serialize;
use tokio::io::AsyncWriteExt;
use tokio::sync::{Mutex, mpsc};
use tracing::warn;

use crate::config::Settings;
use crate::log_retention::{RetentionOutcome, enforce_jsonl_retention};
use crate::log_rotation::RotatingLogger;
use crate::runtime::backpressure::RuntimeBackpressure;
use crate::wallet::activity_stream::PublicActivityParseReport;

#[derive(Clone)]
pub struct RawActivityLogger {
    enabled: bool,
    path: PathBuf,
    rotating_logger: Option<Arc<Mutex<RotatingLogger>>>,
    write_lock: Arc<Mutex<()>>,
    line_tx: Option<mpsc::Sender<Vec<u8>>>,
    backpressure: RuntimeBackpressure,
}

impl RawActivityLogger {
    #[allow(dead_code)]
    pub fn new(settings: &Settings) -> Self {
        let backpressure = RuntimeBackpressure::new(
            settings.hot_path_queue_capacity,
            settings.cold_path_queue_capacity,
        );
        Self::with_backpressure(settings, backpressure)
    }

    pub fn with_backpressure(settings: &Settings, backpressure: RuntimeBackpressure) -> Self {
        let rotating_logger = if settings.enable_log_rotation
            || settings.enable_time_rotation
            || settings.enable_log_clearing
        {
            let base_path = settings.data_dir.join("raw-activity");
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
                        "failed to initialize rotating raw activity logger, using single-file fallback"
                    );
                    None
                }
            }
        } else {
            None
        };

        let path = settings.data_dir.join("raw-activity.jsonl");
        let write_lock = Arc::new(Mutex::new(()));
        let line_tx = if settings.activity_parser_debug {
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
            enabled: settings.activity_parser_debug,
            path,
            rotating_logger,
            write_lock,
            line_tx,
            backpressure,
        }
    }

    pub async fn record_logger_ready(&self, stream: &'static str) {
        let path = self.current_path().await;
        self.try_write(
            &LoggerReadyRecord {
                event_type: "logger_ready",
                logged_at: Utc::now(),
                stream,
                path: path.display().to_string(),
            },
            "logger_ready",
        )
        .await;
    }

    pub async fn record_raw_message(
        &self,
        message_id: &str,
        generation: u64,
        stream: &'static str,
        frame_kind: &'static str,
        raw_payload: &str,
    ) {
        self.try_write(
            &RawMessageRecord {
                event_type: "raw_message",
                logged_at: Utc::now(),
                message_id: message_id.to_owned(),
                generation,
                stream,
                frame_kind,
                raw_payload: raw_payload.to_owned(),
            },
            "raw_message",
        )
        .await;
    }

    pub async fn record_parse_report(
        &self,
        message_id: &str,
        generation: u64,
        report: &PublicActivityParseReport,
    ) {
        if let Some(detail) = &report.failure_detail {
            self.try_write(
                &ParseFailureRecord {
                    event_type: "parse_failure",
                    logged_at: Utc::now(),
                    message_id: message_id.to_owned(),
                    generation,
                    topic: report.topic.clone(),
                    event_type_hint: report.event_type.clone(),
                    detail: detail.clone(),
                },
                "parse_failure",
            )
            .await;
            return;
        }

        self.try_write(
            &ParsedResultRecord {
                event_type: "parsed_result",
                logged_at: Utc::now(),
                message_id: message_id.to_owned(),
                generation,
                topic: report.topic.clone(),
                event_type_hint: report.event_type.clone(),
                parsed_count: report.events.len(),
                parsed_events: report.events.clone(),
            },
            "parsed_result",
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

    async fn try_write<T: Serialize>(&self, value: &T, context: &'static str) {
        if !self.enabled {
            return;
        }
        if self.backpressure.should_shed_diagnostics() {
            self.backpressure.note_diagnostic_drop();
            return;
        }
        match serde_json::to_vec(value) {
            Ok(line) => self.enqueue_line(line, context).await,
            Err(error) => warn!(
                ?error,
                context, "failed to encode raw activity debug record"
            ),
        }
    }

    async fn enqueue_line(&self, line: Vec<u8>, context: &'static str) {
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
                    context,
                    "dropping raw activity debug record due to cold-path backpressure"
                );
            }
        }
    }

    async fn current_path(&self) -> PathBuf {
        if let Some(logger) = &self.rotating_logger {
            let logger = logger.lock().await;
            return logger.current_path();
        }
        self.path.clone()
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
                warn!(?error, "failed to persist raw activity debug record");
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
    let mut file = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await?;
    file.write_all(line).await?;
    file.write_all(b"\n").await?;
    file.flush().await?;
    Ok(())
}

#[derive(Serialize)]
struct LoggerReadyRecord {
    event_type: &'static str,
    logged_at: chrono::DateTime<Utc>,
    stream: &'static str,
    path: String,
}

#[derive(Serialize)]
struct RawMessageRecord {
    event_type: &'static str,
    logged_at: chrono::DateTime<Utc>,
    message_id: String,
    generation: u64,
    stream: &'static str,
    frame_kind: &'static str,
    raw_payload: String,
}

#[derive(Serialize)]
struct ParseFailureRecord {
    event_type: &'static str,
    logged_at: chrono::DateTime<Utc>,
    message_id: String,
    generation: u64,
    topic: Option<String>,
    event_type_hint: Option<String>,
    detail: String,
}

#[derive(Serialize)]
struct ParsedResultRecord {
    event_type: &'static str,
    logged_at: chrono::DateTime<Utc>,
    message_id: String,
    generation: u64,
    topic: Option<String>,
    event_type_hint: Option<String>,
    parsed_count: usize,
    parsed_events: Vec<crate::wallet::wallet_matching::ActivityTradeEvent>,
}
