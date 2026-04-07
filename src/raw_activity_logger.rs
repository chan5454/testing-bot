use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use chrono::Utc;
use serde::Serialize;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;
use tracing::warn;

use crate::config::Settings;
use crate::log_retention::{RetentionOutcome, enforce_jsonl_retention};
use crate::log_rotation::RotatingLogger;
use crate::wallet::activity_stream::PublicActivityParseReport;

#[derive(Clone)]
pub struct RawActivityLogger {
    enabled: bool,
    path: PathBuf,
    rotating_logger: Option<Arc<Mutex<RotatingLogger>>>,
    write_lock: Arc<Mutex<()>>,
}

impl RawActivityLogger {
    pub fn new(settings: &Settings) -> Self {
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

        Self {
            enabled: settings.activity_parser_debug,
            path: settings.data_dir.join("raw-activity.jsonl"),
            rotating_logger,
            write_lock: Arc::new(Mutex::new(())),
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
        if let Err(error) = self.write_json_line(value).await {
            warn!(
                ?error,
                context, "failed to persist raw activity debug record"
            );
        }
    }

    async fn write_json_line<T: Serialize>(&self, value: &T) -> Result<()> {
        let json = serde_json::to_vec(value)?;

        if let Some(logger) = &self.rotating_logger {
            let mut logger = logger.lock().await;
            logger.write_line(&String::from_utf8_lossy(&json))?;
            return Ok(());
        }

        let _guard = self.write_lock.lock().await;
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .await?;
        file.write_all(&json).await?;
        file.write_all(b"\n").await?;
        file.flush().await?;
        Ok(())
    }

    async fn current_path(&self) -> PathBuf {
        if let Some(logger) = &self.rotating_logger {
            let logger = logger.lock().await;
            return logger.current_path();
        }
        self.path.clone()
    }
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
