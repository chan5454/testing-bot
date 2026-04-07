use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Result;
use axum::Router;
use axum::extract::State;
use axum::routing::get;
use chrono::Utc;
use rust_decimal::Decimal;
use tokio::net::TcpListener;
use tokio::sync::RwLock;

use crate::config::ExecutionMode;
use crate::models::HealthSnapshot;

#[derive(Clone)]
pub struct HealthState {
    inner: Arc<RwLock<HealthSnapshot>>,
}

impl HealthState {
    pub fn new(execution_mode: ExecutionMode) -> Self {
        Self {
            inner: Arc::new(RwLock::new(HealthSnapshot {
                execution_mode: execution_mode.to_string(),
                ..HealthSnapshot::default()
            })),
        }
    }

    pub async fn mark_ready(&self, ready: bool) {
        let mut health = self.inner.write().await;
        health.ready = ready;
        health.last_update_iso = Some(Utc::now());
    }

    pub async fn increment_processed(&self) {
        let mut health = self.inner.write().await;
        health.processed_trades += 1;
        health.last_update_iso = Some(Utc::now());
    }

    pub async fn increment_skipped(&self) {
        let mut health = self.inner.write().await;
        health.skipped_trades += 1;
        health.last_update_iso = Some(Utc::now());
    }

    pub async fn record_skip_latency(&self, skip_processing_ms: u64, reason_code: &str) {
        let mut health = self.inner.write().await;
        health.last_skip_processing_ms = skip_processing_ms;
        health.last_skip_reason_code = Some(reason_code.to_owned());
        health.last_update_iso = Some(Utc::now());
    }

    pub async fn set_last_error(&self, error: String) {
        let mut health = self.inner.write().await;
        health.last_error = Some(error);
        health.last_update_iso = Some(Utc::now());
    }

    pub async fn set_portfolio_value(&self, value: Decimal) {
        let mut health = self.inner.write().await;
        health.last_portfolio_value = Some(value);
        health.last_update_iso = Some(Utc::now());
    }

    pub async fn record_latency(&self, detection_ms: u64, execution_ms: u64, total_ms: u64) {
        let mut health = self.inner.write().await;
        health.last_detection_ms = detection_ms;
        health.last_execution_ms = execution_ms;
        health.last_total_latency_ms = total_ms;
        health.last_update_iso = Some(Utc::now());
    }

    pub async fn set_average_latency(&self, average_ms: u64) {
        let mut health = self.inner.write().await;
        health.average_latency_ms = average_ms;
        health.last_update_iso = Some(Utc::now());
    }

    pub async fn set_trading_paused(&self, paused: bool, reason: Option<String>) {
        let mut health = self.inner.write().await;
        health.trading_paused = paused;
        health.last_latency_pause_reason = reason;
        health.last_update_iso = Some(Utc::now());
    }

    pub async fn record_latency_fail_safe_trip(
        &self,
        reason: String,
        observed_total_ms: u64,
        average_latency_ms: u64,
    ) {
        let mut health = self.inner.write().await;
        health.trading_paused = true;
        health.last_latency_pause_reason = Some(reason);
        health.last_total_latency_ms = observed_total_ms;
        health.average_latency_ms = average_latency_ms;
        health.latency_fail_safe_trips += 1;
        health.last_update_iso = Some(Utc::now());
    }

    pub async fn snapshot(&self) -> HealthSnapshot {
        self.inner.read().await.clone()
    }
}

pub async fn spawn_health_server(port: u16, health: Arc<HealthState>) -> Result<()> {
    let app = Router::new()
        .route("/health", get(health_handler))
        .with_state(health);
    let listener = TcpListener::bind(SocketAddr::from(([0, 0, 0, 0], port))).await?;
    tokio::spawn(async move {
        if let Err(error) = axum::serve(listener, app).await {
            tracing::error!(?error, "health server failed");
        }
    });
    Ok(())
}

async fn health_handler(State(health): State<Arc<HealthState>>) -> axum::Json<HealthSnapshot> {
    axum::Json(health.snapshot().await)
}
