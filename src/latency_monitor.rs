use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use tokio::sync::{Mutex, Notify, mpsc};
use tokio::time::{MissedTickBehavior, interval, sleep};

use crate::config::Settings;
use crate::health::HealthState;
use crate::models::TradeStageTimestamps;
use crate::orderbook::orderbook_state::OrderBookState;
use crate::runtime::control::RuntimeControl;

const LATENCY_WINDOW_SIZE: usize = 50;

#[derive(Clone)]
pub struct LatencyMonitor {
    inner: Arc<LatencyMonitorInner>,
}

struct LatencyMonitorInner {
    enabled: bool,
    max_total_latency_ms: u64,
    average_latency_threshold_ms: u64,
    monitor_interval: Duration,
    reconnect_settle: Duration,
    samples: Vec<AtomicU64>,
    write_index: AtomicU64,
    sample_count: AtomicU64,
    last_detection_ms: AtomicU64,
    last_execution_ms: AtomicU64,
    last_total_ms: AtomicU64,
    average_total_ms: AtomicU64,
    sample_notify: Arc<Notify>,
    trip_tx: mpsc::UnboundedSender<LatencyTrip>,
    trip_rx: Mutex<Option<mpsc::UnboundedReceiver<LatencyTrip>>>,
}

#[derive(Clone, Debug)]
pub struct LatencyMeasurement {
    pub detection_latency_ms: u64,
    pub execution_latency_ms: u64,
    pub total_latency_ms: u64,
}

#[derive(Clone, Debug)]
struct LatencyTrip {
    reason: String,
    observed_total_ms: u64,
}

impl LatencyMonitor {
    pub fn new(settings: &Settings) -> Self {
        let (trip_tx, trip_rx) = mpsc::unbounded_channel();
        Self {
            inner: Arc::new(LatencyMonitorInner {
                enabled: settings.latency_fail_safe_enabled,
                max_total_latency_ms: settings.max_latency.as_millis() as u64,
                average_latency_threshold_ms: settings.average_latency_threshold.as_millis() as u64,
                monitor_interval: settings.latency_monitor_interval,
                reconnect_settle: settings.latency_reconnect_settle,
                samples: (0..LATENCY_WINDOW_SIZE)
                    .map(|_| AtomicU64::new(0))
                    .collect(),
                write_index: AtomicU64::new(0),
                sample_count: AtomicU64::new(0),
                last_detection_ms: AtomicU64::new(0),
                last_execution_ms: AtomicU64::new(0),
                last_total_ms: AtomicU64::new(0),
                average_total_ms: AtomicU64::new(0),
                sample_notify: Arc::new(Notify::new()),
                trip_tx,
                trip_rx: Mutex::new(Some(trip_rx)),
            }),
        }
    }

    pub fn measure_submission(
        &self,
        stage_timestamps: &TradeStageTimestamps,
        submitted_at: Instant,
    ) -> LatencyMeasurement {
        let measurement = LatencyMeasurement {
            detection_latency_ms: stage_timestamps.detection_latency_ms(),
            execution_latency_ms: stage_timestamps.execution_latency_ms(submitted_at),
            total_latency_ms: stage_timestamps.total_latency_ms(submitted_at),
        };

        self.inner
            .last_detection_ms
            .store(measurement.detection_latency_ms, Ordering::Relaxed);
        self.inner
            .last_execution_ms
            .store(measurement.execution_latency_ms, Ordering::Relaxed);
        self.inner
            .last_total_ms
            .store(measurement.total_latency_ms, Ordering::Relaxed);

        let slot =
            self.inner.write_index.fetch_add(1, Ordering::Relaxed) as usize % LATENCY_WINDOW_SIZE;
        self.inner.samples[slot].store(measurement.total_latency_ms, Ordering::Relaxed);
        let count = self.inner.sample_count.load(Ordering::Relaxed);
        if count < LATENCY_WINDOW_SIZE as u64 {
            self.inner.sample_count.fetch_add(1, Ordering::Relaxed);
        }

        self.inner.sample_notify.notify_one();
        if self.inner.enabled && measurement.total_latency_ms > self.inner.max_total_latency_ms {
            let _ = self.inner.trip_tx.send(LatencyTrip {
                reason: format!(
                    "total latency {} ms exceeded max {} ms",
                    measurement.total_latency_ms, self.inner.max_total_latency_ms
                ),
                observed_total_ms: measurement.total_latency_ms,
            });
        }

        measurement
    }

    pub fn should_pause_before_submit(
        &self,
        stage_timestamps: &TradeStageTimestamps,
    ) -> Option<u64> {
        if !self.inner.enabled {
            return None;
        }

        let elapsed_total_ms = stage_timestamps.elapsed_total_ms();
        if elapsed_total_ms > self.inner.max_total_latency_ms {
            let _ = self.inner.trip_tx.send(LatencyTrip {
                reason: format!(
                    "in-flight latency {} ms exceeded max {} ms before submission",
                    elapsed_total_ms, self.inner.max_total_latency_ms
                ),
                observed_total_ms: elapsed_total_ms,
            });
            return Some(elapsed_total_ms);
        }

        None
    }

    pub fn spawn_supervisor(
        &self,
        runtime_control: Arc<RuntimeControl>,
        orderbooks: Arc<OrderBookState>,
        health: Arc<HealthState>,
    ) {
        let inner = self.inner.clone();
        tokio::spawn(async move {
            let mut trip_rx = match inner.trip_rx.lock().await.take() {
                Some(receiver) => receiver,
                None => return,
            };
            let mut monitor_tick = interval(inner.monitor_interval.max(Duration::from_millis(10)));
            monitor_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    _ = monitor_tick.tick() => {
                        recompute_average(&inner);
                        let average_latency_ms = inner.average_total_ms.load(Ordering::Relaxed);
                        health.set_average_latency(average_latency_ms).await;
                        if inner.enabled && average_latency_ms > inner.average_latency_threshold_ms {
                            engage_pause(
                                &inner,
                                &runtime_control,
                                &orderbooks,
                                &health,
                                LatencyTrip {
                                    reason: format!(
                                        "average latency {} ms exceeded threshold {} ms across last {} trades",
                                        average_latency_ms,
                                        inner.average_latency_threshold_ms,
                                        LATENCY_WINDOW_SIZE
                                    ),
                                    observed_total_ms: average_latency_ms,
                                },
                            )
                            .await;
                        }
                    }
                    _ = inner.sample_notify.notified() => {
                        recompute_average(&inner);
                        health
                            .set_average_latency(inner.average_total_ms.load(Ordering::Relaxed))
                            .await;
                    }
                    Some(trip) = trip_rx.recv() => {
                        recompute_average(&inner);
                        engage_pause(&inner, &runtime_control, &orderbooks, &health, trip).await;
                    }
                }
            }
        });
    }
}

fn recompute_average(inner: &LatencyMonitorInner) {
    let count = inner
        .sample_count
        .load(Ordering::Relaxed)
        .min(LATENCY_WINDOW_SIZE as u64) as usize;
    if count == 0 {
        inner.average_total_ms.store(0, Ordering::Relaxed);
        return;
    }

    let mut sum = 0_u64;
    for index in 0..count {
        sum = sum.saturating_add(inner.samples[index].load(Ordering::Relaxed));
    }
    inner
        .average_total_ms
        .store(sum / count as u64, Ordering::Relaxed);
}

async fn engage_pause(
    inner: &LatencyMonitorInner,
    runtime_control: &RuntimeControl,
    orderbooks: &OrderBookState,
    health: &HealthState,
    trip: LatencyTrip,
) {
    if !inner.enabled || runtime_control.is_paused() {
        return;
    }

    let average_latency_ms = inner.average_total_ms.load(Ordering::Relaxed);
    let generation = runtime_control.request_pause_and_reconnect();
    health
        .record_latency_fail_safe_trip(
            trip.reason.clone(),
            trip.observed_total_ms,
            average_latency_ms,
        )
        .await;
    orderbooks.clear().await;

    let deadline = Instant::now() + inner.reconnect_settle;
    while Instant::now() < deadline {
        if runtime_control.streams_ready(generation) {
            break;
        }
        sleep(Duration::from_millis(25)).await;
    }

    runtime_control.resume();
    health.set_trading_paused(false, Some(trip.reason)).await;
}
