use std::collections::{HashSet, VecDeque};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow};
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use reqwest::Client;
use serde::Deserialize;
use serde_json::json;
use tokio::time::{Interval, MissedTickBehavior, interval, interval_at, sleep};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::{Error as WsError, Message};
use tracing::{error, info, warn};

use crate::attribution_logger::AttributionLogger;
use crate::config::Settings;
use crate::health::HealthState;
use crate::orderbook::orderbook_state::{AssetCatalog, AssetMetadata};
use crate::runtime::control::RuntimeControl;
use crate::runtime::ring_buffer::RingBuffer;
use crate::runtime::websocket_reconnect::{
    WS_HEARTBEAT_INTERVAL, WebSocketReconnectState, is_expected_disconnect, log_websocket_error,
};

const GAMMA_BOOTSTRAP_PAGE_LIMIT: usize = 500;
const GAMMA_BOOTSTRAP_MIN_TIMEOUT: Duration = Duration::from_secs(10);
const GAMMA_BOOTSTRAP_RETRY_DELAYS_MS: &[u64] = &[250, 500, 1_000, 2_000];
const MARKET_SUBSCRIPTION_SAFE_MAX_BATCH_SIZE: usize = 100;
const MARKET_SUBSCRIPTION_RETRY_BATCH_SIZE: usize = 50;
const MARKET_SUBSCRIPTION_SEVERE_RETRY_BATCH_SIZE: usize = 25;
const MARKET_SUBSCRIPTION_MIN_DELAY: Duration = Duration::from_millis(40);
const MARKET_SUBSCRIPTION_HOT_PATH_MIN_DELAY: Duration = Duration::from_millis(10);
const MARKET_SUBSCRIPTION_RETRY_DELAY: Duration = Duration::from_millis(100);
const MARKET_SUBSCRIPTION_HOT_PATH_RETRY_DELAY: Duration = Duration::from_millis(50);
const MARKET_SUBSCRIPTION_SEVERE_RETRY_DELAY: Duration = Duration::from_millis(150);
const MARKET_SUBSCRIPTION_HOT_PATH_SEVERE_RETRY_DELAY: Duration = Duration::from_millis(75);
const MARKET_DATA_STALE_AFTER: Duration = Duration::from_secs(30);
const MARKET_DATA_STALE_CHECK_INTERVAL: Duration = Duration::from_secs(5);
const MARKET_INITIAL_DATA_GRACE_MIN: Duration = Duration::from_secs(45);
const MARKET_INITIAL_DATA_GRACE_EXTRA: Duration = Duration::from_secs(15);
const MARKET_INITIAL_DATA_GRACE_MAX: Duration = Duration::from_secs(180);

#[derive(Clone, Debug)]
pub struct RawMarketMessage {
    pub sequence: u64,
    pub generation: u64,
    pub received_at: std::time::Instant,
    pub received_at_utc: chrono::DateTime<Utc>,
    pub payload: Box<str>,
}

#[derive(Clone, Default)]
pub struct MarketStreamHandle;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct MarketSubscriptionPlan {
    batch_size: usize,
    delay: Duration,
}

struct PendingMarketSubscription {
    plan: MarketSubscriptionPlan,
    remaining_chunks: VecDeque<Vec<String>>,
    ticker: Interval,
    total_chunks: usize,
    sent_chunks: usize,
    initial_data_deadline: Instant,
}

impl PendingMarketSubscription {
    fn new(
        plan: MarketSubscriptionPlan,
        remaining_chunks: VecDeque<Vec<String>>,
        asset_count: usize,
    ) -> Self {
        let mut ticker = interval_at(tokio::time::Instant::now() + plan.delay, plan.delay);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let total_chunks = asset_count.div_ceil(plan.batch_size).max(1);
        Self {
            plan,
            remaining_chunks,
            ticker,
            total_chunks,
            sent_chunks: 1,
            initial_data_deadline: Instant::now() + initial_market_data_grace(asset_count, plan),
        }
    }

    fn has_pending(&self) -> bool {
        !self.remaining_chunks.is_empty()
    }

    fn next_chunk(&mut self) -> Option<Vec<String>> {
        let chunk = self.remaining_chunks.pop_front()?;
        self.sent_chunks = self.sent_chunks.saturating_add(1);
        Some(chunk)
    }
}

impl MarketStreamHandle {
    pub async fn spawn_with_catalog(
        settings: Settings,
        catalog: AssetCatalog,
        raw_input: RingBuffer<RawMarketMessage>,
        health: Arc<HealthState>,
        runtime_control: Arc<RuntimeControl>,
        attribution_logger: Arc<AttributionLogger>,
    ) -> Result<Self> {
        info!(
            subscribed_assets = catalog.asset_ids().len(),
            subscribed_conditions = catalog.condition_ids().len(),
            "loaded active market catalog"
        );

        tokio::spawn(run_market_stream(
            settings,
            catalog.clone(),
            raw_input,
            health,
            runtime_control,
            attribution_logger,
        ));

        Ok(Self)
    }
}

async fn run_market_stream(
    settings: Settings,
    catalog: AssetCatalog,
    raw_input: RingBuffer<RawMarketMessage>,
    health: Arc<HealthState>,
    runtime_control: Arc<RuntimeControl>,
    attribution_logger: Arc<AttributionLogger>,
) {
    let sequence = Arc::new(AtomicU64::new(0));
    let mut reconnect_state = WebSocketReconnectState::default();
    loop {
        runtime_control.mark_market_unready();
        let generation = runtime_control.current_generation();
        let reconnect_attempt = if let Some(attempt) = reconnect_state.next_attempt() {
            attribution_logger
                .record_stream_event(
                    "reconnect_attempt",
                    "market_ws",
                    generation,
                    Some(format!(
                        "attempt={} backoff_ms={}",
                        attempt.attempt,
                        attempt.delay.as_millis()
                    )),
                )
                .await;
            sleep(attempt.delay).await;
            attempt.attempt
        } else {
            0
        };

        let generation = runtime_control.current_generation();
        match connect_async(&settings.polymarket_market_ws).await {
            Ok((stream, _)) => {
                attribution_logger
                    .record_stream_event("stream_connected", "market_ws", generation, None)
                    .await;
                let (mut sink, mut source) = stream.split();
                let mut subscription = match start_market_subscription(
                    &mut sink,
                    &settings,
                    &catalog,
                    reconnect_attempt,
                )
                .await
                {
                    Ok(plan) => plan,
                    Err(error) => {
                        log_websocket_error(&error, "failed to subscribe market stream");
                        attribution_logger
                            .record_stream_event(
                                "stream_subscribe_failed",
                                "market_ws",
                                generation,
                                Some(error.to_string()),
                            )
                            .await;
                        if !is_expected_disconnect(&error) {
                            health
                                .set_last_error(format!("market stream subscribe failed: {error}"))
                                .await;
                        }
                        runtime_control.mark_market_unready();
                        reconnect_state.note_disconnect();
                        continue;
                    }
                };
                {
                    attribution_logger
                        .record_stream_event(
                            "stream_subscribed",
                            "market_ws",
                            generation,
                            Some(format!(
                                "assets={} conditions={} batch_size={} delay_ms={} reconnect_attempt={} initial_data_grace_ms={}",
                                catalog.asset_ids().len(),
                                catalog.condition_ids().len(),
                                subscription.plan.batch_size,
                                subscription.plan.delay.as_millis(),
                                reconnect_attempt,
                                initial_market_data_grace(catalog.asset_ids().len(), subscription.plan).as_millis()
                            )),
                        )
                        .await;
                    if let Some(success) = reconnect_state.mark_recovered() {
                        attribution_logger
                            .record_stream_event(
                                "reconnect_success",
                                "market_ws",
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
                let mut stale_watchdog = interval(MARKET_DATA_STALE_CHECK_INTERVAL);
                stale_watchdog.set_missed_tick_behavior(MissedTickBehavior::Delay);
                let reconnect_notify = runtime_control.reconnect_notify();
                let subscribed_at = Instant::now();
                let mut last_market_data_at = subscribed_at;
                let mut market_data_ready = false;

                loop {
                    tokio::select! {
                        _ = reconnect_notify.notified() => break,
                        _ = subscription.ticker.tick(), if subscription.has_pending() => {
                            let Some(chunk) = subscription.next_chunk() else {
                                continue;
                            };
                            let payload = market_subscription_payload(&chunk, false);
                            if let Err(error) = sink.send(Message::Text(payload.to_string().into())).await {
                                log_websocket_error(&error, "failed to continue market stream subscription");
                                attribution_logger
                                    .record_stream_event(
                                        "stream_subscribe_failed",
                                        "market_ws",
                                        generation,
                                        Some(error.to_string()),
                                    )
                                    .await;
                                runtime_control.mark_market_unready();
                                reconnect_state.note_disconnect();
                                break;
                            }
                            if !subscription.has_pending() {
                                attribution_logger
                                    .record_stream_event(
                                        "stream_subscription_completed",
                                        "market_ws",
                                        generation,
                                        Some(format!(
                                            "chunks_sent={} total_chunks={} subscribe_duration_ms={}",
                                            subscription.sent_chunks,
                                            subscription.total_chunks,
                                            subscribed_at.elapsed().as_millis()
                                        )),
                                    )
                                    .await;
                            }
                        }
                        _ = stale_watchdog.tick() => {
                            let now = Instant::now();
                            if is_market_data_stale(
                                last_market_data_at,
                                now,
                                market_data_ready,
                                subscription.initial_data_deadline,
                            ) {
                                let stale_for_ms = if market_data_ready {
                                    last_market_data_at.elapsed().as_millis()
                                } else {
                                    subscribed_at.elapsed().as_millis()
                                };
                                runtime_control.mark_market_unready();
                                health
                                    .set_last_error(format!(
                                        "market stream stale after {stale_for_ms} ms without inbound market data"
                                    ))
                                    .await;
                                attribution_logger
                                    .record_stream_event(
                                        "stream_data_stale",
                                        "market_ws",
                                        generation,
                                        Some(format!(
                                            "no inbound market data for {stale_for_ms} ms after subscribe; forcing reconnect pending_chunks={}",
                                            subscription.remaining_chunks.len()
                                        )),
                                    )
                                    .await;
                                reconnect_state.note_disconnect();
                                break;
                            }
                        }
                        _ = heartbeat.tick() => {
                            if runtime_control.should_reconnect(generation) {
                                attribution_logger
                                    .record_stream_event(
                                        "stream_generation_replaced",
                                        "market_ws",
                                        generation,
                                        Some("runtime requested reconnect".to_owned()),
                                    )
                                    .await;
                                break;
                            }
                            if let Err(error) = send_market_heartbeat(&mut sink).await {
                                log_websocket_error(&error, "market stream heartbeat failed");
                                attribution_logger
                                    .record_stream_event(
                                        "stream_heartbeat_failed",
                                        "market_ws",
                                        generation,
                                        Some(error.to_string()),
                                    )
                                    .await;
                                runtime_control.mark_market_unready();
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
                                            log_websocket_error(&error, "failed to answer market text ping");
                                            attribution_logger
                                                .record_stream_event(
                                                    "stream_ping_response_failed",
                                                    "market_ws",
                                                    generation,
                                                    Some(error.to_string()),
                                                )
                                                .await;
                                            runtime_control.mark_market_unready();
                                            reconnect_state.note_disconnect();
                                            break;
                                        }
                                        continue;
                                    }

                                    last_market_data_at = Instant::now();
                                    if !market_data_ready {
                                        runtime_control.mark_market_ready(generation);
                                        attribution_logger
                                            .record_stream_event(
                                                "stream_data_flow_resumed",
                                                "market_ws",
                                                generation,
                                                Some(format!(
                                                    "first_market_payload_ms={}",
                                                    subscribed_at.elapsed().as_millis()
                                                )),
                                            )
                                            .await;
                                        market_data_ready = true;
                                    }

                                    raw_input.push(RawMarketMessage {
                                        sequence: sequence.fetch_add(1, Ordering::Relaxed),
                                        generation,
                                        received_at: std::time::Instant::now(),
                                        received_at_utc: Utc::now(),
                                        payload: text.to_string().into_boxed_str(),
                                    });
                                }
                                Some(Ok(Message::Ping(payload))) => {
                                    if let Err(error) = sink.send(Message::Pong(payload)).await {
                                        log_websocket_error(&error, "failed to answer market ping");
                                        attribution_logger
                                            .record_stream_event(
                                                "stream_ping_response_failed",
                                                "market_ws",
                                                generation,
                                                Some(error.to_string()),
                                            )
                                            .await;
                                        runtime_control.mark_market_unready();
                                        reconnect_state.note_disconnect();
                                        break;
                                    }
                                }
                                Some(Ok(Message::Pong(_))) => {}
                                Some(Ok(Message::Close(frame))) => {
                                    attribution_logger
                                        .record_stream_event(
                                            "stream_closed",
                                            "market_ws",
                                            generation,
                                            Some(format!("{frame:?}")),
                                        )
                                        .await;
                                    runtime_control.mark_market_unready();
                                    reconnect_state.note_disconnect();
                                    break;
                                }
                                Some(Err(error)) => {
                                    log_websocket_error(&error, "market stream read failed");
                                    attribution_logger
                                        .record_stream_event(
                                            "stream_read_failed",
                                            "market_ws",
                                            generation,
                                            Some(error.to_string()),
                                        )
                                        .await;
                                    runtime_control.mark_market_unready();
                                    reconnect_state.note_disconnect();
                                    break;
                                }
                                None => {
                                    attribution_logger
                                        .record_stream_event(
                                            "stream_ended",
                                            "market_ws",
                                            generation,
                                            Some("market websocket returned EOF".to_owned()),
                                        )
                                        .await;
                                    runtime_control.mark_market_unready();
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
                    info!(?error, "market stream connect interrupted; reconnecting");
                } else {
                    error!(?error, "market stream connect failed");
                }
                attribution_logger
                    .record_stream_event(
                        "stream_connect_failed",
                        "market_ws",
                        generation,
                        Some(error.to_string()),
                    )
                    .await;
                if !is_expected_disconnect(&error) {
                    health
                        .set_last_error(format!("market stream connect failed: {error}"))
                        .await;
                }
                runtime_control.mark_market_unready();
                reconnect_state.note_disconnect();
            }
        }
    }
}

async fn start_market_subscription<S>(
    sink: &mut S,
    settings: &Settings,
    catalog: &AssetCatalog,
    reconnect_attempt: u32,
) -> std::result::Result<PendingMarketSubscription, WsError>
where
    S: futures_util::Sink<Message, Error = WsError> + Unpin,
{
    let asset_ids = catalog.asset_ids();
    let plan = market_subscription_plan(settings, reconnect_attempt);
    info!(
        reconnect_attempt,
        subscribed_assets = asset_ids.len(),
        batch_size = plan.batch_size,
        delay_ms = plan.delay.as_millis() as u64,
        "subscribing market stream with adaptive batching"
    );

    let mut chunks = asset_ids
        .chunks(plan.batch_size)
        .map(|chunk| chunk.to_vec())
        .collect::<VecDeque<_>>();
    let Some(initial_chunk) = chunks.pop_front() else {
        return Err(WsError::Io(std::io::Error::other(
            "market catalog is empty; nothing to subscribe",
        )));
    };

    let payload = market_subscription_payload(&initial_chunk, true);
    sink.send(Message::Text(payload.to_string().into())).await?;

    if chunks.is_empty() {
        let mut ticker = interval(plan.delay);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        return Ok(PendingMarketSubscription {
            plan,
            remaining_chunks: VecDeque::new(),
            ticker,
            total_chunks: 1,
            sent_chunks: 1,
            initial_data_deadline: Instant::now()
                + initial_market_data_grace(asset_ids.len(), plan),
        });
    }

    Ok(PendingMarketSubscription::new(
        plan,
        chunks,
        asset_ids.len(),
    ))
}

fn market_subscription_plan(settings: &Settings, reconnect_attempt: u32) -> MarketSubscriptionPlan {
    let min_delay = if settings.hot_path_mode {
        MARKET_SUBSCRIPTION_HOT_PATH_MIN_DELAY
    } else {
        MARKET_SUBSCRIPTION_MIN_DELAY
    };
    let retry_delay = if settings.hot_path_mode {
        MARKET_SUBSCRIPTION_HOT_PATH_RETRY_DELAY
    } else {
        MARKET_SUBSCRIPTION_RETRY_DELAY
    };
    let severe_retry_delay = if settings.hot_path_mode {
        MARKET_SUBSCRIPTION_HOT_PATH_SEVERE_RETRY_DELAY
    } else {
        MARKET_SUBSCRIPTION_SEVERE_RETRY_DELAY
    };
    let mut batch_size = settings
        .market_subscription_batch_size
        .max(1)
        .min(MARKET_SUBSCRIPTION_SAFE_MAX_BATCH_SIZE);
    let mut delay = settings.market_subscription_delay.max(min_delay);

    if reconnect_attempt >= 2 {
        batch_size = batch_size.min(MARKET_SUBSCRIPTION_RETRY_BATCH_SIZE);
        delay = delay.max(retry_delay);
    }
    if reconnect_attempt >= 4 {
        batch_size = batch_size.min(MARKET_SUBSCRIPTION_SEVERE_RETRY_BATCH_SIZE);
        delay = delay.max(severe_retry_delay);
    }

    MarketSubscriptionPlan { batch_size, delay }
}

fn market_subscription_payload(chunk: &[String], initial: bool) -> serde_json::Value {
    if initial {
        json!({
            "type": "market",
            "assets_ids": chunk,
            "custom_feature_enabled": true
        })
    } else {
        json!({
            "operation": "subscribe",
            "assets_ids": chunk,
            "custom_feature_enabled": true
        })
    }
}

fn is_market_data_stale(
    last_market_data_at: Instant,
    now: Instant,
    market_data_ready: bool,
    initial_data_deadline: Instant,
) -> bool {
    if market_data_ready {
        now.duration_since(last_market_data_at) >= MARKET_DATA_STALE_AFTER
    } else {
        now >= initial_data_deadline
    }
}

fn initial_market_data_grace(asset_count: usize, plan: MarketSubscriptionPlan) -> Duration {
    let chunk_count = asset_count.div_ceil(plan.batch_size).max(1);
    let delayed_chunks = chunk_count.saturating_sub(1);
    let delayed_chunks_u32 = u32::try_from(delayed_chunks).unwrap_or(u32::MAX);
    let estimated_subscription_duration = plan.delay.saturating_mul(delayed_chunks_u32);
    let grace = estimated_subscription_duration.saturating_add(MARKET_INITIAL_DATA_GRACE_EXTRA);
    grace
        .max(MARKET_INITIAL_DATA_GRACE_MIN)
        .min(MARKET_INITIAL_DATA_GRACE_MAX)
}

async fn send_market_heartbeat<S>(sink: &mut S) -> std::result::Result<(), WsError>
where
    S: futures_util::Sink<Message, Error = WsError> + Unpin,
{
    sink.send(Message::Text("PING".into())).await
}

pub async fn load_active_asset_catalog(settings: &Settings) -> Result<AssetCatalog> {
    let cache_path = settings.data_dir.join("gamma-asset-catalog.json");
    let client = Client::builder()
        .connect_timeout((settings.http_timeout / 2).max(Duration::from_secs(2)))
        .timeout(settings.http_timeout.max(GAMMA_BOOTSTRAP_MIN_TIMEOUT))
        .pool_max_idle_per_host(4)
        .pool_idle_timeout(Duration::from_secs(30))
        .tcp_keepalive(Duration::from_secs(30))
        .tcp_nodelay(true)
        .http2_adaptive_window(true)
        .user_agent("polymarket-copy-bot/0.1.0")
        .build()?;

    match fetch_active_gamma_assets(&client, settings).await {
        Ok(assets) => {
            if let Err(error) = save_gamma_asset_catalog_cache(&cache_path, &assets).await {
                warn!(
                    ?error,
                    cache_path = %cache_path.display(),
                    "failed to persist gamma asset catalog cache"
                );
            }
            Ok(AssetCatalog::new(assets))
        }
        Err(error) => {
            let cached_assets = load_gamma_asset_catalog_cache(&cache_path).await;
            match cached_assets {
                Ok(cached_assets) if !cached_assets.is_empty() => {
                    warn!(
                        ?error,
                        cache_path = %cache_path.display(),
                        cached_assets = cached_assets.len(),
                        "gamma bootstrap failed; falling back to cached asset catalog"
                    );
                    Ok(AssetCatalog::new(cached_assets))
                }
                Ok(_) | Err(_) => Err(error),
            }
        }
    }
}

async fn fetch_active_gamma_assets(
    client: &Client,
    settings: &Settings,
) -> Result<Vec<AssetMetadata>> {
    let mut offset = 0_usize;
    let mut seen_assets = HashSet::new();
    let mut assets = Vec::new();

    loop {
        let page = fetch_gamma_market_page(client, settings, offset).await?;

        if page.is_empty() {
            break;
        }

        let page_len = page.len();
        for market in page {
            let Some(condition_id) = market.condition_id else {
                continue;
            };
            let title = market.question.unwrap_or_else(|| condition_id.clone());
            let slug = market.slug.unwrap_or_else(|| condition_id.clone());
            let event_slug = market.event_slug.unwrap_or_else(|| slug.clone());

            for (index, asset_id) in market.clob_token_ids.into_iter().enumerate() {
                if !seen_assets.insert(asset_id.clone()) {
                    continue;
                }
                let outcome = market
                    .outcomes
                    .get(index)
                    .cloned()
                    .unwrap_or_else(|| format!("OUTCOME_{index}"));
                assets.push(AssetMetadata {
                    asset_id,
                    condition_id: condition_id.clone(),
                    title: title.clone(),
                    slug: slug.clone(),
                    event_slug: event_slug.clone(),
                    outcome,
                    outcome_index: index as i64,
                });
            }
        }
        offset += page_len;
    }

    if assets.is_empty() {
        return Err(anyhow!("gamma market discovery returned zero asset ids"));
    }

    Ok(assets)
}

async fn fetch_gamma_market_page(
    client: &Client,
    settings: &Settings,
    offset: usize,
) -> Result<Vec<GammaMarket>> {
    let url = format!("{}/markets", settings.polymarket_gamma_api);
    let mut last_error = None;
    let limit = GAMMA_BOOTSTRAP_PAGE_LIMIT.to_string();
    let offset_string = offset.to_string();

    for (attempt_index, backoff_ms) in GAMMA_BOOTSTRAP_RETRY_DELAYS_MS.iter().enumerate() {
        let result = client
            .get(&url)
            .query(&[
                ("active", "true"),
                ("closed", "false"),
                ("limit", limit.as_str()),
                ("offset", offset_string.as_str()),
            ])
            .send()
            .await
            .context("requesting gamma markets")
            .and_then(|response| {
                response
                    .error_for_status()
                    .context("gamma markets request failed")
            });
        let result = match result {
            Ok(response) => response
                .json::<Vec<GammaMarket>>()
                .await
                .context("decoding gamma markets response"),
            Err(error) => Err(error),
        };

        match result {
            Ok(page) => return Ok(page),
            Err(error) => {
                last_error = Some(error);
                let attempt = attempt_index + 1;
                let retry_count = GAMMA_BOOTSTRAP_RETRY_DELAYS_MS.len();
                if attempt == retry_count {
                    break;
                }
                warn!(
                    ?last_error,
                    offset,
                    attempt,
                    retry_count,
                    backoff_ms,
                    "gamma markets page fetch failed; retrying"
                );
                sleep(Duration::from_millis(*backoff_ms)).await;
            }
        }
    }

    Err(last_error
        .unwrap_or_else(|| anyhow!("gamma markets page fetch failed without an error"))
        .context(format!(
            "gamma markets bootstrap failed after {} attempts at offset {}",
            GAMMA_BOOTSTRAP_RETRY_DELAYS_MS.len(),
            offset
        )))
}

async fn save_gamma_asset_catalog_cache(path: &Path, assets: &[AssetMetadata]) -> Result<()> {
    let payload = serde_json::to_vec(assets)?;
    tokio::fs::write(path, payload).await?;
    Ok(())
}

async fn load_gamma_asset_catalog_cache(path: &Path) -> Result<Vec<AssetMetadata>> {
    let payload = tokio::fs::read(path).await?;
    let assets = serde_json::from_slice::<Vec<AssetMetadata>>(&payload)?;
    Ok(assets)
}

#[derive(Debug, Deserialize)]
struct GammaMarket {
    #[serde(rename = "conditionId")]
    condition_id: Option<String>,
    question: Option<String>,
    slug: Option<String>,
    #[serde(rename = "eventSlug")]
    event_slug: Option<String>,
    #[serde(default, deserialize_with = "deserialize_string_array")]
    outcomes: Vec<String>,
    #[serde(rename = "clobTokenIds", deserialize_with = "deserialize_string_array")]
    clob_token_ids: Vec<String>,
}

fn deserialize_string_array<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = serde_json::Value::deserialize(deserializer)?;
    match value {
        serde_json::Value::String(raw) => {
            serde_json::from_str::<Vec<String>>(&raw).map_err(serde::de::Error::custom)
        }
        serde_json::Value::Array(values) => values
            .into_iter()
            .map(|value| match value {
                serde_json::Value::String(item) => Ok(item),
                other => Err(serde::de::Error::custom(format!(
                    "unexpected string array value {other}"
                ))),
            })
            .collect(),
        other => Err(serde::de::Error::custom(format!(
            "unexpected string array payload {other}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use super::*;

    fn test_settings() -> Settings {
        Settings {
            execution_mode: crate::config::ExecutionMode::Paper,
            polymarket_host: "https://clob.polymarket.com".to_owned(),
            polymarket_data_api: "https://data-api.polymarket.com".to_owned(),
            polymarket_gamma_api: "https://gamma-api.polymarket.com".to_owned(),
            polymarket_market_ws: "wss://example.com/ws/market".to_owned(),
            polymarket_user_ws: "wss://example.com/ws/user".to_owned(),
            polymarket_activity_ws: "wss://example.com/ws/activity".to_owned(),
            polymarket_chain_id: 137,
            polymarket_signature_type: 0,
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
            target_profile_addresses: vec!["0xabc".to_owned()],
            start_capital_usd: Decimal::ZERO,
            paper_execution_delay: Duration::ZERO,
            copy_only_new_trades: true,
            source_trades_limit: 50,
            http_timeout: Duration::from_secs(2),
            market_cache_ttl: Duration::from_secs(10),
            market_raw_ring_capacity: 8192,
            market_parser_workers: 1,
            market_subscription_batch_size: 250,
            market_subscription_delay: Duration::from_millis(20),
            wallet_ring_capacity: 1024,
            wallet_parser_workers: 1,
            wallet_subscription_batch_size: 250,
            wallet_subscription_delay: Duration::from_millis(20),
            hot_path_mode: true,
            hot_path_queue_capacity: 128,
            cold_path_queue_capacity: 512,
            attribution_fast_cache_capacity: 256,
            persistence_flush_interval: Duration::from_millis(250),
            analytics_flush_interval: Duration::from_millis(500),
            telegram_async_only: true,
            fast_risk_only_on_hot_path: true,
            exit_priority_strict: true,
            parse_tasks_market: 1,
            parse_tasks_wallet: 1,
            liquidity_sweep_threshold: Decimal::ONE,
            imbalance_threshold: Decimal::ONE,
            delta_price_move_bps: 40,
            delta_size_drop_ratio: Decimal::ONE,
            delta_min_size_drop: Decimal::ONE,
            inference_confirmation_window: Duration::from_millis(400),
            activity_stream_enabled: true,
            activity_match_window: Duration::from_millis(200),
            activity_price_tolerance: Decimal::new(1, 2),
            activity_size_tolerance_ratio: Decimal::new(5, 1),
            activity_cache_ttl: Duration::from_millis(1_500),
            fallback_market_request_interval: Duration::from_millis(1_000),
            fallback_global_requests_per_minute: 30,
            activity_correlation_window: Duration::from_millis(400),
            attribution_lookback: Duration::from_millis(2_500),
            attribution_trades_limit: 100,
            copy_scale_above_five_usd: Decimal::ONE,
            min_copy_notional_usd: Decimal::ONE,
            max_copy_notional_usd: Decimal::ONE,
            max_total_exposure_usd: Decimal::ONE,
            max_market_exposure_usd: Decimal::ONE,
            min_source_trade_usdc: Decimal::ZERO,
            max_market_spread_bps: 0,
            min_top_of_book_ratio: Decimal::ZERO,
            max_slippage_bps: 0,
            max_source_price_slippage_bps: 0,
            latency_fail_safe_enabled: true,
            max_latency: Duration::from_millis(500),
            average_latency_threshold: Duration::from_millis(350),
            latency_monitor_interval: Duration::from_millis(50),
            latency_reconnect_settle: Duration::from_millis(750),
            prediction_validation_timeout: Duration::from_millis(1500),
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
            max_position_age_hours: 6,
            max_hold_time_seconds: 1_800,
            enable_exit_retry: true,
            exit_retry_window: Duration::from_secs(30),
            exit_retry_interval: Duration::from_millis(500),
            closing_max_age: Duration::from_secs(30),
            force_exit_on_closing_timeout: true,
            telegram_bot_token: "token".to_owned(),
            telegram_chat_id: "chat".to_owned(),
            health_port: 8080,
            data_dir: std::path::PathBuf::from("."),
        }
    }

    #[test]
    fn market_subscription_plan_slows_after_retries() {
        let settings = test_settings();

        assert_eq!(
            market_subscription_plan(&settings, 0),
            MarketSubscriptionPlan {
                batch_size: 100,
                delay: Duration::from_millis(20),
            }
        );
        assert_eq!(
            market_subscription_plan(&settings, 2),
            MarketSubscriptionPlan {
                batch_size: 50,
                delay: Duration::from_millis(50),
            }
        );
        assert_eq!(
            market_subscription_plan(&settings, 4),
            MarketSubscriptionPlan {
                batch_size: 25,
                delay: Duration::from_millis(75),
            }
        );
    }

    #[test]
    fn market_subscription_payload_matches_documented_protocol() {
        let assets = vec!["asset-a".to_owned(), "asset-b".to_owned()];

        let initial = market_subscription_payload(&assets, true);
        assert_eq!(initial["type"], "market");
        assert_eq!(initial["assets_ids"][0], "asset-a");
        assert_eq!(initial["custom_feature_enabled"], true);
        assert!(initial.get("operation").is_none());

        let incremental = market_subscription_payload(&assets, false);
        assert_eq!(incremental["operation"], "subscribe");
        assert_eq!(incremental["assets_ids"][1], "asset-b");
        assert_eq!(incremental["custom_feature_enabled"], true);
        assert!(incremental.get("type").is_none());
    }

    #[test]
    fn market_data_watchdog_trips_after_threshold() {
        let now = Instant::now();
        let initial_deadline = now + MARKET_DATA_STALE_AFTER;

        assert!(!is_market_data_stale(
            now,
            now + MARKET_DATA_STALE_AFTER - Duration::from_millis(1),
            true,
            initial_deadline
        ));
        assert!(is_market_data_stale(
            now,
            now + MARKET_DATA_STALE_AFTER,
            true,
            initial_deadline
        ));
    }

    #[test]
    fn initial_market_data_grace_scales_with_subscription_size() {
        let baseline = initial_market_data_grace(
            100,
            MarketSubscriptionPlan {
                batch_size: 100,
                delay: Duration::from_millis(40),
            },
        );
        let default_full_universe = initial_market_data_grace(
            72_640,
            MarketSubscriptionPlan {
                batch_size: 100,
                delay: Duration::from_millis(40),
            },
        );
        let slower_full_universe = initial_market_data_grace(
            72_640,
            MarketSubscriptionPlan {
                batch_size: 50,
                delay: Duration::from_millis(100),
            },
        );
        let very_slow = initial_market_data_grace(
            72_640,
            MarketSubscriptionPlan {
                batch_size: 25,
                delay: Duration::from_millis(150),
            },
        );

        assert_eq!(baseline, MARKET_INITIAL_DATA_GRACE_MIN);
        assert_eq!(default_full_universe, MARKET_INITIAL_DATA_GRACE_MIN);
        assert!(slower_full_universe > baseline);
        assert_eq!(very_slow, MARKET_INITIAL_DATA_GRACE_MAX);
    }

    #[test]
    fn market_data_watchdog_uses_initial_deadline_before_first_payload() {
        let now = Instant::now();
        let initial_deadline = now + Duration::from_secs(90);

        assert!(!is_market_data_stale(
            now,
            now + Duration::from_secs(30),
            false,
            initial_deadline
        ));
        assert!(is_market_data_stale(
            now,
            initial_deadline,
            false,
            initial_deadline
        ));
    }
}
