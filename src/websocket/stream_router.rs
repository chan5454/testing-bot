use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use chrono::{DateTime, Utc};
use futures_util::future::BoxFuture;
use serde::Deserialize;
use tokio::sync::mpsc;
use tracing::{debug, error, warn};

use crate::config::Settings;
use crate::detection::liquidity_sweep_detector::LiquiditySweepDetector;
use crate::detection::trade_inference::ConfirmedTradeSignal;
use crate::detection::trade_inference::{LastTradeConfirmation, TradeInferenceEngine};
use crate::execution::ExecutionSide;
use crate::health::HealthState;
use crate::models::OrderBookResponse;
use crate::orderbook::orderbook_levels::{BookSide, PriceLevelUpdate};
use crate::orderbook::orderbook_state::OrderBookState;
use crate::runtime::control::RuntimeControl;
use crate::runtime::ring_buffer::RingBuffer;
use crate::runtime::worker_pool::spawn_worker_pool;
use crate::websocket::market_stream::RawMarketMessage;

#[derive(Clone)]
pub struct StreamRouterHandle {
    input: RingBuffer<RawMarketMessage>,
}

#[derive(Clone, Debug)]
struct SequencedParsedMessage {
    sequence: u64,
    generation: u64,
    events: Vec<ParsedMarketEvent>,
}

#[derive(Clone, Debug)]
enum ParsedMarketEvent {
    Book {
        book: OrderBookResponse,
        received_at: Instant,
        received_at_utc: DateTime<Utc>,
        parse_completed_at: Instant,
        parse_completed_at_utc: DateTime<Utc>,
    },
    PriceUpdates {
        asset_id: String,
        condition_id: String,
        updates: Vec<PriceLevelUpdate>,
        received_at: Instant,
        received_at_utc: DateTime<Utc>,
        parse_completed_at: Instant,
        parse_completed_at_utc: DateTime<Utc>,
    },
    LastTrade(LastTradeConfirmation),
}

const MAX_PENDING_SEQUENCE_GAP: usize = 1024;

impl StreamRouterHandle {
    pub fn spawn(
        settings: Settings,
        orderbooks: Arc<OrderBookState>,
        execution_tx: mpsc::UnboundedSender<ConfirmedTradeSignal>,
        health: Arc<HealthState>,
        runtime_control: Arc<RuntimeControl>,
    ) -> Self {
        let input = RingBuffer::new(settings.market_raw_ring_capacity);
        let (parsed_tx, parsed_rx) = mpsc::unbounded_channel();

        let handler = Arc::new(move |raw: RawMarketMessage| {
            let parsed_tx = parsed_tx.clone();
            Box::pin(async move {
                let sequence = raw.sequence;
                let generation = raw.generation;
                match parse_market_message(raw) {
                    Ok(parsed) => {
                        let _ = parsed_tx.send(parsed);
                    }
                    Err(error) => {
                        debug!(
                            ?error,
                            sequence, generation, "ignoring market router payload"
                        );
                        let _ = parsed_tx.send(SequencedParsedMessage {
                            sequence,
                            generation,
                            events: Vec::new(),
                        });
                    }
                }
            }) as BoxFuture<'static, ()>
        });

        spawn_worker_pool(
            "market-parser",
            settings
                .parse_tasks_market
                .max(settings.market_parser_workers),
            input.clone(),
            handler,
        );

        tokio::spawn(run_processor(
            parsed_rx,
            orderbooks,
            execution_tx,
            health,
            runtime_control,
            settings.inference_confirmation_window,
            LiquiditySweepDetector::new(&settings),
        ));

        Self { input }
    }

    pub fn input(&self) -> RingBuffer<RawMarketMessage> {
        self.input.clone()
    }
}

async fn run_processor(
    mut parsed_rx: mpsc::UnboundedReceiver<SequencedParsedMessage>,
    orderbooks: Arc<OrderBookState>,
    execution_tx: mpsc::UnboundedSender<ConfirmedTradeSignal>,
    health: Arc<HealthState>,
    runtime_control: Arc<RuntimeControl>,
    confirmation_window: std::time::Duration,
    detector: LiquiditySweepDetector,
) {
    let mut inference = TradeInferenceEngine::new(confirmation_window);
    let mut pending = BTreeMap::<u64, Vec<ParsedMarketEvent>>::new();
    let mut expected_sequence = 0_u64;
    let mut current_generation = runtime_control.current_generation();

    while let Some(parsed) = parsed_rx.recv().await {
        if parsed.generation < current_generation {
            continue;
        }

        if parsed.generation > current_generation {
            current_generation = parsed.generation;
            pending.clear();
            expected_sequence = parsed.sequence;
            inference = TradeInferenceEngine::new(confirmation_window);
        }

        pending.insert(parsed.sequence, parsed.events);
        if let Some((skipped_from, resumed_at, buffered_messages)) =
            recover_from_irrecoverable_gap(&pending, expected_sequence)
        {
            warn!(
                skipped_from,
                resumed_at,
                skipped_messages = resumed_at.saturating_sub(skipped_from),
                buffered_messages,
                "market router detected an unrecoverable sequence gap; fast-forwarding processor"
            );
            expected_sequence = resumed_at;
        }

        while let Some(events) = pending.remove(&expected_sequence) {
            for event in events {
                if let Err(error) =
                    process_event(event, &orderbooks, &detector, &mut inference, &execution_tx)
                        .await
                {
                    error!(?error, "market event processing failed");
                    health.set_last_error(format!("{error:#}")).await;
                }
            }
            expected_sequence = expected_sequence.saturating_add(1);
        }
    }
}

async fn process_event(
    event: ParsedMarketEvent,
    orderbooks: &Arc<OrderBookState>,
    detector: &LiquiditySweepDetector,
    inference: &mut TradeInferenceEngine,
    execution_tx: &mpsc::UnboundedSender<ConfirmedTradeSignal>,
) -> Result<()> {
    match event {
        ParsedMarketEvent::Book {
            book,
            received_at,
            received_at_utc,
            parse_completed_at,
            parse_completed_at_utc,
        } => {
            if let Some(delta) = orderbooks
                .apply_book_snapshot(
                    book,
                    received_at,
                    received_at_utc,
                    parse_completed_at,
                    parse_completed_at_utc,
                )
                .await?
                && let Some(signal) = detector.detect(&delta)
            {
                inference.record_probable_trade(signal, delta.observed_at);
            }
        }
        ParsedMarketEvent::PriceUpdates {
            asset_id,
            condition_id,
            updates,
            received_at,
            received_at_utc,
            parse_completed_at,
            parse_completed_at_utc,
        } => {
            if let Some(delta) = orderbooks
                .apply_price_updates(
                    &asset_id,
                    &condition_id,
                    &updates,
                    received_at,
                    received_at_utc,
                    parse_completed_at,
                    parse_completed_at_utc,
                )
                .await?
                && let Some(signal) = detector.detect(&delta)
            {
                inference.record_probable_trade(signal, delta.observed_at);
            }
        }
        ParsedMarketEvent::LastTrade(confirmation) => {
            orderbooks
                .record_last_trade_price(&confirmation.asset_id, confirmation.price)
                .await?;
            if let Some(signal) = inference.confirm(confirmation) {
                let _ = execution_tx.send(signal);
            }
        }
    }

    Ok(())
}

fn parse_market_message(raw: RawMarketMessage) -> Result<SequencedParsedMessage> {
    let parse_completed_at = Instant::now();
    let parse_completed_at_utc = Utc::now();
    let header = serde_json::from_str::<MarketMessageHeader>(&raw.payload)?;

    let events = match header.event_type.as_deref().unwrap_or_default() {
        "book" => vec![ParsedMarketEvent::Book {
            book: serde_json::from_str(&raw.payload)?,
            received_at: raw.received_at,
            received_at_utc: raw.received_at_utc,
            parse_completed_at,
            parse_completed_at_utc,
        }],
        "price_change" => parse_price_change_events(
            &raw.payload,
            raw.received_at,
            raw.received_at_utc,
            parse_completed_at,
            parse_completed_at_utc,
        )?,
        "last_trade_price" => parse_last_trade_event(
            &raw.payload,
            raw.received_at,
            raw.received_at_utc,
            parse_completed_at,
            parse_completed_at_utc,
            raw.generation,
        )
        .map(|event| vec![ParsedMarketEvent::LastTrade(event)])
        .unwrap_or_default(),
        _ => Vec::new(),
    };

    Ok(SequencedParsedMessage {
        sequence: raw.sequence,
        generation: raw.generation,
        events,
    })
}

fn recover_from_irrecoverable_gap(
    pending: &BTreeMap<u64, Vec<ParsedMarketEvent>>,
    expected_sequence: u64,
) -> Option<(u64, u64, usize)> {
    let (&lowest_pending_sequence, _) = pending.first_key_value()?;
    if lowest_pending_sequence <= expected_sequence || pending.len() < MAX_PENDING_SEQUENCE_GAP {
        return None;
    }

    Some((expected_sequence, lowest_pending_sequence, pending.len()))
}

fn parse_price_change_events(
    payload: &str,
    received_at: Instant,
    received_at_utc: DateTime<Utc>,
    parse_completed_at: Instant,
    parse_completed_at_utc: DateTime<Utc>,
) -> Result<Vec<ParsedMarketEvent>> {
    let message = serde_json::from_str::<PriceChangeMessage>(payload)?;
    let mut grouped: HashMap<String, Vec<PriceLevelUpdate>> = HashMap::new();

    for change in message.price_changes {
        grouped
            .entry(change.asset_id.clone())
            .or_default()
            .push(PriceLevelUpdate {
                side: if change.side.eq_ignore_ascii_case("BUY") {
                    BookSide::Bid
                } else {
                    BookSide::Ask
                },
                price: parse_f64(&change.price)?,
                size: parse_f64(&change.size)?,
            });
    }

    Ok(grouped
        .into_iter()
        .map(|(asset_id, updates)| ParsedMarketEvent::PriceUpdates {
            asset_id,
            condition_id: message.market.clone(),
            updates,
            received_at,
            received_at_utc,
            parse_completed_at,
            parse_completed_at_utc,
        })
        .collect())
}

fn parse_last_trade_event(
    payload: &str,
    received_at: Instant,
    received_at_utc: DateTime<Utc>,
    parse_completed_at: Instant,
    parse_completed_at_utc: DateTime<Utc>,
    generation: u64,
) -> Option<LastTradeConfirmation> {
    let message = serde_json::from_str::<LastTradeMessage>(payload).ok()?;

    Some(LastTradeConfirmation {
        asset_id: message.asset_id,
        condition_id: message.condition_id,
        transaction_hash: message.transaction_hash,
        price: message.price,
        side: message.side,
        size: message.size,
        observed_at: received_at,
        observed_at_utc: received_at_utc,
        parse_completed_at,
        parse_completed_at_utc,
        generation,
    })
}

fn parse_f64(raw: &str) -> Result<f64> {
    raw.parse::<f64>().map_err(Into::into)
}

#[derive(Debug, Deserialize)]
struct MarketMessageHeader {
    event_type: Option<String>,
}

#[derive(Debug, Deserialize)]
struct PriceChangeMessage {
    market: String,
    #[serde(rename = "price_changes")]
    price_changes: Vec<PriceChangeLevel>,
}

#[derive(Debug, Deserialize)]
struct PriceChangeLevel {
    #[serde(rename = "asset_id")]
    asset_id: String,
    price: String,
    side: String,
    size: String,
}

#[derive(Debug, Deserialize)]
struct LastTradeMessage {
    asset_id: String,
    #[serde(alias = "market", alias = "condition_id")]
    condition_id: Option<String>,
    #[serde(
        alias = "price",
        alias = "last_trade_price",
        deserialize_with = "deserialize_f64"
    )]
    price: f64,
    #[serde(default, deserialize_with = "deserialize_optional_execution_side")]
    side: Option<ExecutionSide>,
    #[serde(default, deserialize_with = "deserialize_optional_f64")]
    size: Option<f64>,
    #[serde(
        default,
        alias = "transaction_hash",
        alias = "tx_hash",
        alias = "hash",
        alias = "trade_hash",
        alias = "taker_transaction_hash"
    )]
    transaction_hash: Option<String>,
}

fn deserialize_f64<'de, D>(deserializer: D) -> std::result::Result<f64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct NumberVisitor;

    impl<'de> serde::de::Visitor<'de> for NumberVisitor {
        type Value = f64;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("a string or number")
        }

        fn visit_f64<E>(self, value: f64) -> std::result::Result<Self::Value, E> {
            Ok(value)
        }

        fn visit_i64<E>(self, value: i64) -> std::result::Result<Self::Value, E> {
            Ok(value as f64)
        }

        fn visit_u64<E>(self, value: u64) -> std::result::Result<Self::Value, E> {
            Ok(value as f64)
        }

        fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            value.parse::<f64>().map_err(E::custom)
        }
    }

    deserializer.deserialize_any(NumberVisitor)
}

fn deserialize_optional_f64<'de, D>(deserializer: D) -> std::result::Result<Option<f64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Ok(Some(deserialize_f64(deserializer)?))
}

fn deserialize_optional_execution_side<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<ExecutionSide>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let raw = Option::<String>::deserialize(deserializer)?;
    Ok(raw.and_then(|raw| {
        if raw.eq_ignore_ascii_case("BUY") {
            Some(ExecutionSide::Buy)
        } else if raw.eq_ignore_ascii_case("SELL") {
            Some(ExecutionSide::Sell)
        } else {
            None
        }
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::websocket::market_stream::RawMarketMessage;

    #[test]
    fn preserves_sequence_for_ignored_payloads() {
        let parsed = parse_market_message(RawMarketMessage {
            sequence: 7,
            generation: 3,
            received_at: Instant::now(),
            received_at_utc: Utc::now(),
            payload: r#"{"status":"ok"}"#.into(),
        })
        .expect("parse");

        assert_eq!(parsed.sequence, 7);
        assert_eq!(parsed.generation, 3);
        assert!(parsed.events.is_empty());
    }

    #[test]
    fn recovers_when_pending_gap_can_no_longer_close() {
        let pending = (10_u64..(10 + MAX_PENDING_SEQUENCE_GAP as u64))
            .map(|sequence| (sequence, Vec::new()))
            .collect::<BTreeMap<_, _>>();

        let recovery = recover_from_irrecoverable_gap(&pending, 4).expect("recovery");

        assert_eq!(recovery.0, 4);
        assert_eq!(recovery.1, 10);
    }

    #[test]
    fn parses_last_trade_event_with_side_and_size() {
        let parsed = parse_market_message(RawMarketMessage {
            sequence: 9,
            generation: 2,
            received_at: Instant::now(),
            received_at_utc: Utc::now(),
            payload: r#"{"event_type":"last_trade_price","market":"condition-1","asset_id":"asset-1","price":"0.61","side":"SELL","size":"42.5","tx_hash":"0xtrade"}"#.into(),
        })
        .expect("parse");

        assert_eq!(parsed.events.len(), 1);
        let ParsedMarketEvent::LastTrade(event) = &parsed.events[0] else {
            panic!("expected last trade event");
        };
        assert_eq!(event.asset_id, "asset-1");
        assert_eq!(event.condition_id.as_deref(), Some("condition-1"));
        assert_eq!(event.price, 0.61);
        assert_eq!(event.side, Some(ExecutionSide::Sell));
        assert_eq!(event.size, Some(42.5));
        assert_eq!(event.transaction_hash.as_deref(), Some("0xtrade"));
    }
}
