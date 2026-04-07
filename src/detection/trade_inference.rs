use std::collections::HashMap;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};

use crate::detection::liquidity_sweep_detector::ProbableTradeEvent;
use crate::execution::ExecutionSide;
use crate::models::TradeStageTimestamps;

#[derive(Clone, Debug)]
pub struct LastTradeConfirmation {
    pub asset_id: String,
    pub condition_id: Option<String>,
    pub transaction_hash: Option<String>,
    pub price: f64,
    pub side: Option<ExecutionSide>,
    pub size: Option<f64>,
    pub observed_at: Instant,
    pub observed_at_utc: DateTime<Utc>,
    pub generation: u64,
}

#[derive(Clone, Debug)]
pub struct ConfirmedTradeSignal {
    pub asset_id: String,
    pub condition_id: String,
    pub transaction_hash: Option<String>,
    pub side: ExecutionSide,
    pub price: f64,
    pub estimated_size: f64,
    pub stage_timestamps: TradeStageTimestamps,
    pub confirmed_at: DateTime<Utc>,
    pub generation: u64,
}

#[derive(Clone, Debug)]
struct PendingSignal {
    event: ProbableTradeEvent,
    expires_at: Instant,
}

pub struct TradeInferenceEngine {
    confirmation_window: Duration,
    pending: HashMap<String, PendingSignal>,
}

impl TradeInferenceEngine {
    pub fn new(confirmation_window: Duration) -> Self {
        Self {
            confirmation_window,
            pending: HashMap::new(),
        }
    }

    pub fn record_probable_trade(&mut self, event: ProbableTradeEvent, observed_at: Instant) {
        self.evict_expired(observed_at);
        let expires_at = observed_at + self.confirmation_window;
        match self.pending.get(&event.asset_id) {
            Some(existing) if existing.event.score > event.score => {}
            _ => {
                self.pending
                    .insert(event.asset_id.clone(), PendingSignal { event, expires_at });
            }
        }
    }

    pub fn confirm(&mut self, confirmation: LastTradeConfirmation) -> Option<ConfirmedTradeSignal> {
        self.evict_expired(confirmation.observed_at);
        if let Some(pending) = self.pending.remove(&confirmation.asset_id) {
            return Some(ConfirmedTradeSignal {
                asset_id: pending.event.asset_id,
                condition_id: confirmation
                    .condition_id
                    .unwrap_or(pending.event.condition_id),
                transaction_hash: confirmation.transaction_hash,
                side: confirmation.side.unwrap_or(pending.event.side),
                price: confirmation.price,
                estimated_size: confirmation
                    .size
                    .filter(|size| *size > 0.0)
                    .unwrap_or(pending.event.estimated_size),
                stage_timestamps: pending.event.stage_timestamps,
                confirmed_at: confirmation.observed_at_utc,
                generation: confirmation.generation,
            });
        }

        Some(ConfirmedTradeSignal {
            asset_id: confirmation.asset_id,
            condition_id: confirmation.condition_id?,
            transaction_hash: confirmation.transaction_hash,
            side: confirmation.side?,
            price: confirmation.price,
            estimated_size: confirmation.size.filter(|size| *size > 0.0)?,
            stage_timestamps: TradeStageTimestamps {
                websocket_event_received_at: confirmation.observed_at,
                websocket_event_received_at_utc: confirmation.observed_at_utc,
                detection_triggered_at: confirmation.observed_at,
                detection_triggered_at_utc: confirmation.observed_at_utc,
            },
            confirmed_at: confirmation.observed_at_utc,
            generation: confirmation.generation,
        })
    }

    fn evict_expired(&mut self, now: Instant) {
        self.pending.retain(|_, pending| pending.expires_at > now);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::TradeStageTimestamps;

    #[test]
    fn confirms_pending_signal_for_matching_asset() {
        let mut engine = TradeInferenceEngine::new(Duration::from_millis(400));
        let observed_at = Instant::now();
        engine.record_probable_trade(
            ProbableTradeEvent {
                asset_id: "asset-1".to_owned(),
                condition_id: "condition-1".to_owned(),
                side: ExecutionSide::Buy,
                estimated_size: 10.0,
                stage_timestamps: TradeStageTimestamps {
                    websocket_event_received_at: observed_at,
                    websocket_event_received_at_utc: Utc::now(),
                    detection_triggered_at: observed_at,
                    detection_triggered_at_utc: Utc::now(),
                },
                score: 3,
            },
            observed_at,
        );

        let confirmed = engine.confirm(LastTradeConfirmation {
            asset_id: "asset-1".to_owned(),
            condition_id: Some("condition-1".to_owned()),
            transaction_hash: Some("0xtrade".to_owned()),
            price: 0.51,
            side: Some(ExecutionSide::Buy),
            size: Some(12.0),
            observed_at,
            observed_at_utc: Utc::now(),
            generation: 1,
        });

        assert!(confirmed.is_some());
        let signal = confirmed.expect("signal");
        assert_eq!(signal.price, 0.51);
        assert_eq!(signal.transaction_hash.as_deref(), Some("0xtrade"));
    }

    #[test]
    fn emits_direct_signal_from_last_trade_without_pending_delta() {
        let mut engine = TradeInferenceEngine::new(Duration::from_millis(400));
        let observed_at = Instant::now();
        let observed_at_utc = Utc::now();

        let confirmed = engine.confirm(LastTradeConfirmation {
            asset_id: "asset-1".to_owned(),
            condition_id: Some("condition-1".to_owned()),
            transaction_hash: Some("0xdirect".to_owned()),
            price: 0.61,
            side: Some(ExecutionSide::Sell),
            size: Some(42.0),
            observed_at,
            observed_at_utc,
            generation: 7,
        });

        let signal = confirmed.expect("direct signal");
        assert_eq!(signal.asset_id, "asset-1");
        assert_eq!(signal.condition_id, "condition-1");
        assert_eq!(signal.side, ExecutionSide::Sell);
        assert_eq!(signal.price, 0.61);
        assert_eq!(signal.estimated_size, 42.0);
        assert_eq!(signal.transaction_hash.as_deref(), Some("0xdirect"));
        assert_eq!(signal.confirmed_at, observed_at_utc);
        assert_eq!(signal.generation, 7);
        assert_eq!(
            signal.stage_timestamps.detection_triggered_at_utc,
            observed_at_utc
        );
    }
}
