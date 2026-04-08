use chrono::Utc;

use crate::config::Settings;
use crate::execution::ExecutionSide;
use crate::models::TradeStageTimestamps;
use crate::orderbook::orderbook_state::OrderBookDelta;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SweepRule {
    BestLevelSweep,
    MultiLevelSweep,
    LargeLiquidityRemoval,
    PriceTickChange,
    ImbalanceSpike,
}

#[derive(Clone, Debug)]
pub struct ProbableTradeEvent {
    pub asset_id: String,
    pub condition_id: String,
    pub side: ExecutionSide,
    pub estimated_size: f64,
    pub stage_timestamps: TradeStageTimestamps,
    pub score: u32,
}

#[derive(Clone)]
pub struct LiquiditySweepDetector {
    liquidity_sweep_threshold: f64,
    price_move_bps: f64,
    size_drop_ratio: f64,
    min_size_drop: f64,
    imbalance_threshold: f64,
}

impl LiquiditySweepDetector {
    pub fn new(settings: &Settings) -> Self {
        Self {
            liquidity_sweep_threshold: settings
                .liquidity_sweep_threshold
                .to_string()
                .parse::<f64>()
                .unwrap_or(0.0),
            price_move_bps: settings.delta_price_move_bps as f64,
            size_drop_ratio: settings
                .delta_size_drop_ratio
                .to_string()
                .parse::<f64>()
                .unwrap_or(0.25),
            min_size_drop: settings
                .delta_min_size_drop
                .to_string()
                .parse::<f64>()
                .unwrap_or(1.0),
            imbalance_threshold: settings
                .imbalance_threshold
                .to_string()
                .parse::<f64>()
                .unwrap_or(2.0),
        }
    }

    pub fn detect(&self, delta: &OrderBookDelta) -> Option<ProbableTradeEvent> {
        let buy = self.detect_buy(delta);
        let sell = self.detect_sell(delta);

        match (buy, sell) {
            (Some(left), Some(right)) => {
                if left.score >= right.score {
                    Some(left)
                } else {
                    Some(right)
                }
            }
            (Some(event), None) | (None, Some(event)) => Some(event),
            (None, None) => None,
        }
    }

    fn detect_buy(&self, delta: &OrderBookDelta) -> Option<ProbableTradeEvent> {
        let mut rules = Vec::new();
        let previous_best_ask = delta.previous.asks[0];
        let current_best_ask = delta.current.asks[0];
        let removed_ratio = removal_ratio(delta.best_ask_removed, previous_best_ask.size);
        let removal_large_enough = delta.removed_ask_liquidity >= self.min_size_drop;

        if previous_best_ask.size > 0.0
            && removal_large_enough
            && delta.best_ask_removed >= previous_best_ask.size * 0.99
            && current_best_ask.price > previous_best_ask.price
        {
            rules.push(SweepRule::BestLevelSweep);
        }
        if previous_best_ask.size > 0.0
            && delta.previous.asks[1].size > 0.0
            && removal_large_enough
            && delta.best_ask_removed + delta.second_ask_removed
                >= (delta.previous.asks[0].size + delta.previous.asks[1].size) * 0.99
        {
            rules.push(SweepRule::MultiLevelSweep);
        }
        if removal_large_enough
            && (delta.removed_ask_liquidity >= self.liquidity_sweep_threshold
                || removed_ratio >= self.size_drop_ratio)
        {
            rules.push(SweepRule::LargeLiquidityRemoval);
        }
        if removal_large_enough
            && moved_up(
                delta.previous_mid_price,
                delta.current_mid_price,
                self.price_move_bps,
            )
        {
            rules.push(SweepRule::PriceTickChange);
        }
        if removal_large_enough
            && delta
                .current_imbalance
                .zip(delta.previous.imbalance())
                .map(|(current, previous)| {
                    current >= self.imbalance_threshold && previous < self.imbalance_threshold
                })
                .unwrap_or(false)
        {
            rules.push(SweepRule::ImbalanceSpike);
        }

        if rules.is_empty() {
            return None;
        }

        Some(ProbableTradeEvent {
            asset_id: delta.metadata.asset_id.clone(),
            condition_id: delta.metadata.condition_id.clone(),
            side: ExecutionSide::Buy,
            estimated_size: delta.removed_ask_liquidity.max(delta.best_ask_removed),
            stage_timestamps: TradeStageTimestamps {
                websocket_event_received_at: delta.event_received_at,
                websocket_event_received_at_utc: delta.event_received_at_utc,
                parse_completed_at: delta.parse_completed_at,
                parse_completed_at_utc: delta.parse_completed_at_utc,
                detection_triggered_at: std::time::Instant::now(),
                detection_triggered_at_utc: Utc::now(),
                attribution_completed_at: None,
                attribution_completed_at_utc: None,
                fast_risk_completed_at: None,
                fast_risk_completed_at_utc: None,
            },
            score: rules.len() as u32,
        })
    }

    fn detect_sell(&self, delta: &OrderBookDelta) -> Option<ProbableTradeEvent> {
        let mut rules = Vec::new();
        let previous_best_bid = delta.previous.bids[0];
        let current_best_bid = delta.current.bids[0];
        let removed_ratio = removal_ratio(delta.best_bid_removed, previous_best_bid.size);
        let removal_large_enough = delta.removed_bid_liquidity >= self.min_size_drop;

        if previous_best_bid.size > 0.0
            && removal_large_enough
            && delta.best_bid_removed >= previous_best_bid.size * 0.99
            && current_best_bid.price < previous_best_bid.price
        {
            rules.push(SweepRule::BestLevelSweep);
        }
        if previous_best_bid.size > 0.0
            && delta.previous.bids[1].size > 0.0
            && removal_large_enough
            && delta.best_bid_removed + delta.second_bid_removed
                >= (delta.previous.bids[0].size + delta.previous.bids[1].size) * 0.99
        {
            rules.push(SweepRule::MultiLevelSweep);
        }
        if removal_large_enough
            && (delta.removed_bid_liquidity >= self.liquidity_sweep_threshold
                || removed_ratio >= self.size_drop_ratio)
        {
            rules.push(SweepRule::LargeLiquidityRemoval);
        }
        if removal_large_enough
            && moved_down(
                delta.previous_mid_price,
                delta.current_mid_price,
                self.price_move_bps,
            )
        {
            rules.push(SweepRule::PriceTickChange);
        }
        if removal_large_enough
            && delta
                .current_imbalance
                .zip(delta.previous.imbalance())
                .map(|(current, previous)| {
                    current <= 1.0 / self.imbalance_threshold.max(1.0)
                        && previous > 1.0 / self.imbalance_threshold.max(1.0)
                })
                .unwrap_or(false)
        {
            rules.push(SweepRule::ImbalanceSpike);
        }

        if rules.is_empty() {
            return None;
        }

        Some(ProbableTradeEvent {
            asset_id: delta.metadata.asset_id.clone(),
            condition_id: delta.metadata.condition_id.clone(),
            side: ExecutionSide::Sell,
            estimated_size: delta.removed_bid_liquidity.max(delta.best_bid_removed),
            stage_timestamps: TradeStageTimestamps {
                websocket_event_received_at: delta.event_received_at,
                websocket_event_received_at_utc: delta.event_received_at_utc,
                parse_completed_at: delta.parse_completed_at,
                parse_completed_at_utc: delta.parse_completed_at_utc,
                detection_triggered_at: std::time::Instant::now(),
                detection_triggered_at_utc: Utc::now(),
                attribution_completed_at: None,
                attribution_completed_at_utc: None,
                fast_risk_completed_at: None,
                fast_risk_completed_at_utc: None,
            },
            score: rules.len() as u32,
        })
    }
}

fn removal_ratio(removed: f64, previous_size: f64) -> f64 {
    if previous_size <= 0.0 {
        return 0.0;
    }
    removed / previous_size
}

fn moved_up(previous: Option<f64>, current: Option<f64>, threshold_bps: f64) -> bool {
    moved(previous, current, threshold_bps, true)
}

fn moved_down(previous: Option<f64>, current: Option<f64>, threshold_bps: f64) -> bool {
    moved(previous, current, threshold_bps, false)
}

fn moved(previous: Option<f64>, current: Option<f64>, threshold_bps: f64, up: bool) -> bool {
    let (Some(previous), Some(current)) = (previous, current) else {
        return false;
    };
    if previous <= 0.0 || current <= 0.0 {
        return false;
    }
    let delta = (current - previous) / previous * 10_000.0;
    if up {
        delta >= threshold_bps
    } else {
        delta <= -threshold_bps
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::orderbook::orderbook_levels::{OrderBook, OrderBookLevel};
    use crate::orderbook::orderbook_state::{AssetMetadata, OrderBookDelta};

    #[test]
    fn detects_best_level_buy_sweep() {
        let detector = LiquiditySweepDetector {
            liquidity_sweep_threshold: 10.0,
            price_move_bps: 10.0,
            size_drop_ratio: 0.25,
            min_size_drop: 1.0,
            imbalance_threshold: 2.0,
        };
        let delta = sample_delta(
            OrderBook {
                bids: [level(0.48, 50.0), empty(), empty()],
                asks: [level(0.52, 20.0), level(0.53, 10.0), empty()],
            },
            OrderBook {
                bids: [level(0.48, 50.0), empty(), empty()],
                asks: [level(0.53, 10.0), empty(), empty()],
            },
            0.0,
            20.0,
        );

        let signal = detector.detect(&delta).expect("signal");
        assert_eq!(signal.side, ExecutionSide::Buy);
    }

    fn sample_delta(
        previous: OrderBook,
        current: OrderBook,
        removed_bid: f64,
        removed_ask: f64,
    ) -> OrderBookDelta {
        OrderBookDelta {
            metadata: AssetMetadata {
                asset_id: "asset-1".to_owned(),
                condition_id: "condition-1".to_owned(),
                title: "Market".to_owned(),
                slug: "market".to_owned(),
                event_slug: "event".to_owned(),
                outcome: "YES".to_owned(),
                outcome_index: 0,
            },
            previous,
            current,
            best_bid_removed: removed_bid,
            second_bid_removed: 0.0,
            best_ask_removed: removed_ask,
            second_ask_removed: 0.0,
            removed_bid_liquidity: removed_bid,
            removed_ask_liquidity: removed_ask,
            previous_mid_price: previous.mid_price(),
            current_mid_price: current.mid_price(),
            current_imbalance: current.imbalance(),
            event_received_at: std::time::Instant::now(),
            event_received_at_utc: Utc::now(),
            parse_completed_at: std::time::Instant::now(),
            parse_completed_at_utc: Utc::now(),
            observed_at: std::time::Instant::now(),
        }
    }

    fn level(price: f64, size: f64) -> OrderBookLevel {
        OrderBookLevel { price, size }
    }

    fn empty() -> OrderBookLevel {
        OrderBookLevel {
            price: 0.0,
            size: 0.0,
        }
    }
}
