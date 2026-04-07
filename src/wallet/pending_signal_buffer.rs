use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::config::Settings;
use crate::detection::trade_inference::ConfirmedTradeSignal;
use crate::orderbook::orderbook_state::AssetCatalog;
use crate::prediction::signal_cache_key;
use crate::wallet::wallet_matching::{
    ActivityMatchResult, ActivityTradeEvent, MarketSignalValidation, activity_match_result,
    activity_match_score, effective_fallback_match_window,
};

#[derive(Clone, Debug)]
struct PendingSignal {
    signal_key: String,
    signal: ConfirmedTradeSignal,
    expires_at: Instant,
}

pub enum PendingMatchOutcome {
    Matched(MarketSignalValidation),
    OutcomeMismatch(ConfirmedTradeSignal),
    NoMatch,
}

#[derive(Default)]
pub struct PendingSignalBuffer {
    active_signals: HashMap<String, Vec<PendingSignal>>,
    validation_signals: HashMap<String, Vec<PendingSignal>>,
    pending_signal_ttl: Duration,
    late_validation_ttl: Duration,
}

impl PendingSignalBuffer {
    pub fn new(settings: &Settings) -> Self {
        let validation_ttl = settings.effective_prediction_validation_timeout();
        Self {
            active_signals: HashMap::new(),
            validation_signals: HashMap::new(),
            pending_signal_ttl: validation_ttl,
            late_validation_ttl: effective_fallback_match_window(settings).max(validation_ttl),
        }
    }

    pub fn pending_signal_ttl(&self) -> Duration {
        self.pending_signal_ttl
    }

    pub fn late_validation_ttl(&self) -> Duration {
        self.late_validation_ttl
    }

    pub fn pending_signal_count(&self) -> usize {
        self.active_signals.values().map(Vec::len).sum()
    }

    pub fn recent_activity_market_count(&self) -> usize {
        0
    }

    pub fn recent_activity_event_count(&self) -> usize {
        0
    }

    pub fn clear(&mut self) {
        self.active_signals.clear();
        self.validation_signals.clear();
    }

    pub fn push_signal(&mut self, signal: ConfirmedTradeSignal) -> usize {
        let now = Instant::now();
        let signal_key = signal_cache_key(&signal);
        self.active_signals
            .entry(signal.condition_id.clone())
            .or_default()
            .push(PendingSignal {
                signal_key: signal_key.clone(),
                signal: signal.clone(),
                expires_at: now + self.pending_signal_ttl,
            });
        self.validation_signals
            .entry(signal.condition_id.clone())
            .or_default()
            .push(PendingSignal {
                signal_key,
                signal,
                expires_at: now + self.late_validation_ttl,
            });
        self.pending_signal_count()
    }

    pub fn match_pending_signal(
        &mut self,
        event: &ActivityTradeEvent,
        settings: &Settings,
        _catalog: &AssetCatalog,
    ) -> PendingMatchOutcome {
        self.evict_expired_validation_signals();
        self.evict_expired_active_signals();
        match self.match_bucket(&event.market_id, event, settings, true) {
            PendingMatchOutcome::Matched(validation) => PendingMatchOutcome::Matched(validation),
            PendingMatchOutcome::OutcomeMismatch(active_signal) => {
                match self.match_bucket(&event.market_id, event, settings, false) {
                    PendingMatchOutcome::NoMatch => {
                        PendingMatchOutcome::OutcomeMismatch(active_signal)
                    }
                    outcome => outcome,
                }
            }
            PendingMatchOutcome::NoMatch => {
                self.match_bucket(&event.market_id, event, settings, false)
            }
        }
    }

    fn match_bucket(
        &mut self,
        market_id: &str,
        event: &ActivityTradeEvent,
        settings: &Settings,
        active_only: bool,
    ) -> PendingMatchOutcome {
        let mut outcome_mismatch_signal = None;
        let (pending_signal, activity_match, should_remove_market) = {
            let store = if active_only {
                &mut self.active_signals
            } else {
                &mut self.validation_signals
            };
            let Some(pending) = store.get_mut(market_id) else {
                return PendingMatchOutcome::NoMatch;
            };
            let candidate = pending
                .iter()
                .enumerate()
                .filter_map(|(index, candidate)| {
                    match activity_match_result(event, &candidate.signal, settings) {
                        ActivityMatchResult::Matched(activity_match) => Some((
                            index,
                            activity_match,
                            activity_match_score(event, &candidate.signal, activity_match),
                        )),
                        ActivityMatchResult::OutcomeMismatch => {
                            outcome_mismatch_signal.get_or_insert_with(|| candidate.signal.clone());
                            None
                        }
                        ActivityMatchResult::NoMatch => None,
                    }
                })
                .min_by_key(|(_, _, score)| *score)
                .map(|(index, activity_match, _)| (index, activity_match));
            let Some((index, activity_match)) = candidate else {
                return outcome_mismatch_signal
                    .map(PendingMatchOutcome::OutcomeMismatch)
                    .unwrap_or(PendingMatchOutcome::NoMatch);
            };
            let pending_signal = pending.remove(index);
            (pending_signal, activity_match, pending.is_empty())
        };

        if should_remove_market {
            if active_only {
                self.active_signals.remove(market_id);
            } else {
                self.validation_signals.remove(market_id);
            }
        }
        self.remove_validation_signal(market_id, &pending_signal.signal_key);
        self.remove_active_signal(market_id, &pending_signal.signal_key);
        PendingMatchOutcome::Matched(MarketSignalValidation {
            signal: pending_signal.signal,
            activity_match,
        })
    }

    pub fn evict_expired_signals(&mut self) -> Vec<ConfirmedTradeSignal> {
        self.evict_expired_validation_signals();
        self.evict_expired_active_signals()
    }

    fn evict_expired_active_signals(&mut self) -> Vec<ConfirmedTradeSignal> {
        let now = Instant::now();
        let mut expired = Vec::new();
        self.active_signals.retain(|_, signals| {
            signals.retain(|pending| {
                if pending.expires_at > now {
                    return true;
                }
                expired.push(pending.signal.clone());
                false
            });
            !signals.is_empty()
        });
        expired
    }

    fn evict_expired_validation_signals(&mut self) {
        let now = Instant::now();
        self.validation_signals.retain(|_, signals| {
            signals.retain(|pending| pending.expires_at > now);
            !signals.is_empty()
        });
    }

    fn remove_active_signal(&mut self, market_id: &str, signal_key: &str) {
        remove_signal_from_store(&mut self.active_signals, market_id, signal_key);
    }

    fn remove_validation_signal(&mut self, market_id: &str, signal_key: &str) {
        remove_signal_from_store(&mut self.validation_signals, market_id, signal_key);
    }
}

fn remove_signal_from_store(
    store: &mut HashMap<String, Vec<PendingSignal>>,
    market_id: &str,
    signal_key: &str,
) {
    let Some(signals) = store.get_mut(market_id) else {
        return;
    };
    signals.retain(|pending| pending.signal_key != signal_key);
    if signals.is_empty() {
        store.remove(market_id);
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use chrono::Utc;
    use rust_decimal_macros::dec;

    use super::*;
    use crate::config::{ExecutionMode, Settings};
    use crate::execution::ExecutionSide;
    use crate::models::TradeStageTimestamps;
    use crate::orderbook::orderbook_state::{AssetCatalog, AssetMetadata};
    use crate::wallet::wallet_matching::{
        ActivitySource, ActivityTradeEvent, TradeCorrelationKind,
    };

    fn sample_settings() -> Settings {
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
            rpc_latency_threshold: Duration::from_millis(300),
            rpc_confirmation_timeout: Duration::from_secs(10),
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
            start_capital_usd: dec!(100),
            paper_execution_delay: Duration::ZERO,
            copy_only_new_trades: true,
            source_trades_limit: 50,
            http_timeout: Duration::from_secs(2),
            market_cache_ttl: Duration::from_secs(3),
            market_raw_ring_capacity: 128,
            market_parser_workers: 1,
            market_subscription_batch_size: 10,
            market_subscription_delay: Duration::ZERO,
            wallet_ring_capacity: 128,
            wallet_parser_workers: 1,
            wallet_subscription_batch_size: 10,
            wallet_subscription_delay: Duration::ZERO,
            liquidity_sweep_threshold: dec!(1),
            imbalance_threshold: dec!(2),
            delta_price_move_bps: 40,
            delta_size_drop_ratio: dec!(0.25),
            delta_min_size_drop: dec!(1),
            inference_confirmation_window: Duration::from_millis(400),
            activity_stream_enabled: true,
            activity_match_window: Duration::from_millis(200),
            activity_price_tolerance: dec!(0.01),
            activity_size_tolerance_ratio: dec!(0.5),
            activity_cache_ttl: Duration::from_millis(1500),
            fallback_market_request_interval: Duration::from_secs(1),
            fallback_global_requests_per_minute: 30,
            activity_correlation_window: Duration::from_millis(400),
            attribution_lookback: Duration::from_millis(2500),
            attribution_trades_limit: 100,
            copy_scale_above_five_usd: dec!(0.25),
            min_copy_notional_usd: dec!(1),
            max_copy_notional_usd: dec!(25),
            max_total_exposure_usd: dec!(100),
            max_market_exposure_usd: dec!(25),
            min_source_trade_usdc: dec!(0),
            max_market_spread_bps: 500,
            min_top_of_book_ratio: dec!(1),
            max_slippage_bps: 300,
            max_source_price_slippage_bps: 200,
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
            telegram_bot_token: "token".to_owned(),
            telegram_chat_id: "chat".to_owned(),
            health_port: 3000,
            data_dir: std::path::PathBuf::from("./data"),
        }
    }

    fn sample_catalog() -> AssetCatalog {
        AssetCatalog::new(vec![
            AssetMetadata {
                asset_id: "asset-yes".to_owned(),
                condition_id: "condition-1".to_owned(),
                title: "Market".to_owned(),
                slug: "market".to_owned(),
                event_slug: "event".to_owned(),
                outcome: "YES".to_owned(),
                outcome_index: 0,
            },
            AssetMetadata {
                asset_id: "asset-no".to_owned(),
                condition_id: "condition-1".to_owned(),
                title: "Market".to_owned(),
                slug: "market".to_owned(),
                event_slug: "event".to_owned(),
                outcome: "NO".to_owned(),
                outcome_index: 1,
            },
        ])
    }

    fn sample_signal() -> ConfirmedTradeSignal {
        let now = std::time::Instant::now();
        ConfirmedTradeSignal {
            asset_id: "asset-yes".to_owned(),
            condition_id: "condition-1".to_owned(),
            transaction_hash: None,
            side: ExecutionSide::Buy,
            price: 0.43,
            estimated_size: 10.0,
            stage_timestamps: TradeStageTimestamps {
                websocket_event_received_at: now,
                websocket_event_received_at_utc: Utc::now(),
                detection_triggered_at: now,
                detection_triggered_at_utc: Utc::now(),
            },
            confirmed_at: Utc::now(),
            generation: 1,
        }
    }

    fn sample_event(price: f64) -> ActivityTradeEvent {
        let observed_at = std::time::Instant::now();
        let observed_at_utc = Utc::now();
        ActivityTradeEvent {
            event_id: "event-1".to_owned(),
            wallet: "0xtarget".to_owned(),
            wallet_candidates: vec!["0xtarget".to_owned()],
            market_id: "condition-1".to_owned(),
            price,
            size: 10.0,
            timestamp_ms: Utc::now().timestamp_millis(),
            transaction_hash: "0xhash".to_owned(),
            asset_id: Some("asset-no".to_owned()),
            side: Some("BUY".to_owned()),
            generation: 1,
            source: ActivitySource::ActivityStream,
            observed_at,
            observed_at_utc,
        }
    }

    #[test]
    fn matches_pending_signal_from_event() {
        let settings = sample_settings();
        let catalog = sample_catalog();
        let signal = sample_signal();
        let event = sample_event(0.57);
        let mut buffer = PendingSignalBuffer::new(&settings);

        buffer.push_signal(signal);
        let matched = match buffer.match_pending_signal(&event, &settings, &catalog) {
            PendingMatchOutcome::Matched(matched) => matched,
            PendingMatchOutcome::OutcomeMismatch(_) => panic!("unexpected outcome mismatch"),
            PendingMatchOutcome::NoMatch => panic!("expected matched trade"),
        };

        assert_eq!(
            matched.activity_match.correlation_kind,
            TradeCorrelationKind::Complementary
        );
        assert_eq!(buffer.pending_signal_count(), 0);
    }

    #[test]
    fn active_confirmation_window_is_clamped_shorter_than_late_validation_window() {
        let settings = sample_settings();
        let buffer = PendingSignalBuffer::new(&settings);

        assert_eq!(buffer.pending_signal_ttl(), Duration::from_millis(500));
        assert!(buffer.late_validation_ttl() >= Duration::from_secs(3));
    }

    #[test]
    fn live_mode_uses_same_short_confirmation_window() {
        let mut settings = sample_settings();
        settings.execution_mode = ExecutionMode::Live;
        let buffer = PendingSignalBuffer::new(&settings);

        assert_eq!(buffer.pending_signal_ttl(), Duration::from_millis(500));
        assert!(buffer.late_validation_ttl() >= Duration::from_secs(3));
    }
}
