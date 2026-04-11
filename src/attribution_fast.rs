use std::collections::VecDeque;

use chrono::Utc;

use crate::config::Settings;
use crate::detection::trade_inference::ConfirmedTradeSignal;
use crate::orderbook::orderbook_state::AssetCatalog;
use crate::position_registry::PositionRegistry;
use crate::wallet::wallet_matching::ActivityTradeEvent;
use crate::wallet::wallet_matching::{
    ActivityMatch, MarketSignalValidation, MatchWindow, MatchedTrackedTrade, TradeCorrelationKind,
    build_wallet_triggered_trade,
};
use crate::wallet_registry::WalletRegistry;

#[derive(Clone)]
struct RecentSignal {
    signal: ConfirmedTradeSignal,
    confirmed_at_ms: i64,
}

pub struct FastAttributionIndex {
    capacity: usize,
    correlation_window_ms: i64,
    recent_signals: VecDeque<RecentSignal>,
}

impl FastAttributionIndex {
    pub fn new(settings: &Settings) -> Self {
        Self {
            capacity: settings.attribution_fast_cache_capacity.max(1),
            correlation_window_ms: settings.activity_correlation_window.as_millis() as i64,
            recent_signals: VecDeque::new(),
        }
    }

    pub fn record_signal(&mut self, signal: &ConfirmedTradeSignal) {
        self.recent_signals.push_front(RecentSignal {
            signal: signal.clone(),
            confirmed_at_ms: signal.confirmed_at.timestamp_millis(),
        });
        while self.recent_signals.len() > self.capacity {
            self.recent_signals.pop_back();
        }
    }

    pub fn resolve_direct_trade(
        &self,
        event: &ActivityTradeEvent,
        catalog: &AssetCatalog,
        wallet_registry: &WalletRegistry,
        position_registry: &PositionRegistry,
    ) -> Option<MatchedTrackedTrade> {
        let tracked_wallet = event
            .wallet_candidates
            .iter()
            .filter_map(|wallet| {
                let owns_market =
                    position_registry.has_open_position_for_wallet_market(wallet, &event.market_id);
                let meta = wallet_registry.get_wallet_meta(wallet);
                (owns_market || meta.as_ref().is_some_and(|meta| meta.active)).then(|| {
                    (
                        wallet,
                        owns_market,
                        meta.map(|meta| meta.score).unwrap_or_default(),
                    )
                })
            })
            .max_by(|left, right| {
                left.1
                    .cmp(&right.1)
                    .then_with(|| left.2.total_cmp(&right.2))
            })?
            .0;

        let mut event = event.clone();
        event.wallet = tracked_wallet.clone();

        let validation = if event.asset_id.is_some() && event.side.is_some() {
            None
        } else {
            self.recent_signals.iter().find_map(|candidate| {
                validation_for_event(candidate, &event, self.correlation_window_ms)
            })
        };

        let mut matched_trade = build_wallet_triggered_trade(&event, catalog, validation.as_ref())?;
        let now = Utc::now();
        matched_trade
            .signal
            .stage_timestamps
            .attribution_completed_at = Some(std::time::Instant::now());
        matched_trade
            .signal
            .stage_timestamps
            .attribution_completed_at_utc = Some(now);
        Some(matched_trade)
    }
}

fn validation_for_event(
    recent_signal: &RecentSignal,
    event: &ActivityTradeEvent,
    correlation_window_ms: i64,
) -> Option<MarketSignalValidation> {
    if recent_signal.signal.generation != event.generation
        || recent_signal.signal.condition_id != event.market_id
    {
        return None;
    }

    if let Some(side) = event.side.as_deref() {
        let signal_side = match recent_signal.signal.side {
            crate::execution::ExecutionSide::Buy => "BUY",
            crate::execution::ExecutionSide::Sell => "SELL",
        };
        if !side.eq_ignore_ascii_case(signal_side) {
            return None;
        }
    }

    if (event.timestamp_ms - recent_signal.confirmed_at_ms).abs() > correlation_window_ms
        && (event.timestamp_ms
            - recent_signal
                .signal
                .stage_timestamps
                .detection_triggered_at_utc
                .timestamp_millis())
        .abs()
            > correlation_window_ms
    {
        return None;
    }

    Some(MarketSignalValidation {
        signal: recent_signal.signal.clone(),
        activity_match: ActivityMatch {
            correlation_kind: TradeCorrelationKind::Direct,
            match_window: MatchWindow::Fallback,
            tx_hash_matched: false,
        },
    })
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use chrono::Utc;

    use super::FastAttributionIndex;
    use crate::config::Settings;
    use crate::detection::trade_inference::ConfirmedTradeSignal;
    use crate::execution::ExecutionSide;
    use crate::orderbook::orderbook_state::{AssetCatalog, AssetMetadata};
    use crate::position_registry::PositionRegistry;
    use crate::wallet::wallet_matching::{ActivitySource, ActivityTradeEvent};
    use crate::wallet_registry::WalletRegistry;

    fn sample_settings(data_dir: std::path::PathBuf) -> Settings {
        let mut settings = Settings::default_for_tests(data_dir);
        settings.polymarket_host = "https://example.com".to_owned();
        settings.polymarket_data_api = "https://example.com".to_owned();
        settings.polymarket_gamma_api = "https://example.com".to_owned();
        settings.polymarket_market_ws = "wss://example.com".to_owned();
        settings.polymarket_user_ws = "wss://example.com".to_owned();
        settings.polymarket_activity_ws = "wss://example.com".to_owned();
        settings.target_profile_addresses = vec!["0xsource".to_owned()];
        settings.hot_path_queue_capacity = 8;
        settings.cold_path_queue_capacity = 64;
        settings.attribution_fast_cache_capacity = 32;
        settings.market_subscription_batch_size = 1;
        settings.market_subscription_delay = Duration::from_millis(1);
        settings.wallet_subscription_batch_size = 1;
        settings.wallet_subscription_delay = Duration::from_millis(1);
        settings.min_top_of_book_ratio = rust_decimal_macros::dec!(1);
        settings.prediction_validation_timeout = Duration::from_millis(500);
        settings
    }

    #[test]
    fn resolves_direct_tracked_trade_from_recent_signal_when_event_is_missing_asset() {
        let data_dir = std::env::temp_dir().join("copytrade-fast-attribution-test");
        let settings = sample_settings(data_dir.clone());
        let wallet_registry = WalletRegistry::load(&settings).expect("wallet registry");
        let position_registry = PositionRegistry::load(&settings).expect("position registry");
        let catalog = AssetCatalog::new(vec![AssetMetadata {
            asset_id: "asset-1".to_owned(),
            condition_id: "condition-1".to_owned(),
            title: "Market".to_owned(),
            slug: "market".to_owned(),
            event_slug: "event".to_owned(),
            outcome: "YES".to_owned(),
            outcome_index: 0,
        }]);
        let mut index = FastAttributionIndex::new(&settings);
        let now = Utc::now();
        index.record_signal(&ConfirmedTradeSignal {
            asset_id: "asset-1".to_owned(),
            condition_id: "condition-1".to_owned(),
            transaction_hash: None,
            side: ExecutionSide::Buy,
            price: 0.55,
            estimated_size: 12.0,
            stage_timestamps: crate::models::TradeStageTimestamps {
                websocket_event_received_at: Instant::now(),
                websocket_event_received_at_utc: now,
                parse_completed_at: Instant::now(),
                parse_completed_at_utc: now,
                detection_triggered_at: Instant::now(),
                detection_triggered_at_utc: now,
                attribution_completed_at: None,
                attribution_completed_at_utc: None,
                fast_risk_completed_at: None,
                fast_risk_completed_at_utc: None,
            },
            confirmed_at: now,
            generation: 1,
        });

        let trade = index
            .resolve_direct_trade(
                &ActivityTradeEvent {
                    event_id: "event-1".to_owned(),
                    wallet: "0xsource".to_owned(),
                    wallet_candidates: vec!["0xsource".to_owned()],
                    market_id: "condition-1".to_owned(),
                    price: 0.55,
                    size: 12.0,
                    timestamp_ms: now.timestamp_millis(),
                    transaction_hash: "0xhash".to_owned(),
                    asset_id: None,
                    side: Some("BUY".to_owned()),
                    generation: 1,
                    source: ActivitySource::ActivityStream,
                    observed_at: Instant::now(),
                    observed_at_utc: now,
                    parse_completed_at: Instant::now(),
                    parse_completed_at_utc: now,
                },
                &catalog,
                &wallet_registry,
                &position_registry,
            )
            .expect("fast trade");

        assert_eq!(trade.entry.asset, "asset-1");
        assert_eq!(trade.entry.proxy_wallet, "0xsource");
        assert!(
            trade
                .signal
                .stage_timestamps
                .attribution_completed_at_utc
                .is_some()
        );
    }
}
