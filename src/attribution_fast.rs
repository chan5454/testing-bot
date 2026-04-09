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
        let tracked_wallet = event.wallet_candidates.iter().find(|wallet| {
            wallet_registry.is_tracked(wallet)
                || position_registry.has_open_position_for_wallet_market(wallet, &event.market_id)
        })?;

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
        Settings {
            execution_mode: crate::config::ExecutionMode::Paper,
            polymarket_host: "https://example.com".to_owned(),
            polymarket_data_api: "https://example.com".to_owned(),
            polymarket_gamma_api: "https://example.com".to_owned(),
            polymarket_market_ws: "wss://example.com".to_owned(),
            polymarket_user_ws: "wss://example.com".to_owned(),
            polymarket_activity_ws: "wss://example.com".to_owned(),
            polymarket_chain_id: 137,
            polymarket_signature_type: 0,
            polymarket_private_key: None,
            polymarket_funder_address: None,
            polymarket_profile_address: None,
            polygon_rpc_url: "https://polygon-rpc.com".to_owned(),
            polygon_rpc_fallback_urls: Vec::new(),
            rpc_latency_threshold: Duration::from_millis(300),
            rpc_confirmation_timeout: Duration::from_secs(10),
            min_required_matic: rust_decimal_macros::dec!(0.1),
            min_required_usdc: rust_decimal_macros::dec!(25),
            polymarket_usdc_address: "0x1".to_owned(),
            polymarket_spender_address: "0x2".to_owned(),
            auto_approve_usdc_allowance: false,
            usdc_approval_amount: rust_decimal_macros::dec!(1000),
            target_activity_ws_api_key: None,
            target_activity_ws_secret: None,
            target_activity_ws_passphrase: None,
            target_profile_addresses: vec!["0xsource".to_owned()],
            start_capital_usd: rust_decimal_macros::dec!(200),
            paper_execution_delay: Duration::ZERO,
            copy_only_new_trades: true,
            source_trades_limit: 50,
            http_timeout: Duration::from_secs(2),
            market_cache_ttl: Duration::from_secs(10),
            market_raw_ring_capacity: 128,
            market_parser_workers: 1,
            market_subscription_batch_size: 1,
            market_subscription_delay: Duration::from_millis(1),
            wallet_ring_capacity: 128,
            wallet_parser_workers: 1,
            wallet_subscription_batch_size: 1,
            wallet_subscription_delay: Duration::from_millis(1),
            hot_path_mode: true,
            hot_path_queue_capacity: 8,
            cold_path_queue_capacity: 64,
            attribution_fast_cache_capacity: 32,
            persistence_flush_interval: Duration::from_millis(250),
            analytics_flush_interval: Duration::from_millis(500),
            telegram_async_only: true,
            fast_risk_only_on_hot_path: true,
            exit_priority_strict: true,
            parse_tasks_market: 1,
            parse_tasks_wallet: 1,
            liquidity_sweep_threshold: rust_decimal_macros::dec!(1),
            imbalance_threshold: rust_decimal_macros::dec!(2),
            delta_price_move_bps: 40,
            delta_size_drop_ratio: rust_decimal_macros::dec!(0.25),
            delta_min_size_drop: rust_decimal_macros::dec!(1),
            inference_confirmation_window: Duration::from_millis(400),
            activity_stream_enabled: true,
            activity_match_window: Duration::from_millis(200),
            activity_price_tolerance: rust_decimal_macros::dec!(0.01),
            activity_size_tolerance_ratio: rust_decimal_macros::dec!(0.5),
            activity_cache_ttl: Duration::from_millis(1500),
            fallback_market_request_interval: Duration::from_millis(1000),
            fallback_global_requests_per_minute: 30,
            activity_correlation_window: Duration::from_millis(400),
            attribution_lookback: Duration::from_millis(2500),
            attribution_trades_limit: 100,
            copy_scale_above_five_usd: rust_decimal_macros::dec!(0.25),
            min_copy_notional_usd: rust_decimal_macros::dec!(1),
            max_copy_notional_usd: rust_decimal_macros::dec!(25),
            max_total_exposure_usd: rust_decimal_macros::dec!(150),
            max_market_exposure_usd: rust_decimal_macros::dec!(40),
            min_source_trade_usdc: rust_decimal_macros::dec!(1),
            max_market_spread_bps: 500,
            enable_ultra_short_markets: false,
            min_visible_liquidity: rust_decimal_macros::dec!(50),
            max_spread_bps: 500,
            max_entry_slippage: rust_decimal_macros::dec!(0.03),
            min_top_of_book_ratio: rust_decimal_macros::dec!(1),
            max_slippage_bps: 300,
            max_source_price_slippage_bps: 200,
            latency_fail_safe_enabled: true,
            max_latency: Duration::from_millis(500),
            average_latency_threshold: Duration::from_millis(350),
            latency_monitor_interval: Duration::from_millis(50),
            latency_reconnect_settle: Duration::from_millis(750),
            prediction_validation_timeout: Duration::from_millis(500),
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
            min_edge_threshold: rust_decimal_macros::dec!(0.05),
            max_copy_delay_ms: 1_500,
            min_liquidity: rust_decimal_macros::dec!(50),
            min_wallet_score: rust_decimal_macros::dec!(0.6),
            min_wallet_avg_hold_ms: 15_000,
            max_wallet_trades_per_min: 6,
            market_cooldown: Duration::from_secs(15),
            min_trade_quality_score: rust_decimal_macros::dec!(0.65),
            max_position_age_hours: 6,
            max_hold_time_seconds: 1_800,
            enable_exit_retry: true,
            exit_retry_window: Duration::from_secs(30),
            exit_retry_interval: Duration::from_millis(500),
            unresolved_exit_initial_retry: Duration::from_millis(250),
            unresolved_exit_total_window: Duration::from_secs(30),
            unresolved_exit_max_retry: Duration::from_secs(4),
            position_pending_open_ttl: Duration::from_secs(20),
            rpc_global_rate_limit_per_second: 10,
            rpc_per_market_rate_limit_per_second: 3,
            closing_max_age: Duration::from_secs(30),
            force_exit_on_closing_timeout: true,
            telegram_bot_token: "token".to_owned(),
            telegram_chat_id: "chat".to_owned(),
            health_port: 3000,
            data_dir,
        }
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
