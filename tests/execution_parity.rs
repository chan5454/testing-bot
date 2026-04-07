use std::path::PathBuf;
use std::time::Duration;

use chrono::Utc;
use polymarket_copy_bot::config::{ExecutionMode, Settings};
use polymarket_copy_bot::execution::{
    ExecutionRequest, ExecutionResult, ExecutionSide, ExecutionStatus, OrderBook, simulate_fill,
};
use polymarket_copy_bot::models::{ActivityEntry, PortfolioSnapshot};
use polymarket_copy_bot::portfolio::PortfolioService;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tempfile::tempdir;

fn sample_settings(data_dir: PathBuf) -> Settings {
    Settings {
        execution_mode: ExecutionMode::Paper,
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
        target_profile_addresses: vec!["0xabc".to_owned()],
        start_capital_usd: dec!(100),
        paper_execution_delay: Duration::ZERO,
        copy_only_new_trades: true,
        source_trades_limit: 50,
        http_timeout: Duration::from_secs(2),
        market_cache_ttl: Duration::from_secs(10),
        market_raw_ring_capacity: 8192,
        market_parser_workers: 1,
        market_subscription_batch_size: 100,
        market_subscription_delay: Duration::from_millis(40),
        wallet_ring_capacity: 1024,
        wallet_parser_workers: 1,
        wallet_subscription_batch_size: 250,
        wallet_subscription_delay: Duration::from_millis(20),
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
        fallback_market_request_interval: Duration::from_millis(1000),
        fallback_global_requests_per_minute: 30,
        activity_correlation_window: Duration::from_millis(400),
        attribution_lookback: Duration::from_millis(2500),
        attribution_trades_limit: 100,
        copy_scale_above_five_usd: dec!(0.25),
        min_copy_notional_usd: dec!(1),
        max_copy_notional_usd: dec!(25),
        max_total_exposure_usd: dec!(150),
        max_market_exposure_usd: dec!(40),
        min_source_trade_usdc: dec!(0),
        max_market_spread_bps: 500,
        min_top_of_book_ratio: dec!(1.25),
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
        data_dir,
    }
}

fn sample_request() -> ExecutionRequest {
    ExecutionRequest {
        token_id: "asset-1".to_owned(),
        side: ExecutionSide::Buy,
        size: dec!(10),
        limit_price: dec!(0.5),
        requested_notional: dec!(5),
        source_trade_id: "trade-1".to_owned(),
    }
}

fn sample_entry() -> ActivityEntry {
    ActivityEntry {
        proxy_wallet: "0xsource".to_owned(),
        timestamp: 1,
        condition_id: "condition-1".to_owned(),
        type_name: "TRADE".to_owned(),
        size: 10.0,
        usdc_size: 5.0,
        transaction_hash: "0xhash".to_owned(),
        price: 0.5,
        asset: "asset-1".to_owned(),
        side: "BUY".to_owned(),
        outcome_index: 0,
        title: "Sample market".to_owned(),
        slug: "sample-market".to_owned(),
        event_slug: "sample-event".to_owned(),
        outcome: "YES".to_owned(),
    }
}

fn empty_snapshot() -> PortfolioSnapshot {
    PortfolioSnapshot {
        fetched_at: Utc::now(),
        total_value: dec!(100),
        total_exposure: Decimal::ZERO,
        cash_balance: dec!(100),
        realized_pnl: Decimal::ZERO,
        unrealized_pnl: Decimal::ZERO,
        positions: Vec::new(),
    }
}

#[test]
fn test_live_vs_paper_parity() {
    let request = sample_request();
    let mock_orderbook = OrderBook {
        best_ask: dec!(0.51),
        best_bid: dec!(0.49),
        top_level_liquidity: dec!(5),
    };

    let paper_result = simulate_fill(&request, &mock_orderbook);
    let live_result = ExecutionResult {
        mode: ExecutionMode::Live,
        order_request: request.clone(),
        order_id: "live-order".to_owned(),
        success: true,
        transaction_hashes: Vec::new(),
        filled_size: Decimal::ZERO,
        filled_price: Decimal::ZERO,
        requested_size: dec!(10),
        requested_price: dec!(0.5),
        status: ExecutionStatus::NoFill,
        filled_notional: Decimal::ZERO,
    };

    assert_eq!(paper_result.status, ExecutionStatus::NoFill);
    assert_eq!(paper_result.filled_size, Decimal::ZERO);
    assert_eq!(paper_result.status, live_result.status);
    assert_eq!(paper_result.filled_size, live_result.filled_size);
}

#[test]
fn test_zero_fill_does_not_create_position() {
    let temp = tempdir().expect("tempdir");
    let service = PortfolioService::new(sample_settings(temp.path().to_path_buf()), None);
    let request = sample_request();
    let result = ExecutionResult {
        mode: ExecutionMode::Paper,
        order_request: request.clone(),
        order_id: "paper-order".to_owned(),
        success: true,
        transaction_hashes: Vec::new(),
        filled_size: Decimal::ZERO,
        filled_price: Decimal::ZERO,
        requested_size: dec!(10),
        requested_price: dec!(0.5),
        status: ExecutionStatus::NoFill,
        filled_notional: Decimal::ZERO,
    };

    let snapshot = empty_snapshot();
    let projected = service
        .project_fill_on_snapshot(&snapshot, &sample_entry(), &result)
        .expect("projected snapshot");

    assert_eq!(projected.positions.len(), 0);
    assert_eq!(projected.cash_balance, snapshot.cash_balance);
    assert_eq!(projected.total_exposure, snapshot.total_exposure);
}
