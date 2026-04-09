use chrono::Utc;
use polymarket_copy_bot::config::{ExecutionMode, Settings};
use polymarket_copy_bot::execution::{
    ExecutionRequest, ExecutionResult, ExecutionSide, ExecutionStatus, OrderBook, simulate_fill,
};
use polymarket_copy_bot::models::{ActivityEntry, PortfolioSnapshot};
use polymarket_copy_bot::portfolio::PortfolioService;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::path::PathBuf;
use tempfile::tempdir;

fn sample_settings(data_dir: PathBuf) -> Settings {
    let mut settings = Settings::default_for_tests(data_dir);
    settings.start_capital_usd = dec!(100);
    settings
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
        ..PortfolioSnapshot::default()
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
        .project_fill_on_snapshot(&snapshot, &sample_entry(), &result, None)
        .expect("projected snapshot");

    assert_eq!(projected.positions.len(), 0);
    assert_eq!(projected.cash_balance, snapshot.cash_balance);
    assert_eq!(projected.total_exposure, snapshot.total_exposure);
}
