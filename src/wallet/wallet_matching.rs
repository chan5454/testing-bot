use std::time::Instant;

use chrono::{DateTime, TimeZone, Utc};

use crate::config::Settings;
use crate::detection::trade_inference::ConfirmedTradeSignal;
use crate::execution::ExecutionSide;
use crate::models::{ActivityEntry, TradeStageTimestamps};
use crate::orderbook::orderbook_state::{AssetCatalog, AssetMetadata};
use crate::wallet::wallet_filter::normalize_wallet;

pub const PRIMARY_MATCH_WINDOW: std::time::Duration = std::time::Duration::from_millis(120);
pub const MIN_FALLBACK_MATCH_WINDOW: std::time::Duration = std::time::Duration::from_secs(3);
pub const OUTCOME_MISMATCH_WINDOW: std::time::Duration = std::time::Duration::from_secs(2);
const COMPLEMENTARY_PRICE_SUM_TOLERANCE: f64 = 0.03;

pub fn effective_fallback_match_window(settings: &Settings) -> std::time::Duration {
    settings
        .slow_validation_window
        .max(settings.activity_match_window)
        .max(settings.activity_cache_ttl)
        .max(settings.activity_correlation_window)
        .max(MIN_FALLBACK_MATCH_WINDOW)
}

#[derive(Clone, Copy, Debug, serde::Serialize)]
pub enum ActivitySource {
    UserStream,
    ActivityStream,
}

impl ActivitySource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::UserStream => "user_ws",
            Self::ActivityStream => "activity_ws",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TradeCorrelationKind {
    Direct,
    Complementary,
}

impl TradeCorrelationKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Direct => "direct",
            Self::Complementary => "complementary",
        }
    }

    fn rank(self) -> u8 {
        match self {
            Self::Direct => 0,
            Self::Complementary => 1,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MatchWindow {
    Primary,
    Fallback,
}

impl MatchWindow {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Primary => "primary",
            Self::Fallback => "fallback",
        }
    }

    fn rank(self) -> u8 {
        match self {
            Self::Primary => 0,
            Self::Fallback => 1,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ActivityMatch {
    pub correlation_kind: TradeCorrelationKind,
    pub match_window: MatchWindow,
    pub tx_hash_matched: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ActivityMatchResult {
    Matched(ActivityMatch),
    OutcomeMismatch,
    NoMatch,
}

#[derive(Clone, Debug, serde::Serialize)]
pub struct ActivityTradeEvent {
    pub event_id: String,
    pub wallet: String,
    pub wallet_candidates: Vec<String>,
    pub market_id: String,
    pub price: f64,
    pub size: f64,
    pub timestamp_ms: i64,
    pub transaction_hash: String,
    pub asset_id: Option<String>,
    pub side: Option<String>,
    pub generation: u64,
    pub source: ActivitySource,
    #[serde(skip_serializing)]
    pub observed_at: Instant,
    #[serde(skip_serializing)]
    pub observed_at_utc: DateTime<Utc>,
    #[serde(skip_serializing)]
    pub parse_completed_at: Instant,
    #[serde(skip_serializing)]
    pub parse_completed_at_utc: DateTime<Utc>,
}

#[derive(Clone, Debug)]
pub struct MatchedTrackedTrade {
    pub entry: ActivityEntry,
    pub signal: ConfirmedTradeSignal,
    pub source: &'static str,
    pub validation_correlation_kind: Option<TradeCorrelationKind>,
    pub validation_match_window: Option<MatchWindow>,
    pub tx_hash_matched: bool,
    pub validation_signal: Option<ConfirmedTradeSignal>,
}

#[derive(Clone, Debug)]
pub struct MarketSignalValidation {
    pub signal: ConfirmedTradeSignal,
    pub activity_match: ActivityMatch,
}

pub fn activity_match_result(
    event: &ActivityTradeEvent,
    signal: &ConfirmedTradeSignal,
    settings: &Settings,
) -> ActivityMatchResult {
    if event.generation != signal.generation || event.market_id != signal.condition_id {
        return ActivityMatchResult::NoMatch;
    }

    if event_has_transaction_hash(event)
        && signal_transaction_hash(signal).is_some_and(|transaction_hash| {
            transaction_hash.eq_ignore_ascii_case(event.transaction_hash.as_str())
        })
    {
        return ActivityMatchResult::Matched(ActivityMatch {
            correlation_kind: TradeCorrelationKind::Direct,
            match_window: MatchWindow::Fallback,
            tx_hash_matched: true,
        });
    }

    let timestamp_delta_ms = signal_reference_timestamps(signal)
        .into_iter()
        .map(|reference| (event.timestamp_ms - reference).abs())
        .min()
        .unwrap_or(i64::MAX);
    if timestamp_delta_ms > OUTCOME_MISMATCH_WINDOW.as_millis() as i64 {
        return ActivityMatchResult::NoMatch;
    }

    if !sizes_are_similar(
        event.size,
        signal.estimated_size,
        decimal_to_f64(settings.activity_size_tolerance_ratio, 0.5),
    ) {
        return ActivityMatchResult::NoMatch;
    }

    let same_outcome = event
        .asset_id
        .as_deref()
        .map(|asset_id| asset_id == signal.asset_id)
        .unwrap_or(true);
    let complementary_outcome = event
        .asset_id
        .as_deref()
        .map(|asset_id| asset_id != signal.asset_id)
        .unwrap_or(true);
    let price_tolerance = decimal_to_f64(settings.activity_price_tolerance, 0.01);
    let direct_price_delta = (event.price - signal.price).abs();
    let direct_price_match = direct_price_delta <= price_tolerance;
    let complementary_price_match = is_complementary(event.price, signal.price);

    if timestamp_delta_ms <= PRIMARY_MATCH_WINDOW.as_millis() as i64 {
        if same_outcome && direct_price_match {
            return ActivityMatchResult::Matched(ActivityMatch {
                correlation_kind: TradeCorrelationKind::Direct,
                match_window: MatchWindow::Primary,
                tx_hash_matched: false,
            });
        }
        if complementary_outcome && complementary_price_match {
            return ActivityMatchResult::Matched(ActivityMatch {
                correlation_kind: TradeCorrelationKind::Complementary,
                match_window: MatchWindow::Primary,
                tx_hash_matched: false,
            });
        }
    }

    let fallback_match_window = effective_fallback_match_window(settings);
    if timestamp_delta_ms <= fallback_match_window.as_millis() as i64 {
        if same_outcome && direct_price_match {
            return ActivityMatchResult::Matched(ActivityMatch {
                correlation_kind: TradeCorrelationKind::Direct,
                match_window: MatchWindow::Fallback,
                tx_hash_matched: false,
            });
        }
        if complementary_outcome && complementary_price_match {
            return ActivityMatchResult::Matched(ActivityMatch {
                correlation_kind: TradeCorrelationKind::Complementary,
                match_window: MatchWindow::Fallback,
                tx_hash_matched: false,
            });
        }
    }

    if !same_outcome && !complementary_price_match {
        return ActivityMatchResult::OutcomeMismatch;
    }

    ActivityMatchResult::NoMatch
}

#[cfg_attr(not(test), allow(dead_code))]
pub fn activity_match_kind(
    event: &ActivityTradeEvent,
    signal: &ConfirmedTradeSignal,
    settings: &Settings,
) -> Option<TradeCorrelationKind> {
    match activity_match_result(event, signal, settings) {
        ActivityMatchResult::Matched(activity_match) => Some(activity_match.correlation_kind),
        ActivityMatchResult::OutcomeMismatch | ActivityMatchResult::NoMatch => None,
    }
}

pub fn activity_match_score(
    event: &ActivityTradeEvent,
    signal: &ConfirmedTradeSignal,
    activity_match: ActivityMatch,
) -> (u8, u8, u8, i64, i64, i64) {
    let timestamp_delta_ms = signal_reference_timestamps(signal)
        .into_iter()
        .map(|reference| (event.timestamp_ms - reference).abs())
        .min()
        .unwrap_or(i64::MAX);
    let price_delta_scaled = match activity_match.correlation_kind {
        TradeCorrelationKind::Direct => {
            ((event.price - signal.price).abs() * 1_000_000.0).round() as i64
        }
        TradeCorrelationKind::Complementary => {
            (complementary_price_delta(event.price, signal.price) * 1_000_000.0).round() as i64
        }
    };
    let size_delta_scaled =
        ((event.size - signal.estimated_size).abs() * 1_000_000.0).round() as i64;
    (
        if activity_match.tx_hash_matched { 0 } else { 1 },
        activity_match.match_window.rank(),
        activity_match.correlation_kind.rank(),
        timestamp_delta_ms,
        price_delta_scaled,
        size_delta_scaled,
    )
}

pub fn build_wallet_triggered_trade(
    event: &ActivityTradeEvent,
    catalog: &AssetCatalog,
    validation: Option<&MarketSignalValidation>,
) -> Option<MatchedTrackedTrade> {
    let asset_id = event
        .asset_id
        .clone()
        .or_else(|| validation_asset_id(validation, catalog))?;
    let metadata = catalog
        .metadata(&asset_id)
        .cloned()
        .unwrap_or_else(|| fallback_metadata(&asset_id, &event.market_id));
    let side =
        event.side.as_deref().map(normalize_side).or_else(|| {
            validation.map(|validation| side_label(validation.signal.side).to_owned())
        })?;
    let execution_side = parse_execution_side(&side)?;
    let transaction_hash = if event.transaction_hash.is_empty() {
        event.event_id.clone()
    } else {
        event.transaction_hash.clone()
    };
    let detection_triggered_at = Instant::now();
    let detection_triggered_at_utc = Utc::now();
    let confirmed_at = Utc
        .timestamp_millis_opt(event.timestamp_ms)
        .single()
        .unwrap_or(event.observed_at_utc);

    let entry = ActivityEntry {
        proxy_wallet: normalize_wallet(&event.wallet),
        timestamp: event.timestamp_ms,
        condition_id: event.market_id.clone(),
        type_name: "TRADE".to_owned(),
        size: event.size,
        usdc_size: round_six_decimals(event.size * event.price),
        transaction_hash,
        price: event.price,
        asset: asset_id.clone(),
        side,
        outcome_index: metadata.outcome_index,
        title: metadata.title.clone(),
        slug: metadata.slug.clone(),
        event_slug: metadata.event_slug.clone(),
        outcome: metadata.outcome.clone(),
    };

    let signal = ConfirmedTradeSignal {
        asset_id,
        condition_id: event.market_id.clone(),
        transaction_hash: event_has_transaction_hash(event).then(|| event.transaction_hash.clone()),
        side: execution_side,
        price: event.price,
        estimated_size: event.size,
        stage_timestamps: TradeStageTimestamps {
            websocket_event_received_at: event.observed_at,
            websocket_event_received_at_utc: event.observed_at_utc,
            parse_completed_at: event.parse_completed_at,
            parse_completed_at_utc: event.parse_completed_at_utc,
            detection_triggered_at,
            detection_triggered_at_utc,
            attribution_completed_at: None,
            attribution_completed_at_utc: None,
            fast_risk_completed_at: None,
            fast_risk_completed_at_utc: None,
        },
        confirmed_at,
        generation: event.generation,
    };

    Some(MatchedTrackedTrade {
        entry,
        signal,
        source: event.source.as_str(),
        validation_correlation_kind: validation
            .map(|validation| validation.activity_match.correlation_kind),
        validation_match_window: validation
            .map(|validation| validation.activity_match.match_window),
        tx_hash_matched: validation
            .map(|validation| validation.activity_match.tx_hash_matched)
            .unwrap_or(false),
        validation_signal: validation.map(|validation| validation.signal.clone()),
    })
}

pub fn is_complementary(price_a: f64, price_b: f64) -> bool {
    (price_a + price_b - 1.0).abs() < COMPLEMENTARY_PRICE_SUM_TOLERANCE
}

pub fn signal_reference_timestamps(signal: &ConfirmedTradeSignal) -> [i64; 2] {
    [
        signal
            .stage_timestamps
            .detection_triggered_at_utc
            .timestamp_millis(),
        signal.confirmed_at.timestamp_millis(),
    ]
}

fn signal_transaction_hash(signal: &ConfirmedTradeSignal) -> Option<&str> {
    signal
        .transaction_hash
        .as_deref()
        .map(str::trim)
        .filter(|hash| !hash.is_empty())
}

fn event_has_transaction_hash(event: &ActivityTradeEvent) -> bool {
    !event.transaction_hash.trim().is_empty()
}

pub fn normalize_side(side: &str) -> String {
    side.trim().to_ascii_uppercase()
}

pub fn side_label(side: ExecutionSide) -> &'static str {
    match side {
        ExecutionSide::Buy => "BUY",
        ExecutionSide::Sell => "SELL",
    }
}

fn validation_asset_id(
    validation: Option<&MarketSignalValidation>,
    catalog: &AssetCatalog,
) -> Option<String> {
    let validation = validation?;
    match validation.activity_match.correlation_kind {
        TradeCorrelationKind::Direct => Some(validation.signal.asset_id.clone()),
        TradeCorrelationKind::Complementary => catalog
            .complementary_metadata(&validation.signal.condition_id, &validation.signal.asset_id)
            .map(|metadata| metadata.asset_id.clone()),
    }
}

fn parse_execution_side(side: &str) -> Option<ExecutionSide> {
    match normalize_side(side).as_str() {
        "BUY" => Some(ExecutionSide::Buy),
        "SELL" => Some(ExecutionSide::Sell),
        _ => None,
    }
}

fn fallback_metadata(asset_id: &str, condition_id: &str) -> AssetMetadata {
    AssetMetadata {
        asset_id: asset_id.to_owned(),
        condition_id: condition_id.to_owned(),
        title: asset_id.to_owned(),
        slug: asset_id.to_owned(),
        event_slug: asset_id.to_owned(),
        outcome: "UNKNOWN".to_owned(),
        outcome_index: 0,
    }
}

pub fn sizes_are_similar(actual: f64, expected: f64, tolerance_ratio: f64) -> bool {
    if actual <= 0.0 || expected <= 0.0 {
        return true;
    }

    let tolerance_ratio = tolerance_ratio.max(0.0);
    let baseline = actual.abs().max(expected.abs()).max(1.0);
    (actual - expected).abs() <= baseline * tolerance_ratio
}

fn decimal_to_f64(value: rust_decimal::Decimal, fallback: f64) -> f64 {
    value.to_string().parse::<f64>().unwrap_or(fallback)
}

fn complementary_price_delta(entry_price: f64, signal_price: f64) -> f64 {
    (entry_price - (1.0 - signal_price)).abs()
}

fn round_six_decimals(value: f64) -> f64 {
    (value * 1_000_000.0).round() / 1_000_000.0
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use chrono::Utc;
    use rust_decimal_macros::dec;

    use super::*;
    use crate::models::TradeStageTimestamps;

    fn sample_settings() -> Settings {
        let mut settings = Settings::default_for_tests(std::path::PathBuf::from("./data"));
        settings.target_profile_addresses = vec!["0xtarget".to_owned()];
        settings.start_capital_usd = dec!(100);
        settings.market_cache_ttl = Duration::from_secs(3);
        settings.market_raw_ring_capacity = 128;
        settings.wallet_ring_capacity = 128;
        settings.market_subscription_batch_size = 10;
        settings.market_subscription_delay = Duration::ZERO;
        settings.wallet_subscription_batch_size = 10;
        settings.wallet_subscription_delay = Duration::ZERO;
        settings.min_source_trade_usdc = dec!(3);
        settings
    }

    fn sample_signal(price: f64) -> ConfirmedTradeSignal {
        let now = std::time::Instant::now();
        ConfirmedTradeSignal {
            asset_id: "asset-yes".to_owned(),
            condition_id: "condition-1".to_owned(),
            transaction_hash: None,
            side: ExecutionSide::Buy,
            price,
            estimated_size: 10.0,
            stage_timestamps: TradeStageTimestamps {
                websocket_event_received_at: now,
                websocket_event_received_at_utc: Utc::now(),
                parse_completed_at: now,
                parse_completed_at_utc: Utc::now(),
                detection_triggered_at: now,
                detection_triggered_at_utc: Utc::now(),
                attribution_completed_at: Some(now),
                attribution_completed_at_utc: Some(Utc::now()),
                fast_risk_completed_at: Some(now),
                fast_risk_completed_at_utc: Some(Utc::now()),
            },
            confirmed_at: Utc::now(),
            generation: 1,
        }
    }

    fn sample_event(price: f64) -> ActivityTradeEvent {
        let observed_at = Instant::now();
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
            parse_completed_at: observed_at,
            parse_completed_at_utc: observed_at_utc,
        }
    }

    #[test]
    fn wallet_triggered_trade_preserves_activity_receipt_timestamp() {
        let catalog = AssetCatalog::new(
            [AssetMetadata {
                asset_id: "asset-no".to_owned(),
                condition_id: "condition-1".to_owned(),
                title: "Market".to_owned(),
                slug: "market".to_owned(),
                event_slug: "event".to_owned(),
                outcome: "No".to_owned(),
                outcome_index: 1,
            }]
            .into_iter()
            .collect(),
        );
        let event = sample_event(0.57);
        let matched = build_wallet_triggered_trade(&event, &catalog, None).expect("trade");

        assert_eq!(
            matched
                .signal
                .stage_timestamps
                .websocket_event_received_at_utc,
            event.observed_at_utc
        );
        assert!(
            matched.signal.stage_timestamps.detection_triggered_at_utc
                >= matched
                    .signal
                    .stage_timestamps
                    .websocket_event_received_at_utc
        );
    }

    #[test]
    fn accepts_direct_price_match() {
        let settings = sample_settings();
        let signal = sample_signal(0.43);
        let mut event = sample_event(0.43);
        event.asset_id = Some("asset-yes".to_owned());

        assert_eq!(
            activity_match_kind(&event, &signal, &settings),
            Some(TradeCorrelationKind::Direct)
        );
    }

    #[test]
    fn accepts_complementary_price_match() {
        let settings = sample_settings();
        let signal = sample_signal(0.43);
        let event = sample_event(0.57);

        assert_eq!(
            activity_match_kind(&event, &signal, &settings),
            Some(TradeCorrelationKind::Complementary)
        );
    }

    #[test]
    fn accepts_tx_hash_match_outside_time_window() {
        let settings = sample_settings();
        let mut signal = sample_signal(0.43);
        signal.transaction_hash = Some("0xhash".to_owned());
        let mut event = sample_event(0.99);
        event.asset_id = Some("asset-yes".to_owned());
        event.timestamp_ms += 5_000;

        let matched = activity_match_result(&event, &signal, &settings);
        let ActivityMatchResult::Matched(activity_match) = matched else {
            panic!("expected tx-hash validation match");
        };
        assert!(activity_match.tx_hash_matched);
    }

    #[test]
    fn fallback_match_window_expands_to_slow_validation_window() {
        let mut settings = sample_settings();
        settings.slow_validation_window = std::time::Duration::from_secs(6);

        assert_eq!(
            effective_fallback_match_window(&settings),
            std::time::Duration::from_secs(6)
        );
    }
}
