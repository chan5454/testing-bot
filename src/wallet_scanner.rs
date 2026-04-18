use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::Context;
use chrono::{DateTime, TimeDelta, Utc};
use serde::{Deserialize, Serialize};
use tokio::time::{MissedTickBehavior, interval};
use tracing::{info, warn};

use crate::config::Settings;
use crate::health::HealthState;
use crate::log_retention::RetentionOutcome;
use crate::models::ExecutionAnalyticsState;
use crate::rolling_jsonl::RollingJsonlLogger;
use crate::wallet::wallet_filter::normalize_wallet;
use crate::wallet::wallet_matching::ActivityTradeEvent;
use crate::wallet_registry::{WalletMeta, WalletRegistry};
use crate::wallet_score::{WalletCopyabilityClass, classify_wallet_copyability};

const WALLET_SCANNER_INTERVAL: Duration = Duration::from_secs(180);
const WALLET_SCANNER_LOOKBACK_MINUTES: i64 = 60;
const MIN_TRADES_PER_HOUR: usize = 5;
const SCANNER_TARGET_TRADES_PER_HOUR: f64 = 18.0;

#[derive(Clone)]
pub struct WalletActivityLogger {
    logger: RollingJsonlLogger,
}

#[derive(Clone)]
pub struct WalletScoreLogger {
    logger: RollingJsonlLogger,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct WalletActivityLogEntry {
    pub logged_at: DateTime<Utc>,
    pub wallet: String,
    pub market_id: String,
    pub transaction_hash: String,
    pub price: f64,
    pub size: f64,
    pub timestamp_ms: i64,
    pub source: String,
    pub tracked: bool,
    pub exit_eligible: bool,
}

#[derive(Serialize)]
struct WalletScoreEvent<'a> {
    event_type: &'static str,
    logged_at: DateTime<Utc>,
    wallet: &'a str,
    trades_per_hour: f64,
    last_trade_time: i64,
    avg_trade_size: f64,
    score: f64,
    active: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    copyability_class: Option<WalletCopyabilityClass>,
    #[serde(skip_serializing_if = "Option::is_none")]
    copyability_multiplier: Option<f64>,
}

#[derive(Default)]
struct WalletScanStats {
    trades: usize,
    total_size: f64,
    last_trade_time: i64,
}

#[derive(Clone)]
struct ScannedWallet {
    address: String,
    score: f64,
    last_seen: i64,
    avg_trade_size: f64,
    trades_per_hour: f64,
    active: bool,
    copyability_class: WalletCopyabilityClass,
    copyability_multiplier: f64,
}

#[derive(Clone, Copy)]
struct WalletCloseFeedback {
    multiplier: f64,
    copyability_class: WalletCopyabilityClass,
    copyability_multiplier: f64,
}

impl Default for WalletCloseFeedback {
    fn default() -> Self {
        let copyability_class = WalletCopyabilityClass::Unproven;
        Self {
            multiplier: copyability_class.multiplier(),
            copyability_class,
            copyability_multiplier: copyability_class.multiplier(),
        }
    }
}

impl WalletActivityLogger {
    pub fn new(settings: &Settings) -> Self {
        Self {
            logger: RollingJsonlLogger::new(
                settings.data_dir.join("wallet-activity.jsonl"),
                30_000,
            ),
        }
    }

    pub async fn record_event(
        &self,
        event: &ActivityTradeEvent,
        tracked: bool,
        exit_eligible: bool,
    ) {
        if let Err(error) = self
            .logger
            .append(&WalletActivityLogEntry {
                logged_at: Utc::now(),
                wallet: normalize_wallet(&event.wallet),
                market_id: event.market_id.clone(),
                transaction_hash: event.transaction_hash.clone(),
                price: event.price,
                size: event.size,
                timestamp_ms: event.timestamp_ms,
                source: event.source.as_str().to_owned(),
                tracked,
                exit_eligible,
            })
            .await
        {
            warn!(?error, "failed to persist wallet activity event");
        }
    }

    pub async fn cull_old_entries(&self, retention: Duration) -> anyhow::Result<RetentionOutcome> {
        self.logger.cull_old_entries(retention).await
    }
}

impl WalletScoreLogger {
    pub fn new(settings: &Settings) -> Self {
        Self {
            logger: RollingJsonlLogger::new(settings.data_dir.join("wallet-scores.jsonl"), 30_000),
        }
    }

    async fn record_score(
        &self,
        wallet: &str,
        trades_per_hour: f64,
        last_trade_time: i64,
        avg_trade_size: f64,
        score: f64,
        active: bool,
        copyability_class: WalletCopyabilityClass,
        copyability_multiplier: f64,
    ) {
        if let Err(error) = self
            .logger
            .append(&WalletScoreEvent {
                event_type: "wallet_scanned",
                logged_at: Utc::now(),
                wallet,
                trades_per_hour,
                last_trade_time,
                avg_trade_size,
                score,
                active,
                copyability_class: Some(copyability_class),
                copyability_multiplier: Some(copyability_multiplier),
            })
            .await
        {
            warn!(?error, wallet, "failed to persist wallet score");
        }
    }

    pub async fn cull_old_entries(&self, retention: Duration) -> anyhow::Result<RetentionOutcome> {
        self.logger.cull_old_entries(retention).await
    }
}

pub fn spawn_wallet_scanner(
    settings: Settings,
    wallet_registry: WalletRegistry,
    score_logger: WalletScoreLogger,
    health: std::sync::Arc<HealthState>,
) {
    tokio::spawn(async move {
        let mut ticker = interval(WALLET_SCANNER_INTERVAL);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            ticker.tick().await;
            match scan_recent_wallet_activity(&settings).await {
                Ok(scan_result) => {
                    for scanned_wallet in &scan_result {
                        score_logger
                            .record_score(
                                &scanned_wallet.address,
                                scanned_wallet.trades_per_hour,
                                scanned_wallet.last_seen,
                                scanned_wallet.avg_trade_size,
                                scanned_wallet.score,
                                scanned_wallet.active,
                                scanned_wallet.copyability_class,
                                scanned_wallet.copyability_multiplier,
                            )
                            .await;
                    }
                    let wallet_updates = scan_result
                        .into_iter()
                        .filter(|wallet| wallet.active)
                        .map(|wallet| WalletMeta {
                            address: wallet.address,
                            score: wallet.score,
                            last_seen: wallet.last_seen,
                            active: true,
                            inactive_since: None,
                            copyability_class: Some(wallet.copyability_class),
                            copyability_multiplier: Some(wallet.copyability_multiplier),
                        })
                        .collect::<Vec<_>>();
                    if let Err(error) = wallet_registry.update_wallets(wallet_updates) {
                        warn!(?error, "wallet scanner failed to update registry");
                        health.set_last_error(format!("{error:#}")).await;
                    } else {
                        info!(
                            active_wallets = wallet_registry.active_wallets().len(),
                            known_wallets = wallet_registry.known_wallets().len(),
                            "wallet scanner refreshed registry"
                        );
                    }
                }
                Err(error) => {
                    warn!(?error, "wallet scanner iteration failed");
                    health.set_last_error(format!("{error:#}")).await;
                }
            }
        }
    });
}

async fn scan_recent_wallet_activity(settings: &Settings) -> anyhow::Result<Vec<ScannedWallet>> {
    let now = Utc::now();
    let cutoff = now - TimeDelta::minutes(WALLET_SCANNER_LOOKBACK_MINUTES);
    let entries = read_wallet_activity_entries(&settings.data_dir, cutoff).await?;
    let wallet_feedback = load_wallet_close_feedback(&settings.data_dir).await;
    let mut by_wallet = HashMap::<String, WalletScanStats>::new();

    for entry in entries {
        let wallet = normalize_wallet(&entry.wallet);
        if wallet.is_empty() {
            continue;
        }
        let stats = by_wallet.entry(wallet).or_default();
        stats.trades = stats.trades.saturating_add(1);
        stats.total_size += entry.size.max(0.0);
        stats.last_trade_time = stats.last_trade_time.max(entry.timestamp_ms);
    }

    let min_avg_trade_size = settings
        .min_source_trade_usdc
        .to_string()
        .parse::<f64>()
        .unwrap_or(1.0);
    let max_trades_per_hour = if settings.max_wallet_trades_per_min == 0 {
        f64::INFINITY
    } else {
        settings.max_wallet_trades_per_min as f64 * 60.0
    };

    let mut wallets = by_wallet
        .into_iter()
        .map(|(wallet, stats)| {
            let trades_per_hour = stats.trades as f64;
            let avg_trade_size = if stats.trades > 0 {
                stats.total_size / stats.trades as f64
            } else {
                0.0
            };
            let active = stats.trades >= MIN_TRADES_PER_HOUR
                && stats.last_trade_time >= cutoff.timestamp_millis()
                && avg_trade_size >= min_avg_trade_size
                && trades_per_hour <= max_trades_per_hour;
            let score = if active {
                scanner_wallet_score(trades_per_hour, avg_trade_size, min_avg_trade_size)
            } else {
                0.0
            };
            let feedback = wallet_feedback
                .get(&wallet)
                .copied()
                .unwrap_or_default();
            let score = score * feedback.multiplier;
            ScannedWallet {
                address: wallet,
                score: score.clamp(0.0, 1.0),
                last_seen: stats.last_trade_time,
                avg_trade_size,
                trades_per_hour,
                active,
                copyability_class: feedback.copyability_class,
                copyability_multiplier: feedback.copyability_multiplier,
            }
        })
        .collect::<Vec<_>>();
    wallets.sort_by(|left, right| {
        right
            .score
            .total_cmp(&left.score)
            .then_with(|| right.last_seen.cmp(&left.last_seen))
    });
    limit_active_wallets(&mut wallets, settings.max_active_wallets as usize);
    Ok(wallets)
}

fn scanner_wallet_score(
    trades_per_hour: f64,
    avg_trade_size: f64,
    min_avg_trade_size: f64,
) -> f64 {
    let trade_rate_score = scanner_trade_rate_score(trades_per_hour);
    let size_target = (min_avg_trade_size * 4.0).max(8.0);
    let size_score = (avg_trade_size / size_target).clamp(0.0, 1.0);
    (trade_rate_score * 0.70 + size_score * 0.30).clamp(0.0, 1.0)
}

fn scanner_trade_rate_score(trades_per_hour: f64) -> f64 {
    let minimum = MIN_TRADES_PER_HOUR as f64;
    if trades_per_hour <= minimum {
        return 0.0;
    }

    if trades_per_hour <= SCANNER_TARGET_TRADES_PER_HOUR {
        return ((trades_per_hour - minimum) / (SCANNER_TARGET_TRADES_PER_HOUR - minimum))
            .clamp(0.0, 1.0);
    }

    (SCANNER_TARGET_TRADES_PER_HOUR / trades_per_hour).clamp(0.0, 1.0)
}

fn limit_active_wallets(wallets: &mut [ScannedWallet], max_active_wallets: usize) {
    if max_active_wallets == 0 {
        return;
    }

    let mut active_count = 0usize;
    for wallet in wallets {
        if !wallet.active {
            continue;
        }
        active_count += 1;
        if active_count > max_active_wallets {
            wallet.active = false;
        }
    }
}

async fn load_wallet_close_feedback(data_dir: &Path) -> HashMap<String, WalletCloseFeedback> {
    let path = data_dir.join("execution-analytics-summary.json");
    let payload = match tokio::fs::read_to_string(&path).await {
        Ok(payload) => payload,
        Err(_) => return HashMap::new(),
    };
    let state = match serde_json::from_str::<ExecutionAnalyticsState>(&payload) {
        Ok(state) => state,
        Err(_) => return HashMap::new(),
    };

    #[derive(Default)]
    struct WalletRealizedStats {
        count: u32,
        wins: u32,
        favorable_exits: u32,
        time_exits: u32,
        stop_exits: u32,
        pnl_total: rust_decimal::Decimal,
        hold_total_ms: u128,
        hold_count: u32,
    }

    let mut by_wallet = HashMap::<String, WalletRealizedStats>::new();
    for cohort in state
        .cohorts
        .iter()
        .filter(|cohort| cohort.status == crate::models::TradeCohortStatus::Closed)
    {
        let wallet = normalize_wallet(&cohort.source_wallet);
        if wallet.is_empty() {
            continue;
        }
        let close_reason = cohort.close_reason.as_deref().unwrap_or("UNKNOWN");
        let entry = by_wallet.entry(wallet).or_default();
        entry.count = entry.count.saturating_add(1);
        if cohort.realized_pnl > rust_decimal::Decimal::ZERO {
            entry.wins = entry.wins.saturating_add(1);
        }
        if matches!(
            close_reason,
            "SOURCE_EXIT" | "TAKE_PROFIT" | "PROFIT_PROTECTION"
        ) {
            entry.favorable_exits = entry.favorable_exits.saturating_add(1);
        }
        if matches!(close_reason, "TIME_EXIT") {
            entry.time_exits = entry.time_exits.saturating_add(1);
        }
        if matches!(close_reason, "STOP_LOSS" | "HARD_STOP") {
            entry.stop_exits = entry.stop_exits.saturating_add(1);
        }
        entry.pnl_total += cohort.realized_pnl;
        if let Some(close_time) = cohort.close_time {
            let hold_ms = close_time
                .signed_duration_since(cohort.open_time)
                .num_milliseconds()
                .max(0) as u64;
            entry.hold_total_ms = entry.hold_total_ms.saturating_add(u128::from(hold_ms));
            entry.hold_count = entry.hold_count.saturating_add(1);
        }
    }

    let feedback = by_wallet
        .into_iter()
        .map(|(wallet, stats)| {
            let count = u64::from(stats.count);
            let count_decimal = rust_decimal::Decimal::from(count.max(1));
            let favorable_exit_share =
                rust_decimal::Decimal::from(stats.favorable_exits) / count_decimal;
            let time_exit_share = rust_decimal::Decimal::from(stats.time_exits) / count_decimal;
            let stop_exit_share = rust_decimal::Decimal::from(stats.stop_exits) / count_decimal;
            let win_rate = rust_decimal::Decimal::from(stats.wins) / count_decimal;
            let avg_pnl_per_trade = (stats.pnl_total / count_decimal).round_dp(4);
            let avg_hold_ms = if stats.hold_count == 0 {
                0
            } else {
                (stats.hold_total_ms / u128::from(stats.hold_count)) as u64
            };
            let alpha_score = state
                .wallet_alpha_scores
                .get(&wallet)
                .copied()
                .unwrap_or(rust_decimal_macros::dec!(0.65));
            let copyability_class = classify_wallet_copyability(
                count,
                win_rate,
                favorable_exit_share,
                time_exit_share,
                stop_exit_share,
                avg_hold_ms,
                avg_pnl_per_trade,
                alpha_score,
            );
            let alpha_multiplier = state
                .wallet_alpha_scores
                .get(&wallet)
                .and_then(|score| score.to_string().parse::<f64>().ok())
                .map(|alpha| (0.75 + alpha.clamp(0.0, 1.0) * 0.45).clamp(0.75, 1.20))
                .unwrap_or(1.0);
            let performance_multiplier = if count < 3 {
                1.0
            } else {
                let count_f = count as f64;
                let good_share = stats.favorable_exits as f64 / count_f;
                let bad_share = (stats.time_exits + stats.stop_exits) as f64 / count_f;
                let pnl_penalty = if stats.pnl_total < rust_decimal::Decimal::ZERO {
                    0.15
                } else {
                    0.0
                };
                (1.0 + good_share * 0.15 - bad_share * 0.35 - pnl_penalty).clamp(0.25, 1.2)
            };
            let copyability_multiplier = copyability_class.multiplier();
            (
                wallet,
                WalletCloseFeedback {
                    multiplier: (performance_multiplier
                        * alpha_multiplier
                        * copyability_multiplier)
                        .clamp(0.20, 1.20),
                    copyability_class,
                    copyability_multiplier,
                },
            )
        })
        .collect::<HashMap<_, _>>();
    feedback
}

async fn read_wallet_activity_entries(
    data_dir: &PathBuf,
    cutoff: DateTime<Utc>,
) -> anyhow::Result<Vec<WalletActivityLogEntry>> {
    let mut directory = tokio::fs::read_dir(data_dir)
        .await
        .with_context(|| format!("reading {}", data_dir.display()))?;
    let mut paths = Vec::new();
    while let Some(entry) = directory.next_entry().await? {
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
            continue;
        };
        if name.starts_with("wallet-activity") && name.ends_with(".jsonl") {
            paths.push(path);
        }
    }

    let mut entries = Vec::new();
    for path in paths {
        let contents = match tokio::fs::read_to_string(&path).await {
            Ok(contents) => contents,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error).with_context(|| format!("reading {}", path.display())),
        };
        for line in contents.lines().filter(|line| !line.trim().is_empty()) {
            let Ok(entry) = serde_json::from_str::<WalletActivityLogEntry>(line) else {
                continue;
            };
            if entry.logged_at >= cutoff {
                entries.push(entry);
            }
        }
    }
    Ok(entries)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use rust_decimal_macros::dec;

    use super::*;
    use crate::config::Settings;
    use crate::models::{ExecutionAnalyticsState, TradeCohort, TradeCohortStatus};

    #[tokio::test]
    async fn wallet_close_feedback_penalizes_time_exit_heavy_losers() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let path = temp_dir.path().join("execution-analytics-summary.json");
        let state = ExecutionAnalyticsState {
            cohorts: vec![
                sample_closed_cohort("0xwallet-a", "TIME_EXIT", dec!(-3)),
                sample_closed_cohort("0xwallet-a", "STOP_LOSS", dec!(-2)),
                sample_closed_cohort("0xwallet-a", "TIME_EXIT", dec!(-1)),
                sample_closed_cohort("0xwallet-b", "SOURCE_EXIT", dec!(2)),
                sample_closed_cohort("0xwallet-b", "PROFIT_PROTECTION", dec!(1)),
                sample_closed_cohort("0xwallet-b", "SOURCE_EXIT", dec!(1)),
            ],
            ..ExecutionAnalyticsState::default()
        };
        tokio::fs::write(&path, serde_json::to_vec_pretty(&state).expect("json"))
            .await
            .expect("write");

        let feedback = load_wallet_close_feedback(&PathBuf::from(temp_dir.path())).await;

        assert!(feedback.get("0xwallet-a").expect("wallet a").multiplier < 1.0);
        assert_eq!(
            feedback.get("0xwallet-a").expect("wallet a").copyability_class,
            WalletCopyabilityClass::TimeExitHeavy
        );
        assert!(feedback.get("0xwallet-b").expect("wallet b").multiplier >= 1.0);
        assert_eq!(
            feedback.get("0xwallet-b").expect("wallet b").copyability_class,
            WalletCopyabilityClass::Copyable
        );
    }

    #[tokio::test]
    async fn scan_recent_wallet_activity_rejects_hyperactive_wallets() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let mut settings = Settings::default_for_tests(PathBuf::from(temp_dir.path()));
        settings.min_source_trade_usdc = dec!(1);
        settings.max_wallet_trades_per_min = 1;

        let now = Utc::now();
        let path = temp_dir.path().join("wallet-activity.jsonl");
        let mut lines = Vec::new();
        for index in 0..65 {
            lines.push(
                serde_json::to_string(&WalletActivityLogEntry {
                    logged_at: now,
                    wallet: "0xhyper".to_owned(),
                    market_id: format!("market-{index}"),
                    transaction_hash: format!("0xhyper-{index}"),
                    price: 0.4,
                    size: 10.0,
                    timestamp_ms: now.timestamp_millis() - index as i64,
                    source: "activity_ws".to_owned(),
                    tracked: false,
                    exit_eligible: false,
                })
                .expect("hyper json"),
            );
        }
        for index in 0..5 {
            lines.push(
                serde_json::to_string(&WalletActivityLogEntry {
                    logged_at: now,
                    wallet: "0xsteady".to_owned(),
                    market_id: format!("steady-market-{index}"),
                    transaction_hash: format!("0xsteady-{index}"),
                    price: 0.4,
                    size: 10.0,
                    timestamp_ms: now.timestamp_millis() - 100 - index as i64,
                    source: "activity_ws".to_owned(),
                    tracked: false,
                    exit_eligible: false,
                })
                .expect("steady json"),
            );
        }
        tokio::fs::write(&path, lines.join("\n"))
            .await
            .expect("write wallet activity");

        let scan_result = scan_recent_wallet_activity(&settings)
            .await
            .expect("scan succeeds");

        let hyper = scan_result
            .iter()
            .find(|wallet| wallet.address == "0xhyper")
            .expect("hyper wallet present");
        let steady = scan_result
            .iter()
            .find(|wallet| wallet.address == "0xsteady")
            .expect("steady wallet present");

        assert!(!hyper.active);
        assert!(steady.active);
    }

    #[tokio::test]
    async fn scan_recent_wallet_activity_limits_active_wallet_count() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let mut settings = Settings::default_for_tests(PathBuf::from(temp_dir.path()));
        settings.min_source_trade_usdc = dec!(5);
        settings.max_active_wallets = 3;

        let now = Utc::now();
        let path = temp_dir.path().join("wallet-activity.jsonl");
        let mut lines = Vec::new();
        for wallet_index in 0..5 {
            for trade_index in 0..8 {
                lines.push(
                    serde_json::to_string(&WalletActivityLogEntry {
                        logged_at: now,
                        wallet: format!("0xwallet-{wallet_index}"),
                        market_id: format!("market-{wallet_index}-{trade_index}"),
                        transaction_hash: format!("0xtx-{wallet_index}-{trade_index}"),
                        price: 0.4,
                        size: 20.0 - wallet_index as f64 * 2.0,
                        timestamp_ms: now.timestamp_millis() - trade_index as i64,
                        source: "activity_ws".to_owned(),
                        tracked: false,
                        exit_eligible: false,
                    })
                    .expect("wallet json"),
                );
            }
        }
        tokio::fs::write(&path, lines.join("\n"))
            .await
            .expect("write wallet activity");

        let scan_result = scan_recent_wallet_activity(&settings)
            .await
            .expect("scan succeeds");
        let active_wallets = scan_result
            .into_iter()
            .filter(|wallet| wallet.active)
            .map(|wallet| wallet.address)
            .collect::<Vec<_>>();

        assert_eq!(active_wallets.len(), 3);
        assert_eq!(
            active_wallets,
            vec![
                "0xwallet-0".to_owned(),
                "0xwallet-1".to_owned(),
                "0xwallet-2".to_owned()
            ]
        );
    }

    #[test]
    fn scanner_score_penalizes_overactive_wallets() {
        let moderate = scanner_wallet_score(12.0, 12.0, 1.0);
        let hyperactive = scanner_wallet_score(96.0, 12.0, 1.0);

        assert!(moderate > hyperactive);
        assert!(hyperactive < 0.5);
    }

    fn sample_closed_cohort(
        wallet: &str,
        close_reason: &str,
        pnl: rust_decimal::Decimal,
    ) -> TradeCohort {
        TradeCohort {
            source_wallet: wallet.to_owned(),
            asset: "asset-1".to_owned(),
            condition_id: "condition-1".to_owned(),
            outcome: "YES".to_owned(),
            market_type: crate::models::MarketType::Short,
            side: "BUY".to_owned(),
            source_trade_timestamp_unix: Utc::now().timestamp_millis(),
            source_price: dec!(0.5),
            filled_price: dec!(0.5),
            entry_slippage_pct: dec!(0.01),
            conviction_score: dec!(0.65),
            wallet_alpha_score: dec!(0.65),
            entry_notional: dec!(5),
            sizing_bucket: "standard".to_owned(),
            filled_size: dec!(10),
            execution_mode: "paper".to_owned(),
            open_time: Utc::now() - TimeDelta::minutes(15),
            close_time: Some(Utc::now()),
            close_reason: Some(close_reason.to_owned()),
            realized_pnl: pnl,
            unrealized_pnl: rust_decimal::Decimal::ZERO,
            status: TradeCohortStatus::Closed,
            remaining_size: rust_decimal::Decimal::ZERO,
            cost_basis: dec!(5),
        }
    }
}
