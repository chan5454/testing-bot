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
use crate::models::ExecutionAnalyticsState;
use crate::rolling_jsonl::RollingJsonlLogger;
use crate::wallet::wallet_filter::normalize_wallet;
use crate::wallet::wallet_matching::ActivityTradeEvent;
use crate::wallet_registry::{WalletMeta, WalletRegistry};

const WALLET_SCANNER_INTERVAL: Duration = Duration::from_secs(180);
const WALLET_SCANNER_LOOKBACK_MINUTES: i64 = 60;
const MIN_TRADES_PER_HOUR: usize = 5;

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
}

#[derive(Clone, Copy, Default)]
struct WalletCloseFeedback {
    multiplier: f64,
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
            })
            .await
        {
            warn!(?error, wallet, "failed to persist wallet score");
        }
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
                && avg_trade_size >= min_avg_trade_size;
            let score = if active {
                ((trades_per_hour / 10.0).min(1.0) * 0.6 + (avg_trade_size / 10.0).min(1.0) * 0.4)
                    .clamp(0.0, 1.0)
            } else {
                0.0
            } * wallet_feedback
                .get(&wallet)
                .copied()
                .unwrap_or(WalletCloseFeedback { multiplier: 1.0 })
                .multiplier;
            ScannedWallet {
                address: wallet,
                score: score.clamp(0.0, 1.0),
                last_seen: stats.last_trade_time,
                avg_trade_size,
                trades_per_hour,
                active,
            }
        })
        .collect::<Vec<_>>();
    wallets.sort_by(|left, right| {
        right
            .score
            .total_cmp(&left.score)
            .then_with(|| right.last_seen.cmp(&left.last_seen))
    });
    Ok(wallets)
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

    let mut by_wallet = HashMap::<String, (u32, u32, u32, f64)>::new();
    let mut from_summary_alpha = HashMap::<String, WalletCloseFeedback>::new();
    for (wallet, alpha_score) in &state.wallet_alpha_scores {
        let normalized = normalize_wallet(wallet);
        if normalized.is_empty() {
            continue;
        }
        let alpha = alpha_score
            .to_string()
            .parse::<f64>()
            .unwrap_or(0.65)
            .clamp(0.25, 1.20);
        from_summary_alpha.insert(normalized, WalletCloseFeedback { multiplier: alpha });
    }
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
        let entry = by_wallet.entry(wallet).or_insert((0, 0, 0, 0.0));
        entry.0 = entry.0.saturating_add(1);
        if matches!(
            close_reason,
            "SOURCE_EXIT" | "TAKE_PROFIT" | "PROFIT_PROTECTION"
        ) {
            entry.1 = entry.1.saturating_add(1);
        }
        if matches!(close_reason, "TIME_EXIT" | "STOP_LOSS" | "HARD_STOP") {
            entry.2 = entry.2.saturating_add(1);
        }
        entry.3 += cohort
            .realized_pnl
            .to_string()
            .parse::<f64>()
            .unwrap_or(0.0);
    }

    let mut feedback = by_wallet
        .into_iter()
        .map(|(wallet, (count, good, bad, pnl))| {
            let multiplier = if count < 3 {
                1.0
            } else {
                let count_f = count as f64;
                let good_share = good as f64 / count_f;
                let bad_share = bad as f64 / count_f;
                let pnl_penalty = if pnl < 0.0 { 0.15 } else { 0.0 };
                (1.0 + good_share * 0.15 - bad_share * 0.35 - pnl_penalty).clamp(0.25, 1.2)
            };
            (wallet, WalletCloseFeedback { multiplier })
        })
        .collect::<HashMap<_, _>>();
    for (wallet, alpha_feedback) in from_summary_alpha {
        feedback
            .entry(wallet)
            .and_modify(|feedback| {
                feedback.multiplier =
                    (feedback.multiplier * alpha_feedback.multiplier).clamp(0.25, 1.20);
            })
            .or_insert(alpha_feedback);
    }
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
        assert!(feedback.get("0xwallet-b").expect("wallet b").multiplier >= 1.0);
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
