use std::collections::{HashMap, VecDeque};
use std::fmt::Write as _;

use chrono::Utc;
use rust_decimal::Decimal;

use crate::config::Settings;
use crate::detection::trade_inference::ConfirmedTradeSignal;
use crate::execution::ExecutionSide;
use crate::models::ActivityEntry;
use crate::orderbook::orderbook_state::{AssetCatalog, AssetMetadata, MarketSnapshot};
use crate::position_registry::PositionRegistry;
use crate::wallet::wallet_filter::normalize_wallet;
use crate::wallet::wallet_matching::MatchedTrackedTrade;
use crate::wallet_registry::WalletRegistry;
use crate::wallet_score::{TrackedWallet, WalletStats, compute_wallet_score};

const MAX_RECENT_MARKET_OBSERVATIONS: usize = 16;
const MAX_MARKET_PROFILES: usize = 512;
const MAX_RECENT_DIRECTION_VOTES: usize = 24;
const FULL_EXECUTION_CONFIDENCE: f64 = 0.85;
const REDUCED_EXECUTION_CONFIDENCE: f64 = 0.70;
const REDUCED_SIZE_MULTIPLIER: f64 = 0.50;
const SIZE_SIMILARITY_TOLERANCE_RATIO: f64 = 0.50;
const MAX_MARKET_SPREAD_RATIO: f64 = 0.03;
const PRICE_BEHAVIOR_TOLERANCE_RATIO: f64 = 0.03;
const TIMING_IMMEDIATE_WINDOW_MS: i64 = 5_000;
const TIMING_HOT_WINDOW_MS: i64 = 60_000;
const TIMING_WARM_WINDOW_MS: i64 = 300_000;
const TARGET_PROFIT_MARGIN: f64 = 0.05;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PredictionTier {
    Skip,
    Reduced,
    Full,
}

impl PredictionTier {
    pub fn size_multiplier(self) -> f64 {
        match self {
            Self::Skip => 0.0,
            Self::Reduced => REDUCED_SIZE_MULTIPLIER,
            Self::Full => 1.0,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Skip => "skip",
            Self::Reduced => "reduced",
            Self::Full => "full",
        }
    }

    pub fn should_execute(self) -> bool {
        matches!(self, Self::Full)
    }
}

#[derive(Clone, Debug)]
pub struct PredictionDecision {
    pub confidence: f64,
    pub predicted_wallet: Option<String>,
    pub reasons: Vec<String>,
    pub tier: PredictionTier,
}

impl PredictionDecision {
    pub fn size_multiplier(&self) -> f64 {
        self.tier.size_multiplier()
    }
}

#[derive(Default)]
pub struct PredictionEngine {
    tracked_wallets: Vec<TrackedWallet>,
    wallet_registry: Option<WalletRegistry>,
    position_registry: Option<PositionRegistry>,
    min_wallet_score: Decimal,
    min_visible_market_depth: f64,
    wallet_profiles: HashMap<String, WalletProfile>,
    market_profiles: HashMap<String, MarketProfile>,
    market_order: VecDeque<String>,
}

#[derive(Default)]
struct WalletProfile {
    observations: u32,
    evaluated_trades: u32,
    wins: u32,
    cumulative_profit: f64,
    cumulative_size: f64,
    cumulative_notional: f64,
    cumulative_entry_price: f64,
    early_entry_count: u32,
    last_seen_ms: i64,
    pending_by_condition: HashMap<String, WalletTradeObservation>,
}

#[derive(Clone)]
struct WalletTradeObservation {
    asset_id: String,
    side: ExecutionSide,
    price: f64,
    size: f64,
    timestamp_ms: i64,
}

#[derive(Default)]
struct MarketProfile {
    observations: u32,
    last_seen_ms: i64,
    wallet_hits: HashMap<String, u32>,
    recent_sizes: VecDeque<f64>,
    recent_direction_votes: VecDeque<MarketDirectionVote>,
}

#[derive(Clone)]
struct MarketDirectionVote {
    wallet: String,
    side: ExecutionSide,
    timestamp_ms: i64,
}

#[derive(Clone)]
struct WalletQuality {
    wallet: String,
    win_rate: f64,
    average_trade_size: f64,
    average_entry_price: f64,
    early_entry_ratio: f64,
    profit_per_trade: f64,
    reliability_score: f64,
    wallet_score: Decimal,
    evaluated_trades: u32,
}

impl PredictionEngine {
    pub fn new(settings: &Settings) -> Self {
        Self {
            tracked_wallets: settings
                .target_profile_addresses
                .iter()
                .map(|wallet| TrackedWallet::new(normalize_wallet(wallet)))
                .collect(),
            wallet_registry: None,
            position_registry: None,
            min_wallet_score: settings.min_wallet_score.max(Decimal::ZERO),
            min_visible_market_depth: decimal_to_f64(settings.min_liquidity, 50.0),
            wallet_profiles: HashMap::new(),
            market_profiles: HashMap::new(),
            market_order: VecDeque::new(),
        }
    }

    pub fn with_registries(
        settings: &Settings,
        wallet_registry: WalletRegistry,
        position_registry: PositionRegistry,
    ) -> Self {
        let mut engine = Self::new(settings);
        engine.wallet_registry = Some(wallet_registry);
        engine.position_registry = Some(position_registry);
        engine
    }

    pub fn seed_from_history(&mut self, entries: &[ActivityEntry]) {
        let mut sorted = entries.to_vec();
        sorted.sort_by_key(|entry| normalize_timestamp_ms(entry.timestamp));
        for entry in &sorted {
            self.record_confirmed_trade(entry);
        }
    }

    pub fn record_confirmed_trade(&mut self, entry: &ActivityEntry) {
        let wallet = normalize_wallet(&entry.proxy_wallet);
        if wallet.is_empty() || !self.should_track_wallet(&wallet) {
            return;
        }
        let Some(side) = parse_execution_side(&entry.side) else {
            return;
        };

        let timestamp_ms = normalize_timestamp_ms(entry.timestamp);
        self.finalize_condition_feedback(
            &entry.condition_id,
            &entry.asset,
            entry.price,
            timestamp_ms,
        );

        let profile = self.wallet_profiles.entry(wallet.clone()).or_default();
        profile.observations = profile.observations.saturating_add(1);
        profile.cumulative_size += entry.size.max(0.0);
        profile.cumulative_notional += (entry.size * entry.price).max(0.0);
        profile.cumulative_entry_price += entry.price.max(0.0);
        if entry.price < 0.7 {
            profile.early_entry_count = profile.early_entry_count.saturating_add(1);
        }
        profile.last_seen_ms = profile.last_seen_ms.max(timestamp_ms);
        profile.pending_by_condition.insert(
            entry.condition_id.clone(),
            WalletTradeObservation {
                asset_id: entry.asset.clone(),
                side,
                price: entry.price,
                size: entry.size,
                timestamp_ms,
            },
        );

        let market_profile = self
            .market_profiles
            .entry(entry.condition_id.clone())
            .or_default();
        market_profile.observations = market_profile.observations.saturating_add(1);
        market_profile.last_seen_ms = market_profile.last_seen_ms.max(timestamp_ms);
        *market_profile
            .wallet_hits
            .entry(wallet.clone())
            .or_insert(0) += 1;
        push_recent_value(
            &mut market_profile.recent_sizes,
            entry.size,
            MAX_RECENT_MARKET_OBSERVATIONS,
        );
        push_recent_vote(
            &mut market_profile.recent_direction_votes,
            MarketDirectionVote {
                wallet,
                side,
                timestamp_ms,
            },
        );
        self.touch_market(&entry.condition_id);
    }

    pub fn predict(
        &self,
        signal: &ConfirmedTradeSignal,
        market_snapshot: Option<&MarketSnapshot>,
    ) -> PredictionDecision {
        let Some(market_snapshot) = market_snapshot else {
            return PredictionDecision {
                confidence: 0.0,
                predicted_wallet: None,
                reasons: vec!["market_snapshot_missing".to_owned()],
                tier: PredictionTier::Skip,
            };
        };

        let spread_ratio = market_snapshot.spread_ratio.unwrap_or(1.0);
        if spread_ratio > MAX_MARKET_SPREAD_RATIO {
            return PredictionDecision {
                confidence: 0.0,
                predicted_wallet: None,
                reasons: vec![format!("spread_too_wide={spread_ratio:.4}")],
                tier: PredictionTier::Skip,
            };
        }

        let required_depth = self
            .min_visible_market_depth
            .max(signal.estimated_size.max(0.0) * 3.0);
        if market_snapshot.visible_total_volume < required_depth {
            return PredictionDecision {
                confidence: 0.0,
                predicted_wallet: None,
                reasons: vec![format!(
                    "visible_depth_too_thin={:.2}/required={required_depth:.2}",
                    market_snapshot.visible_total_volume
                )],
                tier: PredictionTier::Skip,
            };
        }

        let market_profile = self.market_profiles.get(&signal.condition_id);
        let now_ms = signal.confirmed_at.timestamp_millis();
        let candidate_wallets = self.candidate_wallets_for_signal(signal);
        let mut wallet_candidates = self
            .candidate_wallets_for_signal(signal)
            .iter()
            .filter_map(|wallet| {
                let quality = self.wallet_quality(wallet)?;
                let market_match =
                    self.market_match_score(wallet, &signal.condition_id, market_profile, &quality);
                let size_score = similarity_score(
                    quality.average_trade_size,
                    signal.estimated_size,
                    SIZE_SIMILARITY_TOLERANCE_RATIO,
                );
                let timing_component = timing_score(signal, market_profile, wallet, now_ms);
                let wallet_score_component =
                    decimal_to_f64(quality.wallet_score, 0.0).clamp(0.0, 1.0);
                let candidate_rank = (market_match * 0.45
                    + timing_component * 0.25
                    + size_score * 0.15
                    + quality.reliability_score * 0.05
                    + wallet_score_component * 0.10)
                    .clamp(0.0, 1.0);
                Some((
                    quality,
                    market_match,
                    size_score,
                    timing_component,
                    candidate_rank,
                ))
            })
            .collect::<Vec<_>>();
        wallet_candidates.sort_by(
            |(left_quality, _, _, _, left_rank), (right_quality, _, _, _, right_rank)| {
                right_rank
                    .total_cmp(left_rank)
                    .then_with(|| right_quality.wallet_score.cmp(&left_quality.wallet_score))
            },
        );

        let Some((quality, market_match, size_score, timing_component, candidate_rank)) =
            wallet_candidates
                .iter()
                .find(|(quality, _, _, _, _)| quality.wallet_score >= self.min_wallet_score)
                .cloned()
        else {
            if let Some((quality, _, _, _, candidate_rank)) = wallet_candidates.first() {
                tracing::info!(
                    reason_code = "wallet_score_rejected",
                    wallet = %quality.wallet,
                    wallet_score = %quality.wallet_score.round_dp(4),
                    min_wallet_score = %self.min_wallet_score.round_dp(4),
                    candidate_rank = %format!("{candidate_rank:.2}"),
                    "wallet_score_rejected"
                );
            }
            let mut reasons = self
                .candidate_wallets_for_signal(signal)
                .iter()
                .filter_map(|wallet| self.wallet_quality(wallet))
                .map(|quality| {
                    format!(
                        "wallet={} wallet_score_rejected score={} win_rate={:.2} evaluated={} reliability={:.2}",
                        quality.wallet,
                        quality.wallet_score.round_dp(4),
                        quality.win_rate,
                        quality.evaluated_trades,
                        quality.reliability_score
                    )
                })
                .collect::<Vec<_>>();
            if candidate_wallets.is_empty() {
                reasons.push("no_seeded_tracked_wallet_history".to_owned());
            } else {
                reasons.push(format!(
                    "low_quality_wallet min_wallet_score={}",
                    self.min_wallet_score.round_dp(4)
                ));
            }
            return PredictionDecision {
                confidence: 0.0,
                predicted_wallet: None,
                reasons,
                tier: PredictionTier::Skip,
            };
        };

        let price_behavior = price_behavior_score(signal, market_snapshot);
        let confidence = (size_score * 0.40
            + market_match * 0.30
            + price_behavior * 0.20
            + timing_component * 0.10)
            .clamp(0.0, 1.0);

        let tier = if confidence >= FULL_EXECUTION_CONFIDENCE {
            PredictionTier::Full
        } else if confidence >= REDUCED_EXECUTION_CONFIDENCE {
            PredictionTier::Reduced
        } else {
            PredictionTier::Skip
        };

        PredictionDecision {
            confidence,
            predicted_wallet: Some(quality.wallet.clone()),
            reasons: vec![
                format!("tier={}", tier.as_str()),
                format!("wallet={}", quality.wallet),
                format!("wallet_score={}", quality.wallet_score.round_dp(4)),
                format!("wallet_avg_entry_price={:.4}", quality.average_entry_price),
                format!("wallet_early_entry_ratio={:.2}", quality.early_entry_ratio),
                format!("candidate_rank={candidate_rank:.2}"),
                format!("wallet_win_rate={:.2}", quality.win_rate),
                format!("wallet_profit_per_trade={:.4}", quality.profit_per_trade),
                format!("wallet_avg_size={:.2}", quality.average_trade_size),
                format!("wallet_reliability={:.2}", quality.reliability_score),
                format!("size_score={size_score:.2}"),
                format!("market_match={market_match:.2}"),
                format!("price_behavior={price_behavior:.2}"),
                format!("timing={timing_component:.2}"),
                format!("spread_ratio={spread_ratio:.4}"),
                format!(
                    "visible_depth={:.2}/required={required_depth:.2}",
                    market_snapshot.visible_total_volume
                ),
            ],
            tier,
        }
    }

    fn finalize_condition_feedback(
        &mut self,
        condition_id: &str,
        current_asset_id: &str,
        current_price: f64,
        timestamp_ms: i64,
    ) {
        for wallet_profile in self.wallet_profiles.values_mut() {
            let Some(previous) = wallet_profile.pending_by_condition.remove(condition_id) else {
                continue;
            };
            if timestamp_ms < previous.timestamp_ms {
                wallet_profile
                    .pending_by_condition
                    .insert(condition_id.to_owned(), previous);
                continue;
            }

            let comparable_price = if previous.asset_id == current_asset_id {
                current_price
            } else {
                (1.0 - current_price).clamp(0.0, 1.0)
            };
            let profit = match previous.side {
                ExecutionSide::Buy => (comparable_price - previous.price) * previous.size,
                ExecutionSide::Sell => (previous.price - comparable_price) * previous.size,
            };
            wallet_profile.evaluated_trades = wallet_profile.evaluated_trades.saturating_add(1);
            if profit > 0.0 {
                wallet_profile.wins = wallet_profile.wins.saturating_add(1);
            }
            wallet_profile.cumulative_profit += profit;
        }
    }

    fn market_match_score(
        &self,
        wallet: &str,
        _condition_id: &str,
        market_profile: Option<&MarketProfile>,
        quality: &WalletQuality,
    ) -> f64 {
        let Some(market_profile) = market_profile else {
            return (quality.reliability_score * 0.35).clamp(0.0, 1.0);
        };

        let market_affinity = market_profile.wallet_hits.get(wallet).copied().unwrap_or(0) as f64
            / market_profile.observations.max(1) as f64;
        let market_sample_score = (market_profile.observations.min(6) as f64 / 6.0).clamp(0.0, 1.0);
        let recency_ms = Utc::now()
            .timestamp_millis()
            .saturating_sub(market_profile.last_seen_ms);
        let recency_score = match recency_ms {
            delta if delta <= TIMING_IMMEDIATE_WINDOW_MS => 1.0,
            delta if delta <= TIMING_HOT_WINDOW_MS => 0.85,
            delta if delta <= TIMING_WARM_WINDOW_MS => 0.55,
            _ => 0.25,
        };

        (market_affinity * 0.55
            + market_sample_score * 0.20
            + recency_score * 0.15
            + quality.reliability_score * 0.10)
            .clamp(0.0, 1.0)
    }

    fn wallet_quality(&self, wallet: &str) -> Option<WalletQuality> {
        let Some(profile) = self.wallet_profiles.get(wallet) else {
            return self.registry_backed_wallet_quality(wallet);
        };
        if profile.observations == 0 {
            return self.registry_backed_wallet_quality(wallet);
        }
        let average_trade_size = profile.cumulative_size / profile.observations.max(1) as f64;
        let average_entry_price =
            profile.cumulative_entry_price / profile.observations.max(1) as f64;
        let early_entry_ratio =
            profile.early_entry_count as f64 / profile.observations.max(1) as f64;
        let win_rate = if profile.evaluated_trades > 0 {
            profile.wins as f64 / profile.evaluated_trades as f64
        } else {
            0.5
        };
        let profit_per_trade = if profile.evaluated_trades > 0 {
            profile.cumulative_profit / profile.evaluated_trades as f64
        } else {
            0.0
        };
        let average_notional = profile.cumulative_notional / profile.observations.max(1) as f64;
        let profit_margin = if average_notional > 0.0 {
            profit_per_trade / average_notional
        } else {
            0.0
        };
        let profitability_score = (profit_margin / TARGET_PROFIT_MARGIN).clamp(0.0, 1.0);
        let sample_score = (profile.observations.min(8) as f64 / 8.0).clamp(0.0, 1.0);
        let reliability_score =
            (win_rate * 0.55 + profitability_score * 0.20 + sample_score * 0.25).clamp(0.0, 1.0);
        let wallet_stats = WalletStats {
            avg_entry_price: decimal_from_f64(average_entry_price),
            win_rate: decimal_from_f64(win_rate),
            avg_trade_size: decimal_from_f64(average_trade_size),
            early_entry_ratio: decimal_from_f64(early_entry_ratio),
        };
        let wallet_score = compute_wallet_score(&wallet_stats);

        Some(WalletQuality {
            wallet: wallet.to_owned(),
            win_rate,
            average_trade_size,
            average_entry_price,
            early_entry_ratio,
            profit_per_trade,
            reliability_score,
            wallet_score,
            evaluated_trades: profile.evaluated_trades,
        })
    }

    fn touch_market(&mut self, condition_id: &str) {
        if let Some(index) = self
            .market_order
            .iter()
            .position(|item| item == condition_id)
        {
            self.market_order.remove(index);
        }
        self.market_order.push_front(condition_id.to_owned());
        while self.market_order.len() > MAX_MARKET_PROFILES {
            if let Some(expired) = self.market_order.pop_back() {
                self.market_profiles.remove(&expired);
            }
        }
    }

    fn candidate_wallets_for_signal(&self, signal: &ConfirmedTradeSignal) -> Vec<String> {
        match signal.side {
            ExecutionSide::Buy => {
                if let Some(wallet_registry) = &self.wallet_registry {
                    let active_wallets = wallet_registry.active_wallets();
                    if !active_wallets.is_empty() {
                        return active_wallets;
                    }
                }
                self.tracked_wallets
                    .iter()
                    .map(|tracked| tracked.address.clone())
                    .collect()
            }
            ExecutionSide::Sell => {
                if let Some(position_registry) = &self.position_registry {
                    return position_registry.open_wallets_for_market(&signal.condition_id);
                }
                self.tracked_wallets
                    .iter()
                    .map(|tracked| tracked.address.clone())
                    .collect()
            }
        }
    }

    fn should_track_wallet(&self, wallet: &str) -> bool {
        self.tracked_wallets
            .iter()
            .any(|tracked| tracked.address == wallet)
            || self
                .wallet_registry
                .as_ref()
                .and_then(|registry| registry.get_wallet_meta(wallet))
                .is_some()
            || self
                .position_registry
                .as_ref()
                .is_some_and(|registry| registry.has_open_position_for_wallet(wallet))
    }

    fn registry_backed_wallet_quality(&self, wallet: &str) -> Option<WalletQuality> {
        self.wallet_registry
            .as_ref()
            .and_then(|registry| registry.get_wallet_meta(wallet))
            .map(|meta| WalletQuality {
                wallet: meta.address,
                win_rate: meta.score.clamp(0.0, 1.0),
                average_trade_size: 1.0,
                average_entry_price: 0.5,
                early_entry_ratio: 0.5,
                profit_per_trade: 0.0,
                reliability_score: meta.score.clamp(0.0, 1.0),
                wallet_score: decimal_from_f64(meta.score.clamp(0.0, 1.0)),
                evaluated_trades: 0,
            })
    }
}

impl MarketProfile {
    fn wallet_side_recency_ms(
        &self,
        wallet: &str,
        side: ExecutionSide,
        now_ms: i64,
    ) -> Option<i64> {
        self.recent_direction_votes
            .iter()
            .filter(|vote| vote.wallet == wallet && vote.side == side)
            .map(|vote| now_ms.saturating_sub(vote.timestamp_ms))
            .min()
    }
}

pub fn signal_cache_key(signal: &ConfirmedTradeSignal) -> String {
    if let Some(transaction_hash) = signal
        .transaction_hash
        .as_deref()
        .map(str::trim)
        .filter(|hash| !hash.is_empty())
    {
        return format!(
            "signal_tx:{}:{}",
            signal.generation,
            transaction_hash.to_ascii_lowercase()
        );
    }

    let mut key = String::new();
    let _ = write!(
        key,
        "signal:{}:{}:{}:{}:{}:{:.6}:{:.6}",
        signal.generation,
        signal.condition_id,
        signal.asset_id,
        signal_side_label(signal.side),
        signal.confirmed_at.timestamp_millis(),
        signal.price,
        signal.estimated_size
    );
    key
}

pub fn build_predicted_trade(
    signal: &ConfirmedTradeSignal,
    catalog: &AssetCatalog,
    decision: &PredictionDecision,
) -> Option<MatchedTrackedTrade> {
    let predicted_wallet = decision.predicted_wallet.clone()?;
    if !decision.tier.should_execute() {
        return None;
    }

    let metadata = catalog
        .metadata(&signal.asset_id)
        .cloned()
        .unwrap_or_else(|| fallback_metadata(&signal.asset_id, &signal.condition_id));
    let transaction_hash = signal
        .transaction_hash
        .clone()
        .filter(|hash| !hash.trim().is_empty())
        .unwrap_or_else(|| format!("predicted:{}", signal_cache_key(signal)));
    let size_multiplier = decision.size_multiplier();
    let scaled_size = round_six_decimals(signal.estimated_size * size_multiplier);
    let scaled_notional =
        round_six_decimals(signal.estimated_size * signal.price * size_multiplier);
    let mut hot_signal = signal.clone();
    hot_signal.stage_timestamps.attribution_completed_at = Some(std::time::Instant::now());
    hot_signal.stage_timestamps.attribution_completed_at_utc = Some(Utc::now());

    Some(MatchedTrackedTrade {
        entry: ActivityEntry {
            proxy_wallet: predicted_wallet,
            timestamp: signal
                .stage_timestamps
                .websocket_event_received_at_utc
                .timestamp_millis(),
            condition_id: signal.condition_id.clone(),
            type_name: "PREDICTED_TRADE".to_owned(),
            size: scaled_size,
            usdc_size: scaled_notional,
            transaction_hash,
            price: signal.price,
            asset: signal.asset_id.clone(),
            side: signal_side_label(signal.side).to_owned(),
            outcome_index: metadata.outcome_index,
            title: metadata.title,
            slug: metadata.slug,
            event_slug: metadata.event_slug,
            outcome: metadata.outcome,
        },
        signal: hot_signal,
        source: "prediction_engine",
        validation_correlation_kind: None,
        validation_match_window: None,
        tx_hash_matched: false,
        validation_signal: None,
    })
}

fn price_behavior_score(signal: &ConfirmedTradeSignal, snapshot: &MarketSnapshot) -> f64 {
    let reference_price = match signal.side {
        ExecutionSide::Buy => snapshot
            .best_ask
            .or(snapshot.mid_price)
            .or(snapshot.last_trade_price)
            .unwrap_or(signal.price),
        ExecutionSide::Sell => snapshot
            .best_bid
            .or(snapshot.mid_price)
            .or(snapshot.last_trade_price)
            .unwrap_or(signal.price),
    };
    if reference_price <= 0.0 {
        return 0.0;
    }

    let proximity_score = similarity_score(
        signal.price,
        reference_price,
        PRICE_BEHAVIOR_TOLERANCE_RATIO,
    );
    let direction_score = snapshot
        .mid_price
        .map(|mid_price| match signal.side {
            ExecutionSide::Buy if signal.price >= mid_price => 1.0,
            ExecutionSide::Sell if signal.price <= mid_price => 1.0,
            _ => 0.45,
        })
        .unwrap_or(0.6);

    (proximity_score * 0.75 + direction_score * 0.25).clamp(0.0, 1.0)
}

fn timing_score(
    signal: &ConfirmedTradeSignal,
    market_profile: Option<&MarketProfile>,
    predicted_wallet: &str,
    now_ms: i64,
) -> f64 {
    let Some(market_profile) = market_profile else {
        return 0.0;
    };

    match market_profile.wallet_side_recency_ms(predicted_wallet, signal.side, now_ms) {
        Some(delta) if delta <= TIMING_IMMEDIATE_WINDOW_MS => 1.0,
        Some(delta) if delta <= TIMING_HOT_WINDOW_MS => 0.85,
        Some(delta) if delta <= TIMING_WARM_WINDOW_MS => 0.55,
        Some(_) => 0.20,
        None => 0.0,
    }
}

fn similarity_score(observed: f64, expected: f64, tolerance_ratio: f64) -> f64 {
    if observed <= 0.0 || expected <= 0.0 {
        return 0.0;
    }
    let baseline = observed.abs().max(expected.abs()).max(1.0);
    let delta_ratio = (observed - expected).abs() / baseline;
    (1.0 - (delta_ratio / tolerance_ratio)).clamp(0.0, 1.0)
}

fn push_recent_value(values: &mut VecDeque<f64>, value: f64, max_len: usize) {
    values.push_front(value);
    while values.len() > max_len {
        values.pop_back();
    }
}

fn push_recent_vote(votes: &mut VecDeque<MarketDirectionVote>, vote: MarketDirectionVote) {
    votes.push_front(vote);
    while votes.len() > MAX_RECENT_DIRECTION_VOTES {
        votes.pop_back();
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

fn parse_execution_side(side: &str) -> Option<ExecutionSide> {
    match side.trim().to_ascii_uppercase().as_str() {
        "BUY" => Some(ExecutionSide::Buy),
        "SELL" => Some(ExecutionSide::Sell),
        _ => None,
    }
}

fn normalize_timestamp_ms(timestamp: i64) -> i64 {
    if timestamp >= 10_000_000_000 {
        timestamp
    } else {
        timestamp.saturating_mul(1000)
    }
}

fn decimal_from_f64(value: f64) -> Decimal {
    Decimal::from_f64_retain(value).unwrap_or(Decimal::ZERO)
}

fn decimal_to_f64(value: Decimal, fallback: f64) -> f64 {
    value.to_string().parse::<f64>().unwrap_or(fallback)
}

fn round_six_decimals(value: f64) -> f64 {
    (value * 1_000_000.0).round() / 1_000_000.0
}

fn signal_side_label(side: ExecutionSide) -> &'static str {
    match side {
        ExecutionSide::Buy => "BUY",
        ExecutionSide::Sell => "SELL",
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use super::*;
    use crate::config::{ExecutionMode, Settings};
    use crate::models::TradeStageTimestamps;
    use rust_decimal_macros::dec;
    use std::path::PathBuf;
    use std::time::{Duration, Instant};

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
            target_profile_addresses: vec![
                "0x03e8a544e97eeff5753bc1e90d46e5ef22af1697".to_owned(),
                "0x1111111111111111111111111111111111111111".to_owned(),
            ],
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
            hot_path_mode: true,
            hot_path_queue_capacity: 128,
            cold_path_queue_capacity: 512,
            attribution_fast_cache_capacity: 256,
            persistence_flush_interval: Duration::from_millis(250),
            analytics_flush_interval: Duration::from_millis(500),
            telegram_async_only: true,
            fast_risk_only_on_hot_path: true,
            exit_priority_strict: true,
            parse_tasks_market: 1,
            parse_tasks_wallet: 1,
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
            data_dir: PathBuf::from("./data"),
        }
    }

    fn sample_signal() -> ConfirmedTradeSignal {
        let now = Instant::now();
        ConfirmedTradeSignal {
            asset_id: "asset-yes".to_owned(),
            condition_id: "condition-1".to_owned(),
            transaction_hash: None,
            side: ExecutionSide::Buy,
            price: 0.43,
            estimated_size: 21.0,
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

    fn sample_entry(wallet: &str, price: f64, timestamp_ms: i64) -> ActivityEntry {
        ActivityEntry {
            proxy_wallet: wallet.to_owned(),
            timestamp: timestamp_ms,
            condition_id: "condition-1".to_owned(),
            type_name: "TRADE".to_owned(),
            size: 21.0,
            usdc_size: round_six_decimals(21.0 * price),
            transaction_hash: format!("0xhash-{timestamp_ms}"),
            price,
            asset: "asset-yes".to_owned(),
            side: "BUY".to_owned(),
            outcome_index: 0,
            title: "Market".to_owned(),
            slug: "market".to_owned(),
            event_slug: "event".to_owned(),
            outcome: "YES".to_owned(),
        }
    }

    fn sample_market_snapshot() -> MarketSnapshot {
        MarketSnapshot {
            spread_ratio: Some((0.43_f64 - 0.42_f64) / 0.425_f64),
            visible_total_volume: 235.0,
            mid_price: Some(0.425),
            best_bid: Some(0.42),
            best_ask: Some(0.43),
            last_trade_price: Some(0.43),
        }
    }

    #[test]
    fn predicts_skip_without_seeded_wallet_history() {
        let engine = PredictionEngine::new(&sample_settings());
        let decision = engine.predict(&sample_signal(), Some(&sample_market_snapshot()));

        assert_eq!(decision.tier, PredictionTier::Skip);
        assert!(decision.confidence < REDUCED_EXECUTION_CONFIDENCE);
    }

    #[test]
    fn predicts_full_size_with_strong_wallet_history() {
        let mut engine = PredictionEngine::new(&sample_settings());
        let base_ms = Utc::now().timestamp_millis() - 60_000;
        let wallet = "0x03e8a544e97eeff5753bc1e90d46e5ef22af1697";

        for offset in 0..4 {
            engine.record_confirmed_trade(&sample_entry(
                wallet,
                0.40 + offset as f64 * 0.01,
                base_ms + offset * 1_000,
            ));
        }
        engine.record_confirmed_trade(&sample_entry(
            "0x1111111111111111111111111111111111111111",
            0.44,
            base_ms + 5_000,
        ));

        let decision = engine.predict(&sample_signal(), Some(&sample_market_snapshot()));

        assert_eq!(decision.tier, PredictionTier::Full);
        assert!(decision.confidence >= FULL_EXECUTION_CONFIDENCE);
        assert_eq!(decision.predicted_wallet.as_deref(), Some(wallet));
    }

    #[test]
    fn reduced_confidence_predictions_do_not_build_executable_trade() {
        let decision = PredictionDecision {
            confidence: 0.75,
            predicted_wallet: Some("0xwallet".to_owned()),
            reasons: vec![],
            tier: PredictionTier::Reduced,
        };
        let signal = sample_signal();
        let catalog = AssetCatalog::new(vec![AssetMetadata {
            asset_id: "asset-yes".to_owned(),
            condition_id: "condition-1".to_owned(),
            title: "Market".to_owned(),
            slug: "market".to_owned(),
            event_slug: "event".to_owned(),
            outcome: "YES".to_owned(),
            outcome_index: 0,
        }]);

        assert!(build_predicted_trade(&signal, &catalog, &decision).is_none());
    }

    #[test]
    fn builds_predicted_trade_with_market_timestamp_precision() {
        let decision = PredictionDecision {
            confidence: 0.90,
            predicted_wallet: Some("0xwallet".to_owned()),
            reasons: vec![],
            tier: PredictionTier::Full,
        };
        let signal = sample_signal();
        let expected_timestamp = signal
            .stage_timestamps
            .websocket_event_received_at_utc
            .timestamp_millis();
        let catalog = AssetCatalog::new(vec![AssetMetadata {
            asset_id: "asset-yes".to_owned(),
            condition_id: "condition-1".to_owned(),
            title: "Market".to_owned(),
            slug: "market".to_owned(),
            event_slug: "event".to_owned(),
            outcome: "YES".to_owned(),
            outcome_index: 0,
        }]);

        let trade = build_predicted_trade(&signal, &catalog, &decision).expect("predicted trade");

        assert_eq!(trade.entry.timestamp, expected_timestamp);
    }
}
