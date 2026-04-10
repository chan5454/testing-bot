use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::config::Settings;
use crate::models::{ActivityEntry, PortfolioSnapshot, PositionKey, position_outcome_is_unknown};
use crate::position_registry::PositionRegistry;
use crate::position_resolver::{PositionResolver, ResolveResult, ResolvedResolverPosition};
use crate::wallet::wallet_matching::MatchedTrackedTrade;

#[derive(Clone)]
pub struct ExitResolutionBuffer {
    path: Arc<PathBuf>,
    retry_window: Duration,
    initial_retry_interval: Duration,
    max_retry_interval: Duration,
    pending: Arc<Mutex<HashMap<String, PendingExitIntent>>>,
    dedupe_window: Duration,
    recent_source_exits: Arc<Mutex<HashMap<String, DateTime<Utc>>>>,
}

#[derive(Clone, Debug)]
pub enum SourceExitResolution {
    Matched(PositionKey),
    MatchedByFallback(PositionKey, String),
    BoundToPending(BoundPendingDisposition),
    DeferredRetry(RetryDisposition),
    NotActionable(NonActionableExit),
    Failed(String),
}

#[derive(Clone, Debug)]
pub struct NonActionableExit {
    pub reason: &'static str,
    pub detail: String,
}

#[derive(Clone, Debug)]
pub struct BoundPendingDisposition {
    pub position_key: PositionKey,
    pub already_bound: bool,
    pub bound_exit_count: usize,
}

#[derive(Clone, Debug)]
pub struct RetryDisposition {
    pub retry_key: String,
    pub reason: String,
    pub already_queued: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PendingExitIntent {
    pub retry_key: String,
    pub source_wallet: String,
    pub condition_id: String,
    pub outcome: String,
    pub side: String,
    pub first_seen_at: DateTime<Utc>,
    pub last_seen_at: DateTime<Utc>,
    pub next_retry_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub attempts: u32,
    pub current_retry_interval_ms: u64,
    pub last_reason: String,
    pub event: ActivityEntry,
}

impl ExitResolutionBuffer {
    pub fn load(settings: &Settings) -> Result<Self> {
        let path = settings.data_dir.join("unresolved-exits.json");
        let pending = load_pending_exits(&path)?;
        Ok(Self {
            path: Arc::new(path),
            retry_window: settings.unresolved_exit_total_window,
            initial_retry_interval: settings.unresolved_exit_initial_retry,
            max_retry_interval: settings.unresolved_exit_max_retry,
            pending: Arc::new(Mutex::new(pending)),
            dedupe_window: settings.source_exit_dedupe_window,
            recent_source_exits: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    pub async fn claim_source_exit(&self, entry: &ActivityEntry, now: DateTime<Utc>) -> bool {
        let key = source_exit_dedupe_key(entry, self.dedupe_window);
        let retain_after = now
            - chrono::TimeDelta::from_std(self.dedupe_window.max(Duration::from_millis(1)))
                .unwrap_or_else(|_| chrono::TimeDelta::zero());
        let mut recent = self.recent_source_exits.lock().await;
        recent.retain(|_, seen_at| *seen_at >= retain_after);
        if recent.contains_key(&key) {
            return false;
        }
        recent.insert(key, now);
        true
    }

    pub async fn resolve_source_exit(
        &self,
        snapshot: &PortfolioSnapshot,
        position_registry: &PositionRegistry,
        position_resolver: &PositionResolver,
        matched_trade: &MatchedTrackedTrade,
        now: DateTime<Utc>,
    ) -> Result<SourceExitResolution> {
        if let Some(resolved_position) = snapshot.resolve_position_to_sell(&matched_trade.entry) {
            return Ok(match resolved_position.fallback_reason {
                Some(fallback_reason) => {
                    SourceExitResolution::MatchedByFallback(resolved_position.key, fallback_reason)
                }
                None => SourceExitResolution::Matched(resolved_position.key),
            });
        }

        match position_resolver.resolve_owned_position(&matched_trade.entry, now) {
            ResolveResult::FoundOpen(resolved) | ResolveResult::FoundClosing(resolved) => {
                return Ok(position_resolution_from_record(resolved));
            }
            ResolveResult::FoundPendingOpen(resolved) => {
                let bind =
                    position_resolver.bind_exit_to_pending(&resolved.key, matched_trade.clone())?;
                return Ok(SourceExitResolution::BoundToPending(
                    BoundPendingDisposition {
                        position_key: resolved.key,
                        already_bound: bind.already_bound,
                        bound_exit_count: bind.bound_exit_count,
                    },
                ));
            }
            ResolveResult::Ambiguous => {
                return Ok(SourceExitResolution::Failed(
                    "resolver_ambiguous_same_wallet_same_condition".to_owned(),
                ));
            }
            ResolveResult::NotFound => {}
        }

        if let Some(non_actionable) = non_actionable_exit(
            snapshot,
            position_registry,
            position_resolver,
            &matched_trade.entry,
            now,
        ) {
            return Ok(SourceExitResolution::NotActionable(non_actionable));
        }

        let requested_key = matched_trade.entry.position_key();
        let reason =
            unresolved_exit_reason(position_registry, position_resolver, &requested_key, now);
        let retry = self
            .enqueue_retry(&matched_trade.entry, &reason, now)
            .await?;
        Ok(SourceExitResolution::DeferredRetry(retry))
    }

    pub async fn enqueue_unresolved_exit(
        &self,
        entry: &ActivityEntry,
        reason: &str,
        now: DateTime<Utc>,
    ) -> Result<RetryDisposition> {
        self.enqueue_retry(entry, reason, now).await
    }

    pub async fn due_retry_keys(&self, now: DateTime<Utc>) -> Vec<String> {
        self.pending
            .lock()
            .await
            .iter()
            .filter(|(_, intent)| intent.next_retry_at <= now)
            .map(|(retry_key, _)| retry_key.clone())
            .collect()
    }

    pub async fn retry_event(&self, retry_key: &str) -> Option<ActivityEntry> {
        self.pending
            .lock()
            .await
            .get(retry_key)
            .map(|intent| intent.event.clone())
    }

    pub async fn has_pending_source_exit_for_position(&self, position_key: &PositionKey) -> bool {
        self.pending.lock().await.values().any(|intent| {
            intent.source_wallet == position_key.source_wallet
                && intent.condition_id == position_key.condition_id
                && intent.outcome == position_key.outcome
        })
    }

    pub async fn retry_unresolved_exit(
        &self,
        retry_key: &str,
        snapshot: &PortfolioSnapshot,
        position_registry: &PositionRegistry,
        position_resolver: &PositionResolver,
        now: DateTime<Utc>,
    ) -> Result<SourceExitResolution> {
        let event = {
            let pending = self.pending.lock().await;
            pending.get(retry_key).map(|intent| intent.event.clone())
        };
        let Some(event) = event else {
            return Ok(SourceExitResolution::Failed(format!(
                "unresolved exit retry key {retry_key} is no longer queued"
            )));
        };

        if let Some(resolved_position) = snapshot.resolve_position_to_sell(&event) {
            self.remove_retry(retry_key).await?;
            return Ok(match resolved_position.fallback_reason {
                Some(fallback_reason) => {
                    SourceExitResolution::MatchedByFallback(resolved_position.key, fallback_reason)
                }
                None => SourceExitResolution::Matched(resolved_position.key),
            });
        }

        match position_resolver.resolve_owned_position(&event, now) {
            ResolveResult::FoundOpen(resolved) | ResolveResult::FoundClosing(resolved) => {
                self.remove_retry(retry_key).await?;
                return Ok(position_resolution_from_record(resolved));
            }
            ResolveResult::FoundPendingOpen(resolved) => {
                let bind = position_resolver
                    .bind_exit_to_pending(&resolved.key, matched_trade_from_entry(&event))?;
                self.remove_retry(retry_key).await?;
                return Ok(SourceExitResolution::BoundToPending(
                    BoundPendingDisposition {
                        position_key: resolved.key,
                        already_bound: bind.already_bound,
                        bound_exit_count: bind.bound_exit_count,
                    },
                ));
            }
            ResolveResult::Ambiguous => {
                self.mark_exit_resolution_failed(
                    retry_key,
                    "resolver_ambiguous_same_wallet_same_condition",
                )
                .await?;
                return Ok(SourceExitResolution::Failed(
                    "resolver_ambiguous_same_wallet_same_condition".to_owned(),
                ));
            }
            ResolveResult::NotFound => {}
        }

        if let Some(non_actionable) =
            non_actionable_exit(snapshot, position_registry, position_resolver, &event, now)
        {
            self.remove_retry(retry_key).await?;
            return Ok(SourceExitResolution::NotActionable(non_actionable));
        }

        let requested_key = event.position_key();
        let reason =
            unresolved_exit_reason(position_registry, position_resolver, &requested_key, now);
        let mut pending = self.pending.lock().await;
        let Some(intent) = pending.get_mut(retry_key) else {
            return Ok(SourceExitResolution::Failed(format!(
                "unresolved exit retry key {retry_key} disappeared during retry"
            )));
        };
        if now >= intent.expires_at {
            let failure_reason = format!("exit resolution expired: {}", intent.last_reason);
            pending.remove(retry_key);
            self.persist_locked(&pending)?;
            return Ok(SourceExitResolution::Failed(failure_reason));
        }

        intent.last_seen_at = now;
        intent.attempts = intent.attempts.saturating_add(1);
        intent.current_retry_interval_ms = next_retry_interval_ms(
            self.initial_retry_interval,
            self.max_retry_interval,
            intent.attempts,
        );
        intent.next_retry_at =
            now + chrono::TimeDelta::milliseconds(intent.current_retry_interval_ms as i64);
        intent.last_reason = reason.clone();
        self.persist_locked(&pending)?;
        Ok(SourceExitResolution::DeferredRetry(RetryDisposition {
            retry_key: retry_key.to_owned(),
            reason,
            already_queued: true,
        }))
    }

    pub async fn mark_exit_resolution_failed(&self, retry_key: &str, _reason: &str) -> Result<()> {
        let mut pending = self.pending.lock().await;
        if pending.remove(retry_key).is_some() {
            self.persist_locked(&pending)?;
        }
        Ok(())
    }

    #[cfg(test)]
    pub async fn pending_intent(&self, retry_key: &str) -> Option<PendingExitIntent> {
        self.pending.lock().await.get(retry_key).cloned()
    }

    async fn enqueue_retry(
        &self,
        entry: &ActivityEntry,
        reason: &str,
        now: DateTime<Utc>,
    ) -> Result<RetryDisposition> {
        let retry_key = exit_intent_key(entry, self.initial_retry_interval);
        let mut pending = self.pending.lock().await;
        let expires_at = now
            + chrono::TimeDelta::from_std(self.retry_window)
                .unwrap_or_else(|_| chrono::TimeDelta::zero());
        let initial_retry_interval_ms =
            next_retry_interval_ms(self.initial_retry_interval, self.max_retry_interval, 0);
        let next_retry_at = now + chrono::TimeDelta::milliseconds(initial_retry_interval_ms as i64);

        let already_queued = match pending.get_mut(&retry_key) {
            Some(intent) if intent.expires_at > now => {
                intent.last_seen_at = now;
                intent.last_reason = reason.to_owned();
                true
            }
            Some(intent) => {
                *intent = PendingExitIntent {
                    retry_key: retry_key.clone(),
                    source_wallet: requested_wallet(entry),
                    condition_id: entry.condition_id.clone(),
                    outcome: entry.position_key().outcome,
                    side: entry.side.to_ascii_uppercase(),
                    first_seen_at: now,
                    last_seen_at: now,
                    next_retry_at,
                    expires_at,
                    attempts: 0,
                    current_retry_interval_ms: initial_retry_interval_ms,
                    last_reason: reason.to_owned(),
                    event: entry.clone(),
                };
                false
            }
            None => {
                pending.insert(
                    retry_key.clone(),
                    PendingExitIntent {
                        retry_key: retry_key.clone(),
                        source_wallet: requested_wallet(entry),
                        condition_id: entry.condition_id.clone(),
                        outcome: entry.position_key().outcome,
                        side: entry.side.to_ascii_uppercase(),
                        first_seen_at: now,
                        last_seen_at: now,
                        next_retry_at,
                        expires_at,
                        attempts: 0,
                        current_retry_interval_ms: initial_retry_interval_ms,
                        last_reason: reason.to_owned(),
                        event: entry.clone(),
                    },
                );
                false
            }
        };

        self.persist_locked(&pending)?;
        Ok(RetryDisposition {
            retry_key,
            reason: reason.to_owned(),
            already_queued,
        })
    }

    async fn remove_retry(&self, retry_key: &str) -> Result<()> {
        let mut pending = self.pending.lock().await;
        if pending.remove(retry_key).is_some() {
            self.persist_locked(&pending)?;
        }
        Ok(())
    }

    fn persist_locked(&self, pending: &HashMap<String, PendingExitIntent>) -> Result<()> {
        let mut intents = pending.values().cloned().collect::<Vec<_>>();
        intents.sort_by(|left, right| left.retry_key.cmp(&right.retry_key));
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating {}", parent.display()))?;
        }
        std::fs::write(&*self.path, serde_json::to_string_pretty(&intents)?)
            .with_context(|| format!("writing {}", self.path.display()))?;
        Ok(())
    }
}

fn load_pending_exits(path: &PathBuf) -> Result<HashMap<String, PendingExitIntent>> {
    let now = Utc::now();
    let intents = match std::fs::read_to_string(path) {
        Ok(contents) => serde_json::from_str::<Vec<PendingExitIntent>>(&contents)
            .with_context(|| format!("parsing {}", path.display()))?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(error) => return Err(error).with_context(|| format!("reading {}", path.display())),
    };

    Ok(intents
        .into_iter()
        .filter(|intent| intent.expires_at > now && !position_outcome_is_unknown(&intent.outcome))
        .map(|intent| (intent.retry_key.clone(), intent))
        .collect())
}

fn non_actionable_exit(
    snapshot: &PortfolioSnapshot,
    position_registry: &PositionRegistry,
    position_resolver: &PositionResolver,
    entry: &ActivityEntry,
    now: DateTime<Utc>,
) -> Option<NonActionableExit> {
    let requested_key = entry.position_key();
    if !entry.side.eq_ignore_ascii_case("SELL") {
        return Some(NonActionableExit {
            reason: "source_exit_normalization_failed",
            detail: format!("source exit side is not SELL: {}", entry.side),
        });
    }
    if requested_key.source_wallet.is_empty() {
        return Some(NonActionableExit {
            reason: "source_exit_normalization_failed",
            detail: "source exit wallet is empty after normalization".to_owned(),
        });
    }
    if requested_key.condition_id.trim().is_empty()
        || requested_key.condition_id.eq_ignore_ascii_case("UNKNOWN")
    {
        return Some(NonActionableExit {
            reason: "source_exit_normalization_failed",
            detail: "source exit condition_id is missing or UNKNOWN".to_owned(),
        });
    }
    if position_outcome_is_unknown(&requested_key.outcome) {
        return Some(NonActionableExit {
            reason: "source_exit_normalization_failed",
            detail: format!(
                "source exit outcome is not PositionKey-actionable: {}",
                entry.outcome
            ),
        });
    }

    let has_candidate = snapshot.active_position_count_for_wallet_condition(
        &requested_key.source_wallet,
        &requested_key.condition_id,
    ) > 0
        || position_resolver.candidate_count_for_source_condition(
            &requested_key.source_wallet,
            &requested_key.condition_id,
            now,
        ) > 0
        || position_registry.has_open_position_for_wallet_market(
            &requested_key.source_wallet,
            &requested_key.condition_id,
        );
    if !has_candidate {
        return Some(NonActionableExit {
            reason: "source_exit_not_owned",
            detail: format!(
                "no pending/open/closing copied position candidate for wallet {} condition {}",
                requested_key.source_wallet, requested_key.condition_id
            ),
        });
    }

    None
}

fn unresolved_exit_reason(
    position_registry: &PositionRegistry,
    position_resolver: &PositionResolver,
    requested_key: &PositionKey,
    now: DateTime<Utc>,
) -> String {
    if position_resolver.has_resolvable_position(requested_key) {
        match position_resolver.resolve_owned_position(
            &ActivityEntry {
                proxy_wallet: requested_key.source_wallet.clone(),
                timestamp: now.timestamp_millis(),
                condition_id: requested_key.condition_id.clone(),
                type_name: "TRADE".to_owned(),
                size: 0.0,
                usdc_size: 0.0,
                transaction_hash: "resolver-check".to_owned(),
                price: 0.0,
                asset: String::new(),
                side: "SELL".to_owned(),
                outcome_index: 0,
                title: String::new(),
                slug: String::new(),
                event_slug: String::new(),
                outcome: requested_key.outcome.clone(),
            },
            now,
        ) {
            ResolveResult::FoundPendingOpen(_) => {
                "owned_position_pending_open_waiting_for_fill".to_owned()
            }
            ResolveResult::FoundOpen(_) | ResolveResult::FoundClosing(_) => {
                "owned_position_local_state_waiting_for_portfolio".to_owned()
            }
            ResolveResult::Ambiguous => "resolver_ambiguous_same_wallet_same_condition".to_owned(),
            ResolveResult::NotFound => "owned_position_not_resolved_yet".to_owned(),
        }
    } else if position_registry.has_open_position_key(requested_key) {
        "owned_position_exact_match_waiting_for_portfolio".to_owned()
    } else if let Some(resolved_position) = position_registry.resolve_open_position(requested_key) {
        resolved_position
            .fallback_reason
            .unwrap_or_else(|| "owned_position_safe_fallback_waiting_for_portfolio".to_owned())
    } else {
        "owned_position_not_resolved_yet".to_owned()
    }
}

fn position_resolution_from_record(resolved: ResolvedResolverPosition) -> SourceExitResolution {
    match resolved.fallback_reason {
        Some(fallback_reason) => {
            SourceExitResolution::MatchedByFallback(resolved.key, fallback_reason)
        }
        None => SourceExitResolution::Matched(resolved.key),
    }
}

fn matched_trade_from_entry(entry: &ActivityEntry) -> MatchedTrackedTrade {
    let now = Utc::now();
    let instant = std::time::Instant::now();
    MatchedTrackedTrade {
        entry: entry.clone(),
        signal: crate::detection::trade_inference::ConfirmedTradeSignal {
            asset_id: entry.asset.clone(),
            condition_id: entry.condition_id.clone(),
            transaction_hash: Some(entry.transaction_hash.clone()),
            side: normalized_execution_side(entry),
            price: entry.price,
            estimated_size: entry.size,
            stage_timestamps: crate::models::TradeStageTimestamps {
                websocket_event_received_at: instant,
                websocket_event_received_at_utc: now,
                parse_completed_at: instant,
                parse_completed_at_utc: now,
                detection_triggered_at: instant,
                detection_triggered_at_utc: now,
                attribution_completed_at: None,
                attribution_completed_at_utc: None,
                fast_risk_completed_at: None,
                fast_risk_completed_at_utc: None,
            },
            confirmed_at: now,
            generation: 0,
        },
        source: "deferred_exit_retry",
        validation_correlation_kind: None,
        validation_match_window: None,
        tx_hash_matched: true,
        validation_signal: None,
    }
}

fn normalized_execution_side(entry: &ActivityEntry) -> crate::execution::ExecutionSide {
    if entry.side.eq_ignore_ascii_case("SELL") {
        crate::execution::ExecutionSide::Sell
    } else {
        crate::execution::ExecutionSide::Buy
    }
}

fn next_retry_interval_ms(initial: Duration, max: Duration, attempts: u32) -> u64 {
    let initial_ms = initial.as_millis().max(1) as u64;
    let max_ms = max.as_millis().max(initial.as_millis().max(1)) as u64;
    let multiplier = 1_u64 << attempts.min(8);
    (initial_ms.saturating_mul(multiplier)).min(max_ms)
}

fn exit_intent_key(entry: &ActivityEntry, retry_interval: Duration) -> String {
    let timestamp_bucket =
        normalized_timestamp_ms(entry.timestamp) / retry_interval.as_millis().max(1) as i64;
    let requested_key = entry.position_key();
    format!(
        "{}:{}:{}:{}:{}:{}:{}",
        requested_key.source_wallet,
        stable_transaction_component(entry),
        stable_asset_component(entry),
        requested_key.condition_id,
        requested_key.outcome,
        entry.side.to_ascii_uppercase(),
        timestamp_bucket
    )
}

fn source_exit_dedupe_key(entry: &ActivityEntry, dedupe_window: Duration) -> String {
    let timestamp_bucket =
        normalized_timestamp_ms(entry.timestamp) / dedupe_window.as_millis().max(1) as i64;
    let requested_key = entry.position_key();
    format!(
        "{}:{}:{}:{}:{}:{}",
        requested_key.source_wallet,
        stable_transaction_component(entry),
        stable_asset_component(entry),
        requested_key.condition_id,
        entry.side.to_ascii_uppercase(),
        timestamp_bucket
    )
}

fn stable_transaction_component(entry: &ActivityEntry) -> String {
    let tx_hash = entry.transaction_hash.trim();
    if tx_hash.is_empty() {
        entry.dedupe_key()
    } else {
        tx_hash.to_ascii_lowercase()
    }
}

fn stable_asset_component(entry: &ActivityEntry) -> String {
    let asset = entry.asset.trim();
    if asset.is_empty() {
        entry.condition_id.clone()
    } else {
        asset.to_owned()
    }
}

fn normalized_timestamp_ms(timestamp: i64) -> i64 {
    if timestamp >= 10_000_000_000 {
        timestamp
    } else {
        timestamp.saturating_mul(1000)
    }
}

fn requested_wallet(entry: &ActivityEntry) -> String {
    entry.position_key().source_wallet
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use chrono::Utc;
    use rust_decimal_macros::dec;

    use super::*;
    use crate::config::Settings;
    use crate::models::{PortfolioPosition, PortfolioSnapshot, PositionState};

    fn sample_settings(data_dir: std::path::PathBuf) -> Settings {
        let mut settings = Settings::default_for_tests(data_dir);
        settings.polymarket_host = "https://example.com".to_owned();
        settings.polymarket_data_api = "https://example.com".to_owned();
        settings.polymarket_gamma_api = "https://example.com".to_owned();
        settings.polymarket_market_ws = "wss://example.com".to_owned();
        settings.polymarket_user_ws = "wss://example.com".to_owned();
        settings.polymarket_activity_ws = "wss://example.com".to_owned();
        settings.target_profile_addresses = vec!["0xsource".to_owned()];
        settings.hot_path_queue_capacity = 32;
        settings.cold_path_queue_capacity = 128;
        settings.attribution_fast_cache_capacity = 64;
        settings.market_subscription_batch_size = 1;
        settings.market_subscription_delay = Duration::from_millis(1);
        settings.wallet_subscription_batch_size = 1;
        settings.wallet_subscription_delay = Duration::from_millis(1);
        settings.min_top_of_book_ratio = dec!(1);
        settings.prediction_validation_timeout = Duration::from_millis(500);
        settings.exit_retry_window = Duration::from_secs(12);
        settings.exit_retry_interval = Duration::from_millis(250);
        settings.unresolved_exit_total_window = Duration::from_secs(12);
        settings
    }

    fn sample_entry(side: &str) -> ActivityEntry {
        ActivityEntry {
            proxy_wallet: "0xsource".to_owned(),
            timestamp: Utc::now().timestamp_millis(),
            condition_id: "condition-1".to_owned(),
            type_name: "TRADE".to_owned(),
            size: 10.0,
            usdc_size: 5.0,
            transaction_hash: format!("0xhash-{side}"),
            price: 0.5,
            asset: "asset-1".to_owned(),
            side: side.to_owned(),
            outcome_index: 0,
            title: "Sample".to_owned(),
            slug: "sample".to_owned(),
            event_slug: "sample".to_owned(),
            outcome: "YES".to_owned(),
        }
    }

    fn sample_entry_with_outcome(side: &str, outcome: &str) -> ActivityEntry {
        ActivityEntry {
            outcome: outcome.to_owned(),
            transaction_hash: format!("0xhash-{side}-{outcome}"),
            ..sample_entry(side)
        }
    }

    fn sample_snapshot() -> PortfolioSnapshot {
        PortfolioSnapshot {
            fetched_at: Utc::now(),
            total_value: dec!(200),
            total_exposure: dec!(5),
            cash_balance: dec!(195),
            realized_pnl: dec!(0),
            unrealized_pnl: dec!(0),
            positions: vec![PortfolioPosition {
                asset: "asset-1".to_owned(),
                condition_id: "condition-1".to_owned(),
                title: "Sample".to_owned(),
                outcome: "YES".to_owned(),
                source_wallet: "0xsource".to_owned(),
                state: PositionState::Open,
                size: dec!(10),
                current_value: dec!(5),
                average_entry_price: dec!(0.5),
                current_price: dec!(0.5),
                cost_basis: dec!(5),
                unrealized_pnl: dec!(0),
                opened_at: Some(Utc::now()),
                source_trade_timestamp_unix: Utc::now().timestamp_millis(),
                closing_started_at: None,
                closing_reason: None,
                last_close_attempt_at: None,
                close_attempts: 0,
                close_failure_reason: None,
                closing_escalation_level: 0,
                stale_reason: None,
            }],
            ..PortfolioSnapshot::default()
        }
    }

    #[tokio::test]
    async fn exact_source_exit_matches_owned_position_key() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let settings = sample_settings(temp_dir.path().to_path_buf());
        let buffer = ExitResolutionBuffer::load(&settings).expect("buffer");
        let registry = PositionRegistry::load(&settings).expect("registry");
        let resolver = PositionResolver::load(&settings).expect("resolver");
        let matched_trade = matched_trade_from_entry(&sample_entry("SELL"));

        let resolution = buffer
            .resolve_source_exit(
                &sample_snapshot(),
                &registry,
                &resolver,
                &matched_trade,
                Utc::now(),
            )
            .await
            .expect("resolution");

        assert!(matches!(
            resolution,
            SourceExitResolution::Matched(position_key)
                if position_key == PositionKey::new("condition-1", "YES", "0xsource")
        ));
    }

    #[tokio::test]
    async fn pending_exit_binds_without_entering_retry_loop() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let settings = sample_settings(temp_dir.path().to_path_buf());
        let buffer = ExitResolutionBuffer::load(&settings).expect("buffer");
        let registry = PositionRegistry::load(&settings).expect("registry");
        let resolver = PositionResolver::load(&settings).expect("resolver");
        let buy_entry = sample_entry("BUY");
        resolver
            .register_pending_open(buy_entry.position_key(), &buy_entry)
            .expect("register pending");

        let resolution = buffer
            .resolve_source_exit(
                &PortfolioSnapshot {
                    fetched_at: Utc::now(),
                    total_value: dec!(200),
                    total_exposure: dec!(0),
                    cash_balance: dec!(200),
                    realized_pnl: dec!(0),
                    unrealized_pnl: dec!(0),
                    positions: Vec::new(),
                    ..PortfolioSnapshot::default()
                },
                &registry,
                &resolver,
                &matched_trade_from_entry(&sample_entry("SELL")),
                Utc::now(),
            )
            .await
            .expect("resolution");

        assert!(matches!(
            resolution,
            SourceExitResolution::BoundToPending(BoundPendingDisposition {
                already_bound: false,
                ..
            })
        ));
    }

    #[tokio::test]
    async fn unknown_outcome_without_owned_candidate_is_not_queued() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let settings = sample_settings(temp_dir.path().to_path_buf());
        let buffer = ExitResolutionBuffer::load(&settings).expect("buffer");
        let registry = PositionRegistry::load(&settings).expect("registry");
        let resolver = PositionResolver::load(&settings).expect("resolver");
        let now = Utc::now();
        let entry = sample_entry_with_outcome("SELL", "UNKNOWN");

        let resolution = buffer
            .resolve_source_exit(
                &PortfolioSnapshot {
                    fetched_at: now,
                    total_value: dec!(200),
                    total_exposure: dec!(0),
                    cash_balance: dec!(200),
                    realized_pnl: dec!(0),
                    unrealized_pnl: dec!(0),
                    positions: Vec::new(),
                    ..PortfolioSnapshot::default()
                },
                &registry,
                &resolver,
                &matched_trade_from_entry(&entry),
                now,
            )
            .await
            .expect("resolution");

        match resolution {
            SourceExitResolution::NotActionable(non_actionable) => {
                assert_eq!(non_actionable.reason, "source_exit_normalization_failed");
            }
            other => panic!("expected non-actionable exit, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn sell_for_wallet_with_no_owned_position_is_not_queued() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let settings = sample_settings(temp_dir.path().to_path_buf());
        let buffer = ExitResolutionBuffer::load(&settings).expect("buffer");
        let registry = PositionRegistry::load(&settings).expect("registry");
        let resolver = PositionResolver::load(&settings).expect("resolver");
        let now = Utc::now();
        let entry = sample_entry("SELL");

        let resolution = buffer
            .resolve_source_exit(
                &PortfolioSnapshot {
                    fetched_at: now,
                    total_value: dec!(200),
                    total_exposure: dec!(0),
                    cash_balance: dec!(200),
                    realized_pnl: dec!(0),
                    unrealized_pnl: dec!(0),
                    positions: Vec::new(),
                    ..PortfolioSnapshot::default()
                },
                &registry,
                &resolver,
                &matched_trade_from_entry(&entry),
                now,
            )
            .await
            .expect("resolution");

        match resolution {
            SourceExitResolution::NotActionable(non_actionable) => {
                assert_eq!(non_actionable.reason, "source_exit_not_owned");
            }
            other => panic!("expected non-actionable not-owned exit, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn duplicate_source_exit_claim_is_suppressed() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let settings = sample_settings(temp_dir.path().to_path_buf());
        let buffer = ExitResolutionBuffer::load(&settings).expect("buffer");
        let entry = sample_entry("SELL");
        let now = Utc::now();

        assert!(buffer.claim_source_exit(&entry, now).await);
        assert!(
            !buffer
                .claim_source_exit(&entry, now + chrono::TimeDelta::milliseconds(100))
                .await
        );
    }

    #[tokio::test]
    async fn unresolved_exit_uses_stepped_backoff_only_for_owned_registry_candidate() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let settings = sample_settings(temp_dir.path().to_path_buf());
        let buffer = ExitResolutionBuffer::load(&settings).expect("buffer");
        let registry = PositionRegistry::load(&settings).expect("registry");
        registry
            .sync_from_portfolio(&sample_snapshot())
            .expect("seed registry candidate");
        let resolver = PositionResolver::load(&settings).expect("resolver");
        let entry = sample_entry("SELL");
        let now = Utc::now();

        let first = buffer
            .resolve_source_exit(
                &PortfolioSnapshot {
                    fetched_at: now,
                    total_value: dec!(200),
                    total_exposure: dec!(0),
                    cash_balance: dec!(200),
                    realized_pnl: dec!(0),
                    unrealized_pnl: dec!(0),
                    positions: Vec::new(),
                    ..PortfolioSnapshot::default()
                },
                &registry,
                &resolver,
                &matched_trade_from_entry(&entry),
                now,
            )
            .await
            .expect("queued");

        let retry_key = match first {
            SourceExitResolution::DeferredRetry(retry) => retry.retry_key,
            other => panic!("expected deferred retry, got {other:?}"),
        };

        let initial = buffer.pending_intent(&retry_key).await.expect("pending");
        assert_eq!(initial.current_retry_interval_ms, 250);

        let retried = buffer
            .retry_unresolved_exit(
                &retry_key,
                &PortfolioSnapshot {
                    fetched_at: now,
                    total_value: dec!(200),
                    total_exposure: dec!(0),
                    cash_balance: dec!(200),
                    realized_pnl: dec!(0),
                    unrealized_pnl: dec!(0),
                    positions: Vec::new(),
                    ..PortfolioSnapshot::default()
                },
                &registry,
                &resolver,
                now + chrono::TimeDelta::milliseconds(250),
            )
            .await
            .expect("retry");

        assert!(matches!(retried, SourceExitResolution::DeferredRetry(_)));
        let updated = buffer.pending_intent(&retry_key).await.expect("updated");
        assert_eq!(updated.current_retry_interval_ms, 500);
    }
}
