use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use anyhow::{Context, Result};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::config::Settings;
use crate::execution::{ExecutionResult, ExecutionSide, ExecutionStatus};
use crate::models::{ActivityEntry, PortfolioSnapshot, PositionKey};
use crate::rolling_jsonl::RollingJsonlLogger;
use crate::wallet::wallet_filter::normalize_wallet;

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub enum PositionLifecycleStatus {
    Open,
    Closed,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TrackedPosition {
    pub position_id: String,
    pub wallet: String,
    pub market: String,
    pub side: ExecutionSide,
    pub entry_price: f64,
    pub opened_at: i64,
    pub status: PositionLifecycleStatus,
    #[serde(default)]
    pub outcome: String,
    #[serde(default)]
    pub closed_at: Option<i64>,
}

#[derive(Clone)]
pub struct PositionRegistry {
    positions: Arc<RwLock<HashMap<String, TrackedPosition>>>,
    path: Arc<PathBuf>,
    lifecycle_logger: RollingJsonlLogger,
}

#[derive(Clone, Debug)]
pub struct ResolvedTrackedPosition {
    pub key: PositionKey,
    pub fallback_reason: Option<String>,
}

#[derive(Serialize)]
struct PositionLifecycleEvent<'a> {
    event_type: &'static str,
    logged_at: chrono::DateTime<Utc>,
    position_id: &'a str,
    wallet: &'a str,
    market: &'a str,
    side: ExecutionSide,
    entry_price: f64,
    status: PositionLifecycleStatus,
}

impl PositionRegistry {
    pub fn load(settings: &Settings) -> Result<Self> {
        let path = settings.data_dir.join("positions.json");
        let positions = load_positions(&path)?;
        Ok(Self {
            positions: Arc::new(RwLock::new(positions)),
            path: Arc::new(path),
            lifecycle_logger: RollingJsonlLogger::new(
                settings.data_dir.join("position-lifecycle.jsonl"),
                30_000,
            ),
        })
    }

    pub fn has_matching_open_position(&self, entry: &ActivityEntry) -> bool {
        self.resolve_open_position(&entry.position_key()).is_some()
    }

    pub fn has_open_position_key(&self, key: &PositionKey) -> bool {
        self.positions
            .read()
            .expect("position registry read lock")
            .values()
            .any(|position| {
                position.status == PositionLifecycleStatus::Open
                    && tracked_position_key(position) == *key
            })
    }

    pub fn resolve_open_position(&self, requested_key: &PositionKey) -> Option<ResolvedTrackedPosition> {
        let state = self.positions.read().expect("position registry read lock");
        if let Some(position) = state.values().find(|position| {
            position.status == PositionLifecycleStatus::Open
                && tracked_position_key(position) == *requested_key
        }) {
            return Some(ResolvedTrackedPosition {
                key: tracked_position_key(position),
                fallback_reason: None,
            });
        }

        let candidates = state
            .values()
            .filter(|position| {
                position.status == PositionLifecycleStatus::Open
                    && position.market == requested_key.condition_id
                    && position.wallet == requested_key.source_wallet
            })
            .collect::<Vec<_>>();
        if candidates.len() == 1 {
            return Some(ResolvedTrackedPosition {
                key: tracked_position_key(candidates[0]),
                fallback_reason: Some("same_wallet_same_condition_single_candidate".to_owned()),
            });
        }

        None
    }

    pub fn has_open_position_for_wallet(&self, wallet: &str) -> bool {
        let wallet = normalize_wallet(wallet);
        self.positions
            .read()
            .expect("position registry read lock")
            .values()
            .any(|position| {
                position.status == PositionLifecycleStatus::Open && position.wallet == wallet
            })
    }

    pub fn has_open_position_for_wallet_market(&self, wallet: &str, market: &str) -> bool {
        let wallet = normalize_wallet(wallet);
        self.positions
            .read()
            .expect("position registry read lock")
            .values()
            .any(|position| {
                position.status == PositionLifecycleStatus::Open
                    && position.wallet == wallet
                    && position.market == market
            })
    }

    pub fn open_wallets_for_market(&self, market: &str) -> Vec<String> {
        let mut wallets = self
            .positions
            .read()
            .expect("position registry read lock")
            .values()
            .filter(|position| {
                position.status == PositionLifecycleStatus::Open && position.market == market
            })
            .map(|position| position.wallet.clone())
            .collect::<Vec<_>>();
        wallets.sort();
        wallets.dedup();
        wallets
    }

    pub fn sync_from_portfolio(&self, snapshot: &PortfolioSnapshot) -> Result<()> {
        let mut state = self
            .positions
            .write()
            .expect("position registry write lock");
        for position in &snapshot.positions {
            if !position.is_active() {
                continue;
            }
            let position_id = position_id_from_key(&position.position_key());
            state.entry(position_id.clone()).or_insert(TrackedPosition {
                position_id,
                wallet: normalize_wallet(&position.source_wallet),
                market: position.condition_id.clone(),
                side: ExecutionSide::Buy,
                entry_price: position
                    .average_entry_price
                    .to_string()
                    .parse::<f64>()
                    .unwrap_or(0.0),
                opened_at: position
                    .opened_at
                    .map(|opened_at| opened_at.timestamp_millis())
                    .unwrap_or(position.source_trade_timestamp_unix),
                status: PositionLifecycleStatus::Open,
                outcome: position.outcome.clone(),
                closed_at: None,
            });
        }
        drop(state);
        self.persist()
    }

    pub async fn record_execution(
        &self,
        entry: &ActivityEntry,
        result: &ExecutionResult,
        position_key: Option<&PositionKey>,
        fully_closed: bool,
    ) -> Result<()> {
        if matches!(
            result.status,
            ExecutionStatus::NoFill | ExecutionStatus::Rejected
        ) || result.filled_size <= rust_decimal::Decimal::ZERO
        {
            return Ok(());
        }

        match result.order_request.side {
            ExecutionSide::Buy => self.open_position(entry, result).await,
            ExecutionSide::Sell => self.close_position(entry, position_key, fully_closed).await,
        }
    }

    async fn open_position(&self, entry: &ActivityEntry, result: &ExecutionResult) -> Result<()> {
        let position = TrackedPosition {
            position_id: position_id_from_entry(entry),
            wallet: normalize_wallet(&entry.proxy_wallet),
            market: entry.condition_id.clone(),
            side: ExecutionSide::Buy,
            entry_price: result
                .filled_price
                .to_string()
                .parse::<f64>()
                .unwrap_or(entry.price),
            opened_at: normalize_timestamp_ms(entry.timestamp),
            status: PositionLifecycleStatus::Open,
            outcome: entry.outcome.clone(),
            closed_at: None,
        };

        self.positions
            .write()
            .expect("position registry write lock")
            .insert(position.position_id.clone(), position.clone());
        self.persist()?;
        self.log_lifecycle("position_opened", &position).await;
        Ok(())
    }

    async fn close_position(
        &self,
        entry: &ActivityEntry,
        position_key: Option<&PositionKey>,
        fully_closed: bool,
    ) -> Result<()> {
        if !fully_closed {
            return Ok(());
        }

        let target_position_id = position_key
            .map(position_id_from_key)
            .unwrap_or_else(|| position_id_from_entry(entry));
        let fallback_position_id = self
            .resolve_open_position(position_key.unwrap_or(&entry.position_key()))
            .map(|position| position_id_from_key(&position.key));
        let mut closed_position = None;
        {
            let mut state = self
                .positions
                .write()
                .expect("position registry write lock");
            if let Some(position) = state.get_mut(&target_position_id) {
                position.status = PositionLifecycleStatus::Closed;
                position.closed_at = Some(Utc::now().timestamp_millis());
                closed_position = Some(position.clone());
            } else if let Some(fallback_position_id) = fallback_position_id.as_ref() {
                if let Some(position) = state.get_mut(fallback_position_id) {
                    position.status = PositionLifecycleStatus::Closed;
                    position.closed_at = Some(Utc::now().timestamp_millis());
                    closed_position = Some(position.clone());
                }
            }
        }

        if let Some(position) = closed_position {
            self.persist()?;
            self.log_lifecycle("position_closed", &position).await;
        }
        Ok(())
    }

    fn persist(&self) -> Result<()> {
        let mut positions = self
            .positions
            .read()
            .expect("position registry read lock")
            .values()
            .cloned()
            .collect::<Vec<_>>();
        positions.sort_by(|left, right| left.position_id.cmp(&right.position_id));
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating {}", parent.display()))?;
        }
        std::fs::write(&*self.path, serde_json::to_string_pretty(&positions)?)
            .with_context(|| format!("writing {}", self.path.display()))?;
        Ok(())
    }

    async fn log_lifecycle(&self, event_type: &'static str, position: &TrackedPosition) {
        if let Err(error) = self
            .lifecycle_logger
            .append(&PositionLifecycleEvent {
                event_type,
                logged_at: Utc::now(),
                position_id: &position.position_id,
                wallet: &position.wallet,
                market: &position.market,
                side: position.side,
                entry_price: position.entry_price,
                status: position.status,
            })
            .await
        {
            warn!(
                ?error,
                event = event_type,
                "failed to persist position lifecycle event"
            );
        }
    }
}

fn load_positions(path: &PathBuf) -> Result<HashMap<String, TrackedPosition>> {
    let positions = match std::fs::read_to_string(path) {
        Ok(contents) => serde_json::from_str::<Vec<TrackedPosition>>(&contents)
            .with_context(|| format!("parsing {}", path.display()))?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(error) => return Err(error).with_context(|| format!("reading {}", path.display())),
    };

    Ok(positions
        .into_iter()
        .map(|position| (position.position_id.clone(), position))
        .collect())
}

fn position_id_from_entry(entry: &ActivityEntry) -> String {
    position_id_from_key(&entry.position_key())
}

fn position_id_from_key(key: &PositionKey) -> String {
    format!("{}:{}:{}", key.condition_id, key.outcome, key.source_wallet)
}

fn tracked_position_key(position: &TrackedPosition) -> PositionKey {
    PositionKey::new(&position.market, &position.outcome, &position.wallet)
}

fn normalize_timestamp_ms(timestamp: i64) -> i64 {
    if timestamp >= 10_000_000_000 {
        timestamp
    } else {
        timestamp.saturating_mul(1000)
    }
}
