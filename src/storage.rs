use std::collections::{HashSet, VecDeque};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use tokio::sync::{Notify, RwLock};
use tokio::time::{MissedTickBehavior, interval};
use tracing::warn;

use crate::models::ActivityEntry;

const MAX_SEEN_KEYS: usize = 4096;
const MAX_SEEN_TX_HASHES: usize = 4096;
const CURSOR_SAFETY_WINDOW_SECONDS: i64 = 2;
const TIMESTAMP_MILLIS_THRESHOLD: i64 = 10_000_000_000;

#[derive(Clone)]
pub struct StateStore {
    inner: Arc<RwLock<RuntimeState>>,
    path: Arc<std::path::PathBuf>,
    dirty: Arc<AtomicBool>,
    flush_notify: Arc<Notify>,
}

#[derive(Debug, Default)]
struct RuntimeState {
    persisted: PersistedState,
    pending_keys: HashSet<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
struct PersistedState {
    seen_keys: VecDeque<String>,
    #[serde(default)]
    seen_tx_hashes: VecDeque<String>,
    #[serde(default)]
    latest_source_timestamp: i64,
    #[serde(default, alias = "last_activity_timestamp")]
    last_source_timestamp: i64,
    updated_at: String,
}

impl StateStore {
    #[allow(dead_code)]
    pub async fn load(data_dir: &Path) -> Result<Self> {
        Self::load_with_flush(data_dir, Duration::from_millis(250)).await
    }

    pub async fn load_with_flush(data_dir: &Path, flush_interval: Duration) -> Result<Self> {
        let path = data_dir.join("state.json");
        let mut persisted = match tokio::fs::read_to_string(&path).await {
            Ok(contents) => serde_json::from_str::<PersistedState>(&contents)
                .with_context(|| format!("parsing {}", path.display()))?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => PersistedState::default(),
            Err(error) => return Err(error).with_context(|| format!("reading {}", path.display())),
        };
        persisted.last_source_timestamp =
            normalize_source_timestamp(persisted.last_source_timestamp);
        persisted.latest_source_timestamp =
            normalize_source_timestamp(if persisted.latest_source_timestamp > 0 {
                persisted.latest_source_timestamp
            } else {
                persisted.last_source_timestamp
            });
        if persisted.latest_source_timestamp < persisted.last_source_timestamp {
            persisted.latest_source_timestamp = persisted.last_source_timestamp;
        }
        if persisted.latest_source_timestamp > 0 {
            persisted.last_source_timestamp = cursor_from_latest(persisted.latest_source_timestamp);
        }

        let store = Self {
            inner: Arc::new(RwLock::new(RuntimeState {
                persisted,
                pending_keys: HashSet::new(),
            })),
            path: Arc::new(path),
            dirty: Arc::new(AtomicBool::new(false)),
            flush_notify: Arc::new(Notify::new()),
        };
        store.spawn_flusher(flush_interval);
        Ok(store)
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub async fn claim_if_unseen(&self, entry: &ActivityEntry) -> bool {
        let key = entry.dedupe_key();
        let mut state = self.inner.write().await;
        if state
            .persisted
            .seen_keys
            .iter()
            .any(|existing| existing == &key)
            || state.pending_keys.contains(&key)
        {
            return false;
        }
        state.pending_keys.insert(key);
        true
    }

    pub async fn mark_seen(&self, entry: &ActivityEntry) -> Result<()> {
        let mut state = self.inner.write().await;
        let key = entry.dedupe_key();
        state.pending_keys.remove(&key);
        if !state
            .persisted
            .seen_keys
            .iter()
            .any(|existing| existing == &key)
        {
            state.persisted.seen_keys.push_front(key);
        }
        while state.persisted.seen_keys.len() > MAX_SEEN_KEYS {
            state.persisted.seen_keys.pop_back();
        }
        apply_source_observation(
            &mut state.persisted,
            normalize_source_timestamp(entry.timestamp),
            Some(entry.transaction_hash.as_str()),
        );
        drop(state);
        self.schedule_persist();
        Ok(())
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub async fn release_pending(&self, entry: &ActivityEntry) {
        let key = entry.dedupe_key();
        self.inner.write().await.pending_keys.remove(&key);
    }

    pub async fn has_seen_tx_hash(&self, tx_hash: &str) -> bool {
        let normalized = normalize_tx_hash(tx_hash);
        if normalized.is_empty() {
            return false;
        }
        self.inner
            .read()
            .await
            .persisted
            .seen_tx_hashes
            .iter()
            .any(|existing| existing == &normalized)
    }

    pub async fn record_activity_observation(
        &self,
        timestamp: i64,
        tx_hash: Option<&str>,
    ) -> Result<()> {
        let mut state = self.inner.write().await;
        apply_source_observation(
            &mut state.persisted,
            normalize_source_timestamp(timestamp),
            tx_hash,
        );
        drop(state);
        self.schedule_persist();
        Ok(())
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub async fn ensure_source_cursor_at_least(&self, timestamp: i64) -> Result<bool> {
        let mut state = self.inner.write().await;
        let normalized_timestamp = normalize_source_timestamp(timestamp);
        if state.persisted.latest_source_timestamp >= normalized_timestamp {
            return Ok(false);
        }
        apply_source_observation(&mut state.persisted, normalized_timestamp, None);
        drop(state);
        self.schedule_persist();
        Ok(true)
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub async fn last_source_timestamp(&self) -> i64 {
        self.inner.read().await.persisted.last_source_timestamp
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub async fn flush_now(&self) -> Result<()> {
        let persisted = self.inner.read().await.persisted.clone();
        persist_state(&self.path, &persisted).await?;
        self.dirty.store(false, Ordering::Relaxed);
        Ok(())
    }

    fn schedule_persist(&self) {
        self.dirty.store(true, Ordering::Relaxed);
        self.flush_notify.notify_one();
    }

    fn spawn_flusher(&self, flush_interval: Duration) {
        let inner = self.inner.clone();
        let path = self.path.clone();
        let dirty = self.dirty.clone();
        let flush_notify = self.flush_notify.clone();
        tokio::spawn(async move {
            let mut ticker = interval(flush_interval.max(Duration::from_millis(50)));
            ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = ticker.tick() => {}
                    _ = flush_notify.notified() => {}
                }
                if !dirty.swap(false, Ordering::Relaxed) {
                    continue;
                }
                let persisted = inner.read().await.persisted.clone();
                if let Err(error) = persist_state(&path, &persisted).await {
                    warn!(?error, path = %path.display(), "failed to persist runtime state");
                    dirty.store(true, Ordering::Relaxed);
                }
            }
        });
    }
}

async fn persist_state(path: &Path, persisted: &PersistedState) -> Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("creating {}", parent.display()))?;
    }
    let contents = serde_json::to_string_pretty(persisted)?;
    tokio::fs::write(path, contents)
        .await
        .with_context(|| format!("writing {}", path.display()))?;
    Ok(())
}

fn normalize_source_timestamp(timestamp: i64) -> i64 {
    if timestamp >= TIMESTAMP_MILLIS_THRESHOLD {
        timestamp / 1000
    } else {
        timestamp
    }
}

fn normalize_tx_hash(tx_hash: &str) -> String {
    tx_hash.trim().to_ascii_lowercase()
}

fn cursor_from_latest(latest_source_timestamp: i64) -> i64 {
    latest_source_timestamp.saturating_sub(CURSOR_SAFETY_WINDOW_SECONDS)
}

fn apply_source_observation(
    persisted: &mut PersistedState,
    normalized_timestamp: i64,
    tx_hash: Option<&str>,
) {
    if normalized_timestamp > 0 {
        persisted.latest_source_timestamp =
            persisted.latest_source_timestamp.max(normalized_timestamp);
        persisted.last_source_timestamp = cursor_from_latest(persisted.latest_source_timestamp);
    }

    if let Some(tx_hash) = tx_hash {
        let normalized = normalize_tx_hash(tx_hash);
        if !normalized.is_empty()
            && !persisted
                .seen_tx_hashes
                .iter()
                .any(|existing| existing == &normalized)
        {
            persisted.seen_tx_hashes.push_front(normalized);
            while persisted.seen_tx_hashes.len() > MAX_SEEN_TX_HASHES {
                persisted.seen_tx_hashes.pop_back();
            }
        }
    }

    persisted.updated_at = Utc::now().to_rfc3339();
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn sample_activity() -> ActivityEntry {
        ActivityEntry {
            proxy_wallet: "0xsource".to_owned(),
            timestamp: 1_773_670_185,
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

    #[tokio::test]
    async fn persists_source_cursor_seed() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let store = StateStore::load(temp_dir.path()).await.expect("load store");

        assert!(
            store
                .ensure_source_cursor_at_least(1_234_567)
                .await
                .expect("seed cursor")
        );
        assert_eq!(store.last_source_timestamp().await, 1_234_565);
        store.flush_now().await.expect("flush state");

        let reloaded = StateStore::load(temp_dir.path())
            .await
            .expect("reload store");
        assert_eq!(reloaded.last_source_timestamp().await, 1_234_565);
    }

    #[tokio::test]
    async fn normalizes_millisecond_cursor_on_load() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let path = temp_dir.path().join("state.json");
        tokio::fs::write(
            &path,
            r#"{
  "seen_keys": [],
  "last_activity_timestamp": 1773670185176,
  "updated_at": "2026-03-16T14:09:45Z"
}"#,
        )
        .await
        .expect("write state file");

        let store = StateStore::load(temp_dir.path()).await.expect("load store");

        assert_eq!(store.last_source_timestamp().await, 1_773_670_183);
    }

    #[tokio::test]
    async fn claim_blocks_duplicate_pending_trade_until_released() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let store = StateStore::load(temp_dir.path()).await.expect("load store");
        let activity = sample_activity();

        assert!(store.claim_if_unseen(&activity).await);
        assert!(!store.claim_if_unseen(&activity).await);

        store.release_pending(&activity).await;

        assert!(store.claim_if_unseen(&activity).await);
    }

    #[tokio::test]
    async fn records_seen_tx_hashes_from_activity_observations() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let store = StateStore::load(temp_dir.path()).await.expect("load store");

        store
            .record_activity_observation(1_773_670_185, Some("0xABC"))
            .await
            .expect("record activity observation");

        assert!(store.has_seen_tx_hash("0xabc").await);
        assert_eq!(store.last_source_timestamp().await, 1_773_670_183);
    }

    #[tokio::test]
    async fn background_flush_persists_without_blocking_state_updates() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let store = StateStore::load_with_flush(temp_dir.path(), Duration::from_millis(250))
            .await
            .expect("load store");

        store
            .mark_seen(&sample_activity())
            .await
            .expect("mark seen");
        assert!(store.has_seen_tx_hash("0xhash").await);
        assert!(!temp_dir.path().join("state.json").exists());

        store.flush_now().await.expect("flush state");
        assert!(temp_dir.path().join("state.json").exists());
    }
}
