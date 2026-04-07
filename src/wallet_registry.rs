use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use anyhow::{Context, Result};
use chrono::Utc;
use serde::{Deserialize, Serialize};

use crate::config::Settings;
use crate::wallet::wallet_filter::normalize_wallet;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct WalletMeta {
    pub address: String,
    pub score: f64,
    pub last_seen: i64,
    pub active: bool,
    #[serde(default)]
    pub inactive_since: Option<i64>,
}

#[derive(Clone)]
pub struct WalletRegistry {
    wallets: Arc<RwLock<HashMap<String, WalletMeta>>>,
    path: Arc<PathBuf>,
}

impl WalletRegistry {
    pub fn load(settings: &Settings) -> Result<Self> {
        let path = settings.data_dir.join("wallets_active.json");
        let mut wallets = load_wallets(&path)?;
        for wallet in &settings.target_profile_addresses {
            let normalized = normalize_wallet(wallet);
            wallets.entry(normalized.clone()).or_insert(WalletMeta {
                address: normalized,
                score: 1.0,
                last_seen: 0,
                active: true,
                inactive_since: None,
            });
        }

        let registry = Self {
            wallets: Arc::new(RwLock::new(wallets)),
            path: Arc::new(path),
        };
        registry.persist()?;
        Ok(registry)
    }

    pub fn is_tracked(&self, wallet: &str) -> bool {
        self.get_wallet_meta(wallet).is_some_and(|meta| meta.active)
    }

    pub fn get_wallet_meta(&self, wallet: &str) -> Option<WalletMeta> {
        self.wallets
            .read()
            .expect("wallet registry read lock")
            .get(&normalize_wallet(wallet))
            .cloned()
    }

    pub fn active_wallets(&self) -> Vec<String> {
        let mut wallets = self
            .wallets
            .read()
            .expect("wallet registry read lock")
            .values()
            .filter(|meta| meta.active)
            .cloned()
            .collect::<Vec<_>>();
        wallets.sort_by(|left, right| {
            right
                .score
                .total_cmp(&left.score)
                .then_with(|| right.last_seen.cmp(&left.last_seen))
        });
        wallets.into_iter().map(|meta| meta.address).collect()
    }

    pub fn known_wallets(&self) -> Vec<String> {
        let mut wallets = self
            .wallets
            .read()
            .expect("wallet registry read lock")
            .values()
            .map(|meta| meta.address.clone())
            .collect::<Vec<_>>();
        wallets.sort();
        wallets
    }

    pub fn update_wallets(&self, new_wallets: Vec<WalletMeta>) -> Result<()> {
        let mut state = self.wallets.write().expect("wallet registry write lock");
        let now_ms = Utc::now().timestamp_millis();
        let mut seen = HashSet::new();

        for wallet in new_wallets {
            let normalized = normalize_wallet(&wallet.address);
            if normalized.is_empty() {
                continue;
            }
            seen.insert(normalized.clone());
            let entry = state.entry(normalized.clone()).or_insert(WalletMeta {
                address: normalized.clone(),
                score: wallet.score,
                last_seen: wallet.last_seen,
                active: true,
                inactive_since: None,
            });
            entry.address = normalized;
            entry.score = wallet.score;
            entry.last_seen = wallet.last_seen;
            entry.active = true;
            entry.inactive_since = None;
        }

        for wallet in state.values_mut() {
            if !seen.contains(&wallet.address) && wallet.active {
                wallet.active = false;
                wallet.inactive_since = Some(now_ms);
            }
        }

        drop(state);
        self.persist()
    }

    #[allow(dead_code)]
    pub fn mark_inactive(&self, wallet: &str) -> Result<()> {
        let normalized = normalize_wallet(wallet);
        if normalized.is_empty() {
            return Ok(());
        }

        let mut state = self.wallets.write().expect("wallet registry write lock");
        if let Some(meta) = state.get_mut(&normalized) {
            meta.active = false;
            meta.inactive_since = Some(Utc::now().timestamp_millis());
        }
        drop(state);
        self.persist()
    }

    fn persist(&self) -> Result<()> {
        let mut wallets = self
            .wallets
            .read()
            .expect("wallet registry read lock")
            .values()
            .cloned()
            .collect::<Vec<_>>();
        wallets.sort_by(|left, right| left.address.cmp(&right.address));
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating {}", parent.display()))?;
        }
        std::fs::write(&*self.path, serde_json::to_string_pretty(&wallets)?)
            .with_context(|| format!("writing {}", self.path.display()))?;
        Ok(())
    }
}

fn load_wallets(path: &PathBuf) -> Result<HashMap<String, WalletMeta>> {
    let wallets = match std::fs::read_to_string(path) {
        Ok(contents) => serde_json::from_str::<Vec<WalletMeta>>(&contents)
            .with_context(|| format!("parsing {}", path.display()))?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(error) => return Err(error).with_context(|| format!("reading {}", path.display())),
    };

    Ok(wallets
        .into_iter()
        .filter_map(|wallet| {
            let normalized = normalize_wallet(&wallet.address);
            (!normalized.is_empty()).then_some((
                normalized.clone(),
                WalletMeta {
                    address: normalized,
                    ..wallet
                },
            ))
        })
        .collect())
}
