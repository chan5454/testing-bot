use std::str::FromStr;
use std::time::{Duration, Instant};

use alloy::primitives::{Address, U256};
use alloy::providers::{Provider, ProviderBuilder};
use alloy::signers::{Signer, local::PrivateKeySigner};
use alloy::sol;
use anyhow::{Result, anyhow};
use futures_util::future::join_all;
use polymarket_client_sdk::POLYGON;
use rust_decimal::Decimal;
use tracing::{info, warn};

use crate::config::AppConfig;

const MATIC_DECIMALS: u32 = 18;
const USDC_DECIMALS: u32 = 6;

sol! {
    #[sol(rpc)]
    interface IERC20 {
        function balanceOf(address owner) external view returns (uint256);
        function allowance(address owner, address spender) external view returns (uint256);
        function approve(address spender, uint256 value) external returns (bool);
    }
}

#[derive(Clone, Debug)]
pub struct WalletReadinessReport {
    pub chain_id_ok: bool,
    pub private_key_ok: bool,
    pub signer_address: String,
    pub profile_address_ok: bool,
    pub funder_address_ok: bool,
    pub usdc_balance_ok: bool,
    pub matic_balance_ok: bool,
    pub allowance_ok: bool,
    pub ready_for_live_trading: bool,
    pub notes: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct WalletReadinessSnapshot {
    pub report: WalletReadinessReport,
    pub execution_flow: &'static str,
    pub profile_address: Option<String>,
    pub funder_address: Option<String>,
    pub trading_address: Option<String>,
    pub matic_balance: Decimal,
    pub usdc_balance: Decimal,
    pub allowance: Option<Decimal>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WalletFlow {
    Eoa,
    Proxy,
}

impl WalletFlow {
    fn as_str(self) -> &'static str {
        match self {
            Self::Eoa => "EOA",
            Self::Proxy => "PROXY",
        }
    }
}

pub async fn check_wallet_readiness(settings: &AppConfig) -> Result<WalletReadinessReport> {
    Ok(inspect_wallet_readiness(settings).await?.report)
}

pub async fn inspect_wallet_readiness(settings: &AppConfig) -> Result<WalletReadinessSnapshot> {
    let flow = wallet_flow(settings);
    let mut snapshot = WalletReadinessSnapshot {
        report: WalletReadinessReport {
            chain_id_ok: false,
            private_key_ok: false,
            signer_address: String::new(),
            profile_address_ok: false,
            funder_address_ok: false,
            usdc_balance_ok: false,
            matic_balance_ok: false,
            allowance_ok: false,
            ready_for_live_trading: false,
            notes: vec![format!("wallet_flow={}", flow.as_str())],
        },
        execution_flow: flow.as_str(),
        profile_address: settings.polymarket_profile_address.clone(),
        funder_address: settings.polymarket_funder_address.clone(),
        trading_address: None,
        matic_balance: Decimal::ZERO,
        usdc_balance: Decimal::ZERO,
        allowance: None,
    };
    if settings.execution_mode.is_paper() {
        snapshot.report.chain_id_ok = true;
        snapshot.report.private_key_ok = true;
        snapshot.report.profile_address_ok = true;
        snapshot.report.funder_address_ok = true;
        snapshot.report.usdc_balance_ok = true;
        snapshot.report.matic_balance_ok = true;
        snapshot.report.allowance_ok = true;
        snapshot.report.ready_for_live_trading = true;
        snapshot.report.notes.push(
            "paper mode: skipped wallet readiness, RPC calls, and allowance checks".to_owned(),
        );
        return Ok(snapshot);
    }
    let rpc = WalletReadinessRpc::new(settings).await?;
    snapshot
        .report
        .notes
        .push(format!("rpc_endpoint={}", rpc.selected_rpc_url()));
    snapshot.report.notes.push(format!(
        "rpc_latency_ms={}",
        rpc.selected_latency().as_millis()
    ));

    let signer = match settings.polymarket_private_key.as_deref() {
        Some(private_key) => match PrivateKeySigner::from_str(private_key) {
            Ok(signer) => {
                snapshot.report.private_key_ok = true;
                snapshot.report.signer_address = signer.address().to_string();
                snapshot
                    .report
                    .notes
                    .push(format!("signer_address={}", snapshot.report.signer_address));
                Some(signer.with_chain_id(Some(settings.polymarket_chain_id)))
            }
            Err(error) => {
                snapshot
                    .report
                    .notes
                    .push(format!("invalid POLYMARKET_PRIVATE_KEY: {error}"));
                None
            }
        },
        None => {
            snapshot
                .report
                .notes
                .push("POLYMARKET_PRIVATE_KEY is not configured".to_owned());
            None
        }
    };

    let signer_address = signer.as_ref().map(Signer::address);
    let profile_address = validate_address(
        settings.polymarket_profile_address.as_deref(),
        "POLYMARKET_PROFILE_ADDRESS",
        &mut snapshot.report.notes,
    );
    snapshot.report.profile_address_ok = profile_address.is_some();

    let funder_address = match settings.polymarket_funder_address.as_deref() {
        Some(value) => {
            snapshot
                .report
                .notes
                .push("proxy/safe flow detected via POLYMARKET_FUNDER_ADDRESS".to_owned());
            validate_address(
                Some(value),
                "POLYMARKET_FUNDER_ADDRESS",
                &mut snapshot.report.notes,
            )
        }
        None => {
            snapshot
                .report
                .notes
                .push("EOA flow detected, using signer as trading wallet".to_owned());
            None
        }
    };
    snapshot.report.funder_address_ok =
        settings.polymarket_funder_address.is_none() || funder_address.is_some();

    if flow == WalletFlow::Eoa
        && let (Some(profile), Some(signer_address)) = (profile_address, signer_address)
        && profile != signer_address
    {
        snapshot.report.notes.push(
            "EOA mode warning: profile address differs from signer/funder address".to_owned(),
        );
    }

    let trading_address = funder_address.or(signer_address);
    snapshot.trading_address = trading_address.map(|address| address.to_string());

    let usdc_address = validate_address(
        Some(&settings.polymarket_usdc_address),
        "POLYMARKET_USDC_ADDRESS",
        &mut snapshot.report.notes,
    );

    let spender_address = if flow == WalletFlow::Eoa {
        validate_address(
            Some(&settings.polymarket_spender_address),
            "POLYMARKET_SPENDER_ADDRESS",
            &mut snapshot.report.notes,
        )
    } else {
        None
    };
    let allowance_inputs_valid =
        validate_allowance_inputs(signer_address, spender_address, usdc_address);

    let provider = match rpc.provider().await {
        Ok(provider) => provider,
        Err(error) => {
            snapshot
                .report
                .notes
                .push(format!("failed to connect to POLYGON_RPC_URL: {error}"));
            refresh_ready_flag(&mut snapshot.report);
            return Ok(snapshot);
        }
    };

    let chain_provider = provider.clone();
    let signer_balance_provider = provider.clone();
    let funder_balance_provider = provider.clone();
    let usdc_balance_provider = provider.clone();
    let allowance_provider = provider.clone();

    let chain_id_fut = async move { chain_provider.get_chain_id().await };
    let signer_balance_fut = async move {
        match signer_address {
            Some(address) => signer_balance_provider
                .get_balance(address)
                .await
                .map(|balance| Some(units_to_decimal(balance, MATIC_DECIMALS))),
            None => Ok(None),
        }
    };
    let funder_balance_fut = async move {
        match (funder_address, signer_address) {
            (Some(address), Some(signer_address)) if address != signer_address => {
                funder_balance_provider
                    .get_balance(address)
                    .await
                    .map(|balance| Some(units_to_decimal(balance, MATIC_DECIMALS)))
            }
            _ => Ok(None),
        }
    };
    let usdc_balance_fut = async move {
        match (usdc_address, trading_address) {
            (Some(token), Some(owner)) => read_usdc_balance(usdc_balance_provider, token, owner)
                .await
                .map(Some),
            _ => Ok(None),
        }
    };
    let allowance_fut = async move {
        match (
            flow,
            allowance_inputs_valid,
            usdc_address,
            signer_address,
            spender_address,
        ) {
            (WalletFlow::Eoa, true, Some(token), Some(owner), Some(spender)) => {
                read_allowance_value(allowance_provider, token, owner, spender)
                    .await
                    .map(Some)
            }
            _ => Ok(None),
        }
    };

    let (
        chain_id_result,
        signer_balance_result,
        funder_balance_result,
        usdc_balance_result,
        allowance_result,
    ) = tokio::join!(
        chain_id_fut,
        signer_balance_fut,
        funder_balance_fut,
        usdc_balance_fut,
        allowance_fut
    );

    match chain_id_result {
        Ok(chain_id) if chain_id == POLYGON && settings.polymarket_chain_id == POLYGON => {
            snapshot.report.chain_id_ok = true;
        }
        Ok(chain_id) => snapshot.report.notes.push(format!(
            "chain id mismatch: env={} rpc={} expected={POLYGON}",
            settings.polymarket_chain_id, chain_id
        )),
        Err(error) => snapshot
            .report
            .notes
            .push(format!("failed to query rpc chain id: {error}")),
    }

    match signer_balance_result {
        Ok(Some(balance)) => {
            snapshot.matic_balance = balance;
            if snapshot.matic_balance >= settings.min_required_matic {
                snapshot.report.matic_balance_ok = true;
            } else {
                snapshot.report.notes.push(format!(
                    "insufficient MATIC: {} < required {}",
                    snapshot.matic_balance.normalize(),
                    settings.min_required_matic.normalize()
                ));
            }
        }
        Ok(None) => snapshot
            .report
            .notes
            .push("cannot verify MATIC balance without a valid signer address".to_owned()),
        Err(error) => snapshot
            .report
            .notes
            .push(format!("failed to query signer MATIC balance: {error}")),
    }

    match funder_balance_result {
        Ok(Some(balance)) => snapshot
            .report
            .notes
            .push(format!("funder_matic_balance={}", balance.normalize())),
        Ok(None) => {}
        Err(error) => snapshot
            .report
            .notes
            .push(format!("failed to query funder MATIC balance: {error}")),
    }

    match usdc_balance_result {
        Ok(Some(balance)) => {
            snapshot.usdc_balance = balance;
            if snapshot.usdc_balance >= settings.min_required_usdc {
                snapshot.report.usdc_balance_ok = true;
            } else {
                snapshot.report.notes.push(format!(
                    "insufficient USDC: {} < required {}",
                    snapshot.usdc_balance.normalize(),
                    settings.min_required_usdc.normalize()
                ));
            }
        }
        Ok(None) if trading_address.is_none() => snapshot
            .report
            .notes
            .push("cannot verify USDC balance without a valid trading address".to_owned()),
        Ok(None) => {}
        Err(error) => snapshot
            .report
            .notes
            .push(format!("failed to query USDC balance: {error}")),
    }

    if flow == WalletFlow::Eoa {
        let required_allowance = settings
            .usdc_approval_amount
            .max(settings.min_required_usdc);
        if allowance_inputs_valid {
            match allowance_result {
                Ok(Some(allowance)) => {
                    snapshot.allowance = Some(allowance);
                    if allowance >= required_allowance {
                        snapshot.report.allowance_ok = true;
                    } else if settings.auto_approve_usdc_allowance {
                        warn!(
                            signer_address = %snapshot.report.signer_address,
                            required_allowance = %required_allowance,
                            current_allowance = %allowance,
                            "wallet allowance below target, attempting automatic approval"
                        );
                        match auto_approve_allowance(
                            settings,
                            &rpc,
                            signer.clone(),
                            usdc_address,
                            spender_address,
                            required_allowance,
                        )
                        .await
                        {
                            Ok(approved_allowance) if approved_allowance >= required_allowance => {
                                snapshot.allowance = Some(approved_allowance);
                                snapshot.report.allowance_ok = true;
                                snapshot.report.notes.push(format!(
                                    "USDC allowance updated to {}",
                                    approved_allowance.normalize()
                                ));
                            }
                            Ok(approved_allowance) => {
                                snapshot.allowance = Some(approved_allowance);
                                snapshot.report.notes.push(format!(
                                    "USDC allowance still insufficient after approval: {} < required {}",
                                    approved_allowance.normalize(),
                                    required_allowance.normalize()
                                ));
                            }
                            Err(error) => snapshot
                                .report
                                .notes
                                .push(format!("automatic USDC approval failed: {error}")),
                        }
                    } else {
                        snapshot.report.notes.push(format!(
                            "USDC allowance too low: {} < required {}",
                            allowance.normalize(),
                            required_allowance.normalize()
                        ));
                    }
                }
                Ok(None) => {}
                Err(error) => snapshot
                    .report
                    .notes
                    .push(format!("failed to query USDC allowance: {error}")),
            }
        } else {
            snapshot.report.notes.push(
                "skipping allowance check until signer, spender, and USDC addresses are configured"
                    .to_owned(),
            );
        }
    } else {
        snapshot.report.allowance_ok = true;
        snapshot
            .report
            .notes
            .push("allowance check skipped for proxy/safe flow".to_owned());
    }

    refresh_ready_flag(&mut snapshot.report);
    Ok(snapshot)
}

pub fn print_wallet_readiness_report(snapshot: &WalletReadinessSnapshot) {
    println!(
        "Signer Address: {}",
        display_value(&snapshot.report.signer_address)
    );
    println!(
        "Profile Address: {}",
        display_option(snapshot.profile_address.as_deref())
    );
    println!(
        "Funder Address: {}",
        display_option(snapshot.funder_address.as_deref())
    );
    println!("Execution Flow: {}", snapshot.execution_flow);
    println!("MATIC Balance: {}", snapshot.matic_balance.normalize());
    println!("USDC Balance: {}", snapshot.usdc_balance.normalize());
    println!(
        "Allowance Status: {}",
        match snapshot.allowance {
            Some(allowance) => format!(
                "{} ({})",
                if snapshot.report.allowance_ok {
                    "ok"
                } else {
                    "not_ok"
                },
                allowance.normalize()
            ),
            None if snapshot.execution_flow == WalletFlow::Proxy.as_str() => {
                "skipped (proxy flow)".to_owned()
            }
            None => "unavailable".to_owned(),
        }
    );
    println!(
        "Ready For Live Trading: {}",
        if snapshot.report.ready_for_live_trading {
            "yes"
        } else {
            "no"
        }
    );
    if !snapshot.report.notes.is_empty() {
        println!("Notes:");
        for note in &snapshot.report.notes {
            println!("- {note}");
        }
    }
}

async fn read_usdc_balance<P: Provider>(
    provider: P,
    usdc_address: Address,
    owner: Address,
) -> Result<Decimal> {
    let usdc = IERC20::new(usdc_address, provider);
    let raw_balance = usdc.balanceOf(owner).call().await?;
    Ok(units_to_decimal(raw_balance, USDC_DECIMALS))
}

async fn read_allowance_value<P: Provider>(
    provider: P,
    usdc_address: Address,
    owner: Address,
    spender: Address,
) -> Result<Decimal> {
    let usdc = IERC20::new(usdc_address, provider);
    let raw_allowance = usdc.allowance(owner, spender).call().await?;
    Ok(units_to_decimal(raw_allowance, USDC_DECIMALS))
}

async fn auto_approve_allowance(
    settings: &AppConfig,
    rpc: &WalletReadinessRpc,
    signer: Option<PrivateKeySigner>,
    usdc_address: Option<Address>,
    spender_address: Option<Address>,
    required_allowance: Decimal,
) -> Result<Decimal> {
    let signer = signer.ok_or_else(|| anyhow!("missing valid signer for USDC approval"))?;
    let usdc_address =
        usdc_address.ok_or_else(|| anyhow!("missing valid USDC address for approval"))?;
    let spender_address =
        spender_address.ok_or_else(|| anyhow!("missing valid spender for USDC approval"))?;
    let approval_amount = decimal_to_units(settings.usdc_approval_amount, USDC_DECIMALS)?;
    let (tx_hash, allowance) = match send_approval_once(
        rpc,
        signer.clone(),
        usdc_address,
        spender_address,
        approval_amount,
    )
    .await
    {
        Ok(result) => result,
        Err(error) if is_nonce_related_error(&error) => {
            warn!(
                signer_address = %signer.address(),
                error = %error,
                "nonce-related approval failure detected, rebuilding provider and retrying once"
            );
            send_approval_once(
                rpc,
                signer.clone(),
                usdc_address,
                spender_address,
                approval_amount,
            )
            .await?
        }
        Err(error) => return Err(error),
    };
    info!(
        signer_address = %signer.address(),
        tx_hash = %tx_hash,
        approval_amount = %settings.usdc_approval_amount,
        rpc_endpoint = %rpc.selected_rpc_url(),
        "submitted automatic USDC approval transaction"
    );

    if allowance < required_allowance {
        return Err(anyhow!(
            "allowance remained below target after approval: {} < {}",
            allowance.normalize(),
            required_allowance.normalize()
        ));
    }
    Ok(allowance)
}

fn validate_address(value: Option<&str>, label: &str, notes: &mut Vec<String>) -> Option<Address> {
    match normalized_address_value(value) {
        Some(value) => match Address::from_str(value) {
            Ok(address) => Some(address),
            Err(error) => {
                notes.push(format!("invalid {label}: {error}"));
                None
            }
        },
        None => {
            notes.push(format!("{label} is not configured"));
            None
        }
    }
}

fn normalized_address_value(value: Option<&str>) -> Option<&str> {
    let value = value?.trim();
    if value.is_empty()
        || value.starts_with("REPLACE_")
        || value.contains("YOUR_")
        || value == "<unset>"
    {
        None
    } else {
        Some(value)
    }
}

fn validate_allowance_inputs(
    signer: Option<Address>,
    spender: Option<Address>,
    usdc_address: Option<Address>,
) -> bool {
    signer.is_some() && spender.is_some() && usdc_address.is_some()
}

fn wallet_flow(settings: &AppConfig) -> WalletFlow {
    if settings.polymarket_funder_address.is_some() {
        WalletFlow::Proxy
    } else {
        WalletFlow::Eoa
    }
}

struct WalletReadinessRpc {
    selected_rpc_url: String,
    selected_latency: Duration,
    chain_id: u64,
    confirmation_timeout: Duration,
}

impl WalletReadinessRpc {
    async fn new(config: &AppConfig) -> Result<Self> {
        let primary = config.polygon_rpc_url.trim();
        if primary.is_empty() {
            return Err(anyhow!("POLYGON_RPC_URL must be set"));
        }
        let mut endpoints = vec![primary.to_owned()];
        for endpoint in &config.polygon_rpc_fallback_urls {
            let trimmed = endpoint.trim();
            if !trimmed.is_empty() && !endpoints.iter().any(|existing| existing == trimmed) {
                endpoints.push(trimmed.to_owned());
            }
        }
        let (selected_rpc_url, selected_latency) =
            select_rpc_endpoint(&endpoints, config.rpc_latency_threshold).await?;
        println!("Wallet readiness using RPC: {}", selected_rpc_url);
        Ok(Self {
            selected_rpc_url,
            selected_latency,
            chain_id: config.polymarket_chain_id,
            confirmation_timeout: config.rpc_confirmation_timeout,
        })
    }

    async fn provider(&self) -> Result<impl Provider + Clone> {
        ProviderBuilder::new()
            .with_call_batching()
            .with_default_caching()
            .connect(self.selected_rpc_url.as_str())
            .await
            .map_err(Into::into)
    }

    async fn wallet_provider(&self, signer: PrivateKeySigner) -> Result<impl Provider + Clone> {
        ProviderBuilder::new()
            .with_cached_nonce_management()
            .with_call_batching()
            .with_chain_id(self.chain_id)
            .wallet(signer)
            .connect(self.selected_rpc_url.as_str())
            .await
            .map_err(Into::into)
    }

    fn selected_rpc_url(&self) -> &str {
        &self.selected_rpc_url
    }

    fn selected_latency(&self) -> Duration {
        self.selected_latency
    }

    fn confirmation_timeout(&self) -> Duration {
        self.confirmation_timeout
    }
}

async fn send_approval_once(
    rpc: &WalletReadinessRpc,
    signer: PrivateKeySigner,
    usdc_address: Address,
    spender_address: Address,
    approval_amount: U256,
) -> Result<(alloy::primitives::FixedBytes<32>, Decimal)> {
    let provider = rpc.wallet_provider(signer.clone()).await?;
    let usdc = IERC20::new(usdc_address, provider.clone());
    let receipt = usdc
        .approve(spender_address, approval_amount)
        .send()
        .await?
        .with_required_confirmations(1)
        .with_timeout(Some(rpc.confirmation_timeout()))
        .get_receipt()
        .await?;
    if !receipt.status() {
        return Err(anyhow!(
            "USDC approval transaction reverted: {}",
            receipt.transaction_hash
        ));
    }
    let allowance =
        read_allowance_value(provider, usdc_address, signer.address(), spender_address).await?;
    Ok((receipt.transaction_hash, allowance))
}

fn is_nonce_related_error(error: &anyhow::Error) -> bool {
    let message = error.to_string().to_ascii_lowercase();
    message.contains("nonce")
        || message.contains("already known")
        || message.contains("underpriced")
        || message.contains("replacement transaction")
}

async fn select_rpc_endpoint(
    endpoints: &[String],
    latency_threshold: Duration,
) -> Result<(String, Duration)> {
    let probe_timeout = latency_threshold.max(Duration::from_millis(300));
    let probes = join_all(endpoints.iter().map(|endpoint| {
        let endpoint = endpoint.clone();
        async move { probe_rpc_endpoint(endpoint, probe_timeout).await }
    }))
    .await;

    let mut successful = probes
        .into_iter()
        .filter_map(Result::ok)
        .collect::<Vec<_>>();
    successful.sort_by_key(|(_, latency)| *latency);

    let preferred = successful
        .iter()
        .find(|(_, latency)| *latency <= latency_threshold)
        .cloned()
        .or_else(|| successful.first().cloned());

    preferred.ok_or_else(|| anyhow!("no responsive Polygon RPC endpoint available"))
}

async fn probe_rpc_endpoint(
    endpoint: String,
    timeout_duration: Duration,
) -> Result<(String, Duration)> {
    let started = Instant::now();
    let provider = tokio::time::timeout(
        timeout_duration,
        ProviderBuilder::new().connect(endpoint.as_str()),
    )
    .await
    .map_err(|_| anyhow!("rpc connect timeout for {endpoint}"))??;
    tokio::time::timeout(timeout_duration, provider.get_block_number())
        .await
        .map_err(|_| anyhow!("rpc latency probe timeout for {endpoint}"))??;
    Ok((endpoint, started.elapsed()))
}

fn refresh_ready_flag(report: &mut WalletReadinessReport) {
    report.ready_for_live_trading = report.chain_id_ok
        && report.private_key_ok
        && report.profile_address_ok
        && report.funder_address_ok
        && report.usdc_balance_ok
        && report.matic_balance_ok
        && report.allowance_ok;
}

fn display_option(value: Option<&str>) -> &str {
    value.unwrap_or("<unset>")
}

fn display_value(value: &str) -> &str {
    if value.is_empty() {
        "<unavailable>"
    } else {
        value
    }
}

fn units_to_decimal(value: U256, decimals: u32) -> Decimal {
    decimal_string_to_decimal(&insert_decimal_point(&value.to_string(), decimals))
}

fn decimal_to_units(value: Decimal, decimals: u32) -> Result<U256> {
    if value < Decimal::ZERO {
        return Err(anyhow!("approval amount cannot be negative"));
    }

    let normalized = value.trunc_with_scale(decimals).normalize().to_string();
    let digits = if let Some(index) = normalized.find('.') {
        let fractional_digits = normalized.len() - index - 1;
        format!(
            "{}{}{}",
            &normalized[..index],
            &normalized[index + 1..],
            "0".repeat(decimals as usize - fractional_digits)
        )
    } else {
        format!("{normalized}{}", "0".repeat(decimals as usize))
    };

    U256::from_str(&digits).map_err(|error| anyhow!("failed to convert decimal to units: {error}"))
}

fn insert_decimal_point(raw: &str, decimals: u32) -> String {
    if decimals == 0 {
        return raw.to_owned();
    }

    let decimals = decimals as usize;
    if raw.len() <= decimals {
        format!("0.{}{}", "0".repeat(decimals - raw.len()), raw)
    } else {
        let split = raw.len() - decimals;
        format!("{}.{}", &raw[..split], &raw[split..])
    }
}

fn decimal_string_to_decimal(value: &str) -> Decimal {
    let trimmed = value.trim_end_matches('0').trim_end_matches('.');
    let normalized = if trimmed.is_empty() || trimmed == "0" {
        "0"
    } else {
        trimmed
    };
    Decimal::from_str(normalized).unwrap_or(Decimal::ZERO)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn formats_usdc_units_as_decimal() {
        assert_eq!(
            units_to_decimal(U256::from(25_500_000_u64), USDC_DECIMALS),
            dec!(25.5)
        );
    }

    #[test]
    fn formats_matic_units_as_decimal() {
        assert_eq!(
            units_to_decimal(U256::from(100_000_000_000_000_000_u128), MATIC_DECIMALS),
            dec!(0.1)
        );
    }

    #[test]
    fn converts_decimal_to_u256_units() {
        assert_eq!(
            decimal_to_units(dec!(1000), USDC_DECIMALS).expect("decimal to units"),
            U256::from(1_000_000_000_u64)
        );
    }

    #[test]
    fn ready_flag_requires_all_checks() {
        let mut report = WalletReadinessReport {
            chain_id_ok: true,
            private_key_ok: true,
            signer_address: "0xabc".to_owned(),
            profile_address_ok: true,
            funder_address_ok: true,
            usdc_balance_ok: true,
            matic_balance_ok: true,
            allowance_ok: true,
            ready_for_live_trading: false,
            notes: Vec::new(),
        };

        refresh_ready_flag(&mut report);
        assert!(report.ready_for_live_trading);

        report.allowance_ok = false;
        refresh_ready_flag(&mut report);
        assert!(!report.ready_for_live_trading);
    }

    #[test]
    fn detects_nonce_related_errors() {
        assert!(is_nonce_related_error(&anyhow!("nonce too low")));
        assert!(is_nonce_related_error(&anyhow!(
            "replacement transaction underpriced"
        )));
        assert!(!is_nonce_related_error(&anyhow!("insufficient funds")));
    }

    #[test]
    fn placeholder_address_is_treated_as_unconfigured() {
        assert_eq!(normalized_address_value(Some("")), None);
        assert_eq!(
            normalized_address_value(Some("REPLACE_WITH_POLYMARKET_SPENDER")),
            None
        );
        assert_eq!(normalized_address_value(Some("0x1234")), Some("0x1234"));
    }

    #[test]
    fn validates_allowance_inputs_only_when_all_fields_exist() {
        let address = Address::repeat_byte(1);
        assert!(validate_allowance_inputs(
            Some(address),
            Some(address),
            Some(address)
        ));
        assert!(!validate_allowance_inputs(
            None,
            Some(address),
            Some(address)
        ));
        assert!(!validate_allowance_inputs(
            Some(address),
            None,
            Some(address)
        ));
        assert!(!validate_allowance_inputs(
            Some(address),
            Some(address),
            None
        ));
    }
}
