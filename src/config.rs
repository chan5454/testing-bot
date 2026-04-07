use std::env;
use std::fmt::{Display, Formatter};
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

use anyhow::{Result, anyhow};
use rust_decimal::Decimal;

pub const MIN_PREDICTION_VALIDATION_TIMEOUT: Duration = Duration::from_millis(300);
pub const MAX_PREDICTION_VALIDATION_TIMEOUT: Duration = Duration::from_millis(500);

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
pub enum ExecutionMode {
    Live,
    Paper,
}

impl ExecutionMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Live => "live",
            Self::Paper => "paper",
        }
    }

    pub fn is_paper(self) -> bool {
        matches!(self, Self::Paper)
    }
}

impl Display for ExecutionMode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for ExecutionMode {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "live" => Ok(Self::Live),
            "paper" => Ok(Self::Paper),
            other => Err(anyhow!(
                "unsupported EXECUTION_MODE {other}, expected live or paper"
            )),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Settings {
    pub execution_mode: ExecutionMode,
    pub polymarket_host: String,
    pub polymarket_data_api: String,
    pub polymarket_gamma_api: String,
    pub polymarket_market_ws: String,
    pub polymarket_user_ws: String,
    pub polymarket_activity_ws: String,
    pub polymarket_chain_id: u64,
    pub polymarket_signature_type: u8,
    pub polymarket_private_key: Option<String>,
    pub polymarket_funder_address: Option<String>,
    pub polymarket_profile_address: Option<String>,
    pub polygon_rpc_url: String,
    pub polygon_rpc_fallback_urls: Vec<String>,
    pub rpc_latency_threshold: Duration,
    pub rpc_confirmation_timeout: Duration,
    pub min_required_matic: Decimal,
    pub min_required_usdc: Decimal,
    pub polymarket_usdc_address: String,
    pub polymarket_spender_address: String,
    pub auto_approve_usdc_allowance: bool,
    pub usdc_approval_amount: Decimal,
    pub target_activity_ws_api_key: Option<String>,
    pub target_activity_ws_secret: Option<String>,
    pub target_activity_ws_passphrase: Option<String>,
    pub target_profile_addresses: Vec<String>,
    pub start_capital_usd: Decimal,
    pub paper_execution_delay: Duration,
    pub copy_only_new_trades: bool,
    #[allow(dead_code)]
    pub source_trades_limit: u32,
    pub http_timeout: Duration,
    pub market_cache_ttl: Duration,
    pub market_raw_ring_capacity: usize,
    pub market_parser_workers: usize,
    pub market_subscription_batch_size: usize,
    pub market_subscription_delay: Duration,
    pub wallet_ring_capacity: usize,
    pub wallet_parser_workers: usize,
    pub wallet_subscription_batch_size: usize,
    pub wallet_subscription_delay: Duration,
    pub liquidity_sweep_threshold: Decimal,
    pub imbalance_threshold: Decimal,
    pub delta_price_move_bps: u32,
    pub delta_size_drop_ratio: Decimal,
    pub delta_min_size_drop: Decimal,
    pub inference_confirmation_window: Duration,
    pub activity_stream_enabled: bool,
    #[allow(dead_code)]
    pub activity_match_window: Duration,
    pub activity_price_tolerance: Decimal,
    pub activity_size_tolerance_ratio: Decimal,
    pub activity_cache_ttl: Duration,
    #[allow(dead_code)]
    pub fallback_market_request_interval: Duration,
    #[allow(dead_code)]
    pub fallback_global_requests_per_minute: u32,
    pub activity_correlation_window: Duration,
    #[allow(dead_code)]
    pub attribution_lookback: Duration,
    #[allow(dead_code)]
    pub attribution_trades_limit: u32,
    pub copy_scale_above_five_usd: Decimal,
    pub min_copy_notional_usd: Decimal,
    pub max_copy_notional_usd: Decimal,
    pub max_total_exposure_usd: Decimal,
    pub max_market_exposure_usd: Decimal,
    pub min_source_trade_usdc: Decimal,
    pub max_market_spread_bps: u32,
    pub min_top_of_book_ratio: Decimal,
    pub max_slippage_bps: u32,
    pub max_source_price_slippage_bps: u32,
    pub latency_fail_safe_enabled: bool,
    pub max_latency: Duration,
    pub average_latency_threshold: Duration,
    pub latency_monitor_interval: Duration,
    pub latency_reconnect_settle: Duration,
    pub prediction_validation_timeout: Duration,
    pub log_attribution_events: bool,
    pub activity_parser_debug: bool,
    pub log_latency_events: bool,
    pub log_skipped_trades: bool,
    pub enable_log_rotation: bool,
    pub log_max_lines: u32,
    pub enable_time_rotation: bool,
    pub log_rotate_hours: u64,
    pub enable_log_clearing: bool,
    pub allow_buy: bool,
    pub allow_sell: bool,
    pub allow_hedging: bool,
    pub enable_price_bands: bool,
    pub enable_realistic_paper: bool,
    pub min_edge_threshold: Decimal,
    pub max_copy_delay_ms: u64,
    pub min_liquidity: Decimal,
    pub min_wallet_score: Decimal,
    pub max_position_age_hours: u64,
    pub max_hold_time_seconds: u64,
    pub enable_exit_retry: bool,
    pub telegram_bot_token: String,
    pub telegram_chat_id: String,
    pub health_port: u16,
    pub data_dir: PathBuf,
}

pub type AppConfig = Settings;

impl Settings {
    pub fn effective_prediction_validation_timeout(&self) -> Duration {
        self.prediction_validation_timeout
            .max(MIN_PREDICTION_VALIDATION_TIMEOUT)
            .min(MAX_PREDICTION_VALIDATION_TIMEOUT)
    }

    pub fn from_env() -> Result<Self> {
        let execution_mode = optional("EXECUTION_MODE")
            .unwrap_or_else(|| "live".to_owned())
            .parse::<ExecutionMode>()?;
        let max_slippage_bps = parse("MAX_SLIPPAGE_BPS")?;
        Ok(Self {
            execution_mode,
            polymarket_host: required("POLYMARKET_HOST")?,
            polymarket_data_api: required("POLYMARKET_DATA_API")?,
            polymarket_gamma_api: optional("POLYMARKET_GAMMA_API")
                .unwrap_or_else(|| "https://gamma-api.polymarket.com".to_owned()),
            polymarket_market_ws: required("POLYMARKET_MARKET_WS")?,
            polymarket_user_ws: optional("POLYMARKET_USER_WS")
                .unwrap_or_else(|| "wss://ws-subscriptions-clob.polymarket.com/ws/user".to_owned()),
            polymarket_activity_ws: optional("POLYMARKET_ACTIVITY_WS")
                .unwrap_or_else(|| "wss://ws-live-data.polymarket.com".to_owned()),
            polymarket_chain_id: parse("POLYMARKET_CHAIN_ID")?,
            polymarket_signature_type: parse("POLYMARKET_SIGNATURE_TYPE")?,
            polymarket_private_key: required_when_live("POLYMARKET_PRIVATE_KEY", execution_mode)?,
            polymarket_funder_address: optional("POLYMARKET_FUNDER_ADDRESS"),
            polymarket_profile_address: required_when_live(
                "POLYMARKET_PROFILE_ADDRESS",
                execution_mode,
            )?,
            polygon_rpc_url: required("POLYGON_RPC_URL")?,
            polygon_rpc_fallback_urls: parse_optional_csv("POLYGON_RPC_FALLBACK_URLS"),
            rpc_latency_threshold: Duration::from_millis(parse_or_default(
                "RPC_LATENCY_THRESHOLD_MS",
                300_u64,
            )?),
            rpc_confirmation_timeout: Duration::from_millis(parse_or_default(
                "RPC_CONFIRMATION_TIMEOUT_MS",
                10_000_u64,
            )?),
            min_required_matic: parse_or_default_decimal("MIN_REQUIRED_MATIC", Decimal::new(1, 1))?,
            min_required_usdc: parse_or_default_decimal("MIN_REQUIRED_USDC", Decimal::new(25, 0))?,
            polymarket_usdc_address: optional("POLYMARKET_USDC_ADDRESS")
                .unwrap_or_else(|| "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174".to_owned()),
            polymarket_spender_address: optional("POLYMARKET_SPENDER_ADDRESS").unwrap_or_default(),
            auto_approve_usdc_allowance: parse_or_default("AUTO_APPROVE_USDC_ALLOWANCE", false)?,
            usdc_approval_amount: parse_or_default_decimal(
                "USDC_APPROVAL_AMOUNT",
                Decimal::new(1_000, 0),
            )?,
            target_activity_ws_api_key: optional("TARGET_ACTIVITY_WS_API_KEY"),
            target_activity_ws_secret: optional("TARGET_ACTIVITY_WS_SECRET"),
            target_activity_ws_passphrase: optional("TARGET_ACTIVITY_WS_PASSPHRASE"),
            target_profile_addresses: parse_target_profile_addresses()?,
            start_capital_usd: parse_decimal("START_CAPITAL_USD")?,
            paper_execution_delay: Duration::from_millis(parse_or_default(
                "PAPER_EXECUTION_DELAY_MS",
                0_u64,
            )?),
            copy_only_new_trades: parse_or_default("COPY_ONLY_NEW_TRADES", true)?,
            source_trades_limit: parse_or_default("SOURCE_TRADES_LIMIT", 50_u32)?,
            http_timeout: Duration::from_millis(parse("HTTP_TIMEOUT_MS")?),
            market_cache_ttl: Duration::from_millis(parse("MARKET_CACHE_TTL_MS")?),
            market_raw_ring_capacity: parse_or_default("MARKET_RAW_RING_CAPACITY", 8192_usize)?,
            market_parser_workers: parse_or_default("MARKET_PARSER_WORKERS", 1_usize)?,
            market_subscription_batch_size: parse_or_default(
                "MARKET_SUBSCRIPTION_BATCH_SIZE",
                250_usize,
            )?,
            market_subscription_delay: Duration::from_millis(parse_or_default(
                "MARKET_SUBSCRIPTION_DELAY_MS",
                20_u64,
            )?),
            wallet_ring_capacity: parse_or_default("WALLET_RING_CAPACITY", 1024_usize)?,
            wallet_parser_workers: parse_or_default("WALLET_PARSER_WORKERS", 1_usize)?,
            wallet_subscription_batch_size: parse_or_default(
                "WALLET_SUBSCRIPTION_BATCH_SIZE",
                250_usize,
            )?,
            wallet_subscription_delay: Duration::from_millis(parse_or_default(
                "WALLET_SUBSCRIPTION_DELAY_MS",
                20_u64,
            )?),
            liquidity_sweep_threshold: parse_or_default_decimal(
                "LIQUIDITY_SWEEP_THRESHOLD",
                Decimal::ONE,
            )?,
            imbalance_threshold: parse_or_default_decimal(
                "IMBALANCE_THRESHOLD",
                Decimal::new(2, 0),
            )?,
            delta_price_move_bps: parse_or_default("DELTA_PRICE_MOVE_BPS", 40_u32)?,
            delta_size_drop_ratio: parse_or_default_decimal(
                "DELTA_SIZE_DROP_RATIO",
                Decimal::new(25, 2),
            )?,
            delta_min_size_drop: parse_or_default_decimal("DELTA_MIN_SIZE_DROP", Decimal::ONE)?,
            inference_confirmation_window: Duration::from_millis(parse_or_default(
                "INFERENCE_CONFIRMATION_WINDOW_MS",
                400_u64,
            )?),
            activity_stream_enabled: parse_or_default("ACTIVITY_STREAM_ENABLED", true)?,
            activity_match_window: Duration::from_millis(parse_or_default(
                "ACTIVITY_MATCH_WINDOW_MS",
                200_u64,
            )?),
            activity_price_tolerance: parse_or_default_decimal(
                "ACTIVITY_PRICE_TOLERANCE",
                Decimal::new(1, 2),
            )?,
            activity_size_tolerance_ratio: parse_or_default_decimal(
                "ACTIVITY_SIZE_TOLERANCE_RATIO",
                Decimal::new(5, 1),
            )?,
            activity_cache_ttl: Duration::from_millis(parse_or_default(
                "ACTIVITY_CACHE_TTL_MS",
                1_500_u64,
            )?),
            fallback_market_request_interval: Duration::from_millis(parse_or_default(
                "FALLBACK_MARKET_REQUEST_INTERVAL_MS",
                1_000_u64,
            )?),
            fallback_global_requests_per_minute: parse_or_default(
                "FALLBACK_GLOBAL_REQUESTS_PER_MINUTE",
                30_u32,
            )?,
            activity_correlation_window: Duration::from_millis(parse_or_default(
                "ACTIVITY_CORRELATION_WINDOW_MS",
                400_u64,
            )?),
            attribution_lookback: Duration::from_millis(parse_or_default(
                "ATTRIBUTION_LOOKBACK_MS",
                2_500_u64,
            )?),
            attribution_trades_limit: parse_or_default("ATTRIBUTION_TRADES_LIMIT", 100_u32)?,
            copy_scale_above_five_usd: parse_decimal("COPY_SCALE_ABOVE_FIVE_USD")?,
            min_copy_notional_usd: parse_decimal("MIN_COPY_NOTIONAL_USD")?,
            max_copy_notional_usd: parse_decimal("MAX_COPY_NOTIONAL_USD")?,
            max_total_exposure_usd: parse_decimal("MAX_TOTAL_EXPOSURE_USD")?,
            max_market_exposure_usd: parse_decimal("MAX_MARKET_EXPOSURE_USD")?,
            min_source_trade_usdc: parse_or_default_decimal(
                "MIN_SOURCE_TRADE_USDC",
                Decimal::ZERO,
            )?,
            max_market_spread_bps: parse_or_default("MAX_MARKET_SPREAD_BPS", 0_u32)?,
            min_top_of_book_ratio: parse_or_default_decimal(
                "MIN_TOP_OF_BOOK_RATIO",
                Decimal::ZERO,
            )?,
            max_slippage_bps,
            max_source_price_slippage_bps: parse_or_default(
                "MAX_SOURCE_PRICE_SLIPPAGE_BPS",
                max_slippage_bps,
            )?,
            latency_fail_safe_enabled: parse_or_default("LATENCY_FAIL_SAFE_ENABLED", true)?,
            max_latency: Duration::from_millis(parse_or_default("MAX_LATENCY_MS", 500_u64)?),
            average_latency_threshold: Duration::from_millis(parse_or_default_u64_alias(
                &[
                    "AVG_LATENCY_THRESHOLD_MS",
                    "AVG_THRESHOLD_MS",
                    "AVG_THRESHOLD",
                ],
                350_u64,
            )?),
            latency_monitor_interval: Duration::from_millis(parse_or_default(
                "LATENCY_MONITOR_INTERVAL_MS",
                50_u64,
            )?),
            latency_reconnect_settle: Duration::from_millis(parse_or_default(
                "LATENCY_RECONNECT_SETTLE_MS",
                750_u64,
            )?),
            prediction_validation_timeout: Duration::from_millis(parse_or_default(
                "PREDICTION_VALIDATION_TIMEOUT_MS",
                1_500_u64,
            )?),
            log_attribution_events: parse_or_default("LOG_ATTRIBUTION_EVENTS", true)?,
            activity_parser_debug: parse_or_default("ACTIVITY_PARSER_DEBUG", false)?,
            log_latency_events: parse_or_default("LOG_LATENCY_EVENTS", true)?,
            log_skipped_trades: parse_or_default("LOG_SKIPPED_TRADES", true)?,
            enable_log_rotation: parse_or_default("ENABLE_LOG_ROTATION", true)?,
            log_max_lines: parse_or_default("LOG_MAX_LINES", 30_000_u32)?,
            enable_time_rotation: parse_or_default("ENABLE_TIME_ROTATION", true)?,
            log_rotate_hours: parse_or_default("LOG_ROTATE_HOURS", 6_u64)?,
            enable_log_clearing: parse_or_default("ENABLE_LOG_CLEARING", false)?,
            allow_buy: parse("ALLOW_BUY")?,
            allow_sell: parse("ALLOW_SELL")?,
            allow_hedging: parse_or_default("ALLOW_HEDGING", false)?,
            enable_price_bands: parse_or_default("ENABLE_PRICE_BANDS", true)?,
            enable_realistic_paper: parse_or_default("ENABLE_REALISTIC_PAPER", true)?,
            min_edge_threshold: parse_or_default_decimal("MIN_EDGE_THRESHOLD", Decimal::new(5, 2))?,
            max_copy_delay_ms: parse_or_default("MAX_COPY_DELAY_MS", 1_500_u64)?,
            min_liquidity: parse_or_default_decimal("MIN_LIQUIDITY", Decimal::new(50, 0))?,
            min_wallet_score: parse_or_default_decimal("MIN_WALLET_SCORE", Decimal::new(6, 1))?,
            max_position_age_hours: parse_or_default("MAX_POSITION_AGE_HOURS", 6_u64)?,
            max_hold_time_seconds: parse_or_default("MAX_HOLD_TIME_SECONDS", 1_800_u64)?,
            enable_exit_retry: parse_or_default("ENABLE_EXIT_RETRY", true)?,
            telegram_bot_token: required("TELEGRAM_BOT_TOKEN")?,
            telegram_chat_id: required("TELEGRAM_CHAT_ID")?,
            health_port: parse("HEALTH_PORT")?,
            data_dir: PathBuf::from(required("DATA_DIR")?),
        })
    }
}

fn parse_target_profile_addresses() -> Result<Vec<String>> {
    let raw = optional("TARGET_WALLETS")
        .or_else(|| optional("TARGET_PROFILE_ADDRESSES"))
        .or_else(|| optional("TARGET_PROFILE_ADDRESS"))
        .ok_or_else(|| anyhow!("missing env var TARGET_PROFILE_ADDRESSES"))?;
    let addresses = raw
        .split(',')
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>();
    if addresses.is_empty() {
        return Err(anyhow!(
            "TARGET_PROFILE_ADDRESSES must contain at least one wallet address"
        ));
    }
    Ok(addresses)
}

fn parse_optional_csv(key: &str) -> Vec<String> {
    optional(key)
        .map(|raw| {
            raw.split(',')
                .map(|value| value.trim().to_owned())
                .filter(|value| !value.is_empty())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

fn required(key: &str) -> Result<String> {
    env::var(key).map_err(|_| anyhow!("missing env var {key}"))
}

fn optional(key: &str) -> Option<String> {
    env::var(key).ok().filter(|value| !value.is_empty())
}

fn required_when_live(key: &str, execution_mode: ExecutionMode) -> Result<Option<String>> {
    match execution_mode {
        ExecutionMode::Live => Ok(Some(required(key)?)),
        ExecutionMode::Paper => Ok(optional(key)),
    }
}

fn parse<T>(key: &str) -> Result<T>
where
    T: std::str::FromStr,
    <T as std::str::FromStr>::Err: std::fmt::Display,
{
    let value = required(key)?;
    value
        .parse::<T>()
        .map_err(|error| anyhow!("failed to parse {key}: {error}"))
}

fn parse_decimal(key: &str) -> Result<Decimal> {
    let value = required(key)?;
    value
        .parse::<Decimal>()
        .map_err(|error| anyhow!("failed to parse decimal {key}: {error}"))
}

fn parse_or_default_decimal(key: &str, default: Decimal) -> Result<Decimal> {
    match optional(key) {
        Some(value) => value
            .parse::<Decimal>()
            .map_err(|error| anyhow!("failed to parse decimal {key}: {error}")),
        None => Ok(default),
    }
}

fn parse_or_default<T>(key: &str, default: T) -> Result<T>
where
    T: std::str::FromStr,
    <T as std::str::FromStr>::Err: std::fmt::Display,
{
    match optional(key) {
        Some(value) => value
            .parse::<T>()
            .map_err(|error| anyhow!("failed to parse {key}: {error}")),
        None => Ok(default),
    }
}

fn parse_or_default_u64_alias(keys: &[&str], default: u64) -> Result<u64> {
    for key in keys {
        if let Some(value) = optional(key) {
            return value
                .parse::<u64>()
                .map_err(|error| anyhow!("failed to parse {key}: {error}"));
        }
    }
    Ok(default)
}
