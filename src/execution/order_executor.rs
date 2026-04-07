use std::fmt::{Display, Formatter};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use alloy::signers::{Signer, local::PrivateKeySigner};
use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use polymarket_client_sdk::POLYGON;
use polymarket_client_sdk::auth::Normal;
use polymarket_client_sdk::auth::state::Authenticated;
use polymarket_client_sdk::clob::types::response::PostOrderResponse;
use polymarket_client_sdk::clob::types::{OrderType, Side, SignatureType};
use polymarket_client_sdk::clob::{Client as ClobClient, Config as ClobConfig};
use polymarket_client_sdk::types::{Decimal, U256};
use serde::{Deserialize, Serialize};
use tokio::time::sleep;
use tracing::info;

use crate::config::{ExecutionMode, Settings};
use crate::models::BestQuote;
use crate::orderbook::orderbook_state::OrderBookState;

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub enum ExecutionSide {
    Buy,
    Sell,
}

#[derive(Clone, Debug, Serialize)]
pub struct ExecutionRequest {
    pub token_id: String,
    pub side: ExecutionSide,
    pub size: Decimal,
    pub limit_price: Decimal,
    pub requested_notional: Decimal,
    pub source_trade_id: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct ExecutionResult {
    pub mode: ExecutionMode,
    pub order_request: ExecutionRequest,
    pub order_id: String,
    pub success: bool,
    pub transaction_hashes: Vec<String>,
    pub filled_price: Decimal,
    pub filled_size: Decimal,
    pub requested_size: Decimal,
    pub requested_price: Decimal,
    pub status: ExecutionStatus,
    pub filled_notional: Decimal,
}

pub type ExecutionSuccess = ExecutionResult;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum ExecutionStatus {
    Filled,
    PartiallyFilled,
    NoFill,
    Rejected,
}

impl ExecutionStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Filled => "Filled",
            Self::PartiallyFilled => "Partial",
            Self::NoFill => "NoFill",
            Self::Rejected => "Rejected",
        }
    }

    pub fn has_fill(self) -> bool {
        matches!(self, Self::Filled | Self::PartiallyFilled)
    }
}

impl Display for ExecutionStatus {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct OrderBook {
    pub best_bid: Decimal,
    pub best_ask: Decimal,
    pub top_level_liquidity: Decimal,
}

impl ExecutionResult {
    pub fn has_fill(&self) -> bool {
        self.status.has_fill()
            && self.filled_size > Decimal::ZERO
            && self.filled_notional > Decimal::ZERO
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct CancelSuccess {
    pub mode: ExecutionMode,
    pub order_id: String,
    pub canceled: bool,
    pub detail: String,
}

#[async_trait]
pub trait TradeExecutor: Send + Sync {
    async fn submit_order(&self, request: ExecutionRequest) -> Result<ExecutionResult>;
    async fn cancel_order(&self, order_id: &str) -> Result<CancelSuccess>;
}

#[derive(Clone)]
pub struct PolymarketExecutor {
    client: ClobClient<Authenticated<Normal>>,
    signer: Arc<PrivateKeySigner>,
}

impl PolymarketExecutor {
    pub async fn new(settings: Settings) -> Result<Self> {
        let private_key = settings
            .polymarket_private_key
            .as_deref()
            .ok_or_else(|| anyhow!("POLYMARKET_PRIVATE_KEY is required in live mode"))?;
        let signer = PrivateKeySigner::from_str(private_key)?
            .with_chain_id(Some(settings.polymarket_chain_id));
        if settings.polymarket_chain_id != POLYGON {
            return Err(anyhow!(
                "expected Polygon chain id 137, got {}",
                settings.polymarket_chain_id
            ));
        }
        let client = ClobClient::new(&settings.polymarket_host, ClobConfig::default())?;
        let mut builder = client.authentication_builder(&signer);
        if let Some(funder) = &settings.polymarket_funder_address {
            builder = builder.funder(funder.parse()?);
        }
        builder = builder.signature_type(signature_type(settings.polymarket_signature_type)?);
        let client = builder.authenticate().await?;
        Ok(Self {
            client,
            signer: Arc::new(signer),
        })
    }
}

#[async_trait]
impl TradeExecutor for PolymarketExecutor {
    async fn submit_order(&self, request: ExecutionRequest) -> Result<ExecutionResult> {
        let side = match request.side {
            ExecutionSide::Buy => Side::Buy,
            ExecutionSide::Sell => Side::Sell,
        };
        let token_id = U256::from_str(&request.token_id)?;

        let order = self
            .client
            .limit_order()
            .token_id(token_id)
            .size(request.size)
            .price(request.limit_price)
            .side(side)
            .order_type(OrderType::FAK)
            .build()
            .await
            .context("building order")?;
        let signed_order = self
            .client
            .sign(self.signer.as_ref(), order)
            .await
            .context("signing order")?;
        let response = self
            .client
            .post_order(signed_order)
            .await
            .context("posting order")?;
        let (filled_price, filled_size, filled_notional, status) =
            live_fill_from_response(&request, &response);

        let result = ExecutionResult {
            mode: ExecutionMode::Live,
            requested_size: request.size,
            requested_price: request.limit_price,
            order_request: request,
            order_id: response.order_id,
            status,
            success: response.success,
            transaction_hashes: response
                .transaction_hashes
                .into_iter()
                .map(|hash| format!("{hash:#x}"))
                .collect(),
            filled_price,
            filled_size,
            filled_notional,
        };
        info!(
            execution_mode = "live",
            order_id = %result.order_id,
            asset = %result.order_request.token_id,
            side = ?result.order_request.side,
            execution_status = %result.status,
            requested_size = %result.requested_size,
            filled_size = %result.filled_size,
            filled_price = %result.filled_price,
            "live execution completed"
        );
        Ok(result)
    }

    async fn cancel_order(&self, order_id: &str) -> Result<CancelSuccess> {
        let response = self
            .client
            .cancel_order(order_id)
            .await
            .context("canceling order")?;
        let canceled = response
            .canceled
            .iter()
            .any(|candidate| candidate == order_id);
        let detail = if canceled {
            "order canceled".to_owned()
        } else if let Some(reason) = response.not_canceled.get(order_id) {
            reason.clone()
        } else if response.not_canceled.is_empty() {
            "cancel response did not include the requested order id".to_owned()
        } else {
            format!("not_canceled={:?}", response.not_canceled)
        };

        Ok(CancelSuccess {
            mode: ExecutionMode::Live,
            order_id: order_id.to_owned(),
            canceled,
            detail,
        })
    }
}

#[derive(Clone)]
pub struct PaperExecutor {
    simulated_delay: Duration,
    realistic_execution: bool,
    orderbooks: Option<Arc<OrderBookState>>,
}

impl PaperExecutor {
    pub fn new(settings: Settings, orderbooks: Option<Arc<OrderBookState>>) -> Self {
        Self {
            simulated_delay: settings.paper_execution_delay,
            realistic_execution: settings.enable_realistic_paper,
            orderbooks,
        }
    }
}

#[async_trait]
impl TradeExecutor for PaperExecutor {
    async fn submit_order(&self, request: ExecutionRequest) -> Result<ExecutionResult> {
        if !self.simulated_delay.is_zero() {
            sleep(self.simulated_delay).await;
        }
        let order_id = format!("paper-{}", request.source_trade_id.replace(':', "-"));
        let result = if self.realistic_execution {
            self.submit_realistic_paper_order(request, order_id).await
        } else {
            legacy_paper_fill(request, order_id)
        };
        info!(
            execution_mode = "paper",
            order_id = %result.order_id,
            asset = %result.order_request.token_id,
            side = ?result.order_request.side,
            execution_status = %result.status,
            requested_size = %result.requested_size,
            filled_size = %result.filled_size,
            filled_price = %result.filled_price,
            "paper execution completed"
        );
        Ok(result)
    }

    async fn cancel_order(&self, order_id: &str) -> Result<CancelSuccess> {
        Ok(CancelSuccess {
            mode: ExecutionMode::Paper,
            order_id: order_id.to_owned(),
            canceled: true,
            detail: "paper order canceled".to_owned(),
        })
    }
}

impl PaperExecutor {
    async fn submit_realistic_paper_order(
        &self,
        request: ExecutionRequest,
        order_id: String,
    ) -> ExecutionResult {
        let Some(orderbook) = self.paper_orderbook(&request).await else {
            return ExecutionResult {
                mode: ExecutionMode::Paper,
                requested_size: request.size,
                requested_price: request.limit_price,
                order_request: request,
                order_id,
                success: true,
                transaction_hashes: Vec::new(),
                filled_price: Decimal::ZERO,
                filled_size: Decimal::ZERO,
                status: ExecutionStatus::NoFill,
                filled_notional: Decimal::ZERO,
            };
        };

        let mut result = simulate_fill(&request, &orderbook);
        result.mode = ExecutionMode::Paper;
        result.order_id = order_id;
        result.transaction_hashes = Vec::new();
        result
    }

    async fn paper_orderbook(&self, request: &ExecutionRequest) -> Option<OrderBook> {
        let quote = self
            .orderbooks
            .as_ref()?
            .best_quote(&request.token_id)
            .await?;
        orderbook_from_best_quote(request.side, &quote)
    }
}

fn legacy_paper_fill(request: ExecutionRequest, order_id: String) -> ExecutionResult {
    let filled_price = request.limit_price;
    let filled_size = request.size;
    let filled_notional = filled_price * filled_size;

    ExecutionResult {
        mode: ExecutionMode::Paper,
        requested_size: request.size,
        requested_price: request.limit_price,
        order_request: request,
        order_id,
        success: true,
        transaction_hashes: Vec::new(),
        filled_price,
        filled_size,
        status: ExecutionStatus::Filled,
        filled_notional,
    }
}

fn signature_type(value: u8) -> Result<SignatureType> {
    match value {
        0 => Ok(SignatureType::Eoa),
        1 => Ok(SignatureType::Proxy),
        2 => Ok(SignatureType::GnosisSafe),
        _ => Err(anyhow!("unsupported signature type {value}")),
    }
}

fn live_fill_from_response(
    request: &ExecutionRequest,
    response: &PostOrderResponse,
) -> (Decimal, Decimal, Decimal, ExecutionStatus) {
    let (filled_size, filled_notional) = match request.side {
        ExecutionSide::Buy => (response.making_amount, response.taking_amount),
        ExecutionSide::Sell => (response.taking_amount, response.making_amount),
    };

    let filled_price = if filled_size > Decimal::ZERO {
        filled_notional / filled_size
    } else {
        Decimal::ZERO
    };

    let status = execution_status_from_fill(response.success, filled_size, request.size);
    (filled_price, filled_size, filled_notional, status)
}

pub fn simulate_fill(request: &ExecutionRequest, orderbook: &OrderBook) -> ExecutionResult {
    let best_price = match request.side {
        ExecutionSide::Buy => orderbook.best_ask,
        ExecutionSide::Sell => orderbook.best_bid,
    };

    if (matches!(request.side, ExecutionSide::Buy) && request.limit_price < best_price)
        || (matches!(request.side, ExecutionSide::Sell) && request.limit_price > best_price)
    {
        return ExecutionResult {
            mode: ExecutionMode::Paper,
            requested_size: request.size,
            requested_price: request.limit_price,
            order_request: request.clone(),
            order_id: String::new(),
            success: true,
            transaction_hashes: Vec::new(),
            filled_price: Decimal::ZERO,
            filled_size: Decimal::ZERO,
            status: ExecutionStatus::NoFill,
            filled_notional: Decimal::ZERO,
        };
    }

    let fill_size = request
        .size
        .min(orderbook.top_level_liquidity.max(Decimal::ZERO));
    let status = if fill_size.is_zero() {
        ExecutionStatus::NoFill
    } else if fill_size < request.size {
        ExecutionStatus::PartiallyFilled
    } else {
        ExecutionStatus::Filled
    };

    ExecutionResult {
        mode: ExecutionMode::Paper,
        requested_size: request.size,
        requested_price: request.limit_price,
        order_request: request.clone(),
        order_id: String::new(),
        success: true,
        transaction_hashes: Vec::new(),
        filled_price: if status.has_fill() {
            best_price
        } else {
            Decimal::ZERO
        },
        filled_size: fill_size,
        status,
        filled_notional: fill_size * best_price,
    }
}

fn execution_status_from_fill(
    success: bool,
    filled_size: Decimal,
    requested_size: Decimal,
) -> ExecutionStatus {
    if !success {
        ExecutionStatus::Rejected
    } else if filled_size <= Decimal::ZERO {
        ExecutionStatus::NoFill
    } else if filled_size < requested_size {
        ExecutionStatus::PartiallyFilled
    } else {
        ExecutionStatus::Filled
    }
}

fn orderbook_from_best_quote(side: ExecutionSide, quote: &BestQuote) -> Option<OrderBook> {
    let (best_price, top_level_liquidity) = match side {
        ExecutionSide::Buy => (
            quote.best_ask?,
            quote.best_ask_size.unwrap_or(Decimal::ZERO),
        ),
        ExecutionSide::Sell => (
            quote.best_bid?,
            quote.best_bid_size.unwrap_or(Decimal::ZERO),
        ),
    };

    Some(OrderBook {
        best_bid: quote.best_bid.unwrap_or(best_price),
        best_ask: quote.best_ask.unwrap_or(best_price),
        top_level_liquidity,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use polymarket_client_sdk::clob::types::OrderStatusType;
    use rust_decimal_macros::dec;

    fn sample_request(side: ExecutionSide) -> ExecutionRequest {
        ExecutionRequest {
            token_id: "1".to_owned(),
            side,
            size: dec!(10),
            limit_price: dec!(0.5),
            requested_notional: dec!(5),
            source_trade_id: "trade-1".to_owned(),
        }
    }

    #[test]
    fn live_buy_fill_uses_response_making_and_taking_amounts() {
        let request = sample_request(ExecutionSide::Buy);
        let response = PostOrderResponse::builder()
            .making_amount(dec!(9.5))
            .taking_amount(dec!(4.56))
            .order_id("order-1")
            .status(OrderStatusType::Matched)
            .success(true)
            .build();

        let (filled_price, filled_size, filled_notional, status) =
            live_fill_from_response(&request, &response);

        assert_eq!(filled_size, dec!(9.5));
        assert_eq!(filled_notional, dec!(4.56));
        assert_eq!(filled_price.round_dp(6), dec!(0.48));
        assert_eq!(status, ExecutionStatus::PartiallyFilled);
    }

    #[test]
    fn live_sell_fill_uses_response_taking_and_making_amounts() {
        let request = sample_request(ExecutionSide::Sell);
        let response = PostOrderResponse::builder()
            .making_amount(dec!(7.35))
            .taking_amount(dec!(15))
            .order_id("order-1")
            .status(OrderStatusType::Matched)
            .success(true)
            .build();

        let (filled_price, filled_size, filled_notional, status) =
            live_fill_from_response(&request, &response);

        assert_eq!(filled_size, dec!(15));
        assert_eq!(filled_notional, dec!(7.35));
        assert_eq!(filled_price.round_dp(6), dec!(0.49));
        assert_eq!(status, ExecutionStatus::Filled);
    }

    #[test]
    fn zero_fill_response_reports_no_fill() {
        let request = sample_request(ExecutionSide::Buy);
        let response = PostOrderResponse::builder()
            .making_amount(Decimal::ZERO)
            .taking_amount(Decimal::ZERO)
            .order_id("order-1")
            .status(OrderStatusType::Live)
            .success(true)
            .build();

        let result = ExecutionResult {
            mode: ExecutionMode::Live,
            requested_size: request.size,
            requested_price: request.limit_price,
            order_request: request.clone(),
            order_id: response.order_id.clone(),
            status: ExecutionStatus::NoFill,
            success: response.success,
            transaction_hashes: Vec::new(),
            filled_price: Decimal::ZERO,
            filled_size: Decimal::ZERO,
            filled_notional: Decimal::ZERO,
        };

        assert!(!result.has_fill());
        assert_eq!(result.status, ExecutionStatus::NoFill);
    }

    #[test]
    fn paper_simulation_reports_no_fill_when_limit_misses_book() {
        let request = sample_request(ExecutionSide::Buy);
        let result = simulate_fill(
            &request,
            &OrderBook {
                best_bid: dec!(0.49),
                best_ask: dec!(0.51),
                top_level_liquidity: dec!(5),
            },
        );

        assert_eq!(result.status, ExecutionStatus::NoFill);
        assert_eq!(result.filled_size, Decimal::ZERO);
    }

    #[test]
    fn paper_simulation_reports_partial_fill_when_top_level_is_thin() {
        let request = sample_request(ExecutionSide::Buy);
        let result = simulate_fill(
            &request,
            &OrderBook {
                best_bid: dec!(0.49),
                best_ask: dec!(0.50),
                top_level_liquidity: dec!(5),
            },
        );

        assert_eq!(result.status, ExecutionStatus::PartiallyFilled);
        assert_eq!(result.filled_size, dec!(5));
        assert_eq!(result.filled_price, dec!(0.50));
    }
}
