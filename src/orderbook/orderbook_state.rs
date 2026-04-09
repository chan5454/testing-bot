use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, Utc};
use reqwest::Client;
use rust_decimal::Decimal;
use tokio::sync::RwLock;

use crate::config::Settings;
use crate::execution::ExecutionSide;
use crate::models::{BestQuote, OrderBookResponse};
use crate::orderbook::orderbook_levels::{BookSide, OrderBook, OrderBookLevel, PriceLevelUpdate};

const PRICE_SCALE: f64 = 1_000_000.0;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct AssetMetadata {
    pub asset_id: String,
    pub condition_id: String,
    pub title: String,
    pub slug: String,
    pub event_slug: String,
    pub outcome: String,
    pub outcome_index: i64,
}

#[derive(Clone, Default)]
pub struct AssetCatalog {
    inner: Arc<HashMap<String, AssetMetadata>>,
    asset_ids: Arc<Vec<String>>,
    condition_ids: Arc<Vec<String>>,
}

impl AssetCatalog {
    pub fn new(items: Vec<AssetMetadata>) -> Self {
        let mut by_asset = HashMap::new();
        let mut asset_ids = Vec::new();
        let mut condition_ids = Vec::new();
        let mut seen_conditions = std::collections::HashSet::new();

        for item in items {
            asset_ids.push(item.asset_id.clone());
            if seen_conditions.insert(item.condition_id.clone()) {
                condition_ids.push(item.condition_id.clone());
            }
            by_asset.insert(item.asset_id.clone(), item);
        }

        Self {
            inner: Arc::new(by_asset),
            asset_ids: Arc::new(asset_ids),
            condition_ids: Arc::new(condition_ids),
        }
    }

    pub fn asset_ids(&self) -> &[String] {
        self.asset_ids.as_ref()
    }

    pub fn condition_ids(&self) -> &[String] {
        self.condition_ids.as_ref()
    }

    pub fn metadata(&self, asset_id: &str) -> Option<&AssetMetadata> {
        self.inner.get(asset_id)
    }

    pub fn complementary_metadata(
        &self,
        condition_id: &str,
        asset_id: &str,
    ) -> Option<&AssetMetadata> {
        self.inner
            .values()
            .find(|metadata| metadata.condition_id == condition_id && metadata.asset_id != asset_id)
    }
}

#[derive(Clone, Debug)]
pub struct OrderBookDelta {
    pub metadata: AssetMetadata,
    pub previous: OrderBook,
    pub current: OrderBook,
    pub best_bid_removed: f64,
    pub second_bid_removed: f64,
    pub best_ask_removed: f64,
    pub second_ask_removed: f64,
    pub removed_bid_liquidity: f64,
    pub removed_ask_liquidity: f64,
    pub previous_mid_price: Option<f64>,
    pub current_mid_price: Option<f64>,
    pub current_imbalance: Option<f64>,
    pub event_received_at: Instant,
    pub event_received_at_utc: DateTime<Utc>,
    pub parse_completed_at: Instant,
    pub parse_completed_at_utc: DateTime<Utc>,
    pub observed_at: Instant,
}

#[derive(Clone, Debug)]
pub struct MarketSnapshot {
    pub spread_ratio: Option<f64>,
    pub visible_total_volume: f64,
    pub mid_price: Option<f64>,
    pub best_bid: Option<f64>,
    pub best_ask: Option<f64>,
    pub last_trade_price: Option<f64>,
}

#[derive(Clone)]
pub struct OrderBookState {
    inner: Arc<RwLock<HashMap<String, AssetBookState>>>,
    catalog: AssetCatalog,
    #[allow(dead_code)]
    rest_client: Client,
    #[allow(dead_code)]
    clob_host: String,
    cache_ttl: Duration,
}

#[derive(Clone)]
struct AssetBookState {
    metadata: AssetMetadata,
    bids: HashMap<i64, f64>,
    asks: HashMap<i64, f64>,
    book: OrderBook,
    tick_size: Decimal,
    min_order_size: Decimal,
    neg_risk: bool,
    observed_at: Instant,
    last_valid_quote: Option<BestQuote>,
    last_valid_quote_observed_at: Option<Instant>,
    last_trade_price: Option<Decimal>,
    last_trade_observed_at: Option<Instant>,
}

impl OrderBookState {
    pub fn new(settings: &Settings, catalog: AssetCatalog) -> Result<Self> {
        let rest_client = Client::builder()
            .connect_timeout(settings.http_timeout / 2)
            .timeout(settings.http_timeout)
            .pool_max_idle_per_host(8)
            .pool_idle_timeout(Duration::from_secs(30))
            .tcp_keepalive(Duration::from_secs(30))
            .tcp_nodelay(true)
            .http2_adaptive_window(true)
            .user_agent("polymarket-copy-bot/0.1.0")
            .build()?;

        Ok(Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
            catalog,
            rest_client,
            clob_host: settings.polymarket_host.clone(),
            cache_ttl: settings.market_cache_ttl,
        })
    }

    pub async fn clear(&self) {
        self.inner.write().await.clear();
    }

    pub async fn apply_book_snapshot(
        &self,
        book: OrderBookResponse,
        event_received_at: Instant,
        event_received_at_utc: DateTime<Utc>,
        parse_completed_at: Instant,
        parse_completed_at_utc: DateTime<Utc>,
    ) -> Result<Option<OrderBookDelta>> {
        let asset_id = book.asset_id.clone();
        let metadata = self.metadata_for(&asset_id, &book.market);
        let observed_at = Instant::now();
        let tick_size = book.tick_size.parse::<Decimal>()?;
        let min_order_size = book.min_order_size.parse::<Decimal>()?;
        let bids = levels_to_map(&book.bids)?;
        let asks = levels_to_map(&book.asks)?;
        let top = top_levels(&bids, &asks);
        let current_quote = best_quote_from_book(
            asset_id.clone(),
            &top,
            tick_size,
            min_order_size,
            book.neg_risk,
        )?;

        let mut state = self.inner.write().await;
        let previous_state = state.get(&asset_id).cloned();
        let previous = previous_state.as_ref().map(|existing| existing.book);
        state.insert(
            asset_id.clone(),
            AssetBookState {
                metadata: metadata.clone(),
                bids,
                asks,
                book: top,
                tick_size,
                min_order_size,
                neg_risk: book.neg_risk,
                observed_at,
                last_valid_quote: Some(current_quote),
                last_valid_quote_observed_at: Some(observed_at),
                last_trade_price: previous_state
                    .as_ref()
                    .and_then(|existing| existing.last_trade_price),
                last_trade_observed_at: previous_state
                    .as_ref()
                    .and_then(|existing| existing.last_trade_observed_at),
            },
        );

        Ok(previous.map(|previous| {
            delta_from_books(
                metadata,
                previous,
                top,
                event_received_at,
                event_received_at_utc,
                parse_completed_at,
                parse_completed_at_utc,
                observed_at,
            )
        }))
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn apply_price_updates(
        &self,
        asset_id: &str,
        condition_id: &str,
        updates: &[PriceLevelUpdate],
        event_received_at: Instant,
        event_received_at_utc: DateTime<Utc>,
        parse_completed_at: Instant,
        parse_completed_at_utc: DateTime<Utc>,
    ) -> Result<Option<OrderBookDelta>> {
        if updates.is_empty() {
            return Ok(None);
        }

        let metadata = self.metadata_for(asset_id, condition_id);
        let observed_at = Instant::now();
        let mut state = self.inner.write().await;
        let book_state = state
            .entry(asset_id.to_owned())
            .or_insert_with(|| AssetBookState {
                metadata: metadata.clone(),
                bids: HashMap::new(),
                asks: HashMap::new(),
                book: OrderBook::default(),
                tick_size: Decimal::ZERO,
                min_order_size: Decimal::ZERO,
                neg_risk: false,
                observed_at,
                last_valid_quote: None,
                last_valid_quote_observed_at: None,
                last_trade_price: None,
                last_trade_observed_at: None,
            });

        let previous = book_state.book;
        for update in updates {
            let side_map = match update.side {
                BookSide::Bid => &mut book_state.bids,
                BookSide::Ask => &mut book_state.asks,
            };
            let key = price_key(update.price);
            if update.size <= 0.0 {
                side_map.remove(&key);
            } else {
                side_map.insert(key, update.size);
            }
        }

        book_state.metadata = metadata.clone();
        book_state.book = top_levels(&book_state.bids, &book_state.asks);
        book_state.observed_at = observed_at;
        if !book_state.tick_size.is_zero()
            && !book_state.min_order_size.is_zero()
            && let Ok(current_quote) = best_quote_from_book(
                asset_id.to_owned(),
                &book_state.book,
                book_state.tick_size,
                book_state.min_order_size,
                book_state.neg_risk,
            )
        {
            book_state.last_valid_quote = Some(current_quote);
            book_state.last_valid_quote_observed_at = Some(observed_at);
        }

        if previous == OrderBook::default() {
            return Ok(None);
        }

        Ok(Some(delta_from_books(
            metadata,
            previous,
            book_state.book,
            event_received_at,
            event_received_at_utc,
            parse_completed_at,
            parse_completed_at_utc,
            observed_at,
        )))
    }

    pub async fn best_quote(&self, asset_id: &str) -> Option<BestQuote> {
        let state = self.inner.read().await;
        let book = state.get(asset_id)?;
        if book.observed_at.elapsed() > self.cache_ttl {
            return None;
        }
        best_quote_from_book(
            asset_id.to_owned(),
            &book.book,
            book.tick_size,
            book.min_order_size,
            book.neg_risk,
        )
        .ok()
    }

    pub async fn refresh_book_snapshot(&self, asset_id: &str) -> Result<Option<BestQuote>> {
        if let Some(quote) = self.best_quote(asset_id).await
            && !quote.tick_size.is_zero()
            && !quote.min_order_size.is_zero()
        {
            return Ok(Some(quote));
        }

        let book = self
            .rest_client
            .get(format!("{}/book", self.clob_host))
            .query(&[("token_id", asset_id)])
            .send()
            .await
            .context("requesting order book")?
            .error_for_status()
            .context("order book returned error")?
            .json::<OrderBookResponse>()
            .await
            .context("decoding order book")?;

        self.apply_book_snapshot(book, Instant::now(), Utc::now(), Instant::now(), Utc::now())
            .await?;
        Ok(self.best_quote(asset_id).await)
    }

    pub async fn last_valid_quote(&self, asset_id: &str) -> Option<BestQuote> {
        self.inner
            .read()
            .await
            .get(asset_id)
            .and_then(|book| book.last_valid_quote.clone())
    }

    pub async fn fallback_quote(&self, asset_id: &str, side: ExecutionSide) -> Option<BestQuote> {
        let state = self.inner.read().await;
        let book = state.get(asset_id)?;

        if let Some(last_valid_quote) = book
            .last_valid_quote
            .clone()
            .filter(|quote| !quote.tick_size.is_zero() && !quote.min_order_size.is_zero())
        {
            return Some(last_valid_quote);
        }

        if book.tick_size.is_zero() || book.min_order_size.is_zero() {
            return None;
        }

        let reference_price = book
            .last_trade_price
            .or_else(|| book.book.mid_price().and_then(decimal_from_f64_lossy));
        let reference_price = reference_price?;
        let tick_size = book.tick_size;
        let bid_seed = (reference_price - tick_size).max(Decimal::new(1, 4));
        let ask_seed = (reference_price + tick_size).min(Decimal::new(9999, 4));
        let bid_size = book
            .last_valid_quote
            .as_ref()
            .and_then(|quote| quote.best_bid_size)
            .or_else(|| decimal_from_size(book.book.bids[0].size).ok().flatten())
            .or(Some(book.min_order_size));
        let ask_size = book
            .last_valid_quote
            .as_ref()
            .and_then(|quote| quote.best_ask_size)
            .or_else(|| decimal_from_size(book.book.asks[0].size).ok().flatten())
            .or(Some(book.min_order_size));

        let (best_bid, best_ask) = match side {
            ExecutionSide::Buy => (Some(bid_seed), Some(ask_seed.max(reference_price))),
            ExecutionSide::Sell => (Some(bid_seed.min(reference_price)), Some(ask_seed)),
        };

        Some(BestQuote {
            asset_id: asset_id.to_owned(),
            best_bid,
            best_bid_size: bid_size,
            best_ask,
            best_ask_size: ask_size,
            tick_size,
            min_order_size: book.min_order_size,
            neg_risk: book.neg_risk,
        })
    }

    pub async fn quote_debug_summary(&self, asset_id: &str) -> String {
        let state = self.inner.read().await;
        let Some(book) = state.get(asset_id) else {
            return "state=missing".to_owned();
        };

        let age_ms = book.observed_at.elapsed().as_millis();
        let is_stale = book.observed_at.elapsed() > self.cache_ttl;
        format!(
            "state=present stale={} age_ms={} tick_size={} min_order_size={} has_book={} last_valid_quote={} last_trade_price={}",
            is_stale,
            age_ms,
            book.tick_size,
            book.min_order_size,
            (book.book.bid_volume() + book.book.ask_volume()) > 0.0,
            book.last_valid_quote.is_some(),
            book.last_trade_price.is_some(),
        )
    }

    pub async fn record_last_trade_price(&self, asset_id: &str, price: f64) -> Result<()> {
        let Some(price_decimal) = Decimal::from_f64_retain(price) else {
            return Err(anyhow!("invalid last trade price {price}"));
        };

        let mut state = self.inner.write().await;
        let observed_at = Instant::now();
        let book_state = state
            .entry(asset_id.to_owned())
            .or_insert_with(|| AssetBookState {
                metadata: self.metadata_for(asset_id, ""),
                bids: HashMap::new(),
                asks: HashMap::new(),
                book: OrderBook::default(),
                tick_size: Decimal::ZERO,
                min_order_size: Decimal::ZERO,
                neg_risk: false,
                observed_at,
                last_valid_quote: None,
                last_valid_quote_observed_at: None,
                last_trade_price: None,
                last_trade_observed_at: None,
            });
        book_state.last_trade_price = Some(price_decimal);
        book_state.last_trade_observed_at = Some(observed_at);
        Ok(())
    }

    pub async fn market_snapshot(&self, asset_id: &str) -> Option<MarketSnapshot> {
        let state = self.inner.read().await;
        let book = state.get(asset_id)?;
        if book.observed_at.elapsed() > self.cache_ttl {
            return None;
        }
        let best_quote = best_quote_from_book(
            asset_id.to_owned(),
            &book.book,
            book.tick_size,
            book.min_order_size,
            book.neg_risk,
        )
        .ok()?;
        let mid_price = book.book.mid_price();
        let spread_ratio = match (best_quote.best_bid, best_quote.best_ask, mid_price) {
            (Some(best_bid), Some(best_ask), Some(mid)) if mid > 0.0 => {
                let best_bid = decimal_to_f64(best_bid)?;
                let best_ask = decimal_to_f64(best_ask)?;
                Some(((best_ask - best_bid) / mid).abs())
            }
            _ => None,
        };

        Some(MarketSnapshot {
            spread_ratio,
            visible_total_volume: book.book.bid_volume() + book.book.ask_volume(),
            mid_price,
            best_bid: best_quote.best_bid.and_then(decimal_to_f64),
            best_ask: best_quote.best_ask.and_then(decimal_to_f64),
            last_trade_price: book.last_trade_price.and_then(decimal_to_f64),
        })
    }

    #[allow(dead_code)]
    pub async fn best_quote_or_fetch(&self, asset_id: &str) -> Result<BestQuote> {
        self.refresh_book_snapshot(asset_id)
            .await?
            .ok_or_else(|| anyhow!("best quote missing after book refresh for asset {asset_id}"))
    }

    fn metadata_for(&self, asset_id: &str, condition_id: &str) -> AssetMetadata {
        self.catalog
            .metadata(asset_id)
            .cloned()
            .unwrap_or_else(|| AssetMetadata {
                asset_id: asset_id.to_owned(),
                condition_id: condition_id.to_owned(),
                title: asset_id.to_owned(),
                slug: asset_id.to_owned(),
                event_slug: asset_id.to_owned(),
                outcome: "UNKNOWN".to_owned(),
                outcome_index: 0,
            })
    }
}

fn levels_to_map(levels: &[crate::models::BookLevel]) -> Result<HashMap<i64, f64>> {
    let mut map = HashMap::with_capacity(levels.len());
    for level in levels {
        let price = level.price_decimal()?.to_string().parse::<f64>()?;
        let size = level.size_decimal()?.to_string().parse::<f64>()?;
        if size > 0.0 {
            map.insert(price_key(price), size);
        }
    }
    Ok(map)
}

fn top_levels(bids: &HashMap<i64, f64>, asks: &HashMap<i64, f64>) -> OrderBook {
    let mut book = OrderBook::default();

    let mut bid_levels = bids
        .iter()
        .filter(|(_, size)| **size > 0.0)
        .map(|(price, size)| OrderBookLevel {
            price: price_from_key(*price),
            size: *size,
        })
        .collect::<Vec<_>>();
    bid_levels.sort_by(|left, right| right.price.total_cmp(&left.price));
    for (index, level) in bid_levels.into_iter().take(3).enumerate() {
        book.bids[index] = level;
    }

    let mut ask_levels = asks
        .iter()
        .filter(|(_, size)| **size > 0.0)
        .map(|(price, size)| OrderBookLevel {
            price: price_from_key(*price),
            size: *size,
        })
        .collect::<Vec<_>>();
    ask_levels.sort_by(|left, right| left.price.total_cmp(&right.price));
    for (index, level) in ask_levels.into_iter().take(3).enumerate() {
        book.asks[index] = level;
    }

    book
}

fn best_quote_from_book(
    asset_id: String,
    book: &OrderBook,
    tick_size: Decimal,
    min_order_size: Decimal,
    neg_risk: bool,
) -> Result<BestQuote> {
    Ok(BestQuote {
        asset_id,
        best_bid: decimal_from_level(book.bids[0])?,
        best_bid_size: decimal_from_size(book.bids[0].size)?,
        best_ask: decimal_from_level(book.asks[0])?,
        best_ask_size: decimal_from_size(book.asks[0].size)?,
        tick_size,
        min_order_size,
        neg_risk,
    })
}

fn decimal_from_level(level: OrderBookLevel) -> Result<Option<Decimal>> {
    if level.is_empty() {
        return Ok(None);
    }
    decimal_from_f64(level.price)
}

fn decimal_from_size(size: f64) -> Result<Option<Decimal>> {
    if size <= 0.0 {
        return Ok(None);
    }
    decimal_from_f64(size)
}

fn decimal_from_f64(value: f64) -> Result<Option<Decimal>> {
    Decimal::from_f64_retain(value)
        .map(Some)
        .ok_or_else(|| anyhow!("invalid decimal conversion from {value}"))
}

fn decimal_to_f64(value: Decimal) -> Option<f64> {
    value.to_string().parse::<f64>().ok()
}

fn decimal_from_f64_lossy(value: f64) -> Option<Decimal> {
    Decimal::from_f64_retain(value)
}

#[allow(clippy::too_many_arguments)]
fn delta_from_books(
    metadata: AssetMetadata,
    previous: OrderBook,
    current: OrderBook,
    event_received_at: Instant,
    event_received_at_utc: DateTime<Utc>,
    parse_completed_at: Instant,
    parse_completed_at_utc: DateTime<Utc>,
    observed_at: Instant,
) -> OrderBookDelta {
    OrderBookDelta {
        metadata,
        best_bid_removed: removed_at_price(previous.bids[0], current.bids),
        second_bid_removed: removed_at_price(previous.bids[1], current.bids),
        best_ask_removed: removed_at_price(previous.asks[0], current.asks),
        second_ask_removed: removed_at_price(previous.asks[1], current.asks),
        removed_bid_liquidity: removed_liquidity(previous.bids, current.bids),
        removed_ask_liquidity: removed_liquidity(previous.asks, current.asks),
        previous_mid_price: previous.mid_price(),
        current_mid_price: current.mid_price(),
        current_imbalance: current.imbalance(),
        event_received_at,
        event_received_at_utc,
        parse_completed_at,
        parse_completed_at_utc,
        previous,
        current,
        observed_at,
    }
}

fn removed_liquidity(previous: [OrderBookLevel; 3], current: [OrderBookLevel; 3]) -> f64 {
    previous
        .into_iter()
        .filter(|level| !level.is_empty())
        .map(|level| removed_at_price(level, current))
        .sum()
}

fn removed_at_price(level: OrderBookLevel, current: [OrderBookLevel; 3]) -> f64 {
    if level.is_empty() {
        return 0.0;
    }
    let current_size = current
        .into_iter()
        .find(|candidate| (candidate.price - level.price).abs() <= f64::EPSILON)
        .map(|candidate| candidate.size)
        .unwrap_or(0.0);
    (level.size - current_size).max(0.0)
}

fn price_key(price: f64) -> i64 {
    (price * PRICE_SCALE).round() as i64
}

fn price_from_key(key: i64) -> f64 {
    key as f64 / PRICE_SCALE
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::orderbook::orderbook_levels::BookSide;

    #[tokio::test]
    async fn price_updates_refresh_top_levels() {
        let catalog = AssetCatalog::new(vec![AssetMetadata {
            asset_id: "asset-1".to_owned(),
            condition_id: "condition-1".to_owned(),
            title: "Market".to_owned(),
            slug: "market".to_owned(),
            event_slug: "event".to_owned(),
            outcome: "YES".to_owned(),
            outcome_index: 0,
        }]);
        let state = OrderBookState::new(&test_settings(), catalog).expect("state");
        state
            .apply_price_updates(
                "asset-1",
                "condition-1",
                &[
                    PriceLevelUpdate {
                        side: BookSide::Bid,
                        price: 0.45,
                        size: 10.0,
                    },
                    PriceLevelUpdate {
                        side: BookSide::Ask,
                        price: 0.55,
                        size: 20.0,
                    },
                ],
                Instant::now(),
                Utc::now(),
                Instant::now(),
                Utc::now(),
            )
            .await
            .expect("apply");

        let quote = state.best_quote("asset-1").await.expect("quote");
        assert_eq!(quote.best_bid.expect("bid").round_dp(2).to_string(), "0.45");
        assert_eq!(quote.best_ask.expect("ask").round_dp(2).to_string(), "0.55");
    }

    fn test_settings() -> Settings {
        let mut settings = Settings::default_for_tests(std::path::PathBuf::from("./data"));
        settings.target_profile_addresses = vec!["0xtarget".to_owned()];
        settings.market_cache_ttl = Duration::from_secs(3);
        settings.min_source_trade_usdc = rust_decimal_macros::dec!(3);
        settings
    }
}
