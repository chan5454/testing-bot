pub const TOP_LEVEL_COUNT: usize = 3;
const EMPTY_LEVEL: OrderBookLevel = OrderBookLevel {
    price: 0.0,
    size: 0.0,
};

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct OrderBookLevel {
    pub price: f64,
    pub size: f64,
}

impl OrderBookLevel {
    pub fn is_empty(self) -> bool {
        self.price <= 0.0 || self.size <= 0.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct OrderBook {
    pub bids: [OrderBookLevel; TOP_LEVEL_COUNT],
    pub asks: [OrderBookLevel; TOP_LEVEL_COUNT],
}

impl Default for OrderBook {
    fn default() -> Self {
        Self {
            bids: [EMPTY_LEVEL; TOP_LEVEL_COUNT],
            asks: [EMPTY_LEVEL; TOP_LEVEL_COUNT],
        }
    }
}

impl OrderBook {
    pub fn bid_volume(self) -> f64 {
        self.bids
            .iter()
            .filter(|level| !level.is_empty())
            .map(|level| level.size)
            .sum()
    }

    pub fn ask_volume(self) -> f64 {
        self.asks
            .iter()
            .filter(|level| !level.is_empty())
            .map(|level| level.size)
            .sum()
    }

    pub fn mid_price(self) -> Option<f64> {
        let bid = self.bids[0];
        let ask = self.asks[0];
        if bid.is_empty() || ask.is_empty() {
            return None;
        }
        Some((bid.price + ask.price) / 2.0)
    }

    pub fn imbalance(self) -> Option<f64> {
        let ask_volume = self.ask_volume();
        if ask_volume <= 0.0 {
            return None;
        }
        Some(self.bid_volume() / ask_volume)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BookSide {
    Bid,
    Ask,
}

#[derive(Clone, Debug)]
pub struct PriceLevelUpdate {
    pub side: BookSide,
    pub price: f64,
    pub size: f64,
}
