use rust_decimal::Decimal;
use rust_decimal_macros::dec;

#[derive(Clone, Debug, PartialEq)]
pub struct WalletScore {
    pub avg_entry_price: Decimal,
    pub win_rate: Decimal,
    pub avg_trade_size: Decimal,
    pub early_entry_ratio: Decimal,
}

pub type WalletStats = WalletScore;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TrackedWallet {
    pub address: String,
}

impl TrackedWallet {
    pub fn new(address: impl Into<String>) -> Self {
        Self {
            address: address.into(),
        }
    }
}

pub fn compute_wallet_score(wallet: &WalletStats) -> Decimal {
    let early_entry_bonus = if wallet.avg_entry_price < dec!(0.7) {
        dec!(1.2)
    } else {
        dec!(0.5)
    };

    wallet.win_rate * early_entry_bonus
}
