use serde::{Deserialize, Serialize};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

#[derive(Clone, Debug, PartialEq)]
pub struct WalletScore {
    pub avg_entry_price: Decimal,
    pub win_rate: Decimal,
    pub avg_trade_size: Decimal,
    pub early_entry_ratio: Decimal,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WalletCopyabilityClass {
    #[default]
    Unproven,
    MicroScalper,
    TimeExitHeavy,
    StopLossHeavy,
    Marginal,
    Copyable,
    HighConviction,
}

impl WalletCopyabilityClass {
    pub fn multiplier(self) -> f64 {
        match self {
            Self::Unproven => 0.90,
            Self::MicroScalper => 0.35,
            Self::TimeExitHeavy => 0.55,
            Self::StopLossHeavy => 0.60,
            Self::Marginal => 0.80,
            Self::Copyable => 1.05,
            Self::HighConviction => 1.15,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unproven => "unproven",
            Self::MicroScalper => "micro_scalper",
            Self::TimeExitHeavy => "time_exit_heavy",
            Self::StopLossHeavy => "stop_loss_heavy",
            Self::Marginal => "marginal",
            Self::Copyable => "copyable",
            Self::HighConviction => "high_conviction",
        }
    }
}

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

pub fn compute_wallet_score(wallet: &WalletScore) -> Decimal {
    let early_entry_bonus = if wallet.avg_entry_price < dec!(0.7) {
        dec!(1.2)
    } else {
        dec!(0.5)
    };

    wallet.win_rate * early_entry_bonus
}

pub fn classify_wallet_copyability(
    trade_count: u64,
    win_rate: Decimal,
    favorable_exit_share: Decimal,
    time_exit_share: Decimal,
    stop_exit_share: Decimal,
    avg_hold_ms: u64,
    avg_pnl_per_trade: Decimal,
    alpha_score: Decimal,
) -> WalletCopyabilityClass {
    if trade_count < 3 {
        return WalletCopyabilityClass::Unproven;
    }

    if avg_hold_ms > 0 && avg_hold_ms < 3_000 {
        return WalletCopyabilityClass::MicroScalper;
    }

    if alpha_score >= dec!(0.78)
        && win_rate >= dec!(0.58)
        && favorable_exit_share >= dec!(0.55)
        && time_exit_share <= dec!(0.30)
        && stop_exit_share <= dec!(0.15)
        && avg_hold_ms >= 20_000
        && avg_pnl_per_trade >= Decimal::ZERO
    {
        return WalletCopyabilityClass::HighConviction;
    }

    if alpha_score >= dec!(0.62)
        && win_rate >= dec!(0.50)
        && favorable_exit_share >= dec!(0.45)
        && stop_exit_share <= dec!(0.25)
        && avg_hold_ms >= 10_000
        && avg_pnl_per_trade >= Decimal::ZERO
    {
        return WalletCopyabilityClass::Copyable;
    }

    if avg_hold_ms < 8_000 && favorable_exit_share < dec!(0.40) {
        return WalletCopyabilityClass::MicroScalper;
    }

    if time_exit_share >= dec!(0.50)
        && favorable_exit_share < dec!(0.40)
        && (avg_pnl_per_trade <= Decimal::ZERO || avg_hold_ms < 60_000)
    {
        return WalletCopyabilityClass::TimeExitHeavy;
    }

    if stop_exit_share >= dec!(0.35)
        && (win_rate < dec!(0.48) || avg_pnl_per_trade < Decimal::ZERO)
    {
        return WalletCopyabilityClass::StopLossHeavy;
    }

    WalletCopyabilityClass::Marginal
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_micro_scalper_from_short_holds() {
        let class = classify_wallet_copyability(
            5,
            dec!(0.55),
            dec!(0.20),
            dec!(0.30),
            dec!(0.10),
            1_500,
            dec!(0.02),
            dec!(0.70),
        );

        assert_eq!(class, WalletCopyabilityClass::MicroScalper);
    }

    #[test]
    fn classifies_copyable_wallet_from_realized_exit_quality() {
        let class = classify_wallet_copyability(
            6,
            dec!(0.67),
            dec!(0.67),
            dec!(0.17),
            dec!(0.16),
            30_000,
            dec!(0.18),
            dec!(0.72),
        );

        assert_eq!(class, WalletCopyabilityClass::Copyable);
    }
}
