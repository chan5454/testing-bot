use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, TimeDelta, Utc};
use reqwest::Client;
use rust_decimal::Decimal;
use serde::Deserialize;
use tokio::sync::Mutex;
use tracing::warn;

use crate::config::{ExecutionMode, Settings};
use crate::execution::{ExecutionRequest, ExecutionSuccess};
use crate::models::{ActivityEntry, ExecutionAnalyticsState, PortfolioSnapshot};
use crate::risk::CopyDecision;

pub const SUMMARY_REFRESH_INTERVAL: Duration = Duration::from_secs(6 * 60 * 60);

#[derive(Clone, Debug, Deserialize, serde::Serialize)]
struct SummaryTradeSnapshot {
    side: String,
    size: Decimal,
    filled_price: Decimal,
    outcome: String,
    title: String,
    status: String,
}

impl SummaryTradeSnapshot {
    fn from_trade(
        source: &ActivityEntry,
        request: &ExecutionRequest,
        result: &ExecutionSuccess,
    ) -> Self {
        Self {
            side: side_label(request).to_owned(),
            size: result.filled_size,
            filled_price: result.filled_price,
            outcome: source.outcome.clone(),
            title: source.title.clone(),
            status: result.status.to_string(),
        }
    }
}

#[derive(Clone)]
pub struct TelegramNotifier {
    client: Client,
    bot_token: String,
    chat_id: String,
    async_only: bool,
    execution_mode: ExecutionMode,
    start_capital_usd: Decimal,
    summary_state_path: PathBuf,
    analytics_state_path: PathBuf,
    summary_lock: Arc<Mutex<()>>,
}

impl TelegramNotifier {
    pub fn new(settings: Settings) -> Self {
        Self {
            client: Client::new(),
            bot_token: settings.telegram_bot_token,
            chat_id: settings.telegram_chat_id,
            async_only: settings.telegram_async_only,
            execution_mode: settings.execution_mode,
            start_capital_usd: settings.start_capital_usd,
            summary_state_path: settings.data_dir.join("telegram-daily-summary.json"),
            analytics_state_path: settings.data_dir.join("execution-analytics-summary.json"),
            summary_lock: Arc::new(Mutex::new(())),
        }
    }

    pub async fn send_trade_notification(
        &self,
        source: &ActivityEntry,
        _decision: &CopyDecision,
        request: &ExecutionRequest,
        result: &ExecutionSuccess,
        previous_portfolio: &PortfolioSnapshot,
        portfolio: &PortfolioSnapshot,
    ) -> Result<()> {
        let summary_trade = SummaryTradeSnapshot::from_trade(source, request, result);
        let trade_text = build_trade_notification_text(
            source,
            request,
            result,
            previous_portfolio,
            portfolio,
            self.start_capital_usd,
        );
        self.send_message(&trade_text)
            .await
            .context("sending trade notification")?;

        self.upsert_daily_summary(
            SummaryUpdateKind::Trade,
            Some(summary_trade),
            previous_portfolio.total_value,
            portfolio,
        )
        .await
        .context("updating pinned daily summary")?;
        Ok(())
    }

    pub fn async_only(&self) -> bool {
        self.async_only
    }

    pub async fn send_startup_summary(&self, portfolio: &PortfolioSnapshot) -> Result<()> {
        self.upsert_daily_summary(
            SummaryUpdateKind::Startup,
            None,
            portfolio.total_value,
            portfolio,
        )
        .await
        .context("sending startup daily summary")
    }

    pub async fn refresh_periodic_summary(&self, portfolio: &PortfolioSnapshot) -> Result<()> {
        self.upsert_daily_summary(
            SummaryUpdateKind::Periodic,
            None,
            portfolio.total_value,
            portfolio,
        )
        .await
        .context("refreshing periodic daily summary")
    }

    pub async fn time_until_next_periodic_summary(&self) -> Result<Duration> {
        let _guard = self.summary_lock.lock().await;
        let now = Utc::now();
        let state = self.load_summary_state().await?;
        Ok(duration_until_next_periodic_summary(state.as_ref(), now))
    }

    async fn upsert_daily_summary(
        &self,
        update_kind: SummaryUpdateKind,
        trade: Option<SummaryTradeSnapshot>,
        opening_value_seed: Decimal,
        portfolio: &PortfolioSnapshot,
    ) -> Result<()> {
        let _guard = self.summary_lock.lock().await;
        let now = Utc::now();
        let today = now.date_naive().to_string();
        let mut state = self.load_summary_state().await?;

        let needs_new_message = state
            .as_ref()
            .map(|summary| summary.day != today || summary.message_id <= 0)
            .unwrap_or(true);
        let opening_value = if needs_new_message {
            opening_value_seed
        } else {
            state
                .as_ref()
                .map(|summary| summary.opening_value)
                .unwrap_or(opening_value_seed)
        };
        let last_trade = trade.or_else(|| {
            state
                .as_ref()
                .and_then(|summary| summary.last_trade.clone())
        });
        let processed_exit_events = self
            .load_execution_analytics_state()
            .await?
            .map(|analytics| {
                let close_filled = analytics
                    .exit_event_counts
                    .get("close_filled")
                    .copied()
                    .unwrap_or(0);
                let close_partial = analytics
                    .exit_event_counts
                    .get("close_partial")
                    .copied()
                    .unwrap_or(0);
                analytics
                    .processed_exit_events
                    .max(close_filled.saturating_add(close_partial))
            })
            .unwrap_or(0);
        let next_periodic_refresh_at =
            next_periodic_refresh_at_for_update(state.as_ref(), now, update_kind);
        let summary_text = self.build_daily_summary_text(
            last_trade.as_ref(),
            portfolio,
            opening_value,
            processed_exit_events,
        );

        let message_id = if needs_new_message {
            let message_id = self.send_message(&summary_text).await?;
            self.pin_message(message_id).await?;
            message_id
        } else {
            let existing_message_id = state
                .as_ref()
                .map(|summary| summary.message_id)
                .unwrap_or(0);
            match self.edit_message(existing_message_id, &summary_text).await {
                Ok(()) => existing_message_id,
                Err(error) => {
                    warn!(
                        ?error,
                        message_id = existing_message_id,
                        "failed to edit daily summary, sending a fresh pinned summary"
                    );
                    let new_message_id = self.send_message(&summary_text).await?;
                    self.pin_message(new_message_id).await?;
                    new_message_id
                }
            }
        };

        state = Some(DailySummaryState {
            day: today,
            opening_value,
            message_id,
            updated_at: now,
            next_periodic_refresh_at,
            last_trade,
        });
        self.persist_summary_state(
            state
                .as_ref()
                .ok_or_else(|| anyhow!("daily summary state missing"))?,
        )
        .await?;
        Ok(())
    }

    fn build_daily_summary_text(
        &self,
        trade: Option<&SummaryTradeSnapshot>,
        portfolio: &PortfolioSnapshot,
        opening_value: Decimal,
        processed_exit_events: u64,
    ) -> String {
        let day_pnl = portfolio.total_value - opening_value;
        let total_pnl = portfolio.realized_pnl + portfolio.unrealized_pnl;
        let window_split = portfolio.window_split(portfolio.fetched_at.date_naive());
        let positions_block = build_positions_block(portfolio);
        let last_trade_block = build_last_copied_trade_block(trade);
        let pnl_summary_block = build_pnl_summary_block(portfolio, total_pnl);
        let warning_block = build_unrealized_warning_block(processed_exit_events);

        format!(
            "<b>Polymarket {} Daily Portfolio Summary</b>\n\
<code>Date: {} UTC | Updated: {} UTC</code>\n\n\
{}\n\
<b>Portfolio</b>\n\
<pre>{:<10}{}\n{:<10}{}\n{:<10}{}\n{:<10}{} ({})\n{:<10}{} ({})\n{:<10}{}</pre>\n\
{}\n\
<b>Analytics</b>\n\
<pre>{:<18}{}\n{:<18}{}\n{:<18}{}\n{:<18}{}</pre>\n\
{}\n\
<b>Open Positions</b>\n\
<pre>{}</pre>",
            self.execution_mode.as_str().to_ascii_uppercase(),
            portfolio.fetched_at.format("%Y-%m-%d"),
            portfolio.fetched_at.format("%H:%M:%S"),
            last_trade_block,
            "Value",
            format_money(portfolio.total_value),
            "Cash",
            format_money(portfolio.cash_balance),
            "Exposure",
            format_money(portfolio.total_exposure),
            "Day P/L",
            format_signed_money(day_pnl),
            format_signed_percent(percent_change(day_pnl, opening_value)),
            "Total P/L",
            format_signed_money(total_pnl),
            format_signed_percent(percent_change(total_pnl, self.start_capital_usd)),
            "Positions",
            portfolio.positions.len(),
            pnl_summary_block,
            "Current Window",
            window_split.current_window_positions,
            "Legacy Positions",
            window_split.legacy_positions,
            "Window U.P/L",
            format_signed_money(window_split.current_window_unrealized_pnl),
            "Legacy U.P/L",
            format_signed_money(window_split.legacy_unrealized_pnl),
            warning_block,
            positions_block,
        )
    }

    async fn send_message(&self, text: &str) -> Result<i64> {
        let response: TelegramEnvelope<TelegramMessage> = self
            .client
            .post(format!(
                "https://api.telegram.org/bot{}/sendMessage",
                self.bot_token
            ))
            .json(&serde_json::json!({
                "chat_id": self.chat_id,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": true
            }))
            .send()
            .await
            .context("requesting telegram sendMessage")?
            .json()
            .await
            .context("decoding telegram sendMessage response")?;

        response
            .into_result("sendMessage")
            .map(|message| message.message_id)
    }

    async fn edit_message(&self, message_id: i64, text: &str) -> Result<()> {
        let response: TelegramEnvelope<TelegramMessage> = self
            .client
            .post(format!(
                "https://api.telegram.org/bot{}/editMessageText",
                self.bot_token
            ))
            .json(&serde_json::json!({
                "chat_id": self.chat_id,
                "message_id": message_id,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": true
            }))
            .send()
            .await
            .context("requesting telegram editMessageText")?
            .json()
            .await
            .context("decoding telegram editMessageText response")?;

        response.into_result("editMessageText").map(|_| ())
    }

    async fn pin_message(&self, message_id: i64) -> Result<()> {
        let response: TelegramEnvelope<bool> = self
            .client
            .post(format!(
                "https://api.telegram.org/bot{}/pinChatMessage",
                self.bot_token
            ))
            .json(&serde_json::json!({
                "chat_id": self.chat_id,
                "message_id": message_id,
                "disable_notification": true
            }))
            .send()
            .await
            .context("requesting telegram pinChatMessage")?
            .json()
            .await
            .context("decoding telegram pinChatMessage response")?;

        response.into_result("pinChatMessage").map(|_| ())
    }

    async fn load_summary_state(&self) -> Result<Option<DailySummaryState>> {
        match tokio::fs::read_to_string(&self.summary_state_path).await {
            Ok(contents) => serde_json::from_str::<DailySummaryState>(&contents)
                .map(Some)
                .with_context(|| format!("parsing {}", self.summary_state_path.display())),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => {
                Err(error).with_context(|| format!("reading {}", self.summary_state_path.display()))
            }
        }
    }

    async fn load_execution_analytics_state(&self) -> Result<Option<ExecutionAnalyticsState>> {
        match tokio::fs::read_to_string(&self.analytics_state_path).await {
            Ok(contents) => serde_json::from_str::<ExecutionAnalyticsState>(&contents)
                .map(Some)
                .with_context(|| format!("parsing {}", self.analytics_state_path.display())),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error)
                .with_context(|| format!("reading {}", self.analytics_state_path.display())),
        }
    }

    async fn persist_summary_state(&self, state: &DailySummaryState) -> Result<()> {
        let body = serde_json::to_string_pretty(state)?;
        tokio::fs::write(&self.summary_state_path, body)
            .await
            .with_context(|| format!("writing {}", self.summary_state_path.display()))?;
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
struct TelegramEnvelope<T> {
    ok: bool,
    result: Option<T>,
    description: Option<String>,
}

impl<T> TelegramEnvelope<T> {
    fn into_result(self, method: &str) -> Result<T> {
        if !self.ok {
            return Err(anyhow!(
                "telegram {method} failed: {}",
                self.description
                    .unwrap_or_else(|| "unknown telegram error".to_owned())
            ));
        }
        self.result
            .ok_or_else(|| anyhow!("telegram {method} returned no result"))
    }
}

#[derive(Debug, Deserialize)]
struct TelegramMessage {
    message_id: i64,
}

#[derive(Clone, Debug, Deserialize, serde::Serialize)]
struct DailySummaryState {
    day: String,
    opening_value: Decimal,
    message_id: i64,
    updated_at: DateTime<Utc>,
    #[serde(default)]
    next_periodic_refresh_at: Option<DateTime<Utc>>,
    #[serde(default)]
    last_trade: Option<SummaryTradeSnapshot>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SummaryUpdateKind {
    Startup,
    Trade,
    Periodic,
}

fn summary_refresh_interval() -> TimeDelta {
    TimeDelta::from_std(SUMMARY_REFRESH_INTERVAL).unwrap_or_else(|_| TimeDelta::hours(6))
}

fn baseline_next_periodic_refresh_at(
    state: Option<&DailySummaryState>,
    now: DateTime<Utc>,
) -> DateTime<Utc> {
    state
        .and_then(|summary| summary.next_periodic_refresh_at)
        .or_else(|| state.map(|summary| summary.updated_at + summary_refresh_interval()))
        .unwrap_or(now + summary_refresh_interval())
}

fn next_periodic_refresh_at_for_update(
    state: Option<&DailySummaryState>,
    now: DateTime<Utc>,
    update_kind: SummaryUpdateKind,
) -> Option<DateTime<Utc>> {
    let baseline = baseline_next_periodic_refresh_at(state, now);
    match update_kind {
        SummaryUpdateKind::Startup | SummaryUpdateKind::Trade => Some(baseline),
        SummaryUpdateKind::Periodic => {
            let interval = summary_refresh_interval();
            let mut next_due = baseline;
            while next_due <= now {
                next_due += interval;
            }
            Some(next_due)
        }
    }
}

fn duration_until_next_periodic_summary(
    state: Option<&DailySummaryState>,
    now: DateTime<Utc>,
) -> Duration {
    let next_due = baseline_next_periodic_refresh_at(state, now);
    if next_due <= now {
        Duration::ZERO
    } else {
        (next_due - now).to_std().unwrap_or(Duration::ZERO)
    }
}

fn build_trade_notification_text(
    source: &ActivityEntry,
    request: &ExecutionRequest,
    result: &ExecutionSuccess,
    previous_portfolio: &PortfolioSnapshot,
    portfolio: &PortfolioSnapshot,
    start_capital_usd: Decimal,
) -> String {
    let portfolio_delta = portfolio.total_value - previous_portfolio.total_value;
    let total_pnl = portfolio.total_value - start_capital_usd;
    let status = result.status.to_string();
    format!(
        "<b>Polymarket {} Copy Fill</b>\n\
<pre>{:<8} {:>10} @ {}\n{} | {}\nOrder: {}\nStatus: {} ({})</pre>\n\
<b>Portfolio</b>: {} total | {} cash | {} exposed | {} positions\n\
<b>P/L</b>: {} ({}) vs prev | {} ({}) total",
        result.mode.as_str().to_ascii_uppercase(),
        side_label(request),
        format_decimal(result.filled_size, 4),
        format_price(result.filled_price),
        html_escape(&truncate(&source.outcome, 16)),
        html_escape(&truncate(&source.title, 44)),
        html_escape(&result.order_id),
        html_escape(&status),
        result.success,
        format_money(portfolio.total_value),
        format_money(portfolio.cash_balance),
        format_money(portfolio.total_exposure),
        portfolio.positions.len(),
        format_signed_money(portfolio_delta),
        format_signed_percent(percent_change(
            portfolio_delta,
            previous_portfolio.total_value
        )),
        format_signed_money(total_pnl),
        format_signed_percent(percent_change(total_pnl, start_capital_usd)),
    )
}

fn build_last_copied_trade_block(trade: Option<&SummaryTradeSnapshot>) -> String {
    match trade {
        Some(trade) => {
            let last_trade = truncate(&trade.title, 44);
            format!(
                "<b>Last Copied Trade</b>\n\
<pre>{:<8} {:>10} @ {}\n{}\nStatus: {}</pre>",
                trade.side,
                format_decimal(trade.size, 4),
                format_price(trade.filled_price),
                html_escape(&format!("{} | {}", trade.outcome, last_trade)),
                html_escape(&trade.status),
            )
        }
        None => "<b>Last Copied Trade</b>\n<pre>Startup refresh\nAwaiting first copied trade</pre>"
            .to_owned(),
    }
}

fn build_positions_block(portfolio: &PortfolioSnapshot) -> String {
    const MAX_POSITIONS_IN_SUMMARY: usize = 6;

    if portfolio.positions.is_empty() {
        return "No open positions".to_owned();
    }

    let mut positions = portfolio.positions.clone();
    positions.sort_by(|left, right| right.current_value.cmp(&left.current_value));

    let mut lines = Vec::new();
    for (index, position) in positions.iter().take(MAX_POSITIONS_IN_SUMMARY).enumerate() {
        lines.push(format!(
            "{}. {:<10} {:>10} {:>10}",
            index + 1,
            html_escape(&truncate(&position.outcome, 10)),
            format_decimal(position.size, 4),
            format_money(position.current_value),
        ));
        lines.push(format!(
            "   {}",
            html_escape(&truncate(&position.title, 44))
        ));
    }

    let remaining = positions.len().saturating_sub(MAX_POSITIONS_IN_SUMMARY);
    if remaining > 0 {
        lines.push(format!("... and {} more", remaining));
    }

    lines.join("\n")
}

fn build_pnl_summary_block(portfolio: &PortfolioSnapshot, total_pnl: Decimal) -> String {
    format!(
        "<b>PnL Summary</b>\n\
<pre>{:<14}{}\n{:<14}{}\n{:<14}{}\n{:<14}{}\n{:<14}{}</pre>",
        "Realized",
        format_signed_money(portfolio.realized_pnl),
        "Unrealized",
        format_signed_money(portfolio.unrealized_pnl),
        "Total",
        format_signed_money(total_pnl),
        "Open Positions",
        portfolio.positions.len(),
        "Exposure",
        format_money(portfolio.total_exposure),
    )
}

fn build_unrealized_warning_block(processed_exit_events: u64) -> String {
    if processed_exit_events == 0 {
        "\u{26A0} No exits executed \u{2014} profit is unrealized\n".to_owned()
    } else {
        String::new()
    }
}

fn percent_change(change: Decimal, base: Decimal) -> Decimal {
    if base.is_zero() {
        Decimal::ZERO
    } else {
        (change / base) * Decimal::from(100)
    }
}

fn format_money(value: Decimal) -> String {
    format!("${}", value.round_dp(2))
}

fn format_signed_money(value: Decimal) -> String {
    let rounded = value.round_dp(2);
    if rounded.is_sign_negative() {
        format!("-${}", rounded.abs())
    } else {
        format!("+${}", rounded)
    }
}

fn format_signed_percent(value: Decimal) -> String {
    let rounded = value.round_dp(2);
    if rounded.is_sign_negative() {
        format!("-{}%", rounded.abs())
    } else {
        format!("+{}%", rounded)
    }
}

fn format_decimal(value: Decimal, dp: u32) -> String {
    value.round_dp(dp).normalize().to_string()
}

fn format_price(value: Decimal) -> String {
    format!("${}", value.round_dp(4).normalize())
}

fn side_label(request: &ExecutionRequest) -> &'static str {
    match request.side {
        crate::execution::ExecutionSide::Buy => "BUY",
        crate::execution::ExecutionSide::Sell => "SELL",
    }
}

fn truncate(input: &str, max_chars: usize) -> String {
    let mut output = String::new();
    for (index, character) in input.chars().enumerate() {
        if index >= max_chars {
            output.push_str("...");
            return output;
        }
        output.push(character);
    }
    output
}

fn html_escape(input: &str) -> String {
    input
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use rust_decimal_macros::dec;

    use super::*;
    use crate::execution::{ExecutionRequest, ExecutionSide};
    use crate::models::{PortfolioPosition, PortfolioSnapshot};

    fn sample_source() -> ActivityEntry {
        ActivityEntry {
            proxy_wallet: "0xsource".to_owned(),
            timestamp: 1,
            condition_id: "condition-1".to_owned(),
            type_name: "TRADE".to_owned(),
            size: 10.0,
            usdc_size: 5.0,
            transaction_hash: "0xhash".to_owned(),
            price: 0.5,
            asset: "asset-1".to_owned(),
            side: "BUY".to_owned(),
            outcome_index: 0,
            title: "Sample market title with a very long label".to_owned(),
            slug: "sample-market".to_owned(),
            event_slug: "sample-event".to_owned(),
            outcome: "YES".to_owned(),
        }
    }

    fn sample_request() -> ExecutionRequest {
        ExecutionRequest {
            token_id: "asset-1".to_owned(),
            side: ExecutionSide::Buy,
            size: dec!(9.8),
            limit_price: dec!(0.51),
            requested_notional: dec!(5),
            source_trade_id: "trade-1".to_owned(),
        }
    }

    fn sample_sell_request() -> ExecutionRequest {
        ExecutionRequest {
            token_id: "asset-1".to_owned(),
            side: ExecutionSide::Sell,
            size: dec!(4.2),
            limit_price: dec!(0.49),
            requested_notional: dec!(2.058),
            source_trade_id: "trade-sell-1".to_owned(),
        }
    }

    fn sample_result() -> ExecutionSuccess {
        ExecutionSuccess {
            mode: ExecutionMode::Paper,
            order_request: sample_request(),
            order_id: "paper-order-1".to_owned(),
            success: true,
            transaction_hashes: Vec::new(),
            filled_price: dec!(0.51),
            filled_size: dec!(9.8),
            requested_size: dec!(9.8),
            requested_price: dec!(0.51),
            status: crate::execution::ExecutionStatus::Filled,
            filled_notional: dec!(4.998),
        }
    }

    fn sample_sell_result() -> ExecutionSuccess {
        ExecutionSuccess {
            mode: ExecutionMode::Paper,
            order_request: sample_sell_request(),
            order_id: "paper-sell-order-1".to_owned(),
            success: true,
            transaction_hashes: Vec::new(),
            filled_price: dec!(0.49),
            filled_size: dec!(4.2),
            requested_size: dec!(4.2),
            requested_price: dec!(0.49),
            status: crate::execution::ExecutionStatus::Filled,
            filled_notional: dec!(2.058),
        }
    }

    fn sample_portfolio() -> PortfolioSnapshot {
        PortfolioSnapshot {
            fetched_at: Utc::now(),
            total_value: dec!(205.12),
            total_exposure: dec!(15.12),
            cash_balance: dec!(190),
            realized_pnl: dec!(3),
            unrealized_pnl: dec!(2.12),
            positions: vec![
                PortfolioPosition {
                    asset: "asset-1".to_owned(),
                    condition_id: "condition-1".to_owned(),
                    title: "Sample market title with a very long label".to_owned(),
                    outcome: "YES".to_owned(),
                    source_wallet: "0xsource".to_owned(),
                    state: crate::models::PositionState::Open,
                    size: dec!(9.8),
                    current_value: dec!(5.12),
                    average_entry_price: dec!(0.5),
                    current_price: dec!(0.5224489795),
                    cost_basis: dec!(4.9),
                    unrealized_pnl: dec!(0.22),
                    opened_at: None,
                    source_trade_timestamp_unix: 0,
                    closing_started_at: None,
                    closing_reason: None,
                    last_close_attempt_at: None,
                    close_attempts: 0,
                    close_failure_reason: None,
                    closing_escalation_level: 0,
                    stale_reason: None,
                },
                PortfolioPosition {
                    asset: "asset-2".to_owned(),
                    condition_id: "condition-2".to_owned(),
                    title: "Second market".to_owned(),
                    outcome: "NO".to_owned(),
                    source_wallet: "0xsource".to_owned(),
                    state: crate::models::PositionState::Open,
                    size: dec!(20),
                    current_value: dec!(10),
                    average_entry_price: dec!(0.4),
                    current_price: dec!(0.5),
                    cost_basis: dec!(8),
                    unrealized_pnl: dec!(2),
                    opened_at: None,
                    source_trade_timestamp_unix: 0,
                    closing_started_at: None,
                    closing_reason: None,
                    last_close_attempt_at: None,
                    close_attempts: 0,
                    close_failure_reason: None,
                    closing_escalation_level: 0,
                    stale_reason: None,
                },
            ],
        }
    }

    fn sample_previous_portfolio() -> PortfolioSnapshot {
        PortfolioSnapshot {
            fetched_at: Utc::now(),
            total_value: dec!(200),
            total_exposure: dec!(10),
            cash_balance: dec!(190),
            realized_pnl: Decimal::ZERO,
            unrealized_pnl: Decimal::ZERO,
            positions: vec![PortfolioPosition {
                asset: "asset-2".to_owned(),
                condition_id: "condition-2".to_owned(),
                title: "Second market".to_owned(),
                outcome: "NO".to_owned(),
                source_wallet: "0xsource".to_owned(),
                state: crate::models::PositionState::Open,
                size: dec!(20),
                current_value: dec!(10),
                average_entry_price: dec!(0.5),
                current_price: dec!(0.5),
                cost_basis: dec!(10),
                unrealized_pnl: Decimal::ZERO,
                opened_at: None,
                source_trade_timestamp_unix: 0,
                closing_started_at: None,
                closing_reason: None,
                last_close_attempt_at: None,
                close_attempts: 0,
                close_failure_reason: None,
                closing_escalation_level: 0,
                stale_reason: None,
            }],
        }
    }

    #[test]
    fn daily_summary_contains_positions_and_profit_lines() {
        let notifier = TelegramNotifier {
            client: Client::new(),
            bot_token: "token".to_owned(),
            chat_id: "chat".to_owned(),
            async_only: true,
            execution_mode: ExecutionMode::Paper,
            start_capital_usd: dec!(200),
            summary_state_path: PathBuf::from("telegram-daily-summary.json"),
            analytics_state_path: PathBuf::from("execution-analytics-summary.json"),
            summary_lock: Arc::new(Mutex::new(())),
        };

        let text = notifier.build_daily_summary_text(
            Some(&SummaryTradeSnapshot::from_trade(
                &sample_source(),
                &sample_request(),
                &sample_result(),
            )),
            &sample_portfolio(),
            dec!(200),
            0,
        );

        assert!(text.contains("Daily Portfolio Summary"));
        assert!(text.contains("Day P/L"));
        assert!(text.contains("Total P/L"));
        assert!(text.contains("Open Positions"));
        assert!(text.contains("Second market"));
        assert!(text.contains("PnL Summary"));
        assert!(text.contains("Realized"));
        assert!(text.contains("Unrealized"));
        assert!(text.contains("Exposure"));
        assert!(text.contains("\u{26A0} No exits executed \u{2014} profit is unrealized"));
    }

    #[test]
    fn startup_daily_summary_mentions_startup_refresh() {
        let notifier = TelegramNotifier {
            client: Client::new(),
            bot_token: "token".to_owned(),
            chat_id: "chat".to_owned(),
            async_only: true,
            execution_mode: ExecutionMode::Paper,
            start_capital_usd: dec!(200),
            summary_state_path: PathBuf::from("telegram-daily-summary.json"),
            analytics_state_path: PathBuf::from("execution-analytics-summary.json"),
            summary_lock: Arc::new(Mutex::new(())),
        };

        let text = notifier.build_daily_summary_text(None, &sample_portfolio(), dec!(205.12), 1);

        assert!(text.contains("Startup refresh"));
        assert!(text.contains("Awaiting first copied trade"));
        assert!(text.contains("Open Positions"));
        assert!(!text.contains("\u{26A0} No exits executed \u{2014} profit is unrealized"));
    }

    #[test]
    fn daily_summary_uses_realized_plus_unrealized_total() {
        let notifier = TelegramNotifier {
            client: Client::new(),
            bot_token: "token".to_owned(),
            chat_id: "chat".to_owned(),
            async_only: true,
            execution_mode: ExecutionMode::Paper,
            start_capital_usd: dec!(200),
            summary_state_path: PathBuf::from("telegram-daily-summary.json"),
            analytics_state_path: PathBuf::from("execution-analytics-summary.json"),
            summary_lock: Arc::new(Mutex::new(())),
        };

        let text = notifier.build_daily_summary_text(None, &sample_portfolio(), dec!(200), 2);

        assert!(text.contains("Realized"));
        assert!(text.contains("+$3"));
        assert!(text.contains("Unrealized"));
        assert!(text.contains("+$2.12"));
        assert!(text.contains("Total"));
        assert!(text.contains("+$5.12"));
        assert!(text.contains("Open Positions"));
        assert!(text.contains("2"));
        assert!(text.contains("Exposure"));
        assert!(text.contains("$15.12"));
    }

    #[test]
    fn startup_updates_preserve_existing_periodic_due_time() {
        let now = DateTime::parse_from_rfc3339("2026-03-24T18:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let existing_due = DateTime::parse_from_rfc3339("2026-03-24T21:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let state = DailySummaryState {
            day: "2026-03-24".to_owned(),
            opening_value: dec!(200),
            message_id: 123,
            updated_at: DateTime::parse_from_rfc3339("2026-03-24T17:30:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
            next_periodic_refresh_at: Some(existing_due),
            last_trade: None,
        };

        assert_eq!(
            next_periodic_refresh_at_for_update(Some(&state), now, SummaryUpdateKind::Startup),
            Some(existing_due)
        );
    }

    #[test]
    fn periodic_updates_advance_overdue_due_time() {
        let now = DateTime::parse_from_rfc3339("2026-03-24T18:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let state = DailySummaryState {
            day: "2026-03-24".to_owned(),
            opening_value: dec!(200),
            message_id: 123,
            updated_at: DateTime::parse_from_rfc3339("2026-03-24T06:00:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
            next_periodic_refresh_at: Some(
                DateTime::parse_from_rfc3339("2026-03-24T12:00:00Z")
                    .expect("timestamp")
                    .with_timezone(&Utc),
            ),
            last_trade: None,
        };

        assert_eq!(
            next_periodic_refresh_at_for_update(Some(&state), now, SummaryUpdateKind::Periodic),
            Some(
                DateTime::parse_from_rfc3339("2026-03-25T00:00:00Z")
                    .expect("timestamp")
                    .with_timezone(&Utc)
            )
        );
    }

    #[test]
    fn overdue_periodic_summary_runs_immediately_after_restart() {
        let now = DateTime::parse_from_rfc3339("2026-03-24T18:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let state = DailySummaryState {
            day: "2026-03-24".to_owned(),
            opening_value: dec!(200),
            message_id: 123,
            updated_at: DateTime::parse_from_rfc3339("2026-03-24T05:00:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
            next_periodic_refresh_at: Some(
                DateTime::parse_from_rfc3339("2026-03-24T12:00:00Z")
                    .expect("timestamp")
                    .with_timezone(&Utc),
            ),
            last_trade: None,
        };

        assert_eq!(
            duration_until_next_periodic_summary(Some(&state), now),
            Duration::ZERO
        );
    }

    #[test]
    fn trade_notification_contains_profit_lines() {
        let text = build_trade_notification_text(
            &sample_source(),
            &sample_request(),
            &sample_result(),
            &sample_previous_portfolio(),
            &sample_portfolio(),
            dec!(200),
        );

        assert!(text.contains("<b>P/L</b>:"));
        assert!(text.contains("+$5.12"));
        assert!(text.contains("vs prev"));
        assert!(text.contains("total"));
    }

    #[test]
    fn sell_trade_notification_labels_side_as_sell() {
        let text = build_trade_notification_text(
            &sample_source(),
            &sample_sell_request(),
            &sample_sell_result(),
            &sample_previous_portfolio(),
            &sample_portfolio(),
            dec!(200),
        );

        assert!(text.contains("SELL"));
        assert!(!text.contains("BUY"));
    }
}
