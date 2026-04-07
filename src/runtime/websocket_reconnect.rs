use std::io::ErrorKind;
use std::time::{Duration, Instant};

use tokio_tungstenite::tungstenite::Error as WsError;
use tokio_tungstenite::tungstenite::error::ProtocolError;
use tracing::{info, warn};

// Polymarket expects market/user heartbeats at least every 10 seconds.
pub const WS_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(8);
const INITIAL_BACKOFF_MS: u64 = 100;
const SECOND_BACKOFF_MS: u64 = 200;
const THIRD_BACKOFF_MS: u64 = 500;
const MAX_BACKOFF_MS: u64 = 5_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReconnectAttempt {
    pub attempt: u32,
    pub delay: Duration,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReconnectSuccess {
    pub attempts: u32,
    pub latency: Duration,
}

#[derive(Clone, Debug, Default)]
pub struct WebSocketReconnectState {
    disconnected_at: Option<Instant>,
    attempts: u32,
}

impl WebSocketReconnectState {
    pub fn note_disconnect(&mut self) {
        if self.disconnected_at.is_none() {
            self.disconnected_at = Some(Instant::now());
            self.attempts = 0;
        }
    }

    pub fn next_attempt(&mut self) -> Option<ReconnectAttempt> {
        self.disconnected_at?;
        self.attempts = self.attempts.saturating_add(1);
        Some(ReconnectAttempt {
            attempt: self.attempts,
            delay: reconnect_backoff_delay(self.attempts),
        })
    }

    pub fn mark_recovered(&mut self) -> Option<ReconnectSuccess> {
        let disconnected_at = self.disconnected_at.take()?;
        let attempts = self.attempts.max(1);
        self.attempts = 0;
        Some(ReconnectSuccess {
            attempts,
            latency: disconnected_at.elapsed(),
        })
    }
}

pub fn reconnect_backoff_delay(attempt: u32) -> Duration {
    let delay_ms = match attempt {
        0 | 1 => INITIAL_BACKOFF_MS,
        2 => SECOND_BACKOFF_MS,
        3 => THIRD_BACKOFF_MS,
        _ => {
            let exponent = attempt.saturating_sub(3).min(16);
            THIRD_BACKOFF_MS
                .saturating_mul(1_u64 << exponent)
                .min(MAX_BACKOFF_MS)
        }
    };
    Duration::from_millis(delay_ms)
}

pub fn log_websocket_error(error: &WsError, message: &'static str) {
    if is_expected_disconnect(error) {
        info!(?error, "{message}; reconnecting");
    } else {
        warn!(?error, "{message}");
    }
}

pub fn is_expected_disconnect(error: &WsError) -> bool {
    let message = error.to_string();
    let lower = message.to_ascii_lowercase();
    matches!(error, WsError::ConnectionClosed | WsError::AlreadyClosed)
        || matches!(
            error,
            WsError::Protocol(ProtocolError::ResetWithoutClosingHandshake)
        )
        || matches!(
            error,
            WsError::Io(io)
                if matches!(
                    io.kind(),
                    ErrorKind::ConnectionReset
                        | ErrorKind::ConnectionAborted
                        | ErrorKind::BrokenPipe
                        | ErrorKind::UnexpectedEof
                )
        )
        || lower.contains("connection reset by peer")
        || lower.contains("without sending tls close_notify")
        || lower.contains("unexpected eof")
}

#[cfg(test)]
mod tests {
    use std::io;

    use super::*;

    #[test]
    fn reconnect_backoff_caps_at_five_seconds() {
        assert_eq!(reconnect_backoff_delay(1), Duration::from_millis(100));
        assert_eq!(reconnect_backoff_delay(2), Duration::from_millis(200));
        assert_eq!(reconnect_backoff_delay(3), Duration::from_millis(500));
        assert_eq!(reconnect_backoff_delay(4), Duration::from_millis(1_000));
        assert_eq!(reconnect_backoff_delay(5), Duration::from_millis(2_000));
        assert_eq!(reconnect_backoff_delay(6), Duration::from_millis(4_000));
        assert_eq!(reconnect_backoff_delay(7), Duration::from_millis(5_000));
        assert_eq!(reconnect_backoff_delay(20), Duration::from_millis(5_000));
    }

    #[test]
    fn recognizes_expected_disconnect_errors() {
        let reset = WsError::Io(io::Error::new(ErrorKind::ConnectionReset, "reset"));
        let eof = WsError::Io(io::Error::new(ErrorKind::UnexpectedEof, "unexpected eof"));

        assert!(is_expected_disconnect(&reset));
        assert!(is_expected_disconnect(&eof));
    }

    #[test]
    fn reconnect_state_reports_attempts_and_recovery_latency() {
        let mut state = WebSocketReconnectState::default();

        assert!(state.next_attempt().is_none());

        state.note_disconnect();
        let attempt = state.next_attempt().expect("attempt");
        assert_eq!(attempt.attempt, 1);
        assert_eq!(attempt.delay, Duration::from_millis(100));

        let recovered = state.mark_recovered().expect("recovered");
        assert_eq!(recovered.attempts, 1);
        assert!(recovered.latency >= Duration::ZERO);
        assert!(state.mark_recovered().is_none());
    }
}
