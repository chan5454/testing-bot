use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use tokio::sync::Notify;

#[derive(Clone)]
pub struct RuntimeControl {
    inner: Arc<RuntimeControlInner>,
}

struct RuntimeControlInner {
    trading_paused: AtomicBool,
    reconnect_generation: AtomicU64,
    market_ready_generation: AtomicU64,
    market_ready: AtomicBool,
    wallet_ready_generation: AtomicU64,
    wallet_stream_expected: bool,
    reconnect_notify: Arc<Notify>,
}

impl RuntimeControl {
    pub fn new(wallet_stream_expected: bool) -> Self {
        Self {
            inner: Arc::new(RuntimeControlInner {
                trading_paused: AtomicBool::new(false),
                reconnect_generation: AtomicU64::new(1),
                market_ready_generation: AtomicU64::new(0),
                market_ready: AtomicBool::new(false),
                wallet_ready_generation: AtomicU64::new(if wallet_stream_expected { 0 } else { 1 }),
                wallet_stream_expected,
                reconnect_notify: Arc::new(Notify::new()),
            }),
        }
    }

    pub fn is_paused(&self) -> bool {
        self.inner.trading_paused.load(Ordering::Acquire)
    }

    pub fn current_generation(&self) -> u64 {
        self.inner.reconnect_generation.load(Ordering::Acquire)
    }

    pub fn should_reconnect(&self, generation: u64) -> bool {
        self.current_generation() != generation
    }

    pub fn reconnect_notify(&self) -> Arc<Notify> {
        Arc::clone(&self.inner.reconnect_notify)
    }

    pub fn request_pause_and_reconnect(&self) -> u64 {
        self.inner.trading_paused.store(true, Ordering::Release);
        self.inner.market_ready.store(false, Ordering::Release);
        let generation = self
            .inner
            .reconnect_generation
            .fetch_add(1, Ordering::AcqRel)
            + 1;
        self.inner.reconnect_notify.notify_waiters();
        generation
    }

    pub fn resume(&self) {
        self.inner.trading_paused.store(false, Ordering::Release);
    }

    pub fn mark_market_ready(&self, generation: u64) {
        self.inner
            .market_ready_generation
            .store(generation, Ordering::Release);
        self.inner.market_ready.store(true, Ordering::Release);
    }

    pub fn mark_market_unready(&self) {
        self.inner.market_ready.store(false, Ordering::Release);
    }

    pub fn mark_wallet_ready(&self, generation: u64) {
        if self.inner.wallet_stream_expected {
            self.inner
                .wallet_ready_generation
                .store(generation, Ordering::Release);
        }
    }

    pub fn streams_ready(&self, generation: u64) -> bool {
        let market_ready = self.inner.market_ready.load(Ordering::Acquire)
            && self.inner.market_ready_generation.load(Ordering::Acquire) >= generation;
        let wallet_ready = !self.inner.wallet_stream_expected
            || self.inner.wallet_ready_generation.load(Ordering::Acquire) >= generation;
        market_ready && wallet_ready
    }
}

#[cfg(test)]
mod tests {
    use super::RuntimeControl;

    #[test]
    fn streams_ready_requires_live_market_flag() {
        let runtime = RuntimeControl::new(false);
        let generation = runtime.current_generation();

        assert!(!runtime.streams_ready(generation));

        runtime.mark_market_ready(generation);
        assert!(runtime.streams_ready(generation));

        runtime.mark_market_unready();
        assert!(!runtime.streams_ready(generation));
    }

    #[test]
    fn pause_and_reconnect_clears_market_ready_state() {
        let runtime = RuntimeControl::new(false);
        let generation = runtime.current_generation();
        runtime.mark_market_ready(generation);

        let next_generation = runtime.request_pause_and_reconnect();

        assert_eq!(next_generation, generation + 1);
        assert!(!runtime.streams_ready(generation));
        assert!(!runtime.streams_ready(next_generation));
    }
}
