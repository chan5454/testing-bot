use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

#[derive(Clone, Copy, Debug, Default)]
pub struct BackpressureSnapshot {
    pub hot_entry_queue_depth: usize,
    pub hot_exit_queue_depth: usize,
    pub cold_path_queue_depth: usize,
    pub dropped_diagnostics: u64,
    pub degradation_mode: bool,
}

#[derive(Clone)]
pub struct RuntimeBackpressure {
    inner: Arc<RuntimeBackpressureInner>,
}

struct RuntimeBackpressureInner {
    hot_entry_queue_depth: AtomicUsize,
    hot_exit_queue_depth: AtomicUsize,
    cold_path_queue_depth: AtomicUsize,
    dropped_diagnostics: AtomicU64,
    degradation_mode: AtomicBool,
    hot_threshold: usize,
    cold_threshold: usize,
    hot_recover_threshold: usize,
    cold_recover_threshold: usize,
}

impl RuntimeBackpressure {
    pub fn new(hot_capacity: usize, cold_capacity: usize) -> Self {
        let hot_capacity = hot_capacity.max(1);
        let cold_capacity = cold_capacity.max(1);
        Self {
            inner: Arc::new(RuntimeBackpressureInner {
                hot_entry_queue_depth: AtomicUsize::new(0),
                hot_exit_queue_depth: AtomicUsize::new(0),
                cold_path_queue_depth: AtomicUsize::new(0),
                dropped_diagnostics: AtomicU64::new(0),
                degradation_mode: AtomicBool::new(false),
                hot_threshold: ((hot_capacity as f64) * 0.75).ceil() as usize,
                cold_threshold: ((cold_capacity as f64) * 0.80).ceil() as usize,
                hot_recover_threshold: ((hot_capacity as f64) * 0.50).floor() as usize,
                cold_recover_threshold: ((cold_capacity as f64) * 0.50).floor() as usize,
            }),
        }
    }

    pub fn set_hot_entry_queue_depth(&self, depth: usize) {
        self.inner
            .hot_entry_queue_depth
            .store(depth, Ordering::Relaxed);
        self.refresh_degradation_mode();
    }

    pub fn set_hot_exit_queue_depth(&self, depth: usize) {
        self.inner
            .hot_exit_queue_depth
            .store(depth, Ordering::Relaxed);
        self.refresh_degradation_mode();
    }

    pub fn increment_cold_path_depth(&self) {
        self.inner
            .cold_path_queue_depth
            .fetch_add(1, Ordering::Relaxed);
        self.refresh_degradation_mode();
    }

    pub fn decrement_cold_path_depth(&self) {
        loop {
            let current = self.inner.cold_path_queue_depth.load(Ordering::Relaxed);
            if current == 0 {
                break;
            }
            if self
                .inner
                .cold_path_queue_depth
                .compare_exchange(current, current - 1, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }
        }
        self.refresh_degradation_mode();
    }

    pub fn note_diagnostic_drop(&self) {
        self.inner
            .dropped_diagnostics
            .fetch_add(1, Ordering::Relaxed);
        self.inner.degradation_mode.store(true, Ordering::Relaxed);
    }

    pub fn should_shed_diagnostics(&self) -> bool {
        self.inner.degradation_mode.load(Ordering::Relaxed)
    }

    pub fn snapshot(&self) -> BackpressureSnapshot {
        BackpressureSnapshot {
            hot_entry_queue_depth: self.inner.hot_entry_queue_depth.load(Ordering::Relaxed),
            hot_exit_queue_depth: self.inner.hot_exit_queue_depth.load(Ordering::Relaxed),
            cold_path_queue_depth: self.inner.cold_path_queue_depth.load(Ordering::Relaxed),
            dropped_diagnostics: self.inner.dropped_diagnostics.load(Ordering::Relaxed),
            degradation_mode: self.inner.degradation_mode.load(Ordering::Relaxed),
        }
    }

    fn refresh_degradation_mode(&self) {
        let hot_entry = self.inner.hot_entry_queue_depth.load(Ordering::Relaxed);
        let hot_exit = self.inner.hot_exit_queue_depth.load(Ordering::Relaxed);
        let cold = self.inner.cold_path_queue_depth.load(Ordering::Relaxed);
        let active = self.inner.degradation_mode.load(Ordering::Relaxed);

        let next = if active {
            hot_entry > self.inner.hot_recover_threshold
                || hot_exit > self.inner.hot_recover_threshold
                || cold > self.inner.cold_recover_threshold
        } else {
            hot_entry >= self.inner.hot_threshold
                || hot_exit >= self.inner.hot_threshold
                || cold >= self.inner.cold_threshold
        };

        self.inner.degradation_mode.store(next, Ordering::Relaxed);
    }
}

impl Default for RuntimeBackpressure {
    fn default() -> Self {
        Self::new(1, 1)
    }
}

#[cfg(test)]
mod tests {
    use super::RuntimeBackpressure;

    #[test]
    fn degradation_mode_engages_on_queue_pressure_and_recovers() {
        let backpressure = RuntimeBackpressure::new(8, 16);

        backpressure.set_hot_exit_queue_depth(6);
        assert!(backpressure.snapshot().degradation_mode);

        backpressure.set_hot_exit_queue_depth(2);
        assert!(!backpressure.snapshot().degradation_mode);

        for _ in 0..13 {
            backpressure.increment_cold_path_depth();
        }
        assert!(backpressure.snapshot().degradation_mode);

        for _ in 0..13 {
            backpressure.decrement_cold_path_depth();
        }
        assert!(!backpressure.snapshot().degradation_mode);
    }
}
