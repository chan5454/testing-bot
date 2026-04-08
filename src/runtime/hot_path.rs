use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::sync::mpsc;

use crate::runtime::backpressure::RuntimeBackpressure;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HotPathTaskKind {
    PredictedEntry,
    DirectEntry,
    SourceExit,
}

impl HotPathTaskKind {
    pub fn is_exit(self) -> bool {
        matches!(self, Self::SourceExit)
    }

    pub fn is_predicted_entry(self) -> bool {
        matches!(self, Self::PredictedEntry)
    }
}

#[derive(Debug)]
pub enum HotPathEnqueue<T> {
    Queued,
    ProcessInline(T),
    Dropped(T),
}

#[derive(Clone)]
pub struct HotPathQueue<T> {
    exit_tx: mpsc::Sender<T>,
    entry_tx: mpsc::Sender<T>,
    depths: Arc<HotPathDepths>,
    backpressure: RuntimeBackpressure,
}

struct HotPathDepths {
    exit_depth: AtomicUsize,
    entry_depth: AtomicUsize,
}

pub struct HotPathReceiver<T> {
    exit_rx: mpsc::Receiver<T>,
    entry_rx: mpsc::Receiver<T>,
    depths: Arc<HotPathDepths>,
    backpressure: RuntimeBackpressure,
    strict_exit_priority: bool,
}

impl<T> HotPathQueue<T> {
    pub fn new(
        capacity: usize,
        strict_exit_priority: bool,
        backpressure: RuntimeBackpressure,
    ) -> (Self, HotPathReceiver<T>) {
        let (exit_tx, exit_rx) = mpsc::channel(capacity.max(1));
        let (entry_tx, entry_rx) = mpsc::channel(capacity.max(1));
        let depths = Arc::new(HotPathDepths {
            exit_depth: AtomicUsize::new(0),
            entry_depth: AtomicUsize::new(0),
        });
        (
            Self {
                exit_tx,
                entry_tx,
                depths: depths.clone(),
                backpressure: backpressure.clone(),
            },
            HotPathReceiver {
                exit_rx,
                entry_rx,
                depths,
                backpressure,
                strict_exit_priority,
            },
        )
    }

    pub fn enqueue(&self, task: T, kind: HotPathTaskKind) -> HotPathEnqueue<T> {
        let (sender, depth) = if kind.is_exit() {
            (&self.exit_tx, &self.depths.exit_depth)
        } else {
            (&self.entry_tx, &self.depths.entry_depth)
        };
        depth.fetch_add(1, Ordering::Relaxed);
        self.publish_depths();

        match sender.try_send(task) {
            Ok(()) => HotPathEnqueue::Queued,
            Err(tokio::sync::mpsc::error::TrySendError::Full(task))
            | Err(tokio::sync::mpsc::error::TrySendError::Closed(task)) => {
                depth.fetch_sub(1, Ordering::Relaxed);
                self.publish_depths();
                if kind.is_predicted_entry() {
                    HotPathEnqueue::Dropped(task)
                } else {
                    HotPathEnqueue::ProcessInline(task)
                }
            }
        }
    }

    fn publish_depths(&self) {
        self.backpressure
            .set_hot_exit_queue_depth(self.depths.exit_depth.load(Ordering::Relaxed));
        self.backpressure
            .set_hot_entry_queue_depth(self.depths.entry_depth.load(Ordering::Relaxed));
    }
}

impl<T> HotPathReceiver<T> {
    pub async fn recv(&mut self) -> Option<T> {
        if self.strict_exit_priority {
            if let Ok(task) = self.exit_rx.try_recv() {
                self.depths.exit_depth.fetch_sub(1, Ordering::Relaxed);
                self.publish_depths();
                return Some(task);
            }
            tokio::select! {
                biased;
                maybe_task = self.exit_rx.recv() => {
                    self.on_receive(maybe_task, true)
                }
                maybe_task = self.entry_rx.recv() => {
                    self.on_receive(maybe_task, false)
                }
            }
        } else {
            tokio::select! {
                maybe_task = self.exit_rx.recv() => {
                    self.on_receive(maybe_task, true)
                }
                maybe_task = self.entry_rx.recv() => {
                    self.on_receive(maybe_task, false)
                }
            }
        }
    }

    fn on_receive(&self, task: Option<T>, is_exit: bool) -> Option<T> {
        if task.is_some() {
            let depth = if is_exit {
                &self.depths.exit_depth
            } else {
                &self.depths.entry_depth
            };
            depth.fetch_sub(1, Ordering::Relaxed);
            self.publish_depths();
        }
        task
    }

    fn publish_depths(&self) {
        self.backpressure
            .set_hot_exit_queue_depth(self.depths.exit_depth.load(Ordering::Relaxed));
        self.backpressure
            .set_hot_entry_queue_depth(self.depths.entry_depth.load(Ordering::Relaxed));
    }
}

#[cfg(test)]
mod tests {
    use super::{HotPathEnqueue, HotPathQueue, HotPathTaskKind};
    use crate::runtime::backpressure::RuntimeBackpressure;

    #[tokio::test]
    async fn exits_are_received_before_entries_when_priority_is_strict() {
        let (queue, mut receiver) = HotPathQueue::new(8, true, RuntimeBackpressure::new(8, 8));

        assert!(matches!(
            queue.enqueue("entry-1", HotPathTaskKind::PredictedEntry),
            HotPathEnqueue::Queued
        ));
        assert!(matches!(
            queue.enqueue("exit-1", HotPathTaskKind::SourceExit),
            HotPathEnqueue::Queued
        ));

        assert_eq!(receiver.recv().await, Some("exit-1"));
        assert_eq!(receiver.recv().await, Some("entry-1"));
    }

    #[test]
    fn overflowing_predicted_entries_are_dropped_but_exits_fall_back_inline() {
        let (queue, _receiver) = HotPathQueue::new(1, true, RuntimeBackpressure::new(1, 1));

        assert!(matches!(
            queue.enqueue(1, HotPathTaskKind::PredictedEntry),
            HotPathEnqueue::Queued
        ));
        assert!(matches!(
            queue.enqueue(2, HotPathTaskKind::PredictedEntry),
            HotPathEnqueue::Dropped(2)
        ));
        assert!(matches!(
            queue.enqueue(3, HotPathTaskKind::SourceExit),
            HotPathEnqueue::Queued
        ));
        assert!(matches!(
            queue.enqueue(4, HotPathTaskKind::SourceExit),
            HotPathEnqueue::ProcessInline(4)
        ));
    }

    #[test]
    fn cold_path_pressure_does_not_block_direct_entries_or_exits() {
        let backpressure = RuntimeBackpressure::new(4, 4);
        for _ in 0..4 {
            backpressure.increment_cold_path_depth();
        }
        assert!(backpressure.snapshot().degradation_mode);

        let (queue, _receiver) = HotPathQueue::new(4, true, backpressure);
        assert!(matches!(
            queue.enqueue("direct-entry", HotPathTaskKind::DirectEntry),
            HotPathEnqueue::Queued
        ));
        assert!(matches!(
            queue.enqueue("source-exit", HotPathTaskKind::SourceExit),
            HotPathEnqueue::Queued
        ));
    }
}
