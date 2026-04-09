use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crossbeam_queue::ArrayQueue;
use tokio::sync::Notify;
use tracing::warn;

pub struct RingBuffer<T> {
    queue: Arc<ArrayQueue<T>>,
    notify: Arc<Notify>,
    dropped_items: Arc<AtomicU64>,
}

impl<T> Clone for RingBuffer<T> {
    fn clone(&self) -> Self {
        Self {
            queue: self.queue.clone(),
            notify: self.notify.clone(),
            dropped_items: self.dropped_items.clone(),
        }
    }
}

impl<T> RingBuffer<T> {
    pub fn new(capacity: usize) -> Self {
        Self {
            queue: Arc::new(ArrayQueue::new(capacity.max(1))),
            notify: Arc::new(Notify::new()),
            dropped_items: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn push(&self, mut item: T) {
        loop {
            match self.queue.push(item) {
                Ok(()) => {
                    self.notify.notify_one();
                    return;
                }
                Err(returned) => {
                    item = returned;
                    let dropped_total = self.dropped_items.fetch_add(1, Ordering::Relaxed) + 1;
                    if dropped_total == 1
                        || dropped_total.is_power_of_two()
                        || dropped_total.is_multiple_of(1024)
                    {
                        warn!(
                            dropped_total,
                            capacity = self.queue.capacity(),
                            "ring buffer full; dropping oldest item"
                        );
                    }
                    let _ = self.queue.pop();
                }
            }
        }
    }

    pub fn try_pop(&self) -> Option<T> {
        self.queue.pop()
    }

    pub async fn recv(&self) -> T {
        loop {
            if let Some(item) = self.try_pop() {
                return item;
            }
            self.notify.notified().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn drops_oldest_item_when_buffer_is_full() {
        let ring = RingBuffer::new(2);

        ring.push(1);
        ring.push(2);
        ring.push(3);

        assert_eq!(ring.try_pop(), Some(2));
        assert_eq!(ring.try_pop(), Some(3));
        assert_eq!(ring.try_pop(), None);
    }
}
