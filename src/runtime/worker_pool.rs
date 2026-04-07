use std::sync::Arc;

use futures_util::future::BoxFuture;
use tracing::info;

use crate::runtime::ring_buffer::RingBuffer;

pub fn spawn_worker_pool<T>(
    name: &'static str,
    workers: usize,
    ring: RingBuffer<T>,
    handler: Arc<dyn Fn(T) -> BoxFuture<'static, ()> + Send + Sync>,
) where
    T: Send + 'static,
{
    let workers = workers.max(1);
    info!(pool = name, workers, "starting worker pool");
    for worker_index in 0..workers {
        let ring = ring.clone();
        let handler = handler.clone();
        tokio::spawn(async move {
            loop {
                let item = ring.recv().await;
                (handler)(item).await;
            }
        });
        info!(pool = name, worker_index, "worker spawned");
    }
}
