use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
pub struct Stats {
    pub connects_attempted: AtomicU64,
    pub connects_succeeded: AtomicU64,
    pub connects_failed: AtomicU64,
    pub publishes_queued: AtomicU64,
    pub publishes_queue_failed: AtomicU64,
}

impl Stats {
    pub fn print(&self) {
        let get = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
        println!(
            "Summary: {} connect(s) attempted, {} succeeded, {} failed\n{} publish(es) queued, {} queue failed",
            get(&self.connects_attempted),
            get(&self.connects_succeeded),
            get(&self.connects_failed),
            get(&self.publishes_queued),
            get(&self.publishes_queue_failed),
        );
    }
}
