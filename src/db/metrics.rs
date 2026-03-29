use std::sync::{Arc, Mutex};

#[derive(Default)]
pub struct Metrics {
    pub set_count: u64,
    pub get_count: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub compactions: u64,
}

pub type SharedMetrics = Arc<Mutex<Metrics>>;

impl Metrics {
    pub fn new() -> SharedMetrics {
        Arc::new(Mutex::new(Self::default()))
    }
}