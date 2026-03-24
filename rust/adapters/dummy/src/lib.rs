use std::hint::spin_loop;
use anyhow::Result;
use async_trait::async_trait;
use bench_core::adapter::{
    EventData, EventStoreAdapter, ReadEvent, ReadRequest, StoreManager, StoreManagerFactory,
};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use lazy_static::lazy_static;
use rayon::ThreadPoolBuilder;

lazy_static! {
    static ref DELAY_POOL: rayon::ThreadPool = ThreadPoolBuilder::new()
        .num_threads(128)
        .thread_name(|i| format!("dummy-delay-{}", i))
        .build()
        .expect("Failed to create delay thread pool");
}

pub struct DummyStoreManager {}

impl DummyStoreManager {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl StoreManager for DummyStoreManager {
    async fn start(&mut self) -> Result<()> {
        Ok(())
    }
    async fn stop(&mut self) -> Result<()> {
        Ok(())
    }
    fn container_id(&self) -> Option<String> {
        None
    }
    fn name(&self) -> &'static str {
        "dummy"
    }
    fn create_adapter(&self) -> Result<Arc<dyn EventStoreAdapter>> {
        Ok(Arc::new(DummyAdapter))
    }
}

pub struct DummyAdapter;

#[async_trait]
impl EventStoreAdapter for DummyAdapter {
    async fn append(&self, _evt: EventData) -> Result<()> {
        precise_delay(Duration::from_micros(5000)).await;
        Ok(())
    }
    async fn read(&self, _req: ReadRequest) -> Result<Vec<ReadEvent>> {
        precise_delay(Duration::from_micros(5000)).await;
        Ok(vec![])
    }
}

pub struct DummyFactory;

impl StoreManagerFactory for DummyFactory {
    fn name(&self) -> &'static str {
        "dummy"
    }
    fn create_store_manager(
        &self,
        _data_dir: Option<String>,
    ) -> Result<Box<dyn StoreManager>> {
        Ok(Box::new(DummyStoreManager::new()))
    }
}

pub async fn precise_delay(delay: Duration) {
    // Execute the blocking delay on our dedicated thread pool
    let (tx, rx) = tokio::sync::oneshot::channel();

    DELAY_POOL.spawn(move || {
        let start = Instant::now();
        let target = start + delay;

        let spin_threshold = Duration::from_micros(2000);

        if delay > spin_threshold {
            thread::sleep(delay - spin_threshold);
        }

        while Instant::now() < target {
            spin_loop();
        }

        let _ = tx.send(());
    });

    let _ = rx.await;
}