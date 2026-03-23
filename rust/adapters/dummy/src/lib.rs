use std::hint::spin_loop;
use anyhow::Result;
use async_trait::async_trait;
use bench_core::adapter::{
    EventData, EventStoreAdapter, ReadEvent, ReadRequest, StoreManager, StoreManagerFactory,
};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

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
        precise_delay(Duration::from_micros(200));
        Ok(())
    }
    async fn read(&self, _req: ReadRequest) -> Result<Vec<ReadEvent>> {
        precise_delay(Duration::from_millis(200));
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
    ) -> Result<Box<dyn StoreManager>> {
        Ok(Box::new(DummyStoreManager::new()))
    }
}

pub fn precise_delay(delay: Duration) {
    let start = Instant::now();
    let target = start + delay;

    let spin_threshold = Duration::from_millis(10);

    if delay > spin_threshold {
        thread::sleep(delay - spin_threshold);
    }

    while Instant::now() < target {
        spin_loop();
    }
}