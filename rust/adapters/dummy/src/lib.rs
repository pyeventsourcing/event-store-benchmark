use anyhow::Result;
use async_trait::async_trait;
use bench_core::adapter::{
    EventData, EventStoreAdapter, ReadEvent, ReadRequest, StoreManager, StoreManagerFactory,
};
use std::sync::Arc;
use std::time::Duration;

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
        tokio::time::sleep(Duration::from_millis(5)).await;
        Ok(())
    }
    async fn read(&self, _req: ReadRequest) -> Result<Vec<ReadEvent>> {
        tokio::time::sleep(Duration::from_millis(5)).await;
        Ok(vec![])
    }
    async fn ping(&self) -> Result<Duration> {
        Ok(Duration::from_millis(1))
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
