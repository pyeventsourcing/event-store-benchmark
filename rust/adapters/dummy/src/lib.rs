use anyhow::Result;
use async_trait::async_trait;
use bench_core::adapter::{
    EventData, EventStoreAdapter, ReadEvent, ReadRequest, StoreManager, StoreManagerFactory,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

pub struct DummyStoreManager {
    uri: String,
    options: HashMap<String, String>,
}

impl DummyStoreManager {
    pub fn new(uri: Option<String>, options: HashMap<String, String>) -> Self {
        Self {
            uri: uri.unwrap_or_else(|| "dummy://".to_string()),
            options,
        }
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
        tokio::time::sleep(Duration::from_micros(10)).await;
        Ok(())
    }
    async fn read(&self, _req: ReadRequest) -> Result<Vec<ReadEvent>> {
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
        uri: Option<String>,
        options: HashMap<String, String>,
    ) -> Result<Box<dyn StoreManager>> {
        Ok(Box::new(DummyStoreManager::new(uri, options)))
    }
}
