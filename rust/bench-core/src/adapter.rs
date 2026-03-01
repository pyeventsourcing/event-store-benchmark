use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionParams {
    pub uri: String,
    #[serde(default)]
    pub options: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventData {
    pub stream: String,
    pub event_type: String,
    pub payload: Vec<u8>,
    #[serde(default)]
    pub tags: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadRequest {
    pub stream: String,
    #[serde(default)]
    pub from_offset: Option<u64>,
    #[serde(default)]
    pub limit: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadEvent {
    pub offset: u64,
    pub event_type: String,
    pub payload: Vec<u8>,
    pub timestamp_ms: u64,
}

/// Lightweight adapter - just wraps a client connection
/// Multiple instances can be created to connect to the same server/container
#[async_trait]
pub trait EventStoreAdapter: Send + Sync {
    async fn append(&self, evt: EventData) -> anyhow::Result<()>;

    async fn batch_append(&self, events: Vec<EventData>) -> anyhow::Result<()> {
        for e in events {
            self.append(e).await?;
        }
        Ok(())
    }

    async fn read(&self, req: ReadRequest) -> anyhow::Result<Vec<ReadEvent>>;

    async fn ping(&self) -> anyhow::Result<Duration>;
}

#[async_trait]
pub trait StoreManager: Send + Sync {
    /// Start the container and return success status
    async fn start(&mut self) -> anyhow::Result<()>;

    /// Stop and cleanup the container
    async fn stop(&mut self) -> anyhow::Result<()>;

    /// Get the container ID for stats collection (if applicable)
    fn container_id(&self) -> Option<String>;

    /// Store name (adapter name)
    fn name(&self) -> &'static str;

    /// Create a new adapter instance (client)
    fn create_adapter(&self) -> anyhow::Result<Arc<dyn EventStoreAdapter>>;
}

/// Creates store manager instances
pub trait StoreManagerFactory: Send + Sync {
    fn name(&self) -> &'static str;

    /// Create a store manager instance with given (internal) connection params or defaults
    fn create_store_manager(&self, uri: Option<String>, options: HashMap<String, String>) -> anyhow::Result<Box<dyn StoreManager>>;
}
