use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionParams {
    pub uri: String,
    #[serde(default)]
    pub options: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventData {
    pub payload: Vec<u8>,
    pub event_type: String,
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
    fn as_any(&self) -> &dyn std::any::Any;
    async fn append(&self, events: Vec<EventData>) -> anyhow::Result<()>;
    async fn read(&self, req: ReadRequest) -> anyhow::Result<Vec<ReadEvent>>;
}

#[async_trait]
pub trait StoreManager: Send + Sync {
    /// Use docker service (need to start and stop)
    fn use_docker(&self) -> bool;

    /// Start the container and return success status
    async fn start(&mut self) -> anyhow::Result<()>;

    /// Pull the container image (if applicable)
    async fn pull(&mut self) -> anyhow::Result<()>;

    /// Stop and cleanup the container
    async fn stop(&mut self) -> anyhow::Result<()>;

    /// Get the container ID for stats collection (if applicable)
    fn container_id(&self) -> Option<String>;

    /// Set memory limit for the container in MB
    fn set_memory_limit(&mut self, limit_mb: Option<u64>);

    /// Set Docker platform for the container (e.g., "linux/amd64")
    fn set_docker_platform(&mut self, platform: Option<String>);


    /// Store name (adapter name)
    fn name(&self) -> &'static str;

    /// Create a new adapter instance (client)
    async fn create_adapter(&self) -> anyhow::Result<Arc<dyn EventStoreAdapter>>;

    /// Get logs from the container
    async fn logs(&self) -> anyhow::Result<String>;
}

/// Helper for managing store data directories
pub struct StoreDataDir {
    base_dir: Option<String>,
    store_name: String,
    active_path: Option<std::path::PathBuf>,
}

impl StoreDataDir {
    pub fn new(base_dir: Option<String>, store_name: &str) -> Self {
        Self {
            base_dir,
            store_name: store_name.to_string(),
            active_path: None,
        }
    }

    pub fn setup(&mut self) -> anyhow::Result<Option<String>> {
        if let Some(ref base) = self.base_dir {
            let path = std::path::PathBuf::from(base).join(&self.store_name);
            if path.exists() {
                anyhow::bail!("Data directory already exists: {}", path.display());
            }
            std::fs::create_dir_all(&path)?;
            let path_str = path.to_string_lossy().to_string();
            self.active_path = Some(path);
            Ok(Some(path_str))
        } else {
            Ok(None)
        }
    }

    pub fn cleanup(&mut self) -> anyhow::Result<()> {
        if let Some(path) = self.active_path.take() {
            if path.exists() {
                std::fs::remove_dir_all(&path)?;
            }
        }
        Ok(())
    }
}

impl Drop for StoreDataDir {
    fn drop(&mut self) {
        let _ = self.cleanup();
    }
}

/// Creates store manager instances
pub trait StoreManagerFactory: Send + Sync {
    fn name(&self) -> &'static str;

    /// Create a store manager instance with given (internal) connection params or defaults
    fn create_store_manager(&self, data_dir: Option<String>, use_docker: bool) -> anyhow::Result<Box<dyn StoreManager>>;
}
