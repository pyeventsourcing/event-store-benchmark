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
    pub payload: Arc<[u8]>,
    pub event_type: Arc<str>,
    #[serde(default)]
    pub tags: Arc<[Arc<str>]>,
}

/// Represents a query item for filtering events
#[derive(Debug, Clone, Default)]
pub struct EsbQueryItem {
    /// Event types to match
    pub types: Vec<String>,
    /// Tags that must all be present in the event
    pub tags: Vec<String>,
}

impl EsbQueryItem {
    /// Creates a new query item
    pub fn new() -> Self {
        Self {
            types: vec![],
            tags: vec![],
        }
    }

    /// Sets the types for this query item
    pub fn types<I, S>(mut self, types: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.types = types.into_iter().map(|s| s.into()).collect();
        self
    }

    /// Sets the tags for this query item
    pub fn tags<I, S>(mut self, tags: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.tags = tags.into_iter().map(|s| s.into()).collect();
        self
    }
}

/// A query composed of multiple query items
#[derive(Debug, Clone, Default)]
pub struct EsbQuery {
    /// List of query items, where events matching any item are included in results
    pub items: Vec<EsbQueryItem>,
}

impl EsbQuery {
    /// Creates a new empty query
    pub fn new() -> Self {
        Self { items: Vec::new() }
    }

    /// Creates a query with the specified items
    pub fn with_items<I>(items: I) -> Self
    where
        I: IntoIterator<Item = EsbQueryItem>,
    {
        Self {
            items: items.into_iter().collect(),
        }
    }

    /// Adds a query item to this query
    pub fn item(mut self, item: EsbQueryItem) -> Self {
        self.items.push(item);
        self
    }

    /// Adds multiple query items to this query
    pub fn items<I>(mut self, items: I) -> Self
    where
        I: IntoIterator<Item = EsbQueryItem>,
    {
        self.items.extend(items);
        self
    }
}

/// Conditions that must be satisfied for an append operation to succeed
#[derive(Debug, Clone, Default)]
pub struct EsbAppendCondition {
    /// Query that, if matching any events, will cause the append to fail
    pub fail_if_events_match: EsbQuery,
    /// Position after which to append; if None, append at the end
    pub after: Option<u64>,
}

impl EsbAppendCondition {
    /// Creates a new empty append condition
    pub fn new(fail_if_events_match: EsbQuery) -> Self {
        Self {
            fail_if_events_match,
            after: None,
        }
    }

    pub fn after(mut self, after: Option<u64>) -> Self {
        self.after = after;
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadRequest {
    pub tag: String,
    pub event_type: Option<String>,
    #[serde(default)]
    pub from_offset: Option<u64>,
    #[serde(default)]
    pub limit: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadEvent {
    pub offset: u64,
    pub event_type: Arc<str>,
    pub payload: Arc<[u8]>,
    pub timestamp_ms: u64,
}

/// Lightweight adapter - just wraps a client connection
/// Multiple instances can be created to connect to the same server/container
#[async_trait]
pub trait EventStoreAdapter: Send + Sync {
    fn as_any(&self) -> &dyn std::any::Any;
    async fn append_dcb(&self, events: &[EventData], condition: Option<EsbAppendCondition>) -> anyhow::Result<Option<u64>>;
    async fn append_to_stream(&self, events: &[EventData], stream_position: Option<usize>, global_position: Option<u64>) -> anyhow::Result<Option<u64>>;
    async fn read_stream(&self, req: ReadRequest) -> anyhow::Result<Vec<ReadEvent>>;
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
    async fn create_adapter(&mut self) -> anyhow::Result<Arc<dyn EventStoreAdapter>>;

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

            #[cfg(unix)]
            {
                // Added this to hopefully make FactDB work on GitHub Actions.
                use std::os::unix::fs::PermissionsExt;
                let mut perms = std::fs::metadata(&path)?.permissions();
                perms.set_mode(0o777);
                std::fs::set_permissions(&path, perms)?;
            }

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
