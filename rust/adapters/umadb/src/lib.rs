use anyhow::Result;
use async_trait::async_trait;
use bench_core::adapter::{
    EventData, EventStoreAdapter, ReadEvent, ReadRequest, StoreManager, StoreManagerFactory,
};
use bench_testcontainers::umadb::{UmaDb, UMADB_PORT};
use futures::StreamExt;
use std::collections::HashMap;
use std::sync::Arc;
use testcontainers::runners::AsyncRunner;
use testcontainers::ContainerAsync;
use tokio::time::Duration;
use umadb_client::UmaDBClient;
use umadb_dcb::{DCBEvent, DCBEventStoreAsync, DCBQuery, DCBQueryItem};
use uuid::Uuid;

// Store manager - handles lifecycle and adapter creation
pub struct UmaDbStoreManager {
    uri: String,
    options: HashMap<String, String>,
    container: Option<ContainerAsync<UmaDb>>,
}

impl UmaDbStoreManager {
    pub fn new(uri: Option<String>, options: HashMap<String, String>) -> Self {
        Self {
            uri: uri.unwrap_or_else(|| "http://localhost:50051".to_string()),
            options,
            container: None,
        }
    }
}

#[async_trait]
impl StoreManager for UmaDbStoreManager {
    async fn start(&mut self) -> Result<()> {
        let container = UmaDb::default().start().await?;
        let host_port = container.get_host_port_ipv4(UMADB_PORT).await?;
        self.uri = format!("http://localhost:{}", host_port);
        self.container = Some(container);

        // Wait for container to be ready
        for _ in 0..60 {
            if let Ok(client) = UmaDBClient::new(self.uri.clone()).connect_async().await {
                if client.head().await.is_ok() {
                    return Ok(());
                }
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        anyhow::bail!("UmaDB container did not become ready within 60s")
    }

    async fn stop(&mut self) -> Result<()> {
        if let Some(container) = self.container.take() {
            container.stop().await?;
        }
        Ok(())
    }

    fn container_id(&self) -> Option<String> {
        self.container.as_ref().map(|c| c.id().to_string())
    }

    fn name(&self) -> &'static str {
        "umadb"
    }

    fn create_adapter(&self) -> Result<Arc<dyn EventStoreAdapter>> {
        Ok(Arc::new(UmaDbAdapter::new(&self.uri, &self.options)?))
    }
}

// Lightweight adapter - just wraps a client
pub struct UmaDbAdapter {
    client: Arc<umadb_client::AsyncUmaDBClient>,
}

impl UmaDbAdapter {
    pub fn new(uri: &str, options: &HashMap<String, String>) -> Result<Self> {
        let mut builder = UmaDBClient::new(uri.to_string());
        if let Some(v) = options.get("api_key") {
            builder = builder.api_key(v.clone());
        }
        if let Some(v) = options.get("ca_path") {
            builder = builder.ca_path(v.clone());
        }
        if let Some(v) = options.get("batch_size") {
            if let Ok(n) = v.parse::<u32>() {
                builder = builder.batch_size(n);
            }
        }

        // Connect synchronously during construction
        let client = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async { builder.connect_async().await })
        })?;

        Ok(Self {
            client: Arc::new(client),
        })
    }
}

#[async_trait]
impl EventStoreAdapter for UmaDbAdapter {
    async fn append(&self, evt: EventData) -> Result<()> {
        let mut tags = evt.tags.clone();
        tags.push(format!("stream:{}", evt.stream));
        let dcb_evt = DCBEvent {
            event_type: evt.event_type,
            tags,
            data: evt.payload,
            uuid: Some(Uuid::new_v4()),
        };
        let _pos: u64 = self.client.append(vec![dcb_evt], None).await?;
        Ok(())
    }

    async fn read(&self, req: ReadRequest) -> Result<Vec<ReadEvent>> {
        let query = DCBQuery {
            items: vec![DCBQueryItem {
                types: vec![],
                tags: vec![format!("stream:{}", req.stream)],
            }],
        };
        let mut rr = self
            .client
            .read(
                Some(query),
                req.from_offset,
                false,
                req.limit.map(|l| l as u32),
                false,
            )
            .await?;
        let mut out = Vec::new();
        let mut got: u64 = 0;
        while let Some(item) = rr.next().await {
            match item {
                Ok(se) => {
                    out.push(ReadEvent {
                        offset: se.position,
                        event_type: se.event.event_type.clone(),
                        payload: se.event.data.clone(),
                        timestamp_ms: 0,
                    });
                    got += 1;
                    if let Some(lim) = req.limit {
                        if got >= lim {
                            break;
                        }
                    }
                }
                Err(_status) => break,
            }
        }
        Ok(out)
    }

    async fn ping(&self) -> Result<Duration> {
        let t0 = std::time::Instant::now();
        let _ = self.client.head().await?;
        Ok(t0.elapsed())
    }
}

pub struct UmaDbFactory;

impl StoreManagerFactory for UmaDbFactory {
    fn name(&self) -> &'static str {
        "umadb"
    }

    fn create_store_manager(&self, uri: Option<String>, options: HashMap<String, String>) -> Result<Box<dyn StoreManager>> {
        Ok(Box::new(UmaDbStoreManager::new(uri, options)))
    }
}
