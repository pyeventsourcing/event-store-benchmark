use anyhow::Result;
use async_trait::async_trait;
use bench_core::adapter::{
    EventData, EventStoreAdapter, ReadEvent, ReadRequest, StoreManager, StoreManagerFactory,
};
use bench_testcontainers::umadb::{UmaDb, UMADB_PORT};
use futures::StreamExt;
use std::sync::Arc;
use testcontainers::runners::AsyncRunner;
use testcontainers::ContainerAsync;
use tokio::time::Duration;
use umadb_client::UmaDBClient;
use umadb_dcb::{DCBEvent, DCBEventStoreAsync, DCBQuery, DCBQueryItem};

// Store manager - handles lifecycle and adapter creation
pub struct UmaDbStoreManager {
    uri: Option<String>,
    container: Option<ContainerAsync<UmaDb>>,
    client: Option<Arc<umadb_client::AsyncUmaDBClient>>,
    local: bool,
    data_dir: Option<String>,
}

impl UmaDbStoreManager {
    pub fn new(data_dir: Option<String>) -> Self {
        Self {
            uri: None,
            container: None,
            client: None,
            local: false,
            data_dir,
        }
    }
}

#[async_trait]
impl StoreManager for UmaDbStoreManager {
    async fn start(&mut self) -> Result<()> {
        if !self.local {
            let container = UmaDb::new(self.data_dir.clone()).start().await?;
            let host_port = container.get_host_port_ipv4(UMADB_PORT).await?;
            self.uri = Some(format!("http://localhost:{}", host_port));
            self.container = Some(container);
        } else {
            self.uri = Some(format!("http://localhost:{}", UMADB_PORT));
        }

        // Wait for container to be ready and create shared client
        for _ in 0..60 {
            if let Ok(client) = UmaDBClient::new(self.uri.clone().unwrap()).connect_async().await {
                if client.head().await.is_ok() {
                    self.client = Some(Arc::new(client));
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
        let client = self.client.as_ref()
            .ok_or_else(|| anyhow::anyhow!("UmaDB client not initialized. Did you call start()?"))?
            .clone();
        Ok(Arc::new(UmaDbAdapter { client }))
    }
}

// Lightweight adapter - just wraps a shared client
pub struct UmaDbAdapter {
    client: Arc<umadb_client::AsyncUmaDBClient>,
}

#[async_trait]
impl EventStoreAdapter for UmaDbAdapter {
    async fn append(&self, evt: EventData) -> Result<()> {
        let dcb_evt = DCBEvent {
            event_type: evt.event_type,
            tags: vec![format!("stream:{}", evt.stream)],
            data: evt.payload,
            uuid: None,
        };
        let _pos: u64 = self.client.append(vec![dcb_evt], None, None).await?;
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

    // async fn ping(&self) -> Result<Duration> {
    //     let t0 = std::time::Instant::now();
    //     let _ = self.client.head().await?;
    //     Ok(t0.elapsed())
    // }
}

pub struct UmaDbFactory;

impl StoreManagerFactory for UmaDbFactory {
    fn name(&self) -> &'static str {
        "umadb"
    }

    fn create_store_manager(&self, data_dir: Option<String>) -> Result<Box<dyn StoreManager>> {
        Ok(Box::new(UmaDbStoreManager::new(data_dir)))
    }
}
