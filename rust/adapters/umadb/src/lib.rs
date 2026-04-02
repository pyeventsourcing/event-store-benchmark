use anyhow::Result;
use async_trait::async_trait;
use bench_core::adapter::{
    EventData, EventStoreAdapter, ReadEvent, ReadRequest, StoreDataDir, StoreManager, StoreManagerFactory,
};
use bench_core::wait_for_ready;
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
    data_dir: StoreDataDir,
}

impl UmaDbStoreManager {
    pub fn new(data_dir: Option<String>) -> Self {
        Self {
            uri: None,
            container: None,
            client: None,
            local: false,
            data_dir: StoreDataDir::new(data_dir, "umadb"),
        }
    }
}

#[async_trait]
impl StoreManager for UmaDbStoreManager {
    async fn start(&mut self) -> Result<()> {
        if !self.local {
            let mount_path = self.data_dir.setup()?;
            let container = UmaDb::new(mount_path).start().await?;
            let host_port = container.get_host_port_ipv4(UMADB_PORT).await?;
            self.uri = Some(format!("http://localhost:{}", host_port));
            self.container = Some(container);
        } else {
            self.uri = Some(format!("http://localhost:{}", UMADB_PORT));
        }

        // Wait for container to be ready and create shared client
        let uri = self.uri.clone().unwrap();
        self.client = Some(Arc::new(wait_for_ready("UmaDB", || async {
            let client = UmaDBClient::new(uri.clone()).connect_async().await?;
            client.head().await?;
            Ok(client)
        }, Duration::from_secs(60)).await?));

        Ok(())
    }

    async fn pull(&mut self) -> Result<()> {
        if !self.local {
            let _ = UmaDb::new(None).pull_image().await?;
        }
        Ok(())
    }

    async fn stop(&mut self) -> Result<()> {
        println!("Stopping container");
        if let Some(container) = self.container.take() {
            container.stop().await?;
        }
        self.data_dir.cleanup()?;
        println!("Stopped container");
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

    async fn logs(&self) -> Result<String> {
        if let Some(container) = &self.container {
            let stdout = container.stdout_to_vec().await?;
            let stderr = container.stderr_to_vec().await?;
            let mut logs = String::from_utf8_lossy(&stdout).to_string();
            if !stderr.is_empty() {
                logs.push_str("\n--- STDERR ---\n");
                logs.push_str(&String::from_utf8_lossy(&stderr));
            }
            Ok(logs)
        } else {
            Ok(String::new())
        }
    }
}

// Lightweight adapter - just wraps a shared client
pub struct UmaDbAdapter {
    client: Arc<umadb_client::AsyncUmaDBClient>,
}

#[async_trait]
impl EventStoreAdapter for UmaDbAdapter {
    async fn append(&self, events: Vec<EventData>) -> Result<()> {
        let dcb_events: Vec<DCBEvent> = events.into_iter().map(|evt| DCBEvent {
            event_type: evt.event_type,
            tags: evt.tags,
            data: evt.payload,
            uuid: None,
        }).collect();
        let _pos: u64 = self.client.append(dcb_events, None, None).await?;
        Ok(())
    }

    async fn read(&self, req: ReadRequest) -> Result<Vec<ReadEvent>> {
        let query = DCBQuery {
            items: vec![DCBQueryItem {
                types: vec![],
                tags: vec![req.stream],
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
        while let Some(item) = rr.next().await {
            match item {
                Ok(se) => {
                    if let Some(lim) = req.limit {
                        if (out.len() as u64) < lim {
                            out.push(ReadEvent {
                                offset: se.position,
                                event_type: se.event.event_type.clone(),
                                payload: se.event.data.clone(),
                                timestamp_ms: 0,
                            });
                        }
                    } else {
                        out.push(ReadEvent {
                            offset: se.position,
                            event_type: se.event.event_type.clone(),
                            payload: se.event.data.clone(),
                            timestamp_ms: 0,
                        });
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
