use anyhow::Result;
use async_trait::async_trait;
use bench_core::adapter::{
    EventData, EventStoreAdapter, ReadEvent, ReadRequest, StoreDataDir, StoreManager,
    StoreManagerFactory,
};
use bench_core::wait_for_ready;
use bench_testcontainers::fact::{FactDb, FACT_PORT};
use std::sync::Arc;
use testcontainers::runners::AsyncRunner;
use testcontainers::ContainerAsync;
use tokio::time::Duration;

pub mod proto {
    tonic::include_proto!("fact.bench");
}

use proto::fact_bench_client::FactBenchClient;

// --- StoreManager ---

pub struct FactStoreManager {
    uri: String,
    container: Option<ContainerAsync<FactDb>>,
    local: bool,
    data_dir: StoreDataDir,
}

impl FactStoreManager {
    pub fn new(data_dir: Option<String>, local: bool) -> Self {
        Self {
            uri: Self::format_uri(FACT_PORT.as_u16()),
            container: None,
            local,
            data_dir: StoreDataDir::new(data_dir, "fact"),
        }
    }

    fn format_uri(host_port: u16) -> String {
        format!("http://127.0.0.1:{}", host_port)
    }
}

#[async_trait]
impl StoreManager for FactStoreManager {
    fn local(&self) -> bool {
        self.local
    }

    async fn start(&mut self) -> Result<()> {
        let mount_path = self.data_dir.setup()?;
        let container = FactDb::new(mount_path).start().await?;
        let host_port = container.get_host_port_ipv4(FACT_PORT).await?;
        self.uri = Self::format_uri(host_port);
        self.container = Some(container);

        let uri = self.uri.clone();
        wait_for_ready(
            "Fact",
            || {
                let uri = uri.clone();
                async move {
                    let mut client = FactBenchClient::connect(uri).await?;
                    let resp = client
                        .healthz(proto::HealthzRequest {})
                        .await?
                        .into_inner();
                    if resp.status == "ok" {
                        Ok(())
                    } else {
                        anyhow::bail!("not ready (status: {})", resp.status)
                    }
                }
            },
            Duration::from_secs(60),
        )
        .await?;

        Ok(())
    }

    async fn pull(&mut self) -> Result<()> {
        let _ = FactDb::new(None).pull_image().await?;
        Ok(())
    }

    async fn stop(&mut self) -> Result<()> {
        if let Some(container) = self.container.take() {
            container.stop().await?;
        }
        self.data_dir.cleanup()?;
        Ok(())
    }

    fn container_id(&self) -> Option<String> {
        self.container.as_ref().map(|c| c.id().to_string())
    }

    fn name(&self) -> &'static str {
        "fact"
    }

    async fn create_adapter(&self) -> Result<Arc<dyn EventStoreAdapter>> {
        let client = FactBenchClient::connect(self.uri.clone()).await?;
        Ok(Arc::new(FactAdapter { client }))
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

// --- EventStoreAdapter ---

pub struct FactAdapter {
    client: FactBenchClient<tonic::transport::Channel>,
}

#[async_trait]
impl EventStoreAdapter for FactAdapter {
    async fn append(&self, events: Vec<EventData>) -> Result<()> {
        let request = proto::AppendRequest {
            events: events
                .into_iter()
                .map(|evt| proto::EventData {
                    payload: evt.payload,
                    event_type: evt.event_type,
                    tags: evt.tags,
                })
                .collect(),
        };

        let mut client = self.client.clone();
        let resp = client.append(request).await?.into_inner();
        if !resp.ok {
            anyhow::bail!("append returned ok=false");
        }
        Ok(())
    }

    async fn read(&self, req: ReadRequest) -> Result<Vec<ReadEvent>> {
        let request = proto::ReadRequest {
            stream: req.stream,
            from_offset: req.from_offset,
            limit: req.limit,
        };

        let mut client = self.client.clone();
        let resp = client.read(request).await?.into_inner();

        Ok(resp
            .events
            .into_iter()
            .map(|evt| ReadEvent {
                offset: evt.offset,
                event_type: evt.event_type,
                payload: evt.payload,
                timestamp_ms: evt.timestamp_ms,
            })
            .collect())
    }
}

// --- Factory ---

pub struct FactFactory;

impl StoreManagerFactory for FactFactory {
    fn name(&self) -> &'static str {
        "fact"
    }

    fn create_store_manager(
        &self,
        data_dir: Option<String>,
        local: bool,
    ) -> Result<Box<dyn StoreManager>> {
        Ok(Box::new(FactStoreManager::new(data_dir, local)))
    }
}
