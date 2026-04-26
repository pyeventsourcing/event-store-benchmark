use anyhow::Result;
use async_trait::async_trait;
use bench_core::adapter::{
    EventData, EventStoreAdapter, ReadEvent, ReadRequest, StoreDataDir, StoreManager, StoreManagerFactory,
};
use bench_core::wait_for_ready;
use bench_testcontainers::umadb::{UmaDb, UMADB_PORT};
use futures::StreamExt;
use std::sync::Arc;
use testcontainers::ImageExt;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, ContainerRequest};
use tokio::time::Duration;
use umadb_client::UmaDbClient;
use umadb_dcb::{DcbAppendCondition, DcbEvent, DcbEventStoreAsync, DcbQuery, DcbQueryItem};

// Store manager - handles lifecycle and adapter creation
pub struct UmaDbStoreManager {
    uri: String,
    container: Option<ContainerAsync<UmaDb>>,
    use_docker: bool,
    data_dir: StoreDataDir,
    memory_limit_mb: Option<u64>,
    docker_platform: Option<String>,
}

impl UmaDbStoreManager {
    pub fn new(data_dir: Option<String>, use_docker: bool) -> Self {
        Self {
            uri: Self::format_uri(UMADB_PORT.as_u16()),
            container: None,
            use_docker,
            data_dir: StoreDataDir::new(data_dir, "umadb"),
            memory_limit_mb: None,
            docker_platform: None,
        }
    }

    fn format_uri(host_port: u16) -> String {
        format!("http://127.0.0.1:{}", host_port)
    }
}

#[async_trait]
impl StoreManager for UmaDbStoreManager {
    fn use_docker(&self) -> bool { self.use_docker }

    async fn start(&mut self) -> Result<()> {
        let mount_path = self.data_dir.setup()?;
        let mut image: ContainerRequest<_> = UmaDb::new(mount_path).into();

        if let Some(ref platform) = self.docker_platform {
            image = image.with_platform(platform);
        }

        if let Some(limit_mb) = self.memory_limit_mb {
            let bytes = limit_mb * 1024 * 1024;
            image = image.with_host_config_modifier(move |host_config| {
                host_config.memory = Some(bytes as i64);
            });
        }

        let container = image.start().await?;

        let host_port = container.get_host_port_ipv4(UMADB_PORT).await?;
        self.uri = Self::format_uri(host_port);
        self.container = Some(container);

        // Wait for container to be ready
        wait_for_ready("UmaDb", || async {
            let client = UmaDbClient::new(self.uri.clone()).connect_async().await?;
            client.head().await?;
            Ok(())
        }, Duration::from_secs(60)).await?;

        Ok(())
    }

    async fn pull(&mut self) -> Result<()> {
        let mut image: ContainerRequest<_> = UmaDb::new(None).into();
        if image.descriptor() != "umadb:local" {
            if let Some(ref platform) = self.docker_platform {
                image = image.with_platform(platform);
            }
            let _ = image.pull_image().await?;
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

    fn set_memory_limit(&mut self, limit_mb: Option<u64>) {
        self.memory_limit_mb = limit_mb;
    }

    fn set_docker_platform(&mut self, platform: Option<String>) {
        self.docker_platform = platform;
    }

    fn name(&self) -> &'static str {
        "umadb"
    }

    async fn create_adapter(&self) -> Result<Arc<dyn EventStoreAdapter>> {
        Ok(Arc::new(UmaDbAdapter::new(self.uri.clone()).await?))
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
            Ok("No logs".to_string())
        }
    }
}

// Lightweight adapter - just wraps a client
pub struct UmaDbAdapter {
    client: umadb_client::AsyncUmaDbClient,
}

impl UmaDbAdapter {
    pub async fn new(uri: String) -> Result<Self> {
        let client = umadb_client::UmaDbClient::new(uri)
            .connect_async()
            .await
            .map_err(|e| anyhow::anyhow!(e))?;
        Ok(Self { client })
    }
}

#[async_trait]
impl EventStoreAdapter for UmaDbAdapter {
    fn as_any(&self) -> &dyn std::any::Any { self }
    async fn append_to_stream(&self, events: &[EventData], _stream_position: Option<usize>, global_position: Option<u64>) -> anyhow::Result<Option<u64>> {
        let dcb_events: Vec<DcbEvent> = events.iter().map(|evt| DcbEvent {
            event_type: evt.event_type.to_string(),
            tags: evt.tags.iter().map(|t| t.to_string()).collect(),
            data: evt.payload.to_vec(),
            uuid: None,
        }).collect();
        let append_condition: Option<DcbAppendCondition> = if global_position.is_some() {
            // One query item with one tag, for each unique tag mentioned in all events.
            Some(DcbAppendCondition {
                fail_if_events_match: DcbQuery::new().item(
                    dcb_events.iter()
                        .flat_map(|evt| &evt.tags)
                        .collect::<std::collections::HashSet<_>>()
                        .into_iter()
                        .fold(DcbQueryItem::new(), |item, tag| item.tags(vec![tag.to_string()]))
                ),
                after: global_position,
            })
        } else {
            None
        };
        let pos: u64 = self.client.append(dcb_events, append_condition, None).await?;
        Ok(Some(pos))
    }

    async fn read_stream(&self, req: ReadRequest) -> Result<Vec<ReadEvent>> {
        let query = DcbQuery {
            items: vec![DcbQueryItem {
                types: vec![],
                tags: vec![req.stream],
            }],
        };
        let mut rr = self.client
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
                                event_type: Arc::from(se.event.event_type.as_str()),
                                payload: Arc::from(se.event.data.as_slice()),
                                timestamp_ms: 0,
                            });
                        }
                    } else {
                        out.push(ReadEvent {
                            offset: se.position,
                            event_type: Arc::from(se.event.event_type.as_str()),
                            payload: Arc::from(se.event.data.as_slice()),
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

    fn create_store_manager(&self, data_dir: Option<String>, use_docker: bool) -> Result<Box<dyn StoreManager>> {
        Ok(Box::new(UmaDbStoreManager::new(data_dir, use_docker)))
    }
}
