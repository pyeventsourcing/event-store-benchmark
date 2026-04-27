use anyhow::Result;
use async_trait::async_trait;
use bench_core::adapter::{EsbAppendCondition, EventData, EventStoreAdapter, ReadEvent, ReadRequest, StoreDataDir, StoreManager, StoreManagerFactory};
use bench_core::wait_for_ready;
use bench_testcontainers::kurrentdb::{KurrentDb, KURRENTDB_PORT};
use kurrentdb::{AppendToStreamOptions, KurrentDbClient, ReadStreamOptions, StreamPosition, StreamState};
use std::sync::Arc;
use testcontainers::ImageExt;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, ContainerRequest};
use tokio::time::Duration;
use uuid::Uuid;

// Store manager - handles lifecycle and adapter creation
pub struct KurrentDbStoreManager {
    uri: String,
    container: Option<ContainerAsync<KurrentDb>>,
    use_docker: bool,
    data_dir: StoreDataDir,
    memory_limit_mb: Option<u64>,
    docker_platform: Option<String>,
}

impl KurrentDbStoreManager {
    pub fn new(data_dir: Option<String>, use_docker: bool) -> Self {
        Self {
            uri: Self::format_uri(KURRENTDB_PORT.as_u16()),
            container: None,
            use_docker,
            data_dir: StoreDataDir::new(data_dir, "kurrentdb"),
            memory_limit_mb: None,
            docker_platform: None,
        }
    }

    fn format_uri(port: u16) -> String {
        format!("esdb://127.0.0.1:{}?tls=false", port)
    }
}

#[async_trait]
impl StoreManager for KurrentDbStoreManager {
    fn use_docker(&self) -> bool { self.use_docker }

    async fn start(&mut self) -> Result<()> {
        if self.use_docker {
            let mount_path = self.data_dir.setup()?;
            let mut image: ContainerRequest<_> = KurrentDb::new(mount_path).into();

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

            let host_port = container.get_host_port_ipv4(KURRENTDB_PORT).await?;
            self.uri = Self::format_uri(host_port);
            self.container = Some(container);
        }

        // Wait for the container to be ready
        wait_for_ready("KurrentDB", || async {
            let client = KurrentDbClient::new(self.uri.clone())
                .await
                .map_err(|e| anyhow::anyhow!(e))?;
            let event = kurrentdb::EventData::binary("ping", vec![].into()).id(Uuid::new_v4());
            let options = AppendToStreamOptions::default();
            client
                .append_to_stream("_ping", &options, vec![event])
                .await?;
            Ok(())
        }, Duration::from_secs(60)).await?;

        Ok(())
    }

    async fn pull(&mut self) -> Result<()> {
        let mut image: ContainerRequest<_> = KurrentDb::new(None).into();
        if let Some(ref platform) = self.docker_platform {
            image = image.with_platform(platform);
        }
        let _ = image.pull_image().await?;
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

    fn set_memory_limit(&mut self, limit_mb: Option<u64>) {
        self.memory_limit_mb = limit_mb;
    }

    fn set_docker_platform(&mut self, platform: Option<String>) {
        self.docker_platform = platform;
    }

    fn name(&self) -> &'static str {
        "kurrentdb"
    }

    async fn create_adapter(&mut self) -> Result<Arc<dyn EventStoreAdapter>> {
        Ok(Arc::new(KurrentDbAdapter::new(self.uri.clone()).await?))
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

// Lightweight adapter - just wraps a client
pub struct KurrentDbAdapter {
    client: KurrentDbClient,
}

impl KurrentDbAdapter {
    pub async fn new(uri: String) -> Result<Self> {
        let client = KurrentDbClient::new(uri)
            .await
            .map_err(|e| anyhow::anyhow!(e))?;
        Ok(Self { client })
    }
}

#[async_trait]
impl EventStoreAdapter for KurrentDbAdapter {
    fn as_any(&self) -> &dyn std::any::Any { self }

    async fn append_dcb(&self, _events: &[EventData], _condition: Option<EsbAppendCondition>) -> anyhow::Result<Option<u64>> {
        anyhow::bail!("append_dcb not implemented in KurrentDbAdapter")
    }

    async fn append_to_stream(&self, events: &[EventData], stream_position: Option<usize>, _global_position: Option<u64>) -> anyhow::Result<Option<u64>> {
        if events.is_empty() {
            return Ok(None);
        }
        let stream_name = events[0].tags[0].to_string();
        let k_events: Vec<kurrentdb::EventData> = events
            .iter()
            .map(|evt| {
                kurrentdb::EventData::binary(evt.event_type.to_string(), evt.payload.to_vec().into()).id(Uuid::new_v4())
            })
            .collect();
        let options = if let Some(stream_position) = stream_position {
            if stream_position == 0 {
                AppendToStreamOptions::default().stream_state(StreamState::NoStream)
            } else {
                AppendToStreamOptions::default().stream_state(StreamState::StreamRevision(stream_position as u64 - 1))
            }
        } else {
            AppendToStreamOptions::default()
        };
        let write_result = self.client
            .append_to_stream(stream_name, &options, k_events)
            .await?;
        Ok(Some(write_result.position.commit))
    }

    async fn read_stream(&self, req: ReadRequest) -> Result<Vec<ReadEvent>> {
        let count = req.limit.unwrap_or(4096) as usize;
        let options = ReadStreamOptions::default()
            .position(match req.from_offset {
                Some(off) => StreamPosition::Position(off),
                None => StreamPosition::Start,
            })
            .max_count(count);
        let mut stream = self.client.read_stream(req.stream, &options).await?;
        let mut out = Vec::new();
        while let Some(event) = stream.next().await? {
            let recorded = event.get_original_event();
            let mut met_limit = false;
            if let Some(lim) = req.limit {
                if (out.len() as u64) < lim {
                    out.push(ReadEvent {
                        offset: recorded.revision,
                        event_type: Arc::from(recorded.event_type.as_str()),
                        payload: Arc::from(recorded.data.as_ref()),
                        timestamp_ms: recorded.created.timestamp_millis() as u64,
                    });
                } else {
                    met_limit = true;
                }
            } else {
                out.push(ReadEvent {
                    offset: recorded.revision,
                    event_type: Arc::from(recorded.event_type.as_str()),
                    payload: Arc::from(recorded.data.as_ref()),
                    timestamp_ms: recorded.created.timestamp_millis() as u64,
                });
            }
            if met_limit {
                // Keep draining the stream to avoid RST_STREAM
                continue;
            }
        }
        Ok(out)
    }
}

    // async fn ping(&self) -> Result<Duration> {
    //     let t0 = std::time::Instant::now();
    //     // Perform an append operation to verify the node is leader and accepting writes
    //     let event = kurrentdb::EventData::binary("ping", vec![].into()).id(Uuid::new_v4());
    //     let options = AppendToStreamOptions::default();
    //     self.client
    //         .append_to_stream("_ping", &options, vec![event])
    //         .await?;
    //     Ok(t0.elapsed())
    // }

pub struct KurrentDbFactory;

impl StoreManagerFactory for KurrentDbFactory {
    fn name(&self) -> &'static str {
        "kurrentdb"
    }

    fn create_store_manager(&self, data_dir: Option<String>, use_docker: bool) -> Result<Box<dyn StoreManager>> {
        Ok(Box::new(KurrentDbStoreManager::new(data_dir, use_docker)))
    }
}
