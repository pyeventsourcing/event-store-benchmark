use anyhow::Result;
use async_trait::async_trait;
use bench_core::adapter::{
    EventData, EventStoreAdapter, ReadEvent, ReadRequest, StoreDataDir, StoreManager, StoreManagerFactory,
};
use bench_core::wait_for_ready;
use bench_testcontainers::kurrentdb::{KurrentDb, KURRENTDB_PORT};
use kurrentdb::{AppendToStreamOptions, KurrentDbClient, ReadStreamOptions, StreamPosition};
use std::sync::Arc;
use testcontainers::runners::AsyncRunner;
use testcontainers::ContainerAsync;
use tokio::time::Duration;
use uuid::Uuid;

// Store manager - handles lifecycle and adapter creation
pub struct KurrentDbStoreManager {
    uri: String,
    container: Option<ContainerAsync<KurrentDb>>,
    client: Option<Arc<KurrentDbClient>>,
    local: bool,
    data_dir: StoreDataDir,
}

impl KurrentDbStoreManager {
    pub fn new(data_dir: Option<String>, local: bool) -> Self {
        Self {
            uri: Self::format_uri(KURRENTDB_PORT.as_u16()),
            container: None,
            client: None,
            local,
            data_dir: StoreDataDir::new(data_dir, "kurrentdb"),
        }
    }

    fn format_uri(port: u16) -> String {
        format!("esdb://127.0.0.1:{}?tls=false", port)
    }
}

#[async_trait]
impl StoreManager for KurrentDbStoreManager {
    fn local(&self) -> bool { self.local }

    async fn start(&mut self) -> Result<()> {
        if !self.local {
            let mount_path = self.data_dir.setup()?;
            let container = KurrentDb::new(mount_path).start().await?;
            let host_port = container.get_host_port_ipv4(KURRENTDB_PORT).await?;
            self.uri = Self::format_uri(host_port);
            self.container = Some(container);
        }

        // Wait for the container to be ready
        self.client = Some(Arc::new(wait_for_ready("KurrentDB", || async {
            let client = KurrentDbClient::new(self.uri.clone())
                .await
                .map_err(|e| anyhow::anyhow!(e))?;
            let event = kurrentdb::EventData::binary("ping", vec![].into()).id(Uuid::new_v4());
            let options = AppendToStreamOptions::default();
            client
                .append_to_stream("_ping", &options, vec![event])
                .await?;
            Ok(client)
        }, Duration::from_secs(60)).await?));

        Ok(())
    }

    async fn pull(&mut self) -> Result<()> {
        let _ = KurrentDb::new(None).pull_image().await?;
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
        "kurrentdb"
    }

    fn create_adapter(&self) -> Result<Arc<dyn EventStoreAdapter>> {
        let client = self.client.as_ref()
            .ok_or_else(|| anyhow::anyhow!("KurrentDB client not initialized. Did you call start()?"))?
            .clone();
        Ok(Arc::new(KurrentDbAdapter { client }))
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
pub struct KurrentDbAdapter {
    client: Arc<KurrentDbClient>,
}

#[async_trait]
impl EventStoreAdapter for KurrentDbAdapter {
    async fn append(&self, events: Vec<EventData>) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }
        let stream_name = events[0].tags[0].clone();
        let k_events: Vec<kurrentdb::EventData> = events
            .into_iter()
            .map(|evt| {
                kurrentdb::EventData::binary(evt.event_type, evt.payload.into()).id(Uuid::new_v4())
            })
            .collect();
        let options = AppendToStreamOptions::default();
        self.client
            .append_to_stream(stream_name, &options, k_events)
            .await?;
        Ok(())
    }

    async fn read(&self, req: ReadRequest) -> Result<Vec<ReadEvent>> {
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
                        event_type: recorded.event_type.clone(),
                        payload: recorded.data.to_vec(),
                        timestamp_ms: recorded.created.timestamp_millis() as u64,
                    });
                } else {
                    met_limit = true;
                }
            } else {
                out.push(ReadEvent {
                    offset: recorded.revision,
                    event_type: recorded.event_type.clone(),
                    payload: recorded.data.to_vec(),
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

    fn create_store_manager(&self, data_dir: Option<String>, local: bool) -> Result<Box<dyn StoreManager>> {
        Ok(Box::new(KurrentDbStoreManager::new(data_dir, local)))
    }
}
