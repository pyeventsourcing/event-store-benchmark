use anyhow::Result;
use async_trait::async_trait;
use bench_core::adapter::{
    EventData, EventStoreAdapter, ReadEvent, ReadRequest, StoreManager, StoreManagerFactory,
};
use bench_testcontainers::kurrentdb::{KurrentDb, KURRENTDB_PORT};
use kurrentdb::{AppendToStreamOptions, Client, ClientSettings, ReadStreamOptions, StreamPosition};
use std::sync::Arc;
use testcontainers::runners::AsyncRunner;
use testcontainers::ContainerAsync;
use tokio::time::Duration;
use uuid::Uuid;

// Store manager - handles lifecycle and adapter creation
pub struct KurrentDbStoreManager {
    uri: Option<String>,
    container: Option<ContainerAsync<KurrentDb>>,
    data_dir: Option<String>,
}

impl KurrentDbStoreManager {
    pub fn new(data_dir: Option<String>) -> Self {
        Self {
            uri: None,
            container: None,
            data_dir,
        }
    }
}

#[async_trait]
impl StoreManager for KurrentDbStoreManager {
    async fn start(&mut self) -> Result<()> {
        let container = KurrentDb::new(self.data_dir.clone()).start().await?;
        let host_port = container.get_host_port_ipv4(KURRENTDB_PORT).await?;
        self.uri = Some(format!("esdb://localhost:{}?tls=false", host_port));
        self.container = Some(container);

        // Wait for the container to be ready
        for _ in 0..60 {
            if let Ok(settings) = self.uri.clone().unwrap().parse::<ClientSettings>() {
                if let Ok(client) = Client::new(settings) {
                    let event =
                        kurrentdb::EventData::binary("ping", vec![].into()).id(Uuid::new_v4());
                    let options = AppendToStreamOptions::default();
                    if client
                        .append_to_stream("_ping", &options, event)
                        .await
                        .is_ok()
                    {
                        return Ok(());
                    }
                }
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        anyhow::bail!("KurrentDB container did not become ready within 60s")
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
        "kurrentdb"
    }

    fn create_adapter(&self) -> Result<Arc<dyn EventStoreAdapter>> {
        Ok(Arc::new(KurrentDbAdapter::new(&self.uri.clone().unwrap())?))
    }
}

// Lightweight adapter - just wraps a client
pub struct KurrentDbAdapter {
    client: Client,
}

impl KurrentDbAdapter {
    pub fn new(uri: &str) -> Result<Self> {
        let settings: ClientSettings = uri.parse()?;
        let client = Client::new(settings).map_err(|e| anyhow::anyhow!("{}", e))?;
        Ok(Self { client })
    }
}

#[async_trait]
impl EventStoreAdapter for KurrentDbAdapter {
    async fn append(&self, evt: EventData) -> Result<()> {
        let event =
            kurrentdb::EventData::binary(evt.event_type, evt.payload.into()).id(Uuid::new_v4());
        let options = AppendToStreamOptions::default();
        self.client
            .append_to_stream(evt.stream, &options, event)
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
            out.push(ReadEvent {
                offset: recorded.revision,
                event_type: recorded.event_type.clone(),
                payload: recorded.data.to_vec(),
                timestamp_ms: recorded.created.timestamp_millis() as u64,
            });
            if let Some(lim) = req.limit {
                if out.len() as u64 >= lim {
                    break;
                }
            }
        }
        Ok(out)
    }

    // async fn ping(&self) -> Result<Duration> {
    //     let t0 = std::time::Instant::now();
    //     // Perform an append operation to verify the node is leader and accepting writes
    //     let event = kurrentdb::EventData::binary("ping", vec![].into()).id(Uuid::new_v4());
    //     let options = AppendToStreamOptions::default();
    //     self.client
    //         .append_to_stream("_ping", &options, event)
    //         .await?;
    //     Ok(t0.elapsed())
    // }
}

pub struct KurrentDbFactory;

impl StoreManagerFactory for KurrentDbFactory {
    fn name(&self) -> &'static str {
        "kurrentdb"
    }

    fn create_store_manager(&self, data_dir: Option<String>) -> Result<Box<dyn StoreManager>> {
        Ok(Box::new(KurrentDbStoreManager::new(data_dir)))
    }
}
