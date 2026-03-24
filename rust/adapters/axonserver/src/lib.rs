use anyhow::Result;
use async_trait::async_trait;
use axonserver_client::proto::dcb::source_events_response;
use axonserver_client::proto::dcb::{Criterion, Event, Tag, TaggedEvent, TagsAndNamesCriterion};
use axonserver_client::AxonServerClient;
use bench_core::adapter::{
    EventData, EventStoreAdapter, ReadEvent, ReadRequest, StoreManager, StoreManagerFactory,
};
use bench_testcontainers::axonserver::{AxonServer, AXONSERVER_GRPC_PORT};
use std::sync::Arc;
use testcontainers::runners::AsyncRunner;
use testcontainers::ContainerAsync;
use tokio::time::Duration;

// Store manager - handles lifecycle and adapter creation
pub struct AxonServerStoreManager {
    uri: Option<String>,
    container: Option<ContainerAsync<AxonServer>>,
    data_dir: Option<String>,
}

impl AxonServerStoreManager {
    pub fn new(data_dir: Option<String>) -> Self {
        Self {
            uri: None,
            container: None,
            data_dir,
        }
    }
}

#[async_trait]
impl StoreManager for AxonServerStoreManager {
    async fn start(&mut self) -> Result<()> {
        let container = AxonServer::new(self.data_dir.clone()).start().await?;
        let host_port = container.get_host_port_ipv4(AXONSERVER_GRPC_PORT).await?;
        self.uri = Some(format!("http://localhost:{}", host_port));
        self.container = Some(container);

        // Wait for the container to be ready
        for _ in 0..60 {
            if let Ok(mut client) = AxonServerClient::connect(self.uri.clone().unwrap()).await {
                if client.get_head().await.is_ok() {
                    return Ok(());
                }
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        anyhow::bail!("Axon Server container did not become ready within 60s")
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
        "axonserver"
    }

    fn create_adapter(&self) -> Result<Arc<dyn EventStoreAdapter>> {
        let adapter = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current()
                .block_on(async { AxonServerAdapter::new(&self.uri.clone().unwrap()).await })
        })?;
        Ok(Arc::new(adapter))
    }
}

// Lightweight adapter - just wraps a client
pub struct AxonServerAdapter {
    client: AxonServerClient,
}

impl AxonServerAdapter {
    pub async fn new(uri: &str) -> Result<Self> {
        let client = AxonServerClient::connect(uri.to_string()).await?;
        Ok(Self { client })
    }
}

#[async_trait]
impl EventStoreAdapter for AxonServerAdapter {
    async fn append(&self, evt: EventData) -> Result<()> {
        // Note: AxonServerClient requires &mut self for operations,
        // but we need &self for the trait. We'll need to clone the client.
        // This is a limitation of the axonserver_client API design.
        let mut client = self.client.clone();

        let mut tags: Vec<Tag> = evt
            .tags
            .iter()
            .map(|t| Tag {
                key: t.as_bytes().to_vec().into(),
                value: Vec::new().into(),
            })
            .collect();
        // Add a stream tag so we can filter by stream on read.
        tags.push(Tag {
            key: b"stream".to_vec().into(),
            value: evt.stream.as_bytes().to_vec().into(),
        });

        let event = Event {
            identifier: uuid::Uuid::new_v4().to_string(),
            timestamp: now_millis(),
            name: evt.event_type,
            version: String::new(),
            payload: evt.payload.into(),
            metadata: Default::default(),
        };
        let tagged = TaggedEvent {
            event: Some(event),
            tag: tags,
        };
        client.append(vec![tagged]).await?;
        Ok(())
    }

    async fn read(&self, req: ReadRequest) -> Result<Vec<ReadEvent>> {
        let mut client = self.client.clone();

        let from = req.from_offset.unwrap_or(0) as i64;
        let criterion = Criterion {
            tags_and_names: Some(TagsAndNamesCriterion {
                name: vec![],
                tag: vec![Tag {
                    key: b"stream".to_vec().into(),
                    value: req.stream.as_bytes().to_vec().into(),
                }],
            }),
        };
        let responses = client.source(from, vec![criterion]).await?;

        let mut out = Vec::new();
        for resp in responses {
            if let Some(result) = resp.result {
                match result {
                    source_events_response::Result::Event(seq_evt) => {
                        if let Some(evt) = seq_evt.event {
                            out.push(ReadEvent {
                                offset: seq_evt.sequence as u64,
                                event_type: evt.name,
                                payload: evt.payload.to_vec(),
                                timestamp_ms: evt.timestamp as u64,
                            });
                        }
                        if let Some(lim) = req.limit {
                            if out.len() as u64 >= lim {
                                break;
                            }
                        }
                    }
                    source_events_response::Result::ConsistencyMarker(_) => {}
                }
            }
        }
        Ok(out)
    }

    // async fn ping(&self) -> Result<Duration> {
    //     let mut client = self.client.clone();
    //     let t0 = std::time::Instant::now();
    //     client.get_head().await?;
    //     Ok(t0.elapsed())
    // }
}

pub struct AxonServerFactory;

impl StoreManagerFactory for AxonServerFactory {
    fn name(&self) -> &'static str {
        "axonserver"
    }

    fn create_store_manager(&self, data_dir: Option<String>) -> Result<Box<dyn StoreManager>> {
        Ok(Box::new(AxonServerStoreManager::new(data_dir)))
    }
}

fn now_millis() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}
