use anyhow::Result;
use async_trait::async_trait;
use axonserver_client::proto::dcb::source_events_response;
use axonserver_client::proto::dcb::{Criterion, Event, Tag, TaggedEvent, TagsAndNamesCriterion};
use axonserver_client::AxonServerClient;
use bench_core::adapter::{
    EventData, EventStoreAdapter, ReadEvent, ReadRequest, StoreDataDir, StoreManager, StoreManagerFactory,
};
use bench_core::wait_for_ready;
use bench_testcontainers::axonserver::{AxonServer, AXONSERVER_GRPC_PORT};
use std::sync::Arc;
use testcontainers::runners::AsyncRunner;
use testcontainers::ContainerAsync;
use tokio::time::Duration;

// Store manager - handles lifecycle and adapter creation
pub struct AxonServerStoreManager {
    uri: Option<String>,
    container: Option<ContainerAsync<AxonServer>>,
    local: bool,
    data_dir: StoreDataDir,
}

impl AxonServerStoreManager {
    pub fn new(data_dir: Option<String>, local: bool) -> Self {
        Self {
            uri: None,
            container: None,
            local,
            data_dir: StoreDataDir::new(data_dir, "axonserver"),
        }
    }

    // Set user so that the container data folder can be removed,
    // tried but disused because Axon Server needs to run a root.
    // fn with_user(image: AxonServer) -> Result<ContainerRequest<AxonServer>> {
    //     let uid = std::process::Command::new("id")
    //         .arg("-u")
    //         .output()?;
    //     let gid = std::process::Command::new("id")
    //         .arg("-g")
    //         .output()?;
    //     let user = format!(
    //         "{}:{}",
    //         String::from_utf8(uid.stdout)?.trim().to_string(),
    //         String::from_utf8(gid.stdout)?.trim().to_string(),
    //     );
    //     let image = image.with_user(user);
    //     Ok(image)
    // }
}

#[async_trait]
impl StoreManager for AxonServerStoreManager {
    async fn start(&mut self) -> Result<()> {
        if !self.local {
            let mount_path = self.data_dir.setup()?;
            let container = AxonServer::new(mount_path).start().await?;
            let host_port = container.get_host_port_ipv4(AXONSERVER_GRPC_PORT).await?;
            self.uri = Some(format!("http://127.0.0.1:{}", host_port));
            self.container = Some(container);
        } else {
            self.uri = Some(format!("http://127.0.0.1:{}", AXONSERVER_GRPC_PORT.as_u16()));
        }

        // Wait for the container to be ready
        let uri = self.uri.clone().unwrap();
        wait_for_ready("Axon Server", || async {
            let mut client = AxonServerClient::connect(uri.clone()).await?;
            client.get_head().await?;
            Ok(())
        }, Duration::from_secs(60)).await?;

        Ok(())
    }

    async fn pull(&mut self) -> Result<()> {
        if !self.local {
            let _ = AxonServer::new(None).pull_image().await?;
        }
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
        "axonserver"
    }

    fn create_adapter(&self) -> Result<Arc<dyn EventStoreAdapter>> {
        let adapter = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current()
                .block_on(async { AxonServerAdapter::new(&self.uri.clone().unwrap()).await })
        })?;
        Ok(Arc::new(adapter))
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
    async fn append(&self, events: Vec<EventData>) -> Result<()> {
        // Note: AxonServerClient requires &mut self for operations,
        // but we need &self for the trait. We'll need to clone the client.
        // This is a limitation of the axonserver_client API design.
        let mut client = self.client.clone();

        let tagged_events: Vec<TaggedEvent> = events.into_iter().map(|evt| {
            let tags: Vec<Tag> = evt
                .tags
                .iter()
                .map(|t| Tag {
                    key: t.as_bytes().to_vec().into(),
                    value: Vec::new().into(),
                })
                .collect();

            let event = Event {
                identifier: uuid::Uuid::new_v4().to_string(),
                timestamp: now_millis(),
                name: evt.event_type,
                version: String::new(),
                payload: evt.payload.into(),
                metadata: Default::default(),
            };
            TaggedEvent {
                event: Some(event),
                tag: tags,
            }
        }).collect();

        client.append(tagged_events).await?;
        Ok(())
    }

    async fn read(&self, req: ReadRequest) -> Result<Vec<ReadEvent>> {
        let mut client = self.client.clone();

        let from = req.from_offset.unwrap_or(0) as i64;
        let criterion = Criterion {
            tags_and_names: Some(TagsAndNamesCriterion {
                name: vec![],
                tag: vec![Tag {
                    key: req.stream.as_bytes().to_vec().into(),
                    value: Vec::new().into(),
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

    fn create_store_manager(&self, data_dir: Option<String>, local: bool) -> Result<Box<dyn StoreManager>> {
        Ok(Box::new(AxonServerStoreManager::new(data_dir, local)))
    }
}

fn now_millis() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}
