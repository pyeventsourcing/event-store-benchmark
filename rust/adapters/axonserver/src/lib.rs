use anyhow::{Result};
use async_trait::async_trait;
use axonserver_client::proto::dcb::{source_events_response, ConsistencyCondition};
use axonserver_client::proto::dcb::{Criterion, Event, Tag, TaggedEvent, TagsAndNamesCriterion};
use axonserver_client::AxonServerClient;
use bench_core::adapter::{EsbAppendCondition, EventData, EventStoreAdapter, ReadEvent, ReadRequest, StoreDataDir, StoreManager, StoreManagerFactory};
use bench_core::wait_for_ready;
use bench_testcontainers::axonserver::{AxonServer, AXONSERVER_GRPC_PORT};
use std::sync::Arc;
use testcontainers::ImageExt;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, ContainerRequest};
use tokio::time::Duration;

// Store manager - handles lifecycle and adapter creation
pub struct AxonServerStoreManager {
    uri: String,
    container: Option<ContainerAsync<AxonServer>>,
    use_docker: bool,
    data_dir: StoreDataDir,
    memory_limit_mb: Option<u64>,
    docker_platform: Option<String>,
}

impl AxonServerStoreManager {
    pub fn new(data_dir: Option<String>, use_docker: bool) -> Self {
        Self {
            uri: format!("http://127.0.0.1:{}", AXONSERVER_GRPC_PORT.as_u16()),
            container: None,
            use_docker,
            data_dir: StoreDataDir::new(data_dir, "axonserver"),
            memory_limit_mb: None,
            docker_platform: None,
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

    fn format_uri(host_port: u16) -> String {
        format!("http://127.0.0.1:{}", host_port)
    }
}

#[async_trait]
impl StoreManager for AxonServerStoreManager {
    fn use_docker(&self) -> bool { self.use_docker }

    async fn start(&mut self) -> Result<()> {
        let mount_path = self.data_dir.setup()?;
        let mut image: ContainerRequest<_> = AxonServer::new(mount_path).into();

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

        let host_port = container.get_host_port_ipv4(AXONSERVER_GRPC_PORT).await?;
        self.uri = Self::format_uri(host_port);
        self.container = Some(container);

        // Wait for the container to be ready
        wait_for_ready("Axon Server", || async {
            let client = AxonServerClient::connect(self.uri.clone()).await?;
            client.get_head().await?;
            Ok(())
        }, Duration::from_secs(60)).await?;

        Ok(())
    }

    async fn pull(&mut self) -> Result<()> {
        let mut image: ContainerRequest<_> = AxonServer::new(None).into();
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
        "axonserver"
    }

    async fn create_adapter(&mut self) -> Result<Arc<dyn EventStoreAdapter>> {
        Ok(Arc::new(AxonServerAdapter::new(self.uri.clone()).await?))
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
    pub async fn new(uri: String) -> Result<Self> {
        let client = AxonServerClient::connect(uri)
            .await
            .map_err(|e| anyhow::anyhow!(e))?;
        Ok(Self { client })
    }

    fn convert_events(events: &[EventData]) -> Vec<TaggedEvent> {
        events
            .iter()
            .map(|evt| {
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
                    name: evt.event_type.to_string(),
                    version: String::new(),
                    payload: evt.payload.to_vec().into(),
                    metadata: Default::default(),
                };

                TaggedEvent {
                    event: Some(event),
                    tag: tags,
                }
            })
            .collect()
    }
}

#[async_trait]
impl EventStoreAdapter for AxonServerAdapter {
    fn as_any(&self) -> &dyn std::any::Any { self }

    async fn append_dcb(&self, events: &[EventData], condition: Option<EsbAppendCondition>) -> anyhow::Result<Option<u64>> {
        let tagged_events = Self::convert_events(events);

        let consistency_condition = condition.map(|c| ConsistencyCondition {
            consistency_marker: c.after.map_or(0, |p| p as i64),
            criterion: c
                .fail_if_events_match
                .items
                .into_iter()
                .map(|item| Criterion {
                    tags_and_names: Some(TagsAndNamesCriterion {
                        name: item.types,
                        tag: item
                            .tags
                            .into_iter()
                            .map(|t| Tag {
                                key: t.as_bytes().to_vec().into(),
                                value: Vec::new().into(),
                            })
                            .collect(),
                    }),
                })
                .collect(),
        });

        let position = self.client.append(tagged_events, consistency_condition).await?;
        Ok(Some(if position >= 0 { position as u64 } else { 0 }))
    }

    async fn append_to_stream(&self, events: &[EventData], _stream_position: Option<usize>, global_position: Option<u64>) -> anyhow::Result<Option<u64>> {
        let tagged_events = Self::convert_events(events);

        let condition = if let Some(global_position) = global_position {
            Some(ConsistencyCondition{
                consistency_marker: global_position as i64,
                criterion: {
                    let mut unique_tags = std::collections::HashSet::new();
                    for tagged_event in &tagged_events {
                        for tag in &tagged_event.tag {
                            unique_tags.insert(tag.value.clone());
                        }
                    }
                    unique_tags.into_iter().map(|tag_value| Criterion {
                        tags_and_names: Some(TagsAndNamesCriterion {
                            name: vec![],
                            tag: vec![Tag {
                                key: "stream".into(),
                                value: tag_value,
                            }],
                        })
                    }).collect()
                }
                
            })
        } else {
            None
        };
        let position = self.client.append(tagged_events, condition).await?;
        Ok(Some(if position >= 0 {position as u64} else {0}))
    }

    async fn read_stream(&self, req: ReadRequest) -> Result<Vec<ReadEvent>> {
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
        let responses = self.client.source(from, vec![criterion]).await?;

        let mut out = Vec::new();
        for resp in responses {
            if let Some(result) = resp.result {
                match result {
                    source_events_response::Result::Event(seq_evt) => {
                        if let Some(evt) = seq_evt.event {
                            out.push(ReadEvent {
                                offset: seq_evt.sequence as u64,
                                event_type: evt.name.into(),
                                payload: evt.payload.to_vec().into(),
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

    fn create_store_manager(&self, data_dir: Option<String>, use_docker: bool) -> Result<Box<dyn StoreManager>> {
        Ok(Box::new(AxonServerStoreManager::new(data_dir, use_docker)))
    }
}

fn now_millis() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}
