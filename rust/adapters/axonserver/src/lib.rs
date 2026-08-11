use anyhow::{Result};
use async_trait::async_trait;
use axonserver_client::proto::dcb::{source_events_response, ConsistencyCondition, StreamEventsResponse};
use axonserver_client::proto::dcb::{Criterion, Event, Tag, TaggedEvent, TagsAndNamesCriterion};
use axonserver_client::AxonServerClient;
use bench_core::adapter::{EsbAppendCondition, EventData, EventStoreAdapter, ReadResponse, ReadEvent, ReadRequest, StoreDataDir, StoreManager, StoreManagerFactory};
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
            uri: Self::get_axon_server_uri(),
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

    fn get_axon_server_uri() -> String {
        let uri: String = std::env::var("AXON_SERVER_URI").ok().unwrap_or(Self::format_uri(AXONSERVER_GRPC_PORT.as_u16()));
        println!("Axon Server URI: {}", uri);
        uri
    }

    fn format_uri(host_port: u16) -> String {
        format!("http://127.0.0.1:{}", host_port)
    }
}

#[async_trait]
impl StoreManager for AxonServerStoreManager {
    fn use_docker(&self) -> bool { self.use_docker }

    async fn start(&mut self) -> Result<()> {
        if self.use_docker {
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
        }

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
                    metadata: evt.metadata.iter().cloned().collect(),
                };

                TaggedEvent {
                    event: Some(event),
                    tag: tags,
                }
            })
            .collect()
    }

    pub async fn read_all(&self) -> Result<Box<dyn ReadResponse>> {
        let stream = self.client.source(0, vec![]).await?;
        Ok(Box::new(AxonServerReadResponse {
            stream: Some(stream),
            limit: None,
            count: 0,
        }))
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

    async fn read_stream(&self, req: ReadRequest) -> Result<Box<dyn ReadResponse>> {
        let from = req.from_offset.unwrap_or(0) as i64;
        let criterion = Criterion {
            tags_and_names: Some(TagsAndNamesCriterion {
                name: if req.event_type.is_some() {vec![req.event_type.expect("event type").into()]} else {vec![]} ,
                tag: vec![Tag {
                    key: req.tag.as_bytes().to_vec().into(),
                    value: Vec::new().into(),
                }],
            }),
        };
        let stream = self.client.source(from, vec![criterion]).await?;

        Ok(Box::new(AxonServerReadResponse {
            stream: Some(stream),
            limit: req.limit,
            count: 0,
        }))
    }

    async fn subscribe(&self, _req: Option<ReadRequest>, from_end: bool) -> Result<Box<dyn ReadResponse>> {
        let from = if from_end {
            self.client.get_head().await?
        } else {
            0
        };
        
        let stream = self.client.stream(from, vec![]).await?;
        Ok(Box::new(AxonServerSubscription { stream }))
    }

    async fn read_all_events(&self) -> anyhow::Result<Box<dyn ReadResponse>> {
        self.read_all().await
    }

    // async fn ping(&self) -> Result<Duration> {
    //     let mut client = self.client.clone();
    //     let t0 = std::time::Instant::now();
    //     client.get_head().await?;
    //     Ok(t0.elapsed())
    // }
}

/// Live subscription backed by Axon Server's infinite `Stream` RPC.
struct AxonServerSubscription {
    stream: axonserver_client::tonic::Streaming<StreamEventsResponse>,
}

#[async_trait]
impl ReadResponse for AxonServerSubscription {
    async fn next_event(&mut self) -> Result<Option<ReadEvent>> {
        while let Some(resp) = self.stream.message().await? {
            if let Some(seq_evt) = resp.event {
                if let Some(evt) = seq_evt.event {
                    return Ok(Some(ReadEvent {
                        offset: seq_evt.sequence as u64,
                        event_type: evt.name,
                        payload: evt.payload,
                        metadata: evt.metadata.into_iter().collect(),
                    }));
                }
            }
        }
        Ok(None)
    }
}

struct AxonServerReadResponse {
    stream: Option<axonserver_client::tonic::Streaming<axonserver_client::proto::dcb::SourceEventsResponse>>,
    limit: Option<u64>,
    count: u64,
}

#[async_trait]
impl ReadResponse for AxonServerReadResponse {
    async fn next_event(&mut self) -> Result<Option<ReadEvent>> {
        if let Some(lim) = self.limit {
            if self.count >= lim {
                // Drain remaining messages before closing to avoid h2 RST_STREAM resets
                if let Some(mut stream) = self.stream.take() {
                    while stream.message().await?.is_some() {}
                }
                return Ok(None);
            }
        }
        let stream = match self.stream.as_mut() {
            Some(s) => s,
            None => return Ok(None),
        };
        while let Some(resp) = stream.message().await? {
            if let Some(result) = resp.result {
                match result {
                    source_events_response::Result::Event(seq_evt) => {
                        if let Some(evt) = seq_evt.event {
                            self.count += 1;
                            return Ok(Some(ReadEvent {
                                offset: seq_evt.sequence as u64,
                                event_type: evt.name,
                                payload: evt.payload,
                                metadata: evt.metadata.into_iter().collect(),
                            }));
                        }
                    }
                    source_events_response::Result::ConsistencyMarker(_) => {}
                }
            }
        }
        Ok(None)
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_read_all() -> Result<()> {
        let mut manager = AxonServerStoreManager::new(None, true);
        manager.start().await?;

        let adapter = manager.create_adapter().await?;
        let axon_adapter = adapter.as_any().downcast_ref::<AxonServerAdapter>().expect("AxonServerAdapter");

        let events = vec![
            EventData {
                payload: Arc::from(vec![1, 2, 3]),
                event_type: Arc::from("type1"),
                tags: Arc::from([Arc::from("tag1")]),
                metadata: Arc::from([]),
            },
            EventData {
                payload: Arc::from(vec![4, 5, 6]),
                event_type: Arc::from("type2"),
                tags: Arc::from([Arc::from("tag2")]),
                metadata: Arc::from([]),
            },
        ];

        axon_adapter.append_dcb(&events, None).await?;

        let mut subscription = axon_adapter.read_all().await?;
        let mut read_events = Vec::new();
        while let Some(event) = subscription.next_event().await? {
            read_events.push(event);
        }

        assert!(read_events.len() >= 2);

        let found1 = read_events.iter().any(|e| e.event_type == "type1" && e.payload == vec![1, 2, 3]);
        let found2 = read_events.iter().any(|e| e.event_type == "type2" && e.payload == vec![4, 5, 6]);

        assert!(found1, "Event type1 not found in read_all results");
        assert!(found2, "Event type2 not found in read_all results");

        manager.stop().await?;
        Ok(())
    }
}
