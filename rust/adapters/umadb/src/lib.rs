use anyhow::Result;
use async_trait::async_trait;
use bench_core::adapter::{
    EsbAppendCondition, EventData, EventStoreAdapter, ReadEvent, ReadRequest, ReadResponse,
    StoreDataDir, StoreManager, StoreManagerFactory,
};
use bench_core::wait_for_ready;
use bench_testcontainers::umadb::{UmaDb, UMADB_PORT};
use futures::StreamExt;
use std::sync::Arc;
use testcontainers::runners::AsyncRunner;
use testcontainers::ImageExt;
use testcontainers::{ContainerAsync, ContainerRequest};
use tokio::time::Duration;
use umadb_client::UmaDbClient;
use umadb_dcb::{
    DcbAppendCondition, DcbEvent, DcbEventStoreAsync, DcbQuery, DcbQueryItem, DcbReadResponseAsync,
    DcbSubscriptionAsync,
};

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
            uri: Self::get_umadb_uri(),
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

    fn get_umadb_uri() -> String {
        let uri: String = std::env::var("UMADB_URI")
            .ok()
            .unwrap_or(Self::format_uri(UMADB_PORT.as_u16()));
        println!("UmaDB Server URI: {}", uri);
        uri
    }
}

#[async_trait]
impl StoreManager for UmaDbStoreManager {
    fn use_docker(&self) -> bool {
        self.use_docker
    }

    async fn start(&mut self) -> Result<()> {
        if self.use_docker {
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

            // At small (e.g. 16 MiB) segment sizes a multi-GB run produces hundreds of segment
            // files, and these engines hold file handles per segment; raise the container's
            // open-file limit so fds — not concurrency — are never the bottleneck (mirrors tephra).
            image = image.with_ulimit("nofile", 1_048_576, Some(1_048_576));

            let container = image.start().await?;

            let host_port = container.get_host_port_ipv4(UMADB_PORT).await?;
            self.uri = Self::format_uri(host_port);
            self.container = Some(container);

            // Wait for container to be ready
            wait_for_ready(
                "UmaDb",
                || async {
                    let client = UmaDbClient::new(self.uri.clone()).connect_async().await?;
                    client.head().await?;
                    Ok(())
                },
                Duration::from_secs(60),
            )
            .await?;
        }
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
        if let Some(container) = self.container.take() {
            println!("Stopping container");
            container.stop().await?;
            println!("Stopped container");
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
        "umadb"
    }

    fn describe(&self) -> serde_json::Value {
        let mut desc = UmaDb::describe();
        if let Some(limit_mb) = self.memory_limit_mb {
            desc["memory_limit_mb"] = serde_json::json!(limit_mb);
        }
        desc
    }

    async fn create_adapter(&mut self) -> Result<Arc<dyn EventStoreAdapter>> {
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

    fn convert_events(events: &[EventData]) -> Vec<DcbEvent> {
        events
            .iter()
            .map(|evt| DcbEvent {
                event_type: evt.event_type.to_string(),
                tags: evt.tags.iter().map(|t| t.to_string()).collect(),
                data: evt.payload.to_vec(),
                uuid: None,
                metadata: evt.metadata.to_vec(),
            })
            .collect()
    }
}

#[async_trait]
impl EventStoreAdapter for UmaDbAdapter {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    async fn append_dcb(
        &self,
        events: &[EventData],
        condition: Option<EsbAppendCondition>,
    ) -> anyhow::Result<Option<u64>> {
        let dcb_events = Self::convert_events(events);

        let dcb_condition: Option<DcbAppendCondition> = condition.map(|cond| DcbAppendCondition {
            fail_if_events_match: DcbQuery {
                items: cond
                    .fail_if_events_match
                    .items
                    .into_iter()
                    .map(|item| DcbQueryItem {
                        types: item.types.into_iter().map(|t| t.to_string()).collect(),
                        tags: item.tags.into_iter().map(|t| t.to_string()).collect(),
                    })
                    .collect(),
            },
            after: cond.after,
        });

        let pos: u64 = self.client.append(dcb_events, dcb_condition, None).await?;
        Ok(Some(pos))
    }

    async fn append_to_stream(
        &self,
        events: &[EventData],
        _stream_position: Option<usize>,
        global_position: Option<u64>,
    ) -> anyhow::Result<Option<u64>> {
        let dcb_events = Self::convert_events(events);
        let append_condition: Option<DcbAppendCondition> = if global_position.is_some() {
            // One query item with one tag, for each unique tag mentioned in all events.
            Some(DcbAppendCondition {
                fail_if_events_match: DcbQuery::new().item(
                    dcb_events
                        .iter()
                        .flat_map(|evt| &evt.tags)
                        .collect::<std::collections::HashSet<_>>()
                        .into_iter()
                        .fold(DcbQueryItem::new(), |item, tag| {
                            item.tags(vec![tag.to_string()])
                        }),
                ),
                after: global_position,
            })
        } else {
            None
        };
        let pos: u64 = self
            .client
            .append(dcb_events, append_condition, None)
            .await?;
        Ok(Some(pos))
    }

    async fn read_stream(&self, req: ReadRequest) -> anyhow::Result<Box<dyn ReadResponse>> {
        // An empty tag and no event type means "no filter" (full scan). Passing an empty-string
        // tag would otherwise match nothing, so build the query from only the set fields and
        // send `None` when neither is present, matching the tephra adapter's semantics.
        let mut types = Vec::new();
        if let Some(event_type) = req.event_type {
            types.push(event_type);
        }
        let mut tags = Vec::new();
        if !req.tag.is_empty() {
            tags.push(req.tag);
        }
        let query = if types.is_empty() && tags.is_empty() {
            None
        } else {
            Some(DcbQuery {
                items: vec![DcbQueryItem { types, tags }],
            })
        };
        let stream = self
            .client
            .read(query, req.from_offset, false, req.limit.map(|l| l as u32))
            .await?;
        Ok(Box::new(UmaDbReadResponse { stream }))
    }

    async fn subscribe(&self, after: Option<u64>) -> anyhow::Result<Box<dyn ReadResponse>> {
        let stream = self.client.subscribe(None, after).await?;
        Ok(Box::new(UmaDbSubscription { stream }))
    }

    async fn read_all(&self) -> Result<Box<dyn ReadResponse>> {
        let stream = self.client.read(None, None, false, None).await?;
        Ok(Box::new(UmaDbReadResponse { stream }))
    }
}

/// Live subscription backed by UmaDB's async gRPC event stream.
struct UmaDbSubscription {
    stream: Box<dyn DcbSubscriptionAsync + Send + 'static>,
}

#[async_trait]
impl ReadResponse for UmaDbSubscription {
    async fn next_event(&mut self) -> anyhow::Result<Option<ReadEvent>> {
        match self.stream.next().await {
            Some(Ok(se)) => Ok(Some(ReadEvent {
                offset: se.position,
                event_type: se.event.event_type,
                payload: se.event.data,
                metadata: se.event.metadata,
            })),
            Some(Err(e)) => Err(e.into()),
            None => Ok(None),
        }
    }
}

struct UmaDbReadResponse {
    stream: Box<dyn DcbReadResponseAsync + Send + 'static>,
}

#[async_trait]
impl ReadResponse for UmaDbReadResponse {
    async fn next_event(&mut self) -> anyhow::Result<Option<ReadEvent>> {
        match self.stream.next().await {
            Some(Ok(se)) => Ok(Some(ReadEvent {
                offset: se.position,
                event_type: se.event.event_type,
                payload: se.event.data,
                metadata: se.event.metadata,
            })),
            Some(Err(e)) => Err(e.into()),
            None => Ok(None),
        }
    }
}

pub struct UmaDbFactory;

impl StoreManagerFactory for UmaDbFactory {
    fn name(&self) -> &'static str {
        "umadb"
    }

    fn create_store_manager(
        &self,
        data_dir: Option<String>,
        use_docker: bool,
    ) -> Result<Box<dyn StoreManager>> {
        Ok(Box::new(UmaDbStoreManager::new(data_dir, use_docker)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_read_all() -> Result<()> {
        let mut manager = UmaDbStoreManager::new(None, true);
        manager.start().await?;

        let adapter = manager.create_adapter().await?;

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

        adapter.append_dcb(&events, None).await?;

        let mut read_response = adapter.read_all().await?;
        let mut received_events = Vec::new();
        while let Some(event) = read_response.next_event().await? {
            received_events.push(event);
        }

        assert!(received_events.len() >= 2);

        let found1 = received_events
            .iter()
            .any(|e| e.event_type == "type1" && e.payload == vec![1, 2, 3]);
        let found2 = received_events
            .iter()
            .any(|e| e.event_type == "type2" && e.payload == vec![4, 5, 6]);

        assert!(found1, "Event type1 not found in read_all results");
        assert!(found2, "Event type2 not found in read_all results");

        manager.stop().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_subscribe() -> Result<()> {
        println!("Starting test");
        let mut manager = UmaDbStoreManager::new(None, true);
        manager.start().await?;

        let adapter = manager.create_adapter().await?;

        let mut subscription = adapter.subscribe(None).await?;

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

        adapter.append_dcb(&events, None).await?;

        // let mut received_events = Vec::new();

        let event1 = subscription.next_event().await?;
        let event2 = subscription.next_event().await?;
        assert!(event1.is_some());
        assert!(event2.is_some());

        manager.stop().await?;
        Ok(())
    }
}
