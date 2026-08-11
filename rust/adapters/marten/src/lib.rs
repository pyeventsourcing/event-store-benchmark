use anyhow::{Context, Result};
use async_trait::async_trait;
use bench_core::adapter::{EsbAppendCondition, EventData, EventStoreAdapter, ReadEvent, ReadRequest, StoreDataDir, StoreManager, StoreManagerFactory};
use bench_core::wait_for_ready;
use marten_rs::read::EventTagQuery;
use marten_rs::{Marten as MartenClient, MartenDcbEvent};
use std::sync::Arc;
use testcontainers::core::{ContainerPort, Mount, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, Image, ImageExt, ContainerRequest};
use tokio::time::Duration;
use std::borrow::Cow;

const NAME: &str = "postgres";
const TAG: &str = "16-alpine";

/// Container port exposed by Postgres.
pub const POSTGRES_PORT: ContainerPort = ContainerPort::Tcp(5432);

#[derive(Debug, Clone)]
pub struct Marten {
    env_vars: Vec<(&'static str, &'static str)>,
    mounts: Vec<Mount>,
}

impl Marten {
    pub fn new(data_dir: Option<String>) -> Self {
        let mount = match data_dir {
            Some(path) => Mount::bind_mount(path, "/var/lib/postgresql/data"),
            None => Mount::volume_mount("", "/var/lib/postgresql/data"),
        };
        Self {
            env_vars: vec![
                ("POSTGRES_DB", "marten"),
                ("POSTGRES_USER", "postgres"),
                ("POSTGRES_PASSWORD", "postgres"),
            ],
            mounts: vec![mount],
        }
    }

    /// The store-config posture recorded in each run's manifest.
    pub fn describe() -> serde_json::Value {
        serde_json::json!({
            "image": format!("{NAME}:{TAG}"),
            "shared_buffers": "1GB",
            "note": "Marten = bare Postgres INSERT path (no reliably-followable sequence)",
        })
    }
}

impl Default for Marten {
    fn default() -> Self {
        Self::new(None)
    }
}

impl Image for Marten {
    fn name(&self) -> &str {
        NAME
    }

    fn tag(&self) -> &str {
        TAG
    }

    fn ready_conditions(&self) -> Vec<WaitFor> {
        vec![WaitFor::message_on_stderr("database system is ready to accept connections")]
    }

    fn cmd(&self) -> impl IntoIterator<Item = impl Into<Cow<'_, str>>> {
        // Pin shared_buffers so Postgres's cache budget is a recorded, comparable value rather
        // than the tiny 128 MB default; the 4 GB container cgroup cap is the outer equalizer.
        ["postgres", "-c", "shared_buffers=1GB", "-c", "max_connections=200"]
    }

    fn env_vars(
        &self,
    ) -> impl IntoIterator<
        Item = (
            impl Into<Cow<'_, str>>,
            impl Into<Cow<'_, str>>,
        ),
    > {
        self.env_vars.iter().map(|(k, v)| (*k, *v))
    }

    fn mounts(&self) -> impl IntoIterator<Item = &Mount> {
        self.mounts.iter()
    }

    fn expose_ports(&self) -> &[ContainerPort] {
        &[POSTGRES_PORT]
    }
}

pub struct MartenStoreManager {
    uri: String,
    container: Option<ContainerAsync<Marten>>,
    use_docker: bool,
    data_dir: StoreDataDir,
    client: Option<MartenClient>,
    memory_limit_mb: Option<u64>,
    docker_platform: Option<String>,
}

impl MartenStoreManager {
    pub fn new(data_dir: Option<String>, use_docker: bool) -> Self {
        Self {
            uri: Self::format_uri(POSTGRES_PORT.as_u16()),
            container: None,
            use_docker,
            data_dir: StoreDataDir::new(data_dir, "postgres-dcb-marten"),
            client: None,
            memory_limit_mb: None,
            docker_platform: None,
        }
    }

    pub fn uri(&self) -> &str {
        &self.uri
    }

    fn format_uri(host_port: u16) -> String {
        // Credentials/db must match the container env set in `Marten::new`
        // (POSTGRES_USER=postgres, POSTGRES_PASSWORD=postgres, POSTGRES_DB=marten); otherwise
        // every connect fails with `role "eventsourcing" does not exist` and the store never
        // becomes ready.
        format!("postgres://postgres:postgres@127.0.0.1:{host_port}/marten")
    }
}

#[async_trait]
impl StoreManager for MartenStoreManager {
    fn use_docker(&self) -> bool {
        self.use_docker
    }

    async fn start(&mut self) -> Result<()> {
        if self.use_docker {
            
            let mount_path = self.data_dir.setup()?;
            let mut image: ContainerRequest<_> = Marten::new(mount_path).into();
    
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
    
            let host_port = container.get_host_port_ipv4(POSTGRES_PORT).await?;
            self.uri = Self::format_uri(host_port);
            self.container = Some(container);
    
            // Reconnect on every attempt: the official Postgres image logs "ready to accept
            // connections" during its init bootstrap and then restarts, so a connection made
            // before that restart goes stale and every create_tables() on it fails for the
            // whole timeout (seen as "Marten did not become ready within 60s"). Connecting
            // inside the loop yields a live client once Postgres is truly up.
            let uri = self.uri.clone();
            let client = wait_for_ready(
                "Marten",
                || {
                    let uri = uri.clone();
                    async move {
                        let client = MartenClient::connect(&uri).await?;
                        client.create_tables().await.map_err(|e| anyhow::anyhow!("{}", e))?;
                        Ok(client)
                    }
                },
                Duration::from_secs(90),
            )
            .await?;

            self.client = Some(client);
        }
        Ok(())
    }

    async fn pull(&mut self) -> Result<()> {
        let mut image: ContainerRequest<_> = Marten::new(None).into();
        if let Some(ref platform) = self.docker_platform {
            image = image.with_platform(platform);
        }
        let _ = image.pull_image().await?;
        Ok(())
    }

    async fn stop(&mut self) -> Result<()> {
        if let Some(container) = self.container.take() {
            let _: () = container.stop().await?;
        }
        self.data_dir.cleanup()?;
        Ok(())
    }

    fn container_id(&self) -> Option<String> {
        self.container.as_ref().map(|c: &ContainerAsync<Marten>| c.id().to_string())
    }

    fn set_memory_limit(&mut self, limit_mb: Option<u64>) {
        self.memory_limit_mb = limit_mb;
    }

    fn set_docker_platform(&mut self, platform: Option<String>) {
        self.docker_platform = platform;
    }

    fn name(&self) -> &'static str {
        "postgres-dcb-marten"
    }

    fn describe(&self) -> serde_json::Value {
        let mut desc = Marten::describe();
        if let Some(limit_mb) = self.memory_limit_mb {
            desc["memory_limit_mb"] = serde_json::json!(limit_mb);
        }
        desc
    }

    async fn create_adapter(&mut self) -> Result<Arc<dyn EventStoreAdapter>> {
        if self.client.is_none() {
            self.client = Some(MartenClient::connect(&self.uri).await?)
        }
        let client = self.client.as_ref().expect("client initialized").clone();
        Ok(Arc::new(MartenAdapter::with_client(client)))    }

    async fn logs(&self) -> Result<String> {
        // Just return empty for now to avoid compilation issues with testcontainers logs API
        Ok(String::new())
    }
}

pub struct MartenAdapter {
    client: MartenClient,
}

impl MartenAdapter {
    pub fn with_client(client: MartenClient) -> Self {
        Self {
            client,
        }
    }

    pub fn client(&self) -> &MartenClient {
        &self.client
    }
}

#[async_trait]
impl EventStoreAdapter for MartenAdapter {
    fn as_any(&self) -> &dyn std::any::Any { self }

    async fn append_dcb(&self, _events: &[EventData], _condition: Option<EsbAppendCondition>) -> anyhow::Result<Option<u64>> {
        anyhow::bail!("append_dcb not implemented in MartenAdapter")
    }

    async fn append_to_stream(&self, events: &[EventData], stream_position: Option<usize>, global_position: Option<u64>) -> anyhow::Result<Option<u64>> {
        let event_tag_query = if stream_position.is_some() || global_position.is_some() {
            let mut query = EventTagQuery::new(global_position.unwrap() as i64);

            let mut seen_tags = std::collections::HashSet::new();
            for event in events {
                for tag in event.tags.iter() {
                    if seen_tags.insert(tag.as_ref()) {
                        query = query.with_tag(tag);
                    }
                }
            }
            Some(query)
        } else {
            None
        };
        let event_count = events.len();
        let event_types_preview = events
            .iter()
            .take(5)
            .map(|evt| evt.event_type.as_ref())
            .collect::<Vec<_>>()
            .join(", ");
        let tags_preview = events
            .first()
            .map(|evt| {
                evt.tags
                    .iter()
                    .take(5)
                    .map(|tag| tag.as_ref())
                    .collect::<Vec<_>>()
                    .join(", ")
            })
            .unwrap_or_default();

        let mut marten_events: Vec<MartenDcbEvent> = events
            .iter()
            .map(|evt| MartenDcbEvent {
                event_type: evt.event_type.to_string(),
                tags: evt.tags.iter().map(|t| t.to_string()).collect(),
                data: serde_json::from_slice(&evt.payload).unwrap_or(serde_json::Value::Null),
            })
            .collect();

        let sequence_ids = self.client
            .append_events(&mut marten_events, event_tag_query.as_ref())
            .await
            .with_context(|| {
                format!(
                    "Marten append failed (events={}, event_types=[{}], first_event_tags=[{}], stream_position={:?}, global_position={:?})",
                    event_count,
                    event_types_preview,
                    tags_preview,
                    stream_position,
                    global_position
                )
            })?;
        Ok(Some(sequence_ids.last().expect("Marten sequence ID").clone() as u64))
    }

    async fn read_stream(&self, req: ReadRequest) -> Result<Box<dyn bench_core::adapter::ReadResponse>> {
        let mut query = EventTagQuery::new(req.from_offset.map(|o| o as i64).unwrap_or(-1));
        if !req.tag.is_empty() {
            query = query.with_tag(&req.tag);
        }

        let events = self.client.read_events(&query).await.with_context(|| {
            format!("Marten read failed for stream '{}'", req.tag)
        })?;

        let mut out = Vec::new();
        for (i, se) in events.into_iter().enumerate() {
            if let Some(lim) = req.limit {
                if i as u64 >= lim {
                    break;
                }
            }
            out.push(ReadEvent {
                offset: se.seq_id as u64,
                event_type: se.event_type,
                payload: serde_json::to_vec(&se.data)?,
                metadata: vec![],
            });
        }
        Ok(Box::new(bench_core::adapter::VecReadResponse::new(out)))
    }
}

pub struct MartenFactory;

impl StoreManagerFactory for MartenFactory {
    fn name(&self) -> &'static str {
        "postgres-dcb-marten"
    }

    fn create_store_manager(
        &self,
        data_dir: Option<String>,
        use_docker: bool,
    ) -> Result<Box<dyn StoreManager>> {
        Ok(Box::new(MartenStoreManager::new(data_dir, use_docker)))
    }
}
