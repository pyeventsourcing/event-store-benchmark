use anyhow::{Context, Result};
use async_trait::async_trait;
use bench_core::adapter::{
    EsbAppendCondition, EventData, EventStoreAdapter, ReadEvent, ReadRequest, ReadResponse,
    StoreDataDir, StoreManager, StoreManagerFactory,
};
use bench_core::wait_for_ready;
use bench_testcontainers::postgres_dcb_ttcte::{PostgresDcbTtctePostgres, POSTGRES_PORT};
use postgres_dcb_ttcte::{
    DcbAppendCondition, DcbEvent, DcbQuery, DcbQueryItem, DcbSequencedEvent, NotificationListener,
    PostgresDCBRecorderTT,
};
use std::collections::VecDeque;
use std::sync::Arc;
use testcontainers::runners::AsyncRunner;
use testcontainers::ImageExt;
use testcontainers::{ContainerAsync, ContainerRequest};
use tokio::time::Duration;

// Store manager - handles lifecycle and adapter creation
pub struct PostgresDcbTtcteStoreManager {
    uri: String,
    container: Option<ContainerAsync<PostgresDcbTtctePostgres>>,
    use_docker: bool,
    data_dir: StoreDataDir,
    recorder: Option<PostgresDCBRecorderTT>,
    memory_limit_mb: Option<u64>,
    docker_platform: Option<String>,
}

impl PostgresDcbTtcteStoreManager {
    pub fn new(data_dir: Option<String>, use_docker: bool) -> Self {
        Self {
            uri: Self::format_uri(POSTGRES_PORT.as_u16()),
            container: None,
            use_docker,
            data_dir: StoreDataDir::new(data_dir, "postgres-dcb-ttcte"),
            recorder: None,
            memory_limit_mb: None,
            docker_platform: None,
        }
    }

    pub fn uri(&self) -> &str {
        &self.uri
    }

    fn format_uri(host_port: u16) -> String {
        format!(
            "postgres://eventsourcing:eventsourcing@localhost:{}/eventsourcing",
            host_port
        )
    }
}

#[async_trait]
impl StoreManager for PostgresDcbTtcteStoreManager {
    fn use_docker(&self) -> bool {
        self.use_docker
    }

    async fn start(&mut self) -> Result<()> {
        if self.use_docker {
            let mount_path = self.data_dir.setup()?;
            let mut image: ContainerRequest<_> = PostgresDcbTtctePostgres::new(mount_path).into();

            // Inject the environment variables to create the role and database
            image = image
                .with_env_var("POSTGRES_USER", "eventsourcing")
                .with_env_var("POSTGRES_PASSWORD", "eventsourcing")
                .with_env_var("POSTGRES_DB", "eventsourcing");

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

            let host_port = container.get_host_port_ipv4(POSTGRES_PORT).await?;
            self.uri = Self::format_uri(host_port);
            self.container = Some(container);

            let recorder = PostgresDCBRecorderTT::connect(&self.uri, "public").await?;

            wait_for_ready(
                "PostgresDcbTtctePostgres",
                || async {
                    let client = recorder.pool.get().await?;
                    client
                        .execute("SELECT 1", &[])
                        .await
                        .map(|_| ())
                        .map_err(|e| anyhow::anyhow!(e))
                },
                Duration::from_secs(60),
            )
            .await?;

            // Initialize tables
            recorder.create_tables().await?;
            self.recorder = Some(recorder);
        }

        Ok(())
    }

    async fn pull(&mut self) -> Result<()> {
        let mut image: ContainerRequest<_> = PostgresDcbTtctePostgres::new(None).into();
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
        "postgres-dcb-ttcte"
    }

    async fn create_adapter(&mut self) -> Result<Arc<dyn EventStoreAdapter>> {
        if self.recorder.is_none() {
            let recorder = PostgresDCBRecorderTT::connect(&self.uri, "public").await?;

            self.recorder = Some(recorder);
        }
        let recorder = self
            .recorder
            .as_ref()
            .expect("recorder initialized")
            .clone();
        Ok(Arc::new(PostgresDcbTtcteAdapter::with_recorder(recorder)))
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
pub struct PostgresDcbTtcteAdapter {
    recorder: PostgresDCBRecorderTT,
}

impl PostgresDcbTtcteAdapter {
    pub async fn new(uri: &str) -> Result<Self> {
        let recorder = PostgresDCBRecorderTT::connect(uri, "public").await?;
        Ok(Self { recorder })
    }

    pub fn with_recorder(recorder: PostgresDCBRecorderTT) -> Self {
        Self { recorder }
    }

    pub fn recorder(&self) -> &PostgresDCBRecorderTT {
        &self.recorder
    }
}

#[async_trait]
impl EventStoreAdapter for PostgresDcbTtcteAdapter {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    async fn append_dcb(
        &self,
        events: &[EventData],
        condition: Option<EsbAppendCondition>,
    ) -> anyhow::Result<Option<u64>> {
        let append_condition: Option<DcbAppendCondition> =
            condition.map(|cond| DcbAppendCondition {
                fail_if_events_match: DcbQuery {
                    items: cond
                        .fail_if_events_match
                        .items
                        .iter()
                        .map(|item| DcbQueryItem {
                            types: item.types.clone(),
                            tags: item.tags.clone(),
                        })
                        .collect(),
                },
                after: cond.after.map(|pos| pos as i64),
            });
        let pg_events: Vec<DcbEvent> = events
            .iter()
            .map(|evt| DcbEvent {
                type_name: evt.event_type.to_string(),
                data: evt.payload.to_vec(),
                tags: evt.tags.iter().map(|t| t.to_string()).collect(),
                uuid: "".to_string(),
                metadata: evt.metadata.to_vec(),
            })
            .collect();

        let pos = self
            .recorder
            .append(pg_events, append_condition)
            .await
            .context("PostgresDcbTtcte append failed")?;
        Ok(Some(pos as u64))
    }

    async fn append_to_stream(
        &self,
        events: &[EventData],
        _stream_position: Option<usize>,
        global_position: Option<u64>,
    ) -> anyhow::Result<Option<u64>> {
        let append_condition: Option<DcbAppendCondition> = if global_position.is_some() {
            // One query item with one tag, for each unique tag mentioned in all events.
            Some(DcbAppendCondition {
                fail_if_events_match: DcbQuery {
                    items: events
                        .iter()
                        .flat_map(|evt| evt.tags.iter())
                        .collect::<std::collections::HashSet<_>>()
                        .into_iter()
                        .map(|tag| DcbQueryItem {
                            types: vec![],
                            tags: vec![tag.to_string()],
                        })
                        .collect(),
                },
                after: Some(global_position.expect("global position") as i64),
            })
        } else {
            None
        };
        let pg_events: Vec<DcbEvent> = events
            .iter()
            .map(|evt| DcbEvent {
                type_name: evt.event_type.to_string(),
                data: evt.payload.to_vec(),
                tags: evt.tags.iter().map(|t| t.to_string()).collect(),
                uuid: "".to_string(),
                metadata: evt.metadata.to_vec(),
            })
            .collect();

        let pos = self
            .recorder
            .append(pg_events, append_condition)
            .await
            .context("PostgresDcbTtcte append failed")?;
        Ok(Some(pos as u64))
    }

    async fn read_stream(
        &self,
        req: ReadRequest,
    ) -> Result<Box<dyn bench_core::adapter::ReadResponse>> {
        let stream = req.tag.clone();
        let query = Some(postgres_dcb_ttcte::DcbQuery {
            items: vec![postgres_dcb_ttcte::DcbQueryItem {
                types: if req.event_type.is_some() {
                    vec![req.event_type.expect("event type").into()]
                } else {
                    vec![]
                },
                tags: vec![req.tag],
            }],
        });

        let events = self
            .recorder
            .read(
                query,
                req.from_offset.map(|o| o as i64),
                req.limit.map(|l| l as i64),
            )
            .await
            .with_context(|| format!("PostgresDcbTtcte read failed for stream '{}'", stream))?;

        let out = events
            .into_iter()
            .map(|e: DcbSequencedEvent| ReadEvent {
                offset: e.position as u64,
                event_type: e.event.type_name,
                payload: e.event.data,
                metadata: e.event.metadata,
            })
            .collect();
        Ok(Box::new(bench_core::adapter::VecReadResponse::new(out)))
    }

    async fn subscribe(&self, after: Option<u64>) -> Result<Box<dyn ReadResponse>> {
        // Start LISTENing before reading the head so no append is missed in the
        // gap between establishing the baseline and receiving notifications.
        let listener = self.recorder.open_listener().await?;
        Ok(Box::new(PostgresSubscription {
            recorder: self.recorder.clone(),
            listener,
            last_position: after.unwrap_or(0) as i64,
            buffer: VecDeque::new(),
        }))
    }
}

/// Live subscription backed by Postgres `LISTEN`/`NOTIFY`.
///
/// Each `NOTIFY` (emitted by the append functions) wakes the subscription, which
/// then reads every event past its last position and yields them in order.
struct PostgresSubscription {
    recorder: PostgresDCBRecorderTT,
    listener: NotificationListener,
    last_position: i64,
    buffer: VecDeque<ReadEvent>,
}

#[async_trait]
impl ReadResponse for PostgresSubscription {
    async fn next_event(&mut self) -> Result<Option<ReadEvent>> {
        loop {
            if let Some(event) = self.buffer.pop_front() {
                return Ok(Some(event));
            }

            // Block until an append is signalled, then coalesce any burst.
            match self.listener.recv().await {
                Some(()) => self.listener.drain(),
                None => return Ok(None), // connection closed
            }

            let events = self
                .recorder
                .read(None, Some(self.last_position), None)
                .await
                .context("PostgresDcbTtcte subscription read failed")?;

            for e in events {
                if e.position > self.last_position {
                    self.last_position = e.position;
                }
                self.buffer.push_back(ReadEvent {
                    offset: e.position as u64,
                    event_type: e.event.type_name,
                    payload: e.event.data,
                    metadata: e.event.metadata,
                });
            }
        }
    }
}

pub struct PostgresDcbTtcteFactory;

impl StoreManagerFactory for PostgresDcbTtcteFactory {
    fn name(&self) -> &'static str {
        "postgres-dcb-ttcte"
    }

    fn create_store_manager(
        &self,
        data_dir: Option<String>,
        use_docker: bool,
    ) -> Result<Box<dyn StoreManager>> {
        Ok(Box::new(PostgresDcbTtcteStoreManager::new(
            data_dir, use_docker,
        )))
    }
}
