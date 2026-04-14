use anyhow::Result;
use async_trait::async_trait;
use bench_core::adapter::{
    EventData, EventStoreAdapter, ReadEvent, ReadRequest, StoreDataDir, StoreManager, StoreManagerFactory,
};
use bench_core::wait_for_ready;
use bench_testcontainers::py_eventsourcing::{
    PyEventsourcingPostgres, POSTGRES_PORT,
};
use py_eventsourcing::{PostgresDCBRecorderTT, DcbEvent, DcbSequencedEvent};
use std::sync::Arc;
use testcontainers::runners::AsyncRunner;
use testcontainers::ContainerAsync;
use tokio::time::Duration;

// Store manager - handles lifecycle and adapter creation
pub struct PyEventsourcingStoreManager {
    uri: String,
    container: Option<ContainerAsync<PyEventsourcingPostgres>>,
    use_docker: bool,
    data_dir: StoreDataDir,
    recorder: Option<PostgresDCBRecorderTT>,
}

impl PyEventsourcingStoreManager {
    pub fn new(data_dir: Option<String>, use_docker: bool) -> Self {
        Self {
            uri: Self::format_uri(POSTGRES_PORT.as_u16()),
            container: None,
            use_docker,
            data_dir: StoreDataDir::new(data_dir, "py-eventsourcing"),
            recorder: None,
        }
    }

    pub fn uri(&self) -> &str {
        &self.uri
    }

    fn format_uri(host_port: u16) -> String {
        format!("postgres://eventsourcing:eventsourcing@localhost:{}/eventsourcing", host_port)
    }
}

#[async_trait]
impl StoreManager for PyEventsourcingStoreManager {
    fn use_docker(&self) -> bool { self.use_docker }

    async fn start(&mut self) -> Result<()> {
        let mount_path = self.data_dir.setup()?;
        let container = PyEventsourcingPostgres::new(mount_path).start().await?;
        let host_port = container.get_host_port_ipv4(POSTGRES_PORT).await?;
        self.uri = Self::format_uri(host_port);
        self.container = Some(container);

        let recorder = PostgresDCBRecorderTT::connect(&self.uri, "public").await?;

        wait_for_ready("PyEventsourcingPostgres", || async {
            let client = recorder.pool.get().await?;
            client.execute("SELECT 1", &[]).await.map(|_| ()).map_err(|e| anyhow::anyhow!(e))
        }, Duration::from_secs(60)).await?;

        // Initialize tables
        recorder.create_tables().await?;
        self.recorder = Some(recorder);

        Ok(())
    }

    async fn pull(&mut self) -> Result<()> {
        let _ = PyEventsourcingPostgres::new(None).pull_image().await?;
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
        "py-eventsourcing"
    }

    async fn create_adapter(&self) -> Result<Arc<dyn EventStoreAdapter>> {
        if let Some(recorder) = &self.recorder {
            return Ok(Arc::new(PyEventsourcingAdapter::with_recorder(recorder.clone())));
        }

        // Lazy initialization for local stores where start() is not called
        let recorder = PostgresDCBRecorderTT::connect(&self.uri, "public").await?;
        Ok(Arc::new(PyEventsourcingAdapter::with_recorder(recorder)))
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
pub struct PyEventsourcingAdapter {
    recorder: PostgresDCBRecorderTT,
}

impl PyEventsourcingAdapter {
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
impl EventStoreAdapter for PyEventsourcingAdapter {
    fn as_any(&self) -> &dyn std::any::Any { self }
    async fn append(&self, events: Vec<EventData>) -> Result<()> {
        let pg_events: Vec<DcbEvent> = events.into_iter().map(|evt| {
            DcbEvent {
                type_name: evt.event_type,
                data: evt.payload,
                tags: evt.tags,
            }
        }).collect();

        self.recorder.append(pg_events, None).await.map_err(|e| {
            anyhow::anyhow!("PyEventsourcing append failed: {}. This might be due to pool exhaustion or high latency in the database.", e)
        })?;
        Ok(())
    }

    async fn read(&self, req: ReadRequest) -> Result<Vec<ReadEvent>> {
        let stream = req.stream.clone();
        let query = Some(py_eventsourcing::DcbQuery {
            items: vec![py_eventsourcing::DcbQueryItem {
                types: vec![],
                tags: vec![req.stream],
            }],
        });

        let events = self.recorder.read(
            query,
            req.from_offset.map(|o| o as i64),
            req.limit.map(|l| l as i64)
        ).await.map_err(|e| {
            anyhow::anyhow!("PyEventsourcing read failed for stream '{}': {}. Check pool availability and database connection.", stream, e)
        })?;

        Ok(events.into_iter().map(|e: DcbSequencedEvent| {
            ReadEvent {
                offset: e.position as u64,
                event_type: e.event.type_name,
                payload: e.event.data,
                timestamp_ms: 0,
            }
        }).collect())
    }
}

pub struct PyEventsourcingFactory;

impl StoreManagerFactory for PyEventsourcingFactory {
    fn name(&self) -> &'static str {
        "py-eventsourcing"
    }

    fn create_store_manager(&self, data_dir: Option<String>, use_docker: bool) -> Result<Box<dyn StoreManager>> {
        Ok(Box::new(PyEventsourcingStoreManager::new(data_dir, use_docker)))
    }
}
