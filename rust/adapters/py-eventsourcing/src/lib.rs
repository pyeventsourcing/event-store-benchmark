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
    local: bool,
    data_dir: StoreDataDir,
}

impl PyEventsourcingStoreManager {
    pub fn new(data_dir: Option<String>, local: bool) -> Self {
        Self {
            uri: Self::format_uri(POSTGRES_PORT.as_u16()),
            container: None,
            local,
            data_dir: StoreDataDir::new(data_dir, "py-eventsourcing"),
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
    fn local(&self) -> bool { self.local }

    async fn start(&mut self) -> Result<()> {
        let mount_path = self.data_dir.setup()?;
        let container = PyEventsourcingPostgres::new(mount_path).start().await?;
        let host_port = container.get_host_port_ipv4(POSTGRES_PORT).await?;
        self.uri = Self::format_uri(host_port);
        self.container = Some(container);

        wait_for_ready("PyEventsourcingPostgres", || async {
            let (client, connection) = tokio_postgres::connect(&self.uri, tokio_postgres::NoTls).await?;
            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    eprintln!("connection error: {}", e);
                }
            });
            client.execute("SELECT 1", &[]).await.map(|_| ()).map_err(|e| anyhow::anyhow!(e))
        }, Duration::from_secs(60)).await?;

        // Initialize tables
        let (client, connection) = tokio_postgres::connect(&self.uri, tokio_postgres::NoTls).await?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });
        let recorder = PostgresDCBRecorderTT::from_client(client, "public");
        recorder.create_tables().await?;

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
        Ok(Arc::new(PyEventsourcingAdapter::new(&self.uri).await?))
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
        let (client, connection) = tokio_postgres::connect(uri, tokio_postgres::NoTls).await?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });
        let recorder = PostgresDCBRecorderTT::from_client(client, "public");
        Ok(Self { recorder })
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

        self.recorder.append(pg_events, None).await?;
        Ok(())
    }

    async fn read(&self, req: ReadRequest) -> Result<Vec<ReadEvent>> {
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
        ).await?;

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

    fn create_store_manager(&self, data_dir: Option<String>, local: bool) -> Result<Box<dyn StoreManager>> {
        Ok(Box::new(PyEventsourcingStoreManager::new(data_dir, local)))
    }
}
