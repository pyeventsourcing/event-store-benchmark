use anyhow::Result;
use async_trait::async_trait;
use bench_core::adapter::{
    EventData, EventStoreAdapter, ReadEvent, ReadRequest, StoreDataDir, StoreManager, StoreManagerFactory,
};
use bench_core::wait_for_ready;
use bench_testcontainers::eventsourcingdb::{
    EventsourcingDb, EVENTSOURCINGDB_API_TOKEN, EVENTSOURCINGDB_PORT,
};
use eventsourcingdb::client::Client;
use eventsourcingdb::event::EventCandidate;
use futures::StreamExt;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use testcontainers::ImageExt;
use testcontainers::runners::AsyncRunner;
use testcontainers::ContainerAsync;
use tokio::time::Duration;
use url::Url;

// Store manager - handles lifecycle and adapter creation
pub struct EventsourcingDbStoreManager {
    uri: String,
    options: HashMap<String, String>,
    container: Option<ContainerAsync<EventsourcingDb>>,
    use_docker: bool,
    data_dir: StoreDataDir,
    memory_limit_mb: Option<u64>,
}

impl EventsourcingDbStoreManager {
    pub fn new(data_dir: Option<String>, use_docker: bool) -> Self {
        Self {
            uri: Self::format_uri(EVENTSOURCINGDB_PORT.as_u16()),
            container: None,
            options: HashMap::new(),
            use_docker,
            data_dir: StoreDataDir::new(data_dir, "eventsourcingdb"),
            memory_limit_mb: None,
        }
    }

    fn format_uri(host_port: u16) -> String {
        format!("http://127.0.0.1:{}/", host_port)
    }
}

#[async_trait]
impl StoreManager for EventsourcingDbStoreManager {
    fn use_docker(&self) -> bool { self.use_docker }

    async fn start(&mut self) -> Result<()> {
        let mount_path = self.data_dir.setup()?;
        let image = EventsourcingDb::new(mount_path);

        let container = if let Some(limit_mb) = self.memory_limit_mb {
            let bytes = limit_mb * 1024 * 1024;
            image.with_host_config_modifier(move |host_config| {
                host_config.memory = Some(bytes as i64);
            })
            .start()
            .await?
        } else {
            image.start().await?
        };

        let host_port = container.get_host_port_ipv4(EVENTSOURCINGDB_PORT).await?;
        self.uri = Self::format_uri(host_port);
        self.container = Some(container);

        // Use the default API token for the container
        self.options
            .insert("api_token".to_string(), EVENTSOURCINGDB_API_TOKEN.to_string());

        wait_for_ready("EventsourcingDB", || async {
            let client = Client::new(Url::parse(&self.uri)?, EVENTSOURCINGDB_API_TOKEN);
            client.ping().await.map_err(|e| anyhow::anyhow!(e))
        }, Duration::from_secs(60)).await?;

        Ok(())
    }

    async fn pull(&mut self) -> Result<()> {
        let _ = EventsourcingDb::new(None).pull_image().await?;
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

    fn name(&self) -> &'static str {
        "eventsourcingdb"
    }

    async fn create_adapter(&self) -> Result<Arc<dyn EventStoreAdapter>> {
        Ok(Arc::new(EventsourcingDbAdapter::new(&self.uri, &self.options)?))
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
pub struct EventsourcingDbAdapter {
    client: Client,
}

impl EventsourcingDbAdapter {
    pub fn new(uri: &str, options: &HashMap<String, String>) -> Result<Self> {
        let api_token = options.get("api_token").cloned().unwrap_or_default();
        let url: Url = uri
            .parse()
            .map_err(|e| anyhow::anyhow!("invalid URL: {}", e))?;
        let client = Client::new(url, api_token);
        Ok(Self { client })
    }
}

#[async_trait]
impl EventStoreAdapter for EventsourcingDbAdapter {
    fn as_any(&self) -> &dyn std::any::Any { self }
    async fn append(&self, events: Vec<EventData>) -> Result<()> {
        let candidates: Vec<EventCandidate> = events.into_iter().map(|evt| {
            let data: serde_json::Value = serde_json::from_slice(&evt.payload).unwrap_or_else(|_| {
                json!({"raw": serde_json::Value::String(
                    String::from_utf8_lossy(&evt.payload).to_string()
                )})
            });
            EventCandidate::builder()
                .source("https://bench.eventsourcingdb.io".to_string())
                .subject(format!("/{}", evt.tags[0]))
                .ty(if evt.event_type.contains('.') {
                    evt.event_type
                } else {
                    format!("io.eventsourcingdb.bench.{}", evt.event_type)
                })
                .data(data)
                .build()
        }).collect();

        self.client
            .write_events(candidates, vec![])
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))?;
        Ok(())
    }

    async fn read(&self, req: ReadRequest) -> Result<Vec<ReadEvent>> {
        let subject = format!("/{}", req.stream);
        let mut stream = self
            .client
            .read_events(&subject, None)
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))?;
        let mut out = Vec::new();
        let mut offset: u64 = 0;
        while let Some(result) = stream.next().await {
            let event = result.map_err(|e| anyhow::anyhow!("{}", e))?;
            let current_offset = offset;
            offset += 1;

            let mut met_limit = false;
            if let Some(lim) = req.limit {
                if out.len() as u64 >= lim {
                    met_limit = true;
                }
            }

            if !met_limit {
                if let Some(from) = req.from_offset {
                    if current_offset < from {
                        continue;
                    }
                }
                let payload = serde_json::to_vec(event.data())?;
                let timestamp_ms = event.time().timestamp_millis() as u64;
                out.push(ReadEvent {
                    offset: current_offset,
                    event_type: event.ty().to_string(),
                    payload,
                    timestamp_ms,
                });
            }
        }
        Ok(out)
    }

    // async fn ping(&self) -> Result<Duration> {
    //     let t0 = std::time::Instant::now();
    //     self.client
    //         .ping()
    //         .await
    //         .map_err(|e| anyhow::anyhow!("{}", e))?;
    //     Ok(t0.elapsed())
    // }
}

pub struct EventsourcingDbFactory;

impl StoreManagerFactory for EventsourcingDbFactory {
    fn name(&self) -> &'static str {
        "eventsourcingdb"
    }

    fn create_store_manager(&self, data_dir: Option<String>, use_docker: bool) -> Result<Box<dyn StoreManager>> {
        Ok(Box::new(EventsourcingDbStoreManager::new(data_dir, use_docker)))
    }
}
