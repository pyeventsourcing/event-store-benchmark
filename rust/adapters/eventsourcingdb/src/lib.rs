use anyhow::Result;
use async_trait::async_trait;
use bench_core::adapter::{
    EventData, EventStoreAdapter, ReadEvent, ReadRequest, StoreManager, StoreManagerFactory,
};
use bench_testcontainers::eventsourcingdb::{
    EventsourcingDb, EVENTSOURCINGDB_API_TOKEN, EVENTSOURCINGDB_PORT,
};
use eventsourcingdb::client::Client;
use eventsourcingdb::event::EventCandidate;
use futures::StreamExt;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use testcontainers::runners::AsyncRunner;
use testcontainers::ContainerAsync;
use tokio::time::Duration;
use url::Url;

// Store manager - handles lifecycle and adapter creation
pub struct EventsourcingDbStoreManager {
    uri: Option<String>,
    options: HashMap<String, String>,
    container: Option<ContainerAsync<EventsourcingDb>>,
    data_dir: Option<String>,
}

impl EventsourcingDbStoreManager {
    pub fn new(data_dir: Option<String>) -> Self {
        Self {
            uri: None,
            container: None,
            options: HashMap::new(),
            data_dir,
        }
    }
}

#[async_trait]
impl StoreManager for EventsourcingDbStoreManager {
    async fn start(&mut self) -> Result<()> {
        let container = EventsourcingDb::new(self.data_dir.clone()).start().await?;
        let host_port = container.get_host_port_ipv4(EVENTSOURCINGDB_PORT).await?;
        self.uri = Some(format!("http://localhost:{}/", host_port));
        self.container = Some(container);

        // Use the default API token for the container
        self.options
            .insert("api_token".to_string(), EVENTSOURCINGDB_API_TOKEN.to_string());

        // Wait for container to be ready
        for _ in 0..60 {
            let url: Url = self.uri.clone().unwrap().parse()?;
            let client = Client::new(url, EVENTSOURCINGDB_API_TOKEN);
            if client.ping().await.is_ok() {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        anyhow::bail!("EventsourcingDB container did not become ready within 60s")
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
        "eventsourcingdb"
    }

    fn create_adapter(&self) -> Result<Arc<dyn EventStoreAdapter>> {
        Ok(Arc::new(EventsourcingDbAdapter::new(&self.uri.clone().unwrap(), &self.options)?))
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
    async fn append(&self, evt: EventData) -> Result<()> {
        let data: serde_json::Value = serde_json::from_slice(&evt.payload).unwrap_or_else(|_| {
            json!({"raw": serde_json::Value::String(
                String::from_utf8_lossy(&evt.payload).to_string()
            )})
        });
        let event = EventCandidate::builder()
            .source("https://bench.eventsourcingdb.io".to_string())
            .subject(format!("/{}", evt.stream))
            .ty(if evt.event_type.contains('.') {
                evt.event_type
            } else {
                format!("io.eventsourcingdb.bench.{}", evt.event_type)
            })
            .data(data)
            .build();
        self.client
            .write_events(vec![event], vec![])
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
            if let Some(lim) = req.limit {
                if out.len() as u64 >= lim {
                    break;
                }
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

    fn create_store_manager(&self, data_dir: Option<String>) -> Result<Box<dyn StoreManager>> {
        Ok(Box::new(EventsourcingDbStoreManager::new(data_dir)))
    }
}
