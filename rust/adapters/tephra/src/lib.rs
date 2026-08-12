use anyhow::{anyhow, Result};
use async_trait::async_trait;
use bench_core::adapter::{
    EsbAppendCondition, EsbQuery, EventData, EventStoreAdapter, ReadEvent, ReadRequest,
    ReadResponse, StoreDataDir, StoreManager, StoreManagerFactory,
};
use bench_core::wait_for_ready;
use bench_testcontainers::tephra::{Tephra, TEPHRA_PORT};
use std::collections::BTreeSet;
use std::sync::Arc;
use tephra_client::{
    AppendCondition, AsyncClient, AsyncReadStream, AsyncSubscribeStream, Event, EventType,
    Position, Query, QueryItem, SubEvent, Tag, Tags,
};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, ContainerRequest, ImageExt};
use tokio::time::Duration;
use tokio_stream::StreamExt;

// Store manager - handles lifecycle and adapter creation.
pub struct TephraStoreManager {
    addr: String,
    container: Option<ContainerAsync<Tephra>>,
    use_docker: bool,
    data_dir: StoreDataDir,
    memory_limit_mb: Option<u64>,
    docker_platform: Option<String>,
}

impl TephraStoreManager {
    pub fn new(data_dir: Option<String>, use_docker: bool) -> Self {
        Self {
            addr: Self::get_uri(),
            container: None,
            use_docker,
            data_dir: StoreDataDir::new(data_dir, "tephra"),
            memory_limit_mb: None,
            docker_platform: None,
        }
    }

    fn get_uri() -> String {
        let uri: String = std::env::var("TEPHRA_URI")
            .ok()
            .unwrap_or(Self::local_uri(TEPHRA_PORT.as_u16()));
        println!("Tephra Server URI: {}", uri);
        uri
    }

    fn local_uri(host_port: u16) -> String {
        format!("127.0.0.1:{}", host_port)
    }
}

#[async_trait]
impl StoreManager for TephraStoreManager {
    fn use_docker(&self) -> bool {
        self.use_docker
    }

    async fn start(&mut self) -> Result<()> {
        if self.use_docker {
            let mount_path = self.data_dir.setup()?;
            let is_bind_mount = mount_path.is_some();
            let mut image: ContainerRequest<_> = Tephra::new(mount_path).into();

            if is_bind_mount {
                // With a bind-mounted data dir, run the server as the invoking host user so the
                // files it writes are host-owned and the benchmark can clean the dir up
                // afterwards. (The image otherwise runs as its own uid, whose files the host
                // user cannot delete.) Not needed for anonymous volumes, which Docker removes.
                let uid = unsafe { libc::getuid() };
                let gid = unsafe { libc::getgid() };
                image = image.with_user(format!("{uid}:{gid}"));
            }

            // A high-concurrency run (hundreds of clients, each an async connection) can blow past
            // the default 1024 open-file limit ("Too many open files"). Raise nofile so concurrency,
            // not fds, is what's measured.
            image = image.with_ulimit("nofile", 1_048_576, Some(1_048_576));

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

            let host_port = container.get_host_port_ipv4(TEPHRA_PORT).await?;
            self.addr = format!("127.0.0.1:{host_port}");
            self.container = Some(container);

            // The container's WaitFor already gates on the "listening" log line; this second
            // check confirms the server actually serves a request over the mapped port. `Some(1)`
            // bounds the probe to a single event so it can never scan the whole log.
            let addr = self.addr.clone();
            wait_for_ready(
                "tephra",
                || {
                    let addr = addr.clone();
                    async move {
                        let client = AsyncClient::connect(&addr)
                            .await
                            .map_err(|err| anyhow!("{err}"))?;
                        client
                            .read_all(Query::all(), Position::ZERO, Some(1))
                            .await
                            .map_err(|err| anyhow!("{err}"))?;
                        Ok::<(), anyhow::Error>(())
                    }
                },
                Duration::from_secs(60),
            )
            .await?;
        }
        Ok(())
    }

    async fn pull(&mut self) -> Result<()> {
        // tephra:local is built locally (`make build-tephra-image`); there is nothing to pull.
        Ok(())
    }

    async fn stop(&mut self) -> Result<()> {
        if let Some(container) = self.container.take() {
            println!("stopping container");
            container.stop().await?;
            println!("stopped container");
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
        "tephra"
    }

    fn describe(&self) -> serde_json::Value {
        let mut desc = Tephra::describe();
        if let Some(limit_mb) = self.memory_limit_mb {
            desc["memory_limit_mb"] = serde_json::json!(limit_mb);
        }
        desc
    }

    async fn create_adapter(&mut self) -> Result<Arc<dyn EventStoreAdapter>> {
        Ok(Arc::new(TephraAdapter::connect(self.addr.clone()).await?))
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

/// Adapter over a single multiplexing async tephra client connection.
///
/// [`AsyncClient`] is a cheap `Clone` handle over one socket — like a gRPC channel, it multiplexes
/// many concurrent in-flight requests (and long-lived subscription streams) over that one
/// connection, so there is no per-operation pool to manage. The benchmark harness creates one
/// adapter per worker, so this is one connection per worker: a writer floods appends over it (the
/// client bounds outstanding requests via `AsyncClientConfig::max_inflight_requests` for end-to-end
/// backpressure), a reader streams over it, and a subscriber owns it for the subscription's life.
///
/// A per-worker *pool* is deliberately avoided: it would multiply sockets by worker count (e.g. 64
/// workers × 16 = 1024 connections) and overwhelm the server, whereas one multiplexed socket per
/// worker is both simpler and what the async client is designed for.
#[derive(Clone)]
pub struct TephraAdapter {
    client: AsyncClient,
}

impl TephraAdapter {
    pub async fn connect(addr: String) -> Result<Self> {
        let client = AsyncClient::connect(&addr)
            .await
            .map_err(|err| anyhow!("{err}"))?;
        Ok(Self { client })
    }

    /// Pack the payload and metadata into one opaque blob for tephra's single-payload `Event`.
    ///
    /// Layout: `[metadata_json_len: u32 LE][metadata_json][raw payload bytes]`. The **payload stays
    /// raw** — serialising the whole thing as JSON would encode the `Vec<u8>` payload as a number
    /// array (`[0,255,...]`), ~3-4x larger and slower. Only the (usually empty) metadata map is
    /// JSON. (`Event` has no native metadata; the subscription-latency and durability workloads
    /// need it for timestamps.)
    fn encode_payload(payload: &[u8], metadata: &[(String, String)]) -> Result<Vec<u8>> {
        let meta = serde_json::to_vec(metadata)?;
        let meta_len =
            u32::try_from(meta.len()).map_err(|_| anyhow!("event metadata too large"))?;
        let mut buf = Vec::with_capacity(4 + meta.len() + payload.len());
        buf.extend_from_slice(&meta_len.to_le_bytes());
        buf.extend_from_slice(&meta);
        buf.extend_from_slice(payload);
        Ok(buf)
    }

    /// Convert the benchmark's `EventData` into validated tephra `Event`s.
    fn convert_events(events: &[EventData]) -> Result<Vec<Event>> {
        events
            .iter()
            .map(|evt| {
                let payload = Self::encode_payload(evt.payload.as_ref(), evt.metadata.as_ref())?;
                let tags: Vec<&str> = evt.tags.iter().map(|t| t.as_ref()).collect();
                Event::new(evt.event_type.as_ref(), &tags, payload).map_err(|err| anyhow!("{err}"))
            })
            .collect()
    }

    /// Decode a tephra event's payload back into the original payload and metadata.
    fn decode_envelope(raw: &[u8]) -> (Vec<u8>, Vec<(String, String)>) {
        if raw.len() >= 4 {
            let meta_len = u32::from_le_bytes([raw[0], raw[1], raw[2], raw[3]]) as usize;
            if let Some(rest) = raw.get(4..) {
                if let Some(meta_bytes) = rest.get(..meta_len) {
                    if let Ok(metadata) =
                        serde_json::from_slice::<Vec<(String, String)>>(meta_bytes)
                    {
                        return (rest[meta_len..].to_vec(), metadata);
                    }
                }
            }
        }
        // Fallback: treat the whole blob as a raw payload with no metadata.
        (raw.to_vec(), Vec::new())
    }

    /// Convert a tephra `SequencedEvent` into the benchmark's `ReadEvent`.
    fn to_read_event(sequenced: &tephra_client::SequencedEvent) -> ReadEvent {
        let event = sequenced.event();
        let (payload, metadata) = Self::decode_envelope(event.payload());
        ReadEvent {
            offset: sequenced.position().get(),
            event_type: event.event_type().to_string(),
            payload,
            metadata,
        }
    }

    /// Build a single `QueryItem` from string type/tag lists, validating each name.
    fn build_query_item(types: &[&str], tags: &[&str]) -> Result<QueryItem> {
        let types: Vec<EventType> = types
            .iter()
            .map(|t| EventType::new(t).map_err(|err| anyhow!("{err}")))
            .collect::<Result<_>>()?;
        let tag_vec: Vec<Tag> = tags
            .iter()
            .map(|t| Tag::new(t).map_err(|err| anyhow!("{err}")))
            .collect::<Result<_>>()?;
        let tags = Tags::new(tag_vec).map_err(|err| anyhow!("{err}"))?;
        Ok(QueryItem::new(types, tags))
    }

    fn convert_query(query: &EsbQuery) -> Result<Query> {
        let items: Vec<QueryItem> = query
            .items
            .iter()
            .map(|item| {
                let types: Vec<&str> = item.types.iter().map(|s| s.as_str()).collect();
                let tags: Vec<&str> = item.tags.iter().map(|s| s.as_str()).collect();
                Self::build_query_item(&types, &tags)
            })
            .collect::<Result<_>>()?;
        Ok(Query::items(items))
    }
}

#[async_trait]
impl EventStoreAdapter for TephraAdapter {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    async fn append_dcb(
        &self,
        events: &[EventData],
        condition: Option<EsbAppendCondition>,
    ) -> Result<Option<u64>> {
        let events = Self::convert_events(events)?;
        let condition = match condition {
            Some(cond) => {
                let query = Self::convert_query(&cond.fail_if_events_match)?;
                // tephra treats `after = 0` (Position::ZERO) as "consider the whole log".
                Some(AppendCondition::new(query).after(Position::new(cond.after.unwrap_or(0))))
            }
            None => None,
        };

        let resp = self
            .client
            .append(events, condition)
            .await
            .map_err(|err| anyhow!("{err}"))?;
        Ok(Some(resp.last.get()))
    }

    async fn append_to_stream(
        &self,
        events: &[EventData],
        _stream_position: Option<usize>,
        global_position: Option<u64>,
    ) -> Result<Option<u64>> {
        // With concurrency control, guard the append on the stream's tags: fail if any event
        // carrying all of them exists after the caller's last-seen position. Mirrors how the
        // umadb adapter builds its append condition. Built before `events` is consumed below.
        let condition = match global_position {
            Some(after) => {
                let unique_tags: BTreeSet<&str> = events
                    .iter()
                    .flat_map(|evt| evt.tags.iter().map(|t| t.as_ref()))
                    .collect();
                let tag_vec: Vec<Tag> = unique_tags
                    .into_iter()
                    .map(|t| Tag::new(t).map_err(|err| anyhow!("{err}")))
                    .collect::<Result<_>>()?;
                let tags = Tags::new(tag_vec).map_err(|err| anyhow!("{err}"))?;
                let query = Query::item(QueryItem::with_tags(tags));
                Some(AppendCondition::new(query).after(Position::new(after)))
            }
            None => None,
        };

        let events = Self::convert_events(events)?;

        let resp = self
            .client
            .append(events, condition)
            .await
            .map_err(|err| anyhow!("{err}"))?;
        Ok(Some(resp.last.get()))
    }

    async fn read_stream(&self, req: ReadRequest) -> anyhow::Result<Box<dyn ReadResponse>> {
        // Build a single-item query from whichever of tag / event_type is set; an empty tag
        // and no event type means "read everything".
        let mut types: Vec<String> = Vec::new();
        if let Some(event_type) = req.event_type {
            types.push(event_type);
        }
        let mut tags: Vec<String> = Vec::new();
        if !req.tag.is_empty() {
            tags.push(req.tag);
        }
        let query = if types.is_empty() && tags.is_empty() {
            Query::all()
        } else {
            let type_refs: Vec<&str> = types.iter().map(|s| s.as_str()).collect();
            let tag_refs: Vec<&str> = tags.iter().map(|s| s.as_str()).collect();
            Query::item(Self::build_query_item(&type_refs, &tag_refs)?)
        };

        let after = Position::new(req.from_offset.unwrap_or(0));
        let limit = req.limit;

        // Push `limit` down to the server (applied during planning) so a selective read
        // materializes only `limit` events instead of the whole match. The stream is consumed
        // lazily by the workload; dropping it early cancels the read server-side.
        let stream = self.client.read(query, after, limit).await;
        Ok(Box::new(TephraReadResponse {
            stream,
            remaining: limit,
        }))
    }

    async fn read_all(&self) -> anyhow::Result<Box<dyn ReadResponse>> {
        let stream = self.client.read(Query::all(), Position::ZERO, None).await;
        Ok(Box::new(TephraReadResponse {
            stream,
            remaining: None,
        }))
    }

    async fn subscribe(&self, after: Option<u64>) -> anyhow::Result<Box<dyn ReadResponse>> {
        // Multiplex the subscription over the adapter's connection; dropping the stream (when the
        // returned response is dropped) cancels the subscription server-side.
        let after = after.map_or(Position::ZERO, Position::new);
        let stream = self.client.subscribe(Query::all(), after).await;
        Ok(Box::new(TephraSubscription { stream }))
    }
}

/// Streams a read/read-all lazily, decoding each event's metadata envelope. `remaining` caps the
/// count for a limited read (the server already applies the limit; this is a client-side belt).
struct TephraReadResponse {
    stream: AsyncReadStream,
    remaining: Option<u64>,
}

#[async_trait]
impl ReadResponse for TephraReadResponse {
    async fn next_event(&mut self) -> anyhow::Result<Option<ReadEvent>> {
        if self.remaining == Some(0) {
            return Ok(None);
        }
        match self.stream.next().await {
            Some(item) => {
                let sequenced = item.map_err(|err| anyhow!("{err}"))?;
                if let Some(remaining) = self.remaining.as_mut() {
                    *remaining -= 1;
                }
                Ok(Some(TephraAdapter::to_read_event(&sequenced)))
            }
            None => Ok(None),
        }
    }
}

/// A live subscription. Dropping this (and the `AsyncSubscribeStream` it owns) cancels the
/// subscription server-side, so no explicit cancel handle is needed.
struct TephraSubscription {
    stream: AsyncSubscribeStream,
}

#[async_trait]
impl ReadResponse for TephraSubscription {
    async fn next_event(&mut self) -> anyhow::Result<Option<ReadEvent>> {
        loop {
            match self.stream.next().await {
                Some(item) => match item.map_err(|err| anyhow!("{err}"))? {
                    SubEvent::Event(sequenced) => {
                        return Ok(Some(TephraAdapter::to_read_event(&sequenced)))
                    }
                    // Live-edge marker (caught up to the tip); keep waiting for the next event.
                    SubEvent::CaughtUp(_) => continue,
                },
                None => return Ok(None),
            }
        }
    }
}

pub struct TephraFactory;

impl StoreManagerFactory for TephraFactory {
    fn name(&self) -> &'static str {
        "tephra"
    }

    fn create_store_manager(
        &self,
        data_dir: Option<String>,
        use_docker: bool,
    ) -> Result<Box<dyn StoreManager>> {
        Ok(Box::new(TephraStoreManager::new(data_dir, use_docker)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_read_all() -> Result<()> {
        let mut manager = TephraStoreManager::new(None, true);
        manager.start().await?;

        println!("Creating adapter");
        let adapter = manager.create_adapter().await?;

        let events = vec![
            EventData {
                payload: Arc::from(vec![1, 2, 3]),
                event_type: Arc::from("type1"),
                tags: Arc::from([Arc::from("tag1")]),
                metadata: Arc::from([("a".to_string(), "b".to_string())]),
            },
            EventData {
                payload: Arc::from(vec![4, 5, 6]),
                event_type: Arc::from("type2"),
                tags: Arc::from([Arc::from("tag2")]),
                metadata: Arc::from([("c".to_string(), "d".to_string())]),
            },
        ];

        adapter.append_dcb(&events, None).await?;

        let mut read_response = adapter.read_all().await?;
        let mut received_events = Vec::new();
        while let Some(event) = read_response.next_event().await? {
            received_events.push(event);
        }

        assert!(received_events.len() >= 2);
        assert_eq!(received_events[0].payload, vec![1, 2, 3]);
        assert_eq!(received_events[1].payload, vec![4, 5, 6]);
        assert_eq!(received_events[0].event_type, "type1".to_string());
        assert_eq!(received_events[1].event_type, "type2".to_string());
        assert_eq!(received_events[0].metadata.len(), 1);
        assert_eq!(received_events[1].metadata.len(), 1);
        assert_eq!(
            received_events[0].metadata[0],
            ("a".to_string(), "b".to_string())
        );
        assert_eq!(
            received_events[1].metadata[0],
            ("c".to_string(), "d".to_string())
        );

        manager.stop().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_subscribe() -> Result<()> {
        println!("Starting test");
        let mut manager = TephraStoreManager::new(None, true);
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

        // Subscribe from the start; the two durable events replay first, then live events arrive.
        let mut subscription = adapter.subscribe(Some(0)).await?;
        let event1 = subscription.next_event().await?;
        assert_eq!(event1.unwrap().event_type, "type1");
        let event2 = subscription.next_event().await?;
        assert_eq!(event2.unwrap().event_type, "type2");

        let events = vec![EventData {
            payload: Arc::from(vec![7, 8, 9]),
            event_type: Arc::from("type3"),
            tags: Arc::from([Arc::from("tag3")]),
            metadata: Arc::from([]),
        }];

        adapter.append_dcb(&events, None).await?;

        let event3 = subscription.next_event().await?;
        assert!(event3.is_some());
        assert_eq!(event3.unwrap().event_type, "type3");

        manager.stop().await?;
        Ok(())
    }
}
