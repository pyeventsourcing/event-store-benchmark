use anyhow::{anyhow, Result};
use async_trait::async_trait;
use bench_core::adapter::{
    EsbAppendCondition, EsbQuery, EventData, EventStoreAdapter, ReadEvent, ReadRequest,
    StoreDataDir, StoreManager, StoreManagerFactory,
};
use bench_core::wait_for_ready;
use bench_testcontainers::tephra::{pool_size, Tephra, TEPHRA_PORT};
use tephra_client::{
    AppendCondition, Client, Event, EventType, Position, Query, QueryItem, Tag, Tags,
};
use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use tokio::sync::Semaphore;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, ContainerRequest, ImageExt};
use tokio::time::Duration;

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
            addr: format!("127.0.0.1:{}", TEPHRA_PORT.as_u16()),
            container: None,
            use_docker,
            data_dir: StoreDataDir::new(data_dir, "tephra"),
            memory_limit_mb: None,
            docker_platform: None,
        }
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

            // tephra-server is thread-per-connection and opens log-file handles per read, so a
            // high-concurrency run (hundreds of clients) blows past the default 1024 open-file
            // limit ("Too many open files"). Raise nofile so concurrency, not fds, is measured.
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
            // check confirms the server actually serves a request over the mapped port.
            let addr = self.addr.clone();
            wait_for_ready(
                "tephra",
                || {
                    let addr = addr.clone();
                    async move {
                        tokio::task::spawn_blocking(move || {
                            let mut client =
                                Client::connect(&addr).map_err(|err| anyhow!("{err}"))?;
                            client
                                .read_all(Query::all(), Position::ZERO)
                                .map_err(|err| anyhow!("{err}"))?;
                            Ok::<(), anyhow::Error>(())
                        })
                        .await?
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

/// Adapter over a lazily-grown pool of blocking tephra client connections.
///
/// The tephra client is synchronous and answers one request at a time per connection, and
/// tephra-server is sequential per connection (it reads the next frame only after the current
/// append is durable). So a single connection can hold just one in-flight append — a worker
/// driving `in_flight_limit` concurrent ops (writeflood) would collapse to one, starving the
/// server's group commit. The adapter therefore keeps a pool of independent connections (the same
/// shape the Marten adapter uses for Postgres) and checks one out per operation, so concurrent ops
/// actually run concurrently.
///
/// The pool grows on demand up to [`pool_size`] (`ESB_TEPHRA_POOL_SIZE`, default 16): a permit
/// bounds live connections at that cap, and a connection is only opened when an op finds none idle.
/// A worker that never runs two ops at once (every reader, and non-flood writers) therefore opens
/// exactly one connection, so read workloads keep their previous single-connection footprint. The
/// blocking calls run on tokio's blocking pool.
pub struct TephraAdapter {
    addr: String,
    /// One permit per allowed connection; held for the duration of an op, so at most `pool_size`
    /// connections are ever live. Ops beyond the cap wait here (natural backpressure).
    permits: Arc<Semaphore>,
    /// Idle connections available for reuse. The `std` mutex is only held for the brief
    /// pop/push (never across an `.await`).
    idle: Arc<Mutex<Vec<Client>>>,
}

impl TephraAdapter {
    pub async fn connect(addr: String) -> Result<Self> {
        // Validate connectivity up front with one connection, kept as the pool's first member; the
        // rest are opened lazily as concurrency demands.
        let first_addr = addr.clone();
        let client = tokio::task::spawn_blocking(move || {
            Client::connect(&first_addr).map_err(|err| anyhow!("{err}"))
        })
        .await??;
        Ok(Self {
            addr,
            permits: Arc::new(Semaphore::new(pool_size())),
            idle: Arc::new(Mutex::new(vec![client])),
        })
    }

    /// Checks out a connection (reusing an idle one, or opening a new one up to the pool cap, or
    /// waiting for one to free up), runs a blocking closure against it on tokio's blocking pool,
    /// then returns it to the pool.
    async fn with_client<F, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&mut Client) -> Result<T> + Send + 'static,
        T: Send + 'static,
    {
        // The permit bounds concurrent connections to the pool size; it is held until the
        // connection is returned, so a waiter that acquires it sees a freed connection.
        let permit = Arc::clone(&self.permits)
            .acquire_owned()
            .await
            .map_err(|_| anyhow!("tephra connection pool closed"))?;
        let existing = self.idle.lock().expect("tephra pool mutex poisoned").pop();
        let mut client = match existing {
            Some(client) => client,
            None => {
                let addr = self.addr.clone();
                tokio::task::spawn_blocking(move || {
                    Client::connect(&addr).map_err(|err| anyhow!("{err}"))
                })
                .await??
            }
        };
        // The closure owns the connection for the blocking call and hands it back with the result.
        let (client, result) = tokio::task::spawn_blocking(move || {
            let result = f(&mut client);
            (client, result)
        })
        .await?;
        self.idle
            .lock()
            .expect("tephra pool mutex poisoned")
            .push(client);
        drop(permit);
        result
    }

    /// Convert the benchmark's `EventData` into validated tephra `Event`s.
    fn convert_events(events: &[EventData]) -> Result<Vec<Event>> {
        events
            .iter()
            .map(|evt| {
                let tags: Vec<&str> = evt.tags.iter().map(|t| t.as_ref()).collect();
                Event::new(evt.event_type.as_ref(), &tags, evt.payload.as_ref().to_vec())
                    .map_err(|err| anyhow!("{err}"))
            })
            .collect()
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

        self.with_client(move |client| {
            let resp = client
                .append(events, condition)
                .map_err(|err| anyhow!("{err}"))?;
            Ok(Some(resp.last.get()))
        })
        .await
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

        self.with_client(move |client| {
            let resp = client
                .append(events, condition)
                .map_err(|err| anyhow!("{err}"))?;
            Ok(Some(resp.last.get()))
        })
        .await
    }

    async fn read_stream(&self, req: ReadRequest) -> Result<Vec<ReadEvent>> {
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

        self.with_client(move |client| {
            let mut stream = client.read(query, after).map_err(|err| anyhow!("{err}"))?;
            let mut out = Vec::new();
            for item in stream.by_ref() {
                if let Some(lim) = limit {
                    if out.len() as u64 >= lim {
                        break;
                    }
                }
                let sequenced = item.map_err(|err| anyhow!("{err}"))?;
                let event = sequenced.event();
                out.push(ReadEvent {
                    offset: sequenced.position().get(),
                    event_type: event.event_type().to_string(),
                    payload: event.payload().to_vec(),
                    metadata: Vec::new(),
                });
            }
            // Dropping the partially-consumed stream drains the rest, keeping the connection
            // frame-aligned for the next request.
            Ok(out)
        })
        .await
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
