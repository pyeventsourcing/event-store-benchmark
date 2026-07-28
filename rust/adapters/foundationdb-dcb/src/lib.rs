use anyhow::Result;
use async_trait::async_trait;
use bench_core::adapter::{
    EsbAppendCondition, EventData, EventStoreAdapter, ReadEvent, ReadRequest, StoreDataDir,
    StoreManager, StoreManagerFactory,
};
use bench_core::wait_for_ready;
use bench_testcontainers::foundationdb_dcb::{FoundationDb, FDB_PORT};
use dcb_layer::{AppendCondition, Event, FdbStore, Query, QueryItem, ReadOptions, Versionstamp};
use foundationdb::Database;
use std::collections::HashMap;
use std::mem::ManuallyDrop;
use std::process::Command;
use std::sync::{Arc, Once};
use std::io::Write;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, ContainerRequest, ImageExt};
use tokio::time::Duration;

// ---------------------------------------------------------------------------
// Versionstamp ↔ u64 registry
// ---------------------------------------------------------------------------

struct VsRegistry {
    next_id: u64,
    vs_to_id: HashMap<[u8; 12], u64>,
    id_to_vs: HashMap<u64, [u8; 12]>,
}

impl VsRegistry {
    fn new() -> Self {
        // ID 0 is reserved as the "beginning" sentinel (maps to no versionstamp).
        Self { next_id: 1, vs_to_id: HashMap::new(), id_to_vs: HashMap::new() }
    }

    fn register(&mut self, vs: Versionstamp) -> u64 {
        if let Some(&id) = self.vs_to_id.get(&vs) {
            return id;
        }
        let id = self.next_id;
        self.next_id += 1;
        self.vs_to_id.insert(vs, id);
        self.id_to_vs.insert(id, vs);
        id
    }

    fn lookup(&self, id: u64) -> Option<Versionstamp> {
        self.id_to_vs.get(&id).copied()
    }
}

// ---------------------------------------------------------------------------
// Shared state between adapter instances
// ---------------------------------------------------------------------------

struct FdbSharedState {
    store: FdbStore,
}

// FdbStore holds foundationdb::Database which is Send+Sync (FDB C client is thread-safe).
unsafe impl Send for FdbSharedState {}
unsafe impl Sync for FdbSharedState {}

// ---------------------------------------------------------------------------
// Adapter
// ---------------------------------------------------------------------------

pub struct FoundationDbDcbAdapter {
    state: Arc<FdbSharedState>,
    // Per-adapter registry: no cross-adapter lock contention.
    vs_registry: std::sync::Mutex<VsRegistry>,
}

fn convert_events(events: &[EventData]) -> Vec<Event> {
    events
        .iter()
        .map(|e| {
            let type_name = e.event_type.to_string();
            let tags: Vec<String> = e.tags.iter().map(|t| t.to_string()).collect();
            Event::new(type_name, tags, bytes::Bytes::from_owner(e.payload.clone()))
        })
        .collect()
}

fn condition_after(registry: &VsRegistry, id: Option<u64>) -> Option<Versionstamp> {
    match id {
        None | Some(0) => None,
        Some(id) => registry.lookup(id),
    }
}

#[async_trait]
impl EventStoreAdapter for FoundationDbDcbAdapter {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    async fn append_dcb(
        &self,
        events: &[EventData],
        condition: Option<EsbAppendCondition>,
    ) -> Result<Option<u64>> {
        let fdb_events = convert_events(events);

        let fdb_conditions: Vec<AppendCondition> = condition
            .into_iter()
            .map(|c| {
                let after = condition_after(&self.vs_registry.lock().unwrap(), c.after);
                AppendCondition {
                    query: Query {
                        items: c
                            .fail_if_events_match
                            .items
                            .into_iter()
                            .map(|item| QueryItem {
                                types: item.types,
                                tags: item.tags,
                            })
                            .collect(),
                    },
                    after,
                }
            })
            .collect();

        let vs = self
            .state
            .store
            .append(fdb_events, fdb_conditions)
            .await
            .map_err(|e| anyhow::anyhow!(e))?;

        let id = self.vs_registry.lock().unwrap().register(vs);
        Ok(Some(id))
    }

    async fn append_to_stream(
        &self,
        events: &[EventData],
        _stream_position: Option<usize>,
        _global_position: Option<u64>,
    ) -> Result<Option<u64>> {
        let fdb_events = convert_events(events);

        let vs = self
            .state
            .store
            .append(fdb_events, vec![])
            .await
            .map_err(|e| anyhow::anyhow!(e))?;

        let id = self.vs_registry.lock().unwrap().register(vs);
        Ok(Some(id))
    }

    async fn read_stream(&self, req: ReadRequest) -> Result<Vec<ReadEvent>> {
        let query = Query {
            items: vec![QueryItem {
                types: req.event_type.into_iter().collect(),
                tags: vec![req.tag],
            }],
        };

        let after = condition_after(&self.vs_registry.lock().unwrap(), req.from_offset);

        let opts = ReadOptions {
            limit: req.limit.unwrap_or(0) as usize,
            after,
            reverse: false,
        };

        let stored = self
            .state
            .store
            .read(query, Some(opts))
            .await
            .map_err(|e| anyhow::anyhow!(e))?;

        let mut registry = self.vs_registry.lock().unwrap();
        let out = stored
            .into_iter()
            .map(|se| {
                let id = registry.register(se.position);
                ReadEvent {
                    offset: id,
                    event_type: Arc::from(se.event.type_name.as_ref()),
                    payload: Arc::from(se.event.data.as_ref()),
                }
            })
            .collect();
        Ok(out)
    }
}

// ---------------------------------------------------------------------------
// FDB network — boot once per process
// ---------------------------------------------------------------------------

fn ensure_foundationdb_dcb_network() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let _ = ManuallyDrop::new(unsafe { foundationdb::boot() });
    });
}

// ---------------------------------------------------------------------------
// Store manager
// ---------------------------------------------------------------------------

pub struct FoundationDbDcbStoreManager {
    container: Option<ContainerAsync<FoundationDb>>,
    cluster_file: Option<tempfile::NamedTempFile>,
    shared_state: Option<Arc<FdbSharedState>>,
    use_docker: bool,
    data_dir: StoreDataDir,
    memory_limit_mb: Option<u64>,
    docker_platform: Option<String>,
}

impl FoundationDbDcbStoreManager {
    pub fn new(data_dir: Option<String>, use_docker: bool) -> Self {
        Self {
            container: None,
            cluster_file: None,
            shared_state: None,
            use_docker,
            data_dir: StoreDataDir::new(data_dir, "foundationdb-dcb"),
            memory_limit_mb: None,
            docker_platform: None,
        }
    }

    async fn init_from_cluster_file(&mut self, cluster_path: &str) -> Result<()> {
        ensure_foundationdb_dcb_network();

        let cluster_path = cluster_path.to_string();
        wait_for_ready(
            "FoundationDB",
            || {
                let path = cluster_path.clone();
                async move {
                    Database::new(Some(path.as_str())).map_err(|e| anyhow::anyhow!(e))?;
                    Ok(())
                }
            },
            Duration::from_secs(30),
        )
        .await?;

        let db = Database::new(Some(cluster_path.as_str())).map_err(|e| anyhow::anyhow!(e))?;
        let store = FdbStore::new(db, "bench");
        self.shared_state = Some(Arc::new(FdbSharedState { store }));
        Ok(())
    }
}

#[async_trait]
impl StoreManager for FoundationDbDcbStoreManager {
    fn use_docker(&self) -> bool {
        self.use_docker
    }

    async fn start(&mut self) -> Result<()> {
        self.data_dir.setup()?;

        if self.use_docker() {
            let mut image: ContainerRequest<_> = FoundationDb::default().into();

            if let Some(ref platform) = self.docker_platform {
                image = image.with_platform(platform);
            }
            if let Some(limit_mb) = self.memory_limit_mb {
                let bytes = limit_mb * 1024 * 1024;
                image = image.with_host_config_modifier(move |hc| {
                    hc.memory = Some(bytes as i64);
                });
            }

            // Fixed 4500:4500 mapping required: FDB_NETWORKING_MODE=host makes the
            // server advertise 127.0.0.1:4500, which must match the host-side address.
            image = image.with_mapped_port(4500u16, FDB_PORT);
            let container = image.start().await?;
            let container_id = container.id().to_string();

            // Initialize the FDB database inside the container.
            let out = Command::new("docker")
                .args(["exec", &container_id, "fdbcli", "--exec", "configure new single ssd"])
                .output()?;
            let stdout = String::from_utf8_lossy(&out.stdout);
            anyhow::ensure!(
                stdout.contains("Database created"),
                "fdbcli configure did not confirm database creation (stdout: {}, stderr: {})",
                stdout,
                String::from_utf8_lossy(&out.stderr)
            );

            // Wait until FDB storage is ready to serve reads.
            let mut available = false;
            for _ in 0..60u32 {
                let s = Command::new("docker")
                    .args(["exec", &container_id, "fdbcli", "--exec", "status minimal"])
                    .output()?;
                if String::from_utf8_lossy(&s.stdout).contains("available") {
                    available = true;
                    break;
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            anyhow::ensure!(available, "FDB did not become available in time");

            // Write a cluster file the host process can use.
            let mut cluster_file = tempfile::NamedTempFile::new()?;
            writeln!(cluster_file, "docker:docker@127.0.0.1:4500")?;

            let path = cluster_file.path().to_str().unwrap().to_string();
            self.cluster_file = Some(cluster_file);
            self.container = Some(container);

            self.init_from_cluster_file(&path).await?;
        } else {
            let path = std::env::var("FDB_CLUSTER_FILE").unwrap_or_default();
            let path = if path.is_empty() { "/usr/local/etc/foundationdb/fdb.cluster".to_string() } else { path };
            self.init_from_cluster_file(&path).await?;
        }
        Ok(())
    }

    async fn pull(&mut self) -> Result<()> {
        // Skip pull if the image already exists locally (e.g. custom-built ARM image).
        let image_ref = format!("{}:{}", bench_testcontainers::foundationdb_dcb::FDB_IMAGE_NAME, bench_testcontainers::foundationdb_dcb::fdb_image_tag());
        let exists = Command::new("docker")
            .args(["image", "inspect", &image_ref])
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false);
        if exists {
            return Ok(());
        }
        let mut image: ContainerRequest<_> = FoundationDb::default().into();
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
        self.cluster_file.take();
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
        "foundationdb-dcb"
    }

    async fn create_adapter(&mut self) -> Result<Arc<dyn EventStoreAdapter>> {
        let state = self
            .shared_state
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("FoundationDbDcbStoreManager not started"))?
            .clone();
        Ok(Arc::new(FoundationDbDcbAdapter {
            state,
            vs_registry: std::sync::Mutex::new(VsRegistry::new()),
        }))
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

// ---------------------------------------------------------------------------
// Factory
// ---------------------------------------------------------------------------

pub struct FoundationDbDcbFactory;

impl StoreManagerFactory for FoundationDbDcbFactory {
    fn name(&self) -> &'static str {
        "foundationdb-dcb"
    }

    fn create_store_manager(
        &self,
        data_dir: Option<String>,
        use_docker: bool,
    ) -> Result<Box<dyn StoreManager>> {
        Ok(Box::new(FoundationDbDcbStoreManager::new(data_dir, use_docker)))
    }
}
