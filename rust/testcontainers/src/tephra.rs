use testcontainers::core::{ContainerPort, Mount, WaitFor};
use testcontainers::Image;

// tephra is not published to a registry; the image is built locally from the tephra
// source tree (see `docker/tephra.Dockerfile` and the `build-tephra-image` Makefile target).
const NAME: &str = "tephra";
const TAG: &str = "local";

/// Container port exposed by the tephra TCP server (length-prefixed protobuf).
pub const TEPHRA_PORT: ContainerPort = ContainerPort::Tcp(9000);

/// Default log-segment size, in bytes (16 MiB). Deliberately smaller than tephra's 256 MiB
/// shipped default: it is set to the SAME value as the other segmented DCB store (axonserver's
/// `axoniq.axonserver.event.segment-size`) so segment size is a controlled constant across the
/// two engines, and it is small enough that the sustained-write configs cross several segment
/// boundaries within a 20 s window, so amortized rollover cost is actually measured (a 256 MiB
/// segment rarely rolls in 20 s and would hide it).
pub const TEPHRA_DEFAULT_SEGMENT_SIZE_BYTES: &str = "16777216";

/// The segment size to run with: the `ESB_SEGMENT_SIZE_BYTES` env override (used by the segment
/// sweep) if set, else the 16 MiB default above. Read (never written) at container construction,
/// so it is set once by the launcher and stable for the process.
pub fn segment_size_bytes() -> String {
    std::env::var("ESB_SEGMENT_SIZE_BYTES")
        .unwrap_or_else(|_| TEPHRA_DEFAULT_SEGMENT_SIZE_BYTES.to_string())
}

/// Default number of client connections each tephra adapter opens.
///
/// The tephra client is synchronous and its server is sequential per connection, so a single
/// connection can hold only one in-flight append — a worker driving `in_flight_limit` concurrent
/// ops would otherwise collapse to one. The adapter therefore pools connections (like the Marten
/// adapter pools Postgres connections), and this is the pool size.
///
/// Tuned from a pool sweep on the 8-vCPU benchmark box (2026-08-11): writeflood throughput is a
/// function of TOTAL client connections (writers × pool), which plateaus at tephra's ceiling
/// (~48-50k eps) between ~256 and ~1024 connections and then *degrades* from thread-per-connection
/// oversubscription beyond ~2048. At the writeflood writer counts (16 and 64), pool 16 is the
/// unique value that lands both on the plateau (16×16=256, 64×16=1024) without oversubscribing;
/// higher values (e.g. 64) sag the 64-writer point badly. The fair value is environment-dependent
/// (a bigger box tolerates more threads), so it stays overridable via `ESB_TEPHRA_POOL_SIZE`.
pub const TEPHRA_DEFAULT_POOL_SIZE: usize = 16;

/// Client-connection pool size per adapter: the `ESB_TEPHRA_POOL_SIZE` override if set, else the
/// default above. Mirrors `ESB_POSTGRES_MAX_CONNECTIONS` for the Marten adapter. Read (never
/// written), so it is stable for the process. The single source of truth for both the adapter's
/// pool and the run manifest.
pub fn pool_size() -> usize {
    std::env::var("ESB_TEPHRA_POOL_SIZE")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(TEPHRA_DEFAULT_POOL_SIZE)
        .max(1)
}

#[derive(Debug, Clone)]
pub struct Tephra {
    env_vars: Vec<(&'static str, String)>,
    mounts: Vec<Mount>,
}

impl Tephra {
    pub fn new(data_dir: Option<String>) -> Self {
        let mount = match data_dir {
            Some(path) => Mount::bind_mount(path, "/data"),
            None => Mount::volume_mount("", "/data"),
        };
        Self {
            env_vars: vec![
                ("RUST_LOG", "info".to_string()),
                // Layered server config: `TEPHRA__SECTION__KEY`. Pin the segment size (16 MiB
                // default, or the ESB_SEGMENT_SIZE_BYTES sweep override). Other tuning stays default.
                ("TEPHRA__SEGMENT__SIZE", segment_size_bytes()),
            ],
            mounts: vec![mount],
        }
    }

    /// The store-config posture recorded in each run's manifest.
    pub fn describe() -> serde_json::Value {
        serde_json::json!({
            "image": format!("{NAME}:{TAG}"),
            "segment_size_bytes": segment_size_bytes(),
            "client_pool_size": pool_size(),
            "cache": "OS page cache within the container cgroup (no private cache)",
        })
    }
}

impl Default for Tephra {
    fn default() -> Self {
        Self::new(None)
    }
}

impl Image for Tephra {
    fn name(&self) -> &str {
        NAME
    }

    fn tag(&self) -> &str {
        TAG
    }

    fn ready_conditions(&self) -> Vec<WaitFor> {
        // The server logs "tephra server listening" once the accept loop is up. The adapter
        // also polls with a real client connection in `start()`, so readiness is confirmed
        // twice.
        vec![WaitFor::message_on_stdout("tephra server listening")]
    }

    fn env_vars(
        &self,
    ) -> impl IntoIterator<
        Item = (
            impl Into<std::borrow::Cow<'_, str>>,
            impl Into<std::borrow::Cow<'_, str>>,
        ),
    > {
        self.env_vars.iter().map(|(k, v)| (*k, v.clone()))
    }

    fn mounts(&self) -> impl IntoIterator<Item = &Mount> {
        self.mounts.iter()
    }

    fn expose_ports(&self) -> &[ContainerPort] {
        &[TEPHRA_PORT]
    }
}
