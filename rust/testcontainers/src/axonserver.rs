use testcontainers::core::{ContainerPort, Mount, WaitFor};
use testcontainers::Image;

const NAME: &str = "axoniq/axonserver";
const TAG: &str = "2026.0.5-jdk-21-nonroot";

/// gRPC API port exposed by Axon Server.
pub const AXONSERVER_GRPC_PORT: ContainerPort = ContainerPort::Tcp(8124);

/// HTTP/Dashboard port exposed by Axon Server.
pub const AXONSERVER_HTTP_PORT: ContainerPort = ContainerPort::Tcp(8024);

#[derive(Debug, Clone)]
pub struct AxonServer {
    env_vars: Vec<(&'static str, String)>,
    mounts: Vec<Mount>,
}

/// The `JAVA_TOOL_OPTIONS` value: pin the heap and set the DCB event-store segment size (16 MiB
/// default, or the `ESB_SEGMENT_SIZE_BYTES` sweep override — the same source tephra reads, so both
/// segmented stores stay equal). This image's entrypoint is a bare `java -jar axonserver.jar` (no
/// launch script), so `JAVA_OPTS` is ignored; the JVM-native `JAVA_TOOL_OPTIONS` is honored
/// directly. Verified on disk that `-Daxoniq.axonserver.event.segment-size` controls
/// `/axonserver/events/<ctx>/*.events` (default 256 MiB). NB: `/axonserver/log/*.log` (16 MiB) is
/// Axon's Raft replication log, a separate transient store, NOT the event segment.
fn java_tool_options() -> String {
    format!(
        "-Xmx2g -Xms2g -Daxoniq.axonserver.event.segment-size={}",
        crate::tephra::segment_size_bytes()
    )
}

impl AxonServer {
    pub fn new(data_dir: Option<String>) -> Self {
        let mount = match data_dir {
            Some(path) => Mount::bind_mount(path, "/axonserver/events"),
            None => Mount::volume_mount("", "/axonserver/events"),
        };
        Self {
            env_vars: vec![
                ("AXONIQ_AXONSERVER_NAME", "bench-axon-server".to_string()),
                ("AXONIQ_AXONSERVER_HOSTNAME", "bench-axon-server".to_string()),
                ("AXONIQ_AXONSERVER_STANDALONE_DCB", "true".to_string()),
                ("JAVA_TOOL_OPTIONS", java_tool_options()),
            ],
            mounts: vec![mount],
        }
    }

    /// The store-config posture recorded in each run's manifest.
    pub fn describe() -> serde_json::Value {
        serde_json::json!({
            "image": format!("{NAME}:{TAG}"),
            "event_segment_size_bytes": crate::tephra::segment_size_bytes(),
            "segment_mechanism": "-Daxoniq.axonserver.event.segment-size via JAVA_TOOL_OPTIONS \
                                  (controls /axonserver/events/<ctx>/*.events; verified on disk); \
                                  set equal to tephra to control segment size and force rollover",
            "replication_log_segment_bytes": "16777216 (Raft log, separate from event store)",
            "heap": "-Xmx2g -Xms2g (via JAVA_TOOL_OPTIONS)",
            "mode": "standalone DCB, unlicensed trial (12h)",
        })
    }
}

impl Default for AxonServer {
    fn default() -> Self {
        Self::new(None)
    }
}

impl Image for AxonServer {
    fn name(&self) -> &str {
        NAME
    }

    fn tag(&self) -> &str {
        TAG
    }

    fn ready_conditions(&self) -> Vec<WaitFor> {
        vec![WaitFor::message_on_stdout("Started AxonServer")]
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
        &[AXONSERVER_GRPC_PORT, AXONSERVER_HTTP_PORT]
    }
}
