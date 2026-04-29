use testcontainers::core::{ContainerPort, Mount, WaitFor};
use testcontainers::Image;

const NAME: &str = "axoniq/axonserver";
const TAG: &str = "2026.0.0-jdk-17-nonroot";

/// gRPC API port exposed by Axon Server.
pub const AXONSERVER_GRPC_PORT: ContainerPort = ContainerPort::Tcp(8124);

/// HTTP/Dashboard port exposed by Axon Server.
pub const AXONSERVER_HTTP_PORT: ContainerPort = ContainerPort::Tcp(8024);

#[derive(Debug, Clone)]
pub struct AxonServer {
    env_vars: Vec<(&'static str, &'static str)>,
    mounts: Vec<Mount>,
}

impl AxonServer {
    pub fn new(data_dir: Option<String>) -> Self {
        let mount = match data_dir {
            Some(path) => Mount::bind_mount(path, "/axonserver/events"),
            None => Mount::volume_mount("", "/axonserver/events"),
        };
        Self {
            env_vars: vec![
                ("AXONIQ_AXONSERVER_NAME", "bench-axon-server"),
                ("AXONIQ_AXONSERVER_HOSTNAME", "bench-axon-server"),
                ("AXONIQ_AXONSERVER_STANDALONE_DCB", "true"),
            ],
            mounts: vec![mount],
        }
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
        self.env_vars.iter().map(|(k, v)| (*k, *v))
    }

    fn mounts(&self) -> impl IntoIterator<Item = &Mount> {
        self.mounts.iter()
    }

    fn expose_ports(&self) -> &[ContainerPort] {
        &[AXONSERVER_GRPC_PORT, AXONSERVER_HTTP_PORT]
    }
}
