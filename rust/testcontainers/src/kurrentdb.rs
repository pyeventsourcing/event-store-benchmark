use testcontainers::core::{ContainerPort, Mount, WaitFor};
use testcontainers::Image;

const NAME: &str = "docker.kurrent.io/kurrent-latest/kurrentdb";
const TAG: &str = "25.1.0-x64-8.0-bookworm-slim";

/// Container port exposed by KurrentDB (HTTP/gRPC).
pub const KURRENTDB_PORT: ContainerPort = ContainerPort::Tcp(2113);

#[derive(Debug, Clone)]
pub struct KurrentDb {
    env_vars: Vec<(&'static str, &'static str)>,
    mounts: Vec<Mount>,
}

impl KurrentDb {
    pub fn new(data_dir: Option<String>) -> Self {
        let mount = match data_dir {
            Some(path) => Mount::bind_mount(path, "/var/lib/kurrentdb"),
            None => Mount::volume_mount("", "/var/lib/kurrentdb"),
        };
        Self {
            env_vars: vec![
                ("KURRENTDB_INSECURE", "true"),
                ("KURRENTDB_RUN_PROJECTIONS", "All"),
                ("KURRENTDB_ENABLE_ATOM_PUB_OVER_HTTP", "true"),
                ("KURRENTDB_CLUSTER_SIZE", "1"),
                ("KURRENTDB_MEM_DB", "false"),
                ("KURRENTDB_TELEMETRY_OPTOUT", "true"),
            ],
            mounts: vec![mount],
        }
    }
}

impl Default for KurrentDb {
    fn default() -> Self {
        Self::new(None)
    }
}

impl Image for KurrentDb {
    fn name(&self) -> &str {
        NAME
    }

    fn tag(&self) -> &str {
        TAG
    }

    fn ready_conditions(&self) -> Vec<WaitFor> {
        vec![]
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
        &[KURRENTDB_PORT]
    }
}
