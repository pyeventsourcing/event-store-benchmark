use testcontainers::core::{ContainerPort, Mount, WaitFor};
use testcontainers::Image;

const NAME: &str = "evntd/fact-bench-server";
const TAG: &str = "0.3.1";

/// Container port exposed by the Fact benchmark server (gRPC).
pub const FACT_PORT: ContainerPort = ContainerPort::Tcp(4000);

#[derive(Debug, Clone)]
pub struct FactDb {
    mounts: Vec<Mount>,
}

impl FactDb {
    pub fn new(data_dir: Option<String>) -> Self {
        let mount = match data_dir {
            Some(path) => Mount::bind_mount(path, "/data/benchmark"),
            None => Mount::volume_mount("", "/data/benchmark"),
        };
        Self {
            mounts: vec![mount],
        }
    }
}

impl Default for FactDb {
    fn default() -> Self {
        Self::new(None)
    }
}

impl Image for FactDb {
    fn name(&self) -> &str {
        NAME
    }
    fn tag(&self) -> &str {
        TAG
    }
    fn ready_conditions(&self) -> Vec<WaitFor> {
        vec![]
    }
    fn mounts(&self) -> impl IntoIterator<Item = &Mount> {
        self.mounts.iter()
    }
    fn expose_ports(&self) -> &[ContainerPort] {
        &[FACT_PORT]
    }
}
