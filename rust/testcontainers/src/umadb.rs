use testcontainers::core::{ContainerPort, Mount, WaitFor};
use testcontainers::Image;

const NAME: &str = "umadb/umadb";
const TAG: &str = "0.4.0";

/// Container port exposed by UmaDB (gRPC).
pub const UMADB_PORT: ContainerPort = ContainerPort::Tcp(50051);

#[derive(Debug, Clone)]
pub struct UmaDb {
    mounts: Vec<Mount>,
}

impl UmaDb {
    pub fn new(data_dir: Option<String>) -> Self {
        let mount = match data_dir {
            Some(path) => Mount::bind_mount(path, "/data"),
            None => Mount::volume_mount("", "/data"),
        };
        Self {
            mounts: vec![mount],
        }
    }
}

impl Default for UmaDb {
    fn default() -> Self {
        Self::new(None)
    }
}

impl Image for UmaDb {
    fn name(&self) -> &str {
        NAME
    }

    fn tag(&self) -> &str {
        TAG
    }

    fn ready_conditions(&self) -> Vec<WaitFor> {
        vec![WaitFor::message_on_stdout("UmaDB started")]
        // vec![]
    }

    fn mounts(&self) -> impl IntoIterator<Item = &Mount> {
        self.mounts.iter()
    }

    fn expose_ports(&self) -> &[ContainerPort] {
        &[UMADB_PORT]
    }
}
