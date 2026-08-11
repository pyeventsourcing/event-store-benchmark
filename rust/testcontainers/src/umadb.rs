use testcontainers::core::{ContainerPort, Mount, WaitFor};
use testcontainers::Image;

// const NAME: &str = "umadb";
// const TAG: &str = "local";
const NAME: &str = "umadb/umadb";
const TAG: &str = "0.7.5";

/// Container port exposed by UmaDB (gRPC).
pub const UMADB_PORT: ContainerPort = ContainerPort::Tcp(50051);

#[derive(Debug, Clone)]
pub struct UmaDb {
    env_vars: Vec<(&'static str, &'static str)>,
    mounts: Vec<Mount>,
}

impl UmaDb {
    pub fn new(data_dir: Option<String>) -> Self {
        let mount = match data_dir {
            Some(path) => Mount::bind_mount(path, "/data"),
            None => Mount::volume_mount("", "/data"),
        };
        Self {
            env_vars: vec![
                ("UMADB_READ_METHOD", "fileio"),
                // UmaDB's own author-set default private page cache (~half the 4 GB container
                // budget). Left as-is so UmaDB runs as its author configures it; recorded in
                // the manifest so the memory posture is stated next to every figure.
                ("UMADB_PAGE_CACHE_MAX_MB", "2000"),
            ],
            mounts: vec![mount],
        }
    }

    /// The store-config posture recorded in each run's manifest.
    pub fn describe() -> serde_json::Value {
        serde_json::json!({
            "image": format!("{NAME}:{TAG}"),
            "read_method": "fileio",
            "page_cache_max_mb": 2000,
        })
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
        &[UMADB_PORT]
    }
}
