use testcontainers::core::{ContainerPort, Mount, WaitFor};
use testcontainers::Image;
use std::borrow::Cow;

const NAME: &str = "postgres";
const TAG: &str = "16-alpine";

/// Container port exposed by Postgres.
pub const POSTGRES_PORT: ContainerPort = ContainerPort::Tcp(5432);

#[derive(Debug, Clone)]
pub struct Marten {
    env_vars: Vec<(&'static str, &'static str)>,
    mounts: Vec<Mount>,
}

impl Marten {
    pub fn new(data_dir: Option<String>) -> Self {
        let mount = match data_dir {
            Some(path) => Mount::bind_mount(path, "/var/lib/postgresql/data"),
            None => Mount::volume_mount("", "/var/lib/postgresql/data"),
        };
        Self {
            env_vars: vec![
                ("POSTGRES_DB", "marten"),
                ("POSTGRES_USER", "postgres"),
                ("POSTGRES_PASSWORD", "postgres"),
            ],
            mounts: vec![mount],
        }
    }

    /// The store-config posture recorded in each run's manifest.
    pub fn describe() -> serde_json::Value {
        serde_json::json!({
            "image": format!("{NAME}:{TAG}"),
            "shared_buffers": "1GB",
            "note": "Marten = bare Postgres INSERT path (no reliably-followable sequence)",
        })
    }
}

impl Default for Marten {
    fn default() -> Self {
        Self::new(None)
    }
}

impl Image for Marten {
    fn name(&self) -> &str {
        NAME
    }

    fn tag(&self) -> &str {
        TAG
    }

    fn ready_conditions(&self) -> Vec<WaitFor> {
        vec![WaitFor::message_on_stderr("database system is ready to accept connections")]
    }

    fn cmd(&self) -> impl IntoIterator<Item = impl Into<Cow<'_, str>>> {
        // Pin shared_buffers so Postgres's cache budget is a recorded, comparable value rather
        // than the tiny 128 MB default; the 4 GB container cgroup cap is the outer equalizer.
        ["postgres", "-c", "shared_buffers=1GB", "-c", "max_connections=200"]
    }

    fn env_vars(
        &self,
    ) -> impl IntoIterator<
        Item = (
            impl Into<Cow<'_, str>>,
            impl Into<Cow<'_, str>>,
        ),
    > {
        self.env_vars.iter().map(|(k, v)| (*k, *v))
    }

    fn mounts(&self) -> impl IntoIterator<Item = &Mount> {
        self.mounts.iter()
    }

    fn expose_ports(&self) -> &[ContainerPort] {
        &[POSTGRES_PORT]
    }
}
