use testcontainers::core::{ContainerPort, Mount, WaitFor};
use testcontainers::Image;
use std::borrow::Cow;

const NAME: &str = "postgres";
const TAG: &str = "16-alpine";

/// Container port exposed by Postgres.
pub const POSTGRES_PORT: ContainerPort = ContainerPort::Tcp(5432);

#[derive(Debug, Clone)]
pub struct PyEventsourcingPostgres {
    env_vars: Vec<(&'static str, &'static str)>,
    mounts: Vec<Mount>,
}

impl PyEventsourcingPostgres {
    pub fn new(data_dir: Option<String>) -> Self {
        let mount = match data_dir {
            Some(path) => Mount::bind_mount(path, "/var/lib/postgresql/data"),
            None => Mount::volume_mount("", "/var/lib/postgresql/data"),
        };
        Self {
            env_vars: vec![
                ("POSTGRES_DB", "postgres"),
                ("POSTGRES_USER", "postgres"),
                ("POSTGRES_PASSWORD", "postgres"),
            ],
            mounts: vec![mount],
        }
    }
}

impl Default for PyEventsourcingPostgres {
    fn default() -> Self {
        Self::new(None)
    }
}

impl Image for PyEventsourcingPostgres {
    fn name(&self) -> &str {
        NAME
    }

    fn tag(&self) -> &str {
        TAG
    }

    fn ready_conditions(&self) -> Vec<WaitFor> {
        vec![WaitFor::message_on_stderr("database system is ready to accept connections")]
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
