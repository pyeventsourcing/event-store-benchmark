use testcontainers::core::{ContainerPort, Mount, WaitFor};
use testcontainers::Image;

const NAME: &str = "thenativeweb/eventsourcingdb";
const TAG: &str = "1.2.0";

/// Container port exposed by EventsourcingDB (HTTP).
pub const EVENTSOURCINGDB_PORT: ContainerPort = ContainerPort::Tcp(3000);

/// Default API token used for the benchmarking container.
pub const EVENTSOURCINGDB_API_TOKEN: &str = "secret";

#[derive(Debug, Clone)]
pub struct EventsourcingDb {
    mounts: Vec<Mount>,
}

impl EventsourcingDb {
    pub fn new(data_dir: Option<String>) -> Self {
        let mount = match data_dir {
            Some(path) => Mount::bind_mount(path, "/var/lib/esdb"),
            None => Mount::volume_mount("", "/var/lib/esdb"),
        };
        Self {
            mounts: vec![mount],
        }
    }
}

impl Default for EventsourcingDb {
    fn default() -> Self {
        Self::new(None)
    }
}

impl Image for EventsourcingDb {
    fn name(&self) -> &str {
        NAME
    }
    fn tag(&self) -> &str {
        TAG
    }
    fn ready_conditions(&self) -> Vec<WaitFor> {
        vec![]
    }
    fn cmd(&self) -> impl IntoIterator<Item = impl Into<std::borrow::Cow<'_, str>>> {
        vec![
            "run",
            "--data-directory-temporary",
            "--https-enabled=false",
            "--http-enabled",
            "--api-token",
            EVENTSOURCINGDB_API_TOKEN,
        ]
    }
    fn mounts(&self) -> impl IntoIterator<Item = &Mount> {
        self.mounts.iter()
    }
    fn expose_ports(&self) -> &[ContainerPort] {
        &[EVENTSOURCINGDB_PORT]
    }
}
