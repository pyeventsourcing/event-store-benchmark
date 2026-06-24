use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::Image;

pub const FDB_IMAGE_NAME: &str = "foundationdb/foundationdb";
pub const FDB_IMAGE_TAG: &str = "7.4.5-arm";

pub const FDB_PORT: ContainerPort = ContainerPort::Tcp(4500);

#[derive(Debug, Clone)]
pub struct FoundationDb {
    env_vars: Vec<(&'static str, &'static str)>,
}

impl Default for FoundationDb {
    fn default() -> Self {
        Self {
            // FDB_NETWORKING_MODE=host makes the server advertise 127.0.0.1 as its
            // public address instead of the container IP (unreachable from macOS).
            // This must be paired with a fixed 4500:4500 port mapping so that
            // 127.0.0.1:4500 resolves correctly both inside and outside the container.
            env_vars: vec![("FDB_NETWORKING_MODE", "host")],
        }
    }
}

impl Image for FoundationDb {
    fn name(&self) -> &str {
        FDB_IMAGE_NAME
    }

    fn tag(&self) -> &str {
        FDB_IMAGE_TAG
    }

    fn ready_conditions(&self) -> Vec<WaitFor> {
        vec![WaitFor::message_on_stdout("FDBD joined cluster.")]
    }

    fn env_vars(
        &self,
    ) -> impl IntoIterator<Item = (impl Into<std::borrow::Cow<'_, str>>, impl Into<std::borrow::Cow<'_, str>>)>
    {
        self.env_vars.iter().map(|(k, v)| (*k, *v))
    }

    fn expose_ports(&self) -> &[ContainerPort] {
        &[FDB_PORT]
    }
}
