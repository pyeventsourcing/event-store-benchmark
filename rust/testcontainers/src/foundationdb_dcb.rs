use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::Image;

pub const FDB_IMAGE_NAME: &str = "foundationdb/foundationdb";
const FDB_IMAGE_VERSION: &str = "7.4.5";

pub const FDB_PORT: ContainerPort = ContainerPort::Tcp(4500);

pub fn fdb_image_tag() -> String {
    if std::env::consts::ARCH == "aarch64" {
        // Sorry - I didn't want to clone the repo and build the Docker image --JohnB
        // format!("{}-arm", FDB_IMAGE_VERSION)
        FDB_IMAGE_VERSION.to_string()
    } else {
        FDB_IMAGE_VERSION.to_string()
    }
}

#[derive(Debug, Clone)]
pub struct FoundationDb {
    tag: String,
    env_vars: Vec<(&'static str, &'static str)>,
}

impl Default for FoundationDb {
    fn default() -> Self {
        Self {
            tag: fdb_image_tag(),
            // FDB_NETWORKING_MODE=host makes fdbserver advertise 127.0.0.1 as its
            // coordinator address instead of the container IP (unreachable from macOS).
            env_vars: vec![("FDB_NETWORKING_MODE", "host")],
        }
    }
}

impl Image for FoundationDb {
    fn name(&self) -> &str {
        FDB_IMAGE_NAME
    }

    fn tag(&self) -> &str {
        &self.tag
    }

    fn ready_conditions(&self) -> Vec<WaitFor> {
        vec![WaitFor::message_on_stdout("FDBD joined cluster.")]
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

    fn expose_ports(&self) -> &[ContainerPort] {
        &[FDB_PORT]
    }
}
