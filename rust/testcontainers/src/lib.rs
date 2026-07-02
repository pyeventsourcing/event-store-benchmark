pub mod axonserver;
pub mod eventsourcingdb;
pub mod fact;
pub mod foundationdb_dcb;
pub mod kurrentdb;
pub mod marten;
pub mod umadb;

pub mod py_eventsourcing;

/// Point DOCKER_HOST at a local socket (Colima or Docker Desktop) when it is
/// unset. Must be called on the main thread before any multi-threaded runtime
/// starts, because std::env::set_var is unsound under concurrent env reads.
pub fn detect_docker_host() {
    if std::env::var_os("DOCKER_HOST").is_some() {
        return;
    }
    let home = match std::env::var_os("HOME") {
        Some(h) => std::path::PathBuf::from(h),
        None => return,
    };
    let candidates = [
        home.join(".colima/default/docker.sock"),
        home.join(".docker/run/docker.sock"),
    ];
    for path in &candidates {
        if path.exists() {
            let _ = std::env::set_var("DOCKER_HOST", format!("unix://{}", path.display()));
            return;
        }
    }
}
