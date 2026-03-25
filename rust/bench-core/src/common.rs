use serde::{Deserialize, Serialize};

/// Setup/prepopulation configuration for workloads that need data seeding
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SetupConfig {
    /// Number of events to prepopulate during setup phase
    pub prepopulate_events: u64,
    /// Number of streams to distribute prepopulated events across
    #[serde(default)]
    pub prepopulate_streams: Option<u64>,
}
