use crate::adapter::StoreManager;
use crate::metrics::{LatencyRecorder, RawSample};
use anyhow::Result;
use async_trait::async_trait;
use std::time::Instant;

/// A workload defines how to execute a specific benchmark scenario
/// against a store managed by a StoreManager.
#[async_trait]
pub trait Workload: Send + Sync {
    /// Execute the workload.
    /// Returns a tuple of (LatencyRecorder, events_written, events_read, samples).
    async fn execute(
        &self,
        store: &dyn StoreManager,
        measurement_start: Instant,
        measurement_end: Instant,
        end_at: Instant,
    ) -> Result<(LatencyRecorder, u64, u64, Vec<RawSample>)>;

    /// Workload name
    fn name(&self) -> String;

    /// Number of writers used in this workload (for reporting)
    fn writers(&self) -> usize;

    /// Number of readers used in this workload (for reporting)
    fn readers(&self) -> usize;

    /// Workload duration in seconds
    fn duration_seconds(&self) -> u64;
}

/// Factory for creating workload instances from YAML configuration
pub trait WorkloadFactory: Send + Sync {
    /// Name of the workload type (used for selection in CLI or automatically detected)
    fn name(&self) -> &'static str;

    /// Create a workload instance from the given YAML configuration
    fn create(&self, yaml_config: &str, seed: u64) -> Result<Box<dyn Workload>>;
}
