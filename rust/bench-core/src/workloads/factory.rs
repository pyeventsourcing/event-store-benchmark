use anyhow::Result;
use tokio_util::sync::CancellationToken;

use crate::adapter::StoreManager;
use crate::metrics::{LatencyPercentile, ThroughputSample, WorkloadResults};
use super::performance::{PerformanceWorkload, PerformanceConfig};
use super::durability::DurabilityWorkload;
use super::consistency::ConsistencyWorkload;
use super::operational::OperationalWorkload;

/// Represents a workload that can be executed
pub enum Workload {
    Performance(PerformanceWorkload),
    Durability(DurabilityWorkload),
    Consistency(ConsistencyWorkload),
    Operational(OperationalWorkload),
}

impl Workload {
    pub fn type_str(&self) -> Result<&str> {
        match self {
            Workload::Performance(_) => Ok("performance"),
            Workload::Durability(_) => Ok("durability"),
            Workload::Consistency(_) => Ok("consistency"),
            Workload::Operational(_) => Ok("operational"),
        }
    }

    pub fn store_name(&self) -> Result<String> {
        match self {
            Workload::Performance(w) => Ok(w.store_name()),
            _ => anyhow::bail!("workload store name not defined"),
        }
    }

    pub fn name(&self) -> Result<&str> {
        match self {
            Workload::Performance(w) => Ok(w.name()),
            Workload::Durability(w) => {
                anyhow::bail!("Durability workloads not yet implemented: {}", w.name());
            }
            Workload::Consistency(w) => {
                anyhow::bail!("Consistency workloads not yet implemented: {}", w.name());
            }
            Workload::Operational(w) => {
                anyhow::bail!("Operational workloads not yet implemented: {}", w.name());
            }
        }
    }

    pub async fn execute(
        &self,
        store: &dyn StoreManager,
        cancel_token: CancellationToken,
    ) -> Result<(WorkloadResults, Vec<ThroughputSample>, Vec<LatencyPercentile>)> {
        match self {
            Workload::Performance(w) => w.execute(store, cancel_token).await,
            Workload::Durability(w) => {
                anyhow::bail!("Durability workloads not yet implemented: {}", w.name());
            }
            Workload::Consistency(w) => {
                anyhow::bail!("Consistency workloads not yet implemented: {}", w.name());
            }
            Workload::Operational(w) => {
                anyhow::bail!("Operational workloads not yet implemented: {}", w.name());
            }
        }
    }

    pub fn performance_config(&self) -> Result<PerformanceConfig> {
        match self {
            Workload::Performance(w) => {
                Ok(w.config.clone())
            }
            _ => anyhow::bail!("Not a performance workload"),
        }
    }
}
