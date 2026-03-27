use anyhow::Result;
use serde_yaml::Value;
use tokio_util::sync::CancellationToken;

use crate::adapter::StoreManager;
use crate::metrics::WorkloadResults;
use super::performance::{PerformanceWorkload, PerformanceConfig};
use super::durability::DurabilityWorkload;
use super::consistency::ConsistencyWorkload;
use super::operational::OperationalWorkload;

/// The workload types available in the benchmark suite
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkloadType {
    Performance,
    Durability,
    Consistency,
    Operational,
}

/// Represents a workload that can be executed
pub enum Workload {
    Performance(PerformanceWorkload),
    Durability(DurabilityWorkload),
    Consistency(ConsistencyWorkload),
    Operational(OperationalWorkload),
}

impl Workload {
    pub fn name(&self) -> &str {
        match self {
            Workload::Performance(w) => w.name(),
            Workload::Durability(w) => w.name(),
            Workload::Consistency(w) => w.name(),
            Workload::Operational(w) => w.name(),
        }
    }

    pub fn writers(&self) -> usize {
        match self {
            Workload::Performance(w) => w.writers(),
            _ => 0,
        }
    }

    pub fn readers(&self) -> usize {
        match self {
            Workload::Performance(w) => w.readers(),
            _ => 0,
        }
    }

    pub fn duration_seconds(&self) -> u64 {
        match self {
            Workload::Performance(w) => w.duration_seconds(),
            _ => 0,
        }
    }

    pub async fn execute(
        &self,
        store: &dyn StoreManager,
        cancel_token: CancellationToken,
    ) -> Result<WorkloadResults> {
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
}

/// Factory for creating workload instances from YAML configuration
pub struct WorkloadFactory;

impl WorkloadFactory {
    /// Create a workload from YAML configuration
    pub fn create_from_yaml(yaml_config: &str, seed: u64) -> Result<Workload> {
        // Parse just enough to determine workload type
        let value: Value = serde_yaml::from_str(yaml_config)?;

        let workload_type = value
            .get("workload_type")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("Missing 'workload_type' field in config"))?;

        match workload_type {
            "performance" => {
                let workload = PerformanceWorkload::from_yaml(yaml_config, seed)?;
                Ok(Workload::Performance(workload))
            }
            "durability" => {
                let workload = DurabilityWorkload::from_yaml(yaml_config)?;
                Ok(Workload::Durability(workload))
            }
            "consistency" => {
                let workload = ConsistencyWorkload::from_yaml(yaml_config)?;
                Ok(Workload::Consistency(workload))
            }
            "operational" => {
                let workload = OperationalWorkload::from_yaml(yaml_config)?;
                Ok(Workload::Operational(workload))
            }
            _ => Err(anyhow::anyhow!("Unknown workload_type: {}", workload_type)),
        }
    }

    /// Extract the workload name from YAML config
    pub fn extract_workload_name(yaml_config: &str) -> Result<String> {
        let value: Value = serde_yaml::from_str(yaml_config)?;
        value
            .get("name")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .ok_or_else(|| anyhow::anyhow!("Missing 'name' field in config"))
    }

    /// Extract the stores list from YAML config (if specified)
    pub fn extract_stores(yaml_config: &str) -> Result<Option<Vec<String>>> {
        let value: Value = serde_yaml::from_str(yaml_config)?;

        match value.get("stores") {
            None => Ok(None),
            Some(stores_value) => {
                if let Some(stores_array) = stores_value.as_sequence() {
                    let stores: Vec<String> = stores_array
                        .iter()
                        .filter_map(|v| v.as_str().map(|s| s.to_string()))
                        .collect();
                    Ok(Some(stores))
                } else if let Some(store_str) = stores_value.as_str() {
                    Ok(Some(vec![store_str.to_string()]))
                } else {
                    Err(anyhow::anyhow!("'stores' must be a string or array"))
                }
            }
        }
    }

    /// Detect if config represents a sweep (only supports performance workloads)
    pub fn is_sweep(yaml_config: &str) -> Result<bool> {
        let value: Value = serde_yaml::from_str(yaml_config)?;

        let workload_type = value
            .get("workload_type")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("Missing 'workload_type' field in config"))?;

        if workload_type != "performance" {
            return Ok(false);
        }

        let config: PerformanceConfig = serde_yaml::from_str(yaml_config)?;
        Ok(config.is_sweep())
    }

    /// Expand a sweep config into multiple workloads (only supports performance workloads)
    pub fn expand_sweep(yaml_config: &str, seed: u64) -> Result<Vec<Workload>> {
        let value: Value = serde_yaml::from_str(yaml_config)?;

        let workload_type = value
            .get("workload_type")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("Missing 'workload_type' field in config"))?;

        if workload_type != "performance" {
            return Err(anyhow::anyhow!("Sweep expansion only supported for performance workloads"));
        }

        let config: PerformanceConfig = serde_yaml::from_str(yaml_config)?;
        let expanded_configs = config.expand_sweep();

        let mut workloads = Vec::new();
        for expanded_config in expanded_configs {
            let yaml = serde_yaml::to_string(&expanded_config)?;
            let workload = PerformanceWorkload::from_yaml(&yaml, seed)?;
            workloads.push(Workload::Performance(workload));
        }

        Ok(workloads)
    }
}
