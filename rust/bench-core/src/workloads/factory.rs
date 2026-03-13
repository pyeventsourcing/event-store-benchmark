use anyhow::Result;
use serde_yaml::Value;

use super::performance::PerformanceWorkload;
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
}
