use anyhow::Result;
use serde::{Deserialize, Serialize};

/// Operational workload configuration (stub for future implementation)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationalConfig {
    pub name: String,
    pub mode: String,
}

/// Operational workload - tests operational characteristics
///
/// This is a stub implementation. Future modes might include:
/// - startup: Measure container/process startup time
/// - shutdown: Measure graceful shutdown time
/// - backup: Test backup/snapshot performance
/// - restore: Test restore from backup performance
/// - storage_growth: Measure storage amplification over time
pub struct OperationalWorkload {
    config: OperationalConfig,
}

impl OperationalWorkload {
    pub fn from_yaml(yaml_config: &str) -> Result<Self> {
        let config: OperationalConfig = serde_yaml::from_str(yaml_config)?;
        Ok(Self { config })
    }

    pub fn name(&self) -> &str {
        &self.config.name
    }
}
