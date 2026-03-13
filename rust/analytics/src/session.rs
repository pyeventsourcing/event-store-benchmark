use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// Session metadata from session.json
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionMetadata {
    pub session_id: String,
    pub benchmark_version: String,
    pub workload_name: String,
    pub workload_type: String,
    pub config_file: String,
    pub seed: u64,
    pub stores_run: Vec<String>,
    pub is_sweep: bool,
}

/// Environment information from environment.json
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnvironmentInfo {
    pub os: OsInfo,
    pub cpu: CpuInfo,
    pub memory: MemoryInfo,
    pub disk: DiskInfo,
    pub container_runtime: ContainerRuntimeInfo,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OsInfo {
    pub name: String,
    pub version: String,
    pub kernel: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CpuInfo {
    pub model: String,
    pub cores: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub threads: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryInfo {
    pub total_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiskInfo {
    #[serde(rename = "type")]
    pub disk_type: String,
    pub filesystem: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContainerRuntimeInfo {
    #[serde(rename = "type")]
    pub runtime_type: String,
    pub version: String,
}

/// Store-level summary from {store}/summary.json
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoreSummary {
    pub workload: String,
    pub adapter: String,
    pub writers: u32,
    pub readers: u32,
    pub events_written: u64,
    pub events_read: u64,
    pub duration_s: f64,
    pub throughput_eps: f64,
    pub latency: LatencyMetrics,
    pub container: ContainerMetrics,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatencyMetrics {
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
    pub p999_ms: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContainerMetrics {
    pub image_size_bytes: Option<u64>,
    pub startup_time_s: f64,
    pub avg_cpu_percent: Option<f64>,
    pub peak_cpu_percent: Option<f64>,
    pub avg_memory_bytes: Option<u64>,
    pub peak_memory_bytes: Option<u64>,
}

/// Individual sample from samples.jsonl
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Sample {
    pub t_ms: u64,
    pub op: String,
    pub latency_us: u64,
    pub ok: bool,
}

/// Complete session data
#[derive(Debug, Clone)]
pub struct Session {
    pub path: PathBuf,
    pub metadata: SessionMetadata,
    pub environment: EnvironmentInfo,
    pub config_yaml: String,
    pub stores: HashMap<String, StoreData>,
}

/// Data for a single store within a session
#[derive(Debug, Clone)]
pub struct StoreData {
    pub summary: StoreSummary,
    pub samples: Vec<Sample>,
}

impl Session {
    /// Load a session from a directory
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();

        // Load session metadata
        let session_path = path.join("session.json");
        let metadata: SessionMetadata = serde_json::from_reader(
            std::fs::File::open(&session_path)
                .with_context(|| format!("Failed to open {}", session_path.display()))?,
        )
        .with_context(|| format!("Failed to parse {}", session_path.display()))?;

        // Load environment info
        let env_path = path.join("environment.json");
        let environment: EnvironmentInfo = serde_json::from_reader(
            std::fs::File::open(&env_path)
                .with_context(|| format!("Failed to open {}", env_path.display()))?,
        )
        .with_context(|| format!("Failed to parse {}", env_path.display()))?;

        // Load config YAML
        let config_path = path.join("config.yaml");
        let config_yaml = std::fs::read_to_string(&config_path)
            .with_context(|| format!("Failed to read {}", config_path.display()))?;

        // Load workload data
        let workload_dir = path.join(&metadata.workload_name);
        let mut stores = HashMap::new();

        if workload_dir.exists() && workload_dir.is_dir() {
            for entry in std::fs::read_dir(&workload_dir)
                .with_context(|| format!("Failed to read {}", workload_dir.display()))?
            {
                let entry = entry?;
                let store_path = entry.path();

                if !store_path.is_dir() {
                    continue;
                }

                let store_name = store_path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or_default()
                    .to_string();

                // Load summary
                let summary_path = store_path.join("summary.json");
                if !summary_path.exists() {
                    continue;
                }

                let summary: StoreSummary = serde_json::from_reader(
                    std::fs::File::open(&summary_path)
                        .with_context(|| format!("Failed to open {}", summary_path.display()))?,
                )
                .with_context(|| format!("Failed to parse {}", summary_path.display()))?;

                // Load samples
                let samples_path = store_path.join("samples.jsonl");
                let mut samples = Vec::new();

                if samples_path.exists() {
                    let file = std::fs::File::open(&samples_path)
                        .with_context(|| format!("Failed to open {}", samples_path.display()))?;
                    let reader = std::io::BufReader::new(file);

                    use std::io::BufRead;
                    for line in reader.lines() {
                        let line = line?;
                        if line.trim().is_empty() {
                            continue;
                        }
                        let sample: Sample = serde_json::from_str(&line).with_context(|| {
                            format!("Failed to parse sample line in {}", samples_path.display())
                        })?;
                        samples.push(sample);
                    }
                }

                stores.insert(store_name, StoreData { summary, samples });
            }
        }

        Ok(Session {
            path: path.to_path_buf(),
            metadata,
            environment,
            config_yaml,
            stores,
        })
    }
}
