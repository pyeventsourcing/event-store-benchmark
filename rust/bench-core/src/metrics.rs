use hdrhistogram::Histogram;
use hdrhistogram::serialization::{Serializer, V2Serializer};
use serde::Serialize;
use std::collections::HashMap;
use std::io::Write;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Throughput time-series sample: timestamp and cumulative operation count
#[derive(Debug, Clone, Serialize)]
pub struct ThroughputSample {
    pub t_ms: u128,
    pub count: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct LatencyStats {
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
    pub p999_ms: f64,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct ContainerMetrics {
    /// Container image size in bytes
    pub image_size_bytes: Option<u64>,
    /// Time to start the container in seconds
    pub startup_time_s: f64,
    /// Average CPU usage percentage during run
    pub avg_cpu_percent: Option<f64>,
    /// Peak CPU usage percentage during run
    pub peak_cpu_percent: Option<f64>,
    /// Average memory usage in bytes during run
    pub avg_memory_bytes: Option<u64>,
    /// Peak memory usage in bytes during run
    pub peak_memory_bytes: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Summary {
    pub workload: String,
    pub adapter: String,
    pub writers: usize,
    pub readers: usize,
    pub events_written: u64,
    pub events_read: u64,
    pub duration_s: f64,
    pub throughput_eps: f64,
    pub latency: LatencyStats,
    #[serde(default)]
    pub container: ContainerMetrics,
}

#[derive(Debug, Clone, Serialize)]
pub struct RunMetrics {
    pub summary: Summary,
    pub throughput_samples: Vec<ThroughputSample>,
    #[serde(default = "default_sample_rate")]
    pub sample_rate: u64,
    #[serde(skip)]  // Don't serialize histogram to JSON
    pub latency_histogram: LatencyRecorder,
}

fn default_sample_rate() -> u64 {
    1 // Default to no sampling (every event sampled)
}

#[derive(Clone, Debug)]
pub struct LatencyRecorder {
    pub hist: Histogram<u64>,
}

impl LatencyRecorder {
    pub fn new() -> Self {
        Self {
            hist: Histogram::new(3).expect("hist"),
        } // 3 sigfigs
    }
    pub fn record(&mut self, dur: Duration) {
        let us = dur.as_micros() as u64;
        let _ = self.hist.record(us.max(1));
    }
    pub fn to_stats(&self) -> LatencyStats {
        LatencyStats {
            p50_ms: self.hist.value_at_quantile(0.50) as f64 / 1000.0,
            p95_ms: self.hist.value_at_quantile(0.95) as f64 / 1000.0,
            p99_ms: self.hist.value_at_quantile(0.99) as f64 / 1000.0,
            p999_ms: self.hist.value_at_quantile(0.999) as f64 / 1000.0,
        }
    }

    /// Serialize histogram to a writer using HdrHistogram V2 format
    pub fn serialize_to_writer<W: Write>(&self, writer: &mut W) -> anyhow::Result<()> {
        V2Serializer::new().serialize(&self.hist, writer)?;
        Ok(())
    }

    /// Serialize histogram to a byte vector
    pub fn serialize_to_vec(&self) -> anyhow::Result<Vec<u8>> {
        let mut vec = Vec::new();
        self.serialize_to_writer(&mut vec)?;
        Ok(vec)
    }
}

pub fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
}

// New metadata structures for session-based results

#[derive(Debug, Clone, Serialize)]
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

#[derive(Debug, Clone, Serialize)]
pub struct OsInfo {
    pub name: String,
    pub version: String,
    pub kernel: String,
    pub arch: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct CpuInfo {
    pub model: String,
    pub cores: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct MemoryInfo {
    pub total_bytes: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct DiskInfo {
    #[serde(rename = "type")]
    pub disk_type: String,
    pub filesystem: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ContainerRuntimeInfo {
    #[serde(rename = "type")]
    pub runtime_type: String,
    pub version: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct EnvironmentInfo {
    pub os: OsInfo,
    pub cpu: CpuInfo,
    pub memory: MemoryInfo,
    pub disk: DiskInfo,
    pub container_runtime: ContainerRuntimeInfo,
}

#[derive(Debug, Clone, Serialize)]
pub struct RunManifest {
    pub session_id: String,
    pub workload_name: String,
    pub store: String,
    pub parameters: HashMap<String, serde_json::Value>,
}
