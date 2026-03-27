use hdrhistogram::Histogram;
use serde::Serialize;
use std::collections::HashMap;
use std::time::{Duration};

/// Throughput time-series sample: elapsed time from workload start and cumulative operation count
#[derive(Debug, Clone, Serialize)]
pub struct ThroughputSample {
    pub elapsed_s: f64,
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
    pub duration_s: f64,
    pub throughput_eps: f64,
    pub latency: LatencyStats,
    #[serde(default)]
    pub container: ContainerMetrics,
}

#[derive(Debug, Clone, Serialize)]
pub struct RunResults {
    pub summary: Summary,
    pub throughput_samples: Vec<ThroughputSample>,
    #[serde(skip)]  // Don't serialize histogram to JSON
    pub latency_histogram: LatencyRecorder,
}

#[derive(Debug, Clone)]
pub struct WorkloadResults {
    pub workload_name: String,
    pub store_name: String,
    pub writers: usize,
    pub readers: usize,
    pub throughput_samples: Vec<ThroughputSample>,
    pub latency_histogram: LatencyRecorder,
}

impl WorkloadResults {
    pub fn new(
        workload_name: String,
        store_name: String,
        writers: usize,
        readers: usize,
        throughput_samples: Vec<ThroughputSample>,
        latency_histogram: LatencyRecorder,
    ) -> Self {
        Self {
            workload_name,
            store_name,
            writers,
            readers,
            throughput_samples,
            latency_histogram,
        }
    }
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

    /// Export histogram percentile data as JSON for analysis
    pub fn to_percentile_json(&self) -> serde_json::Value {
        let mut percentiles = Vec::new();

        // Sample key percentiles with fine granularity in the tail
        for p in 0..100 {
            let quantile = p as f64 / 100.0;
            let latency_us = self.hist.value_at_quantile(quantile);
            percentiles.push(serde_json::json!({
                "percentile": p as f64,
                "latency_us": latency_us
            }));
        }

        // Add fine-grained tail percentiles
        for p in [99.0, 99.5, 99.9, 99.99, 99.999] {
            let quantile = p / 100.0;
            let latency_us = self.hist.value_at_quantile(quantile);
            percentiles.push(serde_json::json!({
                "percentile": p,
                "latency_us": latency_us
            }));
        }

        serde_json::json!({ "percentiles": percentiles })
    }
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
    pub threads: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct MemoryInfo {
    pub total_bytes: u64,
    pub available_bytes: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct FsyncStats {
    pub min_ms: f64,
    pub max_ms: f64,
    pub avg_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct DiskInfo {
    #[serde(rename = "type")]
    pub disk_type: String,
    pub filesystem: String,
    pub fsync_latency: Option<FsyncStats>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ContainerRuntimeInfo {
    #[serde(rename = "type")]
    pub runtime_type: String,
    pub version: String,
    pub ncpu: usize,
    pub mem_total: u64,
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
