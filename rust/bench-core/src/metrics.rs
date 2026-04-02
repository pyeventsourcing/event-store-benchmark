use std::fs;
use std::path::Path;
use anyhow::Result;
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

/// Throughput time-series sample: elapsed time from workload start and cumulative operation count
#[derive(Debug, Clone, Serialize)]
pub struct LatencyPercentile {
    pub percentile: f64,
    pub latency_us: u64,
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

#[derive(Debug, Clone)]
pub struct WorkloadResults {
    pub workload_config: serde_json::Value,
    pub store_name: String,
    pub throughput_samples: Vec<ThroughputSample>,
    pub latency_histogram: LatencyRecorder,
}

impl WorkloadResults {
    pub(crate) fn print_summary(&self) {
        if self.throughput_samples.len() >= 2 {
            let first_sample = self.throughput_samples.first().unwrap();
            let last_sample = self.throughput_samples.last().unwrap();
            let duration = last_sample.elapsed_s - first_sample.elapsed_s;
            let count_delta = last_sample.count - first_sample.count;
            let throughput = (count_delta as f64) / duration.max(0.001);
            println!("Throughput: {:.2} eps", throughput);
        }
    }
}

impl WorkloadResults {
    pub fn new(
        workload_config: serde_json::Value,
        store_name: String,
        throughput_samples: Vec<ThroughputSample>,
        latency_histogram: LatencyRecorder,
    ) -> Self {
        Self {
            workload_config,
            store_name,
            throughput_samples,
            latency_histogram,
        }
    }

    pub fn write_to_dir(&self, path: &Path) -> Result<()> {
        fs::write(
            path.join("config.json"),
            serde_json::to_string_pretty(&self.workload_config)?,
        )?;

        fs::write(
            path.join("throughput.json"),
            serde_json::to_string_pretty(&self.throughput_samples)?,
        )?;

        fs::write(
            path.join("latency.json"),
            serde_json::to_string_pretty(&self.latency_histogram.to_percentiles())?,
        )?;

        Ok(())
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

    /// Export histogram percentiles
    pub fn to_percentiles(&self) -> Vec<LatencyPercentile> {
        let mut percentiles = Vec::new();

        // Sample key percentiles with fine granularity in the tail
        for p in 0..100 {
            let quantile = p as f64 / 100.0;
            let latency_us = self.hist.value_at_quantile(quantile);
            percentiles.push(LatencyPercentile{
                percentile: p as f64,
                latency_us
            });
        }

        // Add fine-grained tail percentiles
        for p in [99.0, 99.5, 99.9, 99.99, 99.999] {
            let quantile = p / 100.0;
            let latency_us = self.hist.value_at_quantile(quantile);
            percentiles.push(LatencyPercentile{
                percentile: p,
                latency_us: latency_us
            });
        }
        percentiles
    }
}

// New metadata structures for session-based results

#[derive(Debug, Clone, Serialize)]
pub struct SessionMetadata {
    pub session_id: String,
    pub benchmark_version: String,
    pub config_file: String,
    pub workload_type: String,
    pub seed: u64,
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
    pub min_us: f64,
    pub max_us: f64,
    pub avg_us: f64,
    pub p95_us: f64,
    pub p99_us: f64,
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
