use std::fs;
use std::path::Path;
use anyhow::Result;
use hdrhistogram::Histogram;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Throughput time-series sample: elapsed time from workload start and cumulative operation count
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThroughputSample {
    pub elapsed_s: f64,
    pub count: u64,
}

/// Throughput time-series sample: elapsed time from workload start and cumulative operation count
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatencyPercentile {
    pub percentile: f64,
    pub latency_us: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CpuSample {
    pub elapsed_s: f64,
    pub cpu_percent: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemorySample {
    pub elapsed_s: f64,
    pub memory_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ProcessMetrics {
    /// Average CPU usage percentage during run
    pub avg_cpu_percent: Option<f64>,
    /// Peak CPU usage percentage during run
    pub peak_cpu_percent: Option<f64>,
    /// Average memory usage in bytes during run
    pub avg_memory_bytes: Option<u64>,
    /// Peak memory usage in bytes during run
    pub peak_memory_bytes: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ContainerStats {
    /// Time to start the container in seconds
    pub startup_time_s: f64,
    /// Image size in bytes
    pub image_size_bytes: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RunMetrics {
    pub resources: ProcessMetrics,
    pub container: Option<ContainerStats>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkloadResults {
    pub workload_config: serde_json::Value,
    pub store_name: String,
}

impl WorkloadResults {
    pub(crate) fn print_summary(&self, throughput_samples: &[ThroughputSample]) {
        if throughput_samples.len() >= 2 {
            let first_sample = throughput_samples.first().unwrap();
            let last_sample = throughput_samples.last().unwrap();
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
    ) -> Self {
        Self {
            workload_config,
            store_name,
        }
    }

    pub fn write_to_dir(
        &self,
        path: &Path,
        throughput_samples: &[ThroughputSample],
        latency_percentiles: &[LatencyPercentile],
        cpu_samples: &[CpuSample],
        memory_samples: &[MemorySample],
    ) -> Result<()> {
        fs::write(
            path.join("config.yaml"),
            serde_yaml::to_string(&self.workload_config)?,
        )?;

        fs::write(
            path.join("throughput.json"),
            serde_json::to_string_pretty(throughput_samples)?,
        )?;

        fs::write(
            path.join("latency.json"),
            serde_json::to_string_pretty(latency_percentiles)?,
        )?;

        fs::write(
            path.join("cpu.json"),
            serde_json::to_string_pretty(cpu_samples)?,
        )?;

        fs::write(
            path.join("memory.json"),
            serde_json::to_string_pretty(memory_samples)?,
        )?;

        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct LatencyRecorder {
    pub hist: Histogram<u64>,
}

#[derive(Clone, Debug)]
pub struct ThroughputRecorder {
    pub counts: Vec<u64>,
    pub samples_per_second: u64,
    pub start_time: Instant,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum RecordingStatus {
    Before,
    During,
    After,
}

impl ThroughputRecorder {
    pub fn new(samples_per_second: u64, num_intervals: usize, start_time: Instant) -> Self {
        Self {
            counts: vec![0; num_intervals + 1],
            samples_per_second,
            start_time,
        }
    }

    pub fn record(&mut self, now: Instant, count: u64) -> RecordingStatus {
        if now < self.start_time {
            return RecordingStatus::Before;
        }
        let elapsed = now.duration_since(self.start_time).as_secs_f64();
        let interval = (elapsed * self.samples_per_second as f64) as usize;
        if interval < self.counts.len() {
            self.counts[interval] += count;
            RecordingStatus::During
        } else {
            RecordingStatus::After
        }
    }

    pub fn to_samples(&self) -> Vec<ThroughputSample> {
        let mut samples = Vec::with_capacity(self.counts.len());
        let mut cumulative_count = 0;
        for (i, &count) in self.counts.iter().enumerate() {
            cumulative_count += count;
            samples.push(ThroughputSample {
                elapsed_s: i as f64 / self.samples_per_second as f64,
                count: cumulative_count,
            });
        }
        samples
    }
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
