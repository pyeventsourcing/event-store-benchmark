use std::fs;
use std::path::Path;
use anyhow::Result;
use hdrhistogram::Histogram;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Sampling configuration message
#[derive(Debug, Clone, Copy)]
pub struct SamplingConfigDecision {
    pub start_time: Instant,
    pub samples_per_second: u64,
    pub duration_seconds: u64,
}

/// Throughput time-series sample: elapsed time from workload start and interval operation count
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThroughputSample {
    pub elapsed_s: f64,
    pub count: u64,
}

/// Throughput time-series sample: elapsed time from workload start and cumulative operation count
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatencyPercentile {
    pub percentile: f64,
    pub latency_ns: u64,
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
pub struct ContainerStats {
    /// Time to start the container in seconds
    pub startup_time_s: f64,
    /// Image size in bytes
    pub image_size_bytes: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceWorkloadResults {
    pub workload_config: serde_json::Value,
    pub throughput_samples: Vec<ThroughputSample>,
    pub operation_error_samples: Vec<ThroughputSample>,
    pub store_latency_percentiles: Vec<LatencyPercentile>,
    pub tool_latency_percentiles: Vec<LatencyPercentile>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WorkloadResults {
    Performance(PerformanceWorkloadResults),
    Durability,
    Consistency,
    Operational,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunResults {
    pub container_stats: Option<ContainerStats>,
    pub workload_results: WorkloadResults,
    pub cpu_samples: Option<Vec<CpuSample>>,
    pub memory_samples: Option<Vec<MemorySample>>,
    pub tool_cpu_samples: Option<Vec<CpuSample>>,
    pub tool_memory_samples: Option<Vec<MemorySample>>,
    pub server_logs: String,
}

impl WorkloadResults {
    pub(crate) fn print_summary(&self) {
        if let WorkloadResults::Performance(results) = self {
            if !results.throughput_samples.is_empty() {
                let total_count: u64 = results.throughput_samples.iter().map(|s| s.count).sum();
                let last_sample = results.throughput_samples.last().unwrap();
                let duration = last_sample.elapsed_s;
                let throughput = (total_count as f64) / duration.max(0.001);
                println!("Throughput: {:.2} eps", throughput);
            }
            if !results.operation_error_samples.is_empty() {
                let total_errors: u64 = results.operation_error_samples.iter().map(|s| s.count).sum();
                println!("Operation errors: {}", total_errors);
            }
        }
    }
}

impl PerformanceWorkloadResults {
    pub fn new(
        workload_config: serde_json::Value,
        throughput_samples: Vec<ThroughputSample>,
        operation_error_samples: Vec<ThroughputSample>,
        store_latency_percentiles: Vec<LatencyPercentile>,
        tool_latency_percentiles: Vec<LatencyPercentile>,
    ) -> Self {
        Self {
            workload_config,
            throughput_samples,
            operation_error_samples,
            store_latency_percentiles,
            tool_latency_percentiles: tool_latency_percentiles,
        }
    }
}

impl WorkloadResults {
    pub fn write_to_dir(
        &self,
        path: &Path,
    ) -> Result<()> {
        match self {
            WorkloadResults::Performance(results) => {
                fs::write(
                    path.join("config.yaml"),
                    serde_yaml::to_string(&results.workload_config)?,
                )?;

                fs::write(
                    path.join("throughput.json"),
                    serde_json::to_string_pretty(&results.throughput_samples)?,
                )?;

                fs::write(
                    path.join("operation_errors.json"),
                    serde_json::to_string_pretty(&results.operation_error_samples)?,
                )?;

                fs::write(
                    path.join("latency.json"),
                    serde_json::to_string_pretty(&results.store_latency_percentiles)?,
                )?;
                fs::write(
                    path.join("tool_latency.json"),
                    serde_json::to_string_pretty(&results.tool_latency_percentiles)?,
                )?;
            }
            WorkloadResults::Durability => {}
            WorkloadResults::Consistency => {}
            WorkloadResults::Operational => {}
        }

        Ok(())
    }
}

impl RunResults {
    pub fn write_to_dir(&self, path: &Path) -> Result<()> {
        self.workload_results.write_to_dir(path)?;

        if let Some(cpu_samples) = &self.cpu_samples {
            fs::write(
                path.join("cpu.json"),
                serde_json::to_string_pretty(cpu_samples)?,
            )?;
        }

        if let Some(memory_samples) = &self.memory_samples {
            fs::write(
                path.join("memory.json"),
                serde_json::to_string_pretty(memory_samples)?,
            )?;
        }

        if let Some(cpu_samples) = &self.tool_cpu_samples {
            fs::write(
                path.join("tool_cpu.json"),
                serde_json::to_string_pretty(cpu_samples)?,
            )?;
        }

        if let Some(memory_samples) = &self.tool_memory_samples {
            fs::write(
                path.join("tool_memory.json"),
                serde_json::to_string_pretty(memory_samples)?,
            )?;
        }

        if let Some(container) = &self.container_stats {
            fs::write(
                path.join("container_stats.json"),
                serde_json::to_string_pretty(container)?,
            )?;
        }

        if !self.server_logs.is_empty() {
            fs::write(path.join("logs.txt"), &self.server_logs)?;
        }

        Ok(())
    }
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
            counts: vec![0; num_intervals],
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
        for (i, &count) in self.counts.iter().enumerate() {
            samples.push(ThroughputSample {
                elapsed_s: (i + 1) as f64 / self.samples_per_second as f64,
                count,
            });
        }
        samples
    }
}

#[derive(Clone, Debug)]
pub struct LatencyRecorder {
    pub hist: Histogram<u64>,
}

impl LatencyRecorder {
    pub fn new_for_store_latencies() -> Self {
        Self {
            hist: Histogram::new_with_bounds(1000, 10_000_000_000, 3).expect("hist"),
        }
    }

    pub fn new_for_tool_latencies() -> Self {
        Self {
            hist: Histogram::new_with_bounds(1, 10_000_000, 3).expect("hist"),
        }
    }

    pub fn record(&mut self, dur: Duration) {
        let ns = dur.as_nanos() as u64;
        self.hist.saturating_record(ns.max(1));
    }

    /// Export histogram percentiles
    pub fn to_percentiles(&self) -> Vec<LatencyPercentile> {
        let mut percentiles = Vec::new();

        // Sample key percentiles with fine granularity in the tail
        for p in 0..100 {
            let quantile = p as f64 / 100.0;
            let latency_ns = self.hist.value_at_quantile(quantile);
            percentiles.push(LatencyPercentile{
                percentile: p as f64,
                latency_ns
            });
        }

        // Add fine-grained tail percentiles
        for p in [99.5, 99.9, 99.99, 99.999] {
            let quantile = p / 100.0;
            let latency_ns = self.hist.value_at_quantile(quantile);
            percentiles.push(LatencyPercentile{
                percentile: p,
                latency_ns
            });
        }
        percentiles
    }
}

// New metadata structures for session-based results

#[derive(Debug, Clone, Serialize)]
pub struct SessionInfo {
    pub session_id: String,
    pub tool_version: String,
    pub workload_name: String,
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
