use crate::session::{Sample, Session};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Aggregated data for the session index page
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionIndex {
    pub sessions: Vec<SessionSummary>,
    pub total_sessions: usize,
    pub workloads: Vec<String>,
    pub stores: Vec<String>,
}

/// Summary information for a single session
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionSummary {
    pub session_id: String,
    pub workload_name: String,
    pub workload_type: String,
    pub benchmark_version: String,
    pub timestamp: String,
    pub stores_run: Vec<String>,
    pub total_events: u64,
    pub duration_s: f64,
    pub is_sweep: bool,
}

/// Detailed data for a single session
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionDetail {
    pub metadata: SessionMetadataView,
    pub environment: EnvironmentView,
    pub config_yaml: String,
    pub stores: Vec<StoreView>,
    pub comparisons: Vec<ComparisonChart>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionMetadataView {
    pub session_id: String,
    pub workload_name: String,
    pub workload_type: String,
    pub benchmark_version: String,
    pub seed: u64,
    pub is_sweep: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnvironmentView {
    pub os: String,
    pub kernel: String,
    pub cpu_model: String,
    pub cpu_cores: u32,
    pub memory_gb: f64,
    pub disk_type: String,
    pub container_runtime: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoreView {
    pub name: String,
    pub throughput_eps: f64,
    pub latency_p50_ms: f64,
    pub latency_p95_ms: f64,
    pub latency_p99_ms: f64,
    pub latency_p999_ms: f64,
    pub events_written: u64,
    pub events_read: u64,
    pub duration_s: f64,
    pub writers: u32,
    pub readers: u32,
    pub container: ContainerView,
    pub samples_data: SamplesData,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContainerView {
    pub image_size_mb: Option<f64>,
    pub startup_time_s: f64,
    pub avg_cpu_percent: Option<f64>,
    pub peak_cpu_percent: Option<f64>,
    pub avg_memory_mb: Option<f64>,
    pub peak_memory_mb: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SamplesData {
    pub latency_cdf: Vec<CdfPoint>,
    pub throughput_timeseries: Vec<TimePoint>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdfPoint {
    pub latency_ms: f64,
    pub percentile: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimePoint {
    pub time_s: f64,
    pub throughput_eps: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComparisonChart {
    pub chart_type: String,
    pub title: String,
    pub stores: Vec<String>,
    pub data: serde_json::Value,
}

/// Compute session index from multiple sessions
pub fn compute_session_index(sessions: &[Session]) -> SessionIndex {
    let mut workloads = std::collections::HashSet::new();
    let mut stores = std::collections::HashSet::new();

    let session_summaries: Vec<SessionSummary> = sessions
        .iter()
        .map(|session| {
            // Collect unique workloads and stores
            workloads.insert(session.metadata.workload_name.clone());
            for store in &session.metadata.stores_run {
                stores.insert(store.clone());
            }

            // Calculate total events across all stores
            let total_events: u64 = session
                .stores
                .values()
                .map(|s| s.summary.events_written + s.summary.events_read)
                .sum();

            // Get average duration
            let duration_s = session
                .stores
                .values()
                .map(|s| s.summary.duration_s)
                .next()
                .unwrap_or(0.0);

            SessionSummary {
                session_id: session.metadata.session_id.clone(),
                workload_name: session.metadata.workload_name.clone(),
                workload_type: session.metadata.workload_type.clone(),
                benchmark_version: session.metadata.benchmark_version.clone(),
                timestamp: session.metadata.session_id.clone(),
                stores_run: session.metadata.stores_run.clone(),
                total_events,
                duration_s,
                is_sweep: session.metadata.is_sweep,
            }
        })
        .collect();

    let mut workload_list: Vec<String> = workloads.into_iter().collect();
    workload_list.sort();

    let mut store_list: Vec<String> = stores.into_iter().collect();
    store_list.sort();

    SessionIndex {
        total_sessions: session_summaries.len(),
        sessions: session_summaries,
        workloads: workload_list,
        stores: store_list,
    }
}

/// Compute detailed view for a single session
pub fn compute_session_detail(session: &Session) -> SessionDetail {
    let metadata = SessionMetadataView {
        session_id: session.metadata.session_id.clone(),
        workload_name: session.metadata.workload_name.clone(),
        workload_type: session.metadata.workload_type.clone(),
        benchmark_version: session.metadata.benchmark_version.clone(),
        seed: session.metadata.seed,
        is_sweep: session.metadata.is_sweep,
    };

    let environment = EnvironmentView {
        os: session.environment.os.name.clone(),
        kernel: session.environment.os.kernel.clone(),
        cpu_model: session.environment.cpu.model.clone(),
        cpu_cores: session.environment.cpu.cores,
        memory_gb: session.environment.memory.total_bytes as f64 / (1024.0 * 1024.0 * 1024.0),
        disk_type: session.environment.disk.disk_type.clone(),
        container_runtime: format!(
            "{} {}",
            session.environment.container_runtime.runtime_type,
            session.environment.container_runtime.version
        ),
    };

    let mut stores: Vec<StoreView> = session
        .stores
        .iter()
        .map(|(name, data)| {
            let container = ContainerView {
                image_size_mb: data
                    .summary
                    .container
                    .image_size_bytes
                    .map(|b| b as f64 / (1024.0 * 1024.0)),
                startup_time_s: data.summary.container.startup_time_s,
                avg_cpu_percent: data.summary.container.avg_cpu_percent,
                peak_cpu_percent: data.summary.container.peak_cpu_percent,
                avg_memory_mb: data
                    .summary
                    .container
                    .avg_memory_bytes
                    .map(|b| b as f64 / (1024.0 * 1024.0)),
                peak_memory_mb: data
                    .summary
                    .container
                    .peak_memory_bytes
                    .map(|b| b as f64 / (1024.0 * 1024.0)),
            };

            let samples_data = compute_samples_data(&data.samples);

            StoreView {
                name: name.clone(),
                throughput_eps: data.summary.throughput_eps,
                latency_p50_ms: data.summary.latency.p50_ms,
                latency_p95_ms: data.summary.latency.p95_ms,
                latency_p99_ms: data.summary.latency.p99_ms,
                latency_p999_ms: data.summary.latency.p999_ms,
                events_written: data.summary.events_written,
                events_read: data.summary.events_read,
                duration_s: data.summary.duration_s,
                writers: data.summary.writers,
                readers: data.summary.readers,
                container,
                samples_data,
            }
        })
        .collect();

    // Sort stores alphabetically
    stores.sort_by(|a, b| a.name.cmp(&b.name));

    // Generate comparison charts
    let comparisons = generate_comparison_charts(session);

    SessionDetail {
        metadata,
        environment,
        config_yaml: session.config_yaml.clone(),
        stores,
        comparisons,
    }
}

/// Compute latency CDF and throughput timeseries from raw samples
fn compute_samples_data(samples: &[Sample]) -> SamplesData {
    // Filter successful samples
    let mut success_samples: Vec<&Sample> = samples.iter().filter(|s| s.ok).collect();

    // Compute latency CDF
    let mut latency_cdf = Vec::new();
    if !success_samples.is_empty() {
        // Sort by latency
        success_samples.sort_by_key(|s| s.latency_us);

        let total = success_samples.len();
        for (idx, sample) in success_samples.iter().enumerate() {
            let percentile = (idx as f64 / total as f64) * 100.0;
            let latency_ms = sample.latency_us as f64 / 1000.0;
            latency_cdf.push(CdfPoint {
                latency_ms,
                percentile,
            });
        }
    }

    // Compute throughput timeseries (50ms bins)
    let throughput_timeseries = compute_throughput_timeseries(samples, 50);

    SamplesData {
        latency_cdf,
        throughput_timeseries,
    }
}

/// Compute throughput over time using time bins
fn compute_throughput_timeseries(samples: &[Sample], bin_size_ms: u64) -> Vec<TimePoint> {
    if samples.is_empty() {
        return Vec::new();
    }

    let success_samples: Vec<&Sample> = samples.iter().filter(|s| s.ok).collect();
    if success_samples.is_empty() {
        return Vec::new();
    }

    let min_time = success_samples.iter().map(|s| s.t_ms).min().unwrap();
    let max_time = success_samples.iter().map(|s| s.t_ms).max().unwrap();
    let duration_ms = max_time - min_time;

    if duration_ms == 0 {
        return Vec::new();
    }

    let num_bins = (duration_ms / bin_size_ms) as usize;
    if num_bins == 0 {
        return Vec::new();
    }

    // Count samples per bin
    let mut bins = vec![0u64; num_bins];
    for sample in &success_samples {
        let bin_idx = ((sample.t_ms - min_time) / bin_size_ms) as usize;
        if bin_idx < num_bins {
            bins[bin_idx] += 1;
        }
    }

    // Convert to events per second
    bins.into_iter()
        .enumerate()
        .map(|(idx, count)| {
            let time_s = (idx as f64 + 0.5) * (bin_size_ms as f64 / 1000.0);
            let throughput_eps = count as f64 * (1000.0 / bin_size_ms as f64);
            TimePoint {
                time_s,
                throughput_eps,
            }
        })
        .collect()
}

/// Generate comparison charts across stores
fn generate_comparison_charts(session: &Session) -> Vec<ComparisonChart> {
    let mut charts = Vec::new();

    // Throughput comparison
    let throughput_data: HashMap<String, f64> = session
        .stores
        .iter()
        .map(|(name, data)| (name.clone(), data.summary.throughput_eps))
        .collect();

    charts.push(ComparisonChart {
        chart_type: "bar".to_string(),
        title: "Throughput Comparison (events/sec)".to_string(),
        stores: session.metadata.stores_run.clone(),
        data: serde_json::to_value(&throughput_data).unwrap(),
    });

    // Latency comparison
    let latency_data: HashMap<String, HashMap<String, f64>> = session
        .stores
        .iter()
        .map(|(name, data)| {
            let mut percentiles = HashMap::new();
            percentiles.insert("p50".to_string(), data.summary.latency.p50_ms);
            percentiles.insert("p95".to_string(), data.summary.latency.p95_ms);
            percentiles.insert("p99".to_string(), data.summary.latency.p99_ms);
            (name.clone(), percentiles)
        })
        .collect();

    charts.push(ComparisonChart {
        chart_type: "latency".to_string(),
        title: "Latency Percentiles (ms)".to_string(),
        stores: session.metadata.stores_run.clone(),
        data: serde_json::to_value(&latency_data).unwrap(),
    });

    charts
}
