use crate::adapter::{EventData, ReadRequest, StoreManager};
use crate::common::{SetupConfig};
use crate::metrics::{LatencyRecorder, ThroughputRecorder, ThroughputSample, WorkloadResults, RecordingStatus};
use anyhow::Result;
use rand::{rngs::StdRng, RngExt, SeedableRng};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, Instant};
use uuid::Uuid;
use tokio::task::{JoinSet};
use tokio_util::sync::CancellationToken;
use crate::EventStoreAdapter;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkloadConfig {
    #[serde(default)]
    pub performance: Option<PerformanceConfig>
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceConfig {
    pub name: String,
    pub mode: PerformanceMode,
    #[serde(default)]
    pub warmup_seconds: u64,
    pub duration_seconds: u64,
    #[serde(default = "default_samples_per_second")]
    pub samples_per_second: u64,
    pub concurrency: ConcurrencyConfig,
    pub operations: OperationConfig,
    #[serde(default)]
    pub local: bool,
    #[serde(default)]
    pub setup: SetupConfig,
    pub stores: StoreValue,
}

pub fn default_samples_per_second() -> u64 {
    1
}

impl PerformanceConfig {
    /// Expand a sweep config into multiple single-value configs
    pub fn expand(&self) -> Vec<Self> {
        let writers_vec = self.concurrency.writers.as_vec();
        let readers_vec = self.concurrency.readers.as_vec();

        let mut configs = Vec::new();
        for &writers in &writers_vec {
            for &readers in &readers_vec {
                for store in self.stores.as_vec() {
                    let mut new_config = self.clone();
                    new_config.concurrency.writers = ConcurrencyValue::Single(writers);
                    new_config.concurrency.readers = ConcurrencyValue::Single(readers);
                    new_config.stores = StoreValue::Single(store.to_string());
                    new_config.local = self.local;
                    // Add sweep suffix to name
                    new_config.name = format!("{}-{}-w{}-r{}", self.name, store, writers, readers);
                    configs.push(new_config);
                }
            }
        }
        configs
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PerformanceMode {
    Write,
    Read,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ConcurrencyValue {
    Single(usize),
    Multiple(Vec<usize>),
}

impl ConcurrencyValue {
    pub fn as_vec(&self) -> Vec<usize> {
        match self {
            ConcurrencyValue::Single(v) => vec![*v],
            ConcurrencyValue::Multiple(v) => {
                if v.len() == 0 {
                    ConcurrencyValue::default().as_vec()
                } else {
                    v.clone()
                }
            },
        }
    }

    pub fn first(&self) -> usize {
        match self {
            ConcurrencyValue::Single(v) => *v,
            ConcurrencyValue::Multiple(v) => v.first().copied().unwrap_or(0),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            ConcurrencyValue::Single(_) => 1,
            ConcurrencyValue::Multiple(v) => v.len(),
        }
    }
}

impl Default for ConcurrencyValue {
    fn default() -> Self {
        ConcurrencyValue::Single(0)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum StoreValue {
    Single(String),
    Multiple(Vec<String>),
}

impl StoreValue {
    pub fn as_vec(&self) -> Vec<String> {
        match self {
            StoreValue::Single(v) => vec![v.clone()],
            StoreValue::Multiple(v) => v.clone(),
        }
    }

    pub fn first(&self) -> String {
        match self {
            StoreValue::Single(v) => v.clone(),
            StoreValue::Multiple(v) => v.first().unwrap().clone(),
        }
    }
}

impl From<String> for StoreValue {
    fn from(s: String) -> Self {
        if s.contains(',') {
            StoreValue::Multiple(s.split(',').map(|s| s.trim().to_string()).collect())
        } else {
            StoreValue::Single(s)
        }
    }
}

impl Default for StoreValue {
    fn default() -> Self {
        StoreValue::Single("default".to_string())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConcurrencyConfig {
    #[serde(default)]
    pub writers: ConcurrencyValue,
    #[serde(default)]
    pub readers: ConcurrencyValue,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct OperationConfig {
    #[serde(default)]
    pub write: WriteOpConfig,
    #[serde(default)]
    pub read: ReadOpConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct WriteOpConfig {
    pub event_size_bytes: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ReadOpConfig {
    #[serde(default = "default_read_limit")]
    pub limit: usize,
}

fn default_read_limit() -> usize {
    1
}

/// Performance workload - generic event store read/write patterns
pub struct PerformanceWorkload {
    pub config: PerformanceConfig,
    seed: u64,
    stream_prefix: String,
}

impl PerformanceWorkload {
    pub fn from_config(config: PerformanceConfig, seed: u64) -> Result<Self> {
        // Validate mode-specific config
        match config.mode {
            PerformanceMode::Write => {
                if config.concurrency.writers.first() == 0 {
                    return Err(anyhow::anyhow!(
                        "Write mode requires writers > 0 in concurrency config"
                    ));
                }
                if config.concurrency.readers.len() != 1 {
                    return Err(anyhow::anyhow!(
                        "Write mode requires exactly one readers value"
                    ));
                }
            }
            PerformanceMode::Read => {
                if config.concurrency.readers.first() == 0 {
                    return Err(anyhow::anyhow!(
                        "Read mode requires readers > 0 in concurrency config"
                    ));
                }
                if config.concurrency.writers.len() != 1 {
                    return Err(anyhow::anyhow!(
                        "Read mode requires exactly one writers value"
                    ));
                }
            }
        }

        let stream_prefix = format!("stream-{}-", Uuid::new_v4());
        Ok(Self { config, seed, stream_prefix })
    }

    pub fn name(&self) -> &str {
        &self.config.name
    }

    pub fn store_name(&self) -> String {
        self.config.stores.first()
    }

    /// Prepare the workload (e.g., prepopulate data for read workloads)
    pub async fn prepare(&self, store: &dyn StoreManager) -> Result<()> {
        let setup_start = Instant::now();

        let total_events = self.config.setup.prepopulate_events;
        let mut num_streams = self.config.setup.prepopulate_streams;
        if num_streams == 0 {
            num_streams = total_events
        }
        println!(
            "Running setup phase: prepopulating {} events in {} streams...",
            total_events, num_streams
        );
        let events_per_stream = (total_events as f64 / num_streams as f64).ceil() as u64;

        // Prepopulate events across streams concurrently
        let mut setup_set = JoinSet::new();
        let concurrency = 1;
        let streams_per_task = (num_streams as f64 / concurrency as f64).ceil() as usize;

        let write_config = self.config.operations.write.clone();
        let event_size = write_config.event_size_bytes;

        for task_idx in 0..concurrency {
            let start_stream = task_idx * streams_per_task;
            let end_stream = (start_stream + streams_per_task).min(num_streams as usize);
            if start_stream >= end_stream {
                continue;
            }

            let adapter = store.create_adapter().await?;

            let stream_prefix = self.stream_prefix.clone();
            setup_set.spawn(async move {
                for stream_idx in start_stream..end_stream {
                    let stream_name = format!("{}{}", stream_prefix, stream_idx);
                    let mut events = Vec::with_capacity(events_per_stream as usize);
                    for _ in 0..events_per_stream {
                        events.push(EventData {
                            payload: vec![0u8; event_size],
                            event_type: "setup".to_string(),
                            tags: vec![stream_name.clone()],
                        });
                    }
                    adapter.append(events).await?;
                }
                Ok::<(), anyhow::Error>(())
            });
        }

        while let Some(res) = setup_set.join_next().await {
            res??;
        }

        let setup_duration = setup_start.elapsed();
        println!(
            "Setup phase completed in {:.2} seconds",
            setup_duration.as_secs_f64()
        );

        Ok(())
    }

    /// Execute the workload
    pub async fn execute(
        &self,
        store: &dyn StoreManager,
        cancel_token: CancellationToken,
    ) -> Result<WorkloadResults> {
        // Run preparation (prepopulation) if configured
        self.prepare(store).await?;

        let mut reader_adapters = Vec::new();
        let readers = self.config.concurrency.readers.first();
        if readers > 0 {
            println!("Creating {} reader clients...", readers);
            for i in 0..readers {
                match store.create_adapter().await {
                    Ok(adapter) => reader_adapters.push(adapter),
                    Err(e) => {
                        eprintln!("Failed to create reader {}: {}", i, e);
                        anyhow::bail!("Failed to create reader {}: {}", i, e);
                    }
                }
            }
            println!("All {} reader clients ready", readers);
        }

        let mut writer_adapters = Vec::new();
        let writers = self.config.concurrency.writers.first();
        if writers > 0 {
            println!("Creating {} writer clients...", writers);
            for i in 0..writers {
                match store.create_adapter().await {
                    Ok(adapter) => writer_adapters.push(adapter),
                    Err(e) => {
                        eprintln!("Failed to create writer {}: {}", i, e);
                        anyhow::bail!("Failed to create writer {}: {}", i, e);
                    }
                }
            }
            println!("All {} writer clients ready", writers);
        }

        println!("Warmup: {}s, Running for {}s", self.config.warmup_seconds, self.config.duration_seconds);
        let mut worker_tasks = JoinSet::new();
        let samples_per_second = self.config.samples_per_second.max(1);
        let num_intervals = (self.config.duration_seconds * samples_per_second) as usize;
        let sampling_starts = Instant::now() + Duration::from_secs(self.config.warmup_seconds);

        // Spawn writer tasks
        for adapter in writer_adapters.into_iter() {
            let activate_metrics = matches!(self.config.mode, PerformanceMode::Write);
            Self::spawn_writer_task(
                &mut worker_tasks,
                adapter,
                self.config.operations.write.clone().clone(),
                cancel_token.clone(),
                activate_metrics,
                samples_per_second,
                num_intervals,
                sampling_starts,
            );
        }

        // Spawn reader tasks
        let mut prepopulated_streams = self.config.setup.prepopulate_streams;
        let prepopulated_events = self.config.setup.prepopulate_events;
        if prepopulated_streams == 0 {
            prepopulated_streams = prepopulated_events
        }

        for (i, adapter) in reader_adapters.into_iter().enumerate() {
            let activate_metrics = matches!(self.config.mode, PerformanceMode::Read);
            Self::spawn_reader_task(
                &mut worker_tasks,
                adapter,
                self.config.operations.read.clone().clone(),
                self.seed + (i as u64),
                cancel_token.clone(),
                self.stream_prefix.clone(),
                prepopulated_streams,
                activate_metrics,
                samples_per_second,
                num_intervals,
                sampling_starts,
            );
        }

        // Collect results
        let mut latency_histogram = LatencyRecorder::new();
        let mut combined_counts = vec![0u64; num_intervals + 1];

        while let Some(worker_result) = worker_tasks.join_next().await {
            if let Ok(Some((worker_latencies, worker_throughput))) = worker_result {
                latency_histogram.hist.add(&worker_latencies.hist).unwrap();
                for (i, count) in worker_throughput.counts.iter().enumerate() {
                    if i < combined_counts.len() {
                        combined_counts[i] += count;
                    }
                }
            }
        }

        // Convert combined counts to ThroughputSamples
        let mut throughput_samples = Vec::with_capacity(combined_counts.len());
        let mut cumulative_count = 0;
        for (i, &count) in combined_counts.iter().enumerate() {
            cumulative_count += count;
            throughput_samples.push(ThroughputSample {
                elapsed_s: i as f64 / samples_per_second as f64,
                count: cumulative_count,
            });
        }

        Ok(WorkloadResults::new(
            serde_json::to_value(&self.config)?,
            store.name().to_string(),
            throughput_samples,
            latency_histogram,
        ))
    }

    fn spawn_writer_task(
        worker_tasks: &mut JoinSet<Option<(LatencyRecorder, ThroughputRecorder)>>,
        adapter: Arc<dyn EventStoreAdapter>,
        write_cfg: WriteOpConfig,
        cancel_token: CancellationToken,
        activate_metrics: bool,
        samples_per_second: u64,
        num_intervals: usize,
        sampling_starts: Instant,
    ) {
        worker_tasks.spawn(async move {
            let mut out_of_time = false;
            let size = write_cfg.event_size_bytes;

            // Pre-allocate strings outside loop
            let event_type = "test".to_string();
            let payload = vec![0u8; size];

            // Sampling for metrics measurement
            let mut throughput_recorder = ThroughputRecorder::new(samples_per_second, num_intervals, sampling_starts);
            let mut latencies = LatencyRecorder::new();

            // Tight loop with minimal overhead
            let mut stream_name = format!("stream-{}-", Uuid::new_v4());
            let stream_len = 10;
            let mut stream_position = 0;
            while !out_of_time && !cancel_token.is_cancelled() {
                let evt = EventData {
                    payload: payload.clone(),
                    event_type: format!("{}-{}", event_type.clone(), stream_position),
                    tags: vec![stream_name.clone()],
                };

                let operation_started = Instant::now();
                let mut success = false;
                for _ in 0..100 {
                    if cancel_token.is_cancelled() {
                        break;
                    }
                    if adapter.append(vec![evt.clone()]).await.is_ok() {
                        success = true;
                        break;
                    }
                    eprintln!("Operation failed after 100 retries");
                }

                if success {
                    if activate_metrics {
                        let operation_completed = Instant::now();
                        // Record throughput sample
                        let status = throughput_recorder.record(operation_completed, 1);
                        if status == RecordingStatus::During {
                            // Record latency sample
                            latencies.record(operation_completed - operation_started);
                        }
                        out_of_time = status == RecordingStatus::After;
                    }
                    // Increment stream position, maybe reset and change name.
                    stream_position += 1;
                    if stream_position == stream_len {
                        stream_name = format!("stream-{}-", Uuid::new_v4());
                        stream_position = 0;
                    }
                }
            }

            if activate_metrics {
                Some((latencies, throughput_recorder))
            } else {
                None
            }
        });
    }

    fn spawn_reader_task(
        worker_tasks: &mut JoinSet<Option<(LatencyRecorder, ThroughputRecorder)>>,
        adapter: Arc<dyn EventStoreAdapter>,
        read_cfg: ReadOpConfig,
        seed: u64,
        cancel_token: CancellationToken,
        stream_prefix: String,
        prepopulated_streams: u64,
        activate_metrics: bool,
        samples_per_second: u64,
        num_intervals: usize,
        sampling_starts: Instant,
    ) {
        worker_tasks.spawn(async move {
            let mut out_of_time = false;
            let mut rng = StdRng::seed_from_u64(seed);

            let mut throughput_recorder = ThroughputRecorder::new(samples_per_second, num_intervals, sampling_starts);
            let mut latencies = LatencyRecorder::new();


            while !out_of_time && !cancel_token.is_cancelled() {
                let stream_idx = rng.random_range(0..prepopulated_streams);

                let req = ReadRequest {
                    stream: format!("{}{}", stream_prefix, stream_idx),
                    from_offset: None,
                    limit: Some(read_cfg.limit as u64),
                };

                let operation_started = Instant::now();
                let result = adapter.read(req).await;
                let operation_completed = Instant::now();

                if activate_metrics {
                    if let Ok(events) = result {
                        // Record throughput sample
                        let status = throughput_recorder.record(operation_completed, events.len() as u64);
                        if status == RecordingStatus::During {
                            // Record latency sample
                            latencies.record(operation_completed - operation_started);
                        }
                        out_of_time = status == RecordingStatus::After;
                    }

                }
            }
            if activate_metrics {
                Some((latencies, throughput_recorder))
            } else {
                None
            }
        });
    }
    
}
