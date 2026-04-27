use crate::adapter::{EventData, ReadRequest, StoreManager};
use crate::common::{SetupConfig};
use crate::metrics::{LatencyRecorder, PerformanceWorkloadResults, ThroughputRecorder, ThroughputSample, RecordingStatus, SamplingConfigDecision};
use anyhow::Result;
use futures::stream::{FuturesUnordered, StreamExt};
use rand::{rngs::StdRng, RngExt, SeedableRng};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::{Barrier, watch};
use std::time::{Duration, Instant};
use uuid::Uuid;
use tokio::task::{JoinSet};
use tokio::time::sleep;
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
    pub use_docker: bool,
    #[serde(default)]
    pub docker_memory_limit_mb: Option<u64>,
    #[serde(default)]
    pub docker_platform: Option<String>,
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
                    new_config.use_docker = self.use_docker;
                    new_config.docker_memory_limit_mb = self.docker_memory_limit_mb;
                    new_config.docker_platform = self.docker_platform.clone();
                    // Add sweep suffix to name
                    let mut name = store.clone();
                    if readers > 0 {
                        name.push_str(&format!("-r{}", readers));
                    }
                    if writers > 0 {
                        name.push_str(&format!("-w{}", writers));
                    }
                    new_config.name = name;
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
    Writeflood,
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
    #[serde(default)]
    pub concurrency_control: bool,
    #[serde(default = "default_in_flight_limit")]
    pub in_flight_limit: usize,
}

fn default_in_flight_limit() -> usize {
    2000
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
            PerformanceMode::Writeflood => {
                if config.concurrency.writers.first() == 0 {
                    return Err(anyhow::anyhow!(
                        "Writeflood mode requires writers > 0 in concurrency config"
                    ));
                }
                if config.concurrency.readers.len() != 1 {
                    return Err(anyhow::anyhow!(
                        "Writeflood mode requires exactly one readers value"
                    ));
                }
                if config.operations.write.in_flight_limit == 0 {
                    return Err(anyhow::anyhow!(
                        "Writeflood mode requires write.in_flight_limit > 0"
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

    /// Execute the workload
    pub async fn execute(
        &self,
        store: &dyn StoreManager,
        cancel_token: CancellationToken,
        tool_tx: watch::Sender<Option<SamplingConfigDecision>>,
        sampling_config_rx: watch::Receiver<Option<SamplingConfigDecision>>,
    ) -> Result<PerformanceWorkloadResults> {
        // Run preparation (prepopulation) if configured
        self.prepare(store).await?;

        let readers = self.config.concurrency.readers.first();
        let writers = self.config.concurrency.writers.first();
        let total_workers = readers + writers;

        // Barrier for workers to be ready
        let ready_barrier = Arc::new(Barrier::new(total_workers + 1));

        let mut reader_adapters = Vec::new();
        if readers > 0 {
            println!("Creating {} reader clients...", readers);
            for _ in 0..readers {
                reader_adapters.push(store.create_adapter().await?);
            }
        }

        let mut writer_adapters = Vec::new();
        if writers > 0 {
            println!("Creating {} writer clients...", writers);
            for _ in 0..writers {
                writer_adapters.push(store.create_adapter().await?);
            }
        }

        println!("Warmup: {}s, Running for {}s", self.config.warmup_seconds, self.config.duration_seconds);
        let mut worker_tasks = JoinSet::new();
        let duration_seconds = self.config.duration_seconds;
        let samples_per_second = self.config.samples_per_second.max(1);

        // Spawn writer tasks
        for adapter in writer_adapters.into_iter() {
            let activate_metrics = matches!(self.config.mode, PerformanceMode::Write | PerformanceMode::Writeflood);
            match self.config.mode {
                PerformanceMode::Write => Self::spawn_stream_writer_task(
                    &mut worker_tasks,
                    adapter,
                    self.config.operations.write.clone(),
                    cancel_token.clone(),
                    activate_metrics,
                    ready_barrier.clone(),
                    sampling_config_rx.clone(),
                ),
                PerformanceMode::Writeflood => Self::spawn_writer_flood_task(
                    &mut worker_tasks,
                    adapter,
                    self.config.operations.write.clone(),
                    cancel_token.clone(),
                    activate_metrics,
                    ready_barrier.clone(),
                    sampling_config_rx.clone(),
                ),
                PerformanceMode::Read => {}
            }
        }

        // Spawn reader tasks
        let mut prepopulated_streams = self.config.setup.prepopulate_streams;
        if prepopulated_streams == 0 {
            prepopulated_streams = self.config.setup.prepopulate_events;
        }

        for (i, adapter) in reader_adapters.into_iter().enumerate() {
            let activate_metrics = matches!(self.config.mode, PerformanceMode::Read);
            Self::spawn_stream_reader_task(
                &mut worker_tasks,
                adapter,
                self.config.operations.read.clone(),
                self.seed + (i as u64),
                cancel_token.clone(),
                self.stream_prefix.clone(),
                prepopulated_streams,
                activate_metrics,
                ready_barrier.clone(),
                sampling_config_rx.clone(),
            );
        }

        // Wait for all workers to be spawned and ready
        ready_barrier.wait().await;
        println!("All {} worker tasks ready, starting benchmark...", total_workers);

        // Signal benchmark start
        let start_time = Instant::now() + Duration::from_secs(self.config.warmup_seconds);
        let msg = SamplingConfigDecision {
            start_time,
            samples_per_second,
            duration_seconds,
        };
        let _ = tool_tx.send(Some(msg));

        // Collect results
        let mut store_latencies = LatencyRecorder::new_for_store_latencies();
        let mut tool_latencies = LatencyRecorder::new_for_tool_latencies();
        let num_intervals = (duration_seconds * samples_per_second) as usize;
        let mut combined_throughput_counts = vec![0u64; num_intervals];
        let mut combined_error_counts = vec![0u64; num_intervals];

        while let Some(worker_result) = worker_tasks.join_next().await {
            if let Ok(Some((worker_latencies, worker_throughput_counts, worker_error_counts, worker_tool_latencies))) = worker_result {
                store_latencies.hist.add(&worker_latencies.hist).unwrap();
                tool_latencies.hist.add(&worker_tool_latencies.hist).unwrap();
                for (i, count) in worker_throughput_counts.counts.iter().enumerate() {
                    if i < combined_throughput_counts.len() {
                        combined_throughput_counts[i] += count;
                    }
                }
                for (i, count) in worker_error_counts.counts.iter().enumerate() {
                    if i < combined_error_counts.len() {
                        combined_error_counts[i] += count;
                    }
                }
            }
        }

        // Convert combined counts to ThroughputSamples
        let mut throughput_samples = Vec::with_capacity(combined_throughput_counts.len());
        for (i, &count) in combined_throughput_counts.iter().enumerate() {
            throughput_samples.push(ThroughputSample {
                elapsed_s: (i + 1) as f64 / samples_per_second as f64,
                count,
            });
        }

        let mut operation_error_samples = Vec::with_capacity(combined_error_counts.len());
        for (i, &count) in combined_error_counts.iter().enumerate() {
            operation_error_samples.push(ThroughputSample {
                elapsed_s: (i + 1) as f64 / samples_per_second as f64,
                count,
            });
        }

        let store_latency_percentiles = store_latencies.to_percentiles();
        let tool_latency_percentiles = tool_latencies.to_percentiles();

        Ok(PerformanceWorkloadResults::new(
            serde_json::to_value(&self.config)?,
            throughput_samples,
            operation_error_samples,
            store_latency_percentiles,
            tool_latency_percentiles,
        ))
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
                let event_type: Arc<str> = Arc::from("setup");
                let payload: Arc<[u8]> = Arc::from(vec![0u8; event_size]);
                for stream_idx in start_stream..end_stream {
                    let stream_name = format!("{}{}", stream_prefix, stream_idx);
                    let tags: Arc<[Arc<str>]> = Arc::from([Arc::from(stream_name.as_str())]);
                    let mut events = Vec::with_capacity(events_per_stream as usize);
                    for _ in 0..events_per_stream {
                        events.push(EventData {
                            payload: payload.clone(),
                            event_type: event_type.clone(),
                            tags: tags.clone(),
                        });
                    }
                    adapter.append_to_stream(&events, None, None).await?;
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

    fn spawn_stream_writer_task(
        worker_tasks: &mut JoinSet<Option<(LatencyRecorder, ThroughputRecorder, ThroughputRecorder, LatencyRecorder)>>,
        adapter: Arc<dyn EventStoreAdapter>,
        write_cfg: WriteOpConfig,
        cancel_token: CancellationToken,
        activate_metrics: bool,
        ready_barrier: Arc<Barrier>,
        mut sampling_config_rx: watch::Receiver<Option<SamplingConfigDecision>>,
    ) {
        worker_tasks.spawn(async move {
            ready_barrier.wait().await;
            
            loop {
                if sampling_config_rx.borrow().is_some() {
                    break;
                }
                if sampling_config_rx.changed().await.is_err() {
                    return None;
                }
            }
            
            let msg = sampling_config_rx.borrow().unwrap();
            let start_time = msg.start_time;
            let samples_per_second = msg.samples_per_second;
            let duration_seconds = msg.duration_seconds;

            let mut out_of_time = false;

            // Pre-allocate strings outside loop
            let payload: Arc<[u8]> = Arc::from(vec![0u8; write_cfg.event_size_bytes]);

            // Sampling for metrics measurement
            let num_intervals = (duration_seconds * samples_per_second) as usize;
            let mut throughput_recorder = ThroughputRecorder::new(samples_per_second, num_intervals, start_time);
            let mut operation_error_recorder = ThroughputRecorder::new(samples_per_second, num_intervals, start_time);
            let mut store_latencies = LatencyRecorder::new_for_store_latencies();
            let mut tool_latencies = LatencyRecorder::new_for_tool_latencies();

            // Tight loop with minimal overhead
            let mut stream_name = format!("stream-{}-", Uuid::new_v4());
            let mut tags: Arc<[Arc<str>]> = Arc::from([Arc::from(stream_name.as_str())]);
            let stream_len = 10;
            let mut stream_position = 0;
            let mut global_position = 0;

            // Cache event types to avoid allocations in the loop
            let event_type_prefix = "test";
            let mut event_types: Vec<Arc<str>> = Vec::with_capacity(stream_len);
            for i in 0..stream_len {
                event_types.push(Arc::from(format!("{}-{}", event_type_prefix, i).as_str()));
            }

            let mut operation_started: Instant;
            let mut operation_completed: Instant;
            let mut operation_duration: Duration;
            let mut loop_started = Instant::now();

            while !out_of_time && !cancel_token.is_cancelled() {
                let evt = EventData {
                    payload: payload.clone(),
                    event_type: event_types[stream_position].clone(),
                    tags: tags.clone(),
                };

                operation_started = Instant::now();
                let operation_response = adapter.append_to_stream(
                    &[evt],
                    if write_cfg.concurrency_control { Some(stream_position) } else { None },
                    if write_cfg.concurrency_control { Some(global_position) } else { None },
                ).await;
                operation_completed = Instant::now();
                operation_duration = operation_completed - operation_started;
                out_of_time = (start_time + Duration::from_secs(duration_seconds + 1)) < operation_completed;

                match operation_response {
                    Ok(returned_global_position) => {
                        if write_cfg.concurrency_control {
                            global_position = returned_global_position.expect("global sequence value not returned");
                        }
                        if activate_metrics {
                            // Record throughput sample
                            if throughput_recorder.record(operation_completed, 1) == RecordingStatus::During {
                                // Record latency sample
                                store_latencies.record(operation_duration);
                            }
                        }
                        // Increment stream position, maybe reset and change name.
                        stream_position += 1;
                        if stream_position == stream_len {
                            stream_name = format!("stream-{}-", Uuid::new_v4());
                            tags = Arc::from([Arc::from(stream_name.as_str())]);
                            stream_position = 0;
                        }
                    },
                    Err(e) => {
                        if activate_metrics {
                            operation_error_recorder.record(operation_completed, 1);
                        }
                        eprintln!("Operation failed after {} ms: {:#}", operation_duration.as_millis(), e);
                        sleep(Duration::from_secs(1)).await;
                        loop_started = Instant::now();
                        continue
                    }
                }

                tool_latencies.record(loop_started.elapsed() - operation_duration);
                loop_started = Instant::now();
            }

            if activate_metrics {
                Some((store_latencies, throughput_recorder, operation_error_recorder, tool_latencies))
            } else {
                None
            }
        });
    }

    fn spawn_stream_reader_task(
        worker_tasks: &mut JoinSet<Option<(LatencyRecorder, ThroughputRecorder, ThroughputRecorder, LatencyRecorder)>>,
        adapter: Arc<dyn EventStoreAdapter>,
        read_cfg: ReadOpConfig,
        seed: u64,
        cancel_token: CancellationToken,
        stream_prefix: String,
        prepopulated_streams: u64,
        activate_metrics: bool,
        ready_barrier: Arc<Barrier>,
        mut sampling_config_rx: watch::Receiver<Option<SamplingConfigDecision>>,
    ) {
        worker_tasks.spawn(async move {
            ready_barrier.wait().await;

            loop {
                if sampling_config_rx.borrow().is_some() {
                    break;
                }
                if sampling_config_rx.changed().await.is_err() {
                    return None;
                }
            }

            let msg = sampling_config_rx.borrow().unwrap();
            let start_time = msg.start_time;
            let samples_per_second = msg.samples_per_second;
            let duration_seconds = msg.duration_seconds;

            let mut out_of_time = false;
            let mut rng = StdRng::seed_from_u64(seed);

            // Pre-calculate stream names to avoid formatting in the loop
            let stream_names: Vec<String> = (0..prepopulated_streams)
                .map(|i| format!("{}{}", stream_prefix, i))
                .collect();

            // Sampling for metrics measurement
            let num_intervals = (duration_seconds * samples_per_second) as usize;
            let mut throughput_recorder = ThroughputRecorder::new(samples_per_second, num_intervals, start_time);
            let mut operation_error_recorder = ThroughputRecorder::new(samples_per_second, num_intervals, start_time);
            let mut store_latencies = LatencyRecorder::new_for_store_latencies();
            let mut tool_latencies = LatencyRecorder::new_for_tool_latencies();

            let mut operation_started: Instant;
            let mut operation_completed: Instant;
            let mut operation_duration: Duration;
            let mut loop_started = Instant::now();

            while !out_of_time && !cancel_token.is_cancelled() {
                let stream_idx = rng.random_range(0..prepopulated_streams);

                let req = ReadRequest {
                    stream: stream_names[stream_idx as usize].clone(),
                    from_offset: None,
                    limit: Some(read_cfg.limit as u64),
                };

                operation_started = Instant::now();
                let operation_response = adapter.read_stream(req).await;
                operation_completed = Instant::now();
                operation_duration = operation_completed - operation_started;
                out_of_time = (start_time + Duration::from_secs(duration_seconds + 1)) < operation_completed;

                match operation_response {
                    Ok(events) => {
                        if activate_metrics {
                            // Record throughput sample
                            if throughput_recorder.record(operation_completed, events.len() as u64) == RecordingStatus::During {
                                // Record latency sample
                                store_latencies.record(operation_duration);
                            }
                        }
                    }
                    Err(e) => {
                        if activate_metrics {
                            operation_error_recorder.record(operation_completed, 1);
                        }
                        eprintln!("Operation failed after {} ms: {:#}", operation_duration.as_millis(), e);
                        sleep(Duration::from_secs(1)).await;
                        loop_started = Instant::now();
                        continue
                    }
                }

                tool_latencies.record(loop_started.elapsed() - operation_duration);
                loop_started = Instant::now();
            }
            if activate_metrics {
                Some((store_latencies, throughput_recorder, operation_error_recorder, tool_latencies))
            } else {
                None
            }
        });
    }

    fn spawn_writer_flood_task(
        worker_tasks: &mut JoinSet<Option<(LatencyRecorder, ThroughputRecorder, ThroughputRecorder, LatencyRecorder)>>,
        adapter: Arc<dyn EventStoreAdapter>,
        write_cfg: WriteOpConfig,
        cancel_token: CancellationToken,
        activate_metrics: bool,
        ready_barrier: Arc<Barrier>,
        mut sampling_config_rx: watch::Receiver<Option<SamplingConfigDecision>>,
    ) {
        worker_tasks.spawn(async move {
            ready_barrier.wait().await;

            loop {
                if sampling_config_rx.borrow().is_some() {
                    break;
                }
                if sampling_config_rx.changed().await.is_err() {
                    return None;
                }
            }

            let msg = sampling_config_rx.borrow().unwrap();
            let start_time = msg.start_time;
            let samples_per_second = msg.samples_per_second;
            let duration_seconds = msg.duration_seconds;
            let benchmark_end = start_time + Duration::from_secs(duration_seconds + 1);

            let size = write_cfg.event_size_bytes;
            let in_flight_limit = write_cfg.in_flight_limit;
            let payload: Arc<[u8]> = Arc::from(vec![0u8; size]);

            let num_intervals = (duration_seconds * samples_per_second) as usize;
            let mut throughput_recorder = ThroughputRecorder::new(samples_per_second, num_intervals, start_time);
            let mut operation_error_recorder = ThroughputRecorder::new(samples_per_second, num_intervals, start_time);
            let mut store_latencies = LatencyRecorder::new_for_store_latencies();
            let tool_latencies = LatencyRecorder::new_for_tool_latencies();

            let mut stream_name = format!("stream-{}-", Uuid::new_v4());
            let mut tags: Arc<[Arc<str>]> = Arc::from([Arc::from(stream_name.as_str())]);
            let stream_len = 10;
            let mut stream_position = 0;

            let event_type_prefix = "test";
            let mut event_types: Vec<Arc<str>> = Vec::with_capacity(stream_len);
            for i in 0..stream_len {
                event_types.push(Arc::from(format!("{}-{}", event_type_prefix, i).as_str()));
            }

            let mut pending = FuturesUnordered::new();

            let mut handle_completion = |completed_at: Instant, operation_duration: Option<Duration>, is_error: bool| {
                if is_error {
                    if activate_metrics {
                        operation_error_recorder.record(completed_at, 1);
                    }
                    return;
                }
                if let Some(duration) = operation_duration {
                    if activate_metrics {
                        let status = throughput_recorder.record(completed_at, 1);
                        if status == RecordingStatus::During {
                            store_latencies.record(duration);
                        }
                    }
                }
            };

            while !cancel_token.is_cancelled() && Instant::now() < benchmark_end {
                if pending.len() >= in_flight_limit {
                    if let Some((completed_at, operation_duration, is_error)) = pending.next().await {
                        handle_completion(completed_at, operation_duration, is_error);
                    }
                    continue;
                }

                let evt = EventData {
                    payload: payload.clone(),
                    event_type: event_types[stream_position].clone(),
                    tags: tags.clone(),
                };

                let adapter_clone = adapter.clone();
                pending.push(async move {
                    let operation_started = Instant::now();
                    match adapter_clone.append_to_stream(&[evt], None, None).await {
                        Ok(_) => {
                            let completed_at = Instant::now();
                            (completed_at, Some(completed_at - operation_started), false)
                        }
                        Err(e) => {
                            eprintln!("Operation failed: {}", e);
                            sleep(Duration::from_secs(1)).await;
                            (Instant::now(), None, true)
                        }
                    }
                });

                stream_position += 1;
                if stream_position == stream_len {
                    stream_name = format!("stream-{}-", Uuid::new_v4());
                    tags = Arc::from([Arc::from(stream_name.as_str())]);
                    stream_position = 0;
                }

                while let std::task::Poll::Ready(Some((completed_at, operation_duration, is_error))) = futures::poll!(pending.next()) {
                    handle_completion(completed_at, operation_duration, is_error);
                }
            }

            while let Some((completed_at, operation_duration, is_error)) = pending.next().await {
                handle_completion(completed_at, operation_duration, is_error);
            }

            if activate_metrics {
                Some((store_latencies, throughput_recorder, operation_error_recorder, tool_latencies))
            } else {
                None
            }
        });
    }
    
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_writeflood_mode_and_default_in_flight_limit() {
        let yaml = r#"
performance:
  name: test
  mode: writeflood
  duration_seconds: 1
  concurrency:
    writers: 1
    readers: 0
  operations:
    write:
      event_size_bytes: 256
  stores: umadb
"#;

        let cfg: WorkloadConfig = serde_yaml::from_str(yaml).unwrap();
        let perf = cfg.performance.unwrap();

        assert!(matches!(perf.mode, PerformanceMode::Writeflood));
        assert_eq!(perf.operations.write.in_flight_limit, default_in_flight_limit());
    }

    #[test]
    fn validates_writeflood_requires_positive_in_flight_limit() {
        let config = PerformanceConfig {
            name: "writeflood-test".to_string(),
            mode: PerformanceMode::Writeflood,
            warmup_seconds: 0,
            duration_seconds: 1,
            samples_per_second: 1,
            concurrency: ConcurrencyConfig {
                writers: ConcurrencyValue::Single(1),
                readers: ConcurrencyValue::Single(0),
            },
            operations: OperationConfig {
                write: WriteOpConfig {
                    event_size_bytes: 256,
                    concurrency_control: false,
                    in_flight_limit: 0,
                },
                read: ReadOpConfig::default(),
            },
            use_docker: false,
            docker_memory_limit_mb: None,
            docker_platform: None,
            setup: SetupConfig::default(),
            stores: StoreValue::Single("umadb".to_string()),
        };

        let err = PerformanceWorkload::from_config(config, 42).err().unwrap().to_string();
        assert!(err.contains("in_flight_limit"));
    }
}
