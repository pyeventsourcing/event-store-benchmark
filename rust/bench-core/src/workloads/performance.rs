use crate::adapter::{EventData, ReadRequest, StoreManager};
use crate::common::{SetupConfig, StreamsConfig};
use crate::metrics::{LatencyRecorder, ThroughputSample};
use anyhow::Result;
use rand::{rngs::StdRng, Rng, SeedableRng};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use uuid::Uuid;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceConfig {
    pub name: String,
    pub mode: PerformanceMode,
    pub duration_seconds: u64,
    pub concurrency: ConcurrencyConfig,
    pub operations: OperationConfig,
    pub streams: StreamsConfig,
    #[serde(default)]
    pub setup: Option<SetupConfig>,
}

impl PerformanceConfig {
    /// Check if this config represents a sweep (has multiple values)
    pub fn is_sweep(&self) -> bool {
        matches!(self.concurrency.writers, ConcurrencyValue::Multiple(_))
            || matches!(self.concurrency.readers, ConcurrencyValue::Multiple(_))
    }

    /// Expand a sweep config into multiple single-value configs
    pub fn expand_sweep(&self) -> Vec<Self> {
        let writers_vec = self.concurrency.writers.as_vec();
        let readers_vec = self.concurrency.readers.as_vec();

        let mut configs = Vec::new();
        for &writers in &writers_vec {
            for &readers in &readers_vec {
                let mut new_config = self.clone();
                new_config.concurrency.writers = ConcurrencyValue::Single(writers);
                new_config.concurrency.readers = ConcurrencyValue::Single(readers);
                // Add sweep suffix to name
                new_config.name = format!("{}-w{}-r{}", self.name, writers, readers);
                configs.push(new_config);
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
    Mixed,
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
            ConcurrencyValue::Multiple(v) => v.clone(),
        }
    }

    pub fn first(&self) -> usize {
        match self {
            ConcurrencyValue::Single(v) => *v,
            ConcurrencyValue::Multiple(v) => v.first().copied().unwrap_or(0),
        }
    }
}

impl Default for ConcurrencyValue {
    fn default() -> Self {
        ConcurrencyValue::Single(0)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConcurrencyConfig {
    #[serde(default)]
    pub writers: ConcurrencyValue,
    #[serde(default)]
    pub readers: ConcurrencyValue,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationConfig {
    #[serde(default)]
    pub write: Option<WriteOpConfig>,
    #[serde(default)]
    pub read: Option<ReadOpConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteOpConfig {
    pub event_size_bytes: usize,
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    #[serde(default)]
    pub probability: Option<f64>, // For mixed mode
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadOpConfig {
    #[serde(default = "default_read_batch")]
    pub batch_size: usize,
    #[serde(default)]
    pub probability: Option<f64>, // For mixed mode
}

fn default_batch_size() -> usize {
    1
}

fn default_read_batch() -> usize {
    100
}

/// Performance workload - generic event store read/write patterns
pub struct PerformanceWorkload {
    config: PerformanceConfig,
    seed: u64,
    stream_prefix: String,
}

impl PerformanceWorkload {
    pub fn from_yaml(yaml_config: &str, seed: u64) -> Result<Self> {
        let config: PerformanceConfig = serde_yaml::from_str(yaml_config)?;

        // Validate mode-specific config
        match config.mode {
            PerformanceMode::Write => {
                if config.concurrency.writers.first() == 0 {
                    return Err(anyhow::anyhow!(
                        "Write mode requires writers > 0 in concurrency config"
                    ));
                }
                if config.operations.write.is_none() {
                    return Err(anyhow::anyhow!(
                        "Write mode requires 'write' operation config"
                    ));
                }
            }
            PerformanceMode::Read => {
                if config.concurrency.readers.first() == 0 {
                    return Err(anyhow::anyhow!(
                        "Read mode requires readers > 0 in concurrency config"
                    ));
                }
                if config.operations.read.is_none() {
                    return Err(anyhow::anyhow!("Read mode requires 'read' operation config"));
                }
            }
            PerformanceMode::Mixed => {
                if config.concurrency.writers.first() == 0 && config.concurrency.readers.first() == 0 {
                    return Err(anyhow::anyhow!(
                        "Mixed mode requires writers > 0 or readers > 0"
                    ));
                }
                if config.operations.write.is_none() && config.operations.read.is_none() {
                    return Err(anyhow::anyhow!(
                        "Mixed mode requires at least one of 'write' or 'read' operation config"
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

    pub fn writers(&self) -> usize {
        self.config.concurrency.writers.first()
    }

    pub fn readers(&self) -> usize {
        self.config.concurrency.readers.first()
    }

    pub fn duration_seconds(&self) -> u64 {
        self.config.duration_seconds
    }

    /// Prepare the workload (e.g., prepopulate data for read workloads)
    pub async fn prepare(&self, store: &dyn StoreManager) -> Result<()> {
        if let Some(setup_config) = &self.config.setup {
            println!(
                "Running setup phase: prepopulating {} events...",
                setup_config.prepopulate_events
            );
            let setup_start = Instant::now();

            let total_events = setup_config.prepopulate_events;
            let num_streams = setup_config
                .prepopulate_streams
                .unwrap_or(total_events);
            let events_per_stream = (total_events as f64 / num_streams as f64).ceil() as u64;

            // Prepopulate events across streams concurrently
            let mut setup_set = JoinSet::new();
            let concurrency = 10;
            let streams_per_task = (num_streams as f64 / concurrency as f64).ceil() as usize;

            let write_config = self.config.operations.write.as_ref().ok_or_else(|| {
                anyhow::anyhow!("Setup requires write operation config for prepopulation")
            })?;
            let event_size = write_config.event_size_bytes;

            for task_idx in 0..concurrency {
                let start_stream = task_idx * streams_per_task;
                let end_stream = (start_stream + streams_per_task).min(num_streams as usize);
                if start_stream >= end_stream {
                    continue;
                }

                let adapter = store.create_adapter()?;

                let stream_prefix = self.stream_prefix.clone();
                setup_set.spawn(async move {
                    for stream_idx in start_stream..end_stream {
                        for _ in 0..events_per_stream {
                            let evt = EventData {
                                stream: format!("{}{}", stream_prefix, stream_idx),
                                event_type: "setup".to_string(),
                                payload: vec![0u8; event_size],
                                tags: vec![],
                            };
                            adapter.append(evt).await?;
                        }
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
        }

        Ok(())
    }

    /// Execute the workload
    pub async fn execute(
        &self,
        store: &dyn StoreManager,
        cancel_token: CancellationToken,
    ) -> Result<(LatencyRecorder, u64, u64, Vec<ThroughputSample>)> {
        match self.config.mode {
            PerformanceMode::Write => {
                self.execute_write_workload(store, cancel_token)
                    .await
            }
            PerformanceMode::Read => {
                self.execute_read_workload(store, cancel_token)
                    .await
            }
            PerformanceMode::Mixed => {
                self.execute_mixed_workload(store, cancel_token)
                    .await
            }
        }
    }

    async fn execute_write_workload(
        &self,
        store: &dyn StoreManager,
        cancel_token: CancellationToken,
    ) -> Result<(LatencyRecorder, u64, u64, Vec<ThroughputSample>)> {
        let writers = self.config.concurrency.writers.first();
        println!("Creating {} writer clients...", writers);

        let mut writer_adapters = Vec::new();
        for i in 0..writers {
            match store.create_adapter() {
                Ok(adapter) => writer_adapters.push(adapter),
                Err(e) => {
                    eprintln!("Failed to create writer {}: {}", i, e);
                    anyhow::bail!("Failed to create writer {}: {}", i, e);
                }
            }
        }
        println!("All {} writer clients ready", writers);

        let mut set = JoinSet::new();

        let write_config = self.config.operations.write.as_ref().unwrap();

        // Per-worker atomic counters to avoid contention
        let worker_counters: Vec<Arc<AtomicU64>> = (0..writers)
            .map(|_| Arc::new(AtomicU64::new(0)))
            .collect();

        let has_stopped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        
        // Spawn writer tasks first
        for (i, adapter) in writer_adapters.into_iter().enumerate() {
            let write_cfg = write_config.clone();
            let worker_counter = worker_counters[i].clone();
            let has_stopped = has_stopped.clone();
            let cancel_token = cancel_token.clone();

            set.spawn(async move {
                let mut local_count = 0u64;
                let size = write_cfg.event_size_bytes;

                // Pre-allocate strings outside loop
                let stream = "bench-stream".to_string();
                let event_type = "test".to_string();
                let payload = vec![0u8; size];

                // Sampling for latency measurement (1 in every N operations)
                let mut rec = LatencyRecorder::new();

                // Tight loop with minimal overhead
                while !has_stopped.load(Ordering::Relaxed) && !cancel_token.is_cancelled() {
                    let evt = EventData {
                        stream: stream.clone(),
                        event_type: event_type.clone(),
                        payload: payload.clone(),
                        tags: vec![],
                    };

                    let operation_started = Instant::now();

                    if adapter.append(evt).await.is_ok() {
                        local_count += 1;

                        // Update shared counter on every operation for maximum throughput accuracy
                        // (atomic store is ~0.5ns, negligible compared to append latency)
                        worker_counter.store(local_count, Ordering::Relaxed);

                        // Record latency sample
                        rec.record(operation_started.elapsed());
                    }
                }

                // Store final count for this worker
                worker_counter.store(local_count, Ordering::Relaxed);
                rec
            });
        }

        // Spawn throughput sampling task that waits for warmup, then samples
        tokio::time::sleep(Duration::from_secs(1)).await;
        let sample_counters = worker_counters.clone();
        let duration_seconds = self.config.duration_seconds;
        let has_stopped_throughput = has_stopped.clone();
        let cancel_token_throughput = cancel_token.clone();
        let throughput_handle = tokio::spawn(async move {
            // Pre-allocate vector for N+1 samples
            let mut samples = Vec::with_capacity((duration_seconds + 1) as usize);

            // Take samples at 1-second intervals (N+1 total for N seconds)
            for i in 0..=duration_seconds {
                if cancel_token_throughput.is_cancelled() {
                    break;
                }
                let total_count: u64 = sample_counters.iter()
                    .map(|c| c.load(Ordering::Relaxed))
                    .sum();

                samples.push(ThroughputSample {
                    elapsed_s: i as f64,
                    count: total_count,
                });

                // Sleep until next second (except after last sample)
                if i < duration_seconds {
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                        _ = cancel_token_throughput.cancelled() => { break; }
                    }
                } else {
                    has_stopped_throughput.store(true, Ordering::Relaxed);
                }
            }

            samples
        });

        // Collect results from writer tasks
        let mut overall = LatencyRecorder::new();
        while let Some(res) = set.join_next().await {
            let rec = res.expect("join");
            overall.hist.add(&rec.hist).unwrap();
        }

        // Get final count from all workers
        let events_written: u64 = worker_counters.iter()
            .map(|c| c.load(Ordering::Relaxed))
            .sum();
        let throughput_samples = throughput_handle.await.expect("throughput task");

        Ok((overall, events_written, 0, throughput_samples))
    }

    async fn execute_read_workload(
        &self,
        store: &dyn StoreManager,
        cancel_token: CancellationToken,
    ) -> Result<(LatencyRecorder, u64, u64, Vec<ThroughputSample>)> {
        let readers = self.config.concurrency.readers.first();
        println!("Creating {} reader clients...", readers);

        let mut reader_adapters = Vec::new();
        for i in 0..readers {
            match store.create_adapter() {
                Ok(adapter) => reader_adapters.push(adapter),
                Err(e) => {
                    eprintln!("Failed to create reader {}: {}", i, e);
                    anyhow::bail!("Failed to create reader {}: {}", i, e);
                }
            }
        }
        println!("All {} reader clients ready", readers);

        let mut set = JoinSet::new();

        let read_config = self.config.operations.read.as_ref().unwrap();

        // Per-worker atomic counters to track operations
        let worker_counters: Vec<Arc<AtomicU64>> = (0..readers)
            .map(|_| Arc::new(AtomicU64::new(0)))
            .collect();

        let has_stopped = Arc::new(std::sync::atomic::AtomicBool::new(false));

        // Spawn reader tasks
        for (i, adapter) in reader_adapters.into_iter().enumerate() {
            let config = self.config.clone();
            let read_cfg = read_config.clone();
            let seed = self.seed + (i as u64);
            let worker_counter = worker_counters[i].clone();
            let has_stopped = has_stopped.clone();
            let cancel_token = cancel_token.clone();
            let stream_prefix = self.stream_prefix.clone();
            set.spawn(async move {
                let mut rng = StdRng::seed_from_u64(seed);
                let use_heavy_tail = config.streams.distribution.to_lowercase() == "zipf";
                let hot_set = 100_u64.min(config.streams.count.max(1));
                let mut rec = LatencyRecorder::new();
                let mut total_events_read = 0u64;

                while !has_stopped.load(Ordering::Relaxed) && !cancel_token.is_cancelled() {
                    let stream_idx = if use_heavy_tail && rng.gen_bool(0.2) {
                        rng.gen_range(0..hot_set)
                    } else {
                        rng.gen_range(0..config.streams.count)
                    };

                    let req = ReadRequest {
                        stream: format!("{}{}", stream_prefix, stream_idx),
                        from_offset: None,
                        limit: Some(read_cfg.batch_size as u64),
                    };

                    let operation_started = Instant::now();
                    let result = adapter.read(req).await;

                    if let Ok(events) = result {
                        total_events_read += events.len() as u64;
                        worker_counter.store(total_events_read, Ordering::Relaxed);
                    }

                    // Record latency for all operations
                    rec.record(operation_started.elapsed());
                }
                (rec, total_events_read)
            });
        }

        // Spawn throughput sampling task that waits for warmup, then samples
        tokio::time::sleep(Duration::from_secs(1)).await;
        let sample_counters = worker_counters.clone();
        let duration_seconds = self.config.duration_seconds;
        let has_stopped_throughput = has_stopped.clone();
        let cancel_token_throughput = cancel_token.clone();
        let throughput_handle = tokio::spawn(async move {
            // Pre-allocate vector for N+1 samples
            let mut samples = Vec::with_capacity((duration_seconds + 1) as usize);

            // Take samples at 1-second intervals (N+1 total for N seconds)
            for i in 0..=duration_seconds {
                if cancel_token_throughput.is_cancelled() {
                    break;
                }
                let total_count: u64 = sample_counters.iter()
                    .map(|c| c.load(Ordering::Relaxed))
                    .sum();

                samples.push(ThroughputSample {
                    elapsed_s: i as f64,
                    count: total_count,
                });

                // Sleep until next second (except after last sample)
                if i < duration_seconds {
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                        _ = cancel_token_throughput.cancelled() => { break; }
                    }
                } else {
                    has_stopped_throughput.store(true, Ordering::Relaxed);
                }
            }

            samples
        });

        // Collect results from reader tasks
        let mut overall = LatencyRecorder::new();
        let mut events_read: u64 = 0;
        while let Some(res) = set.join_next().await {
            let (rec, reader_events_read) = res.expect("join");
            overall.hist.add(&rec.hist)?;
            events_read += reader_events_read;
        }

        let throughput_samples = throughput_handle.await.expect("throughput task");

        Ok((overall, 0, events_read, throughput_samples))
    }

    async fn execute_mixed_workload(
        &self,
        store: &dyn StoreManager,
        cancel_token: CancellationToken,
    ) -> Result<(LatencyRecorder, u64, u64, Vec<ThroughputSample>)> {
        let writers = self.config.concurrency.writers.first();
        let readers = self.config.concurrency.readers.first();
        let total_workers = writers + readers;

        println!("Creating {} worker clients ({} writers, {} readers)...", total_workers, writers, readers);

        let mut worker_adapters = Vec::new();
        for i in 0..total_workers {
            match store.create_adapter() {
                Ok(adapter) => worker_adapters.push(adapter),
                Err(e) => {
                    eprintln!("Failed to create worker {}: {}", i, e);
                    anyhow::bail!("Failed to create worker {}: {}", i, e);
                }
            }
        }
        println!("All {} worker clients ready", total_workers);

        let mut set = JoinSet::new();

        // Per-worker atomic counters to track operations
        let worker_counters: Vec<Arc<AtomicU64>> = (0..total_workers)
            .map(|_| Arc::new(AtomicU64::new(0)))
            .collect();

        let has_stopped = Arc::new(std::sync::atomic::AtomicBool::new(false));

        let write_prob = self
            .config
            .operations
            .write
            .as_ref()
            .and_then(|w| w.probability)
            .unwrap_or(0.5);

        // Spawn worker tasks
        for (i, adapter) in worker_adapters.into_iter().enumerate() {
            let config = self.config.clone();
            let seed = self.seed + (i as u64);
            let is_writer = i < writers;
            let worker_counter = worker_counters[i].clone();
            let has_stopped = has_stopped.clone();
            let cancel_token = cancel_token.clone();

            set.spawn(async move {
                let mut rng = StdRng::seed_from_u64(seed);
                let use_heavy_tail = config.streams.distribution.to_lowercase() == "zipf";
                let hot_set = 100_u64.min(config.streams.count.max(1));
                let mut rec = LatencyRecorder::new();
                let mut events_written = 0u64;
                let mut events_read = 0u64;

                let write_cfg = config.operations.write.as_ref();
                let read_cfg = config.operations.read.as_ref();

                while !has_stopped.load(Ordering::Relaxed) && !cancel_token.is_cancelled() {
                    let stream_idx = if use_heavy_tail && rng.gen_bool(0.2) {
                        rng.gen_range(0..hot_set)
                    } else {
                        rng.gen_range(0..config.streams.count)
                    };

                    // Decide operation based on worker type and probability
                    let should_write = if is_writer {
                        write_cfg.is_some() && (read_cfg.is_none() || rng.gen_bool(write_prob))
                    } else {
                        false
                    };

                    let operation_started = Instant::now();

                    if should_write {
                        if let Some(write_cfg) = write_cfg {
                            let evt = EventData {
                                stream: format!("stream-{}", stream_idx),
                                event_type: "test".to_string(),
                                payload: vec![0u8; write_cfg.event_size_bytes],
                                tags: vec![],
                            };
                            if adapter.append(evt).await.is_ok() {
                                events_written += 1;
                                worker_counter.store(events_written, Ordering::Relaxed);
                            }
                        } else {
                            continue;
                        }
                    } else {
                        if let Some(read_cfg) = read_cfg {
                            let req = ReadRequest {
                                stream: format!("stream-{}", stream_idx),
                                from_offset: None,
                                limit: Some(read_cfg.batch_size as u64),
                            };
                            let result = adapter.read(req).await;
                            if let Ok(events) = result {
                                events_read += events.len() as u64;
                                worker_counter.store(events_read, Ordering::Relaxed);
                            }
                        } else {
                            continue;
                        }
                    };

                    // Record latency for all operations
                    rec.record(operation_started.elapsed());
                }
                (rec, events_written, events_read)
            });
        }

        // Spawn throughput sampling task that waits for warmup, then samples
        tokio::time::sleep(Duration::from_secs(1)).await;
        let sample_counters = worker_counters.clone();
        let duration_seconds = self.config.duration_seconds;
        let has_stopped_throughput = has_stopped.clone();
        let cancel_token_throughput = cancel_token.clone();
        let throughput_handle = tokio::spawn(async move {
            // Pre-allocate vector for N+1 samples
            let mut samples = Vec::with_capacity((duration_seconds + 1) as usize);

            // Take samples at 1-second intervals (N+1 total for N seconds)
            for i in 0..=duration_seconds {
                if cancel_token_throughput.is_cancelled() {
                    break;
                }
                let total_count: u64 = sample_counters.iter()
                    .map(|c| c.load(Ordering::Relaxed))
                    .sum();

                samples.push(ThroughputSample {
                    elapsed_s: i as f64,
                    count: total_count,
                });

                // Sleep until next second (except after last sample)
                if i < duration_seconds {
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                        _ = cancel_token_throughput.cancelled() => { break; }
                    }
                } else {
                    has_stopped_throughput.store(true, Ordering::Relaxed);
                }
            }

            samples
        });

        // Collect results from worker tasks
        let mut overall = LatencyRecorder::new();
        let mut total_events_written: u64 = 0;
        let mut total_events_read: u64 = 0;
        while let Some(res) = set.join_next().await {
            let (rec, written, read) = res.expect("join");
            overall.hist.add(&rec.hist)?;
            total_events_written += written;
            total_events_read += read;
        }

        let throughput_samples = throughput_handle.await.expect("throughput task");

        Ok((overall, total_events_written, total_events_read, throughput_samples))
    }
}
