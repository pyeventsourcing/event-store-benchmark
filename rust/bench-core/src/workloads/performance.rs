use crate::adapter::{EventData, ReadRequest, StoreManager};
use crate::common::{SetupConfig, StreamsConfig};
use crate::metrics::{now_ms, LatencyRecorder, RawSample};
use anyhow::Result;
use rand::{rngs::StdRng, Rng, SeedableRng};
use serde::{Deserialize, Serialize};
use std::time::Instant;
use tokio::task::JoinSet;

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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PerformanceMode {
    Write,
    Read,
    Mixed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConcurrencyConfig {
    #[serde(default)]
    pub writers: usize,
    #[serde(default)]
    pub readers: usize,
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
}

impl PerformanceWorkload {
    pub fn from_yaml(yaml_config: &str, seed: u64) -> Result<Self> {
        let config: PerformanceConfig = serde_yaml::from_str(yaml_config)?;

        // Validate mode-specific config
        match config.mode {
            PerformanceMode::Write => {
                if config.concurrency.writers == 0 {
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
                if config.concurrency.readers == 0 {
                    return Err(anyhow::anyhow!(
                        "Read mode requires readers > 0 in concurrency config"
                    ));
                }
                if config.operations.read.is_none() {
                    return Err(anyhow::anyhow!("Read mode requires 'read' operation config"));
                }
            }
            PerformanceMode::Mixed => {
                if config.concurrency.writers == 0 && config.concurrency.readers == 0 {
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

        Ok(Self { config, seed })
    }

    pub fn name(&self) -> &str {
        &self.config.name
    }

    pub fn writers(&self) -> usize {
        self.config.concurrency.writers
    }

    pub fn readers(&self) -> usize {
        self.config.concurrency.readers
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

            let num_streams = setup_config
                .prepopulate_streams
                .unwrap_or(self.config.streams.count);
            let total_events = setup_config.prepopulate_events;
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

                setup_set.spawn(async move {
                    for stream_idx in start_stream..end_stream {
                        for _ in 0..events_per_stream {
                            let evt = EventData {
                                stream: format!("stream-{}", stream_idx),
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
        measurement_start: Instant,
        measurement_end: Instant,
        end_at: Instant,
    ) -> Result<(LatencyRecorder, u64, u64, Vec<RawSample>)> {
        match self.config.mode {
            PerformanceMode::Write => {
                self.execute_write_workload(store, measurement_start, measurement_end, end_at)
                    .await
            }
            PerformanceMode::Read => {
                self.execute_read_workload(store, measurement_start, measurement_end, end_at)
                    .await
            }
            PerformanceMode::Mixed => {
                self.execute_mixed_workload(store, measurement_start, measurement_end, end_at)
                    .await
            }
        }
    }

    async fn execute_write_workload(
        &self,
        store: &dyn StoreManager,
        measurement_start: Instant,
        measurement_end: Instant,
        end_at: Instant,
    ) -> Result<(LatencyRecorder, u64, u64, Vec<RawSample>)> {
        let writers = self.config.concurrency.writers;
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

        for (i, adapter) in writer_adapters.into_iter().enumerate() {
            let config = self.config.clone();
            let write_cfg = write_config.clone();
            let seed = self.seed + (i as u64);

            set.spawn(async move {
                let mut rng = StdRng::seed_from_u64(seed);
                let use_heavy_tail = config.streams.distribution.to_lowercase() == "zipf";
                let hot_set = 100_u64.min(config.streams.count.max(1));
                let mut rec = LatencyRecorder::new();
                let mut task_samples = Vec::new();
                let size = write_cfg.event_size_bytes;

                while Instant::now() < end_at {
                    let stream_idx = if use_heavy_tail && rng.gen_bool(0.2) {
                        rng.gen_range(0..hot_set)
                    } else {
                        rng.gen_range(0..config.streams.count)
                    };

                    let evt = EventData {
                        stream: format!("stream-{}", stream_idx),
                        event_type: "test".to_string(),
                        payload: vec![0u8; size],
                        tags: vec![],
                    };

                    let t0 = Instant::now();
                    let ok = adapter.append(evt).await.is_ok();
                    let dt = t0.elapsed();
                    let now = Instant::now();

                    if now >= measurement_start && now <= measurement_end {
                        rec.record(dt);
                        let sample = RawSample {
                            t_ms: now_ms(),
                            op: "append".to_string(),
                            latency_us: dt.as_micros() as u64,
                            ok,
                        };
                        task_samples.push(sample);
                    }
                }
                (rec, task_samples)
            });
        }

        let mut overall = LatencyRecorder::new();
        let mut events_written: u64 = 0;
        let mut samples_vec = Vec::new();
        while let Some(res) = set.join_next().await {
            let (rec, task_samples) = res.expect("join");
            overall.hist.add(&rec.hist).unwrap();
            events_written += rec.hist.len() as u64;
            samples_vec.extend(task_samples);
        }

        Ok((overall, events_written, 0, samples_vec))
    }

    async fn execute_read_workload(
        &self,
        store: &dyn StoreManager,
        measurement_start: Instant,
        measurement_end: Instant,
        end_at: Instant,
    ) -> Result<(LatencyRecorder, u64, u64, Vec<RawSample>)> {
        let readers = self.config.concurrency.readers;
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

        for (i, adapter) in reader_adapters.into_iter().enumerate() {
            let config = self.config.clone();
            let read_cfg = read_config.clone();
            let seed = self.seed + (i as u64);

            set.spawn(async move {
                let mut rng = StdRng::seed_from_u64(seed);
                let use_heavy_tail = config.streams.distribution.to_lowercase() == "zipf";
                let hot_set = 100_u64.min(config.streams.count.max(1));
                let mut rec = LatencyRecorder::new();
                let mut total_events_read = 0u64;
                let mut task_samples = Vec::new();

                while Instant::now() < end_at {
                    let stream_idx = if use_heavy_tail && rng.gen_bool(0.2) {
                        rng.gen_range(0..hot_set)
                    } else {
                        rng.gen_range(0..config.streams.count)
                    };

                    let req = ReadRequest {
                        stream: format!("stream-{}", stream_idx),
                        from_offset: None,
                        limit: Some(read_cfg.batch_size as u64),
                    };

                    let t0 = Instant::now();
                    let result = adapter.read(req).await;
                    let dt = t0.elapsed();
                    let now = Instant::now();

                    let (ok, _events_count) = match result {
                        Ok(events) => {
                            let count = events.len() as u64;
                            total_events_read += count;
                            (true, count)
                        }
                        Err(_) => (false, 0),
                    };

                    if now >= measurement_start && now <= measurement_end {
                        rec.record(dt);
                        let sample = RawSample {
                            t_ms: now_ms(),
                            op: "read".to_string(),
                            latency_us: dt.as_micros() as u64,
                            ok,
                        };
                        task_samples.push(sample);
                    }
                }
                (rec, total_events_read, task_samples)
            });
        }

        let mut overall = LatencyRecorder::new();
        let mut events_read: u64 = 0;
        let mut samples_vec = Vec::new();
        while let Some(res) = set.join_next().await {
            let (rec, reader_events_read, task_samples) = res.expect("join");
            overall.hist.add(&rec.hist)?;
            events_read += reader_events_read;
            samples_vec.extend(task_samples);
        }

        Ok((overall, 0, events_read, samples_vec))
    }

    async fn execute_mixed_workload(
        &self,
        store: &dyn StoreManager,
        measurement_start: Instant,
        measurement_end: Instant,
        end_at: Instant,
    ) -> Result<(LatencyRecorder, u64, u64, Vec<RawSample>)> {
        let writers = self.config.concurrency.writers;
        let readers = self.config.concurrency.readers;
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

        let write_prob = self
            .config
            .operations
            .write
            .as_ref()
            .and_then(|w| w.probability)
            .unwrap_or(0.5);

        for (i, adapter) in worker_adapters.into_iter().enumerate() {
            let config = self.config.clone();
            let seed = self.seed + (i as u64);
            let is_writer = i < writers;

            set.spawn(async move {
                let mut rng = StdRng::seed_from_u64(seed);
                let use_heavy_tail = config.streams.distribution.to_lowercase() == "zipf";
                let hot_set = 100_u64.min(config.streams.count.max(1));
                let mut rec = LatencyRecorder::new();
                let mut events_written = 0u64;
                let mut events_read = 0u64;
                let mut task_samples = Vec::new();

                let write_cfg = config.operations.write.as_ref();
                let read_cfg = config.operations.read.as_ref();

                while Instant::now() < end_at {
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

                    let (op_name, dt, ok) = if should_write {
                        if let Some(write_cfg) = write_cfg {
                            let evt = EventData {
                                stream: format!("stream-{}", stream_idx),
                                event_type: "test".to_string(),
                                payload: vec![0u8; write_cfg.event_size_bytes],
                                tags: vec![],
                            };
                            let t0 = Instant::now();
                            let ok = adapter.append(evt).await.is_ok();
                            let dt = t0.elapsed();
                            if ok {
                                events_written += 1;
                            }
                            ("append", dt, ok)
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
                            let t0 = Instant::now();
                            let result = adapter.read(req).await;
                            let dt = t0.elapsed();
                            let (ok, _count) = match result {
                                Ok(events) => {
                                    let count = events.len() as u64;
                                    events_read += count;
                                    (true, count)
                                }
                                Err(_) => (false, 0),
                            };
                            ("read", dt, ok)
                        } else {
                            continue;
                        }
                    };

                    let now = Instant::now();
                    if now >= measurement_start && now <= measurement_end {
                        rec.record(dt);
                        let sample = RawSample {
                            t_ms: now_ms(),
                            op: op_name.to_string(),
                            latency_us: dt.as_micros() as u64,
                            ok,
                        };
                        task_samples.push(sample);
                    }
                }
                (rec, events_written, events_read, task_samples)
            });
        }

        let mut overall = LatencyRecorder::new();
        let mut total_events_written: u64 = 0;
        let mut total_events_read: u64 = 0;
        let mut samples_vec = Vec::new();
        while let Some(res) = set.join_next().await {
            let (rec, written, read, task_samples) = res.expect("join");
            overall.hist.add(&rec.hist)?;
            total_events_written += written;
            total_events_read += read;
            samples_vec.extend(task_samples);
        }

        Ok((overall, total_events_written, total_events_read, samples_vec))
    }
}
