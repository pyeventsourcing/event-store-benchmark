use crate::adapter::{EventData, StoreManager};
use crate::metrics::{now_ms, LatencyRecorder, RawSample};
use crate::workflow_strategy::{Workload, WorkloadFactory};
use crate::workload::StreamsConfig;
use anyhow::Result;
use async_trait::async_trait;
use rand::{rngs::StdRng, Rng, SeedableRng};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;
use tokio::task::JoinSet;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConcurrentWritersConfig {
    pub name: String,
    pub duration_seconds: u64,
    pub writers: usize,
    pub event_size_bytes: usize,
    pub streams: StreamsConfig,
}

/// Workflow that performs concurrent unconditional appends to streams
pub struct ConcurrentWritersWorkload {
    config: ConcurrentWritersConfig,
    seed: u64,
}

impl ConcurrentWritersWorkload {
    pub fn new(config: ConcurrentWritersConfig, seed: u64) -> Self {
        Self { config, seed }
    }
}

#[async_trait]
impl Workload for ConcurrentWritersWorkload {
    async fn execute(
        &self,
        store: &dyn StoreManager,
        measurement_start: Instant,
        measurement_end: Instant,
        end_at: Instant,
    ) -> Result<(LatencyRecorder, u64, u64, Vec<RawSample>)> {
        // Create writer adapters
        println!("Creating {} writer clients...", self.config.writers);
        let mut writer_adapters = Vec::new();
        for i in 0..self.config.writers {
            match store.create_adapter() {
                Ok(adapter) => writer_adapters.push(adapter),
                Err(e) => {
                    eprintln!("Failed to create writer {}: {}", i, e);
                    anyhow::bail!("Failed to create writer {}: {}", i, e);
                }
            }
        }
        println!("All {} writer clients ready", self.config.writers);

        let samples = Arc::new(Mutex::new(Vec::<RawSample>::with_capacity(100_000)));
        let mut set = JoinSet::new();

        // Start one task per writer - each uses its own adapter instance
        for (i, adapter) in writer_adapters.into_iter().enumerate() {
            let samples = samples.clone();
            let config = self.config.clone();
            let seed = self.seed + (i as u64);

            set.spawn(async move {
                let mut rng = StdRng::seed_from_u64(seed);
                let use_heavy_tail = config.streams.distribution.to_lowercase() == "zipf";
                let hot_set = 100_u64.min(config.streams.unique_streams.max(1));
                let mut rec = LatencyRecorder::new();
                let size = config.event_size_bytes;

                while Instant::now() < end_at {
                    let stream_idx = if use_heavy_tail && rng.gen_bool(0.2) {
                        // 20% of the time, pick from a small hot set starting at 0
                        rng.gen_range(0..hot_set)
                    } else {
                        rng.gen_range(0..config.streams.unique_streams)
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

                    // Only record samples during the measurement window (after warmup, before cooldown)
                    if now >= measurement_start && now <= measurement_end {
                        rec.record(dt);
                        let mut s = samples.lock().await;
                        s.push(RawSample {
                            t_ms: now_ms(),
                            op: "append".to_string(),
                            latency_us: dt.as_micros() as u64,
                            ok,
                        });
                    }
                }
                rec
            });
        }

        let mut overall = LatencyRecorder::new();
        let mut events_written: u64 = 0;
        while let Some(res) = set.join_next().await {
            let rec = res.expect("join");
            overall.hist.add(&rec.hist).unwrap();
            events_written += rec.hist.len() as u64;
        }

        let samples_vec = samples.lock().await.clone();
        Ok((overall, events_written, 0, samples_vec))
    }

    fn name(&self) -> String {
        self.config.name.clone()
    }

    fn writers(&self) -> usize {
        self.config.writers
    }

    fn readers(&self) -> usize {
        0
    }

    fn duration_seconds(&self) -> u64 {
        self.config.duration_seconds
    }
}

/// Factory for creating ConcurrentWritersWorkload instances
pub struct ConcurrentWritersFactory;

impl WorkloadFactory for ConcurrentWritersFactory {
    fn name(&self) -> &'static str {
        "concurrent_writers"
    }

    fn create(&self, yaml_config: &str, seed: u64) -> Result<Box<dyn Workload>> {
        let config: ConcurrentWritersConfig = serde_yaml::from_str(yaml_config)?;
        Ok(Box::new(ConcurrentWritersWorkload::new(config, seed)))
    }
}
