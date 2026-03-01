use crate::adapter::{ReadRequest, StoreManager};
use crate::metrics::{now_ms, LatencyRecorder, RawSample};
use crate::workflow_strategy::{Workload, WorkloadFactory};
use crate::workload::{SetupConfig, StreamsConfig};
use anyhow::Result;
use async_trait::async_trait;
use rand::{rngs::StdRng, Rng, SeedableRng};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;
use tokio::task::JoinSet;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConcurrentReadersConfig {
    pub name: String,
    pub duration_seconds: u64,
    pub readers: usize,
    pub event_size_bytes: usize,
    pub streams: StreamsConfig,
    #[serde(default)]
    pub setup: Option<SetupConfig>,
}

/// Workflow that performs concurrent reads from streams
pub struct ConcurrentReadersWorkload {
    config: ConcurrentReadersConfig,
    seed: u64,
}

impl ConcurrentReadersWorkload {
    pub fn new(config: ConcurrentReadersConfig, seed: u64) -> Self {
        Self { config, seed }
    }
}

#[async_trait]
impl Workload for ConcurrentReadersWorkload {
    async fn execute(
        &self,
        store: &dyn StoreManager,
        measurement_start: Instant,
        measurement_end: Instant,
        end_at: Instant,
    ) -> Result<(LatencyRecorder, u64, u64, Vec<RawSample>)> {
        // Run setup phase if configured (prepopulate data for read workloads)
        if let Some(setup_config) = &self.config.setup {
            println!(
                "Running setup phase: prepopulating {} events...",
                setup_config.events_to_prepopulate
            );
            let setup_start = Instant::now();

            // Create a single writer for setup
            let setup_adapter = store.create_adapter()?;
            let num_streams = setup_config
                .prepopulate_streams
                .unwrap_or(self.config.streams.unique_streams);
            let events_per_stream =
                (setup_config.events_to_prepopulate as f64 / num_streams as f64).ceil() as u64;

            // Prepopulate events across streams
            for stream_idx in 0..num_streams {
                for _ in 0..events_per_stream {
                    let evt = crate::adapter::EventData {
                        stream: format!("stream-{}", stream_idx),
                        event_type: "setup".to_string(),
                        payload: vec![0u8; self.config.event_size_bytes],
                        tags: vec![],
                    };
                    setup_adapter.append(evt).await?;
                }
            }

            let setup_duration = setup_start.elapsed();
            println!(
                "Setup phase completed in {:.2} seconds",
                setup_duration.as_secs_f64()
            );
        }

        // Create reader adapters
        println!("Creating {} reader clients...", self.config.readers);
        let mut reader_adapters = Vec::new();
        for i in 0..self.config.readers {
            match store.create_adapter() {
                Ok(adapter) => reader_adapters.push(adapter),
                Err(e) => {
                    eprintln!("Failed to create reader {}: {}", i, e);
                    anyhow::bail!("Failed to create reader {}: {}", i, e);
                }
            }
        }
        println!("All {} reader clients ready", self.config.readers);

        let samples = Arc::new(Mutex::new(Vec::<RawSample>::with_capacity(100_000)));
        let mut set = JoinSet::new();

        // Start one task per reader - each uses its own adapter instance
        for (i, adapter) in reader_adapters.into_iter().enumerate() {
            let samples = samples.clone();
            let config = self.config.clone();
            let seed = self.seed + (i as u64);

            set.spawn(async move {
                let mut rng = StdRng::seed_from_u64(seed);
                let use_heavy_tail = config.streams.distribution.to_lowercase() == "zipf";
                let hot_set = 100_u64.min(config.streams.unique_streams.max(1));
                let mut rec = LatencyRecorder::new();
                let mut total_events_read = 0u64;

                while Instant::now() < end_at {
                    let stream_idx = if use_heavy_tail && rng.gen_bool(0.2) {
                        // 20% of the time, pick from a small hot set starting at 0
                        rng.gen_range(0..hot_set)
                    } else {
                        rng.gen_range(0..config.streams.unique_streams)
                    };

                    let req = ReadRequest {
                        stream: format!("stream-{}", stream_idx),
                        from_offset: None,
                        limit: Some(100), // Read up to 100 events per request
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

                    // Only record samples during the measurement window (after warmup, before cooldown)
                    if now >= measurement_start && now <= measurement_end {
                        rec.record(dt);
                        let mut s = samples.lock().await;
                        s.push(RawSample {
                            t_ms: now_ms(),
                            op: "read".to_string(),
                            latency_us: dt.as_micros() as u64,
                            ok,
                        });
                    }
                }
                (rec, total_events_read)
            });
        }

        let mut overall = LatencyRecorder::new();
        let mut events_read: u64 = 0;
        while let Some(res) = set.join_next().await {
            let (rec, reader_events_read) = res.expect("join");
            overall.hist.add(&rec.hist).unwrap();
            events_read += reader_events_read;
        }

        let samples_vec = samples.lock().await.clone();
        Ok((overall, 0, events_read, samples_vec))
    }

    fn name(&self) -> String {
        self.config.name.clone()
    }

    fn writers(&self) -> usize {
        0
    }

    fn readers(&self) -> usize {
        self.config.readers
    }

    fn duration_seconds(&self) -> u64 {
        self.config.duration_seconds
    }
}

/// Factory for creating ConcurrentReadersWorkload instances
pub struct ConcurrentReadersFactory;

impl WorkloadFactory for ConcurrentReadersFactory {
    fn name(&self) -> &'static str {
        "concurrent_readers"
    }

    fn create(&self, yaml_config: &str, seed: u64) -> Result<Box<dyn Workload>> {
        let config: ConcurrentReadersConfig = serde_yaml::from_str(yaml_config)?;
        Ok(Box::new(ConcurrentReadersWorkload::new(config, seed)))
    }
}
