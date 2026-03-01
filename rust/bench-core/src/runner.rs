use crate::adapter::StoreManager;
use crate::metrics::{RunMetrics, Summary};
use crate::workflow_strategy::Workload;
use crate::{container_stats, metrics::ContainerMetrics};
use anyhow::Result;
use std::time::{Duration, Instant};

pub async fn run_workload(
    mut store: Box<dyn StoreManager>,
    workload: Box<dyn Workload>,
    duration_seconds: u64,
) -> Result<RunMetrics> {
    // Start store container
    println!("Starting {} container...", store.name());
    let setup_start = Instant::now();

    store.start().await?;

    let startup_time_s = setup_start.elapsed().as_secs_f64();
    println!(
        "{} container is ready after {:.2} seconds",
        store.name(),
        startup_time_s
    );

    // Warmup and cooldown durations
    let warmup_duration = Duration::from_secs(1);
    let cooldown_duration = Duration::from_secs(1);
    let total_run_duration =
        Duration::from_secs(duration_seconds) + warmup_duration + cooldown_duration;

    let start_at = Instant::now();
    let measurement_start = start_at + warmup_duration;
    let measurement_end = measurement_start + Duration::from_secs(duration_seconds);
    let end_at = start_at + total_run_duration;

    // Start a background task to periodically collect container stats during the workload
    let container_id = store.container_id();
    let stats_handle = tokio::task::spawn_blocking(move || {
        let mut cpu_samples = Vec::new();
        let mut mem_samples = Vec::new();

        while Instant::now() < end_at {
            if let Some(ref id) = container_id {
                if let Ok(stats) = container_stats::get_container_stats(id) {
                    cpu_samples.push(stats.cpu_percent);
                    mem_samples.push(stats.memory_bytes);
                }
            }
            std::thread::sleep(Duration::from_secs(1));
        }

        (cpu_samples, mem_samples)
    });

    // Delegate workload execution to the workload object
    let (overall, events_written, events_read, samples_vec) = workload
        .execute(
            store.as_ref(),
            measurement_start,
            measurement_end,
            end_at,
        )
        .await?;

    // Wait for stats collection to finish
    let (cpu_samples, mem_samples) = stats_handle.await.unwrap_or_default();

    // Collect final container metrics
    let mut container_metrics = if let Some(id) = store.container_id() {
        let image_size_bytes = container_stats::get_container_image_size(&id).ok();
        ContainerMetrics {
            image_size_bytes,
            startup_time_s,
            avg_cpu_percent: None,
            peak_cpu_percent: None,
            avg_memory_bytes: None,
            peak_memory_bytes: None,
        }
    } else {
        ContainerMetrics {
            startup_time_s,
            ..Default::default()
        }
    };

    if !cpu_samples.is_empty() {
        let avg_cpu = cpu_samples.iter().sum::<f64>() / cpu_samples.len() as f64;
        let peak_cpu = cpu_samples.iter().copied().fold(0.0f64, f64::max);
        container_metrics.avg_cpu_percent = Some(avg_cpu);
        container_metrics.peak_cpu_percent = Some(peak_cpu);
    }

    if !mem_samples.is_empty() {
        let avg_mem = mem_samples.iter().sum::<u64>() / mem_samples.len() as u64;
        let peak_mem = *mem_samples.iter().max().unwrap_or(&0);
        container_metrics.avg_memory_bytes = Some(avg_mem);
        container_metrics.peak_memory_bytes = Some(peak_mem);
    }

    let dur_s = duration_seconds as f64;
    let total_ops = events_written + events_read;
    let summary = Summary {
        workload: workload.name(),
        adapter: store.name().to_string(),
        writers: workload.writers(),
        readers: workload.readers(),
        events_written,
        events_read,
        duration_s: dur_s,
        throughput_eps: (total_ops as f64) / dur_s.max(0.001),
        latency: overall.to_stats(),
        container: container_metrics,
    };

    let metrics = RunMetrics {
        summary,
        samples: samples_vec,
    };

    // Stop container
    store.stop().await?;

    Ok(metrics)
}
