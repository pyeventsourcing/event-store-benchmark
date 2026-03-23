use crate::adapter::StoreManager;
use crate::metrics::{RunMetrics, Summary};
use crate::workloads::{Workload, PerformanceWorkload};
use crate::metrics::ContainerMetrics;
use anyhow::Result;
use std::time::{Duration, Instant};

pub async fn execute_run(
    mut store: Box<dyn StoreManager>,
    workload: &Workload,
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

    // Extract workload details and execute based on type
    let (workload_name, duration_seconds, writers, readers, overall, events_written, events_read, samples_vec) = match workload {
        Workload::Performance(perf_workload) => {
            execute_performance_workload(store.as_ref(), perf_workload).await?
        }
        Workload::Durability(dur_workload) => {
            anyhow::bail!("Durability workloads not yet implemented: {}", dur_workload.name());
        }
        Workload::Consistency(cons_workload) => {
            anyhow::bail!("Consistency workloads not yet implemented: {}", cons_workload.name());
        }
        Workload::Operational(op_workload) => {
            anyhow::bail!("Operational workloads not yet implemented: {}", op_workload.name());
        }
    };

    let dur_s = duration_seconds as f64;
    let total_ops = events_written + events_read;
    let summary = Summary {
        workload: workload_name,
        adapter: store.name().to_string(),
        writers,
        readers,
        events_written,
        events_read,
        duration_s: dur_s,
        throughput_eps: (total_ops as f64) / dur_s.max(0.001),
        latency: overall.to_stats(),
        container: ContainerMetrics {
            image_size_bytes: None,
            startup_time_s,
            avg_cpu_percent: None,
            peak_cpu_percent: None,
            avg_memory_bytes: None,
            peak_memory_bytes: None,
        },
    };

    let metrics = RunMetrics {
        summary,
        samples: samples_vec,
        sample_rate: 100, // 1-in-100 sampling
        latency_histogram: overall,
    };

    // Stop container
    store.stop().await?;

    Ok(metrics)
}

async fn execute_performance_workload(
    store: &dyn StoreManager,
    workload: &PerformanceWorkload,
) -> Result<(String, u64, usize, usize, crate::metrics::LatencyRecorder, u64, u64, Vec<crate::metrics::RawSample>)> {
    // Prepare the workload
    workload.prepare(store).await?;

    // Warmup and cooldown durations
    let duration_seconds = workload.duration_seconds();

    let warmup_duration = Duration::from_secs(1);
    let cooldown_duration = Duration::from_secs(1);
    let total_run_duration =
        Duration::from_secs(duration_seconds) + warmup_duration + cooldown_duration;

    let start_at = Instant::now();
    let measurement_start = start_at + warmup_duration;
    let measurement_end = measurement_start + Duration::from_secs(duration_seconds);
    let end_at = start_at + total_run_duration;

    // Execute the workload
    let (overall, events_written, events_read, samples_vec) = workload
        .execute(
            store,
            measurement_start,
            measurement_end,
            end_at,
        )
        .await?;

    Ok((
        workload.name().to_string(),
        duration_seconds,
        workload.writers(),
        workload.readers(),
        overall,
        events_written,
        events_read,
        samples_vec,
    ))
}
