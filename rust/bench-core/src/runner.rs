use crate::adapter::StoreManager;
use crate::metrics::{RunMetrics, Summary};
use crate::workloads::{Workload, PerformanceWorkload};
use crate::metrics::ContainerMetrics;
use crate::container_stats::ContainerMonitor;
use anyhow::Result;
use std::time::{Instant};
use tokio_util::sync::CancellationToken;

pub async fn execute_run(
    mut store: Box<dyn StoreManager>,
    workload: &Workload,
    cancel_token: CancellationToken,
) -> Result<RunMetrics> {
    // Start store container
    let store_name = store.name();
    if !crate::is_image_pulled(store_name) {
        println!("Pulling {} image...", store_name);
        let mut last_err = None;
        let max_retries = 3;
        for attempt in 1..=(max_retries + 1) {
            let res = tokio::select! {
                res = store.pull() => res,
                _ = cancel_token.cancelled() => {
                    println!("Interrupted while pulling image.");
                    anyhow::bail!("Interrupted");
                }
            };

            match res {
                Ok(_) => {
                    crate::mark_image_pulled(store_name);
                    last_err = None;
                    break;
                }
                Err(e) => {
                    if attempt <= max_retries {
                        println!("Failed to pull {} image (attempt {}/{}): {}. Retrying in 5s...", store_name, attempt, max_retries + 1, e);
                        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    }
                    last_err = Some(e);
                }
            }
        }
        if let Some(e) = last_err {
            return Err(e);
        }
    }

    println!("Starting {} container...", store.name());
    let setup_start = Instant::now();

    tokio::select! {
        res = store.start() => res?,
        _ = cancel_token.cancelled() => {
            println!("Interrupted while starting container.");
            store.stop().await.ok();
            anyhow::bail!("Interrupted");
        }
    }

    let startup_time_s = setup_start.elapsed().as_secs_f64();
    println!(
        "{} container is ready after {:.2} seconds",
        store.name(),
        startup_time_s
    );

    // Initialize container monitoring if possible
    let monitor = if let Some(id) = store.container_id() {
        match ContainerMonitor::new(id) {
            Ok(mut m) => {
                m.start().await;
                Some(m)
            }
            Err(e) => {
                eprintln!("Failed to initialize container monitor: {}", e);
                None
            }
        }
    } else {
        None
    };

    // Extract workload details and execute based on type
    let workload_res = tokio::select! {
        res = async {
            match workload {
                Workload::Performance(perf_workload) => {
                    execute_performance_workload(perf_workload, store.as_ref(), cancel_token.clone()).await
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
            }
        } => res,
        _ = cancel_token.cancelled() => {
            println!("Interrupted during workload execution.");
            store.stop().await.ok();
            anyhow::bail!("Interrupted");
        }
    };

    let (workload_name, duration_seconds, writers, readers, overall, throughput_samples) = match workload_res {
        Ok(vals) => vals,
        Err(e) => {
            // Ensure container is stopped on error/interruption
            store.stop().await.ok();
            return Err(e);
        }
    };

    let (dur_s, throughput_eps) = if throughput_samples.len() >= 2 {
        let first_sample = throughput_samples.first().unwrap();
        let last_sample = throughput_samples.last().unwrap();
        let duration = last_sample.elapsed_s - first_sample.elapsed_s;
        let count_delta = last_sample.count - first_sample.count;
        let throughput = (count_delta as f64) / duration.max(0.001);
        (duration, throughput)
    } else {
        (duration_seconds as f64, 0.0)
    };

    // Collect container metrics
    let mut container_metrics = ContainerMetrics {
        startup_time_s,
        ..Default::default()
    };

    if let Some(m) = monitor {
        match m.get_image_size().await {
            Ok(size) => container_metrics.image_size_bytes = Some(size),
            Err(e) => eprintln!("Failed to get image size: {}", e),
        }

        match m.stop().await {
            Ok((avg_cpu, peak_cpu, avg_mem, peak_mem)) => {
                container_metrics.avg_cpu_percent = avg_cpu;
                container_metrics.peak_cpu_percent = peak_cpu;
                container_metrics.avg_memory_bytes = avg_mem;
                container_metrics.peak_memory_bytes = peak_mem;
            }
            Err(e) => eprintln!("Failed to stop container monitor: {}", e),
        }
    }

    let summary = Summary {
        workload: workload_name,
        adapter: store.name().to_string(),
        writers,
        readers,
        duration_s: dur_s,
        throughput_eps,
        latency: overall.to_stats(),
        container: container_metrics,
    };

    let metrics = RunMetrics {
        summary,
        throughput_samples,
        latency_histogram: overall,
    };

    // Stop container
    store.stop().await?;

    Ok(metrics)
}

async fn execute_performance_workload(
    workload: &PerformanceWorkload,
    store: &dyn StoreManager,
    cancel_token: CancellationToken,
) -> Result<(String, u64, usize, usize, crate::metrics::LatencyRecorder, Vec<crate::metrics::ThroughputSample>)> {
    // Prepare the workload
    workload.prepare(store).await?;

    // Execute the workload
    let (all_latencies, throughput_samples) = workload
        .execute(store, cancel_token)
        .await?;

    Ok((
        workload.name().to_string(),
        workload.duration_seconds(),
        workload.writers(),
        workload.readers(),
        all_latencies,
        throughput_samples,
    ))
}
