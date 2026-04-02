use crate::adapter::StoreManager;
use crate::metrics::{WorkloadResults};
use crate::workloads::Workload;
use crate::metrics::ContainerMetrics;
use crate::container_stats::ContainerMonitor;
use anyhow::Result;
use std::time::{Instant};
use tokio_util::sync::CancellationToken;

pub async fn execute_run(
    mut store: Box<dyn StoreManager>,
    workload: &Workload,
    cancel_token: CancellationToken,
) -> Result<(ContainerMetrics, WorkloadResults, String)> {
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

    // Execute workload
    let workload_res = tokio::select! {
        res = workload.execute(store.as_ref(), cancel_token.clone()) => res,
        _ = cancel_token.cancelled() => {
            println!("Interrupted during workload execution.");
            store.stop().await.ok();
            anyhow::bail!("Interrupted");
        }
    };

    let workload_results = match workload_res {
        Ok(res) => res,
        Err(e) => {
            // Ensure container is stopped on error/interruption
            store.stop().await.ok();
            return Err(e);
        }
    };

    workload_results.print_summary();

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

    // Get container logs before stopping
    let logs = match store.logs().await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("Failed to capture container logs: {}", e);
            String::new()
        }
    };

    // Stop container
    store.stop().await?;

    Ok((container_metrics, workload_results, logs))
}
