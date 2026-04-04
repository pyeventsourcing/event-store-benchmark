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
    let monitor = if !store.local() {
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
            match ContainerMonitor::new(id, startup_time_s) {
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
        monitor
    } else {
        None
    };

    // Execute workload
    let workload_res = tokio::select! {
        res = workload.execute(store.as_ref(), cancel_token.clone()) => res,
        _ = cancel_token.cancelled() => {
            println!("Interrupted during workload execution.");
            if !store.local() {
                store.stop().await.ok();
            }
            anyhow::bail!("Interrupted");
        }
    };

    let workload_results = match workload_res {
        Ok(res) => res,
        Err(e) => {
            if !store.local() {
                // Ensure container is stopped on error/interruption
                store.stop().await.ok();
            }
            return Err(e);
        }
    };

    workload_results.print_summary();

    // Get container logs before stopping
    let (container_metrics, logs) = if !store.local() {
        let container_metrics = if let Some(m) = monitor {
            m.stop().await
        } else {
            ContainerMetrics::default()
        };
        // Ensure container is stopped on error/interruption
        store.stop().await?;
        let logs = store.logs().await.unwrap_or_else(|e| {
            eprintln!("Failed to capture container logs: {}", e);
            String::new()
        });

        (container_metrics, logs)
    } else {
        (ContainerMetrics::default(), String::new())
    };

    Ok((container_metrics, workload_results, logs))
}
