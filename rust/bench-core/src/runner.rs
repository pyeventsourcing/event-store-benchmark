use crate::adapter::StoreManager;
use crate::metrics::{LatencyPercentile, ThroughputSample, WorkloadResults, CpuSample, MemorySample};
use crate::workloads::Workload;
use crate::metrics::{ProcessMetrics, RunMetrics, ContainerStats};
use crate::container_stats::ContainerMonitor;
use crate::process_stats::ProcessMonitor;
use anyhow::Result;
use std::time::{Instant};
use tokio_util::sync::CancellationToken;

enum Monitor {
    Container(ContainerMonitor),
    Process(ProcessMonitor),
}

pub async fn execute_run(
    mut store: Box<dyn StoreManager>,
    workload: &Workload,
    cancel_token: CancellationToken,
) -> Result<(RunMetrics, WorkloadResults, Vec<ThroughputSample>, Vec<LatencyPercentile>, Vec<CpuSample>, Vec<MemorySample>, String)> {
    // Start store container
    let store_name = store.name();
    let (monitor, startup_time_s) = if store.use_docker() {
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
                    Some(Monitor::Container(m))
                }
                Err(e) => {
                    eprintln!("Failed to initialize container monitor: {}", e);
                    None
                }
            }
        } else {
            None
        };
        (monitor, Some(startup_time_s))
    } else {
        let pid_file = format!("{}.pid", store_name);
        let monitor = if let Ok(pid_str) = std::fs::read_to_string(&pid_file) {
            if let Ok(pid) = pid_str.trim().parse::<u32>() {
                println!("Found PID {} for {} in {}, starting process monitor...", pid, store_name, pid_file);
                let mut pm = ProcessMonitor::new(pid);
                pm.start().await;
                Some(Monitor::Process(pm))
            } else {
                eprintln!("Failed to parse PID from {}: {}", pid_file, pid_str);
                None
            }
        } else {
            println!("No PID file {} found for {}, skipping monitoring", pid_file, store_name);
            None
        };
        (monitor, None)
    };

    // Execute workload
    let workload_res = tokio::select! {
        res = workload.execute(store.as_ref(), cancel_token.clone()) => res,
        _ = cancel_token.cancelled() => {
            println!("Interrupted during workload execution.");
            if store.use_docker() {
                store.stop().await.ok();
            }
            anyhow::bail!("Interrupted");
        }
    };

    let (workload_results, throughput_samples, latency_percentiles) = match workload_res {
        Ok(res) => res,
        Err(e) => {
            if store.use_docker() {
                // Ensure container is stopped on error/interruption
                store.stop().await.ok();
            }
            return Err(e);
        }
    };

    workload_results.print_summary(&throughput_samples);

    // Get container logs before stopping
    let (run_metrics, cpu_samples, memory_samples, logs) = if store.use_docker() {
        let (resources, cpu_samples, memory_samples, container) = match monitor {
            Some(Monitor::Container(m)) => {
                let image_size_bytes = m.get_image_size().await.ok();
                let (resources, cpu, mem) = m.stop().await;
                let container = Some(ContainerStats {
                    startup_time_s: startup_time_s.unwrap_or(0.0),
                    image_size_bytes,
                });
                (resources, cpu, mem, container)
            }
            _ => (ProcessMetrics::default(), Vec::new(), Vec::new(), Some(ContainerStats {
                startup_time_s: startup_time_s.unwrap_or(0.0),
                image_size_bytes: None,
            })),
        };
        // Ensure container is stopped on error/interruption
        store.stop().await?;
        let logs = store.logs().await.unwrap_or_else(|e| {
            eprintln!("Failed to capture container logs: {}", e);
            String::new()
        });

        (RunMetrics { resources, container }, cpu_samples, memory_samples, logs)
    } else {
        let (resources, cpu_samples, memory_samples) = match monitor {
            Some(Monitor::Process(m)) => m.stop().await,
            _ => (ProcessMetrics::default(), Vec::new(), Vec::new()),
        };
        (RunMetrics { resources, container: None }, cpu_samples, memory_samples, String::new())
    };

    Ok((run_metrics, workload_results, throughput_samples, latency_percentiles, cpu_samples, memory_samples, logs))
}
