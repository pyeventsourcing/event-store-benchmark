use std::time::Instant;
use anyhow::Result;
use tokio_util::sync::CancellationToken;

use tokio::sync::watch;

use crate::adapter::StoreManager;
use crate::container_stats::ContainerMonitor;
use crate::metrics::{ContainerStats, ProcessMetrics, RunMetrics, RunResults, SamplingConfigDecision, WorkloadResults};
use crate::process_stats::ProcessMonitor;
use super::performance::{PerformanceConfig, PerformanceWorkload};
use super::durability::DurabilityWorkload;
use super::consistency::ConsistencyWorkload;
use super::operational::OperationalWorkload;

/// Represents a workload that can be executed
pub enum WorkloadRunner {
    Performance(PerformanceWorkload),
    Durability(DurabilityWorkload),
    Consistency(ConsistencyWorkload),
    Operational(OperationalWorkload),
}

impl WorkloadRunner {
    pub fn type_str(&self) -> Result<&str> {
        match self {
            WorkloadRunner::Performance(_) => Ok("performance"),
            WorkloadRunner::Durability(_) => Ok("durability"),
            WorkloadRunner::Consistency(_) => Ok("consistency"),
            WorkloadRunner::Operational(_) => Ok("operational"),
        }
    }

    pub fn store_name(&self) -> Result<String> {
        match self {
            WorkloadRunner::Performance(w) => Ok(w.store_name()),
            _ => anyhow::bail!("workload store name not defined"),
        }
    }

    pub fn name(&self) -> Result<&str> {
        match self {
            WorkloadRunner::Performance(w) => Ok(w.name()),
            WorkloadRunner::Durability(w) => {
                anyhow::bail!("Durability workloads not yet implemented: {}", w.name());
            }
            WorkloadRunner::Consistency(w) => {
                anyhow::bail!("Consistency workloads not yet implemented: {}", w.name());
            }
            WorkloadRunner::Operational(w) => {
                anyhow::bail!("Operational workloads not yet implemented: {}", w.name());
            }
        }
    }

    pub async fn execute(
        &self,
        mut store: Box<dyn StoreManager>,
        cancel_token: CancellationToken,
    ) -> Result<RunResults> {
        // Start store container
        let store_name = store.name();
        if store.use_docker() {
            if let Ok(config) = self.performance_config() {
                store.set_memory_limit(config.docker_memory_limit_mb);
                if let Some(ref platform) = config.docker_platform {
                    store.set_docker_platform(Some(platform.clone()));
                }
            }
        }

        let (mut monitor, startup_time_s) = if store.use_docker() {
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

            let start_res = tokio::select! {
            res = store.start() => res,
            _ = cancel_token.cancelled() => {
                println!("Interrupted while starting container.");
                store.stop().await.ok();
                anyhow::bail!("Interrupted");
            }
        };

            if let Err(e) = start_res {
                eprintln!("Failed to start {} container: {}", store.name(), e);
                match store.logs().await {
                    Ok(logs) => {
                        if !logs.is_empty() {
                            eprintln!("--- {} container logs ---", store.name());
                            eprintln!("{}", logs);
                            eprintln!("--- end of logs ---");
                        }
                    }
                    Err(log_err) => {
                        eprintln!("Failed to fetch container logs: {}", log_err);
                    }
                }
                return Err(e);
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
                    Ok(m) => {
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
                    let pm = ProcessMonitor::new(pid);
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

        // Prepare synchronization primitives
        let (tx, rx) = watch::channel(None::<SamplingConfigDecision>);

        // Start benchmark process monitor
        let mut benchmark_monitor = ProcessMonitor::new(std::process::id());
        benchmark_monitor.start(rx.clone()).await;

        // Start monitor if it exists
        if let Some(m) = &mut monitor {
            match m {
                Monitor::Container(cm) => cm.start(rx.clone()).await,
                Monitor::Process(pm) => pm.start(rx.clone()).await,
            }
        }

        // Execute workload run
        let workload_results = match tokio::select! {
            _ = cancel_token.cancelled() => {
                println!("Interrupted during workload execution.");
                if store.use_docker() {
                    store.stop().await.ok();
                }
                anyhow::bail!("Interrupted");
            },
            workload_results = async {
                match self {
                    WorkloadRunner::Performance(w) => Ok(WorkloadResults::Performance(
                        w.execute(store.as_ref(), cancel_token.clone(), tx, rx).await?,
                    )),
                    WorkloadRunner::Durability(w) => {
                        anyhow::bail!("Durability workloads not yet implemented: {}", w.name());
                    }
                    WorkloadRunner::Consistency(w) => {
                        anyhow::bail!("Consistency workloads not yet implemented: {}", w.name());
                    }
                    WorkloadRunner::Operational(w) => {
                        anyhow::bail!("Operational workloads not yet implemented: {}", w.name());
                    }
                }
            } => workload_results
        } {
            Ok(res) => res,
            Err(e) => {
                if store.use_docker() {
                    // Ensure container is stopped on error/interruption
                    store.stop().await.ok();
                }
                return Err(e);
            }
        };

        workload_results.print_summary();

        // Get container logs before stopping
        let (run_metrics, cpu_samples, memory_samples, b_cpu_samples, b_memory_samples, logs) = if store.use_docker() {
            let (resources, cpu_samples, memory_samples, container) = match monitor {
                Some(Monitor::Container(m)) => {
                    let image_size_bytes = m.get_image_size().await.ok();
                    let (resources, cpu, mem) = m.stop().await;
                    let container = Some(ContainerStats {
                        startup_time_s: startup_time_s.unwrap_or(0.0),
                        image_size_bytes,
                    });
                    (resources, Some(cpu), Some(mem), container)
                }
                _ => (ProcessMetrics::default(), None, None, Some(ContainerStats {
                    startup_time_s: startup_time_s.unwrap_or(0.0),
                    image_size_bytes: None,
                })),
            };

            let (b_resources, b_cpu, b_mem) = benchmark_monitor.stop().await;

            // Ensure container is stopped on error/interruption
            store.stop().await?;
            let logs = store.logs().await.unwrap_or_else(|e| {
                let msg = format!("Failed to capture container logs: {}", e);
                eprintln!("{}", msg);
                msg
            });

            (RunMetrics { resources, benchmark_resources: Some(b_resources), container }, cpu_samples, memory_samples, Some(b_cpu), Some(b_mem), logs)
        } else {
            let (resources, cpu_samples, memory_samples) = match monitor {
                Some(Monitor::Process(m)) => {
                    let (res, cpu, mem) = m.stop().await;
                    (res, Some(cpu), Some(mem))
                }
                _ => (ProcessMetrics::default(), None, None),
            };

            let (b_resources, b_cpu, b_mem) = benchmark_monitor.stop().await;

            (RunMetrics { resources, benchmark_resources: Some(b_resources), container: None }, cpu_samples, memory_samples, Some(b_cpu), Some(b_mem), String::new())
        };

        Ok(RunResults {
            run_metrics,
            workload: workload_results,
            cpu_samples,
            memory_samples,
            benchmark_cpu_samples: b_cpu_samples,
            benchmark_memory_samples: b_memory_samples,
            logs,
        })
    }

    // pub async fn execute(
    //     &self,
    //     store: &dyn StoreManager,
    //     cancel_token: CancellationToken,
    //     tx: watch::Sender<Option<SamplingConfigDecision>>,
    //     rx: watch::Receiver<Option<SamplingConfigDecision>>,
    // ) -> Result<WorkloadResults> {
    //     match self {
    //         WorkloadRun::Performance(w) => Ok(WorkloadResults::Performance(
    //             w.execute(store, cancel_token, tx, rx).await?,
    //         )),
    //         WorkloadRun::Durability(w) => {
    //             anyhow::bail!("Durability workloads not yet implemented: {}", w.name());
    //         }
    //         WorkloadRun::Consistency(w) => {
    //             anyhow::bail!("Consistency workloads not yet implemented: {}", w.name());
    //         }
    //         WorkloadRun::Operational(w) => {
    //             anyhow::bail!("Operational workloads not yet implemented: {}", w.name());
    //         }
    //     }
    // }

    pub fn performance_config(&self) -> Result<PerformanceConfig> {
        match self {
            WorkloadRunner::Performance(w) => {
                Ok(w.config.clone())
            }
            _ => anyhow::bail!("Not a performance workload"),
        }
    }
}

enum Monitor {
    Container(ContainerMonitor),
    Process(ProcessMonitor),
}
