use anyhow::Result;
use bollard::Docker;
use bollard::query_parameters::StatsOptions;
use futures::StreamExt;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use crate::metrics::ContainerMetrics;

pub struct ContainerMonitor {
    docker: Docker,
    container_id: String,
    stats: Arc<Mutex<CollectedStats>>,
    stop_tx: Option<tokio::sync::oneshot::Sender<()>>,
    monitor_task: Option<JoinHandle<()>>,
    startup_tims_s: f64,
}

#[derive(Default, Clone)]
struct CollectedStats {
    cpu_samples: Vec<f64>,
    memory_samples: Vec<u64>,
}

impl ContainerMonitor {
    pub fn new(container_id: String, startup_tims_s: f64) -> Result<Self> {
        let docker = Docker::connect_with_local_defaults()?;
        Ok(Self {
            docker,
            container_id,
            stats: Arc::new(Mutex::new(CollectedStats::default())),
            stop_tx: None,
            monitor_task: None,
            startup_tims_s,
        })
    }

    pub async fn start(&mut self) {
        let docker = self.docker.clone();
        let container_id = self.container_id.clone();
        let stats_arc = self.stats.clone();
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
        self.stop_tx = Some(stop_tx);

        let monitor_task = tokio::spawn(async move {
            let mut stream = docker.stats(&container_id, Some(StatsOptions { stream: true, one_shot: false }));
            let mut stop_rx = stop_rx;

            loop {
                tokio::select! {
                    _ = &mut stop_rx => break,
                    Some(Ok(stats)) = stream.next() => {
                        let mut guard = stats_arc.lock().await;

                        if let (Some(cpu_stats), Some(precpu_stats)) = (&stats.cpu_stats, &stats.precpu_stats) {
                            if let (Some(cpu_usage), Some(precpu_usage)) = (&cpu_stats.cpu_usage, &precpu_stats.cpu_usage) {
                                let cpu_delta = (cpu_usage.total_usage.unwrap_or(0) as f64) - (precpu_usage.total_usage.unwrap_or(0) as f64);
                                let system_delta = (cpu_stats.system_cpu_usage.unwrap_or(0) as f64) - (precpu_stats.system_cpu_usage.unwrap_or(0) as f64);
                                let online_cpus = cpu_stats.online_cpus.unwrap_or(1) as f64;

                                if system_delta > 0.0 && cpu_delta > 0.0 {
                                    let cpu_perc = (cpu_delta / system_delta) * online_cpus * 100.0;
                                    guard.cpu_samples.push(cpu_perc);
                                }
                            }
                        }

                        // Memory usage
                        if let Some(memory_stats) = &stats.memory_stats {
                            let mem_usage = memory_stats.usage.unwrap_or(0);
                            guard.memory_samples.push(mem_usage);
                        }
                    }
                    else => break,
                }
            }
        });

        self.monitor_task = Some(monitor_task);
    }

    pub async fn stop(mut self) -> ContainerMetrics {

        let image_size = match self.get_image_size().await {
            Ok(size) => Some(size),
            Err(e) => {
                eprintln!("Failed to get image size: {}", e);
                None
            },
        };

        if let Some(stop_tx) = self.stop_tx.take() {
            let _ = stop_tx.send(());
        }
        if let Some(task) = self.monitor_task.take() {
            let _ = task.await;
        }

        let guard = self.stats.lock().await;

        let avg_cpu = if !guard.cpu_samples.is_empty() {
            Some(guard.cpu_samples.iter().sum::<f64>() / guard.cpu_samples.len() as f64)
        } else {
            None
        };

        let peak_cpu = guard.cpu_samples.iter().cloned().fold(None, |acc: Option<f64>, x| {
            Some(acc.map_or(x, |curr| if x > curr { x } else { curr }))
        });

        let avg_mem = if !guard.memory_samples.is_empty() {
            Some(guard.memory_samples.iter().sum::<u64>() / guard.memory_samples.len() as u64)
        } else {
            None
        };

        let peak_mem = guard.memory_samples.iter().max().cloned();

        ContainerMetrics{
            image_size_bytes: image_size,
            startup_time_s: self.startup_tims_s,
            avg_cpu_percent: avg_cpu,
            peak_cpu_percent: peak_cpu,
            avg_memory_bytes: avg_mem,
            peak_memory_bytes: peak_mem,
        }
    }

    pub async fn get_image_size(&self) -> Result<u64> {
        let inspect = self.docker.inspect_container(&self.container_id, None).await?;
        let image_id = inspect.image.ok_or_else(|| anyhow::anyhow!("No image ID for container"))?;
        let image_inspect = self.docker.inspect_image(&image_id).await?;
        Ok(image_inspect.size.unwrap_or(0) as u64)
    }
}
