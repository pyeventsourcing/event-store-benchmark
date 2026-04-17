use anyhow::Result;
use bollard::Docker;
use bollard::query_parameters::StatsOptions;
use futures::StreamExt;
use std::sync::Arc;
use tokio::sync::{Mutex, watch};
use tokio::task::JoinHandle;
use crate::metrics::{ProcessMetrics, CpuSample, MemorySample, SamplingConfigDecision};

pub struct ContainerMonitor {
    docker: Docker,
    container_id: String,
    stats: Arc<Mutex<CollectedStats>>,
    stop_tx: Option<tokio::sync::oneshot::Sender<()>>,
    monitor_task: Option<JoinHandle<()>>,
}

#[derive(Default, Clone)]
struct CollectedStats {
    cpu_samples: Vec<CpuSample>,
    memory_samples: Vec<MemorySample>,
}

impl ContainerMonitor {
    pub fn new(container_id: String) -> Result<Self> {
        let docker = Docker::connect_with_local_defaults()?;
        Ok(Self {
            docker,
            container_id,
            stats: Arc::new(Mutex::new(CollectedStats::default())),
            stop_tx: None,
            monitor_task: None,
        })
    }

    pub async fn start(&mut self, mut sampling_config_rx: watch::Receiver<Option<SamplingConfigDecision>>) {
        let docker = self.docker.clone();
        let container_id = self.container_id.clone();
        let stats_arc = self.stats.clone();
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
        self.stop_tx = Some(stop_tx);

        let monitor_task = tokio::spawn(async move {
            loop {
                if sampling_config_rx.borrow().is_some() {
                    break;
                }
                if sampling_config_rx.changed().await.is_err() {
                    return;
                }
            }
            
            let msg = sampling_config_rx.borrow().unwrap();
            let start_time = msg.start_time;
            
            let mut stream = docker.stats(&container_id, Some(StatsOptions { stream: true, one_shot: false }));
            let mut stop_rx = stop_rx;

            loop {
                tokio::select! {
                    _ = &mut stop_rx => break,
                    Some(Ok(stats)) = stream.next() => {
                        let elapsed_s = start_time.elapsed().as_secs_f64();
                        let mut guard = stats_arc.lock().await;

                        if let (Some(cpu_stats), Some(precpu_stats)) = (&stats.cpu_stats, &stats.precpu_stats) {
                            if let (Some(cpu_usage), Some(precpu_usage)) = (&cpu_stats.cpu_usage, &precpu_stats.cpu_usage) {
                                let cpu_delta = (cpu_usage.total_usage.unwrap_or(0) as f64) - (precpu_usage.total_usage.unwrap_or(0) as f64);
                                let system_delta = (cpu_stats.system_cpu_usage.unwrap_or(0) as f64) - (precpu_stats.system_cpu_usage.unwrap_or(0) as f64);
                                let online_cpus = cpu_stats.online_cpus.unwrap_or(1) as f64;

                                if system_delta > 0.0 && cpu_delta > 0.0 {
                                    let cpu_perc = (cpu_delta / system_delta) * online_cpus * 100.0;
                                    guard.cpu_samples.push(CpuSample { elapsed_s, cpu_percent: cpu_perc });
                                }
                            }
                        }

                        // Memory usage
                        if let Some(memory_stats) = &stats.memory_stats {
                            let mem_usage = memory_stats.usage.unwrap_or(0);
                            guard.memory_samples.push(MemorySample { elapsed_s, memory_bytes: mem_usage });
                        }
                    }
                    else => break,
                }
            }
        });

        self.monitor_task = Some(monitor_task);
    }

    pub async fn stop(mut self) -> (ProcessMetrics, Vec<CpuSample>, Vec<MemorySample>) {
        if let Some(stop_tx) = self.stop_tx.take() {
            let _ = stop_tx.send(());
        }
        if let Some(task) = self.monitor_task.take() {
            let _ = task.await;
        }

        let guard = self.stats.lock().await;

        let avg_cpu = if !guard.cpu_samples.is_empty() {
            Some(guard.cpu_samples.iter().map(|s| s.cpu_percent).sum::<f64>() / guard.cpu_samples.len() as f64)
        } else {
            None
        };

        let peak_cpu = guard.cpu_samples.iter().map(|s| s.cpu_percent).fold(None, |acc: Option<f64>, x| {
            Some(acc.map_or(x, |curr| if x > curr { x } else { curr }))
        });

        let avg_mem = if !guard.memory_samples.is_empty() {
            Some(guard.memory_samples.iter().map(|s| s.memory_bytes).sum::<u64>() / guard.memory_samples.len() as u64)
        } else {
            None
        };

        let peak_mem = guard.memory_samples.iter().map(|s| s.memory_bytes).max();

        let metrics = ProcessMetrics {
            avg_cpu_percent: avg_cpu,
            peak_cpu_percent: peak_cpu,
            avg_memory_bytes: avg_mem,
            peak_memory_bytes: peak_mem,
        };

        (metrics, guard.cpu_samples.clone(), guard.memory_samples.clone())
    }

    pub async fn get_image_size(&self) -> Result<u64> {
        let inspect = self.docker.inspect_container(&self.container_id, None).await?;
        let image_id = inspect.image.ok_or_else(|| anyhow::anyhow!("No image ID for container"))?;
        let image_inspect = self.docker.inspect_image(&image_id).await?;
        Ok(image_inspect.size.unwrap_or(0) as u64)
    }
}
