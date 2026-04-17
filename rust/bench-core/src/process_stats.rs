use std::sync::Arc;
use tokio::sync::{Mutex, watch};
use tokio::task::JoinHandle;
use std::time::Duration;
use crate::metrics::{ProcessMetrics, CpuSample, MemorySample, BenchmarkMessage};
use sysinfo::{Pid, System, ProcessRefreshKind, RefreshKind, ProcessesToUpdate};

pub struct ProcessMonitor {
    pid: Pid,
    stats: Arc<Mutex<CollectedStats>>,
    stop_tx: Option<tokio::sync::oneshot::Sender<()>>,
    monitor_task: Option<JoinHandle<()>>,
}

#[derive(Default, Clone)]
struct CollectedStats {
    cpu_samples: Vec<CpuSample>,
    memory_samples: Vec<MemorySample>,
}

impl ProcessMonitor {
    pub fn new(pid_u32: u32) -> Self {
        Self {
            pid: Pid::from(pid_u32 as usize),
            stats: Arc::new(Mutex::new(CollectedStats::default())),
            stop_tx: None,
            monitor_task: None,
        }
    }

    pub async fn start(&mut self, mut benchmark_rx: watch::Receiver<Option<BenchmarkMessage>>) {
        let pid = self.pid;
        let stats_arc = self.stats.clone();
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
        self.stop_tx = Some(stop_tx);

        let monitor_task = tokio::spawn(async move {
            loop {
                if benchmark_rx.borrow().is_some() {
                    break;
                }
                if benchmark_rx.changed().await.is_err() {
                    return;
                }
            }
            
            let msg = benchmark_rx.borrow().unwrap();
            let start_time = msg.start_time;
            
            let mut sys = System::new_with_specifics(
                RefreshKind::nothing().with_processes(ProcessRefreshKind::nothing().with_cpu().with_memory())
            );
            let mut stop_rx = stop_rx;

            loop {
                sys.refresh_processes_specifics(
                    ProcessesToUpdate::Some(&[pid]),
                    true,
                    ProcessRefreshKind::nothing().with_cpu().with_memory()
                );
                
                if let Some(process) = sys.process(pid) {
                    let elapsed_s = start_time.elapsed().as_secs_f64();
                    let mut guard = stats_arc.lock().await;
                    guard.cpu_samples.push(CpuSample { elapsed_s, cpu_percent: process.cpu_usage() as f64 });
                    guard.memory_samples.push(MemorySample { elapsed_s, memory_bytes: process.memory() });
                } else {
                    // Process no longer exists
                    break;
                }

                tokio::select! {
                    _ = &mut stop_rx => break,
                    _ = tokio::time::sleep(Duration::from_millis(500)) => {}
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
}
