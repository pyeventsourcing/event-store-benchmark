use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use crate::metrics::{ProcessMetrics};
use sysinfo::{Pid, System};
use std::time::Duration;

pub struct ProcessMonitor {
    pid: Pid,
    stats: Arc<Mutex<CollectedStats>>,
    stop_tx: Option<tokio::sync::oneshot::Sender<()>>,
    monitor_task: Option<JoinHandle<()>>,
}

#[derive(Default, Clone)]
struct CollectedStats {
    cpu_samples: Vec<f64>,
    memory_samples: Vec<u64>,
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

    pub async fn start(&mut self) {
        let pid = self.pid;
        let stats_arc = self.stats.clone();
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
        self.stop_tx = Some(stop_tx);

        let monitor_task = tokio::spawn(async move {
            let mut sys = System::new();
            let mut stop_rx = stop_rx;

            loop {
                sys.refresh_processes(sysinfo::ProcessesToUpdate::Some(&[pid]), true);
                
                if let Some(process) = sys.process(pid) {
                    let mut guard = stats_arc.lock().await;
                    guard.cpu_samples.push(process.cpu_usage() as f64);
                    guard.memory_samples.push(process.memory());
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

    pub async fn stop(mut self) -> ProcessMetrics {
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

        ProcessMetrics {
            avg_cpu_percent: avg_cpu,
            peak_cpu_percent: peak_cpu,
            avg_memory_bytes: avg_mem,
            peak_memory_bytes: peak_mem,
        }
    }
}
