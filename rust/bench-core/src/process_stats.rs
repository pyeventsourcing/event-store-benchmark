use std::sync::Arc;
use tokio::sync::{Mutex, watch};
use tokio::task::JoinHandle;
use std::time::{Duration, Instant};
use crate::metrics::{ProcessMetrics, CpuSample, MemorySample, SamplingConfigDecision};
use sysinfo::{Pid, System, ProcessRefreshKind, RefreshKind, ProcessesToUpdate, Process};

use memory_stats::memory_stats;

fn get_process_memory(process: &Process) -> u64 {
    // If the process is the current one, use memory_stats for better accuracy
    if process.pid().as_u32() == std::process::id() {
        if let Some(usage) = memory_stats() {
            return usage.physical_mem as u64;
        }
    }
    process.memory()
}

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

    pub async fn start(&mut self, mut sampling_config_rx: watch::Receiver<Option<SamplingConfigDecision>>) {
        let pid = self.pid;
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
            let samples_per_second = msg.samples_per_second;
            let duration_seconds = msg.duration_seconds;
            let interval = Duration::from_secs_f64(1.0 / samples_per_second as f64)
                .max(sysinfo::MINIMUM_CPU_UPDATE_INTERVAL * 2);
            let end_time = start_time + Duration::from_secs(duration_seconds);

            let expected_samples = (samples_per_second * duration_seconds) as usize;
            {
                let mut guard = stats_arc.lock().await;
                guard.cpu_samples = Vec::with_capacity(expected_samples);
                guard.memory_samples = Vec::with_capacity(expected_samples);
            }
            
            let mut sys = System::new_with_specifics(
                RefreshKind::nothing().with_processes(ProcessRefreshKind::nothing().with_cpu().with_memory())
            );
            
            // Initial refresh to establish baseline for CPU usage
            sys.refresh_processes_specifics(
                ProcessesToUpdate::Some(&[pid]),
                true,
                ProcessRefreshKind::nothing().with_cpu().with_memory()
            );

            let mut stop_rx = stop_rx;
            let mut sample_count = 1;

            loop {
                let next_sample_time = start_time + interval.mul_f64(sample_count as f64);
                let now = Instant::now();
                
                if next_sample_time > now {
                    tokio::select! {
                        _ = &mut stop_rx => break,
                        _ = tokio::time::sleep(next_sample_time - now) => {}
                    }
                } else if now >= end_time {
                    break;
                }

                // Use refresh_processes_specifics to update CPU and memory for the target PID
                sys.refresh_processes_specifics(
                    ProcessesToUpdate::Some(&[pid]),
                    false, // Use false for subsequent refreshes to allow delta calculation
                    ProcessRefreshKind::nothing().with_cpu().with_memory()
                );
                
                if let Some(process) = sys.process(pid) {
                    let elapsed_s = (next_sample_time - start_time).as_secs_f64();
                    let mut guard = stats_arc.lock().await;
                    guard.cpu_samples.push(CpuSample { elapsed_s, cpu_percent: process.cpu_usage() as f64 });
                    guard.memory_samples.push(MemorySample { elapsed_s, memory_bytes: get_process_memory(process) });
                } else {
                    // Process no longer exists
                    break;
                }

                sample_count += 1;

                if Instant::now() >= end_time {
                    break;
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
