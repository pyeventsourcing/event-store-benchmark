use std::sync::Arc;
use tokio::sync::{Mutex, watch};
use tokio::task::JoinHandle;
use std::time::{Duration, Instant};
use crate::metrics::{CpuSample, MemorySample, SamplingConfigDecision};
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

fn collect_descendants_including_root(sys: &System, root_pid: Pid) -> Vec<Pid> {
    let mut pids = vec![root_pid];
    let mut cursor = 0;

    while cursor < pids.len() {
        let parent = pids[cursor];
        for (pid, process) in sys.processes() {
            if process.parent() == Some(parent) && !pids.contains(pid) {
                pids.push(*pid);
            }
        }
        cursor += 1;
    }

    pids
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
                ProcessesToUpdate::All,
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

                // Refresh all processes so we can aggregate root + descendants.
                sys.refresh_processes_specifics(
                    ProcessesToUpdate::All,
                    false, // Use false for subsequent refreshes to allow delta calculation
                    ProcessRefreshKind::nothing().with_cpu().with_memory()
                );
                
                if sys.process(pid).is_some() {
                    let tracked_pids = collect_descendants_including_root(&sys, pid);
                    let (total_cpu, total_memory) = tracked_pids
                        .iter()
                        .filter_map(|tracked_pid| sys.process(*tracked_pid))
                        .fold((0.0_f64, 0_u64), |(cpu_acc, mem_acc), process| {
                            (
                                cpu_acc + process.cpu_usage() as f64,
                                mem_acc + get_process_memory(process),
                            )
                        });

                    let elapsed_s = (next_sample_time - start_time).as_secs_f64();
                    let mut guard = stats_arc.lock().await;
                    guard.cpu_samples.push(CpuSample { elapsed_s, cpu_percent: total_cpu });
                    guard.memory_samples.push(MemorySample { elapsed_s, memory_bytes: total_memory });
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

    pub async fn stop(mut self) -> (Option<Vec<CpuSample>>, Option<Vec<MemorySample>>) {
        if let Some(stop_tx) = self.stop_tx.take() {
            let _ = stop_tx.send(());
        }
        if let Some(task) = self.monitor_task.take() {
            let _ = task.await;
        }

        let guard = self.stats.lock().await;

        (Some(guard.cpu_samples.clone()), Some(guard.memory_samples.clone()))
    }
}
