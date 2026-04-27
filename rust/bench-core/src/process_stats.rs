use std::sync::Arc;
use tokio::sync::{Mutex, watch};
use tokio::task::JoinHandle;
use std::time::{Duration, Instant};
#[cfg(target_os = "linux")]
use std::path::{Path, PathBuf};
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

#[derive(Clone, Copy)]
pub enum MonitoringScope {
    RootOnly,
    RootPlusDescendants,
    LinuxCgroupOfRoot,
}

#[cfg(target_os = "linux")]
#[derive(Clone)]
struct LinuxCgroupState {
    cpu_stat_path: PathBuf,
    memory_current_path: PathBuf,
    last_cpu_usage_usec: Option<u64>,
    last_cpu_sample_at: Option<Instant>,
}

#[cfg(target_os = "linux")]
impl LinuxCgroupState {
    fn from_root_pid(root_pid: Pid) -> Option<Self> {
        let cgroup_relative = read_unified_v2_cgroup_path(root_pid)?;
        let cgroup_dir = Path::new("/sys/fs/cgroup").join(cgroup_relative.trim_start_matches('/'));
        let cpu_stat_path = cgroup_dir.join("cpu.stat");
        let memory_current_path = cgroup_dir.join("memory.current");

        if !cpu_stat_path.exists() || !memory_current_path.exists() {
            return None;
        }

        Some(Self {
            cpu_stat_path,
            memory_current_path,
            last_cpu_usage_usec: None,
            last_cpu_sample_at: None,
        })
    }

    fn read_cpu_usage_usec(&self) -> Option<u64> {
        let content = std::fs::read_to_string(&self.cpu_stat_path).ok()?;
        for line in content.lines() {
            let mut parts = line.split_whitespace();
            if parts.next() == Some("usage_usec") {
                return parts.next()?.parse::<u64>().ok();
            }
        }
        None
    }

    fn read_memory_current_bytes(&self) -> Option<u64> {
        std::fs::read_to_string(&self.memory_current_path)
            .ok()?
            .trim()
            .parse::<u64>()
            .ok()
    }

    fn sample(&mut self, now: Instant) -> Option<(Option<f64>, u64)> {
        let cpu_usage_usec_now = self.read_cpu_usage_usec()?;
        let memory_bytes = self.read_memory_current_bytes()?;

        let cpu_percent = if let (Some(prev_usage), Some(prev_time)) = (self.last_cpu_usage_usec, self.last_cpu_sample_at) {
            let delta_cpu = cpu_usage_usec_now.saturating_sub(prev_usage) as f64;
            let delta_wall = now.duration_since(prev_time).as_secs_f64() * 1_000_000.0;
            if delta_wall > 0.0 {
                Some((delta_cpu / delta_wall) * 100.0)
            } else {
                None
            }
        } else {
            None
        };

        self.last_cpu_usage_usec = Some(cpu_usage_usec_now);
        self.last_cpu_sample_at = Some(now);

        Some((cpu_percent, memory_bytes))
    }
}

#[cfg(target_os = "linux")]
fn read_unified_v2_cgroup_path(root_pid: Pid) -> Option<String> {
    let cgroup_file = format!("/proc/{}/cgroup", root_pid.as_u32());
    let content = std::fs::read_to_string(cgroup_file).ok()?;
    for line in content.lines() {
        let mut parts = line.splitn(3, ':');
        let hierarchy_id = parts.next()?;
        let controllers = parts.next()?;
        let path = parts.next()?;
        if hierarchy_id == "0" && controllers.is_empty() {
            return Some(path.to_string());
        }
    }
    None
}

pub struct ProcessMonitor {
    pid: Pid,
    scope: MonitoringScope,
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
    pub fn new(pid_u32: u32, scope: MonitoringScope) -> Self {
        Self {
            pid: Pid::from(pid_u32 as usize),
            scope,
            stats: Arc::new(Mutex::new(CollectedStats::default())),
            stop_tx: None,
            monitor_task: None,
        }
    }

    pub async fn start(&mut self, mut sampling_config_rx: watch::Receiver<Option<SamplingConfigDecision>>) {
        let pid = self.pid;
        let scope = self.scope;
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

            #[cfg(target_os = "linux")]
            let mut cgroup_state = if matches!(scope, MonitoringScope::LinuxCgroupOfRoot) {
                LinuxCgroupState::from_root_pid(pid)
            } else {
                None
            };

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
                
                if sys.process(pid).is_none() {
                    // Process no longer exists
                    break;
                }

                let elapsed_s = (next_sample_time - start_time).as_secs_f64();

                #[cfg(target_os = "linux")]
                if matches!(scope, MonitoringScope::LinuxCgroupOfRoot) {
                    if let Some(state) = cgroup_state.as_mut() {
                        if let Some((cpu_percent_opt, memory_bytes)) = state.sample(Instant::now()) {
                            let mut guard = stats_arc.lock().await;
                            if let Some(cpu_percent) = cpu_percent_opt {
                                guard.cpu_samples.push(CpuSample { elapsed_s, cpu_percent });
                            }
                            guard.memory_samples.push(MemorySample { elapsed_s, memory_bytes });

                            sample_count += 1;
                            if Instant::now() >= end_time {
                                break;
                            }
                            continue;
                        }
                    }
                }

                let tracked_pids = match scope {
                    MonitoringScope::RootOnly | MonitoringScope::LinuxCgroupOfRoot => vec![pid],
                    MonitoringScope::RootPlusDescendants => collect_descendants_including_root(&sys, pid),
                };
                let (total_cpu, total_memory) = tracked_pids
                    .iter()
                    .filter_map(|tracked_pid| sys.process(*tracked_pid))
                    .fold((0.0_f64, 0_u64), |(cpu_acc, mem_acc), process| {
                        (
                            cpu_acc + process.cpu_usage() as f64,
                            mem_acc + get_process_memory(process),
                        )
                    });

                let mut guard = stats_arc.lock().await;
                guard.cpu_samples.push(CpuSample { elapsed_s, cpu_percent: total_cpu });
                guard.memory_samples.push(MemorySample { elapsed_s, memory_bytes: total_memory });

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
