use crate::metrics::{ContainerRuntimeInfo, CpuInfo, DiskInfo, EnvironmentInfo, MemoryInfo, OsInfo};
use anyhow::Result;
use std::process::Command;

/// Get the current git commit hash
pub fn get_git_commit_hash() -> Result<String> {
    let output = Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()?;

    if output.status.success() {
        let hash = String::from_utf8(output.stdout)?
            .trim()
            .to_string();
        Ok(hash)
    } else {
        Ok("unknown".to_string())
    }
}

/// Collect system environment information
pub async fn collect_environment_info() -> Result<EnvironmentInfo> {
    Ok(EnvironmentInfo {
        os: collect_os_info()?,
        cpu: collect_cpu_info()?,
        memory: collect_memory_info()?,
        disk: collect_disk_info()?,
        container_runtime: collect_container_runtime_info().await?,
    })
}

fn collect_os_info() -> Result<OsInfo> {
    #[cfg(target_os = "macos")]
    {
        let output = Command::new("uname").arg("-a").output()?;
        let uname_str = String::from_utf8_lossy(&output.stdout);

        let output_version = Command::new("sw_vers").arg("-productVersion").output()?;
        let version = String::from_utf8_lossy(&output_version.stdout).trim().to_string();

        Ok(OsInfo {
            name: "macOS".to_string(),
            version,
            kernel: uname_str.trim().to_string(),
            arch: std::env::consts::ARCH.to_string(),
        })
    }

    #[cfg(target_os = "linux")]
    {
        let output = Command::new("uname").arg("-a").output()?;
        let kernel = String::from_utf8_lossy(&output.stdout).trim().to_string();

        // Try to read /etc/os-release for OS name and version
        let os_release = std::fs::read_to_string("/etc/os-release").unwrap_or_default();
        let mut name = "Linux".to_string();
        let mut version = "unknown".to_string();

        for line in os_release.lines() {
            if line.starts_with("PRETTY_NAME=") {
                name = line.trim_start_matches("PRETTY_NAME=")
                    .trim_matches('"')
                    .to_string();
            } else if line.starts_with("VERSION_ID=") {
                version = line.trim_start_matches("VERSION_ID=")
                    .trim_matches('"')
                    .to_string();
            }
        }

        Ok(OsInfo {
            name,
            version,
            kernel,
            arch: std::env::consts::ARCH.to_string(),
        })
    }

    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        Ok(OsInfo {
            name: std::env::consts::OS.to_string(),
            version: "unknown".to_string(),
            kernel: "unknown".to_string(),
            arch: std::env::consts::ARCH.to_string(),
        })
    }
}

fn collect_cpu_info() -> Result<CpuInfo> {
    #[cfg(target_os = "macos")]
    {
        let output = Command::new("sysctl")
            .args(["-n", "machdep.cpu.brand_string"])
            .output()?;
        let model = String::from_utf8_lossy(&output.stdout).trim().to_string();

        let output_cores = Command::new("sysctl")
            .args(["-n", "hw.physicalcpu"])
            .output()?;
        let cores: usize = String::from_utf8_lossy(&output_cores.stdout)
            .trim()
            .parse()
            .unwrap_or(1);

        let output_threads = Command::new("sysctl")
            .args(["-n", "hw.logicalcpu"])
            .output()?;
        let threads: usize = String::from_utf8_lossy(&output_threads.stdout)
            .trim()
            .parse()
            .unwrap_or(1);

        Ok(CpuInfo { model, cores, threads })
    }

    #[cfg(target_os = "linux")]
    {
        let cpuinfo = std::fs::read_to_string("/proc/cpuinfo").unwrap_or_default();
        let mut model = "unknown".to_string();
        let threads = num_cpus::get();
        let mut cores = threads; // Fallback if we can't find physical cores

        for line in cpuinfo.lines() {
            if line.starts_with("model name") {
                if let Some(value) = line.split(':').nth(1) {
                    model = value.trim().to_string();
                }
            } else if line.starts_with("cpu cores") {
                 if let Some(value) = line.split(':').nth(1) {
                    if let Ok(c) = value.trim().parse::<usize>() {
                         cores = c;
                    }
                }
            }
        }

        Ok(CpuInfo { model, cores, threads })
    }

    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        let threads = num_cpus::get();
        Ok(CpuInfo {
            model: "unknown".to_string(),
            cores: threads,
            threads,
        })
    }
}

fn collect_memory_info() -> Result<MemoryInfo> {
    #[cfg(target_os = "macos")]
    {
        let output = Command::new("sysctl")
            .args(["-n", "hw.memsize"])
            .output()?;
        let total_bytes: u64 = String::from_utf8_lossy(&output.stdout)
            .trim()
            .parse()
            .unwrap_or(0);

        // Get free memory on macOS is complex, we can use vm_stat or just use sysinfo if we had it.
        // For now let's just use vm_stat and parse it simply if possible, or leave it as 0 if it fails.
        let mut available_bytes = 0;
        if let Ok(output) = Command::new("vm_stat").output() {
            let vm_stat = String::from_utf8_lossy(&output.stdout);
            let mut free_pages: u64 = 0;
            let mut speculative_pages: u64 = 0;
            let mut page_size: u64 = 4096; // Default
            
            // Try to get page size
            if let Ok(ps_out) = Command::new("sysctl").args(["-n", "hw.pagesize"]).output() {
                page_size = String::from_utf8_lossy(&ps_out.stdout).trim().parse().unwrap_or(4096);
            }

            for line in vm_stat.lines() {
                if line.starts_with("Pages free:") {
                    free_pages = line.split_whitespace().last().and_then(|s| s.trim_end_matches('.').parse().ok()).unwrap_or(0);
                } else if line.starts_with("Pages speculative:") {
                    speculative_pages = line.split_whitespace().last().and_then(|s| s.trim_end_matches('.').parse().ok()).unwrap_or(0);
                }
            }
            available_bytes = (free_pages + speculative_pages) * page_size;
        }

        Ok(MemoryInfo { total_bytes, available_bytes })
    }

    #[cfg(target_os = "linux")]
    {
        let meminfo = std::fs::read_to_string("/proc/meminfo").unwrap_or_default();
        let mut total_kb: u64 = 0;
        let mut available_kb: u64 = 0;

        for line in meminfo.lines() {
            if line.starts_with("MemTotal:") {
                if let Some(value) = line.split_whitespace().nth(1) {
                    total_kb = value.parse().unwrap_or(0);
                }
            } else if line.starts_with("MemAvailable:") {
                if let Some(value) = line.split_whitespace().nth(1) {
                    available_kb = value.parse().unwrap_or(0);
                }
            }
        }

        Ok(MemoryInfo {
            total_bytes: total_kb * 1024,
            available_bytes: available_kb * 1024,
        })
    }

    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        Ok(MemoryInfo { total_bytes: 0, available_bytes: 0 })
    }
}

fn collect_disk_info() -> Result<DiskInfo> {
    #[cfg(target_os = "macos")]
    {
        let output = Command::new("df")
            .args(["-T", "."])
            .output()?;
        let df_output = String::from_utf8_lossy(&output.stdout);
        let filesystem = df_output
            .lines()
            .nth(1)
            .and_then(|line| line.split_whitespace().nth(1))
            .unwrap_or("unknown")
            .to_string();

        Ok(DiskInfo {
            disk_type: "NVMe".to_string(), // Hardcoded for now, could parse system_profiler
            filesystem,
        })
    }

    #[cfg(target_os = "linux")]
    {
        let output = Command::new("df")
            .args(["-T", "."])
            .output()?;
        let df_output = String::from_utf8_lossy(&output.stdout);
        let filesystem = df_output
            .lines()
            .nth(1)
            .and_then(|line| line.split_whitespace().nth(1))
            .unwrap_or("unknown")
            .to_string();

        Ok(DiskInfo {
            disk_type: "SSD".to_string(), // Could be improved with more detection
            filesystem,
        })
    }

    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        Ok(DiskInfo {
            disk_type: "unknown".to_string(),
            filesystem: "unknown".to_string(),
        })
    }
}

async fn collect_container_runtime_info() -> Result<ContainerRuntimeInfo> {
    // Try to detect Docker using bollard
    let docker_info = async {
        let docker = bollard::Docker::connect_with_local_defaults()?;
        docker.info().await
    }.await;

    if let Ok(info) = docker_info {
        return Ok(ContainerRuntimeInfo {
            runtime_type: "docker".to_string(),
            version: info.server_version.unwrap_or_else(|| "unknown".to_string()),
            ncpu: info.ncpu.unwrap_or(0) as usize,
            mem_total: info.mem_total.unwrap_or(0) as u64,
        });
    }

    // Fallback if bollard fails
    Ok(ContainerRuntimeInfo {
        runtime_type: "unknown".to_string(),
        version: "unknown".to_string(),
        ncpu: 0,
        mem_total: 0,
    })
}
