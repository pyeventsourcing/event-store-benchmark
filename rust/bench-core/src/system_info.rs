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
pub fn collect_environment_info() -> Result<EnvironmentInfo> {
    Ok(EnvironmentInfo {
        os: collect_os_info()?,
        cpu: collect_cpu_info()?,
        memory: collect_memory_info()?,
        disk: collect_disk_info()?,
        container_runtime: collect_container_runtime_info()?,
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
            .args(["-n", "hw.ncpu"])
            .output()?;
        let cores: usize = String::from_utf8_lossy(&output_cores.stdout)
            .trim()
            .parse()
            .unwrap_or(1);

        Ok(CpuInfo { model, cores })
    }

    #[cfg(target_os = "linux")]
    {
        let cpuinfo = std::fs::read_to_string("/proc/cpuinfo").unwrap_or_default();
        let mut model = "unknown".to_string();
        let cores = num_cpus::get();

        for line in cpuinfo.lines() {
            if line.starts_with("model name") {
                if let Some(value) = line.split(':').nth(1) {
                    model = value.trim().to_string();
                    break;
                }
            }
        }

        Ok(CpuInfo { model, cores })
    }

    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        Ok(CpuInfo {
            model: "unknown".to_string(),
            cores: num_cpus::get(),
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

        Ok(MemoryInfo { total_bytes })
    }

    #[cfg(target_os = "linux")]
    {
        let meminfo = std::fs::read_to_string("/proc/meminfo").unwrap_or_default();
        let mut total_kb: u64 = 0;

        for line in meminfo.lines() {
            if line.starts_with("MemTotal:") {
                if let Some(value) = line.split_whitespace().nth(1) {
                    total_kb = value.parse().unwrap_or(0);
                    break;
                }
            }
        }

        Ok(MemoryInfo {
            total_bytes: total_kb * 1024,
        })
    }

    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        Ok(MemoryInfo { total_bytes: 0 })
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

fn collect_container_runtime_info() -> Result<ContainerRuntimeInfo> {
    // Try to detect Docker version
    if let Ok(output) = Command::new("docker").args(["--version"]).output() {
        if output.status.success() {
            let version_str = String::from_utf8_lossy(&output.stdout);
            // Parse "Docker version 24.0.6, build ed223bc" to extract "24.0.6"
            let version = version_str
                .split_whitespace()
                .nth(2)
                .unwrap_or("unknown")
                .trim_end_matches(',')
                .to_string();

            return Ok(ContainerRuntimeInfo {
                runtime_type: "docker".to_string(),
                version,
            });
        }
    }

    // Fallback
    Ok(ContainerRuntimeInfo {
        runtime_type: "unknown".to_string(),
        version: "unknown".to_string(),
    })
}
