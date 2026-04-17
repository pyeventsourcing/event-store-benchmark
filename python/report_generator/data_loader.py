import json
from pathlib import Path
from typing import Optional

import yaml

from .workloads.performance import PerformanceWorkloadResult
from .environment_info import EnvironmentInfo, OsInfo, CpuInfo, MemoryInfo, FsyncStats, DiskInfo, ContainerRuntimeInfo


def _load_environment_info(env_data: dict) -> Optional[EnvironmentInfo]:
    """Loads environment info from a dictionary into the EnvironmentInfo dataclass."""
    if not env_data:
        return None

    try:
        os_info = OsInfo(**env_data.get("os", {}))
        cpu_info = CpuInfo(**env_data.get("cpu", {}))
        memory_info = MemoryInfo(**env_data.get("memory", {}))

        fsync_stats_data = env_data.get("disk", {}).get("fsync_latency")
        fsync_stats = FsyncStats(**fsync_stats_data) if fsync_stats_data else None
        disk_info = DiskInfo(
            disk_type=env_data.get("disk", {}).get("type", "unknown"),
            filesystem=env_data.get("disk", {}).get("filesystem", "unknown"),
            fsync_latency=fsync_stats,
        )

        container_runtime_info = ContainerRuntimeInfo(
            runtime_type=env_data.get("container_runtime", {}).get("type", "unknown"),
            version=env_data.get("container_runtime", {}).get("version", "unknown"),
            ncpu=env_data.get("container_runtime", {}).get("ncpu", 0),
            mem_total=env_data.get("container_runtime", {}).get("mem_total", 0),
        )

        return EnvironmentInfo(
            os=os_info,
            cpu=cpu_info,
            memory=memory_info,
            disk=disk_info,
            container_runtime=container_runtime_info,
        )
    except Exception as e:
        print(f"Warning: Failed to parse environment info: {e}")
        return None


def load_raw_run_data(run_dir: Path) -> dict | None:
    """Loads all raw data files for a single run into a dictionary."""
    config_file = run_dir / "config.yaml"
    throughput_file = run_dir / "throughput.json"
    latency_file = run_dir / "latency.json"
    cpu_file = run_dir / "cpu.json"
    memory_file = run_dir / "memory.json"
    metrics_file = run_dir / "process_metrics.json"

    container_stats_file = run_dir / "container_stats.json"
    logs_file = run_dir / "logs.txt"

    if not config_file.exists():
        print(f"Warning: config.yaml not found in {run_dir}")
        return None

    try:
        with open(config_file) as f:
            config_data = yaml.safe_load(f)

        results_data = {}
        if throughput_file.exists():
            with open(throughput_file) as f:
                results_data["throughput_samples"] = json.load(f)
        
        if latency_file.exists():
            with open(latency_file) as f:
                results_data["latency_percentiles"] = json.load(f)

        if cpu_file.exists():
            with open(cpu_file) as f:
                results_data["cpu_samples"] = json.load(f)

        if memory_file.exists():
            with open(memory_file) as f:
                results_data["memory_samples"] = json.load(f)

        metrics_data = {}
        if metrics_file.exists():
            with open(metrics_file) as f:
                metrics_data = json.load(f)

        # Merge container stats if present
        if container_stats_file.exists():
            with open(container_stats_file) as f:
                container_data = json.load(f)
                metrics_data.update(container_data)

        container_logs = ""
        if logs_file.exists():
            with open(logs_file, "r", errors="replace") as f:
                container_logs = f.read()

        return {
            "config": config_data,
            "results": results_data,
            "metrics": metrics_data,
            "logs": container_logs,
        }
    except Exception as e:
        print(f"Warning: Failed to load run data at {run_dir}: {e}")
        return None


def load_session_workloads(session_dir: Path):
    """
    Loads all runs from a session, groups them by workload, and returns
    a dictionary of workload-specific result objects.
    """
    session_config_file = session_dir / "config.yaml"
    if not session_config_file.exists():
        print(f"Warning: No config.yaml found in session {session_dir}")
        return {}

    workloads = {}
    with open(session_config_file, "r") as f:
        session_configs = list(yaml.safe_load_all(f))

    for workload_config_doc in session_configs:
        if 'performance' in workload_config_doc:
            perf_cfg = workload_config_doc['performance']
            workload_name = perf_cfg.get('name')
            if not workload_name:
                continue

            workload_dir = session_dir / workload_name
            runs = []
            if workload_dir.exists() and workload_dir.is_dir():
                for run_dir in workload_dir.iterdir():
                    if run_dir.is_dir():
                        raw_data = load_raw_run_data(run_dir)
                        if raw_data:
                            runs.append(PerformanceWorkloadResult(raw_data, run_dir))
            workloads[workload_name] = {"config": perf_cfg, "runs": runs}

    return workloads


def load_session_metadata(session_dir: Path) -> dict:
    """Loads session.json, environment.json, and config.yaml for a given session."""
    session_info = {}
    env_info_obj = None
    session_configs = []

    # Load session.json
    try:
        with open(session_dir / "session.json", "r") as f:
            session_info = json.load(f)
    except FileNotFoundError:
        pass
    except Exception as e:
        print(f"Warning: Could not load session.json for {session_dir.name}: {e}")

    # Load environment.json
    try:
        with open(session_dir / "environment.json", "r") as f:
            raw_env_info = json.load(f)
            env_info_obj = _load_environment_info(raw_env_info)
    except FileNotFoundError:
        pass
    except Exception as e:
        print(f"Warning: Could not load environment.json for {session_dir.name}: {e}")

    # Load config.yaml
    try:
        session_config_file = session_dir / "config.yaml"
        if session_config_file.exists():
            with open(session_config_file, "r") as f:
                session_configs = list(yaml.safe_load_all(f))
    except Exception as e:
        print(f"Warning: Could not load config.yaml for {session_dir.name}: {e}")

    return {
        "session_info": session_info,
        "env_info": env_info_obj,
        "session_configs": session_configs,
    }