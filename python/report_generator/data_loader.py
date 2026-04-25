import json
from pathlib import Path
from typing import Any, Dict, NamedTuple

import yaml

from .models import EnvironmentInfo, RawPerformanceWorkloadRunResults, PerformanceWorkflowSamples, SessionInfo
from .workloads.performance import PerformanceWorkloadRun


def load_raw_performance_workload_run_results(run_dir: Path) -> RawPerformanceWorkloadRunResults | None:
    """Loads all raw data files for a single run into a RunData model."""
    config_file = run_dir / "config.yaml"
    throughput_file = run_dir / "throughput.json"
    latency_file = run_dir / "latency.json"
    cpu_file = run_dir / "cpu.json"
    memory_file = run_dir / "memory.json"

    tool_latency_file = run_dir / "tool_latency.json"
    tool_cpu_file = run_dir / "tool_cpu.json"
    tool_memory_file = run_dir / "tool_memory.json"

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

        if tool_latency_file.exists():
            with open(tool_latency_file) as f:
                results_data["tool_latency_percentiles"] = json.load(f)

        if tool_cpu_file.exists():
            with open(tool_cpu_file) as f:
                results_data["tool_cpu_samples"] = json.load(f)

        if tool_memory_file.exists():
            with open(tool_memory_file) as f:
                results_data["tool_memory_samples"] = json.load(f)

        metrics_data = {}
        # Merge container stats if present
        if container_stats_file.exists():
            with open(container_stats_file) as f:
                container_data = json.load(f)
                metrics_data.update(container_data)

        container_logs = ""
        if logs_file.exists():
            with open(logs_file, "r", errors="replace") as f:
                container_logs = f.read()

        return RawPerformanceWorkloadRunResults(
            config=config_data,
            results=PerformanceWorkflowSamples.model_validate(results_data),
            metrics=metrics_data,
            logs=container_logs,
        )
    except Exception as e:
        print(f"Warning: Failed to load run data at {run_dir}: {e}")
        return None


def load_session_workloads(raw_session_dir: Path) -> Dict[str, Any]:
    """
    Loads all runs from a session, groups them by workload, and returns
    a dictionary of workload-specific result objects.
    """
    session_config_file = raw_session_dir / "config.yaml"
    if not session_config_file.exists():
        print(f"Warning: No config.yaml found in session {raw_session_dir}")
        return {}

    workloads = {}
    with open(session_config_file, "r") as f:
        session_configs = list(yaml.safe_load_all(f))

    for workload_config in session_configs:
        if 'performance' in workload_config:
            performance_workload_config = workload_config['performance']
            workload_name = performance_workload_config.get('name')
            if not workload_name:
                continue

            raw_workload_dir = raw_session_dir / workload_name
            runs: list[PerformanceWorkloadRun] = []
            if raw_workload_dir.exists() and raw_workload_dir.is_dir():
                for raw_run_dir in raw_workload_dir.iterdir():
                    if raw_run_dir.is_dir():
                        raw_run_results = load_raw_performance_workload_run_results(raw_run_dir)
                        if raw_run_results is not None:
                            runs.append(PerformanceWorkloadRun(raw_run_results, raw_run_dir))
            workloads[workload_name] = {"config": performance_workload_config, "runs": runs}

    return workloads

class SessionMetadata(NamedTuple):
    session_info: SessionInfo
    environment_info: EnvironmentInfo | None
    session_configs: list[Any]

def load_session_metadata(session_dir: Path) -> SessionMetadata:
    """Loads session.json, environment.json, and config.yaml for a given session."""
    session_configs = []

    # Load session.json
    session_info = load_session_info(session_dir)

    # Load environment.json
    environment_info = load_environment_info(session_dir)

    # Load config.yaml
    try:
        session_config_file = session_dir / "config.yaml"
        if session_config_file.exists():
            with open(session_config_file, "r") as f:
                session_configs = list(yaml.safe_load_all(f))
    except Exception as e:
        print(f"Warning: Could not load config.yaml for {session_dir.name}: {e}")

    return SessionMetadata(session_info, environment_info, session_configs)


def load_environment_info(session_dir: Path) -> EnvironmentInfo | None:
    environment_info_path = session_dir / "environment.json"
    try:
        with open(environment_info_path, "r") as f:
            try:
                return EnvironmentInfo.model_validate(json.load(f))
            except Exception as e:
                print(f"Warning: Failed to parse environment info from {environment_info_path}: {e}")
    except FileNotFoundError:
        pass
    except Exception as e:
        print(f"Warning: Failed to open {environment_info_path}: {e}")
    return None


def load_session_info(session_dir: Path) -> SessionInfo | None:
    file_path = session_dir / "session.json"
    try:
        with open(file_path, "r") as f:
            try:
                return SessionInfo.model_validate(json.load(f))
            except Exception as e:
                print(f"Warning: Failed to parse session metadata from {file_path}: {e}")
    except FileNotFoundError:
        pass
    except Exception as e:
        print(f"Warning: Failed to open {file_path}: {e}")
    return None
