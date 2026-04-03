import json
from pathlib import Path

import yaml

from .workloads.performance import PerformanceWorkloadResult


def load_raw_run_data(run_dir: Path) -> dict | None:
    """Loads all raw data files for a single run into a dictionary."""
    config_file = run_dir / "config.yaml"
    results_file = run_dir / "results.json"
    metrics_file = run_dir / "metrics.json"
    logs_file = run_dir / "logs.txt"

    if not config_file.exists():
        print(f"Warning: config.yaml not found in {run_dir}")
        return None

    try:
        with open(config_file) as f:
            config_data = yaml.safe_load(f)

        results_data = {}
        if results_file.exists():
            with open(results_file) as f:
                results_data = json.load(f)

        metrics_data = {}
        if metrics_file.exists():
            with open(metrics_file) as f:
                metrics_data = json.load(f)

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