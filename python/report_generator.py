import argparse
import json
import re
import yaml
from collections import defaultdict
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.ticker import LogLocator, NullFormatter, ScalarFormatter, FormatStrFormatter

# Consistent color scheme for all adapters across all plots
# Using standard data visualization colors for better clarity
ADAPTER_COLORS = {
    'umadb': '#d62728',        # Red
    'kurrentdb': '#1f77b4',    # Blue
    'axonserver': '#2ca02c',   # Green
    'eventsourcingdb': '#ff7f0e',  # Orange
    'dummy': '#888888',        # Grey
}

def get_adapter_color(adapter_name):
    """Get consistent color for an adapter."""
    return ADAPTER_COLORS.get(adapter_name, '#cccccc')


def load_session_results(session_dir: Path):
    """
    Load runs from a session and group them by workload.
    Returns a dictionary mapping workload name to a dictionary containing 'config' and 'runs'.
    """
    session_config_file = session_dir / "config.yaml"
    if not session_config_file.exists():
        print(f"Warning: No config.yaml found in session {session_dir}")
        return {}

    session_results = []
    try:
        with open(session_config_file, "r") as f:
            session_configs = list(yaml.safe_load_all(f))

        for run_config in session_configs:
            # Check if it's a performance workload config
            if 'performance' in run_config:
                perf_cfg = run_config['performance']
                base_name = perf_cfg.get('name')
                if not base_name:
                    continue

                workload_dir = session_dir / base_name
                runs = []
                if workload_dir.exists() and workload_dir.is_dir():
                    # Find all run subdirectories in this workload directory
                    for run_dir in workload_dir.iterdir():
                        if run_dir.is_dir():
                            try:
                                run = load_performance_run(run_dir)
                                if run:
                                    runs.append(run)
                            except Exception as e:
                                print(f"Warning: Failed to load run at {run_dir}: {e}")
                else:
                    print(f"Warning: Workload directory {workload_dir} not found")

                session_results.append({
                    "workload_type": "performance",
                    "config": perf_cfg,
                    "runs": runs
                })
            else:
                # Potential future support for other workload types
                print(f"Info: Skipping non-performance workload document in {session_config_file}")

    except Exception as e:
        print(f"Error reading session config {session_config_file}: {e}")

    return session_results


def load_performance_run(run_dir: Path):
    """Parse a single run directory."""
    config_file = run_dir / "config.yaml"
    results_file = run_dir / "results.json"
    metrics_file = run_dir / "metrics.json"

    # Load expanded workload config from YAML
    if not config_file.exists():
        print(f"Warning: config.yaml not found in {run_dir}")
        return None

    with open(config_file) as f:
        config_data = yaml.safe_load(f)

    # Load results
    results_data = {}
    if results_file.exists():
        with open(results_file) as f:
            results_data = json.load(f)

    # Load metrics
    metrics_data = {}
    if metrics_file.exists():
        with open(metrics_file) as f:
            metrics_data = json.load(f)

    # Read logs if available
    container_logs = ""
    logs_file = run_dir / "logs.txt"
    if logs_file.exists():
        with open(logs_file, "r", errors="replace") as f:
            container_logs = f.read()

    # Extract adapter and concurrency from config_data
    stores = config_data.get("stores")
    adapter = "unknown"
    if isinstance(stores, dict):
        # Handle cases where it might be serialized as enum (StoreValue::Single)
        adapter = stores.get("Single", "unknown")
    elif isinstance(stores, list) and len(stores) > 0:
        adapter = stores[0]
    elif stores is not None:
        adapter = str(stores)

    concurrency = config_data.get("concurrency", {})
    writers = 0
    readers = 0
    if isinstance(concurrency, dict):
        w_val = concurrency.get("writers", 0)
        if isinstance(w_val, dict):
            writers = w_val.get("Single", 0)
        elif isinstance(w_val, list) and len(w_val) > 0:
            writers = w_val[0]
        else:
            writers = w_val

        r_val = concurrency.get("readers", 0)
        if isinstance(r_val, dict):
            readers = r_val.get("Single", 0)
        elif isinstance(r_val, list) and len(r_val) > 0:
            readers = r_val[0]
        else:
            readers = r_val
    elif isinstance(concurrency, list):
         # Very old format
         pass

    return {
        "run_path": run_dir,
        "workload_name": config_data.get("name", "unknown"),
        "adapter": adapter,
        "writers": writers,
        "readers": readers,
        "container": metrics_data,
        "throughput_samples": results_data.get("throughput_samples", []),
        "latency_percentiles": results_data.get("latency_percentiles", []),
        "container_logs": container_logs,
    }


def plot_latency_cdf_from_json(run, out_path: Path):
    """Plot latency CDF from run latency data."""
    percentiles_data = run["latency_percentiles"]

    if percentiles_data is None or len(percentiles_data) == 0:
        return False

    # Extract percentiles and latencies
    percentiles = [p["percentile"] for p in percentiles_data]
    latencies_ms = [p["latency_us"] / 1000.0 for p in percentiles_data]

    plt.figure(figsize=(6, 4))
    plt.plot(latencies_ms, percentiles, label="append latency CDF", linewidth=2)
    plt.xscale("log")
    plt.xlabel("Latency (ms) [log]")
    plt.ylabel("Percentile (%)")
    # Use FormatStrFormatter for decimals on latency axis
    formatter = plt.FormatStrFormatter('%.1f')
    plt.gca().xaxis.set_major_formatter(formatter)
    plt.gca().xaxis.set_minor_formatter(plt.NullFormatter())
    plt.grid(True, which="both", ls=":", alpha=0.6)
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()
    return True


def compute_throughput_timeseries(throughput_samples: pd.DataFrame):
    """Compute throughput time series from throughput samples.

    The format has samples with (elapsed_s, count) where count is cumulative.
    We calculate throughput by computing differences between consecutive samples
    and applying a moving average for smoothing.

    Args:
        throughput_samples: DataFrame with 'elapsed_s' (time from start) and 'count' (cumulative count)

    Returns:
        dict with 'time_s', 'throughput_eps', and 'throughput_eps_smooth' arrays
        or None if no valid data
    """
    if throughput_samples.empty or "count" not in throughput_samples.columns:
        return None

    df = throughput_samples.copy()

    if len(df) < 2:
        return None

    # Sort by elapsed time
    df = df.sort_values("elapsed_s").reset_index(drop=True)

    # Calculate time differences and count differences
    time_diffs = df["elapsed_s"].diff().iloc[1:]
    count_diffs = df["count"].diff().iloc[1:]

    # Calculate throughput (events per second) for each interval
    eps = count_diffs / time_diffs

    # Apply moving average smoothing (window size 3 for 1-second samples)
    window_size = min(3, len(eps))
    eps_smooth = eps.rolling(window=window_size, center=True, min_periods=1).mean()

    # Time points (use the end time of each interval)
    time_s = df["elapsed_s"].iloc[1:]

    # Add a point at t=0 with 0 throughput to make the stepwise plot look better if it starts from 0
    # and we want to see the first interval.
    # However, if we use steps-pre, the value at t1 will be used for [t0, t1].
    # If the first sample is at t0=0, then the first interval is [0, t1].
    
    # We should ensure we have a point at the very beginning of the first interval if we want to show it.
    # df["elapsed_s"].iloc[0] is typically 0.
    t0 = df["elapsed_s"].iloc[0]
    
    extended_time_s = pd.concat([pd.Series([t0]), time_s])
    # The throughput for the "point" at t0 doesn't really matter for steps-pre 
    # as long as we have the value at t1.
    # But for plotting purposes, we'll repeat the first eps value or use 0.
    # Actually, with steps-pre, the value at index i is used for interval [i-1, i].
    # So if we have:
    # time: [0, 1, 2]
    # eps:  [?, 100, 200]
    # Matplotlib with steps-pre will plot 100 from 0 to 1, and 200 from 1 to 2.
    # So we need to prepend the first time point.
    extended_eps = pd.concat([pd.Series([eps.iloc[0]]), eps])
    extended_eps_smooth = pd.concat([pd.Series([eps_smooth.iloc[0]]), eps_smooth])

    return {
        "time_s": extended_time_s.values,
        "throughput_eps": extended_eps.values,
        "throughput_eps_smooth": extended_eps_smooth.values,
    }


def plot_throughput(throughput_samples: pd.DataFrame, out_path: Path, data_path: Path = None):
    """Plot throughput over time with both raw and smoothed data."""
    result = compute_throughput_timeseries(throughput_samples)

    if result is None:
        return

    # Save computed data as JSON if path provided
    if data_path:
        data = {
            "time_s": result["time_s"].tolist(),
            "throughput_eps": result["throughput_eps"].tolist(),
            "throughput_eps_smooth": result["throughput_eps_smooth"].tolist(),
        }
        with open(data_path, 'w') as f:
            json.dump(data, f, indent=2)

    plt.figure(figsize=(6, 4))
    # Plot raw count deltas with thin line and markers
    plt.plot(result["time_s"], result["throughput_eps"],
             linewidth=2.0, alpha=0.9, color='#1f77b4', marker=None,
             markersize=3, drawstyle='steps-pre')
    plt.xlabel("Elapsed Time (s)")
    plt.ylabel("Throughput (events/sec)")
    plt.title("Throughput over Time")
    plt.grid(True, ls=":", alpha=0.6)
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()


def plot_comparison_latency_cdf(run_data, title, out_path: Path):
    """Plot latency CDF comparing stores for a specific writer count.

    Args:
        run_data: List of (adapter_name, run) tuples
        title: Plot title
        out_path: Output path for the plot
    """
    plt.figure(figsize=(8, 5))

    for adapter_name, run in run_data:
        percentiles_data = run["latency_percentiles"]

        if percentiles_data is None or len(percentiles_data) == 0:
            continue

        # Extract percentiles and latencies
        percentiles = [p["percentile"] for p in percentiles_data]
        latencies_ms = [p["latency_us"] / 1000.0 for p in percentiles_data]

        color = get_adapter_color(adapter_name)
        plt.plot(latencies_ms, percentiles, label=adapter_name, color=color, linewidth=2)

    plt.xscale("log")
    plt.xlabel("Latency (ms) [log]")
    plt.ylabel("Percentile (%)")
    # Use FormatStrFormatter for decimals on latency axis
    formatter = plt.FormatStrFormatter('%.1f')
    plt.gca().xaxis.set_major_formatter(formatter)
    plt.gca().xaxis.set_minor_formatter(plt.NullFormatter())
    plt.ticklabel_format(style='plain', axis='y')
    plt.title(title)
    plt.legend()
    plt.grid(True, which="both", ls=":", alpha=0.6)
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()


def plot_comparison_throughput(run_data, title, out_path: Path, data_path: Path = None):
    """Plot throughput over time comparing stores for a specific writer count."""
    plt.figure(figsize=(8, 5))

    # Store data for all adapters if data_path provided
    all_data = {}

    for label, samples_df in run_data:
        result = compute_throughput_timeseries(samples_df)

        if result is None:
            continue

        color = get_adapter_color(label)
        # Plot throughput data
        plt.plot(result["time_s"], result["throughput_eps"],
                label=label, color=color, linewidth=2.0, alpha=0.9, marker=None,
                drawstyle='steps-pre')

        # Store data
        if data_path:
            all_data[label] = {
                "time_s": result["time_s"].tolist(),
                "throughput_eps": result["throughput_eps"].tolist(),
                "throughput_eps_smooth": result["throughput_eps_smooth"].tolist(),
            }

    # Save combined data as JSON if path provided
    if data_path and all_data:
        with open(data_path, 'w') as f:
            json.dump(all_data, f, indent=2)

    plt.xlabel("Elapsed Time (s)")
    plt.ylabel("Throughput (events/sec)")
    plt.title(title)
    plt.legend()
    plt.grid(True, ls=":", alpha=0.6)
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()


def plot_throughput_scaling(runs, out_path: Path, get_store_rank=None):
    """Plot throughput vs worker count (writers or readers) using grouped bar charts."""
    # Group by worker_count → adapter → throughput
    data = defaultdict(dict)
    all_adapters = set()
    all_worker_counts = set()

    for run in runs:
        adapter = run["adapter"]
        writers = run["writers"]
        readers = run["readers"]
        worker_count = writers if writers > 0 else readers

        throughput = 0
        if "throughput_samples" in run and run["throughput_samples"]:
            df = pd.DataFrame(run["throughput_samples"])
            if len(df) >= 2:
                df = df.sort_values("elapsed_s")
                duration = df["elapsed_s"].iloc[-1] - df["elapsed_s"].iloc[0]
                total_count = df["count"].iloc[-1] - df["count"].iloc[0]
                if duration > 0:
                    throughput = total_count / duration

        if throughput > 0:
            data[worker_count][adapter] = throughput
            all_adapters.add(adapter)
            all_worker_counts.add(worker_count)

    if not data:
        return

    worker_counts = sorted(list(all_worker_counts))
    if get_store_rank:
        adapters = sorted(list(all_adapters), key=get_store_rank)
    else:
        adapters = sorted(list(all_adapters))

    # Determine label based on the workload type
    first_run = runs[0] if runs else {}
    is_readers = first_run.get("readers", 0) > 0 and first_run.get("writers", 0) == 0
    xlabel = "Readers" if is_readers else "Writers"
    title = f"Throughput by {xlabel[:-1]} Count"

    plt.figure(figsize=(10, 6))
    
    x = np.arange(len(worker_counts))
    width = 0.8 / len(adapters)
    
    for i, adapter in enumerate(adapters):
        vals = [data[wc].get(adapter, 0) for wc in worker_counts]
        offset = (i - (len(adapters) - 1) / 2) * width
        plt.bar(x + offset, vals, width, label=adapter, color=get_adapter_color(adapter), alpha=0.9)

    plt.yscale("log")
    plt.ylabel("Throughput (events/sec) [log]")
    plt.xlabel(xlabel)
    plt.title(title)
    plt.xticks(x, [str(wc) for wc in worker_counts])
    
    formatter = ScalarFormatter()
    formatter.set_scientific(False)
    plt.gca().yaxis.set_major_formatter(formatter)
    plt.gca().yaxis.set_major_locator(LogLocator(base=10.0, subs=(1.0, 2.0, 5.0)))
    plt.gca().yaxis.set_minor_formatter(NullFormatter())
    
    plt.legend()
    plt.grid(True, axis='y', ls=":", alpha=0.6)
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()


def plot_latency_scaling(runs, out_path: Path, get_store_rank=None):
    """Plot p50, p99, and p99.9 latency vs worker count (writers or readers) using grouped bar charts."""
    data = defaultdict(lambda: defaultdict(dict))
    all_adapters = set()
    all_worker_counts = set()

    for run in runs:
        adapter = run["adapter"]
        writers = run["writers"]
        readers = run["readers"]
        worker_count = writers if writers > 0 else readers
        
        p50 = 0
        p99 = 0
        p999 = 0
        percentiles_data = run.get("latency_percentiles")
        if not percentiles_data:
             percentiles_data = run.get("latency", {}).get("percentiles", [])
             
        for p in percentiles_data:
            if p["percentile"] == 50.0:
                p50 = p["latency_us"] / 1000.0
            elif p["percentile"] == 99.0:
                p99 = p["latency_us"] / 1000.0
            elif p["percentile"] == 99.9:
                p999 = p["latency_us"] / 1000.0
        
        if p50 > 0 or p99 > 0 or p999 > 0:
            data[worker_count][adapter] = {"p50": p50, "p99": p99, "p999": p999}
            all_adapters.add(adapter)
            all_worker_counts.add(worker_count)

    if not data:
        return

    worker_counts = sorted(list(all_worker_counts))
    if get_store_rank:
        adapters = sorted(list(all_adapters), key=get_store_rank)
    else:
        adapters = sorted(list(all_adapters))

    # Determine label based on workload type
    first_run = runs[0] if runs else {}
    is_readers = first_run.get("readers", 0) > 0 and first_run.get("writers", 0) == 0
    xlabel = "Readers" if is_readers else "Writers"
    title = f"Latency by {xlabel[:-1]} Count"

    plt.figure(figsize=(12, 7))
    x = np.arange(len(worker_counts))
    width = 0.8 / len(adapters)
    
    for i, adapter in enumerate(adapters):
        p50_vals = np.array([data[wc].get(adapter, {}).get("p50", 0) for wc in worker_counts])
        p99_vals = np.array([data[wc].get(adapter, {}).get("p99", 0) for wc in worker_counts])
        p999_vals = np.array([data[wc].get(adapter, {}).get("p999", 0) for wc in worker_counts])
        
        offset = (i - (len(adapters) - 1) / 2) * width
        color = get_adapter_color(adapter)
        
        # Plot stacked segments: p50 (lightest), p99 (medium), p99.9 (darkest)
        # We use 'bottom' to stack them and different alphas to distinguish the segments.
        plt.bar(x + offset, p50_vals, width, color=color, alpha=1.0)
        plt.bar(x + offset, np.maximum(0, p99_vals - p50_vals), width, bottom=p50_vals, color=color, alpha=0.6)
        plt.bar(x + offset, np.maximum(0, p999_vals - p99_vals), width, bottom=p99_vals, color=color, alpha=0.3)

    plt.yscale("log")
    plt.ylabel("Latency (ms) [log]")
    plt.xlabel(xlabel)
    plt.title(title)
    plt.xticks(x, [str(wc) for wc in worker_counts])
    
    formatter = FormatStrFormatter('%.1f')
    plt.gca().yaxis.set_major_formatter(formatter)
    plt.gca().yaxis.set_major_locator(LogLocator(base=10.0, subs=(1.0, 2.0, 5.0)))
    plt.gca().yaxis.set_minor_formatter(NullFormatter())
    
    # Custom legend: showing adapters by color and percentiles by alpha
    # Since we can't easily show both in one legend without duplicate entries, 
    # we'll create a legend that shows all adapters with solid color, 
    # and then some proxy entries for the p-values.
    from matplotlib.lines import Line2D
    
    adapter_handles = [Line2D([0], [0], color=get_adapter_color(a), lw=4, label=a) for a in adapters]
    percentile_handles = [
        Line2D([0], [0], color='gray', alpha=1.0, lw=4, label='p50'),
        Line2D([0], [0], color='gray', alpha=0.6, lw=4, label='p99'),
        Line2D([0], [0], color='gray', alpha=0.3, lw=4, label='p99.9')
    ]
    
    plt.legend(handles=adapter_handles + percentile_handles, ncol=2)
    plt.grid(True, axis='y', ls=":", alpha=0.6)
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()


def plot_p50_scaling(runs, out_path: Path, get_store_rank=None):
    """Plot p50 latency vs worker count (writers or readers) using grouped bar charts."""
    data = defaultdict(dict)
    all_adapters = set()
    all_worker_counts = set()

    for run in runs:
        adapter = run["adapter"]
        writers = run["writers"]
        readers = run["readers"]
        worker_count = writers if writers > 0 else readers
        
        p50 = 0
        percentiles_data = run.get("latency_percentiles")
        if not percentiles_data:
             # Try latency/percentiles as fallback
             percentiles_data = run.get("latency", {}).get("percentiles", [])
             
        for p in percentiles_data:
            if p["percentile"] == 50.0:
                p50 = p["latency_us"] / 1000.0
                break
        
        if p50 > 0:
            data[worker_count][adapter] = p50
            all_adapters.add(adapter)
            all_worker_counts.add(worker_count)

    if not data:
        return

    worker_counts = sorted(list(all_worker_counts))
    if get_store_rank:
        adapters = sorted(list(all_adapters), key=get_store_rank)
    else:
        adapters = sorted(list(all_adapters))

    # Determine label based on workload type
    first_run = runs[0] if runs else {}
    is_readers = first_run.get("readers", 0) > 0 and first_run.get("writers", 0) == 0
    xlabel = "Readers" if is_readers else "Writers"
    title = f"p50 Latency by {xlabel[:-1]} Count"

    plt.figure(figsize=(10, 6))
    x = np.arange(len(worker_counts))
    width = 0.8 / len(adapters)
    
    for i, adapter in enumerate(adapters):
        vals = [data[wc].get(adapter, 0) for wc in worker_counts]
        offset = (i - (len(adapters) - 1) / 2) * width
        plt.bar(x + offset, vals, width, label=adapter, color=get_adapter_color(adapter), alpha=0.9)

    plt.yscale("log")
    plt.ylabel("p50 Latency (ms) [log]")
    plt.xlabel(xlabel)
    plt.title(title)
    plt.xticks(x, [str(wc) for wc in worker_counts])
    
    formatter = FormatStrFormatter('%.1f')
    plt.gca().yaxis.set_major_formatter(formatter)
    plt.gca().yaxis.set_major_locator(LogLocator(base=10.0, subs=(1.0, 2.0, 5.0)))
    plt.gca().yaxis.set_minor_formatter(NullFormatter())
    
    plt.legend()
    plt.grid(True, axis='y', ls=":", alpha=0.6)
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()


def plot_p99_scaling(runs, out_path: Path, get_store_rank=None):
    """Plot p99 latency vs worker count (writers or readers) using grouped bar charts."""
    data = defaultdict(dict)
    all_adapters = set()
    all_worker_counts = set()

    for run in runs:
        adapter = run["adapter"]
        writers = run["writers"]
        readers = run["readers"]
        worker_count = writers if writers > 0 else readers

        p99 = 0
        percentiles_data = run.get("latency_percentiles")
        if not percentiles_data:
             percentiles_data = run.get("latency", {}).get("percentiles", [])

        for p in percentiles_data:
            if p["percentile"] == 99.0:
                p99 = p["latency_us"] / 1000.0
                break

        if p99 > 0:
            data[worker_count][adapter] = p99
            all_adapters.add(adapter)
            all_worker_counts.add(worker_count)

    if not data:
        return

    worker_counts = sorted(list(all_worker_counts))
    if get_store_rank:
        adapters = sorted(list(all_adapters), key=get_store_rank)
    else:
        adapters = sorted(list(all_adapters))

    # Determine label based on workload type
    first_run = runs[0] if runs else {}
    is_readers = first_run.get("readers", 0) > 0 and first_run.get("writers", 0) == 0
    xlabel = "Readers" if is_readers else "Writers"
    title = f"p99 Latency by {xlabel[:-1]} Count"

    plt.figure(figsize=(10, 6))
    x = np.arange(len(worker_counts))
    width = 0.8 / len(adapters)
    
    for i, adapter in enumerate(adapters):
        vals = [data[wc].get(adapter, 0) for wc in worker_counts]
        offset = (i - (len(adapters) - 1) / 2) * width
        plt.bar(x + offset, vals, width, label=adapter, color=get_adapter_color(adapter), alpha=0.9)

    plt.yscale("log")
    plt.ylabel("p99 Latency (ms) [log]")
    plt.xlabel(xlabel)
    plt.title(title)
    plt.xticks(x, [str(wc) for wc in worker_counts])
    
    formatter = FormatStrFormatter('%.1f')
    plt.gca().yaxis.set_major_formatter(formatter)
    plt.gca().yaxis.set_major_locator(LogLocator(base=10.0, subs=(1.0, 2.0, 5.0)))
    plt.gca().yaxis.set_minor_formatter(NullFormatter())
    
    plt.legend()
    plt.grid(True, axis='y', ls=":", alpha=0.6)
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()


def plot_p999_scaling(runs, out_path: Path, get_store_rank=None):
    """Plot p99.9 latency vs worker count (writers or readers) using grouped bar charts."""
    data = defaultdict(dict)
    all_adapters = set()
    all_worker_counts = set()

    for run in runs:
        adapter = run["adapter"]
        writers = run["writers"]
        readers = run["readers"]
        worker_count = writers if writers > 0 else readers

        p999 = 0
        percentiles_data = run.get("latency_percentiles")
        if not percentiles_data:
             percentiles_data = run.get("latency", {}).get("percentiles", [])

        for p in percentiles_data:
            if p["percentile"] == 99.9:
                p999 = p["latency_us"] / 1000.0
                break

        if p999 > 0:
            data[worker_count][adapter] = p999
            all_adapters.add(adapter)
            all_worker_counts.add(worker_count)

    if not data:
        return

    worker_counts = sorted(list(all_worker_counts))
    if get_store_rank:
        adapters = sorted(list(all_adapters), key=get_store_rank)
    else:
        adapters = sorted(list(all_adapters))

    # Determine label based on workload type
    first_run = runs[0] if runs else {}
    is_readers = first_run.get("readers", 0) > 0 and first_run.get("writers", 0) == 0
    xlabel = "Readers" if is_readers else "Writers"
    title = f"p99.9 Latency by {xlabel[:-1]} Count"

    plt.figure(figsize=(10, 6))
    x = np.arange(len(worker_counts))
    width = 0.8 / len(adapters)
    
    for i, adapter in enumerate(adapters):
        vals = [data[wc].get(adapter, 0) for wc in worker_counts]
        offset = (i - (len(adapters) - 1) / 2) * width
        plt.bar(x + offset, vals, width, label=adapter, color=get_adapter_color(adapter), alpha=0.9)

    plt.yscale("log")
    plt.ylabel("p99.9 Latency (ms) [log]")
    plt.xlabel(xlabel)
    plt.title(title)
    plt.xticks(x, [str(wc) for wc in worker_counts])
    
    formatter = FormatStrFormatter('%.1f')
    plt.gca().yaxis.set_major_formatter(formatter)
    plt.gca().yaxis.set_major_locator(LogLocator(base=10.0, subs=(1.0, 2.0, 5.0)))
    plt.gca().yaxis.set_minor_formatter(NullFormatter())
    
    plt.legend()
    plt.grid(True, axis='y', ls=":", alpha=0.6)
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()


def plot_peak_cpu_scaling(runs, out_path: Path, get_store_rank=None):
    """Plot peak CPU usage vs worker count (writers or readers) using grouped bar charts."""
    data = defaultdict(dict)
    all_adapters = set()
    all_worker_counts = set()

    for run in runs:
        adapter = run["adapter"]
        writers = run["writers"]
        readers = run["readers"]
        worker_count = writers if writers > 0 else readers
        container = run.get("container", {})
        peak_cpu = container.get("peak_cpu_percent")
        if peak_cpu is not None:
            data[worker_count][adapter] = peak_cpu
            all_adapters.add(adapter)
            all_worker_counts.add(worker_count)

    if not data:
        return

    worker_counts = sorted(list(all_worker_counts))
    if get_store_rank:
        adapters = sorted(list(all_adapters), key=get_store_rank)
    else:
        adapters = sorted(list(all_adapters))

    first_run = runs[0] if runs else {}
    is_readers = first_run.get("readers", 0) > 0 and first_run.get("writers", 0) == 0
    xlabel = "Readers" if is_readers else "Writers"
    title = f"Peak CPU by {xlabel[:-1]} Count"

    plt.figure(figsize=(10, 6))
    x = np.arange(len(worker_counts))
    width = 0.8 / len(adapters)
    
    for i, adapter in enumerate(adapters):
        vals = [data[wc].get(adapter, 0) for wc in worker_counts]
        offset = (i - (len(adapters) - 1) / 2) * width
        plt.bar(x + offset, vals, width, label=adapter, color=get_adapter_color(adapter), alpha=0.9)

    plt.ylabel("Peak CPU (%)")
    plt.xlabel(xlabel)
    plt.title(title)
    plt.xticks(x, [str(wc) for wc in worker_counts])
    
    plt.legend()
    plt.grid(True, axis='y', ls=":", alpha=0.6)
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()


def plot_peak_mem_scaling(runs, out_path: Path, get_store_rank=None):
    """Plot peak memory usage vs worker count (writers or readers) using grouped bar charts."""
    data = defaultdict(dict)
    all_adapters = set()
    all_worker_counts = set()

    for run in runs:
        adapter = run["adapter"]
        writers = run["writers"]
        readers = run["readers"]
        worker_count = writers if writers > 0 else readers
        container = run.get("container", {})
        peak_mem = container.get("peak_memory_bytes")
        if peak_mem is not None:
            data[worker_count][adapter] = peak_mem / (1024 * 1024)
            all_adapters.add(adapter)
            all_worker_counts.add(worker_count)

    if not data:
        return

    worker_counts = sorted(list(all_worker_counts))
    if get_store_rank:
        adapters = sorted(list(all_adapters), key=get_store_rank)
    else:
        adapters = sorted(list(all_adapters))

    first_run = runs[0] if runs else {}
    is_readers = first_run.get("readers", 0) > 0 and first_run.get("writers", 0) == 0
    xlabel = "Readers" if is_readers else "Writers"
    title = f"Peak Memory by {xlabel[:-1]} Count"

    plt.figure(figsize=(10, 6))
    x = np.arange(len(worker_counts))
    width = 0.8 / len(adapters)
    
    for i, adapter in enumerate(adapters):
        vals = [data[wc].get(adapter, 0) for wc in worker_counts]
        offset = (i - (len(adapters) - 1) / 2) * width
        plt.bar(x + offset, vals, width, label=adapter, color=get_adapter_color(adapter), alpha=0.9)

    plt.ylabel("Peak Memory (MB)")
    plt.xlabel(xlabel)
    plt.title(title)
    plt.xticks(x, [str(wc) for wc in worker_counts])
    
    plt.legend()
    plt.grid(True, axis='y', ls=":", alpha=0.6)
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()


def plot_container_metrics(runs, out_path: Path, get_store_rank=None):
    """Create a dramatic visualization of container resource usage."""
    # Collect data for each unique adapter (use dict to deduplicate and aggregate)
    adapter_data = {}

    for run in runs:
        adapter = run["adapter"]
        container = run.get("container", {})

        # Only include if we have meaningful data
        if not (container.get("image_size_bytes") or container.get("peak_cpu_percent")):
            continue

        if adapter not in adapter_data:
            adapter_data[adapter] = {
                "image_size": 0,
                "startup_time": 0,
                "peak_cpu": 0,
                "peak_mem": 0,
                "count": 0
            }

        # Accumulate data (we'll use max for peaks, average for others)
        data = adapter_data[adapter]
        data["image_size"] = max(data["image_size"], container.get("image_size_bytes", 0) / (1024 * 1024))
        data["startup_time"] += container.get("startup_time_s", 0)
        data["peak_cpu"] = max(data["peak_cpu"], container.get("peak_cpu_percent", 0))
        data["peak_mem"] = max(data["peak_mem"], container.get("peak_memory_bytes", 0) / (1024 * 1024))
        data["count"] += 1

    if not adapter_data:
        return

    # Determine order of adapters
    adapters_list = list(adapter_data.keys())
    if get_store_rank:
        # Sort by store rank from config
        adapters = sorted(adapters_list, key=get_store_rank)
    else:
        # Create a composite score for ordering (lower is better):
        # Normalize each metric to 0-1 range, then compute weighted average

        # Get raw values
        raw_image = [adapter_data[a]["image_size"] for a in adapters_list]
        raw_startup = [adapter_data[a]["startup_time"] / adapter_data[a]["count"] for a in adapters_list]
        raw_cpu = [adapter_data[a]["peak_cpu"] for a in adapters_list]
        raw_mem = [adapter_data[a]["peak_mem"] for a in adapters_list]

        # Normalize to 0-1 range (avoiding division by zero)
        def normalize(values):
            max_val = max(values) if values else 1
            return [v / max_val if max_val > 0 else 0 for v in values]

        norm_image = normalize(raw_image)
        norm_startup = normalize(raw_startup)
        norm_cpu = normalize(raw_cpu)
        norm_mem = normalize(raw_mem)

        # Compute composite score (equal weights, lower is better)
        composite_scores = []
        for i, adapter in enumerate(adapters_list):
            score = (norm_image[i] + norm_startup[i] + norm_cpu[i] + norm_mem[i]) / 4.0
            composite_scores.append((adapter, score))

        # Sort by composite score (best first)
        composite_scores.sort(key=lambda x: x[1])
        adapters = [x[0] for x in composite_scores]

    # Extract ordered lists for plotting
    image_sizes = [adapter_data[a]["image_size"] for a in adapters]
    startup_times = [adapter_data[a]["startup_time"] / adapter_data[a]["count"] for a in adapters]
    peak_cpus = [adapter_data[a]["peak_cpu"] for a in adapters]
    peak_mems = [adapter_data[a]["peak_mem"] for a in adapters]

    # Create a 2x2 subplot for dramatic effect
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(14, 10))
    fig.suptitle("Container Resource Metrics Comparison", fontsize=16, fontweight='bold')

    # Use consistent colors across all plots
    colors = [get_adapter_color(adapter) for adapter in adapters]

    # 1. Image Size - Vertical bar chart
    bars1 = ax1.bar(adapters, image_sizes, color=colors, edgecolor='black', linewidth=1.5)
    ax1.set_ylabel("Image Size (MB)", fontweight='bold')
    ax1.set_title("Container Image Size", fontweight='bold')
    ax1.grid(True, alpha=0.3, axis='y')
    # ax1.set_xticklabels(adapters, rotation=45, ha='right')
    for bar, v in zip(bars1, image_sizes):
        height = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width()/2., height,
                f'{v:.0f}', ha='center', va='bottom', fontweight='bold')

    # 2. Startup Time - Vertical bar chart
    bars2 = ax2.bar(adapters, startup_times, color=colors, edgecolor='black', linewidth=1.5)
    ax2.set_ylabel("Startup Time (seconds)", fontweight='bold')
    ax2.set_title("Container Startup Time", fontweight='bold')
    ax2.grid(True, alpha=0.3, axis='y')
    # ax2.set_xticklabels(adapters, rotation=45, ha='right')
    for bar, v in zip(bars2, startup_times):
        height = bar.get_height()
        ax2.text(bar.get_x() + bar.get_width()/2., height,
                f'{v:.2f}s', ha='center', va='bottom', fontweight='bold')

    # 3. Peak CPU - Vertical bar chart
    bars3 = ax3.bar(adapters, peak_cpus, color=colors, edgecolor='black', linewidth=1.5)
    ax3.set_ylabel("Peak CPU (%)", fontweight='bold')
    ax3.set_title("Peak CPU Usage", fontweight='bold')
    ax3.grid(True, alpha=0.3, axis='y')
    # ax3.set_xticklabels(adapters, rotation=45, ha='right')
    for bar, v in zip(bars3, peak_cpus):
        height = bar.get_height()
        ax3.text(bar.get_x() + bar.get_width()/2., height,
                f'{v:.1f}%', ha='center', va='bottom', fontweight='bold')

    # 4. Peak Memory - Vertical bar chart
    bars4 = ax4.bar(adapters, peak_mems, color=colors, edgecolor='black', linewidth=1.5)
    ax4.set_ylabel("Peak Memory (MB)", fontweight='bold')
    ax4.set_title("Peak Memory Usage", fontweight='bold')
    ax4.grid(True, alpha=0.3, axis='y')
    # ax4.set_xticklabels(adapters, rotation=45, ha='right')
    for bar, v in zip(bars4, peak_mems):
        height = bar.get_height()
        ax4.text(bar.get_x() + bar.get_width()/2., height,
                f'{v:.0f}', ha='center', va='bottom', fontweight='bold')

    plt.tight_layout()
    plt.savefig(out_path, dpi=150, bbox_inches='tight')
    plt.close()


def generate_html(report_dir: Path, run):
    workload_name = run["workload_name"]
    latency_img = report_dir / "latency_cdf.png"
    throughput_img = report_dir / "throughput.png"

    logs_html = ""
    if run.get("container_logs"):
        logs_html = f"""
  <div class='row'>
    <div class='card' style='width: 100%;'>
      <h2>Container Logs</h2>
      <pre style='background: #f8f8f8; padding: 1rem; border-radius: 4px; overflow-x: auto; font-size: 0.85rem; max-height: 500px; overflow-y: auto;'>{run['container_logs']}</pre>
    </div>
  </div>"""

    html = f"""
<!DOCTYPE html>
<html>
<head>
  <meta charset='utf-8'>
  <title>Workload Report — {run['adapter']} / {workload_name}</title>
  <style>
    body {{ font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif; margin: 2rem; }}
    h1, h2 {{ margin-top: 1.2rem; }}
    .row {{ display: flex; gap: 1rem; flex-wrap: wrap; margin-bottom: 1rem; }}
    .card {{ border: 1px solid #eee; border-radius: 8px; padding: 1rem; }}
  </style>
</head>
<body>
  <h1>Benchmark Report</h1>
  <p><b>Adapter:</b> {run['adapter']} &nbsp; | &nbsp; <b>Workload:</b> {workload_name}</p>
  <p><b>Duration:</b> {run.get('_duration_s', 0):.1f}s &nbsp; | &nbsp; <b>Throughput:</b> {run.get('_throughput_eps', 0):.0f} eps</p>
  <div class='row'>
    <div class='card'>
      <h2>Latency CDF</h2>
      <img src='{latency_img.name}' width='560'>
    </div>
    <div class='card'>
      <h2>Throughput over time</h2>
      <img src='{throughput_img.name}' width='560'>
    </div>
  </div>
  {logs_html}
</body>
</html>
"""
    with open(report_dir / "index.html", "w") as f:
        f.write(html)


def generate_workload_html(out_base: Path, workload_name: str, runs, writer_groups, workload_config=None, get_store_rank=None):
    """Generate a consolidated report for a specific workload."""
    # Summary table
    summary_rows = ""
    def row_key(row):
        adapter = row["adapter"]
        writers = row["writers"]
        readers = row["readers"]
        rank = get_store_rank(adapter) if get_store_rank else 0
        return (writers, readers, rank, adapter)

    for run in sorted(runs, key=row_key):
        adapter = run["adapter"]
        writers = run["writers"]
        readers = run["readers"]

        # Determine link format based on workload type
        report_link = f"report-{adapter}-r{readers:03d}-w{writers:03d}/index.html"
        if readers > 0 and writers == 0:
            worker_display = readers
        elif writers > 0 and readers == 0:
            worker_display = writers
        else:
            worker_display = f"{writers}w/{readers}r"

        # Get container metrics
        container = run.get("container", {})
        startup_time = f"{container.get('startup_time_s', 0):.1f}s" if container.get("startup_time_s") else "N/A"
        image_size_mb = f"{container.get('image_size_bytes', 0) / 1024 / 1024:.0f}" if container.get("image_size_bytes") else "N/A"
        
        throughput_eps = run.get("_throughput_eps", 0)
        p50 = 0
        p99 = 0
        p999 = 0
        percentiles_data = run.get("latency_percentiles", [])

        for p in percentiles_data:
            if p["percentile"] == 50.0:
                p50 = p["latency_us"] / 1000.0
            elif p["percentile"] == 99.0:
                p99 = p["latency_us"] / 1000.0
            elif p["percentile"] == 99.9:
                p999 = p["latency_us"] / 1000.0

        # CPU metrics (avg / peak)
        avg_cpu = container.get("avg_cpu_percent")
        peak_cpu = container.get("peak_cpu_percent")
        cpu_display = "N/A"
        if avg_cpu is not None and peak_cpu is not None:
            cpu_display = f"{avg_cpu:.1f}% / {peak_cpu:.1f}%"
        elif avg_cpu is not None:
            cpu_display = f"{avg_cpu:.1f}%"

        # Memory metrics (avg / peak in MB)
        avg_mem = container.get("avg_memory_bytes")
        peak_mem = container.get("peak_memory_bytes")
        mem_display = "N/A"
        if avg_mem is not None and peak_mem is not None:
            mem_display = f"{avg_mem / 1024 / 1024:.0f} / {peak_mem / 1024 / 1024:.0f}"
        elif avg_mem is not None:
            mem_display = f"{avg_mem / 1024 / 1024:.0f}"

        summary_rows += f"""
      <tr>
        <td><a href='{report_link}'>{adapter}</a></td>
        <td>{worker_display}</td>
        <td>{throughput_eps:.0f}</td>
        <td>{p50:.2f}</td>
        <td>{p99:.2f}</td>
        <td>{p999:.2f}</td>
        <td>{image_size_mb}</td>
        <td>{startup_time}</td>
        <td>{cpu_display}</td>
        <td>{mem_display}</td>
      </tr>"""

    # Per-worker-count comparison sections
    # Determine if this is a readers or writers workload
    first_run = runs[0] if runs else {}
    is_readers = first_run.get("readers", 0) > 0 and first_run.get("writers", 0) == 0
    worker_label = "Readers" if is_readers else "Writers"
    worker_suffix = "r" if is_readers else "w"

    comparison_sections = ""
    for wc in sorted(writer_groups.keys()):
        comparison_sections += f"""
    <h2>{worker_label} = {wc}</h2>
    <div class='row'>
      <div class='card'>
        <h3>Latency CDF</h3>
        <img src='{workload_name}_comparison_{worker_suffix}{wc}_latency_cdf.png' width='560'>
      </div>
      <div class='card'>
        <h3>Throughput over time</h3>
        <img src='{workload_name}_comparison_{worker_suffix}{wc}_throughput.png' width='560'>
      </div>
    </div>"""

    # Container metrics section
    container_section = f"""
    <h2>Container Resource Metrics</h2>
    <div class='card' style='max-width: 100%;'>
      <img src='{workload_name}_container_metrics.png' style='width: 100%; max-width: 1200px;'>
    </div>"""

    # Scaling charts (only if multiple writer counts)
    scaling_section = ""
    if len(writer_groups) > 1:
        scaling_section = f"""
    <h2>Scaling</h2>
    <div class='row'>
      <div class='card'>
        <h3>Throughput vs {worker_label}</h3>
        <img src='{workload_name}_scaling_throughput.png' width='560'>
      </div>
      <div class='card'>
        <h3>Latency vs {worker_label}</h3>
        <img src='{workload_name}_scaling_latency.png' width='560'>
      </div>
    </div>
    <div class='row'>
      <div class='card'>
        <h3>Peak CPU vs {worker_label}</h3>
        <img src='{workload_name}_scaling_peak_cpu.png' width='560'>
      </div>
      <div class='card'>
        <h3>Peak Memory vs {worker_label}</h3>
        <img src='{workload_name}_scaling_peak_mem.png' width='560'>
      </div>
    </div>"""

    config_section = ""
    if workload_config:
        config_yaml = yaml.dump(workload_config, indent=2)
        config_section = f"""
    <h2>Workload Configuration</h2>
    <div class='card'>
      <pre style='background-color: #f8f8f8; padding: 1rem; border-radius: 4px; overflow-x: auto;'>{config_yaml}</pre>
    </div>"""

    html = f"""
<!DOCTYPE html>
<html>
<head>
  <meta charset='utf-8'>
  <title>Workload Report — {workload_name}</title>
  <style>
    body {{ font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif; margin: 2rem; }}
    h1, h2, h3 {{ margin-top: 1.2rem; }}
    table {{ border-collapse: collapse; margin: 1rem 0; }}
    th, td {{ border: 1px solid #ddd; padding: 0.5rem 1rem; text-align: left; }}
    th {{ background: #f5f5f5; }}
    .row {{ display: flex; gap: 1rem; flex-wrap: wrap; }}
    .card {{ border: 1px solid #eee; border-radius: 8px; padding: 1rem; }}
  </style>
</head>
<body>
  <h1>Workload Report — {workload_name}</h1>
  <p><a href="../index.html">← Back to all workloads</a></p>
  {container_section}
  {scaling_section}
  {comparison_sections}
  <h2>Summary</h2>
  <table>
    <tr><th>Adapter</th><th>{worker_label}</th><th>Throughput (eps)</th><th>p50 (ms)</th><th>p99 (ms)</th><th>p99.9 (ms)</th><th>Image (MB)</th><th>Startup</th><th>CPU (avg/peak)</th><th>Mem MB (avg/peak)</th></tr>
    {summary_rows}
  </table>
  {config_section}
</body>
</html>
"""
    workload_dir = out_base / workload_name
    workload_dir.mkdir(parents=True, exist_ok=True)
    with open(workload_dir / "index.html", "w") as f:
        f.write(html)


def generate_top_level_index(raw_base: Path, published_base: Path):
    """Generate top-level index.html that links to individual session reports."""

    # Collect summaries for published sessions from raw data
    sessions_summaries = {}
    published_session_ids = sorted([d.name for d in published_base.iterdir() if d.is_dir()])

    for session_id in published_session_ids:
        raw_session_dir = raw_base / session_id
        if not raw_session_dir.exists():
            continue

        try:
            # Load session info
            session_info_file = raw_session_dir / "session.json"
            session_info = {}
            if session_info_file.exists():
                with open(session_info_file, "r") as f:
                    session_info = json.load(f)

            # Load session config
            session_config_file = raw_session_dir / "config.yaml"
            session_configs = []
            if session_config_file.exists():
                with open(session_config_file, "r") as f:
                    session_configs = list(yaml.safe_load_all(f))

            workload_names = []
            all_stores = set()
            for cfg in session_configs:
                perf_cfg = cfg.get('performance', cfg)
                if 'name' in perf_cfg:
                    workload_names.append(perf_cfg['name'])
                if 'stores' in perf_cfg:
                    stores = perf_cfg['stores']
                    if isinstance(stores, list):
                        all_stores.update(stores)
                    elif isinstance(stores, str):
                        all_stores.add(stores)

            sessions_summaries[session_id] = {
                'workload_name': ", ".join(workload_names) if workload_names else "N/A",
                'benchmark_version': session_info.get('benchmark_version', 'N/A'),
                'stores': list(all_stores),
            }
        except Exception as e:
            print(f"Warning: Could not collect summary for session {session_id} from raw data: {e}")

    session_rows = ""
    for session_id, summary in sorted(sessions_summaries.items(), reverse=True):
        stores = ", ".join(sorted(summary['stores']))
        
        session_rows += f"""
      <tr>
        <td><a href='{session_id}/index.html'>{session_id}</a></td>
        <td>{summary.get('workload_name', 'N/A')}</td>
        <td>{stores}</td>
        <td>{summary.get('benchmark_version', 'N/A')}</td>
      </tr>"""

    html = f"""
<!DOCTYPE html>
<html>
<head>
  <meta charset='utf-8'>
  <title>ES-BENCH Benchmark Suite</title>
  <style>
    body {{ font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif; margin: 2rem; }}
    h1, h2, h3 {{ margin-top: 1.2rem; }}
    table {{ border-collapse: collapse; margin: 1rem 0; width: 100%; }}
    th, td {{ border: 1px solid #ddd; padding: 0.8rem 1rem; text-align: left; }}
    th {{ background: #f5f5f5; }}
    a {{ color: #0066cc; text-decoration: none; }}
    a:hover {{ text-decoration: underline; }}
  </style>
</head>
<body>
  <h1>Event Store Benchmark Suite</h1>
  <h2>Benchmark Sessions</h2>
  <table>
    <tr><th>Session ID</th><th>Workload</th><th>Stores</th><th>Version</th></tr>
    {session_rows}
  </table>
</body>
</html>
"""
    with open(published_base / "index.html", "w") as f:
        f.write(html)


def generate_session_index(session_out_dir: Path, session_id: str, workload_summaries, env_info=None, session_info=None):
    """Generate index.html for a specific session."""

    env_section = ""
    if env_info:
        # Check if it's the new environment.json format
        if "os" in env_info:
            cpu = env_info.get('cpu', {})
            cpu_model = cpu.get('model', 'N/A')
            cpu_cores = cpu.get('cores', 'N/A')
            cpu_threads = cpu.get('threads', 'N/A')
            kernel = env_info.get('os', {}).get('kernel', 'N/A')
            
            memory = env_info.get('memory', {})
            total_mem_gb = memory.get('total_bytes', 0) // (1024**3)
            avail_mem_gb = memory.get('available_bytes', 0) / (1024**3)
            
            fs_type = env_info.get('disk', {}).get('filesystem', 'N/A')
            disk_type = env_info.get('disk', {}).get('type', 'N/A')
            fsync = env_info.get('disk', {}).get('fsync_latency')
            fsync_section = ""
            if fsync:
                # Support both old _ms and new _us fields for compatibility
                if 'avg_us' in fsync:
                    fsync_section = f"""
          <p><b>Fsync (avg/p99):</b> {fsync.get('avg_us', 0):.2f} / {fsync.get('p99_us', 0):.2f} μs</p>
"""
                else:
                    fsync_section = f"""
          <p><b>Fsync (avg/p99):</b> {fsync.get('avg_ms', 0):.2f} / {fsync.get('p99_ms', 0):.2f} ms</p>
"""
            
            runtime = env_info.get('container_runtime', {})
            rt_type = runtime.get('type', 'N/A')
            rt_ver = runtime.get('version', 'N/A')
            rt_cpu = runtime.get('ncpu', 'N/A')
            rt_mem = runtime.get('mem_total', 0) / (1024**3)

            env_section = f"""
    <div class='workload-section'>
      <h2>Environment Information</h2>
      <div class='row'>
        <div class='card'>
          <h3>System</h3>
          <p><b>CPU:</b> {cpu_model}</p>
          <p><b>Cores/Threads:</b> {cpu_cores} cores / {cpu_threads} threads</p>
          <p><b>Kernel:</b> {kernel}</p>
          <p><b>Memory:</b> {total_mem_gb} GB total ({avail_mem_gb:.1f} GB available)</p>
        </div>
        <div class='card'>
          <h3>Docker Runtime</h3>
          <p><b>Type:</b> {rt_type}</p>
          <p><b>Version:</b> {rt_ver}</p>
          <p><b>Available Cores:</b> {rt_cpu}</p>
          <p><b>Available Memory:</b> {rt_mem:.1f} GB</p>
        </div>
        <div class='card'>
          <h3>Storage</h3>
          <p><b>Disk Type:</b> {disk_type}</p>
          <p><b>FS Type:</b> {fs_type}</p>
          {fsync_section}
        </div>
      </div>
    </div>"""
        else:
            # Old format
            env_section = f"""
    <div class='workload-section'>
      <h2>Environment Information</h2>
      <div class='row'>
        <div class='card'>
          <h3>System</h3>
          <p><b>CPU:</b> {env_info.get('cpu', {}).get('model', 'N/A')} ({env_info.get('cpu', {}).get('cores', 'N/A')} cores)</p>
          <p><b>Kernel:</b> {env_info.get('kernel', 'N/A')}</p>
          <p><b>Memory:</b> {env_info.get('memory', {}).get('total_bytes', 0) // (1024**3)} GB total</p>
        </div>
        <div class='card'>
          <h3>Filesystem & Disk</h3>
          <p><b>FS Type:</b> {env_info.get('filesystem', {}).get('type', 'N/A')}</p>
          <p><b>Disk Size:</b> {env_info.get('filesystem', {}).get('disk_size_bytes', 0) // (1024**3)} GB</p>
          <p><b>Seq Write:</b> {env_info.get('disk', {}).get('sequential_write_bw_bytes_per_sec', 0) / (1024**2):.2f} MB/s</p>
          <p><b>Seq Read:</b> {env_info.get('disk', {}).get('sequential_read_bw_bytes_per_sec', 0) / (1024**2):.2f} MB/s</p>
          <p><b>Concurrent Read (4x):</b> {env_info.get('disk', {}).get('concurrent_read_bw_bytes_per_sec', 0) / (1024**2):.2f} MB/s</p>
        </div>
        <div class='card'>
          <h3>Fsync Latency</h3>
          <p><b>p50:</b> {env_info.get('fsync_latency_ns', {}).get('p50', 0) / 1000:.2f} μs</p>
          <p><b>p95:</b> {env_info.get('fsync_latency_ns', {}).get('p95', 0) / 1000:.2f} μs</p>
          <p><b>p99:</b> {env_info.get('fsync_latency_ns', {}).get('p99', 0) / 1000:.2f} μs</p>
        </div>
      </div>
    </div>"""

    workload_sections = ""
    for workload_name, summary in sorted(workload_summaries.items()):
        # Include scaling plots if this workload has multiple writer counts
        scaling_plots = ""
        if len(summary['writer_counts']) > 1:
            scaling_plots = f"""
      <div class='row'>
        <div class='card'>
          <h3>Throughput</h3>
          <img src='{workload_name}/{workload_name}_scaling_throughput.png' width='460'>
        </div>
        <div class='card'>
          <h3>Latency</h3>
          <img src='{workload_name}/{workload_name}_scaling_latency.png' width='460'>
        </div>
      </div>"""

        workload_sections += f"""
    <div class='workload-section'>
      <h2><a href='{workload_name}/index.html'>{workload_name}</a></h2>
      {scaling_plots}
    </div>"""

    # Generate session index
    session_title = f"Benchmark Session: {session_id}"
    if session_info and session_info.get('workload_name'):
        session_title += f" — {session_info['workload_name']}"

    html = f"""
<!DOCTYPE html>
<html>
<head>
  <meta charset='utf-8'>
  <title>{session_title}</title>
  <style>
    body {{ font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif; margin: 2rem; }}
    h1, h2, h3 {{ margin-top: 1.2rem; }}
    .workload-section {{ border: 1px solid #ddd; border-radius: 8px; padding: 1.5rem; margin: 1.5rem 0; background: #fafafa; }}
    .workload-section h2 {{ margin-top: 0; }}
    .workload-info {{ margin: 0.5rem 0 1rem 0; }}
    .workload-info p {{ margin: 0.25rem 0; }}
    .row {{ display: flex; gap: 1rem; flex-wrap: wrap; margin-top: 1rem; }}
    .card {{ border: 1px solid #eee; border-radius: 8px; padding: 1rem; background: white; }}
    .card h3 {{ margin-top: 0; font-size: 1rem; }}
    a {{ color: #0066cc; text-decoration: none; }}
    a:hover {{ text-decoration: underline; }}
  </style>
</head>
<body>
  <h1>{session_title}</h1>
  <p><a href="../index.html">← Back to all sessions</a></p>
  {env_section}
  <h2>Workload Reports</h2>
  {workload_sections}
</body>
</html>
"""
    with open(session_out_dir / "index.html", "w") as f:
        f.write(html)




def main():
    # Parse CLI arguments
    parser = argparse.ArgumentParser(description="Generate ES-BENCH benchmark report from raw results")
    parser.add_argument("--raw", type=str, default="results/raw", help="Path to raw results dir")
    parser.add_argument("--out", type=str, default="results/published", help="Output reports dir")
    parser.add_argument("--force", action="store_true", help="Force regeneration of already published sessions")
    args = parser.parse_args()

    # Raw and published folders
    raw_base = Path(args.raw)
    if not raw_base.exists() and raw_base.is_dir():
        print(f"No sessions found in {raw_base}")
        return
    raw_session_ids = sorted([d.name for d in raw_base.iterdir() if d.is_dir()])
    if not raw_session_ids:
        print(f"No sessions found in {raw_base}")
        return

    published_base = Path(args.out)
    published_base.mkdir(parents=True, exist_ok=True)

    unpublished_session_ids = []
    force_regenerate = args.force
    for raw_session_id in raw_session_ids:
        published_session_dir = published_base / raw_session_id
        # published_session_index = published_session_dir / "index.html"
        # Include if forced or not exists
        if force_regenerate or not published_session_dir.exists():
            unpublished_session_ids.append(raw_session_id)

    if not unpublished_session_ids:
        print(f"No unpublished sessions found in {raw_base}")
    
    for session_id in unpublished_session_ids:

        print(f"Processing session: {session_id}")

        raw_session_dir = raw_base / session_id

        published_session_dir = published_base / session_id
        published_session_dir.mkdir(parents=True, exist_ok=True)


        # Load session info.
        session_info = {}
        session_info_file = raw_session_dir / "session.json"
        if session_info_file.exists():
            try:
                with open(session_info_file, "r") as f:
                    session_info = json.load(f)
            except Exception as e:
                print(f"Warning: Could not load {session_info_file}: {e}")

        #
        # From here we assume we are dealing with results from a performance workload...
        #

        # TODO: Encapsulate this with a reporting strategy and support other workload types with different strategies.

        # Load environment info.
        env_info = {}
        env_file = raw_session_dir / "environment.json"
        if env_file.exists():
            try:
                with open(env_file, "r") as f:
                    env_info = json.load(f)
            except Exception as e:
                print(f"Warning: Could not load {env_file}: {e}")

        # Load session config.
        session_config_file = raw_session_dir / "config.yaml"
        if not session_config_file.exists():
            print(f"No config.yaml found for session {session_id}. Skipping.")
            continue

        # Group runs by workload within the session
        session_results = load_session_results(raw_session_dir)
        if not session_results:
            print(f"No runs found for session {session_id}. Skipping.")
            continue

        # For the per-workload consolidated reports for this session
        workload_summaries = {}
        all_adapters = set()

        # Generate individual reports for each run in this session
        for workload_results in session_results:
            if workload_results["workload_type"] != "performance":
                print(f"Unsupported workload type: {workload_results["workload_type"]}. Skipping.")
                continue
            workload_runs = workload_results["runs"]
            workload_config = workload_results["config"]
            workload_name = workload_config["name"]
            print(f"  Processing workload: {workload_name}")
            workload_dir = published_session_dir / workload_name
            workload_dir.mkdir(parents=True, exist_ok=True)

            for run in workload_runs:
                # Calculate duration and throughput once per run
                if "throughput_samples" in run and run["throughput_samples"]:
                    df = pd.DataFrame(run["throughput_samples"])
                    run["_throughput_df"] = df
                    if len(df) >= 2:
                        df = df.sort_values("elapsed_s")
                        duration = df["elapsed_s"].iloc[-1] - df["elapsed_s"].iloc[0]
                        total_count = df["count"].iloc[-1] - df["count"].iloc[0]
                        run["_duration_s"] = duration
                        if duration > 0:
                            run["_throughput_eps"] = total_count / duration
                        else:
                            run["_throughput_eps"] = 0
                    else:
                        # Should not happen with valid data, but provide defaults
                        run["_duration_s"] = 0
                        run["_throughput_eps"] = 0
                else:
                    run["_throughput_df"] = pd.DataFrame()
                    run["_duration_s"] = 0
                    run["_throughput_eps"] = 0

                writers = run["writers"]
                readers = run["readers"]

                # Create nested structure: workload/report-adapter
                adapter = run["adapter"]
                
                # Format directory name based on workload type, zero-padded for sorting
                report_dir_name = f"report-{adapter}-r{readers:03d}-w{writers:03d}"
                report_dir = workload_dir / report_dir_name
                report_dir.mkdir(parents=True, exist_ok=True)

                # Plot latency from JSON percentiles
                plot_latency_cdf_from_json(run, report_dir / "latency_cdf.png")

                plot_throughput(run["_throughput_df"], report_dir / "throughput.png", report_dir / "throughput_data.json")
                generate_html(report_dir, run)

            # Generate per-workload consolidated reports for this session
            if not workload_runs:
                continue

            # Get store order for this workload
            store_order = []
            if workload_config and "stores" in workload_config:
                stores_val = workload_config["stores"]
                if isinstance(stores_val, list):
                    store_order = stores_val
                elif isinstance(stores_val, str):
                    store_order = [stores_val]

            store_order_map = {name: i for i, name in enumerate(store_order)}
            def get_store_rank(adapter_name):
                return store_order_map.get(adapter_name, 999)

            writer_groups = defaultdict(list)
            adapters_set = set()
            writer_counts_set = set()

            first_run = workload_runs[0] if workload_runs else {}
            is_readers = first_run.get("readers", 0) > 0 and first_run.get("writers", 0) == 0
            worker_label = "reader" if is_readers else "writer"
            worker_suffix = "r" if is_readers else "w"

            for run in workload_runs:
                writers = run["writers"]
                readers = run["readers"]
                wc = readers if is_readers else writers
                adapter = run["adapter"]
                writer_groups[wc].append((adapter, run["_throughput_df"], run))
                adapters_set.add(adapter)
                writer_counts_set.add(wc)
                all_adapters.add(adapter)

            for wc, run_data in sorted(writer_groups.items()):
                # Sort run_data by store order before plotting
                run_data = sorted(run_data, key=lambda x: get_store_rank(x[0]))

                # Plot latency comparison using JSON percentiles
                latency_run_data = [(adapter, run) for adapter, _, run in run_data]
                plot_comparison_latency_cdf(
                    latency_run_data,
                    f"Latency CDF — {wc} {worker_label}(s)",
                    workload_dir / f"{workload_name}_comparison_{worker_suffix}{wc}_latency_cdf.png",
                )

                # Plot throughput comparison
                throughput_run_data = [(adapter, throughput_df) for adapter, throughput_df, _ in run_data]
                plot_comparison_throughput(
                    throughput_run_data,
                    f"Throughput — {wc} {worker_label}(s)",
                    workload_dir / f"{workload_name}_comparison_{worker_suffix}{wc}_throughput.png",
                    workload_dir / f"{workload_name}_comparison_{worker_suffix}{wc}_throughput_data.json",
                )

            if len(writer_groups) > 1:
                plot_throughput_scaling(workload_runs, workload_dir / f"{workload_name}_scaling_throughput.png", get_store_rank)
                plot_latency_scaling(workload_runs, workload_dir / f"{workload_name}_scaling_latency.png", get_store_rank)
                plot_peak_cpu_scaling(workload_runs, workload_dir / f"{workload_name}_scaling_peak_cpu.png", get_store_rank)
                plot_peak_mem_scaling(workload_runs, workload_dir / f"{workload_name}_scaling_peak_mem.png", get_store_rank)

            plot_container_metrics(workload_runs, workload_dir / f"{workload_name}_container_metrics.png", get_store_rank)
            generate_workload_html(published_session_dir, workload_name, workload_runs, writer_groups, workload_config, get_store_rank)

            workload_summaries[workload_name] = {
                'run_count': len(workload_runs),
                'adapters': adapters_set,
                'writer_counts': writer_counts_set,
            }

        # Generate session index
        generate_session_index(published_session_dir, session_id, workload_summaries, env_info, session_info)

    # Generate top-level index
    generate_top_level_index(raw_base, published_base)
    print(f"\nTop-level index written to {published_base}/index.html")


if __name__ == "__main__":
    main()
