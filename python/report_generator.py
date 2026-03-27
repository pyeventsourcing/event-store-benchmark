import argparse
import json
import re
from collections import defaultdict
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from matplotlib.ticker import LogLocator, NullFormatter, ScalarFormatter, FormatStrFormatter
from scipy.ndimage import gaussian_filter1d

try:
    from hdrh.histogram import HdrHistogram
    HDR_AVAILABLE = True
except ImportError:
    HDR_AVAILABLE = False
    print("Warning: hdrhistogram library not installed. Run: pip install hdrhistogram")

sns.set_theme(style="whitegrid")

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


def load_session_runs(session_dir: Path, load_samples: bool = True):
    """Load benchmark runs from a single session directory.

    Directory structure: {session_dir}/{run_name}/{adapter}/
    """
    runs = []
    if not session_dir.exists() or not session_dir.is_dir():
        return []

    # Iterate through run directories within each session
    for run_path in sorted(session_dir.iterdir()):
        if not run_path.is_dir():
            continue

        # In the new format, each run directory has subdirectories for adapters
        # Each adapter directory contains summary.json and throughput.jsonl
        for adapter_path in sorted(run_path.iterdir()):
            if not adapter_path.is_dir():
                continue

            summary_file = adapter_path / "summary.json"
            throughput_file = adapter_path / "throughput.jsonl"
            meta_file = adapter_path / "run.meta.json"

            if summary_file.exists():
                with open(summary_file) as f:
                    summary = json.load(f)
                
                meta = {}
                if meta_file.exists():
                    with open(meta_file) as f:
                        meta = json.load(f)

                throughput_samples = []
                if load_samples and throughput_file.exists():
                    with open(throughput_file) as f:
                        for line in f:
                            throughput_samples.append(json.loads(line))

                runs.append({
                    "session_id": session_dir.name,
                    "session_dir": session_dir,
                    "path": adapter_path,
                    "summary": summary,
                    "meta": meta,
                    "throughput_samples": throughput_samples,
                })
    return runs


def load_runs(raw_dir: Path, load_samples: bool = True):
    """Load benchmark runs from raw results directory."""
    runs = []
    if not raw_dir.exists():
        return []

    sessions_dir = raw_dir / "sessions"
    if not sessions_dir.exists():
        print(f"No 'sessions' directory found in {raw_dir}")
        return []

    # Iterate through session directories (timestamped)
    for session_dir in sorted(sessions_dir.iterdir()):
        if not session_dir.is_dir():
            continue
        runs.extend(load_session_runs(session_dir, load_samples))
    return runs


def load_latency_percentiles(file_path: Path):
    """Load latency percentile data from JSON file."""
    if not file_path.exists():
        return None

    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
            return data.get("percentiles", [])
    except Exception as e:
        print(f"Warning: Failed to load latency percentiles from {file_path}: {e}")
        return None


def plot_latency_cdf_from_json(latency_file: Path, out_path: Path):
    """Plot latency CDF from JSON percentile file."""
    percentiles_data = load_latency_percentiles(latency_file)

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

    # Time points (use the end time of each interval)
    time_s = df["elapsed_s"].iloc[1:]

    # Apply moving average smoothing (window size 3 for 1-second samples)
    window_size = min(3, len(eps))
    eps_smooth = eps.rolling(window=window_size, center=True, min_periods=1).mean()

    return {
        "time_s": time_s.values,
        "throughput_eps": eps.values,
        "throughput_eps_smooth": eps_smooth.values,
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
             linewidth=2.0, alpha=0.9, color='#1f77b4', marker='o',
             markersize=3)
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
        run_data: List of (adapter_name, latency_file_path) tuples
        title: Plot title
        out_path: Output path for the plot
    """
    plt.figure(figsize=(8, 5))

    for adapter_name, latency_file in run_data:
        percentiles_data = load_latency_percentiles(latency_file)

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
                label=label, color=color, linewidth=2.0, alpha=0.9, marker='o', markersize=3)

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


def plot_throughput_scaling(runs, out_path: Path):
    """Plot throughput vs worker count (writers or readers), one line per adapter.

    Uses the pre-computed throughput from the summary, which is based on the
    actual test duration (not the response time span).
    """
    # Group by adapter → list of (worker_count, throughput)
    adapter_data = defaultdict(list)

    for run in runs:
        s = run["summary"]
        adapter = s["adapter"]
        writers = s.get("writers", 0)
        readers = s.get("readers", 0)
        worker_count = writers if writers > 0 else readers

        # Use pre-computed throughput from summary
        throughput = s.get("throughput_eps", 0)

        if throughput > 0:
            adapter_data[adapter].append((worker_count, throughput))

    # Determine label based on the workload type
    first_run_summary = runs[0]["summary"] if runs else {}
    is_readers = first_run_summary.get("readers", 0) > 0 and first_run_summary.get("writers", 0) == 0
    xlabel = "Readers" if is_readers else "Writers"
    title = f"Throughput by {xlabel[:-1]} Count"

    plt.figure(figsize=(8, 5))
    for adapter, points in sorted(adapter_data.items()):
        points.sort()
        ws = [p[0] for p in points]
        tps = [p[1] for p in points]
        color = get_adapter_color(adapter)

        # Plot with smoother line interpolation
        plt.plot(ws, tps, marker="o", label=adapter, color=color,
                linewidth=2.5, markersize=8, linestyle='-', alpha=0.9)

    plt.xscale("log")
    plt.yscale("log")
    plt.xlabel(f"{xlabel} [log]")
    plt.ylabel("Throughput (events/sec) [log]")
    formatter = ScalarFormatter()
    formatter.set_scientific(False)
    plt.gca().xaxis.set_major_formatter(formatter)
    plt.gca().yaxis.set_major_formatter(formatter)
    plt.gca().xaxis.set_minor_formatter(NullFormatter())
    
    # Ensure Y-axis has enough ticks/labels even for small ranges on log scale
    plt.gca().yaxis.set_major_locator(LogLocator(base=10.0, subs=(1.0, 2.0, 5.0)))
    plt.gca().yaxis.set_minor_formatter(NullFormatter())
    plt.title(title)
    plt.legend()
    plt.grid(True, which="both", ls=":", alpha=0.6)
    all_worker_counts = sorted({
        (s["summary"].get("writers", 0) if s["summary"].get("writers", 0) > 0
         else s["summary"].get("readers", 0)) for s in runs
    })
    plt.xticks(all_worker_counts, labels=[str(x) for x in all_worker_counts])
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()


def plot_p50_scaling(runs, out_path: Path):
    """Plot p50 latency vs worker count (writers or readers), one line per adapter."""
    adapter_data = defaultdict(list)
    for run in runs:
        s = run["summary"]
        adapter = s["adapter"]
        writers = s.get("writers", 0)
        readers = s.get("readers", 0)
        worker_count = writers if writers > 0 else readers
        p50 = s["latency"]["p50_ms"]
        adapter_data[adapter].append((worker_count, p50))

    # Determine label based on workload type
    first_run_summary_p50 = runs[0]["summary"] if runs else {}
    is_readers = first_run_summary_p50.get("readers", 0) > 0 and first_run_summary_p50.get("writers", 0) == 0
    xlabel = "Readers" if is_readers else "Writers"
    title = f"p50 Latency by {xlabel[:-1]} Count"

    plt.figure(figsize=(8, 5))
    for adapter, points in sorted(adapter_data.items()):
        points.sort()
        ws = [p[0] for p in points]
        p50s = [p[1] for p in points]
        color = get_adapter_color(adapter)
        plt.plot(ws, p50s, marker="o", label=adapter, color=color, linewidth=2, markersize=8)
    plt.xscale("log")
    plt.yscale("log")
    plt.xlabel(f"{xlabel} [log]")
    plt.ylabel("p50 Latency (ms) [log]")
    
    # Use FormatStrFormatter for decimals on latency axis
    formatter = FormatStrFormatter('%.1f')
    plt.gca().yaxis.set_major_formatter(formatter)
    
    # Format x-axis (Writers/Readers count) as integers
    x_formatter = ScalarFormatter()
    x_formatter.set_scientific(False)
    plt.gca().xaxis.set_major_formatter(x_formatter)
    plt.gca().xaxis.set_minor_formatter(NullFormatter())
    
    # Ensure Y-axis has enough ticks/labels even for small ranges on log scale
    plt.gca().yaxis.set_major_locator(LogLocator(base=10.0, subs=(1.0, 2.0, 5.0)))
    plt.gca().yaxis.set_minor_formatter(NullFormatter())
    plt.title(title)
    plt.legend()
    plt.grid(True, which="both", ls=":", alpha=0.6)
    all_worker_counts = sorted({
        (s["summary"].get("writers", 0) if s["summary"].get("writers", 0) > 0
         else s["summary"].get("readers", 0)) for s in runs
    })
    plt.xticks(all_worker_counts, labels=[str(x) for x in all_worker_counts])
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()


def plot_p99_scaling(runs, out_path: Path):
    """Plot p99 latency vs worker count (writers or readers), one line per adapter."""
    adapter_data = defaultdict(list)
    for run in runs:
        s = run["summary"]
        adapter = s["adapter"]
        writers = s.get("writers", 0)
        readers = s.get("readers", 0)
        worker_count = writers if writers > 0 else readers
        p99 = s["latency"]["p99_ms"]
        adapter_data[adapter].append((worker_count, p99))

    # Determine label based on workload type
    first_run_summary_p99 = runs[0]["summary"] if runs else {}
    is_readers = first_run_summary_p99.get("readers", 0) > 0 and first_run_summary_p99.get("writers", 0) == 0
    xlabel = "Readers" if is_readers else "Writers"
    title = f"p99 Latency by {xlabel[:-1]} Count"

    plt.figure(figsize=(8, 5))
    for adapter, points in sorted(adapter_data.items()):
        points.sort()
        ws = [p[0] for p in points]
        p99s = [p[1] for p in points]
        color = get_adapter_color(adapter)
        plt.plot(ws, p99s, marker="o", label=adapter, color=color, linewidth=2, markersize=8)
    plt.xscale("log")
    plt.yscale("log")
    plt.xlabel(f"{xlabel} [log]")
    plt.ylabel("p99 Latency (ms) [log]")
    
    # Use FormatStrFormatter for decimals on latency axis
    formatter = FormatStrFormatter('%.1f')
    plt.gca().yaxis.set_major_formatter(formatter)
    
    # Format x-axis (Writers/Readers count) as integers
    x_formatter = ScalarFormatter()
    x_formatter.set_scientific(False)
    plt.gca().xaxis.set_major_formatter(x_formatter)
    plt.gca().xaxis.set_minor_formatter(NullFormatter())
    
    # Ensure Y-axis has enough ticks/labels even for small ranges on log scale
    plt.gca().yaxis.set_major_locator(LogLocator(base=10.0, subs=(1.0, 2.0, 5.0)))
    plt.gca().yaxis.set_minor_formatter(NullFormatter())
    plt.title(title)
    plt.legend()
    plt.grid(True, which="both", ls=":", alpha=0.6)
    all_worker_counts = sorted({
        (s["summary"].get("writers", 0) if s["summary"].get("writers", 0) > 0
         else s["summary"].get("readers", 0)) for s in runs
    })
    plt.xticks(all_worker_counts, labels=[str(x) for x in all_worker_counts])
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()


def plot_startup_scaling(runs, out_path: Path):
    """Plot startup time vs worker count (writers or readers), one line per adapter."""
    adapter_data = defaultdict(list)
    for run in runs:
        s = run["summary"]
        adapter = s["adapter"]
        writers = s.get("writers", 0)
        readers = s.get("readers", 0)
        worker_count = writers if writers > 0 else readers
        container = s.get("container", {})
        startup = container.get("startup_time_s")
        if startup is not None:
            adapter_data[adapter].append((worker_count, startup))

    if not adapter_data:
        return

    first_run_summary = runs[0]["summary"] if runs else {}
    is_readers = first_run_summary.get("readers", 0) > 0 and first_run_summary.get("writers", 0) == 0
    xlabel = "Readers" if is_readers else "Writers"
    title = f"Startup Times"

    plt.figure(figsize=(8, 5))
    for adapter, points in sorted(adapter_data.items()):
        points.sort()
        ws = [p[0] for p in points]
        startups = [p[1] for p in points]
        color = get_adapter_color(adapter)
        plt.plot(ws, startups, marker="o", label=adapter, color=color, linewidth=2, markersize=8)
    
    plt.xscale("log")
    plt.xlabel(f"{xlabel} [log]")
    plt.ylabel("Startup Time (s)")
    formatter = ScalarFormatter()
    formatter.set_scientific(False)
    plt.gca().xaxis.set_major_formatter(formatter)
    plt.gca().xaxis.set_minor_formatter(NullFormatter())
    
    plt.title(title)
    plt.legend()
    plt.grid(True, which="both", ls=":", alpha=0.6)
    all_worker_counts = sorted({
        (s["summary"].get("writers", 0) if s["summary"].get("writers", 0) > 0
         else s["summary"].get("readers", 0)) for s in runs
    })
    plt.xticks(all_worker_counts, labels=[str(x) for x in all_worker_counts])
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()


def plot_peak_cpu_scaling(runs, out_path: Path):
    """Plot peak CPU usage vs worker count (writers or readers), one line per adapter."""
    adapter_data = defaultdict(list)
    for run in runs:
        s = run["summary"]
        adapter = s["adapter"]
        writers = s.get("writers", 0)
        readers = s.get("readers", 0)
        worker_count = writers if writers > 0 else readers
        container = s.get("container", {})
        peak_cpu = container.get("peak_cpu_percent")
        if peak_cpu is not None:
            adapter_data[adapter].append((worker_count, peak_cpu))

    if not adapter_data:
        return

    first_run_summary = runs[0]["summary"] if runs else {}
    is_readers = first_run_summary.get("readers", 0) > 0 and first_run_summary.get("writers", 0) == 0
    xlabel = "Readers" if is_readers else "Writers"
    title = f"Peak CPU by {xlabel[:-1]} Count"

    plt.figure(figsize=(8, 5))
    for adapter, points in sorted(adapter_data.items()):
        points.sort()
        ws = [p[0] for p in points]
        cpus = [p[1] for p in points]
        color = get_adapter_color(adapter)
        plt.plot(ws, cpus, marker="o", label=adapter, color=color, linewidth=2, markersize=8)
    
    plt.xscale("log")
    plt.xlabel(f"{xlabel} [log]")
    plt.ylabel("Peak CPU (%)")
    formatter = ScalarFormatter()
    formatter.set_scientific(False)
    plt.gca().xaxis.set_major_formatter(formatter)
    plt.gca().xaxis.set_minor_formatter(NullFormatter())
    
    plt.title(title)
    plt.legend()
    plt.grid(True, which="both", ls=":", alpha=0.6)
    all_worker_counts = sorted({
        (s["summary"].get("writers", 0) if s["summary"].get("writers", 0) > 0
         else s["summary"].get("readers", 0)) for s in runs
    })
    plt.xticks(all_worker_counts, labels=[str(x) for x in all_worker_counts])
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()


def plot_peak_mem_scaling(runs, out_path: Path):
    """Plot peak memory usage vs worker count (writers or readers), one line per adapter."""
    adapter_data = defaultdict(list)
    for run in runs:
        s = run["summary"]
        adapter = s["adapter"]
        writers = s.get("writers", 0)
        readers = s.get("readers", 0)
        worker_count = writers if writers > 0 else readers
        container = s.get("container", {})
        peak_mem = container.get("peak_memory_bytes")
        if peak_mem is not None:
            adapter_data[adapter].append((worker_count, peak_mem / (1024 * 1024)))

    if not adapter_data:
        return

    first_run_summary = runs[0]["summary"] if runs else {}
    is_readers = first_run_summary.get("readers", 0) > 0 and first_run_summary.get("writers", 0) == 0
    xlabel = "Readers" if is_readers else "Writers"
    title = f"Peak Memory by {xlabel[:-1]} Count"

    plt.figure(figsize=(8, 5))
    for adapter, points in sorted(adapter_data.items()):
        points.sort()
        ws = [p[0] for p in points]
        mems = [p[1] for p in points]
        color = get_adapter_color(adapter)
        plt.plot(ws, mems, marker="o", label=adapter, color=color, linewidth=2, markersize=8)
    
    plt.xscale("log")
    plt.xlabel(f"{xlabel} [log]")
    plt.ylabel("Peak Memory (MB)")
    formatter = ScalarFormatter()
    formatter.set_scientific(False)
    plt.gca().xaxis.set_major_formatter(formatter)
    plt.gca().xaxis.set_minor_formatter(NullFormatter())
    
    plt.title(title)
    plt.legend()
    plt.grid(True, which="both", ls=":", alpha=0.6)
    all_worker_counts = sorted({
        (s["summary"].get("writers", 0) if s["summary"].get("writers", 0) > 0
         else s["summary"].get("readers", 0)) for s in runs
    })
    plt.xticks(all_worker_counts, labels=[str(x) for x in all_worker_counts])
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()


def plot_container_metrics(runs, out_path: Path):
    """Create a dramatic visualization of container resource usage."""
    # Collect data for each unique adapter (use dict to deduplicate and aggregate)
    adapter_data = {}

    for run in runs:
        s = run["summary"]
        adapter = s["adapter"]
        container = s.get("container", {})

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

    # Create a composite score for ordering (lower is better):
    # Normalize each metric to 0-1 range, then compute weighted average
    adapters_list = list(adapter_data.keys())

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
    summary = run["summary"]
    latency_img = report_dir / "latency_cdf.png"
    throughput_img = report_dir / "throughput.png"

    html = f"""
<!DOCTYPE html>
<html>
<head>
  <meta charset='utf-8'>
  <title>Workload Report — {summary['adapter']} / {summary['workload']}</title>
  <style>
    body {{ font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif; margin: 2rem; }}
    h1, h2 {{ margin-top: 1.2rem; }}
    .row {{ display: flex; gap: 1rem; flex-wrap: wrap; }}
    .card {{ border: 1px solid #eee; border-radius: 8px; padding: 1rem; }}
  </style>
</head>
<body>
  <h1>Benchmark Report</h1>
  <p><b>Adapter:</b> {summary['adapter']} &nbsp; | &nbsp; <b>Workload:</b> {summary['workload']}</p>
  <p><b>Duration:</b> {summary['duration_s']:.1f}s &nbsp; | &nbsp; <b>Throughput:</b> {summary['throughput_eps']:.0f} eps</p>
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
</body>
</html>
"""
    with open(report_dir / "index.html", "w") as f:
        f.write(html)


def generate_workload_html(out_base: Path, workload_name: str, runs, writer_groups):
    """Generate a consolidated report for a specific workload."""
    # Summary table
    summary_rows = ""
    def row_key(row):
        s = row["summary"]
        adapter = s["adapter"]
        writers = s.get("writers", 0)
        readers = s.get("readers", 0)
        return (writers, readers, adapter)

    for run in sorted(runs, key=row_key):
        s = run["summary"]
        adapter = s["adapter"]
        writers = s.get("writers", 0)
        readers = s.get("readers", 0)

        # Determine link format based on workload type
        report_link = f"../{workload_name}/report-{adapter}-r{readers:03d}-w{writers:03d}/index.html"
        if readers > 0 and writers == 0:
            worker_display = readers
        elif writers > 0 and readers == 0:
            worker_display = writers
        else:
            worker_display = f"{writers}w/{readers}r"

        # Get container metrics
        container = s.get("container", {})
        startup_time = f"{container.get('startup_time_s', 0):.1f}s" if container.get("startup_time_s") else "N/A"
        image_size_mb = f"{container.get('image_size_bytes', 0) / 1024 / 1024:.0f}" if container.get("image_size_bytes") else "N/A"

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
        <td>{workload_name}</td>
        <td>{worker_display}</td>
        <td>{s['duration_s']:.1f}s</td>
        <td>{s['throughput_eps']:.0f}</td>
        <td>{s['latency']['p50_ms']:.2f}</td>
        <td>{s['latency']['p99_ms']:.2f}</td>
        <td>{image_size_mb}</td>
        <td>{startup_time}</td>
        <td>{cpu_display}</td>
        <td>{mem_display}</td>
      </tr>"""

    # Per-worker-count comparison sections
    # Determine if this is a readers or writers workload
    first_run = runs[0]["summary"] if runs else {}
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
        <h3>Startup Times</h3>
        <img src='{workload_name}_scaling_startup.png' width='560'>
      </div>
      <div class='card'>
        <h3>Throughput vs {worker_label}</h3>
        <img src='{workload_name}_scaling_throughput.png' width='560'>
      </div>
    </div>
    <div class='row'>
      <div class='card'>
        <h3>p50 Latency vs {worker_label}</h3>
        <img src='{workload_name}_scaling_p50.png' width='560'>
      </div>
      <div class='card'>
        <h3>p99 Latency vs {worker_label}</h3>
        <img src='{workload_name}_scaling_p99.png' width='560'>
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
    <tr><th>Adapter</th><th>Workload</th><th>{worker_label}</th><th>Duration</th><th>Throughput (eps)</th><th>p50 (ms)</th><th>p99 (ms)</th><th>Image (MB)</th><th>Startup</th><th>CPU (avg/peak)</th><th>Mem MB (avg/peak)</th></tr>
    {summary_rows}
  </table>
</body>
</html>
"""
    workload_dir = out_base / workload_name
    workload_dir.mkdir(parents=True, exist_ok=True)
    with open(workload_dir / "index.html", "w") as f:
        f.write(html)


def generate_top_level_index(out_base: Path, sessions_summaries):
    """Generate top-level index.html that links to individual session reports."""
    
    session_rows = ""
    for session_id, summary in sorted(sessions_summaries.items(), reverse=True):
        workloads = ", ".join(sorted(summary['workloads']))
        adapters = ", ".join(sorted(summary['adapters']))
        
        session_rows += f"""
      <tr>
        <td><a href='{session_id}/index.html'>{session_id}</a></td>
        <td>{summary.get('workload_name', 'N/A')}</td>
        <td>{workloads}</td>
        <td>{adapters}</td>
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
    <tr><th>Session ID</th><th>Primary Workload</th><th>Logical Workloads</th><th>Adapters</th><th>Version</th></tr>
    {session_rows}
  </table>
</body>
</html>
"""
    with open(out_base / "index.html", "w") as f:
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
          <h3>Startup Times</h3>
          <img src='{workload_name}/{workload_name}_scaling_startup.png' width='460'>
        </div>
        <div class='card'>
          <h3>Throughput</h3>
          <img src='{workload_name}/{workload_name}_scaling_throughput.png' width='460'>
        </div>
      </div>
      <div class='row'>
        <div class='card'>
          <h3>p50 Latency</h3>
          <img src='{workload_name}/{workload_name}_scaling_p50.png' width='460'>
        </div>
        <div class='card'>
          <h3>p99 Latency</h3>
          <img src='{workload_name}/{workload_name}_scaling_p99.png' width='460'>
        </div>
      </div>
      <div class='row'>
        <div class='card'>
          <h3>Peak Memory</h3>
          <img src='{workload_name}/{workload_name}_scaling_peak_mem.png' width='460'>
        </div>
        <div class='card'>
          <h3>Peak CPU</h3>
          <img src='{workload_name}/{workload_name}_scaling_peak_cpu.png' width='460'>
        </div>
      </div>"""

        workload_sections += f"""
    <div class='workload-section'>
      <h2><a href='{workload_name}/index.html'>{workload_name}</a></h2>
      <div class='workload-info'>
        <p><b>Runs:</b> {summary['run_count']}</p>
        <p><b>Adapters tested:</b> {', '.join(sorted(summary['adapters']))}</p>
        <p><b>Worker counts:</b> {', '.join(map(str, sorted(summary['writer_counts'])))}</p>
      </div>
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
    parser = argparse.ArgumentParser(description="Generate ES-BENCH benchmark report from raw results")
    parser.add_argument("--raw", type=str, default="results/raw", help="Path to raw results dir")
    parser.add_argument("--out", type=str, default="results/published", help="Output reports dir")
    parser.add_argument("--force", action="store_true", help="Force regeneration of already published sessions")
    args = parser.parse_args()

    raw_dir = Path(args.raw)
    out_base = Path(args.out)
    out_base.mkdir(parents=True, exist_ok=True)

    sessions_dir = raw_dir / "sessions"
    if not sessions_dir.exists():
        print(f"No sessions found in {sessions_dir}")
        return

    session_ids = sorted([d.name for d in sessions_dir.iterdir() if d.is_dir()])
    if not session_ids:
        print(f"No sessions found in {sessions_dir}")
        return

    # Group runs by session
    session_runs = defaultdict(list)
    for session_id in session_ids:
        session_out_dir = out_base / session_id
        session_index = session_out_dir / "index.html"
        force_regenerate = args.force
        
        # If not forced and already exists, load without samples for speed
        load_samples = force_regenerate or not session_index.exists()
        
        session_dir = sessions_dir / session_id
        runs = load_session_runs(session_dir, load_samples=load_samples)
        session_runs[session_id].extend(runs)

    if not session_runs:
        print(f"No runs found in {raw_dir}")
        return

    sessions_summaries = {}

    for session_id, runs in sorted(session_runs.items()):
        session_out_dir = out_base / session_id
        
        # Check if session already exists and skip if not forced
        session_index = session_out_dir / "index.html"
        skip_session = session_index.exists() and not args.force
        
        if skip_session:
            print(f">>> Session {session_id} already published, skipping regeneration (loaded without samples)...")
        elif not runs:
            print(f">>> No runs found for session {session_id}. Skipping.")
            continue
        else:
            print(f">>> Processing session: {session_id}")

        session_out_dir.mkdir(parents=True, exist_ok=True)

        # Load environment and session info for this session
        env_info = None
        session_info = None
        
        session_dir = runs[0]["session_dir"]
        env_file = session_dir / "environment.json"
        if env_file.exists():
            try:
                with open(env_file, "r") as f:
                    env_info = json.load(f)
            except Exception as e:
                print(f"Warning: Could not load {env_file}: {e}")
        
        # Fallback to old format if no environment.json
        if not env_info:
            env_check_file = raw_dir / "env_check.json"
            if env_check_file.exists():
                try:
                    with open(env_check_file, "r") as f:
                        env_info = json.load(f)
                except Exception as e:
                    print(f"Warning: Could not load {env_check_file}: {e}")

        session_info_file = session_dir / "session.json"
        if session_info_file.exists():
            try:
                with open(session_info_file, "r") as f:
                    session_info = json.load(f)
            except Exception as e:
                print(f"Warning: Could not load {session_info_file}: {e}")

        # Group runs by workload within the session
        workload_groups = defaultdict(list)
        for run in runs:
            full_workload_name = run["summary"]["workload"]
            workload_name = re.sub(r'-w\d+-r\d+$', '', full_workload_name)
            workload_groups[workload_name].append(run)

        # Skip remaining processing if session already published
        if skip_session:
            # Still need to collect workload information for the top-level index
            workload_summaries = {}
            all_adapters = set()
            for workload_name, workload_runs in workload_groups.items():
                adapters_set = set()
                writer_counts_set = set()
                
                first_run = workload_runs[0]["summary"] if workload_runs else {}
                is_readers = first_run.get("readers", 0) > 0 and first_run.get("writers", 0) == 0

                for run in workload_runs:
                    writers = run["summary"].get("writers", 0)
                    readers = run["summary"].get("readers", 0)
                    wc = readers if is_readers else writers
                    adapter = run["summary"]["adapter"]
                    adapters_set.add(adapter)
                    writer_counts_set.add(wc)
                    all_adapters.add(adapter)
                
                workload_summaries[workload_name] = {
                    'run_count': len(workload_runs),
                    'adapters': adapters_set,
                    'writer_counts': writer_counts_set,
                }
            
            sessions_summaries[session_id] = {
                'workload_name': session_info.get('workload_name') if session_info else 'N/A',
                'benchmark_version': session_info.get('benchmark_version') if session_info else 'N/A',
                'workloads': list(workload_summaries.keys()),
                'adapters': list(all_adapters),
            }
            continue

        # Generate individual reports for each run in this session
        for run in runs:
            throughput_df = pd.DataFrame(run["throughput_samples"])
            run["_throughput_df"] = throughput_df
            adapter = run["summary"]["adapter"]
            workload_name = run["summary"]["workload"]

            writers = run["summary"].get("writers", 0)
            readers = run["summary"].get("readers", 0)

            # Create nested structure: workload/report-adapter
            report_workload_name = re.sub(r'-w\d+-r\d+$', '', workload_name)
            workload_dir = session_out_dir / report_workload_name
            workload_dir.mkdir(parents=True, exist_ok=True)

            # Format directory name based on workload type, zero-padded for sorting
            report_dir_name = f"report-{adapter}-r{readers:03d}-w{writers:03d}"
            report_dir = workload_dir / report_dir_name
            report_dir.mkdir(parents=True, exist_ok=True)

            # Plot latency from JSON percentiles
            latency_file = run["path"] / "latency.json"
            plot_latency_cdf_from_json(latency_file, report_dir / "latency_cdf.png")

            plot_throughput(throughput_df, report_dir / "throughput.png", report_dir / "throughput_data.json")
            generate_html(report_dir, run)

        # Generate per-workload consolidated reports for this session
        workload_summaries = {}
        all_adapters = set()
        for workload_name, workload_runs in workload_groups.items():
            print(f"  Processing workload: {workload_name}")

            writer_groups = defaultdict(list)
            adapters_set = set()
            writer_counts_set = set()

            first_run = workload_runs[0]["summary"] if workload_runs else {}
            is_readers = first_run.get("readers", 0) > 0 and first_run.get("writers", 0) == 0
            worker_label = "reader" if is_readers else "writer"
            worker_suffix = "r" if is_readers else "w"

            for run in workload_runs:
                writers = run["summary"].get("writers", 0)
                readers = run["summary"].get("readers", 0)
                wc = readers if is_readers else writers
                adapter = run["summary"]["adapter"]
                latency_file = run["path"] / "latency.json"
                writer_groups[wc].append((adapter, run["_throughput_df"], latency_file))
                adapters_set.add(adapter)
                writer_counts_set.add(wc)
                all_adapters.add(adapter)

            workload_dir = session_out_dir / workload_name
            workload_dir.mkdir(parents=True, exist_ok=True)

            for wc, run_data in sorted(writer_groups.items()):
                # Plot latency comparison using JSON percentiles
                latency_run_data = [(adapter, latency_file) for adapter, _, latency_file in run_data]
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
                plot_throughput_scaling(workload_runs, workload_dir / f"{workload_name}_scaling_throughput.png")
                plot_p50_scaling(workload_runs, workload_dir / f"{workload_name}_scaling_p50.png")
                plot_p99_scaling(workload_runs, workload_dir / f"{workload_name}_scaling_p99.png")
                plot_startup_scaling(workload_runs, workload_dir / f"{workload_name}_scaling_startup.png")
                plot_peak_cpu_scaling(workload_runs, workload_dir / f"{workload_name}_scaling_peak_cpu.png")
                plot_peak_mem_scaling(workload_runs, workload_dir / f"{workload_name}_scaling_peak_mem.png")

            plot_container_metrics(workload_runs, workload_dir / f"{workload_name}_container_metrics.png")
            generate_workload_html(session_out_dir, workload_name, workload_runs, writer_groups)

            workload_summaries[workload_name] = {
                'run_count': len(workload_runs),
                'adapters': adapters_set,
                'writer_counts': writer_counts_set,
            }

        # Generate session index
        generate_session_index(session_out_dir, session_id, workload_summaries, env_info, session_info)
        
        # Collect session summary for top-level index
        sessions_summaries[session_id] = {
            'workload_name': session_info.get('workload_name') if session_info else 'N/A',
            'benchmark_version': session_info.get('benchmark_version') if session_info else 'N/A',
            'workloads': list(workload_summaries.keys()),
            'adapters': list(all_adapters),
        }

    # Generate top-level index
    generate_top_level_index(out_base, sessions_summaries)
    print(f"\nTop-level index written to {out_base}/index.html")


if __name__ == "__main__":
    main()
