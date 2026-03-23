import argparse
import json
import re
from collections import defaultdict
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from scipy.ndimage import gaussian_filter1d

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
        # Each adapter directory contains summary.json and samples.jsonl
        for adapter_path in sorted(run_path.iterdir()):
            if not adapter_path.is_dir():
                continue

            summary_file = adapter_path / "summary.json"
            samples_file = adapter_path / "samples.jsonl"
            meta_file = adapter_path / "run.meta.json"

            if summary_file.exists():
                with open(summary_file) as f:
                    summary = json.load(f)
                
                meta = {}
                if meta_file.exists():
                    with open(meta_file) as f:
                        meta = json.load(f)

                samples = []
                if load_samples and samples_file.exists():
                    with open(samples_file) as f:
                        for line in f:
                            samples.append(json.loads(line))

                runs.append({
                    "session_id": session_dir.name,
                    "session_dir": session_dir,
                    "path": adapter_path,
                    "summary": summary,
                    "meta": meta,
                    "samples": samples,
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


def plot_latency_cdf(samples: pd.DataFrame, out_path: Path):
    if samples.empty or "ok" not in samples.columns:
        return
    lat_ms = samples.loc[samples["ok"], "latency_us"].astype(float) / 1000.0
    lat_ms = lat_ms.clip(lower=1e-3)
    lat_sorted = np.sort(lat_ms.values)
    p = np.linspace(0, 100, len(lat_sorted), endpoint=False)
    plt.figure(figsize=(6, 4))
    plt.plot(lat_sorted, p, label="append latency CDF")
    plt.xscale("log")
    plt.xlabel("Latency (ms) [log]")
    plt.ylabel("Percentile (%)")
    plt.title("Latency CDF")
    plt.grid(True, which="both", ls=":")
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()


def compute_throughput_timeseries(samples: pd.DataFrame, bin_size_ms: int = 500):
    """Compute throughput time series from response timestamps.

    Uses fixed-size time bins to compute throughput, which handles both
    single-threaded and concurrent workloads correctly.

    Returns:
        dict with 'time_s', 'throughput_eps', and 'throughput_eps_smooth' arrays
        or None if no valid data
    """
    if samples.empty or "ok" not in samples.columns:
        return None
    df = samples.copy()

    # Filter only successful operations
    df = df[df["ok"] == True].copy()

    if len(df) < 2:
        return None

    # Sort by timestamp
    df = df.sort_values("t_ms").reset_index(drop=True)

    t_min = df["t_ms"].min()
    t_max = df["t_ms"].max()
    duration_ms = t_max - t_min

    if duration_ms <= 0:
        return None

    # Create fixed-size bins
    num_bins = max(1, int(np.ceil(duration_ms / bin_size_ms)))

    # Assign each response to a bin
    bins = ((df["t_ms"] - t_min) / bin_size_ms).astype(int)
    bins = np.clip(bins, 0, num_bins - 1)

    # Count responses per bin
    bin_counts = np.bincount(bins, minlength=num_bins)

    # Convert to events per second
    eps = bin_counts * (1000.0 / bin_size_ms)

    # Time points at bin centers
    time_s = (np.arange(num_bins) + 0.5) * bin_size_ms / 1000.0

    # Apply smoothing using Gaussian filter for smoother curves
    # Use wider sigma for better smoothing
    eps_smooth = gaussian_filter1d(eps, sigma=1.5) if len(eps) > 2 else eps

    return {
        "time_s": time_s,
        "throughput_eps": eps,
        "throughput_eps_smooth": eps_smooth,
    }


def plot_throughput(samples: pd.DataFrame, out_path: Path, data_path: Path = None):
    """Plot throughput over time with both raw and smoothed data."""
    result = compute_throughput_timeseries(samples, bin_size_ms=500)

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
    # Plot raw data with thin line
    plt.plot(result["time_s"], result["throughput_eps"],
             linewidth=0.5, alpha=0.4, color='#1f77b4', label='Raw')
    # Plot smoothed data with thick line
    plt.plot(result["time_s"], result["throughput_eps_smooth"],
             linewidth=2.5, alpha=0.9, color='#1f77b4', label='Smoothed')
    plt.xlabel("Time (s)")
    plt.ylabel("Events/sec")
    plt.title("Throughput over time")
    plt.legend()
    plt.grid(True, ls=":", alpha=0.6)
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()


def plot_comparison_latency_cdf(run_data, title, out_path: Path):
    """Plot latency CDF comparing stores for a specific writer count."""
    plt.figure(figsize=(8, 5))
    for label, samples_df in run_data:
        if samples_df.empty or "ok" not in samples_df.columns:
            continue
        lat_ms = samples_df.loc[samples_df["ok"], "latency_us"].astype(float) / 1000.0
        lat_ms = lat_ms.clip(lower=1e-3)
        lat_sorted = np.sort(lat_ms.values)
        p = np.linspace(0, 100, len(lat_sorted), endpoint=False)
        color = get_adapter_color(label)
        plt.plot(lat_sorted, p, label=label, color=color, linewidth=2)
    plt.xscale("log")
    plt.xlabel("Latency (ms) [log]")
    plt.ylabel("Percentile (%)")
    plt.title(title)
    plt.legend()
    plt.grid(True, which="both", ls=":")
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()


def plot_comparison_throughput(run_data, title, out_path: Path, data_path: Path = None):
    """Plot throughput over time comparing stores for a specific writer count."""
    plt.figure(figsize=(8, 5))

    # Store data for all adapters if data_path provided
    all_data = {}

    for label, samples_df in run_data:
        result = compute_throughput_timeseries(samples_df, bin_size_ms=500)

        if result is None:
            continue

        color = get_adapter_color(label)
        # Plot raw data with thin line
        plt.plot(result["time_s"], result["throughput_eps"],
                linewidth=0.5, alpha=0.2, color=color)
        # Plot smoothed data with thick line
        plt.plot(result["time_s"], result["throughput_eps_smooth"],
                label=label, color=color, linewidth=2.5, alpha=0.9)

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

    plt.xlabel("Time (s)")
    plt.ylabel("Events/sec")
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
    title = f"Throughput Scaling by {xlabel[:-1]} Count"

    plt.figure(figsize=(8, 5))
    for adapter, points in sorted(adapter_data.items()):
        points.sort()
        ws = [p[0] for p in points]
        tps = [p[1] for p in points]
        color = get_adapter_color(adapter)

        # Plot with smoother line interpolation
        plt.plot(ws, tps, marker="o", label=adapter, color=color,
                linewidth=2.5, markersize=8, linestyle='-', alpha=0.9)

    plt.xlabel(xlabel)
    plt.ylabel("Throughput (events/sec)")
    plt.title(title)
    plt.legend()
    plt.grid(True, ls=":", alpha=0.6)
    all_worker_counts = sorted({
        (s["summary"].get("writers", 0) if s["summary"].get("writers", 0) > 0
         else s["summary"].get("readers", 0)) for s in runs
    })
    plt.xticks(all_worker_counts)
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
    title = f"p99 Latency Scaling by {xlabel[:-1]} Count"

    plt.figure(figsize=(8, 5))
    for adapter, points in sorted(adapter_data.items()):
        points.sort()
        ws = [p[0] for p in points]
        p99s = [p[1] for p in points]
        color = get_adapter_color(adapter)
        plt.plot(ws, p99s, marker="o", label=adapter, color=color, linewidth=2, markersize=8)
    plt.xlabel(xlabel)
    plt.ylabel("p99 Latency (ms)")
    plt.title(title)
    plt.legend()
    plt.grid(True, ls=":")
    all_worker_counts = sorted({
        (s["summary"].get("writers", 0) if s["summary"].get("writers", 0) > 0
         else s["summary"].get("readers", 0)) for s in runs
    })
    plt.xticks(all_worker_counts)
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
    for run in runs:
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
        <h3>Throughput vs Writers</h3>
        <img src='{workload_name}_scaling_throughput.png' width='560'>
      </div>
      <div class='card'>
        <h3>p99 Latency vs Writers</h3>
        <img src='{workload_name}_scaling_p99.png' width='560'>
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
            cpu_model = env_info.get('cpu', {}).get('model', 'N/A')
            cpu_cores = env_info.get('cpu', {}).get('cores', 'N/A')
            kernel = env_info.get('os', {}).get('kernel', 'N/A')
            mem_gb = env_info.get('memory', {}).get('total_bytes', 0) // (1024**3)
            fs_type = env_info.get('disk', {}).get('filesystem', 'N/A')
            disk_type = env_info.get('disk', {}).get('type', 'N/A')
            
            env_section = f"""
    <div class='workload-section'>
      <h2>Environment Information</h2>
      <div class='row'>
        <div class='card'>
          <h3>System</h3>
          <p><b>CPU:</b> {cpu_model} ({cpu_cores} cores)</p>
          <p><b>Kernel:</b> {kernel}</p>
          <p><b>Memory:</b> {mem_gb} GB total</p>
        </div>
        <div class='card'>
          <h3>Storage</h3>
          <p><b>Disk Type:</b> {disk_type}</p>
          <p><b>FS Type:</b> {fs_type}</p>
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
          <h3>Throughput Scaling</h3>
          <img src='{workload_name}/{workload_name}_scaling_throughput.png' width='460'>
        </div>
        <div class='card'>
          <h3>p99 Latency Scaling</h3>
          <img src='{workload_name}/{workload_name}_scaling_p99.png' width='460'>
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
            samples_df = pd.DataFrame(run["samples"])
            run["_samples_df"] = samples_df
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

            plot_latency_cdf(samples_df, report_dir / "latency_cdf.png")
            plot_throughput(samples_df, report_dir / "throughput.png", report_dir / "throughput_data.json")
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
                writer_groups[wc].append((adapter, run["_samples_df"]))
                adapters_set.add(adapter)
                writer_counts_set.add(wc)
                all_adapters.add(adapter)

            workload_dir = session_out_dir / workload_name
            workload_dir.mkdir(parents=True, exist_ok=True)

            for wc, run_data in sorted(writer_groups.items()):
                plot_comparison_latency_cdf(
                    run_data,
                    f"Latency CDF — {wc} {worker_label}(s)",
                    workload_dir / f"{workload_name}_comparison_{worker_suffix}{wc}_latency_cdf.png",
                )
                plot_comparison_throughput(
                    run_data,
                    f"Throughput — {wc} {worker_label}(s)",
                    workload_dir / f"{workload_name}_comparison_{worker_suffix}{wc}_throughput.png",
                    workload_dir / f"{workload_name}_comparison_{worker_suffix}{wc}_throughput_data.json",
                )

            if len(writer_groups) > 1:
                plot_throughput_scaling(workload_runs, workload_dir / f"{workload_name}_scaling_throughput.png")
                plot_p99_scaling(workload_runs, workload_dir / f"{workload_name}_scaling_p99.png")

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
