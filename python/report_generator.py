import argparse
import json
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
}

def get_adapter_color(adapter_name):
    """Get consistent color for an adapter."""
    return ADAPTER_COLORS.get(adapter_name, '#cccccc')


def load_runs(raw_dir: Path):
    """Load benchmark runs from raw results directory.

    Samples are already filtered to the measurement window by the benchmark runner
    (warmup/cooldown excluded). We load all samples and let individual plot functions
    handle edge case filtering as needed.

    Directory structure: raw_dir/{workload}/{adapter}-r{N}-w{N}/
    """
    runs = []
    # Iterate through workload directories
    for workload_dir in sorted(raw_dir.iterdir()):
        if not workload_dir.is_dir():
            continue
        # Iterate through run directories within each workload
        for run_path in sorted(workload_dir.iterdir()):
            if not run_path.is_dir():
                continue
            summary_file = run_path / "summary.json"
            samples_file = run_path / "samples.jsonl"
            if summary_file.exists() and samples_file.exists():
                with open(summary_file) as f:
                    summary = json.load(f)
                samples = []
                with open(samples_file) as f:
                    for line in f:
                        samples.append(json.loads(line))

                runs.append({
                    "path": run_path,
                    "summary": summary,
                    "samples": samples,
                })
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


def compute_throughput_timeseries(samples: pd.DataFrame, bin_size_ms: int = 50):
    """Compute throughput time series from samples.

    Buckets are aligned so they start at the first sample timestamp and don't
    extend beyond the last sample. This ensures all buckets are complete.

    Returns:
        dict with 'time_s', 'throughput_eps', and 'throughput_eps_smooth' arrays
        or None if no valid data
    """
    if samples.empty or "ok" not in samples.columns:
        return None
    df = samples.copy()

    # Filter only successful operations
    df = df[df["ok"] == True].copy()

    if len(df) == 0:
        return None

    # Find actual time range from samples
    t_min = df["t_ms"].min()
    t_max = df["t_ms"].max()
    duration_ms = t_max - t_min

    if duration_ms <= 0:
        return None

    # Create bins that fit exactly within the sample time range
    # Start bins at t_min, and only create complete bins
    num_bins = int(duration_ms / bin_size_ms)

    if num_bins == 0:
        return None

    # Assign each sample to a bin
    df["bin"] = ((df["t_ms"] - t_min) / bin_size_ms).astype(int)
    # Exclude any samples that fall into an incomplete final bin
    df = df[df["bin"] < num_bins]

    # Count samples per bin
    bin_counts = df.groupby("bin").size()

    # Create complete array with zeros for empty bins
    counts = np.zeros(num_bins)
    for bin_idx, count in bin_counts.items():
        counts[bin_idx] = count

    # Convert to events per second
    eps = counts * (1000.0 / bin_size_ms)

    # Time points at bin centers (more intuitive for plotting)
    time_s = (np.arange(num_bins) + 0.5) * bin_size_ms / 1000.0

    # Apply smoothing using Gaussian filter for smoother curves
    eps_smooth = gaussian_filter1d(eps, sigma=2.0)

    return {
        "time_s": time_s,
        "throughput_eps": eps,
        "throughput_eps_smooth": eps_smooth,
    }


def plot_throughput(samples: pd.DataFrame, out_path: Path, data_path: Path = None):
    """Plot throughput over time with both raw and smoothed data."""
    result = compute_throughput_timeseries(samples, bin_size_ms=50)

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
        result = compute_throughput_timeseries(samples_df, bin_size_ms=50)

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

    Computes throughput from raw samples, filtering by 'ok' status and
    excluding first and last time groups to avoid warm-up/cool-down artifacts.
    """
    # Group by adapter → list of (worker_count, throughput)
    adapter_data = defaultdict(list)

    for run in runs:
        s = run["summary"]
        adapter = s["adapter"]
        writers = s.get("writers", 0)
        readers = s.get("readers", 0)
        worker_count = writers if writers > 0 else readers

        # Compute throughput from raw samples for better accuracy
        samples_df = pd.DataFrame(run["samples"])

        if samples_df.empty or "ok" not in samples_df.columns:
            continue

        # Filter by ok=true
        ok_samples = samples_df[samples_df["ok"] == True].copy()

        if len(ok_samples) == 0:
            continue

        # Convert to datetime and group by finer intervals (50ms for more granularity)
        ok_samples["timestamp"] = pd.to_datetime(ok_samples["t_ms"], unit="ms")
        ok_samples = ok_samples.set_index("timestamp")

        # Group by 50ms intervals
        grp = ok_samples.resample("50ms").size()

        # Drop first and last groups to avoid warm-up/cool-down artifacts
        if len(grp) > 2:
            grp = grp.iloc[1:-1]

        if len(grp) == 0:
            continue

        # Convert to events/sec (50ms → 20 samples per second)
        eps = grp * 20

        # Use median throughput for a more robust estimate
        throughput = eps.median()

        adapter_data[adapter].append((worker_count, throughput))

    # Determine label based on the workload type
    first_run = runs[0]["summary"] if runs else {}
    is_readers = first_run.get("readers", 0) > 0 and first_run.get("writers", 0) == 0
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
    first_run = runs[0]["summary"] if runs else {}
    is_readers = first_run.get("readers", 0) > 0 and first_run.get("writers", 0) == 0
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


def generate_workflow_html(out_base: Path, workflow_name: str, runs, writer_groups):
    """Generate a consolidated report for a specific workflow."""
    # Summary table
    summary_rows = ""
    for run in runs:
        s = run["summary"]
        adapter = s["adapter"]
        workload = Path(s["workload"]).stem
        writers = s.get("writers", 0)
        readers = s.get("readers", 0)

        # Determine link format based on workflow type
        workflow = extract_workflow_name(workload)
        report_link = f"../{workflow}/report-{adapter}-r{readers:03d}-w{writers:03d}/index.html"
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
        <td>{workload}</td>
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
    # Determine if this is a readers or writers workflow
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
        <img src='{workflow_name}_comparison_{worker_suffix}{wc}_latency_cdf.png' width='560'>
      </div>
      <div class='card'>
        <h3>Throughput over time</h3>
        <img src='{workflow_name}_comparison_{worker_suffix}{wc}_throughput.png' width='560'>
      </div>
    </div>"""

    # Container metrics section
    container_section = f"""
    <h2>Container Resource Metrics</h2>
    <div class='card' style='max-width: 100%;'>
      <img src='{workflow_name}_container_metrics.png' style='width: 100%; max-width: 1200px;'>
    </div>"""

    # Scaling charts (only if multiple writer counts)
    scaling_section = ""
    if len(writer_groups) > 1:
        scaling_section = f"""
    <h2>Scaling</h2>
    <div class='row'>
      <div class='card'>
        <h3>Throughput vs Writers</h3>
        <img src='{workflow_name}_scaling_throughput.png' width='560'>
      </div>
      <div class='card'>
        <h3>p99 Latency vs Writers</h3>
        <img src='{workflow_name}_scaling_p99.png' width='560'>
      </div>
    </div>"""

    html = f"""
<!DOCTYPE html>
<html>
<head>
  <meta charset='utf-8'>
  <title>Workload Report — {workflow_name}</title>
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
  <h1>Workload Report — {workflow_name}</h1>
  <p><a href="../index.html">← Back to all workflows</a></p>
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
    workflow_dir = out_base / workflow_name
    workflow_dir.mkdir(parents=True, exist_ok=True)
    with open(workflow_dir / "index.html", "w") as f:
        f.write(html)


def generate_top_level_index(out_base: Path, workflow_summaries, env_info=None):
    """Generate top-level index.html that links to individual workflow reports."""

    env_section = ""
    if env_info:
        env_section = f"""
    <div class='workflow-section'>
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

    workflow_sections = ""
    for workflow_name, summary in sorted(workflow_summaries.items()):
        # Include scaling plots if this workflow has multiple writer counts
        scaling_plots = ""
        if len(summary['writer_counts']) > 1:
            scaling_plots = f"""
      <div class='row'>
        <div class='card'>
          <h3>Throughput Scaling</h3>
          <img src='{workflow_name}/{workflow_name}_scaling_throughput.png' width='460'>
        </div>
        <div class='card'>
          <h3>p99 Latency Scaling</h3>
          <img src='{workflow_name}/{workflow_name}_scaling_p99.png' width='460'>
        </div>
      </div>"""

        workflow_sections += f"""
    <div class='workflow-section'>
      <h2><a href='{workflow_name}/index.html'>{workflow_name}</a></h2>
      <div class='workflow-info'>
        <p><b>Runs:</b> {summary['run_count']}</p>
        <p><b>Adapters tested:</b> {', '.join(sorted(summary['adapters']))}</p>
        <p><b>Writer counts:</b> {', '.join(map(str, sorted(summary['writer_counts'])))}</p>
      </div>
      {scaling_plots}
    </div>"""

    html = f"""
<!DOCTYPE html>
<html>
<head>
  <meta charset='utf-8'>
  <title>ES-BENCH Benchmark Suite</title>
  <style>
    body {{ font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif; margin: 2rem; }}
    h1, h2, h3 {{ margin-top: 1.2rem; }}
    .workflow-section {{ border: 1px solid #ddd; border-radius: 8px; padding: 1.5rem; margin: 1.5rem 0; background: #fafafa; }}
    .workflow-section h2 {{ margin-top: 0; }}
    .workflow-info {{ margin: 0.5rem 0 1rem 0; }}
    .workflow-info p {{ margin: 0.25rem 0; }}
    .row {{ display: flex; gap: 1rem; flex-wrap: wrap; margin-top: 1rem; }}
    .card {{ border: 1px solid #eee; border-radius: 8px; padding: 1rem; background: white; }}
    .card h3 {{ margin-top: 0; font-size: 1rem; }}
    a {{ color: #0066cc; text-decoration: none; }}
    a:hover {{ text-decoration: underline; }}
  </style>
</head>
<body>
  <h1>Event Store Benchmark Suite</h1>
  {env_section}
  <h2>Workload Reports</h2>
  {workflow_sections}
</body>
</html>
"""
    with open(out_base / "index.html", "w") as f:
        f.write(html)


def extract_workflow_name(workload_name: str) -> str:
    """Extract workflow name from workload name (e.g., 'concurrent_writers_w4' -> 'concurrent_writers', 'concurrent_readers_r8' -> 'concurrent_readers')."""
    if "_r" in workload_name:
        return workload_name.rsplit("_r", 1)[0]
    if "_w" in workload_name:
        return workload_name.rsplit("_w", 1)[0]
    return workload_name


def main():
    parser = argparse.ArgumentParser(description="Generate ES-BENCH benchmark report from raw results")
    parser.add_argument("--raw", type=str, default="results/raw", help="Path to raw results dir")
    parser.add_argument("--out", type=str, default="results/published", help="Output reports dir")
    args = parser.parse_args()

    raw_dir = Path(args.raw)
    out_base = Path(args.out)
    out_base.mkdir(parents=True, exist_ok=True)

    runs = load_runs(raw_dir)
    if not runs:
        print(f"No runs found in {raw_dir}")
        return

    # Load environment info if available
    env_info = None
    env_check_file = raw_dir / "env_check.json"
    if env_check_file.exists():
        try:
            with open(env_check_file, "r") as f:
                env_info = json.load(f)
        except Exception as e:
            print(f"Warning: Could not load {env_check_file}: {e}")

    # Generate individual reports for each run
    for run in runs:
        samples_df = pd.DataFrame(run["samples"])
        run["_samples_df"] = samples_df
        adapter = run["summary"]["adapter"]
        workload = Path(run["summary"]["workload"]).stem
        workflow = extract_workflow_name(workload)
        writers = run["summary"].get("writers", 0)
        readers = run["summary"].get("readers", 0)

        # Create nested structure: workflow/report-adapter
        workload_dir = out_base / workflow
        workload_dir.mkdir(parents=True, exist_ok=True)

        # Format directory name based on workload type, zero-padded for sorting
        report_dir_name = f"report-{adapter}-r{readers:03d}-w{writers:03d}"
        report_dir = workload_dir / report_dir_name
        report_dir.mkdir(parents=True, exist_ok=True)

        plot_latency_cdf(samples_df, report_dir / "latency_cdf.png")
        plot_throughput(samples_df, report_dir / "throughput.png", report_dir / "throughput_data.json")
        generate_html(report_dir, run)
        print(f"Report written to {report_dir}/index.html")

    # Group runs by workflow
    workflow_groups = defaultdict(list)
    for run in runs:
        workload = Path(run["summary"]["workload"]).stem
        workflow = extract_workflow_name(workload)
        workflow_groups[workflow].append(run)

    # Generate per-workflow consolidated reports
    workflow_summaries = {}
    for workflow_name, workflow_runs in workflow_groups.items():
        print(f"\nProcessing workflow: {workflow_name}")

        # Group runs by worker count (writers or readers) for this workflow
        writer_groups = defaultdict(list)
        adapters_set = set()
        writer_counts_set = set()

        # Determine if this is a readers or writers workflow
        first_run = workflow_runs[0]["summary"] if workflow_runs else {}
        is_readers = first_run.get("readers", 0) > 0 and first_run.get("writers", 0) == 0
        worker_label = "reader" if is_readers else "writer"
        worker_suffix = "r" if is_readers else "w"

        for run in workflow_runs:
            writers = run["summary"].get("writers", 0)
            readers = run["summary"].get("readers", 0)
            wc = readers if is_readers else writers
            adapter = run["summary"]["adapter"]
            writer_groups[wc].append((adapter, run["_samples_df"]))
            adapters_set.add(adapter)
            writer_counts_set.add(wc)

        # Generate per-worker-count comparison charts for this workflow
        workflow_dir = out_base / workflow_name
        workflow_dir.mkdir(parents=True, exist_ok=True)

        for wc, run_data in sorted(writer_groups.items()):
            plot_comparison_latency_cdf(
                run_data,
                f"Latency CDF — {wc} {worker_label}(s)",
                workflow_dir / f"{workflow_name}_comparison_{worker_suffix}{wc}_latency_cdf.png",
            )
            plot_comparison_throughput(
                run_data,
                f"Throughput — {wc} {worker_label}(s)",
                workflow_dir / f"{workflow_name}_comparison_{worker_suffix}{wc}_throughput.png",
                workflow_dir / f"{workflow_name}_comparison_{worker_suffix}{wc}_throughput_data.json",
            )

        # Generate scaling summary charts for this workflow (if multiple writer counts)
        if len(writer_groups) > 1:
            plot_throughput_scaling(workflow_runs, workflow_dir / f"{workflow_name}_scaling_throughput.png")
            plot_p99_scaling(workflow_runs, workflow_dir / f"{workflow_name}_scaling_p99.png")

        # Generate container metrics visualization for this workflow
        plot_container_metrics(workflow_runs, workflow_dir / f"{workflow_name}_container_metrics.png")

        # Generate consolidated HTML for this workflow
        generate_workflow_html(out_base, workflow_name, workflow_runs, writer_groups)
        print(f"Workflow report written to {workflow_dir}/index.html")

        # Store summary for top-level index
        workflow_summaries[workflow_name] = {
            'run_count': len(workflow_runs),
            'adapters': adapters_set,
            'writer_counts': writer_counts_set,
        }

    # Generate top-level index
    generate_top_level_index(out_base, workflow_summaries, env_info)
    print(f"\nTop-level index written to {out_base}/index.html")


if __name__ == "__main__":
    main()
