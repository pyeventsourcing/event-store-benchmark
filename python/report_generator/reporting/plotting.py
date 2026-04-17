from collections import defaultdict

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.lines import Line2D
from matplotlib.ticker import LogLocator, NullFormatter, ScalarFormatter, FormatStrFormatter, FuncFormatter

from .style import get_adapter_color


def _format_tick(x, pos):
    """Format tick label to show decimal fractions only when necessary, avoiding scientific notation."""
    if x == 0:
        return "0"
    # Using trim='-' to remove unnecessary zeros and dot
    return np.format_float_positional(x, trim='-', precision=6, fractional=True)


def plot_latency_cdf(run, out_path: str):
    """Plot latency CDF from a single run object."""
    latencies_ms, percentiles = run.get_latency_cdf_data()
    if latencies_ms is None:
        return

    plt.figure(figsize=(6, 4))
    plt.plot(latencies_ms, percentiles, label="append latency CDF", linewidth=2)
    plt.xscale("log")
    
    # Ensure x-axis min value is half of the lowest plotted value (excluding zero)
    valid_latencies = [l for l in latencies_ms if l > 0]
    if valid_latencies:
        plt.xlim(left=min(valid_latencies) / 2)

    plt.xlabel("Latency (ms) [log]")
    plt.ylabel("Percentile (%)")
    plt.gca().xaxis.set_major_formatter(FuncFormatter(_format_tick))
    plt.gca().xaxis.set_minor_formatter(NullFormatter())
    plt.grid(True, which="both", ls=":", alpha=0.6)
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()


def plot_throughput_timeseries(run, out_path: str):
    """Plot throughput over time for a single run object."""
    timeseries = run.get_throughput_timeseries()
    if timeseries is None:
        return

    plt.figure(figsize=(6, 4))
    plt.plot(timeseries["time_s"], timeseries["throughput_eps"],
             linewidth=2.0, alpha=0.9, color='#1f77b4', marker=None,
             drawstyle='steps-pre')
    plt.xlabel("Elapsed Time (s)")
    plt.ylabel("Throughput (events/sec)")
    plt.title("Throughput over Time")
    plt.grid(True, ls=":", alpha=0.6)
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()


def plot_cpu_timeseries(run, out_path: str):
    """Plot CPU usage over time for a single run object."""
    ts = run.get_cpu_timeseries()
    if ts is None:
        return

    plt.figure(figsize=(6, 4))
    plt.plot(ts["time_s"], ts["cpu_percent"],
             linewidth=2.0, alpha=0.9, color='#1f77b4', marker=None,
             drawstyle='steps-pre')
    plt.xlabel("Elapsed Time (s)")
    plt.ylabel("CPU Usage (%)")
    plt.title("CPU Usage over Time")
    plt.ylim(bottom=0)
    plt.grid(True, ls=":", alpha=0.6)
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()


def plot_memory_timeseries(run, out_path: str):
    """Plot memory usage over time for a single run object."""
    ts = run.get_memory_timeseries()
    if ts is None:
        return

    plt.figure(figsize=(6, 4))
    plt.plot(ts["time_s"], ts["memory_mb"],
             linewidth=2.0, alpha=0.9, color='#1f77b4', marker=None,
             drawstyle='steps-pre')
    plt.xlabel("Elapsed Time (s)")
    plt.ylabel("Memory Usage (MB)")
    plt.title("Memory Usage over Time")
    plt.ylim(bottom=0)
    plt.grid(True, ls=":", alpha=0.6)
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()


def plot_comparison_latency_cdf(runs, title: str, out_path: str, get_store_rank=None):
    """Plot latency CDF comparing multiple runs."""
    plt.figure(figsize=(8, 5))

    sorted_runs = sorted(runs, key=lambda r: get_store_rank(r.adapter)) if get_store_rank else runs

    all_latencies = []
    for run in sorted_runs:
        latencies_ms, percentiles = run.get_latency_cdf_data()
        if latencies_ms is None:
            continue

        color = get_adapter_color(run.adapter)
        plt.plot(latencies_ms, percentiles, label=run.adapter, color=color, linewidth=2)
        all_latencies.extend([l for l in latencies_ms if l > 0])

    plt.xscale("log")
    
    # Ensure x-axis min value is half of the lowest plotted value (excluding zero)
    if all_latencies:
        plt.xlim(left=min(all_latencies) / 2)

    plt.xlabel("Latency (ms) [log]")
    plt.ylabel("Percentile (%)")
    plt.gca().xaxis.set_major_formatter(FuncFormatter(_format_tick))
    plt.gca().xaxis.set_minor_formatter(NullFormatter())
    plt.ticklabel_format(style='plain', axis='y')
    plt.title(title)
    plt.legend()
    plt.grid(True, which="both", ls=":", alpha=0.6)
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()


def plot_comparison_throughput(runs, title: str, out_path: str, get_store_rank=None):
    """Plot throughput over time comparing multiple runs."""
    plt.figure(figsize=(8, 5))

    sorted_runs = sorted(runs, key=lambda r: get_store_rank(r.adapter)) if get_store_rank else runs

    for run in sorted_runs:
        timeseries = run.get_throughput_timeseries()
        if timeseries is None:
            continue

        color = get_adapter_color(run.adapter)
        plt.plot(timeseries["time_s"], timeseries["throughput_eps"],
                 label=run.adapter, color=color, linewidth=2.0, alpha=0.9, marker=None,
                 drawstyle='steps-pre')

    plt.xlabel("Elapsed Time (s)")
    plt.ylabel("Throughput (events/sec)")
    plt.title(title)
    plt.legend()
    plt.grid(True, ls=":", alpha=0.6)
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()


def plot_comparison_cpu(runs, title: str, out_path: str, get_store_rank=None):
    """Plot CPU usage over time comparing multiple runs."""
    plt.figure(figsize=(8, 5))

    sorted_runs = sorted(runs, key=lambda r: get_store_rank(r.adapter)) if get_store_rank else runs

    for run in sorted_runs:
        ts = run.get_cpu_timeseries()
        if ts is None:
            continue

        color = get_adapter_color(run.adapter)
        plt.plot(ts["time_s"], ts["cpu_percent"],
                 label=run.adapter, color=color, linewidth=2.0, alpha=0.9, marker=None,
                 drawstyle='steps-pre')

    plt.xlabel("Elapsed Time (s)")
    plt.ylabel("CPU Usage (%)")
    plt.title(title)
    plt.ylim(bottom=0)
    plt.legend()
    plt.grid(True, ls=":", alpha=0.6)
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()


def plot_comparison_memory(runs, title: str, out_path: str, get_store_rank=None):
    """Plot memory usage over time comparing multiple runs."""
    plt.figure(figsize=(8, 5))

    sorted_runs = sorted(runs, key=lambda r: get_store_rank(r.adapter)) if get_store_rank else runs

    for run in sorted_runs:
        ts = run.get_memory_timeseries()
        if ts is None:
            continue

        color = get_adapter_color(run.adapter)
        plt.plot(ts["time_s"], ts["memory_mb"],
                 label=run.adapter, color=color, linewidth=2.0, alpha=0.9, marker=None,
                 drawstyle='steps-pre')

    plt.xlabel("Elapsed Time (s)")
    plt.ylabel("Memory Usage (MB)")
    plt.title(title)
    plt.ylim(bottom=0)
    plt.legend()
    plt.grid(True, ls=":", alpha=0.6)
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()


def plot_throughput_scaling(runs, out_path: str, get_store_rank=None):
    """Plot throughput vs worker count using grouped bar charts."""
    data = defaultdict(dict)
    all_adapters = set()
    all_worker_counts = set()

    for run in runs:
        if run.average_throughput > 0:
            data[run.worker_count][run.adapter] = run.average_throughput
            all_adapters.add(run.adapter)
            all_worker_counts.add(run.worker_count)

    if not data:
        return

    worker_counts = sorted(list(all_worker_counts))
    adapters = sorted(list(all_adapters), key=get_store_rank) if get_store_rank else sorted(list(all_adapters))

    first_run = runs[0]
    xlabel = "Readers" if first_run.is_read_workload else "Writers"
    title = f"Throughput by {xlabel[:-1]} Count"

    plt.figure(figsize=(10, 6))
    x = np.arange(len(worker_counts))
    width = 0.8 / len(adapters)

    all_vals = []
    for i, adapter in enumerate(adapters):
        vals = [data[wc].get(adapter, 0) for wc in worker_counts]
        offset = (i - (len(adapters) - 1) / 2) * width
        plt.bar(x + offset, vals, width, label=adapter, color=get_adapter_color(adapter), alpha=0.9)
        all_vals.extend([v for v in vals if v > 0])

    plt.yscale("log")
    
    # Ensure y-axis min value is half of the lowest plotted value (excluding zero)
    if all_vals:
        plt.ylim(bottom=min(all_vals) / 2)

    plt.ylabel("Throughput (events/sec) [log]")
    plt.xlabel(xlabel)
    plt.title(title)
    plt.xticks(x, [str(wc) for wc in worker_counts])

    plt.gca().yaxis.set_major_formatter(FuncFormatter(_format_tick))
    plt.gca().yaxis.set_major_locator(LogLocator(base=10.0, subs=(1.0, 2.0, 5.0)))
    plt.gca().yaxis.set_minor_formatter(NullFormatter())

    plt.legend()
    plt.grid(True, axis='y', ls=":", alpha=0.6)
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()


def plot_latency_scaling(runs, out_path: str, get_store_rank=None):
    """Plot p50, p99, and p99.9 latency vs worker count using grouped bar charts."""
    data = defaultdict(lambda: defaultdict(dict))
    all_adapters = set()
    all_worker_counts = set()

    for run in runs:
        p50 = run.get_latency_percentile(50.0)
        p99 = run.get_latency_percentile(99.0)
        p999 = run.get_latency_percentile(99.9)

        if p50 > 0 or p99 > 0 or p999 > 0:
            data[run.worker_count][run.adapter] = {"p50": p50, "p99": p99, "p999": p999}
            all_adapters.add(run.adapter)
            all_worker_counts.add(run.worker_count)

    if not data:
        return

    worker_counts = sorted(list(all_worker_counts))
    adapters = sorted(list(all_adapters), key=get_store_rank) if get_store_rank else sorted(list(all_adapters))

    first_run = runs[0]
    xlabel = "Readers" if first_run.is_read_workload else "Writers"
    title = f"Latency by {xlabel[:-1]} Count"

    plt.figure(figsize=(12, 7))
    x = np.arange(len(worker_counts))
    width = 0.8 / len(adapters)

    all_p50_vals = []
    for i, adapter in enumerate(adapters):
        p50_vals = np.array([data[wc].get(adapter, {}).get("p50", 0) for wc in worker_counts])
        p99_vals = np.array([data[wc].get(adapter, {}).get("p99", 0) for wc in worker_counts])
        p999_vals = np.array([data[wc].get(adapter, {}).get("p999", 0) for wc in worker_counts])

        offset = (i - (len(adapters) - 1) / 2) * width
        color = get_adapter_color(adapter)

        plt.bar(x + offset, p50_vals, width, color=color, alpha=1.0)
        plt.bar(x + offset, np.maximum(0, p99_vals - p50_vals), width, bottom=p50_vals, color=color, alpha=0.6)
        plt.bar(x + offset, np.maximum(0, p999_vals - p99_vals), width, bottom=p99_vals, color=color, alpha=0.3)
        all_p50_vals.extend([v for v in p50_vals if v > 0])

    plt.yscale("log")
    
    # Ensure y-axis min value is half of the lowest plotted value (excluding zero)
    if all_p50_vals:
        plt.ylim(bottom=min(all_p50_vals) / 2)

    plt.ylabel("Latency (ms) [log]")
    plt.xlabel(xlabel)
    plt.title(title)
    plt.xticks(x, [str(wc) for wc in worker_counts])

    plt.gca().yaxis.set_major_formatter(FuncFormatter(_format_tick))
    plt.gca().yaxis.set_major_locator(LogLocator(base=10.0, subs=(1.0, 2.0, 5.0)))
    plt.gca().yaxis.set_minor_formatter(NullFormatter())

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


def plot_cpu_scaling(runs, out_path: str, get_store_rank=None):
    """Plot average and peak CPU usage vs worker count using overlaid bar charts."""
    data = defaultdict(lambda: defaultdict(dict))
    all_adapters = set()
    all_worker_counts = set()

    for run in runs:
        avg_cpu = run.metrics.get("avg_cpu_percent")
        peak_cpu = run.metrics.get("peak_cpu_percent")
        if avg_cpu is not None or peak_cpu is not None:
            data[run.worker_count][run.adapter] = {"avg": avg_cpu or 0, "peak": peak_cpu or 0}
            all_adapters.add(run.adapter)
            all_worker_counts.add(run.worker_count)

    if not data:
        return

    worker_counts = sorted(list(all_worker_counts))
    adapters = sorted(list(all_adapters), key=get_store_rank) if get_store_rank else sorted(list(all_adapters))

    first_run = runs[0]
    xlabel = "Readers" if first_run.is_read_workload else "Writers"
    title = f"CPU Usage by {xlabel[:-1]} Count"

    plt.figure(figsize=(10, 6))
    x = np.arange(len(worker_counts))
    width = 0.8 / len(adapters)

    for i, adapter in enumerate(adapters):
        avg_vals = np.array([data[wc].get(adapter, {}).get("avg", 0) for wc in worker_counts])
        peak_vals = np.array([data[wc].get(adapter, {}).get("peak", 0) for wc in worker_counts])
        
        offset = (i - (len(adapters) - 1) / 2) * width
        color = get_adapter_color(adapter)
        
        plt.bar(x + offset, avg_vals, width, color=color, alpha=1.0)
        plt.bar(x + offset, np.maximum(0, peak_vals - avg_vals), width, bottom=avg_vals, color=color, alpha=0.5)

    plt.ylabel("CPU Usage (%)")
    plt.xlabel(xlabel)
    plt.title(title)
    plt.xticks(x, [str(wc) for wc in worker_counts])
    plt.ylim(bottom=0)

    adapter_handles = [Line2D([0], [0], color=get_adapter_color(a), lw=4, label=a) for a in adapters]
    metric_handles = [
        Line2D([0], [0], color='gray', alpha=1.0, lw=4, label='Average'),
        Line2D([0], [0], color='gray', alpha=0.5, lw=4, label='Peak')
    ]
    plt.legend(handles=adapter_handles + metric_handles, ncol=2)
    
    plt.grid(True, axis='y', ls=":", alpha=0.6)
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()


def plot_memory_scaling(runs, out_path: str, get_store_rank=None):
    """Plot average and peak memory usage vs worker count using overlaid bar charts."""
    data = defaultdict(lambda: defaultdict(dict))
    all_adapters = set()
    all_worker_counts = set()

    for run in runs:
        avg_mem = run.metrics.get("avg_memory_bytes")
        peak_mem = run.metrics.get("peak_memory_bytes")
        if avg_mem is not None or peak_mem is not None:
            data[run.worker_count][run.adapter] = {
                "avg": (avg_mem or 0) / (1024 * 1024),
                "peak": (peak_mem or 0) / (1024 * 1024)
            }
            all_adapters.add(run.adapter)
            all_worker_counts.add(run.worker_count)

    if not data:
        return

    worker_counts = sorted(list(all_worker_counts))
    adapters = sorted(list(all_adapters), key=get_store_rank) if get_store_rank else sorted(list(all_adapters))

    first_run = runs[0]
    xlabel = "Readers" if first_run.is_read_workload else "Writers"
    title = f"Memory Usage by {xlabel[:-1]} Count"

    plt.figure(figsize=(10, 6))
    x = np.arange(len(worker_counts))
    width = 0.8 / len(adapters)

    for i, adapter in enumerate(adapters):
        avg_vals = np.array([data[wc].get(adapter, {}).get("avg", 0) for wc in worker_counts])
        peak_vals = np.array([data[wc].get(adapter, {}).get("peak", 0) for wc in worker_counts])
        
        offset = (i - (len(adapters) - 1) / 2) * width
        color = get_adapter_color(adapter)
        
        plt.bar(x + offset, avg_vals, width, color=color, alpha=1.0)
        plt.bar(x + offset, np.maximum(0, peak_vals - avg_vals), width, bottom=avg_vals, color=color, alpha=0.5)

    plt.ylabel("Memory Usage (MB)")
    plt.xlabel(xlabel)
    plt.title(title)
    plt.xticks(x, [str(wc) for wc in worker_counts])
    plt.ylim(bottom=0)

    adapter_handles = [Line2D([0], [0], color=get_adapter_color(a), lw=4, label=a) for a in adapters]
    metric_handles = [
        Line2D([0], [0], color='gray', alpha=1.0, lw=4, label='Average'),
        Line2D([0], [0], color='gray', alpha=0.5, lw=4, label='Peak')
    ]
    plt.legend(handles=adapter_handles + metric_handles, ncol=2)

    plt.grid(True, axis='y', ls=":", alpha=0.6)
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()


def plot_process_metrics(runs, out_path: str, get_store_rank=None):
    """Create a visualization of resource usage (CPU/Memory)."""
    adapter_data = {}

    for run in runs:
        metrics = run.metrics
        has_cpu = metrics.get("peak_cpu_percent") is not None
        has_mem = metrics.get("peak_memory_bytes") is not None

        if not (has_cpu or has_mem):
            continue

        if run.adapter not in adapter_data:
            adapter_data[run.adapter] = {
                "peak_cpu": 0,
                "peak_mem": 0,
                "count": 0
            }

        data = adapter_data[run.adapter]
        data["peak_cpu"] = max(data["peak_cpu"], metrics.get("peak_cpu_percent", 0))
        
        peak_mem = metrics.get("peak_memory_bytes")
        if peak_mem is not None:
            data["peak_mem"] = max(data["peak_mem"], peak_mem / (1024 * 1024))
            
        data["count"] += 1

    if not adapter_data:
        return

    adapters_list = list(adapter_data.keys())
    if get_store_rank:
        adapters = sorted(adapters_list, key=get_store_rank)
    else:
        def normalize(values):
            max_val = max(values) if values else 1
            return [v / max_val if max_val > 0 else 0 for v in values]

        raw_cpu = [adapter_data[a]["peak_cpu"] for a in adapters_list]
        raw_mem = [adapter_data[a]["peak_mem"] for a in adapters_list]

        norm_cpu = normalize(raw_cpu)
        norm_mem = normalize(raw_mem)

        composite_scores = []
        for i, adapter in enumerate(adapters_list):
            score = (norm_cpu[i] + norm_mem[i]) / 2.0
            composite_scores.append((adapter, score))

        composite_scores.sort(key=lambda x: x[1])
        adapters = [x[0] for x in composite_scores]

    peak_cpus = [adapter_data[a]["peak_cpu"] for a in adapters]
    peak_mems = [adapter_data[a]["peak_mem"] for a in adapters]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
    fig.suptitle("Process Resource Usage Comparison", fontsize=16, fontweight='bold')

    colors = [get_adapter_color(adapter) for adapter in adapters]

    def plot_bar(ax, data, title, ylabel, fmt_str):
        bars = ax.bar(adapters, data, color=colors, edgecolor='black', linewidth=1.5)
        ax.set_ylabel(ylabel, fontweight='bold')
        ax.set_title(title, fontweight='bold')
        ax.grid(True, alpha=0.3, axis='y')
        for bar, v in zip(bars, data):
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width() / 2., height,
                    fmt_str.format(v) if v > 0 else "N/A", ha='center', va='bottom', fontweight='bold')

    plot_bar(ax1, peak_cpus, "Peak CPU Usage", "Peak CPU (%)", '{:.1f}%')
    plot_bar(ax2, peak_mems, "Peak Memory Usage", "Peak Memory (MB)", '{:.0f}')

    plt.tight_layout()
    plt.savefig(out_path, dpi=150, bbox_inches='tight')
    plt.close()


def plot_container_stats(runs, out_path: str, get_store_rank=None):
    """Create a visualization of container stats (Image size/Startup)."""
    adapter_data = {}

    for run in runs:
        metrics = run.metrics
        has_image = metrics.get("image_size_bytes") is not None
        has_startup = metrics.get("startup_time_s") is not None

        if not (has_image or has_startup):
            continue

        if run.adapter not in adapter_data:
            adapter_data[run.adapter] = {
                "image_size": [],
                "startup_time": 0,
                "startup_count": 0
            }

        data = adapter_data[run.adapter]
        
        img_size = metrics.get("image_size_bytes")
        if img_size is not None:
            data["image_size"].append(img_size / (1024 * 1024))
            
        startup = metrics.get("startup_time_s")
        if startup is not None and startup > 0:
            data["startup_time"] += startup
            data["startup_count"] += 1

    if not adapter_data:
        return

    adapters_list = list(adapter_data.keys())
    if get_store_rank:
        adapters = sorted(adapters_list, key=get_store_rank)
    else:
        adapters = sorted(adapters_list)

    image_sizes = [np.mean(adapter_data[a]["image_size"]) if adapter_data[a]["image_size"] else 0 for a in adapters]
    startup_times = [adapter_data[a]["startup_time"] / adapter_data[a]["startup_count"] if adapter_data[a]["startup_count"] > 0 else 0 for a in adapters]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
    fig.suptitle("Container Stats Comparison", fontsize=16, fontweight='bold')

    colors = [get_adapter_color(adapter) for adapter in adapters]

    def plot_bar(ax, data, title, ylabel, fmt_str, show_if_zero=True):
        if not show_if_zero and all(v == 0 for v in data):
            ax.text(0.5, 0.5, "N/A", ha='center', va='center', fontsize=14, transform=ax.transAxes)
            ax.set_title(title, fontweight='bold')
            ax.set_axis_off()
            return

        bars = ax.bar(adapters, data, color=colors, edgecolor='black', linewidth=1.5)
        ax.set_ylabel(ylabel, fontweight='bold')
        ax.set_title(title, fontweight='bold')
        ax.grid(True, alpha=0.3, axis='y')
        for bar, v in zip(bars, data):
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width() / 2., height,
                    fmt_str.format(v) if v > 0 else "N/A", ha='center', va='bottom', fontweight='bold')

    plot_bar(ax1, image_sizes, "Image Size", "Image Size (MB)", '{:.0f}', show_if_zero=False)
    plot_bar(ax2, startup_times, "Startup Time", "Startup Time (seconds)", '{:.2f}s', show_if_zero=False)

    plt.tight_layout()
    plt.savefig(out_path, dpi=150, bbox_inches='tight')
    plt.close()