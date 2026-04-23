from collections import defaultdict
from typing import Any, Callable, Dict, List, Optional, Sequence

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.lines import Line2D
from matplotlib.ticker import LogLocator, NullFormatter, FuncFormatter

from .style import (
    get_adapter_color, PLOT_WIDTH, PLOT_HEIGHT, PLOT_DPI,
    FONT_SIZE_TITLE, FONT_SIZE_LABEL, FONT_SIZE_TICK, FONT_SIZE_LEGEND
)
from ..workloads.performance import PerformanceWorkloadResult

# Apply global matplotlib styles for consistency
plt.rcParams.update({
    'figure.figsize': (PLOT_WIDTH, PLOT_HEIGHT),
    'figure.dpi': PLOT_DPI,
    'axes.titlesize': FONT_SIZE_TITLE,
    'axes.labelsize': FONT_SIZE_LABEL,
    'xtick.labelsize': FONT_SIZE_TICK,
    'ytick.labelsize': FONT_SIZE_TICK,
    'legend.fontsize': FONT_SIZE_LEGEND,
    'figure.titlesize': FONT_SIZE_TITLE + 2,
})


def _format_tick(x: float, pos: Any) -> str:
    """Format tick label to show decimal fractions only when necessary, avoiding scientific notation."""
    if x == 0:
        return "0"
    # Using trim='-' to remove unnecessary zeros and dot
    return np.format_float_positional(x, trim='-', precision=6, fractional=True)


def _set_y_limit_with_margin(ax: Any, values: Any, margin: float = 0.05) -> None:
    """Set the y-axis limit to a few percent greater than the max plotted value."""
    if values is None or len(values) == 0:
        return
    max_val = np.max(values)
    if max_val > 0:
        ax.set_ylim(bottom=0, top=max_val * (1 + margin))
    else:
        ax.set_ylim(bottom=0, top=1)


def plot_latency_cdf(run: Any, out_path: str) -> None:
    """Plot latency CDF from a single run object."""
    latencies_ms, percentiles = run.get_latency_cdf_data()
    if latencies_ms is None:
        return

    plt.figure()
    plt.plot(latencies_ms, percentiles, label="append latency CDF", linewidth=2)
    plt.xscale("log")
    
    # Ensure x-axis min value is half of the lowest plotted value (excluding zero)
    valid_latencies = [l for l in latencies_ms if l > 0]
    if valid_latencies:
        plt.xlim(left=min(valid_latencies) / 2)

    plt.xlabel("Latency (ms) [log]")
    plt.ylabel("Percentile (%)")
    plt.title("Latency CDF")
    plt.gca().xaxis.set_major_formatter(FuncFormatter(_format_tick))
    plt.gca().xaxis.set_minor_formatter(NullFormatter())
    plt.grid(True, which="both", ls=":", alpha=0.6)
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()


def plot_tool_latency_cdf(run: Any, out_path: str) -> None:
    """Plot benchmark process latency CDF from a single run object."""
    latencies_ms, percentiles = run.get_tool_latency_cdf_data()
    if latencies_ms is None:
        return

    plt.figure()
    plt.plot(latencies_ms, percentiles, label="benchmark latency CDF", linewidth=2, color='#ff7f0e')
    plt.xscale("log")
    
    valid_latencies = [l for l in latencies_ms if l > 0]
    if valid_latencies:
        plt.xlim(left=min(valid_latencies) / 2)

    plt.xlabel("Latency (ms) [log]")
    plt.ylabel("Percentile (%)")
    plt.title("Tool Process Latency CDF")
    plt.gca().xaxis.set_major_formatter(FuncFormatter(_format_tick))
    plt.gca().xaxis.set_minor_formatter(NullFormatter())
    plt.grid(True, which="both", ls=":", alpha=0.6)
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()


def plot_throughput_timeseries(run: Any, out_path: str) -> None:
    """Plot throughput over time for a single run object."""
    timeseries = run.get_throughput_timeseries()
    if timeseries is None:
        return

    plt.figure()
    plt.plot(timeseries["time_s"], timeseries["throughput_eps"],
             linewidth=2.0, alpha=0.9, color='#1f77b4', marker=None,
             drawstyle='steps-pre')
    plt.xlabel("Elapsed Time (s)")
    plt.ylabel("Throughput (events/sec)")
    plt.title("Throughput over Time")
    plt.grid(True, ls=":", alpha=0.6)
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()


def plot_cpu_timeseries(run: Any, out_path: str) -> None:
    """Plot CPU usage over time for a single run object."""
    ts = run.get_cpu_timeseries()
    if ts is None:
        return

    plt.figure()
    plt.plot(ts["time_s"], ts["cpu_percent"],
             linewidth=2.0, alpha=0.9, color='#1f77b4', marker=None,
             drawstyle='steps-pre')
    plt.xlabel("Elapsed Time (s)")
    plt.ylabel("CPU Usage (%)")
    plt.title("CPU Usage over Time")
    _set_y_limit_with_margin(plt.gca(), ts["cpu_percent"])
    plt.grid(True, ls=":", alpha=0.6)
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()


def plot_tool_cpu_timeseries(run: Any, out_path: str) -> None:
    """Plot benchmark process CPU usage over time for a single run object."""
    ts = run.get_tool_cpu_timeseries()
    if ts is None:
        return

    plt.figure()
    plt.plot(ts["time_s"], ts["cpu_percent"],
             linewidth=2.0, alpha=0.9, color='#ff7f0e', marker=None,
             drawstyle='steps-pre')
    plt.xlabel("Elapsed Time (s)")
    plt.ylabel("CPU Usage (%)")
    plt.title("Tool CPU Usage over Time")
    _set_y_limit_with_margin(plt.gca(), ts["cpu_percent"])
    plt.grid(True, ls=":", alpha=0.6)
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()


def plot_memory_timeseries(run: Any, out_path: str) -> None:
    """Plot memory usage over time for a single run object."""
    ts = run.get_memory_timeseries()
    if ts is None:
        return

    plt.figure()
    plt.plot(ts["time_s"], ts["memory_mb"],
             linewidth=2.0, alpha=0.9, color='#1f77b4', marker=None,
             drawstyle='steps-pre')
    plt.xlabel("Elapsed Time (s)")
    plt.ylabel("Memory Usage (MB)")
    plt.title("Memory Usage over Time")
    _set_y_limit_with_margin(plt.gca(), ts["memory_mb"])
    plt.grid(True, ls=":", alpha=0.6)
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()


def plot_tool_memory_timeseries(run: Any, out_path: str) -> None:
    """Plot benchmark process memory usage over time for a single run object."""
    ts = run.get_tool_memory_timeseries()
    if ts is None:
        return

    plt.figure()
    plt.plot(ts["time_s"], ts["memory_mb"],
             linewidth=2.0, alpha=0.9, color='#ff7f0e', marker=None,
             drawstyle='steps-pre')
    plt.xlabel("Elapsed Time (s)")
    plt.ylabel("Memory Usage (MB)")
    plt.title("Tool Memory Usage over Time")
    _set_y_limit_with_margin(plt.gca(), ts["memory_mb"])
    plt.grid(True, ls=":", alpha=0.6)
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()


def plot_worker_slice_latency_cdf(runs: List[Any], title: str, out_path: str, get_store_rank: Optional[Callable[[str], int]] = None) -> None:
    """Plot latency CDF comparing multiple runs."""
    plt.figure()

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
    plt.savefig(out_path)
    plt.close()


def plot_worker_slice_tool_latency_cdf(runs: List[Any], title: str, out_path: str, get_store_rank: Optional[Callable[[str], int]] = None) -> None:
    """Plot benchmark latency CDF comparing multiple runs."""
    plt.figure()

    sorted_runs = sorted(runs, key=lambda r: get_store_rank(r.adapter)) if get_store_rank else runs

    all_latencies = []
    for run in sorted_runs:
        latencies_ms, percentiles = run.get_tool_latency_cdf_data()
        if latencies_ms is None:
            continue

        color = get_adapter_color(run.adapter)
        plt.plot(latencies_ms, percentiles, label=run.adapter, color=color, linewidth=2)
        all_latencies.extend([l for l in latencies_ms if l > 0])

    plt.xscale("log")
    
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
    plt.savefig(out_path)
    plt.close()


def plot_worker_slice_throughput(runs: List[Any], title: str, out_path: str, get_store_rank: Optional[Callable[[str], int]] = None) -> None:
    """Plot throughput over time comparing multiple runs."""
    plt.figure()

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
    plt.savefig(out_path)
    plt.close()


def plot_worker_slice_cpu(runs: List[Any], title: str, out_path: str, get_store_rank: Optional[Callable[[str], int]] = None) -> None:
    """Plot CPU usage over time comparing multiple runs."""
    plt.figure()

    sorted_runs = sorted(runs, key=lambda r: get_store_rank(r.adapter)) if get_store_rank else runs

    all_cpu = []
    for run in sorted_runs:
        ts = run.get_cpu_timeseries()
        if ts is None:
            continue

        color = get_adapter_color(run.adapter)
        plt.plot(ts["time_s"], ts["cpu_percent"],
                 label=run.adapter, color=color, linewidth=2.0, alpha=0.9, marker=None,
                 drawstyle='steps-pre')
        all_cpu.extend(ts["cpu_percent"])

    plt.xlabel("Elapsed Time (s)")
    plt.ylabel("CPU Usage (%)")
    plt.title(title)
    _set_y_limit_with_margin(plt.gca(), all_cpu)
    plt.legend()
    plt.grid(True, ls=":", alpha=0.6)
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()


def plot_worker_slice_tool_cpu(runs: List[Any], title: str, out_path: str, get_store_rank: Optional[Callable[[str], int]] = None) -> None:
    """Plot benchmark CPU usage over time comparing multiple runs."""
    plt.figure()

    sorted_runs = sorted(runs, key=lambda r: get_store_rank(r.adapter)) if get_store_rank else runs

    all_cpu = []
    for run in sorted_runs:
        ts = run.get_tool_cpu_timeseries()
        if ts is None:
            continue

        color = get_adapter_color(run.adapter)
        plt.plot(ts["time_s"], ts["cpu_percent"],
                 label=run.adapter, color=color, linewidth=2.0, alpha=0.9, marker=None,
                 drawstyle='steps-pre')
        all_cpu.extend(ts["cpu_percent"])

    plt.xlabel("Elapsed Time (s)")
    plt.ylabel("CPU Usage (%)")
    plt.title(title)
    _set_y_limit_with_margin(plt.gca(), all_cpu)
    plt.legend()
    plt.grid(True, ls=":", alpha=0.6)
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()


def plot_worker_slice_memory(runs: List[Any], title: str, out_path: str, get_store_rank: Optional[Callable[[str], int]] = None) -> None:
    """Plot memory usage over time comparing multiple runs."""
    plt.figure()

    sorted_runs = sorted(runs, key=lambda r: get_store_rank(r.adapter)) if get_store_rank else runs

    all_mem = []
    for run in sorted_runs:
        ts = run.get_memory_timeseries()
        if ts is None:
            continue

        color = get_adapter_color(run.adapter)
        plt.plot(ts["time_s"], ts["memory_mb"],
                 label=run.adapter, color=color, linewidth=2.0, alpha=0.9, marker=None,
                 drawstyle='steps-pre')
        all_mem.extend(ts["memory_mb"])

    plt.xlabel("Elapsed Time (s)")
    plt.ylabel("Memory Usage (MB)")
    plt.title(title)
    _set_y_limit_with_margin(plt.gca(), all_mem)
    plt.legend()
    plt.grid(True, ls=":", alpha=0.6)
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()


def plot_worker_slice_tool_memory(runs: List[Any], title: str, out_path: str, get_store_rank: Optional[Callable[[str], int]] = None) -> None:
    """Plot benchmark memory usage over time comparing multiple runs."""
    plt.figure()

    sorted_runs = sorted(runs, key=lambda r: get_store_rank(r.adapter)) if get_store_rank else runs

    all_mem = []
    for run in sorted_runs:
        ts = run.get_tool_memory_timeseries()
        if ts is None:
            continue

        color = get_adapter_color(run.adapter)
        plt.plot(ts["time_s"], ts["memory_mb"],
                 label=run.adapter, color=color, linewidth=2.0, alpha=0.9, marker=None,
                 drawstyle='steps-pre')
        all_mem.extend(ts["memory_mb"])

    plt.xlabel("Elapsed Time (s)")
    plt.ylabel("Memory Usage (MB)")
    plt.title(title)
    _set_y_limit_with_margin(plt.gca(), all_mem)
    plt.legend()
    plt.grid(True, ls=":", alpha=0.6)
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()


def plot_throughput_by_workers(runs: List[Any], out_path: str, get_store_rank: Optional[Callable[[str], int]] = None) -> None:
    """Plot average and peak throughput vs worker count using overlaid bar charts."""
    data: Dict[int, Dict[str, Dict[str, float]]] = defaultdict(lambda: defaultdict(dict))
    all_adapters = set()
    all_worker_counts = set()

    for run in runs:
        if run.average_throughput > 0 or (hasattr(run, 'peak_throughput') and run.peak_throughput > 0):
            data[run.worker_count][run.adapter] = {
                "avg": run.average_throughput,
                "peak": getattr(run, 'peak_throughput', 0)
            }
            all_adapters.add(run.adapter)
            all_worker_counts.add(run.worker_count)

    if not data:
        return

    worker_counts = sorted(list(all_worker_counts))
    adapters = sorted(list(all_adapters), key=get_store_rank) if get_store_rank else sorted(list(all_adapters))

    first_run = runs[0] if runs else None
    xlabel = ("Readers" if first_run.is_read_workload else "Writers") if first_run else "Workers"
    title = f"Throughput by {xlabel[:-1]} Count"

    plt.figure()
    x = np.arange(len(worker_counts))
    width = 0.8 / len(adapters)

    all_vals = []
    for i, adapter in enumerate(adapters):
        avg_vals = np.array([data[wc].get(adapter, {}).get("avg", 0) for wc in worker_counts])
        peak_vals = np.array([data[wc].get(adapter, {}).get("peak", 0) for wc in worker_counts])
        
        offset = (i - (len(adapters) - 1) / 2) * width
        color = get_adapter_color(adapter)
        
        plt.bar(x + offset, avg_vals, width, color=color, alpha=1.0)
        plt.bar(x + offset, np.maximum(0, peak_vals - avg_vals), width, bottom=avg_vals, color=color, alpha=0.5)
        
        all_vals.extend([v for v in avg_vals if v > 0])
        all_vals.extend([v for v in peak_vals if v > 0])

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

    adapter_handles = [Line2D([0], [0], color=get_adapter_color(a), lw=4, label=a) for a in adapters]
    metric_handles = [
        Line2D([0], [0], color='gray', alpha=1.0, lw=4, label='Average'),
        Line2D([0], [0], color='gray', alpha=0.5, lw=4, label='Peak')
    ]
    plt.legend(handles=adapter_handles + metric_handles, ncol=2)

    plt.grid(True, axis='y', ls=":", alpha=0.6)
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()


def plot_latency_by_workers(runs: List[Any], out_path: str, get_store_rank: Optional[Callable[[str], int]] = None) -> None:
    """Plot p50, p99, and p99.9 latency vs worker count using grouped bar charts."""
    data: Dict[int, Dict[str, Dict[str, float]]] = defaultdict(lambda: defaultdict(dict))
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

    first_run = runs[0] if runs else None
    xlabel = ("Readers" if first_run.is_read_workload else "Writers") if first_run else "Workers"
    title = f"Latency by {xlabel[:-1]} Count"

    plt.figure()
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
    plt.savefig(out_path)
    plt.close()


def plot_tool_latency_by_workers(runs: List[PerformanceWorkloadResult], out_path: str, get_store_rank: Optional[Callable[[str], int]] = None) -> None:
    """Plot benchmark p50, p99, and p99.9 latency vs worker count using grouped bar charts."""
    data: Dict[int, Dict[str, Dict[str, float]]] = defaultdict(lambda: defaultdict(dict))
    all_adapters = set()
    all_worker_counts = set()

    for run in runs:
        p50 = run.get_tool_latency_percentile(50.0)
        p99 = run.get_tool_latency_percentile(99.0)
        p999 = run.get_tool_latency_percentile(99.9)
        
        if p50 > 0 or p99 > 0 or p999 > 0:
            data[run.worker_count][run.adapter] = {"p50": p50, "p99": p99, "p999": p999}
            all_adapters.add(run.adapter)
            all_worker_counts.add(run.worker_count)

    if not data:
        return

    worker_counts = sorted(list(all_worker_counts))
    adapters = sorted(list(all_adapters), key=get_store_rank) if get_store_rank else sorted(list(all_adapters))

    first_run = runs[0] if runs else None
    xlabel = ("Readers" if first_run.is_read_workload else "Writers") if first_run else "Workers"
    title = f"Tool Latency by {xlabel[:-1]} Count"

    plt.figure()
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
    metric_handles = [
        Line2D([0], [0], color='gray', alpha=1.0, lw=4, label='p50'),
        Line2D([0], [0], color='gray', alpha=0.6, lw=4, label='p99'),
        Line2D([0], [0], color='gray', alpha=0.3, lw=4, label='p99.9')
    ]
    plt.legend(handles=adapter_handles + metric_handles, ncol=len(adapters) if len(adapters) < 4 else 4)

    plt.grid(True, axis='y', ls=":", alpha=0.6)
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()


def plot_cpu_by_workers(runs: List[Any], out_path: str, get_store_rank: Optional[Callable[[str], int]] = None) -> None:
    """Plot average and peak CPU usage vs worker count using overlaid bar charts."""
    data: Dict[int, Dict[str, Dict[str, float]]] = defaultdict(lambda: defaultdict(dict))
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

    first_run = runs[0] if runs else None
    xlabel = ("Readers" if first_run.is_read_workload else "Writers") if first_run else "Workers"
    title = f"CPU Usage by {xlabel[:-1]} Count"

    plt.figure()
    x = np.arange(len(worker_counts))
    width = 0.8 / len(adapters)

    all_cpu: List[float] = []
    for i, adapter in enumerate(adapters):
        avg_vals = np.array([data[wc].get(adapter, {}).get("avg", 0) for wc in worker_counts])
        peak_vals = np.array([data[wc].get(adapter, {}).get("peak", 0) for wc in worker_counts])
        
        offset = (i - (len(adapters) - 1) / 2) * width
        color = get_adapter_color(adapter)
        
        plt.bar(x + offset, avg_vals, width, color=color, alpha=1.0)
        plt.bar(x + offset, np.maximum(0, peak_vals - avg_vals), width, bottom=avg_vals, color=color, alpha=0.5)
        
        all_cpu.extend(avg_vals)
        all_cpu.extend(peak_vals)

    plt.ylabel("CPU Usage (%)")
    plt.xlabel(xlabel)
    plt.title(title)
    plt.xticks(x, [str(wc) for wc in worker_counts])
    _set_y_limit_with_margin(plt.gca(), all_cpu)

    adapter_handles = [Line2D([0], [0], color=get_adapter_color(a), lw=4, label=a) for a in adapters]
    metric_handles = [
        Line2D([0], [0], color='gray', alpha=1.0, lw=4, label='Average'),
        Line2D([0], [0], color='gray', alpha=0.5, lw=4, label='Peak')
    ]
    plt.legend(handles=adapter_handles + metric_handles, ncol=2)
    
    plt.grid(True, axis='y', ls=":", alpha=0.6)
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()


def plot_tool_cpu_by_workers(runs: List[Any], out_path: str, get_store_rank: Optional[Callable[[str], int]] = None) -> None:
    """Plot average and peak benchmark CPU usage vs worker count using overlaid bar charts."""
    data: Dict[int, Dict[str, Dict[str, float]]] = defaultdict(lambda: defaultdict(dict))
    all_adapters = set()
    all_worker_counts = set()

    for run in runs:
        avg_cpu = run.metrics.get("tool_avg_cpu_percent", 0)
        peak_cpu = run.metrics.get("tool_peak_cpu_percent", 0)

        if avg_cpu > 0 or peak_cpu > 0:
            data[run.worker_count][run.adapter] = {"avg": avg_cpu, "peak": peak_cpu}
            all_adapters.add(run.adapter)
            all_worker_counts.add(run.worker_count)

    if not data:
        return

    worker_counts = sorted(list(all_worker_counts))
    adapters = sorted(list(all_adapters), key=get_store_rank) if get_store_rank else sorted(list(all_adapters))

    first_run = runs[0] if runs else None
    xlabel = ("Readers" if first_run.is_read_workload else "Writers") if first_run else "Workers"
    title = f"Tool CPU Usage by {xlabel[:-1]} Count"

    plt.figure()
    x = np.arange(len(worker_counts))
    width = 0.8 / len(adapters)

    all_cpu: List[float] = []
    for i, adapter in enumerate(adapters):
        avg_vals = np.array([data[wc].get(adapter, {}).get("avg", 0) for wc in worker_counts])
        peak_vals = np.array([data[wc].get(adapter, {}).get("peak", 0) for wc in worker_counts])
        
        offset = (i - (len(adapters) - 1) / 2) * width
        color = get_adapter_color(adapter)
        
        plt.bar(x + offset, avg_vals, width, color=color, alpha=1.0)
        plt.bar(x + offset, np.maximum(0, peak_vals - avg_vals), width, bottom=avg_vals, color=color, alpha=0.5)
        
        all_cpu.extend(avg_vals)
        all_cpu.extend(peak_vals)

    plt.ylabel("CPU Usage (%)")
    plt.xlabel(xlabel)
    plt.title(title)
    plt.xticks(x, [str(wc) for wc in worker_counts])
    _set_y_limit_with_margin(plt.gca(), all_cpu)

    adapter_handles = [Line2D([0], [0], color=get_adapter_color(a), lw=4, label=a) for a in adapters]
    metric_handles = [
        Line2D([0], [0], color='gray', alpha=1.0, lw=4, label='Average'),
        Line2D([0], [0], color='gray', alpha=0.5, lw=4, label='Peak')
    ]
    plt.legend(handles=adapter_handles + metric_handles, ncol=2)

    plt.grid(True, axis='y', ls=":", alpha=0.6)
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()


def plot_memory_by_workers(runs: List[Any], out_path: str, get_store_rank: Optional[Callable[[str], int]] = None) -> None:
    """Plot average and peak memory usage vs worker count using overlaid bar charts."""
    data: Dict[int, Dict[str, Dict[str, float]]] = defaultdict(lambda: defaultdict(dict))
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

    first_run = runs[0] if runs else None
    xlabel = ("Readers" if first_run.is_read_workload else "Writers") if first_run else "Workers"
    title = f"Memory Usage by {xlabel[:-1]} Count"

    plt.figure()
    x = np.arange(len(worker_counts))
    width = 0.8 / len(adapters)

    all_mem: List[float] = []
    for i, adapter in enumerate(adapters):
        avg_vals = np.array([data[wc].get(adapter, {}).get("avg", 0) for wc in worker_counts])
        peak_vals = np.array([data[wc].get(adapter, {}).get("peak", 0) for wc in worker_counts])
        
        offset = (i - (len(adapters) - 1) / 2) * width
        color = get_adapter_color(adapter)
        
        plt.bar(x + offset, avg_vals, width, color=color, alpha=1.0)
        plt.bar(x + offset, np.maximum(0, peak_vals - avg_vals), width, bottom=avg_vals, color=color, alpha=0.5)
        
        all_mem.extend(avg_vals)
        all_mem.extend(peak_vals)

    plt.ylabel("Memory Usage (MB)")
    plt.xlabel(xlabel)
    plt.title(title)
    plt.xticks(x, [str(wc) for wc in worker_counts])
    _set_y_limit_with_margin(plt.gca(), all_mem)

    adapter_handles = [Line2D([0], [0], color=get_adapter_color(a), lw=4, label=a) for a in adapters]
    metric_handles = [
        Line2D([0], [0], color='gray', alpha=1.0, lw=4, label='Average'),
        Line2D([0], [0], color='gray', alpha=0.5, lw=4, label='Peak')
    ]
    plt.legend(handles=adapter_handles + metric_handles, ncol=2)

    plt.grid(True, axis='y', ls=":", alpha=0.6)
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()


def plot_tool_memory_by_workers(runs: List[Any], out_path: str, get_store_rank: Optional[Callable[[str], int]] = None) -> None:
    """Plot average and peak benchmark memory usage vs worker count using overlaid bar charts."""
    data: Dict[int, Dict[str, Dict[str, float]]] = defaultdict(lambda: defaultdict(dict))
    all_adapters = set()
    all_worker_counts = set()

    for run in runs:
        avg_mem = run.metrics.get("tool_avg_memory_bytes", 0) / 1024 / 1024
        peak_mem = run.metrics.get("tool_peak_memory_bytes", 0) / 1024 / 1024

        if avg_mem > 0 or peak_mem > 0:
            data[run.worker_count][run.adapter] = {"avg": avg_mem, "peak": peak_mem}
            all_adapters.add(run.adapter)
            all_worker_counts.add(run.worker_count)

    if not data:
        return

    worker_counts = sorted(list(all_worker_counts))
    adapters = sorted(list(all_adapters), key=get_store_rank) if get_store_rank else sorted(list(all_adapters))

    first_run = runs[0] if runs else None
    xlabel = ("Readers" if first_run.is_read_workload else "Writers") if first_run else "Workers"
    title = f"Tool Memory Usage by {xlabel[:-1]} Count"

    plt.figure()
    x = np.arange(len(worker_counts))
    width = 0.8 / len(adapters)

    all_mem: List[float] = []
    for i, adapter in enumerate(adapters):
        avg_vals = np.array([data[wc].get(adapter, {}).get("avg", 0) for wc in worker_counts])
        peak_vals = np.array([data[wc].get(adapter, {}).get("peak", 0) for wc in worker_counts])
        
        offset = (i - (len(adapters) - 1) / 2) * width
        color = get_adapter_color(adapter)
        
        plt.bar(x + offset, avg_vals, width, color=color, alpha=1.0)
        plt.bar(x + offset, np.maximum(0, peak_vals - avg_vals), width, bottom=avg_vals, color=color, alpha=0.5)
        
        all_mem.extend(avg_vals)
        all_mem.extend(peak_vals)

    plt.ylabel("Memory Usage (MB)")
    plt.xlabel(xlabel)
    plt.title(title)
    plt.xticks(x, [str(wc) for wc in worker_counts])
    _set_y_limit_with_margin(plt.gca(), all_mem)

    adapter_handles = [Line2D([0], [0], color=get_adapter_color(a), lw=4, label=a) for a in adapters]
    metric_handles = [
        Line2D([0], [0], color='gray', alpha=1.0, lw=4, label='Average'),
        Line2D([0], [0], color='gray', alpha=0.5, lw=4, label='Peak')
    ]
    plt.legend(handles=adapter_handles + metric_handles, ncol=2)

    plt.grid(True, axis='y', ls=":", alpha=0.6)
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()


def plot_process_metrics(runs: List[Any], out_path: str, get_store_rank: Optional[Callable[[str], int]] = None) -> None:
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
        def normalize(values: Sequence[float]) -> List[float]:
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

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(PLOT_WIDTH * 2, PLOT_HEIGHT))
    fig.suptitle("Process Resource Usage Comparison", fontweight='bold')

    colors = [get_adapter_color(adapter) for adapter in adapters]

    def plot_bar(ax: Any, data: Sequence[float], title: str, ylabel: str, fmt_str: str) -> None:
        bars = ax.bar(adapters, data, color=colors, edgecolor='black', linewidth=1.5)
        ax.set_ylabel(ylabel, fontweight='bold')
        ax.set_title(title, fontweight='bold')
        ax.grid(True, alpha=0.3, axis='y')
        _set_y_limit_with_margin(ax, data, margin=0.1)
        for bar, v in zip(bars, data):
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width() / 2., height,
                    fmt_str.format(v) if v > 0 else "N/A", ha='center', va='bottom', fontweight='bold')

    plot_bar(ax1, peak_cpus, "Peak CPU Usage", "Peak CPU (%)", '{:.1f}%')
    plot_bar(ax2, peak_mems, "Peak Memory Usage", "Peak Memory (MB)", '{:.0f}')

    plt.tight_layout()
    plt.savefig(out_path, bbox_inches='tight')
    plt.close()


def plot_image_size(runs: List[Any], out_path: str, get_store_rank: Optional[Callable[[str], int]] = None) -> None:
    """Create a visualization of container image sizes with average and peak bars."""
    adapter_data: Dict[str, List[float]] = {}

    for run in runs:
        metrics = run.metrics
        img_size = metrics.get("image_size_bytes")

        if img_size is None:
            continue

        if run.adapter not in adapter_data:
            adapter_data[run.adapter] = []

        adapter_data[run.adapter].append(img_size / (1024 * 1024))

    if not adapter_data:
        return

    adapters_list = list(adapter_data.keys())
    if get_store_rank:
        adapters = sorted(adapters_list, key=get_store_rank)
    else:
        adapters = sorted(adapters_list)

    avg_image_sizes: List[float] = []
    peak_image_sizes: List[float] = []

    for a in adapters:
        img_sizes = adapter_data[a]
        avg_image_sizes.append(float(np.mean(img_sizes)))
        peak_image_sizes.append(float(np.max(img_sizes)))

    fig, ax = plt.subplots()
    colors = [get_adapter_color(adapter) for adapter in adapters]

    def plot_bar(ax: Any, avg_data: Sequence[float], peak_data: Sequence[float], title: str, ylabel: str, fmt_str: str) -> None:
        x = np.arange(len(adapters))
        ax.bar(x, avg_data, color=colors, edgecolor='black', linewidth=1.5, alpha=1.0)
        ax.bar(x, np.maximum(0, np.array(peak_data) - np.array(avg_data)), bottom=avg_data,
               color=colors, edgecolor='black', linewidth=1.5, alpha=0.5)

        ax.set_ylabel(ylabel, fontweight='bold')
        ax.set_title(title, fontweight='bold', fontsize=14)
        ax.set_xticks(x)
        ax.set_xticklabels(adapters)
        ax.grid(True, alpha=0.3, axis='y')
        _set_y_limit_with_margin(ax, peak_data)
        
        for i, (avg, peak) in enumerate(zip(avg_data, peak_data)):
            if peak > 0:
                label = fmt_str.format(avg)
                if peak > avg * 1.05:
                    label += f" / {fmt_str.format(peak)}"
                ax.text(i, peak, label, ha='center', va='bottom', fontweight='bold', fontsize=10)

    plot_bar(ax, avg_image_sizes, peak_image_sizes, "Container Image Size", "Image Size (MB)", '{:.0f}')

    metric_handles = [
        Line2D([0], [0], color='gray', alpha=1.0, lw=4, label='Average'),
        Line2D([0], [0], color='gray', alpha=0.5, lw=4, label='Peak')
    ]
    ax.legend(handles=metric_handles, loc='upper right')

    plt.tight_layout()
    plt.savefig(out_path, bbox_inches='tight')
    plt.close()


def plot_startup_time(runs: List[Any], out_path: str, get_store_rank: Optional[Callable[[str], int]] = None) -> None:
    """Create a visualization of container startup times with average and peak bars."""
    adapter_data: Dict[str, List[float]] = {}

    for run in runs:
        metrics = run.metrics
        startup = metrics.get("startup_time_s")

        if startup is None or startup <= 0:
            continue

        if run.adapter not in adapter_data:
            adapter_data[run.adapter] = []

        adapter_data[run.adapter].append(startup)

    if not adapter_data:
        return

    adapters_list = list(adapter_data.keys())
    if get_store_rank:
        adapters = sorted(adapters_list, key=get_store_rank)
    else:
        adapters = sorted(adapters_list)

    avg_startup_times: List[float] = []
    peak_startup_times: List[float] = []

    for a in adapters:
        s_times = adapter_data[a]
        avg_startup_times.append(float(np.mean(s_times)))
        peak_startup_times.append(float(np.max(s_times)))

    fig, ax = plt.subplots()
    colors = [get_adapter_color(adapter) for adapter in adapters]

    def plot_bar(ax: Any, avg_data: Sequence[float], peak_data: Sequence[float], title: str, ylabel: str, fmt_str: str) -> None:
        x = np.arange(len(adapters))
        ax.bar(x, avg_data, color=colors, edgecolor='black', linewidth=1.5, alpha=1.0)
        ax.bar(x, np.maximum(0, np.array(peak_data) - np.array(avg_data)), bottom=avg_data,
               color=colors, edgecolor='black', linewidth=1.5, alpha=0.5)

        ax.set_ylabel(ylabel, fontweight='bold')
        ax.set_title(title, fontweight='bold', fontsize=14)
        ax.set_xticks(x)
        ax.set_xticklabels(adapters)
        ax.grid(True, alpha=0.3, axis='y')
        _set_y_limit_with_margin(ax, peak_data)
        
        for i, (avg, peak) in enumerate(zip(avg_data, peak_data)):
            if peak > 0:
                label = fmt_str.format(avg)
                if peak > avg * 1.05:
                    label += f" / {fmt_str.format(peak)}"
                ax.text(i, peak, label, ha='center', va='bottom', fontweight='bold', fontsize=10)

    plot_bar(ax, avg_startup_times, peak_startup_times, "Container Startup Time", "Startup Time (s)", '{:.2f}')

    metric_handles = [
        Line2D([0], [0], color='gray', alpha=1.0, lw=4, label='Average'),
        Line2D([0], [0], color='gray', alpha=0.5, lw=4, label='Peak')
    ]
    ax.legend(handles=metric_handles, loc='upper right')

    plt.tight_layout()
    plt.savefig(out_path, bbox_inches='tight')
    plt.close()