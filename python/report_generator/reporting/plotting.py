import math
from collections import defaultdict
from typing import Any, Callable, Dict, List, Optional, Sequence

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.lines import Line2D
from matplotlib.ticker import LogLocator, NullFormatter, FuncFormatter

from .style import (
    ADAPTER_COLORS, get_adapter_color, PLOT_WIDTH, PLOT_HEIGHT, PLOT_DPI,
    FONT_SIZE_TITLE, FONT_SIZE_LABEL, FONT_SIZE_TICK, FONT_SIZE_LEGEND
)
from ..workloads.performance import PerformanceWorkloadRun

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


def _grouped_bar_layout(
    group_count: int,
    series_count: int,
    group_width: float = 0.72,
    inter_group_scale: float = 1.2,
    intra_group_gap_ratio: float = 0.06,
) -> tuple[np.ndarray, float, np.ndarray]:
    """Return x positions, bar width, and per-series offsets for grouped bars."""
    x = np.arange(group_count) * inter_group_scale
    width = group_width / max(1, series_count)
    offset_step = width * (1 + intra_group_gap_ratio)
    offsets = (np.arange(series_count) - (series_count - 1) / 2) * offset_step
    return x, width, offsets


def _pixel_align_grouped_bars(ax: Any, x: np.ndarray, width: float, offsets: np.ndarray) -> tuple[np.ndarray, float, np.ndarray]:
    """Quantize grouped-bar geometry to whole device pixels for crisper rendering."""
    if len(x) == 0 or len(offsets) == 0 or width <= 0:
        return x, width, offsets

    bar_centers = x[:, np.newaxis] + offsets[np.newaxis, :]
    min_left = float(np.min(bar_centers - width / 2))
    max_right = float(np.max(bar_centers + width / 2))
    span = max_right - min_left
    pad = span * 0.05 if span > 0 else 0.5
    ax.set_xlim(min_left - pad, max_right + pad)
    ax.figure.canvas.draw()

    transformed = ax.transData.transform(np.column_stack([x, np.zeros_like(x)]))
    x_pixels = transformed[:, 0]

    base_x_pixels = ax.transData.transform(np.array([[0.0, 0.0]]))[0, 0]
    width_pixels = ax.transData.transform(np.array([[width, 0.0]]))[0, 0] - base_x_pixels
    offset_pixels = ax.transData.transform(np.column_stack([offsets, np.zeros_like(offsets)]))[:, 0] - base_x_pixels

    x_pixels_aligned = np.round(x_pixels)
    width_pixels_aligned = max(1.0, np.round(width_pixels))
    offset_pixels_aligned = np.round(offset_pixels)

    inv = ax.transData.inverted()
    x_aligned = inv.transform(np.column_stack([x_pixels_aligned, np.zeros_like(x_pixels_aligned)]))[:, 0]
    base_x_aligned = inv.transform(np.array([[base_x_pixels, 0.0]]))[0, 0]
    width_aligned = inv.transform(np.array([[base_x_pixels + width_pixels_aligned, 0.0]]))[0, 0] - base_x_aligned
    offsets_aligned = inv.transform(np.column_stack([base_x_pixels + offset_pixels_aligned, np.zeros_like(offset_pixels_aligned)]))[:, 0] - base_x_aligned

    return x_aligned, float(width_aligned), offsets_aligned


def _filter_cdf_to_percentile(latencies_ms: Sequence[float], percentiles: Sequence[float], cap: float = 99.9) -> tuple[list[float], list[float]]:
    filtered = [(latency, percentile) for latency, percentile in zip(latencies_ms, percentiles) if percentile <= cap]
    if not filtered:
        return [], []
    filtered_latencies, filtered_percentiles = zip(*filtered)
    return list(filtered_latencies), list(filtered_percentiles)


def _plot_cdf_line_with_end_markers(
    latencies_ms: Sequence[float],
    percentiles: Sequence[float],
    label: str,
    linewidth: float = 2,
    color: Optional[str] = None,
) -> tuple[list[float], list[float]]:
    filtered_latencies, filtered_percentiles = _filter_cdf_to_percentile(latencies_ms, percentiles)
    if not filtered_latencies:
        return [], []

    plt.plot(filtered_latencies, filtered_percentiles, label=label, linewidth=linewidth, color=color)
    plt.scatter(
        [filtered_latencies[0], filtered_latencies[-1]],
        [filtered_percentiles[0], filtered_percentiles[-1]],
        color=color,
        s=20,
        zorder=3,
    )
    return filtered_latencies, filtered_percentiles


def _reorder_for_row_first_legend(items: list[tuple[Any, str]], ncol: int) -> list[tuple[Any, str]]:
    """Reorder legend items so matplotlib's column-wise packing appears row-wise."""
    if ncol <= 1 or len(items) <= ncol:
        return items

    rows = [items[i:i + ncol] for i in range(0, len(items), ncol)]
    reordered: list[tuple[Any, str]] = []
    for col in range(ncol):
        for row in rows:
            if col < len(row):
                reordered.append(row[col])
    return reordered


def _legend_below(*args: Any, **kwargs: Any) -> Any:
    kwargs.setdefault("loc", "upper center")
    kwargs.setdefault("bbox_to_anchor", (0.5, -0.12))
    kwargs.setdefault("frameon", False)

    handles = kwargs.get("handles")
    labels = kwargs.get("labels")
    if labels is None:
        if handles is not None:
            labels = [h.get_label() for h in handles]
        else:
            current_handles, current_labels = plt.gca().get_legend_handles_labels()
            if handles is None:
                handles = current_handles
            labels = current_labels

    if handles is not None and labels is not None and len(handles) == len(labels):
        adapter_items = [(h, l) for h, l in zip(handles, labels) if l in ADAPTER_COLORS and l != "dummy"]
        other_items = [(h, l) for h, l in zip(handles, labels) if not (l in ADAPTER_COLORS and l != "dummy")]
        ordered_items = adapter_items + other_items
        if ordered_items:
            adapter_columns = min(len(adapter_items), 5) if adapter_items else 0
            if adapter_columns > 0:
                kwargs["ncol"] = adapter_columns
                ordered_items = _reorder_for_row_first_legend(ordered_items, adapter_columns)
            kwargs["handles"] = [h for h, _ in ordered_items]
            kwargs["labels"] = [l for _, l in ordered_items]
            labels = kwargs["labels"]

    adapter_columns = len([label for label in labels if label in ADAPTER_COLORS and label != "dummy"])
    if adapter_columns > 0:
        kwargs["ncol"] = min(adapter_columns, 5)

    return plt.legend(*args, **kwargs)


def _legend_below_single_row(*args: Any, **kwargs: Any) -> Any:
    kwargs.setdefault("loc", "lower center")
    kwargs.setdefault("bbox_to_anchor", (0.5, 0.01))
    kwargs.setdefault("frameon", False)

    handles = kwargs.get("handles")
    labels = kwargs.get("labels")
    if labels is None and handles is not None:
        labels = [h.get_label() for h in handles]
        kwargs["labels"] = labels

    if handles is not None:
        kwargs.setdefault("ncol", len(handles))

    return plt.legend(*args, **kwargs)


def _axes_legend_below(ax: Any, *args: Any, **kwargs: Any) -> Any:
    kwargs.setdefault("loc", "upper center")
    kwargs.setdefault("bbox_to_anchor", (0.5, -0.12))
    kwargs.setdefault("frameon", False)

    handles = kwargs.get("handles")
    labels = kwargs.get("labels")
    if labels is None:
        if handles is not None:
            labels = [h.get_label() for h in handles]
        else:
            current_handles, current_labels = ax.get_legend_handles_labels()
            if handles is None:
                handles = current_handles
            labels = current_labels

    if handles is not None and labels is not None and len(handles) == len(labels):
        adapter_items = [(h, l) for h, l in zip(handles, labels) if l in ADAPTER_COLORS and l != "dummy"]
        other_items = [(h, l) for h, l in zip(handles, labels) if not (l in ADAPTER_COLORS and l != "dummy")]
        ordered_items = adapter_items + other_items
        if ordered_items:
            adapter_columns = min(len(adapter_items), 5) if adapter_items else 0
            if adapter_columns > 0:
                kwargs["ncol"] = adapter_columns
                ordered_items = _reorder_for_row_first_legend(ordered_items, adapter_columns)
            kwargs["handles"] = [h for h, _ in ordered_items]
            kwargs["labels"] = [l for _, l in ordered_items]
            labels = kwargs["labels"]

    adapter_columns = len([label for label in labels if label in ADAPTER_COLORS and label != "dummy"])
    if adapter_columns > 0:
        kwargs["ncol"] = min(adapter_columns, 5)

    return ax.legend(*args, **kwargs)


def plot_latency_cdf(run: PerformanceWorkloadRun, out_path: str) -> None:
    """Plot latency CDF from a single run object."""
    latencies_ms, percentiles = run.get_latency_cdf_data()
    if latencies_ms is None or percentiles is None:
        return

    plt.figure()
    filtered_latencies, _ = _plot_cdf_line_with_end_markers(latencies_ms, percentiles, label="append latency CDF", linewidth=2)
    if not filtered_latencies:
        plt.close()
        return
    plt.xscale("log")
    
    # Ensure x-axis min value is half of the lowest plotted value (excluding zero)
    valid_latencies = [l for l in filtered_latencies if l > 0]
    if valid_latencies:
        plt.xlim(left=min(valid_latencies) / 2)

    plt.xlabel("Latency (ms) [log]")
    plt.ylabel("Percentile (%)")
    plt.title("Latency CDF")
    plt.gca().xaxis.set_major_locator(LogLocator(base=10, subs=(1.0, 2.0, 5.0)))
    plt.gca().xaxis.set_major_formatter(FuncFormatter(_format_tick))
    plt.gca().xaxis.set_minor_formatter(NullFormatter())
    plt.grid(True, which="both", ls=":", alpha=0.6)
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()


def plot_tool_latency_cdf(run: PerformanceWorkloadRun, out_path: str) -> None:
    """Plot benchmark process latency CDF from a single run object."""
    latencies_ms, percentiles = run.get_tool_latency_cdf_data()
    if latencies_ms is None or set(latencies_ms) == {0.0}:
        return

    plt.figure()
    filtered_latencies, _ = _plot_cdf_line_with_end_markers(
        latencies_ms,
        percentiles,
        label="benchmark latency CDF",
        linewidth=2,
        color='#ff7f0e',
    )
    if not filtered_latencies:
        plt.close()
        return
    plt.xscale("log")
    
    valid_latencies = [l for l in filtered_latencies if l > 0]
    if valid_latencies:
        plt.xlim(left=min(valid_latencies) / 2)

    plt.xlabel("Latency (ms) [log]")
    plt.ylabel("Percentile (%)")
    plt.title("Tool Process Latency CDF")
    plt.gca().xaxis.set_major_locator(LogLocator(base=10, subs=(1.0, 2.0, 5.0)))
    plt.gca().xaxis.set_major_formatter(FuncFormatter(_format_tick))
    plt.gca().xaxis.set_minor_formatter(NullFormatter())
    plt.grid(True, which="both", ls=":", alpha=0.6)
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()


def plot_throughput_timeseries(run: PerformanceWorkloadRun, out_path: str) -> None:
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


def plot_operation_errors_timeseries(run: PerformanceWorkloadRun, out_path: str) -> None:
    """Plot operation error counts over time for a single run object."""
    timeseries = run.get_operation_errors_timeseries()
    if timeseries is None:
        return

    plt.figure()
    plt.plot(timeseries["time_s"], timeseries["operation_errors"],
             linewidth=2.0, alpha=0.9, color='#d62728', marker=None,
             drawstyle='steps-pre')
    plt.xlabel("Elapsed Time (s)")
    plt.ylabel("Operation Errors")
    plt.title("Operation Errors over Time")
    plt.grid(True, ls=":", alpha=0.6)
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()


def plot_cpu_timeseries(run: PerformanceWorkloadRun, out_path: str) -> None:
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


def plot_tool_cpu_timeseries(run: PerformanceWorkloadRun, out_path: str) -> None:
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


def plot_memory_timeseries(run: PerformanceWorkloadRun, out_path: str) -> None:
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


def plot_tool_memory_timeseries(run: PerformanceWorkloadRun, out_path: str) -> None:
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


def plot_worker_slice_latency_cdf(runs: List[PerformanceWorkloadRun], title: str, out_path: str, get_store_rank: Optional[Callable[[str], int]] = None) -> None:
    """Plot latency CDF comparing multiple runs."""
    plt.figure()

    sorted_runs = sorted(runs, key=lambda r: get_store_rank(r.adapter)) if get_store_rank else runs

    all_latencies = []
    for run in sorted_runs:
        latencies_ms, percentiles = run.get_latency_cdf_data()
        if latencies_ms is None or percentiles is None:
            continue

        color = get_adapter_color(run.adapter)
        filtered_latencies, _ = _plot_cdf_line_with_end_markers(latencies_ms, percentiles, label=run.adapter, color=color, linewidth=2)
        all_latencies.extend([l for l in filtered_latencies if l > 0])

    plt.xscale("log")
    
    # Ensure x-axis min value is half of the lowest plotted value (excluding zero)
    if all_latencies:
        plt.xlim(left=min(all_latencies) / 2)

    plt.xlabel("Latency (ms) [log]")
    plt.ylabel("Percentile (%)")
    plt.gca().xaxis.set_major_locator(LogLocator(base=10, subs=(1.0, 2.0, 5.0)))
    plt.gca().xaxis.set_major_formatter(FuncFormatter(_format_tick))
    plt.gca().xaxis.set_minor_formatter(NullFormatter())
    plt.ticklabel_format(style='plain', axis='y')
    plt.title(title)
    _legend_below()
    plt.grid(True, which="both", ls=":", alpha=0.6)
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()


def plot_worker_slice_tool_latency_cdf(runs: List[PerformanceWorkloadRun], title: str, out_path: str, get_store_rank: Optional[Callable[[str], int]] = None) -> None:
    """Plot benchmark latency CDF comparing multiple runs."""

    sorted_runs = sorted(runs, key=lambda r: get_store_rank(r.adapter)) if get_store_rank else runs

    included_runs = []
    for run in sorted_runs:
        latencies_ms, percentiles = run.get_tool_latency_cdf_data()
        if not latencies_ms or set(latencies_ms) == {0.0}:
            continue
        included_runs.append((run, latencies_ms, percentiles))

    if not included_runs:
        return

    plt.figure()
    all_latencies = []
    for run, latencies_ms, percentiles in included_runs:
        color = get_adapter_color(run.adapter)
        filtered_latencies, _ = _plot_cdf_line_with_end_markers(latencies_ms, percentiles, label=run.adapter, color=color, linewidth=2)
        all_latencies.extend([l for l in filtered_latencies if l > 0])

    plt.xscale("log")

    if all_latencies:
        plt.xlim(left=min(all_latencies) / 2)

    plt.xlabel("Latency (ms) [log]")
    plt.ylabel("Percentile (%)")
    plt.gca().xaxis.set_major_locator(LogLocator(base=10, subs=(1.0, 2.0, 5.0)))
    plt.gca().xaxis.set_major_formatter(FuncFormatter(_format_tick))
    plt.gca().xaxis.set_minor_formatter(NullFormatter())
    plt.ticklabel_format(style='plain', axis='y')
    plt.title(title)
    _legend_below()
    plt.grid(True, which="both", ls=":", alpha=0.6)
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()


def plot_worker_slice_throughput(runs: List[PerformanceWorkloadRun], title: str, out_path: str, get_store_rank: Optional[Callable[[str], int]] = None) -> None:
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
    _legend_below()
    plt.grid(True, ls=":", alpha=0.6)
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()


def plot_worker_slice_operation_errors(runs: List[PerformanceWorkloadRun], title: str, out_path: str, get_store_rank: Optional[Callable[[str], int]] = None) -> None:
    """Plot operation errors over time comparing multiple runs."""
    plt.figure()

    sorted_runs = sorted(runs, key=lambda r: get_store_rank(r.adapter)) if get_store_rank else runs

    for run in sorted_runs:
        timeseries = run.get_operation_errors_timeseries()
        if timeseries is None:
            continue

        color = get_adapter_color(run.adapter)
        plt.plot(timeseries["time_s"], timeseries["operation_errors"],
                 label=run.adapter, color=color, linewidth=2.0, alpha=0.9, marker=None,
                 drawstyle='steps-pre')

    plt.xlabel("Elapsed Time (s)")
    plt.ylabel("Operation Errors")
    plt.title(title)
    _legend_below()
    plt.grid(True, ls=":", alpha=0.6)
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()


def plot_worker_slice_cpu(runs: List[PerformanceWorkloadRun], title: str, out_path: str, get_store_rank: Optional[Callable[[str], int]] = None) -> None:
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
    _legend_below()
    plt.grid(True, ls=":", alpha=0.6)
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()


def plot_worker_slice_tool_cpu(runs: List[PerformanceWorkloadRun], title: str, out_path: str, get_store_rank: Optional[Callable[[str], int]] = None) -> None:
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
    _legend_below()
    plt.grid(True, ls=":", alpha=0.6)
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()


def plot_worker_slice_memory(runs: List[PerformanceWorkloadRun], title: str, out_path: str, get_store_rank: Optional[Callable[[str], int]] = None) -> None:
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
    _legend_below()
    plt.grid(True, ls=":", alpha=0.6)
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()


def plot_worker_slice_tool_memory(runs: List[PerformanceWorkloadRun], title: str, out_path: str, get_store_rank: Optional[Callable[[str], int]] = None) -> None:
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
    _legend_below()
    plt.grid(True, ls=":", alpha=0.6)
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()


def plot_throughput_by_workers(runs: List[PerformanceWorkloadRun], out_path: str, get_store_rank: Optional[Callable[[str], int]] = None) -> None:
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
    x, width, offsets = _grouped_bar_layout(len(worker_counts), len(adapters))
    x, width, offsets = _pixel_align_grouped_bars(plt.gca(), x, width, offsets)

    all_vals = []
    for i, adapter in enumerate(adapters):
        avg_vals = np.array([data[wc].get(adapter, {}).get("avg", 0) for wc in worker_counts])
        peak_vals = np.array([data[wc].get(adapter, {}).get("peak", 0) for wc in worker_counts])
        
        offset = offsets[i]
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
    _legend_below(handles=adapter_handles + metric_handles, ncol=2)

    plt.grid(True, axis='y', ls=":", alpha=0.6)
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()


def plot_operation_errors_by_workers(runs: List[PerformanceWorkloadRun], out_path: str, get_store_rank: Optional[Callable[[str], int]] = None) -> None:
    """Plot operation errors vs worker count using grouped bars."""
    data: Dict[int, Dict[str, float]] = defaultdict(dict)
    all_adapters = set()
    all_worker_counts = set()

    for run in runs:
        if run.total_operation_errors > 0:
            data[run.worker_count][run.adapter] = run.total_operation_errors
            all_adapters.add(run.adapter)
            all_worker_counts.add(run.worker_count)

    if not data:
        return

    worker_counts = sorted(list(all_worker_counts))
    adapters = sorted(list(all_adapters), key=get_store_rank) if get_store_rank else sorted(list(all_adapters))

    first_run = runs[0] if runs else None
    xlabel = ("Readers" if first_run.is_read_workload else "Writers") if first_run else "Workers"
    title = f"Operation Errors by {xlabel[:-1]} Count"

    plt.figure()
    x, width, offsets = _grouped_bar_layout(len(worker_counts), len(adapters))
    x, width, offsets = _pixel_align_grouped_bars(plt.gca(), x, width, offsets)

    for i, adapter in enumerate(adapters):
        vals = np.array([data[wc].get(adapter, 0) for wc in worker_counts])
        offset = offsets[i]
        color = get_adapter_color(adapter)
        plt.bar(x + offset, vals, width, color=color, alpha=0.9)

    plt.ylabel("Operation Errors")
    plt.xlabel(xlabel)
    plt.title(title)
    plt.xticks(x, [str(wc) for wc in worker_counts])

    adapter_handles = [Line2D([0], [0], color=get_adapter_color(a), lw=4, label=a) for a in adapters]
    _legend_below(handles=adapter_handles)
    plt.grid(True, axis='y', ls=":", alpha=0.6)
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()


def plot_latency_by_workers(runs: List[PerformanceWorkloadRun], out_path: str, get_store_rank: Optional[Callable[[str], int]] = None) -> None:
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
    x, width, offsets = _grouped_bar_layout(len(worker_counts), len(adapters))
    x, width, offsets = _pixel_align_grouped_bars(plt.gca(), x, width, offsets)

    all_p50_vals = []
    for i, adapter in enumerate(adapters):
        p50_vals = np.array([data[wc].get(adapter, {}).get("p50", 0) for wc in worker_counts])
        p99_vals = np.array([data[wc].get(adapter, {}).get("p99", 0) for wc in worker_counts])
        p999_vals = np.array([data[wc].get(adapter, {}).get("p999", 0) for wc in worker_counts])

        offset = offsets[i]
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

    _legend_below(handles=adapter_handles + percentile_handles, ncol=2)
    plt.grid(True, axis='y', ls=":", alpha=0.6)
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()


def plot_tool_latency_by_workers(runs: List[PerformanceWorkloadRun], out_path: str, get_store_rank: Optional[Callable[[str], int]] = None) -> None:
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
    x, width, offsets = _grouped_bar_layout(len(worker_counts), len(adapters))
    x, width, offsets = _pixel_align_grouped_bars(plt.gca(), x, width, offsets)

    all_p50_vals = []
    for i, adapter in enumerate(adapters):
        p50_vals = np.array([data[wc].get(adapter, {}).get("p50", 0) for wc in worker_counts])
        p99_vals = np.array([data[wc].get(adapter, {}).get("p99", 0) for wc in worker_counts])
        p999_vals = np.array([data[wc].get(adapter, {}).get("p999", 0) for wc in worker_counts])

        offset = offsets[i]
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
    _legend_below(handles=adapter_handles + metric_handles, ncol=len(adapters) if len(adapters) < 4 else 4)

    plt.grid(True, axis='y', ls=":", alpha=0.6)
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()


def plot_cpu_by_workers(runs: List[PerformanceWorkloadRun], out_path: str, get_store_rank: Optional[Callable[[str], int]] = None) -> None:
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
    x, width, offsets = _grouped_bar_layout(len(worker_counts), len(adapters))
    x, width, offsets = _pixel_align_grouped_bars(plt.gca(), x, width, offsets)

    all_cpu: List[float] = []
    for i, adapter in enumerate(adapters):
        avg_vals = np.array([data[wc].get(adapter, {}).get("avg", 0) for wc in worker_counts])
        peak_vals = np.array([data[wc].get(adapter, {}).get("peak", 0) for wc in worker_counts])
        
        offset = offsets[i]
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
    _legend_below(handles=adapter_handles + metric_handles, ncol=2)
    
    plt.grid(True, axis='y', ls=":", alpha=0.6)
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()


def plot_tool_cpu_by_workers(runs: List[PerformanceWorkloadRun], out_path: str, get_store_rank: Optional[Callable[[str], int]] = None) -> None:
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
    x, width, offsets = _grouped_bar_layout(len(worker_counts), len(adapters))
    x, width, offsets = _pixel_align_grouped_bars(plt.gca(), x, width, offsets)

    all_cpu: List[float] = []
    for i, adapter in enumerate(adapters):
        avg_vals = np.array([data[wc].get(adapter, {}).get("avg", 0) for wc in worker_counts])
        peak_vals = np.array([data[wc].get(adapter, {}).get("peak", 0) for wc in worker_counts])
        
        offset = offsets[i]
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
    _legend_below(handles=adapter_handles + metric_handles, ncol=2)

    plt.grid(True, axis='y', ls=":", alpha=0.6)
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()


def plot_memory_by_workers(runs: List[PerformanceWorkloadRun], out_path: str, get_store_rank: Optional[Callable[[str], int]] = None) -> None:
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
    x, width, offsets = _grouped_bar_layout(len(worker_counts), len(adapters))
    x, width, offsets = _pixel_align_grouped_bars(plt.gca(), x, width, offsets)

    all_mem: List[float] = []
    for i, adapter in enumerate(adapters):
        avg_vals = np.array([data[wc].get(adapter, {}).get("avg", 0) for wc in worker_counts])
        peak_vals = np.array([data[wc].get(adapter, {}).get("peak", 0) for wc in worker_counts])
        
        offset = offsets[i]
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
    _legend_below(handles=adapter_handles + metric_handles, ncol=2)

    plt.grid(True, axis='y', ls=":", alpha=0.6)
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()


def plot_tool_memory_by_workers(runs: List[PerformanceWorkloadRun], out_path: str, get_store_rank: Optional[Callable[[str], int]] = None) -> None:
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
    x, width, offsets = _grouped_bar_layout(len(worker_counts), len(adapters))
    x, width, offsets = _pixel_align_grouped_bars(plt.gca(), x, width, offsets)

    all_mem: List[float] = []
    for i, adapter in enumerate(adapters):
        avg_vals = np.array([data[wc].get(adapter, {}).get("avg", 0) for wc in worker_counts])
        peak_vals = np.array([data[wc].get(adapter, {}).get("peak", 0) for wc in worker_counts])
        
        offset = offsets[i]
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
    _legend_below(handles=adapter_handles + metric_handles, ncol=2)

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
    _plot_container_avg_peak_bars(
        ax,
        adapters,
        avg_image_sizes,
        peak_image_sizes,
        "Container Image Size",
        "Image Size (MB)",
        "{:.0f}",
    )

    metric_handles = [
        Line2D([0], [0], color='gray', alpha=1.0, lw=4, label='Average'),
        Line2D([0], [0], color='gray', alpha=0.5, lw=4, label='Peak')
    ]
    _axes_legend_below(ax, handles=metric_handles)

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
    _plot_container_avg_peak_bars(
        ax,
        adapters,
        avg_startup_times,
        peak_startup_times,
        "Container Startup Time",
        "Startup Time (s)",
        "{:.2f}",
    )

    metric_handles = [
        Line2D([0], [0], color='gray', alpha=1.0, lw=4, label='Average'),
        Line2D([0], [0], color='gray', alpha=0.5, lw=4, label='Peak')
    ]
    _axes_legend_below(ax, handles=metric_handles)

    plt.tight_layout()
    plt.savefig(out_path, bbox_inches='tight')
    plt.close()


def _collect_container_metric(
    runs: List[Any],
    metric_key: str,
    transform: Callable[[float], float],
    allow_value: Callable[[float], bool],
) -> Dict[str, List[float]]:
    adapter_data: Dict[str, List[float]] = {}
    for run in runs:
        raw_val = run.metrics.get(metric_key)
        if raw_val is None:
            continue
        val = transform(raw_val)
        if not allow_value(val):
            continue
        adapter_data.setdefault(run.adapter, []).append(val)
    return adapter_data


def _collect_avg_peak_series(
    adapter_data: Dict[str, List[float]],
    get_store_rank: Optional[Callable[[str], int]],
) -> tuple[list[str], list[float], list[float]]:
    adapters_list = list(adapter_data.keys())
    if get_store_rank:
        adapters = sorted(adapters_list, key=get_store_rank)
    else:
        adapters = sorted(adapters_list)

    avg_values: List[float] = []
    peak_values: List[float] = []
    for adapter in adapters:
        values = adapter_data[adapter]
        avg_values.append(float(np.mean(values)))
        peak_values.append(float(np.max(values)))

    return adapters, avg_values, peak_values


def _plot_container_avg_peak_bars(
    ax: Any,
    adapters: Sequence[str],
    avg_data: Sequence[float],
    peak_data: Sequence[float],
    title: str,
    ylabel: str,
    fmt_str: str,
    show_xtick_labels: bool = True,
    show_peak_segment: bool = True,
    show_title_as_xlabel: bool = False,
) -> None:
    colors = [get_adapter_color(adapter) for adapter in adapters]
    x = np.arange(len(adapters))
    ax.bar(x, avg_data, color=colors, alpha=1.0)
    if show_peak_segment:
        ax.bar(
            x,
            np.maximum(0, np.array(peak_data) - np.array(avg_data)),
            bottom=avg_data,
            color=colors,
            alpha=0.5,
        )

    ax.set_ylabel(ylabel, fontweight='bold')
    if show_title_as_xlabel:
        ax.set_title(title, fontweight='bold', fontsize=FONT_SIZE_LABEL)
    else:
        ax.set_title(title, fontweight='bold', fontsize=FONT_SIZE_TITLE)
    ax.set_xticks(x)
    if show_xtick_labels:
        ax.set_xticklabels(adapters)
    else:
        ax.set_xticklabels([])
    ax.grid(True, alpha=0.3, axis='y')
    _set_y_limit_with_margin(ax, peak_data if show_peak_segment else avg_data)

    for i, (avg, peak) in enumerate(zip(avg_data, peak_data)):
        bar_top = peak if show_peak_segment else avg
        if bar_top > 0:
            label = fmt_str.format(avg)
            if show_peak_segment and peak > avg * 1.05:
                label += f" / {fmt_str.format(peak)}"
            ax.text(i, bar_top, label, ha='center', va='bottom', fontweight='bold', fontsize=FONT_SIZE_TICK)


def plot_container_stats_summary(
    runs: List[Any],
    out_path: str,
    get_store_rank: Optional[Callable[[str], int]] = None,
) -> None:
    image_sizes_by_adapter = _collect_container_metric(
        runs,
        metric_key="image_size_bytes",
        transform=lambda value: value / (1024 * 1024),
        allow_value=lambda value: value > 0,
    )
    startup_times_by_adapter = _collect_container_metric(
        runs,
        metric_key="startup_time_s",
        transform=float,
        allow_value=lambda value: value > 0,
    )

    if not image_sizes_by_adapter and not startup_times_by_adapter:
        return

    fig, axes = plt.subplots(1, 2, figsize=(PLOT_WIDTH * 2, PLOT_HEIGHT))
    image_ax, startup_ax = axes

    image_adapters: list[str] = []
    startup_adapters: list[str] = []

    if image_sizes_by_adapter:
        image_adapters, avg_image_sizes, peak_image_sizes = _collect_avg_peak_series(
            image_sizes_by_adapter,
            get_store_rank,
        )
        _plot_container_avg_peak_bars(
            image_ax,
            image_adapters,
            avg_image_sizes,
            peak_image_sizes,
            "Container Image Size",
            "Image Size (MB)",
            "{:.0f}",
            show_xtick_labels=False,
            show_title_as_xlabel=True,
        )
    else:
        image_ax.set_axis_off()

    if startup_times_by_adapter:
        startup_adapters, avg_startup_times, peak_startup_times = _collect_avg_peak_series(
            startup_times_by_adapter,
            get_store_rank,
        )
        _plot_container_avg_peak_bars(
            startup_ax,
            startup_adapters,
            avg_startup_times,
            peak_startup_times,
            "Container Startup Time",
            "Startup Time (s)",
            "{:.2f}",
            show_xtick_labels=False,
            show_peak_segment=False,
            show_title_as_xlabel=True,
        )
    else:
        startup_ax.set_axis_off()

    legend_adapters: list[str] = []
    if image_sizes_by_adapter:
        legend_adapters.extend(image_adapters)
    if startup_times_by_adapter:
        for adapter in startup_adapters:
            if adapter not in legend_adapters:
                legend_adapters.append(adapter)

    if legend_adapters:
        adapter_handles = [
            Line2D([0], [0], color=get_adapter_color(adapter), lw=8, label=adapter)
            for adapter in legend_adapters
        ]
        _legend_below_single_row(
            handles=adapter_handles,
            loc="lower center",
            bbox_to_anchor=(0.5, 0.01),
            bbox_transform=fig.transFigure,
        )

    fig.subplots_adjust(top=0.90, bottom=0.16, wspace=0.28)
    plt.savefig(out_path, bbox_inches='tight')
    plt.close()


def _select_best_worker_count_metrics_by_workload(
    workloads: List[Any],
) -> tuple[int | None, list[str], list[str], dict[str, dict[str, float]], dict[str, dict[str, float]]]:
    worker_scores_by_workload: dict[str, dict[int, float]] = {}

    for workload in workloads:
        runs = [run_report.run for run_report in workload.runs]
        worker_scores: dict[int, list[float]] = defaultdict(list)

        for run in runs:
            throughput = run.average_throughput
            p99_latency_ms = run.get_latency_percentile(99.0)
            if throughput <= 0 or p99_latency_ms <= 0:
                continue
            worker_scores[run.worker_count].append(throughput / p99_latency_ms)

        if not worker_scores:
            continue

        worker_scores_by_workload[workload.workload_name] = {
            worker_count: float(np.mean(scores))
            for worker_count, scores in worker_scores.items()
            if scores
        }

    global_worker_scores: dict[int, list[float]] = defaultdict(list)
    for workload_scores in worker_scores_by_workload.values():
        if not workload_scores:
            continue

        max_score = max(workload_scores.values())
        if max_score <= 0:
            continue

        for worker_count, score in workload_scores.items():
            global_worker_scores[worker_count].append(score / max_score)

    if not global_worker_scores:
        return None, [], [], {}, {}

    best_worker_count: int | None = None
    best_global_score = float("-inf")
    for worker_count, normalized_scores in global_worker_scores.items():
        if not normalized_scores:
            continue
        score = float(np.mean(normalized_scores))
        if best_worker_count is None or score > best_global_score or (
            score == best_global_score and worker_count < best_worker_count
        ):
            best_worker_count = worker_count
            best_global_score = score

    if best_worker_count is None:
        return None, [], [], {}, {}

    workload_names: list[str] = []
    adapter_order: list[str] = []
    throughput_by_workload: dict[str, dict[str, float]] = {}
    p99_by_workload: dict[str, dict[str, float]] = {}

    for workload in workloads:
        runs = [run_report.run for run_report in workload.runs]

        adapter_throughput: dict[str, list[float]] = defaultdict(list)
        adapter_p99: dict[str, list[float]] = defaultdict(list)

        for run in runs:
            if run.worker_count != best_worker_count:
                continue
            throughput = run.average_throughput
            p99_latency_ms = run.get_latency_percentile(99.0)
            if throughput <= 0 or p99_latency_ms <= 0:
                continue
            adapter_throughput[run.adapter].append(throughput)
            adapter_p99[run.adapter].append(p99_latency_ms)

        if not adapter_throughput:
            continue

        workload_name = workload.workload_name
        workload_names.append(workload_name)
        throughput_by_workload[workload_name] = {
            adapter: float(np.mean(values))
            for adapter, values in adapter_throughput.items()
            if values
        }
        p99_by_workload[workload_name] = {
            adapter: float(np.mean(values))
            for adapter, values in adapter_p99.items()
            if values
        }

        for adapter in workload.store_order:
            if adapter in throughput_by_workload[workload_name] and adapter not in adapter_order:
                adapter_order.append(adapter)
        for adapter in throughput_by_workload[workload_name]:
            if adapter not in adapter_order:
                adapter_order.append(adapter)

    return best_worker_count, workload_names, adapter_order, throughput_by_workload, p99_by_workload


def get_selected_worker_count_for_session_summary(workloads: List[Any]) -> int | None:
    selected_worker_count, _, _, _, _ = _select_best_worker_count_metrics_by_workload(workloads)
    return selected_worker_count


def plot_session_selected_slice_summary_by_workload(workloads: List[Any], out_path: str) -> None:
    selected_worker_count, workload_names, adapters, throughput_by_workload, p99_by_workload = (
        _select_best_worker_count_metrics_by_workload(workloads)
    )
    if selected_worker_count is None or not workload_names or not adapters:
        return

    col_count = len(workload_names)
    fig, axes = plt.subplots(
        2,
        col_count,
        figsize=(PLOT_WIDTH * 2, max(PLOT_HEIGHT * 1.25, 10.0)),
        squeeze=False,
        gridspec_kw={"hspace": 0.32, "wspace": 0.26},
    )

    adapter_handles: list[Line2D] = [
        Line2D([0], [0], color=get_adapter_color(adapter), lw=8, label=adapter) for adapter in adapters
    ]
    plotted_any = False

    for idx, workload_name in enumerate(workload_names):
        throughput_ax = axes[0, idx]
        latency_ax = axes[1, idx]

        throughput_values_by_adapter = throughput_by_workload.get(workload_name, {})
        latency_values_by_adapter = p99_by_workload.get(workload_name, {})
        valid_adapters = [
            adapter
            for adapter in adapters
            if adapter in throughput_values_by_adapter and adapter in latency_values_by_adapter
        ]

        if not valid_adapters:
            throughput_ax.set_visible(False)
            latency_ax.set_visible(False)
            continue

        x = np.arange(len(valid_adapters), dtype=float)
        colors = [get_adapter_color(adapter) for adapter in valid_adapters]
        throughput_values = np.array([throughput_values_by_adapter[adapter] for adapter in valid_adapters], dtype=float)
        latency_values = np.array([latency_values_by_adapter[adapter] for adapter in valid_adapters], dtype=float)

        throughput_ax.bar(x, throughput_values, 0.72, color=colors, alpha=0.9)
        throughput_ax.set_xticks([])
        throughput_ax.set_title(workload_name, fontweight='bold', fontsize=FONT_SIZE_LABEL)
        if idx == 0:
            throughput_ax.set_ylabel("Throughput (events/s)", fontweight='bold')
        throughput_ax.grid(True, axis='y', alpha=0.3)
        throughput_ax.yaxis.set_major_formatter(FuncFormatter(_format_tick))
        _set_y_limit_with_margin(throughput_ax, [float(v) for v in throughput_values if v > 0])
        for x_pos, value in zip(x, throughput_values):
            if value > 0:
                throughput_ax.text(
                    x_pos,
                    value,
                    f"{value:.0f}",
                    ha='center',
                    va='bottom',
                    fontweight='bold',
                    fontsize=FONT_SIZE_TICK,
                )

        latency_ax.bar(x, latency_values, 0.72, color=colors, alpha=0.9)
        latency_ax.set_xticks([])
        if idx == 0:
            latency_ax.set_ylabel("p99 Latency (ms)", fontweight='bold')
        latency_ax.grid(True, axis='y', alpha=0.3)
        latency_ax.yaxis.set_major_formatter(FuncFormatter(_format_tick))
        _set_y_limit_with_margin(latency_ax, [float(v) for v in latency_values if v > 0])
        for x_pos, value in zip(x, latency_values):
            if value > 0:
                latency_ax.text(
                    x_pos,
                    value,
                    f"{value:.2f}",
                    ha='center',
                    va='bottom',
                    fontweight='bold',
                    fontsize=FONT_SIZE_TICK,
                )

        plotted_any = True

    if not plotted_any:
        plt.close()
        return

    _legend_below_single_row(
        handles=adapter_handles,
        loc="lower center",
        bbox_to_anchor=(0.5, 0.01),
        bbox_transform=fig.transFigure,
    )
    fig.subplots_adjust(bottom=0.16, top=0.90)
    plt.savefig(out_path, bbox_inches='tight')
    plt.close()


def _plot_selected_slice_metric_by_workload(
    workload_names: list[str],
    adapters: list[str],
    selected_worker_count: int,
    out_path: str,
    metric_by_workload: dict[str, dict[str, float]],
    title: str,
    ylabel: str,
) -> None:
    if not workload_names or not adapters:
        return

    subplot_count = len(workload_names)
    cols = 1 if subplot_count == 1 else 2
    rows = math.ceil(subplot_count / cols)

    fig, axes = plt.subplots(
        rows,
        cols,
        figsize=(max(PLOT_WIDTH, cols * 6), max(PLOT_HEIGHT, rows * 3.8)),
        squeeze=False,
        gridspec_kw={"hspace": 0.24, "wspace": 0.28},
    )
    axes_flat = axes.flatten()

    adapter_handles: list[Line2D] = [Line2D([0], [0], color=get_adapter_color(adapter), lw=8, label=adapter) for adapter in adapters]
    plotted_any = False

    for idx, workload_name in enumerate(workload_names):
        ax = axes_flat[idx]
        values_by_adapter = metric_by_workload.get(workload_name, {})

        valid_adapters = [adapter for adapter in adapters if adapter in values_by_adapter]
        if not valid_adapters:
            ax.set_visible(False)
            continue

        x = np.arange(len(valid_adapters), dtype=float)
        values = np.array([values_by_adapter[adapter] for adapter in valid_adapters], dtype=float)
        colors = [get_adapter_color(adapter) for adapter in valid_adapters]

        ax.bar(x, values, 0.72, color=colors, edgecolor='black', linewidth=1.2, alpha=0.9)
        ax.set_title(workload_name, fontweight='bold', fontsize=12)
        if idx % cols == 0:
            ax.set_ylabel(ylabel, fontweight='bold')
        ax.set_xticks([])
        ax.grid(True, axis='y', alpha=0.3)
        ax.yaxis.set_major_formatter(FuncFormatter(_format_tick))
        _set_y_limit_with_margin(ax, [float(v) for v in values if v > 0])
        plotted_any = True

    for idx in range(subplot_count, len(axes_flat)):
        axes_flat[idx].set_visible(False)

    if not plotted_any:
        plt.close()
        return

    fig.suptitle(f"{title} (Workers: {selected_worker_count})", fontweight='bold', fontsize=14)
    _legend_below_single_row(
        handles=adapter_handles,
        bbox_transform=fig.transFigure,
    )
    fig.subplots_adjust(bottom=0.16, top=0.90)
    plt.savefig(out_path, bbox_inches='tight')
    plt.close()


def plot_session_selected_slice_throughput_by_workload(workloads: List[Any], out_path: str) -> None:
    plot_session_selected_slice_summary_by_workload(workloads, out_path)


def plot_session_selected_slice_p99_latency_by_workload(workloads: List[Any], out_path: str) -> None:
    plot_session_selected_slice_summary_by_workload(workloads, out_path)