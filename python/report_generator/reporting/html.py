from pathlib import Path
from typing import Any, Dict, List, Optional, Callable

import yaml

from ..models import EnvironmentInfo, SessionInfo, PerformanceWorkloadConfig
from ..data_loader import load_session_metadata, SessionMetadata
from ..workloads.performance import PerformanceWorkloadRun


def _format_bytes(byte_count: Optional[float]) -> str:
    if byte_count is None: return "N/A"
    power = 1024
    n = 0
    power_labels = {0: '', 1: 'K', 2: 'M', 3: 'G', 4: 'T'}
    while byte_count >= power and n < len(power_labels) - 1:
        byte_count /= power
        n += 1
    return f"{byte_count:.1f}{power_labels[n]}B"


def _get_env_summary(env_info: EnvironmentInfo | None) -> str:
    if not env_info:
        return "N/A"

    os_name = env_info.os.name
    cpu_model = env_info.cpu.model
    container_runtime = env_info.container_runtime
    if container_runtime:
        container_str = f"{container_runtime.runtime_type} {container_runtime.ncpu} CPU {_format_bytes(container_runtime.mem_total)}"
    else:
        container_str = "No Container Info"

    return f"{os_name} {cpu_model}, {container_str}"


def _render_environment_info(env_info: EnvironmentInfo | None) -> str:
    """Renders the EnvironmentInfo object into a nice HTML table."""
    if not env_info:
        return ""

    fsync_latency_html = "N/A"
    if env_info.disk and env_info.disk.fsync_latency:
        fsync = env_info.disk.fsync_latency
        fsync_latency_html = f"""
        <ul style="margin: 0; padding-left: 1.2rem;">
            <li><b>Avg:</b> {fsync.avg_us:.2f} µs</li>
            <li><b>p95:</b> {fsync.p95_us:.2f} µs</li>
            <li><b>p99:</b> {fsync.p99_us:.2f} µs</li>
        </ul>
        """

    container_runtime_html = "N/A"
    if env_info.container_runtime:
        cr = env_info.container_runtime
        container_runtime_html = f"{cr.runtime_type} {cr.version} ({cr.ncpu} vCPUs, {_format_bytes(cr.mem_total)} Memory)"

    return f"""
    <div class='workload-section'>
        <h2>Environment Information</h2>
        <div class='card' style='width: 100%;'>
            <table class='env-table'>
                <tr>
                    <th>OS</th>
                    <td>{env_info.os.name} {env_info.os.version} ({env_info.os.arch})</td>
                </tr>
                <tr>
                    <th>Kernel</th>
                    <td>{env_info.os.kernel}</td>
                </tr>
                <tr>
                    <th>CPU</th>
                    <td>{env_info.cpu.model} ({env_info.cpu.cores} cores, {env_info.cpu.threads} threads)</td>
                </tr>
                <tr>
                    <th>Memory</th>
                    <td>{_format_bytes(env_info.memory.total_bytes)} Total / {_format_bytes(env_info.memory.available_bytes)} Available</td>
                </tr>
                <tr>
                    <th>Disk</th>
                    <td>{env_info.disk.disk_type} ({env_info.disk.filesystem})</td>
                </tr>
                <tr>
                    <th>Fsync Latency</th>
                    <td>{fsync_latency_html}</td>
                </tr>
                <tr>
                    <th>Container Runtime</th>
                    <td>{container_runtime_html}</td>
                </tr>
            </table>
        </div>
    </div>
    """


def generate_run_html(report_dir: Path, run: PerformanceWorkloadRun) -> None:
    """Generates an HTML report for a single run."""
    workload_name = run.name
    latency_img = "latency_cdf.png"
    throughput_img = "throughput_timeseries.png"
    cpu_img = "cpu_timeseries.png"
    memory_img = "memory_timeseries.png"
    tool_latency_img = "tool_latency_cdf.png"
    tool_cpu_img = "tool_cpu_timeseries.png"
    tool_memory_img = "tool_memory_timeseries.png"

    metrics = run.metrics
    has_container_stats = bool(metrics.get('startup_time_s') or metrics.get("image_size_bytes"))
    
    container_stats_html = ""
    if has_container_stats:
        startup_time = f"{metrics.get('startup_time_s', 0):.2f}s" if metrics.get('startup_time_s') else "N/A"
        image_size_mb = f"{metrics.get('image_size_bytes', 0) / 1024 / 1024:.0f} MB" if metrics.get("image_size_bytes") else "N/A"
        container_stats_html = f"""
  <div class='row'>
    <div class='card'>
      <h2>Container Stats</h2>
      <p><b>Startup Time:</b> {startup_time}</p>
      <p><b>Image Size:</b> {image_size_mb}</p>
    </div>
  </div>"""

    avg_cpu = metrics.get("avg_cpu_percent")
    peak_cpu = metrics.get("peak_cpu_percent")
    cpu_display = "N/A"
    if avg_cpu is not None and peak_cpu is not None:
        cpu_display = f"{avg_cpu:.1f}% / {peak_cpu:.1f}%"
    elif avg_cpu is not None:
        cpu_display = f"{avg_cpu:.1f}%"

    avg_mem = metrics.get("avg_memory_bytes")
    peak_mem = metrics.get("peak_memory_bytes")
    mem_display = "N/A"
    if avg_mem is not None and peak_mem is not None:
        mem_display = f"{avg_mem / 1024 / 1024:.0f} / {peak_mem / 1024 / 1024:.0f} MB"
    elif avg_mem is not None:
        mem_display = f"{avg_mem / 1024 / 1024:.0f} MB"

    resource_metrics_html = ""
    if avg_cpu is not None or avg_mem is not None:
        resource_metrics_html = f"""
  <div class='row'>
    <div class='card'>
      <h2>Store Process Resource Metrics</h2>
      <p><b>CPU (avg/peak):</b> {cpu_display}</p>
      <p><b>Memory (avg/peak):</b> {mem_display}</p>
    </div>
  </div>"""

    # Benchmark Resource Metrics
    b_avg_cpu = metrics.get("tool_avg_cpu_percent")
    b_peak_cpu = metrics.get("tool_peak_cpu_percent")
    b_cpu_display = "N/A"
    if b_avg_cpu is not None and b_peak_cpu is not None:
        b_cpu_display = f"{b_avg_cpu:.1f}% / {b_peak_cpu:.1f}%"
    
    b_avg_mem = metrics.get("tool_avg_memory_bytes")
    b_peak_mem = metrics.get("tool_peak_memory_bytes")
    b_mem_display = "N/A"
    if b_avg_mem is not None and b_peak_mem is not None:
        b_mem_display = f"{b_avg_mem / 1024 / 1024:.0f} / {b_peak_mem / 1024 / 1024:.0f} MB"

    tool_resource_metrics_html = ""
    if b_avg_cpu is not None or b_avg_mem is not None:
        tool_resource_metrics_html = f"""
  <div class='row'>
    <div class='card'>
      <h2>Benchmark Process Resource Metrics</h2>
      <p><b>CPU (avg/peak):</b> {b_cpu_display}</p>
      <p><b>Memory (avg/peak):</b> {b_mem_display}</p>
    </div>
  </div>"""

    logs_html = ""
    if run.logs:
        logs_html = f"""
  <div class='row'>
    <div class='card' style='width: 100%;'>
      <h2>Container Logs</h2>
      <pre style='background: #f8f8f8; padding: 1rem; border-radius: 4px; overflow-x: auto; font-size: 0.85rem; max-height: 500px; overflow-y: auto;'>{run.logs}</pre>
    </div>
  </div>"""

    cpu_plot_html = ""
    if not run.cpu_df.empty:
        cpu_plot_html = f"""
    <div class='card'>
      <h2>CPU</h2>
      <img src='{cpu_img}' width='600' style='max-width: 100%; height: auto;'>
    </div>"""

    memory_plot_html = ""
    if not run.memory_df.empty:
        memory_plot_html = f"""
    <div class='card'>
      <h2>Memory</h2>
      <img src='{memory_img}' width='600' style='max-width: 100%; height: auto;'>
    </div>"""

    tool_plots_html = ""
    has_b_latency = not run.tool_latency_percentiles == []
    has_b_cpu = not run.tool_cpu_df.empty
    has_b_mem = not run.tool_memory_df.empty

    if has_b_latency or has_b_cpu or has_b_mem:
        plots = []
        if has_b_cpu:
            plots.append(f"""
    <div class='card'>
      <h2>Tool CPU</h2>
      <img src='{tool_cpu_img}' width='600' style='max-width: 100%; height: auto;'>
    </div>""")
        if has_b_mem:
            plots.append(f"""
    <div class='card'>
      <h2>Tool Memory</h2>
      <img src='{tool_memory_img}' width='600' style='max-width: 100%; height: auto;'>
    </div>""")
        if has_b_latency:
            plots.append(f"""
    <div class='card'>
      <h2>Tool Latency</h2>
      <img src='{tool_latency_img}' width='600' style='max-width: 100%; height: auto;'>
    </div>""")

        tool_plots_html = f"<div class='row'>{''.join(plots)}</div>"

    html = f"""
<!DOCTYPE html>
<html>
<head>
  <meta charset='utf-8'>
  <title>Run Report — {run.adapter} / {workload_name}</title>
  <style>
    body {{ font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif; margin: 2rem; }}
    h1, h2 {{ margin-top: 1.2rem; }}
    .row {{ display: flex; gap: 1rem; flex-wrap: wrap; margin-bottom: 1rem; }}
    .card {{ border: 1px solid #eee; border-radius: 8px; padding: 1rem; }}
  </style>
</head>
<body>
  <h1>Run Report</h1>
  <p><b>Adapter:</b> {run.adapter} &nbsp; | &nbsp; <b>Workload:</b> {workload_name}</p>
  <p><b>Duration:</b> {run.duration_s:.1f}s &nbsp; | &nbsp; <b>Throughput:</b> {run.average_throughput:.0f} eps</p>
  <div class='row'>
    <div class='card'>
      <h2>Throughput</h2>
      <img src='{throughput_img}' width='600' style='max-width: 100%; height: auto;'>
    </div>
    <div class='card'>
      <h2>Latency</h2>
      <img src='{latency_img}' width='600' style='max-width: 100%; height: auto;'>
    </div>
  </div>
  <div class='row'>
    {cpu_plot_html}
    {memory_plot_html}
  </div>
  {tool_plots_html}
  {resource_metrics_html}
  {tool_resource_metrics_html}
  {container_stats_html}
  {logs_html}
</body>
</html>
"""
    with open(report_dir / "index.html", "w") as f:
        f.write(html)


def generate_workload_html(
    out_base: Path,
    workload_name: str,
    runs: List[PerformanceWorkloadRun],
    worker_groups: Dict[int, List[Any]],
    orig_yaml: str,
    get_store_rank: Optional[Callable[[str], int]] = None,
) -> None:
    """Generate a consolidated report for a specific workload."""
    workload_dir = out_base / workload_name

    def row_key(r: Any) -> Any:
        rank = get_store_rank(r.adapter) if get_store_rank else 0
        return r.worker_count, rank, r.adapter

    first_run = runs[0] if runs else None
    is_readers = first_run.is_read_workload if first_run else False
    worker_label = "Reader" if is_readers else "Writer"
    worker_suffix = "r" if is_readers else "w"

    summary_rows = ""
    for run in sorted(runs, key=row_key):
        report_dir_name = run.adapter
        if run.readers > 0:
            report_dir_name += f"-r{run.readers}"
        if run.writers > 0:
            report_dir_name += f"-w{run.writers}"
        report_link = f"{report_dir_name}/index.html"

        metrics = run.metrics
        
        avg_cpu = metrics.get("avg_cpu_percent")
        peak_cpu = metrics.get("peak_cpu_percent")
        cpu_display = "N/A"
        if avg_cpu is not None and peak_cpu is not None:
            cpu_display = f"{avg_cpu:.1f}% / {peak_cpu:.1f}%"
        elif avg_cpu is not None:
            cpu_display = f"{avg_cpu:.1f}%"

        avg_mem = metrics.get("avg_memory_bytes")
        peak_mem = metrics.get("peak_memory_bytes")
        mem_display = "N/A"
        if avg_mem is not None and peak_mem is not None:
            mem_display = f"{avg_mem / 1024 / 1024:.0f} / {peak_mem / 1024 / 1024:.0f}"
        elif avg_mem is not None:
            mem_display = f"{avg_mem / 1024 / 1024:.0f}"

        avg_throughput = run.average_throughput
        peak_throughput = getattr(run, 'peak_throughput', 0)
        throughput_display = f"{avg_throughput:.0f}"
        if peak_throughput > 0:
            throughput_display = f"{avg_throughput:.0f} / {peak_throughput:.0f}"

        summary_rows += f"""
      <tr>
        <td><a href='{report_link}'>{run.adapter}</a></td>
        <td>{run.worker_count}</td>
        <td>{throughput_display}</td>
        <td>{run.get_latency_percentile(50.0):.2f}</td>
        <td>{run.get_latency_percentile(99.0):.2f}</td>
        <td>{run.get_latency_percentile(99.9):.2f}</td>
        <td>{cpu_display}</td>
        <td>{mem_display}</td>
      </tr>"""

    worker_slice_sections = ""
    for wc in sorted(worker_groups.keys()):
        group_runs = worker_groups[wc]
        has_cpu = any(not r.cpu_df.empty for r in group_runs)
        has_mem = any(not r.memory_df.empty for r in group_runs)

        cpu_slice_html = ""
        if has_cpu:
            cpu_slice_html = f"""
      <div class='card'>
        <h3>CPU</h3>
        <img src='worker_slice_{worker_suffix}{wc}_cpu_timeseries.png' width='600' style='max-width: 100%; height: auto;'>
      </div>"""

        mem_slice_html = ""
        if has_mem:
            mem_slice_html = f"""
      <div class='card'>
        <h3>Memory</h3>
        <img src='worker_slice_{worker_suffix}{wc}_memory_timeseries.png' width='600' style='max-width: 100%; height: auto;'>
      </div>"""

        tool_latency_png = f"worker_slice_{worker_suffix}{wc}_tool_latency_cdf.png"
        tool_latency_png_exists = (workload_dir / tool_latency_png).exists()
        tool_cpu_timeseries_png = f"worker_slice_{worker_suffix}{wc}_tool_cpu_timeseries.png"
        tool_cpu_timeseries_png_exists = (workload_dir / tool_cpu_timeseries_png).exists()
        tool_memory_timeseries_png = f"worker_slice_{worker_suffix}{wc}_tool_memory_timeseries.png"
        tool_memory_timeseries_png_exists = (workload_dir / tool_memory_timeseries_png).exists()

        tool_latency_slice_html = f"""
      <div class='card'>
        <h3>Tool Latency</h3>
        <img src='{tool_latency_png}' width='600' style='max-width: 100%; height: auto;'>
      </div>""" if tool_latency_png_exists else ""

        tool_cpu_slice_html = f"""
      <div class='card'>
        <h3>Tool CPU</h3>
        <img src='{tool_cpu_timeseries_png}' width='600' style='max-width: 100%; height: auto;'>
      </div>""" if tool_cpu_timeseries_png_exists else ""

        tool_mem_slice_html = f"""
      <div class='card'>
        <h3>Tool Memory</h3>
        <img src='{tool_memory_timeseries_png}' width='600' style='max-width: 100%; height: auto;'>
      </div>""" if tool_memory_timeseries_png_exists else ""

        tool_slice_html = f"""
    <div class='row'>
      {tool_cpu_slice_html}
      {tool_mem_slice_html}
    </div>
    <div class='row'>
      {tool_latency_slice_html}
    </div>""" if tool_cpu_slice_html or tool_mem_slice_html or tool_latency_slice_html else ""

        worker_slice_sections += f"""
    <h2>{worker_label}s = {wc}</h2>
    <div class='row'>
      <div class='card'>
        <h3>Throughput</h3>
        <img src='worker_slice_{worker_suffix}{wc}_throughput.png' width='600' style='max-width: 100%; height: auto;'>
      </div>
      <div class='card'>
        <h3>Latency</h3>
        <img src='worker_slice_{worker_suffix}{wc}_latency_cdf.png' width='600' style='max-width: 100%; height: auto;'>
      </div>
    </div>
    <div class='row'>
      {cpu_slice_html}
      {mem_slice_html}
    </div>
    {tool_slice_html}"""

    has_any_cpu = any(not r.cpu_df.empty for r in runs)
    has_any_mem = any(not r.memory_df.empty for r in runs)

    cpu_by_workers_html = f"""
      <div class='card'>
        <h3>CPU</h3>
        <img src='by_workers_cpu.png' width='600' style='max-width: 100%; height: auto;'>
      </div>""" if has_any_cpu else ""


    mem_by_workers_html = f"""
      <div class='card'>
        <h3>Memory</h3>
        <img src='by_workers_memory.png' width='600' style='max-width: 100%; height: auto;'>
      </div>""" if has_any_mem else ""

    resource_usage_html = f"""
    <div class='row'>
      {cpu_by_workers_html}
      {mem_by_workers_html}
    </div>""" if has_any_cpu or has_any_mem else ""


    performance_section = f"""
    <h2>Performance by {worker_label}s</h2>
    <div class='row'>
      <div class='card'>
        <h3>Throughput</h3>
        <img src='by_workers_throughput.png' width='600' style='max-width: 100%; height: auto;'>
      </div>
      <div class='card'>
        <h3>Latency</h3>
        <img src='by_workers_latency.png' width='600' style='max-width: 100%; height: auto;'>
      </div>
    </div>
    {resource_usage_html}"""

    tool_latency_png = f"by_workers_tool_latency.png"
    tool_latency_png_exists = (workload_dir / tool_latency_png).exists()
    tool_cpu_png = f"by_workers_tool_cpu.png"
    tool_cpu_png_exists = (workload_dir / tool_cpu_png).exists()
    tool_memory_png = f"by_workers_tool_memory.png"
    tool_memory_png_exists = (workload_dir / tool_memory_png).exists()

    tool_performance_section = ""
    if tool_latency_png_exists or tool_cpu_png_exists or tool_memory_png_exists:
        tool_latency_by_workers_html = f"""
      <div class='card'>
        <h3>Tool Latency</h3>
        <img src='{tool_latency_png}' width='600' style='max-width: 100%; height: auto;'>
      </div>""" if tool_latency_png_exists else ""

        tool_cpu_by_workers_html = f"""
      <div class='card'>
        <h3>Tool CPU</h3>
        <img src='{tool_cpu_png}' width='600' style='max-width: 100%; height: auto;'>
      </div>""" if tool_cpu_png_exists else ""

        tool_mem_by_workers_html = f"""
      <div class='card'>
        <h3>Tool Memory</h3>
        <img src='{tool_memory_png}' width='600' style='max-width: 100%; height: auto;'>
      </div>""" if tool_memory_png_exists else ""

        tool_performance_section = f"""
    <div class='row'>
      {tool_cpu_by_workers_html}
      {tool_mem_by_workers_html}
    </div>
    <div class='row'>
      {tool_latency_by_workers_html}
    </div>"""

    config_section = f"""
    <h2>Configuration</h2>
    <div class='card'>
      <pre style='background-color: #f8f8f8; padding: 1rem; border-radius: 4px; overflow-x: auto;'>{orig_yaml}</pre>
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
    .card {{ border: 1px solid #eee; border-radius: 8px; padding: 1rem; margin-bottom: 1rem; }}
  </style>
</head>
<body>
  <h1>Workload Report — {workload_name}</h1>
  <p><a href="../index.html">← Back to all workloads</a></p>
  {performance_section}
  {tool_performance_section}
  {worker_slice_sections}
  <h2>Runs</h2>
  <table>
    <tr><th>Adapter</th><th>{worker_label}s</th><th>Throughput (eps)</th><th>p50 (ms)</th><th>p99 (ms)</th><th>p99.9 (ms)</th><th>CPU (avg/peak)</th><th>Mem MB (avg/peak)</th></tr>
    {summary_rows}
  </table>
  {config_section}
</body>
</html>
"""
    workload_dir.mkdir(parents=True, exist_ok=True)
    with open(workload_dir / "index.html", "w") as f:
        f.write(html)


def generate_top_level_index(raw_base: Path, published_base: Path) -> None:
    """Generate top-level index.html that links to individual session reports."""
    sessions_summaries = {}
    published_session_ids = sorted([d.name for d in published_base.iterdir() if d.is_dir()])

    for session_id in published_session_ids:
        raw_session_dir = raw_base / session_id
        if not raw_session_dir.exists():
            continue

        try:
            session_metadata = load_session_metadata(raw_session_dir)
            if session_metadata is None:
                return None
            workload_name = session_metadata.session_info.workload_name

            all_stores = set()
            for _orig_yaml, perf_cfg in session_metadata.workload_configs:
                stores = perf_cfg.stores
                if isinstance(stores, list):
                    all_stores.update(stores)
                elif isinstance(stores, str):
                    all_stores.add(stores)

            sessions_summaries[session_id] = {
                'workload_name': workload_name,
                'tool_version': session_metadata.session_info.tool_version or 'N/A',
                'stores': list(all_stores),
                'env_summary': _get_env_summary(session_metadata.environment_info),
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
        <td>{summary.get('env_summary', 'N/A')}</td>
        <td>{summary.get('tool_version', 'N/A')}</td>
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
  <h1>Store Benchmark Suite</h1>
  <h2>Benchmark Sessions</h2>
  <table>
    <tr><th>Session ID</th><th>Workload</th><th>Stores</th><th>Environment</th><th>Version</th></tr>
    {session_rows}
  </table>
</body>
</html>
"""
    with open(published_base / "index.html", "w") as f:
        f.write(html)


def generate_session_index(session_out_dir: Path, workload_summaries: Dict[str, Any], session_metadata: SessionMetadata) -> None:
    """Generate index.html for a specific session."""
    env_section = _render_environment_info(session_metadata.environment_info)

    workload_sections = ""
    for workload_name, summary in sorted(workload_summaries.items()):
        by_workers_plots = f"""
      <div class='row'>
        <div class='card'>
          <h3>Throughput</h3>
          <img src='{workload_name}/by_workers_throughput.png' width='600' style='max-width: 100%; height: auto;'>
        </div>
        <div class='card'>
          <h3>Latency</h3>
          <img src='{workload_name}/by_workers_latency.png' width='600' style='max-width: 100%; height: auto;'>
        </div>
      </div>"""

        workload_sections += f"""
    <div class='workload-section'>
      <h2><a href='{workload_name}/index.html'>{workload_name}</a></h2>
      {by_workers_plots}
    </div>"""

    session_title = f"Benchmark Session: {session_metadata.session_info.session_id}"

    container_stats_section = ""
    has_image_size = (session_out_dir / "image_size.png").exists()
    has_startup_time = (session_out_dir / "startup_time.png").exists()
    
    if has_image_size or has_startup_time:
        container_stats_section = f"""
    <div class='workload-section'>
      <h2>Container Stats</h2>
      <div class='row'>
        {f'''<div class='card'>
          <h3>Image Size</h3>
          <img src='image_size.png' width='600' style='max-width: 100%; height: auto;'>
        </div>''' if has_image_size else ''}
        {f'''<div class='card'>
          <h3>Startup Time</h3>
          <img src='startup_time.png' width='600' style='max-width: 100%; height: auto;'>
        </div>''' if has_startup_time else ''}
      </div>
    </div>"""

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
    .row {{ display: flex; gap: 1rem; flex-wrap: wrap; margin-top: 1rem; }}
    .card {{ border: 1px solid #eee; border-radius: 8px; padding: 1rem; background: white; }}
    .card h3 {{ margin-top: 0; font-size: 1rem; }}
    a {{ color: #0066cc; text-decoration: none; }}
    a:hover {{ text-decoration: underline; }}
    .env-table {{ width: 100%; border-collapse: collapse; }}
    .env-table th, .env-table td {{ padding: 0.75rem 1rem; text-align: left; border-bottom: 1px solid #eee; }}
    .env-table th {{ width: 200px; font-weight: 600; background-color: #f9f9f9; }}
  </style>
</head>
<body>
  <h1>{session_title}</h1>
  <p><a href="../index.html">← Back to all sessions</a></p>
  {env_section}
  {container_stats_section}
  <h2 style='margin-bottom: 0;'>Workload Reports</h2>
  {workload_sections}
</body>
</html>
"""
    with open(session_out_dir / "index.html", "w") as f:
        f.write(html)