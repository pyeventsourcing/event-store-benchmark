import json
from pathlib import Path
from typing import Optional

import yaml

from ..environment_info import EnvironmentInfo
from ..data_loader import load_session_metadata


def _format_bytes(byte_count):
    if byte_count is None: return "N/A"
    power = 1024
    n = 0
    power_labels = {0: '', 1: 'K', 2: 'M', 3: 'G', 4: 'T'}
    while byte_count >= power and n < len(power_labels) - 1:
        byte_count /= power
        n += 1
    return f"{byte_count:.1f}{power_labels[n]}B"


def _get_env_summary(env_info: Optional[EnvironmentInfo]) -> str:
    if not env_info:
        return "N/A"

    os_name = env_info.os.name
    cpu_model = env_info.cpu.model
    container_runtime = env_info.container_runtime
    container_str = f"{container_runtime.runtime_type} {container_runtime.ncpu} CPU {_format_bytes(container_runtime.mem_total)}"

    return f"{os_name} {cpu_model}, {container_str}"


def _render_environment_info(env_info: Optional[EnvironmentInfo]) -> str:
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
                    <td>{env_info.container_runtime.runtime_type} {env_info.container_runtime.version} ({env_info.container_runtime.ncpu} vCPUs, {_format_bytes(env_info.container_runtime.mem_total)} Memory)</td>
                </tr>
            </table>
        </div>
    </div>
    """


def generate_run_html(report_dir: Path, run):
    """Generates an HTML report for a single run."""
    workload_name = run.name
    latency_img = "latency_cdf.png"
    throughput_img = "throughput.png"
    cpu_img = "cpu.png"
    memory_img = "memory.png"

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

    resource_metrics_html = f"""
  <div class='row'>
    <div class='card'>
      <h2>Process Resource Metrics</h2>
      <p><b>CPU (avg/peak):</b> {cpu_display}</p>
      <p><b>Memory (avg/peak):</b> {mem_display}</p>
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

    html = f"""
<!DOCTYPE html>
<html>
<head>
  <meta charset='utf-8'>
  <title>Workload Report — {run.adapter} / {workload_name}</title>
  <style>
    body {{ font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif; margin: 2rem; }}
    h1, h2 {{ margin-top: 1.2rem; }}
    .row {{ display: flex; gap: 1rem; flex-wrap: wrap; margin-bottom: 1rem; }}
    .card {{ border: 1px solid #eee; border-radius: 8px; padding: 1rem; }}
  </style>
</head>
<body>
  <h1>Benchmark Report</h1>
  <p><b>Adapter:</b> {run.adapter} &nbsp; | &nbsp; <b>Workload:</b> {workload_name}</p>
  <p><b>Duration:</b> {run.duration_s:.1f}s &nbsp; | &nbsp; <b>Throughput:</b> {run.average_throughput:.0f} eps</p>
  <div class='row'>
    <div class='card'>
      <h2>Latency CDF</h2>
      <img src='{latency_img}' width='560'>
    </div>
    <div class='card'>
      <h2>Throughput over time</h2>
      <img src='{throughput_img}' width='560'>
    </div>
  </div>
  <div class='row'>
    <div class='card'>
      <h2>CPU Usage over time</h2>
      <img src='{cpu_img}' width='560'>
    </div>
    <div class='card'>
      <h2>Memory Usage over time</h2>
      <img src='{memory_img}' width='560'>
    </div>
  </div>
  {resource_metrics_html}
  {container_stats_html}
  {logs_html}
</body>
</html>
"""
    with open(report_dir / "index.html", "w") as f:
        f.write(html)


def generate_workload_html(out_base: Path, workload_name: str, runs, worker_groups, workload_config=None,
                           get_store_rank=None):
    """Generate a consolidated report for a specific workload."""

    def row_key(r):
        rank = get_store_rank(r.adapter) if get_store_rank else 0
        return r.worker_count, rank, r.adapter

    first_run = runs[0] if runs else None
    is_readers = first_run.is_read_workload if first_run else False
    worker_label = "Readers" if is_readers else "Writers"
    worker_suffix = "r" if is_readers else "w"

    summary_rows = ""
    has_container_stats = False
    for run in sorted(runs, key=row_key):
        report_link = f"report-{run.adapter}-r{run.readers:03d}-w{run.writers:03d}/index.html"

        metrics = run.metrics
        
        if metrics.get('startup_time_s') or metrics.get("image_size_bytes"):
            has_container_stats = True

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

        summary_rows += f"""
      <tr>
        <td><a href='{report_link}'>{run.adapter}</a></td>
        <td>{run.worker_count}</td>
        <td>{run.average_throughput:.0f}</td>
        <td>{run.get_latency_percentile(50.0):.2f}</td>
        <td>{run.get_latency_percentile(99.0):.2f}</td>
        <td>{run.get_latency_percentile(99.9):.2f}</td>
        <td>{cpu_display}</td>
        <td>{mem_display}</td>
      </tr>"""

    comparison_sections = ""
    for wc in sorted(worker_groups.keys()):
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
    </div>
    <div class='row'>
      <div class='card'>
        <h3>CPU Usage over time</h3>
        <img src='{workload_name}_comparison_{worker_suffix}{wc}_cpu.png' width='560'>
      </div>
      <div class='card'>
        <h3>Memory Usage over time</h3>
        <img src='{workload_name}_comparison_{worker_suffix}{wc}_memory.png' width='560'>
      </div>
    </div>"""

    performance_section = f"""
    <h2>Performance</h2>
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
        <h3>Average CPU vs {worker_label}</h3>
        <img src='{workload_name}_scaling_avg_cpu.png' width='560'>
      </div>
      <div class='card'>
        <h3>Peak CPU vs {worker_label}</h3>
        <img src='{workload_name}_scaling_peak_cpu.png' width='560'>
      </div>
    </div>
    <div class='row'>
      <div class='card'>
        <h3>Average Memory vs {worker_label}</h3>
        <img src='{workload_name}_scaling_avg_mem.png' width='560'>
      </div>
      <div class='card'>
        <h3>Peak Memory vs {worker_label}</h3>
        <img src='{workload_name}_scaling_peak_mem.png' width='560'>
      </div>
    </div>"""

    container_stats_section = ""
    if has_container_stats:
        container_stats_section = f"""
    <h2>Container Stats</h2>
    <div class='card' style='max-width: 100%;'>
        <img src='{workload_name}_container_stats.png' style='width: 100%; max-width: 1200px;'>
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
    .card {{ border: 1px solid #eee; border-radius: 8px; padding: 1rem; margin-bottom: 1rem; }}
  </style>
</head>
<body>
  <h1>Workload Report — {workload_name}</h1>
  <p><a href="../index.html">← Back to all workloads</a></p>
  {performance_section}
  {container_stats_section}
  {comparison_sections}
  <h2>Summary</h2>
  <table>
    <tr><th>Adapter</th><th>{worker_label}</th><th>Throughput (eps)</th><th>p50 (ms)</th><th>p99 (ms)</th><th>p99.9 (ms)</th><th>CPU (avg/peak)</th><th>Mem MB (avg/peak)</th></tr>
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
    sessions_summaries = {}
    published_session_ids = sorted([d.name for d in published_base.iterdir() if d.is_dir()])

    for session_id in published_session_ids:
        raw_session_dir = raw_base / session_id
        if not raw_session_dir.exists():
            continue

        try:
            metadata = load_session_metadata(raw_session_dir)
            session_info = metadata["session_info"]
            env_info = metadata["env_info"]
            session_configs = metadata["session_configs"]

            config_file = session_info.get('config_file', 'N/A')
            workload_name = Path(config_file).stem if config_file != 'N/A' else 'N/A'

            all_stores = set()
            for cfg in session_configs:
                perf_cfg = cfg.get('performance', cfg)
                if 'stores' in perf_cfg:
                    stores = perf_cfg['stores']
                    if isinstance(stores, list):
                        all_stores.update(stores)
                    elif isinstance(stores, str):
                        all_stores.add(stores)

            sessions_summaries[session_id] = {
                'workload_name': workload_name,
                'benchmark_version': session_info.get('benchmark_version', 'N/A'),
                'stores': list(all_stores),
                'env_summary': _get_env_summary(env_info),
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
    <tr><th>Session ID</th><th>Workload</th><th>Stores</th><th>Environment</th><th>Version</th></tr>
    {session_rows}
  </table>
</body>
</html>
"""
    with open(published_base / "index.html", "w") as f:
        f.write(html)


def generate_session_index(session_out_dir: Path, session_id: str, workload_summaries, env_info: Optional[EnvironmentInfo] = None,
                           session_info=None):
    """Generate index.html for a specific session."""
    env_section = _render_environment_info(env_info)

    workload_sections = ""
    for workload_name, summary in sorted(workload_summaries.items()):
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
  <h2>Workload Reports</h2>
  {workload_sections}
</body>
</html>
"""
    with open(session_out_dir / "index.html", "w") as f:
        f.write(html)