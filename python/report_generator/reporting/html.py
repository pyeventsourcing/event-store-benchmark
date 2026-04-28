from pathlib import Path
from typing import Any, Dict, Optional

from ..models import EnvironmentInfo
from ..data_loader import load_session_metadata, SessionMetadata


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


def generate_top_level_index(raw_base: Path, published_base: Path) -> None:
    """Generate top-level index.html that links to individual session reports."""
    sessions_summaries = {}
    published_session_ids = sorted([d.name for d in published_base.iterdir() if d.is_dir() and d.name.startswith("esb-")])

    for session_id in published_session_ids:
        raw_session_dir = raw_base / session_id
        if not raw_session_dir.exists():
            continue

        try:
            session_metadata = load_session_metadata(raw_session_dir)
            if session_metadata is None:
                print(f"Warning: Could not collect summary for session {session_id} from raw data: missing metadata")
                continue
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


def generate_session_index(
    session_out_dir: Path,
    workload_summaries: Dict[str, Any],
    session_metadata: SessionMetadata,
    has_image_size: Optional[bool] = None,
    has_startup_time: Optional[bool] = None,
) -> None:
    """Generate index.html for a specific session."""
    env_section = _render_environment_info(session_metadata.environment_info)

    workload_sections = ""
    for workload_name, summary in workload_summaries.items():
        by_workers_plots = f"""
      <div class='row'>
        <div class='card'>
          <h3>Throughput</h3>
          <img src='{workload_name}/report/by_workers_throughput.png' width='600' style='max-width: 100%; height: auto;'>
        </div>
        <div class='card'>
          <h3>Latency</h3>
          <img src='{workload_name}/report/by_workers_latency.png' width='600' style='max-width: 100%; height: auto;'>
        </div>
      </div>"""

        workload_sections += f"""
    <div class='workload-section'>
      <h2><a href='{workload_name}/index.html'>{workload_name}</a></h2>
      {by_workers_plots}
    </div>"""

    session_title = f"Benchmark Session: {session_metadata.session_info.session_id}"

    container_stats_section = ""
    if has_image_size is None:
        has_image_size = (session_out_dir / "report" / "image_size.png").exists()
    if has_startup_time is None:
        has_startup_time = (session_out_dir / "report" / "startup_time.png").exists()
    
    if has_image_size or has_startup_time:
        container_stats_section = f"""
    <div class='workload-section'>
      <h2>Container Stats</h2>
      <div class='row'>
        {f'''<div class='card'>
          <h3>Image Size</h3>
          <img src='report/image_size.png' width='600' style='max-width: 100%; height: auto;'>
        </div>''' if has_image_size else ''}
        {f'''<div class='card'>
          <h3>Startup Time</h3>
          <img src='report/startup_time.png' width='600' style='max-width: 100%; height: auto;'>
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