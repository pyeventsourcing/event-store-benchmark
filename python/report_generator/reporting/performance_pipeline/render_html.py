from pathlib import Path

from .models import (
    PerformanceSessionReport,
    PerformanceWorkloadReport,
    RunImageKey,
    RunReport,
    ScalingImageKey,
    SessionImageKey,
    WorkerSliceImageKey,
)


def write_workload_reports(report: PerformanceSessionReport) -> dict[str, dict[str, set[int]]]:
    workload_summaries: dict[str, dict[str, set[int]]] = {}
    for workload in report.workloads:
        workload_dir = Path(workload.workload_out_dir)
        workload_dir.mkdir(parents=True, exist_ok=True)

        for run_report in workload.runs:
            run_dir = workload_dir / run_report.report_dir_name
            run_dir.mkdir(parents=True, exist_ok=True)
            (run_dir / "index.html").write_text(render_run_html(run_report), encoding="utf-8")

        (workload_dir / "index.html").write_text(render_workload_html(workload), encoding="utf-8")
        workload_summaries[workload.workload_name] = {
            "worker_counts": {ws.worker_count for ws in workload.worker_slices}
        }

    return workload_summaries


def get_session_plot_availability(report: PerformanceSessionReport) -> tuple[bool, bool]:
    has_container_stats_summary = report.session_images[SessionImageKey.CONTAINER_STATS_SUMMARY].include_in_html
    has_selected_slice_summary = report.session_images[
        SessionImageKey.SELECTED_SLICE_SUMMARY_BY_WORKLOAD
    ].include_in_html
    return has_container_stats_summary, has_selected_slice_summary


def render_run_html(run_report: RunReport) -> str:
    run = run_report.run
    images = run_report.images
    sections = run_report.sections

    metrics = run.metrics
    has_container_stats = sections.show_container_stats

    container_stats_html = ""
    if has_container_stats:
        startup_time = f"{metrics.get('startup_time_s', 0):.2f}s" if metrics.get("startup_time_s") else "N/A"
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
    if sections.show_store_resources:
        resource_metrics_html = f"""
  <div class='row'>
    <div class='card'>
      <h2>Store Process Resource Metrics</h2>
      <p><b>CPU (avg/peak):</b> {cpu_display}</p>
      <p><b>Memory (avg/peak):</b> {mem_display}</p>
    </div>
  </div>"""

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
    if sections.show_tool_resources:
        tool_resource_metrics_html = f"""
  <div class='row'>
    <div class='card'>
      <h2>Benchmark Process Resource Metrics</h2>
      <p><b>CPU (avg/peak):</b> {b_cpu_display}</p>
      <p><b>Memory (avg/peak):</b> {b_mem_display}</p>
    </div>
  </div>"""

    logs_html = ""
    if sections.show_logs:
        logs_html = f"""
  <div class='row'>
    <div class='card' style='width: 100%;'>
      <h2>Container Logs</h2>
      <pre style='background: #f8f8f8; padding: 1rem; border-radius: 4px; overflow-x: auto; font-size: 0.85rem; max-height: 500px; overflow-y: auto;'>{run.logs}</pre>
    </div>
  </div>"""

    cpu_plot_html = ""
    if images[RunImageKey.CPU_TS].include_in_html:
        cpu_plot_html = f"""
    <div class='card'>
      <h2>CPU</h2>
      <img src='{images[RunImageKey.CPU_TS].relative_path}' width='600' style='max-width: 100%; height: auto;'>
    </div>"""

    throughput_plot_html = ""
    if images[RunImageKey.THROUGHPUT_TS].include_in_html:
        throughput_plot_html = f"""
    <div class='card'>
      <h2>Throughput</h2>
      <img src='{images[RunImageKey.THROUGHPUT_TS].relative_path}' width='600' style='max-width: 100%; height: auto;'>
    </div>"""

    latency_plot_html = ""
    if images[RunImageKey.LATENCY_CDF].include_in_html:
        latency_plot_html = f"""
    <div class='card'>
      <h2>Latency</h2>
      <img src='{images[RunImageKey.LATENCY_CDF].relative_path}' width='600' style='max-width: 100%; height: auto;'>
    </div>"""

    performance_plots_html = ""
    if throughput_plot_html or latency_plot_html:
        performance_plots_html = f"""
  <div class='row'>
    {throughput_plot_html}
    {latency_plot_html}
  </div>"""

    operation_errors_html = ""
    if images[RunImageKey.OPERATION_ERRORS_TS].include_in_html:
        operation_errors_html = f"""<div class='row'>
    <div class='card'>
    <h2>Operation Errors</h2>
    <img src='{images[RunImageKey.OPERATION_ERRORS_TS].relative_path}' width='600' style='max-width: 100%; height: auto;'>
    </div>
  </div>"""

    memory_plot_html = ""
    if images[RunImageKey.MEMORY_TS].include_in_html:
        memory_plot_html = f"""
    <div class='card'>
      <h2>Memory</h2>
      <img src='{images[RunImageKey.MEMORY_TS].relative_path}' width='600' style='max-width: 100%; height: auto;'>
    </div>"""

    tool_latency_plot_html = (
        f"""<div class='card'>
      <h2>Tool Latency</h2>
      <img src='{images[RunImageKey.TOOL_LATENCY_CDF].relative_path}' width='600' style='max-width: 100%; height: auto;'>
    </div>"""
        if images[RunImageKey.TOOL_LATENCY_CDF].include_in_html
        else ""
    )

    tool_cpu_plot_html = (
        f"""<div class='card'>
      <h2>Tool CPU</h2>
      <img src='{images[RunImageKey.TOOL_CPU_TS].relative_path}' width='600' style='max-width: 100%; height: auto;'>
    </div>"""
        if images[RunImageKey.TOOL_CPU_TS].include_in_html
        else ""
    )

    tool_memory_plot_html = (
        f"""<div class='card'>
      <h2>Tool Memory</h2>
      <img src='{images[RunImageKey.TOOL_MEMORY_TS].relative_path}' width='600' style='max-width: 100%; height: auto;'>
    </div>"""
        if images[RunImageKey.TOOL_MEMORY_TS].include_in_html
        else ""
    )

    tool_plots_html = ""
    if tool_latency_plot_html or tool_cpu_plot_html or tool_memory_plot_html:
        tool_plots_html = f"""
  <div class='row'>
    {tool_cpu_plot_html}
    {tool_memory_plot_html}
  </div>
  <div class='row'>
    {tool_latency_plot_html}
  </div>"""

    return f"""
<!DOCTYPE html>
<html>
<head>
  <meta charset='utf-8'>
  <title>Run Report — {run.name} — {run.adapter}</title>
  <style>
    body {{ font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif; margin: 2rem; }}
    h1, h2, h3 {{ margin-top: 1.2rem; }}
    .row {{ display: flex; gap: 1rem; flex-wrap: wrap; margin-top: 1rem; }}
    .card {{ border: 1px solid #eee; border-radius: 8px; padding: 1rem; }}
  </style>
</head>
<body>
  <h1>Run Report</h1>
  <p><b>Adapter:</b> {run.adapter} &nbsp; | &nbsp; <b>Workload:</b> {run.name}</p>
  <p><a href='../index.html'>← Back to workload report</a></p>
  <p><b>Duration:</b> {run.duration_s:.1f}s &nbsp; | &nbsp; <b>Throughput:</b> {run.average_throughput:.0f} eps &nbsp; | &nbsp; <b>Operation Errors:</b> {run.total_operation_errors:.0f}</p>
  {performance_plots_html}
  {operation_errors_html}
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


def render_workload_html(workload: PerformanceWorkloadReport) -> str:
    summary_rows = ""
    for run_report in workload.runs:
        run = run_report.run
        report_link = f"{run_report.report_dir_name}/index.html"

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

        throughput_display = f"{run.average_throughput:.0f}"
        if run.peak_throughput > 0:
            throughput_display = f"{run.average_throughput:.0f} / {run.peak_throughput:.0f}"
        operation_errors_display = f"{run.total_operation_errors:.0f}"

        summary_rows += f"""
      <tr>
        <td><a href='{report_link}'>{run.adapter}</a></td>
        <td>{run.worker_count}</td>
        <td>{throughput_display}</td>
        <td>{operation_errors_display}</td>
        <td>{run.get_latency_percentile(50.0):.2f}</td>
        <td>{run.get_latency_percentile(99.0):.2f}</td>
        <td>{run.get_latency_percentile(99.9):.2f}</td>
        <td>{cpu_display}</td>
        <td>{mem_display}</td>
      </tr>"""

    worker_slice_sections = ""
    for worker_slice in workload.worker_slices:
        images = worker_slice.images
        cpu_slice_html = (
            f"""
      <div class='card'>
        <h3>CPU</h3>
        <img src='{images[WorkerSliceImageKey.CPU_TS].relative_path}' width='600' style='max-width: 100%; height: auto;'>
      </div>"""
            if images[WorkerSliceImageKey.CPU_TS].include_in_html
            else ""
        )
        mem_slice_html = (
            f"""
      <div class='card'>
        <h3>Memory</h3>
        <img src='{images[WorkerSliceImageKey.MEMORY_TS].relative_path}' width='600' style='max-width: 100%; height: auto;'>
      </div>"""
            if images[WorkerSliceImageKey.MEMORY_TS].include_in_html
            else ""
        )
        tool_latency_slice_html = (
            f"""
      <div class='card'>
        <h3>Tool Latency</h3>
        <img src='{images[WorkerSliceImageKey.TOOL_LATENCY_CDF].relative_path}' width='600' style='max-width: 100%; height: auto;'>
      </div>"""
            if images[WorkerSliceImageKey.TOOL_LATENCY_CDF].include_in_html
            else ""
        )
        tool_cpu_slice_html = (
            f"""
      <div class='card'>
        <h3>Tool CPU</h3>
        <img src='{images[WorkerSliceImageKey.TOOL_CPU_TS].relative_path}' width='600' style='max-width: 100%; height: auto;'>
      </div>"""
            if images[WorkerSliceImageKey.TOOL_CPU_TS].include_in_html
            else ""
        )
        tool_mem_slice_html = (
            f"""
      <div class='card'>
        <h3>Tool Memory</h3>
        <img src='{images[WorkerSliceImageKey.TOOL_MEMORY_TS].relative_path}' width='600' style='max-width: 100%; height: auto;'>
      </div>"""
            if images[WorkerSliceImageKey.TOOL_MEMORY_TS].include_in_html
            else ""
        )

        tool_slice_html = (
            f"""
    <div class='row'>
      {tool_cpu_slice_html}
      {tool_mem_slice_html}
    </div>
    <div class='row'>
      {tool_latency_slice_html}
    </div>"""
            if tool_cpu_slice_html or tool_mem_slice_html or tool_latency_slice_html
            else ""
        )

        throughput_slice_html = (
            f"""
      <div class='card'>
        <h3>Throughput</h3>
        <img src='{images[WorkerSliceImageKey.THROUGHPUT].relative_path}' width='600' style='max-width: 100%; height: auto;'>
      </div>"""
            if images[WorkerSliceImageKey.THROUGHPUT].include_in_html
            else ""
        )
        latency_slice_html = (
            f"""
      <div class='card'>
        <h3>Latency</h3>
        <img src='{images[WorkerSliceImageKey.LATENCY_CDF].relative_path}' width='600' style='max-width: 100%; height: auto;'>
      </div>"""
            if images[WorkerSliceImageKey.LATENCY_CDF].include_in_html
            else ""
        )
        operation_errors_slice_html = (
            f"""<div class='row'>
      <div class='card'>
        <h3>Operation Errors</h3>
        <img src='{images[WorkerSliceImageKey.OPERATION_ERRORS].relative_path}' width='600' style='max-width: 100%; height: auto;'>
      </div>
    </div>"""
            if images[WorkerSliceImageKey.OPERATION_ERRORS].include_in_html
            else ""
        )
        performance_slice_html = (
            f"""
    <div class='row'>
      {throughput_slice_html}
      {latency_slice_html}
    </div>
    {operation_errors_slice_html}"""
            if throughput_slice_html or latency_slice_html or operation_errors_slice_html
            else ""
        )

        worker_slice_sections += f"""
    <h2>{workload.worker_label_plural} = {worker_slice.worker_count}</h2>
    {performance_slice_html}
    <div class='row'>
      {cpu_slice_html}
      {mem_slice_html}
    </div>
    {tool_slice_html}"""

    scaling = workload.scaling.images
    cpu_by_workers_html = (
        f"""
      <div class='card'>
        <h3>CPU</h3>
        <img src='{scaling[ScalingImageKey.CPU_BY_WORKERS].relative_path}' width='600' style='max-width: 100%; height: auto;'>
      </div>"""
        if scaling[ScalingImageKey.CPU_BY_WORKERS].include_in_html
        else ""
    )
    mem_by_workers_html = (
        f"""
      <div class='card'>
        <h3>Memory</h3>
        <img src='{scaling[ScalingImageKey.MEMORY_BY_WORKERS].relative_path}' width='600' style='max-width: 100%; height: auto;'>
      </div>"""
        if scaling[ScalingImageKey.MEMORY_BY_WORKERS].include_in_html
        else ""
    )
    resource_usage_html = (
        f"""
    <div class='row'>
      {cpu_by_workers_html}
      {mem_by_workers_html}
    </div>"""
        if cpu_by_workers_html or mem_by_workers_html
        else ""
    )

    operation_errors_html = f"""
    <div class='row'>
      <div class='card'>
        <h3>Operation Errors</h3>
        <img src='{scaling[ScalingImageKey.OPERATION_ERRORS_BY_WORKERS].relative_path}' width='600' style='max-width: 100%; height: auto;'>
      </div>
    </div>""" if scaling[ScalingImageKey.OPERATION_ERRORS_BY_WORKERS].include_in_html else ""

    throughput_by_workers_html = (
        f"""
      <div class='card'>
        <h3>Throughput</h3>
        <img src='{scaling[ScalingImageKey.THROUGHPUT_BY_WORKERS].relative_path}' width='600' style='max-width: 100%; height: auto;'>
      </div>"""
        if scaling[ScalingImageKey.THROUGHPUT_BY_WORKERS].include_in_html
        else ""
    )
    latency_by_workers_html = (
        f"""
      <div class='card'>
        <h3>Latency</h3>
        <img src='{scaling[ScalingImageKey.LATENCY_BY_WORKERS].relative_path}' width='600' style='max-width: 100%; height: auto;'>
      </div>"""
        if scaling[ScalingImageKey.LATENCY_BY_WORKERS].include_in_html
        else ""
    )
    performance_charts_html = (
        f"""
    <div class='row'>
      {throughput_by_workers_html}
      {latency_by_workers_html}
    </div>"""
        if throughput_by_workers_html or latency_by_workers_html
        else ""
    )

    performance_section = f"""
    <h2>Performance by {workload.worker_label_plural}</h2>
    {performance_charts_html}
    {operation_errors_html}
    {resource_usage_html}"""

    tool_latency_by_workers_html = (
        f"""
      <div class='card'>
        <h3>Tool Latency</h3>
        <img src='{scaling[ScalingImageKey.TOOL_LATENCY_BY_WORKERS].relative_path}' width='600' style='max-width: 100%; height: auto;'>
      </div>"""
        if scaling[ScalingImageKey.TOOL_LATENCY_BY_WORKERS].include_in_html
        else ""
    )
    tool_cpu_by_workers_html = (
        f"""
      <div class='card'>
        <h3>Tool CPU</h3>
        <img src='{scaling[ScalingImageKey.TOOL_CPU_BY_WORKERS].relative_path}' width='600' style='max-width: 100%; height: auto;'>
      </div>"""
        if scaling[ScalingImageKey.TOOL_CPU_BY_WORKERS].include_in_html
        else ""
    )
    tool_mem_by_workers_html = (
        f"""
      <div class='card'>
        <h3>Tool Memory</h3>
        <img src='{scaling[ScalingImageKey.TOOL_MEMORY_BY_WORKERS].relative_path}' width='600' style='max-width: 100%; height: auto;'>
      </div>"""
        if scaling[ScalingImageKey.TOOL_MEMORY_BY_WORKERS].include_in_html
        else ""
    )

    tool_performance_section = ""
    if tool_latency_by_workers_html or tool_cpu_by_workers_html or tool_mem_by_workers_html:
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
      <pre style='background-color: #f8f8f8; padding: 1rem; border-radius: 4px; overflow-x: auto;'>{workload.orig_yaml}</pre>
    </div>"""

    return f"""
<!DOCTYPE html>
<html>
<head>
  <meta charset='utf-8'>
  <title>Workload Report — {workload.workload_name}</title>
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
  <h1>Workload Report — {workload.workload_name}</h1>
  <p><a href="../index.html">← Back to session report</a></p>
  {performance_section}
  {tool_performance_section}
  {worker_slice_sections}
  <h2>Runs</h2>
  <table>
    <tr><th>Adapter</th><th>{workload.worker_label_plural}</th><th>Throughput (eps)</th><th>Operation Errors</th><th>p50 (ms)</th><th>p99 (ms)</th><th>p99.9 (ms)</th><th>CPU (avg/peak)</th><th>Mem MB (avg/peak)</th></tr>
    {summary_rows}
  </table>
  {config_section}
</body>
</html>
"""
