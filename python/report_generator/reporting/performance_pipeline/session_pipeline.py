from pathlib import Path

from ...data_loader import SessionMetadata
from ...models import PerformanceWorkloadConfig
from ...workloads.performance import PerformanceWorkloadRun
from .. import html
from ..plotting import get_selected_worker_count_for_session_summary
from .builder import build_session_report
from .image_generation import generate_session_images
from .render_html import get_session_plot_availability, write_workload_reports


def generate_performance_session_reports(
    session_id: str,
    raw_session_dir: Path,
    published_session_dir: Path,
    session_metadata: SessionMetadata,
    session_workloads: list[tuple[str, PerformanceWorkloadConfig, list[PerformanceWorkloadRun]]],
) -> None:
    _ = raw_session_dir
    session_report = build_session_report(session_id, published_session_dir, session_workloads)
    generate_session_images(session_report)
    workload_summaries = write_workload_reports(session_report)
    has_container_stats_summary, has_selected_slice_summary = get_session_plot_availability(session_report)
    selected_worker_count = get_selected_worker_count_for_session_summary(session_report.workloads)
    html.generate_session_index(
        published_session_dir,
        workload_summaries,
        session_metadata,
        has_container_stats_summary=has_container_stats_summary,
        has_selected_slice_summary=has_selected_slice_summary,
        selected_worker_count=selected_worker_count,
    )
