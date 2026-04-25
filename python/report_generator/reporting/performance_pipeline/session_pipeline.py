from pathlib import Path

from ...data_loader import SessionMetadata
from ...models import PerformanceWorkloadConfig
from ...workloads.performance import PerformanceWorkloadRun
from .. import html
from .builder import build_session_report
from .image_generation import generate_session_images
from .render_html import get_session_container_plot_availability, write_workload_reports


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
    has_image_size, has_startup_time = get_session_container_plot_availability(session_report)
    html.generate_session_index(
        published_session_dir,
        workload_summaries,
        session_metadata,
        has_image_size=has_image_size,
        has_startup_time=has_startup_time,
    )
