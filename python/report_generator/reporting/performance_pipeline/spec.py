from collections.abc import Callable

from ...workloads.performance import PerformanceWorkloadRun
from .models import (
    ImageRef,
    RunImageKey,
    RunSections,
    ScalingImageKey,
    ScalingSections,
    SessionImageKey,
    WorkerAxis,
    WorkerSliceImageKey,
    WorkerSliceSections,
)


def build_run_dir_name(run: PerformanceWorkloadRun) -> str:
    report_dir_name = run.adapter
    if run.readers > 0:
        report_dir_name += f"-r{run.readers}"
    if run.writers > 0:
        report_dir_name += f"-w{run.writers}"
    return report_dir_name


def worker_axis_from_runs(runs: list[PerformanceWorkloadRun]) -> WorkerAxis:
    first_run = runs[0] if runs else None
    if first_run and first_run.is_read_workload:
        return WorkerAxis.READERS
    return WorkerAxis.WRITERS


def worker_labels(worker_axis: WorkerAxis) -> tuple[str, str, str]:
    if worker_axis == WorkerAxis.READERS:
        return "Reader", "Readers", "r"
    return "Writer", "Writers", "w"


def run_image_relpath(key: RunImageKey) -> str:
    return {
        RunImageKey.LATENCY_CDF: "report/latency_cdf.png",
        RunImageKey.THROUGHPUT_TS: "report/throughput_timeseries.png",
        RunImageKey.OPERATION_ERRORS_TS: "report/operation_errors_timeseries.png",
        RunImageKey.CPU_TS: "report/cpu_timeseries.png",
        RunImageKey.MEMORY_TS: "report/memory_timeseries.png",
        RunImageKey.TOOL_LATENCY_CDF: "report/tool_latency_cdf.png",
        RunImageKey.TOOL_CPU_TS: "report/tool_cpu_timeseries.png",
        RunImageKey.TOOL_MEMORY_TS: "report/tool_memory_timeseries.png",
    }[key]


def worker_slice_image_relpath(worker_suffix: str, worker_count: int, key: WorkerSliceImageKey) -> str:
    prefix = f"worker_slice_{worker_suffix}{worker_count}"
    return {
        WorkerSliceImageKey.THROUGHPUT: f"report/{prefix}_throughput.png",
        WorkerSliceImageKey.OPERATION_ERRORS: f"report/{prefix}_operation_errors.png",
        WorkerSliceImageKey.LATENCY_CDF: f"report/{prefix}_latency_cdf.png",
        WorkerSliceImageKey.CPU_TS: f"report/{prefix}_cpu_timeseries.png",
        WorkerSliceImageKey.MEMORY_TS: f"report/{prefix}_memory_timeseries.png",
        WorkerSliceImageKey.TOOL_LATENCY_CDF: f"report/{prefix}_tool_latency_cdf.png",
        WorkerSliceImageKey.TOOL_CPU_TS: f"report/{prefix}_tool_cpu_timeseries.png",
        WorkerSliceImageKey.TOOL_MEMORY_TS: f"report/{prefix}_tool_memory_timeseries.png",
    }[key]


def scaling_image_relpath(key: ScalingImageKey) -> str:
    return {
        ScalingImageKey.THROUGHPUT_BY_WORKERS: "report/by_workers_throughput.png",
        ScalingImageKey.OPERATION_ERRORS_BY_WORKERS: "report/by_workers_operation_errors.png",
        ScalingImageKey.LATENCY_BY_WORKERS: "report/by_workers_latency.png",
        ScalingImageKey.CPU_BY_WORKERS: "report/by_workers_cpu.png",
        ScalingImageKey.MEMORY_BY_WORKERS: "report/by_workers_memory.png",
        ScalingImageKey.TOOL_LATENCY_BY_WORKERS: "report/by_workers_tool_latency.png",
        ScalingImageKey.TOOL_CPU_BY_WORKERS: "report/by_workers_tool_cpu.png",
        ScalingImageKey.TOOL_MEMORY_BY_WORKERS: "report/by_workers_tool_memory.png",
    }[key]


def session_image_relpath(key: SessionImageKey) -> str:
    return {
        SessionImageKey.CONTAINER_STATS_SUMMARY: "report/container_stats_summary.png",
        SessionImageKey.SELECTED_SLICE_SUMMARY_BY_WORKLOAD: "report/selected_slice_summary_by_workload.png",
    }[key]


def run_sections_from_run(run: PerformanceWorkloadRun) -> RunSections:
    metrics = run.metrics
    avg_cpu = metrics.get("avg_cpu_percent")
    avg_mem = metrics.get("avg_memory_bytes")
    tool_avg_cpu = metrics.get("tool_avg_cpu_percent")
    tool_avg_mem = metrics.get("tool_avg_memory_bytes")
    has_container_stats = bool(metrics.get("startup_time_s") or metrics.get("image_size_bytes"))
    return RunSections(
        show_store_resources=avg_cpu is not None or avg_mem is not None,
        show_tool_resources=tool_avg_cpu is not None or tool_avg_mem is not None,
        show_container_stats=has_container_stats,
        show_logs=bool(run.logs),
        show_store_cpu_plot=not run.cpu_df.empty,
        show_store_memory_plot=not run.memory_df.empty,
        show_tool_latency_plot=len(run.tool_latency_percentiles) > 0,
        show_tool_cpu_plot=not run.tool_cpu_df.empty,
        show_tool_memory_plot=not run.tool_memory_df.empty,
    )


def worker_slice_sections_from_runs(group_runs: list[PerformanceWorkloadRun]) -> WorkerSliceSections:
    return WorkerSliceSections(
        show_cpu_plot=any(not r.cpu_df.empty for r in group_runs),
        show_memory_plot=any(not r.memory_df.empty for r in group_runs),
        show_tool_latency_plot=any(len(r.tool_latency_percentiles) > 0 for r in group_runs),
        show_tool_cpu_plot=any(not r.tool_cpu_df.empty for r in group_runs),
        show_tool_memory_plot=any(not r.tool_memory_df.empty for r in group_runs),
    )


def scaling_sections_from_runs(runs: list[PerformanceWorkloadRun]) -> ScalingSections:
    return ScalingSections(
        show_cpu_plot=any(not r.cpu_df.empty for r in runs),
        show_memory_plot=any(not r.memory_df.empty for r in runs),
        show_tool_latency_plot=any(len(r.tool_latency_percentiles) > 0 for r in runs),
        show_tool_cpu_plot=any(not r.tool_cpu_df.empty for r in runs),
        show_tool_memory_plot=any(not r.tool_memory_df.empty for r in runs),
    )


def sort_runs(
    runs: list[PerformanceWorkloadRun],
    get_store_rank: Callable[[str], int] | None,
) -> list[PerformanceWorkloadRun]:
    def row_key(run: PerformanceWorkloadRun) -> tuple[int, int, str]:
        rank = get_store_rank(run.adapter) if get_store_rank else 0
        return run.worker_count, rank, run.adapter

    return sorted(runs, key=row_key)


def build_run_images(sections: RunSections) -> dict[RunImageKey, ImageRef]:
    return {
        RunImageKey.LATENCY_CDF: ImageRef(run_image_relpath(RunImageKey.LATENCY_CDF), "Latency"),
        RunImageKey.THROUGHPUT_TS: ImageRef(run_image_relpath(RunImageKey.THROUGHPUT_TS), "Throughput"),
        RunImageKey.OPERATION_ERRORS_TS: ImageRef(
            run_image_relpath(RunImageKey.OPERATION_ERRORS_TS),
            "Operation Errors",
        ),
        RunImageKey.CPU_TS: ImageRef(run_image_relpath(RunImageKey.CPU_TS), "CPU", include_in_html=sections.show_store_cpu_plot),
        RunImageKey.MEMORY_TS: ImageRef(
            run_image_relpath(RunImageKey.MEMORY_TS), "Memory", include_in_html=sections.show_store_memory_plot
        ),
        RunImageKey.TOOL_LATENCY_CDF: ImageRef(
            run_image_relpath(RunImageKey.TOOL_LATENCY_CDF),
            "Tool Latency",
            include_in_html=sections.show_tool_latency_plot,
        ),
        RunImageKey.TOOL_CPU_TS: ImageRef(
            run_image_relpath(RunImageKey.TOOL_CPU_TS), "Tool CPU", include_in_html=sections.show_tool_cpu_plot
        ),
        RunImageKey.TOOL_MEMORY_TS: ImageRef(
            run_image_relpath(RunImageKey.TOOL_MEMORY_TS),
            "Tool Memory",
            include_in_html=sections.show_tool_memory_plot,
        ),
    }


def build_worker_slice_images(
    worker_suffix: str,
    worker_count: int,
    sections: WorkerSliceSections,
) -> dict[WorkerSliceImageKey, ImageRef]:
    return {
        WorkerSliceImageKey.THROUGHPUT: ImageRef(
            worker_slice_image_relpath(worker_suffix, worker_count, WorkerSliceImageKey.THROUGHPUT), "Throughput"
        ),
        WorkerSliceImageKey.OPERATION_ERRORS: ImageRef(
            worker_slice_image_relpath(worker_suffix, worker_count, WorkerSliceImageKey.OPERATION_ERRORS),
            "Operation Errors",
        ),
        WorkerSliceImageKey.LATENCY_CDF: ImageRef(
            worker_slice_image_relpath(worker_suffix, worker_count, WorkerSliceImageKey.LATENCY_CDF), "Latency"
        ),
        WorkerSliceImageKey.CPU_TS: ImageRef(
            worker_slice_image_relpath(worker_suffix, worker_count, WorkerSliceImageKey.CPU_TS),
            "CPU",
            include_in_html=sections.show_cpu_plot,
        ),
        WorkerSliceImageKey.MEMORY_TS: ImageRef(
            worker_slice_image_relpath(worker_suffix, worker_count, WorkerSliceImageKey.MEMORY_TS),
            "Memory",
            include_in_html=sections.show_memory_plot,
        ),
        WorkerSliceImageKey.TOOL_LATENCY_CDF: ImageRef(
            worker_slice_image_relpath(worker_suffix, worker_count, WorkerSliceImageKey.TOOL_LATENCY_CDF),
            "Tool Latency",
            include_in_html=sections.show_tool_latency_plot,
        ),
        WorkerSliceImageKey.TOOL_CPU_TS: ImageRef(
            worker_slice_image_relpath(worker_suffix, worker_count, WorkerSliceImageKey.TOOL_CPU_TS),
            "Tool CPU",
            include_in_html=sections.show_tool_cpu_plot,
        ),
        WorkerSliceImageKey.TOOL_MEMORY_TS: ImageRef(
            worker_slice_image_relpath(worker_suffix, worker_count, WorkerSliceImageKey.TOOL_MEMORY_TS),
            "Tool Memory",
            include_in_html=sections.show_tool_memory_plot,
        ),
    }


def build_scaling_images(sections: ScalingSections) -> dict[ScalingImageKey, ImageRef]:
    return {
        ScalingImageKey.THROUGHPUT_BY_WORKERS: ImageRef(
            scaling_image_relpath(ScalingImageKey.THROUGHPUT_BY_WORKERS), "Throughput"
        ),
        ScalingImageKey.OPERATION_ERRORS_BY_WORKERS: ImageRef(
            scaling_image_relpath(ScalingImageKey.OPERATION_ERRORS_BY_WORKERS),
            "Operation Errors",
        ),
        ScalingImageKey.LATENCY_BY_WORKERS: ImageRef(scaling_image_relpath(ScalingImageKey.LATENCY_BY_WORKERS), "Latency"),
        ScalingImageKey.CPU_BY_WORKERS: ImageRef(
            scaling_image_relpath(ScalingImageKey.CPU_BY_WORKERS), "CPU", include_in_html=sections.show_cpu_plot
        ),
        ScalingImageKey.MEMORY_BY_WORKERS: ImageRef(
            scaling_image_relpath(ScalingImageKey.MEMORY_BY_WORKERS),
            "Memory",
            include_in_html=sections.show_memory_plot,
        ),
        ScalingImageKey.TOOL_LATENCY_BY_WORKERS: ImageRef(
            scaling_image_relpath(ScalingImageKey.TOOL_LATENCY_BY_WORKERS),
            "Tool Latency",
            include_in_html=sections.show_tool_latency_plot,
        ),
        ScalingImageKey.TOOL_CPU_BY_WORKERS: ImageRef(
            scaling_image_relpath(ScalingImageKey.TOOL_CPU_BY_WORKERS),
            "Tool CPU",
            include_in_html=sections.show_tool_cpu_plot,
        ),
        ScalingImageKey.TOOL_MEMORY_BY_WORKERS: ImageRef(
            scaling_image_relpath(ScalingImageKey.TOOL_MEMORY_BY_WORKERS),
            "Tool Memory",
            include_in_html=sections.show_tool_memory_plot,
        ),
    }


def build_session_images() -> dict[SessionImageKey, ImageRef]:
    return {
        SessionImageKey.CONTAINER_STATS_SUMMARY: ImageRef(
            session_image_relpath(SessionImageKey.CONTAINER_STATS_SUMMARY),
            "Container Stats Summary",
        ),
        SessionImageKey.SELECTED_SLICE_SUMMARY_BY_WORKLOAD: ImageRef(
            session_image_relpath(SessionImageKey.SELECTED_SLICE_SUMMARY_BY_WORKLOAD),
            "Selected Slice Performance Summary by Workload",
        ),
    }
