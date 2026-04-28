from collections.abc import Callable
from pathlib import Path

from .. import plotting
from .models import (
    ImageRef,
    PerformanceSessionReport,
    PerformanceWorkloadReport,
    RunImageKey,
    ScalingImageKey,
    SessionImageKey,
    WorkerSliceImageKey,
)


def generate_session_images(report: PerformanceSessionReport) -> None:
    all_runs = []
    for workload in report.workloads:
        all_runs.extend(run_report.run for run_report in workload.runs)
        generate_workload_images(workload)

    session_out_dir = Path(report.session_out_dir)
    (session_out_dir / "report").mkdir(parents=True, exist_ok=True)
    _generate_session_plot(
        all_runs,
        session_out_dir,
        report.session_images[SessionImageKey.IMAGE_SIZE],
        lambda out_path: plotting.plot_image_size(all_runs, out_path, _get_store_rank_from_runs(all_runs)),
    )
    _generate_session_plot(
        all_runs,
        session_out_dir,
        report.session_images[SessionImageKey.STARTUP_TIME],
        lambda out_path: plotting.plot_startup_time(all_runs, out_path, _get_store_rank_from_runs(all_runs)),
    )


def generate_workload_images(workload: PerformanceWorkloadReport) -> None:
    workload_dir = Path(workload.workload_out_dir)
    workload_dir.mkdir(parents=True, exist_ok=True)
    (workload_dir / "report").mkdir(parents=True, exist_ok=True)
    get_store_rank = _make_get_store_rank(workload.store_order)

    for run_report in workload.runs:
        run_dir = workload_dir / run_report.report_dir_name
        run_dir.mkdir(parents=True, exist_ok=True)
        (run_dir / "report").mkdir(parents=True, exist_ok=True)
        run = run_report.run

        _generate_image(
            run_dir,
            run_report.images[RunImageKey.LATENCY_CDF],
            lambda out_path: plotting.plot_latency_cdf(run, out_path),
        )
        _generate_image(
            run_dir,
            run_report.images[RunImageKey.THROUGHPUT_TS],
            lambda out_path: plotting.plot_throughput_timeseries(run, out_path),
        )
        _generate_image(
            run_dir,
            run_report.images[RunImageKey.OPERATION_ERRORS_TS],
            lambda out_path: plotting.plot_operation_errors_timeseries(run, out_path),
        )
        _generate_image(
            run_dir,
            run_report.images[RunImageKey.CPU_TS],
            lambda out_path: plotting.plot_cpu_timeseries(run, out_path),
        )
        _generate_image(
            run_dir,
            run_report.images[RunImageKey.MEMORY_TS],
            lambda out_path: plotting.plot_memory_timeseries(run, out_path),
        )
        _generate_image(
            run_dir,
            run_report.images[RunImageKey.TOOL_LATENCY_CDF],
            lambda out_path: plotting.plot_tool_latency_cdf(run, out_path),
        )
        _generate_image(
            run_dir,
            run_report.images[RunImageKey.TOOL_CPU_TS],
            lambda out_path: plotting.plot_tool_cpu_timeseries(run, out_path),
        )
        _generate_image(
            run_dir,
            run_report.images[RunImageKey.TOOL_MEMORY_TS],
            lambda out_path: plotting.plot_tool_memory_timeseries(run, out_path),
        )

    for worker_slice in workload.worker_slices:
        title = f"{worker_slice.worker_count} {worker_slice.title_suffix}"
        runs = worker_slice.group_runs

        _generate_image(
            workload_dir,
            worker_slice.images[WorkerSliceImageKey.LATENCY_CDF],
            lambda out_path: plotting.plot_worker_slice_latency_cdf(
                runs,
                f"Latency CDF — {title}",
                out_path,
                get_store_rank,
            ),
        )
        _generate_image(
            workload_dir,
            worker_slice.images[WorkerSliceImageKey.THROUGHPUT],
            lambda out_path: plotting.plot_worker_slice_throughput(
                runs,
                f"Throughput — {title}",
                out_path,
                get_store_rank,
            ),
        )
        _generate_image(
            workload_dir,
            worker_slice.images[WorkerSliceImageKey.OPERATION_ERRORS],
            lambda out_path: plotting.plot_worker_slice_operation_errors(
                runs,
                f"Operation Errors — {title}",
                out_path,
                get_store_rank,
            ),
        )
        _generate_image(
            workload_dir,
            worker_slice.images[WorkerSliceImageKey.CPU_TS],
            lambda out_path: plotting.plot_worker_slice_cpu(
                runs,
                f"CPU Usage — {title}",
                out_path,
                get_store_rank,
            ),
        )
        _generate_image(
            workload_dir,
            worker_slice.images[WorkerSliceImageKey.MEMORY_TS],
            lambda out_path: plotting.plot_worker_slice_memory(
                runs,
                f"Memory Usage — {title}",
                out_path,
                get_store_rank,
            ),
        )
        _generate_image(
            workload_dir,
            worker_slice.images[WorkerSliceImageKey.TOOL_LATENCY_CDF],
            lambda out_path: plotting.plot_worker_slice_tool_latency_cdf(
                runs,
                f"Tool Latency CDF — {title}",
                out_path,
                get_store_rank,
            ),
        )
        _generate_image(
            workload_dir,
            worker_slice.images[WorkerSliceImageKey.TOOL_CPU_TS],
            lambda out_path: plotting.plot_worker_slice_tool_cpu(
                runs,
                f"Tool CPU Usage — {title}",
                out_path,
                get_store_rank,
            ),
        )
        _generate_image(
            workload_dir,
            worker_slice.images[WorkerSliceImageKey.TOOL_MEMORY_TS],
            lambda out_path: plotting.plot_worker_slice_tool_memory(
                runs,
                f"Tool Memory Usage — {title}",
                out_path,
                get_store_rank,
            ),
        )

    all_runs = [run_report.run for run_report in workload.runs]
    _generate_image(
        workload_dir,
        workload.scaling.images[ScalingImageKey.THROUGHPUT_BY_WORKERS],
        lambda out_path: plotting.plot_throughput_by_workers(all_runs, out_path, get_store_rank),
    )
    _generate_image(
        workload_dir,
        workload.scaling.images[ScalingImageKey.OPERATION_ERRORS_BY_WORKERS],
        lambda out_path: plotting.plot_operation_errors_by_workers(all_runs, out_path, get_store_rank),
    )
    _generate_image(
        workload_dir,
        workload.scaling.images[ScalingImageKey.LATENCY_BY_WORKERS],
        lambda out_path: plotting.plot_latency_by_workers(all_runs, out_path, get_store_rank),
    )
    _generate_image(
        workload_dir,
        workload.scaling.images[ScalingImageKey.CPU_BY_WORKERS],
        lambda out_path: plotting.plot_cpu_by_workers(all_runs, out_path, get_store_rank),
    )
    _generate_image(
        workload_dir,
        workload.scaling.images[ScalingImageKey.MEMORY_BY_WORKERS],
        lambda out_path: plotting.plot_memory_by_workers(all_runs, out_path, get_store_rank),
    )
    _generate_image(
        workload_dir,
        workload.scaling.images[ScalingImageKey.TOOL_LATENCY_BY_WORKERS],
        lambda out_path: plotting.plot_tool_latency_by_workers(all_runs, out_path, get_store_rank),
    )
    _generate_image(
        workload_dir,
        workload.scaling.images[ScalingImageKey.TOOL_CPU_BY_WORKERS],
        lambda out_path: plotting.plot_tool_cpu_by_workers(all_runs, out_path, get_store_rank),
    )
    _generate_image(
        workload_dir,
        workload.scaling.images[ScalingImageKey.TOOL_MEMORY_BY_WORKERS],
        lambda out_path: plotting.plot_tool_memory_by_workers(all_runs, out_path, get_store_rank),
    )


def _generate_session_plot(
    runs: list,
    base_dir: Path,
    image: ImageRef,
    generator: Callable[[str], None],
) -> None:
    if not runs:
        image.include_in_html = False
        image.reason_not_included = "No runs in session"
        return
    _generate_image(base_dir, image, generator)


def _generate_image(base_dir: Path, image: ImageRef, generator: Callable[[str], None]) -> None:
    if not image.include_in_html:
        image.reason_not_included = "Excluded by policy"
        return

    out_path = base_dir / image.relative_path
    if out_path.exists():
        out_path.unlink()

    generator(str(out_path))

    image.produced = out_path.exists()
    if not image.produced:
        image.include_in_html = False
        image.reason_not_included = "Plot not produced by data"


def _make_get_store_rank(store_order: list[str]) -> Callable[[str], int]:
    rank_map = {name: i for i, name in enumerate(store_order)}
    return lambda name: rank_map.get(name, 999)


def _get_store_rank_from_runs(runs: list) -> Callable[[str], int]:
    seen = []
    for run in runs:
        if run.adapter not in seen:
            seen.append(run.adapter)
    rank_map = {name: i for i, name in enumerate(seen)}
    return lambda name: rank_map.get(name, 999)
