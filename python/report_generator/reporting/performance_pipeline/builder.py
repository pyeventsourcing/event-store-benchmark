from collections import defaultdict
from collections.abc import Callable
from pathlib import Path

from ...models import PerformanceWorkloadConfig
from ...workloads.performance import PerformanceWorkloadRun
from .models import (
    PerformanceSessionReport,
    PerformanceWorkloadReport,
    RunReport,
    ScalingReport,
    WorkerSliceReport,
)
from .spec import (
    build_run_dir_name,
    build_run_images,
    build_scaling_images,
    build_session_images,
    build_worker_slice_images,
    run_sections_from_run,
    scaling_sections_from_runs,
    sort_runs,
    worker_axis_from_runs,
    worker_labels,
    worker_slice_sections_from_runs,
)


def build_session_report(
    session_id: str,
    published_session_dir: Path,
    session_workloads: list[tuple[str, PerformanceWorkloadConfig, list[PerformanceWorkloadRun]]],
) -> PerformanceSessionReport:
    workload_reports: list[PerformanceWorkloadReport] = []

    for orig_yaml, workload_config, workload_runs in session_workloads:
        if not workload_runs:
            continue

        workload_reports.append(
            build_workload_report(
                session_out_dir=published_session_dir,
                workload_name=workload_config.name,
                orig_yaml=orig_yaml,
                runs=workload_runs,
                store_order=_store_order(workload_config),
            )
        )

    return PerformanceSessionReport(
        session_id=session_id,
        session_out_dir=str(published_session_dir),
        workloads=workload_reports,
        session_images=build_session_images(),
    )


def build_workload_report(
    session_out_dir: Path,
    workload_name: str,
    orig_yaml: str,
    runs: list[PerformanceWorkloadRun],
    store_order: list[str],
) -> PerformanceWorkloadReport:
    store_order_map = {name: i for i, name in enumerate(store_order)}
    get_store_rank: Callable[[str], int] = lambda name: store_order_map.get(name, 999)

    worker_axis = worker_axis_from_runs(runs)
    worker_label_singular, worker_label_plural, worker_suffix = worker_labels(worker_axis)

    sorted_runs = sort_runs(runs, get_store_rank)
    run_reports = [_build_run_report(run) for run in sorted_runs]

    worker_groups = defaultdict[int, list[PerformanceWorkloadRun]](list)
    for run in runs:
        worker_groups[run.worker_count].append(run)

    worker_slices: list[WorkerSliceReport] = []
    for worker_count, group_runs in sorted(worker_groups.items()):
        sections = worker_slice_sections_from_runs(group_runs)
        title_suffix = worker_label_singular if worker_count == 1 else worker_label_plural
        worker_slices.append(
            WorkerSliceReport(
                worker_count=worker_count,
                title_suffix=title_suffix,
                group_runs=sort_runs(group_runs, get_store_rank),
                sections=sections,
                images=build_worker_slice_images(worker_suffix, worker_count, sections),
            )
        )

    scaling_sections = scaling_sections_from_runs(runs)
    scaling = ScalingReport(
        sections=scaling_sections,
        images=build_scaling_images(scaling_sections),
    )

    return PerformanceWorkloadReport(
        workload_name=workload_name,
        workload_out_dir=str(session_out_dir / workload_name),
        worker_axis=worker_axis,
        worker_label_singular=worker_label_singular,
        worker_label_plural=worker_label_plural,
        worker_suffix=worker_suffix,
        orig_yaml=orig_yaml,
        runs=run_reports,
        worker_slices=worker_slices,
        scaling=scaling,
        store_order=store_order,
    )


def _build_run_report(run: PerformanceWorkloadRun) -> RunReport:
    sections = run_sections_from_run(run)
    return RunReport(
        run=run,
        report_dir_name=build_run_dir_name(run),
        sections=sections,
        images=build_run_images(sections),
    )


def _store_order(workload_config: PerformanceWorkloadConfig) -> list[str]:
    stores = workload_config.stores
    if isinstance(stores, list):
        return stores
    return [stores]
