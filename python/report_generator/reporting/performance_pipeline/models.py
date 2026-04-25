from dataclasses import dataclass, field
from enum import Enum

from ...workloads.performance import PerformanceWorkloadRun


class WorkerAxis(str, Enum):
    READERS = "readers"
    WRITERS = "writers"


class RunImageKey(str, Enum):
    LATENCY_CDF = "latency_cdf"
    THROUGHPUT_TS = "throughput_timeseries"
    CPU_TS = "cpu_timeseries"
    MEMORY_TS = "memory_timeseries"
    TOOL_LATENCY_CDF = "tool_latency_cdf"
    TOOL_CPU_TS = "tool_cpu_timeseries"
    TOOL_MEMORY_TS = "tool_memory_timeseries"


class WorkerSliceImageKey(str, Enum):
    THROUGHPUT = "throughput"
    LATENCY_CDF = "latency_cdf"
    CPU_TS = "cpu_timeseries"
    MEMORY_TS = "memory_timeseries"
    TOOL_LATENCY_CDF = "tool_latency_cdf"
    TOOL_CPU_TS = "tool_cpu_timeseries"
    TOOL_MEMORY_TS = "tool_memory_timeseries"


class ScalingImageKey(str, Enum):
    THROUGHPUT_BY_WORKERS = "throughput_by_workers"
    LATENCY_BY_WORKERS = "latency_by_workers"
    CPU_BY_WORKERS = "cpu_by_workers"
    MEMORY_BY_WORKERS = "memory_by_workers"
    TOOL_LATENCY_BY_WORKERS = "tool_latency_by_workers"
    TOOL_CPU_BY_WORKERS = "tool_cpu_by_workers"
    TOOL_MEMORY_BY_WORKERS = "tool_memory_by_workers"


class SessionImageKey(str, Enum):
    IMAGE_SIZE = "image_size"
    STARTUP_TIME = "startup_time"


@dataclass(slots=True)
class ImageRef:
    relative_path: str
    title: str
    include_in_html: bool = True
    produced: bool = False
    reason_not_included: str | None = None


@dataclass(slots=True)
class RunSections:
    show_store_resources: bool
    show_tool_resources: bool
    show_container_stats: bool
    show_logs: bool
    show_store_cpu_plot: bool
    show_store_memory_plot: bool
    show_tool_latency_plot: bool
    show_tool_cpu_plot: bool
    show_tool_memory_plot: bool


@dataclass(slots=True)
class WorkerSliceSections:
    show_cpu_plot: bool
    show_memory_plot: bool
    show_tool_latency_plot: bool
    show_tool_cpu_plot: bool
    show_tool_memory_plot: bool


@dataclass(slots=True)
class ScalingSections:
    show_cpu_plot: bool
    show_memory_plot: bool
    show_tool_latency_plot: bool
    show_tool_cpu_plot: bool
    show_tool_memory_plot: bool


@dataclass(slots=True)
class RunReport:
    run: PerformanceWorkloadRun
    report_dir_name: str
    sections: RunSections
    images: dict[RunImageKey, ImageRef]


@dataclass(slots=True)
class WorkerSliceReport:
    worker_count: int
    title_suffix: str
    group_runs: list[PerformanceWorkloadRun]
    sections: WorkerSliceSections
    images: dict[WorkerSliceImageKey, ImageRef]


@dataclass(slots=True)
class ScalingReport:
    sections: ScalingSections
    images: dict[ScalingImageKey, ImageRef]


@dataclass(slots=True)
class PerformanceWorkloadReport:
    workload_name: str
    workload_out_dir: str
    worker_axis: WorkerAxis
    worker_label_singular: str
    worker_label_plural: str
    worker_suffix: str
    orig_yaml: str
    runs: list[RunReport]
    worker_slices: list[WorkerSliceReport]
    scaling: ScalingReport
    store_order: list[str]


@dataclass(slots=True)
class PerformanceSessionReport:
    session_id: str
    session_out_dir: str
    workloads: list[PerformanceWorkloadReport]
    session_images: dict[SessionImageKey, ImageRef] = field(default_factory=dict)
