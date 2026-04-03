from dataclasses import dataclass
from typing import Optional


@dataclass
class OsInfo:
    name: str
    version: str
    kernel: str
    arch: str


@dataclass
class CpuInfo:
    model: str
    cores: int
    threads: int


@dataclass
class MemoryInfo:
    total_bytes: int
    available_bytes: int


@dataclass
class FsyncStats:
    min_us: float
    max_us: float
    avg_us: float
    p95_us: float
    p99_us: float


@dataclass
class DiskInfo:
    disk_type: str
    filesystem: str
    fsync_latency: Optional[FsyncStats]


@dataclass
class ContainerRuntimeInfo:
    runtime_type: str
    version: str
    ncpu: int
    mem_total: int


@dataclass
class EnvironmentInfo:
    os: OsInfo
    cpu: CpuInfo
    memory: MemoryInfo
    disk: DiskInfo
    container_runtime: ContainerRuntimeInfo
