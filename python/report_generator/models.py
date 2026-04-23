from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any


class OsInfo(BaseModel):
    name: str
    version: str
    kernel: str
    arch: str


class CpuInfo(BaseModel):
    model: str
    cores: int
    threads: int


class MemoryInfo(BaseModel):
    total_bytes: int
    available_bytes: int


class FsyncStats(BaseModel):
    min_us: float
    max_us: float
    avg_us: float
    p95_us: float
    p99_us: float


class DiskInfo(BaseModel):
    disk_type: str = Field(alias="type", default="unknown")
    filesystem: str = "unknown"
    fsync_latency: Optional[FsyncStats] = None

    class Config:
        populate_by_name = True


class ContainerRuntimeInfo(BaseModel):
    runtime_type: str = Field(alias="type", default="unknown")
    version: str = "unknown"
    ncpu: int = 0
    mem_total: int = 0

    class Config:
        populate_by_name = True


class EnvironmentInfo(BaseModel):
    os: OsInfo
    cpu: CpuInfo
    memory: MemoryInfo
    disk: DiskInfo
    container_runtime: Optional[ContainerRuntimeInfo] = Field(None, alias="container_runtime")

    class Config:
        populate_by_name = True


class LatencySample(BaseModel):
    percentile: float
    latency_ns: int


class ThroughputSample(BaseModel):
    elapsed_s: float
    count: int


class CpuSample(BaseModel):
    elapsed_s: float
    cpu_percent: float


class MemorySample(BaseModel):
    elapsed_s: float
    memory_bytes: int


class RunResults(BaseModel):
    throughput_samples: List[ThroughputSample] = Field(default_factory=list)
    latency_percentiles: List[LatencySample] = Field(default_factory=list)
    cpu_samples: List[CpuSample] = Field(default_factory=list)
    memory_samples: List[MemorySample] = Field(default_factory=list)
    tool_latency_percentiles: List[LatencySample] = Field(default_factory=list)
    tool_cpu_samples: List[CpuSample] = Field(default_factory=list)
    tool_memory_samples: List[MemorySample] = Field(default_factory=list)


class RunData(BaseModel):
    config: Dict[str, Any]
    results: RunResults
    metrics: Dict[str, Any] = Field(default_factory=dict)
    logs: str = ""
