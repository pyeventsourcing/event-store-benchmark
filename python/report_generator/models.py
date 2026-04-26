from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any, Union, Literal


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


class SessionInfo(BaseModel):
    session_id: str
    tool_version: str
    workload_name: str
    config_file: str
    seed: int


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


class PerformanceWorkflowSamples(BaseModel):
    throughput_samples: List[ThroughputSample] = Field(default_factory=list)
    operation_error_samples: List[ThroughputSample] = Field(default_factory=list)
    latency_percentiles: List[LatencySample] = Field(default_factory=list)
    cpu_samples: List[CpuSample] = Field(default_factory=list)
    memory_samples: List[MemorySample] = Field(default_factory=list)
    tool_latency_percentiles: List[LatencySample] = Field(default_factory=list)
    tool_cpu_samples: List[CpuSample] = Field(default_factory=list)
    tool_memory_samples: List[MemorySample] = Field(default_factory=list)


class PerformanceConcurrencyConfig(BaseModel):
    writers: Union[int, List[int]] = 0
    readers: Union[int, List[int]] = 0


class PerformanceWriteOperationConfig(BaseModel):
    event_size_bytes: int = 0
    in_flight_limit: int = 2000


class PerformanceReadOperationConfig(BaseModel):
    limit: int = 1


class PerformanceOperationConfig(BaseModel):
    write: PerformanceWriteOperationConfig = Field(default_factory=PerformanceWriteOperationConfig)
    read: PerformanceReadOperationConfig = Field(default_factory=PerformanceReadOperationConfig)


class PerformanceSetupConfig(BaseModel):
    prepopulate_events: int = 0
    prepopulate_streams: int = 0


class PerformanceWorkloadConfig(BaseModel):
    name: str
    mode: Literal["write", "writeflood", "read"]
    warmup_seconds: int = 0
    duration_seconds: int
    samples_per_second: int = 1
    concurrency: PerformanceConcurrencyConfig = Field(default_factory=PerformanceConcurrencyConfig)
    operations: PerformanceOperationConfig = Field(default_factory=PerformanceOperationConfig)
    use_docker: bool = False
    docker_memory_limit_mb: Optional[int] = None
    docker_platform: Optional[str] = None
    setup: PerformanceSetupConfig = Field(default_factory=PerformanceSetupConfig)
    stores: Union[str, List[str]]


class RawPerformanceWorkloadRunResults(BaseModel):
    config: PerformanceWorkloadConfig
    results: PerformanceWorkflowSamples
    metrics: Dict[str, Any] = Field(default_factory=dict)
    logs: str = ""
