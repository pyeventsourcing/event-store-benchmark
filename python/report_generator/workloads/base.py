from abc import ABC, abstractmethod
from pathlib import Path

from ..models import RunData


class BaseWorkloadResult(ABC):
    """Abstract base class for a single workload run result."""

    def __init__(self, raw_data: RunData, run_path: Path):
        self.raw_data = raw_data
        self.run_path = run_path
        self.config = raw_data.config
        self.metrics = raw_data.metrics
        self.results = raw_data.results
        self.logs = raw_data.logs

    @property
    @abstractmethod
    def name(self) -> str:
        """Return the name of the workload run."""
        pass

    @property
    @abstractmethod
    def adapter(self) -> str:
        """Return the adapter/store name for this run."""
        pass
