from abc import ABC, abstractmethod
from pathlib import Path


class BaseWorkloadResult(ABC):
    """Abstract base class for a single workload run result."""

    def __init__(self, raw_data: dict, run_path: Path):
        self.raw_data = raw_data
        self.run_path = run_path
        self.config = raw_data.get('config', {})
        self.metrics = raw_data.get('metrics', {})
        self.results = raw_data.get('results', {})
        self.logs = raw_data.get('logs', "")

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