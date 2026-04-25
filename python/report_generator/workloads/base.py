from abc import ABC, abstractmethod
from pathlib import Path

from ..models import RawPerformanceWorkloadRunResults


class BaseWorkloadRun(ABC):
    """Abstract base class for a single workload run result."""

    def __init__(self, run_path: Path):
        self.run_path = run_path

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
