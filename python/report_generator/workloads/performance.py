import pandas as pd
import numpy as np
from pathlib import Path

from .base import BaseWorkloadResult


class PerformanceWorkloadResult(BaseWorkloadResult):
    """Represents and analyzes the results of a single performance workload run."""

    def __init__(self, raw_data: dict, run_path: Path):
        super().__init__(raw_data, run_path)
        self._parse_config()
        self._process_results()

    def _parse_config(self):
        """Extracts key parameters from the run's configuration data."""
        stores = self.config.get("stores")
        # This logic handles various ways 'stores' can be defined in YAML
        if isinstance(stores, list) and stores:
            self.adapter_name = stores[0]
        elif isinstance(stores, str):
            self.adapter_name = stores
        elif isinstance(stores, dict):
            # Handles the {'Single': 'value'} pattern
            self.adapter_name = stores.get("Single", "unknown")
        else:
            self.adapter_name = "unknown"

        concurrency = self.config.get("concurrency", {})
        if isinstance(concurrency, dict):
            writers_val = concurrency.get("writers")
            if isinstance(writers_val, dict):
                self.writers = writers_val.get("Single", 0)
            else:
                self.writers = writers_val if writers_val is not None else 0

            readers_val = concurrency.get("readers")
            if isinstance(readers_val, dict):
                self.readers = readers_val.get("Single", 0)
            else:
                self.readers = readers_val if readers_val is not None else 0
        else:
            # Default to 0 if concurrency is not a dict or not present
            self.writers = 0
            self.readers = 0

        self.worker_count = self.writers if self.writers > 0 else self.readers
        self.is_read_workload = self.readers > 0 and self.writers == 0

    def _process_results(self):
        """Processes raw result data into structured formats and summary metrics."""
        self.throughput_df = pd.DataFrame(self.results.get("throughput_samples", []))
        self.latency_percentiles = self.results.get("latency_percentiles", [])
        self.benchmark_latency_percentiles = self.results.get("benchmark_latency_percentiles", [])
        self.cpu_df = pd.DataFrame(self.results.get("cpu_samples", []))
        self.memory_df = pd.DataFrame(self.results.get("memory_samples", []))
        self.benchmark_cpu_df = pd.DataFrame(self.results.get("benchmark_cpu_samples", []))
        self.benchmark_memory_df = pd.DataFrame(self.results.get("benchmark_memory_samples", []))

        # Calculate summary metrics
        self.duration_s = 0
        self.average_throughput = 0
        self.peak_throughput = 0
        if not self.throughput_df.empty:
            df = self.throughput_df.sort_values("elapsed_s")
            self.duration_s = df["elapsed_s"].iloc[-1]
            total_count = df["count"].sum()
            if self.duration_s > 0:
                self.average_throughput = total_count / self.duration_s
            
            ts = self.get_throughput_timeseries()
            if ts is not None:
                self.peak_throughput = ts["throughput_eps_smooth"].max()

    @property
    def name(self) -> str:
        return self.config.get("name", "unknown")

    @property
    def adapter(self) -> str:
        return self.adapter_name

    def get_latency_percentile(self, percentile: float) -> float:
        """Extracts a specific latency percentile (in ms) from the results."""
        for p in self.latency_percentiles:
            if p["percentile"] == percentile:
                return p["latency_ns"] / 1000000.0
        return 0.0

    def get_benchmark_latency_percentile(self, percentile: float) -> float:
        """Extracts a specific benchmark latency percentile (in ms) from the results."""
        for p in self.benchmark_latency_percentiles:
            if p["percentile"] == percentile:
                return p["latency_ns"] / 1000000.0
        return 0.0

    def get_latency_cdf_data(self):
        """Returns data needed for a latency CDF plot."""
        if not self.latency_percentiles:
            return None, None
        percentiles = [p["percentile"] for p in self.latency_percentiles]
        latencies_ms = [p["latency_ns"] / 1000000.0 for p in self.latency_percentiles]
        return latencies_ms, percentiles

    def get_benchmark_latency_cdf_data(self):
        """Returns data needed for a benchmark latency CDF plot."""
        if not self.benchmark_latency_percentiles:
            return None, None
        percentiles = [p["percentile"] for p in self.benchmark_latency_percentiles]
        latencies_ms = [p["latency_ns"] / 1000000.0 for p in self.benchmark_latency_percentiles]
        return latencies_ms, percentiles

    def get_throughput_timeseries(self) -> dict | None:
        """
        Computes throughput time series from interval samples.
        """
        if self.throughput_df.empty or "count" not in self.throughput_df.columns:
            return None

        df = self.throughput_df.copy()
        if len(df) < 1:
            return None

        df = df.sort_values("elapsed_s").reset_index(drop=True)

        times = df["elapsed_s"].values
        counts = df["count"].values
        
        # Calculate time diffs
        time_diffs = df["elapsed_s"].diff().fillna(df["elapsed_s"].iloc[0])
        eps = counts / time_diffs

        # Apply moving average smoothing
        window_size = min(3, len(eps))
        eps_smooth = pd.Series(eps).rolling(window=window_size, center=True, min_periods=1).mean()

        # Prepend t=0 value for steps-pre plotting as requested
        # The line should start at t=0 with the first interval's throughput.
        extended_time_s = np.concatenate([[0.0], times])
        extended_eps = np.concatenate([[eps[0]], eps])
        extended_eps_smooth = np.concatenate([[eps_smooth.iloc[0]], eps_smooth.values])

        return {
            "time_s": extended_time_s,
            "throughput_eps": extended_eps,
            "throughput_eps_smooth": extended_eps_smooth,
        }

    def get_cpu_timeseries(self) -> dict | None:
        """Returns CPU usage time series."""
        if self.cpu_df.empty:
            return None
        df = self.cpu_df.sort_values("elapsed_s")
        
        times = df["elapsed_s"].values
        cpu_percent = df["cpu_percent"].values
        
        # Prepend t=0 value for steps-pre plotting
        extended_times = np.concatenate([[0.0], times])
        extended_cpu = np.concatenate([[cpu_percent[0]], cpu_percent])
        
        return {
            "time_s": extended_times,
            "cpu_percent": extended_cpu,
        }

    def get_benchmark_cpu_timeseries(self) -> dict | None:
        """Returns benchmark process CPU usage time series."""
        if self.benchmark_cpu_df.empty:
            return None
        df = self.benchmark_cpu_df.sort_values("elapsed_s")
        
        times = df["elapsed_s"].values
        cpu_percent = df["cpu_percent"].values
        
        # Prepend t=0 value for steps-pre plotting
        extended_times = np.concatenate([[0.0], times])
        extended_cpu = np.concatenate([[cpu_percent[0]], cpu_percent])
        
        return {
            "time_s": extended_times,
            "cpu_percent": extended_cpu,
        }

    def get_memory_timeseries(self) -> dict | None:
        """Returns memory usage time series."""
        if self.memory_df.empty:
            return None
        df = self.memory_df.sort_values("elapsed_s")
        
        times = df["elapsed_s"].values
        memory_mb = df["memory_bytes"].values / (1024 * 1024)
        
        # Prepend t=0 value for steps-pre plotting
        extended_times = np.concatenate([[0.0], times])
        extended_memory = np.concatenate([[memory_mb[0]], memory_mb])
        
        return {
            "time_s": extended_times,
            "memory_mb": extended_memory,
        }

    def get_benchmark_memory_timeseries(self) -> dict | None:
        """Returns benchmark process memory usage time series."""
        if self.benchmark_memory_df.empty:
            return None
        df = self.benchmark_memory_df.sort_values("elapsed_s")
        
        times = df["elapsed_s"].values
        memory_mb = df["memory_bytes"].values / (1024 * 1024)
        
        # Prepend t=0 value for steps-pre plotting
        extended_times = np.concatenate([[0.0], times])
        extended_memory = np.concatenate([[memory_mb[0]], memory_mb])
        
        return {
            "time_s": extended_times,
            "memory_mb": extended_memory,
        }