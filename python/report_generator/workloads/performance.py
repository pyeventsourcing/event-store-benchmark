import pandas as pd
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
        if not self.throughput_df.empty and len(self.throughput_df) >= 2:
            df = self.throughput_df.sort_values("elapsed_s")
            duration = df["elapsed_s"].iloc[-1] - df["elapsed_s"].iloc[0]
            total_count = df["count"].iloc[-1] - df["count"].iloc[0]
            self.duration_s = duration
            if duration > 0:
                self.average_throughput = total_count / duration
            
            ts = self.get_throughput_timeseries()
            if ts:
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
                return p["latency_us"] / 1000.0
        return 0.0

    def get_latency_cdf_data(self):
        """Returns data needed for a latency CDF plot."""
        if not self.latency_percentiles:
            return None, None
        percentiles = [p["percentile"] for p in self.latency_percentiles]
        latencies_ms = [p["latency_us"] / 1000.0 for p in self.latency_percentiles]
        return latencies_ms, percentiles

    def get_benchmark_latency_cdf_data(self):
        """Returns data needed for a benchmark latency CDF plot."""
        if not self.benchmark_latency_percentiles:
            return None, None
        percentiles = [p["percentile"] for p in self.benchmark_latency_percentiles]
        latencies_ms = [p["latency_us"] / 1000.0 for p in self.benchmark_latency_percentiles]
        return latencies_ms, percentiles

    def get_throughput_timeseries(self) -> dict | None:
        """
        Computes throughput time series from cumulative samples.
        This logic is moved from the original `compute_throughput_timeseries`.
        """
        if self.throughput_df.empty or "count" not in self.throughput_df.columns:
            return None

        df = self.throughput_df.copy()
        if len(df) < 2:
            return None

        df = df.sort_values("elapsed_s").reset_index(drop=True)

        time_diffs = df["elapsed_s"].diff().iloc[1:]
        count_diffs = df["count"].diff().iloc[1:]

        # Calculate throughput (events per second) for each interval
        eps = count_diffs / time_diffs

        # Apply moving average smoothing
        window_size = min(3, len(eps))
        eps_smooth = eps.rolling(window=window_size, center=True, min_periods=1).mean()

        time_s = df["elapsed_s"].iloc[1:]

        # Prepend t0 to make step plots start from the beginning of the first interval
        t0 = df["elapsed_s"].iloc[0]
        extended_time_s = pd.concat([pd.Series([t0]), time_s])
        extended_eps = pd.concat([pd.Series([eps.iloc[0]]), eps])
        extended_eps_smooth = pd.concat([pd.Series([eps_smooth.iloc[0]]), eps_smooth])

        return {
            "time_s": extended_time_s.values,
            "throughput_eps": extended_eps.values,
            "throughput_eps_smooth": extended_eps_smooth.values,
        }

    def get_cpu_timeseries(self) -> dict | None:
        """Returns CPU usage time series."""
        if self.cpu_df.empty:
            return None
        df = self.cpu_df.sort_values("elapsed_s")
        return {
            "time_s": df["elapsed_s"].values,
            "cpu_percent": df["cpu_percent"].values,
        }

    def get_benchmark_cpu_timeseries(self) -> dict | None:
        """Returns benchmark process CPU usage time series."""
        if self.benchmark_cpu_df.empty:
            return None
        df = self.benchmark_cpu_df.sort_values("elapsed_s")
        return {
            "time_s": df["elapsed_s"].values,
            "cpu_percent": df["cpu_percent"].values,
        }

    def get_memory_timeseries(self) -> dict | None:
        """Returns memory usage time series."""
        if self.memory_df.empty:
            return None
        df = self.memory_df.sort_values("elapsed_s")
        return {
            "time_s": df["elapsed_s"].values,
            "memory_mb": df["memory_bytes"].values / (1024 * 1024),
        }

    def get_benchmark_memory_timeseries(self) -> dict | None:
        """Returns benchmark process memory usage time series."""
        if self.benchmark_memory_df.empty:
            return None
        df = self.benchmark_memory_df.sort_values("elapsed_s")
        return {
            "time_s": df["elapsed_s"].values,
            "memory_mb": df["memory_bytes"].values / (1024 * 1024),
        }