import pandas as pd

from .base import BaseWorkloadResult


class PerformanceWorkloadResult(BaseWorkloadResult):
    """Represents and analyzes the results of a single performance workload run."""

    def __init__(self, raw_data: dict, run_path):
        super().__init__(raw_data, run_path)
        self._parse_config()
        self._process_results()

    def _parse_config(self):
        """Extracts key parameters from the run's configuration data."""
        stores = self.config.get("stores")
        adapter = "unknown"
        if isinstance(stores, dict):
            adapter = stores.get("Single", "unknown")
        elif isinstance(stores, list) and len(stores) > 0:
            adapter = stores[0]
        elif stores is not None:
            adapter = str(stores)
        self.adapter_name = adapter

        concurrency = self.config.get("concurrency", {})
        writers = 0
        readers = 0
        if isinstance(concurrency, dict):
            w_val = concurrency.get("writers", 0)
            if isinstance(w_val, dict):
                writers = w_val.get("Single", 0)
            else:
                writers = w_val

            r_val = concurrency.get("readers", 0)
            if isinstance(r_val, dict):
                readers = r_val.get("Single", 0)
            else:
                readers = r_val

        self.writers = writers
        self.readers = readers
        self.worker_count = writers if writers > 0 else readers
        self.is_read_workload = readers > 0 and writers == 0

    def _process_results(self):
        """Processes raw result data into structured formats and summary metrics."""
        self.throughput_df = pd.DataFrame(self.results.get("throughput_samples", []))
        self.latency_percentiles = self.results.get("latency_percentiles", [])

        # Calculate summary metrics
        self.duration_s = 0
        self.average_throughput = 0
        if not self.throughput_df.empty and len(self.throughput_df) >= 2:
            df = self.throughput_df.sort_values("elapsed_s")
            duration = df["elapsed_s"].iloc[-1] - df["elapsed_s"].iloc[0]
            total_count = df["count"].iloc[-1] - df["count"].iloc[0]
            self.duration_s = duration
            if duration > 0:
                self.average_throughput = total_count / duration

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