// Types for the event-store-benchmark results, plus the aggregated shape the UI consumes.
//
// Two layers:
//   Raw*   – exact shape of the JSON the Rust harness writes per run/session (see metrics.rs).
//   *      – the aggregated, chart-ready shape emitted by scripts/prepare-data.ts and fetched by
//            the app. All heavy parsing/aggregation happens at prep time so the browser just draws.

// ---------------------------------------------------------------------------------------------
// Raw on-disk shapes
// ---------------------------------------------------------------------------------------------

/** throughput.json / operation_errors.json entry. */
export interface RawCountSample {
  elapsed_s: number;
  count: number;
}

/** cpu.json / tool_cpu.json entry. */
export interface RawCpuSample {
  elapsed_s: number;
  cpu_percent: number;
}

/** memory.json / tool_memory.json entry. */
export interface RawMemSample {
  elapsed_s: number;
  memory_bytes: number;
}

/** latency.json / tool_latency.json entry. */
export interface RawPercentile {
  percentile: number;
  latency_ns: number;
}

export interface RawSessionJson {
  session_id: string;
  tool_version?: string;
  workload_name?: string;
  config_file?: string;
  seed?: number;
  /** Optional free-text fairness/provenance note surfaced in the Showcase. */
  note?: string;
}

export interface RawEnvironment {
  os?: { name?: string; version?: string; kernel?: string; arch?: string };
  cpu?: { model?: string; cores?: number; threads?: number };
  memory?: { total_bytes?: number; available_bytes?: number };
  disk?: {
    type?: string;
    filesystem?: string;
    fsync_latency?: { min_us?: number; max_us?: number; avg_us?: number; p95_us?: number; p99_us?: number };
  };
  container_runtime?: { type?: string; version?: string; ncpu?: number; mem_total?: number };
}

/** latency_stats.json (newer runs only). */
export interface RawLatencyStats {
  store_latency_count?: number;
  tool_latency_count?: number;
  store_latency_percentiles?: RawPercentile[];
}

/** readiness.json (newer runs only). */
export interface RawReadiness {
  ready: boolean;
  attempts: number;
  wait_ms: number;
}

/** container_stats.json. */
export interface RawContainerStats {
  startup_time_s?: number;
  image_size_bytes?: number | null;
}

/** run_manifest.json – free-form per-store posture (newer runs only). */
export type RawManifest = Record<string, unknown>;

// ---------------------------------------------------------------------------------------------
// Aggregated shapes (what the UI consumes)
// ---------------------------------------------------------------------------------------------

export type Regime = "warm" | "cold" | "none";
export type RunKind = "w" | "r";

/** A smoothed throughput timeseries (events/sec over elapsed seconds). */
export interface Timeseries {
  t: number[];
  eps: number[];
  eps_smooth: number[];
}

export interface ResourceSeries {
  t: number[];
  v: number[];
}

/** percentile (x, 0..100) -> latency in ms (y). */
export interface LatencyCurve {
  p: number[];
  ms: number[];
}

/** One benchmark run: a single store at a single concurrency for one workload. */
export interface RunSummary {
  id: string; // run dir name, e.g. "umadb-w16"
  store: string;
  workload: string;
  kind: RunKind; // w = writers swept, r = readers swept
  concurrency: number;
  mode: string; // write | writeflood | read | subscribe
  regime: Regime;
  batch: number | null;
  payload: number | null; // event_size_bytes
  appendCondition: string | null;
  concurrencyControl: boolean | null;
  dcbQuery: string | null;
  resume: string | null;
  readLimit: number | null;
  prepopulateEvents: number | null;

  // headline metrics
  eps: number;
  peakEps: number;
  cpuAvg: number | null;
  memPeakMb: number | null;
  errors: number;
  startupS: number | null;

  // latency (ms); null when suppressed or absent
  p50: number | null;
  p90: number | null;
  p99: number | null;
  p999: number | null;
  n: number | null; // store latency sample count
  suppressed: boolean; // 0 < n < MIN_SAMPLES

  readiness: RawReadiness | null;

  // timeseries + curve for charts
  throughput: Timeseries;
  cpu?: ResourceSeries;
  mem?: ResourceSeries;
  latency: LatencyCurve;
}

/** All runs for one workload name (e.g. scale-write), across stores + concurrencies. */
export interface WorkloadGroup {
  name: string;
  mode: string;
  regime: Regime;
  family: WorkloadFamily; // how it should be charted
  runs: RunSummary[];
}

/**
 * How a workload is best visualised:
 *  - "scaling"   : concurrency sweep -> throughput/latency vs concurrency curves (scale-*, matrix-*, writeflood, cold-*)
 *  - "single"    : one (or few) points -> comparison bars
 */
export type WorkloadFamily = "scaling" | "single";

/** A fixed-concurrency dimension sweep (selectivity / resume / payload). */
export interface DimensionData {
  key: string;
  title: string;
  numeric: boolean;
  // ordered dimension values (e.g. ["single","s1000","s100","s10","full"])
  values: string[];
  // per value -> per store -> metrics
  cells: DimensionCell[];
}

export interface DimensionCell {
  value: string;
  store: string;
  eps: number;
  p99: number | null;
  memPeakMb: number | null;
  n: number | null;
  suppressed: boolean;
  errors: number;
}

/** Everything the app needs for one session, produced by prepare-data.ts. */
export interface SessionData {
  id: string;
  session: RawSessionJson;
  environment: RawEnvironment | null;
  stores: string[];
  runs: RunSummary[];
  workloads: WorkloadGroup[];
  dimensions: DimensionData[];
  manifests: Record<string, RawManifest>;
  generatedAt: string;
}

/** index.json entry for the session picker. */
export interface SessionIndexEntry {
  id: string;
  workloadName: string;
  stores: string[];
  runCount: number;
  generatedAt: string;
}
