// Pure aggregation logic — no fs, no React, no echarts — so it runs identically in the prep script
// (Node) and, if ever needed, the browser. This is the SINGLE SOURCE OF TRUTH for how raw run JSON
// becomes chart-ready metrics, and it mirrors the Python report generator so the two agree:
//   * eps = total count / elapsed span            (report_generator/comparison.py:_run_metrics)
//   * smoothed instantaneous eps (3-pt centered)  (report_generator/workloads/performance.py)
//   * ns -> ms percentiles, N + low-N suppression  (comparison.py, MIN_SAMPLES = 1000)
//   * dimension detection by workload-name prefix  (comparison.py:_DIMENSIONS)

import type {
  DimensionCell,
  DimensionData,
  LatencyCurve,
  RawContainerStats,
  RawCountSample,
  RawCpuSample,
  RawLatencyStats,
  RawMemSample,
  RawPercentile,
  RawReadiness,
  Regime,
  RunKind,
  RunSummary,
  Timeseries,
  WorkloadFamily,
  WorkloadGroup,
} from "../types";

/** Percentiles over fewer than this many samples are suppressed (comparison.py MIN_SAMPLES). */
export const MIN_SAMPLES = 1000;

interface DimensionDef {
  key: string;
  prefixes: string[];
  order: string[] | null;
  numeric: boolean;
  title: string;
}

/** Fixed-concurrency dimension sweeps, ported verbatim from comparison.py `_DIMENSIONS`. */
export const DIMENSIONS: DimensionDef[] = [
  {
    key: "selectivity",
    prefixes: ["warm-sel-", "sel-", "smoke-sel-"],
    order: ["single", "s1000", "s100", "s10", "full"],
    numeric: false,
    title: "Read selectivity (warm)",
  },
  {
    key: "resume",
    prefixes: ["warm-resume-", "resume-", "smoke-resume-"],
    order: ["whole", "half", "recent"],
    numeric: false,
    title: "Resume position (warm)",
  },
  { key: "write_payload", prefixes: ["wp-", "smoke-wp-"], order: null, numeric: true, title: "Write payload (bytes)" },
  { key: "read_payload", prefixes: ["rp-", "smoke-rp-"], order: null, numeric: true, title: "Read payload (bytes)" },
];

/** `<store>-w<N>` / `<store>-r<N>` — one suffix, per comparison.py `_RUN_RE`. */
const RUN_RE = /^(?<store>.+)-(?<kind>[rw])(?<n>\d+)$/;

export function parseRunName(name: string): { store: string; kind: RunKind; n: number } | null {
  const m = RUN_RE.exec(name);
  if (!m || !m.groups) return null;
  return { store: m.groups.store, kind: m.groups.kind as RunKind, n: Number(m.groups.n) };
}

export function regimeOf(workload: string): Regime {
  if (workload.startsWith("warm-")) return "warm";
  if (workload.startsWith("cold-")) return "cold";
  return "none";
}

export function dimensionFor(workload: string): { def: DimensionDef; value: string } | null {
  for (const def of DIMENSIONS) {
    for (const p of def.prefixes) {
      if (workload.startsWith(p)) return { def, value: workload.slice(p.length) };
    }
  }
  return null;
}

// -------- numeric helpers --------

function sum(xs: number[]): number {
  return xs.reduce((a, b) => a + b, 0);
}
function mean(xs: number[]): number | null {
  return xs.length ? sum(xs) / xs.length : null;
}
function max(xs: number[]): number | null {
  return xs.length ? Math.max(...xs) : null;
}

/** Run throughput: total ops / elapsed span (comparison.py:_run_metrics). */
export function epsFromSamples(samples: RawCountSample[]): number {
  if (samples.length === 0) return 0;
  const total = sum(samples.map((s) => s.count));
  const elapsed = samples.map((s) => s.elapsed_s);
  const span = samples.length > 1 ? Math.max(...elapsed) - Math.min(...elapsed) : 0;
  return span > 0 ? total / span : 0;
}

/**
 * Instantaneous eps per interval with a 3-point centered moving average, then t=0 prepended for
 * step plotting. Mirrors performance.py:get_throughput_timeseries.
 */
export function throughputSeries(samples: RawCountSample[]): Timeseries {
  if (samples.length === 0) return { t: [], eps: [], eps_smooth: [] };
  const sorted = [...samples].sort((a, b) => a.elapsed_s - b.elapsed_s);
  const times = sorted.map((s) => s.elapsed_s);
  const counts = sorted.map((s) => s.count);
  // Δt: first interval measured from 0.
  const dt = times.map((t, i) => (i === 0 ? t : t - times[i - 1]) || 1);
  const eps = counts.map((c, i) => (dt[i] > 0 ? c / dt[i] : 0));
  // centered window-3 moving average, min_periods=1
  const smooth = eps.map((_, i) => {
    const lo = Math.max(0, i - 1);
    const hi = Math.min(eps.length - 1, i + 1);
    return mean(eps.slice(lo, hi + 1))!;
  });
  return {
    t: [0, ...times],
    eps: [eps[0], ...eps],
    eps_smooth: [smooth[0], ...smooth],
  };
}

/**
 * {percentile(1-decimal) -> ms} plus a sorted curve for percentile charts. Keyed to 1 decimal so
 * p99.9 (99.9) and p100 (100) stay distinct — a plain round() would collide them, and the tail
 * percentiles (99.5/99.9/99.99/99.999) are exactly what we want to read. p50/p90/p99 match the
 * Python report's `round(percentile)` lookups since they're already integers.
 */
export function percentileData(latency: RawPercentile[]): { map: Map<number, number>; curve: LatencyCurve } {
  const map = new Map<number, number>();
  for (const e of latency) map.set(Math.round(e.percentile * 10) / 10, e.latency_ns / 1e6);
  const sorted = [...latency].sort((a, b) => a.percentile - b.percentile);
  return {
    map,
    curve: { p: sorted.map((e) => e.percentile), ms: sorted.map((e) => e.latency_ns / 1e6) },
  };
}

export interface RunInputs {
  id: string;
  config: RunConfig | null;
  throughput: RawCountSample[];
  errors: RawCountSample[];
  latency: RawPercentile[];
  latencyStats: RawLatencyStats | null;
  cpu: RawCpuSample[];
  mem: RawMemSample[];
  readiness: RawReadiness | null;
  containerStats: RawContainerStats | null;
}

/** Subset of a run's expanded config.yaml we surface. */
export interface RunConfig {
  mode?: string;
  operations?: {
    write?: {
      batch_size?: number;
      event_size_bytes?: number;
      append_condition?: string;
      concurrency_control?: boolean;
      in_flight_limit?: number;
    };
    read?: { dcb_query?: string; limit?: number; resume?: string };
  };
  setup?: { prepopulate_events?: number; prepopulate_streams?: number };
}

export function summarizeRun(inputs: RunInputs, store: string, workload: string, kind: RunKind, concurrency: number): RunSummary {
  const { config } = inputs;
  const w = config?.operations?.write;
  const r = config?.operations?.read;

  const { map, curve } = percentileData(inputs.latency);
  const n = inputs.latencyStats?.store_latency_count ?? null;
  const suppressed = n !== null && n > 0 && n < MIN_SAMPLES;
  const pick = (p: number): number | null => (suppressed ? null : (map.get(p) ?? null));

  return {
    id: inputs.id,
    store,
    workload,
    kind,
    concurrency,
    mode: config?.mode ?? "",
    regime: regimeOf(workload),
    batch: w?.batch_size ?? null,
    payload: w?.event_size_bytes ?? null,
    appendCondition: w?.append_condition ?? null,
    concurrencyControl: w?.concurrency_control ?? null,
    dcbQuery: r?.dcb_query ?? null,
    resume: r?.resume ?? null,
    readLimit: r?.limit ?? null,
    prepopulateEvents: config?.setup?.prepopulate_events ?? null,

    eps: epsFromSamples(inputs.throughput),
    peakEps: max(throughputSeries(inputs.throughput).eps_smooth) ?? 0,
    cpuAvg: mean(inputs.cpu.map((c) => c.cpu_percent)),
    memPeakMb: inputs.mem.length ? max(inputs.mem.map((m) => m.memory_bytes))! / 1e6 : null,
    errors: sum(inputs.errors.map((e) => e.count)),
    startupS: inputs.containerStats?.startup_time_s ?? null,

    p50: pick(50),
    p90: pick(90),
    p99: pick(99),
    p999: pick(99.9),
    n,
    suppressed,
    readiness: inputs.readiness,

    throughput: throughputSeries(inputs.throughput),
    cpu: inputs.cpu.length ? { t: inputs.cpu.map((c) => c.elapsed_s), v: inputs.cpu.map((c) => c.cpu_percent) } : undefined,
    mem: inputs.mem.length ? { t: inputs.mem.map((m) => m.elapsed_s), v: inputs.mem.map((m) => m.memory_bytes / 1e6) } : undefined,
    latency: curve,
  };
}

/** Group runs into workloads and classify each family by its distinct-concurrency count. */
export function buildWorkloads(runs: RunSummary[]): WorkloadGroup[] {
  const byName = new Map<string, RunSummary[]>();
  for (const run of runs) {
    const arr = byName.get(run.workload) ?? [];
    arr.push(run);
    byName.set(run.workload, arr);
  }
  const groups: WorkloadGroup[] = [];
  for (const [name, rs] of byName) {
    const concurrencies = new Set(rs.map((r) => r.concurrency));
    const family: WorkloadFamily = concurrencies.size > 1 ? "scaling" : "single";
    groups.push({ name, mode: rs[0].mode, regime: rs[0].regime, family, runs: rs });
  }
  // stable, readable order: writes first, then warm reads, then cold, then dimensions
  const rank = (g: WorkloadGroup): number => {
    if (g.regime === "warm") return 2;
    if (g.regime === "cold") return 3;
    if (dimensionFor(g.name)) return 4;
    return 1;
  };
  groups.sort((a, b) => rank(a) - rank(b) || a.name.localeCompare(b.name));
  return groups;
}

/** Build the fixed-concurrency dimension sweeps (selectivity / resume / payload). */
export function buildDimensions(runs: RunSummary[]): DimensionData[] {
  const out: DimensionData[] = [];
  for (const def of DIMENSIONS) {
    const cells: DimensionCell[] = [];
    const valueSet = new Set<string>();
    for (const run of runs) {
      const d = dimensionFor(run.workload);
      if (!d || d.def.key !== def.key) continue;
      valueSet.add(d.value);
      cells.push({
        value: d.value,
        store: run.store,
        eps: run.eps,
        p99: run.p99,
        memPeakMb: run.memPeakMb,
        n: run.n,
        suppressed: run.suppressed,
        errors: run.errors,
      });
    }
    if (cells.length === 0) continue;
    let values = [...valueSet];
    if (def.numeric) values.sort((a, b) => Number(a) - Number(b));
    else if (def.order) values.sort((a, b) => def.order!.indexOf(a) - def.order!.indexOf(b));
    else values.sort();
    out.push({ key: def.key, title: def.title, numeric: def.numeric, values, cells });
  }
  return out;
}
