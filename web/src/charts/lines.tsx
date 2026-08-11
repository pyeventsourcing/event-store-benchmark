import type { EChartsOption } from "echarts";
import type { LatencyCurve, RunSummary } from "../types";
import { CHART, colorForStore, fmtEps, fmtMs, prettyStore, tooltipStyle, yName } from "../lib/theme";
import { Chart } from "./Chart";

// ---- helpers ----

function multiConcurrency(runs: RunSummary[]): boolean {
  return new Set(runs.map((r) => r.concurrency)).size > 1;
}

function seriesLabel(run: RunSummary, showConc: boolean): string {
  return showConc ? `${prettyStore(run.store)} · ${run.kind}${run.concurrency}` : prettyStore(run.store);
}

/** Nearest latency (ms) at a target percentile from the stored curve. */
function pctAt(curve: LatencyCurve, target: number): number | null {
  let best: number | null = null;
  let bestErr = Infinity;
  for (let i = 0; i < curve.p.length; i++) {
    const err = Math.abs(curve.p[i] - target);
    if (err < bestErr) {
      bestErr = err;
      best = curve.ms[i];
    }
  }
  return bestErr <= 0.6 ? best : null;
}

// ---- Throughput over time (smoothed) ----

export function ThroughputTimeseries({ runs, height }: { runs: RunSummary[]; height?: number }) {
  const showConc = multiConcurrency(runs);
  const option: EChartsOption = {
    grid: CHART.grid,
    textStyle: CHART.textStyle,
    legend: { top: 0, type: "scroll" },
    tooltip: {
      trigger: "axis",
      ...tooltipStyle(),
      valueFormatter: (v) => (typeof v === "number" ? `${fmtEps(v)} eps` : String(v)),
    },
    xAxis: {
      type: "value",
      name: "seconds",
      nameLocation: "middle",
      nameGap: 28,
      axisLine: CHART.axisLine,
      splitLine: { show: false },
    },
    yAxis: {
      type: "value",
      ...yName("events / sec"),
      axisLine: CHART.axisLine,
      splitLine: CHART.splitLine,
      axisLabel: { formatter: (v: number) => fmtEps(v) },
    },
    series: runs.map((run) => ({
      name: seriesLabel(run, showConc),
      type: "line",
      smooth: true,
      showSymbol: false,
      lineStyle: { width: 2 },
      color: colorForStore(run.store),
      data: run.throughput.t.map((t, i) => [t, run.throughput.eps_smooth[i]]),
    })),
  };
  return <Chart option={option} height={height} />;
}

// ---- Metric vs concurrency (scaling curve) ----

export function ScalingCurve({
  runs,
  metric,
  height,
}: {
  runs: RunSummary[];
  metric: "eps" | "p99" | "peakEps";
  height?: number;
}) {
  const concurrencies = [...new Set(runs.map((r) => r.concurrency))].sort((a, b) => a - b);
  const stores = [...new Set(runs.map((r) => r.store))].sort();
  const kind = runs[0]?.kind === "r" ? "readers" : "writers";
  const yLabel = metric === "p99" ? "p99 latency (ms)" : "events / sec";
  const val = (r: RunSummary): number | null => (metric === "p99" ? r.p99 : metric === "peakEps" ? r.peakEps : r.eps);

  const option: EChartsOption = {
    grid: CHART.grid,
    textStyle: CHART.textStyle,
    legend: { top: 0, type: "scroll" },
    tooltip: {
      trigger: "axis",
      ...tooltipStyle(),
      valueFormatter: (v) => (typeof v !== "number" ? "—" : metric === "p99" ? fmtMs(v) : `${fmtEps(v)} eps`),
    },
    xAxis: {
      type: "category",
      data: concurrencies.map(String),
      name: kind,
      nameLocation: "middle",
      nameGap: 28,
      axisLine: CHART.axisLine,
    },
    yAxis: {
      type: "value",
      ...yName(yLabel),
      axisLine: CHART.axisLine,
      splitLine: CHART.splitLine,
      axisLabel: { formatter: (v: number) => (metric === "p99" ? `${v}` : fmtEps(v)) },
    },
    series: stores.map((store) => ({
      name: prettyStore(store),
      type: "line",
      smooth: false,
      symbolSize: 7,
      lineStyle: { width: 2 },
      color: colorForStore(store),
      connectNulls: false,
      data: concurrencies.map((c) => {
        const r = runs.find((x) => x.store === store && x.concurrency === c);
        return r ? val(r) : null;
      }),
    })),
  };
  return <Chart option={option} height={height} />;
}

// ---- Latency by percentile (log y) ----

const LAT_PCTS = [50, 90, 99, 99.9, 99.99];

export function LatencyCurve({ runs, logY, height }: { runs: RunSummary[]; logY?: boolean; height?: number }) {
  const showConc = multiConcurrency(runs);
  const option: EChartsOption = {
    grid: CHART.grid,
    textStyle: CHART.textStyle,
    legend: { top: 0, type: "scroll" },
    tooltip: {
      trigger: "axis",
      ...tooltipStyle(),
      valueFormatter: (v) => (typeof v === "number" ? fmtMs(v) : "—"),
    },
    xAxis: {
      type: "category",
      data: LAT_PCTS.map((p) => `p${p}`),
      name: "percentile",
      nameLocation: "middle",
      nameGap: 28,
      axisLine: CHART.axisLine,
    },
    yAxis: {
      type: logY ? "log" : "value",
      ...yName("latency (ms)"),
      axisLine: CHART.axisLine,
      splitLine: CHART.splitLine,
    },
    series: runs.map((run) => ({
      name: seriesLabel(run, showConc),
      type: "line",
      smooth: false,
      symbolSize: 6,
      lineStyle: { width: 2, type: run.suppressed ? "dashed" : "solid" },
      color: colorForStore(run.store),
      data: LAT_PCTS.map((p) => pctAt(run.latency, p)),
    })),
  };
  return <Chart option={option} height={height} />;
}

// ---- Segment-size sweep ----

interface SweepSeries {
  store: string;
  points: { sizeBytes: number; eps: number; p99: number | null }[];
}
interface SweepWorkload {
  name: string;
  series: SweepSeries[];
}
export interface SegSweep {
  sizes: { bytes: number; label: string }[];
  workloads: SweepWorkload[];
}

export function SegmentSweep({
  sweep,
  workload,
  metric,
  height,
}: {
  sweep: SegSweep;
  workload: string;
  metric: "eps" | "p99";
  height?: number;
}) {
  const w = sweep.workloads.find((x) => x.name === workload);
  const option: EChartsOption = {
    grid: CHART.grid,
    textStyle: CHART.textStyle,
    legend: { top: 0, type: "scroll" },
    tooltip: {
      trigger: "axis",
      ...tooltipStyle(),
      valueFormatter: (v) => (typeof v !== "number" ? "—" : metric === "p99" ? fmtMs(v) : `${fmtEps(v)} eps`),
    },
    xAxis: {
      type: "category",
      data: sweep.sizes.map((s) => s.label),
      name: "segment size",
      nameLocation: "middle",
      nameGap: 28,
      axisLine: CHART.axisLine,
    },
    yAxis: {
      type: "value",
      ...yName(metric === "p99" ? "p99 latency (ms)" : "events / sec"),
      axisLine: CHART.axisLine,
      splitLine: CHART.splitLine,
      axisLabel: { formatter: (v: number) => (metric === "p99" ? `${v}` : fmtEps(v)) },
    },
    series: (w?.series ?? []).map((s) => ({
      name: prettyStore(s.store),
      type: "line",
      symbolSize: 7,
      lineStyle: { width: 2 },
      color: colorForStore(s.store),
      data: sweep.sizes.map((sz) => {
        const p = s.points.find((pt) => pt.sizeBytes === sz.bytes);
        return p ? (metric === "p99" ? p.p99 : p.eps) : null;
      }),
    })),
  };
  return <Chart option={option} height={height} />;
}
