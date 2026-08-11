import type { EChartsOption } from "echarts";
import type { DimensionData, RunSummary } from "../types";
import { CHART, colorForStore, fmtEps, fmtMb, fmtMs, prettyStore, tooltipStyle, yName } from "../lib/theme";
import { Chart } from "./Chart";

export type BarMetric = "eps" | "p99" | "memPeakMb";

function metricAxisName(metric: BarMetric): string {
  return metric === "eps" ? "events / sec" : metric === "p99" ? "p99 latency (ms)" : "peak memory (MB)";
}
function fmtMetric(metric: BarMetric, v: number | null): string {
  if (v === null) return "—";
  return metric === "eps" ? `${fmtEps(v)} eps` : metric === "p99" ? fmtMs(v) : fmtMb(v);
}

// ---- One bar per store (a single-concurrency workload compared across stores) ----

export function StoreBars({ runs, metric, height }: { runs: RunSummary[]; metric: BarMetric; height?: number }) {
  const stores = [...new Set(runs.map((r) => r.store))].sort();
  const val = (r: RunSummary): number | null =>
    metric === "eps" ? r.eps : metric === "p99" ? r.p99 : r.memPeakMb;

  const option: EChartsOption = {
    grid: CHART.grid,
    textStyle: CHART.textStyle,
    tooltip: { trigger: "item", ...tooltipStyle(), valueFormatter: (v) => fmtMetric(metric, v as number) },
    xAxis: { type: "category", data: stores.map(prettyStore), axisLine: CHART.axisLine, axisLabel: { interval: 0, rotate: stores.length > 4 ? 20 : 0 } },
    yAxis: {
      type: "value",
      ...yName(metricAxisName(metric)),
      axisLine: CHART.axisLine,
      splitLine: CHART.splitLine,
      axisLabel: { formatter: (v: number) => (metric === "eps" ? fmtEps(v) : `${v}`) },
    },
    series: [
      {
        type: "bar",
        data: stores.map((s) => {
          const r = runs.find((x) => x.store === s)!;
          const v = r ? val(r) : null;
          return { value: v, itemStyle: { color: colorForStore(s), borderRadius: [4, 4, 0, 0] } };
        }),
        barMaxWidth: 64,
        label: {
          show: true,
          position: "top",
          formatter: (p: { value?: unknown }) => fmtMetric(metric, (p.value ?? null) as number | null),
        },
      },
    ],
  };
  return <Chart option={option} height={height} />;
}

// ---- Grouped bars across a dimension's values (selectivity / resume / payload) ----

export function DimensionBars({ dim, metric, height }: { dim: DimensionData; metric: BarMetric; height?: number }) {
  const stores = [...new Set(dim.cells.map((c) => c.store))].sort();
  const pick = (value: string, store: string): number | null => {
    const c = dim.cells.find((x) => x.value === value && x.store === store);
    if (!c) return null;
    return metric === "eps" ? c.eps : metric === "p99" ? c.p99 : c.memPeakMb;
  };

  const option: EChartsOption = {
    grid: CHART.grid,
    textStyle: CHART.textStyle,
    legend: { top: 0, type: "scroll" },
    tooltip: { trigger: "axis", ...tooltipStyle(), valueFormatter: (v) => fmtMetric(metric, v as number | null) },
    xAxis: {
      type: "category",
      data: dim.values,
      name: dim.title,
      nameLocation: "middle",
      nameGap: 30,
      axisLine: CHART.axisLine,
    },
    yAxis: {
      type: "value",
      ...yName(metricAxisName(metric)),
      axisLine: CHART.axisLine,
      splitLine: CHART.splitLine,
      axisLabel: { formatter: (v: number) => (metric === "eps" ? fmtEps(v) : `${v}`) },
    },
    series: stores.map((store) => ({
      name: prettyStore(store),
      type: "bar",
      color: colorForStore(store),
      barMaxWidth: 40,
      itemStyle: { borderRadius: [3, 3, 0, 0] },
      data: dim.values.map((v) => pick(v, store)),
    })),
  };
  return <Chart option={option} height={height} />;
}
