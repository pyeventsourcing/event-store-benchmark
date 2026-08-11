import ReactECharts from "echarts-for-react";
import type { EChartsOption } from "echarts";

// Thin, memo-friendly wrapper around echarts-for-react so every chart shares sizing + notMerge
// behaviour. Charts are pure SVG/canvas — never PNGs.
export function Chart({ option, height = 320 }: { option: EChartsOption; height?: number }) {
  return (
    <ReactECharts
      option={option}
      notMerge
      lazyUpdate
      style={{ height, width: "100%" }}
      opts={{ renderer: "canvas" }}
    />
  );
}
