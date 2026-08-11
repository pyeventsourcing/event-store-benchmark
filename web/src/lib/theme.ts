// Shared visual language: per-store colors, display names, and number formatters. Kept in one place
// so every chart and table is consistent (the reviewer explicitly wanted each store identifiable).

const STORE_COLORS: Record<string, string> = {
  tephra: "#7c5cfc", // violet (placeholder — no brand color yet; chosen to sit distinctly in the set)
  dcbdb: "#a78bfa", // historical alias of tephra (pre-rename) — lighter violet
  umadb: "#f4bd17", // gold
  axonserver: "#2e90fa", // blue
  kurrentdb: "#ff4a80", // rose
  eventsourcingdb: "#06b6d4", // cyan
  fact: "#7f9b5a", // olive
  "postgres-dcb-marten": "#b18b6a", // tan
  marten: "#b18b6a",
};

const FALLBACK = ["#64748b", "#0ea5e9", "#22c55e", "#eab308", "#f97316", "#14b8a6", "#f43f5e"];

export function colorForStore(store: string): string {
  if (STORE_COLORS[store]) return STORE_COLORS[store];
  let h = 0;
  for (let i = 0; i < store.length; i++) h = (h * 31 + store.charCodeAt(i)) & 0xffff;
  return FALLBACK[h % FALLBACK.length];
}

const STORE_LABELS: Record<string, string> = {
  tephra: "Tephra",
  dcbdb: "dcbdb (→Tephra)",
  umadb: "UmaDB",
  axonserver: "AxonServer",
  kurrentdb: "KurrentDB",
  eventsourcingdb: "EventSourcingDB",
  fact: "Fact",
  "postgres-dcb-marten": "Marten",
  marten: "Marten",
};

export function prettyStore(store: string): string {
  return STORE_LABELS[store] ?? store;
}

// ------------------------------ formatters ------------------------------

export function fmtEps(v: number): string {
  if (v >= 1_000_000) return `${(v / 1_000_000).toFixed(2)}M`;
  if (v >= 1_000) return `${(v / 1_000).toFixed(1)}k`;
  return `${Math.round(v)}`;
}

export function fmtMs(v: number | null): string {
  if (v === null) return "—";
  if (v >= 1000) return `${(v / 1000).toFixed(2)} s`;
  if (v >= 10) return `${v.toFixed(0)} ms`;
  return `${v.toFixed(2)} ms`;
}

export function fmtInt(v: number | null): string {
  return v === null ? "—" : Math.round(v).toLocaleString();
}

export function fmtBytes(v: number): string {
  if (v >= 1024 ** 3) return `${(v / 1024 ** 3).toFixed(1)} GiB`;
  if (v >= 1024 ** 2) return `${(v / 1024 ** 2).toFixed(0)} MiB`;
  if (v >= 1024) return `${(v / 1024).toFixed(0)} KiB`;
  return `${v} B`;
}

export function fmtMb(v: number | null): string {
  if (v === null) return "—";
  if (v >= 1024) return `${(v / 1024).toFixed(2)} GB`;
  return `${v.toFixed(0)} MB`;
}

// ------------------------------ echarts base ------------------------------

/** Common ECharts option fragments for a clean, consistent look (light theme). */
export const CHART = {
  grid: { left: 70, right: 20, top: 34, bottom: 44, containLabel: true },
  textStyle: { fontFamily: "'Inter', system-ui, -apple-system, sans-serif", color: "#334155" },
  axisLine: { lineStyle: { color: "#cbd5e1" } },
  splitLine: { lineStyle: { color: "#eef2f7" } },
  tooltipBg: "rgba(255,255,255,0.97)",
};

/** Y-axis name rendered vertically along the axis (never collides with the top legend). */
export function yName(name: string) {
  return {
    name,
    nameLocation: "middle" as const,
    // Sits outside the tick labels; wide "k"/"M" abbreviations (e.g. "600.0k") are ~46px, so the
    // rotated name needs to clear them to avoid overlapping the axis numbers.
    nameGap: 58,
    nameTextStyle: { color: "#64748b", fontSize: 12, fontWeight: 600 as const },
  };
}

export function tooltipStyle() {
  return {
    backgroundColor: CHART.tooltipBg,
    borderColor: "#e2e8f0",
    borderWidth: 1,
    textStyle: { color: "#0f172a", fontSize: 12 },
    extraCssText: "box-shadow: 0 6px 24px rgba(15,23,42,0.12); border-radius: 8px;",
  };
}
