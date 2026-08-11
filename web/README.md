# esb-web — interactive benchmark results UI

An alternative, **PNG-free** front-end for viewing `event-store-benchmark` results: interactive
[ECharts](https://echarts.apache.org/) graphs, a curated **Showcase** view and a filterable
**Explore** view. It reads the *same* raw per-run JSON the Rust harness writes.

This is a **parallel** option to the existing Python report generator
(`python/report_generator/`, matplotlib PNGs + static HTML) — that one is untouched and still works.

## Stack

Vite + React + TypeScript + Apache ECharts. A small Node/tsx script snapshots a results directory
into aggregated JSON; `npm run build` produces a fully static, portable site.

## Quick start

```bash
cd web
npm install

# 1. Snapshot YOUR es-bench results dir into public/data/ (this repo ships without a dataset;
#    run the benchmarks first, then point --raw at the results dir the harness wrote)
npm run data -- --raw ../results

# 2a. Iterate locally
npm run dev            # http://localhost:5173

# 2b. …or build a portable static bundle
npm run build          # -> dist/  (open via any static host; data is baked in)
npm run preview        # serve dist/ locally to check the build
```

## How data flows

```
results/esb-<ts>/…  --(npm run data)-->  public/data/{index.json, <session>.json, segsweep.json}
                                                   │
                                          app fetches + renders (no server needed)
```

All parsing and aggregation happens **once**, at prep time, in `src/lib/aggregate.ts`. That module
is the single source of truth and mirrors the Python report so the numbers agree:

- throughput `eps` = total ops / elapsed span
- smoothed instantaneous eps (3-point centered moving average) for the timeseries
- latency percentiles in ms, with **low-N suppression** (percentiles over `< 1000` samples are
  hidden — see `MIN_SAMPLES`)
- dimension sweeps (`sel-*`, `resume-*`, `wp-*`, `rp-*`) grouped by workload-name prefix
- warm vs cold read regimes (`warm-*` / `cold-*`)
- per-store config panel from `run_manifest.json`

The browser never re-derives metrics — it only draws what the prep step produced.

## Views

- **Showcase** — curated story: writes (scaling + batch/payload), reads (scaling + selectivity /
  resume / cold), segment-size sweep, and a fairness/environment panel.
- **Explore** — filter by workload and store, toggle chart metric and log axis, and a full run
  table with exact `N` and low-N suppression surfaced.

Tabs are hash-routed (`#showcase` / `#explore`) so a view is shareable/bookmarkable. The session
picker (top-right) switches between benchmark sessions found in the data dir.

## Refreshing

Re-run `npm run data -- --raw <dir>` any time (it rewrites `public/data/`), then refresh the page
(dev) or rebuild (`npm run build`). To view results pulled back from a server run, point `--raw`
at that directory.

## Notes

- Older sessions that predate `latency_stats.json` / `readiness.json` / `run_manifest.json` render
  gracefully: `N` shows `?`, percentiles are not suppressed, readiness shows `—`, and the
  store-config panel notes the manifest is unavailable.
- The segment-size sweep view appears only when `esb-segsweep-<bytes>` sessions are present; it
  combines them into throughput/latency-vs-segment-size curves.
