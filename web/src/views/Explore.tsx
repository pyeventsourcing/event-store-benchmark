import { useMemo, useState } from "react";
import type { RunSummary, SessionData } from "../types";
import { colorForStore, fmtEps, fmtInt, fmtMb, fmtMs, prettyStore } from "../lib/theme";
import { LatencyCurve, ScalingCurve, ThroughputTimeseries } from "../charts/lines";
import { StoreBars, type BarMetric } from "../charts/bars";

type ChartMetric = "eps" | "p99" | "peakEps";

export function Explore({ data }: { data: SessionData }) {
  const workloadNames = useMemo(() => data.workloads.map((w) => w.name), [data]);
  const [activeStores, setActiveStores] = useState<Set<string>>(new Set(data.stores));
  const [workload, setWorkload] = useState<string>(workloadNames[0] ?? "all");
  const [metric, setMetric] = useState<ChartMetric>("eps");
  const [logY, setLogY] = useState(false);

  const toggleStore = (s: string) => {
    setActiveStores((prev) => {
      const next = new Set(prev);
      next.has(s) ? next.delete(s) : next.add(s);
      return next;
    });
  };

  const group = data.workloads.find((w) => w.name === workload);
  const runs = useMemo(() => {
    const base = workload === "all" ? data.runs : (group?.runs ?? []);
    return base.filter((r) => activeStores.has(r.store)).sort(sortRuns);
  }, [data, group, workload, activeStores]);

  const showScaling = group?.family === "scaling";

  return (
    <>
      <div className="card">
        <div className="filters">
          <div className="filter">
            <label>Workload</label>
            <select value={workload} onChange={(e) => setWorkload(e.target.value)}>
              <option value="all">all workloads</option>
              {workloadNames.map((n) => (
                <option key={n} value={n}>
                  {n}
                </option>
              ))}
            </select>
          </div>
          <div className="filter">
            <label>Chart metric</label>
            <select value={metric} onChange={(e) => setMetric(e.target.value as ChartMetric)}>
              <option value="eps">throughput (eps)</option>
              <option value="peakEps">peak throughput</option>
              <option value="p99">p99 latency</option>
            </select>
          </div>
          <div className="filter">
            <label>Latency axis</label>
            <label className="toggle">
              <input type="checkbox" checked={logY} onChange={(e) => setLogY(e.target.checked)} /> log scale
            </label>
          </div>
          <div className="filter" style={{ flex: 1, minWidth: 240 }}>
            <label>Stores</label>
            <div className="chips">
              {data.stores.map((s) => (
                <span
                  key={s}
                  className={`chip ${activeStores.has(s) ? "" : "off"}`}
                  onClick={() => toggleStore(s)}
                >
                  <span className="store-dot" style={{ background: colorForStore(s) }} /> {prettyStore(s)}
                </span>
              ))}
            </div>
          </div>
        </div>
      </div>

      {/* charts for the selected workload */}
      {group && (
        <div className="card">
          <h3>
            {group.name}{" "}
            {group.regime !== "none" && <span className="badge warn">{group.regime}</span>}{" "}
            <span className="muted" style={{ fontWeight: 400, fontSize: 12 }}>
              {group.mode} · {group.family}
            </span>
          </h3>
          {showScaling ? (
            <>
              <ScalingCurve runs={runs} metric={metric} />
              <div className="grid2" style={{ marginTop: 8 }}>
                <ThroughputTimeseries runs={maxConc(runs)} height={260} />
                <LatencyCurve runs={maxConc(runs)} logY={logY} height={260} />
              </div>
            </>
          ) : (
            <div className="grid2">
              <StoreBars runs={runs} metric={metric === "peakEps" ? "eps" : (metric as BarMetric)} />
              <LatencyCurve runs={runs} logY={logY} />
            </div>
          )}
        </div>
      )}

      {/* full run table */}
      <div className="card">
        <h3>Runs</h3>
        <p className="sub">
          {runs.length} runs{workload !== "all" ? ` · ${workload}` : ""}. Percentiles over &lt;1000 samples are
          greyed with their N.
        </p>
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>store</th>
                <th>workload</th>
                <th>conc</th>
                <th>batch</th>
                <th>payload</th>
                <th>eps</th>
                <th>peak</th>
                <th>p50</th>
                <th>p99</th>
                <th>p99.9</th>
                <th>N</th>
                <th>mem</th>
                <th>cpu</th>
                <th>err</th>
                <th>ready</th>
              </tr>
            </thead>
            <tbody>
              {runs.map((r) => (
                <RunRow key={`${r.workload}/${r.id}`} r={r} />
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </>
  );
}

function RunRow({ r }: { r: RunSummary }) {
  const pct = (v: number | null) =>
    r.suppressed ? <span className="suppressed">n/a</span> : <span>{fmtMs(v)}</span>;
  return (
    <tr>
      <td>
        <span className="store-dot" style={{ background: colorForStore(r.store) }} /> {prettyStore(r.store)}
      </td>
      <td>{r.workload}</td>
      <td className="num">
        {r.kind}
        {r.concurrency}
      </td>
      <td className="num">{r.batch ?? "—"}</td>
      <td className="num">{r.payload ?? "—"}</td>
      <td className="num">{fmtEps(r.eps)}</td>
      <td className="num">{fmtEps(r.peakEps)}</td>
      <td className="num">{pct(r.p50)}</td>
      <td className="num">{pct(r.p99)}</td>
      <td className="num">{pct(r.p999)}</td>
      <td className="num">{r.n === null ? <span className="muted">?</span> : fmtInt(r.n)}</td>
      <td className="num">{fmtMb(r.memPeakMb)}</td>
      <td className="num">{r.cpuAvg === null ? "—" : `${r.cpuAvg.toFixed(0)}%`}</td>
      <td className="num">
        {r.errors > 0 ? <span className="badge err">{r.errors}</span> : <span className="muted">0</span>}
      </td>
      <td>
        {r.readiness ? (
          r.readiness.ready ? (
            <span className="badge ok">ok</span>
          ) : (
            <span className="badge err">no</span>
          )
        ) : (
          <span className="muted">—</span>
        )}
      </td>
    </tr>
  );
}

function maxConc(runs: RunSummary[]): RunSummary[] {
  if (runs.length === 0) return [];
  const max = Math.max(...runs.map((r) => r.concurrency));
  return runs.filter((r) => r.concurrency === max);
}

function sortRuns(a: RunSummary, b: RunSummary): number {
  return a.workload.localeCompare(b.workload) || a.store.localeCompare(b.store) || a.concurrency - b.concurrency;
}
