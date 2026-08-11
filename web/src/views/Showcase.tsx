import { useMemo } from "react";
import type { RunSummary, SessionData, WorkloadGroup } from "../types";
import type { SegSweep } from "../charts/lines";
import { dimensionFor } from "../lib/aggregate";
import { colorForStore, prettyStore } from "../lib/theme";
import { LatencyCurve, ScalingCurve, SegmentSweep, ThroughputTimeseries } from "../charts/lines";
import { DimensionBars, StoreBars } from "../charts/bars";
import { EnvPanel, StoreConfigPanel } from "../charts/panels";

function isDimension(name: string): boolean {
  return dimensionFor(name) !== null;
}
function runsAtMaxConcurrency(g: WorkloadGroup): RunSummary[] {
  const max = Math.max(...g.runs.map((r) => r.concurrency));
  return g.runs.filter((r) => r.concurrency === max);
}

export function Showcase({ data, sweep, tephraSeg }: { data: SessionData; sweep: SegSweep | null; tephraSeg: SegSweep | null }) {
  const { writes, reads, scalingWrites, scalingReads, singleWrites, coldReads, warmSingleReads } = useMemo(() => {
    const nonDim = data.workloads.filter((w) => !isDimension(w.name));
    const writes = nonDim.filter((w) => w.mode === "write" || w.mode === "writeflood");
    const reads = nonDim.filter((w) => w.mode === "read" || w.mode === "subscribe");
    return {
      writes,
      reads,
      scalingWrites: writes.filter((w) => w.family === "scaling"),
      scalingReads: reads.filter((w) => w.family === "scaling"),
      singleWrites: writes.filter((w) => w.family === "single"),
      coldReads: reads.filter((w) => w.family === "single" && w.regime === "cold"),
      warmSingleReads: reads.filter((w) => w.family === "single" && w.regime !== "cold"),
    };
  }, [data]);

  const anySuppressed = data.runs.some((r) => r.suppressed);
  const env = data.environment;
  const sweepWorkloads = sweep?.workloads ?? [];
  const sweepStores = [...new Set(sweepWorkloads.flatMap((w) => w.series.map((s) => s.store)))].sort();
  const TSEG_WRITES = ["scale-write", "matrix-b64", "matrix-b512", "writeflood", "wp-64", "wp-1024", "wp-16384"];
  const tsegWrites = (tephraSeg?.workloads ?? []).filter((w) => TSEG_WRITES.includes(w.name));
  const tsegReads = (tephraSeg?.workloads ?? []).filter((w) => !TSEG_WRITES.includes(w.name));

  return (
    <>
      {/* hero */}
      <div className="card hero">
        <h1>{data.session.workload_name ?? data.id}</h1>
        <div className="meta">
          Session <code>{data.id}</code> · seed {data.session.seed ?? "?"} · {data.runs.length} runs ·{" "}
          {data.stores.length} stores
          {env?.cpu?.model ? ` · ${env.cpu.model} (${env.cpu.threads ?? "?"} threads)` : ""}
        </div>
        <div className="chips">
          {data.stores.map((s) => (
            <span className="chip" key={s}>
              <span className="store-dot" style={{ background: colorForStore(s) }} /> {prettyStore(s)}
            </span>
          ))}
        </div>
      </div>

      {/* writes */}
      {writes.length > 0 && <div className="section-title">Writes</div>}
      {scalingWrites.map((g) => (
        <div className="card" key={g.name}>
          <h3>{g.name}</h3>
          <p className="sub">Throughput and tail latency as concurrency scales.</p>
          <div className="grid2">
            <ScalingCurve runs={g.runs} metric="eps" />
            <ScalingCurve runs={g.runs} metric="p99" />
          </div>
          <ThroughputTimeseries runs={runsAtMaxConcurrency(g)} height={260} />
        </div>
      ))}
      {singleWrites.length > 0 && (
        <div className="card">
          <h3>Batch &amp; payload sensitivity</h3>
          <p className="sub">Fixed-concurrency write variants, throughput per store.</p>
          <div className="grid2">
            {singleWrites.map((g) => (
              <div key={g.name}>
                <div className="sub" style={{ marginBottom: 4 }}>
                  {g.name}
                </div>
                <StoreBars runs={g.runs} metric="eps" height={240} />
              </div>
            ))}
          </div>
        </div>
      )}

      {/* reads */}
      {reads.length > 0 && <div className="section-title">Reads</div>}
      {scalingReads.map((g) => (
        <div className="card" key={g.name}>
          <h3>
            {g.name} {g.regime !== "none" && <span className="badge warn">{g.regime}</span>}
          </h3>
          <p className="sub">Read throughput and tail latency vs concurrency.</p>
          <div className="grid2">
            <ScalingCurve runs={g.runs} metric="eps" />
            <ScalingCurve runs={g.runs} metric="p99" />
          </div>
        </div>
      ))}
      {data.dimensions.map((dim) => (
        <div className="card" key={dim.key}>
          <h3>{dim.title}</h3>
          <p className="sub">Throughput and p99 across the sweep (fixed concurrency).</p>
          <div className="grid2">
            <DimensionBars dim={dim} metric="eps" />
            <DimensionBars dim={dim} metric="p99" />
          </div>
        </div>
      ))}
      {coldReads.length > 0 && (
        <div className="card">
          <h3>
            Cold reads <span className="badge warn">out-of-core</span>
          </h3>
          <p className="sub">Corpus larger than the memory budget — every read hits disk.</p>
          <div className="grid2">
            {coldReads.map((g) => (
              <div key={g.name}>
                <div className="sub" style={{ marginBottom: 4 }}>
                  {g.name}
                </div>
                <StoreBars runs={g.runs} metric="eps" height={240} />
              </div>
            ))}
          </div>
          {/* One latency curve per cold workload — merging them would draw two same-colored lines
              per store (one per workload), which reads as duplicates. */}
          <div className="grid2">
            {coldReads.map((g) => (
              <div key={`${g.name}-lat`}>
                <div className="sub" style={{ marginBottom: 4 }}>
                  {g.name} — latency
                </div>
                <LatencyCurve runs={g.runs} logY height={240} />
              </div>
            ))}
          </div>
        </div>
      )}
      {warmSingleReads.length > 0 && (
        <div className="card">
          <h3>Warm read variants</h3>
          <p className="sub">Cache-resident reads at fixed concurrency.</p>
          <div className="grid2">
            {warmSingleReads.map((g) => (
              <div key={g.name}>
                <div className="sub" style={{ marginBottom: 4 }}>
                  {g.name}
                </div>
                <StoreBars runs={g.runs} metric="eps" height={240} />
              </div>
            ))}
          </div>
        </div>
      )}

      {/* segment sweep */}
      {sweepWorkloads.length > 0 && (
        <>
          <div className="section-title">Segment-size sweep — segmented stores</div>
          <p className="sub" style={{ margin: "0 2px 12px" }}>
            How throughput and tail latency change with segment size, held equal between the segmented stores
            ({sweepStores.map(prettyStore).join(" & ")}) across {sweep?.sizes.map((s) => s.label).join(" / ")}.
          </p>
          {sweepWorkloads.map((w) => (
            <div className="card" key={w.name}>
              <h3>{w.name}</h3>
              <p className="sub">Throughput and p99 vs segment size.</p>
              <div className="grid2">
                <SegmentSweep sweep={sweep!} workload={w.name} metric="eps" />
                <SegmentSweep sweep={sweep!} workload={w.name} metric="p99" />
              </div>
            </div>
          ))}
        </>
      )}

      {/* tephra across segment sizes — full suite incl reads */}
      {tephraSeg && tephraSeg.workloads.length > 0 && (
        <>
          <div className="section-title">Tephra across segment sizes — full suite (16 / 64 / 256 MiB)</div>
          <p className="sub" style={{ margin: "0 2px 12px" }}>
            Tephra only, the same workloads at three segment sizes (16 MiB from the main run, 64 &amp; 256 MiB from the
            follow-up). Larger segments help <strong>batched writes</strong> substantially — but <strong>writeflood</strong> and
            <strong> cold reads</strong> stay flat, so those gaps vs umadb are not a segment-size artifact.
          </p>
          <div className="card">
            <h3>Writes</h3>
            <div className="grid2">
              {tsegWrites.map((w) => (
                <div key={w.name}>
                  <div className="sub" style={{ marginBottom: 4 }}>{w.name}</div>
                  <SegmentSweep sweep={tephraSeg} workload={w.name} metric="eps" height={220} />
                </div>
              ))}
            </div>
          </div>
          <div className="card">
            <h3>Reads</h3>
            <div className="grid2">
              {tsegReads.map((w) => (
                <div key={w.name}>
                  <div className="sub" style={{ marginBottom: 4 }}>{w.name}</div>
                  <SegmentSweep sweep={tephraSeg} workload={w.name} metric="eps" height={220} />
                </div>
              ))}
            </div>
          </div>
        </>
      )}

      {/* fairness / environment */}
      <div className="section-title">Fairness &amp; environment</div>
      {data.session.note && <p className="note">{data.session.note}</p>}
      {anySuppressed && (
        <p className="note">
          Some percentiles are hidden because they were computed over fewer than 1000 samples — a tail statistic over a
          handful of requests isn&apos;t comparable to one over hundreds of thousands. See the Explore tab for exact N.
        </p>
      )}
      <div className="card">
        <h3>Per-store configuration</h3>
        <p className="sub">Recorded runtime posture (segment size, cache/heap, memory cap, image) held comparable.</p>
        <StoreConfigPanel manifests={data.manifests} />
      </div>
      {env && (
        <div className="card">
          <h3>Environment</h3>
          <EnvPanel env={env} />
        </div>
      )}
    </>
  );
}
