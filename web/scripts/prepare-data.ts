// Snapshot a benchmark results directory into chart-ready JSON for the static UI.
//
//   npm run data -- --raw ../results [--out public/data]
//
// Emits:
//   <out>/index.json          – session list for the picker
//   <out>/<session>.json      – full aggregated SessionData per session
//   <out>/segsweep.json       – combined segment-size sweep (if any esb-segsweep-* sessions exist)
//
// All parsing/aggregation lives in src/lib/aggregate.ts (shared, matches the Python report).

import { existsSync, mkdirSync, readdirSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
import { basename, join, resolve } from "node:path";
import yaml from "js-yaml";

import {
  buildDimensions,
  buildWorkloads,
  parseRunName,
  summarizeRun,
  type RunConfig,
  type RunInputs,
} from "../src/lib/aggregate.ts";
import type { RawManifest, RunSummary, SessionData, SessionIndexEntry } from "../src/types.ts";

// ------------------------------ args ------------------------------

function parseArgs(argv: string[]): { raw: string; out: string } {
  let raw = "../results";
  let out = "public/data";
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === "--raw") raw = argv[++i];
    else if (argv[i] === "--out") out = argv[++i];
  }
  return { raw: resolve(raw), out: resolve(out) };
}

// ------------------------------ io helpers ------------------------------

function readJson<T>(path: string): T | null {
  try {
    return JSON.parse(readFileSync(path, "utf8")) as T;
  } catch {
    return null;
  }
}

function readJsonArray<T>(path: string): T[] {
  return readJson<T[]>(path) ?? [];
}

function readYaml<T>(path: string): T | null {
  try {
    return yaml.load(readFileSync(path, "utf8")) as T;
  } catch {
    return null;
  }
}

function isDir(path: string): boolean {
  try {
    return statSync(path).isDirectory();
  } catch {
    return false;
  }
}

// ------------------------------ per-run ------------------------------

function loadRun(runDir: string, id: string): RunInputs {
  return {
    id,
    config: readYaml<RunConfig>(join(runDir, "config.yaml")),
    throughput: readJsonArray(join(runDir, "throughput.json")),
    errors: readJsonArray(join(runDir, "operation_errors.json")),
    latency: readJsonArray(join(runDir, "latency.json")),
    latencyStats: readJson(join(runDir, "latency_stats.json")),
    cpu: readJsonArray(join(runDir, "cpu.json")),
    mem: readJsonArray(join(runDir, "memory.json")),
    readiness: readJson(join(runDir, "readiness.json")),
    containerStats: readJson(join(runDir, "container_stats.json")),
  };
}

// ------------------------------ per-session ------------------------------

function buildSession(sessionDir: string, id: string): SessionData {
  const session = readJson<SessionData["session"]>(join(sessionDir, "session.json")) ?? { session_id: id };
  const environment = readJson<SessionData["environment"]>(join(sessionDir, "environment.json"));

  const runs: RunSummary[] = [];
  const manifests: Record<string, RawManifest> = {};

  for (const entry of readdirSync(sessionDir, { withFileTypes: true })) {
    if (!entry.isDirectory() || entry.name === "report") continue;
    const workload = entry.name;
    const workloadDir = join(sessionDir, workload);

    for (const runEntry of readdirSync(workloadDir, { withFileTypes: true })) {
      if (!runEntry.isDirectory()) continue;
      const parsed = parseRunName(runEntry.name);
      if (!parsed) continue; // skips "report" and anything not <store>-[rw]<N>
      const runDir = join(workloadDir, runEntry.name);

      const inputs = loadRun(runDir, runEntry.name);
      // Skip runs that never reached the measured window (e.g. a store still seeding when the
      // snapshot was taken): the dir exists but throughput.json is missing/empty. Including them
      // would render a misleading zero next to completed stores.
      if (inputs.throughput.length === 0) continue;
      runs.push(summarizeRun(inputs, parsed.store, workload, parsed.kind, parsed.n));

      // one manifest per store, first seen
      if (!(parsed.store in manifests)) {
        const mf = readJson<RawManifest>(join(runDir, "run_manifest.json"));
        if (mf) manifests[parsed.store] = mf;
      }
    }
  }

  const stores = [...new Set(runs.map((r) => r.store))].sort();
  return {
    id,
    session,
    environment,
    stores,
    runs,
    workloads: buildWorkloads(runs),
    dimensions: buildDimensions(runs),
    manifests,
    generatedAt: new Date().toISOString(),
  };
}

// ------------------------------ segment sweep ------------------------------

const MIB = 1024 * 1024;

function sizeLabel(bytes: number): string {
  if (bytes >= MIB && bytes % MIB === 0) return `${bytes / MIB} MiB`;
  if (bytes >= 1024) return `${Math.round(bytes / 1024)} KiB`;
  return `${bytes} B`;
}

/** Combine esb-segsweep-<bytes> sessions into eps/p99-vs-segment-size series. */
function buildSegmentSweep(sessions: { id: string; data: SessionData }[]) {
  const sweeps = sessions
    .map((s) => {
      const m = /^segsweep-(\d+)$/.exec(s.id);
      return m ? { bytes: Number(m[1]), data: s.data } : null;
    })
    .filter((x): x is { bytes: number; data: SessionData } => x !== null)
    .sort((a, b) => a.bytes - b.bytes);

  if (sweeps.length === 0) return null;

  const sizes = sweeps.map((s) => ({ bytes: s.bytes, label: sizeLabel(s.bytes) }));

  // workload -> store -> (sizeBytes -> {eps, p99})
  const workloads = new Map<string, Map<string, Map<number, { eps: number; p99: number | null }>>>();
  for (const sweep of sweeps) {
    for (const run of sweep.data.runs) {
      const byStore = workloads.get(run.workload) ?? new Map();
      const bySize = byStore.get(run.store) ?? new Map();
      // keep the highest-concurrency point per (workload, store, size) as the headline
      const prev = bySize.get(sweep.bytes);
      if (!prev || run.eps > prev.eps) bySize.set(sweep.bytes, { eps: run.eps, p99: run.p99 });
      byStore.set(run.store, bySize);
      workloads.set(run.workload, byStore);
    }
  }

  return {
    sizes,
    workloads: [...workloads.entries()].map(([name, byStore]) => ({
      name,
      series: [...byStore.entries()].map(([store, bySize]) => ({
        store,
        points: sizes.map((s) => ({ sizeBytes: s.bytes, ...(bySize.get(s.bytes) ?? { eps: 0, p99: null }) })),
      })),
    })),
  };
}

/**
 * Combine the tephra full-suite sessions (main DCB @16 MiB + tephra-seg @64/256 MiB) into
 * per-workload eps/p99-vs-segment-size for tephra ONLY. Unlike the write-only segsweep, this
 * covers the whole suite including reads. Sources are identified as sessions that contain a
 * tephra `cold-window` run (unique to this suite); each source's segment size is read from its
 * tephra run_manifest.
 */
function buildTephraSegments(sessions: { id: string; data: SessionData }[]) {
  // Sources are the explicit per-size tephra full-suite sessions `tephra-seg-<bytes>` (16/64/256).
  // Matching by id (not by "has a cold-window run") keeps this independent of the multi-store group
  // sessions, so those can show tephra at any segment size without perturbing this section.
  const sources = sessions
    .map((s) => {
      const m = /^tephra-seg-(\d+)$/.exec(s.id);
      return m ? { bytes: Number(m[1]), data: s.data } : null;
    })
    .filter((x): x is { bytes: number; data: SessionData } => x !== null)
    .sort((a, b) => a.bytes - b.bytes);

  if (sources.length < 2) return null;
  const sizes = sources.map((s) => ({ bytes: s.bytes, label: sizeLabel(s.bytes) }));

  // workload -> sizeBytes -> {eps, p99, conc} (keep the highest-concurrency point per size)
  const workloads = new Map<string, Map<number, { eps: number; p99: number | null; conc: number }>>();
  for (const src of sources) {
    for (const run of src.data.runs) {
      if (run.store !== "tephra") continue;
      const bySize = workloads.get(run.workload) ?? new Map();
      const prev = bySize.get(src.bytes);
      if (!prev || run.concurrency > prev.conc) bySize.set(src.bytes, { eps: run.eps, p99: run.p99, conc: run.concurrency });
      workloads.set(run.workload, bySize);
    }
  }

  return {
    sizes,
    workloads: [...workloads.entries()]
      .map(([name, bySize]) => ({
        name,
        series: [
          {
            store: "tephra",
            points: sizes.map((s) => {
              const v = bySize.get(s.bytes);
              return { sizeBytes: s.bytes, eps: v?.eps ?? 0, p99: v?.p99 ?? null };
            }),
          },
        ],
      }))
      .sort((a, b) => a.name.localeCompare(b.name)),
  };
}

// ------------------------------ main ------------------------------

function main() {
  const { raw, out } = parseArgs(process.argv.slice(2));
  if (!isDir(raw)) {
    console.error(`raw results dir not found: ${raw}`);
    process.exit(1);
  }

  const sessionDirs = readdirSync(raw, { withFileTypes: true })
    .filter((e) => e.name.startsWith("esb-") && isDir(join(raw, e.name))) // isDir follows symlinks
    .map((e) => e.name)
    .sort()
    .reverse(); // newest first

  if (sessionDirs.length === 0) {
    console.error(`no esb-* session directories under ${raw}`);
    process.exit(1);
  }

  // fresh output dir
  if (existsSync(out)) rmSync(out, { recursive: true, force: true });
  mkdirSync(out, { recursive: true });

  const index: SessionIndexEntry[] = [];
  const built: { id: string; data: SessionData }[] = [];

  for (const dirName of sessionDirs) {
    const id = dirName.replace(/^esb-/, "");
    const data = buildSession(join(raw, dirName), id);
    if (data.runs.length === 0) {
      console.warn(`  skip ${id}: no runs`);
      continue;
    }
    writeFileSync(join(out, `${id}.json`), JSON.stringify(data));
    built.push({ id, data });
    index.push({
      id,
      workloadName: data.session.workload_name ?? id,
      stores: data.stores,
      runCount: data.runs.length,
      generatedAt: data.generatedAt,
    });
    console.log(`  ${id}: ${data.runs.length} runs, ${data.stores.length} stores`);
  }

  writeFileSync(join(out, "index.json"), JSON.stringify(index, null, 2));

  const sweep = buildSegmentSweep(built);
  if (sweep) {
    writeFileSync(join(out, "segsweep.json"), JSON.stringify(sweep));
    console.log(`  segsweep.json: ${sweep.sizes.length} sizes, ${sweep.workloads.length} workloads`);
  }

  const tseg = buildTephraSegments(built);
  if (tseg) {
    writeFileSync(join(out, "tephraseg.json"), JSON.stringify(tseg));
    console.log(`  tephraseg.json: ${tseg.sizes.length} sizes, ${tseg.workloads.length} workloads (tephra full suite)`);
  }

  console.log(`\nWrote ${index.length} session(s) to ${out} (from ${basename(raw)})`);
}

main();
