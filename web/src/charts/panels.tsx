import type { RawEnvironment, RawManifest } from "../types";
import { colorForStore, fmtBytes, prettyStore } from "../lib/theme";

// ---- Environment panel (from environment.json) ----

export function EnvPanel({ env }: { env: RawEnvironment | null }) {
  if (!env) return null;
  const items: [string, string | undefined][] = [
    ["OS", env.os?.name],
    ["Kernel", env.os?.kernel?.split(" ").slice(0, 3).join(" ")],
    ["Arch", env.os?.arch],
    ["CPU", env.cpu?.model],
    ["Cores / threads", env.cpu ? `${env.cpu.cores} / ${env.cpu.threads}` : undefined],
    ["RAM", env.memory?.total_bytes ? fmtBytes(env.memory.total_bytes) : undefined],
    ["Disk", env.disk ? `${env.disk.type ?? "?"} · ${env.disk.filesystem ?? "?"}` : undefined],
    ["fsync avg", env.disk?.fsync_latency?.avg_us ? `${env.disk.fsync_latency.avg_us.toFixed(0)} µs` : undefined],
    ["Runtime", env.container_runtime ? `${env.container_runtime.type} ${env.container_runtime.version ?? ""}` : undefined],
  ];
  return (
    <div className="env-grid">
      {items
        .filter(([, v]) => v)
        .map(([k, v]) => (
          <div key={k} className="env-item">
            <span className="env-key">{k}</span>
            <span className="env-val">{v}</span>
          </div>
        ))}
    </div>
  );
}

// ---- Store-config panel (from run_manifest.json, union of keys like comparison.py) ----

const PREFERRED_KEYS = [
  "image",
  "segment_size_bytes",
  "event_segment_size_bytes",
  "page_cache_max_mb",
  "shared_buffers",
  "heap",
  "memory_limit_mb",
  "cache",
  "mode",
  "note",
];

function fmtVal(key: string, v: unknown): string {
  if (v === null || v === undefined || v === "") return "—";
  if (typeof v === "number" && key.endsWith("_bytes")) return fmtBytes(v);
  if (typeof v === "object") return JSON.stringify(v);
  return String(v);
}

export function StoreConfigPanel({ manifests }: { manifests: Record<string, RawManifest> }) {
  const stores = Object.keys(manifests).sort();
  if (stores.length === 0) {
    return <p className="muted">No per-store <code>run_manifest.json</code> in this session (older run) — store posture unavailable.</p>;
  }
  const keys = [
    ...PREFERRED_KEYS.filter((k) => stores.some((s) => k in manifests[s])),
    ...[...new Set(stores.flatMap((s) => Object.keys(manifests[s])))].filter((k) => !PREFERRED_KEYS.includes(k)),
  ];
  return (
    <div className="table-wrap">
      <table className="cfg-table">
        <thead>
          <tr>
            <th>store</th>
            {keys.map((k) => (
              <th key={k}>{k}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {stores.map((s) => (
            <tr key={s}>
              <td>
                <span className="store-dot" style={{ background: colorForStore(s) }} /> {prettyStore(s)}
              </td>
              {keys.map((k) => (
                <td key={k}>{fmtVal(k, manifests[s][k])}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
