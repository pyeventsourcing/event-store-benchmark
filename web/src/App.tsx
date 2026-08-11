import { useEffect, useMemo, useState } from "react";
import type { SessionData, SessionIndexEntry } from "./types";
import type { SegSweep } from "./charts/lines";
import { Showcase } from "./views/Showcase";
import { Explore } from "./views/Explore";

const BASE = import.meta.env.BASE_URL;

/** Segment-only sessions (write sweep + tephra full-suite sweep) — surfaced in the segment
 *  comparison sections, not the main session picker. */
const isAux = (id: string) =>
  id.startsWith("segsweep-") || id.startsWith("tephra-seg-") || id.includes(".bak");

async function getJson<T>(path: string): Promise<T | null> {
  try {
    const res = await fetch(`${BASE}data/${path}`);
    if (!res.ok) return null;
    return (await res.json()) as T;
  } catch {
    return null;
  }
}

type Tab = "showcase" | "explore";

export function App() {
  const [index, setIndex] = useState<SessionIndexEntry[] | null>(null);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [session, setSession] = useState<SessionData | null>(null);
  const [sweep, setSweep] = useState<SegSweep | null>(null);
  const [tephraSeg, setTephraSeg] = useState<SegSweep | null>(null);
  const [tab, setTab] = useState<Tab>(() =>
    window.location.hash.replace("#", "") === "explore" ? "explore" : "showcase",
  );

  const selectTab = (t: Tab) => {
    setTab(t);
    window.location.hash = t;
  };

  useEffect(() => {
    getJson<SessionIndexEntry[]>("index.json").then((idx) => {
      // Fall back to [] (not null) on a missing/empty data dir so the "generate your data" empty
      // state renders instead of hanging on "Loading…" — this app ships without a bundled dataset.
      setIndex(idx ?? []);
      // default to newest multi-store session; the segment-only sessions are surfaced in the
      // segment-comparison sections instead of the picker.
      const primary = idx?.find((s) => !isAux(s.id)) ?? idx?.[0];
      if (primary) setSelectedId(primary.id);
    });
    getJson<SegSweep>("segsweep.json").then(setSweep);
    getJson<SegSweep>("tephraseg.json").then(setTephraSeg);
  }, []);

  useEffect(() => {
    if (!selectedId) return;
    setSession(null);
    getJson<SessionData>(`${selectedId}.json`).then(setSession);
  }, [selectedId]);

  const pickable = useMemo(() => index?.filter((s) => !isAux(s.id)) ?? [], [index]);

  return (
    <div className="app">
      <div className="header">
        <div className="header-row">
          <div className="brand">
            <span className="spark" />
            Event Store Benchmark
          </div>
          <div className="tabs">
            <button className={`tab ${tab === "showcase" ? "active" : ""}`} onClick={() => selectTab("showcase")}>
              Showcase
            </button>
            <button className={`tab ${tab === "explore" ? "active" : ""}`} onClick={() => selectTab("explore")}>
              Explore
            </button>
          </div>
          <div className="spacer" />
          {pickable.length > 0 && (
            <select value={selectedId ?? ""} onChange={(e) => setSelectedId(e.target.value)}>
              {pickable.map((s) => (
                <option key={s.id} value={s.id}>
                  {s.workloadName} · {s.stores.length} stores
                </option>
              ))}
            </select>
          )}
        </div>
      </div>

      {!index && <div className="empty">Loading…</div>}
      {index && index.length === 0 && (
        <div className="empty">
          No dataset bundled. Point this UI at your own benchmark results:{" "}
          <code>npm run data -- --raw &lt;your es-bench results dir&gt;</code>, then rebuild.
        </div>
      )}
      {index && index.length > 0 && !session && <div className="empty">Loading session…</div>}
      {session && tab === "showcase" && <Showcase data={session} sweep={sweep} tephraSeg={tephraSeg} />}
      {session && tab === "explore" && <Explore data={session} />}
    </div>
  );
}
