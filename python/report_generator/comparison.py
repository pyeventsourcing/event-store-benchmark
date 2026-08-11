"""Cross-workload dimension comparison for the compare configs (configs/compare/*).

The built-in report plots metric-vs-worker-count within a single workload. The compare
configs instead vary a *dimension* (read selectivity, resume point, write batch size,
payload size) across sibling workloads at a fixed concurrency. This module groups those
workloads by dimension and emits, per dimension:

  * a markdown table (throughput / p50 / p99 / sample count / peak memory / cpu, per store x
    value), with percentiles SUPPRESSED where the sample count is below ``MIN_SAMPLES`` (a p99
    over ~100 samples is just the second-slowest request, not comparable to one over 100k), and
  * grouped-bar PNGs (all stores present) into ``<session>/report/comparison_<dim>_<metric>.png``.

It also renders a per-store config panel from ``run_manifest.json`` so each store's memory
posture (segment size, cache/heap, cgroup cap, image/version) is stated next to the figures.

It reads the raw per-run JSON directly (throughput.json, latency.json, latency_stats.json,
cpu.json, memory.json), so it has no dependency on the pydantic report models. Runnable standalone
(``python -m report_generator.comparison <session_dir>``) and importable
(``generate_comparison`` is called by the HTML session pipeline).
"""

from __future__ import annotations

import json
import re
import statistics
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

# Percentiles taken over fewer than this many samples are suppressed in the table and marked,
# so a tail statistic over ~100 requests is never printed as if comparable to one over 100k.
MIN_SAMPLES = 1000

# Dimension detection: workload name prefix -> (dimension key, optional explicit value order,
# whether values sort numerically). These are the FIXED-concurrency dimension sweeps; the
# concurrency/batch matrices (scale-*, matrix-*, writeflood, cold-*) are covered by the built-in
# per-workload by_workers_* charts instead. The `warm-`/`cold-` prefixes label the read regime.
_DIMENSIONS: list[dict] = [
    {"key": "selectivity", "prefixes": ["warm-sel-", "sel-", "smoke-sel-"],
     "order": ["single", "s1000", "s100", "s10", "full"], "numeric": False,
     "title": "Read selectivity (warm)"},
    {"key": "resume", "prefixes": ["warm-resume-", "resume-", "smoke-resume-"],
     "order": ["whole", "half", "recent"], "numeric": False,
     "title": "Resume position (warm)"},
    {"key": "write_payload", "prefixes": ["wp-", "smoke-wp-"],
     "order": None, "numeric": True, "title": "Write payload (bytes)"},
    {"key": "read_payload", "prefixes": ["rp-", "smoke-rp-"],
     "order": None, "numeric": True, "title": "Read payload (bytes)"},
]

# (metric key, human label, extractor field, y-axis label) for the charted metrics.
_CHART_METRICS = [
    ("throughput", "Throughput", "eps", "events/sec"),
    ("p99", "p99 latency", "p99", "ms"),
    ("memory", "Peak memory", "mem_peak_mb", "MB"),
]

_RUN_RE = re.compile(r"^(?P<store>.+)-(?P<kind>[rw])(?P<n>\d+)$")


def _load(path: Path):
    with open(path) as f:
        return json.load(f)


def _run_metrics(run_dir: Path) -> dict | None:
    """Throughput/latency/cpu/memory for one run dir, or None if it has no throughput."""
    tp_file = run_dir / "throughput.json"
    if not tp_file.exists():
        return None
    tp = _load(tp_file)
    total = sum(s["count"] for s in tp)
    elapsed = [s["elapsed_s"] for s in tp]
    span = (max(elapsed) - min(elapsed)) if len(elapsed) > 1 else 0.0
    eps = total / span if span > 0 else 0.0

    lat = _load(run_dir / "latency.json") if (run_dir / "latency.json").exists() else []
    latmap = {round(s["percentile"]): s["latency_ns"] for s in lat}

    # Sample count behind the percentiles (from latency_stats.json). Percentiles over too few
    # samples are marked suppressed so they are never charted or compared as if solid.
    n = 0
    if (run_dir / "latency_stats.json").exists():
        n = int(_load(run_dir / "latency_stats.json").get("store_latency_count", 0))
    suppressed = 0 < n < MIN_SAMPLES

    cpu = _load(run_dir / "cpu.json") if (run_dir / "cpu.json").exists() else []
    cpu_vals = [s["cpu_percent"] for s in cpu]
    mem = _load(run_dir / "memory.json") if (run_dir / "memory.json").exists() else []
    mem_vals = [s["memory_bytes"] for s in mem]
    errs = 0
    if (run_dir / "operation_errors.json").exists():
        errs = sum(s["count"] for s in _load(run_dir / "operation_errors.json"))

    return {
        "eps": eps,
        "p50": latmap.get(50, 0) / 1e6,
        "p99": latmap.get(99, 0) / 1e6,
        "n": n,
        "suppressed": suppressed,
        "cpu_avg": statistics.mean(cpu_vals) if cpu_vals else 0.0,
        "mem_peak_mb": (max(mem_vals) / 1e6) if mem_vals else 0.0,
        "errors": errs,
    }


def _store_manifests(session_dir: Path) -> dict[str, dict]:
    """{store: manifest} sampled from one run_manifest.json per store across the session, so the
    per-store config (segment size, cache/heap, mem cap, image/version) can be shown once."""
    out: dict[str, dict] = {}
    for workload_dir in session_dir.iterdir():
        if not workload_dir.is_dir():
            continue
        for run_dir in workload_dir.iterdir():
            if not run_dir.is_dir():
                continue
            m = _RUN_RE.match(run_dir.name)
            manifest = run_dir / "run_manifest.json"
            if m and manifest.exists() and m.group("store") not in out:
                try:
                    out[m.group("store")] = _load(manifest)
                except (ValueError, OSError):
                    pass
    return out


def _config_panel(session_dir: Path) -> str:
    """A markdown table of each store's recorded runtime posture, from run_manifest.json."""
    manifests = _store_manifests(session_dir)
    if not manifests:
        return ""
    # Union of keys across manifests, with a stable, readable order.
    preferred = ["image", "segment_size_bytes", "event_segment_size_bytes",
                 "page_cache_max_mb", "shared_buffers", "heap", "memory_limit_mb", "cache",
                 "mode", "note"]
    keys: list[str] = [k for k in preferred if any(k in mf for mf in manifests.values())]
    for mf in manifests.values():
        for k in mf:
            if k not in keys:
                keys.append(k)
    lines = ["### Store configuration (from run_manifest.json)", ""]
    lines.append("| store | " + " | ".join(keys) + " |")
    lines.append("|---|" + "|".join("---" for _ in keys) + "|")
    for store in sorted(manifests):
        mf = manifests[store]
        cells = [str(mf.get(k, "")) for k in keys]
        lines.append(f"| {store} | " + " | ".join(cells) + " |")
    return "\n".join(lines) + "\n"


def _workload_store_metrics(workload_dir: Path) -> dict[str, dict]:
    """{store: metrics} for a workload dir (one run per store at fixed concurrency)."""
    out: dict[str, dict] = {}
    for run_dir in workload_dir.iterdir():
        if not run_dir.is_dir():
            continue
        m = _RUN_RE.match(run_dir.name)
        if not m:
            continue
        metrics = _run_metrics(run_dir)
        if metrics is not None:
            out[m.group("store")] = metrics
    return out


def _strip_prefix(name: str, prefixes: list[str]) -> str | None:
    for p in prefixes:
        if name.startswith(p):
            return name[len(p):]
    return None


def _collect(session_dir: Path) -> dict[str, dict]:
    """Group workloads into dimensions.

    Returns {dim_key: {"title", "values": [ordered value labels],
                       "stores": [ordered stores],
                       "data": {value: {store: metrics}}}}.
    """
    result: dict[str, dict] = {}
    for workload_dir in sorted(p for p in session_dir.iterdir() if p.is_dir()):
        name = workload_dir.name
        for dim in _DIMENSIONS:
            value = _strip_prefix(name, dim["prefixes"])
            if value is None:
                continue
            store_metrics = _workload_store_metrics(workload_dir)
            if not store_metrics:
                continue
            entry = result.setdefault(
                dim["key"], {"title": dim["title"], "order": dim["order"],
                             "numeric": dim["numeric"], "data": {}}
            )
            entry["data"][value] = store_metrics
            break
    # Finalize value ordering and the store list per dimension.
    for entry in result.values():
        values = list(entry["data"].keys())
        if entry["order"]:
            values.sort(key=lambda v: entry["order"].index(v) if v in entry["order"] else 999)
        elif entry["numeric"]:
            values.sort(key=lambda v: int(v) if v.isdigit() else 1 << 62)
        else:
            values.sort()
        entry["values"] = values
        stores: list[str] = []
        for v in values:
            for s in entry["data"][v]:
                if s not in stores:
                    stores.append(s)
        entry["stores"] = sorted(stores)
    return result


def _fmt_pct(m: dict, key: str) -> str:
    """A percentile cell: the value, or `n/a` when suppressed for too few samples."""
    if m.get("suppressed"):
        return "n/a"
    return f"{m[key]:.2f}"


def _markdown_table(dim_key: str, entry: dict) -> str:
    lines = [f"### {entry['title']} ({dim_key})", ""]
    header = "| value | store | eps | p50 ms | p99 ms | N | peak MB | cpu % | errors |"
    sep = "|---|---|--:|--:|--:|--:|--:|--:|--:|"
    lines += [header, sep]
    any_suppressed = False
    for v in entry["values"]:
        for s in entry["stores"]:
            m = entry["data"][v].get(s)
            if not m:
                continue
            if m.get("suppressed"):
                any_suppressed = True
            lines.append(
                f"| {v} | {s} | {m['eps']:.0f} | {_fmt_pct(m, 'p50')} | {_fmt_pct(m, 'p99')} "
                f"| {m.get('n', 0)} | {m['mem_peak_mb']:.0f} | {m['cpu_avg']:.0f} | {m['errors']} |"
            )
    if any_suppressed:
        lines.append("")
        lines.append(f"> Percentiles shown as `n/a` had fewer than {MIN_SAMPLES} samples (N) and "
                     "are suppressed as not comparable.")
    return "\n".join(lines) + "\n"


def _grouped_bar(entry: dict, field: str, ylabel: str, title: str, out_path: Path) -> None:
    values = entry["values"]
    stores = entry["stores"]
    x = range(len(values))
    n = max(len(stores), 1)
    width = 0.8 / n
    fig, ax = plt.subplots(figsize=(max(6, 1.6 * len(values)), 5))
    for i, store in enumerate(stores):
        ys = []
        for v in values:
            m = entry["data"][v].get(store, {})
            # Don't draw a bar for a percentile that was suppressed for too few samples.
            if field in ("p50", "p99") and m.get("suppressed"):
                ys.append(float("nan"))
            else:
                ys.append(m.get(field, 0.0))
        offset = (i - (n - 1) / 2) * width
        ax.bar([xi + offset for xi in x], ys, width=width, label=store)
    ax.set_xticks(list(x))
    ax.set_xticklabels(values)
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.legend()
    ax.grid(axis="y", linestyle=":", alpha=0.5)
    fig.tight_layout()
    fig.savefig(out_path, dpi=110)
    plt.close(fig)


def generate_comparison(session_dir: Path, out_dir: Path | None = None) -> dict:
    """Write comparison PNGs and return {dim_key: {"tables": md, "images": [relpaths]}}.

    ``out_dir`` defaults to ``<session_dir>/report``; image relpaths are relative to the
    session dir (so the HTML at the session root can embed them directly).
    """
    session_dir = Path(session_dir)
    report_dir = out_dir or (session_dir / "report")
    report_dir.mkdir(parents=True, exist_ok=True)

    dims = _collect(session_dir)
    summary: dict = {}
    md_sections: list[str] = []
    config_panel = _config_panel(session_dir)
    if config_panel:
        md_sections.append(config_panel)
    for dim_key, entry in dims.items():
        images: list[str] = []
        for field_key, label, field, ylabel in _CHART_METRICS:
            fname = f"comparison_{dim_key}_{field_key}.png"
            _grouped_bar(entry, field, ylabel, f"{entry['title']} - {label}", report_dir / fname)
            images.append(f"report/{fname}")
        tables = _markdown_table(dim_key, entry)
        md_sections.append(tables)
        summary[dim_key] = {"title": entry["title"], "tables": tables, "images": images}
    # Also emit a single markdown file (config panel + all dimension tables, with sample counts
    # and low-N suppression) so the HTML report can embed the numbers alongside the PNGs.
    if md_sections:
        (report_dir / "comparison_tables.md").write_text("\n".join(md_sections))
    return summary


def main() -> None:
    if len(sys.argv) < 2:
        print("usage: python -m report_generator.comparison <session_dir>", file=sys.stderr)
        raise SystemExit(2)
    session_dir = Path(sys.argv[1])
    summary = generate_comparison(session_dir)
    if not summary:
        print(f"No compare-dimension workloads found in {session_dir}")
        return
    panel = _config_panel(session_dir)
    if panel:
        print(panel)
    for dim in summary.values():
        print(dim["tables"])
    print("Wrote comparison charts under", session_dir / "report")


if __name__ == "__main__":
    main()
