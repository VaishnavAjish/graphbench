"""REPORT.md generation.

REPORT.md is produced entirely from the JSON in ``results/`` and is never
hand-edited. That rule exists so a reader can delete REPORT.md, re-run
``python run.py report``, and get the same file back -- which is the only way a
published results table can be trusted without re-running the whole benchmark.

Two things this generator does that a naive table-printer would not:

* **It reports failures as first-class rows.** A platform that OOM'd, timed
  out, or refused a connection appears in the matrix with the exception text,
  not as a blank cell. Blank cells read as "not measured"; the difference
  between "not measured" and "could not survive the workload" is the most
  interesting result this benchmark can produce at 256 MB.

* **It flags its own noise.** Any summary whose coefficient of variation
  exceeds the threshold in ``stats.py``, and any sample too small to support a
  p99, is listed in a dedicated section. The prose analysis is then obliged to
  address them.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .stats import HIGH_VARIANCE_CV, MIN_SAMPLE_FOR_P99

#: Column order for every latency table. Fixed so tables diff cleanly between
#: runs and between tracks.
READ_WORKLOADS = ["point_lookup", "filtered_lookup", "traverse_1hop",
                  "traverse_2hop", "traverse_3hop", "aggregation"]

DASH = "—"


# --------------------------------------------------------------------------
# small formatting helpers
# --------------------------------------------------------------------------

def _n(v: Any, digits: int = 2, suffix: str = "") -> str:
    if v is None:
        return DASH
    if isinstance(v, (int, float)):
        return f"{v:,.{digits}f}{suffix}"
    return str(v)


def _bytes(v: Any) -> str:
    if not isinstance(v, (int, float)) or v <= 0:
        return DASH
    for unit in ("B", "KB", "MB", "GB"):
        if v < 1024 or unit == "GB":
            return f"{v:,.1f} {unit}"
        v /= 1024
    return DASH


def _table(headers: list[str], rows: list[list[str]]) -> str:
    if not rows:
        return "_No data._\n"
    out = ["| " + " | ".join(headers) + " |",
           "|" + "|".join("---" for _ in headers) + "|"]
    out += ["| " + " | ".join(r) + " |" for r in rows]
    return "\n".join(out) + "\n"


def _load_json(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        return {"_error": f"{path.name} is not valid JSON: {exc}"}


# --------------------------------------------------------------------------
# sections
# --------------------------------------------------------------------------

def _section_platforms(bench: dict, load: dict) -> str:
    rows = []
    src = bench.get("results") or load.get("results") or {}
    for name, entry in src.items():
        d = entry.get("describe", {})
        status = "ok"
        if entry.get("error"):
            status = f"**FAILED** — `{entry['error']}`"
        rows.append([
            f"**{name}**",
            d.get("language", DASH),
            d.get("tier") or DASH,
            d.get("region") or DASH,
            d.get("advertised_specs") or DASH,
            status,
        ])
    return _table(["Platform", "Language", "Tier", "Region",
                   "Advertised specs", "Status"], rows)


def _section_ingest(load: dict) -> str:
    rows = []
    for name, entry in (load.get("results") or {}).items():
        if entry.get("error"):
            rows.append([f"**{name}**", DASH, DASH, DASH,
                         f"**FAILED** — `{entry['error']}`"])
            continue
        ld = entry.get("load", {})
        rows.append([
            f"**{name}**",
            _n(ld.get("wall_clock_s"), 1, " s"),
            _n(ld.get("nodes_per_s"), 0),
            _n(ld.get("rels_per_s"), 0),
            ld.get("method", DASH),
        ])
    return _table(["Platform", "Wall clock", "Nodes/s", "Rels/s",
                   "Ingest method"], rows)


def _latency_table(bench: dict, phase: str, metric: str) -> str:
    """One row per platform, one column per read workload."""
    results = bench.get("results") or {}
    present = [w for w in READ_WORKLOADS
               if any((e.get("reads", {}).get(phase, {}) or {}).get(w)
                      for e in results.values())]
    if not present:
        return "_No data._\n"

    rows = []
    for name, entry in results.items():
        if entry.get("error"):
            rows.append([f"**{name}**"] +
                        [f"`{entry['error'][:28]}`"] + [DASH] * (len(present) - 1))
            continue
        phase_data = (entry.get("reads", {}) or {}).get(phase, {}) or {}
        cells = []
        for w in present:
            s = phase_data.get(w)
            if not s:
                cells.append(DASH)
            elif not s.get("count"):
                cells.append("**fail**")
            else:
                cells.append(_n(s.get(metric)))
        rows.append([f"**{name}**"] + cells)
    return _table(["Platform"] + [w.replace("_", " ") for w in present], rows)


def _section_concurrency(bench: dict) -> str:
    results = bench.get("results") or {}
    levels: list[int] = []
    for entry in results.values():
        for m in entry.get("mixed", []) or []:
            if m.get("concurrency") not in levels:
                levels.append(m["concurrency"])
    levels.sort()
    if not levels:
        return "_No data._\n"

    out = []
    for metric, label, digits, suffix in (
            ("qps", "Throughput (ops/s)", 1, ""),
            ("p95", "Read p95 (ms)", 2, ""),
            ("errors", "Errors", 0, "")):
        rows = []
        for name, entry in results.items():
            by_conc = {m["concurrency"]: m for m in (entry.get("mixed") or [])}
            cells = []
            for lv in levels:
                m = by_conc.get(lv)
                if not m:
                    cells.append(DASH)
                elif metric == "p95":
                    cells.append(_n((m.get("read_latency") or {}).get("p95_ms"),
                                    digits, suffix))
                else:
                    cells.append(_n(m.get(metric), digits, suffix))
            rows.append([f"**{name}**"] + cells)
        out.append(f"**{label}**\n\n"
                   + _table(["Platform"] + [f"{lv} clients" for lv in levels],
                            rows))
    return "\n".join(out)


def _section_footprint(bench: dict) -> str:
    rows = []
    for name, entry in (bench.get("results") or {}).items():
        fp = entry.get("footprint") or {}
        raw = fp.get("raw") or {}
        rows.append([
            f"**{name}**",
            _n(raw.get("node_count"), 0),
            _n(raw.get("relationship_count"), 0),
            _bytes(fp.get("stored_bytes")),
            _bytes(fp.get("memory_bytes")),
        ])
    body = _table(["Platform", "Nodes reported", "Rels reported",
                   "Stored bytes", "Memory bytes"], rows)
    return body + (
        f"\n{DASH} means the platform does not expose the number through its "
        "client API. These cells are **not estimates** — where a managed tier "
        "hides its own resource usage, that is recorded as unobservable rather "
        "than inferred.\n")


def _collect_flags(bench: dict) -> tuple[list[str], list[str]]:
    """Return (variance/sample-size warnings, hard errors)."""
    warnings: list[str] = []
    errors: list[str] = []
    for name, entry in (bench.get("results") or {}).items():
        if entry.get("error"):
            errors.append(f"- **{name}** — entire benchmark phase failed: "
                          f"`{entry['error']}`")
        reads = entry.get("reads") or {}
        for phase in ("cold", "warm"):
            for wl, s in (reads.get(phase) or {}).items():
                for w in s.get("warnings") or []:
                    warnings.append(f"- **{name}** / {phase} / `{wl}` — {w}")
                for es in s.get("error_samples") or []:
                    errors.append(f"- **{name}** / {phase} / `{wl}` — `{es}`")
        for m in entry.get("mixed") or []:
            if m.get("errors"):
                errors.append(
                    f"- **{name}** / mixed @ {m['concurrency']} clients — "
                    f"{m['errors']} error(s) out of "
                    f"{m['total_ops'] + m['errors']} attempted ops")
            for es in m.get("error_samples") or []:
                errors.append(f"- **{name}** / mixed @ {m['concurrency']} — `{es}`")
    return warnings, errors


def _section_queries(bench: dict) -> str:
    """Reproduce the actual query text per platform, if adapters supplied it."""
    out = []
    for name, entry in (bench.get("results") or {}).items():
        cat = entry.get("query_catalog")
        if not cat:
            continue
        lines = "\n".join(f"{k:18s} {v}" for k, v in cat.items())
        out.append(f"**{name}** ({entry.get('describe', {}).get('language', '?')})\n\n"
                   f"```\n{lines}\n```\n")
    if not out:
        return ("_Adapters did not report a query catalog for this run._\n")
    return "\n".join(out)


# --------------------------------------------------------------------------
# charts (optional)
# --------------------------------------------------------------------------

def _charts(bench: dict, out_dir: Path) -> list[str]:
    """Render PNGs if matplotlib is installed; skip silently otherwise.

    Charts are a convenience, not a result. The benchmark must remain fully
    reproducible on a machine with no plotting stack, so a missing matplotlib
    downgrades the report rather than failing the run.
    """
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except Exception:  # noqa: BLE001
        return []

    results = bench.get("results") or {}
    names = [n for n, e in results.items() if not e.get("error")]
    if not names:
        return []
    out_dir.mkdir(parents=True, exist_ok=True)
    written: list[str] = []

    # 1. warm p50/p95 per workload, grouped bars, log y (latencies span orders
    #    of magnitude between a point lookup and a 3-hop traversal).
    workloads = [w for w in READ_WORKLOADS
                 if any((results[n].get("reads", {}).get("warm", {}) or {}).get(w)
                        for n in names)]
    if workloads:
        fig, ax = plt.subplots(figsize=(11, 5))
        width = 0.8 / max(len(names), 1)
        for i, n in enumerate(names):
            warm = results[n].get("reads", {}).get("warm", {}) or {}
            vals = [(warm.get(w) or {}).get("p50_ms") or 0 for w in workloads]
            ax.bar([x + i * width for x in range(len(workloads))], vals,
                   width, label=n)
        ax.set_xticks([x + 0.4 - width / 2 for x in range(len(workloads))])
        ax.set_xticklabels([w.replace("_", "\n") for w in workloads], fontsize=9)
        ax.set_ylabel("p50 latency (ms, log scale)")
        ax.set_yscale("log")
        ax.set_title("Warm read latency — p50 by workload (lower is better)")
        ax.legend(fontsize=8)
        ax.grid(axis="y", alpha=0.3)
        fig.tight_layout()
        p = out_dir / "warm_p50.png"
        fig.savefig(p, dpi=140)
        plt.close(fig)
        written.append(p.name)

    # 2. throughput vs concurrency
    levels = sorted({m["concurrency"] for n in names
                     for m in (results[n].get("mixed") or [])})
    if levels:
        fig, ax = plt.subplots(figsize=(7, 4.5))
        for n in names:
            by = {m["concurrency"]: m for m in (results[n].get("mixed") or [])}
            ax.plot(levels, [(by.get(lv) or {}).get("qps", 0) for lv in levels],
                    marker="o", label=n)
        ax.set_xlabel("concurrent clients")
        ax.set_ylabel("throughput (ops/s)")
        ax.set_title("Mixed 80/20 read-write throughput vs. concurrency")
        ax.legend(fontsize=8)
        ax.grid(alpha=0.3)
        fig.tight_layout()
        p = out_dir / "throughput.png"
        fig.savefig(p, dpi=140)
        plt.close(fig)
        written.append(p.name)

    return written


# --------------------------------------------------------------------------
# entry point
# --------------------------------------------------------------------------

def generate(results_dir: Path, out_path: Path) -> Path:
    """Build REPORT.md from results_dir. Returns the written path."""
    results_dir = Path(results_dir)
    load = _load_json(results_dir / "load.json")
    bench = _load_json(results_dir / "bench.json")
    if not load and not bench:
        raise FileNotFoundError(
            f"no results found in {results_dir}. Run `python run.py all` first.")

    env = bench.get("environment") or load.get("environment") or {}
    ds = bench.get("dataset") or load.get("dataset") or {}
    warnings, errors = _collect_flags(bench)
    charts = _charts(bench, results_dir / "charts")

    p: list[str] = []
    p.append("# Benchmark results\n")
    p.append("> **Generated file — do not edit.** Produced by "
             "`python run.py report` from the JSON in `results/`. "
             "Every number below traces to a raw measurement in that "
             "directory. Prose analysis lives in `README.md`; this file is "
             "data only.\n")

    p.append("## 0. Run provenance\n")
    p.append(_table(["Field", "Value"], [
        ["Run timestamp (UTC)", env.get("timestamp_utc", DASH)],
        ["Client host", env.get("hostname", DASH)],
        ["Client platform", env.get("platform", DASH)],
        ["Client processor", env.get("processor", DASH)],
        ["Python", env.get("python", DASH)],
    ]))
    p.append("\nThe client machine is identical for every platform in this "
             "file — that is what makes the comparison a comparison. A results "
             "file whose provenance block differs from another's must not have "
             "its tables merged.\n")

    p.append("\n## 1. Dataset\n")
    p.append(_table(["Field", "Value"], [
        ["Name", str(ds.get("name", DASH))],
        ["Source", str(ds.get("source_url", DASH))],
        ["Nodes", _n(ds.get("node_count"), 0)],
        ["Relationships", _n(ds.get("relationship_count"), 0)],
        ["Sampling", str(ds.get("sampling", DASH))],
        ["Seed", str(ds.get("seed", DASH))],
        ["sha256", f"`{ds.get('sha256', DASH)}`"],
    ]))
    stats = ds.get("stats") or {}
    if stats:
        p.append("\n**Sampled graph shape** — the degree distribution "
                 "determines whether the traversal workloads measure anything:\n\n")
        p.append(_table(["Metric", "Value"],
                        [[k.replace("_", " "), _n(v, 2)] for k, v in stats.items()]))

    p.append("\n## 2. Platforms\n")
    p.append(_section_platforms(bench, load))

    p.append("\n## 3. Ingest\n")
    p.append("Bulk load of the full dataset. Each platform uses its own "
             "idiomatic bulk path — the `Ingest method` column names it. This "
             "table compares *vendor best-effort loading*, not identical work, "
             "and should not be read as an engine-speed measurement.\n\n")
    p.append(_section_ingest(load))

    p.append("\n## 4. Warm read latency\n")
    p.append("Measured after the discarded warm-up iterations. These are the "
             "headline numbers. Percentiles are nearest-rank: every value is a "
             "latency some request actually experienced.\n\n")
    p.append("**p50 (ms)**\n\n" + _latency_table(bench, "warm", "p50_ms"))
    p.append("\n**p95 (ms)**\n\n" + _latency_table(bench, "warm", "p95_ms"))
    p.append("\n**p99 (ms)**\n\n" + _latency_table(bench, "warm", "p99_ms"))
    p.append("\n**Coefficient of variation** (stdev / mean — unitless, so it "
             f"is comparable across workloads; above {HIGH_VARIANCE_CV:.2f} the "
             "measurement is too noisy to rank)\n\n"
             + _latency_table(bench, "warm", "cv"))

    p.append("\n## 5. Cold read latency\n")
    p.append("First iterations after connecting, before any warm-up. Reported "
             "separately and never averaged into the warm numbers. See the "
             "README's timing-policy section for what 'cold' does and does not "
             "guarantee on a managed tier we do not control.\n\n")
    p.append("**p50 (ms)**\n\n" + _latency_table(bench, "cold", "p50_ms"))

    p.append("\n## 6. Concurrency sweep\n")
    p.append("Sustained mixed read/write workload at fixed client concurrency. "
             "Throughput is completed operations divided by actual elapsed "
             "time, so a slower platform reports fewer ops rather than running "
             "longer.\n\n")
    p.append(_section_concurrency(bench))

    p.append("\n## 7. Resource footprint\n")
    p.append(_section_footprint(bench))

    p.append("\n## 8. Measurement quality flags\n")
    p.append(f"Auto-generated. A summary is flagged when its coefficient of "
             f"variation exceeds {HIGH_VARIANCE_CV:.2f}, or when the sample is "
             f"smaller than {MIN_SAMPLE_FOR_P99} (at which point p99 is just "
             f"max()). **Any gap between two platforms that is smaller than "
             f"the spread flagged here is not a result.**\n\n")
    p.append("\n".join(warnings) + "\n" if warnings
             else "_No measurement-quality flags raised._\n")

    p.append("\n## 9. Recorded errors and failures\n")
    p.append("Every exception, timeout, and refused connection observed during "
             "the run. Nothing here is filtered — a platform that failed a "
             "workload appears here rather than as a blank cell above.\n\n")
    p.append("\n".join(errors) + "\n" if errors
             else "_No errors recorded during this run._\n")

    p.append("\n## 10. Query catalog\n")
    p.append("The exact operation issued to each platform, so the "
             "cross-language translation can be audited without reading the "
             "adapter source.\n\n")
    p.append(_section_queries(bench))

    if charts:
        p.append("\n## 11. Charts\n")
        for c in charts:
            p.append(f"![{c}](results/charts/{c})\n")

    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(p), encoding="utf-8")
    print(f"[report] wrote {out_path} "
          f"({len(warnings)} quality flag(s), {len(errors)} error(s), "
          f"{len(charts)} chart(s))")
    return out_path
