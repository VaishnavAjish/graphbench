#!/usr/bin/env python3
"""Graph database benchmark orchestrator.

One command runs everything:

    python run.py all

Individual phases, useful while debugging a single platform:

    python run.py check                  # connectivity only, no timing
    python run.py load                   # wipe + schema + ingest
    python run.py bench                  # read workloads + concurrency sweep
    python run.py report                 # regenerate tables/charts from results/

Every phase writes JSON to results/ so a crashed run never loses completed work.
"""

from __future__ import annotations

import argparse
import json
import platform
import socket
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from bench import dataset as ds_mod
from bench.config import ConfigError, load_databases, load_dotenv, load_yaml
from bench.workloads import run_mixed_workload, run_read_workloads

ROOT = Path(__file__).parent
CACHE = ROOT / ".cache"

# Set from --results. Track A (cloud) and Track B (Docker) must never write to
# the same directory: REPORT.md merges whatever it finds, and silently mixing a
# cloud run with a local run would produce a table comparing two different
# fairness regimes as if they were one.
RESULTS = ROOT / "results"


def environment_fingerprint() -> dict:
    """Recorded in every result file. The assignment requires the client machine
    to be identical across platforms; this is the evidence."""
    return {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "hostname": socket.gethostname(),
        "python": sys.version.split()[0],
        "platform": platform.platform(),
        "processor": platform.processor() or "unknown",
    }


def build_dataset(wl: dict) -> ds_mod.Dataset:
    d = wl["dataset"]
    print(f"[dataset] building {d['name']} "
          f"(target {d.get('target_edges') or 'full'} edges)")
    ds = ds_mod.build(
        name=d["name"],
        target_edges=d.get("target_edges"),
        target_nodes=d.get("target_nodes"),
        seed=int(d.get("seed", 42)),
        cache_dir=CACHE,
    )
    ds_mod.save_manifest(ds, RESULTS / "dataset_manifest.json")
    print(f"[dataset] {len(ds.nodes):,} nodes / {len(ds.edges):,} relationships "
          f"| sha256={ds.checksum[:16]}...")
    return ds


def phase_check(dbs, ds) -> int:
    failures = 0
    for cfg, db in dbs:
        try:
            db.connect()
            ok, msg = db.healthcheck()
            print(f"  [{'OK ' if ok else 'FAIL'}] {db.name:22s} {msg}")
            failures += 0 if ok else 1
        except Exception as exc:  # noqa: BLE001
            print(f"  [FAIL] {db.name:22s} {type(exc).__name__}: {exc}")
            failures += 1
        finally:
            db.close()
    return failures


def phase_load(dbs, ds, wl) -> None:
    batch = int(wl["load"]["batch_size"])
    out = {}
    for cfg, db in dbs:
        print(f"[load] {db.name} ...")
        entry = {"describe": db.describe()}
        try:
            db.connect()
            db.wipe()
            entry["schema"] = db.create_schema()
            res = db.load(ds.nodes, ds.edges, batch)
            entry["load"] = {
                "node_count": res.node_count,
                "relationship_count": res.relationship_count,
                "wall_clock_s": round(res.wall_clock_s, 3),
                "nodes_per_s": round(res.nodes_per_s, 1),
                "rels_per_s": round(res.rels_per_s, 1),
                "method": res.method,
                "notes": res.notes,
            }
            print(f"       {res.wall_clock_s:.1f}s "
                  f"({res.rels_per_s:,.0f} rel/s)")
        except Exception as exc:  # noqa: BLE001
            entry["error"] = f"{type(exc).__name__}: {exc}"
            print(f"       FAILED: {entry['error']}")
        finally:
            db.close()
        out[db.name] = entry
    _write(RESULTS / "load.json", {"environment": environment_fingerprint(),
                                   "dataset": ds.manifest(), "results": out})


def phase_bench(dbs, ds, wl) -> None:
    rcfg = wl["read"]
    mcfg = wl["mixed"]
    start_nodes = ds_mod.pick_start_nodes(
        ds, int(rcfg["start_nodes"]), int(rcfg["min_out_degree"]),
        int(wl["dataset"].get("seed", 42)))
    buckets = list(range(100))
    print(f"[bench] {len(start_nodes)} shared start nodes "
          f"(min out-degree {rcfg['min_out_degree']})")

    out = {}
    for cfg, db in dbs:
        print(f"[bench] {db.name} ...")
        entry = {"describe": db.describe()}
        try:
            db.connect()
            t0 = time.perf_counter()
            entry["reads"] = run_read_workloads(db, start_nodes, buckets, rcfg)
            sweep = []
            for conc in mcfg["concurrency_levels"]:
                print(f"        mixed workload @ {conc} clients ...")
                r = run_mixed_workload(
                    db, start_nodes, int(conc),
                    float(mcfg["duration_s"]), float(mcfg["read_ratio"]),
                    int(wl["dataset"].get("seed", 42)))
                sweep.append(r.as_dict())
                print(f"        -> {r.qps:,.1f} qps, {r.errors} errors")
            entry["mixed"] = sweep
            # Published in REPORT.md so the cross-language translation can be
            # audited without reading adapter source.
            entry["query_catalog"] = db.query_catalog()
            fp = db.footprint()
            entry["footprint"] = {
                "stored_bytes": fp.stored_bytes,
                "memory_bytes": fp.memory_bytes,
                "advertised_specs": fp.advertised_specs,
                "raw": fp.raw,
            }
            entry["elapsed_s"] = round(time.perf_counter() - t0, 1)
        except Exception as exc:  # noqa: BLE001
            entry["error"] = f"{type(exc).__name__}: {exc}"
            print(f"        FAILED: {entry['error']}")
        finally:
            db.close()
        out[db.name] = entry
    _write(RESULTS / "bench.json", {"environment": environment_fingerprint(),
                                    "dataset": ds.manifest(),
                                    "start_nodes": start_nodes[:20],
                                    "results": out})


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, default=str))
    try:
        print(f"[write] {path.relative_to(ROOT)}")
    except ValueError:
        print(f"[write] {path}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("phase", choices=["all", "check", "load", "bench", "report"])
    ap.add_argument("--only", nargs="*", help="restrict to named databases")
    ap.add_argument("--databases", default="config/databases.yaml")
    ap.add_argument("--workloads", default="config/workloads.yaml")
    ap.add_argument("--results", default="results",
                    help="output directory; use a distinct one per track "
                         "(e.g. results/track-b) so cloud and Docker runs are "
                         "never merged into one REPORT.md")
    args = ap.parse_args()

    global RESULTS
    RESULTS = ROOT / args.results

    load_dotenv(ROOT / ".env")
    try:
        wl = load_yaml(ROOT / args.workloads)
        dbs = load_databases(ROOT / args.databases, args.only)
    except ConfigError as exc:
        print(f"config error: {exc}", file=sys.stderr)
        return 2

    print(f"[config] {len(dbs)} database(s): "
          f"{', '.join(db.name for _, db in dbs)}\n")

    if args.phase == "report":
        from bench.report import generate
        generate(RESULTS, RESULTS / "REPORT.md")
        return 0

    ds = build_dataset(wl)

    if args.phase in ("all", "check"):
        print("\n[check] connectivity")
        failures = phase_check(dbs, ds)
        if failures and args.phase == "all":
            print(f"\n{failures} database(s) unreachable. "
                  f"Fix credentials or use --only to skip them.", file=sys.stderr)
            return 1
        if args.phase == "check":
            return 1 if failures else 0

    if args.phase in ("all", "load"):
        print()
        phase_load(dbs, ds, wl)

    if args.phase in ("all", "bench"):
        print()
        phase_bench(dbs, ds, wl)

    if args.phase == "all":
        from bench.report import generate
        print()
        generate(RESULTS, RESULTS / "REPORT.md")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
