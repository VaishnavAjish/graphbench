#!/usr/bin/env python3
"""End-to-end self-test with an in-memory fake database.

Runs the full pipeline -- schema, load, cold/warm read workloads, concurrency
sweep, report generation -- against a pure-Python graph with no network and no
credentials. Purpose: prove the harness itself works before you burn free-tier
quota, and catch regressions if you change a workload.

    python scripts/selftest.py

This is NOT a benchmark. The fake adapter has artificial latency and its
numbers mean nothing.
"""

from __future__ import annotations

import json
import random
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from bench.adapters.base import Footprint, GraphAdapter, LoadResult  # noqa: E402
from bench.report import generate  # noqa: E402
from bench.workloads import run_mixed_workload, run_read_workloads  # noqa: E402


class FakeAdapter(GraphAdapter):
    """In-memory adjacency list with a small artificial per-query delay."""

    language = "cypher (simulated)"

    def __init__(self, cfg):
        super().__init__(cfg)
        self.delay = float(cfg.get("delay_ms", 0.4)) / 1000.0
        self.adj: dict[int, list[int]] = {}
        self.props: dict[int, int] = {}
        self._connects = 0
        self._pings = 0

    def connect(self):
        self._connects += 1

    def close(self):
        pass

    def ping(self):
        """No-op probe. Exists so the self-test exercises the same
        cold-isolation path the real adapters use -- if ping() were missing the
        harness would silently fall back to aggregate() and the test would not
        catch that regression on a real platform."""
        self._pings += 1

    def wipe(self):
        self.adj.clear()
        self.props.clear()

    def create_schema(self):
        return ["simulated index Person(uid)", "simulated index Person(bucket)"]

    def load(self, nodes, edges, batch_size):
        t0 = time.perf_counter()
        for n in nodes:
            self.props[int(n)] = int(n) % 100
            self.adj.setdefault(int(n), [])
        for s, t in edges:
            self.adj.setdefault(int(s), []).append(int(t))
        return LoadResult(len(nodes), len(edges), time.perf_counter() - t0,
                          f"in-memory dict, batch_size={batch_size}",
                          "simulated; not a real measurement")

    def _sleep(self):
        if self.delay:
            time.sleep(self.delay)

    def point_lookup(self, node_id):
        self._sleep()
        return 1 if int(node_id) in self.props else 0

    def filtered_lookup(self, bucket):
        self._sleep()
        return sum(1 for v in self.props.values() if v == int(bucket))

    def traverse(self, node_id, hops, limit):
        self._sleep()
        frontier = {int(node_id)}
        for _ in range(hops):
            nxt: set[int] = set()
            for n in frontier:
                nxt.update(self.adj.get(n, ())[:64])
                if len(nxt) > limit * 4:
                    break
            frontier = nxt
            if not frontier:
                break
        return min(len(frontier), limit)

    def aggregate(self):
        self._sleep()
        counts: dict[int, int] = {}
        for b in self.props.values():
            counts[b] = counts.get(b, 0) + 1
        return min(len(counts), 20)

    def write_edge(self, src, dst):
        self._sleep()
        self.adj.setdefault(int(src), []).append(int(dst))

    def query_catalog(self):
        return {"point_lookup": "dict[uid]  # simulated, not a real query",
                "traverse_n": "BFS over an in-memory adjacency list"}

    def footprint(self):
        return Footprint(
            stored_bytes=sum(len(v) for v in self.adj.values()) * 16,
            memory_bytes=None,
            advertised_specs="simulated / no real resources",
            raw={"note": "fake adapter — numbers are meaningless"},
        )


def synthetic_graph(n_nodes=4000, avg_degree=6, seed=42):
    """Small synthetic graph so the self-test needs no download."""
    rng = random.Random(seed)
    nodes = list(range(n_nodes))
    edges = []
    for s in nodes:
        for _ in range(rng.randint(1, avg_degree)):
            edges.append((s, rng.randrange(n_nodes)))
    return nodes, edges


def main() -> int:
    print("=== harness self-test (fake in-memory database) ===\n")
    nodes, edges = synthetic_graph()
    print(f"synthetic graph: {len(nodes):,} nodes / {len(edges):,} edges")

    read_cfg = {
        "iterations": 40, "warmup_iterations": 5, "cold_iterations": 5,
        "cold_isolation": "reconnect",
        "start_nodes": 20, "min_out_degree": 2,
        "hop_depths": [1, 2, 3], "traversal_limit": 500,
    }
    out_results = {}

    for label, delay in (("FastFake", 0.2), ("SlowFake", 1.2)):
        db = FakeAdapter({"name": label, "delay_ms": delay,
                          "advertised_specs": "simulated",
                          "tier": "n/a", "region": "n/a"})
        db.connect()
        db.wipe()
        schema = db.create_schema()
        lr = db.load(nodes, edges, 5000)
        print(f"\n[{label}] loaded in {lr.wall_clock_s:.2f}s "
              f"({lr.rels_per_s:,.0f} rel/s)")

        start_nodes = [n for n in nodes[:200] if len(db.adj.get(n, [])) >= 2][:20]
        reads = run_read_workloads(db, start_nodes, list(range(100)), read_cfg)
        warm = reads["warm"]
        print(f"[{label}] warm p50: "
              f"point={warm['point_lookup']['p50_ms']:.2f}ms "
              f"3hop={warm['traverse_3hop']['p50_ms']:.2f}ms")

        mixed = []
        for conc in (1, 4):
            m = run_mixed_workload(db, start_nodes, conc, 2.0, 0.8, 42)
            mixed.append(m.as_dict())
            print(f"[{label}] mixed @{conc}: {m.qps:,.0f} qps, "
                  f"{m.errors} errors")

        fp = db.footprint()
        out_results[label] = {
            "describe": db.describe(),
            "schema": schema,
            "query_catalog": db.query_catalog(),
            "reads": reads,
            "mixed": mixed,
            "footprint": {"stored_bytes": fp.stored_bytes,
                          "memory_bytes": fp.memory_bytes,
                          "advertised_specs": fp.advertised_specs,
                          "raw": fp.raw},
        }
        db.close()

    tmp = ROOT / "results" / "_selftest"
    tmp.mkdir(parents=True, exist_ok=True)
    manifest = {"name": "synthetic", "description": "self-test synthetic graph",
                "source_url": "n/a", "node_count": len(nodes),
                "relationship_count": len(edges), "sampling": "generated",
                "sha256": "n/a"}
    env = {"timestamp_utc": "selftest", "hostname": "local",
           "platform": sys.platform, "python": sys.version.split()[0]}
    (tmp / "bench.json").write_text(json.dumps(
        {"environment": env, "dataset": manifest, "results": out_results},
        indent=2, default=str))
    (tmp / "load.json").write_text(json.dumps(
        {"environment": env, "dataset": manifest,
         "results": {k: {"describe": v["describe"],
                         "load": {"wall_clock_s": 0.1, "nodes_per_s": 1,
                                  "rels_per_s": 1, "method": "simulated"}}
                     for k, v in out_results.items()}},
        indent=2, default=str))

    generate(tmp, tmp / "REPORT.md")
    report = (tmp / "REPORT.md").read_text()
    print(f"\nreport generated: {len(report.splitlines())} lines")
    print("\n--- report excerpt ---")
    print("\n".join(report.splitlines()[:14]))

    assert "Warm read latency" in report, "report missing warm section"
    assert "FastFake" in report and "SlowFake" in report, "platforms missing"
    assert "Cold read latency" in report, "report missing cold section"
    assert "Measurement quality flags" in report, "report missing flags section"
    assert "Query catalog" in report, "report missing query catalog"
    assert "NaN" not in report, "report leaked NaN -- _r() failed to guard"

    # The cold phase must have reconnected once per workload after the first.
    # Six read workloads => five reconnects on top of the initial connect.
    for label, res in out_results.items():
        order = res["reads"].get("cold_order")
        assert order, f"{label}: cold_order not recorded"
        assert res["reads"].get("cold_caveat"), f"{label}: cold_caveat missing"

    # Config loader must fail loudly on an unset env reference rather than
    # substituting an empty string -- this is the check that keeps a
    # half-credentialled run from producing a results file that looks complete.
    import os
    from bench.config import ConfigError, load_yaml
    probe = tmp / "_envprobe.yaml"
    probe.write_text("databases:\n  - name: x\n    password: ${GB_DEFINITELY_UNSET}\n")
    os.environ.pop("GB_DEFINITELY_UNSET", None)
    try:
        load_yaml(probe)
    except ConfigError as exc:
        assert "GB_DEFINITELY_UNSET" in str(exc)
        print("env-var fail-loud check: OK")
    else:
        raise AssertionError("load_yaml did not raise on an unset ${VAR}")

    print("\n=== SELF-TEST PASSED ===")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
