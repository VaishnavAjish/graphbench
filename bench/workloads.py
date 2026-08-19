"""Workload execution.

Timing policy, stated once here and applied identically to every platform.
This module is the single place the policy lives; no adapter may vary it.

  * Cold   -- the first N iterations of a workload on a connection that has
              issued no prior queries of that workload. Reported separately and
              never averaged into the warm numbers.
  * Warm-up -- W iterations whose timings are discarded entirely.
  * Warm   -- the measured sample, >= 100 iterations per read workload. These
              are the headline numbers.

Each iteration is timed around the driver call only. Argument selection and RNG
happen outside the timer, so harness overhead never lands in the measurement.

---------------------------------------------------------------------------
What "cold" does and does not mean -- read this before quoting the cold table
---------------------------------------------------------------------------

There are three caches between the client and the answer, and this harness can
only control one of them:

  1. *Client-side driver state* (connection pool, routing table, prepared
     statement cache). Controlled: with ``cold_isolation: reconnect`` the
     adapter is closed and reopened before each cold workload, so no workload
     inherits another's client state.

  2. *Server-side query plan cache.* Partly controlled. Reconnecting does not
     clear it on any of the engines benchmarked here. The first execution of a
     given query shape still pays planning cost, which is what the cold number
     is mostly capturing.

  3. *Server-side page / buffer cache.* **Not controlled, and not
     controllable.** These are managed free tiers; there is no API to drop
     caches, and on a shared tier the pages may not even be ours. A cold
     measurement taken after an earlier workload has scanned the same nodes is
     therefore warm at the storage layer.

The consequence: cold workloads run in a fixed, recorded order, and the later
ones are progressively less cold than the earlier ones. This is stated in the
results file (``cold_caveat``) and reproduced in REPORT.md rather than being
left for a reader to discover. The honest summary is that the cold table
measures *first-query overhead on an established deployment*, not a genuine
cold start, and it should not be used to rank platforms.

The connectivity probe is deliberately separated from the workloads for the
same reason -- see ``GraphAdapter.ping()``.
"""

from __future__ import annotations

import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Callable

from .adapters.base import GraphAdapter
from .stats import LatencySummary, summarize


@dataclass
class MixedResult:
    concurrency: int
    duration_s: float
    total_ops: int
    read_ops: int
    write_ops: int
    errors: int
    qps: float
    read_latency: LatencySummary
    write_latency: LatencySummary
    error_samples: list[str] = field(default_factory=list)

    def as_dict(self) -> dict:
        return {
            "concurrency": self.concurrency,
            "duration_s": round(self.duration_s, 3),
            "total_ops": self.total_ops,
            "read_ops": self.read_ops,
            "write_ops": self.write_ops,
            "errors": self.errors,
            "qps": round(self.qps, 2),
            "read_latency": self.read_latency.as_dict(),
            "write_latency": self.write_latency.as_dict(),
            "error_samples": self.error_samples[:5],
        }


def _timed_loop(fn: Callable[[int], int], args: list[int],
                iterations: int, label: str,
                notes: str = "") -> LatencySummary:
    samples: list[float] = []
    errors = 0
    err_msgs: list[str] = []
    for i in range(iterations):
        arg = args[i % len(args)]
        t0 = time.perf_counter()
        try:
            fn(arg)
        except Exception as exc:  # noqa: BLE001 - counted, never hidden
            errors += 1
            if len(err_msgs) < 5:
                err_msgs.append(f"{type(exc).__name__}: {exc}")
            continue
        samples.append((time.perf_counter() - t0) * 1000.0)
    return summarize(label, samples, errors, err_msgs, notes)


def run_read_workloads(db: GraphAdapter, start_nodes: list[int],
                       buckets: list[int], cfg: dict) -> dict[str, dict]:
    """Cold sample, then warm-up, then the measured warm sample."""
    iters = int(cfg["iterations"])
    warmup = int(cfg["warmup_iterations"])
    cold_iters = int(cfg.get("cold_iterations", 10))
    hops = list(cfg.get("hop_depths", [1, 2, 3]))
    limit = int(cfg.get("traversal_limit", 1000))
    # reconnect between cold workloads so none inherits another's client-side
    # driver state. Off by default only if a platform's connect() is expensive
    # enough that the reconnects dominate the run.
    isolate = str(cfg.get("cold_isolation", "reconnect")).lower() == "reconnect"

    results: dict[str, dict] = {"cold": {}, "warm": {}}

    def workloads() -> list[tuple[str, Callable[[int], int], list[int]]]:
        items: list[tuple[str, Callable[[int], int], list[int]]] = [
            ("point_lookup", db.point_lookup, start_nodes),
            ("filtered_lookup", db.filtered_lookup, buckets),
            ("aggregation", lambda _: db.aggregate(), [0]),
        ]
        for h in hops:
            items.append((f"traverse_{h}hop",
                          lambda n, h=h: db.traverse(n, h, limit),
                          start_nodes))
        return items

    # --- cold: first touch, before any warm-up ---
    #
    # `workloads()` is rebuilt after every reconnect because the bound methods
    # it closes over belong to the adapter instance whose driver we just
    # replaced. Rebuilding is cheap; forgetting to would silently keep calling
    # through a closed driver.
    order = [label for label, _fn, _args in workloads()]
    results["cold_order"] = order
    results["cold_caveat"] = (
        "Cold workloads ran in the order listed in cold_order"
        + (" with a client reconnect before each" if isolate else "")
        + ". Server-side page caches cannot be dropped on a managed tier, so "
          "later entries in that order are warmer at the storage layer than "
          "earlier ones. Do not rank platforms on this table.")

    for idx, (label, _fn, _args) in enumerate(workloads()):
        if isolate and idx > 0:
            db.close()
            db.connect()
        # Re-resolve after the reconnect so the callable targets the live driver.
        _l, fn, args = workloads()[idx]
        results["cold"][label] = _timed_loop(
            fn, args, cold_iters, label,
            notes=("cold: first execution of this workload on a fresh "
                   "connection; server page cache NOT cleared (see module "
                   "docstring)") if isolate else
                  ("cold: measured immediately after connect, no warm-up; "
                   "server page cache NOT cleared")
        ).as_dict()

    if isolate:
        # Warm-up and warm sampling must share one connection, so restore a
        # single session after the per-workload isolation above.
        db.close()
        db.connect()

    # --- warm-up: discarded ---
    for _, fn, args in workloads():
        for i in range(warmup):
            try:
                fn(args[i % len(args)])
            except Exception:  # noqa: BLE001 - warm-up failures surface later
                pass

    # --- warm: the headline numbers ---
    for label, fn, args in workloads():
        results["warm"][label] = _timed_loop(
            fn, args, iters, label,
            notes=f"warm: {warmup} discarded warm-up iterations before sampling"
        ).as_dict()

    return results


def run_mixed_workload(db: GraphAdapter, start_nodes: list[int],
                       concurrency: int, duration_s: float,
                       read_ratio: float, seed: int) -> MixedResult:
    """Sustained mixed read/write throughput at a fixed client concurrency.

    Each worker loops until the wall-clock deadline. Throughput is total
    completed operations divided by actual elapsed time, so a slow platform
    reports fewer ops rather than running longer.
    """
    stop_at = time.perf_counter() + duration_s
    read_samples: list[float] = []
    write_samples: list[float] = []
    errors = 0
    err_msgs: list[str] = []

    def worker(worker_id: int) -> tuple[list[float], list[float], int, list[str]]:
        rng = random.Random(seed + worker_id)
        reads: list[float] = []
        writes: list[float] = []
        local_errors = 0
        local_msgs: list[str] = []
        while time.perf_counter() < stop_at:
            is_read = rng.random() < read_ratio
            node = rng.choice(start_nodes)
            t0 = time.perf_counter()
            try:
                if is_read:
                    db.traverse(node, 1, 100)
                    reads.append((time.perf_counter() - t0) * 1000.0)
                else:
                    db.write_edge(node, rng.choice(start_nodes))
                    writes.append((time.perf_counter() - t0) * 1000.0)
            except Exception as exc:  # noqa: BLE001
                local_errors += 1
                if len(local_msgs) < 3:
                    local_msgs.append(f"{type(exc).__name__}: {exc}")
        return reads, writes, local_errors, local_msgs

    t_start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = [pool.submit(worker, i) for i in range(concurrency)]
        for fut in as_completed(futures):
            r, w, e, msgs = fut.result()
            read_samples.extend(r)
            write_samples.extend(w)
            errors += e
            err_msgs.extend(msgs)
    elapsed = time.perf_counter() - t_start

    total = len(read_samples) + len(write_samples)
    return MixedResult(
        concurrency=concurrency,
        duration_s=elapsed,
        total_ops=total,
        read_ops=len(read_samples),
        write_ops=len(write_samples),
        errors=errors,
        qps=total / elapsed if elapsed else 0.0,
        read_latency=summarize("mixed_read", read_samples),
        write_latency=summarize("mixed_write", write_samples),
        error_samples=err_msgs[:5],
    )
