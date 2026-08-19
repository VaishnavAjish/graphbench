"""Dataset acquisition and reproducible sampling.

The benchmark needs one dataset that is (a) publicly named in the assignment,
(b) large enough to be non-trivial, and (c) small enough to fit a 256 MB /
1 GB free tier. SNAP `soc-Pokec` is 1.6M nodes / 30M edges -- far too large --
so it is sampled down, and the sampling must be reproducible by anyone who
clones this repo.

Design decisions, all of which the README states and any of which is fair game
in review:

**Multi-pass streaming snowball, not in-memory BFS.**
The full Pokec edge list would need several GB of Python dict to hold as an
adjacency structure. Instead the compressed file is streamed once per BFS
level, each pass admitting the out-neighbours of the currently-selected set.
Cost is a handful of sequential reads; peak memory is the selected node set,
not the source graph. Passes are bounded by ``MAX_LEVELS``.

**Snowball rather than uniform random node sampling.**
Uniform sampling of 150k nodes from a 1.6M-node graph yields an induced
subgraph that is almost entirely disconnected -- multi-hop traversal would
measure nothing. Snowball preserves local structure, at the documented cost of
biasing toward the high-degree core near the seed.

**Degree-proportional edge subsampling (largest-remainder apportionment).**
The induced subgraph of a Pokec snowball has far more edges than the target, so
edges must be dropped. Two obvious methods are both wrong:

  * *Truncating in file order* strands most nodes at out-degree zero.
  * *Round-robin, one edge per source per round* strands nobody but flattens
    the degree distribution to near-uniform. This was the first implementation
    here, and it is worse than it sounds: on a 30k-node test graph it produced
    a maximum out-degree of **2**, which makes a 3-hop traversal reach roughly
    eight nodes and turns the headline workload into a no-op. A benchmark whose
    sampler destroys the power-law structure of a social graph is measuring the
    sampler, not the database.

What is used instead: each source keeps a number of edges proportional to its
induced out-degree, apportioned by the largest-remainder method with a floor of
one. The power-law shape survives, no selected node with any induced edge is
stranded, and the result is deterministic given the seed.

**Traversal viability is checked, not assumed.**
A sampled graph can satisfy its node and edge targets and still be too sparse
for multi-hop traversal to measure anything -- 150k nodes with 250k edges has a
mean out-degree of 1.67, and a 3-hop expansion on that graph touches a handful
of nodes. ``build()`` estimates mean 3-hop reachability on the sampled graph
and emits a loud warning when it falls below ``MIN_VIABLE_3HOP``. Silently
benchmarking a graph that cannot be traversed is the failure mode this guard
exists to prevent.

**Determinism.** Given the same seed and the same source file, this module
produces a byte-identical node list, edge list, and sha256. The checksum is
written into every results file so a reader can prove two runs used the same
graph.

"""

from __future__ import annotations

import gzip
import hashlib
import json
import random
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterator, Sequence

#: Upper bound on BFS levels. A snowball that has not reached its node target
#: after this many levels has almost certainly exhausted the seed's connected
#: component; growing further would just re-scan the file for nothing.
MAX_LEVELS = 10

#: Mean number of distinct nodes a 3-hop traversal must reach for the traversal
#: workload to be measuring graph work rather than an empty result set. Below
#: this, build() warns: the dataset targets need changing, not the benchmark.
MIN_VIABLE_3HOP = 25

#: Registry of supported source graphs. Adding one means adding an entry here
#: and nothing else -- the sampler is source-agnostic.
SOURCES: dict[str, dict] = {
    "soc-Pokec": {
        "url": "https://snap.stanford.edu/data/soc-pokec-relationships.txt.gz",
        "filename": "soc-pokec-relationships.txt.gz",
        "directed": True,
        "description": (
            "Pokec online social network (Slovakia). 1,632,803 nodes / "
            "30,622,564 directed friendship edges."),
        "citation": "J. Takac, M. Zabovsky. SNAP soc-Pokec.",
    },
    "wiki-Vote": {
        "url": "https://snap.stanford.edu/data/wiki-Vote.txt.gz",
        "filename": "wiki-Vote.txt.gz",
        "directed": True,
        "description": (
            "Wikipedia who-votes-on-whom network. 7,115 nodes / 103,689 "
            "directed edges. Small and dense -- fallback only."),
        "citation": "J. Leskovec et al. SNAP wiki-Vote.",
    },
    "soc-Slashdot0902": {
        "url": "https://snap.stanford.edu/data/soc-Slashdot0902.txt.gz",
        "filename": "soc-Slashdot0902.txt.gz",
        "directed": True,
        "description": "Slashdot Zoo social network, Feb 2009. 82,168 nodes / 948,464 edges.",
        "citation": "J. Leskovec et al. SNAP soc-Slashdot0902.",
    },
}


@dataclass
class Dataset:
    """A sampled, reproducible graph ready to load into every platform."""

    name: str
    nodes: list[int]
    edges: list[tuple[int, int]]
    checksum: str
    seed: int
    source_url: str
    description: str
    sampling: str
    stats: dict = field(default_factory=dict)

    def manifest(self) -> dict:
        return {
            "name": self.name,
            "description": self.description,
            "source_url": self.source_url,
            "node_count": len(self.nodes),
            "relationship_count": len(self.edges),
            "seed": self.seed,
            "sampling": self.sampling,
            "sha256": self.checksum,
            "stats": self.stats,
        }


# --------------------------------------------------------------------------
# acquisition
# --------------------------------------------------------------------------

#: Escape hatch for networks that cannot reach snap.stanford.edu (corporate
#: proxies and CI sandboxes routinely block it). Point this at an already
#: downloaded SNAP edge-list file and the sampler uses it verbatim -- the
#: checksum in the manifest still proves which graph the numbers came from.
DATASET_PATH_ENV = "GRAPHBENCH_DATASET_PATH"


def _download(url: str, dest: Path) -> Path:
    """Fetch once, cache forever.

    Downloads to a ``.part`` file and renames only on success, so an
    interrupted download can never be mistaken for a complete one. A truncated
    edge list would silently change the benchmark while still producing a
    plausible-looking results file -- exactly the class of error this whole
    harness is built to avoid.
    """
    import os

    override = os.environ.get(DATASET_PATH_ENV)
    if override:
        p = Path(override)
        if not p.exists():
            raise FileNotFoundError(
                f"{DATASET_PATH_ENV}={override} does not exist")
        print(f"[dataset] using local file from ${DATASET_PATH_ENV}: {p}")
        return p

    if dest.exists() and dest.stat().st_size > 0:
        print(f"[dataset] using cached {dest}")
        return dest
    dest.parent.mkdir(parents=True, exist_ok=True)
    part = dest.with_suffix(dest.suffix + ".part")

    import urllib.error
    import urllib.request

    # SNAP rejects the stock urllib User-Agent with 403 on some paths.
    req = urllib.request.Request(
        url, headers={"User-Agent": "graphbench/1.0 (+benchmark harness)"})

    print(f"[dataset] downloading {url}")
    try:
        with urllib.request.urlopen(req, timeout=120) as resp, part.open("wb") as fh:
            total = int(resp.headers.get("Content-Length") or 0)
            seen = 0
            while chunk := resp.read(1 << 20):
                fh.write(chunk)
                seen += len(chunk)
                if total:
                    print(f"\r[dataset]   {seen / 1e6:,.0f} / "
                          f"{total / 1e6:,.0f} MB", end="", flush=True)
        print()
    except (urllib.error.URLError, urllib.error.HTTPError, OSError) as exc:
        part.unlink(missing_ok=True)
        raise RuntimeError(
            f"could not download {url}: {exc}\n\n"
            f"If your network blocks snap.stanford.edu, download the file by "
            f"hand and re-run with:\n"
            f"    {DATASET_PATH_ENV}=/path/to/{dest.name} python run.py all\n"
            f"The dataset checksum in results/dataset_manifest.json still "
            f"proves which graph produced the numbers.") from exc

    part.rename(dest)
    return dest


def _stream_edges(path: Path) -> Iterator[tuple[int, int]]:
    """Yield (src, dst) from a SNAP-format edge list.

    SNAP files are whitespace-separated with '#' comment headers. Malformed
    lines are skipped rather than raising: a single bad line in a 30M-line file
    should not abort a ten-minute sampling run. The count of skipped lines is
    not currently surfaced -- if that ever matters for a source, it belongs in
    the manifest.
    """
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", encoding="utf-8", errors="replace") as fh:
        for line in fh:
            if not line or line[0] == "#":
                continue
            parts = line.split()
            if len(parts) < 2:
                continue
            try:
                yield int(parts[0]), int(parts[1])
            except ValueError:
                continue


# --------------------------------------------------------------------------
# sampling
# --------------------------------------------------------------------------

def _pick_seed_node(path: Path, seed: int, scan_lines: int = 200_000) -> int:
    """Choose a start node with above-median out-degree in the first slice of
    the file.

    Seeding the snowball from a random node risks landing on a leaf, which
    produces a tiny sample. Scanning a bounded prefix keeps this cheap and,
    because the prefix and the RNG seed are both fixed, keeps it deterministic.
    """
    degree: dict[int, int] = defaultdict(int)
    for i, (s, _t) in enumerate(_stream_edges(path)):
        degree[s] += 1
        if i >= scan_lines:
            break
    if not degree:
        raise ValueError(f"no edges found in {path}")
    ranked = sorted(degree.items(), key=lambda kv: (-kv[1], kv[0]))
    # Top decile, then a seeded choice within it: high enough degree to expand,
    # but not deterministically the single hub node (which would make the
    # sample unusually star-shaped).
    pool = ranked[: max(1, len(ranked) // 10)]
    return random.Random(seed).choice(pool)[0]


def _snowball_nodes(path: Path, start: int, target_nodes: int) -> tuple[set[int], int]:
    """Grow a node set by streamed BFS levels until it reaches target_nodes.

    Returns the selected set and the number of levels consumed. Expansion
    within a level follows file order, so the result depends only on the source
    file and the start node -- no RNG here at all.
    """
    selected: set[int] = {start}
    levels = 0
    for level in range(MAX_LEVELS):
        if len(selected) >= target_nodes:
            break
        frontier_additions: list[int] = []
        for s, t in _stream_edges(path):
            if s in selected and t not in selected:
                frontier_additions.append(t)
                if len(selected) + len(frontier_additions) >= target_nodes:
                    break
        if not frontier_additions:
            break  # connected component exhausted
        selected.update(frontier_additions)
        levels = level + 1
        print(f"[dataset]   level {levels}: {len(selected):,} nodes selected")
    return selected, levels


def _induced_edges(path: Path, selected: set[int]) -> dict[int, list[int]]:
    """One final pass collecting edges whose endpoints are both selected."""
    by_source: dict[int, list[int]] = defaultdict(list)
    for s, t in _stream_edges(path):
        if s in selected and t in selected:
            by_source[s].append(t)
    return by_source


def _proportional_subsample(by_source: dict[int, list[int]], target_edges: int,
                            seed: int) -> list[tuple[int, int]]:
    """Drop edges down to ``target_edges`` while preserving degree structure.

    Largest-remainder (Hamilton) apportionment: each source's ideal share is
    ``out_degree * target/total``; every source first takes the floor of its
    share with a minimum of one, then the leftover budget goes to the sources
    with the largest fractional remainders. This is the same method used to
    apportion legislative seats, and it is used here for the same reason --
    it is proportional, it is deterministic, and its tie-breaking is explicit.

    Why a floor of one: a source that survives into the induced subgraph but
    keeps zero edges becomes an isolated node, and isolated nodes make the
    traversal workload measure nothing.

    Why largest-remainder rather than simply scaling and rounding: rounding
    each share independently does not sum to the budget, so the sample would
    miss its edge target by an unpredictable margin and two runs with different
    seeds would not be comparable in size.

    Determinism: sources are visited in sorted id order, each source's
    adjacency is shuffled with the run seed, and remainder ties break on node
    id. Same seed + same source file => identical output.
    """
    rng = random.Random(seed)
    order = sorted(by_source)
    total = sum(len(by_source[s]) for s in order)

    shuffled: dict[int, list[int]] = {}
    for s in order:
        targets = list(by_source[s])
        rng.shuffle(targets)
        shuffled[s] = targets

    if total <= target_edges:
        # Nothing to drop. Emit in sorted-source order for reproducibility.
        return [(s, t) for s in order for t in shuffled[s]]

    # Pathological case: more distinct sources than the entire edge budget.
    # A floor of one is unaffordable, so keep the highest-degree sources -- they
    # are the ones that carry traversal structure.
    if len(order) > target_edges:
        top = sorted(order, key=lambda s: (-len(shuffled[s]), s))[:target_edges]
        return [(s, shuffled[s][0]) for s in sorted(top)]

    ratio = target_edges / total
    keep: dict[int, int] = {}
    remainders: list[tuple[float, int]] = []
    for s in order:
        share = len(shuffled[s]) * ratio
        base = max(1, int(share))
        base = min(base, len(shuffled[s]))
        keep[s] = base
        remainders.append((share - int(share), s))

    spare = target_edges - sum(keep.values())
    if spare > 0:
        # Largest fractional remainder first; ties break on node id so the
        # result does not depend on dict ordering.
        for _frac, s in sorted(remainders, key=lambda r: (-r[0], r[1])):
            if spare <= 0:
                break
            room = len(shuffled[s]) - keep[s]
            if room <= 0:
                continue
            take = min(room, spare)
            keep[s] += take
            spare -= take
    elif spare < 0:
        # The floor-of-one guarantee overshot the budget. Trim from the
        # highest-allocation sources first, never below one, so the shape is
        # compressed from the top rather than the tail being deleted.
        over = -spare
        while over > 0:
            trimmable = sorted((s for s in order if keep[s] > 1),
                               key=lambda s: (-keep[s], s))
            if not trimmable:
                break
            for s in trimmable:
                keep[s] -= 1
                over -= 1
                if over <= 0:
                    break

    return [(s, t) for s in order for t in shuffled[s][:keep[s]]]


def _estimate_3hop(edges: list[tuple[int, int]], seed: int,
                   samples: int = 200, cap: int = 5000) -> float:
    """Mean distinct nodes reached in exactly-3 hops, over sampled start nodes.

    This is the viability check: a dataset can hit its node and edge targets and
    still be too sparse for the traversal workload to measure anything. Running
    a cheap BFS here costs a second and catches that before five cloud
    platforms spend an hour benchmarking an empty result set.

    Capped at ``cap`` per frontier so a dense sample cannot make this estimate
    itself expensive.
    """
    adj: dict[int, list[int]] = defaultdict(list)
    for s, t in edges:
        adj[s].append(t)
    sources = sorted(adj)
    if not sources:
        return 0.0
    rng = random.Random(seed)
    picks = (sources if len(sources) <= samples
             else rng.sample(sources, samples))
    totals = []
    for start in picks:
        frontier = {start}
        for _ in range(3):
            nxt: set[int] = set()
            for n in frontier:
                nxt.update(adj.get(n, ()))
                if len(nxt) > cap:
                    break
            frontier = nxt
            if not frontier:
                break
        totals.append(len(frontier))
    return sum(totals) / len(totals) if totals else 0.0


def _checksum(nodes: Sequence[int], edges: Sequence[tuple[int, int]]) -> str:
    """sha256 over the canonical (sorted) graph.

    Sorted rather than emission-ordered so two runs that build the same graph
    by different code paths still agree. This is what lets a reader verify that
    the numbers in results/ came from the graph the manifest describes.
    """
    h = hashlib.sha256()
    for n in sorted(nodes):
        h.update(f"{n}\n".encode())
    h.update(b"--edges--\n")
    for s, t in sorted(edges):
        h.update(f"{s} {t}\n".encode())
    return h.hexdigest()


def build(name: str, target_edges: int | None, seed: int,
          cache_dir: Path, target_nodes: int | None = None) -> Dataset:
    """Download (cached), sample, checksum. The only entry point run.py uses."""
    if name not in SOURCES:
        raise ValueError(
            f"unknown dataset {name!r}; known: {', '.join(sorted(SOURCES))}")
    src = SOURCES[name]
    path = _download(src["url"], Path(cache_dir) / src["filename"])

    if target_edges is None:
        # Whole-graph mode: only sane for the small sources.
        by_source: dict[int, list[int]] = defaultdict(list)
        node_set: set[int] = set()
        for s, t in _stream_edges(path):
            by_source[s].append(t)
            node_set.add(s)
            node_set.add(t)
        edges = [(s, t) for s in sorted(by_source) for t in by_source[s]]
        nodes = sorted(node_set)
        sampling = "full graph, no sampling"
        levels = 0
    else:
        # Heuristic: aim for a node target that makes the requested edge count
        # achievable without a degenerate degree distribution. Overridable.
        if target_nodes is None:
            target_nodes = max(1000, int(target_edges * 0.6))
        start = _pick_seed_node(path, seed)
        print(f"[dataset] snowball seed node = {start} "
              f"(target {target_nodes:,} nodes / {target_edges:,} edges)")
        selected, levels = _snowball_nodes(path, start, target_nodes)
        by_source = _induced_edges(path, selected)
        induced_total = sum(len(v) for v in by_source.values())
        print(f"[dataset]   induced subgraph: {induced_total:,} edges "
              f"before subsampling")
        edges = _proportional_subsample(by_source, target_edges, seed)
        # Keep every selected node, including ones that ended with degree zero:
        # dropping them would quietly change the node count away from the
        # manifest's target and hide the sampler's real behaviour.
        nodes = sorted(selected)
        sampling = (
            f"seeded snowball from node {start} ({levels} BFS levels), "
            f"induced subgraph, degree-proportional edge subsample "
            f"to {target_edges:,}, seed={seed}")

    out_degree: dict[int, int] = defaultdict(int)
    for s, _t in edges:
        out_degree[s] += 1
    degrees = sorted(out_degree.values())
    reach3 = _estimate_3hop(edges, seed)
    stats = {
        "nodes_with_outgoing_edges": len(out_degree),
        "isolated_nodes": len(nodes) - len(out_degree),
        "max_out_degree": degrees[-1] if degrees else 0,
        "p95_out_degree": degrees[int(len(degrees) * 0.95)] if degrees else 0,
        "median_out_degree": degrees[len(degrees) // 2] if degrees else 0,
        "mean_out_degree": round(len(edges) / len(nodes), 3) if nodes else 0,
        "bfs_levels": levels,
        "mean_3hop_reach": round(reach3, 1),
    }

    # Viability gate. A dataset can hit both targets and still be useless for
    # the headline workload; that must surface here, loudly, rather than as an
    # unexplained row of near-zero traversal latencies five hours later.
    if reach3 < MIN_VIABLE_3HOP:
        stats["viability_warning"] = (
            f"mean 3-hop reach is {reach3:.1f} nodes (< {MIN_VIABLE_3HOP}). "
            f"The sampled graph is too sparse for the traversal workload to "
            f"measure meaningful graph work. Mean out-degree is "
            f"{stats['mean_out_degree']}. Raise dataset.target_edges or lower "
            f"dataset.target_nodes in config/workloads.yaml until this "
            f"warning clears -- do NOT publish traversal numbers while it is "
            f"present.")
        print("\n" + "!" * 72)
        print("[dataset] VIABILITY WARNING")
        print(f"[dataset] {stats['viability_warning']}")
        print("!" * 72 + "\n")

    ds = Dataset(
        name=name,
        nodes=nodes,
        edges=edges,
        checksum=_checksum(nodes, edges),
        seed=seed,
        source_url=src["url"],
        description=src["description"],
        sampling=sampling,
        stats=stats,
    )
    return ds


def save_manifest(ds: Dataset, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(ds.manifest(), indent=2))


def pick_start_nodes(ds: Dataset, count: int, min_out_degree: int,
                     seed: int) -> list[int]:
    """Choose the traversal start nodes used identically on every platform.

    Uniform selection would pick mostly leaf nodes, whose 2- and 3-hop queries
    return nothing and complete instantly -- flattering every platform equally
    while measuring almost no graph work. Restricting to nodes with at least
    ``min_out_degree`` outgoing edges makes the traversal workload actually
    traverse.

    The returned list is seeded and written into results/bench.json, so the
    exact same nodes are queried on every platform and a reader can re-run
    against them.
    """
    out_degree: dict[int, int] = defaultdict(int)
    for s, _t in ds.edges:
        out_degree[s] += 1
    eligible = sorted(n for n, d in out_degree.items() if d >= min_out_degree)
    if not eligible:
        raise ValueError(
            f"no node has out-degree >= {min_out_degree}; lower "
            f"read.min_out_degree in config/workloads.yaml or increase "
            f"dataset.target_edges")
    rng = random.Random(seed)
    if len(eligible) <= count:
        return eligible
    return sorted(rng.sample(eligible, count))
