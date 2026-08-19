"""Adapter interface.

Every database in the benchmark implements this identical surface. The runner
never touches a driver directly, so adding a platform means adding one file
here and one entry in config/databases.yaml -- nothing else changes.

Design rule: each method must express the SAME LOGICAL OPERATION on every
platform. Query text differs (Cypher vs AQL); semantics must not.
"""

from __future__ import annotations

import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Iterable, Sequence


@dataclass
class LoadResult:
    """Outcome of ingesting the dataset into one platform."""

    node_count: int
    relationship_count: int
    wall_clock_s: float
    method: str  # e.g. "driver batching, UNWIND, batch=5000"
    notes: str = ""

    @property
    def nodes_per_s(self) -> float:
        return self.node_count / self.wall_clock_s if self.wall_clock_s else 0.0

    @property
    def rels_per_s(self) -> float:
        return self.relationship_count / self.wall_clock_s if self.wall_clock_s else 0.0


@dataclass
class Footprint:
    """Whatever the platform exposes about its own resource usage.

    Fields left as None are reported as 'not observable' in the README, which
    the assignment explicitly asks for rather than guessing.
    """

    stored_bytes: int | None = None
    memory_bytes: int | None = None
    advertised_specs: str = ""
    raw: dict[str, Any] = field(default_factory=dict)


class GraphAdapter(ABC):
    """One benchmarked platform."""

    #: Human-readable name used in every results table.
    name: str = "unnamed"
    #: Query language, surfaced in the README so language differences are visible.
    language: str = "cypher"

    def __init__(self, cfg: dict[str, Any]):
        self.cfg = cfg
        self.name = cfg.get("name", self.name)
        self.advertised_specs = cfg.get("advertised_specs", "")
        self.tier = cfg.get("tier", "")
        self.region = cfg.get("region", "")

    # ---- lifecycle -------------------------------------------------------

    @abstractmethod
    def connect(self) -> None:
        """Open the driver/session pool. Must be safe to call once."""

    @abstractmethod
    def close(self) -> None:
        """Release all resources."""

    @abstractmethod
    def wipe(self) -> None:
        """Drop all data and indexes so every run starts from an identical state."""

    # ---- schema and loading ---------------------------------------------

    @abstractmethod
    def create_schema(self) -> list[str]:
        """Create indexes/constraints. Returns a description of what was created,
        so the README can state exactly which properties are indexed where."""

    @abstractmethod
    def load(self, nodes: Sequence[int], edges: Sequence[tuple[int, int]],
             batch_size: int) -> LoadResult:
        """Ingest the dataset. Implementations should use each platform's
        idiomatic bulk path and record which one in LoadResult.method."""

    # ---- workloads -------------------------------------------------------

    @abstractmethod
    def point_lookup(self, node_id: int) -> int:
        """Fetch one node by its indexed id. Returns rows seen."""

    @abstractmethod
    def filtered_lookup(self, bucket: int) -> int:
        """Indexed range/equality scan on a secondary property. Returns rows seen."""

    @abstractmethod
    def traverse(self, node_id: int, hops: int, limit: int) -> int:
        """Collect DISTINCT nodes reachable in exactly `hops` hops from node_id,
        capped at `limit`. Returns rows seen."""

    @abstractmethod
    def aggregate(self) -> int:
        """Group-by style aggregation over the graph. Returns rows seen."""

    @abstractmethod
    def write_edge(self, src: int, dst: int) -> None:
        """Single relationship insert, used by the mixed read/write workload."""

    # ---- observability ---------------------------------------------------

    @abstractmethod
    def footprint(self) -> Footprint:
        """Best-effort resource reporting. Return empty fields where the
        platform does not expose the number -- do not estimate."""

    # ---- shared helpers --------------------------------------------------

    def ping(self) -> None:
        """Cheapest possible round trip that reads no user data.

        This exists because the obvious connectivity probe -- "run the
        aggregation and see if it comes back" -- is a full node scan. Running
        it before the cold-latency phase would page the entire dataset into the
        server's cache and make every subsequent 'cold' number a warm number.
        That is a silent methodology error, so the probe is deliberately
        separated from the workloads.

        Adapters that cannot express a data-free round trip may leave this
        unimplemented; healthcheck() will fall back and label the result.
        """
        raise NotImplementedError

    def healthcheck(self) -> tuple[bool, str]:
        """Connectivity probe. Run before any timing so a dead endpoint fails
        fast instead of polluting results.

        Returns (ok, message). The message names which probe was used, because
        a fallback to aggregate() means the cold measurements that follow are
        contaminated and the report needs to say so.
        """
        try:
            t0 = time.perf_counter()
            try:
                self.ping()
                probe = "ping"
            except NotImplementedError:
                self.aggregate()
                probe = "aggregate (WARNING: scans data, warms server cache)"
            return True, f"ok in {(time.perf_counter() - t0) * 1000:.0f}ms via {probe}"
        except Exception as exc:  # noqa: BLE001 - surfaced verbatim to the user
            return False, f"{type(exc).__name__}: {exc}"

    def query_catalog(self) -> dict[str, str]:
        """The exact query text this adapter issues, for the report.

        Publishing the query strings alongside the numbers is the cheapest way
        to make a cross-language comparison auditable: a reader who disagrees
        with a translation can see it without cloning the repo.
        """
        return {}

    def describe(self) -> dict[str, str]:
        return {
            "name": self.name,
            "language": self.language,
            "tier": self.tier,
            "region": self.region,
            "advertised_specs": self.advertised_specs,
        }

    def __enter__(self) -> "GraphAdapter":
        self.connect()
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()


def chunked(seq: Sequence[Any], size: int) -> Iterable[Sequence[Any]]:
    for i in range(0, len(seq), size):
        yield seq[i:i + size]
