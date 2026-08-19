"""Non-Bolt adapters: FalkorDB (Cypher over RESP) and ArangoDB (AQL).

Including at least one engine that does not speak Cypher is deliberate. A
comparison consisting only of Neo4j-protocol databases would let the harness
reuse a single query string everywhere and never confront the hardest fairness
question in this benchmark: *what does "the same query" mean across two query
languages?*

ArangoDB forces that question. The answers, and the places where an exact
translation is impossible, are documented on each method below and surfaced
into REPORT.md via ``query_catalog()`` so a reader can audit the translation
without reading the code.
"""

from __future__ import annotations

import time
from typing import Any, Sequence

from .base import Footprint, GraphAdapter, LoadResult, chunked

# Both drivers are optional. config._registry() catches the ImportError and
# reports the missing pip package against the platform that needs it, rather
# than preventing the whole harness from starting.
from falkordb import FalkorDB  # noqa: E402
from arango import ArangoClient  # noqa: E402


# ==========================================================================
# FalkorDB
# ==========================================================================

class FalkorAdapter(GraphAdapter):
    """FalkorDB: Cypher, but over the RESP (Redis) protocol rather than Bolt.

    Same query language as the Bolt platforms, so the logical operations are
    literally identical strings. The interesting comparison here is protocol
    and storage engine (sparse-matrix / GraphBLAS, in-memory) against Neo4j's
    disk-backed store -- not query semantics.
    """

    language = "cypher"

    def __init__(self, cfg: dict[str, Any]):
        super().__init__(cfg)
        self.host = cfg.get("host", "localhost")
        self.port = int(cfg.get("port", 6379))
        self.username = cfg.get("username") or None
        self.password = cfg.get("password") or None
        self.ssl = bool(cfg.get("ssl", False))
        self.graph_name = cfg.get("graph", "bench")
        self.timeout_s = float(cfg.get("query_timeout_s", 30))
        self._db = None
        self._g = None

    # ---- lifecycle -------------------------------------------------------

    def connect(self) -> None:
        self._db = FalkorDB(
            host=self.host, port=self.port,
            username=self.username, password=self.password,
            ssl=self.ssl,
        )
        self._g = self._db.select_graph(self.graph_name)
        # Cheap round trip that creates nothing, so connect() has no side
        # effect on the graph a later wipe()/load() will measure.
        self._db.connection.ping()

    def ping(self) -> None:
        """RESP PING -- no graph access at all, so the cold phase stays cold."""
        self._db.connection.ping()

    def close(self) -> None:
        if self._db is not None:
            try:
                self._db.connection.close()
            finally:
                self._db = None
                self._g = None

    def _run(self, cypher: str, **params: Any) -> list[Any]:
        # FalkorDB's timeout is milliseconds and applies server-side, which is
        # what the timeout policy needs: a query that exceeds the budget is
        # killed by the engine, not abandoned by the client while the server
        # keeps burning the 0.5 vCPU we are trying to hold constant.
        res = self._g.query(cypher, params or None,
                            timeout=int(self.timeout_s * 1000))
        return res.result_set or []

    def wipe(self) -> None:
        # Dropping the whole key is O(1) here and avoids the batched-delete
        # dance the Bolt adapter needs. Different mechanism, same end state:
        # an empty graph with no indexes. Recorded in the load notes so the
        # ingest comparison is not read as like-for-like teardown.
        try:
            self._g.delete()
        except Exception:  # noqa: BLE001 - graph did not exist yet
            pass
        self._g = self._db.select_graph(self.graph_name)

    def create_schema(self) -> list[str]:
        created: list[str] = []
        # FalkorDB has carried two index syntaxes across versions. Try the
        # current one, fall back to the legacy one, and record which worked --
        # guessing wrong would silently benchmark an unindexed lookup.
        for prop in ("uid", "bucket"):
            for stmt in (
                f"CREATE INDEX FOR (n:Person) ON (n.{prop})",
                f"CREATE INDEX ON :Person({prop})",
            ):
                try:
                    self._run(stmt)
                    created.append(f"Person({prop}) via `{stmt}`")
                    break
                except Exception as exc:  # noqa: BLE001
                    last = f"{type(exc).__name__}: {exc}"
            else:
                created.append(f"Person({prop}): FAILED ({last})")
        return created

    def load(self, nodes: Sequence[int], edges: Sequence[tuple[int, int]],
             batch_size: int) -> LoadResult:
        t0 = time.perf_counter()
        node_q = ("UNWIND $rows AS row "
                  "CREATE (n:Person {uid: row.uid, bucket: row.bucket})")
        for batch in chunked(nodes, batch_size):
            self._run(node_q, rows=[{"uid": int(n), "bucket": int(n) % 100}
                                    for n in batch])
        edge_q = ("UNWIND $rows AS row "
                  "MATCH (a:Person {uid: row.s}) "
                  "MATCH (b:Person {uid: row.t}) "
                  "CREATE (a)-[:FOLLOWS]->(b)")
        for batch in chunked(edges, batch_size):
            self._run(edge_q, rows=[{"s": int(s), "t": int(t)} for s, t in batch])
        return LoadResult(
            node_count=len(nodes),
            relationship_count=len(edges),
            wall_clock_s=time.perf_counter() - t0,
            method=f"falkordb-py, UNWIND batching, batch_size={batch_size}",
            notes=("Identical Cypher to the Bolt platforms. Teardown differs: "
                   "GRAPH.DELETE drops the key outright rather than deleting "
                   "nodes in batches."),
        )

    # ---- workloads (identical Cypher to the Bolt adapter) ----------------

    def point_lookup(self, node_id: int) -> int:
        return len(self._run(
            "MATCH (n:Person {uid: $uid}) RETURN n.uid AS uid", uid=int(node_id)))

    def filtered_lookup(self, bucket: int) -> int:
        return len(self._run(
            "MATCH (n:Person) WHERE n.bucket = $b RETURN n.uid AS uid LIMIT 500",
            b=int(bucket)))

    def traverse(self, node_id: int, hops: int, limit: int) -> int:
        q = (f"MATCH (a:Person {{uid: $uid}})-[:FOLLOWS*{hops}..{hops}]->(b:Person) "
             "RETURN DISTINCT b.uid AS uid LIMIT $lim")
        return len(self._run(q, uid=int(node_id), lim=int(limit)))

    def aggregate(self) -> int:
        return len(self._run(
            "MATCH (n:Person) RETURN n.bucket AS bucket, count(*) AS c "
            "ORDER BY c DESC LIMIT 20"))

    def write_edge(self, src: int, dst: int) -> None:
        self._run("MATCH (a:Person {uid: $s}) MATCH (b:Person {uid: $t}) "
                  "CREATE (a)-[:FOLLOWS {synthetic: true}]->(b)",
                  s=int(src), t=int(dst))

    # ---- observability ---------------------------------------------------

    def footprint(self) -> Footprint:
        fp = Footprint(advertised_specs=self.advertised_specs)
        try:
            rows = self._run("MATCH (n) RETURN count(n) AS c")
            fp.raw["node_count"] = rows[0][0] if rows else None
            rows = self._run("MATCH ()-[r]->() RETURN count(r) AS c")
            fp.raw["relationship_count"] = rows[0][0] if rows else None
        except Exception as exc:  # noqa: BLE001
            fp.raw["count_error"] = f"{type(exc).__name__}: {exc}"

        # GRAPH.MEMORY USAGE is the only per-graph memory number FalkorDB
        # exposes; INFO memory is process-wide and therefore an upper bound,
        # not this graph's footprint. Both are recorded and labelled.
        try:
            mem = self._db.connection.execute_command(
                "GRAPH.MEMORY", "USAGE", self.graph_name)
            fp.raw["graph_memory_usage_raw"] = mem
            if isinstance(mem, list) and len(mem) >= 2:
                try:
                    fp.memory_bytes = int(mem[1]) * 1024 * 1024
                    fp.raw["graph_memory_units"] = "reported in MB, converted"
                except (TypeError, ValueError):
                    pass
        except Exception:  # noqa: BLE001
            fp.raw["graph_memory_usage_raw"] = "not observable"

        try:
            info = self._db.connection.info("memory")
            fp.raw["server_used_memory_bytes"] = info.get("used_memory")
            fp.raw["server_used_memory_note"] = (
                "process-wide, includes non-graph overhead -- upper bound only")
        except Exception:  # noqa: BLE001
            fp.raw["server_used_memory_bytes"] = "not observable"
        return fp

    def query_catalog(self) -> dict[str, str]:
        return {
            "point_lookup": "MATCH (n:Person {uid: $uid}) RETURN n.uid",
            "filtered_lookup": "MATCH (n:Person) WHERE n.bucket = $b "
                               "RETURN n.uid LIMIT 500",
            "traverse_n": "MATCH (a:Person {uid: $uid})-[:FOLLOWS*n..n]->(b) "
                          "RETURN DISTINCT b.uid LIMIT $lim",
            "aggregation": "MATCH (n:Person) RETURN n.bucket, count(*) "
                           "ORDER BY count(*) DESC LIMIT 20",
            "write_edge": "MATCH (a),(b) CREATE (a)-[:FOLLOWS {synthetic:true}]->(b)",
        }


# ==========================================================================
# ArangoDB
# ==========================================================================

class ArangoAdapter(GraphAdapter):
    """ArangoDB: multi-model, queried with AQL.

    This is the adapter where "the same query" stops being free. Three
    translations are approximations, and all three are stated in
    ``query_catalog()`` and reproduced in REPORT.md:

    1. **Traversal distinctness.** Cypher's ``-[:FOLLOWS*n..n]->`` with
       ``RETURN DISTINCT ... LIMIT`` streams: it emits distinct endpoints and
       stops at the limit. The AQL equivalent that streams the same way is
       ``OPTIONS {uniqueVertices:'global', order:'bfs'}``. These are not
       semantically identical -- global uniqueness suppresses a vertex at
       depth n if it was already seen at a shallower depth, whereas Cypher
       reports it. The alternative (``COLLECT`` to force exact DISTINCT
       semantics) is a blocking operation that would materialise the entire
       n-hop frontier before applying LIMIT, penalising ArangoDB for a
       difference in query-language expressiveness rather than in engine
       speed. The streaming form was chosen so the *cost model* matches, and
       the row counts each platform returns are recorded so the reader can see
       where the result sets diverge.

    2. **"Nodes" and "relationships"** are documents in a vertex collection and
       an edge collection. There is no label; the collection is the label.

    3. **Indexes.** A ``persistent`` index on ``uid`` and on ``bucket``, which
       is ArangoDB's equivalent of a Neo4j secondary range index. No
       hash/skiplist tuning beyond that, matching the "no per-platform
       optimisation" rule.
    """

    language = "aql"

    def __init__(self, cfg: dict[str, Any]):
        super().__init__(cfg)
        self.url = cfg["url"]
        self.db_name = cfg.get("database", "_system")
        self.user = cfg.get("user", "root")
        self.password = cfg.get("password", "")
        self.vertex = cfg.get("vertex_collection", "person")
        self.edge = cfg.get("edge_collection", "follows")
        self.timeout_s = float(cfg.get("query_timeout_s", 30))
        self._client = None
        self._db = None

    # ---- lifecycle -------------------------------------------------------

    def connect(self) -> None:
        self._client = ArangoClient(hosts=self.url,
                                    request_timeout=self.timeout_s + 30)
        self._db = self._client.db(self.db_name, username=self.user,
                                   password=self.password, verify=True)

    def ping(self) -> None:
        """Server version endpoint -- an HTTP round trip that reads no
        collection data, so it cannot warm the cold measurements."""
        self._db.version()

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None
            self._db = None

    def _aql(self, query: str, **bind: Any) -> list[Any]:
        cursor = self._db.aql.execute(query, bind_vars=bind or None,
                                      max_runtime=self.timeout_s)
        return list(cursor)

    def wipe(self) -> None:
        for name in (self.edge, self.vertex):
            if self._db.has_collection(name):
                self._db.delete_collection(name)

    def create_schema(self) -> list[str]:
        created: list[str] = []
        if not self._db.has_collection(self.vertex):
            self._db.create_collection(self.vertex)
        if not self._db.has_collection(self.edge):
            self._db.create_collection(self.edge, edge=True)
        col = self._db.collection(self.vertex)
        for field in ("uid", "bucket"):
            try:
                col.add_index({"type": "persistent", "fields": [field],
                               "unique": field == "uid", "sparse": False,
                               "name": f"idx_{field}"})
                created.append(
                    f"{self.vertex}({field}): persistent index"
                    f"{' (unique)' if field == 'uid' else ''}")
            except Exception as exc:  # noqa: BLE001
                created.append(f"{self.vertex}({field}): FAILED "
                               f"({type(exc).__name__}: {exc})")
        created.append(f"{self.edge}: edge collection (implicit _from/_to index)")
        return created

    def load(self, nodes: Sequence[int], edges: Sequence[tuple[int, int]],
             batch_size: int) -> LoadResult:
        t0 = time.perf_counter()
        vcol = self._db.collection(self.vertex)
        ecol = self._db.collection(self.edge)

        # _key is the string form of the source uid, so edge documents can be
        # built without a lookup. This is ArangoDB's idiomatic bulk path and is
        # genuinely faster than a MATCH-per-edge -- which is the point: each
        # platform gets its own documented bulk path, and the method string
        # records which one, so the ingest column is never read as
        # "same code, different speed".
        for batch in chunked(nodes, batch_size):
            vcol.import_bulk(
                [{"_key": str(int(n)), "uid": int(n), "bucket": int(n) % 100}
                 for n in batch],
                on_duplicate="ignore", sync=False)
        for batch in chunked(edges, batch_size):
            ecol.import_bulk(
                [{"_from": f"{self.vertex}/{int(s)}",
                  "_to": f"{self.vertex}/{int(t)}"} for s, t in batch],
                on_duplicate="ignore", sync=False)

        return LoadResult(
            node_count=len(nodes),
            relationship_count=len(edges),
            wall_clock_s=time.perf_counter() - t0,
            method=f"python-arango import_bulk, batch_size={batch_size}",
            notes=("Edge documents reference vertices by _key, so no lookup is "
                   "needed per edge. Cypher platforms MATCH both endpoints. "
                   "Both are the vendor's idiomatic bulk path; the ingest "
                   "numbers compare vendor best-effort loading, not identical "
                   "work."),
        )

    # ---- workloads -------------------------------------------------------

    def point_lookup(self, node_id: int) -> int:
        return len(self._aql(
            f"FOR p IN {self.vertex} FILTER p.uid == @uid RETURN p.uid",
            uid=int(node_id)))

    def filtered_lookup(self, bucket: int) -> int:
        return len(self._aql(
            f"FOR p IN {self.vertex} FILTER p.bucket == @b LIMIT 500 "
            "RETURN p.uid", b=int(bucket)))

    def traverse(self, node_id: int, hops: int, limit: int) -> int:
        # See the class docstring: uniqueVertices:'global' + bfs is the
        # streaming analogue of Cypher's DISTINCT + LIMIT. The semantic gap is
        # documented rather than papered over.
        q = (f"FOR v IN {hops}..{hops} OUTBOUND @start {self.edge} "
             "OPTIONS {uniqueVertices: 'global', order: 'bfs'} "
             "LIMIT @lim RETURN v.uid")
        return len(self._aql(q, start=f"{self.vertex}/{int(node_id)}",
                             lim=int(limit)))

    def aggregate(self) -> int:
        return len(self._aql(
            f"FOR p IN {self.vertex} COLLECT b = p.bucket WITH COUNT INTO c "
            "SORT c DESC LIMIT 20 RETURN {bucket: b, c: c}"))

    def write_edge(self, src: int, dst: int) -> None:
        self._db.collection(self.edge).insert({
            "_from": f"{self.vertex}/{int(src)}",
            "_to": f"{self.vertex}/{int(dst)}",
            "synthetic": True,
        })

    # ---- observability ---------------------------------------------------

    def footprint(self) -> Footprint:
        fp = Footprint(advertised_specs=self.advertised_specs)
        try:
            vstats = self._db.collection(self.vertex).statistics()
            estats = self._db.collection(self.edge).statistics()
            fp.raw["vertex_stats"] = vstats
            fp.raw["edge_stats"] = estats
            fp.raw["node_count"] = self._db.collection(self.vertex).count()
            fp.raw["relationship_count"] = self._db.collection(self.edge).count()
            sizes = [s.get("documentsSize") for s in (vstats, estats)
                     if isinstance(s, dict) and s.get("documentsSize")]
            if len(sizes) == 2:
                fp.stored_bytes = int(sizes[0]) + int(sizes[1])
        except Exception as exc:  # noqa: BLE001
            fp.raw["stats_error"] = f"{type(exc).__name__}: {exc}"
        # ArangoDB exposes no per-database RSS on managed tiers.
        fp.raw["memory_bytes_note"] = "not observable via the client API"
        return fp

    def query_catalog(self) -> dict[str, str]:
        return {
            "point_lookup": f"FOR p IN {self.vertex} FILTER p.uid == @uid RETURN p.uid",
            "filtered_lookup": f"FOR p IN {self.vertex} FILTER p.bucket == @b "
                               "LIMIT 500 RETURN p.uid",
            "traverse_n": f"FOR v IN n..n OUTBOUND @start {self.edge} "
                          "OPTIONS {uniqueVertices:'global', order:'bfs'} "
                          "LIMIT @lim RETURN v.uid  "
                          "// NOT exactly Cypher DISTINCT -- see adapter docstring",
            "aggregation": f"FOR p IN {self.vertex} COLLECT b = p.bucket "
                           "WITH COUNT INTO c SORT c DESC LIMIT 20 RETURN {b, c}",
            "write_edge": f"INSERT {{_from, _to, synthetic:true}} INTO {self.edge}",
        }
