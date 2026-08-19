# Graph Database Cloud Benchmark — CognoDB vs. four other graph platforms

A reproducible, resource-matched benchmark of **CognoDB Cloud** against four
other graph databases, on one dataset, with one set of logically identical
workloads, from one client machine.

> **This benchmark is not trying to pick a winner.** It is trying to produce
> numbers that are fair enough to argue about. Where a platform failed, timed
> out, ran out of memory, or could not be measured, that appears in the results
> — not in a footnote, and not nowhere.

**Results:** [`results/REPORT.md`](results/REPORT.md), generated directly from
the raw JSON in `results/` and never hand-edited.
**Analysis:** section 5 of this file.

---

## TL;DR — what was measured

| | |
|---|---|
| **Platforms** | CognoDB Cloud · Neo4j AuraDB Free · Memgraph · FalkorDB · ArangoDB |
| **Languages** | Cypher ×4, AQL ×1 — deliberately not an all-Cypher field |
| **Dataset** | SNAP `soc-Pokec`, seeded snowball → 100k nodes / 600k relationships |
| **Workloads** | ingest · point lookup · indexed lookup · 1/2/3-hop traversal · aggregation · mixed read/write |
| **Iterations** | 120 measured, after 20 discarded warm-up iterations |
| **Concurrency** | 1 / 10 / 40 clients, 30 s sustained, 80 % read / 20 % write |
| **Reported** | p50, p95, p99, mean, stdev, CV, min, max — warm and cold separately |
| **Tracks** | A: managed cloud free tiers. B: every engine in Docker under `--cpus=0.5 --memory=256m` |

---

## 1. Methodology

### 1.1 Resource parity

The central fairness rule is that no database gets a hardware advantage.
CognoDB's free `c0` tier is the smallest instance in the comparison at
**0.5 vCPU (burstable) / 256 MB RAM / 1 GB disk**, so it sets the ceiling for
everything else.

| Platform | Tier | Advertised vCPU / RAM / disk | How parity was achieved |
|---|---|---|---|
| CognoDB Cloud | free `c0` | 0.5 / 256 MB / 1 GB | baseline — smallest tier in the comparison |
| Neo4j AuraDB Free | _fill in_ | _fill in_ | _fill in_ |
| Memgraph | _fill in_ | _fill in_ | _fill in_ |
| FalkorDB | _fill in_ | _fill in_ | _fill in_ |
| ArangoDB | _fill in_ | _fill in_ | _fill in_ |

> **Fill this table in from each vendor's console, verbatim.** Do not copy specs
> from documentation — copy what your actually-provisioned instance reports. If
> a vendor does not publish a number, write "not published"; do not estimate.
> The same strings go in `advertised_specs` in `config/databases.yaml`, and
> `REPORT.md` prints them as-is.

### 1.2 The cloud-vs-local honesty problem

The assignment asks for the **same client machine and region** for every
platform. That requirement and the **same resources** requirement pull against
each other: CognoDB Cloud is cloud-only, so a pure cloud comparison means every
vendor's network path, region, and virtualisation differ — and on
sub-millisecond queries the network round trip can dominate the actual database
work. A cloud-only benchmark cannot separate *"this engine is faster"* from
*"this endpoint is closer"*.

So this benchmark reports **two tracks**, and never mixes them in one table:

- **Track A — managed cloud.** Each platform's free tier, one client machine.
  Measures *what a user actually experiences*, network included.
- **Track B — self-hosted parity.** Every engine in Docker on one host, capped
  identically at `cpus: 0.5` / `mem_limit: 256m` / `memswap_limit: 256m`.
  Measures *the engine*, with network variance removed.

`run.py` takes `--results` precisely so the two tracks write to separate
directories; merging them into one `REPORT.md` would produce a table comparing
two different fairness regimes as though they were one.

**CognoDB cannot appear in Track B**, because it is cloud-only. That is a real
limitation of this comparison. It is not worked around by quietly substituting
Neo4j for it — Track B's conclusions are about the four engines that are in it,
and Track A remains the only place CognoDB is measured.

Track A answers *"which should I use?"*. Track B answers *"why do they
differ?"*. Neither answers both, and publishing only one would overstate the
conclusion.

**Client host caveat for this run:** the client is a Windows workstation on a
consumer connection, not a VM in a vendor region. Every Track A latency
therefore carries the same home-ISP path. Because it is the *same* path for all
five platforms, cross-platform comparison within Track A is still valid; the
absolute numbers are not comparable to anything measured from inside a cloud
region. This is exactly why Track B exists.

### 1.3 Timing policy

Defined once in `bench/workloads.py` and `config/workloads.yaml`, applied
identically to every platform. No adapter may vary it.

1. **Cold** — the first 10 iterations of a workload, on a client that has
   issued no prior queries of that workload. Reported in a separate table,
   never averaged into warm numbers.
2. **Warm-up** — 20 iterations whose timings are discarded entirely.
3. **Warm** — 120 measured iterations. These are the headline numbers.

Timing wraps the driver call only. Argument selection and RNG happen outside the
timer, so harness overhead never lands in the measurement.

**Percentiles are nearest-rank, not interpolated.** Every reported value is a
latency some request actually experienced, rather than a synthetic value
between two observations. `bench/stats.py` explains the trade-off and flags any
sample smaller than 100, where a nearest-rank p99 degenerates to `max()`.

**What "cold" does not mean.** There are three caches between the client and
the answer and this harness controls one of them:

| Cache | Controlled? |
|---|---|
| Client driver state (pool, routing, prepared statements) | **Yes** — the client reconnects before each cold workload (`cold_isolation: reconnect`) |
| Server query-plan cache | Partly — reconnecting does not clear it on any engine here |
| Server page / buffer cache | **No, and not controllable.** These are managed free tiers; there is no API to drop caches, and on a shared tier the pages may not even be ours |

Consequence: cold workloads run in a fixed, recorded order (`cold_order` in the
results JSON) and the later ones are progressively less cold than the earlier
ones. The cold table measures *first-query overhead on an established
deployment*, not a genuine cold start, **and should not be used to rank
platforms.** That caveat is written into every results file rather than left
for a reader to discover.

The connectivity probe is deliberately separated from the workloads for the
same reason. The obvious health check — "run the aggregation and see if it comes
back" — is a full node scan, and running it before the cold phase would page the
dataset into the server's cache and make every subsequent "cold" number warm.
`GraphAdapter.ping()` exists solely to avoid that.

### 1.4 Timeout policy

30 seconds per query, identical everywhere, enforced **server-side**:
`neo4j.Query(text, timeout=…)` on Bolt, `GRAPH.QUERY … TIMEOUT` on FalkorDB,
`max_runtime` on ArangoDB. Client-side abandonment is not sufficient — a client
that gives up while the server keeps executing leaves the engine burning the
0.5 vCPU this benchmark is trying to hold constant, contaminating the next
iteration.

A timeout is recorded as an **error with its exception text**, never as a
missing sample and never as a slow success. `REPORT.md` section 9 lists every
one.

### 1.5 Query fairness

The same *logical* operation runs everywhere. Query text differs where the
language differs, which is unavoidable and disclosed. `REPORT.md` section 10
reproduces the exact string each platform received, generated from
`GraphAdapter.query_catalog()`, so the translation can be audited without
reading the adapter source.

| Workload | Cypher | AQL |
|---|---|---|
| Point lookup | `MATCH (n:Person {uid:$uid})` | `FOR p IN person FILTER p.uid == @uid` |
| Indexed lookup | `WHERE n.bucket = $b LIMIT 500` | `FILTER p.bucket == @b LIMIT 500` |
| N-hop traversal | `-[:FOLLOWS*n..n]->` + `DISTINCT` + `LIMIT` | `FOR v IN n..n OUTBOUND` + `uniqueVertices:'global'` |
| Aggregation | `RETURN n.bucket, count(*)` | `COLLECT b = p.bucket WITH COUNT INTO c` |

Four deliberate choices, each of which is a legitimate thing to challenge:

- **Traversals are capped with `LIMIT 1000`.** On a social graph a 3-hop
  fan-out can reach a large fraction of the dataset and OOM a 256 MB instance.
  The identical cap applies everywhere, so the comparison stays fair — but it
  means the 3-hop numbers measure *time to first 1000 results*, not full
  neighbourhood expansion.

- **The AQL traversal is not an exact translation.** Cypher's
  `RETURN DISTINCT … LIMIT` streams: it emits distinct endpoints and stops at
  the limit. The AQL form that streams the same way is
  `OPTIONS {uniqueVertices:'global', order:'bfs'}` — but global uniqueness
  suppresses a vertex at depth *n* if it was already seen shallower, which
  Cypher does not. The alternative, `COLLECT`, gives exact DISTINCT semantics
  but is *blocking*: it materialises the entire n-hop frontier before applying
  the limit, penalising ArangoDB for a difference in language expressiveness
  rather than in engine speed. The streaming form was chosen so the **cost
  model** matches, and each platform's returned row counts are recorded so a
  reader can see where the result sets diverge. Reasonable people would choose
  the other way; this is the single largest judgement call in the harness.

- **Start nodes have a minimum out-degree of 5.** Uniform random selection would
  pick mostly leaf nodes whose 2- and 3-hop queries return instantly, flattering
  every platform equally while measuring almost nothing. The **same node list**
  is used on every platform (seeded, and written into `results/bench.json`).

- **Ingest is not like-for-like and is not presented as such.** Each platform
  uses its own idiomatic bulk path — `UNWIND` batching on Cypher engines,
  `import_bulk` on ArangoDB, which references vertices by key and needs no
  per-edge lookup. `REPORT.md` names the method in every ingest row. That table
  compares *vendor best-effort loading*, not identical work.

### 1.6 Dataset and sampling

SNAP `soc-Pokec` (1.6M nodes / 30.6M edges) is far too large for a 1 GB tier, so
it is sampled — and the sampling has to be reproducible by anyone who clones
this repo. `bench/dataset.py` does it in three stages, all deterministic given
the seed:

1. **Streamed multi-pass snowball.** The compressed edge list is streamed once
   per BFS level rather than loaded into an in-memory adjacency structure, which
   would need several GB. Peak memory is the selected node set, not the source
   graph.
2. **Induced subgraph**, one final pass.
3. **Degree-proportional edge subsample** by largest-remainder apportionment
   with a floor of one edge per source.

Same seed + same source file ⇒ byte-identical graph ⇒ identical sha256, which is
written into `results/dataset_manifest.json` and every results file.

**A sampler bug worth documenting, because it nearly invalidated the whole
benchmark.** The first implementation subsampled edges round-robin — one per
source per round — reasoning that this strands no node at degree zero. It does
not strand anyone, but it flattens the degree distribution to near-uniform: on a
30k-node test graph it produced a **maximum out-degree of 2**. A 3-hop traversal
on that graph reaches about eight nodes. Every platform would have returned
near-identical microsecond timings, the results table would have looked clean,
and it would have been measuring the sampler rather than any database.
Degree-proportional apportionment replaced it, and the sampler now runs a
**viability check**: it estimates mean 3-hop reachability on the sampled graph
and prints a loud warning if it drops below 25 nodes.

**Why not 150k nodes / 250k relationships.** That shape has a mean out-degree of
1.67 and a measured mean 3-hop reach of ~6 nodes — it trips the viability check
for exactly the reason above. **100k nodes / 600k relationships** clears the
100k node floor and gives a mean out-degree of 6: sparse enough that a 3-hop
fan-out does not touch the whole graph, dense enough that it touches something.
Both numbers live in `config/workloads.yaml` with this reasoning attached.

Known sampling biases, stated rather than discovered:

- Snowball sampling biases toward the high-degree core near the seed node. The
  sample is not a uniform random subgraph of Pokec and does not claim to be.
- The seed node is chosen from the top decile of out-degree in a bounded prefix
  of the file, so the snowball has something to expand from.
- Nodes that end with zero edges after subsampling are **kept**, not dropped.
  Dropping them would quietly move the node count away from the target and hide
  what the sampler actually did. `isolated_nodes` is reported in the manifest.

### 1.7 What this benchmark does not establish

- Free tiers are throttled, shared, and burstable. These numbers say little
  about paid-tier or production performance.
- One dataset, one topology. A different graph shape could reorder the results.
- No per-platform index tuning beyond the equivalent secondary indexes. A vendor
  expert could very likely make their own engine faster.
- Single client host, single run window. Run-to-run variance is reported via the
  coefficient of variation, but this is not a multi-day study.
- **Any gap smaller than the flagged variance is not a result.** `REPORT.md`
  section 8 lists every measurement whose CV exceeds 0.50; the analysis is
  obliged to address them rather than rank through them.

---

## 2. Reproducing this benchmark

### Prerequisites

- Python 3.10+
- Free-tier accounts on the platforms you want to include
- Docker (Track B only). On Windows this needs Docker Desktop with the **WSL2**
  backend — the Hyper-V backend does not honour per-container `cpus` the same
  way. Run `docker stats --no-stream` and confirm the caps are real before
  trusting any Track B number.

### Setup

```bash
git clone <this-repo>
cd <this-repo>

python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt

cp .env.example .env
# edit .env with your connection details — .env is gitignored
```

Credentials are read from environment variables only. `config/databases.yaml`
contains `${VAR}` references and no secrets; the loader **fails loudly** if any
referenced variable is unset, listing all of them at once, so a half-configured
run cannot silently produce a results file that looks complete.

### Verify the harness before spending quota

```bash
python scripts/selftest.py
```

Runs the complete pipeline — schema, load, cold and warm reads, concurrency
sweep, report generation — against an in-memory fake database. No network, no
credentials, about 20 seconds. It also asserts that the cold-isolation path ran,
that the report contains no `NaN`, and that the config loader really does reject
an unset `${VAR}`. **If this fails, the problem is the harness, not your
database.**

### Run

```bash
python run.py check     # connectivity only, no timing
python run.py all       # dataset → load → benchmark → report
```

Individual phases (`load`, `bench`, `report`) run separately, and
`--only "CognoDB Cloud"` restricts a run to named platforms — useful when one
provider is down and you don't want to re-run the others. Each phase writes JSON
to `results/` as it completes, so a crash partway through never loses finished
work.

If your network blocks `snap.stanford.edu`, download the edge list by hand and
point the harness at it:

```bash
GRAPHBENCH_DATASET_PATH=/path/to/soc-pokec-relationships.txt.gz python run.py all
```

The dataset checksum still proves which graph produced the numbers.

### Track B (self-hosted parity)

```bash
docker compose -f docker/docker-compose.yml up -d
docker stats --no-stream                       # confirm the caps are applied
python run.py all --databases config/databases.local.yaml --results results/track-b
```

Every container is capped at `cpus: 0.5` / `mem_limit: 256m` /
`memswap_limit: 256m` — swap is disabled so the memory cap cannot be quietly
exceeded. Neo4j's heap and page cache and ArangoDB's block cache are pinned
explicitly, because both size themselves from **host** memory by default and
would be OOM-killed by the kernel with no useful error message.

**Expect JVM-based engines to struggle or fail at 256 MB. That is a result, not
a bug, and it is reported as one.** Do not raise the limit for one engine.

---

## 3. Repository layout

```
run.py                        one-command orchestrator
requirements.txt              exact pins — driver versions move the numbers
.env.example                  every credential the harness reads
bench/
  adapters/base.py            the interface every platform implements
  adapters/bolt.py            CognoDB, Neo4j Aura, Memgraph (Bolt + Cypher)
  adapters/others.py          FalkorDB (Cypher/RESP), ArangoDB (AQL)
  dataset.py                  download, seeded snowball, viability check
  workloads.py                cold/warm timing policy, concurrency sweep
  stats.py                    nearest-rank percentiles, variance flags
  report.py                   results JSON → REPORT.md + charts
  config.py                   YAML + fail-loud env interpolation
config/workloads.yaml         single source of truth for "same queries everywhere"
config/databases.yaml         Track A, secrets by ${ENV_VAR} only
config/databases.local.yaml   Track B
docker/docker-compose.yml     Track B, capped at 0.5 vCPU / 256 MB
scripts/selftest.py           full-pipeline test against a fake in-memory DB
results/                      raw JSON, charts, dataset manifest, REPORT.md
```

Adding a sixth platform means one adapter file and one config block. Nothing in
the runner, the workloads, or the report changes.

`results/` is **committed, not gitignored**. A reader must be able to see the raw
numbers behind every published table without re-running the benchmark.

---

## 4. Known limitations of the harness itself

Listed here rather than left to be found:

- **Cold measurements are not true cold starts.** Server page caches cannot be
  dropped on a managed tier. See §1.3.
- **The AQL traversal is a cost-model match, not a semantic match.** See §1.5.
- **The mixed workload grows the graph.** 20 % of operations are relationship
  inserts, so the dataset is slightly larger at 40 clients than it was at 1.
  Concurrency levels run in ascending order and the growth is bounded, but the
  highest-concurrency numbers are taken against a marginally larger graph.
- **Ingest teardown differs by engine.** FalkorDB's `GRAPH.DELETE` drops a key
  outright; the Bolt adapter deletes in batches because a single
  `DETACH DELETE` over 600k relationships would OOM a 256 MB instance. The end
  state is identical; the teardown cost is not, and is not timed.
- **A driver-level bug existed in an earlier revision and is worth knowing
  about.** `session.run(cypher, timeout=30)` in the Neo4j Python driver does
  *not* set a timeout — `Session.run`'s signature is
  `run(query, parameters=None, **kwparameters)`, so any extra keyword is
  silently treated as a **Cypher parameter**. The stated timeout policy was
  decorative until it was changed to `neo4j.Query(text, timeout=…)`. If you are
  auditing another benchmark, this is a good thing to check.

---

## 5. Analysis

> _Write this after the numbers exist. Replace this section — do not ship the
> placeholder._
>
> Cover, at minimum:
> - Which platform led on which workload, and by how much
> - **Why** — storage engine, in-memory vs. disk-backed, index strategy,
>   protocol overhead, network round trip in Track A vs. Track B
> - Where free-tier throttling visibly distorted a result
> - Where Track A and Track B disagree, and what that gap tells you about how
>   much of a cloud number is network rather than engine
> - Where the numbers are too close to call given the variance flagged in
>   `REPORT.md` section 8
> - What you would measure next given more time

---

## 6. Caveats and failures observed in this run

> _Every timeout, failed run, OOM, throttle, and anomaly goes here — including
> ones that make a platform look bad, and including ones caused by mistakes in
> this harness. `REPORT.md` sections 8 and 9 auto-populate with variance flags
> and recorded errors; expand on them here in prose._

---

## Data attribution

Source graph: J. Leskovec and A. Krevl, *SNAP Datasets: Stanford Large Network
Dataset Collection*, `soc-Pokec` (J. Takac, M. Zabovsky).
<http://snap.stanford.edu/data>

## License

MIT
