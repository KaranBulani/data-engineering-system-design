# Chapter 12 — Lakehouse & Table Formats

> Part IV — Architecture Patterns

This chapter is the modern punchline of Part IV: ACID table formats on object
storage (Iceberg, Delta Lake, Hudi) dissolved the trade-off Lambda was invented
to manage. Understanding *why* requires understanding what these formats
actually do internally — which is also exactly what interviewers now probe.

## The Question

*"Can I have ONE table that streaming writers update, batch jobs read, and BI
queries — with ACID guarantees, time travel, and cheap reprocessing — on cheap
object storage?"*

(The 2014 answer was no. The modern answer is yes, with known caveats.)

## The Physics

### What a table format actually is

Parquet files (ch18) are immutable columnar files — great storage, no
transactional table semantics. A table format adds the *table* layer:

```
TABLE (logical)
  |
  +-- snapshot N      <- "the table as of commit N"
  |     +-- manifest list  (which manifests, stats)
  |     +-- manifest M1    (which files, per-partition stats)
  |     |     +-- data file parquet-001.parquet  (min/max, null counts)
  |     |     +-- data file parquet-002.parquet
  |     +-- manifest M2 ...
  |
  +-- snapshot N-1    <- previous commit (kept: time travel, rollback)
  +-- snapshot N-2 ...
```

- **Snapshots = commits.** Every write (batch overwrite, streaming upsert,
  MERGE) produces a new snapshot pointing at a new set of data files. The
  table's history is a chain of atomic snapshots.
- **Manifests = the index over files.** Readers plan queries from manifests'
  partition/column statistics — this is what enables file skipping without
  listing billions of objects.
- **Atomic commit** via the catalog/metastore pointer flip: a snapshot becomes
  visible only when its manifest list is atomically registered. Readers never
  see partial writes.

### The capabilities this unlocks (map each to the old pain)

| Capability | Mechanism | Replaces / fixes |
|---|---|---|
| ACID on object storage | snapshot + atomic pointer flip | inconsistent "Hive-style" directories |
| Time travel / rollback | query `snapshot N-2`; `ROLLBACK TO` | "restore from backup" archaeology |
| Schema evolution | metadata-level column add/drop/rename | full-table rewrites |
| Hidden partitioning | partition *transforms* (`days(ts)`, `bucket(id, 8)`) | partition-column drift bugs (below) |
| Concurrent writers | optimistic concurrency + retry on conflict | single-writer bottlenecks |
| Streaming upserts | MERGE/eq-delete per micro-batch commit | Lambda's two-path problem |

**Hidden partitioning deserves 30 seconds** (Iceberg's flagship): classic Hive
partitioning stores a literal `dt` column that producers must populate — and
they populate it wrong, and queries that filter `ts` don't prune `dt`
partitions unless hand-written to. Iceberg derives partitions from a
*transform of a real column* (`days(event_ts)`), recorded in metadata: producers
can't get it wrong, and the engine translates `WHERE event_ts > X` into
partition pruning automatically.

### Copy-on-write vs merge-on-read

The fundamental upsert trade inside these formats:

| | Copy-on-Write (CoW) | Merge-on-Read (MoR) |
|---|---|---|
| On upsert | rewrite the affected data files now | write small delete/log files; merge at read |
| Write latency | higher (rewrites) | low (appends) |
| Read latency | unchanged (best) | higher (merge deletes) |
| Best for | read-heavy tables | write-heavy / streaming tables |

The streaming-lakehouse pattern (Flink/Structured Streaming -> table) usually
writes MoR or small frequent CoW commits — which creates **the compaction
duty**: background jobs merge delete logs and small files into clean large
files. Compaction is the price of streaming into a table format; pretend it
doesn't exist and reads degrade month over month (ch19).

### The unified pattern — Lambda's correctness, Kappa's single path

```
 events -> Kafka -> stream processor --upserts every N sec--> TABLE (Iceberg/Delta)
                               |                                   |
                               |                                   +--> BI / SQL reads
 raw plane (object storage) ---+--> batch/backfill --snapshot rewrite--> same TABLE
```

- **Streaming writers** upsert into the table with sub-minute commits.
- **Batch and BI** read the same table with full ACID semantics.
- **Reprocessing** = rewrite a snapshot (or a partition) — cheap, because the
  table *is* the unit of versioning.

This is why "Lambda vs Kappa" is now mostly historical: the *reason* for two
paths (reprocessing was expensive; streaming couldn't be exact) is gone. One
table serves fresh-enough streaming reads and exact historical reads. The
trade moved rather than vanished: you now pay in **compaction, file sizing,
and catalog governance**.

### The big three, honestly compared

| | Iceberg | Delta Lake | Hudi |
|---|---|---|---|
| Origin | Netflix | Databricks | Uber |
| Engine support | broadest (Spark, Flink, Trino, Presto, BigQuery, Snowflake...) | strongest in Databricks; open elsewhere | Spark, Flink, Presto |
| Signature strengths | hidden partitioning, spec-driven open governance, v2 row-level deletes | mature ecosystem, simple mental model, Photon/DBR integration | first to upserts/timeline; MoR/CoW explicit |
| Catalog story | REST catalog / Polaris / Nessie / Unity | Unity Catalog / metastore | metastore / HUDI timeline |
| Choose when | multi-engine, open-standards org | Databricks-centered org | fine-grained upsert latency needs |

The interviewer-grade honesty: **converged features, diverged ecosystems.**
All three do ACID, time travel, schema evolution, upserts; the decision is
engine/ vendor gravity and catalog governance, not a checkbox feature.

### The senior caveat list

- **Small files** are the chronic disease: streaming commits every 10s produce
  thousands of files/day; compaction/flinking jobs are *mandatory*, not
  optional extras.
- **Catalog is the real governance surface**: who owns the atomic commit
  pointer (HMS, Glue, Unity, Polaris, Nessie) decides multi-writer safety and
  cross-engine trust (ch20).
- **Not a warehouse replacement out of the box**: lakehouse tables + query
  engines (Trino, Spark, warehouse external tables) close most of the gap, but
  BI concurrency and workload management still favor warehouses for the last
  mile (ch16).
- **Table format != data quality**: ACID guarantees storage consistency, not
  semantic correctness — that's ch21's problem.

## The Options

| Pattern | Freshness | Exactness | Codepaths | Ops load |
|---|---|---|---|---|
| Pure batch + warehouse (ch07) | hours | exact | 1 | low |
| Lambda (ch10) | seconds | exact (healed) | 2 | high |
| Kappa (ch11) | seconds | rebuildable | 1 + replay ops | medium |
| **Lakehouse unified** | ~minute (streaming commits) | exact (snapshots) | 1 per concern | medium (compaction/catalog) |

## Decision Rules

- **Default to a lakehouse table when** you need both streaming-fresh writes
  and batch/BI reads over the same data — that's the whole point.
- **Streaming writes -> MoR (or small CoW) + scheduled compaction.** No
  compaction plan, no approval.
- **Pick by ecosystem gravity** (Databricks->Delta, multi-engine/open->Iceberg,
  fine-grained upsert latency->Hudi), then stop relitigating.
- **Design partitions from query predicates** (and let hidden partitioning
  carry it); re-review partitioning when query patterns shift.
- **Treat the catalog as production infrastructure** — it is the commit
  authority and the governance surface.
- **Keep the raw plane beneath the tables** (ch03/06): snapshots make
  reprocessing cheap, raw makes *re-deriving tables themselves* possible.

## Failure Modes

- **Small-file suffocation**: streaming commits without compaction; query
  planning over 2M files; the "table is slow" ticket that is really a
  compaction backlog.
- **Compaction starvation during traffic spikes**: upsert volume spikes,
  compaction can't keep pace, MoR read latency climbs — read and write paths
  degrade together.
- **Catalog split-brain**: two writers committing through different catalogs
  (or an outdated HMS) — "the table" diverges depending on who you ask.
  Symptom: reconciliation queries that disagree per engine.
- **Hidden-partition complacency**: trusting auto-derivation so fully that
  nobody reviews partition strategy after query patterns changed; pruning
  silently stops helping.
- **Time-travel as a backup strategy**: snapshots age out (expiration policy!)
  and reference the *same* underlying files — expired snapshots plus object
  lifecycle rules can make "rollback" a hollow promise. Raw plane remains the
  real archive.

## Interview Narration

"Table formats are the answer to 'why are we still debating Lambda versus
Kappa.' What Iceberg, Delta, and Hudi add on top of Parquet is the *table*
layer: every write commits an atomic snapshot — a manifest list pointing at
data files with statistics — so object storage suddenly has ACID, time travel,
schema evolution, and concurrent writers. Readers plan from manifests, which is
also why file skipping works at scale.

The pattern that matters: a streaming processor upserts into the table with
sub-minute commits, batch and BI read the same table with full ACID semantics,
and reprocessing is a snapshot rewrite. That gives me Lambda's exactness and
Kappa's single-path simplicity, on cheap storage — the trade didn't vanish, it
moved: I now pay in compaction, file sizing, and catalog governance.

I'd narrate the upsert trade explicitly: copy-on-write rewrites files at write
time — best reads; merge-on-read appends delete logs and merges at read time —
best writes. Streaming tables usually go MoR plus scheduled compaction, and
compaction is part of the design, not an optimization.

On choosing among the three: they've converged on features — all do ACID, time
travel, upserts — so the decision is ecosystem gravity: Delta in a Databricks
org, Iceberg for multi-engine open standards, Hudi when upsert latency is the
sharp edge. And I'd close with the caveat that a table format guarantees
storage consistency, not semantic correctness — that's an orchestration and
data-quality problem, and the catalog that owns commits is genuinely
production infrastructure."

---

| <- Previous | Next -> |
|---|---|
| [Chapter 11 — Kappa Architecture](ch11-kappa-architecture.md) | [Chapter 13 — The SQL Family](ch13-the-sql-family.md) |