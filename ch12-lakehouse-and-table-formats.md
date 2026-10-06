# Chapter 12 — Lakehouse & Table Formats

> Part IV — Architecture Patterns

This chapter is the modern punchline of Part IV. A **lakehouse** is a
platform where one set of tables — sitting on cheap object storage (S3 and
friends) — serves streaming writers, batch jobs, and BI queries at the same
time. What makes that possible is an **ACID table format** (Iceberg, Delta
Lake, Hudi): a metadata layer added on top of ordinary Parquet files. These
formats dissolved the trade-off Lambda was invented to manage (ch10):
reprocessing became cheap and streaming writes became exact, in one table.
Understanding *why* requires understanding what these formats actually do
internally — which is also exactly what interviewers now probe.

## The Question

*"Can I have ONE table that streaming writers update, batch jobs read, and BI
queries — with ACID guarantees, time travel, and cheap reprocessing — on cheap
object storage?"*

(The 2014 answer was no. The modern answer is yes, with known caveats.)

Unpacked with the running example for this chapter: an **orders** table for
an e-commerce company.

- **Streaming writers**: order events arrive constantly, and orders *change* —
  one is created, then paid, then shipped, then maybe refunded. The live
  stream needs to update existing rows in the table every few seconds.
- **BI queries**: analysts run "revenue by day" and dashboard queries over
  the same table, and they must never see a half-updated one.
- **Reprocessing**: someone finds a bug in the tax calculation — six months
  of history must be recomputed without taking the table offline.
- **Cheap object storage**: the table holds years of orders — petabyte-scale
  storage at S3 prices, not warehouse-appliance prices.

The three requirements in the question, in beginner terms:

- **ACID guarantees** — transactions: a writer's changes become visible
  all-or-nothing, and concurrent readers/writers don't corrupt each other.
- **Time travel** — query the table "as of yesterday 09:00," for debugging
  ("what did the report actually say when finance saw it?") and undo.
- **Cheap reprocessing** — recomputing a wrong slice of history is a small
  rewrite operation, not a re-architecture project.

## The Physics

### What a table format actually is

Start from the problem. Parquet files (ch18) are immutable columnar files —
great storage format, but a folder of Parquet files on S3 has *no table
semantics*: nothing stops two writers from stomping on each other's files, there is
no notion of "the table as of 09:00," and a crashed write leaves readers
looking at a half-populated table. That was the "Hive-style table" world. A
table format adds the missing *table* layer on top of the files:

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

Four pieces, walked through with the orders table:

- **Data files** are the actual Parquet files holding the rows.
- **Manifests** are index files that list which data files belong to the
  table, together with per-file statistics: row counts, min/max values per
  column, null counts.
- **A snapshot** is one complete, consistent version of the table — "the
  table consists of exactly the files these manifests list." Every commit
  creates a new snapshot; the table's history is a chain of them.
- **The catalog** (Glue, Hive Metastore, Unity, Nessie — a small database
  somewhere) stores one pointer per table: "orders -> snapshot 418." A write
  finishes by flipping that pointer in one atomic operation. A reader asks
  the catalog once, gets a snapshot, and reads only that snapshot's files —
  so it sees either the full old version or the full new version, never a
  mix of both.

The three claims that matter:

- **Snapshots = commits.** Every write — a batch overwrite, a streaming
  upsert, a MERGE — produces a new snapshot pointing at the new set of data
  files. Example: snapshot 417 lists files [a, b, c]; the streaming job
  commits 500 changed orders, which land in a new file d plus a note that
  those rows' old copies are superseded; snapshot 418 now lists the
  corrected set. The table's history is a chain of atomic snapshots, and
  "what changed" between any two of them is inspectable.
- **Manifests = the index over files.** Readers plan queries from the
  manifests' statistics instead of touching the data. Concrete example:
  `SELECT sum(total) FROM orders WHERE order_date = '2026-09-25'` — the
  engine reads the manifests, sees which files' min/max date ranges could
  contain that day, and reads *only those files*. This is "file skipping,"
  and it is what makes a table with millions of objects on S3 queryable
  without listing the bucket.
- **Atomic commit via the catalog pointer flip**: a snapshot becomes visible
  only when the catalog registers it, in one atomic operation. Readers never
  see partial writes — this single mechanism is where "ACID on object
  storage" comes from. The isolation lives in the metadata, not in the
  files.

### The capabilities this unlocks (map each to the old pain)

| Capability | Mechanism | Replaces / fixes |
|---|---|---|
| ACID on object storage | snapshot + atomic pointer flip | inconsistent "Hive-style" directories |
| Time travel / rollback | query `snapshot N-2`; `ROLLBACK TO` | "restore from backup" archaeology |
| Schema evolution | metadata-level column add/drop/rename | full-table rewrites |
| Hidden partitioning | instead of physically partitioning a table's column, table declares a rule like days(ts) — "partition by the calendar day of the ts column" — or bucket(id, 8) — "hash id into 8 buckets." The derived partition values are recorded in the format's metadata, not in any column; that's why it's called hidden. | What it fixes is the partition-column drift bug: a late event from the 24th lands in recd_dt=2026-09-25, so every query filtering on recd_dt quietly returns wrong results, and queries filtering on the real event_ts don't prune partitions at all unless someone remembers to also write WHERE dt = .... With hidden partitioning there's nothing for a producer to populate wrongly, and the engine automatically translates WHERE event_ts > X into partition pruning. |
| Concurrent writers | Each writer prepares its changes against the snapshot it read, and committing means atomically flipping the catalog's pointer to its new snapshot. If another writer committed first, the second one detects at commit time that the table changed underneath it and retries against the new snapshot — like git, where conflicts are caught at commit time rather than prevented with locks. It works well when writers touch different partitions. | single-writer bottlenecks — with classic Hive directories on object storage there was no safe way to coordinate writers, so teams had to serialize ingestion through one job at a time. |
| Streaming upserts | mechanism is row-level updates (MERGE, equality deletes) committed once per micro-batch: every few seconds, the streaming job's corrected rows become a new snapshot of the table. Because the table format accepts upserts, one table holds both the live updates and the full history, so the speed-layer/serving-layer split simply dissolves — that's the "unified pattern"  | In Lambda architecture, the streaming path wrote fresh-but-approximate results to one fast store and the batch path recomputed accurate results into another, and you forever reconciled the two — "the streaming store says 1,200 orders but the warehouse says 1,180." |

Each capability, in beginner terms:

- **ACID on object storage** — explained above: the catalog pointer flip,
  gives transactions on top of files that were never designed for them.
- **Time travel / rollback** — because old snapshots still exist, you can
  query the table as of an earlier commit ("what did finance see when the
  report was wrong?") or `ROLLBACK TO` a known-good snapshot. Example: a bad
  ETL run at 09:15 corrupts aggregates; rolling back to the 09:00 snapshot
  undoes it in seconds instead of an incident-length restore.
- **Schema evolution** — adding, renaming, or dropping a column is a
  *metadata* edit. Example: add `discount_code`; old Parquet files are not
  rewritten — the format simply knows the column doesn't exist in them and
  returns nulls for old rows. In the Hive world this operation ranged from
  chaos to a full table rewrite.
- **Hidden partitioning** — **Hidden partitioning deserves 30 seconds** (Iceberg's flagship). Classic
Hive partitioning stores a literal `dt` partition column that every producer
must populate correctly by hand — and they populate it wrong: a late event
from the 24th lands in `dt=2026-09-25`, and now every query that filters on
`dt` quietly returns wrong results, while queries that filter on the real
timestamp `event_ts` don't prune partitions at all unless someone remembers
to hand-write `WHERE dt = ...` too. Iceberg instead derives partitions from
a *transform of a real column* — the table declares `days(event_ts)` — and
records the derived values in metadata. Producers can't populate it wrong
(there is nothing to populate), and the engine translates
`WHERE event_ts > X` into partition pruning automatically.
- **Concurrent writers** — two jobs writing the same table at once, without
  locks: both prepare their changes; the first commits; the second detects
  that the table changed underneath it and retries against the new
  snapshot. It works like git — conflicts are detected at commit time, not
  prevented with locks — and it works well when writers touch different
  partitions.
- **Streaming upserts** — the format supports row-level updates (MERGE,
  equality deletes), so a streaming job can commit corrected rows every
  micro-batch. One table receives both the live updates and the history —
  no separate fast store to reconcile, which is Lambda's two-path problem
  dissolved (ch10).



### Copy-on-write vs merge-on-read

The fundamental upsert trade inside these formats. Set it up concretely: 100
orders change status, and their rows happen to live in a 1GB Parquet file.
Files are immutable — nobody edits 100 rows in place. The two strategies:

| | Copy-on-Write (CoW) | Merge-on-Read (MoR) |
|---|---|---|
| On upsert | rewrite the affected data files now | write small delete/log files; merge at read |
| Write latency | higher (rewrites) | low (appends) |
| Read latency | unchanged (best) | higher (merge deletes) |
| Best for | read-heavy tables | write-heavy / streaming tables |

- **Copy-on-write** rewrites the whole 1GB file now, with the 100 rows
  corrected. The write is expensive (1GB rewritten to change 100 rows), but
  readers get clean files and pay nothing extra.
- **Merge-on-read** does the opposite: it leaves the 1 GB file untouched and instead writes a small side file that records, essentially, "in that file X, rows at these 100 positions are no longer valid, and here are the 100 corrected replacement rows." That side file is the "delete file." It doesn't delete data physically — it marks old row positions as superseded (outdated, replaced by the new versions carried alongside). 

The streaming-lakehouse pattern (Flink/Structured Streaming -> table) usually
writes MoR or small frequent CoW commits — which creates **the compaction
duty**: background jobs that periodically merge delete logs and small files
into clean large files, restoring fast reads. Compaction is the price of
streaming into a table format; pretend it doesn't exist and reads degrade
month over month (ch19). It is part of the architecture, not an optional
optimization.

### The unified pattern — Lambda's correctness, Kappa's single path

```
 events -> Kafka -> stream processor --upserts every N sec--> TABLE (Iceberg/Delta)
                               |                                   |
                               |                                   +--> BI / SQL reads
 raw plane (object storage) ---+--> batch/backfill --snapshot rewrite--> same TABLE
```

- **Streaming writers** upsert into the table with sub-minute commits — every
  commit is a new snapshot, so BI sees fresh data within a minute.
- **Batch and BI** read the same table with full ACID semantics — no more
  "the streaming store says 1,200 orders but the warehouse says 1,180."
- **Reprocessing** = rewrite a snapshot (or a partition) — cheap, because
  the table *is* the unit of versioning. The tax-calculation bug: recompute
  the affected six months, atomically swap them in; the table was never
  offline.

This is why "Lambda vs Kappa" is now mostly historical: the *reason* for two
paths — reprocessing was expensive; streaming couldn't be exact — is gone.
One table serves fresh-enough streaming reads and exact historical reads.
The trade moved rather than vanished: you now pay in **compaction, file
sizing, and catalog governance** — background compaction jobs, tuning commit
intervals and target file sizes, and treating the catalog as critical
infrastructure. Cheaper than two codebases, but not free.

### The big three, honestly compared

| | Iceberg | Delta Lake | Hudi |
|---|---|---|---|
| Origin | Netflix | Databricks | Uber |
| Engine support | broadest (Spark, Flink, Trino, Presto, BigQuery, Snowflake...) | strongest in Databricks; open elsewhere | Spark, Flink, Presto |
| Signature strengths | hidden partitioning, spec-driven open governance, v2 row-level deletes | mature ecosystem, simple mental model, Photon/DBR integration | first to upserts/timeline; MoR/CoW explicit |
| Catalog story | REST catalog / Polaris / Nessie / Unity | Unity Catalog / metastore | metastore / HUDI timeline |
| Choose when | multi-engine, open-standards org | Databricks-centered org | fine-grained upsert latency needs |

The interviewer-grade honesty: **converged features, diverged ecosystems.**
All three do ACID, time travel, schema evolution, and upserts today — a
checkbox comparison finds no winner. The real decision is *ecosystem
gravity*: which engines and vendor your organization already runs (a
Databricks shop defaults to Delta; a multi-engine, open-standards org
defaults to Iceberg; fine-grained upsert latency at Uber scale is Hudi's
home turf), and who governs the catalog. Choose on that, then stop
relitigating.

### The senior caveat list

- **Small files** are the chronic disease. A streaming table committing
  every 10 seconds produces thousands of files per day, and small files hurt
  twice: query planning must read metadata for every one of them, and
  engines pay per-file open/read overhead on object storage. Compaction
  jobs are *mandatory*, not optional extras.
- **Catalog is the real governance surface**: whoever owns the atomic commit
  pointer (HMS, Glue, Unity, Polaris, Nessie) decides multi-writer safety
  and cross-engine trust (ch20). If two engines commit through different
  catalogs, there is no single source of truth anymore.
- **Not a warehouse replacement out of the box**: lakehouse tables plus
  query engines (Trino, Spark, warehouse external tables) close most of the
  gap, but BI concurrency and workload management still favor warehouses
  for the last mile (ch16). Five hundred concurrent dashboard users is a
  workload-management problem, not just a table-format problem.
- **Table format != data quality**: ACID guarantees storage consistency, not
  semantic correctness. The table can transactionally contain "revenue =
  $1,399" and be *wrong* — because the upstream dedup didn't run. ACID made
  the wrong number durable and consistent, which is worse than no guarantee
  if you confuse the two. Semantic correctness is ch21's problem.

## The Options

How to read the table: for each pattern — how fresh its answers are, whether
they're exact, how many codepaths maintain them, and what it costs to
operate.

| Pattern | Freshness | Exactness | Codepaths | Ops load |
|---|---|---|---|---|
| Pure batch + warehouse (ch07) | hours | exact | 1 | low |
| Lambda (ch10) | seconds | exact (healed) | 2 | high |
| Kappa (ch11) | seconds | rebuildable | 1 + replay ops | medium |
| **Lakehouse unified** | ~minute (streaming commits) | exact (snapshots) | 1 per concern | medium (compaction/catalog) |

## Decision Rules

- **Default to a lakehouse table when** you need both streaming-fresh writes
  and batch/BI reads over the same data — that is the whole point of the
  format, and the case where it beats Lambda and Kappa outright.
- **Streaming writes -> MoR (or small CoW) + scheduled compaction.** Treat
  "no compaction plan" as an incomplete design: an un-compacted streaming
  table is a slow table with a scheduled arrival date.
- **Pick by ecosystem gravity** (Databricks->Delta, multi-engine/open->Iceberg,
  fine-grained upsert latency->Hudi), then stop relitigating — the features
  have converged, so the org's gravity is the decision.
- **Design partitions from query predicates** — partition by what queries
  actually filter on (dates, tenant IDs), and let hidden partitioning carry
  it; re-review the strategy when query patterns shift.
- **Treat the catalog as production infrastructure** — it is the commit
  authority and the governance surface; its outage is a write outage for
  every table it owns.
- **Keep the raw plane beneath the tables** (ch03/06): snapshots make
  reprocessing cheap, raw makes *re-deriving the tables themselves*
  possible — format migration, corruption recovery, or a metric you decide
  to define differently.

## Failure Modes

- **Small-file suffocation**: streaming commits without compaction; query
  planning over 2M files takes minutes; the "table is slow" ticket that is
  really a compaction backlog. Alert on file counts and compaction lag, not
  just query latency.
- **Compaction starvation during traffic spikes**: upsert volume spikes,
  compaction can't keep pace, delete logs pile up, MoR read latency climbs —
  read and write paths degrade together. Size compaction for peak, not
  average.
- **Catalog split-brain**: two writers committing through different catalogs
  (or an outdated HMS) — "the table" diverges depending on who you ask.
  Symptom: reconciliation queries that disagree per engine. One table, one
  catalog.
- **Hidden-partition complacency**: trusting auto-derivation so completely
  that nobody re-reviews partition strategy after query patterns changed —
  e.g., queries now filter by tenant but the table is partitioned by day
  only, so pruning silently stopped helping. Partitioning still needs an
  owner.
- **Time-travel as a backup strategy**: snapshots age out (expiration
  policy!) and reference the *same* underlying files — expired snapshots
  plus object-lifecycle rules can make "rollback" a hollow promise. Time
  travel is a debugging/undo window measured in days; the raw plane remains
  the real archive.

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
