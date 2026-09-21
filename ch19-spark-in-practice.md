# Chapter 19 — Spark in Practice

> Part VI — Serving, Formats & Spark

Spark questions in interviews are really three questions: do you understand
that Spark is a *distributed shuffle engine*, do you know where the data
physically lives (partitions, files, skew), and have you felt the small-files
and skew pain personally. This chapter is the practical layer: layout, reads,
shuffles, and the tuning moves that matter.

## The Question

*"How do I lay out and read data so Spark does the minimum work, and how do I
diagnose the pathologies — small files, skew, shuffle storms — when it
doesn't?"*

## The Physics

### Physical layout: partitioning, bucketing, clustering

What your data looks like *on disk* decides what Spark can skip:

- **Directory partitioning** (`/dt=2026-09-20/`): predicate on `dt` ->
  partition pruning — entire directories never listed or read. The default
  first move; choose grain by query predicates and recompute cost (ch07).
- **Bucketing** (`bucketed by (user_id, 64)`): hash-sharded files *within*
  partitions; two bucketed tables on the same key/join column can skip the
  shuffle for joins (pre-shuffled at write time). Niche but interview-gold
  when it applies: repeated joins of the same two big tables.
- **Z-ordering / clustering** (lakehouse tables, ch12): multi-column sort
  within files so min/max stats prune on *several* predicates at once —
  `WHERE user_id AND dt AND event_type` all benefit, at the cost of periodic
  re-clustering jobs.
- Over-partitioning is the standing hazard: `dt/hour/minute` at moderate
  volume -> millions of tiny directories -> the small-files disease (below).

### Read efficiency: pushdown, skipping, pruning

Against Parquet/Iceberg (ch17/18), Spark will:

1. **prune partitions** (directory level),
2. **skip row-groups/chunks** via min/max statistics (predicate pushdown),
3. **read only projected columns** (columnar I/O).

Your job is to *let* it: filter and select **early** —
`df.select("user_id","amt").filter(col("dt") === ...)` — because the engine
can only skip what the plan's predicates and projections expose. Late filters
after wide transformations (or UDFs that hide logic from the optimizer) turn
pushdown off silently. The gap between "reads 5% of bytes" and "reads 100%"
is usually one lazy `.select()` placed too late.

### The small-files problem — and the sizing targets

Symptoms, in order of appearance: slow query *planning* (driver enumerates
millions of files), task scheduling overhead (10x more tasks than data
warrants), name-node/catalog pressure, finally "everything is slow and nobody
knows why."

Causes: over-partitioning; streaming micro-batch commits every N seconds
(ch09); high-frequency incremental writers.

The fixes, in order of preference:

- **Write fewer, larger files**: target **128MB-1GB per file** (rule of
  thumb; HDFS block-era heritage at 128MB, lakehouse comfort up to ~1GB).
  `df.repartition(n)` before write (below), or `coalesce` for shrinking.
- **Compaction jobs** (scheduled): rewrite many small files into few large
  ones — a standing duty in every lakehouse (ch12's price).
- **AQE coalescing** on the read side (below) mitigates, doesn't cure.
- **Design partitions right the first time**: fewer, coarser partitions that
  stay big.

### Shuffle: the real cost center

Most Spark stages are: read -> shuffle (repartition by key) -> aggregate/join.
The shuffle moves data across the network and spills to disk when memory
runs out — it *is* the distributed part of distributed computing.

- **Default 200 shuffle partitions is wrong for nearly every job.** Too few
  -> huge tasks, spills; too many -> tiny tasks, overhead. Size to data:
  aim for ~100-200MB per shuffle partition; let AQE fix the rest.
- **AQE (Adaptive Query Execution)**: coalesces post-shuffle partitions
  (kills the 200-default problem), switches join strategies at runtime,
  handles **skewed joins** by splitting skewed keys' partitions. On modern
  Spark, AQE is the first lever, manual `spark.sql.shuffle.partitions` the
  second.
- **Broadcast joins**: when one side is small (~10MB-100MB rule of thumb,
  configurable), ship it to every executor in memory; no shuffle at all.
  Traps: auto-broadcast of a "small" table that's actually 2GB -> OOM or the
  famous **broadcast timeout**; explicit `.broadcast()` on the wrong side.
- **Skew**: one hot key (user_id=42 with 30% of rows) -> one straggler task
  while the cluster idles. Fixes: AQE skew handling; **salting** (add random
  suffix to key, aggregate twice) as the manual classic; or isolate the hot
  key and process it separately.

### repartition vs coalesce — the interview classic

| | `repartition(n)` | `coalesce(n)` |
|---|---|---|
| Mechanism | full shuffle | merge existing partitions (narrow) |
| Can increase parallelism | yes | no (only shrink) |
| Cost | expensive (network) | cheap |
| Use | before write (even files), fix skew, balance | shrink output files cheaply, post-filter |

Rule: **coalesce to shrink, repartition when balance or more parallelism
matters.** Writing 1TB as 8 files? repartition(8) is wrong (8 huge files,
8 tasks); writing 10k tiny files? repartition to ~data/512MB.

### Caching — and its trap

`df.cache()` / `persist()`: keep a DataFrame in memory/disk across actions —
pays off when reused (iterative algorithms, a DataFrame feeding several
downstream writes). The traps: caching *twice* (two storages levels, double
memory), caching a plan that's never reused (pure overhead), and eviction
silence (cached partition evicted -> recomputed, and nobody notices until the
job doubles in runtime). Storage levels (`MEMORY_ONLY`,
`MEMORY_AND_DISK`, `DISK_ONLY`, serialized variants) are a memory-vs-CPU
knob; default `MEMORY_AND_DISK` (deserialized) is usually fine, serialized
`MEMORY_ONLY_SER` for wide DataFrames.

### Structured Streaming specifics

- **Checkpointing**: offsets + state + task state to durable storage — the
  resume contract (ch09). Treat checkpoint location as production data.
- **Watermarks** in Spark: `withWatermark("ts", "10 minutes")` — bounds state
  for stream-stream joins and aggregations (ch08 semantics, micro-batch
  granularity).
- **Output modes**: `append` (finalized rows only — requires watermark for
  aggregations), `update` (changed rows), `complete` (full result each
  trigger — only for small state). Choosing wrong = unbounded state or
  missing updates; the mode/state/watermark triangle is the whole
  Structured Streaming exam.
- **foreachBatch**: the escape hatch for non-idempotent sinks — apply a
  batch function per trigger (e.g., MERGE into a lakehouse table, ch12) with
  idempotent writes inside.

### The mindset

**Spark is a distributed shuffle engine; tuning = minimizing data movement.**
Every pathology in this chapter (skew, small files, spills, OOM, broadcast
timeouts) is data landing in the wrong place at the wrong size. When a job is
slow: look at the shuffle read/write per stage, the task count vs data size,
and the stragglers — the plan tells you which shape of wrong you have.

## The Options

| Tuning lever | What it fixes | Overhead |
|---|---|---|
| Partitioning / layout (pushdown-friendly) | scan volume | one-time design + discipline |
| AQE | shuffle sizing, skew, join strategy | on by default; verify |
| Broadcast joins | shuffle elimination | memory risk on misjudged size |
| Salting / key isolation | stubborn skew | code complexity |
| repartition before write | file count/size | one shuffle |
| Compaction jobs | small-files at rest | scheduled duty |
| Caching | recomputation | memory + eviction risk |

## Decision Rules

- **Design layout for the query**: partition by the dominant predicate;
  cluster/z-order when multiple predicates matter; bucket only for repeated
  big joins on a stable key.
- **Target 128MB-1GB files**; repartition before write; schedule compaction
  for streaming-written tables.
- **Let AQE work**; override `spark.sql.shuffle.partitions` only when the
  plan shows persistent mis-sizing.
- **Broadcast the small side consciously** — check its actual size, know the
  timeout failure mode.
- **Skew: AQE first, salting when AQE can't, hot-key isolation as the
  surgical option.**
- **Cache only what's reused; never twice; monitor evictions.**
- **Diagnose from the plan**: shuffle bytes per stage, tasks vs data,
  stragglers — before touching any config.

## Failure Modes

- **The straggler stage**: 199 tasks finish in 3 minutes, one runs 40 —
  skew. Symptom: progress bar at "199/200" forever. Salt or isolate the hot
  key.
- **Small-file suffocation**: streaming commits every 10s into 50
  partitions; a month later every query plans over 400k files. Fix: compaction
  + coarser partitions + write-time repartition (this is ch12's price, paid
  in Spark).
- **Broadcast timeout**: auto-broadcast picks a "small" fact dimension
  table that's grown for two years; one day it's 3GB and jobs fail with
  `SparkException: Could not broadcast` or timeouts. Fix: check sizes,
  disable auto for that join, sort-merge instead.
- **The 200-partition shuffle at 10GB scale**: 50MB partitions, scheduling
  overhead dominating; or at 10TB scale: 50GB partitions spilling everything.
  AQE fixes both if enabled; verify it actually is.
- **Cache-and-forget**: cached DataFrame feeding three downstream writes —
  but evicted halfway; job silently recomputes and "randomly" takes 3x on
  Fridays.
- **UDD-hidden predicates**: a UDF in the filter chain disables pushdown;
  the "optimized" pipeline full-scans because the optimizer can't see through
  the lambda.

## Interview Narration

"My mental model of Spark is a distributed shuffle engine — nearly every
tuning decision is about moving less data. That starts on disk: partition by
the dominant query predicate so pruning happens at the directory level, and
let Parquet's min/max statistics skip row groups — which means filters and
column selection early in the plan, because pushdown only works for
predicates the optimizer can see.

When a job is slow I read the plan, not the configs: shuffle bytes per stage
tell me if I'm moving too much data; task count versus data size tells me if
partitions are mis-sized — the default 200 shuffle partitions being wrong in
both directions; and the classic '199 tasks done, one straggler' is skew, so
AQE's skew handling first, salting the hot key when AQE can't.

Small files are the chronic lakehouse disease: streaming commits every ten
seconds produce hundreds of thousands of files, and query planning drowns. My
standing rules: target 128 megabytes to a gigabyte per file, repartition
before write, and schedule compaction as a first-class job — it's the
operational price of streaming into table formats.

And the classics: broadcast joins for genuinely small sides — with the
honesty that auto-broadcast of a table that grew for two years is how you
meet the broadcast timeout — repartition versus coalesce as
shuffle-to-rebalance versus narrow-merge-to-shrink, and caching only
reused DataFrames, once, with eviction monitoring. The theme underneath all
of it: data in the wrong place at the wrong size is every Spark pathology."

---

| <- Previous | Next -> |
|---|---|
| [Chapter 18 — File & Wire Format Catalog](ch18-file-and-wire-format-catalog.md) | [Chapter 20 — Security, Governance & Catalogs](ch20-security-governance-and-catalogs.md) |