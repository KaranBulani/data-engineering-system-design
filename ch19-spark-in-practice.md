# Chapter 19 — Spark in Practice

> Part VI — Serving, Formats & Spark

Spark interview questions are really three questions bundled together. First, do you understand that Spark is essentially a distributed shuffle engine — an engine whose main job is moving data between machines? Second, do you know where the data physically lives: how it is split into partitions and files, and why one key can be much heavier than the others? Third, have you personally felt the two chronic pains, too many small files and skewed data? This chapter is the practical layer: how to lay data out on disk, how to read it efficiently, what shuffles cost, and the tuning moves that actually matter.

A quick definition before starting: a **shuffle** is the step in a Spark job where records with the same key must be gathered onto the same machine — for example, all rows for `user_id = 42` must end up together before a `GROUP BY user_id` can finish. Gathering them means copying data across the network and often writing it to disk. Shuffles are usually the most expensive part of a job, which is why most Spark tuning is about avoiding or shrinking them.

## The Question

How do I lay out data on disk and read it so that Spark does the minimum possible work? And when a job is still slow, how do I diagnose the usual pathologies — small files, skewed keys, and oversized shuffles — instead of guessing at configs?

## The Physics

### Physical layout: partitioning, bucketing, clustering

How the data looks on disk decides what Spark can skip before reading anything. Three layout techniques matter:

- **Directory partitioning** means splitting the data into directories named by a column, for example `/dt=2026-09-20/`. A query that filters on `dt = '2026-09-20'` then reads only that one directory and never even lists the others. This is called **partition pruning**, and it is the default first move when designing a table. The partition column should be chosen by looking at what your queries filter on and how much data one partition holds for recomputation (ch07).
- **Bucketing** means pre-sharding the data into a fixed number of files inside each partition by hashing a column, for example `bucketed by (user_id, 64)`. Every row with the same `user_id` always lands in the same bucket file. If two large tables are bucketed the same way on the join column, Spark can join them without a shuffle, because matching keys are already sitting on the same file layout — the shuffle happened once at write time instead of on every query. **This is a niche technique (both tables must be written with the same bucketing, and writers must keep that discipline)**, but it is interview-gold when it applies: repeated joins of the same two big tables on a stable key.
- **Z-ordering / clustering** (used by lakehouse tables, ch12) means sorting the data inside files by several columns at once using a space-filling curve. The effect is that the min/max statistics of each file become tight for *several* columns, so data skipping works for several predicates at once — a query filtering on `user_id`, `dt`, and `event_type` all benefits. The cost is a periodic re-clustering job that rewrites files.

The standing hazard is **over-partitioning**: partitioning by `dt/hour/minute` at moderate volume creates millions of tiny directories, and each directory ends up holding tiny files. That is the small-files disease, covered below.

### Read efficiency: pushdown, skipping, pruning

Against a columnar format with statistics (Parquet, or an Iceberg table, ch17/ch18), Spark applies three filters before any real work:

1. **Partition pruning** — decide at the directory level which folders the query needs.
2. **Row-group skipping** — use each Parquet chunk's min/max statistics to skip chunks that cannot match the filter (predicate pushdown, ch18).
3. **Column projection** — read only the columns the query references, because the format is columnar.

Your job is to *let* Spark do this: filter and select **early**, for example `df.select("user_id", "amt").filter(col("dt") === "2026-09-20")` at the top of the job. The engine can only skip data that the query plan's predicates and projections expose to it. A filter placed after a wide transformation no longer maps to files on disk, so nothing can be skipped. A UDF is worse: the optimizer cannot see inside a function, so any logic hidden there is invisible to pushdown. The gap between "reads 5% of the bytes" and "reads 100% of the bytes" is often just one lazy `.select()` or one filter placed too late in the chain.

### The small-files problem — and the sizing targets

Small files hurt in stages. First, query *planning* gets slow, because the driver enumerates every file one by one. Then, task scheduling overhead dominates: Spark creates at least one task per file, and a task that reads 200KB still costs the same scheduling effort as one reading 500MB. Then the catalog or name-node feels pressure from tracking millions of objects. Finally, the vague state nobody can diagnose: "everything is slow and nobody knows why."

The causes are predictable: over-partitioning, streaming jobs committing a micro-batch every few seconds (each commit writes its own small files into every partition, ch09), and incremental writers running too frequently.

The fixes, in order of preference:

- **Write fewer, larger files.** Target **128MB–1GB per file**. The 128MB floor is heritage from the HDFS block size; lakehouse engines are comfortable up to about 1GB. Use `df.repartition(n)` before writing to control the file count (see the repartition section below), or `coalesce(n)` when only shrinking is needed.
- **Run scheduled compaction jobs** that rewrite many small files into few large ones. In any lakehouse fed by streaming, compaction is a standing duty, not an emergency fix (this is ch12's price).
- **Let AQE coalesce small partitions on the read side** (explained below). This mitigates the pain at query time but does not cure the disease at rest.
- **Design partitions correctly the first time**: fewer, coarser partitions that each stay large.

### Shuffle: the real cost center

Most Spark stages follow the same shape: read, then a shuffle to repartition data by key, then an aggregation or join. The shuffle is where data crosses the network and spills to disk when memory runs out — it *is* the distributed part of distributed computing, and it is where most jobs lose their time.

- **The default of 200 shuffle partitions is wrong for nearly every job.** It is a historical default, not a recommendation. Too few partitions means huge tasks that exhaust memory and spill to disk; too many means tiny tasks whose scheduling overhead dominates. The rule of thumb is to aim for roughly 100–200MB of data per shuffle partition, and to let AQE fix the remainder automatically.
- **AQE (Adaptive Query Execution)** re-plans the job at runtime using actual sizes instead of estimates. It coalesces post-shuffle partitions that turned out too small (which fixes the 200-default problem), switches join strategies when a side turns out smaller than estimated, and handles **skewed joins** by splitting a skewed key's oversized partition into pieces. On modern Spark, AQE is the first lever and manual `spark.sql.shuffle.partitions` tuning is the second.
- **Broadcast joins** avoid the shuffle entirely when one side is small — roughly 10MB to 100MB as a rule of thumb, configurable. Spark ships the small table to every executor's memory, and each executor joins its local data against it locally; nothing crosses the network at join time. The traps: auto-broadcast of a "small" table that has quietly grown to 2GB, which causes out-of-memory errors or the famous **broadcast timeout** (building the broadcast must finish within a time limit, and a grown table misses it); and explicitly calling `.broadcast()` on the wrong, large side.
- **Skew** means one key holds a disproportionate share of rows — for example `user_id = 42` owns 30% of the table. All that data must land in one partition, so one straggler task runs for an hour while the rest of the cluster finishes in minutes and idles. The fixes, in order: AQE's skew handling splits the oversized partition automatically; **salting** is the manual classic — add a random suffix to the hot key (turning `42` into `42_0` … `42_9`), aggregate once on the salted key to spread the work, then aggregate those partial results by the real key; or isolate the hot key entirely — filter `user_id = 42` out, process it with dedicated code, and union the results back.

### repartition vs coalesce — the interview classic

Both operations change how many partitions a DataFrame has, and the difference between them is a favorite interview question.

| | `repartition(n)` | `coalesce(n)` |
|---|---|---|
| How it works | Full shuffle: all data is redistributed across the cluster | Merges existing partitions into fewer ones, without a shuffle |
| Can it increase parallelism? | Yes | No — it can only shrink the count |
| Cost | Expensive — data crosses the network | Cheap — no data movement between machines |
| Typical use | Before writing (to size files evenly), fixing skew, balancing load | Cheaply shrinking output files after a filter removed most rows |

The rule: **coalesce to shrink, repartition when balance or more parallelism matters.** Two concrete examples of getting it wrong: writing 1TB as `repartition(8)` produces 8 enormous files read by 8 tasks — a self-inflicted parallelism ceiling; and writing 10,000 tiny files happens when you skip repartitioning entirely — instead, repartition to roughly one partition per 512MB of output.

### Caching — and its trap

`df.cache()` or `df.persist()` tells Spark to keep a DataFrame's data in memory (and optionally disk) after it is first computed, so later actions reuse it instead of recomputing the whole lineage. It pays off when the data is genuinely reused — iterative algorithms that touch the same DataFrame repeatedly, or one expensive DataFrame that feeds several downstream writes.

The traps are just as real. Caching the same underlying data twice — through two DataFrames that each independently cache their common source — **doubles memory usage**. Caching a plan that is never used again is pure overhead with no benefit. And eviction is silent: if a cached partition is evicted to free memory, Spark quietly recomputes it on the next use, and nobody notices until the job randomly doubles in runtime. Storage levels (`MEMORY_ONLY`, `MEMORY_AND_DISK`, `DISK_ONLY`, and serialized variants) are a memory-versus-CPU knob: the default `MEMORY_AND_DISK` is usually fine, and serialized levels store a compact byte representation rather than full in-memory objects. This can reduce memory use, especially for DataFrames with many columns, but takes extra CPU to serialize and deserialize. 
### Structured Streaming specifics

- **Checkpointing** writes the stream's progress to durable storage: the offsets read so far (how far into the source the stream got), the running state (partial aggregates), and commit records (which micro-batches finished). On restart, the stream resumes from the checkpoint instead of reprocessing or losing data — this is the resume contract (ch09). Treat the checkpoint location as production data: deleting it means losing progress and state, and there is no way to rebuild it from the output.
- **Watermarks** bound how long state is kept for late events. `withWatermark("ts", "10 minutes")` on an aggregation tells Spark to keep a window's state until event time has moved 10 minutes past the window's end, then drop it. Without a watermark, state for every window ever opened is kept forever and the job eventually dies; with one, too-late events are discarded rather than stored. This is ch08's watermark semantics, enforced at micro-batch granularity.
- **Output modes** control what each micro-batch emits: `append` emits only finalized rows (for aggregations this requires a watermark, because a row is final only once the watermark has passed it), `update` emits only rows that changed since the last trigger (suited to upsert sinks), and `complete` re-emits the entire result every trigger (only viable when the whole result is small). Choosing the wrong mode means either unbounded state or missing updates; the mode–state–watermark triangle is the whole Structured Streaming exam.
- **foreachBatch** is the escape hatch for sinks that cannot receive streaming writes directly: it applies an ordinary batch function to each micro-batch, for example a `MERGE INTO` a lakehouse table (ch12). Because Spark may re-execute a batch after a failure, the function inside must be idempotent.

### The mindset

**Spark is a distributed shuffle engine; tuning means minimizing data movement.** Every pathology in this chapter — skew, small files, spills, out-of-memory, broadcast timeouts — is the same disease: data landing in the wrong place at the wrong size. When a job is slow, look at three things before touching any config: the shuffle read/write bytes per stage (how much data is moving), the task count versus the data size (whether partitions are mis-sized), and the straggler tasks (whether the work is unevenly shaped). The plan tells you which shape of wrong you have.

## The Options

| Tuning lever | What it fixes | Overhead |
|---|---|---|
| Partitioning / layout designed for pushdown | Scan volume | One-time design plus ongoing discipline |
| AQE | Shuffle sizing, skew, join strategy | On by default — verify it is enabled |
| Broadcast joins | Shuffle elimination | Memory risk if the side's size is misjudged |
| Salting / hot-key isolation | Skew AQE cannot fix | Code complexity |
| repartition before write | File count and size | One shuffle |
| Scheduled compaction | Small files at rest | A standing scheduled job |
| Caching | Recomputation | Memory usage plus silent-eviction risk |

## Decision Rules

- **Design the layout for the queries.** Partition by the dominant filter column, add clustering/Z-ordering when several columns are filtered together, and use bucketing only for repeated large joins on a stable key.
  - Bucketing hashes rows into a fixed number of groups (“buckets”) based on a column. Unlike partitioning, it usually doesn’t create a separate directory for every key value.
  - The recommendation means: bucket tables by a join key when you repeatedly join large tables on that same key, and the query engine can use the bucket layout to match rows from each table without—or with less—data shuffling.
  - For example, if large orders and customers tables are often joined on customer_id, bucketing both tables by customer_id with compatible bucket settings may help the engine line up the matching rows.
  - “Stable key” means the join column is a consistent, dependable choice for those joins—not a temporary or frequently changing field. A customer ID is a typical example; a changing status value is not.
  - Bucketing is usually not worthwhile for occasional or small joins: it adds work when writing and maintaining the tables, and the benefit depends on the engine and compatible table layouts. It can also be less helpful if the key is heavily skewed, so a few buckets end up much larger than the rest.
- **Target 128MB–1GB files.** Repartition before writing, and schedule compaction as a normal job for any table that streaming writes into.
- **Let AQE work.** Only override `spark.sql.shuffle.partitions` when the query plan shows persistent mis-sizing that AQE is not correcting.
- **Broadcast the small side deliberately.** Check its actual size first, and know the timeout failure mode for when it has grown.
- **Handle skew in escalation order:** AQE first, salting when AQE cannot split the key, and hot-key isolation as the surgical option for extreme cases.
  - Hot-key isolation means separating an exceptionally frequent key from the normal data path and handling it with a specialized path, then combining the results.
  - For example, if user_id = 42 accounts for a huge share of a join:
    - Process all other user IDs with the regular join.
    - Filter out user_id = 42 and handle those rows separately—using a strategy suited to that case, such as splitting the work by another column.
    - Union the two results.
  - The separate path must still distribute the hot key’s work; merely filtering it into a separate job that sends all its rows to one task won’t solve the skew.

- **Cache only what is reused, cache it exactly once, and monitor evictions** in the Spark UI's storage tab.
- **Diagnose from the plan, not from configs.** Shuffle bytes per stage, task count versus data size, and straggler tasks tell you the shape of the problem before any setting is changed.

## Failure Modes

- **The straggler stage.** 199 tasks finish in 3 minutes and one task runs for 40 — the progress bar sits at "199/200" forever. That is skew: a hot key concentrated all the work into one partition. Fix by salting or isolating the hot key.
- **Small-file suffocation.** A streaming job commits every 10 seconds into 50 partitions; a month later every query plans over 400,000 files and planning time exceeds query time. Fix: coarser partitions, write-time repartition, and scheduled compaction — the operational price of streaming into table formats (ch12), paid here in Spark.
- **The broadcast timeout.** Auto-broadcast picks a "small" dimension table that has been growing for two years; one day it is 3GB, and jobs fail with `SparkException: Could not broadcast` or timeout errors. Fix: check actual sizes, disable auto-broadcast for that join, and let sort-merge join handle it.
- **The 200-partition shuffle at the wrong scale.** At 10GB total, 200 partitions means 50MB each — mostly scheduling overhead. At 10TB total, it means 50GB each — everything spills to disk. AQE fixes both directions if it is enabled, so verify it actually is.
- **Cache-and-forget.** A cached DataFrame feeds three downstream writes, but part of it is evicted halfway through; the job silently recomputes the lineage and "randomly" takes 3x on Fridays. Eviction never announces itself — monitor the storage tab.
- **UDF-hidden predicates.** A UDF inside the filter chain makes the predicate invisible to the optimizer, so pushdown silently turns off and the "optimized" pipeline full-scans the table. The optimizer cannot see through a lambda; keep filters as plain column expressions.

## Interview Narration

"My mental model of Spark is a distributed shuffle engine — nearly every tuning decision comes down to moving less data. That starts on disk. I partition by the dominant query predicate so pruning happens at the directory level, and I keep Parquet's min/max statistics usable, which means filters and column selection early in the plan, because pushdown only works for predicates and projections the optimizer can actually see.

When a job is slow, I read the plan before touching configs. Shuffle bytes per stage tell me whether too much data is moving. Task count versus data size tells me whether partitions are mis-sized — the default 200 shuffle partitions is wrong in both directions, too few at large scale and too many at small scale. And the classic 199-tasks-done-one-straggler picture is skew, so I take AQE's skew handling first and salt the hot key when AQE cannot split it.

Small files are the chronic lakehouse disease. Streaming commits every ten seconds produce hundreds of thousands of files, and query planning drowns before any data is read. My standing rules: target 128 megabytes to a gigabyte per file, repartition before writing, and run compaction as a first-class scheduled job — that is the operational price of streaming into table formats.

And the classics: broadcast joins for genuinely small sides, with the honesty that auto-broadcasting a table that has grown for two years is exactly how you meet the broadcast timeout. Repartition versus coalesce is a full shuffle to rebalance versus a cheap narrow merge to shrink. And caching only reused DataFrames, exactly once, with eviction monitoring. The theme under all of it: every Spark pathology is data in the wrong place at the wrong size, and the query plan shows you which shape of wrong you have."

---

| <- Previous | Next -> |
|---|---|
| [Chapter 18 — File & Wire Format Catalog](ch18-file-and-wire-format-catalog.md) | [Chapter 20 — Security, Governance & Catalogs](ch20-security-governance-and-catalogs.md) |
