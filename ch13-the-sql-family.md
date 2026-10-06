# Chapter 13 — The SQL Family

> Part V — Storage Engines: SQL vs NoSQL

"SQL doesn't scale" is the single most interview-dockable sentence in data
engineering. The reason it's wrong: **SQL is a language, not an engine** —
and the family of engines that speak it contains half a dozen very different
machines. A row-store like Postgres, a cloud warehouse like Snowflake, a
distributed database like Spanner, and a query engine like Trino all run
SQL, and they scale in completely different ways for completely different
jobs. So the precise truth is: *a specific SQL engine on a specific
workload* may not scale. This chapter is about telling those machines apart,
picking the right one, and defending the choice.

## The Question

*"When do I reach for a relational engine — and which kind — versus a NoSQL
store, for this specific workload?"*

Unpacked: the answer depends on the workload's *shape*, not on fashion or
on "SQL vs NoSQL" as a slogan. The running example for this chapter is the
e-commerce company from ch12, which has two utterly different SQL jobs:

- **Checkouts**: when a customer clicks "buy," the app reads one order, one
  inventory row, one customer row, writes an order, and decrements stock —
  in one transaction, in milliseconds, thousands of times per minute.
- **Analytics**: an analyst asks "revenue by day, by category, for the last
  two years" — a scan over billions of rows, once, producing one chart.

Both are "SQL." No single engine does both well. The first half of this
chapter is the split that explains why.

## The Physics

### The first split: OLTP vs OLAP

Two pieces of vocabulary: **OLTP** (online transaction processing) is the
operational database the application talks to — Postgres, MySQL. **OLAP**
(online analytical processing) is the analytics engine — Snowflake,
BigQuery, Redshift. Same language, opposite workloads:

| | OLTP (Postgres, MySQL) | OLAP (Snowflake, BigQuery, Redshift) |
|---|---|---|
| Query shape | point lookups, small ranges, short transactions | wide scans, big aggregations, joins |
| Layout | row store | column store (ch17) |
| Writes | frequent, small, transactional | bulk, batched |
| Latency unit | milliseconds | hundreds of ms to minutes |
| Concurrency model | many small txns | fewer big queries, queue/slot-managed |
| Indexes | B-trees everywhere | partition/clustering pruning, zone maps |

Walked through with the orders example:

- **Query shape**: "load order #48213's page" is a point lookup — one row by
  key. "Revenue by day for two years" is a wide scan — most of the table,
  aggregated. These are not two sizes of the same query; they are different
  species.
- **Layout**: a row store keeps each order's fields together on disk (good
  for loading one whole order); a column store keeps all values of a column
  together (good for summing one column across billions of rows — deep dive
  in ch17).
- **Writes**: checkouts write small rows constantly, each needing
  transactional safety; a warehouse ingests yesterday's events in bulk,
  batched.
- **Concurrency**: OLTP juggles thousands of tiny transactions at once;
  a warehouse runs a handful of huge queries, managed through queues and
  slots.

The classic failure: running analytics (wide scans) on an OLTP engine. The
analyst's two-year revenue scan drags through every row of the orders table,
evicts the database's cache of hot pages, and contends for locks — checkout
latency collapses because someone ran a chart query on the checkout
database. The inverse failure: running order-processing transactions on a
warehouse — possible, but every small write is priced and scheduled like a
mini analytics query, which is expensive and uses the wrong concurrency
model. First question for any SQL decision: *which side of this table is the
workload?*

### Row-store mechanics (why OLTP is fast at its job)

- **B-tree indexes**: a B-tree is a sorted, tree-shaped index kept on disk
  in pages, with high branching factor so it stays shallow. "Shallow" is the
  point: finding one order is 3-4 page reads from root to leaf — a handful
  of disk touches. A **secondary index** (say, on `customer_email`) is just
  another B-tree whose leaves store the primary key, so the engine finds the
  email, gets the primary key, and hops to the row.
- **Clustered vs secondary**: in InnoDB (MySQL's engine), the clustered
  index *is* the table — rows are physically stored in primary-key order.
  Secondary indexes therefore work as "index -> PK -> lookup" hops into that
  layout, which is why a lookup through a secondary index costs two index
  traversals.
- **The write cost**: every index on a table must be updated on every
  insert/update. A table with 8 secondary indexes costs 9 writes per insert
  (the row plus 8 index entries). This is why OLTP tables carry few,
  deliberate indexes — and why "add an index for my dashboard query" on a
  production OLTP table is a loaded suggestion: it taxes every checkout
  write, forever, to speed up a query that belongs in a warehouse.
- **Transactions (ACID)**: the machinery that makes "transfer money" safe —
  debit one account, credit another; both happen or neither does, even
  mid-crash. Under the hood: row locks plus **MVCC** (multi-version
  concurrency control) — readers see a consistent snapshot of the data
  without blocking writers, and writers don't block readers. This capability
  is exactly what NoSQL systems trade away for scale (ch14).

### Warehouse mechanics (why OLAP is fast at its job)

- **Columnar storage** (deep dive ch17): to compute revenue by day, the
  engine reads only the `total` and `date` columns and skips the other 40
  columns entirely; similar values compress extremely well (dates repeat,
  prices cluster); and **vectorized execution** processes values in CPU
  cache-sized batches instead of one row at a time.
- **Compute isolation**: Snowflake virtual warehouses / BigQuery slots —
  compute is spun up per workload while storage stays shared and central.
  The BI dashboard's compute and the data-science scan are separate clusters
  — they can't contend for the same cores, and they are *separate bills*.
  That is simultaneously the feature (no noisy neighbors) and the cost trap
  (shadow warehouses quietly multiplying).
- **Result caching**: identical query text -> cached result, near-zero cost.
  Dashboards hammering the same 6 queries are (nearly) free — until one
  dashboard filters on a volatile `CURRENT_TIMESTAMP`. Every refresh has
  different query text and a different time window, so every refresh is a
  fresh full-priced scan; the cache never hits.
- **Partition/clustering pruning**: physically organizing data by date or
  cluster key so that `WHERE dt = '2026-09-20'` reads one day's slice of the
  table instead of scanning two years. In BigQuery, partition + cluster keys
  *are* the index — there is no B-tree to add — and because the cost model
  is bytes scanned, pruning is a *financial* decision, not just a
  performance one.
- **Cost = bytes scanned** (BigQuery) or time-sized compute (Snowflake): in
  a scan-priced warehouse, `SELECT *` on a 500-column table pays for all
  500 columns' bytes — the `SELECT *` habit is a bill, not a style choice.
  This is why ch18's column pruning matters even inside warehouses.

### Distributed SQL (the brief aside)

Spanner, CockroachDB, YugabyteDB: SQL with horizontal scale — data sharded
across many nodes — *and* strong consistency, achieved by running consensus
(Paxos/Raft) on each replicated shard. Consensus in one sentence: replicas
vote, and a write commits only when a majority agrees, so a committed
transaction survives node loss — no "the node that had your data died"
anomalies. The honest positioning: they exist for *operational* workloads
that outgrow single-node Postgres while still needing ACID — not for
analytics. If an interviewer probes "how does Spanner get external
consistency," the answer is consensus + TrueTime (Google's atomically
synchronized clock hardware, which lets transactions be ordered globally);
for most designs, "distributed SQL exists, it's for OLTP-at-scale" is
sufficient depth.

### When SQL wins — the decision list

1. **Complex ad-hoc joins and BI semantics**: SQL is the lingua franca of
   analysis — "revenue by day by category, excluding refunds, joined to the
   product catalog" is one query. Joins *are* the analytical workload, and
   NoSQL stores mostly can't do them.
2. **ACID transactions**: money, inventory, state machines — anywhere a
   half-applied update is unacceptable.
3. **Strong consistency as a requirement** (not a preference): when the
   answer must reflect every committed write, immediately.
4. **Ecosystem/tooling**: BI tools speak SQL over JDBC/ODBC (ch16); every
   hiring pool knows SQL; every orchestration tool has a SQL sensor.
5. **Data with relational shape and evolving ad-hoc query patterns** —
   i.e., most analytics. Nobody knows today's questions next quarter; a
   schema-plus-optimizer adapts, a pre-built KV store doesn't.

### The scaling truth (defusing the trap)

- Postgres on a big box with good indexes and read replicas serves tens of
  thousands of transactions/sec. Most "SQL doesn't scale" stories are
  actually one of: "we never indexed" (every lookup a sequential scan), "we
  ran analytics on OLTP" (layout mismatch, above), or "we never pooled
  connections" (10,000 connections, each with overhead, vs a pool of 100).
  All three are fixable without leaving SQL.
- When OLTP genuinely outgrows one node, the paths are: **sharding**
  (splitting rows across nodes by key — application-level complexity, cross-shard
  transactions get hard), **distributed SQL** (the consensus-based engines
  above), or — for read-heavy serving — a **KV cache in front** (ch14/16).
- OLAP scaling is a solved pricing problem: warehouses scale compute and
  storage independently, so terabytes are routine. The constraint is cost
  governance, not capability.

### The query-path physics: how a SQL engine actually answers

Worth 60 seconds of any interview — the four things an engine does with a
query, because every "why is this slow?" is one of these four:

1. **Parse & plan**: SQL -> logical plan -> optimized physical plan (join
   reordering, predicate pushdown, pruning). Example of a bad plan: joining
   two billion-row tables and *then* filtering by date, instead of pushing
   the date filter down and joining a day's worth of rows. A bad plan is
   the #1 silent performance killer — the data is indexed, the hardware is
   fine, the plan is a disaster.
2. **Pruning**: which chunks of the table the engine can skip *without
   reading at all*, decided from cheap metadata before any data is touched.
   Three mechanisms do this. **Partition pruning**: the table is physically
   split by date, so `WHERE order_date >= '2026-09-01'` reads one month's
   slice instead of two years. **Index descent**: the B-tree walks directly
   to the matching rows instead of reading the whole table. **Zone maps**:
   each storage chunk carries min/max statistics per column — the ch17/18
   statistics — so a chunk whose `order_date` range can't possibly match the
   filter is never read from disk, never decompressed. In warehouses this is
   a *cost* decision as much as a performance one — bytes not scanned are
   dollars not spent. And pruning is the step that silently fails: if the
   predicate doesn't mention the partition key, or the statistics are stale,
   the query returns the same result at full price — same answer, many times
   the bill.
3. **Access path**: now that the plan knows *what* to read, this decides
   *how* it is physically fetched. OLTP: a **B-tree seek** — descend three
   or four levels of the tree, land on the handful of matching rows, done in
   microseconds; perfect for "this customer's last order." OLAP: a
   **columnar scan** — read only the referenced columns' chunks and stream
   millions of values per second through the filter; the ch17 layout
   decision, executed. Each path is terrible at the other's job. A B-tree
   answering "average order value over two years" means millions of
   individual seeks — each one cheap, the sum a disaster — while a columnar
   scan answering "one customer's last order" reads a mountain of data to
   return one row. Same SQL text, opposite physics: the question's *shape*,
   not its syntax, is what picks the engine.
4. **Concurrency model**: what happens when many queries arrive at once —
   where OLTP and OLAP differ most in *feel*. An OLTP engine runs thousands
   of short transactions per second; row locks plus MVCC (multi-version
   concurrency control — readers see a consistent snapshot without blocking
   writers, as covered above) mean each query touches a few rows for a few
   milliseconds and leaves. The machine's job is throughput of small work. A
   warehouse inverts this: one query is a big scan holding cores and memory
   for seconds to minutes, so uncontrolled concurrency would melt the
   machine. OLAP engines therefore admit work through **slots/queues**
   (Snowflake warehouse sizes, BigQuery slot fair sharing): queries wait in
   line, then run at full speed. That is the "40 dashboards" physics of
   ch16 — scheduled dashboard refreshes form a queue, not a pile of
   independent fast lookups, and the system stays up precisely because big
   queries are queued rather than all running at once.

### Indexing and partitioning — the OLTP pair, in one table

| | Index (B-tree) | Table partitioning |
|---|---|---|
| Speeds up | point lookups, small ranges on the indexed columns | maintenance + partition pruning on range predicates |
| Costs | every write updates every index | every query must include the partition key to prune |
| Rule | index the foreign keys and the where-clauses you actually run | partition by time (almost always), sub-partition only at real scale |
| Anti-pattern | indexing "everything just in case" | partitioning by a column queries never filter on |

How to read it with the orders example: `customer_id` is a foreign key used
by "show this customer's orders," so it gets an index; the table is
partitioned by month because nearly every query — dashboards, billing,
cleanup — filters on a date range. The anti-patterns are symmetric mistakes:
eight speculative indexes that tax every write, or a partition scheme by
`status` that no query ever filters on and therefore never helps.

### HTAP and the virtualization escape hatch

Two hybrid families worth naming:

- **HTAP** (TiDB, SingleStore) — one engine attempting OLTP + OLAP together,
  eliminating the sync between an operational DB and an analytical copy, but
  paying both workloads' costs in one system, and being second-best at both
  in most benchmarks. Useful at moderate scale; a compromise, not a magic
  unification.
- **Query virtualization** (Trino/Presto federation, Snowflake external
  tables, warehouse sharing) — SQL over *other people's* storage without
  moving data: Trino queries the Postgres database, the S3 Parquet files,
  and the Iceberg table as if they were one schema (the ch03/06 "no
  pipeline" idea, now as a query layer). Great for exploratory questions;
  not where heavy recurring workloads should live.

Neither replaces the OLTP/OLAP split for serious workloads; both are
legitimate answers to "how do we query this *without* building a pipeline
first?"

## The Options

How to read the table: match the *need* first — the engine class and
examples follow from it, not the other way around.

| Need | Engine class | Examples |
|---|---|---|
| Transactions, point queries | OLTP | Postgres, MySQL |
| Analytics, BI, SQL on big data | OLAP warehouse | Snowflake, BigQuery, Redshift |
| SQL on the lake | lakehouse query engines | Trino/Presto, Spark SQL, Databricks SQL |
| OLTP at planetary scale + ACID | distributed SQL | Spanner, CockroachDB |
| Embedded/light | SQLite, DuckDB | prototypes, local analytics (DuckDB is a phenomenon) |

## Decision Rules

- **Classify the workload (OLTP vs OLAP) before naming an engine.** The
  query shapes decide the machine; picking Postgres-vs-NoSQL first skips the
  only question that matters.
- **Analytics on an OLTP database is a bug**: replica + offload, or move it
  to the warehouse. There is no tuning fix for a layout mismatch.
- **Default analytics substrate: warehouse or lakehouse** — decided by
  ecosystem gravity and cost model (ch12, ch16), not by feature checklists.
- **`SELECT *` is a cost decision** in scan-priced warehouses; prune columns.
  Every unneeded column is bytes billed.
- **In Postgres-scale OLTP problems, suspect indexing/connection pooling
  before suspecting Postgres.** The engine is rarely the bottleneck at
  this scale; the configuration usually is.
- **ACID as a hard requirement eliminates most NoSQL options immediately**
  (ch14) — say so out loud and the decision tree shortens to SQL engines
  (plus a KV store in front for reads).

## Failure Modes

- **Analytics on prod OLTP**: the ch04 incident, now from the storage side —
  a full scan reads every row, evicts the buffer pool (the database's hot
  page cache), and suddenly every checkout is reading from disk. OLTP response times explode — every checkout goes from milliseconds to seconds — while the dashboard 'finished fine.'
- **The un-indexed foreign key**: every join on the un-indexed side is a
  sequential scan *per row* of the other side — "the query is slow" is
  really "the plan is a disaster," and the fix is one index, not new
  hardware.
- **Warehouse cost runaway**: `SELECT *` on a wide partitioned table, per
  dashboard refresh, 40 dashboards — the bill is the incident. Fix: column
  pruning, partition pruning, materialized/pre-aggregated marts (ch16).
- **Cache-busting query patterns**: `WHERE ts > NOW() - interval 1 hour` in
  a 30-dashboard rotation — every refresh has new query text, so the result
  cache never hits and every refresh is a fresh scan; nobody notices until
  the finance review. Fix: rounded time windows (e.g., floor to the hour)
  or pre-aggregation.
- **Cross-graded engines**: transactions on a warehouse (concurrency model
  mismatch) or terabyte scans on Postgres (layout mismatch) — both are
  "right tool, wrong job" failures that present as mysterious slowness, and
  both are decided by the OLTP/OLAP classification the chapter opened with.

## Interview Narration

"I'd start by splitting the SQL family, because 'SQL' is three different
machines: OLTP engines like Postgres — row stores, B-trees, MVCC, built for
millisecond transactions; warehouses like Snowflake or BigQuery — columnar,
compute-isolated, partition-pruned, built for wide scans and BI; and lakehouse
engines like Trino over Iceberg — SQL over open table formats.

For this workload [given case], the reads are analytical — wide scans feeding
dashboards — so the serving substrate is a warehouse or lakehouse, and the
operational source stays an OLTP database with CDC feeding the pipeline —
never analytics queries against prod, that's the classic incident.

On scaling: I'd push back on 'SQL doesn't scale' — Postgres with proper
indexing, pooling, and read replicas handles most workloads people assume
needs NoSQL; what actually doesn't scale is analytics on OLTP layout, and
that's a workload mismatch, not a SQL limitation. When OLTP genuinely
outgrows a node there's distributed SQL — Spanner, CockroachDB — which keeps
ACID via consensus. And inside the warehouse, my cost lever is scan
reduction: partition and cluster pruning, column pruning, and pre-aggregation
for hot dashboards — in a scan-priced model, that's not tuning, that's the
budget."

---

| <- Previous | Next -> |
|---|---|
| [Chapter 12 — Lakehouse & Table Formats](ch12-lakehouse-and-table-formats.md) | [Chapter 14 — The NoSQL Families](ch14-the-nosql-families.md) |
