# Chapter 13 — The SQL Family

> Part V — Storage Engines: SQL vs NoSQL

"SQL doesn't scale" is the single most interview-dockable sentence in data
engineering. The truth: *a specific SQL engine on a specific workload* may not
scale, and the SQL family contains half a dozen very different machines. This
chapter is about picking the right one and defending the choice.

## The Question

*"When do I reach for a relational engine — and which kind — versus a NoSQL
store, for this specific workload?"*

## The Physics

### The first split: OLTP vs OLAP

Same language (SQL), opposite workloads:

| | OLTP (Postgres, MySQL) | OLAP (Snowflake, BigQuery, Redshift) |
|---|---|---|
| Query shape | point lookups, small ranges, short transactions | wide scans, big aggregations, joins |
| Layout | row store | column store (ch17) |
| Writes | frequent, small, transactional | bulk, batched |
| Latency unit | milliseconds | hundreds of ms to minutes |
| Concurrency model | many small txns | fewer big queries, queue/slot-managed |
| Indexes | B-trees everywhere | partition/clustering pruning, zone maps |

The classic failure: running analytics (wide scans) on an OLTP engine — every
full scan is a fight against its layout, memory model, and locking. The
inverse failure: running order-processing transactions on a warehouse — possible
but expensive and wrong-graded concurrency. First question for any SQL
decision: *which side of this table is the workload?*

### Row-store mechanics (why OLTP is fast at its job)

- **B-tree indexes**: ordered, disk-friendly structures; a point lookup is
  3-4 page reads from root to leaf. Secondary indexes are more B-trees whose
  leaves point at the primary key (or row location).
- **Clustered vs secondary**: the clustered index *is* the table layout
  (InnoDB PK = table order); secondary indexes double as "index -> PK ->
  lookup" hops.
- **The write cost**: every index on a table must be updated on every
  insert/update — which is why OLTP tables carry few, deliberate indexes, and
  why "add an index for my dashboard query" on a production OLTP table is a
  loaded suggestion.
- **Transactions (ACID)**: row locks, MVCC (readers don't block writers) —
  the machinery that makes "transfer money" safe. This is the capability
  NoSQL systems trade away for scale (ch14).

### Warehouse mechanics (why OLAP is fast at its job)

- **Columnar storage** (deep dive ch17): read only referenced columns;
  compression on similar values; vectorized execution.
- **Compute isolation**: Snowflake virtual warehouses / BigQuery slots —
  compute is spun per workload, storage is shared. The BI dashboard queue and
  the data-science scan don't fight for the same cores; they are *separate
  bills*, which is both the feature and the cost trap.
- **Result caching**: identical query text -> cached result, near-zero cost.
  Dashboards hammering the same 6 queries are (nearly) free — until one
  filters on a volatile `CURRENT_TIMESTAMP`, busting the cache.
- **Partition/clustering pruning**: physically organizing by date or cluster
  key so `WHERE dt = '2026-09-20'` scans one partition. In BigQuery, partition
  + cluster keys *are* the index; the cost model (bytes scanned) makes pruning
  a financial, not just performance, concern.
- **Cost = bytes scanned** (BigQuery) or time-sized compute (Snowflake): the
  `SELECT *` habit is a bill, not a style choice. This is why ch18's column
  pruning matters even inside warehouses.

### Distributed SQL (the brief aside)

Spanner, CockroachDB, YugabyteDB: SQL with horizontal scale and strong
consistency via consensus (Paxos/Raft) on replicated shards. The honest
positioning: they exist for *operational* workloads that outgrow single-node
Postgres while needing ACID — not for analytics. If an interviewer probes
"how does Spanner get external consistency," the answer is consensus +
TrueTime (Google's clock hardware); for most designs, "distributed SQL exists,
it's for OLTP-at-scale" is sufficient depth.

### When SQL wins — the decision list

1. **Complex ad-hoc joins and BI semantics**: SQL is the lingua franca of
   analysis; joins are the analytical workload.
2. **ACID transactions**: money, inventory, state machines.
3. **Strong consistency as a requirement** (not a preference).
4. **Ecosystem/tooling**: BI tools speak SQL over JDBC/ODBC (ch16); every
   hiring pool knows SQL; every orchestration tool has a SQL sensor.
5. **Data with relational shape and evolving ad-hoc query patterns** —
   i.e., most analytics.

### The scaling truth (defusing the trap)

- Postgres on a big box with good indexes and read replicas serves tens of
  thousands of transactions/sec — most "SQL doesn't scale" stories are
  actually "we never indexed / we ran analytics on OLTP / we never pooled
  connections."
- When OLTP genuinely outgrows one node: sharding (application-level),
  distributed SQL, or — for read-heavy serving — a KV cache in front (ch14/16).
- OLAP scaling is a solved pricing problem: warehouses scale compute and
  storage independently; the constraint is cost governance, not capability.

### The query-path physics: how a SQL engine actually answers

Worth 60 seconds of any interview — the four things an engine does with a
query, because every "why is this slow?" is one of these four:

1. **Parse & plan**: SQL → logical plan → optimized physical plan (join
   reordering, predicate pushdown, pruning). A bad plan (wrong join order,
   no pushdown) is the #1 silent performance killer.
2. **Pruning**: which partitions/indexes/zone-maps can be skipped entirely
   (ch17/18 statistics). In warehouses this is a *cost* decision — bytes
   not scanned are dollars not spent.
3. **Access path**: B-tree seek (OLTP) vs columnar scan (OLAP) — the ch17
   layout decision, executed.
4. **Concurrency model**: row locks/MVCC (OLTP) vs slot/queue-managed big
   queries (OLAP) — the "40 dashboards" physics of ch16.

### Indexing and partitioning — the OLTP pair, in one table

| | Index (B-tree) | Table partitioning |
|---|---|---|
| Speeds up | point lookups, small ranges on the indexed columns | maintenance + partition pruning on range predicates |
| Costs | every write updates every index | every query must include the partition key to prune |
| Rule | index the foreign keys and the where-clauses you actually run | partition by time (almost always), sub-partition only at real scale |
| Anti-pattern | indexing "everything just in case" | partitioning by a column queries never filter on |

### HTAP and the virtualization escape hatch

Two hybrid families worth naming: **HTAP** (TiDB, SingleStore) — one engine
attempting OLTP + OLAP, eliminating the sync but paying both workloads'
costs in one system; and **query virtualization** (Trino/Presto federation,
Snowflake external tables, warehouse sharing) — SQL over *other people's*
storage without moving data (the ch03/06 "no pipeline" idea, now as a query
layer). Neither replaces the split for serious workloads; both are
legitimate answers to "how do we query this *without* building a pipeline
first?"
## The Options

| Need | Engine class | Examples |
|---|---|---|
| Transactions, point queries | OLTP | Postgres, MySQL |
| Analytics, BI, SQL on big data | OLAP warehouse | Snowflake, BigQuery, Redshift |
| SQL on the lake | lakehouse query engines | Trino/Presto, Spark SQL, Databricks SQL |
| OLTP at planetary scale + ACID | distributed SQL | Spanner, CockroachDB |
| Embedded/light | SQLite/DuckDB | prototypes, local analytics (DuckDB is a phenomenon) |

## Decision Rules

- **Classify the workload (OLTP vs OLAP) before naming an engine.**
- **Analytics on an OLTP database is a bug**: replica + offload, or move it.
- **Default analytics substrate: warehouse or lakehouse** — decided by
  ecosystem gravity and cost model (ch12, ch16).
- **`SELECT *` is a cost decision** in scan-priced warehouses; prune columns.
- **In Postgres-scale OLTP problems, suspect indexing/connection pooling before
  suspecting Postgres.**
- **ACID as a hard requirement eliminates most NoSQL options immediately**
  (ch14) — say so and the decision tree shortens.

## Failure Modes

- **Analytics on prod OLTP**: the ch04 incident, now from the storage side —
  full scans evict the buffer pool, OLTP latency collapses.
- **The un-indexed foreign key**: every join on the un-indexed side is a
  sequential scan per row; "the query is slow" is actually "the plan is a
  disaster."
- **Warehouse cost runaway**: `SELECT *` on a wide partitioned table, per
  dashboard refresh, 40 dashboards; the bill is the incident. Fix: column
  pruning, partition pruning, materialized/pre-aggregated marts (ch16).
- **Cache-busting query patterns**: `WHERE ts > NOW() - interval 1 hour` in a
  30-dashboard rotation — every refresh is a fresh scan; nobody notices until
  the finance review.
- **Cross-graded engines**: transactions on a warehouse (concurrency model
  mismatch) or terabyte scans on Postgres (layout mismatch) — both are
  "right tool, wrong job" failures that present as mysterious slowness.

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