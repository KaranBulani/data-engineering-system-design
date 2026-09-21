# Chapter 14 — The NoSQL Families

> Part V — Storage Engines: SQL vs NoSQL

"NoSQL" is not one technology — it's four distinct storage philosophies that
each sacrificed something (joins, strong consistency, flexible ad-hoc queries)
to maximize something else (scale, uptime, latency, write throughput). The
senior skill is matching the *family* to the access pattern — the product name
comes last.

## The Question

*"My workload has known access patterns, brutal scale/latency requirements, or
shape that relational modeling punishes. Which NoSQL family fits — and what
exactly am I giving up?"*

## The Physics

### The four families, by access pattern

| Family | Examples | Access pattern | Gave up |
|---|---|---|---|
| Key-value | DynamoDB, Redis | get/put by single key; sub-ms | queries, scans, joins |
| Wide-column | Cassandra, Bigtable, HBase | partition-key lookups + range within row; massive write throughput | ad-hoc queries, cross-partition ops |
| Document | MongoDB, Firestore | flexible-schema JSON docs by id or indexed field | joins, normalized integrity |
| Search | Elasticsearch, OpenSearch | full-text and analytic queries over text via inverted index | everything that isn't search-shaped |

The unifying design decision: **you design the data layout for the queries you
know** — the opposite of SQL's model-once-query-anything. NoSQL is a
*query-first* contract: get the access patterns right up front, because
changing them later means rewriting the data, not adding an index.

### Key-value: the simplest possible contract

- One key -> one value (Redis: any structure — strings, lists, hashes, sorted
  sets; DynamoDB: items of attributes). One hop of indirection: key hash ->
  node -> value. That's the entire reason it's sub-millisecond at any scale.
- Redis: in-memory, optional persistence — caching, session store, rate
  limiters, leaderboards (sorted sets). Volatility is the design.
- DynamoDB: single-digit-ms at any scale, provisioned or on-demand
  throughput, items up to 400KB. Scales *only* how you designed it: by
  partition key.

**DynamoDB footguns worth narrating**: partition key choice decides everything
 — a hot key (all traffic to one item) throttles the whole table; GSIs
 (secondary indexes) are their own tables with their own throughput and
 *eventual* consistency; scans are the anti-pattern everyone eventually runs.

### Wide-column: the write-throughput machine

Cassandra's write path explains its superpower — **LSM-trees**:

```
write -> commit log (durability) -> memtable (sorted in memory)
              when memtable fills: flush -> immutable SSTable on disk
reads: memtable -> SSTables (merge)   [read amplification]
background: compaction merges SSTables (the real cost of LSM)
```

Writes are sequential appends — no B-tree page splits, no in-place updates —
which is why Cassandra sustains millions of writes/sec on commodity nodes. The
price: **reads merge multiple SSTables (amplified), and compaction is eternal**
(an IO tax that must be provisioned, monitored, and sized). This is the exact
mirror image of B-trees (fast reads, careful writes) — the deepest
row-vs-columnar-style layout trade in storage.

Data model: partition key (hash -> nodes) + clustering columns (order within
partition). `WHERE partition_key = X AND clustering > Y` is the native, fast
query. Everything else is a scatter-gather across the ring — slow and
cluster-hostile. **You must model around your queries**: denormalize into
query-shaped tables (one table per access pattern); there are no joins to
rescue you.

### Document: schema flexibility as a feature

- BSON/JSON documents, queried by field paths; schema is per-document.
- Wins when: the shape genuinely varies (catalog items, CMS content, event
  payloads), teams iterate fast, and the data is read/written whole.
- The honest cost: flexibility moves integrity from the database to *your
  code* — every reader must tolerate every historical shape (schema evolution
  discipline, ch18, applies to documents too); joins/links across documents
  are application-side.
- MongoDB's later additions (transactions, aggregation pipeline, even
  columnar indexes) are real but Come With Fine Print: distributed
  transactions across shards pay latency; "Mongo but with joins" is often a
  sign the workload wanted Postgres.

### Search: the inverted index engine

- Inverted index: term -> list of documents. Full-text search, relevance
  scoring (BM25), highlighting, faceting — nothing else does this well.
- Also used (historically) as a poor-man's analytics store — resist that:
  it's an operational liability at scale (shard rebalancing, JVM heaps,
  merge storms). For analytics: a warehouse (ch13); for search UX: search.
- The data-engineering role: search clusters are *serving stores fed by
  pipelines* (CDC/events -> transforms -> index), i.e., a downstream consumer
  of your architecture (ch16), not a place business logic lives.

### Consistency: CAP and PACELC in practical terms

- **CAP**: under network partition, choose consistency (refuse writes) or
  availability (serve possibly-stale). Partition tolerance isn't optional —
  networks partition. So CAP is really "what do you do *during* a partition."
- **PACELC** (the useful extension): even without partitions — Else — there's
  a **L**atency vs **C**onsistency trade. Strong consistency costs a
  cross-node round trip on every write; asking for it is choosing latency.
- **Tunable consistency** (Cassandra/DynamoDB style): per-operation levels —
  `ONE` (fast, weak) to `QUORUM`/`ALL` (slower, stronger). The quorum math:
  reads and writes both at quorum (R + W > N) overlap at least one node ->
  read-your-writes for that key.
- The engineering translation: "eventual consistency" in practice = "for
  milliseconds-to-seconds, replicas disagree." Whether that's acceptable is a
  *business* question per feature — and NoSQL pushes that question to you.

## The Options

Choosing among families is choosing *which* access pattern to optimize:

| Workload | Family | Because |
|---|---|---|
| Session/profile/feature lookup by id | KV | one-hop read at any scale |
| Extreme write ingest, time-series-ish, known partition lookups | wide-column | LSM appends, linear scale-out |
| Variable-shape content, fast iteration | document | schema per document |
| Search UX / full-text relevance | search | inverted index, BM25 |
| Ad-hoc joins, BI, relational integrity | (back to) SQL | ch13 |

## Decision Rules

- **No known access patterns -> not NoSQL.** NoSQL is a query-first contract;
  unknown queries are a SQL problem.
- **Pick the family by access pattern; the product by ops/ecosystem.**
- **Design partition keys like your latency depends on it** — because it does:
  hot keys throttle; bad keys scatter-gather.
- **Denormalize deliberately** (wide-column): one table per query, data
  duplicated on purpose, kept in sync by the pipeline — the sync mechanism is
  part of the design (ch15).
- **Quorum R+W > N where read-your-writes matter**; `ONE` where speed wins and
  staleness is acceptable — per query, stated out loud.
- **Search engines are serving stores, not analytics warehouses.**
- **Needing transactions/joins from your NoSQL store is the signal you wanted
  SQL (or a polyglot split, ch15).**

## Failure Modes

- **The un-queryable store**: data modeled for writes, questions need
  cross-partition scans. Symptom: "analytics" jobs doing full cluster
  scatter-gather at 2am. Fix is a data remodel, not tuning.
- **Hot partition**: all traffic hashes to one shard (celebrity user, popular
  tenant). Symptom: throttling on a 30-node cluster that's 95% idle.
- **Compaction debt** (LSM): write-heavy cluster, compaction falls behind,
  read latency climbs, then disks fill. Symptom: "Cassandra is slow" — it's
  the background tax arriving late.
- **Eventual-consistency bug in a strong-consistency feature**: user updates
  profile, next read (different region) shows the old value; support ticket
  says "bug," architecture says "tuesday." The business didn't buy in to the
  consistency choice.
- **Search-as-warehouse creep**: analysts running heavy aggregations on
  Elasticsearch; JVM heaps and shard rebalances make it an outage factory.
- **Document schema sprawl**: three years of "flexible" shapes; every consumer
  carries 50 lines of defensive parsing; no one can delete a field.

## Interview Narration

"I treat NoSQL as four families, not one decision. Key-value for one-hop
lookups — sub-millisecond at any scale, and in DynamoDB the partition key is
the entire architecture: hot keys throttle and GSIs are eventually consistent
by design. Wide-column when write throughput is the headline — Cassandra's
LSM append path is why it sustains millions of writes a second, and compaction
is the permanent tax; the data model is partition key plus clustering columns,
and you denormalize into one table per query because there are no joins.
Document stores when shape genuinely varies and iteration speed matters —
with the honesty that integrity moves into application code. Search engines
for full-text — and as serving stores fed by pipelines, not as analytics
warehouses.

On consistency I'd give the practical version: partition tolerance isn't
optional, so CAP is really a partition-time policy; and even without
partitions there's a latency-consistency trade — strong reads cost a quorum
round trip. Cassandra-style tunable levels let me make that call per query —
quorum reads where users must see their own writes, ONE where stale-by-seconds
is fine. And the meta-rule: NoSQL is a query-first contract — I commit to
access patterns up front and the pipeline keeps denormalized copies in sync.
If the interviewer's workload has unknown, ad-hoc queries, the honest answer
is a SQL engine, not a NoSQL with fingers crossed."

---

| <- Previous | Next -> |
|---|---|
| [Chapter 13 — The SQL Family](ch13-the-sql-family.md) | [Chapter 15 — Polyglot Persistence](ch15-polyglot-persistence.md) |