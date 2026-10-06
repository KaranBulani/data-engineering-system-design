# Chapter 14 — The NoSQL Families

> Part V — Storage Engines: SQL vs NoSQL

"NoSQL" is not one technology. It is an umbrella term for **several different
storage families**. This chapter focuses on four of the most useful families
for large-scale application design. Each family gives up some capabilities that relational
databases provide by default, and gains an advantage somewhere else.

The things NoSQL databases often give up, or make you choose more carefully:

- **Joins** — the database does not combine data from separate places for you.
  If an answer needs pieces stored apart, your application fetches each piece
  and combines them.
- **Strong consistency** — when data is copied to several machines, the copies
  may briefly disagree. A read immediately after a write may return the old
  value.
- **Flexible ad-hoc queries** — you cannot ask any new question at any time.
  You need to know the important questions when you design the data.

The things they buy with those sacrifices:

- **Scale** — keep working as data and traffic grow by adding machines.
- **Uptime** — keep serving even when some machines or networks fail.
- **Latency** — answer reads in milliseconds, even at high volume.
- **Write throughput** — absorb millions of writes per second.

The key skill is matching the *family* to the access pattern. An **access
pattern** is the shape of a read or write that your application performs. For
example: "fetch a user's profile by user id" or "list a user's last 20 posts,
newest first." Most applications have a small, knowable set of access
patterns. Once you know which one matters most, choose the family built for
it. Choose a specific product only after that.

### What "NoSQL" means in practice

**NoSQL does not mean "no SQL."** It usually means the database is not built
around one relational model of tables, rows, and joins. Some NoSQL products
support SQL-like query languages, and many applications use SQL and NoSQL
together. The useful distinction is not the query syntax; it is how the
database stores data and what kinds of queries it makes fast.

Three ideas appear in almost every NoSQL system:

- **Partitioning (or sharding)** splits data across machines. A partition key
  decides where a record lives. Partitioning provides scale, but a poor key
  can create a hot partition or force a query to contact every machine.
- **Replication** keeps extra copies of data on different machines or in
  different regions. Replicas improve availability and can serve reads, but
  they must be kept in sync. If they are allowed to catch up later, the system
  is eventually consistent.
- **Durability and consistency are different.** Durability asks, "Will an
  acknowledged write survive a crash?" Consistency asks, "Will a read see the
  newest write?" A system can durably store a write while another replica
  still returns an older value. Eventual consistency does not mean data is
  eventually saved; it means replicas eventually agree.

Transactions are another important boundary. A relational database commonly
lets one transaction update many rows while preserving rules between them.
NoSQL systems usually make single-record or single-partition operations easy
and fast. Multi-record transactions may be supported, but they often cost more
latency and reduce the scaling advantage. If an operation must update several
independent records as one indivisible unit, check that the chosen product
supports that requirement before choosing it.

Finally, a NoSQL database is not automatically a system of record. A cache,
search index, or materialized view may be rebuilt from a durable source such as
a relational database or an event log. Decide which system owns the truth and
which systems are downstream copies; this prevents accidental data loss and
conflicting updates.

## The Question

*"My workload has known access patterns, brutal scale/latency requirements, or
a shape that relational modeling punishes. Which NoSQL family fits — and what
exactly am I giving up by choosing it?"*

## The Physics

### The four families, by access pattern

| Family | Examples | Access pattern | Gave up |
|---|---|---|---|
| Key-value | DynamoDB (GCP: Firestore), Redis | get/put by a single key; sub-ms | queries, scans, joins |
| Wide-column | Cassandra, Bigtable, HBase | partition-key lookups + range within row; massive write throughput | ad-hoc queries, cross-partition ops |
| Document | MongoDB, Firestore | flexible-schema JSON docs by id or indexed field | joins, normalized integrity |
| Search | Elasticsearch, OpenSearch | full-text and analytic queries over text via inverted index | everything that isn't search-shaped |

How to read this table — each family in one plain sentence:

- **Key-value** is a giant dictionary spread across many machines. You store a
  value under a key and later retrieve that value with the same key. That is
  the basic operation: no searching by value and no combining entries.
- **Wide-column** is a write-optimized store that lets you look up one data
  partition and read an ordered range inside it. It is built to absorb huge
  write volumes.
- **Document** is a collection of self-contained JSON files. Each file can
  have a different structure. You can look up documents by id or by an indexed
  field.
- **Search** is like the index at the back of a textbook. It maps each word to
  the documents containing that word, making text lookup fast.

**A note on graph databases.** Graph databases are another important NoSQL
family, although this chapter focuses on the four families in the table above.
They store **nodes** (things such as people or accounts) and **edges** (the
relationships between them). They are a good fit for questions such as
"which friends-of-friends have access to this document?" or "what payment
accounts are connected through these entities?" A graph database makes
relationship traversal fast, but it is not automatically the best choice for
simple key lookups, large sequential writes, or full-text search. Choose it
when the relationships themselves are the main data you need to explore.

Time-series databases and vector databases are other specialized categories
you may encounter. Time-series stores optimize for timestamped measurements,
such as metrics and sensor readings. Vector stores optimize for finding items
with similar numeric representations, such as semantically similar documents
or images. They are useful when that specialized access pattern dominates;
they do not replace a general-purpose database for every part of an
application.

The shared design decision behind all four families is this: **design the data
layout for the queries you already know you will run**. This is the **opposite
of the SQL approach**. In SQL, you model the data once in normalized tables,
then ask new questions later. Joins and indexes help the database answer those
questions.

With NoSQL, the contract is *query-first*. You choose your access patterns up
front, and arrange the data to answer those patterns quickly. If the questions
change later, a new query often requires a new table and a backfill or a new
copy of the data. You usually cannot just add an index and move on.

A concrete comparison makes this vivid. Suppose you already store orders and
someone asks, "average order value by city for the last 30 days?"

- In SQL: one `SELECT` with a `GROUP BY` and a join. The database works out how
  to execute it. The query may take minutes to write.
- In Cassandra or DynamoDB: no joins, and a query grouped by city is only
  fast if a table *keyed by city* already exists. If it doesn't, you create a
  new table, backfill it from the old one, and keep it in sync. That can take
  days of work instead of minutes.

That is the price of the speed: the layout is tuned for known questions, so
unknown questions are expensive by design.

### Key-value: the simplest possible contract

The entire contract is two operations:

- `PUT(key, value)` — store a value under a key.
- `GET(key)` — return the value stored under that key.

Think of a coat-check counter: you hand over a coat, receive a numbered
ticket, and later use that ticket to get the same coat back. You cannot ask
"show me all brown coats" — you can only ask "give me the coat for ticket 37."

One key maps to one value. In Redis the value can be a rich in-memory
structure — a string, a list, a hash, a sorted set. In DynamoDB the value is
an *item*: a set of attributes, up to 400 KB. (If a value doesn't fit — say
a large image — the pattern is to store the image in S3 (GCP: Cloud
Storage) and put the pointer in the item.)

**Why it stays sub-millisecond at any scale.** When you `GET("session:abc")`,
the database hashes the key. That hash identifies the machine that owns it,
and that machine returns the value. The path is simple: *key → hash → node →
value*. There is no query planner, index scan, or join. The key tells the
database exactly where to look. Adding machines spreads keys across more
machines, so each lookup remains fast.

**Redis: fast because it lives in memory.** Redis keeps data in RAM rather
than on disk. RAM is much faster than disk, which explains most of Redis's
speed. Redis can also persist data to disk using snapshots or a write-ahead
log. Even so, it treats data as *volatile*: data can be lost. That is usually
acceptable because Redis holds data that can be rebuilt. A cache can be
refilled from the database, and a session can be recreated when a user logs in
again.

Typical Redis uses, with the feature that makes each one work:

- **Cache** — store the result of an expensive database query under a key with
  a time-to-live, so the next thousand requests hit Redis instead of the
  database.
- **Session store** — `GET("session:abc")` returns everything you know about
  that logged-in browser session. One lookup per request, forever.
- **Rate limiter** — a counter per user per minute (`INCR` the key, set an
  expiry); if the counter exceeds the limit, reject the request.
- **Leaderboard** — a sorted set keeps members ordered by a score, so
  "top 10 players" is one instant command rather than a sort.

**DynamoDB (GCP: Firestore): predictable single-digit milliseconds at any
scale.** You can use *provisioned* throughput, where you pay for a fixed
number of reads and writes per second, or *on-demand* throughput, where you
pay per request and avoid capacity planning.

There is an important catch: **it scales only along the lines you define when
you design the table — mainly the partition key.** The partition key decides
which physical partition, and therefore which machines, store each item. A
fast query must specify a partition key. Other queries leave the fast path
(see the footguns below).

**DynamoDB footguns worth narrating:**

- **Hot key.** All items sharing one partition key land on the same partition,
  and each partition has limited throughput. If every request touches the
  *same* key — such as one event's concert tickets or a celebrity user's
  profile — that partition throttles while the rest of the table sits idle.
  Fix this during design: spread the traffic, add a random suffix to create
  several keys, put a cache in front, or avoid one hot item entirely.
- **GSIs are separate tables in disguise.** A Global Secondary Index lets you
  query by a different attribute, such as email instead of user id. Physically,
  it is a separate copy of the data with its own throughput budget and storage
  cost. Every write is copied to it. GSIs are also *eventually consistent*: if
  you write an item and query the GSI immediately, the new value may not be
  there yet.
- **Scan is the anti-pattern everyone eventually runs.** A scan reads *every
  item in the table* because the query cannot target a partition key. It is
  slow, costs a full-table read each time, and usually means the table was
  designed for the wrong access pattern. The fix is to remodel the data, not
  tune the scan.

### Wide-column: the write-throughput machine

Cassandra's strength is absorbing huge write volumes. The reason is its
storage engine: the **LSM-tree** (Log-Structured Merge tree). First compare it
with a traditional relational database. On a write, a relational database
finds the row in a B-tree, updates the page *in place*, and may split pages
when they become full. Reads are fast because the data stays sorted, but
writes require more random work.

An LSM-tree never updates in place. Every write is an append:

```
write -> commit log (durability) -> memtable (sorted in memory)
              when memtable fills: flush -> immutable SSTable on disk
reads: memtable -> SSTables (merge)   [read amplification]
background: compaction merges SSTables (the real cost of LSM)
```

Step by step:

1. **Commit log.** The write is first appended to a log on disk. A sequential
   append simply adds data to the end of a file, which is one of the fastest
   disk operations. If the machine crashes, the database can replay the log.
2. **Memtable.** The same write goes into an in-memory structure kept in
   sorted order. The data is readable from this point onward.
3. **Flush to an SSTable.** When the memtable fills up, it is written to disk
   as an *SSTable* (sorted string table): an immutable, sorted file. It is
   never edited again. New data always goes into new files.
4. **Reads merge.** To read a key, the engine checks the memtable and then
   the SSTables, using the newest value when versions differ. A single logical
   read may touch several files. This extra work is called **read
   amplification**, and it grows as SSTables pile up.
5. **Compaction.** In the background, the engine continuously merges small
   SSTables into fewer, larger ones. It discards overwritten and deleted
   values along the way. Compaction is the price of the append-only design:
   an ongoing I/O cost that competes with your workload for disk resources.
   You must provision and monitor capacity for it.

**What an SSTable actually is.** SSTable stands for *sorted string table*. The
name comes from Google's Bigtable (the GCP system in the family table above).
An SSTable is a file of key-value entries sorted by key, written once and
never edited again. Because the memtable is already sorted, flushing it to
disk is one sequential write. Each SSTable also has a small index showing
where each block of keys starts. A lookup can jump to the relevant block and
scan only that block. It is like a book index that points to page ranges.

Immutability sounds like a limitation, but it is the key idea. Since files are
never edited, a write never waits for a random disk update; it only appends.
The cost appears during reads of SST tables, in two ways:

- **One key can live in several SSTables at once.** Updating a key doesn't
  touch the old value; it appends a newer version that lands in a newer
  file. Example: `PUT(balance = 100)` on Monday is flushed into SSTable 1;
  `PUT(balance = 250)` on Tuesday is flushed into SSTable 4. A read of that
  key finds it in both files and returns 250 — the newest file wins. The
  old 100 is never overwritten; it just loses the comparison, and
  compaction drops it later.
- **Deletes are writes too.** Deleting appends a **tombstone** — a small
  marker meaning "this key is gone" — which shadows older values exactly
  the way a newer write does. The data isn't truly removed until compaction
  clears the tombstone along with everything it shadows.

This is **read amplification**: one logical read may check the memtable and
several SSTables before it finds the newest version. Even skipping a file
requires checking its index or bloom filter first. A **bloom filter** is a
small in-memory structure that can quickly answer, "This key is definitely not
in this file." It lets reads skip most files. The more files accumulate, the
more work each read requires, which is why compaction is necessary.

**Compaction is a background merge-sort.** Every SSTable is sorted, so several
files can be merged in one linear pass. The engine repeatedly takes the
smallest key from the input files, writes the newest version, and removes
tombstones with the stale values they shadow. This keeps reads fast by
reducing the number of files and reclaims space. It runs continuously as new
data arrives. That is why the diagram calls compaction "the real cost of LSM":
the write path is cheap because the system pays the cost later, in the
background, on every node.

Because writes are sequential appends — no B-tree page splits, no in-place
updates, no random seeks — Cassandra sustains millions of writes per second
on ordinary commodity machines.

Notice this is the exact mirror image of the B-tree. B-tree: fast reads,
careful, slower writes. LSM: fast writes, reads that do more merging, plus a
permanent background compaction tax. Neither is "better" — they are opposite
answers to the same layout trade, and you pick based on whether your workload
is read-heavy or write-heavy.

**The data model: partition key plus clustering columns.** A Cassandra table
is defined by two things:

- The **partition key** is hashed to decide which nodes own the row group —
  all rows sharing a partition key live together, on the same nodes.
- **Clustering columns** sort the rows *within* a partition, on disk.

Example — a user timeline:

| partition key: `user_id` | clustering: `posted_at` | `content` |
|---|---|---|
| 42 | 2026-09-01T10:00 | "morning run done" |
| 42 | 2026-09-02T09:00 | "back at it" |
| 43 | 2026-09-01T11:30 | "hello world" |

User 42's posts live together, physically ordered by time. The native, fast
query is exactly the shape of the layout:

```sql
SELECT * FROM user_timeline
 WHERE user_id = 42                 -- one partition: one node group
   AND posted_at > '2026-09-01';    -- a range within it: already sorted
```

That query reads one contiguous slice of one partition — fast and cheap.

Anything else is a **scatter-gather**. The coordinating node sends the request
to *every* node in the ring. Each node scans its local data, and the
coordinator waits for all nodes before merging the results. "All posts from
today, across all users" has no partition key, so it becomes a full-cluster
scatter-gather. It is slow and puts load on every machine at once.

**You must model around your queries.** Since joins are not available to
rescue you, the standard practice is *denormalization*: create one table per
access pattern and deliberately duplicate the data. If your application needs
"posts by user" and "post by post id," store each post twice: once in a
`user_timeline` table and once in a `posts_by_id` table. Your application must
write both copies for every post. **In SQL, duplicated data is usually treated
as a risk. In wide-column stores, it is part of the design.** Your application
or pipeline must keep the copies in sync, and that sync mechanism belongs in
the architecture (chapter 15).

### Document: schema flexibility as a feature

A document store holds self-contained JSON (or BSON — JSON in binary form)
documents and queries them by field paths. Its defining property is that **the
schema belongs to each document**. There is no central `ALTER TABLE`; each
document contains the fields it needs.

Example — two products in one catalog collection:

```json
{ "_id": "tv-100", "name": "42in TV", "price": 299,
  "specs": { "screen_size": 42, "hdmi_ports": 3 } }

{ "_id": "tee-7", "name": "T-shirt", "price": 19,
  "sizes": ["S", "M", "L"], "color": "navy" }
```

A TV has HDMI ports; a t-shirt has sizes. In SQL you would need either many
nullable columns or a separate attributes table. Here, both documents simply
coexist.

**When documents win:** the shape genuinely varies, as with catalog items,
CMS content, and event payloads. They also work well when teams change the
shape often and read or write each document *as a whole* instead of assembling
an answer from fragments.

**The honest cost: integrity moves from the database into your code.** The
database may happily store a product without a price because nothing says that
`price` is required. Every guarantee once provided by a database constraint
becomes a check that you must write in code. Without a central schema, *old shapes
never go away*: years later, readers may need to handle every schema version
ever written. That is why the schema-evolution discipline in chapter 18 also
applies to documents.

When one document needs data from another, there are no joins. The application
fetches each document separately and combines them; this is an
"application-side join." If the documents must stay consistent, such as when
an order references a product that just changed, keeping them consistent is
also your code's job.

**A note on MongoDB's later additions.** MongoDB has added real features —
multi-document transactions, an aggregation pipeline, even columnar indexes.
But these come with fine print: distributed transactions across shards pay
extra coordination latency, and "$lookup" (Mongo's join) on a hot path is
usually a warning sign. A useful rule of thumb: if you find yourself saying
"I want Mongo, but with joins and transactions," the workload you are
describing is relational — it probably wanted Postgres from the start.

### Search: the inverted index engine

A search engine's core structure is the **inverted index**. It stores, for
each term, the list of documents that contain it. It is called *inverted*
because it reverses the usual direction: instead of "document → its words," it
stores "word → its documents."

For two documents:

- doc 1: "the quick red fox"
- doc 2: "the slow red turtle"

the index holds:

```
"quick"  -> [doc 1]
"red"    -> [doc 1, doc 2]
"fox"    -> [doc 1]
"turtle" -> [doc 2]
```

Searching for "red fox" intersects the relevant lists and ranks the results;
it does not need to scan the document text. That is why full-text search stays
fast over millions of documents. The SQL equivalent,
`WHERE description LIKE '%red fox%'`, may examine every row and does not rank
results by relevance.

Real engines add several layers on top of that index:

- **Relevance scoring (BM25)** — a ranking formula that decides which matches
  come first. In plain terms: documents that mention your term more often
  score higher, rare terms count much more than common ones (a match on
  ".kafka" beats a match on "the"), and shorter documents that mention the
  term score higher than long ones that mention it once in passing.
- **Highlighting** — show the matched words in context in the result snippet.
- **Faceting** — alongside the results, return counts by category: "brand:
  Acme (142), Zenith (87)…" — the numbers behind the filter sidebar on a
  shopping site.

This is what the search family is designed to do.

**Resist using it as an analytics store.** Elasticsearch was historically
pressed into service as a "poor man's analytics engine" because its
aggregations are convenient. At scale this becomes an operational liability:
big aggregations pressure the JVM heap, nodes joining and leaving trigger
shard rebalancing, and background merges compete with your queries — a
pattern of recurring outages. The right split: analytics questions go to a
warehouse (chapter 13); search-UX questions go to a search engine.

**The data-engineering role:** a search cluster is a *serving store fed by
pipelines*. The typical architecture is: source of truth (say Postgres) →
CDC or events → transforms → index in Elasticsearch. It is a downstream
consumer of your architecture (chapter 16) — not the place where your
business logic lives.

### Consistency: CAP and PACELC in practical terms

When data is replicated across machines, you inherit a fundamental tension.
Two acronyms name it precisely.

**CAP** describes three properties of a distributed system: Consistency (every
read sees the latest write), Availability (every request gets an answer), and
Partition tolerance (the system continues despite a network break between
nodes). The theorem is: **when a network partition happens, you must choose
between C and A.**

A concrete picture: imagine two data centers with the cable between them cut.
A user writes "address = new value" to data center 1. At the same time,
another user reads that record from data center 2. Data center 2 cannot know
about the write. It must either *refuse to answer* (choosing consistency) or
*return the old address* (choosing availability). It cannot do both.

Partition tolerance is not optional because networks *do* partition. Switches
fail, packet loss can look like a partition, and a long garbage-collection
pause can make a node miss heartbeats. So CAP is not simply "pick two of
three." In practice, it asks: **what do you do *during* a partition** — serve
stale data or stop serving?

**PACELC** extends CAP into everyday system design. It says that even when
there is *no* partition — **E**lse — there is still a trade-off between
**L**atency and **C**onsistency. Guaranteeing "you will always see the latest
write" requires confirmation from more than one node before acknowledging the
write. That adds a cross-node round trip to *every* write, whether or not a
partition exists. Strong consistency means paying that latency on every
operation.

**Tunable consistency** (the Cassandra/DynamoDB style) lets you make that
trade per operation instead of choosing one setting for the whole database.
With a replication factor of N=3, each key lives on three nodes:

- `ONE` — the first replica acknowledges. Fastest, weakest: two replicas
  haven't heard of the write yet.
- `QUORUM` — a majority (2 of 3) acknowledges. Slower, stronger.
- `ALL` — all three acknowledge. Strongest, slowest, and tolerates zero node
  failures.

The quorum rule is R + W > N. When reads and writes both use quorum, the
replicas that answered the read and the replicas that confirmed the write
**must overlap in at least one node**. The read is therefore guaranteed to see
at least the latest write. Example: with N=3, W=2, and R=2, a write may be
confirmed by replicas {1, 2}, and a later read may use {2, 3}. Replica 2 saw
the write, so the read returns it. With R=1 and W=1, there is no such
guarantee.

**The engineering translation.** "Eventual consistency" sounds abstract. In
practice, it means that replicas may disagree for milliseconds or seconds,
then converge. Whether that is acceptable is a **business** question for
each feature:

- A "like" counter showing a slightly stale number for two seconds: fine.
- A user's profile showing their *old* name after they just changed it:
  annoying, probably not fine.
- A bank balance: not fine.

Relational databases made this decision for you (they chose consistency).
NoSQL hands the decision to you — which means you must make it consciously,
per feature, and say it out loud.

## The Options

Choosing a family means choosing *which* access pattern to optimize:

| Workload | Family | Because |
|---|---|---|
| Session/profile/feature lookup by id | KV | one-hop read at any scale |
| Extreme write ingest, time-series-ish, known partition lookups | wide-column | LSM appends, linear scale-out |
| Variable-shape content, fast iteration | document | schema per document |
| Search UX / full-text relevance | search | inverted index, BM25 |
| Ad-hoc joins, BI, relational integrity | (back to) SQL | ch13 |

The "Because" column, in plain words:

- **KV** — when every access is "give me the record for this exact id," the
  one-hop key → hash → node → value path is extremely fast and scales across
  machines.
- **Wide-column** — when *writes per second* is the main requirement, the LSM
  append path and linear scale-out make this family a good fit. **Every query
still needs to name a partition key.**
- **Document** — when the data's shape varies record by record and changes
  week to week, per-document schemas absorb that variation without migrations.
- **Search** — when the query is *text* ("find documents about…, ranked by
  relevance"), an inverted index can answer at interactive speed.
- **SQL** — and when the workload is unknown ad-hoc questions, joins, or
  integrity-critical transactions, that is exactly what relational engines
  are for. "Which NoSQL?" is a valid answer only when the access patterns
  are known; if they aren't, the honest answer is SQL.

## From NoSQL to the Warehouse: business use cases and the transfer to SQL

Each family above earns its place with a specific business job. But the same
data usually has a second audience. Finance wants monthly revenue by region;
marketing wants funnel conversion; a data scientist wants one clean training
table. Those are join-and-aggregate questions — warehouse questions
(chapter 13) — and a NoSQL store answers them badly *on purpose*: its layout
is optimized for its known access patterns, not for new ones. So the data
must be moved. This section covers both halves of that story: the business
use case that justifies each family, and the mechanics of moving its data
into a SQL warehouse when SQL-style transformations are needed. It covers
all seven families: the four detailed above plus graph, time-series, and
vector stores.

The mismatch is structural, not incidental. The warehouse wants a fixed
schema, joins, set-based `GROUP BY`, and the freedom to ask new questions
next month. The NoSQL store offers layouts designed around known queries, no
joins, and — in the document family — a different shape per record.
**Moving NoSQL data into SQL is therefore never a plain copy; it is a
re-shape.** The pipeline must flatten, cast, and lay the data out again.

### Each family's business job — and the SQL questions that arrive later

| Family | Business use case that justifies the store | The SQL question that arrives later |
|---|---|---|
| Key-value | Login sessions for a large web site: one `GET` per request, sub-millisecond | "Weekly active users by region" — aggregate over all sessions |
| Wide-column | Clickstream ingest: millions of appends per second into per-user timelines | "View-to-purchase funnel by campaign" — joins events across partitions |
| Document | Product catalog where each product type has its own shape | "Margin by category" — join the catalog against orders and costs |
| Search | Site search that returns ranked, relevant results | "Which queries return zero results?" — analytics over query logs |
| Graph | Fraud-ring detection: accounts linked through shared devices and addresses | "Total exposure per ring" — aggregate over the traversal output |
| Time-series | IoT sensors and system metrics at millions of readings per second | "Monthly averages per device type over 3 years" — long-horizon aggregation |
| Vector | Semantic search / RAG: find documents similar in meaning to a question | "Which documents were retrieved, how often, how fresh" — metadata reporting |

Read the middle column as the reason each store exists, and the right column
as the moment another team walks over with a warehouse-shaped question.
Neither column is wrong; they are different jobs. The store serves its access
pattern all day at millisecond latency; the warehouse — built for scans,
joins, and aggregations (ch13, ch17) — answers the question the store was
never shaped to ask. The transfer pipeline between the two is the subject of
the rest of this section.

### The universal transfer pattern

Whatever the family, the path has the same shape:

```
NoSQL store
  -> export (batch scan | snapshot | change stream)
  -> object storage: raw copy, exactly as exported
  -> flatten + cast to a fixed schema (Spark or SQL ELT)
  -> warehouse tables (ch13) -> BI, joins, ad-hoc SQL
```

Two rules separate a bridge from a liability:

- **Land the raw copy in object storage first** (S3 (GCP: Cloud Storage)),
  exactly as exported, before any transformation. This is the raw zone of the
  two-plane landing pattern (chapter 6, chapter 12). If the flattening logic
  has a bug, you fix it and re-run against the raw copy instead of hammering
  the source store with another full export.
- **Never point a production NoSQL cluster directly at BI.** The store was
  sized for point lookups, not for someone's year-long `GROUP BY`. Heavy
  scans throttle or crash it — the Failure Modes section below calls this the
  un-queryable store. Treat the export job as an ordinary pipeline citizen:
  scheduled (chapter 7), monitored (chapter 22), and pointed at replicas or
  dedicated export endpoints where the product offers them.

Three export mechanisms cover every family:

- **Batch export** — a scheduled job scans or snapshots the whole store and
  writes the result to object storage. Simple and complete, but it re-reads
  everything on every run; fine for small data or a daily cadence
  (chapter 7).
- **Change data capture** — read the store's change feed (DynamoDB Streams,
  MongoDB change streams, Debezium connectors) so every insert, update, and
  delete flows into the pipeline as an event (chapter 6, chapter 8).
  Incremental by design; the standard choice once full exports get slow. CDC
  also carries deletes, which a scan of current state cannot show.
- **Fan out from the event log** — the application writes each change once to
  an event log (chapter 11), and separate consumers build the NoSQL store and
  the warehouse feed from the same events. The strongest pattern when the log
  already exists, because nothing has to learn how to export.

### Per family: the transfer mechanics

**Key-value (DynamoDB (GCP: Firestore), Redis).** Values are small and
self-contained, so the mechanics are the easiest of the seven. For DynamoDB,
the managed path is *export to S3*: a point-in-time copy of the table written
to S3 (GCP: Cloud Storage) without consuming the table's read capacity, with
DynamoDB Streams as the incremental feed. Database Migration Service (GCP:
Datastream) is the generic managed mover for databases without a dedicated
export. For Redis, the options are a snapshot (RDB file) or a keyspace scan —
but the more important rule comes from chapter 15: **export the system of
record, not the cache.** If a session exists only in Redis, first ask whether
analytics needs it at all.

**Wide-column (Cassandra, Bigtable, HBase).** The store is denormalized per
access pattern, so mapping one source table onto warehouse tables is a design
decision, not a given. Spark is the standard export engine: the
Spark-Cassandra connector reads partitions in parallel, turning the cluster's
layout into an advantage instead of a scatter-gather (chapter 19). Bigtable
exports through Dataflow into Cloud Storage and BigQuery; Kafka Connect
source connectors cover the rest. Read from replicas or off-peak windows,
and take incremental sync from a change feed or a dual-write, because a scan
only shows current state.

**Document (MongoDB, Firestore).** Two jobs: get the documents out, then
flatten them. The export is the easy half — change streams through Debezium
for incremental sync, or a snapshot dump to object storage for batch. The
flattening is the real work: **nested objects become columns or a JSON column**;
**arrays of sub-documents become child tables with a parent foreign key**; and
**documents written under older shapes arrive as variants the staging layer
must still parse**. This family is where the schema problem below bites
hardest.

**Search (Elasticsearch, OpenSearch).** Ask first whether the index is the
truth or a copy. If it is a derived serving copy — the usual case
(chapter 16) — export the source of truth instead; the index can always be
rebuilt from the log (chapter 11). If the index genuinely is the only store
(log analytics), export with snapshots to object storage, or page through
with `scroll` / `search_after` for one-off extracts. Export `_source`
documents, not aggregation results: analyzed text fields may not retain the
original string, and the warehouse should re-derive its own aggregates.

**Graph (Neo4j and friends).** Map the graph to relational shapes: nodes
become entity tables (`account`, `device`), and edges become junction tables
with two foreign keys, such as `account_device(account_id, device_id)`. The
export runs Cypher queries — often with the APOC library's CSV/JSON
exporters — through a driver in a batch job. What SQL cannot reproduce is
deep traversal, so do not try to ship traversal *results* as rows. Export
the nodes and edges and let the warehouse answer bounded questions, or
precompute graph metrics in the graph store (ring id, community id, degree)
and ship them as ordinary columns.

**Time-series (InfluxDB; Bigtable and Cassandra also serve this family).**
The mapping is the most natural of the seven: readings are already
timestamped rows, and warehouses are extremely good at date-partitioned wide
tables. Export through the query API or line-protocol dumps on a schedule,
then load. Two traps: retention policies may delete raw readings before the
export runs (export before expiry, or consciously settle for downsampled
rollups), and very high cardinality — millions of distinct device ids — is
exactly as painful to group in the warehouse as in the store. Decide the
aggregation levels before loading.

**Vector (Pinecone, Milvus, pgvector).** The key fact: **a vector is derived
data** — the output of running an embedding model over source text or
images. So the best transfer often never touches the vector store: keep the
source documents in the warehouse, run the same pinned model version inside
the pipeline, and recompute embeddings there. When copying is the right call
— the model is too expensive to rerun, or the store is the system of record
— export id + vector + metadata to object storage and load it into the
warehouse's vector columns. The trap is model-version drift: vectors from
two model versions live in different spaces and must never be mixed in one
similarity search.

### The schema problem: schema-on-read meets schema-on-write

In the NoSQL store, each record carries its own shape. In a warehouse table,
the shape is fixed before the first row is written. The transfer pipeline is
where those two models collide, and the collision needs a deliberate design:

- Someone must define the target schema: which fields become columns, which
  nested structures become child tables, and which rare fields are kept in a
  JSON column for later.
- Type mapping is a real job. Numbers stored as strings, dates in three
  different formats, and embedded units do not line up with SQL types by
  accident; cast explicitly in the staging layer.
- Late-arriving variants — a document shape from 2023 that still shows up —
  must not break the load. Land raw, parse defensively, and track schema
  versions the way chapter 18 requires for any format.

The standard resolution is the staging pattern: the raw zone holds the export
exactly as written; a staging schema flattens and casts it with explicit,
tested rules; curated warehouse tables hold the clean result (chapter 12).
When someone asks why a number moved 5% month over month, the answer should
live in versioned staging SQL, not in a parser nobody has read since 2023.

### The transfer decision table

| Family | Batch export | Incremental sync | Watch out for |
|---|---|---|---|
| Key-value | DynamoDB export to S3 (GCP: Cloud Storage); Redis snapshot | DynamoDB Streams | exporting the cache instead of the system of record |
| Wide-column | Spark connector, partition-parallel reads | change feed (CDC) or dual-write | scatter-gathers against the production cluster |
| Document | snapshot / `mongoexport` dump | change streams via Debezium | every historical schema variant must still flatten |
| Search | snapshots to object storage | CDC on the source of truth | exporting the derived index instead of the truth |
| Graph | Cypher / APOC export to CSV or JSON | edge events on the log | shipping deep traversal results as rows |
| Time-series | query API / line-protocol dumps | tail by timestamp | retention deleting readings before export |
| Vector | id + vector + metadata dump | recompute from source text | mixing vectors from different model versions |

**The takeaway: the NoSQL store stays the fast path for its access pattern,
and the warehouse becomes the fast path for every other question. The export
pipeline is the bridge, and it deserves the same design care as any other
pipeline — land raw, flatten with tested rules, schedule it, monitor it.**
Chapter 15 takes the next step: when the same data lives in several stores at
once, who owns which copy, and how do the copies stay in sync?

## Decision Rules

- **No known access patterns → not NoSQL.** NoSQL is a query-first contract:
  the layout is built for questions you define up front. If the questions are
  unknown or ad hoc, SQL is usually the better fit because a relational engine
  models the data once and supports new questions later.
- **Pick the family by access pattern; pick the product by operations and
ecosystem.** Family choice depends on the shape of reads and writes. Among
  products in the same family, compare managed versus self-hosted cost, team
  familiarity, tooling, and support.
- **Design partition keys like your latency depends on it — because it does.**
  The partition key decides where every item lives and how traffic spreads.
  A hot key funnels all traffic onto one partition and throttles; a key that
  forces queries without a partition key turns every request into a
  scatter-gather.
- **Denormalize deliberately** (wide-column): one table per access pattern,
  with data duplicated on purpose. The duplication is not an accident to
  clean up — it is the design. The mechanism that keeps the copies in sync
  (application dual-writes, CDC, a pipeline) is part of the architecture and
  must be designed as such (chapter 15).
- **Quorum R + W > N where read-your-writes matter; `ONE` where speed wins
and staleness is acceptable.** Make the choice per query, per feature — and
  state it out loud in design docs. The failure to avoid is an implicit,
  undocumented consistency assumption.
- **Search engines are serving stores, not analytics warehouses.** Heavy
  aggregations on Elasticsearch are an outage factory at scale (heap
  pressure, shard rebalancing, merge storms). Analytics belongs in a
  warehouse (ch13).
- **Needing transactions and joins from your NoSQL store is the signal that
  you wanted SQL** — or a deliberate polyglot split where the relational
  part lives in SQL and only the NoSQL-shaped part lives in NoSQL
  (chapter 15).

## Failure Modes

- **The un-queryable store.** The data was modeled for writes, but business
  questions require cross-partition scans that the layout does not support.
  A symptom is an analytics job running full-cluster scatter-gathers at 2 a.m.
  and slowing production traffic. Fix it by remodeling the data with new
  tables for the real access patterns and feeding them through a pipeline.
- **Hot partition.** All traffic hashes to one shard — the celebrity user,
  the one popular product, the one big tenant. Symptom: throttling and
  timeouts on a 30-node cluster that is 95% idle, because the bottleneck is
  one partition, not the cluster. Prevention is at design time: spread the
  key, cache the hot item, or isolate the big tenant.
- **Compaction debt** (LSM stores). On a write-heavy cluster, incoming writes
  can outpace the background compactor. SSTables then pile up, reads merge
  more files, latency rises, and disk fills with unmerged files. A symptom is
  "Cassandra got slow" even though no query changed. Add spare I/O capacity
  for compaction and monitor its lag; restarting the node does not fix the
  cause.
- **Eventual-consistency bug in a strong-consistency feature.** A user
  updates their profile; their next read, served from a different region,
  shows the old value. The ticket says "bug," but the architecture was
  working exactly as designed — the business never agreed to eventual
  consistency for that feature. Prevention: decide consistency per feature,
  explicitly (quorum for the ones like this), before launch.
- **Search-as-warehouse creep.** Analysts discover Elasticsearch
  aggregations and start running heavy reports on the serving cluster. JVM
  heaps fill, shards rebalance, merges stack up — and now the product search
  that pays the bills has an outage factory bolted onto it. Fix: route
  analytics to a warehouse and keep the search cluster for search.
- **Document schema sprawl.** After years of "flexible" shapes, every
  consumer may need defensive parsing such as
  (`if "price" in doc … elif "pricing" in doc …`). Fields become difficult to
  remove because an old reader may still use them, and no one knows all the
  shapes in the collection. Flexibility is a loan: future readers pay the
  interest. Use schema versions and an agreed evolution policy, even in a
  schemaless store.

## Interview Narration

"I treat NoSQL as four families, not one decision. Key-value for one-hop
lookups — sub-millisecond at any scale, and in DynamoDB (GCP: Firestore)
the partition key is the entire architecture: hot keys throttle and GSIs
are eventually consistent by design. Wide-column when write throughput is the headline — Cassandra's
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

*(Every claim in this narration is justified by the sections above — if any
sentence feels like shorthand you couldn't defend, re-read the matching
section.)*

---

| <- Previous | Next -> |
|---|---|
| [Chapter 13 — The SQL Family](ch13-the-sql-family.md) | [Chapter 15 — Polyglot Persistence](ch15-polyglot-persistence.md) |
