# Chapter 17 — Row vs Columnar: The Physics

> Part VI — Serving, Formats & Spark

Why does a columnar scan beat a row scan by 10–100x on analytics workloads?
Because the two layouts differ in three physical resources at once — **bytes
read from disk, how well the data compresses, and how the CPU executes the
query** — and all three differences point the same direction for analytics.
The 10–100x is not one big trick; it is three medium tricks multiplying.
That is the kind of "why" that separates tool-users from engineers in
interviews: anyone can say "Parquet is fast"; this chapter is about knowing
*why* it is fast.

## The Question

*"Should this data be stored row-wise or column-wise — and why is that
choice almost entirely determined by the query pattern?"*

## The Physics

### The two layouts

```
ROW LAYOUT (record-wise)          COLUMN LAYOUT (column-wise)
                                  (each column is its own file/segment)
id | user | amt | status          id:      [1, 2, 3, 4, ...]
---+------+-----+--------        user:    [u42, u7, u42, u9, ...]
 1 | u42  | 29  | paid           amt:     [29, 12, 63, 8, ...]
 2 | u7   | 12  | paid           status:  [paid, paid, refunded, paid, ...]
 3 | u42  | 63  | refunded
 4 | u9   |  8  | paid
```

Same logical table — identical rows if you read them back — but stored
physically in two different orders:

- **Row layout** stores one complete record, then the next complete record:
  `1,u42,29,paid`, then `2,u7,12,paid`, and so on. All of a record's values
  are *adjacent* on disk.
- **Column layout** stores all the values of one column together, then all
  the values of the next column. One record's values are *scattered* across
  N places; one column's values are contiguous.

That adjacency difference is the entire story. Every consequence below —
I/O, compression, CPU behavior — follows from *which values sit next to
each other on disk*. And because the two layouts make different things
adjacent, the same query can be cheap in one and expensive in the other.

### Why analytics loves columns

Take a concrete analytics query on an **orders table with 100 columns**
(id, user, amount, status … plus 95 more: timestamps, addresses, marketing
attribution, and so on):

```sql
SELECT status, SUM(amt) FROM orders WHERE dt = '2026-09-20';
```

The query references only a handful of columns — `dt`, `status`, `amt`,
maybe `id` — call it 5 of the 100. Watch what each layout must do:

- **Row layout:** rows are stored whole, so answering the query means
  reading *every byte of every row* — all 100 columns — to use 5. The 95
  unused columns' bytes are read from disk, moved through memory, and
  immediately discarded. The query does 20x more I/O than its logic needs.
- **Column layout:** the engine reads only the segments for the 5 columns
  it needs — the other 95 column segments are never touched at all.

| Effect | Row layout | Column layout |
|---|---|---|
| Bytes read (5/100 cols) | 100% of the row bytes | ~5% + small overhead |
| Compression | weak (adjacent bytes are unrelated types) | strong (like-typed, similar values adjacent) |
| CPU execution | row-at-a-time, branchy | vectorized: same op on column batches, SIMD-friendly |

Each effect in plain words:

- **Bytes read** is the first multiplier, shown above: ~5% of the I/O
  before anything else happens.
- **Compression** is the second. Compression works by finding repetition,
  and repetition is a property of *what sits next to what*. In a column
  layout, adjacent values are the same type and from the same domain — a
  `status` column is `paid, paid, refunded, paid, …` — so the values repeat
  heavily and compress enormously (mechanisms below). In a row layout,
  adjacent bytes are a timestamp, then a string, then a number, then a
  boolean — different types, unrelated values, nothing for the compressor
  to latch onto.
- **CPU execution** is the third (its own section below): processing values
  in bulk, column by column, lets the CPU run tight predictable loops
  instead of hopping between unrelated fields.

**The storage math that makes it concrete.** 100-column table, query
touches 5 columns → columnar reads roughly **5% of the bytes** before
compression even enters. Then compression compounds it: a `status` column
with 6 distinct values compresses 50–100x via dictionary + run-length
encoding (explained below, detailed in ch18) — while the same values
scattered through rows barely compress at all, because the compressor sees a
few useful bytes separated by 95 irrelevant fields. Total effect: **10–100x
less I/O**, and then vectorized execution spends fewer CPU cycles per value
on top of that.

This one layout decision is why "scan the whole fact table" became a
*viable* query plan — reading a billion rows is acceptable when you read 5%
of the bytes, compressed, at full CPU efficiency. And it is why the
MapReduce-era row formats (ch18 — sequence files, Avro-in-map-only jobs)
died for analytics: they had to read every byte to answer a 5-column
question.

### Why OLTP loves rows

Turn the query shape around and the ranking flips. OLTP access — "get order
42", "update order 42", "insert a new order" — touches *all columns of one
record*. Three mechanical reasons rows win:

- **A row is one contiguous write.** In a row store, an insert appends one
  record — one sequential write. In a column store, the same insert must
  append to *every* column's file or segment — N separate small writes. And
  if those N writes are not wrapped in a transaction, a crash between them
  leaves the record half-present: the `id` column knows the row exists but
  the `amt` column never heard of it. (Real columnar systems handle this
  with transactions or write-ahead structures — but it is machinery the row
  layout simply doesn't need, because its atomic unit is already the whole
  record.)
- **Point updates and deletes.** `UPDATE orders SET status='paid' WHERE
  id=42` in a row store rewrites one row block — the record is *right
  there*. In a column store, the row is smeared across N segments, so the
  update must touch the id-column segment *plus every other column's
  segment holding that row* — or, the common modern approach, append a
  delete-marker plus a corrected row, and let readers skip the old version.
  That delete-marker dance is exactly the merge-on-read cost that lakehouse
  table formats (ch12) spend real effort managing.
- **`SELECT *` by id — reading the whole record.** The row layout already
  has all the record's values adjacent: one page read and you're done. The
  column layout must fetch the row's value from *each* of the N column
  segments and stitch them back together — a step with a name, **tuple
  reconstruction** — which is pure overhead when you wanted the whole row
  anyway.

### The underlying tension: read vs write amplification

"Amplification" names the core mismatch: doing one logical thing, but
physically touching more than that thing's worth of data. Read amplification:
one logical read that touches extra bytes. Write amplification: one logical
write that touches extra bytes.

| | Row (B-tree-ish) | Columnar |
|---|---|---|
| Read wide rows | minimal | amplified (N segments + reconstruction) |
| Read few columns, many rows | amplified (reads unneeded columns) | minimal |
| Write/update a record | minimal (one page) | amplified (N segments or delete+append) |

Read the table as a division of labor. **Analytics is the "few columns,
many rows" cell** — scans touching 5 columns across a billion rows — and
that is columnar's home cell, where the *row* layout amplifies the read 20x.
**OLTP is the "all columns, one row" cell** — point reads and writes — and
that is the row layout's home cell, where *columnar* amplifies every
operation. There is no universally better layout: **the layout *is* the
workload fit**, and the question is only which amplification your workload
can afford.

Hybrid designs exist precisely to hedge the trade. **PAX** (Partition
Attributes Across) is the key one: the file is divided horizontally into
**row groups** (say, 128 MB of rows at a time), and *within* each row group
the values are laid out column-wise. This is the layout inside Parquet and
ORC (ch18). It buys columnar I/O for scans while bounding the tuple-
reconstruction cost of full-row reads — you only ever reassemble rows from
one row group's worth of columns, not the whole file.

### Where you'll actually feel it

- **Parquet/ORC files** (ch18) are columnar storage at rest — the physical
  layout of every serious lakehouse (ch12). When this chapter's math says
  "read 5% of the bytes," Parquet is the file format that makes it happen.
- **Warehouse engines** (ch13) are columnar storage plus vectorized
  execution inside: Snowflake, Redshift, BigQuery's internals, and DuckDB on
  your laptop all store data by column and process it in batches. The
  "analytics is fast in the warehouse" experience is this chapter, running.
- **Spark** reading Parquet gets column batches and whole-stage codegen
  (ch19). One habit follows directly: **column pruning** — doing
  `.select()` early so Spark reads only needed columns — is not style
  tidiness. Against a columnar source, the columns you don't select are
  segments never read from disk: selecting early *is* the I/O reduction.
- **OLTP databases** are row stores. Running wide analytical scans on them
  (ch13's anti-pattern) is now explained mechanically: every query reads
  100 columns' bytes to use 4, row-at-a-time, on an engine tuned for the
  opposite shape.

### Encodings: the compression engine room (preview of ch18's formats)

Columnar compression is not "gzip a column." It is *type-aware encoding
applied before* general-purpose compression — each encoding looks for one
specific kind of redundancy, and each has a data shape it loves:

| Encoding | Thrives on | Example |
|---|---|---|
| Dictionary | low-cardinality strings | `status`: 6 distinct values → 3-bit codes |
| Run-length (RLE) | sorted or repeated runs | `country_code` sorted: 'IN' x 40M rows → one triple |
| Delta | sorted numerics | timestamps 1ms apart → small deltas |
| Byte-stream split | high-entropy floats | sensor readings |

Each in plain words:

- **Dictionary encoding** handles columns with few distinct values
  ("low cardinality" — cardinality = number of distinct values). List the
  distinct values once (`paid=0, refunded=1, …`), then store each row's
  value as a tiny code instead of the string. Six distinct statuses fit in
  3 bits — about 1 byte instead of a ~6-byte string, a ~6x shrink before
  anything else runs.
- **Run-length encoding (RLE)** handles *consecutive repetition*: store a
  run as one triple — (value, count, position) — instead of the value N
  times. A sorted `country_code` column with 40 million rows of 'IN' in a
  row becomes one triple. Sorting a column before storing it is therefore a
  compression decision, not just a query decision.
- **Delta encoding** handles sorted numbers where neighbors are close:
  store the first value, then only the difference from the previous one.
  Timestamps 1 millisecond apart become a stream of 1s — tiny numbers,
  which compress further and are cheap to scan.
- **Byte-stream split** handles high-entropy floats (sensor readings,
  where the bytes look random) by splitting the bytes of each float into
  separate streams so each stream's bytes are more alike and compress
  better.

 The compound effect: dictionary-encode `status` (`Write each distinct status down once, then replace it with a small number: || paid → 000, pending → 001, refunded → 010 || 6 values → 3-bit codes`),
then RLE the runs of identical codes (`If the same code repeats next to itself, store the code once plus how many times it repeats: 000, 000, 000, 001, 001 becomes something like: 000 × 3, 001 × 2`), then hand the result to Zstd for
general-purpose compression (`general-purpose compressor looks for any more patterns in that compact representation and shrinks it further.`) — a combined "10–100x" boost that row layouts OLTPs cannot
achieve, because their adjacent bytes are *different types* and defeat every
one of these encodings at the first step. (Chapter 18 puts these encodings
inside Parquet/ORC.)



And here is the query-acceleration secret that ties compression to speed:
**dictionary-encoded columns let the engine evaluate predicates on codes,
not values.** `WHERE status = 'paid'` becomes "compare 3-bit integers"
instead of "compare strings" — a cheaper comparison, on a column already in
CPU-friendly form. On top of that, engines store min/max statistics per
chunk, so `WHERE dt = '2026-09-20'` can *skip whole chunks* whose min/max
exclude the date — before decoding anything (the pushdown mechanics of
ch19). This is the deep point: **compression and query speed are the same
mechanism, not a trade-off** — the encoding that shrinks the column is the
same one that makes predicates cheap.

### Vectorized execution — why the CPU cares

The third multiplier is how the CPU executes the query plan.

A **row-at-a-time engine** (the classic volcano-style iterator) processes
one record per operator step. Each row pays interpretive overhead: virtual
function calls between operators, unpredictable branches (does this row
pass the filter? yes/no, differently each time), and cache misses as the
CPU jumps between a row's scattered fields. The CPU spends a large share of
its cycles on *managing* the rows rather than computing on the values.

A **vectorized engine** (ClickHouse, DuckDB, modern warehouse executors)
processes **arrays of values per operator invocation** — a filter gets a
batch of 1,024 `status` values and returns a batch of matching ones, in one
tight loop over contiguous memory. Three benefits stack up:

- **Tight loops** over contiguous arrays stay in CPU cache (the column came
  off disk together, so its values sit together in memory).
- **SIMD instructions** apply: SIMD (Single Instruction, Multiple Data) is a
  CPU feature where one instruction operates on several values
  simultaneously — e.g., compare 8 integers in one cycle. That only helps
  when like-typed values are packed together — which is exactly what the
  columnar layout provides.
- **Branch-predictable paths** — the same comparison repeated over a batch
  trains the CPU's branch predictor, versus a filter decision that flips
  row by row.

Combined with columnar I/O, this is where the "10–100x" gets its second
multiplier: **fewer bytes moved *and* fewer cycles per value processed.**
In an interview, mentioning vectorization is the difference between
"Parquet is fast" and knowing why it is fast.

## The Options

| Data access profile | Layout | Embodiment |
|---|---|---|
| Point lookups, full-row reads, small writes | row | Postgres/MySQL tables, RocksDB SSTables (ch14) |
| Wide scans, few columns, bulk loads | column | Parquet/ORC files, warehouse internals |
| Mixed within one file format | hybrid (row groups x column chunks) | Parquet's layout: row-group-wise, column-wise inside |
| Mixed within one system | split by store | OLTP db + warehouse (the ch15 pattern) |

Each option, in plain words:

- **Row** for record-shaped access: Postgres/MySQL tables, RocksDB's
  SSTables (ch14's LSM world is row-oriented at its core). One record = one
  place = one read, one write.
- **Column** for scan-shaped access: Parquet/ORC files and warehouse
  internals. One column = one contiguous read, over as many rows as you
  like.
- **Hybrid within one file** — Parquet's actual design: data divided into
  **row groups** (horizontal partitions, e.g., 128 MB), and **within each row
group** each column stored contiguously **with its own encoding and min/max
statistics**. Worth one sentence in any interview, because it explains the
  design: you get columnar I/O for scans *and* bounded tuple-reconstruction
  cost for full-row reads — you only ever reconstruct rows within one row
  group, never the whole file.
- **Split by store** when one system has genuinely mixed workloads: the
  OLTP database holds the row-shaped operational side, the warehouse holds
  the column-shaped analytical side, and CDC keeps them in sync — the
  chapter 15 pattern, with this chapter as its physical justification.

## Decision Rules

- **Analytics-shaped access (few columns, many rows) → columnar. Always.**
  The three multipliers all point the same way; there is no analytics
  workload for which row storage is the right answer at scale.
- **OLTP-shaped access (whole rows by key) → row. Always.** Point reads and
  record-at-a-time writes are what row layouts make cheap; columnar would
  amplify every one of them.
- **Mixed workload → split stores by workload (ch15), not compromise
layouts.** A "middle" layout serves neither cell well; two stores, each
  in its home cell, synced by pipeline, beats one store bad at both.
- **Select columns early in Spark/SQL against columnar sources.** Against a
  columnar layout, unselected columns are segments never read — projection
  pushdown is an I/O *decision*, not code tidiness.
- **Bulk-load patterns suit columnar; record-at-a-time patterns suit row.**
  Columnar writes are batched appends per column — perfect for pipelines
  loading thousands of rows at once, wrong for inserting one order as it
  happens.
- **When someone proposes "the warehouse for the app's user profile
reads," point at the physics.** Tuple reconstruction on every read, plus
  warehouse concurrency queues in front of a millisecond-latency
  requirement — that is a KV projection job (ch16), not a warehouse
  workload.

## Failure Modes

- **Column store for point lookups.** Using a columnar engine for "get
  record by id": latency lands 10–50x worse than a row store, because the
  read touches N column segments and pays tuple reconstruction.
  Symptom: "our columnar database is slow at our #1 query." It is not a
  bug — it is by construction; the layout was chosen for a different
  workload.
- **Row store for scans.** The eternal analytics-on-OLTP mistake (ch13's
  incident, now mechanically explained): every query reads 100 columns'
  bytes to use 4, row-at-a-time, with weak compression and no vectorization
  — all three multipliers running in reverse. The fix is the warehouse or
  lakehouse, not more indexes.
- **Row-at-a-time thinking in pipelines (column based).** Reading *all* columns and
  filtering late, in code that runs against columnar storage. The fix is
  free — select the columns early and push the filters down (ch19), and
  the engine skips the unneeded segments entirely — and it is routinely
  ignored because the code "works." It works at 20x the I/O budget.
- **Many-small-updates into columnar tables.** Per-record upserts against
  Parquet-backed tables trigger the merge-on-read / delete-file spiral
  (ch12): every update appends markers and new row fragments, reads slow
  under accumulating markers, compaction falls behind. The correct shape is
  *batched* upserts (micro-batches, minutes not milliseconds) plus
  scheduled compaction — or keeping truly record-at-a-time updates in the
  OLTP store where they belong.
- **Compression assumptions unexamined.** "Columnar compresses 10x" is a
  property of *repetitive* columns. High-cardinality random values — UUIDs
  above all: 16 essentially random bytes, no repetition, nothing for
  dictionary, RLE, or delta to find — compress poorly even columnar.
  Sizing storage by a blanket compression ratio hits reality on
  entropy-heavy columns; measure per column family instead.

## Interview Narration

"Row versus columnar is decided by the query, and the reason is physics.
Analytics queries touch few columns across many rows — so a columnar layout
reads about five percent of the bytes for a five-of-a-hundred-column query,
and then compresses better because adjacent values are like-typed and
similar: dictionary and run-length encoding collapse a status column by two
orders of magnitude. Vectorized execution finishes the job — same operation
applied to column batches, SIMD-friendly. That stack — fewer bytes, tighter
compression, tighter CPU loops — is the 10-to-100x, and it's why every
serious analytics format and engine is columnar.

OLTP is the mirror: a row is one contiguous write and one page read, while
columnar pays per-column segment writes and tuple reconstruction for
full-row reads — so record-at-a-time systems stay row-oriented. The deep
frame is amplification: row layouts amplify reads of few columns, column
layouts amplify writes and full-row reads; you **pick which amplification
your workload can tolerate.**

Parquet, for the record, is a hybrid: row groups horizontally, columnar
within each group — columnar I/O with bounded reconstruction cost. And the
practical corollary I actually enforce in code: select columns early and
push predicates down, because against a columnar source, projection is not
style — it is the I/O budget."

*(Every claim in this narration is justified by the sections above — if any
sentence feels like shorthand you couldn't defend, re-read the matching
section.)*

---

| <- Previous | Next -> |
|---|---|
| [Chapter 16 — The Serving Layer & Consumption](ch16-the-serving-layer-and-consumption.md) | [Chapter 18 — File & Wire Format Catalog](ch18-file-and-wire-format-catalog.md) |
