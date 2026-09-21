# Chapter 17 — Row vs Columnar: The Physics

> Part VI — Serving, Formats & Spark

Why does a columnar scan beat a row scan by 10-100x on analytics workloads?
The answer is physics — bytes read, compression, and CPU execution models —
and it's the kind of "why" that separates tool-users from engineers in
interviews.

## The Question

*"Should this data be stored row-wise or column-wise — and why is that choice
almost entirely determined by the query pattern?"*

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

Same logical table. Radically different I/O behavior per query type.

### Why analytics loves columns

Take: `SELECT status, SUM(amt) FROM orders WHERE dt='2026-09-20'` on a
100-column table — 5 columns referenced (dt, status, amt + id maybe).

| Effect | Row layout | Column layout |
|---|---|---|
| Bytes read (5/100 cols) | 100% of the row bytes | ~5% + small overhead |
| Compression | weak (adjacent bytes are unrelated types) | strong (like-typed, similar values adjacent) |
| CPU execution | row-at-a-time, branchy | vectorized: same op on column batches, SIMD-friendly |

**The storage math** that makes it concrete: 100-column table, query touches
5 columns -> columnar reads roughly 5% of the bytes (before compression).
Compression then compounds it: a column of `status` values with 6 distinct
values compresses 50-100x via dictionary + run-length encoding (ch18); the
same values scattered through rows barely compress at all. Total effect:
**10-100x less I/O**, then vectorized execution spends fewer cycles per value.
This one layout decision is why "scan the whole fact table" became a viable
query plan — and why MapReduce-era row formats (ch18) died.

### Why OLTP loves rows

- **A row is one contiguous write**: insert = append one record. Columnar
  insert = append to *every* column file — N small writes, and if they are
  not transactional, a crash leaves the record half-present.
- **Point updates/deletes**: `UPDATE ... WHERE id=42` rewrites one row block;
  in a column store it rewrites the id-column segment *plus every other
  column's segment for that row* (or appends delete markers — which is
  exactly the merge-on-read dance of ch12).
- **SELECT * by id**: the row layout already has all values adjacent — one
  page read. Columnar must reassemble the row from N column segments
  ("tuple reconstruction"), which is pure overhead for full-row reads.

### The underlying tension: read vs write amplification

| | Row (B-tree-ish) | Columnar |
|---|---|---|
| Read wide rows | minimal | amplified (N segments + reconstruction) |
| Read few columns, many rows | amplified (reads unneeded columns) | minimal |
| Write/update a record | minimal (one page) | amplified (N segments or delete+append) |

Analytics = "few columns, many rows" — columnar's cell. OLTP = "all columns,
one row" — row's cell. The layout *is* the workload fit; there is no
universally better layout, and hybrid designs (PAX — partition attributes
across, pages laid out column-wise inside row-groups; the layout inside
Parquet/ORC) exist precisely to balance the two.

### Where you'll actually feel it

- **Parquet/ORC files** (ch18) = columnar at rest; the physical layout of
  every serious lakehouse (ch12).
- **Warehouse engines** (ch13) = columnar storage + vectorized execution
  (Snowflake, Redshift, BigQuery internals; DuckDB on your laptop).
- **Spark** reading Parquet = column batches -> whole-stage codegen; column
  pruning (`.select()` early) is not style, it is I/O reduction (ch19).
- **OLTP databases** = row stores; running wide scans there is the ch13
  anti-pattern, now explained mechanically.

### Encodings: the compression engine room (preview of ch18's formats)

Columnar compression is not "gzip on a column" — it is type-aware encoding
*before* compression, and each encoding has a data shape it loves:

| Encoding | Thrives on | Example |
|---|---|---|
| Dictionary | low-cardinality strings | `status`: 6 distinct values → 3-bit codes |
| Run-length (RLE) | sorted or repeated runs | `country_code` sorted: 'IN' x 40M rows → one triple |
| Delta | sorted numerics | timestamps 1ms apart → small deltas |
| Byte-stream split | high-entropy floats | sensor readings |

The compound effect: dictionary-encode `status` (6 values → 3 bits), then
RLE the runs, then Zstd the result — a "10-100x" compression that row
layouts cannot touch because their adjacent bytes are *different types*
(ch18 puts these encodings inside Parquet/ORC). And the query-acceleration
secret: **dictionary-encoded columns evaluate predicates on codes, not
values** — `WHERE status = 'paid'` compares 3-bit ints, and min/max
statistics skip whole chunks pre-decode (ch19 pushdown). Compression and
query speed are the same mechanism, not a trade-off.

### Vectorized execution — why the CPU cares

Row-at-a-time engines spend cycles on per-row interpretation (virtual
calls, branches, cache misses jumping between columns). Vectorized engines
(ClickHouse, DuckDB, modern warehouse executors) process **arrays of
values per operator invocation** — tight loops over contiguous memory,
SIMD instructions, branch-predictable paths. Combined with columnar I/O,
this is where the "10-100x" gets its second multiplier: fewer bytes *and*
fewer cycles per value. Mentioning vectorization is the difference between
"Parquet is fast" and knowing *why* it is fast.
## The Options

| Data access profile | Layout | Embodiment |
|---|---|---|
| Point lookups, full-row reads, small writes | row | Postgres/MySQL tables, RocksDB SSTables (ch14) |
| Wide scans, few columns, bulk loads | column | Parquet/ORC files, warehouse internals |
| Mixed within one file format | hybrid (row groups x column chunks) | Parquet's layout: row-group-wise, column-wise inside |
| Mixed within one system | split by store | OLTP db + warehouse (the ch15 pattern) |

Parquet's internal layout is worth one sentence in any interview: data is
divided into **row groups** (horizontal partitions, e.g., 128MB) and within
each row group each column is stored contiguously with its own encoding and
statistics — giving columnar I/O with bounded tuple-reconstruction cost
(read one row group's columns, reconstruct rows there).

## Decision Rules

- **Analytics-shaped access (few columns, many rows) -> columnar. Always.**
- **OLTP-shaped access (whole rows by key) -> row. Always.**
- **Mixed workload -> split stores by workload (ch15), not compromise
  layouts.**
- **Select columns early in Spark/SQL against columnar sources** — projection
  pushdown is an I/O decision, not code tidiness.
- **Bulk load patterns suit columnar** (append-oriented, batched writes);
  record-at-a-time patterns suit row.
- **When someone proposes "the warehouse for the app's user profile reads,"
  point at the tuple-reconstruction + concurrency physics** — that's a KV
  projection job (ch16).

## Failure Modes

- **Column store for point lookups**: latency 10-50x worse than a row store
  for `get by id` — N segment reads + reconstruction. Symptom: "the NoSQL
  columnar database is slow at our #1 query" — yes, by construction.
- **Row store for scans**: the eternal `SELECT *` full scan on OLTP (ch13's
  incident, mechanically explained); every query reads 100 columns to use 4.
- **Row-at-a-time thinking in pipelines**: reading all columns then filtering
  late; in columnar systems the fix is free (select + predicate pushdown,
  ch19) and routinely ignored.
- **Many-small-updates into columnar tables**: per-record upserts to
  Parquet-backed tables -> the ch12 merge-on-read/delete-file spiral; the
  correct shape is batched upserts + compaction.
- **Compression assumptions unexamined**: high-cardinality random strings
  (UUIDs) compress poorly even columnar; sizing storage by "columnar
  compresses 10x" hits reality on entropy-heavy columns.

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
layouts amplify writes and full-row reads; you pick which amplification your
workload can tolerate.

Parquet, for the record, is a hybrid: row groups horizontally, columnar
within each group — columnar I/O with bounded reconstruction cost. And the
practical corollary I actually enforce in code: select columns early and
push predicates down, because against a columnar source, projection is not
style — it is the I/O budget."

---

| <- Previous | Next -> |
|---|---|
| [Chapter 16 — The Serving Layer & Consumption](ch16-the-serving-layer-and-consumption.md) | [Chapter 18 — File & Wire Format Catalog](ch18-file-and-wire-format-catalog.md) |