# Chapter 04 — Pull Patterns (Files, APIs, JDBC, SaaS)

> Part II — Ingestion Patterns (Getting Data In)

Pull means **you** initiate: you control the pace, you handle the failures, and —
critically — the source's quirks (rate limits, watermarks, indexes) become your
engineering problems. This chapter covers the four pull families and the JDBC
mechanics that interviewers love to probe in detail.

## The Question

*"The source won't push to me — I have to go get it. How do I pull files, APIs,
and databases reliably, incrementally, and without hurting the source?"*

## The Physics

### Files: the oldest pattern, still everywhere

Partner SFTP drops, database exports, "we'll email you the spreadsheet" — files
land in object storage and you process them.

- **Arrival detection**: event notifications (S3 -> SNS/SQS, GCS -> Pub/Sub) are
  strictly better than polling listings — lower latency, no listing cost at scale.
- **Splittability decides parallelism**: one giant CSV reads with one task; a
  directory of files reads with N tasks. When you control the export, **ask for many files** .
- **Dealing with partial writes**: a file appearing is not a file being complete.
  Defensive patterns: write-then-rename convention (`tmp/` then `final/`
  prefixes/partition), or size-stability checks before committing. These patterns prevent processing a file while it is still being uploaded.

    **Write-then-rename**

    1. The producer writes the file to a temporary location, such as `tmp/orders.csv`.
    2. Once writing finishes, it moves or renames it to `final/orders.csv`.
    3. Consumers monitor only `final/`, so they process files only after publication.

    For partitioned data, the producer may write an entire partition under a temporary prefix and publish it afterward:

    ```text
    tmp/date=2026-09-25/part-000.csv
    tmp/date=2026-09-25/part-001.csv
    
    final/date=2026-09-25/part-000.csv
    final/date=2026-09-25/part-001.csv
    ```

    The important idea is that temporary paths are invisible to consumers.

    **Caveat:** on local filesystems, rename is usually atomic. In object storage such as S3, a “rename” is commonly implemented as copy-then-delete, so it is not truly atomic. A completion marker such as:

    ```text
    final/date=2026-09-25/_SUCCESS
    ```

    can signal that the whole partition is ready.

    **Size-stability checks**

    If the producer cannot use temporary and final locations:

    1. Detect the file.
    2. Record its size.
    3. Wait for a period.
    4. Check the size again.
    5. Process it only if the size has not changed.

    For example, process `data.csv` only after its size remains `2.4 GB` across two checks. This is weaker than a completion marker because a stalled upload can appear stable, but it is useful when the producer’s behavior cannot be changed.
- **Semantics**: each file is a snapshot of a moment. History = keeping files.
  Most file feeds have no change events; merges are yours to design.

### API polling: watermarks and their lies

The core loop: call endpoint with a cursor, get a page, advance, repeat, checkpoint
the cursor.

**Pagination shapes:**

| Shape | Example | Notes |
|---|---|---|
| Offset | `?page=3&limit=100` | breaks under concurrent inserts (skips/dupes); fine for static data |
| Cursor/token | `?after=eyJpZCI6...` | stable under inserts; the good case |
| Date-windowed | `?updated_since=...` | natural for sync; overlaps windows to dodge boundary races |

Think of pagination as asking an API to return a large result in smaller
pages. The three common approaches differ in how the next page is identified:

- **Offset** means "skip the first N rows." For example,
  `GET /orders?page=3&limit=100` asks for rows 201-300. If a new order is
  inserted at the beginning after page 1 was read, the row positions shift:
  page 2 may repeat a row from page 1 or cause another row to be skipped. This
  is acceptable for a static report, but risky for a live system.
- **Cursor/token** means "continue after this exact position." The API returns
  a token with the page, for example,
  `GET /orders?after=abc123&limit=100`. The client sends that token to get the
  next page. New rows inserted while reading do not normally shift the already
  established position, so this is usually the safest option.
- **Date-windowed** means "return records changed after this time."
  For example, `GET /orders?updated_since=2026-09-25T10:00:00Z` returns orders
  updated since 10:00. A sync job might use a five-minute overlap and request
  from 09:55 on the next run, then remove duplicates by order id. The overlap
  protects against clock differences and updates arriving exactly at the
  boundary.

In practice, prefer cursor pagination when the API provides it. Use date
windows for incremental synchronization when the source exposes a reliable
`updated_at` field. Treat offset pagination mainly as a choice for static or
small datasets.

**The watermark pattern and its failure modes** — `WHERE updated_at > last_cursor`
is the workhorse and the classic trap:

1. **No index on the watermark column** -> every poll is a full table scan on
   their side. You discover this as rate-limit 429s or angry partner emails.
2. **Clock skew / non-monotonic timestamps** -> rows written with earlier
   timestamps after you checkpointed the cursor are *invisible forever*. The fix:
   overlap windows ("updated_at > cursor - 5 minutes") plus dedup by id.
3. **Updates that do not bump the column** -> the row changed but `updated_at`
   did not; you never see it. If you cannot fix the producer, you are reduced to
   periodic full-table checksums.
4. **Deletes are invisible** -> a polling API cannot tell you a row vanished.
   Full-snapshot diffing, or tombstone feeds, are the only answers.

**Rate limits**: respect `Retry-After` / 429s with exponential backoff + jitter;
budget throughput accordingly (200 req/min at 100 rows/req = 20k rows/min max —
do that math before promising a 500M-row sync).

### DB query-pull: rules of engagement

- **Never full-scan a production OLTP database from analytics.** Point lookups and
  short transactions are what its indexes and memory are tuned for; your
  `SELECT *` is neither.
- **Read replica** is the minimum: isolates your scan cost from the OLTP path.
  Still: long-running watermark queries can lag replication — watch replica lag.
- **Isolation for consistency**: an incremental pull should read in a single
  repeatable-read / snapshot transaction so the watermark and the rows move
  atomically. Two queries, two timestamps = the classic missed-rows race.
- **SELECT only what you need**: column pruning is not just faster — it is
  cheaper for them and less exposure if there is PII you do not need.

### JDBC mechanics — the interview classic

*"How do you actually move 500M rows from Postgres?"* — the answer that separates
"used Spark" from "understood Spark."

**Connection string anatomy:**

```
jdbc:postgresql://db.host.internal:5432/analytics?sslmode=require&connectTimeout=10
 \___/   \________/ \_________________/ \__/ \______/ \_________________/
 driver   host            port          db   params
```

**The naive read** (one partition, one connection):

```python
df = spark.read.jdbc(url, "orders", properties={"user": "...", "password": "..."})
# => SELECT * FROM orders  on ONE connection, ONE task. Hours. Maybe OOM.
```

**The parallel read** — Spark can shard the query across `numPartitions`
connections *if* you give it a numeric, uniformly-distributed column:

```python
df = (spark.read
  .format("jdbc")
  .option("url", url)
  .option("dbtable", "orders")
  .option("partitionColumn", "id")        # numeric, ideally PK
  .option("lowerBound", 1)                # min-ish; used for stride math only
  .option("upperBound", 500_000_000)      # max-ish; does NOT need to be exact
  .option("numPartitions", 64)            # = 64 parallel connections
  .option("fetchsize", 10000)             # rows per round-trip per connection
  .load())
```

What Spark actually executes: 64 queries of the shape
`SELECT ... WHERE id >= a AND id < b` — each on its own connection, each a
range scan on the PK index, all running concurrently.

**The knobs that matter:**

| Knob | What it does | Getting it wrong |
|---|---|---|
| `partitionColumn` | column to shard on | non-uniform column -> one giant partition dominates runtime |
| `lowerBound`/`upperBound` | stride computation only | wildly wrong bounds -> uneven partitions (still correct, just slow) |
| `numPartitions` | parallel connections | too many = you just DDoS'd the source |
| `fetchsize` | rows per network round-trip | default (often small) -> round-trip-bound transfer |

The senior coda: 64 parallel connections against a replica is a load decision you
make *with the source owners* — "how many connections and what scan rate can your
replica absorb?" is the real production question. And for anything recurring or
heavier, CDC (ch06) beats query-pull anyway.

### SaaS connectors: buy the boring plumbing

Salesforce, Google Ads, HubSpot, Stripe, Zendesk... all are "just APIs" (Chapter's
API section applies underneath), but the pattern is distinct: incremental cursor
sync + schema-drift handling + sync scheduling, managed. Fivetran/Airbyte/Airflow
providers industrialize exactly this.

**Build vs buy, honestly:**

| | Buy (Fivetran/Airbyte) | Build |
|---|---|---|
| Time to first sync | hours | days-weeks per source |
| Cost | per-row/month pricing grows with you | engineer-hours + maintenance forever |
| Edge cases | connector's backlog | yours |
| Custom logic | limited hooks | unlimited |
| Strategic sources | fine | often worth owning |

Rule of thumb: buy commodity sources; own the two or three sources that are
strategic (weird, high-volume, or business-critical).

## The Options

| Pull family | Latency | Source load | Incrementality | Use when |
|---|---|---|---|---|
| File drop + events | file-dependent | none | snapshot-per-file | partners, exports, bulk |
| API polling | min-hours | theirs (rate limits) | cursor/watermark | no other interface exists |
| DB query-pull (replica) | min-hours | heavy (scan) | watermark | one-off, low-frequency |
| JDBC parallel pull | min-hours | heavy but bounded | watermark | bulk migration/backfill |
| SaaS connector | min-hours | managed | managed | commodity SaaS sources |

And the standing alternative: if you find yourself polling a *database* on a
schedule forever, the correct escalation is usually CDC (ch06) — change events
without the watermark lies.

## Decision Rules

- **Event notifications over polling listings** — for arrival detection, always (when checking whether a new file has arrived, prefer a notification from the storage system over repeatedly asking, “Is there a new file yet?”).
- **Cursor pagination > offset pagination**; overlap date windows and dedup.
- **Treat `updated_at` watermarks as guilty until proven innocent**: index?
  monotonic? bumped on every update? Any "no" needs a mitigation.
- **Pull from replicas, never prod OLTP**; read in one snapshot transaction.
- **Parallelize JDBC by a numeric key; size `numPartitions` against the source's
  capacity, not your cluster's.**
- **Deletes cannot be polled — only diffed or CDC'd.**
- **Buy the plumbing for ordinary sources; invest engineering effort in sources that differentiate or materially affect the business.**
  - Eg: A company-owned database with unusual requirements.
  A very high-volume source where per-row connector pricing is expensive.
  A proprietary system with no reliable existing connector.
  A source requiring custom transformations or special compliance controls
- **If a pull is recurring + heavy + from a DB, propose CDC instead.**

## Failure Modes

- **Invisible rows**: clock skew + checkpointed watermark = rows never seen.
  Symptom: row counts reconcile only after a full re-pull.
- **The Monday-morning prod incident**: watermark scan without index on the
  primary. Symptom: app-team latency alerts, then your PagerDuty.
- **One-connection JDBC pull**: 500M rows through a straw; the job "runs" for 14
  hours and blocks the SLA.
- **Partial-file processing**: reading `data.csv` while the uploader was still
  writing it. Symptom: truncated final batch, mysteriously fixed by re-running.
- **Offset pagination under writes**: pages shift mid-sync; silent skips and
  dupes. Symptom: reconciliation drift that only full re-syncs repair.
- **Connector sprawl, built**: 14 hand-rolled connectors, 14 failure modes, one
  on-call rotation (yours).

## Interview Narration

"For pull sources I care about three things: pagination strategy, watermark
correctness, and source load. Files are easy — event notifications over polling —
but snapshots, so change detection is mine. APIs: cursor pagination when offered,
and I treat `updated_at` watermarks with suspicion until I've confirmed the column
is indexed, monotonic, and actually bumps on every update — clock skew and
non-bumping updates are the classic silent-data-loss bugs, so I'll overlap the
window and dedup by id, and if deletes matter, polling fundamentally can't see
them.

For databases, the rule is never touch prod — replica or CDC. If I do a bulk pull
with Spark JDBC, I parallelize on a numeric primary key: partitionColumn with
sensible bounds and numPartitions sized to what the replica can absorb, fetchsize
in the thousands — 64 range-scan connections, not one giant SELECT. And if this is
recurring rather than one-off, I'd push back on polling entirely and propose CDC,
because reading the transaction log gives me inserts, updates, *and* deletes as
events, with no watermark lies and near-zero source load."

---

| <- Previous | Next -> |
|---|---|
| [Chapter 03 — The Ingestion Taxonomy](ch03-the-ingestion-taxonomy.md) | [Chapter 05 — Push Patterns](ch05-push-patterns.md) |