# Chapter 06 — CDC, the Long Tail & Normalization

> Part II — Ingestion Patterns (Getting Data In)

CDC — Change Data Capture — is the highest-leverage ingestion pattern of the last
decade: it turns a database into an event stream without touching the
application. It is also the bridge that connects Part II to everything streaming
in Part III and IV.

## The Question

*"I need a database's changes — inserts, updates, AND deletes — as they happen,
without hammering the database. Do I query it, or do I read its diary?"*

## The Physics

### The transaction log is the diary

Every serious OLTP database writes a log before it commits: Postgres WAL, MySQL
binlog, DynamoDB streams, SQL Server CDC. The log already contains every change,
ordered, as the cost of doing business. CDC = **read the log, publish the changes**.

```
app -> BEGIN; UPDATE orders SET status='paid' WHERE id=42; COMMIT
                          |
                          v  (already written, as a side effect)
        WAL/binlog: [row image: id=42, before=..., after=..., txid, lsn]
                          |
                          v  (Debezium / DMS / Datastream)
        Kafka topic: orders.cdc  {op:UPDATE, before:{...}, after:{...}, lsn}
```

Compare the approaches:

| | Query-pull (ch04) | CDC |
|---|---|---|
| Source load | full/partial scans | read the log — near zero |
| Deletes visible | no (row is gone) | yes (delete event) |
| Updates on old rows | only if watermark bumps | always |
| Latency | poll interval | seconds |
| Source coupling | SQL against prod/replica | log reading + replication slot |

The deletes row is the one interviewers probe: polling cannot see a row that no
longer exists; CDC hands you a tombstone event. If a requirement says "our
warehouse must reflect deletions" (GDPR, cart cleanups), CDC is not an
optimization — it is the *only* correct pull-side pattern.

### Snapshot + incremental: the two phases

CDC pipelines bootstrap in two phases:

1. **Snapshot**: read the current full table (consistent, often chunked with the
   same JDBC parallelism as ch04), emit as initial events.
2. **Incremental**: tail the log from the snapshot's exact log position (LSN,
   GTID, binlog coordinate).

The engineering care is in the seam: the snapshot must be taken *at a recorded log
position* so no change is missed and none double-counted between phases.
Off-the-shelf Debezium does this; hand-rolled pipelines usually get it wrong
twice (once with gaps, once with duplicates).

### Debezium architecture — the household name

```
Postgres -> [Debezium Kafka Connect connector]
              |-> reads WAL via logical replication slot
              |-> tracks: offsets topic (log position per slot)
              |-> tracks: schema history topic (DDL as-of position)
              v
            Kafka: server.db.table  (op: c/u/d/r, before, after, ts, lsn)
```

Three details that matter in production:

- **Offsets**: the connector resumes from the last recorded log position. Offsets
  lost = re-read from an earlier position + idempotent consumers, or gap.
- **Schema history**: DDL is replayed in order so the connector can decode old
  row images. A schema change without history sync = connector stops safely
  (which is the *right* failure — loud, not silent).
- **Replication slot retention (Postgres)**: WAL is retained while the slot is
  unconsumed. **Connector down for two days = WAL accumulates = disk fills =
  the source database is at risk.** This is the classic CDC operational incident:
  your pipeline's failure mode is *on someone else's critical system*. Alert on
  slot lag like it's production (it is).

### "A database pretending to be a stream"

Once CDC exists, the orders table *is* a Kafka topic of change events. That
reframes everything downstream:

- Stream processing can maintain real-time aggregates over the *operational*
  database — no dual-write from the app, no "publish to Kafka AND write the DB,
  hope both succeed" inconsistency.
- The anti-pattern it kills: **dual writes**. App writes DB, then publishes to
  Kafka — one fails, they diverge forever. CDC makes the DB the single source of
  truth and the topic a *derived projection*.
- Serving stores (chapter 14/16) can be kept in sync as materialized views of
  the log — the Kappa idea (ch11) applied to databases.

### When CDC does NOT win

Honesty about limits, from the source owner's perspective:

- **DBA resistance**: you are reading their transaction log. Replication slots
  hold WAL. Mismanaged CDC can threaten the source. Mitigations: managed CDC
  (DMS, Datastream), monitored slots, connectors deployed with the DBA team as
  stakeholders.
- **Log retention windows**: if the connector is down past log availability, you
  must re-snapshot — for big tables, hours.
- **Schema churn**: every DDL is an event the pipeline must understand; frequent
  migrations = frequent connector care.
- **Fan-out amplification**: one UPDATE becomes a full row-image event —
  wide rows at high update rates are real volume; compact/partition sensibly.
- **When polling is fine**: low-frequency, small, append-only reference data —
  a nightly pull of 50k rows is simpler than any CDC deployment. Match the
  machinery to the stakes.

## The Long Tail

Three more source families complete the landscape — each rare in interviews but
each is a senior signal when named unprompted.

### Data sharing — "the fastest pipeline is no pipeline"

Snowflake Secure Shares, BigQuery dataset sharing, Delta Sharing: mount another
org's *live* table, zero-copy, no ETL.

- When both parties are on the same platform: don't build a pipeline — share the
  table. Cross-cloud, cross-region, and governance boundaries are what these
  technologies actually negotiate.
- The interview point: pipelines exist to move data to where computation is. If
  you can move *the permission* instead, do that. Zero latency, zero drift, zero
  cost — and the provider keeps operating it.

### Manual / human uploads

Spreadsheets from marketing, survey exports, an ops CSV every Friday. Unglamorous
and universal. The design is not "how to ingest a file" (ch04) but **how to make
human input safe**: an upload contract (schema, owner, cadence), validation at
ingestion with *actionable* rejection (tell the human what broke), quarantine not
silence, and audit lineage like any other source. Most data-quality horror
stories start with a human and a spreadsheet.

### Web scraping

For sources with no API: brittle (selectors break), legally loaded (ToS,
robots, jurisdiction), and rate-sensitive. If you must: treat it as a pull source
with the strictest watermark discipline (ch04), snapshot-diff for incrementality,
and a compliance sign-off. In an interview, name the legal risk unprompted — it
is the adult thing to say.

## Normalization: the two-plane landing pattern

Pulling together all of Part II — every source, whatever its shape, lands in the
same two planes (introduced in ch03):

```
 CSV drops   APIs   DB query   CDC   webhooks   logs   SaaS   shares
    |         |        |        |       |        |      |       |
    v         v        v        v       v        v      v       v
 +---------------------------------------------------------------+
 | STREAM PLANE (Kafka / Pub/Sub)                                |
 |   normalized event envelopes, keyed by entity, DLQ configured |
 +-----------------------------+---------------------------------+
                               |  (also archived on land)
                               v
 +---------------------------------------------------------------+
 | RAW PLANE (object storage, immutable)                         |
 |   as-landed events (Avro/JSON), partitioned by arrival time   |
 |   = the replayable source of truth ("bronze")                 |
 +---------------------------------------------------------------+
                               |
                               v
                    derive everything downstream
```

Why immutable raw is the source of truth, stated precisely:

- **Replay**: re-run any transformation over any window by re-reading raw.
  Bounded by nothing but storage cost — not by queue retention (ch05) or API
  memory (ch04).
- **Reprocess after bug**: the "we aggregated wrong for 3 weeks" incident becomes
  a re-run, not an archaeology project.
- **Schema evolution**: new logic applied to old events — read the same bytes
  with a new reader schema (ch18).
- **Audit**: what did we *actually receive* — not what our pipeline chose to
  keep.

The cost discipline: raw storage is cheap but not free; lifecycle-tier it
(hot -> infrequent -> archive) and delete deliberately under governance (GDPR
erasure vs immutability is a real design conversation — ch20).

## The Options

The chapter's recurring decision — how to extract data from a system that
was never designed as a source:

| Source situation | Options | Senior default |
|---|---|---|
| Operational DB, need changes | query-pull / CDC / app dual-write | CDC; dual-write is the anti-pattern |
| Operational DB, small & slow-moving | nightly pull / CDC | nightly pull — match machinery to stakes |
| Same-platform consumer | build a pipeline / data share | share — move the permission, not the data |
| Partner data, structured | SFTP drops / API / share | whatever they support; land to two planes |
| Human-generated | ad-hoc spreadsheets / upload contract | contract + validation + quarantine |
| No API at all | scraping / requested export | request the export; scrape last |

And the landing decision — what everything normalizes into:

| Landing shape | What it gives | What it costs |
|---|---|---|
| Stream plane only | low latency, replay within retention | no long-term replay; retention bill grows |
| Raw plane only | cheap, forever-replayable source of truth | no low-latency consumers |
| Both planes | latency AND replay — the senior default | two systems + arrival-partitioning discipline |

## Decision Rules

- **Operational DB + need changes (especially deletes) + recurring -> CDC.**
- **CDC slots are production-critical on someone else's system**: alert on slot
  lag, size WAL retention with the DBA team, have a re-snapshot runbook.
- **Never dual-write** — DB as truth, CDC/log as the derived stream.
- **Snapshot+incremental seam is where hand-rolled CDC fails**: use Debezium or
  managed equivalents unless the seam is your explicit engineering project.
- **Same-platform data consumers -> data sharing, not pipelines.**
- **Human uploads need a contract and a rejection loop**, not just a file
  landing.
- **Everything lands in two planes**; raw immutable is the source of truth;
  every downstream artifact must be derivable from it.

## Failure Modes

- **WAL disk-full from an idle slot**: connector down 2 days, WAL retained,
  source DB stalls. Symptom: the app team pages *you*. The one everyone learns
  the hard way.
- **Silent re-snapshot gap**: connector resumed past available logs, re-snapshot
  missed in-flight transactions, reconciliation drifts by exactly the seam.
- **CDC as a hammer**: nightly 50k-row reference table wrapped in Debezium +
  Kafka + Connect — machinery cost exceeding the data's value.
- **Dual-write zombie**: app team "helpfully" still publishing to the old topic
  beside CDC; two versions of truth diverge slowly.
- **Quarantine-less manual uploads**: one malformed spreadsheet silently nulls a
  column downstream for a week.
- **Raw archive with no lifecycle**: 3 years later, the archive is the biggest
  line item in the bill and the legal team wants deletes you can't perform.

## Interview Narration

"For operational databases, my default is CDC — reading the transaction log —
rather than query-pulls. Three reasons: near-zero source load, seconds-level
latency, and the killer feature — deletes. Polling literally cannot see a row
that no longer exists, and if the warehouse must reflect deletions, CDC is the
only correct pattern. I'd run Debezium into Kafka, with the snapshot-then-log
bootstrap, and I'd treat the replication slot as production-critical
infrastructure on the source system: slot lag alerting and a re-snapshot runbook,
because an idle connector retaining WAL until the source disk fills is *the*
classic CDC incident.

Once CDC exists, the database is effectively a stream — which kills the dual-write
anti-pattern: the app writes only the DB, and the topic is a derived projection.
That gives me the Kappa-style option of rebuilding serving tables as materialized
views of the log.

I'd be honest about when CDC is overkill: small, low-frequency reference data is
a nightly pull, not a log-tailing deployment. And across all sources, I land
everything into two planes — a stream plane for latency and a raw immutable
archive as the source of truth — so that every downstream table is derivable and
a bad transformation is a re-run, not an investigation."

---

| <- Previous | Next -> |
|---|---|
| [Chapter 05 — Push Patterns](ch05-push-patterns.md) | [Chapter 07 — Batch](ch07-batch.md) |