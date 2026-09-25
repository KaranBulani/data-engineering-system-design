# Chapter 06 — CDC, the Long Tail & Normalization

> Part II — Ingestion Patterns (Getting Data In)

Change Data Capture, usually called CDC, is a way to copy changes from a database into a stream of events. It can capture new rows, changed rows, and deleted rows. CDC is useful because it lets us react to database changes without repeatedly scanning the database. It also connects the ingestion ideas in this part of the book with the streaming systems discussed in Parts III and IV.

## The Question

Suppose we need our data warehouse or analytics system to know about every insert, update, and delete in an operational database. We want those changes quickly, but we do not want to put heavy query load on the production database.

There are two broad choices:

- Periodically query the database and look for rows that changed.
- Read the database's transaction log, which already records those changes.

CDC uses the second approach. It reads the database's own change history and publishes each change as an event.

## The Physics

### The transaction log is the diary

Before a serious OLTP database confirms a transaction, it writes information about that transaction to a log. This log is needed for recovery if the database crashes. Different databases give this log different names: PostgreSQL has WAL, MySQL has the binlog, and SQL Server has a transaction log. DynamoDB provides a similar feature through DynamoDB Streams.

The important idea is that the database is already recording its changes. A CDC connector reads that existing record instead of asking the database to repeatedly search its tables.

```text
app -> BEGIN; UPDATE orders SET status='paid' WHERE id=42; COMMIT
                          |
                          v  (already written as part of the transaction)
        WAL/binlog: [row image: id=42, before=..., after=..., txid, lsn]
                          |
                          v  (Debezium / DMS / Datastream)
        Kafka topic: orders.cdc  {op:UPDATE, before:{...}, after:{...}, lsn}
```

Here, `before` is the row as it existed before the update, and `after` is the row after the update. The log position, such as an LSN or binlog coordinate, tells the connector exactly where this change occurred in the database's history.

The two approaches have different trade-offs:

| | Query-pull (ch04) | CDC |
|---|---|---|
| Source load | Repeated full or partial scans | Reads the existing log; usually much lighter |
| Deletes visible | Usually no, because the row is gone | Yes, as a delete event |
| Updates to old rows | Only when the chosen watermark changes | Yes, whenever the database records the update |
| Latency | Depends on how often we poll | Often seconds, depending on the connector and destination |
| Source coupling | SQL queries against the database or a replica | Log-reading permissions and, for some databases, a replication slot |

Deletes are one of the most important differences. After a row is deleted, a normal query cannot find it. A CDC connector can still see the delete record in the transaction log and send a tombstone or delete event downstream. Therefore, if a warehouse must reflect deletions—for example, because of GDPR requests or abandoned shopping carts—CDC is often the correct ingestion pattern rather than merely a faster version of polling.

### Snapshot + incremental: the two phases

A new CDC pipeline has to learn both the old data and the future changes. It normally does this in two phases:

1. **Snapshot:** Read the current contents of the table and publish them as initial events. Large tables can be read in consistent chunks, often using the same kind of JDBC parallelism discussed in ch04.
2. **Incremental capture:** Continue reading the transaction log from the exact log position associated with the snapshot.

The handoff between these phases is called the seam. It must be handled carefully. If the pipeline starts reading too late, it misses changes made during the snapshot. If it starts too early without deduplication, it may publish the same change twice.

For example, imagine that a snapshot reads a customer row at log position 1,000. The connector must remember that position and make sure that changes after position 1,000 are captured when it switches to log reading. Production tools such as Debezium implement this coordination. A hand-written connector often fails here by creating either a gap or a duplicate.

### Debezium architecture — the household name

```text
Postgres -> [Debezium Kafka Connect connector]
              |-> reads WAL via logical replication slot
              |-> stores offsets (the last log position read)
              |-> stores schema history (DDL in log order)
              v
            Kafka: server.db.table  (op: c/u/d/r, before, after, ts, lsn)
```

In this example, `c` means create, `u` means update, `d` means delete, and `r` means a record read during the initial snapshot.

Three operational details are especially important:

- **Offsets:** The connector records the last log position that it successfully processed. If the connector restarts, it uses that offset to continue. If the offset is lost, the connector may read old changes again, so downstream consumers must be able to handle duplicates, or the pipeline must recover from a known position. If the connector starts after the log has already been discarded, it may create a gap.
- **Schema history:** A row image can only be decoded correctly if the connector knows what the table looked like at that point in time. The schema-history topic records DDL changes, such as adding or renaming a column, in order. If the connector cannot reconcile the schema history, stopping loudly is safer than silently producing incorrect data.
- **Replication-slot retention in PostgreSQL:** A replication slot tells PostgreSQL which WAL records the connector still needs. PostgreSQL keeps those records until the connector consumes them. If the connector is down for two days, WAL can accumulate and fill the database disk. This can put the source database at risk. Slot lag therefore needs production-level monitoring and alerting because a failure in the data pipeline can damage another team's critical system.

### A database pretending to be a stream

After CDC is enabled, every change to a table can be viewed as an event in a stream. This changes how downstream systems can be designed:

- A stream-processing job can maintain real-time counts or aggregates from the operational database. The application does not need to write to the database and separately publish to Kafka.
- This avoids **dual writes**. In a dual-write design, the application writes to the database and then writes an event to Kafka. If one write succeeds and the other fails, the two systems disagree. With CDC, the application writes only to the database. The CDC topic is derived from the database's transaction log.
- Serving stores can be updated from the same event history. They become materialized views of the database changes and can be rebuilt if necessary. This is one way to apply the Kappa-style idea from ch11 to operational data.

### When CDC does NOT win

CDC is powerful, but it is not automatically the best choice.

- **The source database team must be involved.** Reading a transaction log requires permissions and can affect log retention. In PostgreSQL, a replication slot can keep WAL files alive. Use managed services when appropriate, monitor the slot, and agree on the design with the database administrators.
- **Logs are not kept forever.** If the connector is stopped longer than the source's log-retention window, the missing changes are no longer available. The usual recovery is a new snapshot, which can take hours for a large table.
- **Schema changes require care.** Every database migration can affect the connector and downstream consumers. Frequent schema changes mean the CDC pipeline needs good schema compatibility rules and operational ownership.
- **Events can become large.** An update to a wide row may include a large before-and-after image. A high update rate can therefore create substantial event volume. Choose useful columns, partition topics sensibly, and measure the actual traffic.
- **Polling may be simpler for small, slow data.** If a reference table has only 50,000 rows and changes once a day, a nightly pull may be easier and cheaper than deploying Kafka and a CDC connector. The design should match the business need and the operational risk.

## The Long Tail

Not every useful source is an operational database. Three other source types appear less often in interviews, but they are common in real organizations.

### Data sharing — “the fastest pipeline is no pipeline”

Services such as Snowflake Secure Data Sharing, BigQuery dataset sharing, and Delta Sharing can let one organization give another organization controlled access to a table. The consumer reads the provider's data directly instead of receiving a copied file through an ETL pipeline.

When both organizations use a compatible platform, sharing may be better than building a data-movement system. It can avoid duplicate copies, reduce synchronization delays, and let the provider continue managing the data. The main questions become permission, governance, region, cloud, and access boundaries rather than how to copy every row.

The general lesson is simple: pipelines are needed when data must be moved to where it will be used. If access can be granted safely instead, moving the permission may be easier than moving the data.

### Manual / human uploads

Many important sources arrive as spreadsheets, survey exports, or an operations CSV sent every Friday. These sources are not technically sophisticated, but they require careful design because humans can change a file without realizing that they broke its format.

The goal is not merely to place a file in object storage. Define an upload contract that states the expected columns, data types, owner, delivery schedule, and naming rules. Validate the file as soon as it arrives. If validation fails, tell the person exactly what is wrong—for example, “column `customer_id` is missing” or “row 18 contains an invalid date.” Put the rejected file in quarantine instead of silently dropping it, and record who uploaded it and what happened. This turns an unreliable manual process into an observable source.

### Web scraping

Some sources do not provide an API. Scraping a website may be possible, but it is fragile because page structure and CSS selectors can change. It may also be restricted by terms of service, robots rules, contracts, or local law.

If scraping is necessary, treat it as a pull source. Record a reliable watermark when possible, compare snapshots to detect changes, limit request rates, and obtain a compliance review. Before scraping, ask the source owner for an export or API. That option is usually more stable and easier to govern.

## Normalization: the two-plane landing pattern

The sources in this part of the book have different formats and delivery methods, but they can share a common landing design. Each source can be normalized into two planes, introduced in ch03:

```text
 CSV drops   APIs   DB query   CDC   webhooks   logs   SaaS   shares
    |         |        |        |       |        |      |       |
    v         v        v        v       v        v      v       v
 +---------------------------------------------------------------+
 | STREAM PLANE (Kafka / Pub/Sub)                                |
 |   normalized event envelopes, keyed by entity, DLQ configured |
 +-----------------------------+---------------------------------+
                               |  (also archived on landing)
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

The **stream plane** is for consumers that need low latency. It contains a common event envelope, such as an event ID, source, entity key, event time, and payload. A dead-letter queue (DLQ) holds events that cannot be processed so that one bad event does not stop the whole stream.

The **raw plane** is an immutable copy of what arrived. “Immutable” means that normal processing does not update or overwrite old files. The raw data is stored in object storage, commonly in Avro or JSON, and is partitioned by arrival time so that it can be found and managed efficiently.

The raw plane is the long-term source of truth for the ingestion system for several reasons:

- **Replay:** We can run a transformation again for any time range by reading the raw events. We are no longer limited by the short retention period of a queue or by the memory of an API process.
- **Recovery after a bug:** If a transformation produced incorrect results for three weeks, we can fix the transformation and rerun those three weeks. We do not have to guess what happened from incomplete downstream tables.
- **Schema evolution:** New code can read old events using a newer reader schema, as discussed in ch18. The original bytes remain available.
- **Audit:** We can inspect what we actually received from the source, including records that a later validation or transformation chose not to keep.

Raw storage is inexpensive, but it is not free. Use lifecycle policies to move data from hot storage to infrequent-access and archive tiers. Delete data only through an intentional governance process. For example, GDPR erasure requirements may conflict with a raw archive's normal immutability, so that conflict must be designed and documented rather than discovered later.

## The Options

The recurring decision in this chapter is how to extract data from a system that was not designed primarily as a data source:

| Source situation | Options | Senior default |
|---|---|---|
| Operational DB, need changes | Query-pull / CDC / app dual-write | CDC; dual-write creates two possible truths |
| Operational DB, small & slow-moving | Nightly pull / CDC | Nightly pull when the business does not need real-time data |
| Same-platform consumer | Build a pipeline / data share | Share the data when permissions and governance allow it |
| Partner data, structured | SFTP drops / API / share | Use the supported method, then land it in both planes |
| Human-generated | Ad-hoc spreadsheets / upload contract | Contract, validation, quarantine, and an audit trail |
| No API at all | Scraping / requested export | Request an export first; scrape only as a last resort |

There is a second decision: where should the normalized data land?

| Landing shape | What it gives | What it costs |
|---|---|---|
| Stream plane only | Low latency and replay during queue retention | No long-term replay; retention costs can grow |
| Raw plane only | Cheap, durable, replayable source of truth | No low-latency consumers |
| Both planes | Low latency and durable replay | Two systems to operate and careful arrival partitioning |

For most production systems, both planes are the safest default. The stream serves immediate consumers, while the raw plane provides durable recovery and auditability.

## Decision Rules

- If an operational database changes frequently, deletes matter, and the data is needed repeatedly, prefer CDC.
- Treat CDC replication slots as production-critical resources on the source database. Alert on lag, agree on WAL-retention limits with the DBA team, and document how to take a new snapshot.
- Avoid application dual writes. Let the database be the source of truth and derive the event stream from its log.
- Be especially careful at the snapshot-to-incremental handoff. Use Debezium or a managed equivalent unless building that handoff is the explicit purpose of the project.
- If a consumer can safely receive shared access to data on the same platform, prefer data sharing to a custom copy pipeline.
- Give human uploads a clear contract, validation, quarantine, and a way to correct rejected files.
- Land data in a stream plane and an immutable raw plane when the system needs both fast consumers and reliable replay.

## Failure Modes

- **WAL disk-full from an idle slot:** The connector stops, PostgreSQL retains WAL for the slot, and the source disk eventually fills. The application team may experience the outage even though the original failure was in the data pipeline.
- **A gap during re-snapshot:** The connector resumes after the required log records have expired. If the new snapshot does not correctly account for transactions that happened during the handoff, the warehouse can permanently miss or misrepresent those changes.
- **CDC used as a hammer:** A 50,000-row reference table that changes nightly is put behind Debezium, Kafka, and Kafka Connect even though a simple nightly pull would meet the requirement. The operational machinery costs more than the data is worth.
- **Dual-write zombie:** An application continues publishing to an old topic while CDC publishes to a new topic. The two streams slowly develop different contents, and teams no longer know which one is correct.
- **Manual uploads without quarantine:** A malformed spreadsheet is accepted and a column becomes null downstream. Because the bad file was not quarantined or clearly reported, the problem remains unnoticed for a week.
- **Raw archive without lifecycle management:** Years of data remain in expensive storage. Later, the organization needs to delete personal data but has no clear retention policy or deletion process.

## Interview Narration

“For an operational database, I usually start with CDC rather than periodic query-pulls when I need recurring changes. CDC puts much less query load on the source, can deliver changes within seconds, and—most importantly—can capture deletes. A query cannot find a row after it has been deleted. If the warehouse must reflect those deletions, reading the transaction log is the reliable approach.

I would use Debezium or a managed CDC service to perform the initial snapshot and then continue from the correct log position. I would treat the replication slot as production-critical infrastructure because an idle connector can cause WAL to accumulate until the source database runs out of disk space. Monitoring slot lag and having a re-snapshot procedure are therefore part of the design.

CDC also avoids the dual-write problem. The application writes to the database once, and the CDC topic becomes a derived stream of database changes. Downstream serving tables can then be rebuilt as materialized views of that stream.

I would not use CDC automatically for every source. A small reference table that changes once a day may be better handled by a nightly pull. For all source types, I would normally keep both a stream copy for low-latency consumers and an immutable raw copy in object storage. That raw copy lets us audit the received data, fix a transformation, and rerun the affected time range instead of reconstructing the past from incomplete downstream results.”

---

| <- Previous | Next -> |
|---|---|
| [Chapter 05 — Push Patterns](ch05-push-patterns.md) | [Chapter 07 — Batch](ch07-batch.md) |
