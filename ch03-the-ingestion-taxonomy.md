# Chapter 03 — The Ingestion Taxonomy

> Part II — Ingestion Patterns (Getting Data In)

Before you decide *how* to move data, you must see the full landscape of *what kind
of thing is handing you the data*. Candidates routinely name three sources (CSV,
API, database) and miss that the taxonomy has ten entries — and more importantly,
that the technology names matter far less than five cross-cutting properties.

## The Question

*"What kinds of sources exist, and which of their properties — not their brand
names — determine my ingestion design?"*

## The Physics

### The full source landscape

| # | Source | Example | Mechanism |
|---|---|---|---|
| 1 | Files / drops | CSV/JSON exports, S3/GCS drops, partner SFTP | batch files landing in object storage |
| 2 | APIs | internal REST/GraphQL, third-party HTTP | you poll, they respond |
| 3 | DB query-pull | Postgres/MySQL read replica | you SELECT with a watermark |
| 4 | CDC | WAL, binlog, DynamoDB streams via Debezium | you read their transaction log |
| 5 | Message topics | Kafka, GCP Pub/Sub, Kinesis | producers publish, you subscribe |
| 6 | Webhooks | Stripe, GitHub, Shopify | they call you, HTTP push |
| 7 | Logs / clickstream | app logs, product analytics events | agents (Vector/Fluentd/Filebeat) ship events |
| 8 | SaaS connectors | Salesforce, Google Ads, HubSpot | managed incremental sync (Fivetran/Airbyte) |
| 9 | Data sharing | Snowflake Secure Shares, BQ datasets, Delta Sharing | zero-copy cross-warehouse mounts |
| 10 | Manual / scraping | business spreadsheets; pages with no API | humans or scrapers |

### The five properties that actually drive design

The senior insight: classify sources by properties, not by technology. Two sources
with the same properties are the *same ingestion problem* regardless of vendor.

| Property | Question it asks | Why it matters downstream |
|---|---|---|
| **Pull vs push** | Who initiates transfer? | Pull: you control pacing, backoff, retries. Push: you must buffer or you drop data — backpressure is your problem |
| **Replayability** | Can I re-read last week's data? | Replayable (Kafka log, raw archive) enables Kappa and backfill. Non-replayable (webhooks) forces persist-first architectures |
| **Incrementality** | Snapshot, change events, or append-only? | Determines merge logic: overwrite, upsert, or append — and idempotency requirements |
| **Schema contract** | Enforced, versioned, or chaos? | DB schemas are enforced upstream; APIs drift across versions; logs lie. Drives validation and schema-evolution strategy (ch18) |
| **Load on source** | What does my reading cost them? | Querying a production OLTP DB is how you cause an incident. CDC and replicas exist for a reason |

### Property profiles of the classic sources

| Source | Pull/Push | Replayable | Incrementality | Schema | Source load |
|---|---|---|---|---|---|
| File drop | pull | yes (file persists) | snapshot per file | none — chaos | none |
| REST API | pull | no (state changes) | cursor/watermark | versioned, drifting | rate-limited, theirs |
| DB query-pull | pull | no | watermark query | enforced | heavy on source |
| CDC | pull (log tail) | bounded by log retention | change events | enforced, but changes surprise you | minimal |
| Kafka topic | push (to consumer) | yes — retention window | append-only stream | registry-enforced | none |
| Webhook | push | **no** | change events | per-vendor docs | none |
| Logs | push | partial (agent buffers) | append-only | chaos | none |
| SaaS connector | pull (managed) | vendor-dependent | managed cursor | managed drift | managed |
| Data share | neither (mount) | yes | snapshot | enforced | none — zero-copy |
| Manual upload | pull | yes | snapshot | chaos | none |

Read that table twice. Notice that two columns — **replayability** and
**incrementality** — decide most of your architecture: replayable + append-only
(Kafka) is the Kappa-native shape; snapshot-only forces you to build diffing;
non-replayable (webhooks) forces persist-before-process.

### Patterns compose

The ten sources are not fixed categories — they convert into each other:

- **CDC converts DB -> stream**: the transaction log *is* an append-only event
  stream. This is why CDC is the bridge between "database source" and "streaming
  architecture" (ch06).
- **S3 event notifications convert file -> trigger**: a file landing fires a
  Lambda/function that enqueues it — a batch source pushing into a stream.
- **Polling + diff converts API -> change events**: snapshot today, snapshot
  yesterday, diff = changes. You synthesize incrementality that the API never
  offered you.
- **Anything -> object storage**: every source can be archived as raw immutable
  events. This is not optional hygiene; it is your replay insurance.

### The senior normalization: two planes

Once you see the properties, the ingestion design converges: **normalize every
source into two planes** at the earliest possible moment.

```
   files   APIs   DBs   CDC   webhooks   logs   SaaS ...
     |      |     |     |      |         |      |
     v      v     v     v      v         v      v
   +--------------------------------------------------+
   |  STREAM PLANE  (Kafka / Pub/Sub)                 |  <- low-latency consumers,
   +--------------------------------------------------+     replay within retention
                        |
                        v
   +--------------------------------------------------+
   |  RAW PLANE  (object storage, immutable,          |  <- replay/backfill forever,
   |   Avro/JSON as-landed, "bronze")                 |     reprocessible history
   +--------------------------------------------------+
```

- The **stream plane** serves low-latency consumers (alerting, features) and
  gives you a buffer that absorbs downstream outages.
- The **raw plane** is your source of truth: immutable, cheap, and replayable
  far beyond the stream's retention. Every downstream table is a *derivable
  view* of it — which is exactly what makes reprocessing and Kappa-style rebuilds
  possible (ch11).

Sources that cannot replay (webhooks, APIs) must be persisted to the raw plane
*first* — before any processing — because that is your only chance to capture the
event.

### The event envelope — design it once, benefit forever

Whatever crosses the two-plane boundary should carry a standard envelope. The fields that earn their keep:

| Field | Purpose | Why it matters downstream |
|---|---|---|
| `event_id` (uuid) | global identity | dedup under at-least-once delivery (ch05, ch08) |
| `event_type` + `event_version` | routing + evolution | stream routing; schema resolution (ch18) |
| `event_time` (producer clock) | when it happened | event-time processing (ch08); the watermark's raw material |
| `ingested_time` (platform clock) | when we saw it | lag SLOs; the event-vs-processing gap made measurable |
| `producer` / `source` | lineage | tracing; contract enforcement (ch20) |
| `partition_key` (entity id) | per-entity ordering | partition assignment (ch05) |
| `payload` | the business event | Avro/Protobuf, registry-governed (ch18) |

Two timestamps (`event_time` + `ingested_time`) are non-negotiable: their difference *is* ingestion lag — an SLI you can measure, alert, and SLO (ch02, ch22). Without the pair, "is the pipeline slow?" is a vibe; with it, it's a chart. A minimal JSON sketch, then the Avro version at the registry:

```
{ "event_id": "b3d1...", "event_type": "order.created", "event_version": 2,
  "event_time": "2026-09-21T14:59:03.117Z", "ingested_time": "2026-09-21T14:59:04.002Z",
  "producer": "checkout-svc", "partition_key": "order-84213",
  "payload": { ... } }
```
## The Options

Choosing "how to ingest" is really choosing where each source lands and how it is
normalized:

| Strategy | When it fits | Risk |
|---|---|---|
| Direct-to-warehouse (source -> warehouse) | tiny scale, few sources, all analytics | no replay, no buffer, vendor lock on logic |
| Everything-to-Kafka first | event-heavy org, many consumers | Kafka becomes a snowflake-shaped bottleneck; retention cost |
| Everything-to-datalake first | batch-dominant, BI-heavy org | latency floor = file arrival; streaming consumers suffer |
| Two-plane (stream + raw) | the senior default for anything non-trivial | two systems to operate; needs discipline |

## Decision Rules

- **Classify the source by its five properties before naming any connector.**
- **Non-replayable source -> persist to raw immediately.** Webhook handlers write
  to storage/queue and return 200 — nothing else.
- **Replayable + low-latency need -> stream plane. Analytics + backfill need ->
  raw plane. Most real systems need both.**
- **Snapshot-only source (API, files) -> either accept overwrite semantics or
  synthesize change events via diffing.**
- **Never read a production OLTP database directly for analytics** — use CDC or a
  replica (ch04, ch06).
- **When two sources share a property profile, they share an ingestion design.**
  Stop designing twice.
- **The raw plane is not a backup.** It is the architectural enabler for
  reprocessing, audits, and schema-migration replays.

## Failure Modes

- **Webhook handler that does real work.** Endpoint down for 10 minutes = events
  gone forever. Symptom: "we missed some Stripe events around 2am" — unrecoverable.
- **No raw archive.** A transformation bug corrupts aggregates; you cannot recompute
  because the source API only exposes current state. The data is *structurally*
  unrecoverable.
- **Hammering prod.** A marketing analytics job full-scanning the orders table at
  9am Monday. Symptom: page load latency alerts from the app team, not your
  pipeline.
- **Kafka as religion.** Force-feeding nightly CSV drops through a topic to be
  "event-driven" — paying streaming costs for batch semantics.
- **Trusting schema chaos.** Ingesting logs with no validation; a producer renames
  a field; dashboards silently go null for two weeks before anyone notices.
- **Reinventing connectors.** Writing your 14th bespoke Salesforce sync instead of
  Fivetran/Airbyte — build-vs-build-again is a real senior discussion (ch04).

## Interview Narration

"I start ingestion by classifying sources, but not by technology — by five
properties: is it pull or push, is it replayable, what is its incrementality, how
strong is its schema contract, and what load does reading place on the source.
Those five answers determine the design; the vendor names are interchangeable.

So: a Postgres orders table and a Kafka orders topic are *different problems* —
one is a pull with watermark risk, the other an append-only replayable stream. But
a Postgres CDC feed and that Kafka topic are *the same problem* — append-only
change events — which is why CDC is the standard way to feed databases into a
streaming architecture.

My default shape is two planes: everything lands in a queue for low-latency
consumers, and everything lands in immutable raw object storage as the replayable
source of truth. Sources that cannot replay — webhooks especially — get persisted
to raw *before* anything else touches them, because that first write is the only
chance to capture the event. Downstream of those two planes, every table is
derivable, which is what makes backfills and rebuilds cheap instead of
impossible."

---

| <- Previous | Next -> |
|---|---|
| [Chapter 02 — The Requirements That Drive Everything](ch02-requirements-that-drive-everything.md) | [Chapter 04 — Pull Patterns](ch04-pull-patterns.md) |
