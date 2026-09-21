# Chapter 24 — Worked Cases

> Part VIII — Interview Execution

Four end-to-end cases, each walked through the full decision cascade:
requirements → decisions with trade-offs → architecture → failure modes →
what to say. Each case cites the chapters whose decisions it applies — this
is the book, integrated.

---

## The Question

*"A real prompt just landed: design fraud detection for payments, build the
analytics platform, feed an ML ranking model, ingest a million IoT devices.
How does the entire book assemble into one 45-minute performance?"*

## The Physics

What makes integration harder than any single chapter: **decisions
interact.** The fraud case's late-arriving labels force a two-product
architecture; the dashboard case's cost profile defends batch against
fashion; the feature case's consistency requirement creates a governance
layer; the IoT case's volume makes the cost model itself the architecture.
The cascade (ch01) is linear to walk but non-linear in effect - exactly
what these cases exist to show. In each case the chapter skeleton applies
internally: the prompt is the Question, the requirements are the Physics
inputs, the cascade walk is the Options and Decision Rules, failure modes
are volunteered per component, and a 60-second narration closes it out.

## The Options

Four cases, four different stresses on the same cascade:

| Case | Cascade level it stresses most | The hard requirement |
|---|---|---|
| Fraud (payments) | streaming correctness + serving (ch08, ch16) | 500ms in-flight decision; labels arrive days later |
| Dashboards (e-commerce) | batch + lakehouse + serving cost (ch07, ch12, ch16) | finance-grade exactness at hours of latency |
| ML features | hybrid paradigm + governance (ch08-09, ch12) | training/serving consistency; point-in-time correctness |
| IoT telemetry | ingestion scale + cost tiers (ch05, ch18-19) | 100k events/sec; unbounded storage growth |

## Case 1 — Fraud Detection at a Payments Company

**Prompt:** "Design the data system that flags potentially fraudulent
card transactions."

### Requirements (ch02)

- Latency: decision *while the payment is in-flight* — the auth call
  waits ~200-500ms for our score. That number is the whole design.
- Correctness: a fraud decision may be reversible (hold-and-review), so
  effectively-once processing suffices; but the *label* data (was it really
  fraud?) arrives days later — batch truth, streaming action.
- Volume: 50k transactions/sec peak (payments scale), growth 2x/2yr.
- PII: cardholder data — PCI scope exists.

### The cascade

1. **Paradigm (ch07-09)**: 500ms in-flight decision = true streaming. Batch
   physically cannot participate in the decision path. But labels, model
   training, and review analytics are batch — this is the "two products"
   shape (ch10): a streaming decision path *and* a batch truth path.
2. **Architecture (ch10-12)**: honest Lambda-*shaped* answer — not because
   reprocessing is expensive (ch12 made it cheap) but because the fast
   artifact (in-flight score) and the exact artifact (labeled history,
   trained models) are *different products with different SLAs*. Streaming
   upserts into a lakehouse table unify the analytics side.
3. **Ingestion (ch03-06)**: transactions arrive as events on a queue
   (Kafka/Pub/Sub); the OLTP payments DB is CDC'd (ch06) so state changes
   (chargebacks = labels!) flow as events. Raw plane archives everything
   (ch03) — model retraining and audit depend on it.
4. **Processing (ch08)**: Flink — sub-second, event-time windows
   (velocity features: "5 transactions in 60s per card"), keyed state per
   card (RocksDB-backed), watermarks from observed lateness. Effectively-once
   via idempotent sink writes.
5. **Serving (ch16, ch14)**: features and scores to a KV store (Redis/
   DynamoDB) for ms reads by the decision API; the decision itself is a
   model + rules call. Analysts query the lakehouse/warehouse.
6. **Quality/ops (ch21)**: feature freshness SLOs; volume anomaly gates
   (a fraud-model silent failure is catastrophic — monitor the *score
   distribution*, not just jobs).

### Architecture sketch

```
card txn events --> Kafka --+--> Flink (velocity features, rules)
                            |        |
payments DB --CDC (ch06)--> +        +--> KV serving (scores/features) <-- decision API
                            |                       ^
                            v                       |
                     raw plane (S3) --> batch labels/chargebacks --> model training
                            |                                       |
                            +--> lakehouse table (ch12) <-----------+   (features + labels)
                                        |
                                        v
                              BI / fraud analytics (ch16)
```

### Failure modes to volunteer

- Consumer lag past retention (ch05) = silent missing transactions = missed
  fraud: alert on lag-vs-retention headroom.
- Training/serving skew (below, Case 3's disease): the batch-trained model
  sees features computed differently than the streaming path computes them.
  The feature-store discipline is the cure.
- Poison message wedges a partition: DLQ from day one (ch05).

### The 60-second close

"Sub-second in-flight decision forces streaming; but fraud's ground truth
is labels that arrive days later — so this is honestly two products: a
Flink decision path with event-time velocity features over keyed state,
serving ms reads from a KV store; and a batch truth path — CDC'd
chargebacks and raw-archived events — that trains models and reconciles.
The lakehouse table unifies the analytics side of both. The failure modes
I'd name: lag-past-retention as silent loss, and training/serving feature
skew — both have named mitigations."

---

## Case 2 — Executive & Analytics Dashboards for E-commerce

**Prompt:** "Design the analytics platform for a mid-size e-commerce
company: exec dashboards, analyst SQL, marketing reports."

### Requirements (ch02)

- Latency: hourly refresh is *fine* for execs; analysts tolerate daily.
  Nobody said real-time — don't invent it.
- Correctness: exact — finance-grade numbers (this is reporting, not
  alerting).
- Volume: 20GB/day of events, 50 or so SaaS/DB sources.
- Consumers: BI tools + SQL analysts; ~40 dashboards.

### The cascade

1. **Paradigm (ch07)**: batch. Latency budget is minutes-to-hours;
   correctness demands recompute-on-bug. The cheapest correct answer — say
   that sentence.
2. **Ingestion (ch03-06)**: mixed sources — CDC for the OLTP databases
   (orders, users; deletes matter to finance), SaaS connectors for Shopify/
   Ads (buy, ch04), event tracking for clickstream landing in the raw plane.
   Two planes (ch03) even in batch-world: raw first, always.
3. **Storage (ch12-13)**: lakehouse tables (Iceberg/Delta) as the
   open-format core; warehouse serving for BI (ch16). ELT: land, then
   transform with SQL (dbt-style).
4. **Serving (ch16)**: warehouse-direct as default — compute-isolated BI
   warehouse (connection-string-level workload management), result caching,
   connection pooling; marts only when scan-cost x frequency x concurrency
   says no. Semantic layer for "revenue."
5. **Quality/ops (ch21)**: dbt tests + volume anomaly gates + freshness
   SLOs ("exec dashboard current through yesterday by 6:30am") with the
   circuit breaker: yesterday's right data beats today's wrong data.
6. **The "real-time dashboard" follow-up**: when someone asks for
   real-time, the answer is micro-batch (ch09) — Structured Streaming
   committing hourly→minute-level upserts to the same lakehouse table.
   One codebase, one table, no new platform.

### Architecture sketch

```
OLTP DBs --CDC-->  +--------------------------------+
SaaS (buy, ch04)-> | raw plane (S3, as-landed)      | --lineage/contracts (ch20)
clickstream ------>|                                |
                   +------> lakehouse tables (Iceberg/Delta; dbt transforms)
                                        |
                                        v
                     warehouse serving layer (BI_WH compute isolated, ch13/16)
                                        |
                     BI (JDBC/ODBC, pooled, SSO) / semantic layer / analysts
```

### Failure modes to volunteer

- Morning queue storm (ch16): 40 dashboards x 8am refresh — pooling,
  caching, staggered schedules, separate BI compute.
- Silent half-load (ch07/21): completeness checks before compute.
- Cost creep: scan-priced dashboards + `SELECT *` — column pruning,
  partition pruning, marts for the hot few.

### The 60-second close

"Latency budget is hours and correctness is finance-grade — that's batch,
and I'll defend it as the cheapest correct answer. CDC for databases,
bought connectors for SaaS, raw plane for everything. Iceberg tables with
SQL transforms; warehouse serving with the BI fleet on its own compute,
pooled and cached. Quality gates with a circuit breaker, freshness SLOs
per consumer. And when 'real-time' arrives — and it will — the upgrade
path is micro-batch upserts into the same tables, not a new platform."

---

## Case 3 — ML Feature Pipeline

**Prompt:** "Design the feature pipeline that feeds both model training and
a low-latency ranking model."

### Requirements (ch02)

- Training: batch — full history, point-in-time correct labels.
- Serving: features for online inference within ~10ms of request.
- The killer requirement, usually unstated: **training/serving
  consistency** — the model must be trained on features computed *exactly*
  the way serving computes them, and historically (no leakage).

### The cascade

1. **Paradigm (ch07-09)**: hybrid by construction — streaming features
   (velocity: "user's clicks in last 10 min") need event-time windows on
   live events (ch08); batch features (demographics, 30-day aggregates)
   are lakehouse batch jobs (ch07). No single paradigm survives.
2. **Architecture (ch12)**: the lakehouse is the meeting point — batch
   features as tables; streaming features upserted by Flink/micro-batch
   into the same table format for training reads.
3. **Serving (ch14/16)**: KV store (DynamoDB/Redis) for online features —
   the *feature store* concept: one definition, two materializations
   (offline table + online KV), synced by pipeline.
4. **The correctness core — point-in-time correctness**: training rows must
   carry features *as they existed at event time*, not as they exist now.
   This means: event-time joins (ch08 semantics), and the offline store
   must be queryable "as of" (table formats' time travel, ch12, helps).
   Feature leakage is the silent model killer — volunteer it.
5. **Training/serving skew**: the two materializations drift (streaming
   computes "10-min clicks" slightly differently than the batch backfill
   does). Mitigations: shared transformation definitions (one codebase —
   ch09's argument), skew monitoring (compare online/offline feature
   distributions for the same entity/time).

### Architecture sketch

```
events --> Kafka --+--> Flink: streaming features --upsert--> KV store (online)
                   |                                    |
                   v                                    |
raw plane --> batch: batch features ----+--> lakehouse feature tables (offline)
                                        |         ^  (point-in-time, time travel)
                                        +--> training jobs (labels joined as-of)
skew monitor: compare online vs offline for same entity/time (ch21 thinking)
```

### Failure modes to volunteer

- **Feature leakage** via non-as-of joins — the model looks great offline
  and is useless live. Point-in-time correctness is the fix; name it
  before asked.
- **Skew drift**: online/offline definitions diverge over releases —
  monitor distributions, version features.
- **KV cold-start/rebuild** (ch11/15): the online store is a projection —
  it must be rebuildable from the offline truth.

### The 60-second close

"Features are two workloads wearing one name: streaming features for
recency, batch features for depth — Flink upserting the online KV, batch
jobs writing lakehouse tables, one transformation definition shared
between them. The two correctness requirements that make or break this:
point-in-time correctness in training — as-of joins, or the model leaks —
and training/serving consistency — monitored, because definitions drift.
The feature store is really a governance pattern: one definition, two
materializations, both rebuildable."

---

## Case 4 — IoT Telemetry Platform

**Prompt:** "Design ingestion and analytics for 1M devices emitting
telemetry every 10 seconds."

### Requirements (ch02)

- Volume: 100k events/sec sustained (1M devices / 10s) — the headline
  number. Growth: devices double yearly.
- Latency: alerting on device anomalies within ~1 min; dashboards hourly;
  most queries are time-windowed per device/fleet.
- Correctness: telemetry is logs-class (ch05) — lossy is *acceptable by
  convention*; duplicates are common (device retries).
- Access pattern: recent-data queries dominate; 95% of reads touch last
  24-48h (the classic time-series shape).

### The cascade

1. **Ingestion (ch05)**: agent/embedded publishers -> Kafka/Pub/Sub; the
   buffer sizing *is* the architecture — devices burst on reconnect, and
   100k/sec sustained means provisioning for 3-5x spikes. Dedup by
   (device_id, event_time) downstream — at-least-once everywhere,
   idempotent writes (ch08 discipline).
2. **Paradigm (ch07-09)**: tiered by need — streaming only for the
   anomaly-alert path (1-min SLA justifies it); everything else batch/micro
   -batch. Don't stream the dashboard path.
3. **Storage (ch13-14, 12)**: time-series shape — downsampling tiers:
   raw (S3, cheap, 30-90d) → hourly/daily rollups (lakehouse tables,
   forever). The wide-column/KV option (ch14) for *latest-state per
   device* ("device dashboard"), the lakehouse for history.
4. **Format (ch17-18)**: Parquet partitioned by time (ch19 sizing:
   128MB-1GB files, compaction jobs — at 100k/sec the small-files disease
   arrives fast without them).
5. **Cost control (ch02/21)**: this case is a cost case — storage growth
   is unbounded; lifecycle tiering + downsampling + retention policy are
   *requirements*, not optimizations. Chargeback per product team.

### Architecture sketch

```
1M devices --> edge/gateway (batch, compress) --> Kafka (3-5x burst headroom)
                                                   |
                     +-----------------------------+--------------------------+
                     v                             v                          v
              Flink: anomaly rules          micro-batch rollups         raw -> S3
              (1-min SLA, keyed             (hourly aggregates           (30-90d,
               by device, ch08)              into lakehouse, ch09)        lifecycle)
                     |                             |                          |
                     v                             v                          v
               alerting/KV              BI dashboards (ch16)      archive tier /
                                                                  downsampling
```

### Failure modes to volunteer

- **Reconnect storms**: fleet firmware update → 1M devices reconnect and
  replay buffers → 10x spike. The queue headroom + backpressure story
  (ch05) is the answer; say "reconnect storm" unprompted.
- **Hot partitions** (ch05/14): device_id keying is uniform, but a single
  noisy device (broken sensor, retry loop) can hot-key a partition —
  per-device quotas in the gateway.
- **The unbounded bill**: no lifecycle policy → raw telemetry forever →
  the cost review in year two. Downsampling tiers stated up front.

### The 60-second close

"A hundred thousand events a second, loss-tolerant, time-windowed reads —
so: Kafka sized for reconnect-storm spikes, dedup downstream because
devices retry, and a tiered design: streaming only for the one-minute
anomaly path; micro-batch rollups into lakehouse tables for everything
else; raw to S3 with a lifecycle policy and downsampling tiers, because at
this volume the cost model *is* the architecture. And I'd provision for
the reconnect storm — a firmware update is a self-inflicted DDoS, and the
queue headroom is what turns that from an outage into a busy morning."

---

## Decision Rules

The pattern across all four cases

| Case | Paradigm | Architecture | The one senior insight |
|---|---|---|---|
| Fraud | streaming decision + batch truth | Lambda-shaped (two products) | labels arrive late — separate the decision from the truth |
| Dashboards | batch | lakehouse + warehouse serving | cheapest correct answer; micro-batch is the upgrade path |
| Features | hybrid by construction | lakehouse + KV, feature-store governance | point-in-time correctness and skew are the real problems |
| IoT | streaming alert + batch rollups | tiered downsampling | cost model is the architecture at unbounded scale |

Every case: requirements first (ch02), paradigm forced by latency (ch07-09),
architecture forced by correctness (ch10-12), storage by access pattern
(ch13-16), formats by physics (ch17-19), and the -ilities volunteered
(ch20-22). The cascade (ch01), executed.

## Failure Modes

Each case has one defining failure mode - the one to volunteer unprompted:

| Case | The defining failure | The named mitigation |
|---|---|---|
| Fraud | consumer lag past retention = silently missed fraud | lag-vs-retention alerting (ch05) |
| Dashboards | morning queue storm; silent half-load | pooling/caching/stagger (ch16); completeness gates (ch21) |
| Features | feature leakage + training/serving skew | point-in-time joins; skew monitoring |
| IoT | reconnect storm + unbounded storage bill | queue headroom; lifecycle tiers + downsampling |

## Interview Narration

These cases are for your voice, not your eyes. The final preparation move:
pick a prompt, set a 45-minute timer, and narrate the whole cascade out
loud - requirements, decisions with trade-offs, failure modes, close - then
compare what you said against the case above. The gap between the two is
your study list. Repeat until the four-beat decision pattern (options,
trade-offs, call, what-would-change-it - ch23) is reflex. The last line of
this book is also its first instruction: go practice it out loud.

---

| <- Previous | Next -> |
|---|---|
| [Chapter 23 — Running the Room](ch23-running-the-room.md) | [README](README.md) |
