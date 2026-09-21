# Chapter 05 — Push Patterns (Kafka, Pub/Sub, Webhooks, Logs)

> Part II — Ingestion Patterns (Getting Data In)

Push means **they** initiate: producers write when they want, at the volume they
want. Your system must absorb it — buffer it, survive bursts, handle redelivery —
or drop it. Push ingestion is really the engineering of *backpressure and
durability*.

## The Question

*"Data arrives at me, uncontrolled. How do I ingest pushed events without losing
them, without lying about ordering, and without drowning the source?"*

## The Physics

### Kafka internals — the reference model

Kafka is the canonical push substrate; most other systems (Pub/Sub, Kinesis,
Event Hubs) are variations on its ideas. Learn the model once, translate forever.

```
TOPIC: orders  (partition count fixed at creation)
 p0: [m0 m1 m2 m3 m4 m5 ...]   <- append-only logs, on disk
 p1: [m0 m1 m2 m3 ...]
 p2: [m0 m1 m2 m3 m4 ...]

producer chooses partition from KEY hash   consumer group:
 e.g. order-42 -> p1                          instance A reads p0,p1
 (same key -> same partition, forever)        instance B reads p2
```

**The guarantees, precisely:**

| Guarantee | Rule | Consequence |
|---|---|---|
| Ordering | within a partition only | need order per entity? key by entity id. Global order = one partition = no parallelism — refuse it |
| Delivery to consumer | at-least-once by default | consumer must be idempotent, or... |
| Durability | acks=all + min.insync.replicas=2 | message accepted only after in-sync replicas — the no-data-loss setting |
| Retention | time/size-based window (default ~7 days) | replay possible *within retention* — this is what makes Kappa possible (ch11) |

**Producer side, briefly:** `acks=0` (fire and forget, loss possible), `acks=1`
(leader only), `acks=all` (in-sync replicas — the only safe default). Enable
idempotent producers (`enable.idempotence=true`) to prevent retry-induced
duplicates within a session. Kafka's transactional producer + `read_committed`
consumers give you exactly-once *within the Kafka ecosystem* — processing
exactly-once end-to-end needs more (ch08).

**Consumer groups and rebalances:** each partition is owned by exactly one
consumer in a group. Add consumers -> rebalance; consumers die -> rebalance.
Rebalances are the operational tax: during one, processing pauses; poorly behaved
consumers (long poll loops, slow commits) trigger them in a loop. Interview
signal: know that *scaling consumers past the partition count buys nothing* —
three consumers on a two-partition topic means one sits idle.

**Compacted topics**: keep the *latest* value per key, not a window of history —
a changelog, not a stream. This is how Kafka Streams materializes state, and how
you rebuild a serving table from a topic (ch11).

### GCP Pub/Sub — the same model, different words

| Kafka concept | Pub/Sub concept | Notes |
|---|---|---|
| topic | topic | same idea |
| consumer group | subscription | decoupled: N subscriptions independently read the same topic |
| partition | — | no user-visible partitions (except via ordering keys) |
| offset commit | ack | ack deadline: must ack in time or redelivery |
| compaction | — | no direct equivalent |

The distinguishing mechanics: **push vs pull subscriptions** (server pushes to
your HTTP endpoint, or you pull), **ack deadlines with redelivery** (extend for
slow processing or you will process everything twice — at-least-once is the
contract), **ordering keys** (opt-in ordering, mirroring Kafka's key-partition
trick, with throughput trade-offs), **dead-letter topics** (after N delivery
attempts, route to a DLT — build this from day one, not after the first poison
message), and **exactly-once delivery support** (dedup on the service side —
still not processing exactly-once; your write must still be idempotent or
transactional).

The conceptual bridge: Pub/Sub subscriptions decoupling is genuinely useful — one
topic, three teams, three independent subscriptions, no coordination. In Kafka
the same effect needs three consumer groups (fine too, different ops surface).

### Webhooks — the fire-and-forget trap

Stripe, GitHub, Shopify call *you*. The contract is brutal: **if your endpoint
was down, the event may be gone** (retries exist but are finite and vendor-specific).

The correct pattern, without exception:

```
vendor --HTTP--> [receiver: validate + persist to queue/storage] --> 200 OK
                        |
                        v  (later, at your own pace)
                 process from queue, idempotently (event id dedup)
```

Rules:
- **Never do real work in the handler.** Persist, 200, done. Sub-second response.
- **Dedup by event id** — webhook delivery is at-least-once; retries overlap with
  your processing.
- **Verify signatures** — anyone who finds the URL can POST to it.
- The receiver writing to durable storage *is* your replayability — the vendor
  will not give you a second chance (some offer event APIs for backfill; never
  design as if they do).

### Logs / clickstream — high volume, low trust

Application logs and product events, shipped by agents (Vector, Fluentd, Filebeat)
or embedded SDKs (Segment, Snowplow):

- **Semi-structured**: JSON-ish, but field presence is aspirational. Validation
  must happen on your side (schema-on-read at the raw layer, enforced later).
- **Lossy by convention**: agents buffer, then drop on overflow; mobile SDKs lose
  events offline. The business *accepts* approximate counts here — a 1% click
  deficit changes no decision. Say this out loud in interviews; knowing *where*
  approximate is acceptable is a senior mark.
- **Out-of-order**: retries, offline flushes, clock skew. Event-time thinking
  (ch08) is not optional.
- **Volume is the defining cost**: batch, compress (ch18), and aggregate early;
  raw clickstream at 50k events/sec is a storage bill, not a dashboard.

### Push vs pull — the buffering implication

Pull: you throttle yourself; a stalled downstream simply stops asking. Push: the
source does not care about your state — the buffer absorbs the mismatch, or the
data drops. Every push architecture is therefore a **buffer sizing and retention**
decision. The queue is not a detail; it is the load-bearing wall.

## The Options

| Substrate | Sweet spot | Watch out |
|---|---|---|
| Kafka | high throughput, replay window, stream processing ecosystem | partition-count ops; ZooKeeper/KRaft history; retention cost |
| Pub/Sub | GCP ecosystems, many independent consumers, serverless | ack deadline tuning; no compaction |
| Kinesis / Event Hubs | AWS / Azure gravity | per-shard limits; similar model, regional dialects |
| Webhook receiver -> queue | SaaS event sources | finite retries; signature verification |
| Agent-shipped logs | logs/clickstream | lossy; validate early; volume cost |

## Decision Rules

- **Key by entity whenever per-entity order matters**; accept per-partition
  (not global) ordering as the physical law.
- **acks=all + idempotent producer** as the default producer posture.
- **Consumers are idempotent by design** — assume duplicates, dedup by event id
  or business key.
- **Provision a dead-letter path on day one** — poison messages are a when, not
  an if.
- **Webhook handler = validate, persist, 200. Nothing else. Ever.**
- **Set retention to cover your realistic outage window** (consumer down for 2
  days? retention > 2 days, or raw-archive re-ingest is your recovery).
- **Don't force batch sources through a stream** — a nightly CSV does not become
  better engineering because it passed through Kafka.

## Failure Modes

- **Global ordering requirement** — "all events in exact order" collapses to one
  partition, one consumer, no parallelism. Symptom: throughput ceiling exactly at
  one broker's write rate. Fix the requirement (per-entity order), not the topic.
- **Consumer lag spiral**: processing slower than production, lag grows, retention
  expires oldest, *silent data loss*. Symptom: dashboards fine, reconciliation
  broken. Alert on lag *and* on retention headroom.
- **Ack-deadline misses** (Pub/Sub): every message processed twice, exactly-once
  marketing notwithstanding. Symptom: duplicate side effects downstream.
- **Webhook handler "doing a little work"**: one slow downstream call inside the
  handler and Stripe retries fire, your endpoint 500s under load, and the retry
  storm loses events.
- **Poison message without a DLT**: one malformed event fails, gets redelivered,
  fails — the partition is effectively wedged.
- **Trust in the schema that isn't there**: log field renamed upstream; nulls
  flow silently into aggregates for weeks.

## Interview Narration

"Push ingestion is really a backpressure problem: producers don't care about my
state, so something between us must absorb the mismatch — that's the queue, and
its retention is an explicit correctness decision, not a config default.

On Kafka specifically: ordering is per-partition, so I key by entity — order id —
which gives me per-order ordering with full parallelism, and I explicitly refuse
global ordering because it means one partition and no scale. Delivery to consumers
is at-least-once, so my consumers are idempotent by construction, deduping by
event id. Producers run acks=all with idempotence on. And I size retention against
my worst realistic consumer outage — if the consumer can be down two days,
retention better be longer, or I need raw-archive re-ingest as the recovery path.

Webhooks get the strictest pattern: validate, persist, return 200 — the handler
never does real work, because the vendor's retries are finite and if I miss the
event it's gone. Processing happens later, from durable storage, idempotently.

And for logs and clickstream I set expectations up front: this data is lossy by
convention and out of order by nature — the business accepts approximate counts,
so I spend my rigor on event-time processing and early validation, not on
pretending every click is sacred."

---

| <- Previous | Next -> |
|---|---|
| [Chapter 04 — Pull Patterns](ch04-pull-patterns.md) | [Chapter 06 — CDC, the Long Tail & Normalization](ch06-cdc-long-tail-and-normalization.md) |