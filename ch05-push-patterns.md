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
Read Kafta internals - [Kafka Internals](ch05.1-kafka-pubsub.md)

**The guarantees, precisely:**

| Guarantee | Rule | Consequence |
|---|---|---|
| Ordering | within a partition only | **need order per entity?** key by entity id. So Order by customer id or order id: use that as the key. Same key -> same partition. Same partition -> events are processed in order. Different keys -> different partitions -> they can be processed concurrently. That gives you: per-entity ordering, parallelism across entities. **Global order = one partition = no parallelism -- refuse it.** If all events must be strictly ordered globally, then all writes must land in the same partition. That creates a bottleneck: one partition can only be written/read serially, you lose horizontal scaling, throughput collapses to the speed of a single partition/broker path |
| Delivery to consumer | at-least-once by default | consumer must be idempotent, or... |
| Durability | acks=all + min.insync.replicas=2 | message accepted only after in-sync replicas — the no-data-loss setting. **acks** =all tells the Kafka producer: “consider the write successful only when all currently in-sync replicas have stored the record.” **min.insync.replicas=2** tells Kafka: “there must be at least two in-sync replicas available; otherwise reject the write.” |
| Retention | time/size-based window (default ~7 days) | replay possible *within retention* — this is what makes Kappa possible (ch11) |

**Producer side, briefly:** `acks=0` (fire and forget, loss possible), `acks=1`
(leader only), `acks=all` (in-sync replicas — the only safe default). Enable
idempotent producers (`enable.idempotence=true` _Kafka assigns producer sequence numbers so retries of the same send are deduplicated, preventing duplicate records caused by network errors or timeouts._ ) to prevent retry-induced
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

Application logs: `User login failed`. Clickstream events: `User clicked the Buy button`. Product events: `User added an item to the cart`. These events are usually sent automatically by tools such as Vector, Fluentd, Filebeat, Segment, or Snowplow. This data arrives quickly and in huge amounts, but it is not perfectly reliable or consistent.

#### 1. Semi-structured data

The events often look like JSON:

```json
{
  "user_id": 123,
  "event": "button_click",
  "page": "/checkout"
}
```

But different applications or versions may send different fields:

```json
{
  "event": "button_click"
}
```

Or they may use the wrong data type:

```json
{
  "user_id": "unknown"
}
```

Fields are supposed to be present, but you cannot trust that they always will be.

Therefore, the receiving data system must validate the data. Usually:

1. Store the raw event first, even if it is imperfect.
2. Inspect and validate it later.
3. Move valid data into cleaned, structured tables.
4. Quarantine or fix invalid records.

This is called **schema-on-read** : apply the schema when reading or processing the data, rather than trusting the producer completely.

#### 2. Some data loss is acceptable

Logs and click events are often `lossy` , meaning some events may never arrive.

For example:

- An agent may temporarily store events in memory.
- If its buffer becomes full, it may drop older events.
- A mobile phone may be offline and fail to upload events.
- A retry may fail permanently.

If a website records 99,000 clicks instead of the actual 100,000, the business may still make the same decision. A 1% difference might not matter for a marketing dashboard.

This is different from payments or bank transfers, where losing even one event could be serious.

The important design question is:

> Can the business tolerate approximate results for this type of data?

Recognizing that trade-off is an important data-engineering skill.

#### 3. Events may arrive out of order

Events do not always arrive in the same order in which they happened.

For example:

1. A user clicks “Add to cart.”
2. The phone goes offline.
3. The user clicks “Checkout.”
4. The phone reconnects.
5. The “Checkout” event arrives first.
6. The older “Add to cart” event arrives later.

Other causes include retries and incorrect device clocks.

Therefore, systems should usually use the event’s own timestamp—when the action happened—instead of only using the timestamp when the server received it.

This is called event-time processing.

#### 4. The huge volume is the main challenge

Clickstream data can arrive extremely quickly. At 50,000 events per second, the system receives billions of events per day.

That creates large costs for:

- Storage
- Network transfer
- Processing
- Querying

So the system should:

- Batch events together.
- Compress them before storing or sending them.
- Aggregate data early where exact raw events are no longer needed.

For example, instead of repeatedly querying billions of individual clicks, the system might create hourly summaries:

```text
hour        page       click_count
10:00       /home      1,250,000
10:00       /pricing   430,000
```

The phrase “raw clickstream is a storage bill, not a dashboard” means that storing every raw event is expensive, and dashboards usually need summaries rather than every individual click.

### Simple summary

Logs and clickstream data are:

- Fast and high-volume
- Inconsistent in format
- Sometimes incomplete
- Sometimes out of order
- Usually acceptable to process approximately

A good system stores the raw data safely, validates it later, handles event-time ordering, compresses and batches it, and aggregates it before exposing it to dashboards.

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

- Global ordering requirement

    > “All events must be processed in exact order.”

    Imagine events from many orders:

    ```text
    Order A: created → paid → shipped
    Order B: created → paid → shipped
    ```

    A team may say that every event in the entire system must be processed in one global sequence:

    ```text
    A-created → B-created → A-paid → B-paid → A-shipped → B-shipped
    ```

    In Kafka, the normal way to preserve ordering is to put related events in the same partition. But Kafka only guarantees ordering within a partition.

    If you require one global order, you effectively need:

    ```text
    One topic → One partition → One consumer
    ```

    That creates a bottleneck. You can no longer process many partitions in parallel.

    The “throughput ceiling” means the entire system can process only as fast as that one broker, partition, and consumer allow.

    Usually, global ordering is unnecessary. What matters is ordering for one entity:

    ```text
    Order A: created → paid → shipped
    Order B: created → paid → shipped
    ```

    Events for the same order are sent to the same partition using `order_id` as the key. Different orders can be processed in parallel.

    The lesson is:

    > Require ordering only where the business needs it, usually per order, account, device, or user.

- **Consumer lag spiral**: processing slower than production, lag grows, retention
  expires oldest, *silent data loss*. Symptom: dashboards fine, reconciliation
  broken. Alert on lag *and* on retention headroom. _`Detailed explanation provided below`_
- **Ack-deadline misses** (Pub/Sub): 
    Pub/Sub usually requires a consumer to acknowledge a message within a time limit.

    For example:

    ```text
    Message delivered
    → Consumer has 30 seconds to acknowledge it
    ```

    If processing takes longer than 30 seconds and the consumer does not extend the deadline, Pub/Sub assumes the consumer failed. It sends the message again.

    The original consumer may still finish processing the first copy, so the same event can be processed twice:

    ```text
    10:00:00  Message delivered
    10:00:31  Ack deadline expires
    10:00:31  Message delivered again
    10:00:35  First processing finishes
    10:00:40  Second processing finishes
    ```

    This can create duplicate side effects:

    ```text
    Charge credit card
    Send email
    Create shipment
    Increment account balance
    ```

    For example, one order event might accidentally create two shipments.

    The phrase “exactly-once marketing notwithstanding” means that a platform may advertise exactly-once-related features, but those features do not automatically make your entire business operation exactly once. Your database write or external API call may still be repeated.

    The system should:

    - Set the ack deadline based on realistic processing time
    - Extend the deadline while long processing is underway
    - Make the consumer idempotent
    - Deduplicate using an event ID
    - Use database uniqueness constraints where appropriate

    For example:

    ```sql
    INSERT INTO processed_events(event_id)
    VALUES ('evt-123');
    ```

    If `evt-123` already exists, skip the business operation.

    The lesson is:

    > Assume a message can be delivered more than once, even when the messaging system provides advanced delivery guarantees.

- **Webhook handler "doing a little work"**: 
    A webhook is an HTTP request from another company, such as Stripe, GitHub, or Shopify.

    A dangerous design looks like this:

    ```text
    Stripe → Your webhook endpoint
              ├─ validate request
              ├─ call database
              ├─ call shipping service
              ├─ send email
              └─ update analytics
    ```

    The sender expects a fast response. If your endpoint does too much work, it may take several seconds or fail because one downstream service is slow.

    Then the sender assumes delivery failed and retries:

    ```text
    First request: slow or times out
    → Stripe retries
    → second request overlaps
    → second request also becomes slow
    → more retries begin
    ```

    This is a retry storm.

    It can cause:

    - Duplicate orders
    - Duplicate payments or refunds
    - Many simultaneous requests
    - HTTP 500 errors
    - More load on already-slow dependencies
    - Lost events if the sender eventually stops retrying

    The safer design is:

    ```text
    Stripe → Webhook endpoint → Durable queue
                                  ↓
                            Background worker
    ```

    The endpoint should:

    1. Verify the signature.
    2. Validate the event.
    3. Persist it durably.
    4. Return `200 OK` quickly.

    The worker later performs the actual business logic and deduplicates using the webhook event ID.

    The principle is:

    > A webhook endpoint should accept and store the event, not process the whole business workflow synchronously.

- **Poison message without a DLT**: 
    A poison message is an event that repeatedly fails processing.

    For example:

    ```json
    {
      "order_id": "123",
      "amount": "not-a-number"
    }
    ```

    Suppose the consumer expects `amount` to be numeric:

    ```text
    Read message
    → parsing fails
    → message is retried
    → parsing fails again
    → message is retried again
    ```

    If there is no dead-letter topic or queue, the bad message may remain in the normal processing path forever.

    If ordering is involved, later messages may not be processed until the bad one succeeds. That makes the partition appear “wedged”—it is technically running, but progress is stuck.

    The correct pattern is:

    ```text
    Try message several times
    → still failing
    → send to dead-letter topic
    → continue processing other messages
    ```

    The dead-letter record should include:

    - Original payload
    - Error message
    - Number of attempts
    - Original topic or queue
    - Timestamp
    - Consumer version

    Operators can then inspect and repair the message later.

    The lesson is:

    > Every production event pipeline should have a failure path for permanently invalid messages.

- **Trust in the schema that isn't there**:
    Suppose your team expects every log event to look like this:

    ```json
    {
      "user_id": 42,
      "event_name": "purchase",
      "amount": 99.99
    }
    ```

    Later, another application changes the field:

    ```json
    {
      "user_id": 42,
      "event": "purchase",
      "amount": 99.99
    }
    ```

    The pipeline may not crash. Instead, it may look for `event_name`, fail to find it, and produce a null value:

    ```text
    event_name = null
    ```

    Those nulls may flow into reports and aggregates:

    ```text
    Purchases by event_name:
    purchase: 0
    null: 250,000
    ```

    If nobody monitors this field, the problem can continue for weeks. The dashboard may still load, so the failure is quiet rather than obvious.

    Good protections include:

    - Validate required fields at ingestion
    - Track schema versions
    - Alert when null rates increase
    - Reject or quarantine invalid records
    - Keep raw events for investigation
    - Test producer changes against consumers
    - Monitor important fields, not just pipeline uptime

    The lesson is:

    > A pipeline can be technically healthy while producing incorrect data. Validate the meaning and shape of the data, not just whether messages are flowing.

## How to handle Backpressure problem?

> Put a durable buffer—usually a queue or streaming system—between the producer and the consumer.

### The problem

A producer may send data faster than your system can process it:

```text
Producer: 100,000 events/sec
Consumer: 60,000 events/sec
```

The extra 40,000 events per second create **backpressure**. Without a buffer, events are dropped or the producer overwhelms the consumer.

### How the page suggests handling it

1. **Buffer the incoming data**

Use Kafka, Pub/Sub, Kinesis, Event Hubs, or a queue. The queue temporarily absorbs bursts and lets consumers process data at their own speed.

```text
Producer → Durable queue → Consumer
```

The queue is described as the “load-bearing wall” of a push architecture.

2. **Size retention for outages**

The queue must retain messages long enough to survive a realistic consumer outage.

For example, if a consumer might be unavailable for two days, configure retention for more than two days. Otherwise, unprocessed messages expire and are silently lost.

3. **Scale consumers**

For Kafka-like systems, divide data into partitions and add consumers to process partitions in parallel. Key records by an entity, such as `order_id`, when ordering matters.

This provides per-order ordering without forcing the entire system through one consumer.

4. **Monitor consumer lag**

Consumer lag shows how far behind the consumer is.

If production is faster than processing:

```text
lag grows → retention window shrinks → old messages expire → data loss
```

Recommendation is to add:

- Consumer lag
- Remaining retention headroom

5. **Make consumers idempotent**

Push systems commonly provide at-least-once delivery, so messages may be delivered again during retries or recovery.

Consumers should safely process duplicates by using:

- Event IDs
- Business keys
- Idempotent writes

2. **Use dead-letter queues**

If one malformed message repeatedly fails, move it to a dead-letter topic or queue after several attempts. Otherwise, that “poison message” can block progress.

7. **For webhooks: acknowledge quickly**

A webhook handler should:

```text
Validate → Persist to queue/storage → Return HTTP 200
```

It should not perform slow business processing inside the request. Processing happens later from the durable queue, preventing vendor retries from creating a retry storm.

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