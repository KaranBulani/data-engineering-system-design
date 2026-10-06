# Chapter 08 — Streaming

> Part III — Processing Paradigms

Streaming processing works on data while that data is still arriving. For example, a streaming system can update a fraud score as each payment arrives instead of waiting for a nightly batch job.

Streaming is difficult because the input has no natural end. Events can arrive late, out of order, or more than once. The processor must keep state between events, recover that state after a crash, and explain what happens when an event is replayed. This chapter focuses on those practical correctness problems.

## The Question

Suppose a business needs results while events are still arriving. How can we calculate over an endless stream when events may be out of order or duplicated? What does it actually mean to say that the result is correct?

The answer begins with four ideas: event time, processing time, watermarks, and delivery guarantees. These ideas describe when an event happened, when the system saw it, when a window is considered complete, and how duplicates or lost events are handled.

## The Physics

### Event time vs processing time

**Event time** is the time at which something happened in the source system. It is usually stored in the event itself. For an order, it might be the time at which the customer placed the order.

**Processing time** is the time at which our streaming system receives or processes the event. The two times can be different because of network delays, retries, incorrect clocks, a phone working offline, or a vendor sending an event again later.

```text
event time:   e1 ---- e2 ------ e3 ----- e4 ------>   (what happened)
                   \      \        \       \
processing          e1     e2      e4    e3    -->   (what you saw)
time:                                          e4 arrived before e3!
```

In this example, event `e3` happened before `e4`, but the system received `e4` first. If the business asks for “orders per minute,” it normally means the minute in which the orders actually happened. Grouping by processing time would place delayed orders in the wrong minute.

Processing-time windows are still useful for operational questions such as “How many events did our service receive each minute?” For business questions about customer activity, event time is usually the correct choice.

### Watermarks: how unbounded streams make progress

Imagine that we want to calculate the total revenue for the 12:00–12:01 event-time window. We cannot know immediately whether another event for that minute is still on its way. The stream has no end that tells us the answer is complete.

A **watermark** is the system's estimate that it has seen almost all events up to a particular event time. In simple terms, it says: “We believe that events with event time at or before `T` will not arrive anymore, or that any such events will be rare and handled separately.” When the watermark passes the end of a window, the processor can emit the window's result.

One common calculation is:

```text
observed maximum event time: 12:05:40
allowed disorder:             2 minutes
watermark:                    12:03:40
-> windows ending before 12:03:40 may be finalized
```

The watermark is not a proof. It is a decision based on observed lateness and an acceptable lateness bound. A short two-minute bound produces results quickly, but more late events will miss the normal window. A longer bound gives late events more time to arrive, but output is delayed. There is no setting that gives both immediate output and perfect knowledge of all future events.

### Late data: allowed lateness and side outputs

An event is late when it arrives after the watermark has passed the event-time window to which it belongs. The system needs an explicit policy for such events:

- **Allowed lateness:** Keep a window open for a defined extra period. If an event arrives during that period, update the window and emit a new result. The **destination must support updates or upserts because the earlier result is no longer final** .
- **Side output:** Send events that are too late to a separate stream. That stream can be audited, counted, or processed later in a batch job.

Late events should not be silently discarded. The number and percentage of side-output events tell us whether the watermark is reasonable. A sudden increase may indicate a source delay or a watermark that is too aggressive.

### Windowing on event time

A window is the range of events that a streaming operation groups together. Windows limit how much data and state the processor must keep.

| Window | Meaning | Example |
|---|---|---|
| Tumbling | Fixed-size windows that do not overlap | Revenue for each one-minute bucket |
| Sliding | Fixed-size windows that overlap and move by a chosen interval | Five-minute revenue recalculated every minute |
| Session | A window that closes after a period of user inactivity | A user session closes after 30 minutes without activity |
| Global | One logical window for the whole stream, with explicit triggers | A running total emitted every few minutes |

Session windows are common for clickstream data. We group events by user ID and keep the session open while activity continues. After the inactivity gap is reached, the session closes.

Every open window consumes state. A sliding window may keep several overlapping windows for the same key. A session can remain open for an unpredictable amount of time if the user continues to be active. Therefore, **state size must be estimated as part of capacity planning** rather than treated as an implementation detail.

### State management

Many streaming operations need to remember information. An aggregation remembers counts and sums. A sessionization job remembers the last activity for each user. A join may remember records from one input while waiting for matching records from another. This remembered information is called **keyed state** when it is organized by a key such as `user_id`.

Important state-management mechanisms include:

- **Checkpointing:** The processor periodically saves its state to durable storage and records which source offsets belong with that state. After a crash, it restores both pieces together. For example, Flink uses coordinated asynchronous barrier checkpoints, and Spark Structured Streaming stores state and offsets in its checkpoint location.
- **State TTL:** State that is never removed becomes a memory leak. Windows naturally remove state when they close, but other state needs an explicit time-to-live rule. For example, a user profile kept only for a seven-day join should expire after seven days.
- **Disk-backed state:** State may be larger than memory. Flink commonly uses RocksDB to store larger keyed state on local disk while keeping access manageable.
- **Restore behavior:** The checkpoint interval affects recovery time. If a job has 200 GB of state and checkpoints take four minutes, a crash may require several minutes to restore and then catch up on events that arrived after the last checkpoint. This recovery cost must be included in the design.

### Delivery semantics — the part everyone gets wrong

Delivery semantics describe what a consumer can expect when messages are processed:

1. **At-most-once:** An event is processed zero or one time. The system may lose an event, but it will not intentionally retry it. This is appropriate only when losing an event is acceptable.
2. **At-least-once:** The system retries until it believes the event was processed. Events are not intentionally lost, but an event may be processed more than once. This is the practical default for many systems, including the path from Kafka to a consumer.
3. **Exactly-once:** Each event affects the final result one time. This promise is meaningful only when every relevant part of the processing path participates in the guarantee.

The important distinction is that **exactly-once delivery is not automatically exactly-once processing**. Kafka transactions can coordinate reads and writes inside the Kafka ecosystem. If the consumer then writes to Redis, an external API, or an ordinary database table, that external operation may happen again during a retry unless it is designed to be idempotent.

A common production design is **at-least-once delivery plus an idempotent sink**. The system may retry an event, but the repeated operation produces the same final result:

| Mechanism | How duplicates are handled | Example |
|---|---|---|
| Idempotent write | Writing the same key replaces the previous value | `HSET user:42 ...` in a key-value store |
| Idempotent SQL | A merge updates the same business key instead of inserting another row | `MERGE` in a warehouse |
| Transactional sink | Source offsets and output are committed as one atomic action | A transactional Flink Kafka sink or an Iceberg/Delta commit |

An atomic action means that either both the source progress and the output are committed, or neither is committed. This prevents a crash from creating a mismatch such as “the offset says the event was processed, but the output was never written.” Transactional sinks exist and can provide strong guarantees, but they add latency and operational complexity. They should be chosen when the business needs them, not enabled without understanding the downstream sink.

The phrase **effectively-once** is often more accurate. The system may deliver an event more than once, but idempotency makes the final business result look as though it was applied once. That result comes from a design discipline, not merely from switching on an engine setting.

### The engines

| | Flink | Spark Structured Streaming | Kafka Streams |
|---|---|---|---|
| Execution model | Continuous processing, event by event | Micro-batches; see ch09 | A library that processes events near the Kafka application |
| Event time and watermarks | Native and very strong | Supported, with micro-batch behavior | Supported through a grace-period model |
| State | Keyed state, RocksDB, asynchronous snapshots | Checkpointed state stores | Local RocksDB with changelog topics |
| Operations model | Flink cluster with JobManagers and TaskManagers | Spark cluster or serverless Spark | JVM library; no separate stream cluster required |
| Best fit | Sub-second latency, large state, complex event processing | Teams already using Spark and accepting seconds-to-minute latency | Kafka-focused services and smaller teams that do not want another cluster |

The choice usually follows the requirements:

- Choose **Flink** for very low latency, complex event-time logic, sessionization, or large keyed state.
- Choose **Spark Structured Streaming** when the team already operates Spark, wants to share code with batch jobs, and a latency of several seconds to about a minute is acceptable.
- Choose **Kafka Streams** when the application is centered on Kafka, the processing is close to the application, and the team does not want to operate a separate processing cluster.

### Where streaming hurts

- **Cost:** Streaming applications usually run 24 hours a day. They need compute, storage for checkpoints, and extra capacity for traffic spikes.
- **Operational work:** The team must monitor watermarks, state size, checkpoint duration, consumer lag, rebalances, and backpressure. These create a larger on-call responsibility than many batch jobs.
- **Correctness risks:** A wrong event-time choice, an overly aggressive watermark, a missing TTL, or a non-idempotent sink can silently produce wrong business results.
- **Debugging:** To understand what the system saw at a particular time, the team needs retained input data and replay tools. Ordinary application logs rarely contain enough information to reconstruct the state of a streaming job.

## The Options

The main streaming decision is not simply “streaming or batch.” It is how much streaming machinery the business requirement justifies:

| Tier | What you run | What you get |
|---|---|---|
| Log-and-serve | Kafka plus simple consumers writing to key-value stores | Fast access to current records, but limited aggregation |
| Micro-batch | Structured Streaming with triggers every 10–60 seconds | Windowed aggregates and shared batch/stream code |
| True streaming | Flink or Kafka Streams | Sub-second processing, event-time handling, and rich state |
| Streaming plus table formats | Flink writing upserts to Iceberg or Delta | Streaming updates that batch consumers can query later |

## Decision Rules

- A measurable freshness requirement should justify streaming. The team pays for the added compute and operational complexity every month.
- Use event time for business event calculations. Use processing time for questions about when the pipeline received or handled data.
- Choose the watermark from observed lateness data. Monitor the side-output rate to see whether the choice is working.
- Make every consumer idempotent from the beginning. At-least-once delivery is common, and effectively-once behavior must be designed.
- Estimate the size and lifetime of keyed state before selecting an engine. Large state favors an engine with strong state-management support, such as Flink. Stateless enrichment is less demanding.
- When someone claims “exactly-once,” ask: exactly once to which sink, and through what atomic mechanism? Without a transactional or idempotent sink, the guarantee stops before the final write.
- Use an engine with real keyed state for sessions and heavy windows. Micro-batch state stores may be a poor fit for sub-second sessionization.

## Failure Modes

- **Processing-time windows for an event-time question:** Revenue is grouped by the minute when events arrived instead of the minute when orders happened. Late events make the dashboard change retroactively, and totals do not match a warehouse that used event time.
- **A watermark with zero allowed lateness:** Small network delays send many valid events to the side output. The side-output stream may grow slowly enough to be ignored while still causing audit totals to fail.
- **Unbounded state:** A session window has no timeout or the timeout is too long. State grows for weeks until checkpoints become too large and start timing out. The job may look healthy for a long time before failing suddenly.
- **Duplicate side effects:** At-least-once delivery sends an event twice to a non-idempotent sink. An alert fires twice or a charge counter doubles. This is one of the most common real streaming bugs.
- **Lag spiral:** State grows, processing slows, lag increases, and the processor has even more state to handle. The slowdown reinforces itself until the job falls further behind.
- **Exactly-once theater:** The source and processing engine support transactions, but the final step appends to an ordinary table. After a restore, the final table receives duplicate rows even though earlier parts of the pipeline looked exactly-once.

## Interview Narration

“Streaming changes the problem from calculating over a fixed collection of data to calculating while time is still moving. I first separate event time from processing time. Business questions usually refer to when an event happened, while the processor sees the event later or out of order. Using the wrong time produces incorrect windows.

I would choose a watermark from the observed distribution of event delays. A short watermark gives faster output but sends more late events aside. A longer watermark waits longer and captures more events. **Events within the allowed-lateness period should update the window through an upsert-capable sink. Events that are too late should go to a side output for audit or later batch processing; they should not disappear silently.**

For guarantees, I would **assume at-least-once delivery and make consumers idempotent** . That gives effectively-once business results even when an event is replayed. **True end-to-end exactly-once requires a sink that atomically commits both the source progress and the output, so I would ask which sink is covered and what mechanism provides that atomicity.**

For engine choice, sub-second latency or heavy event-time state such as sessionization points toward Flink. A team already using Spark may choose Structured Streaming when several seconds to a minute is acceptable. Kafka Streams is useful for Kafka-centered services that do not want a separate processing cluster. Finally, I would state the cost clearly: **streaming requires continuous compute and monitoring for state, watermarks, checkpoints, and lag.** I would ask for the latency requirement before accepting that cost.”

---

| <- Previous | Next -> |
|---|---|
| [Chapter 07 — Batch](ch07-batch.md) | [Chapter 09 — Micro-batch](ch09-micro-batch.md) |
