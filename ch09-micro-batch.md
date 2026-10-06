# Chapter 09 — Micro-batch

> Part III — Processing Paradigms

Micro-batch processing handles a stream by collecting events for a short period and processing them together. For example, a job may read new events every 10 seconds, process that small group, and then repeat.

Micro-batch is a deliberate design choice. It is useful when the business needs results in seconds or a minute, but does not need results in milliseconds. It provides many of the tools and operational habits of batch processing while still offering much fresher results than a normal scheduled job. The important skill is knowing when its delay and overhead are acceptable and when a true per-event streaming engine is necessary.

## The Question

Suppose the requirement is “fresh within a few seconds,” not “respond within a few milliseconds.” Do we need a true streaming engine, or would a batch engine that runs every 10 seconds be simpler and more reliable?

The answer depends on the required latency, the size of the state, the destination storage, and the skills the team already has. **Micro-batch is often the right answer when the team already uses Spark and the target is a lakehouse.**

## The Physics

### What micro-batch actually is

Micro-batch treats a continuous stream as a sequence of small, bounded batches called **triggers**. Spark Structured Streaming is a common implementation:

```text
continuous event stream
   |--- trigger 1 ---|--- trigger 2 ---|--- trigger 3 ---|  (every N seconds)
        batch             batch             batch
```

During each trigger, the engine performs three main actions:

1. It reads the events that arrived after the last checkpointed source offset. This creates a small, bounded input batch.
2. It applies the streaming query to that batch. The engine keeps the query plan and executes the relevant work for the new data.
3. It saves the output, updated state, and new source offsets to durable storage so that the next trigger can continue from the correct position.

The useful mental model is: **micro-batch is batch processing with a durable bookmark.** Each trigger is a small batch job over the latest few seconds or minutes. This gives it batch's familiar APIs and tools, but it also creates a minimum delay because each batch must be scheduled, processed, and committed.

### Where micro-batch wins

1. **One codebase for batch and streaming:** The same DataFrame or SQL logic can often process a bounded table for a backfill and an unbounded stream for live updates. This avoids maintaining two versions of the business rules. Without this discipline, the live calculation and the nightly recomputation can slowly disagree, which is one of the problems with a Lambda-style design discussed in ch10.
2. **Natural lakehouse writes:** Each trigger produces one bounded write. That fits table formats such as Iceberg and Delta, where each write creates a new table snapshot. A streaming upsert becomes a small `MERGE` operation repeated every few seconds.
3. **Operational familiarity:** If the team already runs Spark, it can use familiar clusters, monitoring, deployment processes, and incident runbooks. The team does not need to learn and operate an entirely different streaming platform just to meet a seconds-level requirement.
4. **Good-enough freshness:** Dashboards, feature refreshes, and many alerts work well with results that are 10 to 60 seconds old. Requiring sub-second output when nobody uses it adds cost without improving the product.

### Where it bites

- **There is a latency floor.** The minimum delay is roughly the trigger interval plus the time needed to process and commit the batch, plus scheduling and coordination overhead. If the trigger is 10 seconds and the batch takes 9 seconds, a small garbage-collection pause or traffic spike can make the next batch late. A sub-second requirement is not a good fit for this model.
- **Small files accumulate.** Writing every 10 seconds can create thousands of small files in a lakehouse each day. Small files increase metadata work and make queries slower. Compaction must be planned as part of the system rather than added after performance has already degraded.
- **Long windows create large state.** A seven-day rolling calculation may be possible, but the processor has to retain state for the window. The state grows with the window duration and the number of distinct keys. Spark's state store is useful, but it may be less convenient for very large keyed state than Flink's RocksDB-based state management described in ch08.
- **Very short triggers waste work.** Every trigger has overhead for planning, scheduling, checkpointing, and committing. A 200-millisecond trigger performs that batch overhead five times per second. At that point, the design is paying for batch boundaries while trying to behave like a per-event engine.

### The honest comparison

| | Flink (true streaming) | Spark Structured Streaming |
|---|---|---|
| Latency | Tens to hundreds of milliseconds are possible | Usually seconds because of the trigger interval |
| Work unit | Per event | Per batch, with work amortized over the batch |
| State | Strong keyed-state support, including RocksDB | Checkpointed state store with simpler ergonomics for Spark teams |
| Codebase | Streaming API; batch processing is a different mode | **Often shared with batch DataFrame and SQL code** |
| Team skills | Requires Flink operational knowledge | Builds on an existing Spark investment |
| Event time | Watermarks can advance as events are processed | Watermarks advance at batch boundaries |

The last row is important. In micro-batch, the watermark normally moves forward once per batch. With a 30-second trigger, late-data handling effectively advances in 30-second steps. That is usually fine for a dashboard, but it may be too coarse for a system that detects a safety event or fraud pattern within milliseconds.

### Micro-batch vs continuous processing — the spectrum

“Real-time” is not a single speed. It describes a range of possible freshness targets:

| Model | Processing unit | Typical latency floor | Example engines | Event-time handling |
|---|---|---|---|---|
| Batch | Hours or larger windows | Schedule interval plus runtime | Spark batch, dbt, warehouse SQL | Bounded windows and complete input |
| Micro-batch | Seconds or minutes of events | Trigger interval plus batch runtime | Spark Structured Streaming | Watermarks move per batch |
| Continuous processing | Individual records | Roughly milliseconds to hundreds of milliseconds | Flink, Kafka Streams, experimental Spark continuous mode | Watermarks can move per event |

A **200-millisecond trigger is still micro-batch**. It still creates a bounded batch and pays for planning, checkpointing, and commit work at every boundary. Continuous processing instead keeps a long-running operator graph and moves records through it without creating a batch boundary for each small interval. Checkpointing still occurs, but it is separate from a tiny batch trigger.

Therefore, do not choose micro-batch by repeatedly shrinking the trigger until it resembles continuous processing. First identify the required latency floor. Then choose the engine that can meet that requirement with the team's operational skills and the available budget.

## The Options

| Trigger interval | What it usually means |
|---|---|
| 200 milliseconds–1 second | Trying to imitate Flink; per-batch overhead often dominates |
| 5–30 seconds | Common sweet spot; fresh results with enough work per batch to amortize overhead |
| 1–5 minutes | Streaming-shaped processing for slower dashboards or tables |
| Continuous mode | Per-event processing; Spark's mode is specialized and should be checked for maturity before use |

The trigger interval is not the same as the end-to-end latency. A 10-second trigger does not guarantee a result within 10 seconds. The batch must wait for the trigger, process the data, write the result, and commit successfully. Service-level objectives should be based on the full observed latency, including slow or overloaded batches.

## Decision Rules

- Use a true streaming engine such as Flink for sub-second latency or heavy per-event state.
- Use micro-batch when the target is roughly 5–60 seconds, the team already operates Spark, and the destination is a lakehouse or another system that benefits from bounded commits.
- Prefer micro-batch when the same business logic must run live and later as a backfill. **One codebase reduces the risk that the live and historical results drift apart.**
- Ensure that the worst-case batch runtime is comfortably shorter than the trigger interval. Two to three times of headroom is a useful starting point, although the final amount depends on the service-level objective and traffic variation. **For example,** with a 30-second trigger interval, aim for the slowest normal batch to finish in about 10–15 seconds. That leaves roughly two or three times as much time in the interval as the batch needs. If batches regularly take close to 30 seconds, a small traffic spike or slow write can make them run late and cause processing lag to grow.
- Plan compaction from the beginning when each trigger writes files to a table format.
- Describe micro-batch as a deliberate cost and latency choice. It is not an inferior version of Flink; it is appropriate for a different requirement.

## Failure Modes

- **Micro-batch at streaming frequency:** The system triggers every 500 milliseconds, but planning and checkpoint overhead consume most of each interval. Processing time approaches the trigger interval, lag starts growing, and the system is asking for a true streaming engine.
- **Small-file flood:** A job commits Parquet files every 10 seconds without compaction. Over a year, queries must plan over millions of files, so query latency gets worse month by month.
- **State-store overflow on long windows:** A 30-day rolling window with millions of keys creates more state than the system can handle efficiently. State spills to disk, checkpoints take longer, and triggers begin to overrun their schedule.
- **Watermark quantization surprise:** The business expects late events to be handled with per-event precision, but the watermark only advances every 30 seconds because that is the trigger interval. The resulting behavior does not match the requirement.
- **An unstated latency floor:** Stakeholders hear “real-time” but later discover that the typical result takes 25 seconds and the slowest results take 90 seconds after a compaction backlog. Define latency objectives using percentiles such as p95 or p99, not only an average.

## Interview Narration

“I treat micro-batch as a deliberate point on the cost and latency curve, not as a failed attempt at true streaming. The engine runs the query at each trigger interval over events after the last checkpointed offset. It then commits the output, updated state, and new offsets. Each trigger is therefore a small bounded batch with familiar batch-style recovery.

Micro-batch is a strong choice when the requirement is seconds to a minute, the team already runs Spark, and especially when the **same logic must run both live and as a backfill**. One codebase prevents the streaming result and the nightly recomputation from slowly using different rules. It also fits lakehouse tables because every trigger can commit a bounded snapshot or merge into Iceberg or Delta.

The trade-off is a real latency floor: trigger interval, processing time, and commit overhead all contribute to the result delay. Micro-batch also creates small files, so **compaction must be part of the original design**. Long, high-cardinality windows can make its state store expensive and difficult to manage.

My rule would be: use Flink for sub-second latency or heavy event-time state, and use micro-batch for roughly 10–60 seconds when the team is already invested in Spark. If asked why I am not choosing Flink, I would say that its extra capabilities do not provide enough value for this latency requirement and team setup to justify the additional operational cost.”

---

| <- Previous | Next -> |
|---|---|
| [Chapter 08 — Streaming](ch08-streaming.md) | [Chapter 10 — Lambda Architecture](ch10-lambda-architecture.md) |
