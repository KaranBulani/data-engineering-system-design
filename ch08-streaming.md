# Chapter 08 — Streaming

> Part III — Processing Paradigms

Streaming is where the hard computer science lives: unbounded data, no natural
"end of window," events arriving out of order, state that must survive crashes,
and delivery guarantees that sound better in marketing than in physics. This
chapter is the one to actually master — it separates senior candidates from
senior-sounding ones.

## The Question

*"I need results while the data is still arriving. How do I compute over an
unbounded, out-of-order, duplicated event stream — correctly — and what exactly
am I guaranteed about 'correctly'?"*

## The Physics

### Event time vs processing time

The foundational distinction. **Event time** = when it happened (timestamp in the
event). **Processing time** = when your system saw it. They diverge because of
ingestion lag, retries, clock skew, mobile offline flushes, and vendor retries.

```
event time:   e1 ---- e2 ------ e3 ----- e4 ------>   (what happened)
                   \      \        \       \
processing          e1     e2      e4    e3    -->   (what you saw)
time:                                          e4 arrived BEFORE e3!
```

Every streaming correctness bug is some flavor of "we used processing time when
the question was about event time." "Per-minute order counts" means *minute of
event time* — buckets defined by when orders happened, not when your consumer
saw them.

### Watermarks: how unbounded streams make progress

The core problem of event-time computation: you cannot aggregate a minute until
you know the minute's data is *done arriving* — but the stream never ends, so
how do you know? **Watermark** = a heuristic assertion: "we believe no more
events with event time <= T will arrive." When the watermark passes a window's
end, the window is finalized and emitted.

Watermarks are derived from observed events + a configured bound on disorder:

```
observed max event-time: 12:05:40
allowed lateness bound:  2 minutes
watermark = 12:05:40 - 00:02:00 = 12:03:40
-> windows ending before 12:03:40 may be finalized
```

The trade is fundamental: **a tight watermark emits fast but drops late events;
a loose watermark waits and stays correct but adds latency.** There is no
setting that is both instant and complete — anyone who claims otherwise is
selling something.

### Late data: allowed lateness and side outputs

Events that arrive *after* the watermark passed their window:

- **Allowed lateness**: re-open and update the window for events within the
  bound (emitting an update downstream — which requires upsert-capable sinks).
- **Side outputs**: route unacceptably late events to a separate stream —
  audit them, batch-reprocess them, or count them. Never silently drop; the
  count of side-output events is a health metric for your watermark choice.

### Windowing (on event time)

| Window | Semantics | Example |
|---|---|---|
| Tumbling | fixed size, non-overlapping | revenue per 1-minute bucket |
| Sliding | fixed size, slides by interval | "5-min revenue updated every 1 min" |
| Session | gap-based: inactivity closes the window | user sessions: 30-min inactivity cut |
| Global | whole stream, with triggers | running totals with periodic emission |

Session windows are the interview favorite for clickstream: "sessionize user
activity" = session windows keyed by user id, closed by a gap. Note what windows
do to state: every open window is state; sliding windows multiply open windows
per key; sessions are unbounded per active key. State size is a capacity plan,
not an afterthought.

### State management

Stream operators that aggregate *must remember things* (open windows, counts,
the last value per key). This is **keyed state**, and it is the operational
heart of streaming systems:

- **Checkpointing**: state snapshots to durable storage, coordinated with
  source offsets, so a crash resumes exactly at (offset, state) consistency.
  Flink's asynchronous barrier checkpointing is the reference design; Spark
  Structured Streaming checkpoints state + offsets to storage.
- **State TTL**: state that never expires is a leak. Windows close it naturally;
  everything else needs explicit TTL.
- **RocksDB-backed state**: larger-than-memory state, spilled to disk — the
  production default in Flink for real state sizes.
- **Restore semantics**: checkpoint interval is your restart cost bound. If
  state is 200GB and checkpoints take 4 minutes, every crash costs minutes of
  catch-up plus reprocessing from the checkpoint offset.

### Delivery semantics — the part everyone gets wrong

Three claims, precisely stated:

1. **At-most-once**: process, maybe lose. Nobody admits choosing this.
2. **At-least-once**: never lose, maybe duplicate. The *honest default* —
   this is what Kafka gives you between source and consumer.
3. **Exactly-once**: every event affects the result exactly once.

The critical precision: **exactly-once delivery is not exactly-once
processing.** Systems like Kafka transactions deliver each message to the
consumer exactly once *within the Kafka ecosystem* — but if your consumer writes
to Redis, an external sink, all bets are off unless the sink participates.

The production pattern is **at-least-once + idempotent sink = effectively-once**:

| Mechanism | How it dedups | Example |
|---|---|---|
| Idempotent writes | same key overwrites: `HSET user:42 ...` | KV sinks |
| Idempotent SQL | `MERGE` on business key | warehouse upserts |
| Transactional sink | two-phase commit: offsets + output commit atomically | Flink Kafka sink, Iceberg/Delta commits |

The transactional-sink row is what "exactly-once end-to-end" actually means:
offset commit and output write are one atomic action. It exists, it works, it
costs latency and operational complexity — narrate it as a priced option, not a
default.

Also: **effectively-once is the senior phrasing.** The result *looks* exactly
once; the mechanism is dedup/idempotency riding on at-least-once delivery.
Anyone who says "we just turned on exactly-once" has not read the fine print
about what their sink does on replay.

### The engines

| | Flink | Spark Structured Streaming | Kafka Streams |
|---|---|---|---|
| Execution model | true streaming: per-event, continuous | micro-batch (ch09) | library: per-event, co-located with Kafka |
| Event-time / watermarks | native, best-in-class | supported, micro-batch granularity | supported (grace period model) |
| State | keyed state, RocksDB, async snapshots | checkpointed state store, weaker ergonomics | local RocksDB + changelog topics |
| Ops model | cluster (JobManager/TaskManager) | Spark cluster / serverless | JVM library, no extra cluster |
| Sweet spot | sub-second, heavy event-time state, CEP | unified batch+stream codebase, 10s+ latency | Kafka-centric services, small teams |

The decision usually collapses to: **true low-latency + rich state -> Flink;
team already runs Spark + latency is seconds-to-a-minute -> Structured
Streaming; all-in on Kafka + no cluster appetite -> Kafka Streams.**

### Where streaming hurts

- **Cost**: 24/7 compute; state checkpoints; operational headroom.
- **Ops burden**: watermarks, state, rebalances, lag — an on-call surface that
  batch never has.
- **Correctness traps**: every section above is a way to be silently wrong.
- **Debugging**: "what did the pipeline see at 2:14am" requires replay
  infrastructure, not log greps.

## The Options

There is no options table for "streaming vs batch" here (that cascade is ch02 +
ch07); the options are *how much streaming correctness machinery you sign up
for*:

| Tier | What you run | What you get |
|---|---|---|
| Log-and-serve | Kafka + simple consumers writing KV stores | low-latency *records*, no aggregates |
| Micro-batch | Structured Streaming, 10-60s triggers | windowed aggregates, one codebase with batch |
| True streaming | Flink / Kafka Streams | sub-second, event-time-native, rich state |
| Streaming + table formats | Flink -> Iceberg/Delta upserts | streaming correctness *with* batch consumers (ch12) |

## Decision Rules

- **A latency requirement justifies streaming; nothing else does.** Cost and
  complexity are the price, paid monthly.
- **Aggregate on event time, always.** Processing-time windows are for
  operational metrics about your pipeline, not business events.
- **Choose watermark bound from observed lateness distribution, not vibes** —
  and monitor side-output volume to validate the choice.
- **Design every consumer idempotent from day one** — at-least-once is the
  physical reality; effectively-once is a design, not a feature flag.
- **Size state before choosing an engine**: big keyed state favors Flink's
  RocksDB story; stateless enrichment tolerates anything.
- **"Exactly-once" claims must be answered with: to which sink, via what
  atomic mechanism?** If there's no transactional sink, it's effectively-once.
- **Sessions and heavy windows -> an engine with real keyed state.** Don't
  fight micro-batch state stores for sub-second sessionization.

## Failure Modes

- **Processing-time windows on event-time questions**: "per-minute revenue"
  computed by arrival minute. Symptom: dashboards that reshape retroactively as
  late events pour in, and totals that never match the warehouse (which *did*
  use event time).
- **Watermark set to zero lateness**: every network blip drops events into side
  outputs. Symptom: side-output topic growing at 0.1% of main flow — small
  enough to ignore, large enough to fail audits.
- **Unbounded state**: session windows with no TTL; state grows until
  checkpointing collapses. Symptom: job stable for weeks, then checkpoint
  timeouts after a traffic spike.
- **Duplicate side effects**: at-least-once + non-idempotent sink = the alert
  fires twice, the charge counter doubles. The most common *real* streaming bug
  in production.
- **Lag spiral** (from ch05, now at the processor): state grows -> processing
  slows -> lag grows -> more state. Symptom: exactly the failure you designed
  streaming to avoid.
- **Exactly-once theater**: transactional source, idempotent engine, then a
  plain `append` to a table — the last hop silently duplicates on every
  restore.

## Interview Narration

"Streaming changes the problem from computing over data to computing over
*time*. The first thing I pin down is event time versus processing time —
business questions are almost always about event time, and the gap between the
two is where correctness bugs live. That leads directly to watermarks: I define
them from the observed lateness distribution, because they're a trade — tight
watermarks give fresh output and drop late events, loose ones stay complete and
add latency — and I monitor side-output volume to check I chose well. Late data
within an allowed-lateness bound updates the window and requires an
upsert-capable sink; beyond it, side outputs, never silent drops.

On guarantees, I'm precise: delivery is at-least-once in practice, so my
consumers are idempotent by construction — that's effectively-once, and it's a
design discipline, not a feature flag. True end-to-end exactly-once means a
transactional sink where offsets and output commit atomically — Flink to Kafka
with transactions, or upsert commits into Iceberg — and I treat that as a priced
option when the downstream can't tolerate replays.

Engine choice follows requirements: sub-second latency or heavy event-time
state, especially sessionization, points at Flink; seconds-to-a-minute latency
with a team that already lives in Spark points at Structured Streaming —
micro-batch is a legitimate point on the cost/latency curve, not a compromise
to apologize for. And I'd close by naming the price: streaming is 24/7 compute
and an on-call surface for state, watermarks, and lag — so I'd want the latency
requirement in writing before signing the team up for it."

---

| <- Previous | Next -> |
|---|---|
| [Chapter 07 — Batch](ch07-batch.md) | [Chapter 09 — Micro-batch](ch09-micro-batch.md) |