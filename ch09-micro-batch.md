# Chapter 09 — Micro-batch

> Part III — Processing Paradigms

Micro-batch is not a compromise that apologizes for itself — it is a specific
point on the latency/cost/complexity curve, and for a huge fraction of
"real-time-ish" requirements it is the *correct* point. The senior skill is
knowing exactly where it stops being the right answer.

## The Question

*"My latency requirement is seconds-to-a-minute, not milliseconds. Do I need a
true streaming engine, or is a batch engine that runs every 10 seconds the
better engineering decision?"*

## The Physics

### What micro-batch actually is

Process the stream as a sequence of small bounded batches ("triggers"). Spark
Structured Streaming is the canonical implementation:

```
continuous event stream
   |--- trigger 1 ---|--- trigger 2 ---|--- trigger 3 ---|  (every N seconds)
        batch             batch             batch
```

Each trigger:
1. reads events since the last checkpointed offset (bounded batch),
2. executes an *incremental execution plan* — the engine plans once, runs
   per-batch,
3. commits output + state + offsets together to durable storage.

The mental model that matters: **micro-batch = batch machinery + a checkpointed
bookmark.** Every trigger is a mini batch job over the last N seconds of data.
That is why it inherits batch's strengths (one codebase, one API, batch-grade
tooling) and its floor (per-trigger overhead).

### Where micro-batch wins

1. **Unified batch + streaming codebase.** The same DataFrame/SQL code runs
   over a bounded table or an unbounded stream. In a world where the same
   business logic must usually exist in both a backfill job and a live job,
   *one definition of the logic* is a superpower: no drift between the nightly
   recompute and the live view (the Lambda disease, ch10).
2. **Lakehouse writes.** Each trigger's commit is exactly a bounded write —
   which maps perfectly onto table-format commits (Iceberg/Delta snapshot per
   trigger, ch12). Streaming upserts become "small MERGE every 10 seconds."
3. **Operational simplicity.** It is a Spark job: same clusters, same
   monitoring, same on-call runbooks, same people. A team already operating
   Spark adds streaming with near-zero new operational surface.
4. **Good-enough latency.** For dashboards, feature refreshes, and most
   alerting, 10-60 seconds is inside the requirement. Freshness beyond that is
   gold-plating (ch02).

### Where it bites

- **Latency floor = trigger interval + batch runtime + scheduling overhead.**
  The batch must *finish* before the next can commit; a 10s trigger whose
  processing takes 9s is one GC pause from collapse. Sub-second latency is
  structurally out of reach.
- **Per-batch small files.** Writing every 10 seconds produces thousands of
  small files per day; without compaction, metadata chokes (ch19). This is the
  #1 operational cost of micro-batch-to-lakehouse.
- **Long windows across batches**: a 7-day window is re-derivable from state,
  but state grows with window length × key cardinality; the state store is
  weaker than Flink's RocksDB keyed state (ch08).
- **Per-batch overhead is proportional**: trigger overhead × trigger count. At
  small intervals the overhead dominates; "micro-batch every 200ms" is paying
  batch overhead at streaming frequency — the worst of both.

### The honest comparison

| | Flink (true streaming) | Spark Structured Streaming |
|---|---|---|
| Latency | 10s-100s of milliseconds | seconds (trigger-bound) |
| Per-event overhead | O(event) | O(batch) amortized — wins at coarse triggers |
| State | best-in-class keyed state, RocksDB | checkpointed state store, less ergonomic |
| Codebase | streaming-only (batch is a different API) | **unified with batch** |
| Team skills | needs Flink operators | rides existing Spark investment |
| Event-time | native, per-event watermarks | supported, per-batch watermarks |

The last row is subtle and interview-worthy: in micro-batch, the watermark
advances once per batch, so a 30-second trigger quantizes lateness handling to
30-second granularity. Fine for dashboards; potentially fatal for
millisecond-sensitive detection.

### Micro-batch vs continuous processing — the spectrum, precisely

"Real-time" is a spectrum, and the engine choice follows the *quantum* of
processing:

| Model | Quantum | Latency floor | Engines | Event-time handling |
|---|---|---|---|---|
| Batch | hours | schedule + runtime | Spark batch, dbt, warehouse SQL | bounded windows, exact |
| Micro-batch | seconds-minutes | trigger + batch runtime | Spark Structured Streaming | per-batch watermarks (ch08) |
| Continuous processing | per-record | ~1ms-100ms | Flink, Kafka Streams, Spark continuous mode (experimental) | per-event watermarks |

The interview-grade nuance: **continuous ≠ micro-batch at small intervals.** A
200ms trigger is still a *bounded batch* paying per-batch overhead (planning,
commit, state snapshot) 5x per second — the worst of both worlds. Continuous
processing moves records through a long-running operator graph with no batch
boundary at all; the commit quantum shrinks to the checkpoint interval, not
the trigger. The decision rule stays: pick by *required latency floor*, then
by team/engine gravity — and never simulate continuous with shrinking
triggers.
## The Options

| Trigger interval | What you're really choosing |
|---|---|
| 200ms-1s | pretending to be Flink — overhead dominates; wrong tool |
| 5-30s | the sweet spot: fresh enough, overhead amortized |
| 1-5 min | batch-shaped streaming; fine for slow tables |
| continuous mode | Spark's experimental per-event mode; niche, verify maturity |

## Decision Rules

- **Sub-second latency or heavy per-event state -> true streaming (Flink).**
- **5s-60s latency + Spark team + lakehouse target -> micro-batch.**
- **The same business logic needed live AND as backfill -> micro-batch's unified
  codebase is the killer argument.**
- **Trigger interval must clear worst-case batch runtime with headroom** (2-3x);
  otherwise you are building a lag spiral.
- **Plan compaction from day one** if writing to a table format per trigger.
- **Don't apologize for it**: narrate micro-batch as the correct cost/latency
  point, not as "we couldn't afford Flink."

## Failure Modes

- **Micro-batch at streaming frequency**: 500ms triggers, overhead-bound,
  unstable throughput. Symptom: processing time approaching trigger interval,
  then lag. The system is asking for Flink.
- **Small-file flood**: 10s commits to Parquet without compaction; a year later
  every query plans over millions of files. Symptom: query latency degrading
  month over month (ch19).
- **State store overflow on long windows**: 30-day rolling windows with high
  key cardinality; state spills, checkpoints stretch, triggers over-run.
- **Watermark quantization surprise**: late-data handling in 30s steps when the
  business assumed per-event precision.
- **The unspoken latency floor**: stakeholders discover "real-time" means
  "median 25 seconds, p99 90 after compaction backlog." Latency SLOs must be
  stated as p99, per trigger design (ch02).

## Interview Narration

"Micro-batch is a deliberate point on the curve, and I'd defend it as a choice,
not a compromise. The mechanics: the engine runs the same query every trigger
interval over the last checkpointed offset, committing state and output
atomically — so each trigger is a tiny bounded batch job, with all of batch's
correctness simplicity.

It wins when latency is seconds-to-a-minute, the team already operates Spark,
and especially when the same logic must run live and as backfill — one
codebase, no drift between the streaming view and the nightly recompute. It's
also the natural fit for lakehouse targets: each trigger commits a snapshot or
MERGE to Iceberg, so streaming writes and batch reads share one table.

The price is a hard latency floor — trigger interval plus processing time plus
overhead, so sub-second is structurally out of reach — plus small-file
accumulation, which means compaction is part of the design from day one, and a
state store that's workable but not Flink's keyed state for long-window,
high-cardinality workloads.

So my rule: sub-second or heavy event-time state -> Flink; 10-60 seconds and a
Spark team -> micro-batch and I spend the saved complexity on monitoring and
compaction. If an interviewer pushes 'why not Flink,' the honest answer is: for
this latency band and this team shape, Flink's advantages don't cash out, and
its operational cost is real — that's a requirement-driven trade, not
ignorance."

---

| <- Previous | Next -> |
|---|---|
| [Chapter 08 — Streaming](ch08-streaming.md) | [Chapter 10 — Lambda Architecture](ch10-lambda-architecture.md) |