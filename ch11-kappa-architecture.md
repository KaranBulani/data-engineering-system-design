# Chapter 11 — Kappa Architecture

> Part IV — Architecture Patterns

Kappa is Jay Kreps' (Kafka's creator) answer to Lambda's two-codebase
disease. His proposal, in plain terms: instead of running a batch system and
a streaming system side by side (ch10), make **the log — Kafka — the single
source of truth**, and treat every downstream table or index as just a *view*
that a streaming job builds from that log. Then only one program implements
the business logic, and the way you fix or rebuild history is always the
same: replay the log through that program. Elegant — with one famous catch:
reprocessing, which is where most of this chapter lives.

## The Question

*"Can I run ONE processing codebase — no batch layer, no duplicated logic —
and still get correct, rebuildable results over history?"*

Unpacked with the running example (the same video site as ch10). The
recommendation team needs a serving table — "views per video over the last
hour" — refreshed within seconds, because the recommendation service reads
it on every page load. And when the logic turns out to be wrong — say,
someone realizes bot views were being counted — the table must be rebuilt
correctly from history, not just patched going forward.

Lambda's answer was a second codebase: a batch layer that recomputes
everything and heals the fast layer. Kappa's answer is to refuse the second
codebase: keep only the streaming job, and treat *rebuilding* as "run the
same job again over the log." The question is whether that is possible and
affordable — which turns out to depend entirely on how long your log keeps
data, and what your logic does while it replays.

## The Physics

### The core claim: the log is the database

In Lambda, the raw master dataset lived on HDFS/S3 and Kafka was just a
delivery pipe. Kappa's move is to promote the log itself to the source of
truth: the log holds every event, in order, and a serving table is simply
*what you get after processing the log*. "The log is the database" means:
if the table is wrong, lost, or needs new logic, you don't edit the table —
you recompute it from the log, exactly as you'd rebuild a view.

```
                      +---------------------------+
   producers ------>  |  KAFKA (the log)          |  <- source of truth
                      |  compacted + retained     |
                      +------+-------------+------+
                             |             |
                (normal run) |             | (reprocess run)
                             v             v
                    +----------------+  +----------------+
                    | stream job v2  |  | stream job v2' |
                    | (live)         |  | (new group,    |
                    |                |  |  from offset 0)|
                    +-------+--------+  +-------+--------+
                            |                   |
                            v                   v
                    +----------------+  +----------------+
                    | serving table  |  | table-v2       |
                    | (materialized  |  | (rebuilt view) |
                    |  view)         |  |                |
                    +----------------+  +-------+--------+
                                                |
                                    swap: v2 -> serving
```

Two pieces of Kafka vocabulary make this diagram readable:

- An **offset** is a position marker in the log — "I have read events up to
  number 4,382,194."
- A **consumer group** is a named reader with its own position. Two
  different groups can read the *same* log independently: one can be live at
  the newest events while another slowly re-reads from the very beginning.

Kappa in one sentence: **serving tables are materialized views of the log,
and reprocessing is replaying the log with new logic into a new table, then
swapping.** Unpacked piece by piece:

- **"Materialized view of the log"**: the table's contents are fully defined
  by "process every event in the log with this logic." Like a cached query
  result, it can be thrown away and recomputed at any time — nothing lives
  in the table that isn't derivable from the log.
- **"Replaying with new logic into a new table"**: suppose bots must be
  excluded from view counts. You do not touch the live table. You start the
  same streaming job with the corrected code as a *new consumer group from
  offset 0* — it re-reads the entire log, re-derives counts, and writes to
  `table-v2` while the old table keeps serving traffic.
- **"Then swapping"**: once `table-v2` is validated (row counts reconcile,
  spot checks pass), you point the readers at it and retire the old table.
  The swap is the only "deployment" the rebuild needs.

Every downstream artifact — a KV serving store, an aggregate table, an
Elasticsearch index — is *derivable* by consuming the log. That is why there
is no drift: Lambda's disease (ch10) was two hand-maintained implementations
of one metric disagreeing; here there is only one implementation, just
executed more than once.

### The reprocessing problem — the famous catch

Replay requires the data to still be there. Three layers of constraint:

1. **Retention window.** Kafka topics keep events only for a configured time
   (the default is ~7 days), then delete them. So "replay from the
   beginning" really means "replay from whatever the log still holds." This
   quietly turns retention sizing into an *architecture decision*: the
   "replayable window" — how far back you can rebuild any table — is a
   correctness SLA, purchased with disk.

2. **Beyond retention: re-ingest from the raw archive.** The raw plane
   (immutable event files on S3, ch03/06) exists precisely for this: to
   rebuild history older than the log's retention, load the raw events into
   a fresh topic (or feed them to the job directly) and replay from there.
   Kappa purists dislike this — "that's just batch re-ingestion!" — and the
   pragmatic answer is that the *codepath* is still one: the same streaming
   logic, replayed; only the source of the events is the archive instead of
   the live topic.

3. **State size vs stateless transformation.** Replaying a stateless step —
   a filter, an enrichment — is cheap: the job just re-processes events and
   writes output, holding nothing. Replaying a *stateful* step — "views per
   video over the last 3 years" — means the job must re-derive all of its
   state by re-processing history key by key. That is the compute that
   Lambda's batch layer amortized across a nightly window, now paid as one
   long streaming replay.

This is the honest center of every Kappa discussion: **Kappa moves the
reprocessing cost from architecture (a second layer) into operations (log
retention + replay capacity).** In beginner terms: Lambda pays with a second
codebase; Kappa pays with bigger disks, longer retention, replay clusters,
and the discipline to operate them. Which deal is better depends on how
often you actually reprocess and how much history you must be able to
rebuild.

### Compacted topics as state

Log compaction (ch05) is a topic setting that keeps only the *latest* record
per key. A normal topic is history — every event in order; a compacted topic
is a changelog — like a continuously updated dictionary where the newest
entry per key wins and old entries are eventually removed. Two Kappa-specific
uses:

- **Rebuilding a serving table**: if the table holds "latest state per video"
  (current total, last updated-at, and so on), it can be rebuilt by simply
  re-consuming the compacted topic — latest-per-key wins, and when the
  re-consumption finishes, the table is fully populated. This is Kappa's
  version of "restore from backup": the backup *is* the topic.
- **Changelog-backed state**: the processor's own internal state (its
  counters and windows) can itself be written to a compacted topic (this is
  how Kafka Streams does it). If an instance dies, another instance restores
  by re-reading that topic — state and log are the same technology, and
  recovery is just re-consumption. Very elegant; also the first place to
  look when state grows unbounded, because the compacted topic grows with
  it.

### What Kappa assumes about your computation

Kappa works when the business logic is expressible as **stream
transformations over an append-only log**. Concretely, for each shape of
work:

- **event -> enriched event** (e.g., stamp each view event with the viewer's
  country and device): perfect fit. Stateless, replay is cheap, and the
  output is just the input with more columns.
- **event -> per-key aggregate with windows** (e.g., views per video per
  hour): fits, with state machinery (ch08). The live job maintains windowed
  counters; a replay re-derives all of that state — correct, but it costs
  real compute proportional to history.
- **"train a model over 3 years of history"**: does not fit. That is a
  bounded batch workload — read everything once, produce an artifact. Forcing
  it through a streaming engine means paying streaming costs (standing
  state, checkpoints, continuous operation) to get batch semantics, which is
  the worst of both.
- **"ad-hoc SQL over full history"**: does not fit — that is what warehouses
  and lakehouses are for. No analyst should wait for a log replay to answer
  "revenue by country in 2023."

The mature position: **Kappa for the operational/serving path, batch or
lakehouse for analytics** — which is exactly the "two products" shape of
ch10's real-world variant, minus the healing relationship. The serving path
and the analytics path don't correct each other; they do different jobs.

### The reprocessing decision tree

When logic changes, Kappa gives you exactly one built-in mechanism — replay —
and the cost model decides whether to use it:

```
Did the change affect historical output?
  no  -> rolling deploy, no replay (state carries forward)
  yes -> can raw-plane recompute answer it cheaper?
           yes  -> batch job over raw -> partition overwrite (ch07, ch12);
                   the streaming job keeps only the live path
           no   -> how far back must history be rebuilt?
                    within retention -> replay: new group, shadow table,
                                        validate, swap (the Kappa move)
                    beyond retention -> re-ingest raw into a fresh topic,
                                        then replay as above
```

Walked through with examples, branch by branch:

- **Change that doesn't affect historical output**: e.g., "starting today,
  also record the viewer's app version" or an alert threshold change. Deploy
  the new code; the table's history stays valid; nothing is replayed.
- **Change that affects history, but a raw-plane recompute is cheaper**: the
  bot-exclusion fix is the example. A batch job re-reads the raw event files,
  recomputes counts, and overwrites the affected partitions of the serving
  table (atomic, if the table is a lakehouse format, ch12) — while the
  streaming job keeps serving the live edge.
- **History rebuild within retention**: the classic Kappa move — new consumer
  group from offset 0, shadow table, validate, swap.
- **Beyond retention**: re-ingest raw events into a fresh topic, then replay
  as above.

The branch people miss is the middle one: **Kappa does not obligate you to
replay.** If the fix is expressible as a batch recompute over the raw plane
into the same tables, that is *still* one logical codepath — the same
transformation definition, executed at two batch sizes (a tiny live "batch"
every second, a huge one over history when needed) — and it is usually an
order of magnitude cheaper than re-deriving years of streaming state. Purists
call that Lambda; pragmatists call it the same logic at two batch sizes. Own
that sentence; it is the mature Kappa position.

### Retention sizing arithmetic (rule of thumb)

Retention is a correctness SLA purchased with disk — so do the math out
loud: `peak events/sec x avg event bytes x seconds in window x replication
factor`. At 10k events/sec, 1KB events, 7 days, RF=3:

10,000 events/s x 1KB = 10 MB/s; x 604,800 seconds (7 days) = ~6 TB; x 3
replicas = **~18TB of log — fine** on a modest cluster.

The same formula at 100k events/sec is **~180TB** — suddenly "replayable
window" is a budget line item with a real number attached, and the
raw-archive re-ingest path stops being optional. State it as: *retention
buys the pure-Kappa replay window; beyond it, the raw plane is the replay
mechanism, and the two together are the real architecture.*

## The Options

How to read the table: for each pattern — how many implementations of the
logic exist, how history gets rebuilt, what counts as the source of truth,
and where each pattern's weak spot is.

| Dimension | Lambda | Kappa | Lakehouse unified (ch12) |
|---|---|---|---|
| Codebases | 2 (batch + speed) | 1 (stream) | 1 per concern, shared table |
| Reprocessing | batch recompute | log replay (retention-bound) | snapshot rewrite |
| Source of truth | master dataset (files) | the log | the table (with raw beneath) |
| State | batch views + rt views | changelog/state in log | table snapshots + checkpoints |
| Best for | different latency products | event-native serving | analytics + streaming convergence |
| Weak spot | drift, 2x ops | retention/replay cost | compaction, small files |

## Decision Rules

- **Kappa fits when**: the product is event-native — serving stores fed from
  streams (recommendation features, personalization, live operational views)
  — the logic is expressible as stream transformations, and the rebuild
  window you must support is bounded (or raw-archive re-ingest is
  acceptable).
- **Kappa does not fit when**: heavy full-history ML training or ad-hoc
  analytics dominate the workload. Keep a batch/lakehouse path for those —
  don't apologize; forcing batch work through a stream pays streaming costs
  for batch semantics.
- **Size retention as a correctness SLA**: "we can rebuild N days" must be a
  written, monitored number (retention headroom vs consumer lag, ch05) —
  an unmonitored SLA is just a hope.
- **Reprocess via new consumer group + new table + swap** — never mutate the
  live table in place while validating new logic. If the new logic has a
  bug, a mutated live table is now wrong *and* serving traffic.
- **Use compacted topics for serving-table rebuilds**, and monitor their
  sizes like state — they grow with key cardinality, not with event volume.
- **Say the sentence**: "Kappa is one codepath plus operations discipline."
  The code is the easy part; the architecture lives in the ops — retention
  sizing, replay capacity, swap discipline. That is the interview one-liner.

## Failure Modes

- **Retention shorter than the outage**: the consumer is down 9 days,
  retention is 7 — on restart it replays everything the log still holds,
  silently missing the first 2 days. Symptom: the rebuilt table reconciles
  *except* for a mysterious 2-day gap. Prevention: alert when consumer lag
  approaches retention headroom.
- **Replay storm**: a reprocess run reading from offset 0 at full speed
  competes with the live path for the same brokers and partitions — live
  lag spikes during every rebuild. Rate-limit replays.
- **State re-derivation cost surprise**: "it's just replay" meets a 3-year
  keyed-aggregate rebuild that takes a week of compute. Do the arithmetic
  before promising a rebuild timeline.
- **The analytics creep**: ad-hoc analysts pointed at serving stores because
  "everything is Kafka" — scan-heavy analytical queries degrade a path that
  was sized for fast point lookups (ch16). Point analytics at the
  lakehouse, not the serving table.
- **Unbounded compacted topics**: key cardinality grows forever (every video
  ever published, every user ever seen), so the "changelog" quietly becomes
  the biggest topic in the cluster. Monitor size and key growth.

## Interview Narration

"Kappa is Jay Kreps' answer to Lambda's two-codebase problem: make the log the
source of truth, make every serving table a materialized view of it, and run
one stream-processing codebase. Reprocessing becomes replay: new consumer
group from the earliest offset, write to a new table, validate, swap. No
drift, because there's nothing to drift — one definition of the logic.

The catch everyone should name is retention: Kafka holds days-to-weeks, not
years, so 'replay from zero' only covers the retention window — which turns
retention sizing into a correctness SLA, and beyond it I'm re-ingesting from
the immutable raw archive. Replay costs more when the logic has to rebuild a lot of state: reprocessing a simple enrichment is cheap, but recalculating three years of per-key totals takes substantial computing power—a job Lambda handles in its batch layer.

So my placement is precise: Kappa is the right shape for event-native serving
paths — real-time features, operational views — where logic is a stream
transformation and rebuild windows are bounded. It is not a replacement for
the analytics warehouse: model training over full history and ad-hoc SQL are
batch workloads, and forcing them through a stream is paying streaming costs
for batch semantics. Which is why the mature version of Kappa in most
companies is: one streaming codepath for serving, a lakehouse underneath, and
raw replay as the recovery story that ties them together."

---

| <- Previous | Next -> |
|---|---|
| [Chapter 10 — Lambda Architecture](ch10-lambda-architecture.md) | [Chapter 12 — Lakehouse & Table Formats](ch12-lakehouse-and-table-formats.md) |
