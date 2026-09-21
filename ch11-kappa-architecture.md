# Chapter 11 — Kappa Architecture

> Part IV — Architecture Patterns

Kappa is Jay Kreps' (Kafka's creator) answer to Lambda's two-codebase disease:
if the log is the source of truth and everything is a stream transformation,
you only need one codepath. Elegant — with one famous catch: reprocessing.

## The Question

*"Can I run ONE processing codebase — no batch layer, no duplicated logic —
and still get correct, rebuildable results over history?"*

## The Physics

### The core claim: the log is the database

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

Kappa in one sentence: **serving tables are materialized views of the log, and
reprocessing is replaying the log with new logic into a new table, then
swapping.**

Every downstream artifact — a KV serving store, an aggregate table, an
Elasticsearch index — is *derivable* by consuming the log. If logic changes,
don't mutate the live table: spin up a second consumer group from the beginning,
write to `table-v2`, validate, then atomically switch readers. One codebase,
one paradigm, no drift (ch10's disease).

### The reprocessing problem — the famous catch

Replay requires the data to still be there. Three layers of constraint:

1. **Retention window**: Kafka retention is days-to-weeks (default ~7 days), not
   years. Replay-from-zero only covers what the log still holds.
   Retention sizing becomes an *architecture decision*: "replayable window"
   is a correctness SLA (how far back can we rebuild?) purchased with disk.
2. **Beyond retention: re-ingest from raw archive**. The raw plane (ch03/06)
   exists precisely for this: replay raw events into a fresh topic (or into the
   job's input), rebuild from there. Kappa purists dislike this ("that's just
   batch re-ingestion!") — the pragmatic answer is that the *codepath* is still
   one: the same streaming logic, replayed; only the source is the archive.
3. **State size vs stateless transformation**: replaying a filter/enrichment is
   cheap; replaying a 3-year keyed aggregate means re-deriving all state — the
   compute cost Lambda's batch layer amortized, now paid as one giant
   replay.

This is the honest center of every Kappa discussion: **Kappa moves the
reprocessing cost from architecture (a second layer) into operations (log
retention + replay capacity).** Whether that's a win depends on how often you
reprocess and how big your history is.

### Compacted topics as state

Log compaction (ch05) keeps the latest record per key — a changelog, not
history. Two Kappa-specific uses:

- **Rebuilding a serving table** = re-consume the compacted topic, latest-per-key
  wins, finish with a fully populated table. This is Kappa's version of
  "restore from backup."
- **Changelog-backed state**: the processor's own state is itself a compacted
  topic (Kafka Streams) — state and log are the same technology, and recovery
  is re-consumption. Very elegant; also the first place to check when state
  grows unbounded.

### What Kappa assumes about your computation

Kappa works when the business logic is expressible as **stream transformations
over an append-only log**:

- event -> enriched event: perfect fit.
- event -> per-key aggregate with windows: fits (with state machinery, ch08).
- "train a model over 3 years of history": does not fit — that's a batch
  workload over bounded data; pretending it's an infinite stream wastes the
  streaming engine's strengths and pays its costs.
- "ad-hoc SQL over full history": does not fit — that's what warehouses are.
  Kappa systems still end up with a warehouse or lakehouse for analytics; the
  Kappa claim properly applies to the *operational* serving path, not the
  whole platform.

The mature position: **Kappa for the operational/serving path, batch/lakehouse
for analytics — which is exactly the "two products" shape of ch10's real-world
variant, minus the healing relationship.**

### The reprocessing decision tree

When logic changes, Kappa gives you exactly one mechanism — replay — and the
cost model decides whether to use it:

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

The branch people miss is the middle one: **Kappa does not obligate you to
replay.** If the fix is expressible as a batch recompute over the raw plane
into the same tables (which lakehouse formats make atomic, ch12), that is
*still* one logical codepath — the streaming job for the live edge, the
replay-shaped-as-batch for history — and it is usually an order of magnitude
cheaper than re-deriving streaming state over years. Purists call that
Lambda; pragmatists call it the same transformation definition executed at
two batch sizes. Own that sentence; it is the mature Kappa position.

### Retention sizing arithmetic (rule of thumb)

Retention is a correctness SLA purchased with disk — do the math out loud:
`peak events/sec x avg event bytes x seconds x replication factor`. At
10k events/sec, 1KB events, 7 days, RF=3, that is ~18TB of log — fine. The
same formula at 100k events/sec is ~180TB — suddenly "replayable window"
is a budget line, and the raw-archive re-ingest path stops being optional.
State it as: *retention buys the pure-Kappa replay window; beyond it, the
raw plane is the replay mechanism, and the two together are the real
architecture.*
## The Options

| Dimension | Lambda | Kappa | Lakehouse unified (ch12) |
|---|---|---|---|
| Codebases | 2 (batch + speed) | 1 (stream) | 1 per concern, shared table |
| Reprocessing | batch recompute | log replay (retention-bound) | snapshot rewrite |
| Source of truth | master dataset (files) | the log | the table (with raw beneath) |
| State | batch views + rt views | changelog/state in log | table snapshots + checkpoints |
| Best for | different latency products | event-native serving | analytics + streaming convergence |
| Weak spot | drift, 2x ops | retention/replay cost | compaction, small files |

## Decision Rules

- **Kappa fits when**: the product is event-native (serving stores fed from
  streams), logic is stream-transformable, and rebuild window needs are
  bounded (or raw-archive re-ingest is acceptable).
- **Kappa does not fit when**: heavy full-history ML training or ad-hoc
  analytics dominate — keep a batch/lakehouse path for those; don't apologize.
- **Size retention as a correctness SLA**: "we can rebuild N days" must be a
  written number, monitored (consumer lag vs retention headroom, ch05).
- **Reprocess via new consumer group + new table + swap** — never mutate the
  live table in place while validating new logic.
- **Compacted topics for serving-table rebuilds**; monitor their sizes like
  state.
- **Say the sentence**: "Kappa is one codepath plus operations discipline" —
  the ops discipline *is* the architecture.

## Failure Modes

- **Retention shorter than the outage**: consumer down 9 days, retention 7 —
  replay from zero silently covers only 7. Symptom: rebuilt table reconciles
  except for a mysterious 2-day gap.
- **Replay storm**: a reprocess run reading from offset 0 at full speed,
  competing with the live path for the same brokers/partitions — the live
  path's lag spikes during every rebuild. Rate-limit replays.
- **State re-derivation cost surprise**: "it's just replay" meets a 3-year
  keyed-aggregate rebuild that takes a week of compute.
- **The analytics creep**: ad-hoc analysts pointed at serving stores because
  "everything is Kafka" — the serving path degrades under scan traffic it was
  never sized for (ch16).
- **Unbounded compacted topics**: key cardinality grows forever; the
  "changelog" quietly becomes the biggest topic in the cluster.

## Interview Narration

"Kappa is Jay Kreps' answer to Lambda's two-codebase problem: make the log the
source of truth, make every serving table a materialized view of it, and run
one stream-processing codebase. Reprocessing becomes replay: new consumer
group from the earliest offset, write to a new table, validate, swap. No
drift, because there's nothing to drift — one definition of the logic.

The catch everyone should name is retention: Kafka holds days-to-weeks, not
years, so 'replay from zero' only covers the retention window — which turns
retention sizing into a correctness SLA, and beyond it I'm re-ingesting from
the immutable raw archive. And I'm honest that replay cost scales with what
the logic accumulates: replaying an enrichment is cheap; re-deriving three
years of keyed aggregates is the compute that Lambda's batch layer used to
amortize.

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