# Chapter 10 — Lambda Architecture

> Part IV — Architecture Patterns

Lambda is the architecture everyone loves to criticize and almost everyone
still runs some version of. In one sentence: **Lambda answers the question
"I need fast answers AND exactly-right answers from the same events" by
running two systems side by side** — a slow, exact one and a fast,
approximate one — and combining their answers whenever someone asks a
question.

To handle it well in an interview you need to understand the *problem it
solved in 2014* — because that problem (cheap reprocessing did not exist) is
exactly what modern table formats erased (ch12), and the history is what
makes the erasure intelligible.

## The Question

*"I need both: low-latency answers now AND exactly-correct answers over the
full history. One system can't give me both. Now what?"*

That question is denser than it looks, so let's unpack it with the running
example for this chapter. Suppose you run a video site, and the number
everyone cares about is **views per video**.

- **"Low-latency answers now."** The content team is watching a dashboard
  during a product launch. When a creator publishes a video, its view count
  should update on the dashboard within seconds. To them, a number that only
  refreshes overnight is useless.
- **"Exactly-correct over the full history."** Finance pays creators based on
  these views. They need counts rebuilt from the raw event log: every event
  accounted for, duplicates handled, no approximations, covering every day
  since the company started. "Roughly 1,000,003" is not acceptable when money
  moves.

Why couldn't one system do both in 2014? Because the two systems of that era
each gave you exactly one half:

- **Batch systems (MapReduce, early Hive)** read *all* the raw events from
  disk and recompute the answer from scratch. That is exact — but the job
  takes hours, so the freshest number the dashboard can show is always hours
  old.
- **Streaming systems (Storm-era)** update the number within a second of each
  event arriving. But in 2014 they were approximate: if a machine crashed
  mid-update, some events got counted twice and others not at all, and there
  was no clean way to prove the total was right. Fine for a dashboard; not
  fine for payouts.
- And **reprocessing history through a streaming system was brutally
  expensive**: a streaming job keeps a counter in memory for every key it has
  seen. To make it produce "counts over all history," you would have to
  replay every event ever written and hold a counter for every video ever
  published. 2014 hardware said no.

Lambda's move: **stop trying to make one system do both.** Run both systems,
give each the job it is actually good at, and combine their answers at query
time.

## The Physics

### The 2014 context — why Lambda existed

Nathan Marz's problem statement, spelled out: batch gives correctness over
all history but is slow; streaming gives freshness but was (then) approximate
and hard to make correct; reprocessing history through a streaming system was
brutally expensive. His solution: **run both, and make the fast one
*disposable*.**

"Disposable" is the word that carries the whole design. The streaming layer's
numbers are never treated as the final truth. They are temporary stand-ins
that only need to be roughly right *until the next batch run replaces them
with exact ones*. When the fast layer is wrong, nobody fixes it by hand — it
just gets overwritten.

### The three layers

```
                         +---------------------+
   events -------------> |  MASTER DATASET     |  (immutable, append-only,
                         |  (raw, on HDFS/S3)  |   forever)
                         +----------+----------+
              +---------------------+---------------------+
              v                                           v
   +---------------------+                    +---------------------+
   |  BATCH LAYER        |                    |  SPEED LAYER        |
   |  recompute views    |                    |  incremental views  |
   |  over ALL history   |                    |  over recent hours; |
   |  (mapreduce/spark)  |                    |  approximate, cheap |
   +----------+----------+                    +----------+----------+
              v                                          v
   +---------------------+                    +---------------------+
   |  BATCH VIEWS        |                    |  REALTIME VIEWS     |
   +----------+----------+                    +----------+----------+
              v                                          v
              +--------------------+--------------------+
                                   v
                         +---------------------+
                         |  SERVING LAYER      |  query: merge batch + rt
                         |  (HBase/Druid/...)  |  at read time
                         +---------------------+
```

The same diagram, walked through with the views-per-video example:

- **Batch layer**: holds the **master dataset** — every view event ever
  received, stored raw and immutable (append-only, never edited: the same raw
  plane as ch03/06) on HDFS or S3. On a schedule (say, nightly), a
  MapReduce/Spark job recomputes **batch views** from *all* of history —
  "views per video per day, from 2013 through yesterday."
- **Speed layer**: consumes the *same* events incrementally (from Kafka, as
  they arrive) and maintains **real-time views** over the last few hours
  only — "views per video today so far." Its entire job is to fill the gap
  between "now" and "the last batch recompute" — the hours where the batch
  view has no data yet.
- **Serving layer**: stores both views and answers queries by merging them.
  When someone asks "how many views does this video have?", the answer is
  batch view (through yesterday) + real-time view (today so far). The merge
  happens *at query time*, not in advance.

### The core insight: the speed layer is disposable

Speed-layer errors are constantly healed. Suppose a client retries during a
network blip and the speed layer double-counts 500 views for a video. That
mistake is not permanent: tonight the batch layer recomputes the full count
from the raw event log, and its exact number overwrites the wrong one. The
error lived for a few hours and then vanished. In Lambda vocabulary, the
batch layer *heals* the speed layer — scheduled recompute is the
architecture's self-repair mechanism.

**Correctness is maintained by scheduled recompute; latency is bought with a
layer that's allowed to be wrong.** In beginner terms: you trust the system's
numbers *eventually*, because a scheduled job periodically recomputes
everything exactly; you get *fresh* numbers because a component whose
mistakes are tolerated on purpose fills the gaps in between. The deal being
struck is "slightly wrong for a few hours" in exchange for "fresh within
seconds."

That is the elegant idea — and also the origin of every Lambda complaint,
because the healing mechanism is *two implementations of the logic*. For the
batch layer to heal the speed layer, both must compute the *same metric*, so
someone writes "count views per video" twice — once per system — and must
keep those two programs in agreement forever. Every classic Lambda pain point
grows from that one fact.

### Why it hurts (the standard critique, stated precisely)

1. **Two codebases in two paradigms.** The "count revenue per minute" logic
   exists twice: once as a batch SQL job over the warehouse, and once as a
   streaming topology with its own way of managing state, its own API, and
   its own testing tools. These are not two copies of the same file; they are
   two programs written in two different programming models. Every change to
   the metric gets implemented twice, reviewed twice, and deployed twice.

2. **Logic drift.** Because the two implementations are hand-maintained, they
   *will* disagree sometimes. Concrete example: in October, finance decides
   "revenue" must exclude refunded orders. The speed layer gets the fix on
   Friday; the batch job's fix ships Monday. Between the two, the serving
   layer adds a batch total (old definition) to a speed total (new
   definition) — producing a number that matches *neither* definition.
   Symptom in production: dashboards whose "today" total shifts when the
   batch heals it — the number visibly changes overnight with no event — so
   the business learns to distrust the morning numbers.

3. **Double operations.** Two pipelines to deploy, monitor, and page on — and
   they fail differently. Batch jobs fail by timing out or writing partial
   output; streaming jobs fail by lagging, crashing, or silently corrupting
   their state. That is two failure zoos, two runbooks, and 2x the on-call
   surface for one business metric.

4. **Serving-layer merge complexity.** "How many views does this video
   have?" is answered by *adding two numbers from two tables* on every query.
   That merge has correctness rules of its own — the biggest one being: for a
   time range covered by both views (the *overlap window*), which view wins?
   Get it wrong and events double-count or vanish (see "The serving merge,
   concretely" below).

### When Lambda is still the right call

- **Genuinely different latency products from the same events.** Example: a
  payments company needs (a) a sub-second fraud decision while a card is
  being swiped, and (b) an exact end-of-day reconciliation of every
  transaction for accounting. The fraud check's output is a risk score
  attached to a live request; the reconciliation's output is an audit-grade
  report. Different artifacts, different consumers, different reliability
  guarantees. Building two paths here is honest engineering — not duplicated
  logic but *different* logic.
- **Reprocessing remains genuinely expensive** at your scale/compute budget.
  Example: five years of clickstream, where the metric requires stitching
  user sessions together across days of events. Recomputing that frequently
  means paying days of cluster time per day — not realistic. When exact
  recompute is that costly, a speed layer that heals by recompute is still
  rational.
- **Regulatory/event-driven hybrid needs**, where the batch layer serves
  audit (exact, reproducible numbers you can defend to a regulator) and the
  speed layer serves live operations (on-call dashboards, alerting).

### The real-world variant (what teams actually run)

Most "Lambda" systems in the wild are not Marz's symmetric design. What
teams actually have is:

- a **batch warehouse pipeline** — the primary product: nightly jobs feeding
  BI dashboards, finance reporting, and ML training tables; plus
- a **separate real-time feature/alert path** (Kafka -> Flink -> Redis) that
  powers fraud checks or live recommendations — which is *not* healed by
  recompute. The nightly job never overwrites the fraud scores; the two paths
  simply produce different things.

That is not Lambda; that is **two pipelines for two requirements** — which is
fine and common. The distinction matters because Lambda's whole critique
(two implementations of one metric drifting apart, two views merged at query
time) only applies when the fast path is a *mirror* of the batch path. The
interview discipline: ask whether the fast path's output is *healed by* the
batch path (Lambda), or *independent of it* (two products). The answer
changes what you criticize.

### Designing the speed layer — the part Lambda answers skip

The speed layer is not "a Flink job"; it has four design decisions of its
own:

1. **Horizon: how far back it maintains state.** The speed layer only needs
   to cover the gap between "now" and the last batch recompute. If batch
   recomputes at 02:00 and 14:00, the worst-case gap is ~12 hours, so ~13
   hours of state suffices — hours, not days. Keep the horizon short: every
   hour of horizon is *standing state* (ch08) — counters, windows, and dedup
   records held in memory or Redis around the clock — paid 24/7 whether or
   not events are flowing.

2. **Overlap window: exactly which rows come from batch versus speed.** The
   clean rule: **speed owns the open window; batch owns everything sealed.**
   Unpacked: a time bucket is *sealed* once the batch layer has recomputed it
   (say, everything up to 23:59:59 UTC yesterday); it is *open* while it can
   still change (today so far). Queries read sealed buckets from the batch
   view and the open bucket from the real-time view — never both for the same
   bucket. Ambiguity here is the double-counting bug: a bucket covered by
   both views and summed counts its events twice; a bucket covered by
   neither loses its events entirely.

3. **Approximation budget: what is the speed layer *allowed* to get wrong?**
   Decide it deliberately and write it down. Examples: distinct-viewer counts
   may use HLL (HyperLogLog — a small sketch that estimates distinct counts
   to within ~1-2% using kilobytes instead of gigabytes of memory);
   duplicate events within the window may go un-deduplicated; the top-10
   list may lag a few seconds. State it as a budget, because the batch layer
   heals every one of these on the next recompute — that is the
   architecture's contract (and its only excuse for the second codebase).

4. **Disposability drill: rehearse losing the speed layer.** If it died and
   restarted empty, the system degrades to "batch-fresh": queries still
   return correct-but-stale numbers (as of the last recompute) while the
   speed layer replays the log and rebuilds. That is *by design*, and the
   runbook should say so (ch22 §6): restart it, let it replay, no incident.
   A speed layer whose loss is a P1 contradicts its own architecture — if
   the approximate layer is that load-bearing, it was never disposable.

### The serving merge, concretely

The classic implementation: batch views and real-time views live in the same
KV store (HBase, Druid, Redis), and a query is literally
`batch_view.get(key) + rt_view.get(key)` — batch's "views through yesterday"
plus speed's "views today." That one-line merge only works if the two views
are **additive by construction**: the metric must be one where "part + part
= whole" actually holds. Counts pass: 950 views through yesterday plus 50
views today is exactly 1,000 views until now.

Most metrics people actually care about fail this test, and each one forces
the merge to grow its own logic:

- **Distinct counts**: 1,000 distinct viewers yesterday plus 900 today is
  not 1,900 if 400 of them watched on both days — the true answer is 1,500.
  You cannot add distinct counts; the views would have to store mergeable
  sketches (HLL) instead of plain numbers, and "add" becomes "merge the
  sketches."
- **Averages and ratios**: an average watch time of 4 minutes yesterday plus
  6 minutes today is neither 10 nor 5 — the merge needs the underlying sums
  and counts from both views to recompute the weighted average.
- **Min/max/percentiles**: "the max so far" is max(batch, rt) and works; a
  percentile needs the full distribution and cannot be computed from the two
  stored numbers at all.

Each non-additive metric thus adds a third piece of logic — merge code that
lives in neither the batch nor the streaming codebase and appears in no
architecture diagram. That is where Lambda's hidden third codebase lives.(The serving layer then needs extra logic to combine those results correctly.) 
Say this out loud in interviews; interviewers who have run Lambda will
recognize the scar.

## The Options

How to read the table: for each pattern — who recomputes history, how many
implementations of the logic exist, whether the fast layer gets corrected by
recompute, and what the pattern costs to operate.

| Architecture | Reprocessing | Codebases | Speed-layer healing | Ops |
|---|---|---|---|---|
| Lambda (classic) | batch recompute | two (batch + speed) | yes — that's the point | heavy |
| Kappa (ch11) | log replay | one (stream) | n/a — rebuild views | medium |
| Lakehouse unified (ch12) | snapshot rewrite | one per concern | upserts converge | medium |

## Decision Rules

- **Default to lakehouse-unified (ch12) for the "fresh + exact" combination.**
  Modern table formats made reprocessing cheap: streaming upserts keep one
  ACID (transactional) table fresh, and any range of history can be
  recomputed from it in minutes. The trade Lambda managed —
  fresh-but-approximate versus exact-but-stale — mostly dissolved, so start
  from one system and make yourself justify two.
- **Choose Lambda-shaped only when** the fast artifact and the exact artifact
  are genuinely *different products* (a fraud score versus a reconciliation
  report), or recompute is genuinely unaffordable at your scale and budget.
- **If you run Lambda, own the drift problem explicitly**: don't hand-write
  the metric twice. Define the transformation once (a shared SQL/logic
  library) and compile or generate both runtimes from it, plus automated
  reconciliation checks over the overlap window, so drift pages you before
  the business notices.
- **Name the serving merge** in any Lambda answer: which view answers queries
  for the overlap window, and what artifact (a reconciliation dashboard, a
  daily diff report) proves the two views actually agree.
- **In interviews, tell the history**: “Lambda was useful when recomputing history was too slow or costly to provide fresh results. Modern table formats make incremental updates and corrections easier, so I’d first see whether one pipeline can meet both freshness and correctness needs. If full recomputation is still too expensive, or the fast and exact outputs are genuinely different products, Lambda may still be justified.” — that one sentence shows you understand both why the pattern was
  invented and why it is fading, which is the senior version of the whole
  chapter.

## Failure Modes

- **Silent logic drift between layers**: the classic. Nothing alerts,
  because both pipelines are individually healthy — they just implement
  slightly different logic. The only symptom is on the business side:
  dashboards "settle" when batch heals them (numbers change overnight with
  no event). Nobody budgets for the distrust this breeds — once analysts
  learn that morning numbers are provisional, they stop using them at all.
- **Serving-layer merge bugs**: for "the last 2 hours," neither view is
  authoritative, so naive batch+rt merges double-count (the event landed in
  both) or gap (it landed in neither). These bugs are intermittent and
  time-shaped, which makes them miserable to debug.
- **Speed layer promoted to load-bearing**: management loves the realtime
  dashboard; executives start quoting it in meetings; it silently becomes
  the product. Nobody remembers it was designed to be approximate and
  disposable. Then its failure is a P1 — a company-level incident — for a
  component whose design contract was "it's okay to be wrong."
- **Two of everything**: 2x pipelines, 2x bugs, 2x pager — the ops bill that
  the 2014 blog posts underplayed.
- **Cargo-cult Lambda**: adopting the pattern "for scale" by imitation, when
  a single well-indexed warehouse plus incremental models would have met
  every stated requirement. Before choosing Lambda, name the requirement
  that one system cannot meet; if you cannot, it's cargo cult.

## Interview Narration

"Lambda answers a real question: how do I get low-latency answers and
exactly-correct answers over full history from the same events, when
reprocessing history is expensive? The 2014 answer: run a batch layer that
recomputes views from an immutable master dataset, a speed layer that
maintains disposable approximate views over the last few hours, and a serving
layer that merges them at query time. The elegant part is the disposal: the
speed layer is allowed to be wrong because the next batch recompute heals it.

The cost is also the design: the same business logic in two paradigms, which
drifts — so the merge window disagrees until healed — and two of everything
operationally.

Today my honest default is the lakehouse pattern instead: streaming upserts
into an ACID table format give me fresh reads and exact reads from *one*
table, because reprocessing became cheap — that's the historical punchline:
Iceberg and Delta dissolved the trade Lambda was built to manage. **I'd still
choose a Lambda-*shaped* answer when the fast artifact and the exact artifact
are genuinely different products** — fraud alerts versus end-of-day
reconciliation — because then it's not duplicated logic, it's two requirements
with two SLAs. And **I'd be careful calling a batch pipeline plus a real-time
feature path 'Lambda': if the fast path isn't healed by recompute, it's just a
second pipeline, and it should be owned and monitored as one.**"

---

| <- Previous | Next -> |
|---|---|
| [Chapter 09 — Micro-batch](ch09-micro-batch.md) | [Chapter 11 — Kappa Architecture](ch11-kappa-architecture.md) |
