# Chapter 10 — Lambda Architecture

> Part IV — Architecture Patterns

Lambda is the architecture everyone loves to criticize and almost everyone
still runs some version of. To handle it well in an interview you need to
understand the *problem it solved in 2014* — because that problem (cheap
reprocessing did not exist) is exactly what modern table formats erased
(ch12), and the history is what makes the erasure intelligible.

## The Question

*"I need both: low-latency answers now AND exactly-correct answers over the
full history. One system can't give me both. Now what?"*

## The Physics

### The 2014 context — why Lambda existed

Nathan Marz's problem statement: batch gives correctness over all history but
is slow; streaming gives freshness but was (then) approximate and hard to make
correct. Reprocessing history through a streaming system was brutally
expensive. So: run both, and make the fast one *disposable*.

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

- **Batch layer**: master dataset (immutable raw — the same raw plane as ch03/06)
  plus periodic recomputation of views over all (or recent) history.
- **Speed layer**: consumes the *same* events incrementally, maintaining views
  over the last few hours only. Its job is to fill the gap between "now" and
  "the last batch recompute."
- **Serving layer**: at query time, answer = batch view + real-time view
  (e.g., daily counts from batch + today's counts from speed layer).

### The core insight: the speed layer is disposable

Speed-layer errors are constantly healed: the next batch recompute overwrites
whatever the speed layer got approximately wrong. **Correctness is maintained
by scheduled recompute; latency is bought with a layer that's allowed to be
wrong.** That is the elegant idea — and also the origin of every Lambda
complaint, because the healing mechanism is *two implementations of the logic*.

### Why it hurts (the standard critique, stated precisely)

1. **Two codebases in two paradigms**: the "count revenue per minute" logic
   exists as batch Java/SQL *and* as streaming topology. Same semantics must
   be hand-maintained in both.
2. **Logic drift**: the speed layer is fixed before the batch layer (or vice
   versa); for one window every day, the two disagree. Symptom: dashboards
   whose "today" total shifts when the batch heals it — business learns to
   distrust the morning numbers.
3. **Double operations**: two pipelines to monitor, two failure modes, two
  sets of bugs, 2x the on-call surface.
4. **Serving-layer merge complexity**: merging batch and real-time views is a
  real query-time join with its own correctness rules (which view wins for the
  overlap window?).

### When Lambda is still the right call

- **Genuinely different latency products from the same events**: a payments
  company needs sub-second fraud *and* exact end-of-day reconciliation. If the
  fast product and the exact product are different artifacts with different
  consumers, two paths is honest engineering — not duplicated logic but
  *different* logic.
- **Reprocessing remains genuinely expensive** at your scale/compute budget:
  rebuilding years of stateful aggregates is days of compute — then a
  speed layer that heals by recompute is still rational.
- **Regulatory/event-driven hybrid needs** where the batch layer serves audit
  and the speed layer serves ops.

### The real-world variant (what teams actually run)

Most "Lambda" systems in the wild are not Marz's symmetric design. They are:

- a **batch warehouse pipeline** (the primary product: BI, finance, reporting),
  plus
- a **separate real-time feature/alert path** (Kafka -> Flink -> Redis), which
  is *not* healed by recompute — it's a different consumer with its own SLA.

That is not Lambda; that is **two pipelines for two requirements** — which is
fine and common. Calling it Lambda confuses the architecture conversation. The
interview discipline: ask whether the fast path's output is *healed by* the
batch path (Lambda), or *independent of it* (two products). The answer changes
what you criticize.

### Designing the speed layer — the part Lambda answers skip

The speed layer is not "a Flink job"; it has four design decisions of its own:

1. **Horizon**: how far back it maintains state — the gap between "now" and
   the last batch recompute. Hours, not days: every hour of horizon is
   standing state (ch08) paid 24/7.
2. **Overlap window**: the serving layer must know exactly which rows come
   from batch versus speed. The clean rule: **speed owns the open window;
   batch owns everything sealed.** Ambiguity here is the double-counting bug.
3. **Approximation budget**: what is the speed layer *allowed* to get wrong?
   Approximate counts (HLL) instead of exact? Missing dedup within the
   window? State it as a budget, because the batch layer heals it on the
   next recompute — that is the architecture's contract (and its only
   excuse for the second codebase).
4. **Disposability drill**: if the speed layer died and restarted empty, the
   system degrades to "batch-fresh" until it catches up. That is *by
   design* — and the runbook should say so (ch22 §6). A speed layer whose
   loss is a P1 contradicts its own architecture.

### The serving merge, concretely

The classic implementation: batch views and real-time views in the same KV
store, query = `batch_view.get(key) + rt_view.get(key)` — which only works
if the two views are **additive by construction** (batch counts through
yesterday; rt counts today). The moment a metric isn't cleanly splittable
(averages, distincts, ratios), the merge needs its own logic — and that is
where Lambda's hidden third codebase lives. Say this out loud; interviewers
who have run Lambda will recognize the scar.
## The Options

| Architecture | Reprocessing | Codebases | Speed-layer healing | Ops |
|---|---|---|---|---|
| Lambda (classic) | batch recompute | two (batch + speed) | yes — that's the point | heavy |
| Kappa (ch11) | log replay | one (stream) | n/a — rebuild views | medium |
| Lakehouse unified (ch12) | snapshot rewrite | one per concern | upserts converge | medium |

## Decision Rules

- **Default to lakehouse-unified (ch12) for the "fresh + exact" combination** —
  table formats replaced the reason Lambda existed.
- **Choose Lambda-shaped only when** the fast artifact and the exact artifact
  are genuinely *different products*, or recompute is genuinely unaffordable.
- **If you run Lambda, own the drift problem explicitly**: same transformation
  definition compiled to both runtimes (shared SQL/logic library), plus
  reconciliation checks in the overlap window.
- **Name the serving merge** in any Lambda answer: which view answers queries
  for the overlap window, and what artifact proves they reconcile.
- **In interviews, tell the history**: "Lambda exists because reprocessing was
  expensive; Iceberg/Delta made it cheap; so today I'd solve this with..." —
  that sentence is the senior version of the whole chapter.

## Failure Modes

- **Silent logic drift** between layers: the classic. Morning dashboards
  "settle" when batch heals them; nobody budgets for the distrust this breeds.
- **Serving-layer merge bugs**: overlap-window queries double-count or gap
  because neither view is authoritative for "the last 2 hours."
- **Speed layer promoted to load-bearing**: management loves the realtime
  dashboard; it silently becomes the product; nobody remembers it was designed
  to be approximate and disposable. Then its failure is a P1 — for a component
  whose design contract was "it's okay to be wrong."
- **Two of everything**: 2x pipelines, 2x bugs, 2x pager — the ops bill that
  the 2014 blog posts underplayed.
- **Cargo-cult Lambda**: adopting it "for scale" when a single well-indexed
  warehouse + incremental models would have served every stated requirement.

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
Iceberg and Delta dissolved the trade Lambda was built to manage. I'd still
choose a Lambda-*shaped* answer when the fast artifact and the exact artifact
are genuinely different products — fraud alerts versus end-of-day
reconciliation — because then it's not duplicated logic, it's two requirements
with two SLAs. And I'd be careful calling a batch pipeline plus a real-time
feature path 'Lambda': if the fast path isn't healed by recompute, it's just a
second pipeline, and it should be owned and monitored as one."

---

| <- Previous | Next -> |
|---|---|
| [Chapter 09 — Micro-batch](ch09-micro-batch.md) | [Chapter 11 — Kappa Architecture](ch11-kappa-architecture.md) |
