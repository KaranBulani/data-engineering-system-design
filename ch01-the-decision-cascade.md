# Chapter 01 — The Decision Cascade

> Part I — Foundations: How Decisions Get Made

Every data engineering system design interview is the same conversation in disguise:
somebody hands you a business problem, and you have 45 minutes to make six or seven
interlocking technology decisions, out loud, with reasons. This chapter is about the
*order* in which you make them — because the order is not optional, and getting it
wrong is the single most common way candidates fail.

## The Question

*"Given this business problem, how do I go from a vague ask to a defensible
architecture — and in what order do I make the decisions so they don't contradict
each other?"*

The interview version: you are asked to design something like "a pipeline that
powers fraud alerts" or "the analytics backend for a food-delivery app." The
junior move is to name tools. The senior move is to walk a cascade.

## The Physics

### Decisions constrain each other

A data architecture is not a bag of independent choices. Each decision narrows the
space of sensible next decisions:

```
business ask
    |
    v
REQUIREMENTS      latency SLA, volume, correctness, cost, ops maturity
    |
    v
PARADIGM          batch  vs  streaming  vs  micro-batch
    |
    v
ARCHITECTURE      lambda  vs  kappa  vs  lakehouse (unified)
    |
    v
STORAGE           SQL vs NoSQL, per workload (OLTP, serving, analytics)
    |
    v
FORMAT & TUNING   row vs columnar, Parquet/Avro/ORC, partitioning, file sizing
    |
    v
SERVING           warehouse-direct, marts, KV serving, search
    |
    v
THE -ILITIES      security, orchestration, data quality, cost
```

Concrete example of the constraint chain:

- If the SLA is "fraud alert within 2 seconds of the transaction" — that is a
  **streaming** decision. You cannot meet it with hourly batch, no matter how
  clever the SQL.
- Streaming then forces the **architecture** question: how do I also get correct
  historical views? Lambda, Kappa, or lakehouse upserts.
- If you choose streaming upserts into a lakehouse table, the **storage** decision
  is already half-made (a table format on object storage), and the serving layer
  for analysts reads from that same table.
- The **format** follows: Parquet at rest, Avro/Protobuf on the wire, because
  streaming writes + batch reads is exactly the workload those two split.

Notice: nobody picked "Kafka because I like Kafka." Each choice was forced by a
requirement plus the previous choice. That is what interviewers mean when they say
a candidate is "structured."

### Decision half-life

Early decisions live longer than late ones. Requirements and paradigm choices are
load-bearing walls — changing batch to streaming later is a rebuild. Storage
engines are painful but movable. File formats and table schemas change weekly.
Spend your interview minutes proportionally to half-life: fight about the paradigm,
not the serialization library.

### The tool-first anti-pattern

Choosing tools before requirements inverts the cascade. Symptoms: an architecture
that is a logo collage (Kafka + Flink + ClickHouse + dbt) with no sentence
explining *why any of them*. Every tool is defensible in isolation; the failure is
that the choices are unconnected. The interviewer's silent follow-up is always the
same: "OK, but what did you give up?" If you cannot answer what you gave up, you
did not make a trade-off — you made a wish list.

### Requirements are the only forcing function

Three requirement axes do almost all of the cascade-driving work:

1. **Latency** (data arrival -> queryable/actable) decides batch vs streaming.
2. **Correctness** (approximate-now vs exact-eventually) decides whether you need
   reprocessing, and hence Lambda/lakehouse vs pure Kappa.
3. **Volume + access pattern** decides storage engines and formats.

Cost and ops maturity act as *governors*: they do not change the direction of the
cascade, but they cap how exotic you are allowed to get.

## The Options

There is no "options table" for the cascade itself — it is the frame the rest of
the book hangs on. But here is the honest map of where each later chapter plugs in:

| Cascade level | The decision | Chapter |
|---|---|---|
| Requirements | What axes matter, what to ask | 02 |
| Ingestion | Pull vs push, replay, raw archive | 03-06 |
| Paradigm | Batch vs streaming vs micro-batch | 07-09 |
| Architecture | Lambda vs Kappa vs lakehouse | 10-12 |
| Storage | SQL vs NoSQL per workload | 13-15 |
| Serving | How consumers touch the data | 16 |
| Format | Row vs columnar, which format where | 17-18 |
| Compute tuning | Spark mechanics | 19 |
| -ilities | Security, orchestration, quality, runbooks | 20-22 |
| Execution | Running the room, worked cases | 23-24 |

And the three candidate "shapes" almost every interview answer collapses into:

| Shape | When it appears | Rough sketch |
|---|---|---|
| Batch-first lakehouse | Analytics/BI dominant, latency in minutes-hours | sources -> raw zone -> scheduled transforms -> warehouse/lakehouse -> BI |
| Stream-first with lakehouse truth | Alerting/features + analytics | sources -> queue -> stream processor -> serving KV + table upserts -> BI |
| Two-speed (Lambda-ish) | Hard correctness + hard latency at heavy scale | batch recompute path + speed path, merged at serving |

### The cascade in ninety seconds — a worked trace

Take one prompt end-to-end: *"Design alerts for suspicious login attempts."*

- **Level 0 (requirements):** Who acts on an alert? Security analysts, on call. How fast after the attempt? "Within a minute" — that single number is the paradigm decision. Volume: 20k logins/sec peak. Correctness: a missed alert is bad, a false alert is tolerable — approximate-now, exact-later is acceptable. PII: credentials and IPs — yes.
- **Level 1 (paradigm):** 60 seconds latency → micro-batch or streaming. Per-user velocity windows ("5 failed logins in 2 min") at 20k events/sec → streaming; ch08's engine choice would sharpen it by state needs.
- **Level 2 (architecture):** the alert is one product; the exact daily security report is another → the two-products shape (ch10, ch24 Case 1), with a table format unifying the analytics side (ch12).
- **Level 3 (storage):** alert path → KV serving (ch14); analytics → lakehouse/warehouse (ch13).
- **Level 4 (format):** Avro on the wire from the auth events; Parquet at rest (ch18).
- **Level 5 (serving + -ilities):** alerts UI reads the KV; analysts read the warehouse; PII tokenization at ingestion (ch20); freshness SLO on alert lag (ch21, ch22).

Each arrow above is a sentence you say out loud. The full trace takes about ninety seconds, and the interviewer has heard the entire architecture *with its reasons* before any deep-dive begins.

### Cascade levels as checkpoints

Interviewer pullback maps to a level — recognize it and answer at the right altitude instead of re-deriving the whole chain:

| Interviewer says... | They're pulling you to level... |
|---|---|
| "What's the latency requirement?" | 0 — requirements (ch02) |
| "Why streaming?" | 1 — paradigm (ch07-09) |
| "How do you handle reprocessing?" | 2 — architecture (ch10-12) |
| "Where does that live?" | 3-4 — storage/format (ch13-18) |
| "How do analysts get this?" | 5 — serving (ch16) |
| "What happens when it breaks at 3am?" | 6 — operations (ch22) |
## Decision Rules

- **Never name a tool until you have stated the requirement that forces it.** If
  you catch yourself saying a product name first, stop and back up one level.
- **Walk the cascade top-down in the interview**, and narrate the transitions:
  "given seconds-level latency, this becomes a streaming problem; given the
  correctness need, I need reprocessing, which pushes me toward..."
- **Latency decides paradigm; correctness decides architecture; volume + access
  pattern decide storage and format; cost and team decide how fancy you get.**
- **Make each decision reversible-in-principle but commit out loud.** State what
  would change your mind: "if the SLA were 5 minutes instead of 5 seconds, I would
  drop the speed layer."
- **Budget minutes by half-life.** ~5 min requirements, ~5 high-level flow,
  ~20 deep-dives on 2-3 cascade levels, ~10 failure modes and -ilities, ~5 wrap.
- **When the interviewer pushes back, treat it as new information entering the
  cascade**, not as an attack: re-walk from the level their pushback hits.

## Failure Modes

- **Tool-first design.** Symptom: logos before requirements. Consequence: no
  coherent answer to "why X over Y?", because Y was never considered.
- **Designing for scale you were not given.** 10 events/day does not need Kafka.
  Interviewers dock points for inventing requirements; real companies burn actual
  money doing this.
- **Contradictory choices.** Streaming latency SLA, then an hourly warehouse-only
  serving layer. The chain broke in the middle and nobody noticed — because nobody
  walked the chain.
- **Solving only one level.** A perfect streaming answer with no answer to "and
  how do analysts query it?" Serving is part of the system (chapter 16).
- **Ignoring the governors.** A three-person data team operating Flink + Kafka +
  ClickHouse + Airflow + custom CDC is an on-call death sentence. Ops maturity is
  a requirement, not a footnote.
- **No failure-mode discussion.** If you never volunteer "here is what breaks,"
  the interviewer assumes you have never operated anything.

## Interview Narration

"Before I draw anything, I want to pin down requirements, because they drive every
downstream choice. The two that matter most to me are latency — from an event
happening to it being usable, what's the budget? — and correctness: is
approximately-right-now, exactly-right-eventually acceptable, or do we need strong
guarantees? Then volume and access pattern, because those pick my storage, and
finally cost and team size, because those cap how exotic I'm allowed to get.

Once I have those, I'll work top-down: the latency budget decides batch versus
streaming; the correctness requirement decides whether I need a reprocessing path,
which is really the Lambda-versus-Kappa-versus-lakehouse question; volume and
access pattern then pick the storage engines; and format follows storage. Each
choice narrows the next one, so I'll narrate the transitions as I go — that way,
if you push back on any level, we can re-walk from there rather than redrawing
everything."

That narration does four senior things in 40 seconds: it shows you know what drives
what, it sets the agenda for the whole interview, it invites pushback at a level
you can defend, and it has not named a single product.

---

| <- Previous | Next -> |
|---|---|
| — | [Chapter 02 — The Requirements That Drive Everything](ch02-requirements-that-drive-everything.md) |
