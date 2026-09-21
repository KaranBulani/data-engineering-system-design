# Chapter 02 — The Requirements That Drive Everything

> Part I — Foundations: How Decisions Get Made

Interviewers deliberately hand you vague prompts. The vagueness is not an obstacle —
it is the first test: can you extract the handful of numbers and guarantees that
actually shape a data architecture? This chapter is the extraction kit.

## The Question

*"What do I need to know before I'm allowed to draw a box — and how do I get it out
of the interviewer (or stakeholder) in five minutes without sounding like a
requirements checklist robot?"*

## The Physics

### The axes and their units

Seven axes drive essentially every downstream decision. Learn them with units —
numbers with units are the difference between senior and soundbite.

| Axis | Unit / shape | What it decides downstream |
|---|---|---|
| Latency | time from event occurrence to queryable/actable; state p50 vs p99 | batch vs streaming vs micro-batch (ch07-09) |
| Throughput / volume | events/sec peak and sustained; GB/day; growth rate per year | partitioning, file sizing, engine sizing, sharding (ch13-14, 19) |
| Correctness / consistency | eventual vs strong; dedup requirements; late-data tolerance; exactly-once processing | architecture pattern, idempotency design, watermarks (ch08, 10-12) |
| Freshness vs correctness | approximate-now vs exact-eventually tension | speed layer vs batch recompute balance (ch10) |
| Cost | compute spend/mo, storage tiering, egress | engine choice, serverless vs cluster, retention policy |
| Ops maturity | team size, on-call rotation, deepest skill | governor: how many moving parts you may run (ch20-21) |
| Security / compliance | PII present? GDPR/SOC2/HIPAA? data residency? | encryption, tokenization, access model, region pinning (ch20) |

### Latency is the loudest axis

Latency is almost always the axis that picks the paradigm:

| Latency budget | Paradigm | Typical stack shape |
|---|---|---|
| 1s or less | true streaming | Flink/Kafka Streams, in-memory serving |
| 1s - ~1min | streaming or aggressive micro-batch | Spark Structured Streaming small triggers, Kafka consumers |
| ~1min - hours | micro-batch or batch | scheduled Spark/SQL/dbt jobs |
| Next morning is fine | batch | nightly warehouse loads — the cheapest correct answer |

The interview trap is inventing a latency requirement that was never stated. If the
prompt is "executive dashboards," the honest default is hourly-or-daily, not
sub-second. Saying "I'll assume end-of-day freshness unless told otherwise — is
that right?" earns more points than a real-time dashboard nobody asked for.

### Correctness has layers

"Correct" is not one bit. Separate three questions:

1. **Dedup**: can the same event arrive twice? (At-least-once delivery is the
   realistic default; duplication must be handled by idempotent writes or keys.)
2. **Late data**: do events arrive after their window closed? Hours late? Ever?
3. **Reprocessability**: when a bug corrupts a week of aggregates, can you
   recompute from source? From how far back?

If the answer to (3) is "we must be able to rebuild the last 5 years," you have
just decided you need a replayable raw archive — a storage requirement masquerading
as a correctness requirement.

### Volume: absolute numbers, not adjectives

"High volume" is meaningless. 10k events/sec sustained is a modest Kafka topic and
an aggressive Postgres. 1M events/sec is a different engineering universe. Convert
adjectives to numbers out loud: "When you say high-volume clickstream, are we
talking 1k, 10k, or 100k events per second at peak?" The follow-up that impresses:
"and what's the growth rate — 2x a year or 10x?"

### Cost and ops maturity are real requirements

A design that only a 30-person platform team can operate is not wrong — it is
wrong *for a 3-person team*. The same holds for cost: a design that scans the full
history every hour is correct until the warehouse bill arrives. Senior engineers
treat "who operates this and who pays for it" as first-class requirements.

## The Options

### The extraction script (what to actually ask)

In the first five minutes, ask — in roughly this order:

1. "Who consumes the output, and what do they do with it?" (finds serving + latency)
2. "When an event happens, how fast until someone needs to see or act on it?"
   (paradigm)
3. "Roughly what volume — events per second or GB per day — and how fast is it
   growing?" (sizing)
4. "Is approximately-right-now + exactly-right-later acceptable, or do we need
   exact immediately?" (architecture)
5. "Can data arrive late or duplicated? How do we handle it?" (correctness)
6. "Is there PII or compliance scope?" (security)
7. "What does the team look like — just me, or a platform group?" (governor)

You will rarely get all seven answered. That is fine — the move is to *state an
assumption and keep moving*: "I'll assume p99 under a minute, 5k events/sec,
doubling yearly — correct me and I'll adjust."

### Functional vs non-functional for data systems

| Functional (what it does) | Non-functional (how well it does it) |
|---|---|
| sources ingested, transformations, outputs served | latency, throughput, freshness SLO |
| alerting behavior, backfill behavior | correctness, availability, durability |
| data contracts / schema enforcement | cost, operability, security, compliance |

Interviewers probe non-functional requirements on data systems harder than
functional ones, because data engineering failures are usually NFR failures:
silently late data, silently wrong aggregates, silently ballooning cost.

### SLO, SLI, SLA — say them precisely

| Term | Definition | Data example |
|---|---|---|
| **SLI** | the measured indicator | p95 pipeline lag; % of partitions passing quality gates |
| **SLO** | the target you hold yourself to | "freshness p95 < 30 min, on 99% of days" |
| **SLA** | the contractual promise, with consequences | "dashboard data current by 6am or service credits" |

Interviewers listen for SLO discipline because it converts "fast" and "reliable" into numbers a pager can enforce (ch21, ch22). The standard SLI set for data systems: **freshness** (event time → queryable time), **completeness** (rows vs expected), **correctness** (gate pass rate, reconciliation drift), **latency** (p95/p99), **availability** (serving uptime). Latency SLOs always as percentiles — a "30-minute freshness" average that hides a 6-hour Friday tail is a lie with a chart.

### A mock requirements dialogue

> **Interviewer:** Design the analytics pipeline for our food-delivery app.
> **You:** Who's the primary consumer — analysts, dashboards, or product features?
> **I:** Leadership dashboards, mostly. Some analyst queries.
> **You:** When an order happens, how stale can the dashboard be — is next morning acceptable, or do you need intraday?
> **I:** Next morning is fine for the exec view. Analysts want same-day.
> **You:** Volume — orders per day? And is clickstream in scope?
> **I:** ~2M orders/day. Yes, clickstream — maybe 200M events/day.
> **You:** 200M/day averages ~2.3k events/sec with 5-10x meal peaks — that's Kafka-class ingestion (ch05). Orders come from Postgres — CDC or query-pull? And do cancellations need to be reflected — deletes?
> **I:** CDC sounds right. Yes, cancellations matter.
> **You:** Last two: is there PII in scope — delivery addresses? And who operates this — a platform team, or is it three of you?
> **I:** Addresses, yes. Small team — three engineers.
> **You:** Then I'm designing for: nightly-exact exec views plus same-day analyst freshness; CDC with deletes (finance cares about cancellations); 200M events/day to the lake; address tokenization; and an ops budget of three people — which caps moving parts hard. I'll write those in the corner; every choice points back at one.

Five questions, roughly two minutes, and the entire design space is bounded — including the governor (three engineers) that will veto the exotic version of everything downstream.
## Decision Rules

- **Ask latency and volume with numbers; assume out loud when unanswered.**
- **Latency picks the paradigm; correctness picks the architecture; volume +
  access pattern pick storage; cost and team size cap exoticness.**
- **Default latency assumptions by prompt type**: dashboards -> daily/hourly;
  alerting/fraud -> seconds; personalization/features -> 100ms serving, minutes
  refresh; ML training -> hours.
- **Every correctness answer is also a storage answer**: reprocessing needs a
  replayable raw layer. Say so explicitly.
- **Growth rate matters more than current size** — it decides whether you design
  for today's engine with headroom or a different engine entirely.
- **Volunteer security/PII even if unasked** (ch20): one sentence — "if there's
  PII, I'd want column-level access control and tokenization at ingestion."
- **Write assumptions where the interviewer can see them** and keep them visible;
  every later decision should point back at one.

## Failure Modes

- **Assuming instead of asking, silently.** The interviewer cannot correct an
  assumption they never heard, and your whole cascade inherits a fiction.
- **Inventing hero requirements** (sub-second latency for a dashboard) — the
  interviewer hears gold-plating and cost-insensitivity.
- **Treating correctness as a single bit.** Dedup, late data, and reprocessability
  fail differently and are fixed by different mechanisms.
- **NFR blindness.** A functionally complete design with no answer for cost,
  operations, or security reads as junior no matter how sophisticated the boxes.
- **Requirements theater**: 15 minutes of questions with no architecture progress.
  Five minutes of pointed questions, then draw — you can refine while drawing.

## Interview Narration

"Let me pin down requirements first — they drive everything downstream. Two
questions matter most. First, latency: when an event happens, how quickly does
someone need to see or act on it — is this a next-morning report, a few minutes,
or seconds? Second, correctness: is approximately-right-now then
exactly-right-later acceptable, or do we need exact numbers immediately? Then I
want volume — events per second, GB per day, growth rate — because that sizes my
storage and compute. And two quick ones: can data arrive late or duplicated, and is
there PII or compliance scope?

If the answers are 'daily freshness, exact numbers, moderate volume, some PII,'
that's a batch-lakehouse shape and I'll optimize for cost and simplicity. If it's
'seconds, approximate-now acceptable, high volume,' that's a streaming shape and
I'll spend my design time on event-time correctness and serving. Everything I draw
next follows from those numbers — and if any assumption is wrong, tell me now and
the design adapts from the top."

---

| <- Previous | Next -> |
|---|---|
| [Chapter 01 — The Decision Cascade](ch01-the-decision-cascade.md) | [Chapter 03 — The Ingestion Taxonomy](ch03-the-ingestion-taxonomy.md) |
