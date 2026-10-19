# Chapter 22 — Production Operations Runbooks

> Part VII — Security & Operations

An architecture diagram is only the beginning. A production data system must also tell people when it is late or wrong, make a failed run safe to repair, keep an audit trail, protect data, and survive a serious outage. This chapter puts those day-to-day practices in one place. Use it as a design checklist, an on-call reference, and a way to explain how your design works in the real world.

A **runbook** is a written, step-by-step recipe for operating the system: "how to re-run yesterday's load," "what to do when the streaming job falls behind," "how to restore the catalog from backup." The chapters before this one explained how to build the system; this one explains how to live with it once it is running. Two running examples from earlier chapters appear throughout: a nightly **batch** job that loads the e-commerce orders table into the warehouse (ch07), and the video site's **streaming** view-count job (ch08, ch11). A 15-minute **micro-batch** (ch09) sits between them and appears wherever its behavior differs from both.

If you remember only one sentence from this chapter, make it this one: every operational question — reloading, schema changes, alerts, repairs, retention, security, deployments, disasters — has a different answer for batch, micro-batch, and streaming, and the difference is almost always the same difference: *what exactly is the unit you repair, and what saved progress do you have to respect while repairing it?*

## The Question

*"The design passed review and the pipeline is running. Now: how do you re-run yesterday's load, survive a 3am failure, change a schema without paging every consumer, notice wrong data before the business does, prove what happened when it breaks, and rebuild the platform when a region disappears - and how does each answer change between batch, micro-batch, and streaming?"*

That is really ten ordinary operational questions written as one long sentence, and each one gets its own numbered section below: reloading a day's load (§1), schema changes (§2), alerts (§3), validation (§4), logging (§5), the repair flow (§6), retention (§7), security duties (§8), canary deployments (§9), and disaster recovery (§10). Each section answers its question three times — once per paradigm — because the recovery mechanics genuinely differ, and using the wrong paradigm's procedure is itself a common cause of outages.

## What every production pipeline needs

Before choosing a tool, make sure every important pipeline has these things:

| Need | Minimum evidence that it exists |
|---|---|
| Owner and promise | A named owner, consumers, and a freshness/correctness SLO |
| Safe re-run | A partition or offset range that can be processed again without duplicates |
| Data checks | Tests that can stop bad data before it is published |
| Monitoring | Dashboards and alerts for freshness, failures, volume, and lag |
| Evidence | Structured job logs and a queryable `pipeline_runs` table |
| Recovery | A written, tested restore flow and a known source of truth for replay |
| Safe change | Schema compatibility checks plus canary and smoke tests |
| Protection | Access control, encryption, secrets handling, and a retention policy |
| Disaster plan | Backups/replication, an RTO and RPO, and regular restore drills |

Each row, in plain words:

- **Owner and promise.** A specific team or person is responsible — not
"the data team," a name — together with a list of who consumes the data and a measurable promise (an SLO, ch21), such as "the orders table is fresh by 06:00 and its row count is within 1% of the payment system's." Without a named owner, an alert has nowhere to arrive; without a written promise, "late" is a matter of opinion.
- **Safe re-run.** Any window of the pipeline can be processed a second
time without creating duplicate rows — a partition that gets overwritten, or an offset range that can be replayed into a clean target. This single property is what makes every repair in §1 and §6 possible.
- **Data checks.** Automatic tests that inspect the data itself — row
counts, null rates, value ranges — and can block bad data from reaching consumers (§4).
- **Monitoring.** Dashboards you actually look at, and alerts wired to the
four things that go wrong most: data is late, the job failed, the volume is strange, or the stream is falling behind (§3).
- **Evidence.** Every run leaves a queryable record of what it did — the
`pipeline_runs` table of §5 — so "what happened last night" is a query, not an archaeology project.
- **Recovery.** The restore flow of §6, written down in advance and
rehearsed, plus a known place to re-read raw data from (the raw plane, ch03/ch06).
- **Safe change.** Schema changes are checked for compatibility before
they ship (§2), and new code versions are proven on real data before they fully take over (§9).
- **Protection.** Who can read what, how data is encrypted, how secrets
are handled, and when data is deleted (§7, §8; the architecture is ch20).
- **Disaster plan.** Backups and replication exist, someone has written
down how much data loss (RPO) and downtime (RTO) is acceptable per tier, and the restore is actually practiced (§10).

If one row is missing, the system is not fully production-ready. A tool can help with these controls, but no tool makes the decisions for you.

## The Physics

### The three operational models

The operator's first question on any incident: **what shape is the recovery?** Each paradigm has a different answer:

```
BATCH          recovery unit = the window/partition
               "re-run the day" is (almost) the whole runbook (ch07)

MICRO-BATCH    recovery unit = partition + checkpointed offset
               batch mechanics plus "where exactly is the bookmark?" (ch09)

STREAMING      recovery unit = (committed offset, checkpointed state)
               resume | replay-with-new-identity | savepoint surgery (ch08, ch11)
```

Read the diagram as three definitions of "the unit of repair." For a batch job, the unit is a window — a day or an hour of data, one partition, which can be thrown away and recomputed from scratch. For a micro-batch job, it is a window *plus* a checkpointed offset — the bookmark saying "I have processed everything up to 14:45" (ch09). For a streaming job, it is the committed offset *and* the job's internal state — the running aggregates and windows the job has been building in memory for weeks (ch08, ch11). The state is the extra thing: a batch job has no memory between runs, but a streaming job's correctness lives partly in the state it has accumulated, which is why streaming repairs have to be so much more careful.

Everything below is these three shapes applied to ten questions.

**Plain English:** a batch job normally repairs one day or hour. A micro-batch job repairs a time window and must also respect its saved progress. A streaming job repairs a range of events and its saved state. Never use a batch repair procedure on a live streaming checkpoint.

Concretely: if the orders job for `dt=2026-09-25` is wrong, you delete and recompute that one partition and nothing else. If the 15-minute orders micro-batch wrote a bad window, you repair that window but must not disturb the checkpoint that tells the live job where it was. And if the view-count streaming job's logic was wrong for three days, you must repair three days of events *and* three days of accumulated state — and you do that beside the live job, never by rewinding the live job itself. The rest of the chapter is these three shapes, applied ten times.

### 1. Reloading a day's load (backfilling)

**Backfilling** means reprocessing data that has already been processed once — usually because the code had a bug, or an upstream source was incomplete, and you need the output recomputed for some past period. It is the most common repair in data engineering, and each paradigm does it differently:

| | Batch | Micro-batch | Streaming |
|---|---|---|---|
| Reload unit | the partition (day/hour) | partition + source range | offset range + state |
| Mechanism | re-run the parameterized job; overwrite-partition (ch07) | separate backfill job reads raw/topic from the window's start; overwrites target partitions | new consumer group replays from offset into a *shadow* output; validate; swap (ch11) |
| State involved | none | live checkpoint untouched | re-derive state over the replay, or restore a savepoint |
| Blast radius | exactly the partitions you name | exactly the partitions you name | everything downstream of the replay |
| The trap | non-idempotent INSERT INTO (ch07) | surgery on the live job's checkpoint | replay storm against live brokers (ch11) |

How each column reads in practice:

- **Batch.** The tax logic in the nightly orders job was wrong for
`dt=2026-09-20`. Fix the code, then re-run the *same* job with the same parameter for that day: `run_orders_job --ds 2026-09-20`. Because the job uses `INSERT OVERWRITE` on the partition (ch07), the old output is replaced atomically and re-running it twice is harmless. The reload unit is exactly the partition you name, and no run state is involved — the job has no memory between runs. The trap: if the job used plain `INSERT INTO`, the re-run would *add* a second copy of every row, silently doubling revenue for that day.
- **Micro-batch.** The 15-minute orders job produced bad output between
14:00 and 16:00. You do not touch the live job or its checkpoint. Instead you start a *separate* backfill job — same code, different invocation, with no live checkpoint — that reads the raw topic (or raw table) from the start of the window, reprocesses it, and overwrites exactly the affected target partitions. The live job's bookmark stays where it is and keeps moving. The trap is the opposite temptation: pointing the live job's checkpoint backward to 14:00. That mixes replayed old events into a job that is simultaneously processing new ones.
- **Streaming.** The view-count job mis-counted for three days. Two hard
rules apply. First, replay with a *new consumer group identity* — a fresh group like `view-counter-repair-0926` that starts from the offset three days ago and writes into a *shadow* output table (`view_counts_repair`), never into the live one. You validate the shadow output, then swap it in (ch11). Second, never rewind the live job's checkpoint to "reprocess yesterday": the live job's downstream consumers have already received events up to now, and old events re-delivered into live state corrupt it. The trap, the *replay storm*: pointing a full re-read at the live brokers can saturate them and slow down every other consumer of the same topics (ch05).

One more streaming shortcut worth knowing: for *aggregate* tables — daily view totals, for example — a batch correction job over the raw event plane is usually cheaper than a full stream replay. Recompute the three bad days straight from the raw events and overwrite those three daily partitions in the lakehouse (ch12), instead of re-deriving three days of streaming state. Use the streaming replay when you genuinely need the stream's logic re-executed; use the batch correction when only the output numbers are wrong.

The discipline that makes all three safe: **the backfill is the same parameterized job, never a special script** (ch07, ch21). For streaming, two additions: replay uses a *new identity* - never rewind the live consumer's checkpoint, because out-of-order re-delivery into live state corrupts it; and for aggregate tables, a batch correction job from the raw plane is usually cheaper than full stream replay - lakehouse partition overwrite (ch12) beats re-deriving state over weeks of events.

Why the discipline matters: a "quick one-off script" written at 3am is not reviewed, not tested, and drifts from production logic — it computes *slightly different numbers* than the nightly job does, and next week nobody can explain which version was right (see Failure Modes, backfill-by-hand-edit). The same code, run with a different date parameter, inherits all the tests and review the daily job already has.

### 2. Handling schema changes

Schemas change because the business changes: a new field, a renamed field, a wider ID. The operational question is who finds out about the change and when — the producer at write time, or every consumer at read time, days later, in production:

| | Batch | Micro-batch | Streaming |
|---|---|---|---|
| Enforcement point | read time - table-format schema evolution (ch12, ch18) | read time + registry on the wire | Schema Registry at produce time (ch18) |
| Safe change | additive column with default | same | registry-verified backward-compatible change |
| Deploy order | consumers may deploy first | mixed | consumer-first for BACKWARD mode (ch18 table) |
| Incompatible change | new column era + backfill | new topic/table version | **new topic v2 + dual-publish migration window**, then retire v1 |

Unpacking the rows:

- **Enforcement point.** In batch and lakehouse land, the schema is
enforced when the data is *read*: the table format stores each file's schema (ch12, ch18), and evolution rules decide how old and new files coexist. On the streaming wire, the schema is enforced when the data is *produced*: the Schema Registry rejects an incompatible new schema before any message is ever written (ch18). Micro-batch jobs that read from a registry-governed topic get both.
- **Safe change.** The universally safe move everywhere is *additive with
a default*: the producer adds a `discount_code` column that defaults to null. Old readers ignore it; new readers find it (null where absent). Nothing breaks, in either direction.
- **Deploy order.** In BACKWARD compatibility mode, a *new* consumer can
read data written by an *old* producer (ch18) — so consumers upgrade first, and only when every consumer is new does the producer start writing the new shape. Get the order backwards and you have deployed a reader for data that does not exist yet, or produced data nobody can read.
- **Incompatible change.** Renaming `user_id` to `account_id` cannot be
made safe by defaults. The streaming answer is a migration window: publish to a new topic `orders-v2` while still publishing to `orders-v1`, let every consumer move over at its own pace, then retire v1. In batch land, the analogous move is a "new column era": start writing the new column as of a cutoff date and backfill history separately (§1), rather than rewriting the meaning of the old column in place.

Two nuances worth narrating: table formats store schema *per file* (ch12), so transition day legitimately contains mixed-schema files and readers get the merged schema - that's a feature, not corruption. And CDC connectors (Debezium) **stop safely on unmanaged DDL** - a loud stop is the designed behavior (ch06); the runbook is "add the DDL to the schema history, restart the connector," not "why did it pause?!"

The first nuance, plainly: on the day after the `discount_code` column is added, the lake contains some files with the column and some without. That is *correct* — each file carries its own schema, and the reader merges them into one view where old files show null. Nobody needs to rewrite a day of history to add a column. The second nuance, plainly: Debezium captures database changes into a stream, and if someone runs an `ALTER TABLE` the connector does not recognize, it stops on purpose rather than guessing how to encode the change. The 3am instinct — "restart it and hope" — is wrong; the fix is to record the DDL in the connector's schema history and restart, so it resumes knowingly.

### 3. Alert setup

Alerts are not all the same urgency, and treating them as if they were is how paging systems die. The tiers are cut by *what the consumer feels*, not by what broke:

| Tier | Definition | Example |
|---|---|---|
| Page | SLA breach with external/consumer impact | freshness SLO missed (ch21); wrong data published |
| Ticket | degraded, workaround exists | duration p95 creeping; DLQ depth > 0 |
| Digest | trend worth watching | cost drift; volume growth anomaly |

In plain words: a **Page** wakes a human tonight, because a written promise (SLO) to consumers is being broken right now — the orders table did not load by 06:00, or wrong numbers were published. A **Ticket** is work for tomorrow morning: the job's p95 duration has been creeping from 40 to 55 minutes (still inside the window), or a few messages have landed in the dead-letter queue — degraded, but with a workaround and no consumer impact yet. A **Digest** is a weekly-read trend: cloud cost drifting up 8% per week, or input volume growing faster than capacity plans assumed. The test for each alert: *if this fired right now, what would a human do, and by when?* If the honest answer is "nothing, eventually," it is not a page.

Per paradigm, the alert sets that matter:

| Batch | Micro-batch (adds) | Streaming |
|---|---|---|
| job failure | checkpoint write failure | consumer lag (and **lag-vs-retention headroom**, ch05) |
| duration p95 trend | trigger overrun (runtime approaching interval, ch09) | checkpoint duration/size growth |
| completeness check (rows vs expected, ch21) | small-file backlog | DLQ depth |
| freshness SLO miss | - | watermark stall (nothing progressing) |

Reading the columns: batch needs the basics — the job failed, it is getting slower, the row count is off from expectation, the SLO was missed. Micro-batch adds the failures specific to its machinery: the checkpoint could not be written (the bookmark is at risk), the batch is taking nearly as long as its 15-minute interval (batches are starting to overlap, and lag will accumulate), or the sink is accumulating a backlog of small files (ch19's disease appearing as a symptom). Streaming adds lag: how many events are waiting in Kafka versus how fast the job consumes them — and specifically the *headroom* between that lag and the topic's retention (ch05). If the queue keeps three days of events and your lag is two days, you are hours away from events expiring before anyone reads them. Growing checkpoint duration or size means the state is swelling; a stalled watermark means *nothing* is progressing — either the input went quiet or the job is wedged, and you cannot tell which without looking.

The meta-rule from ch21: **alert on symptoms, page on SLO breaches, and treat alert fatigue as a failure mode** - 40 paging alerts means operators ignore all 40, which means you have zero alerts.

The mechanism of alert fatigue is worth stating plainly. If every job failure pages — including the ten routine upstream hiccups that self-heal — the on-call phone buzzes all night for non-events. Humans adapt by muting the channel. Then the one alert that mattered (the real SLA breach) arrives as ticket #41 in an ignored queue. The fix is ruthless pruning: every paging alert must have caused a real human action recently, or it is demoted to a ticket or digest.

Every alert should answer five questions in its definition: **what is wrong, which dataset or pipeline is affected, who owns it, what should the responder do first, and when does it become urgent?** Link the alert to the runbook and include the dashboard, failing partition or consumer group, and recent run ID. Test alerts when they are created and after major monitoring changes. An alert that has not been tested is only a hopeful configuration.

A definition that answers all five, filled in: *"The orders table's freshness SLO is breached — `lake.orders` has no successful load for `dt=2026-09-25` (dataset: orders; owner: @orders-oncall). First action: work the §6 restore flow from DETECT, starting with the blast-radius query (§5). Escalates to the engineering manager if two consecutive windows fail."* The alert carries the dashboard link, the failing partition, and the run ID. And "test the alert" is literal: inject the condition in staging (or flip the threshold) and confirm a human actually gets paged — an untested pager route is a hope, not a control.

### 4. Data validation & quality

ch21 is the full treatment; the per-paradigm placement:

| | Batch | Streaming |
|---|---|---|
| Gate position | pre-publish, partition-level: schema, nulls, ranges, uniqueness, row-count vs expectation | inline at consume: schema conformance, ranges -> DLQ on violation |
| Stop behavior | circuit breaker: quarantine partition, halt downstream, serve yesterday | poison message to DLQ, partition keeps flowing |
| Deep check | anomaly on volume/distribution vs 7-day baseline | **reconciliation vs batch truth**: streaming aggregate vs recomputed batch (Lambda healing, ch10, used as a *validation*) |

The placement difference is forced by physics, not preference. A batch job holds a complete, bounded window of data before it publishes, so it can afford to check the whole partition first — schema, nulls, ranges, uniqueness, row counts — and simply not publish if a check fails. That is the circuit breaker: the bad partition is quarantined, downstream jobs are halted, and consumers are served yesterday's good data with a banner. A streaming job can never "hold the whole day back" — it must emit continuously, so stopping is expensive. Instead it checks each event as it flows: an event that fails a range or schema check is a *poison message* sent to the dead-letter queue, while the partition keeps flowing. One bad event costs one DLQ row, not an outage.

The deep checks differ the same way. Batch can compare this partition's volume and distributions against a 7-day baseline — "orders are down 60% and it is not Black Friday." Streaming's strongest deep check is **reconciliation vs batch truth**: recompute yesterday's view counts in a batch job from the raw event plane and compare them to what the streaming job published (this is the Lambda architecture's healing loop of ch10, reused as a validation). If the stream said 41,200 views and the batch recompute says 44,900, the stream missed or double-counted something that its inline checks could not see.

The one-line summary: batch validates *before* publishing (bounded data, cheap to stop); streaming validates *while* flowing (unbounded, stopping is expensive) and relies on reconciliation to catch what inline checks missed.

For each important dataset, write down the checks and their action: schema (are fields and types expected?), completeness (did enough data arrive?), validity (are values in allowed ranges?), uniqueness, timeliness, and reconciliation with a trusted source. Not every check must fail a run, but every failed check needs a named action: publish, warn, quarantine, or stop. Store the result of every check in a table so quality history is visible.

Made concrete for the orders table: *schema* — every expected field present with expected types; *completeness* — row count within ±3% of the same weekday last week, or reconciled against the payment system's count; *validity* — no negative `order_total`, `order_date` matches the partition date; *uniqueness* — no duplicate `order_id`; *timeliness* — ingestion lag within SLO (ch03); *reconciliation* — daily revenue matches the finance report. Each check is registered with one action: uniqueness violation → stop and quarantine; timeliness miss → warn; count anomaly → warn and investigate. The results go into a quality-results table, so a dashboard can show each dataset's gate pass rate over time — quality history, not just quality moments.

### 5. Logging & the log tables

The unglamorous backbone of every recovery story below. **Pipelines emit their own operational data**, and it belongs in queryable tables, not just in orchestrator logs:

```
pipeline_runs (one row per task execution)
  run_id         uuid
  window         '2026-09-20'            -- logical partition (ds / hour)
  code_version   git sha                 -- WHAT logic ran
  status         success | failed | quarantined
  started_at / ended_at
  rows_in / rows_out                      -- the completeness signature
  bytes_read / bytes_written
  source_offset  'kafka:orders:4213398'  -- watermark / offset advanced
  target         'lake.orders/dt=2026-09-20'  -- what it wrote
  snapshot_id    45123                    -- table-format commit (ch12)
  error_class    'upstream_partial'       -- taxonomy, not stack traces

data_events (one row per publish)
  table, partition, snapshot_id, run_id, ts
```

Each field exists to answer a specific 3am question. `run_id` ties everything about one execution together. `window` says which logical partition it worked on. `code_version` (the git sha) says *which version of the logic* produced the output — the single most useful field when a bug shipped. `rows_in`/`rows_out` record the completeness signature. `status` distinguishes "failed" from "quarantined" (the quality gate stopped it on purpose). `source_offset` says how far the job read into the source. `target` and `snapshot_id` say exactly what it wrote and which table-format commit to inspect or roll back to (ch12). `error_class` is a small vocabulary of failure types — `upstream_partial`, `schema_mismatch`, `timeout` — not a raw stack trace, so failures can be grouped and trended.

What this buys at 3am, in query time:

- **Blast radius in one query**: "which partitions did `code_version =
bad_sha` touch?" - SELECT target, window FROM pipeline_runs WHERE code_version = '...'. Containment scope in seconds, not archaeology.
- **Completeness signature**: rows_out/rows_in ratio per window over time -
the cheapest anomaly detector that exists (ch21's volume check, fed).
- **Freshness proof**: last successful run per table - the freshness SLO's
ground truth.
- **Reproducibility**: snapshot_id + code_version + source_offset = the
exact recipe of any historical output.

Each of these is a story. A bad commit ships at 14:00 and the bug is found at 20:00: without the table, someone greps job logs for hours guessing which partitions are tainted; with it, one query returns the exact list, and §6's CONTAIN step starts from knowledge instead of suspicion. The completeness signature — rows out divided by rows in, plotted per window — catches the silent partial loads that never crash anything. The freshness SLO ("orders fresh by 06:00") is only enforceable if `max(ended_at) WHERE status = 'success'` is queryable per table. And reproducibility means any historical number can be traced to the exact code, inputs, and commit that produced it — which is what an audit or a finance dispute actually asks for.

Hygiene rules: **never log payloads** (PII lives in data, not in ops metadata - ch20); log keys, counts, and ids. Job *logs* (stdout, structured JSON) answer "why did it fail"; the log *tables* answer "what did it affect" - you need both, and the second is the one teams skip.

The payload rule, plainly: the ops layer must never contain customer emails, addresses, or card numbers. Log `rows_out = 1204, distinct_customers = 312`, not sample records. Otherwise every log table becomes PII-bearing data that needs the full protection regime of ch20 — and log tables are read by everyone during incidents. The two-layer split matters because job logs (stack traces, structured JSON stdout) tell the engineer *why* a run failed, while the log tables tell the incident commander *what data is affected* — and it is the second layer that teams skip, then regret.

Use a correlation ID such as `run_id` in every task log, quality result, published-data event, alert, and incident ticket. Then one identifier ties a human report to the failing code, source range, table version, and repair. Keep log tables separate from business data and apply their own access and retention rules.

The correlation ID in action: a consumer reports "yesterday's revenue number looks wrong." The ticket carries a `run_id`. That ID pulls up the run row (code version, offsets, snapshot), the quality results from that run, the publish event, and eventually the repair that reloaded the partition. One identifier, the whole story — instead of three people matching timestamps by hand. And the log tables themselves are operational data: keep them apart from business tables, with their own access rules (most people should read, few should write) and their own retention (§7).

### 6. Error in a run - the restore flow

The generic runbook, in strict order:

```
1. DETECT        alert / quality gate fires
2. SIZE          blast-radius query on pipeline_runs (what partitions,
                 tables, downstream consumers are touched)
3. CONTAIN       circuit breaker (ch21): quarantine bad partitions, halt
                 downstream, serve yesterday's data with a banner
4. DIAGNOSE      job logs + error_class; reproduce on the quarantined data
5. FIX           code fix, reviewed
6. RELOAD        re-run affected windows (per paradigm, section 1)
7. VALIDATE      quality gates on reloaded partitions; reconcile counts
8. RESUME        release downstream; clear the banner
9. POSTMORTEM    blameless; add the detection that was missing (ch21)
```

Walking the steps with a real-shaped story. At 03:10 the freshness alert fires: no successful orders load for `dt=2026-09-25` (**DETECT**). The on-call engineer runs the blast-radius query: the run failed after writing a partial partition; downstream the revenue dashboard and two consumer tables read `lake.orders` (**SIZE**). She quarantines the partition, halts the downstream jobs, and puts a banner on the dashboard — "orders data delayed, showing 09-24" (**CONTAIN**). The job log shows `error_class = upstream_partial`: the payment API returned errors for two hours, so the source extract is incomplete; she reproduces the failure on the quarantined data to confirm (**DIAGNOSE**). The fix is a retry-with- backoff on the extract, reviewed by a second engineer even at 3am (**FIX**). She re-runs the parameterized job for `dt=2026-09-25`, overwriting the partition (**RELOAD**), the quality gates pass and counts reconcile against the payment system (**VALIDATE**), downstream jobs are released and the banner cleared (**RESUME**). The blameless postmortem the next day adds a completeness check that would have caught the partial extract at 01:00 instead of 03:10 (**POSTMORTEM**).

During a customer-facing incident, add two parallel actions: name an incident lead and communicate the impact. Tell consumers whether the data is delayed, stale, or wrong; say which tables and time windows are affected; and update them when the repaired data is available. Do not silently replace a published number when people may already have acted on it.

The last sentence is the one that gets skipped and matters most: if a published revenue number was wrong and someone emailed it to their boss, silently fixing the dashboard means the correction exists only for people who look again. Announce the correction with the window it affects — the people who acted on the bad number deserve to know they did.

Per paradigm, steps 6-8 differ:

- **Batch**: re-run the parameterized job for the affected windows -
idempotent overwrite makes re-running twice a non-event (ch07).
- **Micro-batch**: partition re-run for table targets; if the *checkpoint*
is inconsistent with the sink, reset to the last good checkpoint and re-trigger - the committed-offset bookmark is the source of truth for "where were we" (ch09).
- **Streaming**: transient failure -> the group resumes from committed
offsets automatically (rebalance, ch05); stateful corruption -> restore the last good checkpoint/savepoint and let it catch up (restart cost = state size + lag, ch08); bad *logic* ran live -> stop, fix, replay the affected offset range into a shadow output, validate, swap (section 1); poison messages -> after the fix, replay the DLQ in order.

Reading the streaming line, which is really four different incidents. A *transient* failure — broker blip, network partition — needs no operator at all: the consumer group rebalances and resumes from its committed offsets (ch05). *Corrupted state* — the aggregates themselves are wrong — means stop, restore the last good checkpoint or savepoint, and let the job replay from there; the restart cost is the state size plus the accumulated lag (ch08). *Bad logic that ran live* is the §1 shadow-replay procedure. *Poison messages* — the handful of events sitting in the DLQ — are replayed in their original order once the fix is deployed, because replaying a stream out of order breaks event-time logic.

The containment instinct is the senior marker: **serve yesterday's right data over today's wrong data** (ch21) - the banner is cheaper than the retraction.

Plainly: a banner saying "data delayed" costs an apology. Publishing wrong numbers costs retraction emails, re-made decisions, and eroded trust in every future number — and people who made decisions on wrong data cannot always be identified or reached. When in doubt, contain first, repair second.

### 7. Data retention policy

Retention means deciding, per dataset, how long each copy of the data lives and where. It is a written matrix, reviewed with cost and legal - "we'll decide later" is itself a decision, and it ends in a year-2 incident (ch06).

| Plane | Typical retention | Notes |
|---|---|---|
| Queue (Kafka/Pub/Sub) | days-weeks | sized against worst realistic consumer outage (ch05) |
| Hot warehouse tables | months | priced storage; partition pruning keeps queries off old data |
| Raw plane (bronze) | years, tiered | hot -> infrequent -> archive; the replay/backfill anchor (ch03/06) |
| Rollups / aggregates | forever | small; the compounding asset |
| pipeline_runs / logs | 1-2 years | compliance may extend; they contain no payloads |

The reasoning behind each row: 

- the **queue** only needs to hold what a repairing consumer might re-read, so its retention is sized against the worst realistic consumer outage — if the worst believable repair replays three days, keep at least three days plus margin (ch05). 
- **Hot warehouse tables** are expensive, price-per-GB storage, so months, not years — partition pruning (ch07) means old partitions are simply never scanned, so the cost of keeping them is pure storage. 
- The **raw plane** is the insurance layer every backfill and replay anchors to (§1, ch03/ch06), so it lives for years but *tiers down*: hot storage for recent months, infrequent-access for older, archive for the long tail. 
- **Rollups** — daily view totals, monthly revenue — are tiny and grow more valuable as history accumulates, so they are kept forever. 
- The **log tables** (§5) contain no payloads, cost almost nothing, and are exactly what an audit or postmortem two years later needs, so they outlive most of the data they describe.

GDPR note (ch20): erasure versus the immutable raw plane is a real tension; tokenized raw + crypto-shredding is the standing answer. Retention policy is where that mechanism gets scheduled, not just designed.

The tension, plainly: a user exercises their right to erasure ("delete my data"), but the raw plane is immutable by design — you keep history exactly as received so it can be replayed. The standing answer (ch20): raw data stores a *token* instead of the direct identifier, and the token is encrypted with a per-user key. "Deleting the user" becomes "shred that one key" — every historical copy containing their token becomes unreadable without any rewrite. The retention policy is where shredding actually gets scheduled and verified, not merely where the idea is admired.

A policy must say more than "keep data for seven years." For every dataset, record the purpose, data classification, owner, retention period, storage tier, deletion method, backup retention, and legal-hold process. Automate expiry where possible and periodically prove that expiry worked. A copy in a backup, export, or dead-letter queue is still a copy that the policy must cover.

The last sentence is the audit finding waiting to happen: the lifecycle rule deletes from the main table, but the quarterly export on a forgotten bucket, the backup from last March, and the DLQ still hold the data. Each is a copy, each needs its own expiry path, and "periodically prove that expiry worked" means actually querying the old locations for rows that should be gone — not trusting the policy document.

### 8. Data security - the recurring duties

ch20 is the architecture; operations makes it a *schedule*: key rotation (auto where possible, CMK rotation on a calendar), quarterly access reviews (grants drift toward over-permission - the eternal service account), service-account hygiene (short-lived credentials, one account per pipeline, not per team), and credential scanning in repos and configs. Security as a one-time setup decays; security as recurring ops does not.

Unpacking each duty into something you can put on a calendar:

- **Key rotation.** Encryption keys (a CMK — customer-managed key in the
cloud KMS — especially) rotate on a schedule; where the provider supports automatic rotation, enable it and stop thinking about it; everything else goes on a calendar.
- **Quarterly access reviews.** Permissions only ever accumulate: nobody
ever loses access on purpose. The intern from last year still has editor on the production catalog; the vendor's account from a finished project is still active; one service account has quietly accumulated grants from three teams ("the eternal service account"). Once a quarter, list every grant and its justification, and delete the ones with no owner.
- **Service-account hygiene.** One account per pipeline, not one shared
account per team. Shared accounts mean an audit log says *something* accessed the PII table, not *which pipeline*; and a leaked long-lived credential is a standing breach, while a short-lived one (minutes to hours) expires before it can be abused.
- **Credential scanning.** Someone *will* eventually commit an API key
into a repo or a config file. Scanners (repo and CI-integrated) catch it at push time, when the fix is "revoke and recommit," not "rotate everything and pray" six months later.

The shared theme is decay: the day the security setup is finished, it starts eroding — people join and leave, projects end, credentials age. Scheduled recurring work is the only thing that fights erosion.

Also define the security incident first step: disable or rotate a suspected credential, preserve audit evidence, identify data that was reachable, and notify the security owner. Test that an emergency access revoke actually removes access; a permission model is only useful if it works under pressure.

The order matters: rotate the credential *first* (stop the bleeding before analyzing it — analysis can take hours, and every hour the credential is live is exposure), *then* preserve the audit logs (before rotation complicates them), then work out which tables the credential could reach, then notify the security owner. And test the emergency path — drill "revoke this role for this pipeline" once, in staging, so the first time you press that button is not during an incident.

### 9. Canary & production smoke tests

"A merge is not a deployment - it's done when smoke passes."

A **canary** is the deployment practice of running the new code version on a small slice of real production data *beside* the old version, and comparing their outputs before the new version fully takes over. A **smoke test** is the small set of assertions run right after a deployment to confirm the system is actually working — not merely that it started:

| Stage | Batch/Micro-batch | Streaming |
|---|---|---|
| CI (PR) | run transformations on a staged sample; dbt build on a slim dataset | unit-test the topology logic; mini local cluster |
| Canary | new version processes the first windows/hours; **compare outputs**: row counts and key aggregates vs old version within tolerance | new job version writes to a *shadow* sink in parallel; compare |
| Promote | on parity across the canary window | savepoint -> start v2 from savepoint -> verify -> stop v1 (state compatibility required, ch08) |
| Post-deploy smoke | assertions after deploy: freshness advancing, row counts moving, gates green, no DLQ spike | same + lag stable, checkpoint succeeding, output distribution unchanged |

Walking the stages: 

- in **CI**, every pull request proves it computes correctly on a small staged sample (a slim orders dataset, a mini local cluster for the streaming topology). 
- The **canary** is the production exam: the new batch version processes the first windows while the old version still runs, and their outputs are *diffed* — row counts, revenue totals, distinct customers, within agreed tolerance. The streaming version of the same idea is a shadow sink: v2 consumes the same topics in parallel and writes to a side table nobody reads yet. 
- **Promotion** happens only on parity; for stateful streaming jobs the mechanism is the savepoint (ch08): stop-compatible v1, take a savepoint of its state, start v2 from that savepoint, verify, only then retire v1. 
- The **post-deploy smoke** then guards the merge-to-production gap: freshness is advancing, row counts are moving, quality gates are green, no DLQ spike, and for streaming — lag is stable, checkpoints are succeeding.

The core idea is **output parity, not process health**: a canary that only checks "the job ran" deploys on faith; a canary that diffs old-versus-new *data* catches the logic regressions that tests miss. For stateful streaming upgrades, the savepoint is the mechanism - and restore-from- savepoint must be rehearsed (below), because an untested restore path is a rumor.

The distinction is easiest to see in a failure. The canary window runs and the new version's job is green: started, finished, no errors. Process health says deploy. But the diff shows revenue 2% lower — v2's rewritten join silently drops cancelled orders. No test in CI caught it, because CI ran on a slim sample where the cancelled-order path never appeared. Only the output diff catches it. That is why the promote criterion is parity of *data*, not absence of errors — and why the restore-from-savepoint path must be rehearsed: the streaming promote depends on it, and an unrehearsed restore is a rumor of a capability.

Define promotion and rollback before deployment. For example: promote only after two successful canary windows with count and aggregate differences inside agreed limits; roll back immediately if a critical quality gate fails, freshness stops moving, lag grows, or the error/DLQ rate rises. A production smoke test should verify that new data was actually read, transformed, published, and visible to a real consumer—not merely that a process started.

The point of writing these down *before* deployment: at promote time, someone is tired and the new version looks fine, and unwritten criteria quietly become "looks fine." Written criteria turn the decision into a checklist. And the smoke test's last phrase is the real bar — the pipeline's job is data reaching a consumer, so the smoke test should end at a real query: *can the dashboard see today's rows?*

### 10. Disaster recovery

Two acronyms first, because the whole section prices with them. **RTO** (Recovery Time Objective): how long until the system works again after a disaster — the downtime budget. **RPO** (Recovery Point Objective): how much data can be lost, measured backwards from the disaster — an RPO of 5 minutes means everything written more than 5 minutes before the outage may be gone. Every tier below gets its own pair, because rebuilding a serving cache and rebuilding a warehouse are different jobs.

The strategy in one sentence: **everything downstream is derivable from the replicated raw plane, so region loss is a rebuild, not a bankruptcy** (ch03/06/11). RTO/RPO per tier:

| Tier | RPO | RTO | Mechanism |
|---|---|---|---|
| Serving KV | seconds-minutes | minutes | replay CDC log / rebuild projection from truth (ch15) |
| Warehouse/lakehouse tables | 0 (raw replicated) | hours-days | re-derive from raw plane |
| Streaming state | last checkpoint | restore + catch-up | replicated checkpoints/savepoints |
| Catalog & metadata | last backup (minutes) | minutes | small - back it up constantly (ch20) |
| Log tables (section 5) | last backup | minutes | they are tables too |

The strategy sentence is the whole design, so it is worth slowly: if the raw event plane is continuously replicated to a second region (ch03/ch06), then every downstream artifact — cleaned tables, aggregates, serving caches, even the streaming job's derived data — can be *recomputed* there. Losing a region costs compute time, not data. That is why the table's second row can promise RPO 0: the warehouse itself was lost, but nothing irreplaceable was in it — the truth lives in the raw plane, which survived. The serving KV has a tighter RTO because it is small and can be rebuilt from the CDC log or re-projected from truth (ch15) in minutes; the warehouse accepts hours-to-days because re-deriving everything is a large batch job. Streaming state can only recover to its last checkpoint — that is its RPO — and then catches up by replaying from the source. The catalog row looks trivial and is not: it is small, so it backs up in minutes, and it is the *commit authority* (ch12) — lose it, and the intact Parquet files in the lake become an unqueryable heap.

Mechanics: cross-region object replication for the raw plane, Kafka mirroring (MirrorMaker) where the queue must survive too, catalog backups (frequently - it is small and it is the commit authority, ch12), infrastructure-as-code for redeploying the compute layer.

Each mechanism in one line: object-store replication copies every raw file to the second region as it lands. MirrorMaker replicates Kafka topics so a region loss does not also destroy the queue's recent history (otherwise your RPO becomes the queue's retention window). Catalog backups run constantly, because the catalog is small and is the thing that makes the lake a database. And infrastructure-as-code means the entire compute layer — clusters, jobs, dashboards — is defined in code and can be redeployed in the surviving region by running the deployment, not by remembering how the platform was built.

The rule that separates real DR from documentation: **untested DR is no DR**. Quarterly game days: kill a pipeline in staging, restore from checkpoints and raw, measure the RTO you *actually* achieve against the RTO you promised. The first drill always finds the missing backup; that is its purpose.

Why the first drill always finds something: documents are written optimistically and the world drifts — the backup job was silently failing since March, the checkpoint replication was never turned on for the new job, the restore script references a bucket that no longer exists, the one person who knew the catalog restore procedure left the team. The game day converts each of these from a 3am discovery into a Tuesday-afternoon fix. A game day is a rehearsal with a script: kill a pipeline in staging, run the real restore, stop the clock, compare measured RTO against the promised RTO.

For each drill, record the scenario, start time, recovered data boundary, actual RTO/RPO, gaps found, owner, and due date for the fix. Include the people path as well as the technology path: who declares a disaster, who can change DNS or cloud regions, how consumers are informed, and when normal operations resume. A backup that cannot be restored by the available team is not a recovery plan.

The people path is where drills most often stall: the technology restores in forty minutes, and then nobody is sure who has the authority to say "this is a disaster, switch regions," or who is allowed to change the DNS entry, or how the twenty downstream teams get told. Those are organizational facts, and they must be written down and rehearsed like the backups are. The last sentence is the acceptance test for the whole section: a backup that only one departed expert could restore, or that needs credentials nobody on call has, is not a recovery plan — it is storage with good intentions.

## The Options

Operational maturity comes in tiers - know which one you are in, and which one the business is paying for:

| Tier | What it looks like | Signal you're here |
|---|---|---|
| L1 - Manual | runbooks as documents; humans execute recovery; debugging by log-grep and git archaeology | recovery time = whoever is on call |
| L2 - Orchestrated | idempotent parameterized jobs; log tables; quality gates; SLO alerts; written restore flows | blast-radius query answers in seconds |
| L3 - Productized | automated canary with output parity; DR drills with measured RTO; cost attribution per team (ch21) | deployments are boring; incidents are short |

Each tier, in plain words. 

- At **L1**, the runbooks exist but as prose nobody has rehearsed; recovery is whatever the on-call engineer can improvise, repairs are hand-run scripts, and "what did the bad version touch" is answered by grepping logs and hoping. Recovery time is a property of the person, not the system. 
- At **L2**, the machinery of this chapter is in place: jobs are idempotent and parameterized (§1), every run writes a `pipeline_runs` row (§5), quality gates quarantine bad partitions (§4), alerts tier by consumer impact (§3), and the restore flow (§6) is written and ordered. The signal is that the blast-radius question — which partitions, which consumers — is a seconds-long query, which makes every incident shorter. 
- At **L3**, the controls run themselves: canary deployments diff old-versus-new outputs automatically (§9), disaster drills happen quarterly with a *measured* RTO (§10), and cost is attributed per team (ch21). The signal is cultural: deployments are boring, incidents are short, and nobody is surprised by either.

The interview-useful sentence: most real teams live at L2, L2 is a reasonable thing to promise, and the failure modes below are mostly L1 habits surviving inside bigger systems. Claiming L3 without measured DR drills and automated output parity is how "untested DR is no DR" happens.

## Decision Rules

- **The backfill is the same parameterized job - never a special script.**
A special script is unreviewed, untested, and drifts from production logic, so the "repair" produces different numbers than the daily job. The same code with a different date parameter inherits every test and review the daily job already has.
- **Streaming reloads use a new consumer identity into a shadow output;
never rewind a live checkpoint.** Out-of-order re-delivery into live state corrupts the aggregates that are already published. Repair beside the live job, validate, then swap — never under it.
- **Schema changes additive-first, registry-verified; incompatible = new
topic + dual-publish migration window.** Additive-with-default is the only change safe in both directions (ch18); renames and type changes need a migration window where both versions publish, so consumers move at their own pace.
- **Alert tiers: page on SLO symptoms, ticket on degradation, digest on
trends - and prune paging alerts ruthlessly.** Alert fatigue turns 40 pages into zero alerts; every paging alert must have recently caused a real human action or it is demoted.
- **Log tables from day one**: rows in/out, offsets, snapshot ids, code
version - they are the 3am interface. Blast radius, freshness proof, and reproducibility are all one query away — and retrofitting the table after the first bad incident means living without it during that incident.
- **Restore flow is written, ordered, and rehearsed**: detect -> size ->
contain -> diagnose -> fix -> reload -> validate -> resume -> postmortem. The order matters: contain *before* diagnose, so repair does not happen while bad data keeps flowing downstream.
- **Retention is a written matrix per plane**, reviewed with cost and
legal; tokenization + crypto-shred for erasure (ch20). "We'll decide later" is a decision — the year-2 cost and legal incident is the decision making itself.
- **Security runs on a schedule**: rotation, quarterly access reviews,
per-pipeline service accounts. One-time security setup decays as people and projects change; only recurring work fights the decay.
- **Deployment = canary with output parity + post-deploy smoke**; a merge
alone is not a deployment. Process health is not output parity: only a diff of old-versus-new *data* catches the logic regressions that CI misses.
- **DR anchored on replicated raw + catalog backups + IaC; drilled
quarterly** - measured RTO or it doesn't exist. Every downstream artifact is derivable from the raw plane, so a region loss is a rebuild with a known budget; and the drill's measured RTO, not the document's promised one, is the real number.

## Failure Modes

- **Backfill-by-hand-edit**: the "quick fix" script drifts from production
logic; the reload produces different numbers than the daily job. Symptom: the corrected partition disagrees with the next day's run. The deeper cause is that the script bypassed review and tests, so it encoded the fixer's 3am understanding, not the system's logic. The fix is the discipline of §1: same parameterized job, different parameter.
- **Live-checkpoint surgery**: rewinding the streaming job's checkpoint to
"reprocess yesterday" - out-of-order events pour into live state and corrupt it; now you have two incidents. The replayed events were already emitted downstream and folded into running aggregates; re-delivering them double-counts, and the corrupted state is published for as long as it takes to notice. Repair beside the live job into a shadow output (§1).
- **Mixed-schema transition without a registry check**: the ch05/ch18
null-column incident, now with a timeline: producer ships v2, three consumers silently null a field for two weeks. Nothing crashes — the consumers' parsers fill missing fields with null and keep running — so the corruption is discovered by a downstream report, weeks late. The registry check (§2, ch18) would have failed the producer's deploy on day zero.
- **Alert fatigue**: every job failure pages; the real SLA breach is ticket
#41 in the queue. Symptom: operators have a muted channel and a habit. The system did not lack alerts — it lacked *tiering* (§3), so the signal drowned in routine noise and the humans adapted by stopping listening.
- **No log tables**: "which partitions did the bad version touch?" is
answered by grepping job logs and guessing. Containment takes hours; the postmortem promises log tables "next quarter." The cost is paid twice: once in the slow incident, and again in every incident until the table exists — plus the audit that eventually asks for reproducibility (§5) and gets a shrug.
- **Untested savepoint restore / DR-as-document**: the game day finds the
checkpoints were never replicated; the RTO is actually two weeks. Symptom: the incident that ends the architecture. The document promised hours; reality had a missing backup job, and the region loss became a data loss — the difference between the two numbers was invisible until someone measured it (§10).
- **Retention "decided later"**: year-2 cost review finds raw grown 40x
with no lifecycle policy; legal simultaneously asks where PII lives. The two questions arrive together because both are properties of the same unmade decision (§7): nobody wrote down what is kept, where, and for how long, so the raw plane grew forever and the PII in it grew with it.
- **Canary without comparison**: new version processes the canary window,
dashboards look fine, semantic regression ships anyway - because "looks fine" is not a parity diff. The job ran, so process health passed; the numbers quietly moved 2%, and only a deliberate old-versus-new output diff (§9) would have caught it before consumers did.

## Interview Narration

“Operations is where I'd separate the design from the runbook, and the first question is always 'what shape is recovery': batch is partition- shaped - re-run the window, idempotent overwrite; micro-batch adds the checkpoint bookmark; streaming is offset-and-state - resume from checkpoint for failures, replay with a new consumer group into a shadow table for reloads, savepoints for upgrades, and never touch the live checkpoint for backfills.

The backbone is unglamorous: a pipeline_runs log table - every run with rows in and out, offsets, snapshot ids, and code version - because at 3am the question is 'what did the bad version touch,' and that should be a one-second query, not archaeology. On top of that: alerts that page on SLO symptoms rather than causes; validation gates that quarantine and serve yesterday's right data instead of publishing today's wrong data; retention as a written matrix per plane; and deployments as canary-with-output- parity plus smoke tests - a merge isn't a deployment until smoke passes.

For disaster recovery, the anchor is the replicated raw plane plus catalog backups plus infrastructure-as-code: everything downstream is derivable, so region loss is a rebuild with defined RTO and RPO per tier - serving store in minutes from CDC replay, tables in hours from raw. And I'd insist on quarterly restore drills with measured RTO, because untested DR is a document, not a capability. If the interviewer nods here, the follow-up I'm hoping for is 'tell me about a real 3am incident' - because this chapter is just the theory of the nights I've actually lived.”

---

| <- Previous | Next -> |
|---|---|
| [Chapter 21 — Orchestration, Data Quality & Operations](ch21-orchestration-data-quality-and-operations.md) | [Chapter 23 — Running the Room](ch23-running-the-room.md) |
