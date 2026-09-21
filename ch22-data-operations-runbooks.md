# Chapter 22 — Production Operations Runbooks

> Part VII — Security & Operations

An architecture diagram is only the beginning. A production data system must
also tell people when it is late or wrong, make a failed run safe to repair,
keep an audit trail, protect data, and survive a serious outage. This chapter
puts those day-to-day practices in one place. Use it as a design checklist,
an on-call reference, and a way to explain how your design works in the real
world.

## The Question

*"The design passed review and the pipeline is running. Now: how do you
re-run yesterday's load, survive a 3am failure, change a schema without
paging every consumer, notice wrong data before the business does, prove
what happened when it breaks, and rebuild the platform when a region
disappears - and how does each answer change between batch, micro-batch,
and streaming?"*

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

If one row is missing, the system is not fully production-ready. A tool can
help with these controls, but no tool makes the decisions for you.

## The Physics

### The three operational models

The operator's first question on any incident: **what shape is the
recovery?** Each paradigm has a different answer:

```
BATCH          recovery unit = the window/partition
               "re-run the day" is (almost) the whole runbook (ch07)

MICRO-BATCH    recovery unit = partition + checkpointed offset
               batch mechanics plus "where exactly is the bookmark?" (ch09)

STREAMING      recovery unit = (committed offset, checkpointed state)
               resume | replay-with-new-identity | savepoint surgery (ch08, ch11)
```

Everything below is these three shapes applied to ten questions.

**Plain English:** a batch job normally repairs one day or hour. A
micro-batch job repairs a time window and must also respect its saved progress.
A streaming job repairs a range of events and its saved state. Never use a
batch repair procedure on a live streaming checkpoint.

### 1. Reloading a day's load (backfilling)

| | Batch | Micro-batch | Streaming |
|---|---|---|---|
| Reload unit | the partition (day/hour) | partition + source range | offset range + state |
| Mechanism | re-run the parameterized job; overwrite-partition (ch07) | separate backfill job reads raw/topic from the window's start; overwrites target partitions | new consumer group replays from offset into a *shadow* output; validate; swap (ch11) |
| State involved | none | live checkpoint untouched | re-derive state over the replay, or restore a savepoint |
| Blast radius | exactly the partitions you name | exactly the partitions you name | everything downstream of the replay |
| The trap | non-idempotent INSERT INTO (ch07) | surgery on the live job's checkpoint | replay storm against live brokers (ch11) |

The discipline that makes all three safe: **the backfill is the same
parameterized job, never a special script** (ch07, ch21). For streaming, two
additions: replay uses a *new identity* - never rewind the live consumer's
checkpoint, because out-of-order re-delivery into live state corrupts it; and
for aggregate tables, a batch correction job from the raw plane is usually
cheaper than full stream replay - lakehouse partition overwrite (ch12) beats
re-deriving state over weeks of events.

### 2. Handling schema changes

| | Batch | Micro-batch | Streaming |
|---|---|---|---|
| Enforcement point | read time - table-format schema evolution (ch12, ch18) | read time + registry on the wire | Schema Registry at produce time (ch18) |
| Safe change | additive column with default | same | registry-verified backward-compatible change |
| Deploy order | consumers may deploy first | mixed | consumer-first for BACKWARD mode (ch18 table) |
| Incompatible change | new column era + backfill | new topic/table version | **new topic v2 + dual-publish migration window**, then retire v1 |

Two nuances worth narrating: table formats store schema *per file* (ch12),
so transition day legitimately contains mixed-schema files and readers get
the merged schema - that's a feature, not corruption. And CDC connectors
(Debezium) **stop safely on unmanaged DDL** - a loud stop is the designed
behavior (ch06); the runbook is "add the DDL to the schema history, restart
the connector," not "why did it pause?!"

### 3. Alert setup

Tiered by *what the consumer feels*, not by what broke:

| Tier | Definition | Example |
|---|---|---|
| Page | SLA breach with external/consumer impact | freshness SLO missed (ch21); wrong data published |
| Ticket | degraded, workaround exists | duration p95 creeping; DLQ depth > 0 |
| Digest | trend worth watching | cost drift; volume growth anomaly |

Per paradigm, the alert sets that matter:

| Batch | Micro-batch (adds) | Streaming |
|---|---|---|
| job failure | checkpoint write failure | consumer lag (and **lag-vs-retention headroom**, ch05) |
| duration p95 trend | trigger overrun (runtime approaching interval, ch09) | checkpoint duration/size growth |
| completeness check (rows vs expected, ch21) | small-file backlog | DLQ depth |
| freshness SLO miss | - | watermark stall (nothing progressing) |

The meta-rule from ch21: **alert on symptoms, page on SLO breaches, and
treat alert fatigue as a failure mode** - 40 paging alerts means operators
ignore all 40, which means you have zero alerts.

Every alert should answer five questions in its definition: **what is wrong,
which dataset or pipeline is affected, who owns it, what should the responder
do first, and when does it become urgent?** Link the alert to the runbook and
include the dashboard, failing partition or consumer group, and recent run
ID. Test alerts when they are created and after major monitoring changes.
An alert that has not been tested is only a hopeful configuration.

### 4. Data validation & quality

ch21 is the full treatment; the per-paradigm placement:

| | Batch | Streaming |
|---|---|---|
| Gate position | pre-publish, partition-level: schema, nulls, ranges, uniqueness, row-count vs expectation | inline at consume: schema conformance, ranges -> DLQ on violation |
| Stop behavior | circuit breaker: quarantine partition, halt downstream, serve yesterday | poison message to DLQ, partition keeps flowing |
| Deep check | anomaly on volume/distribution vs 7-day baseline | **reconciliation vs batch truth**: streaming aggregate vs recomputed batch (Lambda healing, ch10, used as a *validation*) |

The one-line summary: batch validates *before* publishing (bounded data,
cheap to stop); streaming validates *while* flowing (unbounded, stopping is
expensive) and relies on reconciliation to catch what inline checks missed.

For each important dataset, write down the checks and their action: schema
(are fields and types expected?), completeness (did enough data arrive?),
validity (are values in allowed ranges?), uniqueness, timeliness, and
reconciliation with a trusted source. Not every check must fail a run, but
every failed check needs a named action: publish, warn, quarantine, or stop.
Store the result of every check in a table so quality history is visible.

### 5. Logging & the log tables

The unglamorous backbone of every recovery story below. **Pipelines emit
their own operational data**, and it belongs in queryable tables, not just
in orchestrator logs:

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

What this buys at 3am, in query time:

- **Blast radius in one query**: "which partitions did `code_version =
  bad_sha` touch?" - SELECT target, window FROM pipeline_runs WHERE
  code_version = '...'. Containment scope in seconds, not archaeology.
- **Completeness signature**: rows_out/rows_in ratio per window over time -
  the cheapest anomaly detector that exists (ch21's volume check, fed).
- **Freshness proof**: last successful run per table - the freshness SLO's
  ground truth.
- **Reproducibility**: snapshot_id + code_version + source_offset = the
  exact recipe of any historical output.

Hygiene rules: **never log payloads** (PII lives in data, not in ops
metadata - ch20); log keys, counts, and ids. Job *logs* (stdout, structured
JSON) answer "why did it fail"; the log *tables* answer "what did it
affect" - you need both, and the second is the one teams skip.

Use a correlation ID such as `run_id` in every task log, quality result,
published-data event, alert, and incident ticket. Then one identifier ties a
human report to the failing code, source range, table version, and repair.
Keep log tables separate from business data and apply their own access and
retention rules.

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

During a customer-facing incident, add two parallel actions: name an incident
lead and communicate the impact. Tell consumers whether the data is delayed,
stale, or wrong; say which tables and time windows are affected; and update
them when the repaired data is available. Do not silently replace a published
number when people may already have acted on it.

Per paradigm, steps 6-8 differ:

- **Batch**: re-run the parameterized job for the affected windows -
  idempotent overwrite makes re-running twice a non-event (ch07).
- **Micro-batch**: partition re-run for table targets; if the *checkpoint*
  is inconsistent with the sink, reset to the last good checkpoint and
  re-trigger - the committed-offset bookmark is the source of truth for
  "where were we" (ch09).
- **Streaming**: transient failure -> the group resumes from committed
  offsets automatically (rebalance, ch05); stateful corruption -> restore
  the last good checkpoint/savepoint and let it catch up (restart cost =
  state size + lag, ch08); bad *logic* ran live -> stop, fix, replay the
  affected offset range into a shadow output, validate, swap (section 1);
  poison messages -> after the fix, replay the DLQ in order.

The containment instinct is the senior marker: **serve yesterday's right
data over today's wrong data** (ch21) - the banner is cheaper than the
retraction.

### 7. Data retention policy

Retention is a written matrix, reviewed with cost and legal - "we'll decide
later" is itself a decision, and it ends in a year-2 incident (ch06).

| Plane | Typical retention | Notes |
|---|---|---|
| Queue (Kafka/Pub/Sub) | days-weeks | sized against worst realistic consumer outage (ch05) |
| Hot warehouse tables | months | priced storage; partition pruning keeps queries off old data |
| Raw plane (bronze) | years, tiered | hot -> infrequent -> archive; the replay/backfill anchor (ch03/06) |
| Rollups / aggregates | forever | small; the compounding asset |
| pipeline_runs / logs | 1-2 years | compliance may extend; they contain no payloads |

GDPR note (ch20): erasure versus the immutable raw plane is a real tension;
tokenized raw + crypto-shredding is the standing answer. Retention policy
is where that mechanism gets scheduled, not just designed.

A policy must say more than "keep data for seven years." For every dataset,
record the purpose, data classification, owner, retention period, storage
tier, deletion method, backup retention, and legal-hold process. Automate
expiry where possible and periodically prove that expiry worked. A copy in a
backup, export, or dead-letter queue is still a copy that the policy must
cover.

### 8. Data security - the recurring duties

ch20 is the architecture; operations makes it a *schedule*: key rotation
(auto where possible, CMK rotation on a calendar), quarterly access reviews
(grants drift toward over-permission - the eternal service account),
service-account hygiene (short-lived credentials, one account per pipeline,
not per team), and credential scanning in repos and configs. Security as a
one-time setup decays; security as recurring ops does not.

Also define the security incident first step: disable or rotate a suspected
credential, preserve audit evidence, identify data that was reachable, and
notify the security owner. Test that an emergency access revoke actually
removes access; a permission model is only useful if it works under pressure.

### 9. Canary & production smoke tests

"A merge is not a deployment - it's done when smoke passes."

| Stage | Batch/Micro-batch | Streaming |
|---|---|---|
| CI (PR) | run transformations on a staged sample; dbt build on a slim dataset | unit-test the topology logic; mini local cluster |
| Canary | new version processes the first windows/hours; **compare outputs**: row counts and key aggregates vs old version within tolerance | new job version writes to a *shadow* sink in parallel; compare |
| Promote | on parity across the canary window | savepoint -> start v2 from savepoint -> verify -> stop v1 (state compatibility required, ch08) |
| Post-deploy smoke | assertions after deploy: freshness advancing, row counts moving, gates green, no DLQ spike | same + lag stable, checkpoint succeeding, output distribution unchanged |

The core idea is **output parity, not process health**: a canary that only
checks "the job ran" deploys on faith; a canary that diffs old-versus-new
*data* catches the logic regressions that tests miss. For stateful
streaming upgrades, the savepoint is the mechanism - and restore-from-
savepoint must be rehearsed (below), because an untested restore path is a
rumor.

Define promotion and rollback before deployment. For example: promote only
after two successful canary windows with count and aggregate differences
inside agreed limits; roll back immediately if a critical quality gate fails,
freshness stops moving, lag grows, or the error/DLQ rate rises. A production
smoke test should verify that new data was actually read, transformed,
published, and visible to a real consumer—not merely that a process started.

### 10. Disaster recovery

The strategy in one sentence: **everything downstream is derivable from the
replicated raw plane, so region loss is a rebuild, not a bankruptcy**
(ch03/06/11). RTO/RPO per tier:

| Tier | RPO | RTO | Mechanism |
|---|---|---|---|
| Serving KV | seconds-minutes | minutes | replay CDC log / rebuild projection from truth (ch15) |
| Warehouse/lakehouse tables | 0 (raw replicated) | hours-days | re-derive from raw plane |
| Streaming state | last checkpoint | restore + catch-up | replicated checkpoints/savepoints |
| Catalog & metadata | last backup (minutes) | minutes | small - back it up constantly (ch20) |
| Log tables (section 5) | last backup | minutes | they are tables too |

Mechanics: cross-region object replication for the raw plane, Kafka
mirroring (MirrorMaker) where the queue must survive too, catalog backups
(frequently - it is small and it is the commit authority, ch12),
infrastructure-as-code for redeploying the compute layer.

The rule that separates real DR from documentation: **untested DR is no
DR**. Quarterly game days: kill a pipeline in staging, restore from
checkpoints and raw, measure the RTO you *actually* achieve against the
RTO you promised. The first drill always finds the missing backup; that is
its purpose.

For each drill, record the scenario, start time, recovered data boundary,
actual RTO/RPO, gaps found, owner, and due date for the fix. Include the
people path as well as the technology path: who declares a disaster, who can
change DNS or cloud regions, how consumers are informed, and when normal
operations resume. A backup that cannot be restored by the available team is
not a recovery plan.

## The Options

Operational maturity comes in tiers - know which one you are in, and which
one the business is paying for:

| Tier | What it looks like | Signal you're here |
|---|---|---|
| L1 - Manual | runbooks as documents; humans execute recovery; debugging by log-grep and git archaeology | recovery time = whoever is on call |
| L2 - Orchestrated | idempotent parameterized jobs; log tables; quality gates; SLO alerts; written restore flows | blast-radius query answers in seconds |
| L3 - Productized | automated canary with output parity; DR drills with measured RTO; cost attribution per team (ch21) | deployments are boring; incidents are short |

## Decision Rules

- **The backfill is the same parameterized job - never a special script.**
- **Streaming reloads use a new consumer identity into a shadow output;
  never rewind a live checkpoint.**
- **Schema changes additive-first, registry-verified; incompatible = new
  topic + dual-publish migration window.**
- **Alert tiers: page on SLO symptoms, ticket on degradation, digest on
  trends - and prune paging alerts ruthlessly.**
- **Log tables from day one**: rows in/out, offsets, snapshot ids, code
  version - they are the 3am interface.
- **Restore flow is written, ordered, and rehearsed**: detect -> size ->
  contain -> diagnose -> fix -> reload -> validate -> resume -> postmortem.
- **Retention is a written matrix per plane**, reviewed with cost and
  legal; tokenization + crypto-shred for erasure (ch20).
- **Security runs on a schedule**: rotation, quarterly access reviews,
  per-pipeline service accounts.
- **Deployment = canary with output parity + post-deploy smoke**; a merge
  alone is not a deployment.
- **DR anchored on replicated raw + catalog backups + IaC; drilled
  quarterly** - measured RTO or it doesn't exist.

## Failure Modes

- **Backfill-by-hand-edit**: the "quick fix" script drifts from production
  logic; the reload produces different numbers than the daily job. Symptom:
  the corrected partition disagrees with the next day's run.
- **Live-checkpoint surgery**: rewinding the streaming job's checkpoint to
  "reprocess yesterday" - out-of-order events pour into live state and
  corrupt it; now you have two incidents.
- **Mixed-schema transition without a registry check**: the ch05/ch18
  null-column incident, now with a timeline: producer ships v2, three
  consumers silently null a field for two weeks.
- **Alert fatigue**: every job failure pages; the real SLA breach is ticket
  #41 in the queue. Symptom: operators have a muted channel and a habit.
- **No log tables**: "which partitions did the bad version touch?" is
  answered by grepping job logs and guessing. Containment takes hours; the
  postmortem promises log tables "next quarter."
- **Untested savepoint restore / DR-as-document**: the game day finds the
  checkpoints were never replicated; the RTO is actually two weeks. Symptom:
  the incident that ends the architecture.
- **Retention "decided later"**: year-2 cost review finds raw grown 40x
  with no lifecycle policy; legal simultaneously asks where PII lives.
- **Canary without comparison**: new version processes the canary window,
  dashboards look fine, semantic regression ships anyway - because "looks
  fine" is not a parity diff.

## Interview Narration

"Operations is where I'd separate the design from the runbook, and the
first question is always 'what shape is recovery': batch is partition-
shaped - re-run the window, idempotent overwrite; micro-batch adds the
checkpoint bookmark; streaming is offset-and-state - resume from
checkpoint for failures, replay with a new consumer group into a shadow
table for reloads, savepoints for upgrades, and never touch the live
checkpoint for backfills.

The backbone is unglamorous: a pipeline_runs log table - every run with
rows in and out, offsets, snapshot ids, and code version - because at 3am
the question is 'what did the bad version touch,' and that should be a
one-second query, not archaeology. On top of that: alerts that page on SLO
symptoms rather than causes; validation gates that quarantine and serve
yesterday's right data instead of publishing today's wrong data; retention
as a written matrix per plane; and deployments as canary-with-output-
parity plus smoke tests - a merge isn't a deployment until smoke passes.

For disaster recovery, the anchor is the replicated raw plane plus catalog
backups plus infrastructure-as-code: everything downstream is derivable,
so region loss is a rebuild with defined RTO and RPO per tier - serving
store in minutes from CDC replay, tables in hours from raw. And I'd insist
on quarterly restore drills with measured RTO, because untested DR is a
document, not a capability. If the interviewer nods here, the follow-up
I'm hoping for is 'tell me about a real 3am incident' - because this
chapter is just the theory of the nights I've actually lived."

---

| <- Previous | Next -> |
|---|---|
| [Chapter 21 — Orchestration, Data Quality & Operations](ch21-orchestration-data-quality-and-operations.md) | [Chapter 23 — Running the Room](ch23-running-the-room.md) |
