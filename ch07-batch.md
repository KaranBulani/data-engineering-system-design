# Chapter 07 — Batch

> Part III — Processing Paradigms

Batch is not the legacy option — it is the *default* option. Most data in most
companies is still processed on schedules, in windows, with retries, at the
lowest cost per byte of any paradigm. The senior mistake is treating batch as
boring; the interview mistake is proposing streaming without a latency
requirement that justifies it.

## The Question

*"When is 'data ready in minutes-to-hours' not just acceptable but the right
engineering choice — and how do I design batch so it is idempotent, backfillable,
and cheap?"*

## The Physics

### Why batch wins on fundamentals

1. **Bounded data = easy correctness.** A batch processes a *known, complete*
   input window. No watermarks, no late data mid-aggregation, no state that must
   survive restarts. The data being finite makes almost every hard streaming
   problem (ch08) disappear.
2. **Reprocessing is trivial.** Ran with a bug? Fix the code, re-run the window.
   The re-run *is* the recovery model. This is the property streaming
   architectures spend enormous effort to approximate (ch10, ch11, ch12).
3. **Cost physics.** Batch buys compute at the cheapest price that exists:
   spot/preemptible machines (interruptible, because a killed job just re-runs),
   scheduled clusters, or serverless per-query pricing. Streaming runs 24/7 and
   pays 24/7.
4. **The tools are the most mature in the industry**: Spark, warehouse SQL,
   dbt, Airflow — the deepest ecosystems, the most operators, the most answers
   on Stack Overflow.

### Scheduled windows and incremental strategies

A batch pipeline is a schedule (cron/Airflow) plus an incrementality strategy.
The strategies, precisely:

| Strategy | Semantics | Idempotency mechanism |
|---|---|---|
| **Append** | new rows only, never rewrites | partition + event id dedup at read |
| **Overwrite-partition** | recompute this day/month, replace atomically | same input -> same output; re-run safe |
| **Merge/upsert** | apply change events to a table | idempotent on business key |

**Overwrite-partition is the workhorse** and the one to narrate in interviews:

```
INSERT OVERWRITE TABLE events_agg PARTITION (dt='2026-09-20')
SELECT ... FROM raw WHERE dt='2026-09-20'
```

Run it once, run it three times, run it after a partial failure — the result is
identical. **Idempotency by partition overwrite** is the cheapest correctness
mechanism in data engineering: no exactly-once machinery, no transactional sink,
just "recompute the whole bounded window and swap it in atomically."

The preconditions are honest, though: the window must be *complete* before you
compute it (late data arriving after the partition is written is the classic
corruption), and partitions should be sized so recompute is affordable.

### Late data in a batch world

Batch handles late data by *re-running the window* — "close of business is a
lie, so we re-process yesterday at 6am, and once a week we re-process the week."
That is a legitimate, operationally simple answer. The failure mode is silent
late data: partition written at 00:10, stragglers land 00:40, dashboard is wrong
forever. Defenses: an ingestion cutoff with a late-arrivals audit, or a
reconciliation job that compares raw counts to partition counts and re-runs on
drift.

### Failure semantics

- **Task retry**: Spark retries failed tasks on other nodes — the unit of
  failure is small and internal.
- **Job retry**: failed *job* re-runs the whole window; idempotent writes make
  that safe. This is why idempotency is not a nicety but the load-bearing wall.
- **Backfill is a first-class operation**: parameterize the job by window
  (`{ds}` in Airflow), and "backfill 2026-01-01..2026-08-31" is 240 scheduled
  runs — same code, different parameters, no special backfill mode to maintain.

### The cost model, concretely

- **Spot/preemptible compute**: batch tolerates interruption by design; 60-80%
  discounts are normal.
- **Serverless batch** (Databricks serverless, BigQuery, Athena): per-query
  pricing, zero cluster ops — the low-volume answer.
- **Autoscaling clusters**: for sustained pipelines, spinning up per-schedule
  and tearing down beats an always-on cluster.
- **The efficiency lever is scan reduction**, not faster machines: partition
  pruning, columnar formats (ch17-18), incremental windows instead of
  full-history scans. Most "slow batch jobs" are full scans that should be
  incremental.

### The honest downsides

- **Latency floor** = schedule interval + runtime. No batch design gives you
  30-second freshness.
- **Wasted work when upstream is late**: the 2am job runs at 2am whether the
  data landed at 1am or 2:30. Datasets-aware scheduling (ch21) fixes this.
- **Small-files accumulation** (ch19): many small partition writes bloat
  metadata and slow reads; compaction becomes a scheduled duty.

### Modeling the window: grain, boundary, and the cutoff

Three decisions hide inside "schedule + incrementality":

1. **Grain** — the window size (hour/day/month). The rule: **grain follows the
   recompute unit and the late-data window.** If stragglers arrive up to 48h
   late, daily grain with a 48h-cutoff re-run beats hourly grain with 48 hourly
   re-runs. Partition grain is the batch world's version of partition keys
   (ch14): chosen for the *queries and the recovery*, not aesthetics.
2. **Boundary** — when does a window close? A cutoff rule stated in code:
   "the 09-20 partition finalizes when the 09-21 06:00 run starts." Before
   the boundary, the partition is provisional (re-writable); after, it is
   sealed — and late arrivals route to the late-arrivals queue for the
   weekly reconcile (below).
3. **Idempotency mechanics for every write pattern**:

| Write pattern | Idempotent? | The rule |
|---|---|---|
| `INSERT OVERWRITE` partition | yes | the default; same input → same output |
| append + dedup key | yes if enforced | event_id dedup at read/write (ch03 envelope) |
| `MERGE` on business key | yes | upsert semantics; last-write-wins needs event_time tiebreak |
| plain `INSERT INTO` | **no** | retries duplicate rows — ban it in production windows |

### The backfill economics of grain

Grain decides backfill cost: re-deriving one day is one partition overwrite;
re-deriving one hour of a fine-grained table is the same compute spread over
24x the metadata. The interview line: "partition grain is the batch world's
partition key — I pick it for recovery semantics first, dashboard filters
second."
## The Options

| Batch shape | When |
|---|---|
| Warehouse-native (ELT, dbt) | transformations are SQL; data already lands in the warehouse |
| Spark/Databricks batch | heavy transformations, non-SQL logic, ML |
| Serverless SQL (Athena/BigQuery) | sporadic ad-hoc over lake data |
| Scheduled scripts | tiny scale — do not industrialize 200 rows |

ELT vs ETL belongs here too: modern default is EL(T) — land raw in a
warehouse/lakehouse, transform *in* it with SQL (dbt), because warehouse compute
is elastic and SQL talent is abundant. ETL (transform before load) survives where
the transform is the only way to make the volume/PII manageable.

## Decision Rules

- **No stated sub-minute latency need -> batch.** Justify every streaming
  proposal with a requirement (ch02), not vibes.
- **Idempotency via overwrite-partition as the default write pattern.**
- **Parameterize every job by window from day one** — backfills are then free.
- **Choose partition grain by recompute cost and late-data window**, not by
  dashboard aesthetics.
- **ELT when the target is a warehouse and logic is SQL; Spark when logic is
  heavy or data is huge.**
- **Alert on upstream completeness, not just job success** — a green job over
  half the data is the worst failure mode in batch.
- **Budget by scan, not by cluster**: partitioning + columnar + incremental
  windows first, bigger machines last.

## Failure Modes

- **Silent partial input**: job "succeeded" over 60% of the window's data.
  Symptom: totals reconciling wrong weeks later. Fix: completeness checks at
  job start (ch21).
- **Non-idempotent incrementality** (`INSERT INTO` without dedup): retries and
  re-runs duplicate rows. Symptom: counts drift upward after every incident.
- **Late stragglers after partition close**: wrong-yesterday dashboards; found
  by customers, not dashboards.
- **The eternal full scan**: a "daily" job scanning all history; cost grows
  linearly with data. Symptom: the bill is the incident.
- **Backfill by hand-edit**: a "special" one-off script that is really the
  production logic, drifted. Every backfill must be the same parameterized job.
- **Batch insecurity**: proposing Spark for 50k rows/day — machinery cost
  exceeding data value.

## Interview Narration

"My default is batch unless a requirement forces streaming, and I'll say why:
bounded data makes correctness almost free — the window is complete, so there's
no watermark or late-data machinery; reprocessing is just re-running the window;
and compute is the cheapest it can be, because batch tolerates spot machines and
pays only while running.

Concretely I'd design it as scheduled, window-parameterized jobs with
overwrite-partition semantics — the same job code runs today's partition or
backfills January, and running it three times produces the same result as once,
because idempotency comes from atomically replacing the partition rather than
from any exactly-once protocol. Late data is handled by re-running windows with
an ingestion cutoff plus a late-arrivals audit, and backfills are first-class:
parameterize by date, trigger the range, done.

Where it gets interesting is the edges: completeness checks before compute —
because a green job over half the data is the worst batch failure mode — and
cost discipline through scan reduction: partitioning, columnar formats,
incremental windows instead of full-history scans. And I'd be explicit about the
latency floor: if someone needs this data in 30 seconds, this whole design is
the wrong tool and I'll say so — that's a streaming requirement and it should
be priced and staffed like one."

---

| <- Previous | Next -> |
|---|---|
| [Chapter 06 — CDC, the Long Tail & Normalization](ch06-cdc-long-tail-and-normalization.md) | [Chapter 08 — Streaming](ch08-streaming.md) |