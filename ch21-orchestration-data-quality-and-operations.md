# Chapter 21 — Orchestration, Data Quality & Operations

> Part VII — Security & Operations

Pipelines do not fail loudly enough. A job that crashes alerts someone; the much more common failure — data arriving incomplete, wrong, or stale while every job reports success — fails silently, and someone finds out from a dashboard or an executive. The operational layer is what catches this: orchestration (scheduling and sequencing jobs), quality gates (checks that can stop bad data), freshness SLOs, and monitoring of the *data itself*, not just the jobs. This layer is what separates a platform from a pile of cron jobs, and it is where "data downtime" — the time wrong data exists before anyone fixes it — becomes a first-class engineering concept with numbers attached.

## The Question

How do I schedule, sequence, and re-run pipelines safely? How do I prevent bad data from propagating into downstream tables and dashboards? And how do I even *notice* when data is silently wrong — when nothing has crashed and every job says it succeeded?

## The Physics

### Orchestration: DAGs, sensors, datasets

An orchestrator (Airflow, Dagster) runs **DAGs** — directed acyclic graphs of tasks with dependencies. In plain terms: your pipeline is broken into tasks, the tasks are nodes in a graph, and arrows say which task must finish before the next starts ("extract → clean → aggregate → publish"). "Acyclic" means no circular dependencies, so the graph always has a valid execution order. The concepts that matter:

- **Idempotent tasks** — the ch07 contract, now mandatory. _Idempotent means running the task again produces the same result as running it once._ The standard way to get there is a partition-parameterized job: the job receives a date as a parameter (Airflow's `{ds}` template fills it in automatically) and overwrites exactly that date's partition. Run it once, three times, or after a crash — the output partition ends up identical. This is *the* prerequisite for everything else in this chapter: retries, backfills, and recovery all assume that re-running is safe.
- **Sensors vs datasets** is old scheduling versus modern scheduling. The old pattern is a sensor — a task that wakes up every minute and asks "is the upstream file there yet?", burning a task slot while it waits. The related habit is scheduling purely by the clock: the downstream DAG fires at 2am whether or not the upstream data actually arrived. The modern pattern is **dataset-aware / data-driven scheduling**: the orchestrator tracks datasets as first-class objects, and the downstream DAG triggers when the upstream *data* actually lands (Airflow datasets, Dagster assets and partitions). This kills the 2am-runs-anyway waste from ch07.
- **Retries vs replay** are two different recovery tools for two different problems. A **retry is the automatic re-run of a failed task** for a transient failure — a network blip or a lost worker node — and it is safe because the task is idempotent. A **replay is the deliberate re-run of a whole time window after a logic bug:** fix the code, re-run last Tuesday. Replay is only possible because tasks are idempotent and parameterized — otherwise the re-run would duplicate or corrupt data.
- **Backfills are first-class operations**, not emergencies. A backfill means triggering the same job across a range of dates — for example, reprocessing the last three months after a bug fix — and the orchestrator parallelizes the runs within reason and tracks which dates have completed. A "backfill" that is a hand-edited script run from someone's laptop is a design smell (ch07): it has no tracking, no alerting, and no idempotency guarantee.
- **SLAs and alerting**: when a freshness SLA is missed — the data was supposed to be ready by 6am and was not — a human gets paged. The DAG is the enforcement point, because the orchestrator is the one component that knows when a run was supposed to finish and did not.

### Data quality: gates, not vibes

The difference between gates and vibes: a **gate** is a check that runs inside the pipeline and can stop it — bad data ends its journey there. "Vibes" are quality dashboards that show problems after the fact but block nothing. The layered quality model, in pipeline order:

1. **Ingestion expectations**: at the door, check that incoming records conform to the registered schema (ch18) — types, nullability, value ranges. Records that fail are rejected or quarantined, not loaded. The webhook-and-log chaos of ch05 meets its first gate here.
2. **Tests** (dbt tests, Great Expectations) are assertions on *transformed* tables: uniqueness of keys, not-null columns, referential integrity (every `order_id` in the payments table exists in the orders table), accepted values (`status` is one of a known list), and freshness. In dbt these are declared in YAML next to the model; Great Expectations is a Python library of reusable assertions. They run as part of the pipeline, and a failing test fails the task — on purpose, because that is what makes it a gate.
3. **Anomaly detection on volume and distribution** is statistical rather than assertion-based: today's row count is 40% of the 7-day median; the `status` column's null rate jumped from 0.1% to 9%. These checks catch what nobody thought to assert — the failure mode you never wrote a test for is exactly the one this layer exists for.
4. **Freshness SLOs** are per-consumer guarantees, for example "the executive dashboard's data must be complete through yesterday by 6:30am". SLO means service-level objective — a stated, measurable promise. Measured, alerted, and owned, they operationalize the requirements gathered in ch02.
5. **Circuit breakers / quarantine**: when a gate fails, *stop propagation* — mark the partition as quarantined, halt the downstream tasks, page a human. The alternative is today's wrong data flowing into every dashboard and downstream table, which is strictly worse than yesterday's right data. The pattern to say out loud: **"yesterday's data with a banner beats today's wrong data silently"** — keep serving the last known-good partition, clearly labeled as stale, rather than silently serving wrong numbers.

The **trust score** idea formalizes all of this — it is what data observability products sell: each dataset is continuously scored on freshness, volume, schema, and distribution, and degradation triggers an alert before consumers notice. You can build the poor-but-real version in-house: run the four checks above on a schedule, write the results to a table, and alert on regressions.

### Operations: data downtime

The DRE (data reliability engineering) discipline measures and manages **data downtime** with two numbers. **MTTD** (mean time to detect) is the time between bad data being *delivered* and bad data being *detected*. **MTTR** (mean time to resolve) is the time between detection and *resolution*. Example timeline: a bad partition lands at 6:00am, a volume-anomaly alert fires at 6:15am (MTTD: 15 minutes), the fix is deployed and the partition recomputed by noon (MTTR: about 6 hours). Without the alert, the same incident might surface a week later from a confused analyst — MTTD: 5 working days.

The inversion that defines the whole discipline: **pipelines can be green while data is wrong**. Job success measures execution, not correctness — if upstream sends half the expected rows, your job processes them faithfully and exits 0. Hence two rules:

- Monitor *data* — freshness, volume, schema, distribution — not just *jobs* (exit codes).
- **Bad data delivered is an incident** even when every DAG is green. It gets the same treatment as a software outage: a blameless postmortem, a root cause, and a prevention step — not a quiet fix and no record.

The cost asymmetry drives everything: a wrong number in the dashboard is discovered by customers (maximum blast radius, minimum control over the situation); the same wrong number is discovered by a quality gate at 6:15am (minimum blast radius, full control). Every gate and every alert in this chapter is the second discovery happening instead of the first.

### Cost observability

The last operational duty is **per-team cost attribution** — often called chargeback or showback: 
  - warehouse query costs attributed per query (bytes scanned or credits burned — ch13/ch16), 
  - pipeline compute attributed per DAG, 
  - storage attributed per table and lifecycle stage. 

  The purpose is not only finance. Teams that can see their own cost curves self-correct: the habit of `SELECT *` over needed columns, unbounded data retention, and over-provisioned warehouses all shrink once the cost lands on the team's own report (this closes the loop on ch02's cost axis). An unattributed bill is an unbudgeted bill — when nobody owns a cost, nobody manages it, and it only appears when finance asks why the bill doubled.

## The Options

| Layer | Options | Selection reality |
|---|---|---|
| Orchestrator | Airflow / Dagster / Prefect / managed (MWAA, Composer) | Airflow is the default; Dagster when you want asset-centric scheduling |
| Quality framework | dbt tests / Great Expectations / Soda / custom | dbt-native if it is already a dbt shop |
| Observability | Monte Carlo / Elementary / in-house checks | Product when scale justifies it, build when it does not |
| SLO tracking | Orchestrator SLAs / custom freshness jobs | Must exist somewhere, with a named owner |

Two clarifications for the table: "asset-centric" is Dagster's model where everything is declared as a data asset and scheduling follows the data rather than the clock; MWAA and Composer are the managed Airflow offerings on AWS and Google Cloud, which trade flexibility for less operational burden.

The honest sizing note: a three-person team runs Airflow, dbt tests, and hand-rolled freshness checks, and that is *plenty* — a 30-team platform needs the productized versions. Match the machinery to the stakes (ch06's rule, applied to operations).

## Decision Rules

- **Make every task idempotent and partition-parameterized.** Without this, none of the rest of the chapter works: retries would duplicate data, backfills would corrupt it, and recovery would be manual surgery.
- **Prefer dataset-aware scheduling over wall-clock scheduling** wherever upstream arrival times are variable — jobs should run when the data lands, not when the clock guesses it might have.
- **Put quality gates at the boundaries**: ingestion expectations for schema, types, and ranges at the door, and pre-publish tests for uniqueness, referential integrity, and freshness before results are visible. Fail the task, quarantine the partition — never propagate known-bad data.
- **Set freshness SLOs per consumer, and treat them as contracts**: stated, measured, and alerted. They are the operational form of the serving SLAs from ch16.
- **Monitor data, not just jobs**: freshness, volume, schema, and distribution — the four observability pillars — because green DAGs deliver wrong data every week.
- **Circuit-break on gate failure**: quarantine the partition, halt downstream tasks, page a human. Yesterday's right data with a banner beats today's wrong data silently.
- **Attribute cost per team.** The bill nobody owns is the bill nobody manages.

## Failure Modes

- **The silent half-load** (ch07's classic, now detectable). The job is green, but upstream delivered only 60% of the rows; without a volume-anomaly gate, the dashboards run wrong for a week and someone notices by eyeball. Detection is a `row count vs 7-day median` check — the cheapest test in this book, and the one that pays for this entire chapter the first time it fires.
- **Cascading corruption.** One bad partition passes through four downstream DAGs before a human asks "why is revenue negative?" — and now four tables need cleanup, not one. The circuit breaker exists precisely to make this impossible: the first gate quarantines the partition and halts the chain.
- **2am cron theater.** The DAG runs on schedule whether or not upstream data arrived, "succeeds" over partial data, and gets backfilled nightly — an operations team paying daily for a missing scheduling feature. Dataset-aware scheduling is an upgrade, not a luxury.
- **Test sprawl without ownership.** 900 dbt tests exist, 40 have been failing "temporarily" for a month, and nobody is paged. Tests that do not gate anything are documentation of decay, not quality: once the team learns to ignore red checks, the gates have no force at all. Every failing test needs an owner and a ticket.
- **The unmonitored freshness SLO.** The SLA exists in a document; the 6:30am miss is discovered by the CFO at 9am when the dashboard is stale. An SLO without an alert is a wish — the promise was never actually enforced.
- **Cost blackout.** The platform bill doubles over two quarters — a streaming table's compaction backlog, a morning-queue pattern from ch16, and retention creep all adding up — and nobody sees it until finance asks. Chargeback is the detection layer: with per-team attribution, the curve shows up on the responsible team's report months earlier.

## Interview Narration

“I treat operations as part of the architecture, and I start from the hardest truth: pipelines can be green while data is wrong, because job success measures execution, not correctness. So my design has two loops. The control loop: every task is idempotent and parameterized by partition, so retries and backfills are safe by construction; scheduling is dataset-aware, so jobs run when data lands rather than when the clock says so; and freshness SLOs per consumer with alerts attached, so a missed 6am promise pages someone instead of waiting for a complaint.

The correctness loop is data quality as gates, not dashboards. At ingestion, expectations against the schema registry — types, nulls, ranges — with rejects quarantined. Before publishing, dbt-style tests on uniqueness and referential integrity. Then statistical anomaly checks on volume and distribution, because the failure nobody wrote an assertion for is the one that ships. On gate failure, a circuit breaker: quarantine the partition, halt downstream tasks, page a human. The principle is that yesterday's right data with a banner beats today's wrong data silently — propagating known-bad data is the one unforgivable pipeline behavior.

And I monitor data, not just jobs — freshness, volume, schema, and distribution as the four pillars — because data downtime is measured from bad data delivered to bad data detected, and I want that number in minutes, not weeks. The same discipline applies to cost: per-team attribution of warehouse and pipeline spend, because the bill nobody owns is the bill nobody manages. I would close with the cultural point: a pipeline that fails loudly is a good pipeline; one that silently produces wrong numbers is the worst system in the company.”

---

| <- Previous | Next -> |
|---|---|
| [Chapter 20 — Security, Governance & Catalogs](ch20-security-governance-and-catalogs.md) | [Chapter 22 — Production Operations Runbooks](ch22-data-operations-runbooks.md) |
