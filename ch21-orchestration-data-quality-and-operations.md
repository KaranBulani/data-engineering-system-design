# Chapter 21 — Orchestration, Data Quality & Operations

> Part VII — Security & Operations

Pipelines don't fail loudly enough. The operational layer — orchestration,
quality gates, freshness SLOs, and the monitoring of *data itself* (not just
jobs) — is what separates a platform from a pile of cron jobs. This is also
the chapter where "data downtime" becomes a first-class engineering concept.

## The Question

*"How do I schedule, sequence, and re-run pipelines safely; how do I prevent
bad data from propagating; and how do I even *notice* when data is silently
wrong?"*

## The Physics

### Orchestration: DAGs, sensors, datasets

An orchestrator (Airflow, Dagster) runs **DAGs** — directed acyclic graphs of
tasks with dependencies. The concepts that matter:

- **Idempotent tasks** (the ch07 contract, now mandatory): a task re-run
  produces the same result. Partition-parameterized jobs
  (`{ds}`-templated) make every run safe to replay. This is *the*
  prerequisite for everything else in this chapter — retries, backfills,
  and recovery all assume idempotency.
- **Sensors vs datasets**: the old pattern polls ("is the file there yet?"
  every minute); the modern pattern is **dataset-aware / data-driven
  scheduling** — the downstream DAG triggers when upstream *data* lands
  (Airflow datasets, Dagster assets/partitions), not when the clock says so.
  This kills the 2am-runs-whether-or-not-data-arrived waste of ch07.
- **Retries vs replay**: task-level retries for transient failures (network,
  node loss); full replay for logic bugs — which is only possible because
  tasks are idempotent and parameterized.
- **Backfills as first-class operations**: trigger the same job across a
  date range; the orchestrator parallelizes (within reason) and tracks
  state. A "backfill" that is a hand-edited script is a design smell (ch07).
- **SLAs and alerting**: an SLA miss on freshness (data should be there by
  6am) pages a human; the DAG is the enforcement point.

### Data quality: gates, not vibes

The layered quality model, in pipeline order:

1. **Ingestion expectations**: schema conformance (against the registry,
   ch18), nullability, types, ranges — reject/quarantine at the door. The
   webhook/log chaos of ch05 meets its first gate.
2. **Tests** (dbt tests, Great Expectations): uniqueness, not-null,
   referential integrity, accepted values, freshness — assertions on
   *transformed* tables, run as part of the pipeline (a failing test fails
   the task, on purpose).
3. **Anomaly detection on volume/distribution**: today's row count is 40%
  of the 7-day median; the `status` column's null rate jumped from 0.1% to
  9%. Statistical, not assertion-based — catches what nobody thought to
  assert.
4. **Freshness SLOs**: per-consumer guarantees ("exec dashboard data
  through yesterday, by 6:30am") — measured, alerted, owned (ch02's
  requirements, now operationalized).
5. **Circuit breakers / quarantine**: on gate failure, *stop propagation* —
  quarantine the partition, halt downstream tasks, page a human. The
  alternative is today's wrong data flowing into every dashboard and
  downstream table, which is strictly worse than yesterday's right data.
   "Yesterday's data with a banner" beats "today's wrong data silently."

The **trust score** idea (data observability products formalize this):
freshness, volume, schema, distribution — each dataset continuously scored;
degradation alerts before consumers notice. You can build the poor-version
in-house: the four checks above on a schedule, results to a table, alerts on
regression.

### Operations: data downtime

The DRE (data site reliability) concept: measure and manage **data
 downtime** — time between bad data being *delivered* and being *detected*
 (MTTD), and between detection and *resolution* (MTTR). The inversion that
 defines the discipline: **pipelines can be green while data is wrong** —
 job success measures execution, not correctness. Hence:

- Monitor *data* (freshness, volume, schema, distribution), not just
  *jobs* (exit codes).
- **Bad data delivered is an incident** even when every DAG is green —
  incident review for data (blameless postmortems, root cause, prevention)
  like software.
- The cost asymmetry drives everything: wrong-number-in-the-dashboard is
  discovered by customers (maximum blast radius, minimum control); the
  quality-gated pipeline discovers it at 6:15am (minimum radius).

### Cost observability

The last operational duty: **per-team chargeback** and query cost
attribution (warehouse per-query bytes/credits — ch13/16), pipeline compute
by DAG, storage by table/lifecycle stage. Not (only) for finance: teams
that see their own cost curves self-correct the `SELECT *` habit, the
unbounded retention, and the over-provisioned warehouse (ch02's cost axis,
closed-loop). An unattributed bill is an unbudgeted bill.

## The Options

| Layer | Options | Selection reality |
|---|---|---|
| Orchestrator | Airflow / Dagster / Prefect / managed (MWAA, Composer) | Airflow default; Dagster when asset-centric |
| Quality framework | dbt tests / Great Expectations / Soda / custom | dbt-native if dbt shop |
| Observability | Monte Carlo / Elementary / in-house checks | product vs build by scale |
| SLO tracking | orchestrator SLAs / custom freshness jobs | must exist somewhere, owned |

The honest sizing note: a three-person team runs Airflow + dbt tests +
hand-rolled freshness checks and that's *plenty*; a 30-team platform needs
the productized versions. Match machinery to stakes (ch06's rule,
operationalized).

## Decision Rules

- **Every task idempotent and partition-parameterized** — or none of the
  rest of this chapter works.
- **Dataset-aware scheduling over wall-clock** wherever upstream arrival is
  variable.
- **Quality gates at the boundaries**: ingestion expectations (schema,
  ranges) and pre-publish tests (uniqueness, referential, freshness) — fail
  the task, quarantine the partition, never propagate known-bad data.
- **Freshness SLOs per consumer** — stated, measured, alerted; they are the
  contract between platform and consumers (ch16's serving SLAs, monitored).
- **Monitor data, not just jobs**: volume, schema, distribution, freshness
  — the four observability pillars — because green DAGs deliver wrong data
  every week.
- **Circuit-break on gate failure**: quarantine + halt downstream + page.
  Yesterday's right data beats today's wrong data.
- **Cost attribution per team** — the bill nobody owns is the bill nobody
  manages.

## Failure Modes

- **The silent half-load** (ch07's classic, now detectable): job green,
  upstream delivered 60% of rows; without a volume-anomaly gate the dashboards
  run wrong for a week. Detection is a `rowcount vs 7-day median` check —
  the cheapest test in this book.
- **Cascading corruption**: one bad partition passes through four
  downstream DAGs before a human asks "why is revenue negative?" — the
  circuit breaker exists precisely to make this impossible.
- **2am cron theater**: DAG runs on schedule regardless of data arrival,
  "succeeds" over partial data, backfills nightly. The dataset-aware fix
  is an upgrade, not a luxury.
- **Test sprawl without ownership**: 900 dbt tests, 40 failing "temporarily,"
  nobody paged — tests that don't gate are documentation of decay, not
  quality.
- **The unmonitored freshness SLO**: the SLA exists in a doc; the 6:30am
  miss is discovered by the CFO at 9am. An SLO without an alert is a wish.
- **Cost blackout**: platform bill doubles over two quarters (streaming
  table compaction backlog + a ch16 morning-queue pattern + retention
  creep); nobody sees it until finance asks. Chargeback is the detection
  layer.

## Interview Narration

"I'd treat operations as part of the architecture, and start from the
hardest truth: pipelines can be green while data is wrong — job success
measures execution, not correctness. So my design has two loops. The
control loop: every task idempotent and partition-parameterized, so retries
and backfills are safe by construction; dataset-aware scheduling so jobs
run when data lands, not when the clock says so; and SLAs with alerts on
freshness per consumer.

The correctness loop is data quality as gates, not dashboards: ingestion
expectations against the schema registry — types, nulls, ranges — then
dbt-style tests pre-publish on uniqueness and referential integrity, then
statistical anomaly checks on volume and distribution, because the failure
nobody asserted is the one that ships. On gate failure, a circuit breaker:
quarantine the partition, halt downstream, page a human. The principle is
that yesterday's right data with a banner beats today's wrong data
silently — propagation of known-bad data is the one unforgivable pipeline
behavior.

And I'd monitor data, not just jobs — freshness, volume, schema,
distribution as the four pillars — because 'data downtime' is measured from
bad data delivered to bad data detected, and I want that number in minutes,
not weeks. Same discipline for cost: per-team attribution of warehouse and
pipeline spend, because the bill nobody owns is the bill nobody manages.
Close with the cultural point: a pipeline that fails loudly is a good
pipeline; one that silently produces wrong numbers is the worst system
in the company."

---

| <- Previous | Next -> |
|---|---|
| [Chapter 20 — Security, Governance & Catalogs](ch20-security-governance-and-catalogs.md) | [Chapter 22 — Production Operations Runbooks](ch22-data-operations-runbooks.md) |
