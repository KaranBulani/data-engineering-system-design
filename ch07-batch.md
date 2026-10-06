# Chapter 07 — Batch

> Part III — Processing Paradigms

Batch processing means collecting data for a period of time and processing that collection together. For example, a job might process all orders from yesterday every morning. Batch is not an outdated approach. It is still the normal choice for many companies because it is easier to operate, costs less when real-time results are unnecessary, and can be retried safely.

Streaming is useful when a business truly needs results within seconds or a few minutes. If no such requirement exists, batch is often the more practical design. The important engineering question is not whether batch sounds modern. It is whether the required freshness justifies the extra complexity and cost of a continuously running streaming system.

## The Question

When is data being ready in minutes or hours not only acceptable, but actually the right design? The answer is usually: when the business can wait and when processing a complete time window makes correctness and recovery easier.

The batch design should answer three practical questions:

- Can the same time window be processed again without creating incorrect duplicates?
- Can we process an old time window later if a bug is discovered?
- Can we control cost by reading only the data that is needed?

These properties are called idempotency, backfillability, and cost efficiency.

## The Physics

### Why batch wins on fundamentals

1. **A bounded input is easier to make correct.** A batch job processes a known input window, such as one day of events. Once the input is complete, the job does not have to keep waiting for new records. It does not need to decide how to handle late records while an aggregation is still open, and it does not need to keep large amounts of state alive between restarts. Many difficult streaming problems, discussed in ch08, do not appear in the same form.
2. **Reprocessing is a normal recovery method.** If a transformation has a bug, fix the code and run the affected window again. The old input is still available, so the corrected output can replace the incorrect output. This is much simpler than trying to repair a continuously changing streaming state store.
3. **Batch compute can be purchased only when it is needed.** A batch job can use spot or preemptible machines, which are cheaper but can be interrupted. An interruption is acceptable because the job can retry. It can also use a temporary cluster or serverless query pricing. A streaming system usually has to keep resources running all day and therefore pays continuously.
4. **The tools are mature.** Spark, warehouse SQL, dbt, and Airflow have large user communities and well-understood operational patterns. This means teams can find tested solutions for scheduling, retries, monitoring, and transformations.

### Scheduled windows and incremental strategies

A batch pipeline has two basic parts: a scheduler and a strategy for deciding which data to process. The scheduler may be cron, Airflow, or a managed workflow service. The incrementality strategy determines how new results are written.

| Strategy | Meaning | How repeated runs stay safe |
|---|---|---|
| **Append** | Add only new rows and never change old rows | Partition the data and remove duplicates using an event ID when reading or writing |
| **Overwrite-partition** | Recompute one day or month and replace that whole partition | The same input produces the same replacement output |
| **Merge/upsert** | Apply changes to existing rows using keys | Update each business key in an idempotent way |

**Overwrite-partition is usually the simplest and most useful default.** Suppose we calculate an aggregate for September 20:

```sql
INSERT OVERWRITE TABLE events_agg PARTITION (dt='2026-09-20')
SELECT ... FROM raw WHERE dt='2026-09-20'
```

The command computes the complete result for that date and replaces the existing September 20 partition. If it runs once, three times, or once after a partial failure, the final partition has the same result, assuming the input has not changed. Replacing a bounded partition is a simple way to achieve idempotency. It does not require a complicated exactly-once protocol or a special transactional output system.

This method has two important conditions. First, the input window should be complete before the output is treated as final. If records arrive later, the job must be run again. Second, the partition should not be so large that recomputing it becomes too expensive.

### Late data in a batch world

Batch does not make late data disappear. It handles late data by running the affected window again. For example, a company might process yesterday at 6:00 a.m. and then reprocess the previous week every Sunday to catch records that arrived late.

This is simple and valid, but it needs an explicit rule. Imagine that the daily partition is written at 12:10 a.m. and some source records arrive at 12:40 a.m. If no later job includes those records, the dashboard remains wrong forever. A good design uses one or both of these protections:

- Define an ingestion cutoff and keep an audit of records that arrive after the cutoff.
- Run a reconciliation job that compares raw input counts with output partition counts and reruns a partition when the numbers do not agree.

The important point is that a successful job does not necessarily mean a complete result. The job may have succeeded technically while processing an incomplete input window.

### Failure semantics

Batch systems usually have several levels of retry:

- **Task retry:** A distributed engine such as Spark divides a job into tasks. If one task fails on a machine, Spark can retry that task on another machine. The entire job does not necessarily need to restart.
- **Job retry:** If the job itself fails, the scheduler can run the complete time window again. Idempotent writes make this safe. This is why idempotency is a core design requirement, not a minor convenience.
- **Backfill:** A backfill runs the normal job for an old time window. The job should accept the window as a parameter, such as `{ds}` in Airflow. Then “backfill January 1 through August 31” means running the same job with 243 different dates. There should not be a separate hand-written backfill program that can drift away from production logic.

### The cost model, concretely

- **Spot or preemptible compute:** These machines are cheaper but may be interrupted. Batch can use them because a failed run can be retried. Discounts of 60–80% are common, although the exact price depends on the cloud and workload.
- **Serverless batch:** Services such as Databricks serverless, BigQuery, and Athena charge for query or execution usage and do not require a team to manage a long-running cluster. This is often suitable for occasional or smaller workloads.
- **Autoscaling clusters:** For regular, heavier pipelines, a cluster can start for the scheduled run and shut down afterward. This is usually cheaper than keeping a large cluster running between jobs.
- **Reduce the amount scanned before buying faster machines.** Partition pruning lets the engine skip unrelated partitions. Columnar formats let it read only the needed columns. Incremental windows avoid scanning all history. A slow job is often slow because it reads too much data, not because its machines are too small.

### The honest downsides

- **Batch has a latency floor.** Freshness is at least the schedule interval plus the job runtime. A job that runs every hour and takes 20 minutes cannot provide reliable 30-second freshness.
- **A scheduled job may run before upstream data is ready.** A 2:00 a.m. job might start whether the input arrived at 1:00 a.m. or 2:30 a.m. Dataset-aware scheduling, discussed in ch21, can wait for upstream completion instead.
- **Small files can accumulate.** Many tiny output files increase metadata overhead and make reads slower. A compaction job may be needed to combine small files into larger ones, which becomes another scheduled operational responsibility.

### Modeling the window: grain, boundary, and the cutoff

The phrase “run a daily batch” hides three design choices.

1. **Grain:** Grain is the size of each processing window: an hour, a day, or a month. Choose it based on how much data must be recomputed and how late data can arrive. If records can arrive up to 48 hours late, daily partitions with a 48-hour reprocessing rule may be simpler than hourly partitions that require 48 separate reruns. The partition size should serve recovery and query needs, not just look tidy.
2. **Boundary:** The boundary states when a window becomes final. For example, “the September 20 partition becomes final when the September 21 6:00 a.m. job starts.” Before that point, the partition is provisional and may be replaced. After that point, new records go to a late-arrivals process and are included during a later reconciliation.
3. **Write behavior:** Every write pattern needs an explicit duplicate-handling rule:

| Write pattern | Idempotent? | Rule |
|---|---|---|
| `INSERT OVERWRITE` partition | Yes | Prefer this when recomputing a complete window |
| Append plus deduplication key | Yes, if deduplication is enforced | Deduplicate by an event ID from the common event envelope |
| `MERGE` on a business key | Yes, if the merge rule is deterministic | For last-write-wins, use event time and a tie-breaker |
| Plain `INSERT INTO` | **No** | A retry can add the same rows again; do not use it for production windows without a deduplication plan |

### The backfill economics of grain

Grain affects how much work a backfill requires. With daily partitions, correcting one day means replacing one partition. With hourly partitions, the same day is represented by 24 partitions, so the orchestration and metadata work are greater even if the total data volume is similar.

Choose the partition grain based first on recovery behavior and recomputation cost. Dashboard filters are also important, but they should not be the only reason for choosing a grain.

## The Options

| Batch shape | When it fits |
|---|---|
| Warehouse-native ELT with dbt | Transformations are mainly SQL and the data already lands in a warehouse |
| Spark or Databricks batch | Transformations are computationally heavy, require non-SQL code, or include large-scale machine learning |
| Serverless SQL such as Athena or BigQuery | Queries are occasional or exploratory and the data is in a lake |
| Scheduled scripts | The workload is genuinely tiny; do not build a distributed platform for 200 rows |

ELT and ETL are two ways to order the work. In ELT, raw data is loaded into the warehouse or lakehouse first, and SQL transformations run there afterward. This is a common modern default because warehouse compute can scale and SQL skills are widely available. In ETL, data is transformed before it is loaded. ETL is still useful when transformation is necessary to reduce the volume, remove sensitive information, or make the data acceptable to the destination.

## Decision Rules

- If there is no stated need for sub-minute freshness, start with batch. A streaming design should be justified by a measurable latency requirement, not by the assumption that streaming is always better.
- Prefer overwrite-partition as the default way to write a recomputed window because it makes retries predictable.
- Parameterize every job by its time window from the beginning. This makes backfills ordinary scheduled runs instead of special projects.
- Choose partition grain according to recomputation cost and the expected late-data window, not only according to dashboard filters.
- Use ELT when the target is a warehouse and the logic is mostly SQL. Use Spark when the logic is computationally heavy, requires custom code, or operates at a scale that warehouse SQL cannot handle economically.
- Check upstream completeness before processing. A green job that processed only half of the expected input is one of the most dangerous batch failures because it looks healthy.
- Reduce the amount of data scanned before increasing machine size. Use partitioning, columnar storage, and incremental windows first.

## Failure Modes

- **Silent partial input:** The job succeeds while only 60% of the expected data is available. The error may be discovered weeks later when totals no longer reconcile. Check completeness before the transformation starts.
- **Non-idempotent incrementality:** A job uses `INSERT INTO` without deduplication. Retries and manual reruns add the same rows again, so totals increase after every incident.
- **Late records after partition close:** Records arrive after yesterday's partition is considered final. The dashboard is wrong until a reconciliation job finds the late records. If there is no reconciliation, customers may find the problem first.
- **The eternal full scan:** A job called “daily” scans all historical data every day. Its cost and runtime grow as the company accumulates data. The bill eventually becomes the incident.
- **Backfill by hand-edit:** Someone creates a one-off script for an old date. That script slowly becomes different from the production transformation, so the historical data is calculated by different rules. Backfills should call the same parameterized job as normal runs.
- **Batch insecurity:** A team proposes Spark for 50,000 rows per day. The cluster and orchestration overhead are greater than the value of the data. Choose the simplest tool that satisfies the requirement.

## Interview Narration

“My default is batch unless a requirement forces streaming. A bounded time window makes correctness easier because the job can work on a known set of input, without continuously managing watermarks or open streaming state. If the result is wrong, I can fix the transformation and rerun the affected window. Batch also allows cheaper compute, including temporary or preemptible machines, because resources are needed only while the job runs.

I would design scheduled jobs that accept a time window as a parameter and write with overwrite-partition semantics. The same code should calculate today's partition or backfill January. Running the job three times should produce the same final result as running it once because the complete partition is replaced atomically.

For late data, I would define an ingestion cutoff, keep a late-arrivals audit, and periodically rerun recent windows. I would also check upstream completeness before starting the transformation, because a successful job over half the expected data is still a failed data product.

Finally, I would control cost by reducing the amount scanned: partition the data, use columnar formats, and process only the required windows. If the requirement is reliable data within 30 seconds, batch is the wrong tool. That is a streaming requirement and should be designed, operated, and budgeted as such.”

---

| <- Previous | Next -> |
|---|---|
| [Chapter 06 — CDC, the Long Tail & Normalization](ch06-cdc-long-tail-and-normalization.md) | [Chapter 08 — Streaming](ch08-streaming.md) |
