# Chapter 16 — The Serving Layer & Consumption

> Part VI — Serving, Formats & Spark

The last mile: how consumers — BI tools, analysts, applications, other
pipelines — actually touch your data. Teams over-build the pipeline and
under-design the serving layer, then discover their "real-time platform" ends
in a dashboard that queries a table nobody sized for 40 concurrent users.
Serving is part of the system.

## The Question

*"Who consumes this data, with what latency and concurrency, through what
protocol — and where should the query actually run?"*

## The Physics

### The four serving patterns

| Pattern | Consumers | Latency | When |
|---|---|---|---|
| **Warehouse-direct** | BI (Tableau/Looker/PowerBI), analysts, SQL clients | seconds-minutes | the modern default for analytics |
| **Pre-aggregated marts** | dashboards with fixed hot queries | sub-second | scans too expensive / concurrency too high for raw |
| **KV serving layer** | product/app features | single-digit ms | user-facing reads (profiles, features, sessions) |
| **Search serving** | search UX, log exploration | tens of ms | full-text relevance, faceting |

**Warehouse-direct** works because modern warehouses isolate compute per
consumer group (ch13) and cache results. **Marts** re-enter when the physics
says no: 40 dashboards x 6 queries x every 5 minutes against a 10TB fact table
is a scan bill and a queue — pre-aggregate the hot paths (the *serving*
argument for summary tables, distinct from modeling-for-modeling's-sake).
**KV serving** is ch14/15's projection: pipeline or CDC-fed DynamoDB/Redis,
single-digit-ms reads for the app. **Search** likewise: a projection fed by
transforms, owned as serving infrastructure.

### Consumer-side JDBC/ODBC — what a connection actually is

BI tools don't "query the warehouse" by magic; they hold a driver connection.
Two standards, one reality:

| | JDBC | ODBC |
|---|---|---|
| World | Java | C-level ABI; everything else wraps it |
| Used by | Java services, Spark, Databricks, many BI backends | Tableau/PowerBI/Excel on Windows, C++ tools |
| Latency note | JVM pooling behavior matters | driver manager layers add quirks |

What the connection carries — read a Snowflake JDBC URL once and it never
looks like magic again:

```
jdbc:snowflake://myaccount.snowflakecomputing.com/?warehouse=BI_WH&role=ANALYST&db=ANALYTICS&schema=PUBLIC
       \_______/ \_____________________________/  \_________/ \__________/ \________\________/
       protocol   account gateway (region)         compute     authz        namespace
                                                  resource    (role)
```

The **compute resource in the connection string** is the key serving-layer
lever: point BI at `BI_WH` (small, auto-suspend, concurrency-queued) and data
science at `DS_WH` (big, aggressive) — same data, different queues, different
bills. This is workload management implemented at connection time.

The BI connection lifecycle that bites: every dashboard refresh opens
connections (often several parallel), each burning a concurrency slot;
auto-suspend warehouses wake with cold-start latency; long-running extract
refreshes hold slots. Without **connection pooling** (BI server-side pools,
driver-level) and **result caching** (ch13), a 40-dashboard fleet DDoSes
itself every morning. Timeout/retry semantics are the hidden reliability
layer: BI tools retry on timeout — if your queries run 90s against a 60s
timeout, the retry doubles load and the dashboard "randomly" fails.

**Auth**: password -> keypair (Snowflake style) -> OAuth/SSO (Okta/Entra) ->
service accounts for machines. The senior default: humans via SSO, services
via short-lived credentials, no shared dashboard accounts — because
chargeback and audit (ch20) are impossible against a shared login.

### Semantic layers — one definition of "revenue"

dbt metrics, LookML, Cube: a shared definition layer between warehouse and
BI — `revenue` defined once, exposed consistently to every tool. Two birds:
consistency ("why do these two dashboards disagree" — the ch15
reconciliation pain, prevented at the definition layer) and governance (the
metric is code-reviewed, versioned, owned). Plus **pre-aggregation
acceleration** (Cube-style): hot queries answered from pre-built aggregates
with automatic rewrite — the mart pattern, automated.

### Workload management — the fairness layer

Warehouse concurrency is slots/queues (Snowflake) or slots (BigQuery). The
serving-layer duties:

- **Separate queues per consumer class** — BI vs batch vs ad-hoc (via
  separate compute resources, ch13, or query queueing).
- **Rate-limit noisy consumers** — the analyst's `SELECT *` at 9am; resource
  monitors / per-query timeouts / maximum scan bytes.
- **Prioritize within SLAs** — the exec dashboard's freshness SLO (ch02)
  outranks curiosity queries; implement via queue priority, not politeness.

### Where a query should run — the decision table

| If the consumer needs... | Run it in... |
|---|---|
| Ad-hoc SQL, joins, exploration | warehouse / lakehouse engine |
| Sub-second fixed dashboards | marts / semantic-layer pre-ags / result cache |
| Millisecond app reads | KV projection (CDC-fed) |
| Full-text search | search projection |
| ML training scans | lakehouse (batch), not the BI warehouse |

### The fifth pattern: real-time OLAP engines

Between warehouse-direct and KV serving sits a family built for exactly the
"fresh dashboards at scale" case: **real-time OLAP** — ClickHouse, Apache
Druid, Apache Pinot (and StarRocks/Doris in the same class).

| | Warehouse (Snowflake/BQ) | Real-time OLAP (ClickHouse/Pinot/Druid) | KV (Redis/DynamoDB) |
|---|---|---|---|
| Freshness | minutes-hours (batch loads) | **seconds** (streaming ingest) | milliseconds (CDC-fed) |
| Query shape | ad-hoc SQL, joins, BI | **aggregation scans** over recent data, filters, group-bys | point lookups by key |
| Latency | seconds-minutes | **sub-second, high concurrency** | single-digit ms |
| Joins/ad-hoc | full SQL | limited/denormalized-first (star-schema pre-joined) | none |
| Sweet spot | BI + analysts | user-facing analytics, fresh operational dashboards | feature/profile reads |

The pattern: streaming pipeline (Kafka → Flink/micro-batch) **also** writes
into the OLAP engine; dashboards query it directly. This is the standard
answer to "exec wants live dashboards" when warehouse-direct is too stale
and marts are too heavy — fresh *and* sub-second *and* concurrent, without
building a KV projection per dashboard. The costs to narrate: another
operational system (ch22's ops bill grows), denormalization-first modeling
(pre-join at ingest — joins at query time are weak), and per-engine
sharding/compaction care. Placement rule: **real-time OLAP when the
*consumer* is a dashboard/user-facing UI needing fresh aggregates; KV when
the consumer is application code needing records by key.**
## The Options

| Serving design | Cost profile | Ops profile | Risk |
|---|---|---|---|
| All warehouse-direct | per-scan | lowest | morning queue storms; bill shocks |
| Marts everywhere | storage + refresh | moderate | stale marts; modeling sprawl |
| KV for everything | projection infra | highest | KV as poor man's warehouse creep |
| Layered (warehouse + marts + KV by consumer class) | balanced | moderate | requires the decision table above, enforced |

## Decision Rules

- **Classify consumers before sizing the pipeline**: internal analysts vs
  customer-facing features vs exec dashboards — each maps to a different
  serving pattern.
- **Warehouse-direct by default; add marts when physics (scan cost x
  frequency x concurrency) says no.**
- **Millisecond product reads go to a KV projection, never the warehouse.**
- **Put the compute resource in the connection string** — separate BI from
  batch from data science at connection time.
- **Pool connections, cache results, set timeouts above p99 runtime** — the
  three unglamorous settings that prevent most "the dashboard is down."
- **Humans via SSO, services via short-lived creds; no shared accounts.**
- **One semantic layer for contested metrics** — "revenue" defined once, or
  you will reconcile forever (ch15).

## Failure Modes

- **The morning queue storm**: 40 dashboards refresh at 8am against one
  warehouse; slots exhaust; timeouts; BI retries double the load; the
  warehouse is "slow" every day and fine at noon. Fix: pooling, caching,
  queue separation, staggered refreshes.
- **Timeout-retry feedback loop**: p99 runtime creeps past the BI timeout;
  every retry re-scans; the system fails under the weight of its own
  retries.
- **The shared `analyst@company.com` connection**: impossible chargeback,
  impossible audit, one leaked password from losing attribution entirely.
- **KV creep**: someone points the product at the KV for "one quick
  aggregation"; the projection becomes load-bearing for queries it was never
  shaped for (ch14's query-first contract violated at serving time).
- **Un-owned marts**: 300 summary tables "for performance," none documented,
  half wrong — the warehouse equivalent of ch15's projection-without-owner.
- **Excel via ODBC against prod warehouse**: one analyst's pivot table
  full-scans the fact table every refresh; the cost page shows one query
  bigger than the rest of the week.

## Interview Narration

"I treat serving as part of the architecture, not the end of the diagram. The
first question is who consumes: analysts and BI tools map to
warehouse-direct — that's the modern default — with pre-aggregated marts only
when the physics demand it: scan cost times refresh frequency times
concurrency. Product-facing features needing single-digit milliseconds map to
a KV projection fed by CDC, and search UX to a search index. Same truth, four
projections, each with an owner (ch15).

On the BI side I'd get concrete: connections are JDBC or ODBC, and the
connection string is a workload-management lever — I put the BI fleet on its
own compute resource, separate from batch and data science, so a dashboard
storm can't starve the pipelines. Then the three unglamorous settings that
prevent most 'the dashboard is down' incidents: connection pooling, result
caching, and timeouts set above p99 runtime so retries don't create a
feedback loop. Humans authenticate via SSO, services via short-lived
credentials — shared BI accounts make chargeback and audit impossible.

And for contested metrics I want a semantic layer — revenue defined once, in
code, reviewed — because the alternative is two dashboards disagreeing and
the meeting where we find out why. If the case has exec dashboards with
freshness SLOs, I'd add queue priority so the SLA queries outrank the
curiosity queries — fairness in the serving layer is implemented with
queues, not politeness."

---

| <- Previous | Next -> |
|---|---|
| [Chapter 15 — Polyglot Persistence](ch15-polyglot-persistence.md) | [Chapter 17 — Row vs Columnar](ch17-row-vs-columnar.md) |
