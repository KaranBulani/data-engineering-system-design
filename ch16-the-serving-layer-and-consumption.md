# Chapter 16 — The Serving Layer & Consumption

> Part VI — Serving, Formats & Spark

This chapter is about the **last mile**: how consumers — BI tools, analysts,
applications, other pipelines — actually get their hands on your data. The
pipeline that produces the data is only half the story; the *serving layer*
is whatever sits between your stored data and the person or program reading
it, and it has its own engineering problems.

Teams routinely over-build the pipeline and under-design the serving layer.
The classic ending: you have a sophisticated "real-time platform," but it
terminates in a dashboard that queries a table nobody sized for 40
concurrent users. That last phrase deserves unpacking, because it is the
whole chapter in miniature. A warehouse does not answer unlimited questions
at once — it has a fixed amount of query slots (think of them as checkout
lanes). "40 concurrent users" means 40 dashboards refreshing at once, each
issuing queries, each occupying lanes; if the table is huge and the queries
scan it whole, the lanes jam and everyone waits. Nobody sized the table —
nobody asked "how many people will query this, how often, and how big is
each query?" — so the platform fails at the point where it touches humans,
not in the fancy pipeline. **Serving is part of the system.**

## The Question

*"Who consumes this data, with what latency and concurrency, through what
protocol — and where should the query actually run?"*

Each part of that question matters: *who* (analysts? a customer-facing app?
executives?), *latency* (is a 30-second wait fine, or must it be 10
milliseconds?), *concurrency* (one analyst, or 5,000 users hitting refresh?),
*protocol* (SQL over JDBC? an HTTPS API? a search endpoint?), and *where the
query runs* (the big expensive warehouse, a pre-computed summary, or a
key-value store).

## The Physics

### The four serving patterns

| Pattern | Consumers | Latency | When |
|---|---|---|---|
| **Warehouse-direct** | BI (Tableau/Looker/PowerBI), analysts, SQL clients | seconds-minutes | the modern default for analytics |
| **Pre-aggregated marts** | dashboards with fixed hot queries | sub-second | scans too expensive / concurrency too high for raw |
| **KV serving layer** | product/app features | single-digit ms | user-facing reads (profiles, features, sessions) |
| **Search serving** | search UX, log exploration | tens of ms | full-text relevance, faceting |

Each pattern, in plain words:

- **Warehouse-direct** means the BI tool queries the warehouse (or
  lakehouse) directly — raw detail tables, real SQL, no copies. It works as
  the default because modern warehouses (chapter 13) do two things for you:
  they run *separate compute per consumer group* (the BI team's queries get
  their own compute resource, so they can't starve anyone else's) and they
  *cache results* (the same query asked twice in a row is answered from the
  cache, not re-scanned). For most analytics, seconds of latency is fine and
  this is the simplest thing that works.
- **Pre-aggregated marts** are summary tables you compute ahead of time.
  "Marts re-enter when the physics says no" — meaning: do the arithmetic on
  the raw path. 40 dashboards × 6 queries each × every 5 minutes is 40 × 6 ×
  12 = **2,880 queries per hour**, each scanning a 10 TB fact table. That is
  a scan bill (you pay per data scanned) and a queue (those queries compete
  for the same lanes). The fix is to pre-compute the hot paths: a small
  `daily_revenue_by_region` table that answers the dashboard in milliseconds
  instead of re-deriving it from 10 TB every five minutes. Note the *reason*
  for the summary table here is serving performance — distinct from
  modeling-for-modeling's-sake (building summaries because a methodology says
  so). These marts are chapter 15's "derived projections" wearing a different
  hat.
- **KV serving** is the key-value projection from chapters 14/15: a pipeline
  or CDC feeds DynamoDB/Redis, and the application reads user profiles,
  feature flags, or sessions in single-digit milliseconds. The consumer is
  application code, not humans.
- **Search serving** is likewise a projection — a search index fed by
  transforms — owned as serving infrastructure (chapter 14 covered why it
  must not become an analytics warehouse).

### Consumer-side JDBC/ODBC — what a connection actually is

BI tools don't "query the warehouse" by magic. Under the hood, the BI tool
holds a **connection** — a live, authenticated session — opened through a
**driver**: a piece of software that knows how to speak the database's wire
protocol and translate it for the BI tool. Two standards dominate:

| | JDBC | ODBC |
|---|---|---|
| World | Java | C-level ABI; everything else wraps it |
| Used by | Java services, Spark, Databricks, many BI backends | Tableau/PowerBI/Excel on Windows, C++ tools |
| Latency note | JVM connection pooling behavior matters | driver manager layers add quirks |

Why two standards: **JDBC** is the Java standard — any Java program (Spark,
Databricks, many BI backends) uses it. **ODBC** is the older C-level
standard ("ABI" — application binary interface — means programs link against
it directly at the machine level); everything non-Java wraps it, which is
why the Windows-native tools (Tableau, PowerBI, Excel) speak ODBC. The
practical notes: with JDBC, how the JVM pools (reuses) connections affects
latency; with ODBC, the extra driver-manager layers add their own quirks.
Either way, a connection is a real, limited resource — which the next part
is about.

A connection is described by a **connection string** (a URL carrying all the
parameters). 
```
jdbc:<subprotocol>://<host>:<port>/<database>?<query-params>
jdbc:mysql://localhost:3306/mydb?useSSL=false&serverTimezone=UTC

┌─────────────────── JDBC connection address ───────────────────┐
│                                                               │
jdbc:mysql://localhost:3306/mydb?useSSL=false&serverTimezone=UTC
│    │       │               │  │   │
│    │       │               │  │   └── Query parameters
│    │       │               │  └────── `?` starts query parameters
│    │       │               └───────────── Database
│    │       └─────────────────────── Host:Port
│    └─────────────────────────────── Subprotocol
└──────────────────────────────────── JDBC

jdbc:<subprotocol>://<host>:<port>/<database>?<parameter1>=<value1>&<parameter2>=<value2>
jdbc:postgresql://db.example.com:5432/sales?sslmode=require&connectTimeout=10
```

Read a Snowflake JDBC URL once and it never looks like magic
again:



```
jdbc:snowflake://myaccount.snowflakecomputing.com/?warehouse=BI_WH&role=ANALYST&db=ANALYTICS&schema=PUBLIC
JDBC scheme:   jdbc:snowflake:
Authority:     //myaccount.snowflakecomputing.com
Path:          /
Query string:  ?warehouse=BI_WH&role=ANALYST&db=ANALYTICS&schema=PUBLIC
```

- `jdbc:snowflake:` — the JDBC scheme/driver subprotocol; `//` introduces
  the URL authority (the host).
- `myaccount.snowflakecomputing.com` — the Snowflake account endpoint.
  Here, `myaccount` is the account identifier; the hostname identifies the
  account endpoint, not a separate gateway field.
- `/` — the URL path; this example uses the root path.
- `warehouse=BI_WH` — **which compute resource the queries will run on.**
- `role=ANALYST` — which authorization role the session runs under
  (determines what the user may read/write).
- `db=ANALYTICS&schema=PUBLIC` — the default database and schema context
  for the session.

**The compute resource in the connection string is the key serving-layer
lever.** Because the URL names the warehouse, you decide *at connection
time* which compute a consumer's queries land on. Point the BI tools at
`BI_WH` — a small warehouse that auto-suspends when idle and queues when
busy — and point data science at `DS_WH` — a big one, aggressive, allowed to
spend. Same data, different queues, different bills. A dashboard storm now
affects only `BI_WH`, and each group's spend is visible separately. This is
**workload management implemented at connection time** — no code changes,
just connection strings.

**The BI connection lifecycle that bites.** Every dashboard refresh opens
connections — often several in parallel, one per chart — and each one burns
a concurrency slot. Warehouses that auto-suspend to save money *wake up*
with cold-start latency (the first query after a suspend waits for compute
to boot). Long-running "extract" refreshes (a BI tool copying data into its
own local store) hold their slots the whole time. Without two countermeasures
— **connection pooling** (the BI server keeps a small set of connections
open and *reuses* them across requests, instead of opening fresh ones per
refresh) and **result caching** (chapter 13 — identical queries answered
from the cache) — a fleet of 40 dashboards DDoSes its own warehouse every
morning when everyone's scheduled refreshes fire at once.

**Timeout/retry semantics are the hidden reliability layer.** BI tools retry
a query when it times out. Now do the math: if your queries take 90 seconds
at the worst (p99 — the 99th percentile, the runtime level that 99% of
queries come in under) but the BI tool's timeout is 60 seconds, then the
slowest queries *always* die at 60s — after having already done most of
their work — and are retried *from scratch*, adding a second full scan to
the queue. Retries of doomed queries double the load, which makes everything
slower, which times out more queries. The dashboard "randomly" fails, and
the root cause is a timeout set below p99. Set timeouts above p99 runtime —
see the decision rules.

**Auth** — who is allowed to connect — has a maturity ladder:

1. **Password** per person: the start; simple but weak (reused passwords, no
   central control).
2. **Keypair** (Snowflake-style): the client signs requests with a private
   key instead of sending a password — no password to phish or leak.
3. **OAuth/SSO** (via Okta, Entra/Azure AD): humans log in with their
   company identity — one place to grant and revoke access when someone
   joins or leaves.
4. **Service accounts** for machines: pipelines and BI servers are
   non-human principals with their own short-lived credentials.

The senior default: **humans via SSO, services via short-lived credentials,
no shared dashboard accounts** — because *chargeback* (attributing warehouse
spend to the team that caused it) and *audit* (attributing access — who read
this data, when) are impossible against a login shared by 30 people
(chapter 20).

### Semantic layers — one definition of "revenue"

The problem: "revenue" gets defined in SQL inside each dashboard — one tool
counts gross including refunds, another nets them out, a third uses a
different date boundary. Both dashboards are "correct" against their own
query, and you get the chapter 15 reconciliation pain: two numbers, one
meeting.

dbt metrics, LookML, and Cube are **semantic layers** — a shared definition
layer between the warehouse and the BI tools. `revenue` is defined *once*,
in code, and every tool asks the semantic layer instead of hand-writing SQL.
This kills two birds:

- **Consistency** — every tool computes the metric from the same definition,
  so dashboards can't drift apart; the disagreement is prevented at the
  definition layer rather than reconciled after the fact.
- **Governance** — the metric is code: reviewed in a pull request,
  versioned, with a named owner — exactly the ownership discipline of
  chapter 15, applied to metric definitions.

Plus one serving bonus: **pre-aggregation acceleration** (Cube-style). The
semantic layer can automatically build and maintain aggregate tables for hot
queries and rewrite incoming dashboard queries to hit those aggregates —
the mart pattern from above, automated instead of hand-built.

### Workload management — the fairness layer

A warehouse has finite concurrency — Snowflake models it as slots and
queues; BigQuery as slots. When demand exceeds capacity, *someone* waits,
and workload management decides who. The serving-layer duties:

- **Separate queues per consumer class** — BI vs batch vs ad-hoc, via
  separate compute resources (chapter 13) or query queueing. A nightly
  backfill must not be able to starve the exec dashboard, because they don't
  share lanes.
- **Rate-limit noisy consumers** — the analyst's `SELECT *` on the fact
  table at 9am. Tools: resource monitors (spend caps), per-query timeouts,
  maximum-scan-bytes limits — so one query cannot consume unbounded
  resources.
- **Prioritize within SLAs** — if the exec dashboard carries a freshness
  SLO (a service-level *objective*, the internal target; chapter 2), its
  queries must outrank curiosity queries when the queue fills. Implement
  that via queue priority — an actual mechanism — not via asking people to
  be polite.

### Where a query should run — the decision table

| If the consumer needs... | Run it in... |
|---|---|
| Ad-hoc SQL, joins, exploration | warehouse / lakehouse engine |
| Sub-second fixed dashboards | marts / semantic-layer pre-ags / result cache |
| Millisecond app reads | KV projection (CDC-fed) |
| Full-text search | search projection |
| ML training scans | lakehouse (batch), not the BI warehouse |

The reasoning behind each row:

- **Ad-hoc SQL, joins, exploration → the warehouse itself.** Unknown
  questions need the full engine; this is the warehouse-direct default, and
  the consumer (an analyst) tolerates seconds.
- **Sub-second fixed dashboards → marts / pre-aggregates / result cache.**
  Known, repeated, latency-sensitive queries should not re-scan raw data —
  answer them from something precomputed.
- **Millisecond app reads → KV projection.** Application code that must
  respond in milliseconds cannot wait on a warehouse queue at all — it reads
  a CDC-fed key-value copy (chapters 14/15).
- **Full-text search → search projection.** Ranked text lookup is the
  inverted index's job (chapter 14).
- **ML training scans → the lakehouse, batch.** Training reads enormous
  volumes, is batch-tolerant, and would clog the BI warehouse's queues and
  bill — point it at the lakehouse storage plane (chapter 12) instead.

### The fifth pattern: real-time OLAP engines

Between warehouse-direct and KV serving sits a family built for exactly the
"fresh dashboards at scale" case: **real-time OLAP** — ClickHouse, Apache
Druid, Apache Pinot (and StarRocks/Doris in the same class). OLAP means
"online analytical processing" — aggregations and group-bys rather than
single-record lookups.

| | Warehouse (Snowflake/BQ) | Real-time OLAP (ClickHouse/Pinot/Druid) | KV (Redis/DynamoDB) |
|---|---|---|---|
| Freshness | minutes-hours (batch loads) | **seconds** (streaming ingest) | milliseconds (CDC-fed) |
| Query shape | ad-hoc SQL, joins, BI | **aggregation scans** over recent data, filters, group-bys | point lookups by key |
| Latency | seconds-minutes | **sub-second, high concurrency** | single-digit ms |
| Joins/ad-hoc | full SQL | limited/denormalized-first (star-schema pre-joined) | none |
| Sweet spot | BI + analysts | user-facing analytics, fresh operational dashboards | feature/profile reads |

Reading the table: a warehouse is endlessly flexible but loads data in
batches and answers in seconds-to-minutes; a KV store is instant but can
only answer "the record for this key"; a real-time OLAP engine is the middle
engineered deliberately — it ingests *from streams* (so data is queryable
seconds after the event happens) and answers *aggregation queries* (counts,
sums, group-bys over filters) in sub-second, under high concurrency.

The pattern in practice: your streaming pipeline (Kafka → Flink or
micro-batch — chapters 8/9) **also** writes into the OLAP engine, and
dashboards query it directly. This is the standard answer to "exec wants
live dashboards" **when warehouse-direct is too stale** (batch loads lag by
minutes-to-hours) and hand-built marts are too heavy to maintain — you get
fresh *and* sub-second *and* concurrent, **without building a separate KV
projection per dashboard.**

The costs to narrate honestly:

- **Another operational system.** The chapter 22 ops bill grows by one:
  upgrades, capacity, monitoring, on-call.
- **Denormalization-first modeling.** Joins at query time are weak in these
  engines, so you *pre-join at ingest*: the pipeline writes wide,
  already-joined rows (the star schema flattened into one table). Your join
  logic moves into the pipeline — which is chapter 15's sync-by-pipeline
  discipline again.
- **Per-engine sharding and compaction care.** These are distributed engines
  with their own partitioning and background-merge mechanics — the same
  class of care Cassandra's compaction demands (chapter 14).

The placement rule, made concrete: **real-time OLAP when the *consumer* is a
dashboard or user-facing UI needing fresh aggregates** (e.g., an ops console
showing orders-per-minute as they happen); **KV when the consumer is
application code needing records by key** (e.g., checkout fetching a user's
cart). Freshness is not the only axis — *what shape of query* the consumer
issues decides the engine.

## The Options

| Serving design | Cost profile | Ops profile | Risk |
|---|---|---|---|
| All warehouse-direct | per-scan | lowest | morning queue storms; bill shocks |
| Marts everywhere | storage + refresh | moderate | stale marts; modeling sprawl |
| KV for everything | projection infra | highest | KV as poor man's warehouse creep |
| Layered (warehouse + marts + KV by consumer class) | balanced | moderate | requires the decision table above, enforced |

Each design, in plain words:

- **All warehouse-direct.** Everything queries the warehouse raw; nothing
  else to build or operate (lowest ops). The risks: every dashboard refresh
  is a full scan (per-scan costs scale with usage), so 8am refresh storms
  jam the queues, and the bill shocks at month-end.
- **Marts everywhere.** Pre-aggregate everything dashboards touch. Costs
  shift to storage plus refresh pipelines; risks: marts go stale when a
  refresh fails silently, and "modeling sprawl" — hundreds of summary tables
  nobody remembers the purpose of.
- **KV for everything.** Serve all reads, even analytics, from key-value
  projections. Highest ops burden (projection infrastructure everywhere),
  and the risk is KV creep: the store gets pressed into answering
  aggregation queries its key-shaped layout was never built for (chapter
  14's query-first contract, violated at serving time).
- **Layered.** The deliberate design this chapter recommends: warehouse for
  ad-hoc, marts for hot dashboards, KV for app reads — chosen per consumer
  class using the decision table. Balanced cost and ops. Its only risk is
  that it *requires* the decision table to actually be enforced — the
  moment "just query the warehouse" becomes the shortcut for every new
  consumer, the layering silently erodes.

## Decision Rules

- **Classify consumers before sizing the pipeline.** Internal analysts,
  customer-facing features, and exec dashboards have different latency,
  concurrency, and freshness needs — each class maps to a different serving
  pattern, and knowing the classes first tells you what to build.
- **Warehouse-direct by default; add marts when the physics say no.** The
  physics are a product: scan cost × refresh frequency × concurrency. When
  that product gets expensive (the 2,880-queries-per-hour example), a mart
  pays for itself; until then, skip the extra machinery.
- **Millisecond product reads go to a KV projection, never the warehouse.**
  A user-facing app cannot depend on a warehouse queue; this boundary keeps
  the customer experience out of the analytics blast radius.
- **Put the compute resource in the connection string.** Separate BI from
  batch from data science at connection time — the cheapest workload
  management you will ever implement, and it makes cost attribution
  automatic.
- **Pool connections, cache results, set timeouts above p99 runtime.** The
  three unglamorous settings that prevent most "the dashboard is down"
  incidents — and the timeout rule is what breaks the retry feedback loop.
- **Humans via SSO, services via short-lived credentials; no shared
accounts.** Revocation, chargeback, and audit all depend on knowing who
  connected.
- **One semantic layer for contested metrics.** "Revenue" defined once, in
  code, reviewed — or you will reconcile two disagreeing dashboards forever
  (the chapter 15 meeting, recurring).

## Failure Modes

- **The morning queue storm.** 40 dashboards' scheduled refreshes fire at
  8am against one warehouse; slots exhaust; queries time out; BI tools
  retry, doubling the load; the warehouse is "slow" every single morning and
  perfectly fine at noon. The daily rhythm of the failure is the tell that
  it's concurrency, not the data. Fix: connection pooling, result caching,
  separate queues per consumer class, and staggered refresh schedules.
- **Timeout-retry feedback loop.** p99 query runtime creeps up (data grew)
  until it passes the BI tool's timeout; every timed-out query is retried
  from scratch, re-scanning everything; the retries add load, which slows
  everything further, which times out more queries — the system fails under
  the weight of its own retries. Fix: timeouts above p99, plus the queue
  separation that keeps p99 from creeping in the first place.
- **The shared `analyst@company.com` connection.** One login used by the
  whole team: chargeback is impossible (whose queries cost $8k?), audit is
  impossible (who read the salary table?), and one leaked password loses
  attribution and access control entirely. Fix: SSO per human, service
  accounts for machines.
- **KV creep.** Someone points a product at the KV store for "one quick
  aggregation" over the projection; it works; it becomes load-bearing; soon
  the serving store is running aggregation queries its key-shaped layout was
  never designed for, and latency for the real app reads degrades. This is
  chapter 14's query-first contract violated at serving time. Fix: route the
  aggregation need to the right engine (warehouse or real-time OLAP) and
  keep the KV projection to its named access pattern.
- **Un-owned marts.** 300 summary tables built "for performance" over the
  years, none documented, half of them wrong or stale — and analysts don't
  know which they can trust. The warehouse equivalent of chapter 15's
  projection-without-an-owner. Fix: every mart gets an owner, a documented
  derivation, and a refresh-freshness check — or it gets dropped.
- **Excel via ODBC against the prod warehouse.** One analyst's pivot table
  full-scans the fact table on every refresh, unthrottled, from a desktop
  tool with no queue discipline — and the cost page shows a single query
  bigger than the rest of the week combined. Fix: rate limits and
  max-scan-bytes for ad-hoc consumers, and a separate compute resource so
  the blast radius is contained.

## Interview Narration

"I treat serving as part of the architecture, not the end of the diagram.
The first question is who consumes: analysts and BI tools map to
warehouse-direct — that's the modern default — with pre-aggregated marts
only when the physics demand it: scan cost times refresh frequency times
concurrency. Product-facing features needing single-digit milliseconds map
to a KV projection fed by CDC, and search UX to a search index. Same truth,
four projections, each with an owner (ch15).

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
queues, not politeness. And when 'fresh' means seconds rather than minutes,
I'd reach for a real-time OLAP engine fed by the stream, with the honesty
that it's another system to operate and a denormalization-first model."

*(Every claim in this narration is justified by the sections above — if any
sentence feels like shorthand you couldn't defend, re-read the matching
section.)*

---

| <- Previous | Next -> |
|---|---|
| [Chapter 15 — Polyglot Persistence](ch15-polyglot-persistence.md) | [Chapter 17 — Row vs Columnar](ch17-row-vs-columnar.md) |
