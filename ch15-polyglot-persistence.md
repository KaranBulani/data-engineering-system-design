# Chapter 15 — Polyglot Persistence

> Part V — Storage Engines: SQL vs NoSQL

"Polyglot persistence" means using *several different databases in one
system* — the word "polyglot" means "many languages," and the idea is that
each store is the right "language" for one kind of workload. Real systems
don't run one database; they run three to six, each owning the workload it is
best at.

A typical e-commerce platform makes this concrete. One system might run:

- **Postgres** (relational SQL) for orders and payments — transactions,
  integrity, joins.
- **Redis or DynamoDB** (key-value) for sessions and precomputed product
  features — sub-millisecond reads.
- **Elasticsearch** (search) for product search — full-text with relevance
  ranking.
- **Snowflake or a lakehouse** for analytics — columnar scans, BI queries.
- **S3 plus a table format like Iceberg** for raw event history — the
  cheapest durable bytes, replayable for years.

Each store is excellent at its own row of workloads and bad at the others.
That is polyglot persistence, and it is not an anti-pattern — a deliberate,
bounded mix of stores is simply how platforms at scale work. The
anti-patterns are the *failure shapes* it drifts into when done carelessly:
every service touching every store, one store stretched over workloads it was
never built for, or a new database added every quarter. Those are covered
below. The senior architecture skill is drawing clean boundaries between
stores — deciding who owns which copy of the data — and keeping the copies
in sync.

## The Question

*"Which stores does my system actually need, who owns which copy of the
data, and how do I keep them consistent with each other — without building a
distributed monolith where nothing can change independently?"*

## The Physics

### Workload-to-store mapping — the core table

| Workload | Right store | Why |
|---|---|---|
| Operational transactions (orders, payments) | OLTP SQL (ch13) | ACID, row layout, small writes |
| Analytics / BI / ad-hoc joins | Warehouse or lakehouse (ch12-13) | columnar scans, SQL ecosystem |
| Product-facing reads by id (sessions, profiles, features) | KV (Redis/DynamoDB) (ch14) | sub-ms one-hop reads |
| Full-text search UX | Search (Elasticsearch) (ch14) | inverted index, relevance |
| High-volume event retention / replay | Object storage raw plane (ch03/06) + table format (ch12) | cheapest durable bytes + ACID table semantics |
| Stream state / in-flight processing | Stream processor state (ch08) | keyed, checkpointed |
| Slow-moving reference data serving | Wide-column or KV | partition-key lookups, write-throughput tolerant |

The "Why" column, in plain words, row by row:

- **Operational transactions → OLTP SQL.** "Take payment, decrement stock,
  create order" must succeed or fail as one unit, with no partial results.
  That is exactly ACID transactions (chapter 13), and the row-oriented layout
  is built for reading and writing one complete record at a time.
- **Analytics → warehouse or lakehouse.** Analysts scan millions of rows but
  only a few columns ("revenue by month"). Columnar storage reads just those
  columns, and the SQL ecosystem (BI tools, notebooks) plugs straight in.
- **Reads by id → key-value.** "Get the session for this browser," "get the
  profile for this user id" — every access names one key, so the one-hop
  key → hash → node → value path (ch14) answers in under a millisecond.
- **Search UX → search engine.** Typing words into a box and getting ranked,
  relevant results is only fast over an inverted index; nothing else does
  this well.
- **Event retention → object storage + table format.** Years of clickstream
  and event logs are enormous. Object storage (S3-style) is the cheapest
  durable place to put them, and a table format (Iceberg and friends,
  chapter 12) adds schema, ACID updates, and SQL queryability on top — so the
  archive is both cheap *and* usable.
- **Stream processing state → the stream processor's own state store.**
  A job computing "purchases per user in the last hour" keeps a running
  total per user and must survive crashes. The inputs arrive as events:
  the checkout service appends one "purchase" event to a Kafka topic per
  completed order, and the processor consumes that topic partitioned by
  `user_id`, so every event for a given user lands on the same task.
  Checkpointing (chapter 8) saves the keyed state together with the
  topic offsets it was computed from — after a crash, both come back as
  one unit and the totals resume where they stopped. That coupling is
  also why the totals do not live in Redis, even though they look like a
  key-value workload: only the processor's own state store saves state
  and input position as a single unit.
- **Slow-moving reference data → wide-column or KV.** Product metadata,
  region tables, config — read heavily, changed rarely. A store built around
  partition-key lookups serves them cheaply, and their low write rate means
  the sync pipeline is easy.

Almost every real platform ends up running four or more of these rows. So
the architecture is not "one store to rule them all" — it is *a clear,
written reason for each store* (which workload row it serves), plus a
synchronization story for the data that appears in more than one of them.

### The sync problem: copies must be maintained

The moment two stores hold the same data — say, product prices in Postgres
*and* in the search index — you own a replication problem. Every copy has to
be kept up to date, forever, by machinery you design:

```
                    +-> OLTP (source of truth: orders)
                    |        |
                    |        +-- CDC (ch06) -->  warehouse/lakehouse (analytics)
   app writes       |                         |
                    |                         +-- transform --> KV serving (features)
                    |                         |
                    +-- event --> search index (search UX)
                                              |
                                              +-- stream processor state (realtime)
```

Reading the diagram: the application writes *only* to the OLTP store — the
**source of truth**. Every other store learns about the change from events,
not from the application:

- **CDC** (Change Data Capture, chapter 6) is a tool that watches the source
  database's own transaction log and turns each committed change into an
  event: "order 1001 was updated to status=shipped."
- Those events fan out to every consumer: the warehouse, a transform job that
  maintains the KV serving copy, the search indexer, the stream processor.

This is the **CDC/event fan-out** pattern — the right side of the diagram —
and it is the senior default. Every store except the source of truth is a
**derived projection**: a copy computed *from* the truth by a pipeline, which
means it can be deleted and rebuilt at any time. Two properties follow, and
both must be understood as deliberate design, not luck:

- **Eventual consistency between stores.** The copies lag the truth by some
  amount — measured in seconds, not years, but lag they do. Concretely: a
  user changes their email, Postgres commits instantly, and the KV copy
  serves the new email a second or two later. The point is that this lag
  must be a *stated, business-accepted* property — "the KV copy reflects a
  commit within 5 seconds" is a design SLA (a service-level agreement: a
  measurable promise someone can be held to and paged against), not an
  accident nobody noticed. If the product needs "user sees their own change
  immediately," the design answers that differently (read-your-own-writes
  from the source of truth for that flow).
- **Rebuildability.** Because every derived store is just truth +
  transformation, any of them can be reconstructed from scratch: re-run the
  pipeline over the source of truth. A corrupted Elasticsearch index goes
  from *disaster* to *rebuild job* — drop the index, replay the events,
  done. This is the Kappa idea (chapter 11, "the log is the truth, everything
  else is a replay of it") applied to storage.

One arrow in the diagram deserves a closer look: the one into the warehouse.
When the source feeding it is itself a NoSQL family — clickstream collected
in Cassandra, for example, has no OLTP system behind it — the arrow is not a
plain copy. Each family exports differently (batch scan, snapshot, change
stream, event fan-out), and the data must be flattened and cast into fixed
warehouse tables before SQL can use it, because schema-on-read meets
schema-on-write at that boundary. Chapter 14's "From NoSQL to the Warehouse"
section walks through the mechanics family by family.

### Who owns which copy

One governance rule prevents most of the fights between teams:

**Each data item has exactly one system of record. Every other copy is a
cache or projection with a documented derivation (which pipeline computes it,
from what) and a documented owner (which team is paged when it's wrong).**

A "system of record" (or "source of truth") is the store whose value you
trust when copies disagree — for orders, the OLTP database; every Elasticsearch
or Redis copy of an order is *information about* the order, not the order
itself.

The "owner" part is what fails in practice. An owned projection has a team
that gets its alerts, a runbook for its rebuild, and someone who notices when
its pipeline stalls. An *unowned* projection is one nobody was assigned:
its sync job breaks quietly, the copy drifts out of date silently, and the
drift is only discovered months later — when a reconciliation job (a job
that compares copies and reports mismatches) finally disagrees with it, or
worse, when a customer notices first.

### The three failure shapes

Polyglot persistence fails in three recognizable ways. Learning to name them
is most of the defense.

1. **The distributed monolith.** Every service is allowed to read every
   store. The symptom: changing one table's schema breaks consumers in three
   teams, so every schema change needs a cross-team negotiation, and nobody
   can deploy independently. The stores were supposed to be *boundaries*
   that decouple teams — each service owns its store, others consume via
   events — and instead the stores became shared internals that couple every
   team to every other team. You have one monolithic system, just spread
   across network hops and different technologies (hence the name).
2. **One-store-for-everything.** Someone asks "can't we run analytics on the
   Cassandra cluster? We already have it." Every workload dragged outside
   its row in the mapping table degrades: analytics scatter-gathers hammer
   the serving path (ch14), the columnar-BI tools don't plug in, and the
   store is simultaneously too slow at the new job and less reliable at its
   old one. Performance, cost, and operability all degrade *together*,
   because the store is being forced to be something it isn't. The escape is
   not more tuning of the wrong store — it is moving the data to where the
   question belongs, using the per-family transfer patterns of chapter 14.
3. **Store sprawl.** Every team adds the database of the month; the platform
   ends up running 14 stores with 6 operators who can't possibly operate
   them all well. This is the realistic failure at growing companies, and
   the standard defense is a **paved-road store menu**: a small, approved
   set of stores (3–5) that the platform team supports for real — upgrades,
   backups, security patching, on-call — plus a process for adding a new
   one that requires the operators' consent. A store outside the menu isn't
   forbidden so much as *unsupported*: your team owns it alone, including
   the 3am pages.

### The cost ledger

Polyglot persistence is not free. Each cost below is real and re-paid every
month — knowing them is what separates deliberate polyglot from accidental
sprawl:

- **Per-store operational surface.** Every store needs upgrades, backups,
  security patching, capacity planning, and on-call rotation. Five stores
  means five of each — the operational work multiplies, it doesn't add.
- **Per-copy schemas to evolve in sync.** When the source-of-truth schema
  changes, every projection's schema must evolve too, in the right order,
  without breaking its readers. The schema-evolution discipline of chapter
  18, times N copies.
- **N failure modes to monitor.** Each store brings its own ways to break —
  compaction lag in Cassandra (ch14), heap pressure in Elasticsearch,
  replication lag in the CDC pipeline — and each needs its own alerts.
- **Cross-store reconciliation as a permanent duty.** Does the KV copy match
  the warehouse? Does the search index match the OLTP truth? Someone must
  own a job that compares copies and alerts on drift — forever, not just at
  launch.
- **Cognitive load.** "Which store is truth for which question?" is the
  first question every new engineer asks, and only clear written ownership
  answers it. Six well-documented stores beat three undocumented ones.

The discipline that makes the ledger worth paying: **add a store only when a
workload actually needs its row in the mapping table** — not because a team
fancies the technology. And every addition should come with an exit story:
"if we're wrong, the data lives in the source of truth; we delete the
projection and the pipeline, and move on." A store that *is* the only copy
of its data has no exit — that's why derived projections matter.

### A CQRS-flavored view of the pattern

The sync diagram above has a name in the wider software world: **CQRS**
(Command Query Responsibility Segregation). The idea, in plain terms: stop
trying to serve writes and reads from one model. Keep a *write side*
optimized for transactions, and build separate *read models*, each one
optimized for a particular kind of query.

Applied to a data platform, that's exactly the polyglot architecture:

- The write side is the OLTP store and the event log — optimized for
  correct, fast transactions.
- Each read side — the warehouse, the KV serving copy, the search index —
  is a projection optimized for its queries.
- The CDC/event fan-out is the *sync* mechanism between the sides.

Naming the pattern gives you the vocabulary for the two questions every
polyglot design must answer — both borrowed from CQRS practice:

1. **Read-model staleness** (CQRS calls this eventual consistency): every
   projection lags the truth, so every projection needs an explicit lag
   target — an SLA (chapter 15 rules) — and the lag must be *measured*, not
   assumed (chapter 22 shows the operational side).
2. **Replayability of read models** (CQRS calls this rebuildable
   projections): every serving store must be re-derivable from the source of
   truth plus its transformation. **If a store can't be rebuilt, it isn't a
   projection — it's a second source of truth, and that's a design error.**

### Data mesh — polyglot at org scale

Polyglot *persistence* is the system-level view (which stores, which
copies, which pipelines). **Data mesh** is the same idea at the
*organizational* scale — about which *teams* own which data. Its core
commitments, in plain language:

- **Data as a product per domain.** Each business domain (orders, catalog,
  logistics) owns and serves its own datasets like products, with users and
  support — not as a by-product dumped into a central lake.
- **A contract per product.** Each dataset promises a schema, a freshness
  SLA ("updated hourly"), and agreed semantics ("revenue means …") — the
  chapters 20/24 governance material made concrete.
- **Federated computational governance.** Standards (quality checks,
  cataloging, access control) are defined once and enforced by shared
  platform tooling, with the catalog as the shared plane where all products
  are discoverable — **not by a central bottleneck team approving every
change.**

The polyglot rules of this chapter scale into a mesh unchanged: still one
system of record, still derived projections, still sync via events/CDC. What
mesh *adds* is ownership boundaries at federated scale — "which team's
product is which projection" — which is precisely this chapter's "every
projection has an owner" rule, applied across many autonomous teams. Mention
mesh when the interviewer asks about org design; anchor it back to these
mechanics and it stops being a buzzword.

## The Options

| Strategy | When it fits | Failure mode |
|---|---|---|
| Single store (SQL) | small scale, one workload class | pushed past its row; "we'll add Redis later" pain |
| Single vendor suite | org gravity; managed integration | suite lock-in; suite's weak rows hurt |
| Deliberate polyglot + CDC fan-out | platform scale, distinct workloads | sprawl; sync debt; ownership blur |

Each strategy in plain words:

- **Single store.** One Postgres does everything: transactions, and (with
  enough care) caching, search, analytics. The right choice at small scale —
  one thing to operate, no sync problem at all. The failure mode is
  workload growth: the moment the same store is asked to serve millisecond
  lookups *and* heavy analytics *and* full-text search, it is being pushed
  past its row in the mapping table, and each new job makes it worse at all
  the others. "We'll add Redis later" becomes painful precisely when traffic
  is highest — which is the worst time to be introducing a second store
  under pressure.
- **Single vendor suite.** Buy everything from one cloud vendor — their
  database, their warehouse, their search — with managed integrations
  between them. The gravity is real: one bill, one support line, components
  that already connect. The failure mode is lock-in: pricing and data
  formats bind you to the vendor, and where the suite has a weak component
  (their search, say), you use it anyway because it's in the suite — and
  the weak row hurts every day.
- **Deliberate polyglot with CDC fan-out.** The full pattern from this
  chapter: the right store per workload, one system of record, derived
  projections kept in sync by events. The right choice at platform scale.
  Its failure modes are the ones this chapter is about: sprawl (too many
  stores), sync debt (copies drifting without anyone noticing), and
  ownership blur (nobody owns the projections).

## Decision Rules

- **Map each workload to its store row first.** Write the mapping table for
  your system, workload by workload, before choosing any product. A store
  that fills no row in the table is a hobby, not architecture — it has
  costs (see the ledger) and no job.
  - "a store — any system that keeps data and serves it: a database, a key-value cache, a search engine, object storage…"
- **One system of record per data item.** **For every piece of data, exactly
one store is the truth; everything else is a documented, rebuildable
projection.** When copies disagree, the system of record wins — by rule,
  not by debate.
- **CDC/event fan-out as the default sync mechanism** (ch06). Never
  application dual-writes — the app writing two stores itself — which is
  the anti-pattern of chapter 6 now repeated at platform scale (the failure
  mode section shows why).
- **State cross-store consistency as a business SLA**, not an engineering
  hope. Concretely: "the KV copy reflects a commit within 5 seconds, search
  within 60 seconds, the warehouse within 15 minutes." Each number is a
  promise someone can be paged against, and each is a *business* decision
  about how stale each view may be — made with the product owner, not by
  default.
- **Keep the paved-road menu small** — 3–5 stores for most orgs — and make
  adding one a platform decision with operator consent. A new store must
  bring a workload row *and* someone willing to operate it, or it doesn't
  get added.
- **Every projection has an owner and a rebuild runbook.** No owner, no
  projection — an unowned copy is a future silent outage (see failure
  modes).
- **Design the exit.** Every derived store must be deletable without data
  loss, because its data lives in the source of truth and can be rebuilt.
  If you can't describe how to delete and rebuild a store, it isn't a
  projection — it's a second truth, and that needs fixing before launch.

## Failure Modes

- **Dual-write divergence** (the platform-scale version). The application
  writes to the OLTP database *and* publishes an event "for other
  consumers" — two separate network calls made by app code. One succeeds,
  one fails (a crash between them, a network blip), and now the KV copy and
  the warehouse silently disagree about reality — and nothing in the app
  knows. This is why chapter 6 calls dual-writes an anti-pattern, and the
  cure is the same at platform scale: let CDC read changes from the source
  of truth's own transaction log, where a committed change *cannot* be
  missed, and let every other store consume from there. Dual-writes are the
  disease; CDC is the cure.
- **Reconciliation debt.** Nobody ever compares the copies, so drift
  accumulates for quarters. The first reconciliation job finds 3% of rows
  disagree — and nobody knows which store is right, because there is no
  owner and no derivation documented. Symptom, and the classic one: two
  dashboards show two different revenue numbers, both technically
  defensible, and a meeting is called. Prevention is cheap by comparison:
  an owned reconciliation job from day one.
- **The shared-store tangle.** Three teams write one Cassandra cluster with
  different consistency expectations — one wants fast `ONE` writes, one
  wants quorum reads, one runs heavy jobs. There is no per-team isolation,
  so one team's compaction storm (ch14) is everyone's outage. Stores are
  boundaries; a store shared write-write by teams with conflicting needs
  erases the boundary and couples their fates.
- **Sprawl-induced blindness.** Fourteen stores, each added with a niche
  justification, collectively unoperable: no team can hold the operational
  picture — upgrades, backups, failure modes — of fourteen systems. The
  platform team quits (or is dissolved), and the company runs on luck.
  The paved-road menu exists to make this state unreachable.
- **The projection without an owner.** Built by a contractor, maintained by
  inertia, quietly wrong for six months before anyone noticed. Nobody got
  its alerts because nobody was assigned. The detection layer for this is
  the quality-gate machinery of chapter 21 (freshness checks, reconciliation
  checks, alert routing) — but detection only works once an *owner* exists
  to receive the alert. Hence the rule: no owner, no projection.

## Interview Narration

"I design storage polyglot-ly on purpose: each store owns the workload it's
built for — OLTP for transactions, warehouse or lakehouse for analytics, KV
for sub-millisecond serving, search for full-text, object storage plus a
table format for the raw and derived history. The two rules that keep it
from becoming a mess: one system of record per data item, and everything
else is a derived projection maintained by CDC or events — never application
dual-writes, because the moment two writes can disagree, they eventually
will, silently.

That makes every non-truth store rebuildable, which turns 'the search index
is corrupted' from an incident into a re-run. I'd also state the consistency
story as explicit SLAs — serving KV within seconds of commit, warehouse
within the hour — because 'eventually consistent between stores' is a
business decision, not an engineering shrug.

And I'd name the failure shapes so the interviewer knows I've operated this:
the distributed monolith where every service reads every store; one store
stretched across workloads it was never built for; and sprawl, which is why
I'd keep the paved-road menu small — four or five stores with real owners,
real runbooks, and a rule that every projection has a rebuild path. Adding a
store is a platform decision with an exit story, not a team preference."

*(Every claim in this narration is justified by the sections above — if any
sentence feels like shorthand you couldn't defend, re-read the matching
section.)*

---

| <- Previous | Next -> |
|---|---|
| [Chapter 14 — The NoSQL Families](ch14-the-nosql-families.md) | [Chapter 16 — The Serving Layer & Consumption](ch16-the-serving-layer-and-consumption.md) |
