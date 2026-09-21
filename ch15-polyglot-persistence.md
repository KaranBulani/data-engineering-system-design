# Chapter 15 — Polyglot Persistence

> Part V — Storage Engines: SQL vs NoSQL

Real systems don't run one database — they run three to six, each owning the
workload it's best at. That's polyglot persistence, and it's not an
anti-pattern; the anti-patterns are the *failure shapes* it drifts into. The
senior architecture skill is drawing boundaries between stores and keeping the
copies in sync.

## The Question

*"Which stores does my system actually need, who owns which copy of the data,
and how do I keep them consistent without building a distributed monolith?"*

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

Almost every real platform uses 4+ of these rows. The architecture isn't "one
store to rule them all" — it's *a clear reason for each store*, and a
synchronization story between them.

### The sync problem: copies must be maintained

The moment two stores hold the same data, you own a replication problem:

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

The **CDC/event fan-out** pattern (right side of the diagram) is the senior
default: source-of-truth store emits changes; every other store is a
*derived projection*, rebuilt on demand. This gives:

- **Eventual consistency between stores** — measured in seconds, and it must
  be a *stated, business-accepted* property ("read-your-own-write to the KV
  within 5s of commit" is a design SLA, not an accident).
- **Rebuildability** — every derived store can be reconstructed from the
  source of truth + transformations. A corrupted Elasticsearch index is a
  rebuild, not a disaster (the Kappa idea, ch11, applied to stores).

### Who owns which copy

Governance rule that prevents most fights: **each data item has exactly one
system of record; every other copy is a cache/projection with a documented
derivation and a documented owner of the pipeline that maintains it.** The
"owner" part is what fails in practice — unowned projections drift silently
until a reconciliation job disagrees.

### The three failure shapes

1. **The distributed monolith**: every service reads every store. Symptom:
   changing one schema breaks three teams' consumers; nobody can deploy
   independently. Stores were supposed to decouple teams, and instead became
   shared internals.
2. **One-store-for-everything**: using the wide-column store for analytics
   because "we already have Cassandra." Every store dragged outside its
   workload row in the table above — performance, cost, and operability all
   degrade together.
3. **Store sprawl**: every team adds the database of the month; the platform
   runs 14 stores with 6 operators. The realistic failure at growing
   companies, and why platform teams publish a *paved-road store menu* — a
   small approved set with real support, and a process for adding one that
   involves the operators' consent.

### The cost ledger

Polyglossia is not free, and the costs are re-paid monthly: per-store
operational surface (upgrades, backups, security patching, on-call), per-copy
schemas to evolve in sync (ch18 discipline × N), N failure modes to monitor,
cross-store reconciliation as a permanent duty (does the KV copy match the
warehouse? — someone must own that check), and the cognitive load of "which
store is truth for which question" — a question every new engineer asks, and
which only clear ownership answers.

The discipline that makes it worth it: **add a store only when a workload
needs its row in the table** — not because a team fancies it. Every addition
should come with the exit story: if we're wrong, the data lives in the source
of truth; we delete the projection and move on.

### An CQRS-flavored view of the pattern

The sync diagram above is, structurally, **CQRS** (Command Query
Responsibility Segregation) applied to data platforms: the write side (OLTP,
event logs) is optimized for transactions; each read side (warehouse, KV,
search) is a projection optimized for its queries; the CDC/event fan-out is
the *sync* mechanism. Naming it gives you the vocabulary for the two
questions every polyglot design must answer, borrowed from CQRS:

1. **Read-model staleness** (CQRS: eventual consistency): each projection's
   lag is an SLA (ch15 rules) — measure it (ch22).
2. **Replayability of read models** (CQRS: rebuildable projections): every
   serving store must be re-derivable from truth + transformation (the ch11
   idea, generalized to stores).

### Data mesh — polyglot at org scale

Polyglot *persistence* is the system view; **data mesh** is the org view:
data-as-a-product per domain, each product with an owner, a contract
(schema + freshness SLA + semantics — ch20/24), and a federated
computational-governance layer (the catalog as the shared plane). The
polyglot rules of this chapter scale into it unchanged: one system of
record, derived projections, sync via events/CDC. What mesh adds is
*ownership boundaries* — "which team's product is which projection" — which
is ch15's "every projection has an owner" at federated scale. Mention mesh
when the interviewer asks about org design; anchor it back to these
mechanics and it stops being a buzzword.
## The Options

| Strategy | When it fits | Failure mode |
|---|---|---|
| Single store (SQL) | small scale, one workload class | pushed past its row; "we'll add Redis later" pain |
| Single vendor suite | org gravity; managed integration | suite lock-in; suite's weak rows hurt |
| Deliberate polyglot + CDC fan-out | platform scale, distinct workloads | sprawl; sync debt; ownership blur |

## Decision Rules

- **Map each workload to its store row first**; a store with no workload row
  is a hobby, not architecture.
- **One system of record per data item**; everything else is a documented,
  rebuildable projection.
- **CDC/event fan-out as the default sync mechanism** (ch06) — never
  application dual-writes (the anti-pattern of ch06, now at platform scale).
- **State cross-store consistency as a business SLA**, not an engineering
  hope — "KV within 5s, search within 60s, warehouse within 15min" — each
  number is a promise someone can be paged against.
- **Keep the paved-road menu small** (3-5 stores for most orgs) and make
  adding one a platform decision with operator consent.
- **Every projection has an owner and a rebuild runbook.** No owner, no
  projection.
- **Design the exit**: every derived store must be deletable without data
  loss — data lives in the source of truth.

## Failure Modes

- **Dual-write divergence** (the platform-scale version): app writes OLTP and
  publishes an event "for other consumers"; one succeeds, one fails; KV copy
  and warehouse silently disagree about reality. CDC from the source of truth
  is the cure; dual-writes are the disease.
- **Reconciliation debt**: nobody ever compares copies; drift accumulates for
  quarters; the first reconciliation job finds 3% divergence and nobody knows
  which store is right. Symptom: two dashboards, two different revenue
  numbers, both defensible.
- **The shared-store tangle**: three teams writing one Cassandra cluster with
  different consistency expectations; one team's compaction storm is
  everyone's outage.
- **Sprawl-induced blindness**: 14 stores, each with a niche justification,
  collectively unoperable — the platform team quits and the company runs on
  luck.
- **The projection without an owner**: built by a contractor, maintained by
  inertia, wrong for six months before anyone noticed (ch21's quality gates
  are the detection layer).

## Interview Narration

"I design storage polyglot-ly on purpose: each store owns the workload it's
built for — OLTP for transactions, warehouse or lakehouse for analytics, KV
for sub-millisecond serving, search for full-text, object storage plus a table
format for the raw and derived history. The two rules that keep it from
becoming a mess: one system of record per data item, and everything else is a
derived projection maintained by CDC or events — never application
dual-writes, because the moment two writes can disagree, they eventually will,
silently.

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

---

| <- Previous | Next -> |
|---|---|
| [Chapter 14 — The NoSQL Families](ch14-the-nosql-families.md) | [Chapter 16 — The Serving Layer & Consumption](ch16-the-serving-layer-and-consumption.md) |
