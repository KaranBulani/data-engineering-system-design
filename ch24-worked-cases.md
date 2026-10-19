# Chapter 24 — Worked Cases

> Part VIII — Interview Execution

Four end-to-end cases, each walked through the full decision cascade:
- requirements → decisions with trade-offs → architecture → failure modes →

what to say. Each case cites the chapters whose decisions it applies — this is the book, integrated.

The chapters before this one each isolated one decision — one paradigm, one format, one operational duty. Real prompts never isolate anything: a single 45-minute question forces you to walk the whole cascade (ch01) and make a dozen interacting decisions coherently, on a clock, out loud. That skill — assembly — is what this chapter practices. Read each case in its own order: the **Prompt** as the interviewer stated it, the **Requirements** you extract in the first five minutes (ch02), the **cascade** walked step by step with the four-beat pattern (ch23), the **architecture sketch** you draw, the **failure modes** you volunteer unprompted, and the **60-second close** you deliver. The closes are written the way you would actually say them — they are templates for your voice, not prose to memorize word for word.

## The Question

A real prompt just landed: 
- design fraud detection for payments, 
- build the analytics platform, 
- feed an ML ranking model, 
- ingest a million IoT devices.

How does the entire book assemble into one 45-minute performance?

Four genuinely different prompts, one shared answer: the same cascade executed with different emphasis. None of the four is answered by any single chapter — each needs the paradigm decision (ch07-09), the architecture shape (ch10-12), the storage and serving choices (ch13-16), and the operational layer (ch20-22), assembled in a different proportion. The cases below are deliberately picked so that the *same* cascade produces four different architectures, which is the proof that the framework is a framework and not a memorized design.

## The Physics

What makes integration harder than any single chapter: **decisions interact.** 
- The fraud case's late-arriving labels force a two-product architecture; 
- the dashboard case's cost profile defends batch against fashion; 
- the feature case's consistency requirement creates a governance layer; 
- the IoT case's volume makes the cost model itself the architecture.

The cascade (ch01) is linear to walk but non-linear in effect - exactly what these cases exist to show. In each case the chapter skeleton applies internally: the prompt is the Question, the requirements are the Physics inputs, the cascade walk is the Options and Decision Rules, failure modes are volunteered per component, and a 60-second narration closes it out.

"Decisions interact" deserves a concrete reading, because it is the difference between reciting chapters and designing. 
- In the fraud case, one requirement — *labels arrive days late* — reaches forward and reshapes the whole architecture: a system whose action is instant but whose truth is delayed cannot be one product, so the Lambda-shaped split stops being an optimization and becomes the answer. 
- In the dashboard case, the cost profile (many small queries, no urgency) reaches backward and *defends* the boring choice — batch — against the fashion of streaming. 
- In the feature case, one unstated requirement — the model must be trained on features computed exactly as serving computes them — creates an entire governance layer (the feature store) that no single-latency analysis would have produced. 
- And in the IoT case, raw volume promotes cost from a side-constraint to *the* architectural driver: tiering and downsampling are not optimizations bolted on later, they are the design. 

A linear walk of the cascade produces these interactions only if you keep asking, at each level, "what does the decision I just made do to the decisions I haven't made yet?"

## The Options

Four cases, four different stresses on the same cascade:

| Case | Cascade level it stresses most | The hard requirement |
|---|---|---|
| Fraud (payments) | streaming correctness + serving (ch08, ch16) | 500ms in-flight decision; labels arrive days later |
| Dashboards (e-commerce) | batch + lakehouse + serving cost (ch07, ch12, ch16) | finance-grade exactness at hours of latency |
| ML features | hybrid paradigm + governance (ch08-09, ch12) | training/serving consistency; point-in-time correctness |
| IoT telemetry | ingestion scale + cost tiers (ch05, ch18-19) | 100k events/sec; unbounded storage growth |

Reading the table: each row names where that case's minutes should go (deep-dive where the problem stresses, ch23) and the single requirement that, if you get it wrong, breaks the design. 
- Fraud breaks on *latency in the decision path* and *late truth*. 
- Dashboards break on *silently wrong finance numbers* and *serving cost at 40 dashboards*. 
- ML features break on *the model and the training data disagreeing*. 
- IoT breaks on *burst volume* and *an unbounded storage bill*. 

Two of the four (fraud, features) end up hybrid architectures; two (dashboards, IoT) end up mostly batch with a thin streaming slice — a distribution worth noticing, because it says the streaming-heavy answer is the exception real systems pay for, not the default.

## Case 1 — Fraud Detection at a Payments Company

**Prompt:** "Design the data system that flags potentially fraudulent card transactions."

### Requirements (ch02)

- Latency: decision *while the payment is in-flight* — the auth call waits ~200-500ms for our score. That number is the whole design.
- Correctness: a fraud decision may be reversible (hold-and-review), so effectively-once processing suffices; but the *label* data (was it really fraud?) arrives days later — batch truth, streaming action.
- Volume: 50k transactions/sec peak (payments scale), growth 2x/2yr.
- PII: cardholder data — PCI scope exists.

Each requirement, unpacked. 
- *"While the payment is in-flight"* means the card network's authorization call is literally waiting on our answer before it approves or declines — if we cannot score the transaction inside the auth window of roughly 200-500 milliseconds, we are not in the decision path at all, and the design silently collapses into post-hoc analytics. That single number rules out batch from the hot path and dictates a millisecond-class serving store. 
- *Reversibility* is the subtle correctness gift: a fraud *hold* can be reviewed and reversed by a human, so a duplicated score never double-charges anyone — at-least-once processing with idempotent writes ("effectively once", ch08) is enough, and the expensive exactly-once machinery is not required on the decision path. 
- But the *label* — was this transaction actually fraud? — is only known days later, when the chargeback lands or the analyst finishes the review. 
- Action is streaming; truth is batch; the architecture has to hold both. *50k transactions/sec peak* with 2x growth in two years sizes the queue and the processing fleet. 
- And *cardholder data* means PCI scope: encryption, access control, and tokenization from ch20 are requirements, not decorations.

### The cascade

- **Paradigm (ch07-09)**: 500ms in-flight decision = true streaming. Batch physically cannot participate in the decision path. But labels, model training, and review analytics are batch — this is the "two products" shape (ch10): a streaming decision path *and* a batch truth path.

Said with the four beats: the options are streaming, micro-batch, or batch; a batch job's fastest possible output is one run interval, and even a 1-minute micro-batch misses the 500-millisecond window, so *no batch variant is physically eligible* for the decision — the call is streaming, forced by the requirement rather than chosen by taste. What would change it: if the product were "flag suspicious transactions for an analyst review queue within an hour," batch or micro-batch wins and the whole design shrinks. That said aloud is beat 4 doing its work. But the *labels* side has no latency requirement at all — chargebacks arrive on their own schedule — so the truth path is batch, and the honest architecture is the two-products shape of ch10.

- **Architecture (ch10-12)**: honest Lambda-*shaped* answer — not because reprocessing is expensive (ch12 made it cheap) but because the fast artifact (in-flight score) and the exact artifact (labeled history, trained models) are *different products with different SLAs*. Streaming upserts into a lakehouse table unify the analytics side.

The ch10 Lambda architecture earned its bad reputation for the wrong reason: maintaining the same computation twice was painful. The lakehouse (ch12) made reprocessing cheap, so "we need Lambda because replay is expensive" is no longer an honest argument — and saying the honest one is the senior move. Here the justification is different and real: the in-flight score and the labeled training history are *different artifacts with different SLAs* (milliseconds vs. days), so they are different products that happen to share a raw event plane. The unification is the lakehouse table: streaming upserts flow into it continuously, batch labels join it, and one table serves both analytics and training.

- **Ingestion (ch03-06)**: transactions arrive as events on a queue (Kafka/Pub/Sub); the OLTP payments DB is CDC'd (ch06) so state changes (chargebacks = labels!) flow as events. Raw plane archives everything (ch03) — model retraining and audit depend on it.

Two ingestion paths, each earning its place. The transaction *events* flow from the payment service onto a queue, because that is the shape the streaming decision path consumes. But the *database state* — and specifically chargebacks, which are the fraud labels — lives in the OLTP payments database, so change-data-capture (ch06) streams every state change out as events too. Without CDC, the labels would be locked inside the operational database where the streaming and batch sides could never reach them. And everything, events and CDC, lands in the raw plane exactly as received (ch03): model retraining needs full history, and a fraud audit needs a faithful record — the raw plane is the insurance both depend on.

- **Processing (ch08)**: Flink — sub-second, event-time windows (velocity features: "5 transactions in 60s per card"), keyed state per card (RocksDB-backed), watermarks from observed lateness. Effectively-once via idempotent sink writes.

The fraud signal is mostly *velocity*: not any single transaction, but a card's behavior in a time window — "5 transactions in 60 seconds," "3 countries in 10 minutes." That is event-time windowing over a stream (ch08), which means Flink-class processing with state keyed by card: each keyed card carries its recent-transaction state (RocksDB-backed, so it survives restarts and can grow to millions of keys). Events arrive late — network retries guarantee it — so watermarks with allowed lateness, tuned from *observed* lateness rather than a guess, decide when each window closes. And because a redelivered transaction must not double-count a card's velocity, every sink write is idempotent: effectively-once by design.

- **Serving (ch16, ch14)**: features and scores to a KV store (Redis/ DynamoDB) for ms reads by the decision API; the decision itself is a model + rules call. Analysts query the lakehouse/warehouse.

The decision API has a few milliseconds to fetch features and a score, so they live in a KV store keyed by card (ch14, ch16) — single-digit- millisecond reads, populated continuously by the streaming path. The decision itself is a rules-plus-model call against those features. The *analyst* side has no such constraint: fraud analysts investigating cases query the lakehouse/warehouse, where latency tolerance is minutes and the queries are ad-hoc. Two consumer classes, two serving layers, one architecture — the ch16 lesson applied rather than restated.

- **Quality/ops (ch21)**: feature freshness SLOs; volume anomaly gates (a fraud-model silent failure is catastrophic — monitor the *score distribution*, not just jobs).

The operational danger unique to fraud: the system can fail *silently* in a way no job monitoring sees. A model that starts scoring everything 0.01 — because an upstream feature went stale, or the model drifted — still runs green, still writes outputs, and fraud flows through untouched. That is why the gates watch the *score distribution* (a sudden spike at one value is a scream) and the *feature freshness* per feature (ch21's SLO discipline), not just job success and row counts.

### Architecture sketch

```
card txn events --> Kafka --+--> Flink (velocity features, rules)
                            |        |
payments DB --CDC (ch06)--> +        +--> KV serving (scores/features) <-- decision API
                            |                       ^
                            v                       |
                     raw plane (S3) --> batch labels/chargebacks --> model training
                            |                                       |
                            +--> lakehouse table (ch12) <-----------+   (features + labels)
                                        |
                                        v
                              BI / fraud analytics (ch16)
```

Reading the sketch: the top path is the streaming decision path (events plus CDC into Flink, features and scores into the KV store, the decision API on the right). The bottom path is the batch truth path (raw plane into label joins and model training), and the lakehouse table in the middle is where the two products meet — features and labels land together for analytics and retraining.

### Failure modes to volunteer

- Consumer lag past retention (ch05) = silent missing transactions = missed fraud: alert on lag-vs-retention headroom.
- Training/serving skew (below, Case 3's disease): the batch-trained model sees features computed differently than the streaming path computes them. The feature-store discipline is the cure.
- Poison message wedges a partition: DLQ from day one (ch05).

Each with its plain reading. *Lag past retention* is the catastrophic one because it is silent: if the decision path falls behind and events expire from the queue before processing, transactions are simply never scored — no error, no failed job, just fraud that passes unflagged. The mitigation is lag-vs-retention headroom alerting (ch05, ch22), tuned to page long before the queue expires anything. *Training/serving skew* is the model-quality killer: the batch trainer computes "transactions in the last hour" one way, the streaming path computes it slightly differently (a different window boundary, a different dedup rule), and the model's performance live is worse than its performance in training — Case 3 treats this disease in full. *Poison messages* — one malformed event that crashes the processing task every time it is redelivered — wedge a whole keyed partition, so the dead-letter queue exists from day one, not after the first incident (ch05, ch22).

### The 60-second close

"Sub-second in-flight decision forces streaming; but fraud's ground truth is labels that arrive days later — so this is honestly two products: a Flink decision path with event-time velocity features over keyed state, serving ms reads from a KV store; and a batch truth path — CDC'd chargebacks and raw-archived events — that trains models and reconciles. The lakehouse table unifies the analytics side of both. The failure modes I'd name: lag-past-retention as silent loss, and training/serving feature skew — both have named mitigations."

Notice what the close does, in ch23's terms: it recaps the decisions as a chain (latency → streaming; late labels → two products; the lakehouse as the meeting point) and names the failure modes with their mitigations — the recap plus volunteered risks that make the last minute the loudest.

---

## Case 2 — Executive & Analytics Dashboards for E-commerce

**Prompt:** "Design the analytics platform for a mid-size e-commerce company: exec dashboards, analyst SQL, marketing reports."

### Requirements (ch02)

- Latency: hourly refresh is *fine* for execs; analysts tolerate daily. Nobody said real-time — don't invent it.
- Correctness: exact — finance-grade numbers (this is reporting, not alerting).
- Volume: 20GB/day of events, 50 or so SaaS/DB sources.
- Consumers: BI tools + SQL analysts; ~40 dashboards.

Unpacking: the latency requirement is almost a *non*-requirement, and saying so is the first senior move of the case — hourly refresh is fine for executives scanning a dashboard, analysts tolerate daily for deep dives, and *nobody asked for real-time*, so designing streaming here is designing for ungiven scale (ch23's trap #3; ch02's governor). The correctness requirement is the opposite: these numbers go to finance and the board, so they must be *exact* — no approximation, no sampling — and recomputable when a bug is found, which is a batch superpower (re-run the window, ch07). Volume is modest: 20GB/day is a small-data day by the standards of this book, with the complexity living in *source count* — around fifty SaaS applications and databases, each with its own API, auth, and quirks. And forty dashboards plus SQL analysts defines the serving problem: concurrency and query patterns, not raw data size, are what the serving layer must survive (ch16).

### The cascade

- **Paradigm (ch07)**: batch. Latency budget is minutes-to-hours; correctness demands recompute-on-bug. The cheapest correct answer — say that sentence.

"The cheapest correct answer is batch" is the sentence to say aloud, because it does two things at once: it makes the call (beat 3), and it frames batch as a *cost-justified engineering decision* rather than a conservative default (beat 4's setup — "if exec freshness ever needed to be minutes, here's the upgrade path," which arrives in step 6). The correctness argument is the stronger half: finance-grade numbers that can be recomputed from scratch when a bug is found is exactly the batch recovery shape (§1 of ch22), and no streaming system gives you that simple guarantee.

- **Ingestion (ch03-06)**: mixed sources — CDC for the OLTP databases (orders, users; deletes matter to finance), SaaS connectors for Shopify/ Ads (buy, ch04), event tracking for clickstream landing in the raw plane. Two planes (ch03) even in batch-world: raw first, always.

Three source classes, three ingestion mechanisms. The operational databases (orders, users) stream through CDC (ch06) — and CDC specifically because it captures *deletes* and *updates*, which a nightly table export would miss and which finance absolutely notices when revenue reconciliation drifts. The fifty SaaS sources (shop platform, ad networks, email tooling) are *bought* connectors (ch04) — building and maintaining fifty API integrations in-house is a product company, not a data pipeline, and buy is the honest answer. Clickstream arrives as event tracking. All three land in the raw plane exactly as received (ch03) before any transformation: even in a batch world, the raw plane is what makes a discovered conversion- logic bug fixable by re-run instead of by apology.

- **Storage (ch12-13)**: lakehouse tables (Iceberg/Delta) as the open-format core; warehouse serving for BI (ch16). ELT: land, then transform with SQL (dbt-style).

The storage call is lakehouse tables (Iceberg/Delta, ch12) as the core: open formats mean no vendor lock on the canonical data, schema evolution, and time travel for the "what did the dashboard show last Tuesday" question. The letter order matters and is worth saying: *ELT*, not ETL — land raw first, then transform *inside* the platform with SQL (dbt-style, ch21), because transforms written as SQL over the raw plane are versioned, testable, and re-runnable, while transforms applied during loading are where correctness bugs go to hide.

- **Serving (ch16)**: warehouse-direct as default — compute-isolated BI warehouse (connection-string-level workload management), result caching, connection pooling; marts only when scan-cost x frequency x concurrency says no. Semantic layer for "revenue."

The serving default is *warehouse-direct*: analysts and BI tools query the warehouse's lakehouse tables through its own compute layer, isolated from other workloads, with connection pooling and result caching absorbing the repeat-load (ch16). Precomputed marts are the exception, decided by a little arithmetic — scan cost × refresh frequency × concurrency: a dashboard whose underlying scan is expensive, refreshed hourly, hit by forty users simultaneously justifies a mart; a niche analyst query does not. And the *semantic layer* exists because "revenue" is a theological question — with or without refunds? with or without tax? booked on order date or ship date? — and the answer must be defined *once*, in code, so all forty dashboards cannot quietly disagree with each other.

- **Quality/ops (ch21)**: dbt tests + volume anomaly gates + freshness SLOs ("exec dashboard current through yesterday by 6:30am") with the circuit breaker: yesterday's right data beats today's wrong data.

The quality layer is the ch21 treatment applied: schema and null tests in dbt, volume anomaly gates against a baseline, and *per-consumer* freshness SLOs — the exec dashboard's is "current through yesterday by 6:30am," a promise specific enough to alert on. The circuit breaker completes it: when a gate fails, the bad partition is quarantined and consumers see yesterday's correct data with a banner (ch22 §6), because a CFO who quotes a wrong number in a board meeting is a permanent trust cost.

- **The "real-time dashboard" follow-up**: when someone asks for real-time, the answer is micro-batch (ch09) — Structured Streaming committing hourly→minute-level upserts to the same lakehouse table. One codebase, one table, no new platform.

The follow-up is coming — someone always asks for real-time — and the senior answer is already staged: tighten the batch to micro-batch (ch09). Structured Streaming writing minute-level upserts into the *same* lakehouse tables means no new platform, no second copy of the transformations, and no re-architecture — the design's upgrade path was built in on day one, which is exactly what "cheapest correct answer" was protecting.

### Architecture sketch

```
OLTP DBs --CDC-->  +--------------------------------+
SaaS (buy, ch04)-> | raw plane (S3, as-landed)      | --lineage/contracts (ch20)
clickstream ------>|                                |
                   +------> lakehouse tables (Iceberg/Delta; dbt transforms)
                                        |
                                        v
                     warehouse serving layer (BI_WH compute isolated, ch13/16)
                                        |
                     BI (JDBC/ODBC, pooled, SSO) / semantic layer / analysts
```

Reading the sketch: three source classes land in the raw plane as-received (with lineage and contracts registered on the way in, ch20), dbt transforms turn raw into lakehouse tables, and the warehouse serving layer — its own compute, pooled, cached — fronts the BI fleet, the semantic layer, and the analysts.

### Failure modes to volunteer

- Morning queue storm (ch16): 40 dashboards x 8am refresh — pooling, caching, staggered schedules, separate BI compute.
- Silent half-load (ch07/21): completeness checks before compute.
- Cost creep: scan-priced dashboards + `SELECT *` — column pruning, partition pruning, marts for the hot few.

The morning queue storm is the predictable one: forty dashboards all configured to refresh at 8am is a self-inflicted thundering herd against the warehouse, and the fixes are pooling, caching, staggered refresh schedules, and the BI fleet on its own isolated compute (ch16) — say "stagger the schedules" and the interviewer hears someone who has been in the room where every dashboard failed at once. The *silent half-load* is the finance-killer: an ingestion that lands half the orders rows with no error, and the revenue dashboard is confidently wrong — completeness checks (row counts against expectation, ch21) run *before* compute, not after the CFO asks. And *cost creep* is the slow one: scan-priced dashboards plus a `SELECT *` somewhere in a hot path, multiplied by forty dashboards and months — the defenses are column and partition pruning (ch17, ch07), and marts for the few genuinely hot scans.

### The 60-second close

"Latency budget is hours and correctness is finance-grade — that's batch, and I'll defend it as the cheapest correct answer. CDC for databases, bought connectors for SaaS, raw plane for everything. Iceberg tables with SQL transforms; warehouse serving with the BI fleet on its own compute, pooled and cached. Quality gates with a circuit breaker, freshness SLOs per consumer. And when 'real-time' arrives — and it will — the upgrade path is micro-batch upserts into the same tables, not a new platform."

Notice the close's structure: decisions as a chain, then the *conditions* — the real-time follow-up answered before it is asked, with an upgrade path instead of a re-architecture. Beat 4 of ch23, delivered as the last sentence.

---

## Case 3 — ML Feature Pipeline

**Prompt:** "Design the feature pipeline that feeds both model training and a low-latency ranking model."

### Requirements (ch02)

- Training: batch — full history, point-in-time correct labels.
- Serving: features for online inference within ~10ms of request.
- The killer requirement, usually unstated: **training/serving consistency** — the model must be trained on features computed *exactly* the way serving computes them, and historically (no leakage).

Unpacking: training consumes *full history* — every feature value for every entity, as it existed at each past moment — which is a batch workload by definition. Serving computes features for *one entity* at *request time* within ~10 milliseconds — a KV-shaped workload. Two workloads wearing one name. And the requirement that is almost never written on the whiteboard but decides the design: the model that serves live must have been *trained* on features computed exactly the way the serving path computes them — same definitions, same window boundaries, same dedup rules — and computed *as of their historical timestamps*, not as of today. Any gap between how features were computed for training and how they are computed live, or any future information leaking into a historical training row, and the model is quietly broken.

### The cascade

- **Paradigm (ch07-09)**: hybrid by construction — streaming features (velocity: "user's clicks in last 10 min") need event-time windows on live events (ch08); batch features (demographics, 30-day aggregates) are lakehouse batch jobs (ch07). No single paradigm survives.

This is the case where the hybrid answer is forced by inspection, not chosen: some features are *recency-shaped* — "user's clicks in the last 10 minutes" is meaningless computed nightly, it only exists as an event-time window over live events (ch08) — and some features are *depth-shaped* — demographics and 30-day aggregates are recomputable nightly from history (ch07). A streaming-only design cannot cheaply produce full-history training data; a batch-only design cannot produce a 10-minute click window. No single paradigm survives, and saying which feature class forced which paradigm is the four-beat pattern in action.

- **Architecture (ch12)**: the lakehouse is the meeting point — batch features as tables; streaming features upserted by Flink/micro-batch into the same table format for training reads.

The lakehouse is where the two feature worlds meet: batch features are written as lakehouse tables (ch12); streaming features are upserted by Flink or a micro-batch into the same table format. The payoff is training data: a training job reads *one* kind of table — with time travel (ch12) for as-of reads — rather than joining a batch warehouse to a streaming store and hoping they agree.

- **Serving (ch14/16)**: KV store (DynamoDB/Redis) for online features — the *feature store* concept: one definition, two materializations (offline table + online KV), synced by pipeline.

Online inference needs each feature within ~10ms, so the live features sit in a KV store keyed by entity (ch14, ch16). The *feature store* is the governance pattern wrapped around that: every feature has one definition — one transformation codebase — and two *materializations*, the offline lakehouse table for training and the online KV for serving, kept in sync by the pipeline that computes both from the same code. The store is not a product you buy; it is this discipline, whether or not it has a vendor logo.

- **The correctness core — point-in-time correctness**: training rows must carry features *as they existed at event time*, not as they exist now. This means: event-time joins (ch08 semantics), and the offline store must be queryable "as of" (table formats' time travel, ch12, helps). Feature leakage is the silent model killer — volunteer it.

Made concrete: a training row for "did this user click the recommendation on March 3rd" must carry that user's "clicks in last 10 minutes" *as it was at the moment of the March 3rd request* — not the value recomputed today, which includes everything the user has done since. Joining "current" feature values onto historical events silently feeds the model the future: a churn model trained with "account_closed_date" filled in is memorizing the answer. That is *feature leakage*, and it is silent — the offline metrics look superb and the model is useless in production — which is exactly why it is volunteered unprompted. The mechanics that prevent it: event-time (as-of) joins with ch08 semantics, and an offline store queryable "as of" a timestamp, where table-format time travel (ch12) helps.

- **Training/serving skew**: the two materializations drift (streaming computes "10-min clicks" slightly differently than the batch backfill does). Mitigations: shared transformation definitions (one codebase — ch09's argument), skew monitoring (compare online/offline feature distributions for the same entity/time).

Even with one intended definition, the two materializations *drift*: the streaming path computes "clicks in last 10 minutes" with a tumbling- window boundary and the batch backfill uses event-time joins with a slightly different boundary, and over releases the definitions diverge one "small fix" at a time. The mitigations are structural: one shared transformation codebase materialized twice (ch09's "the same parameterized job" argument, applied to features), and *skew monitoring* — periodically computing the same feature for the same entity and time both ways and comparing the distributions (ch21's reconciliation, re pointed at features).

### Architecture sketch

```
events --> Kafka --+--> Flink: streaming features --upsert--> KV store (online)
                   |                                    |
                   v                                    |
raw plane --> batch: batch features ----+--> lakehouse feature tables (offline)
                                        |         ^  (point-in-time, time travel)
                                        +--> training jobs (labels joined as-of)
skew monitor: compare online vs offline for same entity/time (ch21 thinking)
```

Reading the sketch: one event plane feeds both feature paths — Flink computes the recency features and upserts the online KV store; batch jobs compute the depth features into the offline lakehouse tables; training jobs join labels as-of event time against those tables; and the skew monitor continuously compares the two materializations for the same entity and time.

### Failure modes to volunteer

- **Feature leakage** via non-as-of joins — the model looks great offline and is useless live. Point-in-time correctness is the fix; name it before asked.
- **Skew drift**: online/offline definitions diverge over releases — monitor distributions, version features.
- **KV cold-start/rebuild** (ch11/15): the online store is a projection — it must be rebuildable from the offline truth.

The three, in the volunteer's voice. *Feature leakage* is the one to name first because it is the most expensive to discover late: an as-of join done wrong produces a model whose offline AUC is brilliant and whose production value is zero, and nobody can tell until launch. *Skew drift* is the slow version of the same disease — the model degrades by fractions over releases as the two materializations diverge, so the monitor and feature versioning exist to catch it continuously. *KV cold-start* is the ops one: the online store is a *projection*, not a source of truth, so when it is lost or corrupted (ch22 §6's restore flow), it must be rebuildable from the offline tables — an online store that cannot be rebuilt from offline truth is a database pretending to be a cache.

### The 60-second close

"Features are two workloads wearing one name: streaming features for recency, batch features for depth — Flink upserting the online KV, batch jobs writing lakehouse tables, one transformation definition shared between them. The two correctness requirements that make or break this: point-in-time correctness in training — as-of joins, or the model leaks — and training/serving consistency — monitored, because definitions drift. The feature store is really a governance pattern: one definition, two materializations, both rebuildable."

---

## Case 4 — IoT Telemetry Platform

**Prompt:** "Design ingestion and analytics for 1M devices emitting telemetry every 10 seconds."

### Requirements (ch02)

- Volume: 100k events/sec sustained (1M devices / 10s) — the headline number. Growth: devices double yearly.
- Latency: alerting on device anomalies within ~1 min; dashboards hourly; most queries are time-windowed per device/fleet.
- Correctness: telemetry is logs-class (ch05) — lossy is *acceptable by convention*; duplicates are common (device retries).
- Access pattern: recent-data queries dominate; 95% of reads touch last 24-48h (the classic time-series shape).

Unpacking: the arithmetic is the headline — a million devices every ten seconds is *100,000 events per second, sustained*, doubling yearly, so provisioning decisions made at launch are revisited within the year. Latency is *tiered*, and the tiers matter: device-anomaly alerting wants ~1 minute (a genuinely streaming requirement), dashboards are happy hourly (batch territory), and most queries are time-windowed per device or fleet (a storage-shape requirement). Correctness is deliberately relaxed: telemetry is logs-class (ch05) — losing the odd reading is acceptable by convention, and duplicates are *expected* because devices retry — which relaxes the correctness machinery and saves real money. And the access pattern is the classic time-series shape: 95% of reads touch the last 24-48 hours, which makes *recency tiering* the core storage decision rather than an optimization.

### The cascade

- **Ingestion (ch05)**: agent/embedded publishers -> Kafka/Pub/Sub; the buffer sizing *is* the architecture — devices burst on reconnect, and 100k/sec sustained means provisioning for 3-5x spikes. Dedup by (device_id, event_time) downstream — at-least-once everywhere, idempotent writes (ch08 discipline).

The ingestion layer is where this case is won or lost. Devices publish to a queue (Kafka/Pub/Sub), and the queue's *buffer sizing is itself an architectural decision*: devices do not emit smoothly — a gateway restart or network blip brings thousands back at once, replaying their buffered readings, so a system provisioned for 100k/sec sustained will fall over at the first 400k/sec burst. Provision for 3-5x spikes, and treat that headroom as architecture, not slack (ch05). Correctness comes from the logs-class discipline: at-least-once delivery everywhere (cheap, loss- tolerant) plus dedup by `(device_id, event_time)` at the consumer — idempotent writes, because device retries make duplicates a *certainty*, not a risk.

- **Paradigm (ch07-09)**: tiered by need — streaming only for the anomaly-alert path (1-min SLA justifies it); everything else batch/micro -batch. Don't stream the dashboard path.

The paradigm is *tiered by need*, and the discipline is in the second sentence: only the anomaly-alert path has a latency requirement (1 minute) that justifies streaming; everything else — rollups, dashboards, history — is batch or micro-batch against the same events. "Don't stream the dashboard path" is the ch02 governor speaking again: streaming every path because the *headline number* is big is how platforms become unaffordable and undebuggable. One streaming topology with a real SLA; everything else reuses batch mechanics.

- **Storage (ch13-14, 12)**: time-series shape — downsampling tiers: raw (S3, cheap, 30-90d) → hourly/daily rollups (lakehouse tables, forever). The wide-column/KV option (ch14) for *latest-state per device* ("device dashboard"), the lakehouse for history.

Storage follows the access pattern instead of the data volume. Raw readings land in cheap object storage (S3) kept 30-90 days — because 95% of reads touch the last 24-48 hours, and the tail is kept only for backfills and audits. On top of the raw, hourly and daily *rollups* (averages, min/max, counts per device and fleet) are written to lakehouse tables and kept forever: they are tiny, they answer almost every dashboard query, and they compound in value as history grows. For the "what is each device's state *right now*" view (the device dashboard), the wide-column/KV pattern (ch14) holding latest-state-per-device is the right tool. Three tiers, each matched to a read pattern, none matched to "fancy."

- **Format (ch17-18)**: Parquet partitioned by time (ch19 sizing: 128MB-1GB files, compaction jobs — at 100k/sec the small-files disease arrives fast without them).

At 100k events/sec, the small-files disease (ch19) is not a risk — it is a *schedule*: device bursts and 10-second emission cycles will shred the raw plane into millions of files without deliberate sizing. So: Parquet (ch17-18) because the queries are columnar aggregations over time windows, partitioned by time because every query is time-windowed, files sized 128MB-1GB, and *scheduled compaction jobs* from day one to merge the small files the bursts will inevitably create.

- **Cost control (ch02/21)**: this case is a cost case — storage growth is unbounded; lifecycle tiering + downsampling + retention policy are *requirements*, not optimizations. Chargeback per product team.

The final step is the reframe that makes this case senior: at a million devices doubling yearly with unbounded retention-by-default, *the cost model is the architecture*. Lifecycle tiering (hot → infrequent → archive), downsampling tiers, and a written retention policy (ch22 §7) are not optimizations appended to a finished design — they are requirements, stated in the first ten minutes like the latency numbers. Chargeback per product team (ch21) closes the loop: teams that see their own telemetry bill design their queries differently.

### Architecture sketch

```
1M devices --> edge/gateway (batch, compress) --> Kafka (3-5x burst headroom)
                                                   |
                     +-----------------------------+--------------------------+
                     v                             v                          v
              Flink: anomaly rules          micro-batch rollups         raw -> S3
              (1-min SLA, keyed             (hourly aggregates           (30-90d,
               by device, ch08)              into lakehouse, ch09)        lifecycle)
                     |                             |                          |
                     v                             v                          v
               alerting/KV              BI dashboards (ch16)      archive tier /
                                                                  downsampling
```

Reading the sketch: devices batch and compress at the edge, then one Kafka topic — provisioned with 3-5x burst headroom — fans out to three consumers by need: Flink for the 1-minute anomaly path only, micro-batch rollups into lakehouse tables for the dashboard path, and raw landing in S3 with lifecycle rules for the archive tier.

### Failure modes to volunteer

- **Reconnect storms**: fleet firmware update → 1M devices reconnect and replay buffers → 10x spike. The queue headroom + backpressure story (ch05) is the answer; say "reconnect storm" unprompted.
- **Hot partitions** (ch05/14): device_id keying is uniform, but a single noisy device (broken sensor, retry loop) can hot-key a partition — per-device quotas in the gateway.
- **The unbounded bill**: no lifecycle policy → raw telemetry forever → the cost review in year two. Downsampling tiers stated up front.

Each in plain words. A *reconnect storm* is a self-inflicted DDoS: the fleet's firmware update finishes, a million devices come back online at once, and each replays everything it buffered while offline — a 10x spike arriving on a schedule *you* created. The answer is the queue headroom plus backpressure (ch05), and saying "reconnect storm" unprompted is the operational-experience signal of this case. *Hot partitions* are the skew surprise: device_id keying looks uniformly distributed until one broken sensor enters a retry loop and sends a thousand times its share, overloading one partition while the rest idle — per-device quotas at the gateway bound the damage (ch05, ch14). And *the unbounded bill* is the year-two certainty: without lifecycle tiers, raw telemetry accumulates forever while its read value expired months ago — which is why downsampling and retention were stated as requirements, not retrofitted after the cost review.

### The 60-second close

"A hundred thousand events a second, loss-tolerant, time-windowed reads — so: Kafka sized for reconnect-storm spikes, dedup downstream because devices retry, and a tiered design: streaming only for the one-minute anomaly path; micro-batch rollups into lakehouse tables for everything else; raw to S3 with a lifecycle policy and downsampling tiers, because at this volume the cost model *is* the architecture. And I'd provision for the reconnect storm — a firmware update is a self-inflicted DDoS, and the queue headroom is what turns that from an outage into a busy morning."

---

## Decision Rules

The pattern across all four cases

| Case | Paradigm | Architecture | The one senior insight |
|---|---|---|---|
| Fraud | streaming decision + batch truth | Lambda-shaped (two products) | labels arrive late — separate the decision from the truth |
| Dashboards | batch | lakehouse + warehouse serving | cheapest correct answer; micro-batch is the upgrade path |
| Features | hybrid by construction | lakehouse + KV, feature-store governance | point-in-time correctness and skew are the real problems |
| IoT | streaming alert + batch rollups | tiered downsampling | cost model is the architecture at unbounded scale |

The table is the chapter compressed to one view, and the right-hand column is the part to internalize: each case's senior insight is a *separation* — decision from truth, correct from fancy, definition from materialization, architecture from cost. Read the paradigm column next and notice the distribution: only one case is "pure" anything. Fraud is streaming for action and batch for truth; features are hybrid by construction; IoT streams one narrow path and batches everything else; dashboards are batch with a micro-batch upgrade path pre-built. Streaming is a spice, not the meal — even in a book with four streaming chapters.

Every case: requirements first (ch02), paradigm forced by latency (ch07-09), architecture forced by correctness (ch10-12), storage by access pattern (ch13-16), formats by physics (ch17-19), and the -ilities volunteered (ch20-22). The cascade (ch01), executed.

That closing sentence is the whole book as a checklist, and it is worth rehearsing as one: requirements before anything; the latency number picks the paradigm; the correctness requirement picks the architecture shape; the access pattern picks the storage; physics picks the formats; and the failure modes, security, cost, and ops come out of your mouth before the interviewer asks for them.

## Failure Modes

Each case has one defining failure mode - the one to volunteer unprompted:

| Case | The defining failure | The named mitigation |
|---|---|---|
| Fraud | consumer lag past retention = silently missed fraud | lag-vs-retention alerting (ch05) |
| Dashboards | morning queue storm; silent half-load | pooling/caching/stagger (ch16); completeness gates (ch21) |
| Features | feature leakage + training/serving skew | point-in-time joins; skew monitoring |
| IoT | reconnect storm + unbounded storage bill | queue headroom; lifecycle tiers + downsampling |

Why "defining": each of these is the failure that is *specific to that architecture* — not a generic outage any system could have. Lag past retention only exists where a queue fronts a decision path; a morning queue storm only exists where forty dashboards share a warehouse; leakage only exists where training and serving are separate materializations; a reconnect storm only exists where a million devices buffer and retry. Naming the failure that only *your* design can have — with its mitigation — is the strongest available proof that you understand your own architecture rather than a template.

## Interview Narration

These cases are for your voice, not your eyes. The final preparation move: pick a prompt, set a 45-minute timer, and narrate the whole cascade out loud - requirements, decisions with trade-offs, failure modes, close - then compare what you said against the case above. The gap between the two is your study list. Repeat until the four-beat decision pattern (options, trade-offs, call, what-would-change-it - ch23) is reflex. The last line of this book is also its first instruction: go practice it out loud.

The method deserves its last paragraph: *out loud*, because the interview is a spoken performance and silent reading rehearses nothing you will actually do; *against a timer*, because the failure mode of the knowledgeable is running out of clock (ch23's unwatched clock); and *compared against the case*, because self-assessed narration always feels complete — the gap between what you said and what the case says is the only honest inventory, and it *is* your study list. When the four-beat pattern survives a 45-minute timer without notes, the cascade has become reflex — and this book has done its job. Go practice it out loud.

---

| <- Previous | Next -> |
|---|---|
| [Chapter 23 — Running the Room](ch23-running-the-room.md) | [README](README.md) |
