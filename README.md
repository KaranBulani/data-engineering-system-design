# Data Engineering System Design — A Practical Guide

A plain-English textbook about designing, running, and explaining data
systems. It is for data engineers who need to make sound architecture choices
in real projects and in system-design interviews.

**Scope:** the decisions *around* data modeling — ingestion, processing
paradigms, end-to-end architecture, storage engines, file formats, serving,
and operations. Dimensional modeling (star schemas, SCDs, normalization
debates) is deliberately out of scope.

**How to read it:** start here, then read Chapter 01 through Chapter 24 in
order. The chapters build on each other. Each chapter starts with the problem,
explains the important ideas in simple terms, compares the real options, and
ends with rules, common failures, and words you can use in an interview.

**A useful promise:** this book does not stop at drawing a pipeline. It also
covers how to monitor it, find bad data, recover from a failed run, protect
data, deploy changes, and recover from a disaster.

---

## Reading maps

| Path | Chapters | For |
|---|---|---|
| **Read in order** | README → 01 → 24 | Learn the full story from requirements to production operations and interview practice |
| **Interview-prep (fast)** | 01 → 02 → 03 → 07 → 08 → 10 → 11 → 12 → 22 → 23 → 24 | Prepare in about one to two weeks |
| **Reference** | Parts V-VII | Look up a specific design choice while working |

---

## Table of contents

### Part I — Foundations: How Decisions Get Made
| # | Chapter |
|---|---|
| 01 | [The Decision Cascade](ch01-the-decision-cascade.md) |
| 02 | [The Requirements That Drive Everything](ch02-requirements-that-drive-everything.md) |

### Part II — Ingestion Patterns (Getting Data In)
| # | Chapter |
|---|---|
| 03 | [The Ingestion Taxonomy](ch03-the-ingestion-taxonomy.md) |
| 04 | [Pull Patterns (Files, APIs, JDBC, SaaS)](ch04-pull-patterns.md) |
| 05 | [Push Patterns (Kafka, Pub/Sub, Webhooks, Logs)](ch05-push-patterns.md) |
| 06 | [CDC, the Long Tail & Normalization](ch06-cdc-long-tail-and-normalization.md) |

### Part III — Processing Paradigms
| # | Chapter |
|---|---|
| 07 | [Batch](ch07-batch.md) |
| 08 | [Streaming](ch08-streaming.md) |
| 09 | [Micro-batch](ch09-micro-batch.md) |

### Part IV — Architecture Patterns
| # | Chapter |
|---|---|
| 10 | [Lambda Architecture](ch10-lambda-architecture.md) |
| 11 | [Kappa Architecture](ch11-kappa-architecture.md) |
| 12 | [Lakehouse & Table Formats](ch12-lakehouse-and-table-formats.md) |

### Part V — Storage Engines: SQL vs NoSQL
| # | Chapter |
|---|---|
| 13 | [The SQL Family](ch13-the-sql-family.md) |
| 14 | [The NoSQL Families](ch14-the-nosql-families.md) |
| 15 | [Polyglot Persistence](ch15-polyglot-persistence.md) |

### Part VI — Serving, Formats & Spark
| # | Chapter |
|---|---|
| 16 | [The Serving Layer & Consumption](ch16-the-serving-layer-and-consumption.md) |
| 17 | [Row vs Columnar: The Physics](ch17-row-vs-columnar.md) |
| 18 | [File & Wire Format Catalog](ch18-file-and-wire-format-catalog.md) |
| 19 | [Spark in Practice](ch19-spark-in-practice.md) |

### Part VII — Security & Operations
| # | Chapter |
|---|---|
| 20 | [Security, Governance & Catalogs](ch20-security-governance-and-catalogs.md) |
| 21 | [Orchestration, Data Quality & Operations](ch21-orchestration-data-quality-and-operations.md) |
| 22 | [Production Operations Runbooks](ch22-data-operations-runbooks.md) |

### Part VIII — Interview Execution
| # | Chapter |
|---|---|
| 23 | [Running the Room](ch23-running-the-room.md) |
| 24 | [Worked Cases](ch24-worked-cases.md) |

---

## Chapter conventions

Every chapter follows the same skeleton:

1. **The Question** — the decision as it appears in an interview
2. **The Physics** — the mechanics that make the trade-off real (no hand-waving)
3. **The Options** — comparison tables across the realistic candidates
4. **Decision Rules** — "if X → choose Y because Z" heuristics
5. **Failure Modes** — what breaks when you choose wrong, and the real-world symptoms
6. **Interview Narration** — 60-second scripts of how a senior engineer says it out loud

### Words used throughout the book

- **Pipeline:** the steps that move and transform data from a source to a
  place people or systems can use.
- **Partition:** one manageable slice of a dataset, usually a day or hour.
- **Idempotent:** safe to run again; a re-run leaves the same correct result.
- **SLA/SLO:** a promise to users about service; an SLO is the measurable
  target, such as "yesterday's sales data is ready by 06:30."
- **Checkpoint:** saved progress for a streaming job, so it can restart from
  the right place.
- **Raw plane:** the original, durable copy of incoming data. It is the
  safety net for reprocessing and disaster recovery.

### Production operations coverage

[Chapter 22 — Production Operations Runbooks](ch22-data-operations-runbooks.md)
answers the practical questions that come after a pipeline is deployed:

- alert design, severity, ownership, and alert testing;
- data validation, quality gates, quarantine, and reconciliation;
- structured logs and queryable `pipeline_runs` log tables;
- failed-run containment, repair, validation, and communication;
- retention, deletion, backups, and legal holds;
- recurring security work and security-incident response;
- canary releases, smoke tests, promotion, and rollback; and
- disaster recovery, RTO/RPO targets, backups, replication, and restore drills.

---

## The one diagram to keep in your head

```
business ask
    |
    v
requirements (latency SLA, volume, consistency, cost, ops maturity)   <- ch02
    |
    v
batch  vs  streaming  vs  micro-batch                                 <- ch07-09
    |
    v
lambda  vs  kappa  vs  lakehouse                                      <- ch10-12
    |
    v
storage engines: SQL vs NoSQL (per workload)                          <- ch13-15
    |
    v
serving & consumption (the last mile)                                 <- ch16
    |
    v
file & wire formats, layout, tuning                                   <- ch17-19
    |
    v
security, orchestration, quality, and production runbooks              <- ch20-22
    |
    v
execution: running the room, worked cases                              <- ch23-24
```

Decisions **cascade**: every upstream choice constrains everything
downstream, and senior engineers walk the chain top-down, driven by
requirements — never by favorite tools (ch01).
