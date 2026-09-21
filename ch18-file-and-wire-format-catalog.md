# Chapter 18 — File & Wire Format Catalog

> Part VI — Serving, Formats & Spark

CSV, JSON, Avro, Parquet, ORC, Protobuf — plus the compression codecs wrapped
around them. The format decision has three parts: **at rest** (storage
efficiency, scan speed), **on the wire** (schema evolution, compactness), and
**splittability** (parallelism). Get the split right and the rest follows.

## The Question

*"Which format do I land the data in, which do I move it in, and which codec
do I compress it with — such that reads are fast, schemas can evolve, and
Spark can actually parallelize?"*

## The Physics

### The catalog

| Format | Layout | Schema | Splittable | Compresses | Role |
|---|---|---|---|---|---|
| CSV | row text | none (header lie) | as-is yes; gzip'd NO | poorly | human exchange, legacy |
| JSON | row text (semi) | implicit, drifting | as-is yes; gzip'd NO | poorly | APIs, logs, human-readable events |
| Avro | row binary | schema in every file | yes (sync markers) | well | **the wire format**; event streams |
| Parquet | **columnar** | schema + stats | yes (row groups) | very well | **the at-rest analytics format** |
| ORC | columnar | schema + index footers | yes (stripes) | very well | Hive-ecosystem Parquet |
| Protobuf | row binary (message) | .proto, external | per-message framing needed | well | the other wire format (RPC, Kafka) |

**The idiom to memorize: Avro (or Protobuf) on the wire, Parquet at rest.**
The reasoning, piece by piece, is the rest of this chapter.

### Row-wise binary with schemas: Avro

- Schema stored *with the data* (JSON header in each file); rows as compact
  binary. Sync markers every block make files splittable even when
  compressed — a reader can start mid-file at a block boundary.
- **Schema evolution is the superpower**: writer schema and reader schema are
  different, resolved by field *name* (not position): add a column with a
  default -> old readers fine; rename breaks (aliases help). This is why Avro
  owns event streaming: producers and consumers deploy independently, and the
  format itself negotiates the gap.
- **Protobuf vs Avro** (the wire decision): Protobuf — smaller, faster,
  tag-position field numbers, required/optional discipline, vast RPC
  ecosystem; evolution via new field numbers, unknown fields preserved. Avro —
  schema carried with data, name-based evolution, the Kafka/Schema-Registry
  native. Choose by ecosystem; both are correct, CSV is not.

### Columnar at rest: Parquet (and ORC)

- Column chunks within row groups (ch17); per-chunk encodings — dictionary
  (low cardinality), run-length (sorted/repeated), delta (sorted numerics) —
  and **per-chunk statistics: min/max/null counts**.
- Those statistics are the query-acceleration engine: a predicate
  `WHERE dt='2026-09-20'` consults chunk min/max and skips whole chunks —
  **predicate pushdown / data skipping** before a single page is decoded
  (ch19 makes practical use of this).
- Nested data (lists, structs) via definition/repetition levels — Parquet is
  columnar *even for semi-structured payloads*, which is why it ate the JSON
  analytics world.
- ORC: same ideas (stripes, index, encodings), Hive lineage, lightweight
  indexes — functionally a sibling; ecosystem (Hive/Trino/Presto shops)
  decides.

### Splittability — the constraint people forget

A file is splittable if a reader can start reading at an arbitrary offset and
still make sense of the stream.

- **One giant gzip file is NOT splittable**: gzip is a single deflate stream;
  you must decompress from byte 0. One 20GB `.csv.gz` = one Spark task = one
  core, while the cluster stares. *The classic trap.*
- **bzip2** is splittable but so slow nobody bothers. **LZO** splittable when
  indexed (legacy Hadoop answer).
- **The robust pattern: block-based compression or splittable containers** —
  Avro with block-level codec (each block independently compressed, sync
  markers between), Parquet/ORC (row groups/stripes are natural split
  points), or simply **many files** instead of one giant one (ch19 file
  sizing).

### Codecs, honestly

| Codec | Speed | Ratio | Splittable (alone) | Use |
|---|---|---|---|---|
| Snappy | very fast | modest | n/a (used inside containers) | hot data, shuffle (Spark default-ish) |
| Zstd | fast | best speed/ratio balance | n/a (container) | **modern default** at rest |
| Gzip | slow | good | **no** | small files, legacy, avoid at scale |
| LZ4 | very fast | modest | n/a | hot path, shuffle alternative |

Rule of thumb: **Snappy/LZ4 when CPU is the bottleneck (hot, shuffle),
Zstd when storage/network is (cold, at rest), Gzip never at scale.** (The
Codec Central Benchmarks are workload-dependent — treat ratios as
rule-of-thumb, not gospel.)

### Schema evolution & the Schema Registry

| Compatibility mode | Meaning | Who must deploy when |
|---|---|---|
| Backward | new schema reads old data | consumers first-safe |
| Forward | old schema reads new data | producers first-safe |
| Full | both | both-safe, strictest |

The **Schema Registry** pattern (Confluent, AWS Glue Schema Registry): a
broker-side check — producer registers schema v2; registry verifies
compatibility against v1 per the topic's policy; reject the *deployment* of
an incompatible schema, not the runtime message. This is **data contracts**
enforced at the pipeline door (ch21 extends this to SLAs and ownership).

Avro's name-based resolution + registry-enforced compatibility = the
producer/consumer independence that makes streaming pipelines evolvable.
JSON "schemas" (JSON Schema) exist but are advisory — nothing enforces them
at the wire by default, which is the ch05 logs-chaos story.

## The Options

| Decision | Options | Default |
|---|---|---|
| Wire format | Avro / Protobuf / JSON | Avro (or PB) + Schema Registry; JSON only at the edges (humans, public APIs) |
| At-rest analytics | Parquet / ORC | Parquet (open ecosystem) |
| At-rest operational row | engine-native (RocksDB SST, etc.) | not your decision — the store owns it |
| Compression | Snappy/LZ4/Zstd/gzip | Snappy-ish hot, Zstd cold |
| Raw/bronze landing | Avro/JSON as-received | as-landed fidelity > elegance (ch03/06) |

## Decision Rules

- **Wire: Avro or Protobuf with a Schema Registry and an explicit
  compatibility policy.** JSON at public edges only.
- **Rest: Parquet (or ORC where the ecosystem dictates).** Columnar + stats
  + encodings is the whole game (ch17).
- **Never one giant gzip.** Block-compressed containers (Avro+codec, Parquet)
  or many files.
- **Hot path/shuffle: Snappy/LZ4; cold/rest: Zstd; gzip only for
  compatibility.**
- **Schemas evolve by design, not accident**: additive changes with defaults,
  registry-enforced — plan the compatibility mode with the team's deployment
  order.
- **Land raw as-received** (fidelity for replay), convert to analytical
  formats in the derive step — the raw plane is insurance, not a showcase.
- **File *count* is a format decision too**: target 128MB-1GB files (ch19) —
  one giant file is unparallelizable, a million tiny ones are unmanageable.

## Failure Modes

- **The 20GB `.csv.gz`**: one task, one core, 14-hour read stage, cluster
  idle. Everyone's first big-data scar. Fix: many files, splittable
  container, or block codec.
- **Schema drift without a registry**: producer adds a field as JSON;
  downstream parser breaks — or worse, silently nulls. The two-week null
  column (ch05) is a format-governance failure.
- **Position-based evolution**: dropping/reordering fields in a
  position-sensitive format (or sloppy Protobuf field-number reuse) — old
  data decodes into nonsense. Name-based (Avro) or number-discipline (PB)
  exists for a reason.
- **Gzip at scale**: "storage is 30% smaller" and CPU is 5x — the nightly
  job that exists to decompress data it mostly won't read.
- **JSON analytics**: querying nested JSON at scan time (Spark `from_json`)
  on every pipeline run — paying parse cost forever instead of converting to
  Parquet once at landing.
- **Tiny-file explosion** from over-partitioning (ch19's disease, seeded
  here): `dt/hour/minute` partitions at low volume -> millions of KB-scale
  files; metadata overhead swamps the data.

## Interview Narration

"I split the format decision in two: the wire and rest. On the wire I want a
schema-carrying binary — Avro, or Protobuf where the ecosystem points that
way — because producer/consumer independence is the real requirement, and
that's what Avro's name-based evolution plus a Schema Registry buys: additive
changes with defaults pass a compatibility check at registration time, so an
incompatible schema fails the deployment, not 2am consumers. JSON stays at
the human edges.

At rest it's Parquet, and the reason is mechanical: columnar layout reads
only referenced columns, dictionary and run-length encoding collapse
low-cardinality columns, and per-chunk min/max statistics let engines skip
chunks before decoding — that's predicate pushdown, and it's why
full-history scans became affordable. Compression: Snappy or LZ4 on hot paths
and shuffle, Zstd for cold storage, and gzip never at scale.

The constraint people forget is splittability — one giant gzip file is a
single deflate stream, so it's one task on one core no matter how big the
cluster. The robust answers are block-compressed containers like Avro with a
codec, or Parquet's row groups, or simply many reasonably-sized files. And
I'd close with file sizing as a format decision: target hundreds of
megabytes, because one giant file is unparallelizable and a million tiny
ones drown the metadata layer — the format choice and the file layout
choice are the same decision."

---

| <- Previous | Next -> |
|---|---|
| [Chapter 17 — Row vs Columnar](ch17-row-vs-columnar.md) | [Chapter 19 — Spark in Practice](ch19-spark-in-practice.md) |