# Chapter 18 — File & Wire Format Catalog

> Part VI — Serving, Formats & Spark

Data formats show up in two situations. "At rest" means data saved on disk for later analysis, for example Parquet files sitting in a data lake. "On the wire" means data moving between systems, for example events flowing from a service into Kafka. This chapter is a catalog of the common formats — CSV, JSON, Avro, Parquet, ORC, and Protobuf — plus the compression codecs that wrap around them.

Three properties decide the format choice:

- **At-rest efficiency** covers storage cost and scan speed: how much disk space the format uses, and how fast an engine can read only the columns a query actually needs.
- **Wire efficiency** covers size and schema change: how compact the data is while moving, and how safely producers and consumers can change their schemas independently over time.
- **Splittability** covers parallelism: whether one large file can be divided into pieces so that many Spark tasks can read different parts of it at the same time.

If you remember only one sentence from this chapter, make it this one: **Avro (or Protobuf) on the wire, Parquet at rest.** The rest of the chapter explains why, piece by piece.

## The Question

Which format should I use to store data, which format should I use to move data, and which compression codec should I pick — such that reads are fast, schemas can evolve without breaking consumers, and Spark can actually split the work across the cluster?

## The Physics

### The catalog

Before reading the table, here is what each column means. "Row" layout means the file stores one complete record after another. "Columnar" layout means the values of each column are stored together, so a query can read one column without touching the others. "Schema" describes where the field names and types are defined — inside the file, in an external definition, or nowhere. "Splittable" means a reader can start reading from the middle of the file and still parse correct records. "Compresses" is a rough judgment of how well compression codecs shrink the format.

| Format | Layout | Schema | Splittable | Compresses | Typical role |
|---|---|---|---|---|---|
| CSV | Text, one row per line | None — a header row names columns but enforces nothing | Yes as plain text; no once gzipped | Poorly | Human exchange, legacy exports |
| JSON | Text, one record per line (semi-structured) | Implicit — field names live inside each record and drift over time | Yes as plain text; no once gzipped | Poorly | APIs, logs, human-readable events |
| Avro | Binary, one record after another | Stored inside every file, as a JSON header | Yes — sync markers between blocks | Well | The wire format; event streams |
| Parquet | Columnar | Stored in the file, with per-column statistics | Yes — row groups are natural split points | Very well | The at-rest analytics format |
| ORC | Columnar | Stored in the file, with an index in the footer | Yes — stripes are natural split points | Very well | The Hive-ecosystem alternative to Parquet |
| Protobuf | Binary messages | Defined in external `.proto` files | Not per file — messages need explicit framing | Well | The other wire format; RPC and Kafka |

A short plain-language tour of each format:

**CSV** is plain text with one line per record and commas between values. The first line usually names the columns, but that is only a promise — the "header lie". Nothing stops a later row from having a different number of values, a value of the wrong type, or a column that quietly disappears. Every reader must guess types (is `007` a number or text?), and two tools can guess differently. CSV is fine for handing a spreadsheet to a person, but at scale it is large, slow, and schema-free.

**JSON** is text where each record carries its own field names, for example `{"user_id": 42, "event": "login", "ts": "2026-09-20T10:03:00Z"}`. This makes it self-describing and easy to debug, which is why APIs and logs use it everywhere. The cost is that the schema is implicit: there is no central definition, so over months the producer adds fields, renames fields, or changes types, and different consumers end up expecting different shapes. The schema "drifts". Repeating field names in every record also makes JSON much larger than a binary format carrying the same data.

**Avro** writes records as compact binary, one after another, and stores the schema inside every file as a JSON header. A reader that has never seen this kind of data can still decode it, because the instructions travel with the data. Blocks of records are separated by sync markers, which is what makes even compressed Avro files splittable (explained below). Avro is the standard format for events moving through Kafka.

**Parquet** is a columnar file format built for analytics. It groups rows into row groups, stores each column's values together, encodes and compresses each column independently, and writes statistics (minimum, maximum, null count) for every column chunk. Query engines use those statistics to skip data they do not need. It is the default choice for storing analytical tables.

**ORC** implements the same ideas as Parquet — columnar layout, encodings, statistics — with different vocabulary (its row groups are called stripes) and lighter-weight indexes. It came from the Hive ecosystem. Functionally it is a sibling of Parquet, and the surrounding ecosystem (Hive, Trino, or Presto shops) usually decides between them.

**Protobuf** encodes one message at a time as compact binary, with the message structure defined in a separate `.proto` file. Unlike Avro, nothing is stored inside the data itself — the definition is external. It also has no file container: to write many messages to a stream or file you must add your own boundary, such as a length prefix before each message. That is what "per-message framing needed" means. Protobuf dominates RPC systems (it is the language of gRPC) and is common inside Kafka at Google-style shops.

### Row-wise binary with schemas: Avro

An Avro file is a sequence of blocks. The file header contains the full schema as JSON. Between blocks the writer inserts a **sync marker** — a unique value that appears nowhere in the data. A reader told to start at some offset in the middle of the file simply scans forward until it finds the next sync marker and starts decoding there. Because each block is compressed independently, this works even when the whole file is compressed. That is why Avro is splittable where a gzipped CSV is not.

The real superpower is schema evolution. Every Avro value was written with some writer schema, and every program reads with its own reader schema — and the two do not have to be identical. Avro resolves the difference by field *name*, not by position. Suppose version 1 of an event schema has fields `id` and `name`, and version 2 adds `country` with a default value of an empty string:

- A new reader (v2) reading old data looks for `country`, does not find it, and uses the default `""`.
- An old reader (v1) reading new data looks up the fields it knows by name (`id`, `name`) and simply ignores the extra `country`.

Both directions work, so producers and consumers can upgrade at different times — which is exactly the situation in event streaming, where you can never redeploy every service at once.

Two changes are dangerous. Renaming a field (`name` to `full_name`) breaks resolution, because readers look up the old name and fail. Avro has a mitigation: the new schema can declare an *alias*, telling the resolver that `full_name` is also known as `name`. This works but requires coordination, so many teams avoid renames entirely — they add the new field, write both for a while, then remove the old one in a planned step. The second danger is changing a field's type without a compatible path.

**Protobuf vs Avro** is the other wire-format decision. Protobuf messages are typically **smaller and decode faster**, because each field is written as its permanent field *number* rather than its name **when file is getting transferred over network**. The evolution rule is strict: add new fields with new numbers, and never reuse or renumber old ones, because an old reader silently skips numbers it does not know — but if a number's meaning changed, old data decodes into wrong values. Protobuf also carries a large RPC ecosystem (gRPC). 

Avro's advantage is that the schema travels with the data and resolution is by name, and it is the native citizen of the Kafka and Confluent Schema Registry world. Both are correct choices; pick by ecosystem. 

CSV is not a correct choice for the wire — it has no schema enforcement, no types, and it is bulky text.

### Columnar at rest: Parquet (and ORC)

A Parquet file groups rows into row groups, often a few hundred megabytes each. Inside a row group, each column's values are stored together as a column chunk. Each chunk is encoded and compressed independently, and each chunk carries small statistics: its minimum value, maximum value, and null count.

The encodings matter because they shrink typical data dramatically:

- **Dictionary encoding**: a `status` column with only three distinct values (`active`, `cancelled`, `refunded`) repeated across ten million rows stores a three-entry dictionary plus a tiny integer code per row, instead of the full string per row.
- **Run-length encoding**: a `dt` column sorted by date, where `2026-09-20` appears one million times in a row, stores "this value, repeated one million times" instead of a million copies.
- **Delta encoding**: a sorted numeric column such as timestamps or IDs stores the first value plus the small difference to the next one, instead of full-size numbers.

The statistics are the query-acceleration engine. Consider a query with the filter `WHERE dt = '2026-09-20'`. Before reading anything, the engine reads each chunk's min/max statistics. A chunk whose minimum and maximum are both `2026-09-18` cannot contain the requested date, so the engine skips the entire chunk — it is never read from disk and never decompressed. This is called **predicate pushdown** or **data skipping**: the filter is pushed down to the storage layer and applied before decoding. It is the mechanical reason full-history scans on Parquet tables became affordable.

Parquet also supports nested data — lists and structures — through definition levels and repetition levels. These are two small numbers stored alongside each value that record how deep in the structure the value sits and whether it starts a new list entry. You do not need to work with them directly; the practical takeaway - In parquet, JSON-like payloads can be stored in columnar form and queried by their inner fields, instead of being parsed as raw JSON on every query. This is the main reason Parquet replaced JSON for analytics.

ORC follows the same design — its row groups are called stripes, its footer carries indexes, and its encodings are similar. It came from the Hive world, and shops built around Hive, Trino, or Presto often standardize on it. Functionally it is a sibling of Parquet; the ecosystem decides.

### Splittability — the constraint people forget

A file is splittable if a reader can start reading at an arbitrary offset in the middle and still make sense of what follows. This matters because Spark reads large files by splitting them into pieces and giving one task to each piece. A splittable 200GB file becomes hundreds of tasks running in parallel; a non-splittable 200GB file becomes one task.

**One giant gzip file is not splittable.** A `.gz` file is a single continuous compressed stream: each byte's meaning depends on the compression history before it, so decompression must start from byte 0. The classic trap is landing a 20GB `.csv.gz` in the lake: Spark can create exactly one task for it, so one core decompresses and parses all 20GB while the rest of the cluster watches. A read - that should take minutes, takes hours. Everyone in data engineering hits this once; make sure you hit it in a dev environment.

Two codecs solve splittability in unhelpful ways. **bzip2** is technically splittable because it compresses in independent blocks, but it is so slow that nobody uses it at scale. **LZO** is splittable only when an extra index file has been built alongside it — a legacy Hadoop-era answer.

The robust pattern is one of three things. First, use a container with block-level compression: Avro compresses each block independently and places sync markers between blocks, so any block boundary is a safe starting point. Second, use Parquet or ORC, whose row groups and stripes are natural split points that the file footer points to. Third, simply land data as many reasonably sized files instead of one giant file. All three give Spark hundreds of tasks where gzip gives it one.

### Codecs, honestly

A codec is the compression algorithm that shrinks bytes and expands them back. The permanent trade-off is between ratio (how much smaller the data gets) and speed (how much CPU compressing and decompressing costs).

“No — container use” below means Snappy, Zstd, and LZ4 aren’t typically splittable when used alone, but work well when the file format applies them to separate blocks

| Codec | Speed | Ratio | Splittable on its own | Typical use |
|---|---|---|---|---|
| Snappy | Very fast | Modest | No — designed for use inside containers | Hot data, Spark shuffle |
| Zstd | Fast | Best speed-to-ratio balance | No — container use | Modern default for stored data |
| Gzip | Slow | Good | No — one continuous stream | Small files, legacy systems |
| LZ4 | Very fast | Modest | No — container use | Hot paths, shuffle alternative |

The "**splittable on its own**" column deserves explanation: Snappy, Zstd, and LZ4 compress an input as one stream, so by themselves they have the same problem as gzip. They are safe because the file format does the splitting — Parquet compresses each column chunk separately, Avro compresses each block separately — and then applies the codec to each piece.

The rule of thumb follows from where the bottleneck is. 

* If a job is CPU-bound — hot data read many times, or the shuffle between Spark stages — use the fastest codec, Snappy or LZ4, so CPU is not wasted on decompression. 
* If storage or network is the bottleneck — cold data kept for months — use Zstd, which compresses noticeably smaller at nearly the same speed, saving disk and transfer time. 
* Gzip at scale is the worst of both worlds: slow to decompress and unsplittable. Use it only when a legacy system demands it. One caution: compression benchmarks are workload-dependent, so treat published ratios as rules of thumb, not gospel.

### Schema evolution & the Schema Registry

Compatibility modes describe which directions of schema change are allowed:

| Compatibility mode | Meaning | Safe deployment order |
|---|---|---|
| Backward | A new schema can read data written with the old schema | Consumers first, then producers |
| Forward | An old schema can read data written with the new schema | Producers first, then consumers |
| Full | Both directions hold | Either order, but strictest to satisfy |

Why deployment order follows from the mode: in backward mode, an upgraded (new) consumer can still read messages produced by not-yet-upgraded (old) producers, so the consumer may be deployed first. Forward mode is the mirror image. Full compatibility guarantees both directions, so order does not matter, but the rules on what changes are allowed are the tightest.

The **Schema Registry** pattern (Confluent Schema Registry, AWS Glue Schema Registry, GCP Pub/Sub schemas) turns these rules from good intentions into an enforced gate. The flow: a producer tries to register schema v2 for a topic. The registry compares v2 against the registered v1 according to the topic's compatibility policy. If v2 is incompatible, registration is rejected and the producer fails at deployment or startup time — not at message-send time in production, hours after the change shipped. An incompatible schema fails the deployment, not the 2am consumer. On Kafka specifically, each message then carries only a small schema ID, and the registry provides the full schema, keeping messages compact.

Avro's name-based resolution plus a registry-enforced compatibility policy is what gives streaming pipelines their producer/consumer independence: either side can evolve as long as the policy holds, and mistakes are caught before rollout. This is a **data contract** enforced at the pipeline door — ch21 extends the same idea to SLAs and ownership.

JSON has a schema language (JSON Schema), but it is advisory: nothing on the wire checks it by default, so a producer can emit any shape and consumers discover the break at parse time — or worse, do not discover it at all. That is the logs-chaos story from ch05.

## The Options

| Decision | Options | Sensible default |
|---|---|---|
| Wire format (moving data) | Avro / Protobuf / JSON | Avro or Protobuf with a Schema Registry; JSON only at the edges, where humans or public APIs need it |
| At-rest analytics format | Parquet / ORC | Parquet, unless the surrounding ecosystem (Hive, Trino, Presto) is ORC-first |
| At-rest operational row storage | The store's own engine format (for example RocksDB SST files) | Not your decision — the storage engine owns its file format |
| Compression codec | Snappy / LZ4 / Zstd / gzip | Snappy or LZ4 for hot paths and shuffle; Zstd for stored data |
| Raw/bronze landing format | Avro or JSON exactly as received | Keep the data exactly as it arrived; convert later (ch03/ch06) |

## Decision Rules

- **On the wire, use Avro or Protobuf with a Schema Registry and an explicit compatibility policy.** Moving data needs a schema-carrying binary format so producers and consumers can deploy independently. The registry turns an incompatible schema into a failed deployment instead of a broken consumer. JSON is acceptable only at the edges, where a human or an external public API requires it.
- **At rest, use Parquet (or ORC where the ecosystem dictates).** Columnar layout, per-column encodings, and min/max statistics are the whole game — they are what make analytical queries skip most of the data (ch17).
- **Never land one giant gzip file.** A single `.gz` stream cannot be split, so it cannot be parallelized. Use block-compressed containers (Avro with a codec, Parquet), or land many reasonably sized files.
- **Match the codec to the bottleneck.** Hot paths and shuffle are CPU-bound, so use Snappy or LZ4. Cold, stored data is storage- and network-bound, so use Zstd. Gzip only for compatibility with legacy systems, never at scale.
- **Let schemas evolve by design, not accident.** Prefer additive changes — new fields with defaults — and enforce them through a registry whose compatibility mode matches the team's real deployment order.
- **Land raw data exactly as received.** The raw layer is insurance: if a conversion bug is found later, you can replay from a faithful copy. Convert to analytical formats in the derive step, not at landing (ch03/ch06).
- **Treat file count as part of the format decision.** Target files of roughly 128MB to 1GB (ch19). One giant file cannot be parallelized, and a million tiny files drown the metadata layer in overhead.

## Failure Modes

- **The 20GB `.csv.gz`.** A single gzipped file is one continuous stream, so Spark assigns one task, one core reads all of it, and the stage takes 14 hours while the cluster idles. This is everyone's first big-data scar. Fix: land many files, use a splittable container, or use a block-level codec.
- **Schema drift without a registry.** A producer quietly adds a field to its JSON. A downstream parser either crashes — or worse, stops filling a column and starts writing nulls, and nobody notices for two weeks (the ch05 story). This is a format-governance failure: without an enforced contract at the door, drift is only a matter of time.
- **Position-based evolution.** In formats where meaning comes from position rather than name — a CSV where consumers read column 3, or a Protobuf message where a field number was reused — removing or reordering fields makes old data decode into plausible-looking nonsense. Name-based resolution (Avro) and field-number discipline (Protobuf) exist precisely to prevent this.
- **Gzip at scale.** The storage team celebrates 30% smaller files; the compute team pays 5x CPU to decompress them on every read. The nightly job ends up existing mostly to decompress data it will not even read. Compression that costs more than it saves at the system level is not a win.
- **JSON analytics.** A pipeline stores events as JSON and calls `from_json` in Spark on every single run, re-parsing the same text forever. The parse cost is paid on every query, every day, indefinitely. Convert to Parquet once at landing and the cost is paid exactly once.
- **Tiny-file explosion.** Over-partitioning by `dt/hour/minute` at low volume produces millions of kilobyte-scale files. Each file carries metadata, and the cost of listing, planning, and opening files swamps the cost of the data itself. ch19 covers the cure (sizing targets and compaction); the disease starts with the format and layout decision made here.

## Interview Narration

“I split the format decision in two: data at rest and data on the wire. On the wire I want a schema-carrying binary format — Avro, or Protobuf where the ecosystem points that way. The real requirement is that producers and consumers can deploy independently, and that is exactly what Avro's name-based schema evolution plus a Schema Registry buys. Additive changes with defaults pass the registry's compatibility check, so an incompatible schema fails the deployment, not the consumers at 2am. JSON stays at the human edges.

At rest I choose Parquet, and the reason is mechanical. The columnar layout means a query reads only the columns it references. Dictionary and run-length encoding collapse repetitive columns cheaply. And each column chunk carries min/max statistics, so the engine can skip chunks that cannot match the filter before decoding anything — that is predicate pushdown, and it is why full-history scans became affordable.

The constraint people forget is splittability. One giant gzip file is a single deflate stream, so it becomes exactly one Spark task on one core no matter how large the cluster is. The robust answers are block-compressed containers like Avro with a codec, Parquet's row groups, or simply many reasonably sized files. Compression follows the bottleneck: Snappy or LZ4 on hot paths and shuffle where CPU is the constraint, Zstd for cold storage where disk and network are, and gzip never at scale.

I would close with file sizing as part of the same decision. I target files in the hundreds of megabytes, because one giant file cannot be parallelized and a million tiny files drown the metadata layer. The format choice and the file layout choice are really the same decision.”

---

| <- Previous | Next -> |
|---|---|
| [Chapter 17 — Row vs Columnar](ch17-row-vs-columnar.md) | [Chapter 19 — Spark in Practice](ch19-spark-in-practice.md) |
