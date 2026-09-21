# Chapter 20 — Security, Governance & Catalogs

> Part VII — Security & Operations

Security is where senior candidates either score quiet bonus points by
volunteering it, or lose loud points by being unable to answer the follow-up
they invited. The material is not deep crypto — it's a handful of correct
reflexes, plus the governance layer (catalogs, lineage, contracts) that makes
a platform governable at all.

## The Question

*"How is this data encrypted, who can read which parts of it, how does it
move between environments, and how does anyone know what data exists and
where it came from?"*

## The Physics

### Encryption: the two places

| | In transit | At rest |
|---|---|---|
| What | TLS on every hop | storage-level encryption |
| Default | platform-managed | provider-managed keys |
| Senior move | **mTLS service-to-service** | **customer-managed keys (CMK/BYOK)** |
| Why it matters | your pipelines hop networks constantly | compliance boundaries + revocation |

- **In transit**: TLS is table stakes; the interview-relevant upgrade is
  mutual TLS for service-to-service pipelines (both sides authenticate), and
  remembering the forgotten hops — JDBC connections (ch04/16, `sslmode`
  params), Kafka client links, CDC connectors reading WAL over the network.
- **At rest**: provider-managed keys are the default and fine for most data.
  **CMK** matters when compliance requires proof of key control or the right
  to revoke — revoking the key is the nuclear option that renders the data
  unreadable even to the provider. Say "CMK for regulated scopes,
  provider-managed otherwise" and move on.

### Authn/authz: who and what

- **Humans**: SSO (OIDC) — one identity provider, groups mapped to roles.
  Passwords on data platforms are a finding, not a design.
- **Services**: service accounts with **short-lived credentials** (token
  exchange, workload identity) — the anti-pattern being the eternal service
  account whose key rotates never.
- **Authorization model**: role-based at minimum; the platform-native
  version in warehouses/lakehouses is **RBAC + grants on objects**, evolving
  toward attribute/policy-based for scale ("analyst AND PII-cleared AND
  region=EU").
- The JDBC/ODBC tie-in (ch16): auth is part of the connection — SSO flows
  for humans, keypair/OAuth for BI service accounts; shared dashboard
  accounts destroy both audit and chargeback.

### Network isolation

- **VPC peering / private endpoints / PrivateLink**: pipelines and consumers
  reach data stores without public internet; egress is controlled and
  monitored.
- Why data platforms specifically care: (a) **egress cost** — naive
  cross-region pipeline designs are a bill multiplier; (b) **exfiltration
  surface** — a warehouse with a public endpoint and a leaked credential is
  a data breach with a SQL interface; (c) **data residency** — EU data
  staying in EU regions is a legal constraint expressible as network
  topology.

### PII: the special cargo

The layered toolkit, in order of strength:

1. **Classification first**: you cannot protect what you haven't labeled.
   Tag columns (catalog-level — see below) as PII/PCI/PHI.
2. **Column-level security**: grants that expose some columns and not
   others (`SELECT` on non-PII view only).
3. **Row-level security**: policies that filter rows by role/region
   ("analysts in EU see EU rows").
4. **Dynamic data masking**: PII present but masked to unauthorized readers
   (`email` -> `j***@x.com`); the authorized path sees plaintext.
5. **Tokenization/pseudonymization at ingestion**: replace the identifier
   with a token *before* wide distribution; the mapping lives in a vault.
   Strongest for pipelines: raw plane holds tokens; the re-identification
   path is a separate, audited gate.
6. **Hashing**: for join-without-reveal (hash the email in two systems to
  correlate without sharing the email) — not encryption, no key, not
  reversible; say the word "one-way" when you say it.

GDPR-flavored realism: **erasure versus the immutable raw plane** (ch03/06)
is a genuine design tension — immutability is your replay guarantee;
erasure is a legal right. The production answers: tokenized raw (delete the
token mapping = effectively erased), crypto-shredding (delete the key for
that subject's data), or purpose-scoped retention policies. Naming this
tension unprompted is a strong senior signal.

### Catalogs: "what data exists"

The missing layer of most architectures: a **catalog** — inventory, schemas,
ownership, tags, and the **atomic commit pointer** for lakehouse tables
(ch12).

Evolution, one line each: **Hive Metastore** (the origin: tables =
directories) -> **AWS Glue** (managed metastore) -> **Unity Catalog /
Polaris / Nessie** (governance-first catalogs: unified permissions across
engines, lineage, tagging, credential passthrough). The strategic point:
when multiple engines (Spark, Trino, Snowflake, Flink) read one lakehouse,
**the catalog is the shared source of table truth and the governance
chokepoint** — who owns it is an org-design decision disguised as a tooling
decision.

### Lineage: "where did this come from"

- **Table-level** (this dashboard <- these tables <- these topics) is now
  table-stakes in modern catalogs/orchestrators.
- **Column-level** ("this `revenue` figure descends from
  `orders.amount_charged` minus `refunds.amount`") is the expensive,
  high-value version — the difference between "blame the pipeline" and
  "this number is wrong because upstream changed a column's meaning."
- Lineage is what makes **impact analysis** possible: before renaming a
  column, see every downstream consumer. It's also the detection layer for
  the ch15 reconciliation problem.

### Data contracts: the API-ification of datasets

A dataset with an owner, a schema, compatibility policy (ch18's registry
enforcement), SLAs (freshness, completeness — ch21), and semantic
definitions (ch16's semantic layer). The contract is the *interface* between
producing and consuming teams: breaking it is a versioned negotiation, not a
surprise. This is the cultural heart of "data as a product."

## The Options

| Layer | Options | Default posture |
|---|---|---|
| Encryption | TLS/mTLS; provider vs CMK | TLS everywhere; CMK for regulated |
| Identity | passwords / keys / SSO+workload identity | SSO humans, short-lived services |
| Authorization | RBAC / ABAC / row+column policies / masking | RBAC + column grants; policies at scale |
| PII | masking / tokenization / hashing | classify -> tokenized landing -> masked serving |
| Network | public / peering / private endpoints | private; egress controlled |
| Catalog | HMS / Glue / Unity / Polaris / Nessie | one governance-grade catalog |
| Contracts | none / schemas / full contracts | registry-enforced schema + owner + SLA |

## Decision Rules

- **Volunteer the basics unprompted**: TLS in transit, encryption at rest,
  least-privilege grants, private networking — thirty seconds, then design.
- **Classify before you protect**: PII tagging at catalog level is the
  precondition for column/row security and masking.
- **Tokenize at ingestion for regulated identifiers**; the raw plane holds
  tokens; re-identification is a separate audited gate.
- **Name the erasure-vs-immutability tension** and pick a mechanism
  (tokenized raw / crypto-shred / retention policy) rather than pretending
  it away.
- **One governance-grade catalog** as the single table-truth + permission
  surface across engines; owning it is a platform decision.
- **Lineage for impact analysis** — column-level where the budget allows.
- **Contracts at team boundaries** — schema + compatibility + SLA + owner;
  enforce at the registry/deployment door (ch18), not in a wiki.

## Failure Modes

- **The public warehouse**: endpoint reachable from the internet, one phished
  credential from a headline. Symptom: the security review that ends the
  architecture review.
- **Shared BI account** (ch16's failure, now a security event): no audit
  trail per human; the breach investigation starts with "which of 200
  analysts had the password?"
- **Eternal service account**: pipeline credential from 2021, never rotated,
  over-granted ("admin was easier"). The quiet over-privilege standard of
  the industry.
- **Masking theater**: masking applied in the BI tool but not the warehouse
  grants — anyone with SQL access reads plaintext. Masking must live at the
  enforcement layer, not the presentation layer.
- **Catalog sprawl**: Glue for some tables, Unity for others, a spreadsheet
  of "important tables" as the real inventory. Two catalogs means no catalog.
- **Unowned data products**: contract-less datasets that other teams depend
  on; upstream renames a field; the ch18 null-column incident now has a
  governance diagnosis.

## Interview Narration

"I'll volunteer the security posture early because it shapes the design:
TLS everywhere including every JDBC and connector hop, mTLS for
service-to-service pipelines, encryption at rest with customer-managed keys
for regulated scopes. Identity-wise, humans through SSO and services through
short-lived credentials — no shared BI accounts, because they destroy both
audit and chargeback. Network-wise, private endpoints over public ones:
egress control is both a security surface and a cost multiplier.

If there's PII — and in this case there is — I'd classify at the catalog
level first, then tokenize identifiers at ingestion so the raw plane only
ever holds tokens, with re-identification as a separate audited gate.
Serving-side, column grants and dynamic masking at the warehouse layer —
masking in the BI tool is presentation, not enforcement. And I'd name the
honest tension: GDPR erasure versus the immutable raw archive that my replay
guarantees depend on — tokenized raw plus crypto-shredding is how you honor
both.

For governance, one governance-grade catalog as the single source of table
truth across engines — it owns the atomic commit pointer for lakehouse
tables and the permission model — plus lineage for impact analysis, and data
contracts at team boundaries: schema, compatibility policy, freshness SLA,
and a named owner. The contract is what turns upstream schema changes from
surprises into negotiations."

---

| <- Previous | Next -> |
|---|---|
| [Chapter 19 — Spark in Practice](ch19-spark-in-practice.md) | [Chapter 21 — Orchestration, Data Quality & Operations](ch21-orchestration-data-quality-and-operations.md) |