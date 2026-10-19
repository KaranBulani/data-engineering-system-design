# Chapter 20 — Security, Governance & Catalogs

> Part VII — Security & Operations

Security is where senior candidates either earn quiet bonus points by bringing it up on their own, or lose loud points by mentioning it and then being unable to answer the follow-up questions they invited. The good news is that the material is not deep cryptography. It is a handful of correct reflexes — encryption, identity, network isolation, PII handling — plus the governance layer (catalogs, lineage, data contracts) that makes a data platform governable at all. "Governable" means anyone can answer two basic questions: what data do we have, and where did this number come from?

## The Question

How is this data encrypted, who is allowed to read which parts of it, how does data move safely between environments, and how does anyone find out what data exists and where it came from?

## The Physics

### Encryption: the two places

Data can be intercepted in two situations: while it is moving between systems, and while it is sitting on disk. Each situation has its own defense.

| | In transit | At rest |
|---|---|---|
| What it protects | Data moving over the network | Data stored on disk |
| Mechanism | TLS on every connection | Storage-level encryption |
| Default | Managed by the platform | Keys managed by the cloud provider |
| Senior move | **mTLS between services** | **Customer-managed keys (CMK/BYOK)** |
| Why it matters here | Pipelines hop between networks constantly | Compliance requirements and the ability to revoke access |

- **In transit** means every network connection carries its data encrypted with TLS — the same protocol that turns `http` into `https`. This is table stakes, meaning it is expected as a minimum, not a differentiator. Two upgrades are worth mentioning.
  -  The first is **mutual TLS (mTLS)** for service-to-service traffic: with ordinary TLS, only the client verifies the server's identity, while with mTLS both sides present certificates, so a server can prove it is talking to a legitimate service and not an impostor. 
  - The second is remembering the forgotten hops: the JDBC connection from Spark or a BI tool to a database (ch04/ch16 — check the `sslmode` parameter), the connection between Kafka clients and brokers, and CDC connectors reading a database's write-ahead log over the network. Data is in transit on every one of these links, and each one needs TLS, not just the "main" pipeline.
- **At rest** means the bytes on disk are encrypted by the storage service, using keys the provider manages by default. That default is fine for most data. **Customer-managed keys (CMK, sometimes called BYOK — bring your own key)** matter when compliance requires you to prove that your organization controls the encryption keys, or when you need the right to revoke: deleting the key is the nuclear option that makes the data unreadable to everyone, including the cloud provider. Revocation breaks every reader, so it is reserved for genuinely regulated scopes. The interview-ready line: "CMK for regulated scopes, provider-managed keys otherwise."

### Authn/authz: who and what

Two words that are easy to mix up: **authentication** (authn) answers "who are you?", and **authorization** (authz) answers "what are you allowed to do?". A platform needs a story for both, for two kinds of users: humans and services.

- **Humans** should log in through SSO using OIDC — a standard protocol where one central identity provider (Okta, Entra ID, and similar) vouches for who you are. Each employee logs in once, and membership in groups (for example, `data-analysts`) maps to roles on the data platform. The benefit is one place to grant and revoke access, and no per-tool passwords. A separate password sitting on a data platform is a finding — meaning an auditor writes it up as a defect — not a design.
- **Services** (pipelines, schedulers, connectors) authenticate as service accounts — identities that belong to a program rather than a person. The important property is **short-lived credentials**: tokens that expire after minutes or hours and are re-issued automatically through token exchange or the cloud's workload identity feature, which lets a running service prove what it is without any stored secret. The anti-pattern is the eternal service account whose key was created in 2021 and rotated never: if that key leaks, it is valid forever.
- **The authorization model** should be role-based at minimum: roles (analyst, engineer, steward) bundle permissions, and users receive roles instead of individual grants. In warehouses and lakehouses the platform-native version is **RBAC plus grants on objects** — for example, `GRANT SELECT ON sales.orders TO ROLE analyst`. At larger scale this evolves toward attribute- or policy-based access, where the decision considers attributes of the requester and the data: "the user is an analyst AND is PII-cleared AND the row's region is EU".
- The JDBC/ODBC tie-in (ch16): authentication is part of the connection itself. Humans connect through SSO flows; BI tools connect as service accounts using key-pair or OAuth credentials. A shared dashboard account — one login used by 200 analysts — destroys both audit (you cannot tell which human ran a query) and chargeback (you cannot attribute query costs to a team).

### Network isolation

- **VPC peering, private endpoints, and PrivateLink** are three mechanisms for the same goal: pipelines and consumers reach the data stores over private network paths instead of the public internet, and outbound traffic (egress) is controlled and monitored.
- Why data platforms care about this more than typical applications: (a) **egress cost** — cloud providers charge per gigabyte for data leaving a region, so a naive design that copies data across regions on every pipeline run multiplies the bill; (b) **exfiltration surface** — a warehouse with a public endpoint and one leaked credential is a data breach with a SQL interface; if there is no public endpoint, a leaked credential alone cannot pull the data out; (c) **data residency** — the legal requirement that EU data stays in EU regions can be expressed directly as network topology: the routes simply do not exist.

### PII: the special cargo

PII (personally identifiable information) is data that can identify a person — names, emails, national IDs — and it gets a layered toolkit, ordered from weakest to strongest:

1. **Classification first.** You cannot protect what you have not labeled. Tag columns in the catalog (see below) as PII, PCI (payment card data), or PHI (health data). Every later control — column grants, masking, tokenization — keys off these tags.
2. **Column-level security** grants access to some columns and not others. For example, analysts get `SELECT` on a view that contains everything except the `ssn` and `email` columns.
3. **Row-level security** filters rows by who is asking. A policy says "analysts in the EU region see only rows where region = EU" — the same table, different rows per caller.
4. **Dynamic data masking** leaves the value in place but masks it at query time for unauthorized readers: `email` returns as `j***@x.com`, while the authorized path sees the plaintext. The data is protected per query, without a second copy.
5. **Tokenization/pseudonymization at ingestion** replaces the identifier with a meaningless token *before* the data is widely distributed; the mapping between tokens and real values lives in a separate vault. This is the strongest pattern for pipelines: the raw plane holds only tokens, and re-identification happens through a separate, audited gate.
6. **Hashing** serves one specific purpose: join-without-reveal. Two systems can hash the same email addresses and match on the hashes to correlate records without either side sharing the actual emails. A hash is not encryption — it has no key and cannot be reversed — so say the word "one-way" when you propose it.

A GDPR-flavored design tension worth naming: **erasure versus the immutable raw plane** (ch03/ch06). Immutability — never rewriting landed data — is what makes your replay and backfill guarantees possible; the GDPR right to erasure demands that a person's data can be deleted. These two goals pull against each other, and mature designs pick a mechanism: 

- tokenized raw (deleting the token mapping renders the archived data effectively erased, because it can no longer be tied to a person), 
- crypto-shredding (encrypt each subject's data with its own key; deleting that key makes the data permanently unreadable), 
- or purpose-scoped retention policies (delete after the legal retention period). Naming this tension unprompted is a strong senior signal.

### Catalogs: "what data exists"

The layer most architectures forget is a **catalog**: a searchable inventory of every dataset, with its schema, its owner, its tags (including PII tags), and — for lakehouse tables — the **atomic commit pointer** (ch12). The commit pointer is the record of which version of a table is currently valid: when a writer finishes adding data, the catalog flips the pointer, and every reader switches to the new version atomically, with no partial reads in between.

The evolution, one step at a time: the **Hive Metastore** was the origin — it mapped table names to directories. **AWS Glue** is the managed version of the same idea. The current generation — **Unity Catalog, Polaris, Nessie** — is governance-first: one permission model enforced across every engine, lineage, tagging, and credential passthrough (engines run queries with the querying user's own credentials instead of a shared service login). The strategic point: once several engines — Spark, Trino, Snowflake, Flink — read the same lakehouse, the catalog becomes the shared source of table truth and the governance chokepoint, meaning every permission check and every table lookup flows through it. Who owns the catalog is therefore an organization-design decision disguised as a tooling decision.

### Lineage: "where did this come from"

- **Table-level lineage** answers "this dashboard is built from these tables, which were built from these topics". It is now table stakes — included by default — in modern catalogs and orchestrators.
- **Column-level lineage** is the expensive, high-value version: it traces that the `revenue` figure descends from `orders.amount_charged` minus `refunds.amount`. It is the difference between "blame the pipeline" and "this number is wrong because upstream changed what a column means".
- Lineage is what makes **impact analysis** possible: before renaming a column, you can see every downstream consumer that will break. It also serves as the detection layer for the reconciliation problem of ch15 — knowing where a number came from is how you check whether it is still consistent with its source.

### Data contracts: the API-ification of datasets

A data contract treats a dataset like an API with a published interface. The contract bundles: a named owner, a schema, a compatibility policy (enforced by the registry mechanisms of ch18), SLAs for freshness and completeness (ch21), and semantic definitions — what the columns actually mean (ch16's semantic layer). The contract is the interface between the producing team and the consuming teams: when the producer needs a breaking change, that becomes a versioned negotiation, not a surprise that lands at 2am in someone's pager. This is the cultural heart of "data as a product": a product has an owner, a documented interface, and a promise attached.

## The Options

| Layer | Options | Default posture |
|---|---|---|
| Encryption | TLS / mTLS; provider-managed keys vs CMK | TLS everywhere; CMK for regulated scopes |
| Identity | Passwords / static keys / SSO + workload identity | SSO for humans, short-lived credentials for services |
| Authorization | RBAC / attribute-based / row+column policies / masking | RBAC with column grants; policies as scale demands |
| PII | Masking / tokenization / hashing | Classify first, tokenize at landing, mask at serving |
| Network | Public endpoints / peering / private endpoints | Private endpoints; egress controlled and monitored |
| Catalog | Hive Metastore / Glue / Unity / Polaris / Nessie | One governance-grade catalog for everything |
| Contracts | None / schemas only / full contracts | Registry-enforced schema + owner + SLA |

## Decision Rules

- **Volunteer the basics unprompted.** TLS in transit, encryption at rest, least-privilege grants, private networking — say them in thirty seconds, then get on with the design. Interviewers notice who offers them and who has to be asked.
- **Classify before you protect.** PII tagging at the catalog level is the precondition for everything downstream: column grants, row policies, and masking all key off the tags.
- **Tokenize regulated identifiers at ingestion.** The raw plane holds only tokens, so wide distribution never carries real identifiers, and re-identification is a separate, audited gate.
- **Name the erasure-versus-immutability tension** and pick a mechanism — tokenized raw, crypto-shredding, or a retention policy — rather than pretending the conflict does not exist.
  - immutable storage (such as append-only logs, backups, or object-locked data) is designed not to change, while privacy rules or company policy may require a person’s data to be erased.
  - So the design should explain how it handles that conflict. For example:
  - Tokenized raw data: Store a token instead of a person’s identifier; delete the separate mapping to break the link.
  - Crypto-shredding: Encrypt the person’s data with a dedicated key; destroy the key so the stored ciphertext can no longer be read.
  - Retention policy: Keep data only for a defined period, then delete it from active storage and account for replicas and backups.

- **Run one governance-grade catalog** as the single source of table truth and the permission surface across all engines. Owning it is a platform-level decision, not a team-level one.
- **Build lineage for impact analysis**, and pay for column-level lineage where the budget allows — it is the version that answers "why is this number wrong".
- **Sign contracts at team boundaries**: schema + compatibility policy + freshness SLA + named owner, enforced at the registry and deployment door (ch18), not written in a wiki nobody checks.

## Failure Modes

- **The public warehouse.** The warehouse endpoint is reachable from the public internet, and one phished credential away from a headline. The symptom is the security review that ends the architecture review: the design is rejected before any pipeline discussion begins. Private endpoints are the fix.
- **The shared BI account** (ch16's failure, now a security event). Two hundred analysts share one dashboard login, so there is no audit trail per human. When a breach investigation starts, the first question — "which of these 200 people pulled the data?" — has no answer. Per-human identity through SSO is the fix.
- **The eternal service account.** A pipeline credential created in 2021, never rotated, and over-granted because "admin was easier". It is the quiet over-privilege standard of the industry, and it turns any single leak into a permanent breach. Short-lived workload credentials are the fix.
- **Masking theater.** Masking is configured in the BI tool only, while the warehouse grants allow full `SELECT`. Anyone who opens a SQL client instead of the dashboard reads plaintext. Masking must live at the enforcement layer — the warehouse or lakehouse that holds the data — not at the presentation layer, which anyone can route around.
- **Catalog sprawl.** Some tables are in Glue, others in Unity, and the real inventory of "important tables" lives in a spreadsheet. Two catalogs means no catalog: neither is complete, so neither can be the source of truth, and every permission question reopens the argument.
- **Unowned data products.** Datasets without contracts or owners that other teams silently depend on. Upstream renames a field, downstream columns go null, and the ch18 null-column incident returns — this time with a governance diagnosis: nobody owned the interface, so nobody was obligated to negotiate the change.

## Interview Narration

“I volunteer the security posture early because it shapes the whole design. Encryption in transit on every hop, including the forgotten ones like JDBC connections and CDC connectors; mTLS between services; encryption at rest with customer-managed keys for the regulated scopes and provider-managed keys elsewhere. For identity, humans come in through SSO and services use short-lived workload credentials — and no shared BI accounts, because they destroy both audit and chargeback. For the network, private endpoints rather than public ones, since egress control is both a security surface and a cost multiplier.

If there is PII — and in this case there is — I classify at the catalog level first, then tokenize identifiers at ingestion so the raw plane only ever holds tokens, with re-identification as a separate audited gate. On the serving side, column grants and dynamic masking enforced at the warehouse layer, because masking in the BI tool is presentation, not enforcement — anyone with SQL access routes around it. And I name the honest tension out loud: GDPR erasure versus the immutable raw archive that my replay guarantees depend on. Tokenized raw plus crypto-shredding is how you honor both.

For governance, one governance-grade catalog as the single source of table truth across all engines — it holds the atomic commit pointer for lakehouse tables and the permission model — plus lineage for impact analysis, and data contracts at team boundaries: schema, compatibility policy, freshness SLA, and a named owner. The contract is what turns upstream schema changes from surprises into negotiations.”

---

| <- Previous | Next -> |
|---|---|
| [Chapter 19 — Spark in Practice](ch19-spark-in-practice.md) | [Chapter 21 — Orchestration, Data Quality & Operations](ch21-orchestration-data-quality-and-operations.md) |
