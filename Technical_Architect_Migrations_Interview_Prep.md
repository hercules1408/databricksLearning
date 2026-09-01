# Technical Architect – Migrations: Interview Prep Guide

> **Note on approach:** Since the role explicitly states Snowflake-specific knowledge is optional, this guide is built primarily around **generic, transferable skills** of a Migrations Technical Architect — data platform migration methodology, cloud architecture, data engineering fundamentals, and stakeholder leadership — with a dedicated Snowflake-specific section woven in for the migration-approach topics, plus a light vocabulary section at the end.

---

## 1. How to Use This Document

For each topic below:
1. Read the concept summary and the pros/cons.
2. Be ready to give a **real example from your experience** (STAR format: Situation, Task, Action, Result).
3. Anticipate the "why" and "trade-off" follow-up questions — architects are tested on judgment, not just knowledge. Every technique below has a cost; naming the cost unprompted is what separates a senior answer from a junior one.

---

## 2. Understand the Role First

A "Technical Architect – Migrations" is typically responsible for:
- Assessing a customer's/organization's **current (legacy) data platform** (on-prem DW, Teradata, Netezza, Oracle Exadata, Hadoop, Redshift, Synapse, BigQuery, etc.)
- Designing the **target architecture** and migration approach
- Leading **discovery, assessment, planning, execution, and validation** phases
- Acting as a **technical bridge** between sales/pre-sales, engineering, and the customer's stakeholders
- De-risking migrations: performance, cost, security, data integrity, cutover
- Often pre-sales adjacent: producing SOWs, migration roadmaps, effort estimates, architecture diagrams

**Prep tip:** Read the actual job description again and highlight every noun (tools, methodologies, industries) and verb (design, lead, assess, evangelize). Map each to a story from your background.

---

## 3. Core Topic Areas to Study

### 3.1 Data Migration Methodology (Highest Priority)

This is the heart of the role. Be fluent in:

**Migration lifecycle phases:**
1. Discovery & Assessment (inventory workloads, data volumes, dependencies, SLAs)
2. Strategy & Planning (approach selection, sequencing, risk register)
3. Architecture & Design (target state, mapping, non-functional requirements)
4. Build/Migrate (schema conversion, ETL/ELT rebuild, code conversion)
5. Validation & Testing (data parity, reconciliation, performance testing)
6. Cutover & Hypercare (go-live, rollback plan, post-migration support)
7. Decommission of legacy systems

**Migration strategies (the "R"s)** — know these cold, they get asked often:

| Strategy | What it means | Pros | Cons |
|---|---|---|---|
| **Rehost** ("lift and shift") | Move workload as-is to new infra with minimal changes | Fastest, lowest initial risk, minimal retraining | Doesn't leverage cloud-native features; can carry legacy inefficiencies forward; "cloud-washing" the same cost problems |
| **Replatform** ("lift, tinker, and shift") | Move with some optimization (e.g., swap DB engine, minor code changes) | Balance of speed and benefit; some cost/performance gains | Still not fully cloud-native; can create hybrid tech debt if scope creeps |
| **Refactor / Re-architect** | Redesign for cloud-native patterns (elastic compute, ELT, microservices) | Maximum long-term benefit: cost, performance, scalability | Highest cost, time, and risk; requires deep source-system knowledge; longer time-to-value |
| **Repurchase** | Move to a new SaaS/COTS product instead of migrating the platform | Offloads maintenance; often faster for standard use cases | Loss of customization; vendor lock-in; data migration/integration still required |
| **Retire** | Decommission the workload — no migration needed | Removes cost and risk entirely | Requires confidence that nothing depends on it; often skipped due to fear of unknown dependencies |
| **Retain** | Keep as-is (hybrid state) | Avoids unnecessary risk for low-value/short-lived systems | Prolongs technical debt; can complicate the target architecture's "single source of truth" story |

**Migration patterns specific to data platforms:**

| Pattern | Description | Pros | Cons |
|---|---|---|---|
| **Big-bang cutover** | Migrate everything, then switch over in one event | Simple to plan; short overall project timeline; no need to maintain two systems long-term | High risk concentrated in one event; hard to roll back cleanly; requires a full outage/freeze window |
| **Phased/incremental migration** | Migrate subject areas, business units, or workloads one at a time | Lower risk per phase; learnings from phase 1 improve later phases; stakeholders see incremental value | Longer overall timeline; must maintain interoperability between migrated and non-migrated parts; more coordination overhead |
| **Strangler-fig pattern** | Incrementally replace legacy components behind a routing/facade layer until nothing legacy remains | Very low risk; continuous delivery of value; natural rollback (route back to legacy) | Requires investment in a routing/abstraction layer; can extend timeline significantly; complexity of running two paradigms simultaneously |
| **Dual-write / dual-run** | Write to both old and new systems during transition | Keeps both systems live and comparable in real time | Added latency/complexity in write path; risk of divergence if one write fails and isn't retried on both; doubles operational load temporarily |

**Data validation & reconciliation approaches:**

| Approach | Pros | Cons |
|---|---|---|
| Full reconciliation (100% row/column compare) | Highest confidence, catches everything | Expensive in compute/time for very large datasets |
| Sampling-based reconciliation | Fast, cheap, good for continuous checks | Can miss edge-case discrepancies; false sense of confidence if sample isn't representative |
| Checksum/hash-based comparison | Efficient way to compare large datasets without moving all data | Doesn't tell you *what* differs, only *that* it differs — needs a drill-down step |
| Business-outcome validation (comparing reports/KPIs) | Validates what actually matters to the business, catches logic errors that row counts miss | Slower to set up; requires business user involvement, not just engineering |

**Common migration challenges to have a story for:**
- Handling massive historical data volumes within time/cost constraints
- Managing hundreds/thousands of dependent ETL jobs and reports
- Business logic hidden in legacy stored procedures
- Minimizing downtime for 24/7 systems
- Data quality issues surfaced only during migration
- Change management / user adoption resistance

**Sample questions:**
- "Walk me through how you'd approach migrating a 50TB on-prem Teradata warehouse to a cloud data platform with near-zero downtime."
- "How do you decide between rehost vs. re-architect for a given workload?"
- "How do you validate that a migrated dataset is 100% accurate?"
- "Tell me about the most difficult migration you've led and what went wrong."

---

### 3.2 Deep Dive: Zero-Downtime Migration

The core idea: **the old system keeps serving all reads/writes while the new system is built, populated, and validated in the background**, and cutover is a near-instant switch rather than a scheduled outage.

**Typical mechanism, step by step:**

1. **Bulk historical load** — Move all existing historical data from source to target once, using snapshot/full-extract methods. This can take hours/days for large volumes, but happens while the source system stays live and untouched.
2. **Change Data Capture (CDC) begins** — From the moment the bulk-extract snapshot was taken, every subsequent insert/update/delete on the source is captured continuously (log-based CDC, database triggers, or timestamp-based polling) and streamed to the target.
3. **Target stays in near-real-time sync** — The target keeps catching up to the source continuously, so the "gap" between the two systems shrinks to seconds/minutes.
4. **Continuous validation** — Row counts, checksums, and business-critical report reconciliation run continuously against both systems while both are live.
5. **Tiny cutover window** — When confidence is high, a very brief freeze (seconds to a couple of minutes): stop writes to source, let the last bit of CDC drain to target, redirect application/BI connections to target, resume writes — now on target.
6. **Fallback plan** — Keep the source running in read-only/standby mode for a defined period in case rollback is needed.

**Key enablers:** log-based CDC tools (Debezium, Qlik Replicate/Attunity, Oracle GoldenGate, Fivetran, AWS DMS); idempotent writes on the target so replayed/duplicate CDC events don't corrupt data; a connection-routing/feature-flag layer so traffic can flip without redeploying apps; a reconciliation service running continuously, not just at the end.

**Pros:**
- Minimal to no business disruption — critical for 24/7 systems (finance, e-commerce, healthcare)
- Reduces the "point of maximum risk" from hours/days down to seconds/minutes
- Allows extensive validation before commitment, since both systems run in parallel
- Rollback is cheap and fast (source is still warm/live)

**Cons / trade-offs:**
- Significantly more engineering complexity than a scheduled-outage migration — CDC pipelines, idempotency, routing layers all need to be built and tested
- CDC tooling licensing/infrastructure cost adds up, especially for high-volume, high-change-rate sources
- Debugging data drift between two live systems is harder than debugging a single offline batch load
- Requires the source system to expose usable change logs (not all legacy databases support log-based CDC well — some force you into slower trigger-based or timestamp-based CDC, which can miss hard deletes or be less real-time)
- Extended dual-running period increases total infrastructure cost during transition

**When to use it:** high-availability systems where downtime has real business/financial cost, and where the team has (or can build) the CDC/engineering maturity to support it. For a low-traffic internal reporting system, this level of complexity is often overkill — a scheduled maintenance-window migration may be far cheaper and simpler.

---

### 3.3 Deep Dive: Parallel Run (Dual-Run)

This is primarily a **risk-reduction strategy**, not a single technical mechanism — you run old and new systems side-by-side for a period, feeding them comparable data, and compare outputs before fully trusting the new one.

**Two common flavors:**

**A. Dual-write** — Application/ETL writes to both old and new systems simultaneously.
- **Pros:** Both systems stay in sync in real time; no separate CDC pipeline required if you control the write path
- **Cons:** Adds latency and complexity to the write path; risk of divergence if one write succeeds and the other fails without proper retry/compensation logic; doubles the operational burden on every write-owning application, which can be hard to enforce across many upstream systems

**B. Shadow/parallel processing** — The new system independently processes the same inputs (via CDC replay, or the same upstream data feeds), and its outputs are compared to the legacy system's outputs — but legacy remains the sole system of record serving downstream consumers.
- **Pros:** Zero risk to production since the new system's output is only compared, never depended upon; safest option for regulated/high-stakes systems
- **Cons:** Requires fully replicating business logic (transformations, aggregations) in the new environment just to produce a comparable output — this duplicate-build effort is often underestimated; doesn't test the *actual* cutover mechanics (routing, failover), only the data/logic correctness

**Typical validation cycle during parallel run:**
- Run both systems for a full business cycle (e.g., a month-end close, a quarter-end) to catch edge cases like month-end batch jobs, year-end logic, seasonal spikes
- Compare key reports/dashboards, not just raw tables — legacy business logic hidden in stored procedures often surfaces as discrepancies here, not in row counts
- Get **business user sign-off**, not just engineering validation — a report that matches row counts but not what finance expects is still a failed migration

**Pros of parallel run generally:**
- Highest possible confidence before full cutover — real production data, real timing, real edge cases
- Builds stakeholder trust because business users see the new system "prove itself" before depending on it
- Surfaces subtle logic bugs (rounding, timezone handling, currency conversion, null-handling differences) that unit tests rarely catch

**Cons of parallel run generally:**
- Expensive — running two full production systems simultaneously (compute, storage, licensing) for weeks/months
- Extends the overall project timeline substantially
- Requires disciplined governance to keep both systems' inputs truly comparable — if the two diverge in schema or timing, comparisons become noisy and erode trust in the process itself
- Team fatigue — maintaining and triaging discrepancies over a long parallel-run period is operationally taxing

**When to use parallel run vs. a simpler CDC cutover:**
- High-stakes, regulated, or financial systems → parallel run for at least one full business cycle is close to mandatory
- Lower-risk internal analytics → a CDC-based near-zero-downtime cutover without a long parallel run is often sufficient and much cheaper

---

### 3.4 Deep Dive: On-Prem to Cloud Migration

On top of the general migration lifecycle, on-prem→cloud adds specific considerations:

**Network/bandwidth planning** — Moving terabytes/petabytes over the wire is often the real bottleneck.

| Option | Pros | Cons |
|---|---|---|
| Online transfer (direct network copy) | Simple, no extra hardware, good for smaller/ongoing volumes | Bandwidth-constrained; can take impractically long for very large initial loads; competes with production network traffic |
| Offline/physical transfer appliances (AWS Snowball, Azure Data Box, GCP Transfer Appliance) | Fast for very large one-time bulk loads; avoids saturating network | Physical logistics (shipping, security custody); only solves the *initial* bulk load, not ongoing sync; added cost/coordination |
| Hybrid (appliance for bulk + CDC over network for delta) | Combines speed of offline transfer with real-time accuracy of CDC | More moving parts to orchestrate and validate; two different data paths to reconcile |

**Connectivity architecture** — VPN or dedicated interconnect (AWS Direct Connect, Azure ExpressRoute) between on-prem and cloud during the transition.
- **Pros:** Reliable, secure, lower-latency channel for CDC streaming and hybrid access during parallel run
- **Cons:** Dedicated interconnects have lead time to provision (weeks) and ongoing cost; VPN alternatives are cheaper/faster to set up but can bottleneck under high CDC volume

**Security model translation** — On-prem often uses AD/LDAP and network-perimeter security; cloud needs IAM/RBAC redesign, not just a lift-and-shift of permissions.
- **Pros of redesigning properly:** Cloud-native IAM enables finer-grained, auditable access control than most legacy perimeter models
- **Cons/risk:** Doing a literal 1:1 permission mapping (common shortcut) often over- or under-grants access; getting this wrong is a common source of post-migration security incidents

**Compute model shift** — On-prem systems are fixed-capacity (sized for peak); cloud targets are elastic.
- **Pros:** Right-sizing for actual workload patterns instead of peak capacity typically reduces cost significantly
- **Cons:** Requires genuinely understanding workload patterns (not just replicating on-prem hardware specs) — get this wrong and you either overpay or under-provision and hurt performance; this is a common area where inexperienced migration teams underestimate effort

**SQL/dialect and stored procedure conversion** — Legacy platforms (Teradata, Oracle, DB2) have proprietary SQL extensions and stored procedures with embedded business logic.
- **Pros of automated conversion tools:** Speed up the bulk of syntax translation
- **Cons:** Automated tools rarely handle 100% of complex procedural logic; the "last 10-20%" of stored procedures often contain the most business-critical (and least documented) logic, and this is consistently the single largest source of underestimated effort in a migration

**Downstream dependency mapping** — On-prem systems tend to accumulate years of undocumented downstream consumers (reports, scripts, other systems reading directly from tables).
- **Pros of doing this discovery early:** Prevents "surprise breakage" after cutover
- **Cons/challenge:** Genuinely hard to do completely; query log analysis on the source system over a long enough window (ideally a full business cycle) is the most reliable method, but even that can miss quarterly/annual jobs

---

### 3.5 If It's Specifically a Snowflake Migration

Here's how the same three concepts map onto Snowflake, plus pros/cons of the Snowflake-specific mechanisms.

**Zero-downtime / CDC approach with Snowflake as target:**
- Use a CDC tool (Fivetran, HVR, Qlik Replicate, Debezium + Kafka, or native connectors) to stream changes into **staging tables** in Snowflake
- Use **Snowpipe** or **Snowpipe Streaming** for continuous low-latency ingestion of the CDC stream into Snowflake
- Use **Streams and Tasks** (Snowflake's native change-tracking + scheduled/triggered SQL execution) to continuously merge CDC changes from staging into final target tables — Snowflake's built-in equivalent of a micro-batch ELT pipeline
- Because Snowflake **separates storage and compute**, the bulk historical backfill can run on one warehouse while ongoing CDC merge runs on a separate, smaller warehouse — so the initial heavy load doesn't compete with or slow down ongoing sync

| Mechanism | Pros | Cons |
|---|---|---|
| Snowpipe / Snowpipe Streaming | Low operational overhead (serverless ingestion); scales automatically; pay-per-use | Micro-batch (Snowpipe) has some latency (seconds-minutes), not truly sub-second; Snowpipe Streaming reduces this but adds cost/complexity to set up |
| Streams & Tasks for CDC merge | Native to Snowflake, no extra tooling; integrates cleanly with SQL-based transformation logic | Task scheduling granularity and orchestration are more limited than a dedicated orchestrator (e.g., Airflow) for complex dependency chains |
| Separate warehouses for backfill vs. ongoing sync | True workload isolation — no resource contention; easy to size/cost independently | Requires careful warehouse-sizing and monitoring to avoid over-provisioning; adds a bit of cost-governance overhead |

**Parallel run in Snowflake:**
- **Zero-copy cloning** lets you instantly clone a fully-loaded target database (metadata-only, no storage duplication) to create isolated environments for validation, UAT, or "what-if" comparisons without touching the live migration target
- Run business logic (transformations) via SQL or **Snowpark** (Python/Java/Scala), and compare outputs against legacy using a reconciliation layer — dbt or a custom SQL-based reconciliation framework are common choices
- **Time Travel** provides a safety net during parallel-run/cutover — if something goes wrong shortly after cutover, you can query/restore prior states within the retention window without a separate backup process

| Mechanism | Pros | Cons |
|---|---|---|
| Zero-copy cloning | Instant, no storage cost duplication (only diverging data consumes new storage); great for spinning up safe validation/test environments repeatedly | Clones share underlying micro-partitions until modified — teams sometimes misunderstand this and assume clones are "free forever," leading to surprise storage cost as clones diverge over time |
| Snowpark for logic parity | Lets teams port complex procedural logic (not just SQL) more naturally than pure SQL rewrites | Still a rewrite effort, not automatic translation — legacy procedural logic in Teradata/Oracle needs to be manually reasoned through and reimplemented |
| Time Travel as safety net | No separate backup infrastructure needed for short-term rollback; simple to use (just a SQL clause) | Retention period is bounded by edition (typically up to 90 days on higher tiers) — not a substitute for long-term backup/archival strategy; storage cost for retained historical micro-partitions adds up during heavy-change periods |

**Cutover specifics:**
- Because Snowflake compute (warehouses) is elastic and spun up on demand, cutover has minimal "infrastructure readiness" risk — no need to pre-warm fixed-capacity hardware
- Use **Resource Monitors** immediately post-cutover to catch runaway costs from unexpectedly heavy initial workloads (a common gotcha as users hit the new platform for the first time)

| Mechanism | Pros | Cons |
|---|---|---|
| On-demand elastic warehouses | No capacity-planning risk at cutover; can scale up instantly if load is heavier than expected | Without governance, "just scale up" can mask underlying inefficient queries and drive unexpected cost |
| Resource Monitors | Automated cost guardrails (alert or auto-suspend at thresholds) | Poorly tuned thresholds can unexpectedly suspend warehouses mid-business-critical job if not carefully configured with the right actions/notifications |

---

### 3.6 Consolidated Decision Framework: Choosing the Right Approach

Interviewers care less about whether you can *name* rehost/replatform/refactor or big-bang/phased/zero-downtime, and more about whether you can **choose correctly given constraints and defend the trade-off out loud**. Use this framework to structure any "how would you approach X migration" answer.

**Step 1 — Ask/establish the constraints first (say this out loud in the interview):**
- What's the uptime/SLA tolerance? Can the business accept a maintenance window, or is this 24/7?
- What's the data volume and rate of change (GB/day of deltas)?
- What's the regulatory/compliance stake (finance, healthcare) — does correctness need to be *proven*, not just achieved?
- What's the team's CDC/engineering maturity — do they already run CDC pipelines, or is this net-new?
- What's the budget and timeline pressure — is dual-running for months affordable?
- How much undocumented business logic sits in stored procedures/ETL jobs?
- What's the stakeholder risk appetite — has this org been burned by a bad migration before?

**Step 2 — Match constraints to approach using this matrix:**

| Approach | Speed to complete | Risk concentration | Relative cost | Engineering complexity required | Rollback ease |
|---|---|---|---|---|---|
| Big-bang cutover | Fastest overall timeline | High (all-or-nothing event) | Lowest (no long dual-run) | Low | Hard — must restore from backup |
| Phased/incremental | Medium | Spread across phases | Medium | Medium (interop between old/new) | Medium — per-phase rollback |
| Zero-downtime (CDC-based) | Slower to fully trust, but no outage | Low (tiny cutover window) | Medium-high (CDC tooling + dual infra) | High (CDC, idempotency, routing) | Easy — source stays warm |
| Parallel run (dual-write / shadow) | Slowest (weeks-months of dual-run) | Lowest (nothing depended on until proven) | Highest (two live systems + reconciliation effort) | High (logic parity build + reconciliation) | Trivial — legacy never stopped being source of truth |

**Step 3 — State your answer as a conditional, not a single "best" approach:**
A strong senior answer sounds like: *"For a 24/7 revenue-critical system with a mature engineering org, I'd default to CDC-based zero-downtime with a short parallel-run validation window. For a low-traffic internal reporting mart with a hard budget/timeline, I'd default to a phased, scheduled-outage migration — the operational complexity of CDC isn't justified by the risk profile."* Naming the *conditions* under which you'd choose differently is what separates architects from people reciting a decision once memorized.

**Common trap to avoid:** defaulting to "zero-downtime CDC for everything" because it sounds impressive. Interviewers will probe with "would you really build all that CDC/routing machinery for a nightly batch reporting workload?" — the correct answer is no, and saying so unprompted shows judgment.

---

### 3.7 Worked Example: 50TB On-Prem Teradata → Snowflake, Near-Zero Downtime

This exact scenario is a common opening question (see 3.1 sample questions). Here's a model answer structure you can adapt and deliver in ~2–3 minutes, then let the interviewer drill into any part.

1. **Discovery (weeks 1–2):** Profile the 50TB — table sizes, change rates, which tables are hot (high write volume) vs. cold (append-only history). Run query-log analysis on Teradata over a full business cycle to map every downstream consumer (BI tools, scripts, other systems reading tables directly) so nothing breaks silently at cutover. Inventory stored procedures and flag ones with non-trivial logic for manual review, since automated SQL-translation tools won't fully convert them.
2. **Target design:** Map Teradata schema to Snowflake (adjust for Snowflake's lack of indexes/distribution keys — lean on micro-partitioning and clustering keys only where profiling shows it's needed, not by default). Decide warehouse sizing strategy: separate warehouses for bulk backfill vs. ongoing CDC merge vs. BI query serving, so they don't contend for compute.
3. **Bulk historical load:** Given the volume, evaluate network transfer vs. an offline appliance (Section 3.4) — at 50TB, a direct network copy is often still feasible depending on available bandwidth and timeline, but it's worth stating you'd size this explicitly rather than assume either way. Load into Snowflake staging tables.
4. **CDC starts at the bulk-load snapshot point:** Choose a CDC tool matched to what Teradata can expose (log-based if supported; otherwise trigger- or timestamp-based, and be explicit about the trade-off — e.g., timestamp-based CDC will miss hard deletes, so you'd need a periodic full-table delete-reconciliation sweep to compensate). Stream changes via the chosen tool into Snowflake, using Snowpipe/Snowpipe Streaming for ingestion and Streams & Tasks to merge into final tables.
5. **Continuous validation:** Run row-count and checksum reconciliation continuously, plus business-outcome validation (key reports/KPIs) at least once per business cycle (e.g., a month-end close) before trusting the target for anything real.
6. **Parallel run for the highest-risk workloads:** For anything regulated/financial, run a genuine parallel run (shadow processing, Section 3.3) for at least one full business cycle, with business-user sign-off — not just engineering sign-off — before cutover.
7. **Cutover:** Brief freeze window (seconds–minutes): stop writes to Teradata, let final CDC events drain, redirect BI/application connections to Snowflake, resume writes on Snowflake. Use Resource Monitors from minute one to catch cost surprises as real traffic hits the new warehouses.
8. **Fallback & hypercare:** Keep Teradata live in read-only standby for a defined window (e.g., 2–4 weeks) as a rollback safety net, then decommission.

**If pressed "what's the riskiest part?"** — a strong answer names the stored-procedure conversion and undocumented downstream dependencies (Section 3.4), not the data-movement mechanics, because those are the parts automated tooling can't fully solve and where senior judgment is actually needed.

---

### 3.8 Trade-off Cheat Sheet (Quick Recall Before Whiteboarding)

One line per approach — useful for a fast mental refresh right before the interview, not a substitute for the detail above.

- **Rehost:** Fast and safe, but you carry legacy inefficiency (and legacy cost) into the new platform.
- **Replatform:** Some quick wins, but risks becoming permanent hybrid tech debt if scope isn't controlled.
- **Refactor:** Best long-term outcome, worst near-term cost/time/risk.
- **Repurchase:** Fast for standard needs, costs you customization and adds vendor lock-in.
- **Retire:** Free risk reduction — if you're actually sure nothing depends on it.
- **Retain:** Defers risk, doesn't remove it — technical debt keeps compounding.
- **Big-bang cutover:** Simple plan, one high-stakes moment, hard to roll back.
- **Phased migration:** Lower risk per step, longer calendar time, must run two paradigms at once.
- **Strangler-fig:** Safest incremental path, but the routing/facade layer is real engineering investment.
- **Dual-write:** Real-time parity, but every write path must handle partial-failure without divergence.
- **Zero-downtime CDC:** Removes the outage, adds CDC/idempotency/routing engineering you must own.
- **Parallel run:** Maximum confidence, maximum cost — two production systems running at once.
- **On-prem → cloud (general):** The bottleneck is rarely the data movement itself — it's SQL/stored-procedure conversion and undocumented downstream dependencies.
- **Snowflake specifically:** Storage/compute separation lets you isolate backfill from ongoing sync workloads for free — but zero-copy clones and Time Travel retention both carry storage costs people forget to budget for.

---

## 4. Cloud Platform Architecture Fundamentals

- **Compute vs. storage separation** — decouples scaling of processing power from data volume. *Pro:* pay only for compute when running queries, storage scales independently and cheaply. *Con:* requires careful workload management, since compute costs can spike unpredictably if not governed.
- **Elasticity & auto-scaling** — resources grow/shrink with demand. *Pro:* handles variable workloads without over-provisioning. *Con:* scaling policies need tuning; naive auto-scaling can create cost surprises or lag behind sudden spikes.
- **Cloud storage tiers** (object storage like S3/ADLS/GCS as the data lake layer) — *Pro:* extremely cheap, durable, virtually unlimited. *Con:* not optimized for low-latency transactional access; needs a compute/query layer on top.
- **Networking basics:** VPC/VNet, private connectivity, peering, security groups — foundational for designing secure migration connectivity.
- **IAM fundamentals:** roles vs. users, least privilege, federated identity/SSO — *Pro:* enables fine-grained, auditable access. *Con:* complex to design correctly at scale; misconfiguration is a leading cause of cloud security incidents.
- **Multi-cloud / cross-cloud considerations:** *Pro:* avoids vendor lock-in, enables redundancy. *Con:* data egress costs, latency, and operational complexity of managing multiple platforms.
- **High availability & disaster recovery:** RTO/RPO concepts, backup strategies, failover design — central to any migration's non-functional requirements.

**Sample questions:**
- "How would you design for high availability across regions?"
- "What are the trade-offs of a multi-cloud data architecture?"
- "How do you control cloud costs in a data platform?"

---

## 5. Data Warehousing & Data Modeling

- **OLTP vs. OLAP** distinctions — transactional vs. analytical workload design
- **Schema design:**

| Schema | Pros | Cons |
|---|---|---|
| Star schema | Simple, fast for BI/reporting queries, widely understood | Some data redundancy in dimensions; less flexible for complex many-to-many relationships |
| Snowflake schema | Normalized dimensions save storage, reduces redundancy | More joins required, can hurt query performance and complexity |
| Data Vault | Highly auditable, flexible to source changes, good for regulated/enterprise environments | Complex to model and query directly — usually needs a presentation layer on top; steeper learning curve for teams |
| 3NF | Minimizes redundancy, good for OLTP-style integrity | Poor performance for large analytical queries due to many joins |

- **Slowly Changing Dimensions (SCD Types 0–6)** — know Type 1 (overwrite), Type 2 (historize with new rows), and Type 3 (add columns) at minimum, with trade-offs: Type 1 loses history but is simple; Type 2 preserves full history but grows table size and complicates joins; Type 3 is limited to tracking a fixed number of historical states.
- **Partitioning, clustering, indexing strategies** — *Pro:* dramatically improves query performance by pruning irrelevant data. *Con:* poorly chosen keys can *hurt* performance (skew, over-partitioning) and add maintenance overhead.
- **Data lake vs. data warehouse vs. lakehouse:**

| Architecture | Pros | Cons |
|---|---|---|
| Data lake | Cheap, stores any data type/format, schema-on-read flexibility | Can become a "data swamp" without governance; not optimized for fast structured queries |
| Data warehouse | Optimized for structured, fast analytical queries; strong consistency | More expensive per TB; less flexible for unstructured/semi-structured data |
| Lakehouse | Combines lake economics with warehouse performance/governance | Still a maturing pattern; tooling and best practices vary more across vendors |

- **Medallion architecture** (bronze/silver/gold layers) — *Pro:* clear separation of raw, cleansed, and business-ready data, aids governance and reprocessing. *Con:* adds storage/compute overhead of maintaining multiple copies at different stages.

**Sample questions:**
- "When would you choose Data Vault over a star schema?"
- "How do you model slowly changing dimensions in a cloud warehouse?"
- "Explain the medallion architecture and why it's useful in migrations."

---

## 6. ETL/ELT & Data Integration

- **ETL vs. ELT:**

| Approach | Pros | Cons |
|---|---|---|
| ETL (transform before load) | Can enforce data quality/structure before it hits the warehouse; useful when target has limited compute | Requires separate transformation infrastructure; slower to adapt to new requirements |
| ELT (load then transform in warehouse) | Leverages the elastic compute of modern cloud warehouses; raw data preserved for reprocessing; faster to implement changes | Requires the warehouse to have strong compute/SQL transformation capability; raw data sitting untransformed can raise governance questions if not managed |

- **Batch vs. streaming/real-time ingestion** — *Pro of streaming:* near-real-time insights, supports zero-downtime migration patterns. *Con:* significantly more complex infrastructure and failure-handling than batch.
- **CDC tools/concepts** (log-based, trigger-based, timestamp-based):

| CDC Type | Pros | Cons |
|---|---|---|
| Log-based | Low overhead on source system, captures all changes including deletes, near-real-time | Requires source DB to expose transaction logs; not all legacy systems support it |
| Trigger-based | Works on databases without log access | Adds write overhead to source system; can impact production performance |
| Timestamp-based | Simple to implement, no special DB features needed | Misses hard deletes; can miss rapid updates between polling intervals; relies on trustworthy timestamp columns |

- **Orchestration concepts:** DAGs, dependency management, retries, idempotency (Airflow, ADF, dbt, Informatica, Talend, Matillion) — mention any you've used, and be ready to explain why idempotency matters (safe to retry/replay without corrupting data).
- **Data quality & observability:** validation rules, anomaly detection, lineage tracking — increasingly expected in enterprise migrations for audit and trust.

**Sample questions:**
- "How would you re-architect a legacy nightly-batch ETL pipeline for a cloud-native platform?"
- "What's your approach to handling schema drift in source systems?"
- "How do you ensure idempotency in your pipelines?"

---

## 7. Performance, Scalability & Cost Optimization

- **Query optimization fundamentals:** execution plans, statistics, pruning, caching — *Pro:* dramatically reduces cost/latency. *Con:* requires ongoing tuning discipline; easy to regress as data grows or query patterns change.
- **Workload management / concurrency handling** — isolating workloads (e.g., ETL vs. BI vs. ad hoc) prevents one from starving another. *Con:* under-isolation causes contention; over-isolation (too many separate compute clusters) increases cost.
- **Storage optimization:** compression, columnar file formats (Parquet, ORC, Avro) — *Pro:* massive storage and I/O savings for analytical workloads. *Con:* columnar formats are less efficient for row-by-row transactional access patterns.
- **Cost governance:** chargeback/showback models, rightsizing compute, usage monitoring — *Pro:* keeps cloud spend accountable and predictable. *Con:* requires organizational buy-in and tooling investment; without it, cloud's pay-as-you-go model can lead to runaway costs.
- **Capacity planning for migrations:** sizing the target environment based on legacy workload profiling — *Pro:* avoids both over- and under-provisioning. *Con:* legacy systems' peak-sized hardware doesn't map linearly to elastic cloud sizing; requires real workload analysis, not just spec translation.

**Sample questions:**
- "A migrated workload is running slower on the new platform than legacy — how do you troubleshoot?"
- "How do you estimate compute sizing for a new platform before workloads are running?"

---

## 8. Security, Governance & Compliance

- **Data security fundamentals:** encryption at rest/in transit, key management, tokenization/masking
- **Access control models:**

| Model | Pros | Cons |
|---|---|---|
| RBAC (role-based) | Simple to reason about, widely supported, easy to audit | Can become unwieldy with many fine-grained exceptions ("role explosion") |
| ABAC (attribute-based) | Very flexible, can express complex context-aware policies | Harder to audit/reason about; more complex to implement and troubleshoot |
| Row/column-level security | Enables fine-grained data protection within a single shared table | Adds query complexity and potential performance overhead if not implemented carefully |

- **Data governance concepts:** data catalogs, metadata management, lineage, stewardship — *Pro:* builds trust and auditability, essential for regulated industries. *Con:* requires ongoing investment and cultural buy-in — governance tooling alone doesn't solve governance without process and ownership.
- **Compliance frameworks:** GDPR, HIPAA, SOC 2, PCI-DSS — know at a high level which industries care about which, and that migrations often trigger a fresh compliance review since data location/access patterns change.
- **Audit and monitoring:** access logging, anomaly detection for sensitive data — critical to prove compliance post-migration, not just achieve it.

**Sample questions:**
- "How would you design row-level security for a multi-tenant analytics platform?"
- "How do you handle PII during a migration when source and target have different security models?"

---

## 9. Enterprise Architecture & Solution Design Skills

- Ability to produce and explain: **architecture diagrams, migration roadmaps, effort/cost estimates, RAID logs (Risks, Assumptions, Issues, Dependencies)**
- **TCO (Total Cost of Ownership) analysis** — comparing legacy vs. target platform costs, including often-missed factors like staff retraining, dual-running costs, and decommissioning costs
- **Proof of Concept (POC) / Proof of Value (POV) design:**

| Approach | Pros | Cons |
|---|---|---|
| Narrow, single-workload POC | Fast to execute, lower cost, easier to get stakeholder buy-in for scope | May not surface issues that only appear at scale or with workload diversity |
| Broad, multi-workload POC | More representative of real production risk, builds stronger confidence | Expensive, slower, harder to scope and resource |

- Vendor/tool evaluation frameworks — weighted scoring against defined criteria (cost, performance, support, ecosystem fit) rather than gut-feel comparisons
- Non-functional requirements gathering (performance, scalability, security, compliance, DR) — often the most-skipped step that causes the most rework later

**Sample questions:**
- "How do you structure a POC to prove out a migration approach to a skeptical stakeholder?"
- "Walk me through how you'd build a TCO comparison between the current and proposed platform."

---

## 10. Stakeholder Management, Communication & Leadership

This is heavily weighted for architect roles — expect several behavioral questions.

- Translating **business requirements into technical architecture** and vice versa (explaining technical trade-offs to non-technical execs)
- Managing **conflicting priorities** between business units, IT, and vendors
- Leading **cross-functional teams** without direct authority
- Handling **pushback/skepticism** from customer engineering teams during pre-sales or delivery
- **Pre-sales vs. delivery mindset** — if this role touches pre-sales, be ready to discuss how you scope and estimate before you own delivery
- Conflict resolution, escalation management, executive communication

**Sample behavioral questions:**
- "Tell me about a time you had to convince a resistant technical team to adopt your proposed architecture."
- "Describe a migration project that fell behind schedule — how did you communicate this to leadership?"
- "Tell me about a time you disagreed with a colleague's/customer's technical decision. What did you do?"
- "How do you handle a situation where the customer wants a timeline that isn't technically realistic?"

---

## 11. Industry & Legacy Platform Knowledge (Nice to Have)

- **Teradata, Netezza, Oracle Exadata, IBM DB2** — traditional on-prem MPP warehouses; strong at fixed, predictable workloads but expensive to scale and maintain, with rigid capacity planning
- **Hadoop/Hive/Spark ecosystem** — data lake heritage; flexible and cheap for raw storage, but operationally heavy (cluster management) and slower for interactive analytics compared to modern cloud warehouses
- **Redshift, Synapse, BigQuery, Databricks** — cloud competitors: Redshift ties compute/storage more tightly (improving but historically less elastic); Synapse integrates deeply with the Microsoft ecosystem; BigQuery is serverless with a different pricing model (on-demand per query vs. reserved capacity); Databricks leans lakehouse-first with strong ML/data-science tooling
- **Legacy ETL tools:** Informatica PowerCenter, DataStage, Ab Initio, SSIS — mature, feature-rich, but often licensing-heavy and less cloud-native than modern ELT tools

You don't need deep expertise — just enough to discuss common pain points that drive migrations away from each.

---

## 12. Snowflake Bonus Vocabulary (Optional but Recommended — Light Touch)

| Concept | One-liner |
|---|---|
| Separation of storage & compute | Virtual Warehouses (compute) scale independently of storage |
| Micro-partitions | Automatic, immutable, columnar storage chunks — enable pruning, time travel, cloning |
| Zero-copy cloning | Instant, metadata-only clones of databases/tables/schemas — huge for migration testing |
| Time Travel | Query/restore historical data up to a retention period — useful for migration rollback safety |
| Snowpipe | Continuous/near-real-time data ingestion |
| Streams & Tasks | Native change-tracking plus scheduled/triggered SQL execution — built-in micro-batch ELT |
| Resource Monitors | Cost governance — cap or alert on warehouse credit usage |
| RBAC model | Roles assigned to users; roles can be hierarchical |
| Data Sharing / Marketplace | Share live data without copying it |
| Snowpark | Write transformations in Python/Java/Scala instead of just SQL |
| Multi-cloud | Runs on AWS, Azure, and GCP — relevant for migration source flexibility |

**One good line to have ready:** *"I understand Snowflake's core differentiator is the separation of storage and compute combined with near-zero maintenance — which changes how I'd approach migration sizing and POCs compared to a fixed-capacity legacy MPP system."*

---

## 13. Snowflake Migration Scenarios: On-Prem → Snowflake and Databricks → Snowflake (Detailed Steps)

This is the one Snowflake-specific module worth knowing cold — the two source scenarios you're most likely to be handed in a case-study or scenario question. Everything else in this doc stays platform-agnostic; this section maps that generic playbook onto the two concrete source platforms.

### 13.1 Scenario A: On-Prem (Teradata/Oracle/DB2-style MPP) → Snowflake

**Why this scenario is different from a cloud-to-cloud migration:** you're crossing a network boundary, a security-model boundary (perimeter/AD → IAM), and usually a SQL-dialect boundary all at once.

| Step | What happens | Key trade-off to name |
|---|---|---|
| 1. Assess & inventory | Profile table sizes, change rates, stored-procedure count/complexity, downstream consumers via query-log analysis over a full business cycle | Skipping this is the #1 cause of mid-migration surprises — cheap to do, expensive to skip |
| 2. Connectivity design | Stand up VPN or dedicated interconnect (Direct Connect/ExpressRoute) between on-prem and Snowflake's cloud region | Dedicated interconnect = reliable but weeks of lead time; VPN = fast but can bottleneck under CDC load |
| 3. Bulk historical load | Online network transfer vs. offline appliance (Snowball/Data Box) depending on volume/bandwidth, landing into Snowflake **staging tables** | Appliance is fast for one-time bulk but doesn't help with ongoing sync — plan the handoff to CDC explicitly |
| 4. Schema & SQL conversion | Convert schema (drop legacy indexes/distribution keys, rely on micro-partitioning), convert stored procedures (SQL translation tooling for the bulk, manual rewrite for the complex 10–20%) | This, not data movement, is usually the real schedule risk |
| 5. CDC for the delta | Log-based CDC if the source exposes it; otherwise trigger- or timestamp-based, streamed via Snowpipe/Snowpipe Streaming into staging, merged with Streams & Tasks | Timestamp-based CDC misses hard deletes — needs a periodic reconciliation sweep to compensate |
| 6. Security model rebuild | Redesign IAM/RBAC in Snowflake rather than 1:1-mapping on-prem AD/LDAP permissions | 1:1 mapping is the common shortcut and the common source of post-migration access incidents |
| 7. Validation | Continuous row/checksum reconciliation + business-outcome (report/KPI) validation across at least one full business cycle | Row counts matching ≠ migration success if business logic in stored procs was translated incorrectly |
| 8. Cutover | Short freeze window, drain final CDC, redirect BI/app connections, resume writes on Snowflake; Resource Monitors live from minute one | Elastic warehouses remove capacity-planning risk at cutover, but can mask cost blowouts from inefficient queries if ungoverned |
| 9. Fallback & decommission | Keep source in read-only standby for a defined window before decommissioning | Balancing "long enough to be safe" against "not paying for two systems forever" |

### 13.2 Scenario B: Databricks (Lakehouse) → Snowflake

**Why this scenario is different from on-prem:** both platforms are already cloud-native and elastic, so the challenge shifts away from network/bandwidth and toward **format, compute-paradigm, and workload-fit differences**.

| Step | What happens | Key trade-off to name |
|---|---|---|
| 1. Assess & inventory | Catalog Delta Lake tables, notebooks, Spark jobs, and downstream BI/ML consumers; identify which workloads are truly better served by Snowflake (BI/SQL-heavy) vs. which are genuinely lakehouse/ML-native and might stay on Databricks (hybrid end-state, not full migration) | The most common mistake is treating this as "migrate everything" when the honest answer is often a hybrid architecture |
| 2. Storage/format bridge | Snowflake can query Delta Lake tables directly via external tables/Iceberg support, or you can convert Delta → native Snowflake tables for full performance | External-table approach is fast to stand up but gives you Snowflake-on-someone-else's-storage performance, not native performance; converting gets full performance but is a real load/rebuild effort |
| 3. Compute paradigm shift | Re-implement Spark/PySpark transformation logic — either as SQL/ELT inside Snowflake, or as Snowpark (Python) if the team wants to preserve a Python-first workflow | Snowpark eases the rewrite but is still a genuine re-implementation, not an automatic port — don't undersell this effort in an estimate |
| 4. Notebook/orchestration logic | Databricks notebooks and Jobs/Workflows need to be re-expressed as dbt models, Snowflake Tasks, or an external orchestrator (Airflow) | Losing the notebook-as-documentation pattern is a real workflow change for the team, not just a technical swap — worth flagging as a change-management item |
| 5. Delta change data | If ongoing sync is needed during transition, use Delta's native change data feed (CDF) as the CDC source feeding into Snowpipe Streaming/Streams & Tasks, rather than building generic log-based CDC from scratch | CDF gives you clean, already-structured change events — genuinely easier than most on-prem CDC scenarios |
| 6. ML/advanced-analytics workloads | Decide explicitly what happens to ML training/feature-engineering pipelines — Snowpark ML covers some cases, but deep MLOps-heavy workloads may be a legitimate reason to keep a Databricks footprint | Pretending "full migration" is always right here is a red flag to a technical interviewer — name the hybrid option unprompted |
| 7. Validation | Compare query/report outputs between Delta-based and Snowflake-native pipelines, focused on any transformation logic that was manually re-implemented (not auto-converted, unlike a lot of on-prem SQL) | Since more logic here is hand-rewritten (Spark code has no direct SQL equivalent for some operations), validation needs to be logic-focused, not just row-count-focused |
| 8. Cutover | Typically phased by workload/domain rather than big-bang, since Databricks migrations often end in a hybrid architecture rather than a clean full cutover | Phased-by-workload avoids forcing an artificial "everything moves on day X" narrative that doesn't match the real hybrid end-state |

**One-line summary to have ready for each:** *"On-prem to Snowflake is mostly a network, security-model, and SQL-conversion problem. Databricks to Snowflake is mostly a compute-paradigm and workload-fit problem — the honest answer is often hybrid, not full migration, and saying that unprompted is usually the right move."*

---

## 14. Questions to Ask the Interviewer

- "What does a typical migration engagement look like here — pre-sales scoping through to delivery, or delivery only?"
- "What's the most common source platform you're migrating customers from right now?"
- "How is success measured for this role — deals influenced, migrations delivered, customer satisfaction?"
- "What's the biggest technical challenge the team is currently facing on migrations?"

---

## 15. Quick Self-Check Before the Interview

- [ ] I can walk through the full migration lifecycle end-to-end without notes
- [ ] I have 3–4 STAR stories covering: a technically hard migration, a stakeholder conflict, a failure/lesson learned, and a cost/performance win
- [ ] I can explain rehost vs. replatform vs. refactor with pros/cons and examples
- [ ] I can explain zero-downtime CDC-based migration and parallel-run strategies, with trade-offs, without notes
- [ ] I can deliver the 50TB Teradata → Snowflake worked example (3.7) end-to-end in under 3 minutes
- [ ] I can state, unprompted, *when I would NOT use* zero-downtime CDC (3.6) — judgment, not just capability
- [ ] I can whiteboard a basic target-state architecture diagram (source → ingestion → storage/lake → warehouse → BI)
- [ ] I know 5–10 Snowflake terms well enough to use naturally, not recite
- [ ] I can walk through on-prem → Snowflake (13.1) and Databricks → Snowflake (13.2) step-by-step, and explain why the two scenarios differ in what's actually hard
- [ ] I have 3–4 thoughtful questions prepared for the interviewer

---

*Good luck — the role is testing judgment and communication as much as technical depth, so prioritize clear storytelling and honest trade-off discussion over jargon.*
