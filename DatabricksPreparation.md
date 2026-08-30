# Databricks Technical Interview Preparation: Master Index

A structured syllabus and tracking index covering the 7 core domains required for Senior & Lead Data Engineer / Lakehouse Architect interviews.

---

## Table of Contents

* [Module 1: Apache Spark Core & PySpark Internals](#module-1-apache-spark-core--pyspark-internals)
* [Module 2: Delta Lake Deep Dive & Storage Mechanics](#module-2-delta-lake-deep-dive--storage-mechanics)
* [Module 3: Compute Architecture, Cluster Sizing & Engine Optimization](#module-3-compute-architecture-cluster-sizing--engine-optimization)
* [Module 4: Ingestion Patterns, Structured Streaming & Delta Live Tables (DLT)](#module-4-ingestion-patterns-structured-streaming--delta-live-tables-dlt)
* [Module 5: Data Governance, Security & Unity Catalog](#module-5-data-governance-security--unity-catalog)
* [Module 6: Production Operations, Orchestration & CI/CD](#module-6-production-operations-orchestration--cicd)
* [Module 7: Lakehouse System Design, CDC & Troubleshooting Playbooks](#module-7-lakehouse-system-design-cdc--troubleshooting-playbooks)

---

### Module 1: Apache Spark Core & PySpark Internals
* **1.1 Distributed Runtime Architecture**
  * Driver Program, Cluster Manager, Worker Nodes, and Executors
  * Application $\rightarrow$ Job $\rightarrow$ Stage $\rightarrow$ Task execution breakdown
  * Slot allocation and thread concurrency models
* **1.2 DAG Construction & Execution Model**
  * Directed Acyclic Graph (DAG) generation and Lineage Graphs
  * Lazy evaluation lifecycle and trigger actions
  * Task scheduling (FIFO vs. FAIR scheduler)
* **1.3 Transformations, Actions & Shuffling Mechanics**
  * Narrow vs. Wide transformations
  * Shuffle write, shuffle spill, and shuffle fetch operations
  * Pipelining and execution stage boundaries
* **1.4 Catalyst Optimizer Deep Dive**
  * Analysis Phase (Catalog resolution & AST generation)
  * Logical Optimization (Rule-based: Predicate Pushdown, Projection Pruning, Constant Folding)
  * Physical Planning (Cost-based join and operator selection)
  * Code Generation (Janino compiler integration)
* **1.5 Project Tungsten Engine**
  * Off-heap memory management via `sun.misc.Unsafe`
  * Compact binary memory layout
  * Whole-Stage Code Generation (WSCG) and cache-aware computation
* **1.6 Unified Memory Management Architecture**
  * Reserved Memory ($300\text{ MB}$) vs. User Memory
  * Spark Memory: Dynamic sharing between Execution and Storage pools
  * Eviction rules, Storage levels, and Disk Spills
  * JVM Garbage Collection mechanics (G1GC tuning)
* **1.7 Partitioning & Data Distribution**
  * `repartition()` vs. `coalesce()` execution paths
  * Hash Partitioner vs. Range Partitioner
  * Optimal partition sizing ($100\text{ MB} - 200\text{ MB}$ uncompressed target)
* **1.8 Distributed Join Internals**
  * Broadcast Hash Join (BHJ)
  * Shuffle Hash Join (SHJ)
  * Sort-Merge Join (SMJ)
  * Cartesian Product & Broadcast Nested Loop Join (BNLJ)
* **1.9 PySpark Architecture & Optimization**
  * Py4J gateway communication overhead
  * Python Worker IPC sockets and serialization (Pickling)
  * PyArrow vectorization and Pandas UDFs (Scalar, Grouped Map, Grouped Agg)

---

### Module 2: Delta Lake Deep Dive & Storage Mechanics
* **2.1 Transaction Log (`_delta_log`) Internals**
  * Write-Ahead Log (WAL) structure and Atomicity
  * JSON Commit logs (`00000N.json`) and CRC checksum files
  * Checkpoint Parquet files (`.checkpoint.parquet`) and Log Compaction
* **2.2 Concurrency Control & Isolation**
  * Optimistic Concurrency Control (OCC) protocols
  * Concurrency isolation levels: WriteSerializable vs. Serializable
  * Mutual exclusion, conflict detection matrix, and automated retry loops
* **2.3 Storage Layout Optimization**
  * Small file compaction via `OPTIMIZE` (target $1\text{ GB}$ file size)
  * `VACUUM` command, retention period constraints, and tombstone lifecycle
  * Auto-compaction and Optimized Writes
* **2.4 Deletion Vectors (DV)**
  * Merge-on-Read (MoR) vs. Copy-on-Write (CoW)
  * Bitmap file layout (`.pvd`) and Row ID tracking
  * Write amplification reduction during `UPDATE`, `DELETE`, and `MERGE`
* **2.5 Data Skipping & Multi-Dimensional Clustering**
  * Hive directory partitioning (trade-offs & limits)
  * Z-Ordering (Space-filling curves and multi-column skipping)
  * Liquid Clustering (`CLUSTER BY`): Dynamic re-clustering, schema independence
* **2.6 Time Travel & Metadata Recovery**
  * `VERSION AS OF` and `TIMESTAMP AS OF` mechanisms
  * Transaction log state reconstruction
  * `RESTORE TABLE` execution model
* **2.7 Change Data Feed (CDF)**
  * Enabling CDF (`delta.enableChangeDataFeed`)
  * Commit log change types: `insert`, `update_preimage`, `update_postimage`, `delete`
  * Incremental downstream propagation
* **2.8 Table Cloning Mechanics**
  * Shallow Clones (zero-copy pointer referencing) vs. Deep Clones (full data copy)
  * Use cases in staging promotion and ML experimentation
* **2.9 UniForm (Universal Format)**
  * Delta Lake UniForm architecture
  * Automated asynchronous Iceberg and Hudi metadata generation

---

### Module 3: Compute Architecture, Cluster Sizing & Engine Optimization
* **3.1 Compute Topologies & Types**
  * All-Purpose Clusters vs. Automated Job Clusters vs. Serverless Compute
  * Worker and Driver role separation and node sizing
  * Databricks Container Services (DCS) and custom Docker runtimes
* **3.2 Cluster Access & Isolation Modes**
  * Single User mode
  * Shared User Isolation mode (Unity Catalog compliance)
  * No Isolation (Legacy shared mode)
* **3.3 Adaptive Query Execution (AQE)**
  * Dynamic Coalescing of post-shuffle partitions
  * Dynamic Join Strategy Switching (SMJ $\rightarrow$ BHJ at runtime)
  * Dynamic Handling of Data Skew (automatic sub-partitioning)
* **3.4 Dynamic File & Partition Pruning**
  * Dynamic Partition Pruning (DPP) vs. Dynamic File Pruning (DFP)
  * Subquery broadcast filter pushdown
* **3.5 Caching Architectures**
  * Spark Memory Cache (`.cache()`, `.persist()`)
  * Delta Disk Cache / Databricks Cache (Local NVMe page caching)
* **3.6 Cluster Sizing & Resource Allocation**
  * Compute-Optimized vs. Memory-Optimized vs. Storage-Optimized instances
  * Golden rules for Executor sizing ($4-5\text{ cores/executor}$, JVM overhead tuning)
  * Driver memory allocation for broadcasts and metadata management
* **3.7 Cost Management & Governance**
  * Spot instance orchestration (On-Demand Driver + Spot Workers)
  * Databricks Enhanced Autoscaling & auto-termination policies
  * Workload tagging and DBU budget attribution
* **3.8 Serverless Compute Architecture**
  * Rapid startup mechanics ($<20\text{ seconds}$)
  * Serverless Workflows, Serverless SQL Warehouses, and Serverless Notebooks

---

### Module 4: Ingestion Patterns, Structured Streaming & Delta Live Tables (DLT)
* **4.1 Auto Loader (`cloudFiles`)**
  * Directory Listing mode vs. File Notification mode (SNS/SQS, Event Grid)
  * Incremental state tracking via RocksDB
* **4.2 Schema Evolution & Quality Defense**
  * Schema inference, schema hints, and evolution modes
  * Handling malformed records via `_rescued_data` column
* **4.3 Structured Streaming Core Architecture**
  * Micro-batch engine vs. Continuous Processing engine
  * Sources, Sinks, and `foreachBatch` patterns
  * Exactly-Once end-to-end processing semantics
* **4.4 State Management & Watermarking**
  * Stateful transformations (`mapGroupsWithState`, `flatMapGroupsWithState`)
  * State Stores (Default HDFS vs. RocksDB StateStore)
  * Event-time watermarking for late-arriving data handling
  * Output Modes: Append, Update, Complete
* **4.5 Fault Tolerance & Trigger Controls**
  * Checkpoint directory layout (offsets, commits, metadata, state)
  * Triggers: `ProcessingTime`, `AvailableNow`, `Continuous`
* **4.6 Delta Live Tables (DLT) Declarative Framework**
  * Declarative SQL & Python pipeline construction
  * Automatic execution DAG compilation and dataset dependency tracking
* **4.7 DLT Table Constructs**
  * Streaming Tables (incremental append streams)
  * Materialized Views (managed precomputed aggregates)
  * Temporary Views (pipeline-scoped transformations)
* **4.8 Data Quality Expectations in DLT**
  * `@dlt.expect` (Track and warn)
  * `@dlt.expect_or_drop` (Filter out bad rows)
  * `@dlt.expect_or_fail` (Halt pipeline execution)
  * Quarantining invalid records design pattern
* **4.9 DLT Observability & Maintenance**
  * Event log analysis (`system.event_log`)
  * Automated vacuuming and optimization within DLT runtimes

---

### Module 5: Data Governance, Security & Unity Catalog
* **5.1 Unity Catalog Architecture & Namespace**
  * Metastore attachment and multi-workspace federation
  * 3-level namespace: `catalog.schema.table_or_view`
  * Centralized Identity Management (SCIM synchronization)
* **5.2 Cloud Storage Security & Abstraction**
  * Storage Credentials (cloud IAM roles / Managed Identities)
  * External Locations (URI path permissions)
* **5.3 Table Lifecycles: Managed vs. External**
  * Managed Tables: Storage path control and cascade deletion behavior
  * External Tables: Independent lifecycle and metadata-only detachment
* **5.4 Volumes for Unstructured/Semi-Structured Data**
  * Managed Volumes vs. External Volumes
  * POSIX file API paths (`/Volumes/...`) and governance policies
* **5.5 Fine-Grained Security & Privacy Controls**
  * Dynamic Column Masking functions
  * Row-Level Filtering functions (`IS_ACCOUNT_GROUP_MEMBER`, `CURRENT_USER`)
  * Attribute-Based Access Control (ABAC) with Governance Tags
* **5.6 Privilege Hierarchy & Access Control**
  * Securable objects and privilege inheritance (`USAGE`, `SELECT`, `MODIFY`, `EXECUTE`)
  * Declarative RBAC permission management via SQL (`GRANT`/`REVOKE`)
* **5.7 Data Lineage Tracking**
  * Automated table-level and column-level lineage capture
  * Upstream and downstream impact visualization
* **5.8 System Tables & Operational Auditing**
  * Cost analysis via `system.billing.usage`
  * Security auditing via `system.access.audit`
  * Compute and workflow observability via `system.lakeflow.*`
* **5.9 Delta Sharing Protocol**
  * Open, zero-copy cross-platform data sharing
  * Provider and recipient management across clouds

---

### Module 6: Production Operations, Orchestration & CI/CD
* **6.1 Databricks Workflows**
  * Multi-Task DAG definitions across diverse tasks (Notebooks, Python, SQL, DLT, dbt)
  * Dynamic metadata passing via Task Values (`dbutils.jobs.taskValues`)
  * Conditional branching (If/Else, For-Each loops)
* **6.2 Reliability, Failure Handling & Retries**
  * Task-level retry policies, backoff intervals, and task timeouts
  * Partial run repair and individual failed task reruns
  * Alerting integrations (PagerDuty, Webhooks, Slack)
* **6.3 Databricks Asset Bundles (DABs)**
  * Infrastructure as Code (IaC) configuration (`databricks.yml`)
  * Target environment isolation (dev $\rightarrow$ staging $\rightarrow$ prod)
  * CLI bundle lifecycle commands: `validate`, `deploy`, `run`
* **6.4 Developer Tooling & Integrations**
  * Databricks CLI v0.200+ and REST APIs
  * Databricks Connect v2 (Spark Connect remote IDE debugging)
  * Git Folders lifecycle and branch integration
* **6.5 Spark UI Profiling & Diagnostics**
  * Jobs Tab: Event timeline and bottleneck stages
  * Stages Tab: Task execution skew, GC time vs. CPU time, Shuffle Read/Write
  * SQL/DataFrame Tab: Physical plan verification and Whole-Stage Code Gen inspection
  * Executors Tab: Thread dumps, task distribution, Memory/Disk Spills
* **6.6 Cluster & Infrastructure Observability**
  * Ganglia metrics interpretation (CPU vs. Memory vs. Network bottlenecks)
  * Log4j setup and driver log shipping to CloudWatch/Log Analytics

---

### Module 7: Lakehouse System Design, CDC & Troubleshooting Playbooks
* **7.1 Medallion Architecture Blueprint**
  * Bronze: Raw ingest, append-only, schema preservation, audit columns
  * Silver: Deduplication, enterprise cleansing, conformed data models
  * Gold: Dimensional models (Star Schema), KPI aggregates, Feature Stores
* **7.2 Change Data Capture (CDC) Design Patterns**
  * SCD Type 1: In-place upserts via deterministic `MERGE INTO`
  * SCD Type 2: Historical validity tracking (`is_current`, `start_date`, `end_date`, surrogate keys)
  * Streaming CDC pipelines with CDF
* **7.3 Memory Troubleshooting: Driver OOM Playbook**
  * Root causes: `.collect()`, `.toPandas()`, oversized broadcast joins
  * Remediation strategies: Selective fetching, broadcast tuning, DAG checkpointing
* **7.4 Memory Troubleshooting: Executor OOM & Disk Spill Playbook**
  * Root causes: Severe data skew, excessive executor core count, memory overhead deficits
  * Remediation strategies: Core-to-memory tuning, adjusting `memoryOverhead`, Off-heap configuration
* **7.5 Data Skew Mitigation Playbook**
  * Detection via Spark UI task duration distribution
  * Fixes: Salting keys with random hash prefixes, AQE skew join parameters, Broadcast conversion
* **7.6 Storage Optimization Playbook**
  * Remediating the "Small File Problem"
  * Mitigating over-partitioning on high-cardinality keys
  * Automated compaction and Liquid Clustering implementation
* **7.7 Streaming Bottleneck & Incident Triage**
  * RocksDB state store memory exhaustion fixes
  * Watermark lag resolution and micro-batch tuning
  * Cloud storage rate-limit throttling (HTTP 503/429) backoff handling
