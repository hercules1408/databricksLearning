# Databricks & PySpark Interview Preparation Guide

## Table of Contents

1. [Module 1: Apache Spark Core & PySpark Internals](#module-1-apache-spark-core--pyspark-internals)
2. [Module 2: Delta Lake Deep Dive & Storage Mechanics](#module-2-delta-lake-deep-dive--storage-mechanics)
3. [Module 3: Compute Architecture, Cluster Sizing & Engine Optimization](#module-3-compute-architecture-cluster-sizing--engine-optimization)
4. [Module 4: Ingestion Patterns, Structured Streaming & Delta Live Tables (DLT)](#module-4-ingestion-patterns-structured-streaming--delta-live-tables-dlt)
5. [Module 5: Data Governance, Security & Unity Catalog](#module-5-data-governance-security--unity-catalog)
6. [Module 6: Production Operations, Orchestration & CI/CD](#module-6-production-operations-orchestration--cicd)
7. [Module 7: Lakehouse System Design, CDC & Troubleshooting Playbooks](#module-7-lakehouse-system-design-cdc--troubleshooting-playbooks)

---

## Module 1: Apache Spark Core & PySpark Internals

### 1.1 Distributed Runtime Architecture

**Driver, Cluster Manager, Worker Nodes, Executors**

- **Driver**: hosts the `SparkSession`/`SparkContext`, builds the DAG, converts it into stages/tasks, and schedules work. If the driver dies, the whole application dies.
- **Cluster Manager**: negotiates resources (in Databricks this is managed for you; standalone/YARN/K8s elsewhere).
- **Executors**: JVM processes on worker nodes that run tasks and cache data. Each executor has a fixed number of cores and a memory budget.
- **Slots**: an executor with `N` cores runs `N` tasks concurrently (one slot per core, roughly).

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("InterviewPrep").getOrCreate()

# Inspect the runtime
print(spark.sparkContext.defaultParallelism)   # total cores across executors
print(spark.sparkContext.getConf().getAll())   # driver/executor config
```

**Application → Job → Stage → Task**

| Level | Triggered by | Example |
|---|---|---|
| Application | `spark-submit` / notebook attach | The whole PySpark program |
| Job | Every **action** (`.collect()`, `.count()`) | `df.count()` |
| Stage | A **shuffle boundary** | groupBy causes a new stage |
| Task | One unit of work per **partition** | 200 partitions = 200 tasks in that stage |

```python
df = spark.range(0, 1000000, 1, 8)          # 8 partitions
result = df.groupBy((df.id % 10)).count()    # triggers a shuffle -> 2 stages
result.collect()                             # this is the action -> 1 job
```

**Interview answer:** *"An action triggers a job. The DAG scheduler splits the job into stages at shuffle boundaries. Each stage is split into tasks, one per partition, and the task scheduler assigns tasks to executor slots."*

---

### 1.2 DAG Construction & Execution Model

- Spark builds a **logical plan** from your transformations, then a **lineage graph** (DAG) tracking how each RDD/DataFrame was derived.
- **Lazy evaluation**: nothing runs until an action is called. This lets Catalyst optimize the *entire* chain instead of executing line-by-line.
- **FIFO vs FAIR scheduler**: controls how multiple jobs submitted concurrently (e.g., from threads) share cluster resources. FIFO (default) runs jobs in submission order; FAIR gives round-robin slices so no single job starves the others.

```python
spark.conf.set("spark.scheduler.mode", "FAIR")
```

```python
# Lazy evaluation demo
df = spark.read.csv("dbfs:/tmp/sample.csv", header=True)
step1 = df.filter(df.age > 30)      # nothing executes
step2 = step1.select("name", "age") # nothing executes
step2.explain(True)                  # shows the plan, still no execution
step2.show()                         # NOW it executes
```

---

### 1.3 Transformations, Actions & Shuffling Mechanics

**Narrow vs Wide transformations**

| Type | Definition | Examples | Shuffle? |
|---|---|---|---|
| Narrow | Each output partition depends on one input partition | `map`, `filter`, `select`, `union` | No |
| Wide | Output partition depends on multiple input partitions | `groupBy`, `join`, `distinct`, `repartition` | Yes |

```python
from pyspark.sql.functions import col

df = spark.createDataFrame([(1, "a"), (2, "b"), (3, "a")], ["id", "grp"])

narrow = df.filter(col("id") > 1).select("grp")   # narrow, no shuffle
wide = df.groupBy("grp").count()                  # wide, causes a shuffle
```

**Shuffle mechanics**
- **Shuffle write**: each map task writes output partitioned by key to local disk.
- **Shuffle fetch**: reduce tasks pull the relevant blocks over the network.
- **Shuffle spill**: if data for a task doesn't fit in memory, it spills to disk — a major perf killer.
- **Pipelining**: consecutive narrow transformations are fused into a single stage (no materialization in between).

```python
# See shuffle partitions config (default 200 - usually needs tuning)
spark.conf.get("spark.sql.shuffle.partitions")
spark.conf.set("spark.sql.shuffle.partitions", "64")
```

---

### 1.4 Catalyst Optimizer Deep Dive

Catalyst turns your DataFrame code into an optimized physical plan in 4 phases:

1. **Analysis**: resolves column/table names against the catalog, builds an unresolved → resolved logical plan (AST).
2. **Logical Optimization**: rule-based rewrites — predicate pushdown, projection pruning, constant folding, null propagation.
3. **Physical Planning**: generates multiple physical plans (e.g., which join strategy) and picks the cheapest using cost-based estimates.
4. **Code Generation**: compiles the chosen plan to JVM bytecode via the Janino compiler (Whole-Stage Code Gen).

```python
df1 = spark.table("sales")
df2 = df1.filter(col("year") == 2024).select("id", "amount")

df2.explain(mode="extended")
# Shows: Parsed Logical Plan -> Analyzed Logical Plan
#        -> Optimized Logical Plan -> Physical Plan
```

**Predicate pushdown example** — the filter is pushed down to the file scan itself (Parquet), so unneeded row groups are skipped entirely:

```python
spark.read.parquet("dbfs:/data/sales") \
    .filter(col("year") == 2024) \
    .explain()
# PushedFilters: [IsNotNull(year), EqualTo(year,2024)]
```

---

### 1.5 Project Tungsten Engine

- **Off-heap memory** via `sun.misc.Unsafe`: Spark manages its own binary memory layout outside the JVM heap, avoiding GC overhead and object header waste.
- **Compact binary format**: rows are stored as byte arrays (`UnsafeRow`) instead of Java objects.
- **Whole-Stage Code Generation (WSCG)**: fuses multiple operators (filter → project → aggregate) into a single generated Java function, avoiding virtual function call overhead — a CPU-cache-friendly execution model.

```python
spark.conf.set("spark.sql.codegen.wholeStage", "true")   # default is True

df.filter(col("amount") > 100).select("id").explain("codegen")
# Look for "*(1) Filter" / "*(1) Project" - the * means WSCG is active
```

---

### 1.6 Unified Memory Management Architecture

Per-executor JVM memory is divided as:

```
Total Executor Memory
 ├── Reserved Memory (~300MB, hardcoded, for Spark internals)
 ├── User Memory (spark.memory.fraction complement, ~40% typically)
 │      -> your own data structures, UDF objects
 └── Spark Memory (spark.memory.fraction, ~60% typically)
        ├── Storage Memory (cached DataFrames, broadcast vars)
        └── Execution Memory (shuffles, joins, sorts, aggregations)
```

- Storage and Execution memory **share a unified pool** and can borrow from each other, but Execution can evict Storage (cached blocks) if needed — never the other way for currently-running tasks.
- **Storage levels**: `MEMORY_ONLY`, `MEMORY_AND_DISK`, `MEMORY_AND_DISK_SER`, `DISK_ONLY`.

```python
from pyspark import StorageLevel

df.persist(StorageLevel.MEMORY_AND_DISK)
df.count()          # materializes the cache
df.unpersist()
```

**GC tuning**: G1GC is recommended for large heaps to reduce long stop-the-world pauses.

```
spark.executor.extraJavaOptions -XX:+UseG1GC
```

---

### 1.7 Partitioning & Data Distribution

**`repartition()` vs `coalesce()`**

| | `repartition(n)` | `coalesce(n)` |
|---|---|---|
| Shuffle | Full shuffle | No shuffle (merges adjacent partitions) |
| Can increase partitions? | Yes | No (only decreases) |
| Use case | Even distribution, before wide ops | Reducing partitions before a write |

```python
df = spark.range(0, 1000000)

df_more = df.repartition(50)          # full shuffle, evenly distributed
df_fewer = df.coalesce(4)             # cheap, no shuffle, may be unbalanced

# Partition by column (useful before groupBy/join on that column)
df_by_key = df.repartition(50, col("id"))
```

- **Hash Partitioner**: default for `groupBy`/`join` — partition = `hash(key) % numPartitions`.
- **Range Partitioner**: used by `sortBy`/`orderBy` — partitions data into contiguous sorted ranges.
- **Optimal partition size**: aim for **100–200 MB per partition** uncompressed. Too many small partitions = task scheduling overhead; too few large partitions = poor parallelism/spill risk.

```python
# Rule of thumb sizing
target_partitions = total_data_size_mb // 150
df = df.repartition(target_partitions)
```

---

### 1.8 Distributed Join Internals

| Join Strategy | When Used | Mechanism |
|---|---|---|
| **Broadcast Hash Join (BHJ)** | One side is small (< `autoBroadcastJoinThreshold`, default 10MB) | Small table sent whole to every executor; no shuffle |
| **Shuffle Hash Join (SHJ)** | Both sides large but one side small enough to build a hash map per partition | Both sides shuffled by key, hash map built on smaller side |
| **Sort-Merge Join (SMJ)** | Both sides large | Both sides shuffled + sorted by key, then merged — Spark's default for large-large joins |
| **Cartesian / Broadcast Nested Loop (BNLJ)** | No join key / non-equi join | Every row compared with every row — very expensive |

```python
from pyspark.sql.functions import broadcast

big = spark.table("fact_sales")
small = spark.table("dim_store")

# Force broadcast join
result = big.join(broadcast(small), "store_id")

# Check which strategy was actually chosen
result.explain()
# Look for: BroadcastHashJoin / SortMergeJoin / ShuffledHashJoin
```

```python
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 20 * 1024 * 1024)  # 20MB
```

---

### 1.9 PySpark Architecture & Optimization

- **Py4J gateway**: PySpark's driver-side Python process talks to the JVM driver over a Py4J socket gateway. All DataFrame API calls are just building a logical plan on the JVM side — this part is **not** slow.
- **Python worker IPC**: for actual row-by-row Python execution (plain UDFs), each executor spawns a Python subprocess; data is **serialized (pickled)**, sent over a socket, processed in Python, and sent back. This round trip is the main PySpark performance tax.
- **PyArrow vectorization & Pandas UDFs**: instead of row-by-row pickling, Arrow batches data in a columnar format and passes it to Python in bulk — 10-100x faster than plain UDFs.

```python
import pandas as pd
from pyspark.sql.functions import pandas_udf

# Scalar Pandas UDF - vectorized, Arrow-backed
@pandas_udf("double")
def celsius_to_fahrenheit(temp: pd.Series) -> pd.Series:
    return temp * 9/5 + 32

df.withColumn("temp_f", celsius_to_fahrenheit(col("temp_c")))

# Grouped Map Pandas UDF - apply a function per group, returns a DataFrame
def subtract_mean(pdf: pd.DataFrame) -> pd.DataFrame:
    pdf["value"] = pdf["value"] - pdf["value"].mean()
    return pdf

df.groupBy("group_id").applyInPandas(subtract_mean, schema=df.schema)
```

```python
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
```

**Interview tip:** *Always prefer built-in `pyspark.sql.functions` over UDFs. If a UDF is unavoidable, use a Pandas UDF over a plain Python UDF for the Arrow speedup.*

---

## Module 2: Delta Lake Deep Dive & Storage Mechanics

### 2.1 Transaction Log (`_delta_log`) Internals

- Every write produces a new JSON commit file: `_delta_log/00000000000000000000.json`, `...0001.json`, etc.
- Each commit records **actions**: `add` (new file), `remove` (tombstoned file), `metaData`, `commitInfo`.
- **CRC files** store per-commit statistics for fast validation.
- Every 10 commits (default), Delta writes a **checkpoint** (`.checkpoint.parquet`) — a compacted snapshot of the log so readers don't need to replay every JSON file from version 0.

```python
# Inspect the log directly
display(spark.read.json("dbfs:/path/to/table/_delta_log/00000000000000000000.json"))

# Table history (built from the log)
from delta.tables import DeltaTable
dt = DeltaTable.forPath(spark, "dbfs:/path/to/table")
dt.history().select("version", "timestamp", "operation").show()
```

---

### 2.2 Concurrency Control & Isolation

- Delta uses **Optimistic Concurrency Control (OCC)**: a writer reads the current table version, applies changes, and tries to commit a new version. If another writer committed first, Delta checks for **conflicts** and retries automatically if the operations don't overlap.
- **Isolation levels**:
  - `WriteSerializable` (default): allows more concurrency, small edge cases of serialization anomalies.
  - `Serializable`: strictest, fully prevents anomalies at the cost of more conflicts.

```python
spark.sql("""
ALTER TABLE my_table SET TBLPROPERTIES (
  'delta.isolationLevel' = 'Serializable'
)
""")
```

```python
# Conflict example: two concurrent MERGE operations on overlapping partitions
# will cause one to fail with a ConcurrentAppendException and retry
```

---

### 2.3 Storage Layout Optimization

```python
# Compact small files into ~1GB target files
spark.sql("OPTIMIZE my_table")

# Optimize with Z-order (see 2.5)
spark.sql("OPTIMIZE my_table ZORDER BY (customer_id)")

# Remove old, unreferenced data files (tombstones) beyond retention window
spark.sql("VACUUM my_table RETAIN 168 HOURS")   # default 7 days

# Enable auto-compaction + optimized writes at table level
spark.sql("""
ALTER TABLE my_table SET TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
)
""")
```

**Interview answer:** *"`OPTIMIZE` bin-packs many small files into fewer, larger files (~1GB) to reduce metadata overhead and improve scan speed. `VACUUM` physically deletes files no longer referenced by the log after the retention window, freeing storage — but it breaks time travel to versions that needed those files."*

---

### 2.4 Deletion Vectors (DV)

- Traditional Delta `UPDATE`/`DELETE`/`MERGE` = **Copy-on-Write**: rewrite the entire Parquet file even to change one row.
- **Deletion Vectors** = **Merge-on-Read**: instead of rewriting the file, a small bitmap file (`.pvd`) marks specific row IDs as deleted/updated. Readers apply the bitmap at read time.
- Massively reduces **write amplification** for selective updates/deletes.

```python
spark.sql("""
ALTER TABLE my_table SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')
""")

# A single-row delete now only writes a small DV file, not a rewritten Parquet file
spark.sql("DELETE FROM my_table WHERE id = 42")
```

---

### 2.5 Data Skipping & Multi-Dimensional Clustering

- **Hive-style partitioning** (`/year=2024/month=01/`): great for low-cardinality, coarse filters but causes small-file and over-partitioning problems on high-cardinality columns.
- **Z-Ordering**: co-locates related data across *multiple* columns using a space-filling curve, so Delta's file-level min/max statistics can skip more files for multi-column filters.
- **Liquid Clustering** (`CLUSTER BY`): the modern replacement — clusters data incrementally without fixed partition boundaries, works well even as query patterns change, and doesn't require choosing partition columns upfront.

```python
# Z-Order on two frequently-filtered columns
spark.sql("OPTIMIZE sales ZORDER BY (customer_id, order_date)")

# Liquid Clustering (newer approach - no need to pick partition columns)
spark.sql("""
CREATE TABLE sales_clustered (
  customer_id STRING, order_date DATE, amount DOUBLE
) CLUSTER BY (customer_id, order_date)
""")
spark.sql("OPTIMIZE sales_clustered")   # re-clusters incrementally
```

---

### 2.6 Time Travel & Metadata Recovery

```python
# Query a prior version
df_v5 = spark.read.format("delta").option("versionAsOf", 5).load("dbfs:/path/to/table")

# Query as of a timestamp
df_ts = spark.read.format("delta").option("timestampAsOf", "2024-01-01").load("dbfs:/path/to/table")

# Restore the table to an earlier version (writes a new commit, doesn't delete history)
spark.sql("RESTORE TABLE my_table TO VERSION AS OF 5")
```

Time travel works because the transaction log can reconstruct the exact set of `add`/`remove` files valid at any given version.

---

### 2.7 Change Data Feed (CDF)

```python
# Enable CDF on a table
spark.sql("ALTER TABLE my_table SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

# Read only the changes between two versions
changes = spark.read.format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", 10) \
    .option("endingVersion", 15) \
    .table("my_table")

changes.select("id", "amount", "_change_type", "_commit_version").show()
# _change_type: insert | update_preimage | update_postimage | delete
```

CDF is the backbone for incrementally propagating changes downstream (e.g., Bronze → Silver) without rescanning the whole table.

---

### 2.8 Table Cloning Mechanics

```python
# Shallow clone: copies only metadata, points to the SAME data files (zero-copy, instant)
spark.sql("CREATE TABLE staging_table SHALLOW CLONE prod_table")

# Deep clone: copies metadata AND all data files (full physical copy)
spark.sql("CREATE TABLE backup_table DEEP CLONE prod_table")
```

**Use cases**: shallow clones for cheap staging/testing environments or ML experiments; deep clones for backups or migrating a table to a different location entirely.

---

### 2.9 UniForm (Universal Format)

- **UniForm** lets a single physical Delta table also be read as **Iceberg** or **Hudi**, by asynchronously generating the metadata those formats expect alongside the native Delta log.
- No data duplication — one copy of Parquet data, multiple metadata "views."

```python
spark.sql("""
ALTER TABLE my_table SET TBLPROPERTIES (
  'delta.universalFormat.enabledFormats' = 'iceberg'
)
""")
```

---

## Module 3: Compute Architecture, Cluster Sizing & Engine Optimization

### 3.1 Compute Topologies & Types

| Type | Use Case | Lifecycle |
|---|---|---|
| All-Purpose Cluster | Interactive notebooks, ad-hoc dev | Manually started/stopped, shared |
| Automated Job Cluster | Scheduled production jobs | Spun up per job run, terminated after |
| Serverless Compute | Fast-start ad-hoc or job workloads | Fully managed, no cluster config |

- **Databricks Container Services (DCS)**: lets you supply a custom Docker image for the cluster runtime — useful for custom system libraries.

---

### 3.2 Cluster Access & Isolation Modes

| Mode | Description |
|---|---|
| **Single User** | Assigned to exactly one user/service principal; full access incl. RDD API |
| **Shared User Isolation** | Multiple users on one cluster; enforces Unity Catalog permissions per user, restricts some low-level APIs |
| **No Isolation (Legacy)** | Shared cluster with no per-user credential isolation — deprecated, avoid for governed data |

---

### 3.3 Adaptive Query Execution (AQE)

AQE re-optimizes the physical plan at runtime using actual shuffle statistics instead of static estimates.

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")  # default True in modern DBR

spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")  # merges tiny post-shuffle partitions
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")            # splits skewed partitions automatically
```

Three key AQE features:
1. **Dynamic coalescing** of post-shuffle partitions (fixes the "200 shuffle partitions is too many for small data" problem).
2. **Dynamic join strategy switching**: a planned Sort-Merge Join can be switched to a Broadcast Hash Join at runtime if actual data turns out small.
3. **Dynamic skew handling**: splits an oversized skewed partition into smaller sub-tasks automatically.

---

### 3.4 Dynamic File & Partition Pruning

- **Dynamic Partition Pruning (DPP)**: when joining a fact table to a filtered dimension table, Spark pushes the dimension's filter into the fact table scan to skip whole partitions.
- **Dynamic File Pruning (DFP)**: same idea but at the file/data-skipping level for Delta tables (uses file-level min/max stats).

```python
# Example that benefits from DPP: filtering dim then joining to fact
dim_filtered = spark.table("dim_date").filter(col("year") == 2024)
fact = spark.table("fact_sales")

fact.join(dim_filtered, "date_id").explain()
# Look for "PartitionFilters" / "dynamicpruning#" in the plan
```

---

### 3.5 Caching Architectures

```python
# Spark in-memory cache - explicit, counts against Storage Memory
df.cache()          # shorthand for persist(MEMORY_AND_DISK)
df.count()          # materialize

# Databricks Disk Cache (a.k.a. IO cache) - automatic, local NVMe SSD caching
# of remote Parquet/Delta files. No code change needed; controlled at cluster level.
```

| | Spark Cache | Databricks Disk Cache |
|---|---|---|
| Level | In-memory (JVM) | Local disk (NVMe) |
| Trigger | Explicit `.cache()` | Automatic on read |
| Survives job end | No | Yes (as long as cluster is up) |
| Best for | Iterative reuse in one job | Repeated reads of same files across jobs |

---

### 3.6 Cluster Sizing & Resource Allocation

**Instance families:**
- **Compute-Optimized**: CPU-bound transforms, heavy shuffles.
- **Memory-Optimized**: large joins/aggregations, caching.
- **Storage-Optimized**: Delta Disk Cache-heavy workloads needing fast local NVMe.

**Executor sizing golden rule**: 4–5 cores per executor is the sweet spot — more cores per executor increases HDFS/I/O contention and GC pause impact.

```python
# Example spark-submit style config
# --executor-cores 5
# --executor-memory 20g
# --num-executors 10
```

- **Driver memory**: must be large enough to hold broadcast variables and collect small results; a driver OOM is common when `.collect()` pulls too much data back, or the broadcast side of a join is bigger than expected.

---

### 3.7 Cost Management & Governance

```python
# Common production pattern: On-Demand driver (stability) + Spot workers (cost savings)
# Configured in cluster JSON:
{
  "driver_node_type_id": "i3.xlarge",
  "aws_attributes": {"availability": "SPOT_WITH_FALLBACK", "first_on_demand": 1}
}
```

- **Enhanced Autoscaling**: scales workers up/down based on actual queue depth/shuffle needs, and scales down more aggressively than legacy autoscaling to save cost.
- **Auto-termination**: idle clusters shut down after N minutes — critical cost control for all-purpose clusters.
- **Workload tagging**: tag clusters/jobs for chargeback via `custom_tags`, queried later from `system.billing.usage`.

---

### 3.8 Serverless Compute Architecture

- Starts in **under ~20 seconds** because Databricks maintains a warm pool of pre-initialized compute.
- Available as **Serverless Workflows** (jobs), **Serverless SQL Warehouses** (BI/SQL), and **Serverless Notebooks** (interactive).
- No cluster sizing/config needed — Databricks manages scaling transparently, billed per-second of actual usage.

---

## Module 4: Ingestion Patterns, Structured Streaming & Delta Live Tables (DLT)

### 4.1 Auto Loader (`cloudFiles`)

```python
df = (spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "json")
      .option("cloudFiles.schemaLocation", "dbfs:/schema/checkpoint")
      .load("dbfs:/raw/events/"))

df.writeStream \
  .option("checkpointLocation", "dbfs:/checkpoints/bronze_events") \
  .table("bronze_events")
```

- **Directory Listing mode**: periodically lists cloud storage directories for new files — simple, no extra cloud setup, but scales worse with huge directories.
- **File Notification mode**: subscribes to cloud events (S3 → SNS/SQS, ADLS → Event Grid) — scales to millions of files, near real-time, needs extra IAM/event setup.
- Auto Loader tracks which files it has already ingested using an internal **RocksDB**-backed state store, so restarts don't reprocess files.

---

### 4.2 Schema Evolution & Quality Defense

```python
df = (spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "json")
      .option("cloudFiles.schemaLocation", "dbfs:/schema/checkpoint")
      .option("cloudFiles.schemaEvolutionMode", "addNewColumns")  # or "rescue", "failOnNewColumns", "none"
      .option("cloudFiles.rescuedDataColumn", "_rescued_data")
      .load("dbfs:/raw/events/"))
```

- New/unexpected fields that don't match the inferred schema land in `_rescued_data` (a JSON string column) instead of causing job failure or silent data loss.

---

### 4.3 Structured Streaming Core Architecture

- **Micro-batch engine** (default): processes data in small batches at a configurable trigger interval — latency in seconds.
- **Continuous Processing engine**: experimental, low-latency (~1ms) mode with tighter operator support restrictions.

```python
(spark.readStream.format("delta").table("bronze")
 .writeStream
 .foreachBatch(lambda batch_df, batch_id: batch_df.write.mode("append").saveAsTable("silver"))
 .option("checkpointLocation", "dbfs:/checkpoints/silver")
 .start())
```

- `foreachBatch` lets you run arbitrary batch logic (e.g., `MERGE INTO`) per micro-batch — the standard pattern for streaming upserts.
- **Exactly-once**: guaranteed end-to-end via checkpointing + idempotent sinks (Delta tables are idempotent-write friendly).

---

### 4.4 State Management & Watermarking

```python
from pyspark.sql.functions import window

events = spark.readStream.format("delta").table("bronze_events")

agg = (events
       .withWatermark("event_time", "10 minutes")
       .groupBy(window("event_time", "5 minutes"), "device_id")
       .count())

agg.writeStream.outputMode("update").format("delta") \
   .option("checkpointLocation", "dbfs:/checkpoints/agg") \
   .table("device_counts")
```

- **Watermarking**: tells Spark "I won't wait for events more than 10 minutes late" — lets it safely drop old state and avoid unbounded memory growth.
- **State Store backends**: default in-memory/HDFS-backed store vs **RocksDB StateStore** (recommended for large stateful streams — better memory efficiency, spills to local disk).
- **Output modes**: `Append` (only final rows, no updates to previous output), `Update` (only changed rows), `Complete` (entire result table every trigger — only for aggregations).

---

### 4.5 Fault Tolerance & Trigger Controls

```python
query = (df.writeStream
          .format("delta")
          .option("checkpointLocation", "dbfs:/checkpoints/job1")
          .trigger(availableNow=True)     # process all available data then stop (great for scheduled jobs)
          # .trigger(processingTime="1 minute")  # micro-batch every 1 min
          # .trigger(continuous="1 second")      # continuous processing mode
          .table("target_table"))
```

The checkpoint directory stores `offsets/` (what's been read), `commits/` (what's been fully processed), and `state/` (operator state) — this is what allows exact recovery after a failure.

---

### 4.6 & 4.7 Delta Live Tables (DLT): Declarative Framework & Table Constructs

```python
import dlt
from pyspark.sql.functions import col

@dlt.table(comment="Raw ingested events")
def bronze_events():
    return (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .load("dbfs:/raw/events/"))

@dlt.table(comment="Cleaned, deduplicated events")
def silver_events():
    return (dlt.read_stream("bronze_events")
            .dropDuplicates(["event_id"])
            .filter(col("event_time").isNotNull()))

@dlt.table(comment="Daily aggregated metrics")
def gold_daily_metrics():
    return (dlt.read("silver_events")
            .groupBy("event_date")
            .count())
```

- You declare **what** each table should contain; DLT compiles the dependency graph and DAG automatically (no manual orchestration).
- **Streaming Tables**: incremental, append-oriented (`dlt.read_stream`).
- **Materialized Views**: fully recomputed/maintained aggregates (`dlt.read`, plain `@dlt.table` without streaming source).
- **Temporary Views**: `@dlt.view` — intermediate transformations not persisted as tables, scoped to the pipeline.

---

### 4.8 Data Quality Expectations in DLT

```python
@dlt.table
@dlt.expect("valid_amount", "amount > 0")                       # track & warn, keep row
@dlt.expect_or_drop("valid_id", "id IS NOT NULL")                # drop bad rows silently
@dlt.expect_or_fail("valid_currency", "currency IN ('USD','EUR')")  # halt pipeline on violation
def validated_orders():
    return dlt.read_stream("bronze_orders")
```

**Quarantine pattern**: route failing rows to a separate table instead of dropping them outright, so they can be inspected/reprocessed later.

```python
@dlt.table
def orders_quarantine():
    return (dlt.read_stream("bronze_orders")
            .filter("amount <= 0 OR id IS NULL"))
```

---

### 4.9 DLT Observability & Maintenance

```python
# Query the pipeline event log for data quality metrics, lineage, and run history
event_log_df = spark.read.format("delta").load("dbfs:/pipelines/<pipeline-id>/system/events")
event_log_df.selectExpr("details:flow_progress.data_quality").show(truncate=False)
```

DLT pipelines automatically run `VACUUM`/`OPTIMIZE` on managed tables as part of their maintenance cycle — no manual scheduling needed.

---

## Module 5: Data Governance, Security & Unity Catalog

### 5.1 Unity Catalog Architecture & Namespace

- **3-level namespace**: `catalog.schema.table_or_view` (adds a layer above the classic `schema.table`).
- One **metastore** can be attached to multiple workspaces, giving a single governance plane across an org.
- **SCIM** syncs users/groups from an identity provider (Azure AD, Okta) into Databricks automatically.

```python
spark.sql("USE CATALOG main")
spark.sql("USE SCHEMA sales")
spark.sql("SELECT * FROM main.sales.orders LIMIT 10")
```

---

### 5.2 Cloud Storage Security & Abstraction

```sql
-- Storage Credential: wraps a cloud IAM role / Managed Identity
CREATE STORAGE CREDENTIAL my_cred
  WITH (AZURE_MANAGED_IDENTITY = '...');

-- External Location: a governed URI path built on top of the credential
CREATE EXTERNAL LOCATION my_location
  URL 's3://my-bucket/data/'
  WITH (STORAGE CREDENTIAL my_cred);
```

This separates *who can access which cloud path* from the *table definition itself* — no more embedding cloud keys in notebooks.

---

### 5.3 Table Lifecycles: Managed vs External

| | Managed Table | External Table |
|---|---|---|
| Storage location | Controlled by Unity Catalog | User-specified path |
| `DROP TABLE` | Deletes underlying data too | Only removes metadata, data stays |
| Use case | Standard governed tables | Data shared with other tools/systems |

```sql
CREATE TABLE main.sales.orders (id INT, amount DOUBLE);              -- Managed

CREATE TABLE main.sales.orders_ext (id INT, amount DOUBLE)
LOCATION 's3://my-bucket/orders/';                                    -- External
```

---

### 5.4 Volumes for Unstructured/Semi-Structured Data

```python
# POSIX-style path access to governed non-tabular files (images, PDFs, models)
df = spark.read.text("/Volumes/main/sales/raw_files/notes.txt")
```

```sql
CREATE VOLUME main.sales.raw_files;                     -- Managed Volume
CREATE EXTERNAL VOLUME main.sales.ext_files
  LOCATION 's3://my-bucket/files/';                      -- External Volume
```

---

### 5.5 Fine-Grained Security & Privacy Controls

```sql
-- Dynamic column masking
CREATE FUNCTION mask_ssn(ssn STRING)
RETURNS STRING
RETURN CASE WHEN is_account_group_member('hr_admin') THEN ssn ELSE 'XXX-XX-XXXX' END;

ALTER TABLE main.hr.employees ALTER COLUMN ssn SET MASK mask_ssn;

-- Row-level filtering
CREATE FUNCTION region_filter(region STRING)
RETURNS BOOLEAN
RETURN region = current_user() OR is_account_group_member('global_admin');

ALTER TABLE main.sales.orders SET ROW FILTER region_filter ON (region);
```

- **ABAC (Attribute-Based Access Control)**: apply access rules based on **governance tags** on columns/tables (e.g., tag a column `PII=true` and write one policy that applies to every tagged column, instead of one policy per table).

---

### 5.6 Privilege Hierarchy & Access Control

```sql
GRANT USAGE ON CATALOG main TO `data_engineers`;
GRANT SELECT ON SCHEMA main.sales TO `analysts`;
GRANT MODIFY ON TABLE main.sales.orders TO `etl_service_principal`;
GRANT EXECUTE ON FUNCTION main.sales.mask_ssn TO `hr_admin`;
REVOKE SELECT ON TABLE main.sales.orders FROM `contractor_group`;
```

Privileges are hierarchical: `USAGE` on a catalog/schema is required before any privilege on objects inside it takes effect.

---

### 5.7 Data Lineage Tracking

Unity Catalog automatically captures **table-level and column-level lineage** from every query executed through it — no extra instrumentation needed. Viewable via the **Catalog Explorer** UI or the `system.access.*` / lineage system tables, showing full upstream/downstream impact for a given table or column.

---

### 5.8 System Tables & Operational Auditing

```sql
-- Cost analysis
SELECT sku_name, SUM(usage_quantity) AS dbus
FROM system.billing.usage
GROUP BY sku_name;

-- Security audit trail
SELECT event_time, action_name, user_identity.email
FROM system.access.audit
WHERE action_name = 'deleteTable';

-- Job/workflow observability
SELECT * FROM system.lakeflow.job_run_timeline
WHERE result_state = 'FAILED';
```

---

### 5.9 Delta Sharing Protocol

An **open protocol** for sharing live Delta tables across organizations/clouds without copying data — the recipient reads directly via a REST-based sharing server, no vendor lock-in on the consumer side.

```sql
CREATE SHARE sales_share;
ALTER SHARE sales_share ADD TABLE main.sales.orders;
CREATE RECIPIENT partner_org;
GRANT SELECT ON SHARE sales_share TO RECIPIENT partner_org;
```

---

## Module 6: Production Operations, Orchestration & CI/CD

### 6.1 Databricks Workflows

```python
# Passing values between tasks in a multi-task job
dbutils.jobs.taskValues.set(key="row_count", value=df.count())

# In a downstream task:
count = dbutils.jobs.taskValues.get(taskKey="upstream_task", key="row_count", default=0)
```

Workflows support **conditional branching** (If/Else tasks based on task outcomes) and **For-Each** tasks (looping a task template over a list of parameters), all defined as a DAG of tasks across notebooks, Python scripts, SQL, DLT pipelines, and dbt.

---

### 6.2 Reliability, Failure Handling & Retries

```json
{
  "task_key": "load_silver",
  "timeout_seconds": 3600,
  "max_retries": 2,
  "min_retry_interval_millis": 60000,
  "retry_on_timeout": true
}
```

- **Partial run repair**: re-run only the failed tasks in a job, not the whole DAG — saves compute and time.
- **Alerting**: jobs can notify Slack/PagerDuty/webhooks on start, success, or failure.

---

### 6.3 Databricks Asset Bundles (DABs)

```yaml
# databricks.yml
bundle:
  name: sales_pipeline

targets:
  dev:
    workspace:
      host: https://dev-workspace.databricks.com
  prod:
    workspace:
      host: https://prod-workspace.databricks.com

resources:
  jobs:
    etl_job:
      name: sales-etl
      tasks:
        - task_key: main
          notebook_task:
            notebook_path: ./notebooks/etl.py
```

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run -t dev etl_job
```

DABs give you **Infrastructure as Code** for jobs/pipelines/clusters, with clean `dev → staging → prod` environment separation — the standard way to CI/CD Databricks projects.

---

### 6.4 Developer Tooling & Integrations

```bash
databricks clusters list
databricks jobs run-now --job-id 12345
```

- **Databricks Connect v2**: built on Spark Connect — lets you run PySpark code from a local IDE (VS Code, PyCharm) against a remote cluster, with real breakpoint debugging, without needing the code to physically execute on the cluster driver.
- **Git Folders**: native Git integration for notebooks/repos inside the workspace, supporting branches, PRs, and CI triggers.

---

### 6.5 Spark UI Profiling & Diagnostics

| Tab | What to look for |
|---|---|
| **Jobs** | Which stage in the timeline is the bottleneck |
| **Stages** | Task duration skew (some tasks taking far longer = data skew), GC time vs CPU time, shuffle read/write size |
| **SQL/DataFrame** | The actual physical plan chosen, whether Whole-Stage Code Gen (`*`) kicked in |
| **Executors** | Thread dumps (stuck tasks), memory/disk spill per executor, uneven task distribution |

```python
df.explain("formatted")   # readable physical plan, similar to what the SQL tab shows
```

---

### 6.6 Cluster & Infrastructure Observability

- **Ganglia metrics** (legacy) / cluster metrics UI: distinguish **CPU-bound** (high CPU, low network) vs **memory-bound** (high GC time) vs **network-bound** (shuffle-heavy, high network I/O) bottlenecks.
- **Log4j**: configure driver/executor logs to ship to external systems (CloudWatch, Azure Log Analytics) for centralized monitoring and alerting outside the Databricks UI.

---

## Module 7: Lakehouse System Design, CDC & Troubleshooting Playbooks

### 7.1 Medallion Architecture Blueprint

```python
# Bronze: raw, append-only, schema preserved, with audit columns
bronze = (spark.readStream.format("cloudFiles").option("cloudFiles.format", "json")
          .load("dbfs:/raw/")
          .withColumn("ingest_ts", current_timestamp())
          .withColumn("source_file", input_file_name()))
bronze.writeStream.table("bronze_orders")

# Silver: deduplicated, cleansed, conformed types
silver = (spark.readStream.table("bronze_orders")
          .dropDuplicates(["order_id"])
          .filter(col("order_id").isNotNull())
          .withColumn("amount", col("amount").cast("double")))
silver.writeStream.table("silver_orders")

# Gold: business-level aggregates / star schema facts
gold = (spark.read.table("silver_orders")
        .groupBy("customer_id", "order_date")
        .agg(sum("amount").alias("daily_spend")))
gold.write.mode("overwrite").saveAsTable("gold_customer_daily_spend")
```

- **Bronze**: source of truth for raw data, replayable.
- **Silver**: business entities, cleaned and conformed, still fairly granular.
- **Gold**: curated, aggregated, ready for BI/ML — star schemas, KPIs, feature tables.

---

### 7.2 Change Data Capture (CDC) Design Patterns

**SCD Type 1 (overwrite / in-place upsert)**

```python
from delta.tables import DeltaTable

target = DeltaTable.forName(spark, "dim_customer")
updates = spark.table("staged_customer_updates")

(target.alias("t")
 .merge(updates.alias("s"), "t.customer_id = s.customer_id")
 .whenMatchedUpdateAll()
 .whenNotMatchedInsertAll()
 .execute())
```

**SCD Type 2 (full history tracking)**

```python
from pyspark.sql.functions import lit, current_timestamp

target = DeltaTable.forName(spark, "dim_customer_scd2")
updates = spark.table("staged_customer_updates")

# Step 1: close out changed current records
(target.alias("t")
 .merge(updates.alias("s"), "t.customer_id = s.customer_id AND t.is_current = true")
 .whenMatchedUpdate(
     condition="t.address != s.address",
     set={"is_current": lit(False), "end_date": current_timestamp()})
 .execute())

# Step 2: insert new versions for changed/new records
new_versions = updates.withColumn("is_current", lit(True)) \
                       .withColumn("start_date", current_timestamp()) \
                       .withColumn("end_date", lit(None))
new_versions.write.mode("append").saveAsTable("dim_customer_scd2")
```

- **Streaming CDC**: combine Auto Loader (source) → CDF (`readChangeFeed`) → `foreachBatch` with a `MERGE` to propagate changes incrementally through Bronze → Silver → Gold without full-table rescans.

---

### 7.3 Memory Troubleshooting: Driver OOM Playbook

**Root causes**
- `.collect()` or `.toPandas()` on a large DataFrame — pulls the *entire* dataset into driver memory.
- An oversized broadcast join where the "small" side turned out to be huge.

**Remediation**

```python
# BAD - pulls everything to driver
pdf = big_df.toPandas()

# BETTER - sample or aggregate first
pdf = big_df.limit(10000).toPandas()

# Guard against runaway broadcasts
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)  # disable auto-broadcast entirely
```

Also increase driver memory (`spark.driver.memory`) if broadcasts are legitimately large, and prefer writing results to a table over `.collect()`.

---

### 7.4 Memory Troubleshooting: Executor OOM & Disk Spill Playbook

**Root causes**: severe data skew concentrating too much data on one task, too many cores per executor competing for the same memory pool, insufficient `memoryOverhead` for off-heap/native library usage (common with Pandas UDFs).

```python
# Reduce cores-per-executor to lower per-task memory pressure
# --executor-cores 4  (instead of 8)

# Increase off-heap overhead for Python/Arrow-heavy workloads
# --conf spark.executor.memoryOverhead=4g
```

---

### 7.5 Data Skew Mitigation Playbook

**Detection**: in the Spark UI Stages tab, look at the task duration distribution — a handful of tasks taking 10-100x longer than the median is classic skew.

**Fixes**

```python
from pyspark.sql.functions import rand, concat, lit, floor

# Salting technique for skewed join keys
salt_count = 10
big_salted = big_df.withColumn("salt", floor(rand() * salt_count))
small_salted = small_df.withColumn("salt", floor(rand() * salt_count))  # or explode across all salts

big_salted.join(small_salted, ["join_key", "salt"])

# Or let AQE handle it automatically
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
```

---

### 7.6 Storage Optimization Playbook

- **Small file problem**: too many tiny files (common with streaming micro-batches) → fix with `OPTIMIZE`, auto-compaction, or larger streaming trigger intervals.
- **Over-partitioning on high-cardinality keys**: partitioning by e.g. `customer_id` creates millions of tiny partition directories — prefer Z-Ordering or Liquid Clustering over Hive partitioning for high-cardinality columns.

```python
spark.sql("ALTER TABLE events SET TBLPROPERTIES ('delta.autoOptimize.autoCompact' = 'true')")
spark.sql("OPTIMIZE events ZORDER BY (customer_id)")
```

---

### 7.7 Streaming Bottleneck & Incident Triage

- **RocksDB state store exhaustion**: reduce state TTL, tighten watermark thresholds, or scale up executor memory/local disk for state.
- **Watermark lag**: if the watermark keeps falling behind real event time, late data keeps arriving beyond the threshold and gets silently dropped — check upstream ingestion delay.
- **Cloud storage throttling (HTTP 503/429)**: implement exponential backoff, reduce the number of concurrent list/read operations (e.g., prefer File Notification mode over Directory Listing at high file volumes).

```python
# Example: widening the watermark if legitimate late data is being dropped
events.withWatermark("event_time", "30 minutes")  # increased from 10 minutes
```

---

## Quick-Fire Interview Q&A Cheat Sheet

| Question | One-line Answer |
|---|---|
| Narrow vs wide transformation? | Narrow = no shuffle, one-to-one partition mapping; wide = shuffle required |
| Why avoid `.collect()` on large data? | Pulls all data to the driver, causing driver OOM |
| `repartition` vs `coalesce`? | `repartition` = full shuffle, can increase partitions; `coalesce` = cheap, decrease-only |
| What triggers a Spark job? | An action (`.collect()`, `.count()`, `.write()`, `.show()`) |
| What does `OPTIMIZE` do? | Compacts small files into larger ones (~1GB) for faster scans |
| What does `VACUUM` do? | Physically deletes old unreferenced data files past the retention period |
| Deletion Vectors purpose? | Merge-on-read updates/deletes without rewriting whole Parquet files |
| Z-Order vs partitioning? | Z-Order co-locates data across multiple columns for multi-column data skipping; partitioning is a physical directory split on one/few low-cardinality columns |
| Why use Pandas UDFs over plain UDFs? | Arrow-based vectorized execution avoids row-by-row Python serialization overhead |
| AQE main benefits? | Runtime partition coalescing, dynamic join strategy switching, automatic skew handling |
| Broadcast join threshold default? | 10MB (`spark.sql.autoBroadcastJoinThreshold`) |
| SCD Type 1 vs Type 2? | Type 1 overwrites in place; Type 2 preserves history with `is_current`/`start_date`/`end_date` |
| Shallow vs Deep clone? | Shallow = metadata-only, zero-copy; Deep = full physical data copy |
