# Databricks & PySpark Interview Preparation Guide

## Table of Contents

1. [Module 1: Apache Spark Core & PySpark Internals](#module-1-apache-spark-core--pyspark-internals)
2. [Module 2: Delta Lake Deep Dive & Storage Mechanics](#module-2-delta-lake-deep-dive--storage-mechanics)
3. [Module 3: Compute Architecture, Cluster Sizing & Engine Optimization](#module-3-compute-architecture-cluster-sizing--engine-optimization)
4. [Module 4: Ingestion Patterns, Structured Streaming & Delta Live Tables (DLT)](#module-4-ingestion-patterns-structured-streaming--delta-live-tables-dlt)
5. [Module 5: Data Governance, Security & Unity Catalog](#module-5-data-governance-security--unity-catalog)
6. [Module 6: Production Operations, Orchestration & CI/CD](#module-6-production-operations-orchestration--cicd)
7. [Module 7: Lakehouse System Design, CDC & Troubleshooting Playbooks](#module-7-lakehouse-system-design-cdc--troubleshooting-playbooks)
8. [Module 8: ML, GenAI & Emerging Databricks Services](#module-8-ml-genai--emerging-databricks-services)
9. [Module 9: PySpark & SQL Command/Function Cookbook](#module-9-pyspark--sql-commandfunction-cookbook)
10. [Module 10: Performance & Optimization Playbook — Scenario-Based Decisions](#module-10-performance--optimization-playbook--scenario-based-decisions)

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

**Cost-Based Optimization (CBO) & `ANALYZE TABLE`**

By default Spark's physical planner uses simple heuristics (e.g. "is this table under the broadcast threshold?"). To make genuinely cost-based decisions (best join order for a multi-way join, choosing SMJ vs BHJ based on *actual* row counts rather than just file size), Spark needs **table/column statistics**, which are not collected automatically — you must run `ANALYZE TABLE`.

```sql
-- Table-level stats (row count, size in bytes)
ANALYZE TABLE sales COMPUTE STATISTICS;

-- Column-level stats (min, max, distinct count, null count, histogram)
ANALYZE TABLE sales COMPUTE STATISTICS FOR COLUMNS customer_id, order_date;
```

```python
spark.conf.set("spark.sql.cbo.enabled", "true")
spark.conf.set("spark.sql.cbo.joinReorder.enabled", "true")   # reorder multi-way joins by estimated cost
```

**Interview answer:** *"Predicate/projection pushdown and constant folding are rule-based (always applied). Join reordering and join-strategy selection based on real cardinalities are cost-based and require statistics from `ANALYZE TABLE` — without them, the CBO falls back to conservative defaults, which is why stale/missing statistics are a common cause of a bad join plan being chosen."* In modern Databricks Runtime, AQE (see 3.3) reduces reliance on CBO by re-planning using **actual runtime** shuffle statistics instead of pre-computed ones.

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

**SQL join hints** — the SQL-syntax equivalent of `broadcast()`, useful in `spark.sql()` string queries or when you want to force a specific strategy regardless of size estimates:

```sql
SELECT /*+ BROADCAST(dim_store) */ *
FROM fact_sales JOIN dim_store ON fact_sales.store_id = dim_store.store_id;

SELECT /*+ MERGE(a, b) */ * FROM a JOIN b ON a.id = b.id;              -- force Sort-Merge Join
SELECT /*+ SHUFFLE_HASH(a, b) */ * FROM a JOIN b ON a.id = b.id;        -- force Shuffle Hash Join
SELECT /*+ SHUFFLE_REPLICATE_NL(a, b) */ * FROM a JOIN b;               -- force Broadcast Nested Loop
```

```python
# Same hints work via DataFrame API too
df1.hint("broadcast").join(df2, "id")
df1.hint("merge").join(df2, "id")
df1.hint("shuffle_hash").join(df2, "id")
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

### 1.10 Photon Engine

**Photon** is Databricks' native, vectorized query engine written in C++ (not JVM), designed as a drop-in replacement for Spark's execution engine at the operator level. It's the single most-asked "Databricks-specific" performance question after Delta Lake.

| | Tungsten / WSCG (open-source Spark) | Photon (Databricks-only) |
|---|---|---|
| Language | JVM bytecode (generated via Janino) | Native C++ |
| Execution model | Whole-Stage Code Gen, row-at-a-time-ish fused loops | True columnar, vectorized batch processing (SIMD) |
| Scope | All Spark, open source | Databricks Runtime only |
| Transparent to code? | Yes | Yes — no code changes; falls back to Spark JVM engine for unsupported operators |
| Best gains on | General workloads | Scans, filters, aggregations, joins, writes to Parquet/Delta — especially I/O and shuffle heavy SQL/DataFrame workloads |

```python
# Photon is enabled at the CLUSTER level (a runtime + checkbox choice), not via a Spark conf.
# You verify it's active via the Spark UI / query profile:
df.write.format("delta").saveAsTable("sales_photon")
# In the query's "Query Profile" (Databricks SQL) or Spark UI SQL tab,
# operators prefixed like "Photon" (e.g., PhotonScan, PhotonShuffleExchange)
# indicate Photon executed that operator instead of the JVM engine.
```

**Interview answer:** *"Photon is a native, vectorized C++ replacement for parts of Spark's JVM execution engine. It operates on columnar batches using SIMD instructions instead of the JVM's whole-stage-codegen row-oriented fused loops. It's transparent — your DataFrame/SQL code doesn't change — but not every operator is Photon-accelerated; unsupported operators silently fall back to the regular Spark engine, which is why a query can show a mix of Photon and non-Photon nodes in its plan."*

---

### 1.11 Accumulators & Broadcast Variables

Two low-level shared-variable mechanisms, distinct from the broadcast **join** discussed in 1.8:

**Broadcast variables** — read-only variable cached on each executor once instead of shipped with every task (useful for large lookup maps used inside a UDF).

```python
lookup = {"US": "United States", "IN": "India", "UK": "United Kingdom"}
broadcast_lookup = spark.sparkContext.broadcast(lookup)

from pyspark.sql.functions import udf
@udf("string")
def expand_country(code):
    return broadcast_lookup.value.get(code, "Unknown")

df.withColumn("country_name", expand_country(col("country_code")))
```

**Accumulators** — write-only (from executors' perspective) shared counters, aggregated back to the driver. Common use: counting bad records during a transformation.

```python
bad_record_count = spark.sparkContext.accumulator(0)

def validate(row):
    global bad_record_count
    if row.amount is None or row.amount < 0:
        bad_record_count.add(1)
        return False
    return True

filtered_rdd = df.rdd.filter(validate)
filtered_rdd.count()          # action - triggers evaluation
print(bad_record_count.value) # read on driver AFTER an action has run
```

**Classic gotcha (frequently asked):** accumulators updated inside a **transformation** (not an action) can be **double-counted** if a task is retried or recomputed (e.g., due to executor failure, or if the RDD is used in two separate actions without caching) — Spark guarantees exactly-once accumulator updates only for actions, not transformations. Always be skeptical of accumulator values used for anything beyond rough debugging counters.

---

### 1.12 Checkpointing as a Performance Feature (Lineage Truncation)

Distinct from **Structured Streaming checkpoints** (Module 4.5, which store offsets/state for fault recovery). **RDD/DataFrame checkpointing** is a performance and stability feature: it **physically materializes** a DataFrame to reliable storage (DBFS/cloud storage) and **truncates its lineage graph**.

**Why it matters:**
- Very long transformation chains (common in iterative algorithms — ML training loops, graph algorithms, recursive feature engineering) build an increasingly large DAG. Recomputing that DAG on any partial failure gets more expensive with every iteration.
- A huge lineage can also cause driver-side stack overflow / very slow DAG serialization.
- Checkpointing writes the current state to disk once, and any future recomputation starts fresh from that checkpoint instead of replaying the entire original lineage.

```python
spark.sparkContext.setCheckpointDir("dbfs:/tmp/checkpoints/")

df = spark.range(0, 1000000)
for i in range(20):                       # simulate a long iterative chain
    df = df.withColumn(f"col_{i}", col("id") + i)

df.checkpoint()      # eager by default - materializes immediately, truncates lineage
# df.checkpoint(eager=False)   # lazy variant - materializes on next action

df.count()            # subsequent actions use the checkpointed data, not the 20-step lineage
```

| | `.cache()` / `.persist()` | `.checkpoint()` |
|---|---|---|
| Storage | Memory (+ optional disk spill) | Reliable storage (disk/cloud) |
| Truncates lineage? | **No** — original DAG is retained for recovery | **Yes** — lineage is cut, cannot recompute past this point if the checkpoint itself is lost |
| Survives executor loss? | Recomputes from lineage if lost | Reads from durable storage, no recompute needed |
| Typical use case | Reusing a DataFrame across multiple actions in one job | Very long iterative chains, breaking up massive DAGs, avoiding stack overflow in recursive pipelines |

**Interview answer:** *"Caching keeps data in memory but keeps the lineage around as a recovery fallback. Checkpointing writes data to durable storage and discards the lineage entirely, which is the only way to truly bound DAG growth in long iterative jobs — the trade-off is it's an eager disk write, so it has an upfront I/O cost that caching doesn't."*

---

### 1.13 Complex & Nested Data Types — Common Coding Patterns

A very common practical/coding-round area: manipulating semi-structured data (arrays, structs, maps) and doing everyday string/date wrangling without dropping into UDFs.

```python
from pyspark.sql.functions import (
    explode, posexplode, col, struct, array, map_from_arrays,
    to_date, date_add, datediff, regexp_extract, regexp_replace,
    concat_ws, split, collect_list, collect_set, first
)

# Sample nested data
data = [(1, "Alice", ["python", "sql"], {"city": "Pune", "zip": "411001"})]
df = spark.createDataFrame(data, ["id", "name", "skills", "address"])

# explode array -> one row per element
df.select("id", "name", explode("skills").alias("skill")).show()

# posexplode -> keep the position/index too
df.select("id", posexplode("skills").alias("pos", "skill")).show()

# Access map / struct fields
df.select(col("address")["city"].alias("city")).show()

# Build a struct column (grouping related fields)
df.withColumn("info", struct(col("name"), col("address")))

# Flatten a nested struct read from JSON
json_df = spark.read.json("dbfs:/data/nested.json")   # e.g. column "user.address.city"
flat_df = json_df.select("id", col("user.address.city").alias("city"))
```

**String & date utilities frequently asked in coding rounds:**

```python
df2 = spark.createDataFrame([("2024-01-15", "Order-1023-XL")], ["order_date", "sku"])

df2.select(
    to_date(col("order_date")).alias("dt"),
    date_add(to_date(col("order_date")), 7).alias("dt_plus_7"),
    regexp_extract(col("sku"), r"Order-(\d+)-", 1).alias("order_id"),
    regexp_replace(col("sku"), "-", "_").alias("sku_clean")
).show()

# Pivot / Unpivot
sales = spark.createDataFrame([("A", "Jan", 100), ("A", "Feb", 150), ("B", "Jan", 200)],
                               ["store", "month", "amount"])
sales.groupBy("store").pivot("month").sum("amount").show()   # wide format

# collect_list/collect_set - aggregate rows into an array per group
df.groupBy("id").agg(collect_list("name").alias("names"))
```

---

### 1.14 Bucketing

**Bucketing** pre-shuffles and pre-sorts data at **write time**, storing rows into a fixed number of hash-based buckets by a key column. If two tables are bucketed identically on their join key, Spark can perform a **join without a shuffle at query time** (both sides are already partitioned compatibly on disk) — a huge win for tables that are joined repeatedly.

```python
(df.write
   .bucketBy(8, "customer_id")
   .sortBy("customer_id")
   .mode("overwrite")
   .saveAsTable("main.sales.orders_bucketed"))

# A second table bucketed the SAME way (same bucket count + column) joins without a shuffle
(dim_df.write
   .bucketBy(8, "customer_id")
   .sortBy("customer_id")
   .mode("overwrite")
   .saveAsTable("main.sales.customers_bucketed"))

spark.table("main.sales.orders_bucketed") \
     .join(spark.table("main.sales.customers_bucketed"), "customer_id") \
     .explain()   # no Exchange (shuffle) node for either side if bucketing matches
```

**Trade-offs:** bucket count is fixed at write time — repartitioning/rebalancing later requires a full table rewrite, and it only helps if the *same* join key is reused repeatedly. On Databricks, **Liquid Clustering** (2.5) has largely superseded manual bucketing for most new tables, since it doesn't lock you into a fixed bucket count upfront — but bucketing is still a fair-game interview topic and appears in legacy Hive-influenced pipelines.

---

### 1.15 `mapPartitions` & `foreachPartition`

`map`/`foreach` invoke your function **once per row**. `mapPartitions`/`foreachPartition` invoke it **once per partition**, handing you an iterator over that partition's rows — the standard pattern when a per-record cost (opening a DB connection, loading a large ML model, initializing an API client) would be wasteful to repeat for every single row.

```python
# BAD - opens a new DB connection for every single row
def save_row(row):
    conn = create_db_connection()
    conn.write(row)
    conn.close()
df.rdd.foreach(save_row)

# GOOD - one connection per partition, reused across all rows in that partition
def save_partition(rows_iterator):
    conn = create_db_connection()
    for row in rows_iterator:
        conn.write(row)
    conn.close()
df.rdd.foreachPartition(save_partition)

# mapPartitions - same idea but returns transformed data instead of just side-effecting
def score_partition(rows_iterator):
    model = load_expensive_ml_model()     # loaded ONCE per partition, not once per row
    return [model.predict(row) for row in rows_iterator]

scored_rdd = df.rdd.mapPartitions(score_partition)
```

**Interview answer:** *"`mapPartitions` amortizes expensive setup cost across an entire partition instead of paying it per row — it's the RDD-level equivalent of what a Pandas UDF gives you at the DataFrame level via batching."*

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

**Schema-on-write options** — controlling how a write reconciles with the target table's existing schema:

```python
# mergeSchema - allow a write to ADD new columns to the target table automatically
df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable("my_table")

# overwriteSchema - completely REPLACE the target table's schema (used with mode("overwrite"))
df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("my_table")

# Session-wide default instead of per-write option
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
```

*"`mergeSchema` is additive — safe for evolving append-only pipelines where new columns show up over time. `overwriteSchema` is destructive — it's for deliberate schema redesigns and should be paired with `mode('overwrite')`; without it, Delta rejects a write whose schema doesn't match the existing table by design, to prevent silent data corruption."*

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

### 2.10 Constraints, Generated Columns & Identity Columns

**Constraints** enforce data quality directly at the table level — a write violating a constraint fails outright rather than landing bad data silently.

```sql
-- NOT NULL constraint
ALTER TABLE main.sales.orders ALTER COLUMN order_id SET NOT NULL;

-- CHECK constraint - arbitrary boolean expression
ALTER TABLE main.sales.orders ADD CONSTRAINT positive_amount CHECK (amount > 0);

-- Any INSERT/MERGE violating this fails the transaction, not just the offending row
INSERT INTO main.sales.orders VALUES ('o1', -50.0);  -- raises an exception
```

**Generated columns** — a column whose value is computed automatically from other columns, commonly used to derive a **partition column** from a timestamp without requiring the writer to compute it manually:

```sql
CREATE TABLE main.sales.events (
  event_id STRING,
  event_ts TIMESTAMP,
  event_date DATE GENERATED ALWAYS AS (CAST(event_ts AS DATE))
) PARTITIONED BY (event_date);

-- Insert only needs event_id and event_ts - event_date is computed automatically
INSERT INTO main.sales.events (event_id, event_ts) VALUES ('e1', '2024-01-15 10:30:00');
```

**Identity columns** — auto-incrementing surrogate keys, generated server-side (useful for dimension table surrogate keys in a star schema):

```sql
CREATE TABLE main.sales.dim_customer (
  customer_sk BIGINT GENERATED ALWAYS AS IDENTITY,   -- or GENERATED BY DEFAULT AS IDENTITY (allows manual override)
  customer_id STRING,
  customer_name STRING
);

INSERT INTO main.sales.dim_customer (customer_id, customer_name) VALUES ('C1', 'Alice');
-- customer_sk is assigned automatically (1, 2, 3, ... - not guaranteed strictly gapless/sequential in a distributed write)
```

**Interview answer:** *"`GENERATED ALWAYS AS IDENTITY` never allows an explicit value to be inserted — the engine always assigns it. `GENERATED BY DEFAULT AS IDENTITY` will auto-assign only if no value is provided, letting you override it (e.g., during a historical backfill where you need to preserve original surrogate keys)."*

---

### 2.11 `COPY INTO` vs Auto Loader

Both load files into a Delta table incrementally and idempotently (won't reprocess a file already loaded), but they solve different scales of problem — this comparison is a very common interview question.

```sql
COPY INTO main.sales.orders
FROM 's3://bucket/raw/orders/'
FILEFORMAT = JSON
FORMAT_OPTIONS ('mergeSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');
```

| | `COPY INTO` | Auto Loader (`cloudFiles`) |
|---|---|---|
| Execution model | A single SQL command, run repeatedly (e.g., on a schedule) | A streaming source, runs continuously or via `trigger(availableNow=True)` |
| File tracking | Tracks loaded files in the Delta transaction log itself | Tracks via RocksDB-backed state + `schemaLocation` |
| Scale | Best for thousands of files per run | Scales to millions/billions of files, especially with File Notification mode |
| Schema evolution | Basic (`mergeSchema` option) | Rich (`addNewColumns`/`rescue`/`failOnNewColumns`/`none`, `_rescuedDataColumn`) |
| Setup complexity | Minimal — just a SQL statement | Requires a checkpoint + schema location, more moving parts |
| Typical use case | Simple, low-file-count periodic batch loads | High-volume, continuous or near-real-time ingestion pipelines |

**Interview answer:** *"`COPY INTO` is the simpler tool — a single idempotent SQL statement, great for periodic batch loads of a few thousand files. Auto Loader is the scalable tool — built for continuous, high-volume ingestion where file counts can reach millions, with much richer schema evolution and state tracking. If someone describes a small nightly batch job, `COPY INTO` is usually the right answer; if they describe a continuously arriving stream of files, it's Auto Loader."*

---

### 2.12 Predictive Optimization

A **managed service** (Unity Catalog-enabled) that automatically runs `OPTIMIZE`, `VACUUM`, and `ANALYZE` on tables **without manual scheduling** — Databricks decides when and how often based on actual table activity, instead of a human guessing a maintenance cadence.

```sql
-- Enable at the metastore, catalog, schema, or table level
ALTER TABLE main.sales.orders SET TBLPROPERTIES ('delta.enablePredictiveOptimization' = 'true');
```

```sql
-- Check what Predictive Optimization has been doing
SELECT * FROM system.storage.predictive_optimization_operations_history
WHERE table_name = 'orders';
```

**Interview answer:** *"Before Predictive Optimization, teams had to schedule their own `OPTIMIZE`/`VACUUM` jobs and guess the right frequency — too often wastes compute, too rarely lets small files or bloat accumulate. Predictive Optimization uses Databricks' own visibility into table write patterns to decide when maintenance is actually worth running, removing that manual tuning entirely for Unity Catalog managed tables."*

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

### 3.9 Key Spark Performance Configurations

Beyond `spark.sql.shuffle.partitions` and `autoBroadcastJoinThreshold` (already covered), these three come up constantly in perf-tuning interview questions:

**Dynamic Resource Allocation** — lets the number of executors scale up/down automatically based on workload, instead of a fixed count for the whole application lifetime.

```python
spark.conf.set("spark.dynamicAllocation.enabled", "true")
spark.conf.set("spark.dynamicAllocation.minExecutors", "2")
spark.conf.set("spark.dynamicAllocation.maxExecutors", "20")
spark.conf.set("spark.dynamicAllocation.executorIdleTimeout", "60s")   # scale down after idle
```
On Databricks, cluster-level **autoscaling** (configured on the cluster itself, not via this Spark conf) is the primary mechanism — Dynamic Allocation still matters conceptually and can come up when discussing open-source Spark on YARN/K8s.

**Speculative Execution** — re-launches a duplicate copy of a task that's running much slower than its peers (a "straggler," often due to skew or a flaky node), and takes whichever copy finishes first.

```python
spark.conf.set("spark.speculation", "true")
spark.conf.set("spark.speculation.multiplier", "1.5")   # flag a task as a straggler at 1.5x the median task duration
spark.conf.set("spark.speculation.quantile", "0.9")      # only after 90% of tasks in the stage have finished
```
**Caution:** speculation duplicates work and can make things worse for non-idempotent side-effecting tasks (e.g., a task that writes to an external system) — prefer fixing the root skew cause (Module 7.5) over masking it with speculation where possible.

**Kryo vs Java Serialization** — controls how objects are serialized for shuffles, caching, and task dispatch.

```python
spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
spark.conf.set("spark.kryoserializer.buffer.max", "512m")
```
Java's default serializer is more flexible (works on anything Serializable, no config needed) but slower and produces larger serialized objects. **Kryo** is significantly faster and more compact, but requires registering custom classes for best performance and doesn't support every object graph out of the box. **Interview answer:** *"Kryo is almost always worth enabling for RDD-heavy or UDF-heavy workloads — the DataFrame/Dataset API already uses Tungsten's own binary encoding internally, so the serializer choice matters far less there than it does for RDD shuffles and cached RDD/object data."*

---

### 3.10 Cluster Policies & Instance Pools

**Cluster Policies** — admin-defined templates that restrict what configurations users are allowed to create clusters with (max DBU/hour, allowed instance types, forced auto-termination, enforced tags) — a governance and cost-control mechanism, not a performance one.

```json
{
  "spark_conf.spark.databricks.cluster.profile": {"type": "fixed", "value": "singleNode"},
  "autotermination_minutes": {"type": "fixed", "value": 30, "hidden": true},
  "node_type_id": {"type": "allowlist", "values": ["i3.xlarge", "i3.2xlarge"]}
}
```

**Instance Pools** — a pre-warmed set of idle cloud VMs that clusters draw from instead of provisioning fresh instances from the cloud provider on every cluster start. Dramatically cuts cluster start/scale-up time (often from minutes to seconds) at the cost of paying for idle instance capacity while the pool sits unused.

```json
{
  "instance_pool_id": "<pool-id>",
  "min_idle_instances": 2,
  "max_capacity": 50,
  "idle_instance_autotermination_minutes": 20
}
```

**Interview answer:** *"Cluster Policies control *what* users are allowed to spin up — a cost/governance guardrail. Instance Pools control *how fast* a permitted cluster actually starts, by keeping a warm buffer of provisioned VMs ready to attach. They're complementary, often used together in enterprise setups: a policy restricts which pool + instance types a team can use, and the pool itself makes their job clusters start quickly."*

---

### 3.11 Init Scripts & Databricks Runtime Variants

**Cluster-scoped init scripts** run custom shell commands during cluster startup — installing OS-level packages, custom JARs, or environment setup that isn't achievable via `%pip`/library UI alone.

```bash
#!/bin/bash
# init_script.sh - stored in a Volume or cloud storage path, referenced in cluster config
apt-get install -y some-native-library
pip install --upgrade some-python-package
```

```python
# Referencing the script in a cluster's configuration (JSON)
{
  "init_scripts": [
    {"volumes": {"destination": "/Volumes/main/init_scripts/setup.sh"}}
  ]
}
```

**Databricks Runtime (DBR) variants** — choosing the right runtime is itself a common config-screen interview question:

| Runtime | Contents |
|---|---|
| **Standard DBR** | Spark + Delta Lake + core libraries — general-purpose data engineering |
| **DBR for Machine Learning (ML Runtime)** | Standard DBR + pre-installed ML libraries (scikit-learn, TensorFlow, PyTorch, XGBoost, MLflow) and GPU driver support |
| **Photon Runtime** | A checkbox on top of Standard/ML runtimes enabling the native vectorized engine (Module 1.10) |
| **LTS (Long-Term Support)** | A DBR version with an extended support/patch window — preferred for production workloads that need stability over the newest features |

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

**Note:** `trigger(once=True)` is the older, now-deprecated way to express "process everything available then stop" — `trigger(availableNow=True)` replaced it and should always be preferred, since it can break the backlog into multiple micro-batches (better for very large backlogs) rather than forcing one giant batch.

---

### 4.6 & 4.7 Delta Live Tables (DLT): Declarative Framework & Table Constructs

**Pipeline execution modes** (a DLT pipeline-level setting, distinct from the streaming triggers above):

| Mode | Behavior |
|---|---|
| **Triggered** | Runs once, processes all currently available data, then stops — cheaper, good for scheduled/batch-like DLT pipelines |
| **Continuous** | Keeps the pipeline's cluster running and processes new data as it arrives — lower latency, higher cost since compute never spins down |

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

### 4.10 Auto Loader — Complete Options Reference

A full, working example touching the options actually asked about in interviews, followed by a categorized reference table.

```python
df = (spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "json")                        # csv, json, parquet, avro, orc, text, binaryFile
      .option("cloudFiles.schemaLocation", "dbfs:/schema/events")  # REQUIRED - where inferred schema + evolution history is tracked
      .option("cloudFiles.schemaEvolutionMode", "addNewColumns")   # addNewColumns | rescue | failOnNewColumns | none
      .option("cloudFiles.inferColumnTypes", "true")               # infer actual types instead of defaulting everything to string
      .option("cloudFiles.rescuedDataColumn", "_rescued_data")     # column to capture unparseable/mismatched data
      .option("cloudFiles.schemaHints", "id BIGINT, event_time TIMESTAMP")  # override/assist inference for specific columns
      .option("cloudFiles.maxFilesPerTrigger", 1000)               # micro-batch size cap - number of files
      .option("cloudFiles.maxBytesPerTrigger", "10g")              # micro-batch size cap - total bytes (alternative to file count)
      .option("cloudFiles.useNotifications", "true")               # True = File Notification mode, False (default) = Directory Listing mode
      .option("cloudFiles.includeExistingFiles", "true")           # process files that already existed before the stream first started
      .option("pathGlobFilter", "*.json")                          # only pick up files matching this glob pattern
      .option("cloudFiles.validateOptions", "true")                # fail fast on invalid/conflicting cloudFiles.* options (default true)
      .load("dbfs:/raw/events/"))
```

**Core options (format & schema)**

| Option | Purpose |
|---|---|
| `cloudFiles.format` | Underlying file format being ingested: `json`, `csv`, `parquet`, `avro`, `orc`, `text`, `binaryFile` |
| `cloudFiles.schemaLocation` | **Required.** Directory where Auto Loader persists the inferred schema and evolution history across restarts |
| `cloudFiles.schemaEvolutionMode` | `addNewColumns` (default, adds new cols and restarts the stream), `rescue` (new cols go to `_rescuedDataColumn` instead of failing), `failOnNewColumns` (stream fails, forces you to update schema manually), `none` (ignore schema changes entirely) |
| `cloudFiles.inferColumnTypes` | Whether to infer real types (`true`) or treat everything as `string` (`false`, faster inference pass) |
| `cloudFiles.schemaHints` | Manually specify/override types for specific columns the inferencer gets wrong |
| `cloudFiles.rescuedDataColumn` | Name of the column that captures fields that don't match the inferred schema (extra fields, type mismatches) instead of dropping or erroring |

**Discovery mode options**

| Option | Purpose |
|---|---|
| `cloudFiles.useNotifications` | `false` (default) = **Directory Listing**: periodically lists cloud directories — simple, no extra setup, scales worse past millions of files. `true` = **File Notification**: subscribes to cloud storage events (S3→SNS/SQS, ADLS→Event Grid, GCS→Pub/Sub) — near real-time, scales to huge volumes, requires extra IAM/event infra |
| `cloudFiles.includeExistingFiles` | Whether to process files already present in the source directory when the stream starts for the very first time (default `true`) |
| `cloudFiles.backfillInterval` | Schedules periodic full directory backfill listings (e.g. `"1 day"`) as a safety net in File Notification mode, in case any cloud events were ever missed |
| `pathGlobFilter` | Standard glob filter to restrict which files under the path get picked up (e.g. `"*.json"`, `"data_*.csv"`) |

**Cloud-specific notification setup (when `useNotifications = true`)**

```python
# AWS example
.option("cloudFiles.useNotifications", "true")
.option("cloudFiles.region", "us-east-1")
.option("cloudFiles.queueUrl", "https://sqs.us-east-1.amazonaws.com/1234/my-queue")   # optional - auto-created if omitted
.option("cloudFiles.roleArn", "arn:aws:iam::1234:role/autoloader-role")

# Azure example
.option("cloudFiles.useNotifications", "true")
.option("cloudFiles.subscriptionId", "<sub-id>")
.option("cloudFiles.tenantId", "<tenant-id>")
.option("cloudFiles.clientId", "<sp-client-id>")
.option("cloudFiles.clientSecret", dbutils.secrets.get("scope", "sp-secret"))
.option("cloudFiles.resourceGroup", "<resource-group>")
```

**Throughput / batch-size control**

| Option | Purpose |
|---|---|
| `cloudFiles.maxFilesPerTrigger` | Caps how many new files are processed per micro-batch (default 1000) |
| `cloudFiles.maxBytesPerTrigger` | Caps total data volume per micro-batch (soft limit — may slightly exceed to avoid splitting a single file) |
| `cloudFiles.maxFileAge` | Ignore/expire tracking for files older than this duration — bounds the internal RocksDB state size for very long-running streams with huge backlogs |

**Cleanup & robustness options**

| Option | Purpose |
|---|---|
| `cloudFiles.allowOverwrites` | Whether Auto Loader should re-process a file if it's overwritten at the same path (default `false`) |
| `cloudFiles.cleanSource` | Automatically `MOVE` or `DELETE` source files after successful ingestion (use carefully — usually only in tightly controlled landing zones) |
| `cloudFiles.cleanSource.retentionDuration` | How long to retain source files before cleanup kicks in, when `cleanSource` is enabled |
| `cloudFiles.ignoreMissingFiles` | Don't fail the stream if a listed file disappears before it can be read (handles race conditions with upstream file deletion) |
| `cloudFiles.partitionColumns` | Explicitly declare Hive-style partition columns to extract from the file path (e.g. `/year=2024/month=01/`) instead of relying on auto-detection |

**Interview answer:** *"The two options that matter most in interviews are `schemaLocation` (required, tracks schema evolution across restarts) and `useNotifications` (the Directory Listing vs File Notification trade-off). Everything else is throughput tuning (`maxFilesPerTrigger`/`maxBytesPerTrigger`) or robustness (`rescuedDataColumn`, `ignoreMissingFiles`, `cleanSource`)."*

---

### 4.11 Kafka Integration — Reading, Writing, Performance & Troubleshooting

**Reading from Kafka**

```python
kafka_df = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker1:9092,broker2:9092")
    .option("subscribe", "orders_topic")               # single topic
    # .option("subscribePattern", "orders_.*")         # regex for multiple topics
    # .option("assign", '{"orders_topic":[0,1,2]}')    # pin to specific partitions
    .option("startingOffsets", "latest")               # "earliest" | "latest" | explicit JSON offsets
    .option("maxOffsetsPerTrigger", 100000)            # throttle: max records read per micro-batch across all partitions
    .option("minPartitions", 20)                       # split Kafka partitions into more Spark tasks for parallelism
    .option("failOnDataLoss", "false")                 # don't kill the stream if offsets were deleted by Kafka retention
    .load())

# Raw Kafka schema: key, value (both binary), topic, partition, offset, timestamp, timestampType
kafka_df.printSchema()
```

**Deserializing the payload (JSON)**

```python
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

schema = StructType([
    StructField("order_id", StringType()),
    StructField("amount", DoubleType())
])

parsed_df = (kafka_df
    .select(col("key").cast("string"),
            from_json(col("value").cast("string"), schema).alias("data"),
            "topic", "partition", "offset", "timestamp")
    .select("key", "data.*", "topic", "partition", "offset", "timestamp"))
```

**Deserializing with Avro + Schema Registry** (production-grade, handles schema evolution)

```python
from pyspark.sql.avro.functions import from_avro

schema_registry_options = {
    "mode": "PERMISSIVE",
    "confluent.schema.registry.url": "http://schema-registry:8081"
}

# from_avro can auto-fetch the writer schema per record from the registry
avro_parsed_df = kafka_df.select(
    from_avro(col("value"), options=schema_registry_options).alias("data")
).select("data.*")
```

**Writing to Kafka**

```python
from pyspark.sql.functions import to_json, struct

output_df = parsed_df.select(
    col("order_id").alias("key"),
    to_json(struct("order_id", "amount")).alias("value")
)

query = (output_df.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker1:9092")
    .option("topic", "orders_processed")
    .option("checkpointLocation", "dbfs:/checkpoints/kafka_writer")
    .start())
```

**Sinking into Delta with `foreachBatch` (the standard streaming-ETL pattern)**

```python
def upsert_to_delta(batch_df, batch_id):
    from delta.tables import DeltaTable
    target = DeltaTable.forName(spark, "main.sales.orders")
    (target.alias("t")
     .merge(batch_df.alias("s"), "t.order_id = s.order_id")
     .whenMatchedUpdateAll()
     .whenNotMatchedInsertAll()
     .execute())

(parsed_df.writeStream
    .foreachBatch(upsert_to_delta)
    .option("checkpointLocation", "dbfs:/checkpoints/kafka_to_delta")
    .trigger(processingTime="30 seconds")
    .start())
```

---

**Performance tuning knobs**

| Option/Technique | Effect |
|---|---|
| `maxOffsetsPerTrigger` | Caps records per micro-batch — prevents one huge catch-up batch after downtime from overwhelming the cluster |
| `minPartitions` | Spark maps 1 Kafka partition → 1 task by default; setting `minPartitions` higher sub-divides each Kafka partition into multiple Spark tasks for better parallelism when partition counts are low relative to cluster size |
| `trigger(processingTime=...)` | Longer intervals = fewer, larger, more efficient files written downstream; shorter = lower latency but more small files |
| Repartitioning after read | `kafka_df.repartition(200)` — rebalances if Kafka partition count doesn't match desired Spark parallelism |
| RocksDB state store | For stateful aggregations on the stream (Module 4.4), always prefer RocksDB over the default in-memory store at scale |
| Kafka consumer `fetch.max.bytes` / `max.poll.records` | Passed via `kafka.*`-prefixed options — tune consumer-side batch fetch size for network efficiency |

---

**Common Issues, Root Causes & Solutions**

| Issue | Root Cause | Solution |
|---|---|---|
| **Data loss / `failOnDataLoss` exception** | Kafka topic retention expired before Spark consumed those offsets (consumer fell behind, or was down too long) | Increase Kafka topic retention (`retention.ms`), monitor consumer lag proactively, or set `failOnDataLoss=false` if occasional gaps are acceptable — but this masks real data loss, so pair it with lag alerting |
| **Consumer lag keeps growing / can't keep up** | Producer throughput exceeds consumer processing rate; too few Spark tasks vs Kafka partitions; undersized cluster | Increase `minPartitions` for more parallelism, scale up executors, reduce per-record processing cost (avoid heavy UDFs in the hot path), increase `maxOffsetsPerTrigger` cautiously to catch up faster once capacity is added |
| **Duplicate records downstream** | At-least-once delivery is Kafka's default; a micro-batch can be reprocessed after a failure before its offset commit is checkpointed | Make the **sink idempotent** — use Delta `MERGE` keyed on a natural/business key (not `INSERT` blindly) inside `foreachBatch`, so re-application of the same batch is a no-op |
| **Small file problem on the Delta sink** | Very short trigger intervals create tiny Delta files every micro-batch | Increase `trigger(processingTime=...)` interval, enable `delta.autoOptimize.autoCompact`, schedule periodic `OPTIMIZE` |
| **Skewed Kafka partitions** | Uneven key distribution at the producer causes some partitions to have far more data/traffic than others, creating straggler tasks | Fix the producer's partitioning key to be higher-cardinality/better distributed; as a stream-side mitigation, increase `minPartitions` to further split the largest partitions into more Spark tasks |
| **Schema evolution breaks the stream** | Producer starts emitting a new/changed field, hardcoded JSON schema no longer matches, or Avro schema evolves incompatibly | Prefer Avro + Confluent Schema Registry (built-in compatibility checking, `BACKWARD`/`FORWARD` compatibility modes) over raw JSON with a hardcoded schema; for JSON, add `_rescued_data`-style handling manually via `from_json`'s `PERMISSIVE` mode |
| **Out-of-order / late-arriving events** | Network delays, retries, or multi-producer clock skew mean events don't arrive in event-time order | Apply `withWatermark("event_time", "10 minutes")` (Module 4.4) before any windowed aggregation, tuned to the realistic worst-case lateness in the pipeline |
| **Stream fails on restart after a code/schema change** | Checkpoint stores operator IDs tied to the previous query plan; changing the transformation logic (e.g., adding a new aggregation) can invalidate old state | For genuinely incompatible changes, start a **new checkpoint location** (accepting a full reprocess from `startingOffsets`) rather than trying to force-restart against an incompatible old checkpoint |
| **Broker connection drops / transient network errors** | Normal transient broker unavailability, rolling broker restarts, or network blips | Kafka consumer client has built-in retry/backoff (`kafka.reconnect.backoff.ms`, `kafka.retry.backoff.ms`); Structured Streaming's own checkpoint-based recovery means a failed micro-batch simply retries from the last committed offset without manual intervention |
| **Need to replay historical data for backfill/debugging** | A downstream bug requires reprocessing already-consumed messages | Point a **new** stream (new checkpoint directory) at the same topic with `startingOffsets` set to an explicit JSON offset map or `"earliest"` (if retention still holds the data) — never reuse the production checkpoint for a replay, to avoid corrupting live offset tracking |
| **Exactly-once end-to-end semantics** | At-least-once is Kafka's native guarantee; achieving effective exactly-once requires the whole chain (source + processing + sink) to cooperate | Structured Streaming's checkpointing gives exactly-once **read** semantics from Kafka; combine with an **idempotent sink** (Delta `MERGE`, or a sink with unique-key upsert support) to get effective exactly-once end-to-end — there is no Kafka-native "exactly-once sink" for arbitrary Delta writes, idempotency is the practical solution |

**Interview answer (the one they're really testing):** *"Kafka guarantees at-least-once delivery, not exactly-once. Structured Streaming's checkpoints give you exactly-once semantics on the **read** side — an offset is only considered committed once its micro-batch fully succeeds. To get effective end-to-end exactly-once, the **sink** must also be idempotent, which is why `foreachBatch` + a keyed Delta `MERGE` is the standard production pattern instead of a blind `append`."*

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

**DBFS vs Mount Points vs Unity Catalog Volumes** — a frequently asked "which one is current/deprecated" question:

| | DBFS Root | Mount Points (`dbutils.fs.mount`) | UC Volumes |
|---|---|---|---|
| What it is | Default workspace-internal storage, backed by a cloud bucket Databricks manages | A legacy technique aliasing a cloud storage path into the `/dbfs/mnt/...` namespace using embedded credentials/instance profiles | A first-class Unity Catalog governed object, path `/Volumes/catalog/schema/volume/...` |
| Governance | None — effectively a shared bucket, any workspace user can read/write | Credential-based, not identity-based; hard to audit per-user access | Full UC governance — GRANT/REVOKE, audit logging, lineage, per-user permissions |
| Status | Legacy; avoid for new production data | **Deprecated** — Databricks actively recommends migrating off mounts | **Current recommended approach** for any non-tabular file access |

**Interview answer:** *"Mount points predate Unity Catalog and rely on cluster-level credentials (instance profiles/service principals) baked into the mount, which makes per-user access control effectively impossible — anyone with cluster access inherits the mount's permissions. UC Volumes replace that with proper identity-based governance, the same GRANT/REVOKE model as tables, which is why new architectures should use Volumes, not mounts, for unstructured data."*

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

### 5.10 Secrets Management

Never hardcode credentials (API keys, DB passwords, storage account keys) in notebooks or code. Databricks **Secret Scopes** store secrets encrypted and expose them only via a redacted API — printing a secret's value in a notebook shows `[REDACTED]` in output/logs.

```bash
# Create a Databricks-backed secret scope (CLI)
databricks secrets create-scope my-scope
databricks secrets put-secret my-scope db-password
```

```python
# Reference the secret inside a notebook / job - value itself is never exposed in cleartext
db_password = dbutils.secrets.get(scope="my-scope", key="db-password")

jdbc_url = f"jdbc:postgresql://host:5432/db?user=admin&password={db_password}"
```

**Two backends:**

| Backend | Description |
|---|---|
| **Databricks-backed scope** | Secrets stored and managed entirely inside Databricks |
| **Azure Key Vault-backed scope** | Secrets actually live in Azure Key Vault; Databricks just proxies read access — preferred in enterprise Azure environments for centralized secret rotation/audit |

```bash
# Azure Key Vault-backed scope (created via UI at https://<workspace>#secrets/createScope, or CLI)
databricks secrets create-scope my-akv-scope \
  --scope-backend-type AZURE_KEYVAULT \
  --resource-id <key-vault-resource-id> \
  --dns-name <key-vault-dns-name>
```

**Interview answer:** *"Secret scopes decouple credential storage from code. `dbutils.secrets.get()` fetches the value at runtime, and Databricks automatically redacts any printed output that matches a fetched secret value, so it never leaks into notebook results or driver logs by accident."*

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

**Job cluster reuse** — a cost-efficiency detail worth knowing: by default, each task in a multi-task job can spin up its *own* job cluster, meaning startup time (and cost) is paid repeatedly. Assigning multiple tasks the **same `job_cluster_key`** makes them share a single cluster across the whole job run instead.

```json
{
  "job_clusters": [{"job_cluster_key": "shared_cluster", "new_cluster": {"num_workers": 4, "spark_version": "..."}}],
  "tasks": [
    {"task_key": "extract", "job_cluster_key": "shared_cluster", "notebook_task": {...}},
    {"task_key": "transform", "job_cluster_key": "shared_cluster", "notebook_task": {...}}
  ]
}
```
**Interview answer:** *"Sharing a `job_cluster_key` across tasks avoids paying cluster startup cost per task — the trade-off is that tasks now compete for the same compute resources instead of getting dedicated capacity, so it's the right call for lightweight sequential tasks, not for a heavy Spark job followed by a heavy ML training task that would rather have isolated resources."*

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

**`dbutils` — the Databricks notebook utility API**

```python
# File system operations (works across DBFS, mounted cloud storage, Volumes)
dbutils.fs.ls("/mnt/data/")
dbutils.fs.cp("dbfs:/tmp/a.csv", "dbfs:/tmp/b.csv")
dbutils.fs.rm("dbfs:/tmp/old_file.csv", recurse=True)

# Notebook parameterization (used heavily with Workflows for job parameters)
dbutils.widgets.text("run_date", "2024-01-01")
run_date = dbutils.widgets.get("run_date")

# Chain notebooks together
result = dbutils.notebook.run("/Shared/child_notebook", timeout_seconds=600, arguments={"run_date": run_date})
dbutils.notebook.exit("SUCCESS")   # return a value to the caller
```

**Notebook magic commands**

```python
# %run - includes another notebook's code/variables into the current notebook (compile-time-like include)
# %run ./utils_notebook

# %sql - run a SQL cell directly in a Python notebook
# %sql SELECT * FROM main.sales.orders LIMIT 10

# %fs - shorthand for dbutils.fs commands
# %fs ls /mnt/data/

# %pip - install a library scoped to the current notebook session
# %pip install great_expectations
```

**`%run` vs `dbutils.notebook.run()`:** `%run` textually includes the other notebook (shares variables/session, runs in the same Spark context) — used for shared utility functions. `dbutils.notebook.run()` executes the target notebook as a **separate job run** with its own context, returns only a string exit value, and supports timeouts/parameters — used for actual orchestration/modularity.

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

### 6.7 Testing PySpark Code

Production-grade PySpark pipelines should be unit-tested outside of interactive notebooks, using `pytest` with a local `SparkSession`, plus DataFrame-comparison helper libraries.

```python
# conftest.py - a reusable local SparkSession fixture for tests
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    return (SparkSession.builder
            .master("local[2]")
            .appName("pytest-spark")
            .getOrCreate())
```

```python
# transformations.py - the code under test
from pyspark.sql.functions import col

def add_bonus(df):
    return df.withColumn("bonus", col("salary") * 0.1)
```

```python
# test_transformations.py
from chispa.dataframe_comparer import assert_df_equality
from transformations import add_bonus

def test_add_bonus(spark):
    input_df = spark.createDataFrame([(1, 1000.0)], ["id", "salary"])
    expected_df = spark.createDataFrame([(1, 1000.0, 100.0)], ["id", "salary", "bonus"])

    result_df = add_bonus(input_df)

    assert_df_equality(result_df, expected_df, ignore_row_order=True)
```

```python
# Mocking dbutils in unit tests (dbutils only exists inside a live notebook/cluster context)
class MockDBUtils:
    class secrets:
        @staticmethod
        def get(scope, key):
            return "fake-secret-value"

def test_uses_dbutils():
    dbutils = MockDBUtils()
    assert dbutils.secrets.get("scope", "key") == "fake-secret-value"
```

**Key libraries:** `pytest` (test runner), `chispa` (Spark-friendly DataFrame equality assertions with readable diffs), `unittest.mock` (mocking `dbutils`, external API calls). Run these as a step in CI (e.g., a GitHub Actions/Azure DevOps pipeline) **before** a Databricks Asset Bundle deploy — fail the pipeline before it ever reaches a real cluster.

---

### 6.8 Authentication Methods (CLI, API, CI/CD)

| Method | Typical Use | Notes |
|---|---|---|
| **Personal Access Token (PAT)** | Quick CLI/API scripting, local dev | Tied to a specific user; expires; simplest to set up but weakest for production automation (leaves if the user leaves) |
| **OAuth (User-to-Machine / Machine-to-Machine)** | Interactive CLI login (U2M), or app-to-app automation (M2M) | Short-lived tokens, refreshable, better security posture than long-lived PATs |
| **Service Principal** | CI/CD pipelines, production jobs, Terraform/DABs deployments | Not tied to any individual human user — the recommended identity for automated/production workflows |

```bash
# PAT-based CLI auth
databricks configure --token   # prompts for host + PAT

# OAuth U2M (opens a browser login)
databricks auth login --host https://<workspace>.databricks.com

# Service Principal (M2M OAuth) - typical CI/CD pattern
export DATABRICKS_CLIENT_ID=<sp-client-id>
export DATABRICKS_CLIENT_SECRET=<sp-secret>
export DATABRICKS_HOST=https://<workspace>.databricks.com
databricks bundle deploy   # authenticates as the service principal automatically
```

**Interview answer:** *"PATs are fine for a developer's own local scripting but are a liability in CI/CD since they're tied to a person's identity and don't rotate automatically. Production pipelines should authenticate as a **Service Principal** via OAuth M2M — it's an identity owned by the platform/team, not an individual, so it survives personnel changes and can be scoped/audited independently."*

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

## Module 8: ML, GenAI & Emerging Databricks Services

### 8.1 MLflow (Tracking, Registry, Model Stages)

MLflow is built into every Databricks workspace and is the standard way experiments, models, and deployments are tracked — asked in almost every ML-adjacent interview even for "just DE" roles that feed ML pipelines.

```python
import mlflow
import mlflow.sklearn
from sklearn.ensemble import RandomForestClassifier

mlflow.set_experiment("/Shared/experiments/churn_model")

with mlflow.start_run(run_name="rf_v1"):
    model = RandomForestClassifier(n_estimators=100, max_depth=5)
    model.fit(X_train, y_train)

    mlflow.log_param("n_estimators", 100)
    mlflow.log_param("max_depth", 5)
    mlflow.log_metric("accuracy", model.score(X_test, y_test))
    mlflow.sklearn.log_model(model, "model", registered_model_name="churn_model")
```

```python
# Model Registry - promote a model through lifecycle stages
from mlflow import MlflowClient
client = MlflowClient()

client.transition_model_version_stage(
    name="churn_model", version=3, stage="Production"
)

# Load the current Production model anywhere, without knowing the run ID
model = mlflow.pyfunc.load_model("models:/churn_model/Production")
```

- **Tracking**: logs parameters, metrics, artifacts, and code version per run — fully queryable/comparable across runs.
- **Model Registry**: a centralized store with lifecycle stages (`None → Staging → Production → Archived`), enabling controlled promotion and rollback.
- **Unity Catalog-integrated Model Registry** (current recommended approach): models registered as `catalog.schema.model_name`, governed the same way as tables (GRANT/REVOKE, lineage) instead of the older workspace-local registry.

---

### 8.2 Feature Store

A **Feature Store** centralizes feature computation so features are defined once, reused consistently between training and real-time inference, and automatically joined by primary key — eliminating training/serving skew.

```python
from databricks.feature_engineering import FeatureEngineeringClient, FeatureLookup

fe = FeatureEngineeringClient()

fe.create_table(
    name="main.ml.customer_features",
    primary_keys=["customer_id"],
    df=features_df,
    description="Customer-level features for churn model"
)

# Training: automatically join features by key, avoiding manual joins/leakage
training_set = fe.create_training_set(
    df=labels_df,
    feature_lookups=[FeatureLookup(table_name="main.ml.customer_features", lookup_key="customer_id")],
    label="churned"
)
training_df = training_set.load_df()
```

At inference time, the same `FeatureLookup` definitions are reused so the serving path pulls **identical** feature logic — the core value proposition over manually re-deriving features in two places.

---

### 8.3 Model Serving

Real-time (low-latency REST) and batch inference for registered models, fully managed — no manual container/infra setup.

```python
from mlflow.deployments import get_deploy_client

client = get_deploy_client("databricks")

client.create_endpoint(
    name="churn-model-endpoint",
    config={
        "served_entities": [{
            "entity_name": "main.ml.churn_model",
            "entity_version": "3",
            "workload_size": "Small",
            "scale_to_zero_enabled": True
        }]
    }
)
```

```python
# Query the endpoint (REST call, illustrated here via the SDK)
response = client.predict(endpoint="churn-model-endpoint", inputs={"dataframe_records": [{"tenure": 12, "monthly_charges": 70.5}]})
```

- **Scale-to-zero**: endpoint compute scales down to zero when idle, reducing cost for low-traffic models.
- Same infrastructure also serves **external/foundation models** (e.g., a hosted LLM) behind a unified endpoint interface — see 8.4.

---

### 8.4 Vector Search & RAG (Mosaic AI)

The backbone for building **Retrieval-Augmented Generation (RAG)** applications directly on lakehouse data — a very hot current interview topic if the role touches GenAI at all.

```python
from databricks.vector_search.client import VectorSearchClient

vsc = VectorSearchClient()

# A Vector Search index can sync automatically from a Delta table (Delta Sync Index)
vsc.create_delta_sync_index(
    endpoint_name="vs_endpoint",
    index_name="main.rag.docs_index",
    source_table_name="main.rag.documents",
    pipeline_type="TRIGGERED",             # or "CONTINUOUS" for near-real-time sync
    primary_key="doc_id",
    embedding_source_column="content",
    embedding_model_endpoint_name="databricks-bge-large-en"
)

# Similarity search at query time
results = vsc.get_index("vs_endpoint", "main.rag.docs_index").similarity_search(
    query_text="How do I enable deletion vectors?",
    columns=["doc_id", "content"],
    num_results=5
)
```

- **Delta Sync Index**: automatically keeps the vector index in sync with an underlying Delta table as rows change — no manual re-indexing pipeline needed.
- Typical RAG flow on Databricks: Delta table (chunks of text) → embeddings via a served embedding model → Vector Search index → retrieved chunks fed as context into an LLM serving endpoint (8.3) → generated answer.

---

### 8.5 Databricks SQL, Warehouses & AI/BI (Genie)

```sql
-- Runs on a SQL Warehouse (Classic or Serverless), Photon-accelerated by default
SELECT region, SUM(amount) AS total_sales
FROM main.sales.orders
GROUP BY region
ORDER BY total_sales DESC;
```

| Warehouse Type | Description |
|---|---|
| **Classic SQL Warehouse** | Dedicated compute you size/manage, Photon-enabled |
| **Serverless SQL Warehouse** | Instant start, fully managed, auto-scaling, billed per query |
| **Pro SQL Warehouse** | Adds advanced performance features (e.g., predictive I/O) on top of Classic |

- **AI/BI Dashboards**: modern replacement for legacy SQL Dashboards, with AI-assisted chart/summary generation.
- **Genie (AI/BI Genie)**: a natural-language-to-SQL conversational interface over governed Unity Catalog tables — business users ask questions in plain English, Genie generates and runs the SQL, governed by the same UC permissions as any other query.

---

### 8.6 Lakehouse Federation

Lets you query external operational databases (PostgreSQL, MySQL, Snowflake, Redshift, SQL Server, BigQuery, etc.) **directly through Unity Catalog** as if they were native tables, without an ETL copy step first.

```sql
CREATE CONNECTION pg_connection TYPE POSTGRESQL
OPTIONS (host 'db.company.com', port '5432', user 'reader', password secret('scope','pg_pwd'));

CREATE FOREIGN CATALOG pg_catalog USING CONNECTION pg_connection
OPTIONS (database 'sales_db');

-- Now query the live external table directly
SELECT * FROM pg_catalog.public.orders LIMIT 10;
```

Query pushdown happens where possible (filters/aggregations pushed to the source system), and results are governed by the same Unity Catalog permission model as native Delta tables — useful for one-off joins against operational systems without building a full ingestion pipeline first.

---

### 8.7 Enterprise Networking & Security

- **VNet Injection (Azure) / Customer-Managed VPC (AWS)**: deploy Databricks compute plane resources inside your own network, giving full control over subnets, NSGs/security groups, and routing — required in most regulated enterprise environments.
- **PrivateLink**: keeps all traffic between the control plane, compute plane, and cloud storage on private network paths, never traversing the public internet.
- **IP Access Lists**: restrict which source IPs can reach the workspace UI/API at all.

These are typically **Solutions Architect / Platform Engineer** interview topics rather than day-to-day DE questions, but worth knowing at a conceptual level: *what problem does PrivateLink solve, why would an enterprise require VNet injection.*

---

### 8.8 Infrastructure as Code: DABs vs Terraform

| | Databricks Asset Bundles (DABs) | Databricks Terraform Provider |
|---|---|---|
| Scope | Jobs, pipelines, DLT, ML resources — app-level | Full workspace/account-level infra (clusters, users, catalogs, workspaces themselves) |
| Owned by | Databricks-native CLI tool | HashiCorp Terraform ecosystem |
| Best for | CI/CD of a specific data project's jobs/pipelines | Provisioning the platform itself (new workspaces, Unity Catalog metastores, network config) |
| Typical user | Data engineer / ML engineer | Platform / DevOps engineer |

In practice, many orgs use **both**: Terraform to stand up the workspace/catalog/network once, and DABs for the ongoing day-to-day deployment of jobs and pipelines on top of that platform.

---

### 8.9 AI Functions in Databricks SQL

Built-in SQL functions that call an LLM inline, directly against table data, without standing up a separate pipeline or serving endpoint call:

```sql
-- General-purpose prompt against a foundation model, applied per row
SELECT product_name,
       ai_query('databricks-meta-llama-3-70b-instruct', CONCAT('Summarize this review: ', review_text)) AS summary
FROM main.sales.reviews;

-- Purpose-built AI functions (thin wrappers over ai_query for common tasks)
SELECT ai_classify(review_text, ARRAY('positive', 'negative', 'neutral')) AS sentiment FROM reviews;
SELECT ai_translate(review_text, 'es') AS review_es FROM reviews;
SELECT ai_summarize(review_text) AS short_summary FROM reviews;
SELECT ai_extract(review_text, ARRAY('product_defect', 'shipping_issue')) AS extracted_entities FROM reviews;
SELECT ai_fix_grammar(review_text) AS cleaned_text FROM reviews;
```

**Interview answer:** *"AI Functions let you invoke an LLM as a plain SQL expression, batched and governed like any other query — the model call happens through Model Serving under the hood, so it inherits Unity Catalog governance and cost/usage tracking, without needing a custom Python UDF or a separate API integration to bolt an LLM onto tabular data."*

---

### 8.10 Lakehouse Monitoring

A managed service for automatically tracking **data quality and drift** on Delta tables over time, without hand-writing custom DQ check pipelines.

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import MonitorSnapshot

w = WorkspaceClient()

w.quality_monitors.create(
    table_name="main.sales.orders",
    assets_dir="/Shared/monitoring/orders",
    output_schema_name="main.monitoring",
    snapshot=MonitorSnapshot()   # or MonitorTimeSeries / MonitorInferenceLog for streaming/ML tables
)
```

- Automatically generates **profile metrics** (nulls, distinct counts, distributions per column) and **drift metrics** (comparing the current window against a baseline or previous window) into two auto-created Delta tables you can query or build dashboards/alerts on top of.
- Three monitor types: **Snapshot** (point-in-time tables), **Time Series** (tables with a timestamp column, tracks metrics over time), **Inference Log** (ML model input/output tables — tracks prediction drift and label drift for deployed models).

**Interview answer:** *"Lakehouse Monitoring is Databricks' answer to hand-rolled data quality frameworks (like Great Expectations) for the specific case of ongoing drift/quality tracking on a table already in Unity Catalog — it's complementary to DLT Expectations (Module 4.8), which validate rows at ingestion time, whereas Monitoring tracks aggregate statistical health of a table over time."*

---

### 8.11 Databricks AutoML

Generates a **baseline set of trained models + full source notebooks** for a classification/regression/forecasting problem from a single command — meant as a fast starting point (and a transparent one, since you get the actual generated code, not a black box).

```python
from databricks import automl

summary = automl.classify(
    dataset=training_df,
    target_col="churned",
    timeout_minutes=30
)

print(summary.best_trial.model_path)   # MLflow model URI of the best run
```

- Runs multiple algorithms/hyperparameter combinations, tracks every trial in **MLflow** automatically, and generates an editable notebook per trial so you can take the best one and keep iterating manually instead of treating it as a final answer.
- **Interview framing:** *"AutoML is a fast baseline generator, not a replacement for a data scientist's judgment — its real value is the generated, fully-editable notebook you can build on top of, not just a single 'best model' output."*

---

### 8.12 Databricks Marketplace

An **open marketplace** for discovering and subscribing to third-party datasets, ML models, and Databricks-native applications/solution accelerators — delivered via **Delta Sharing** (Module 5.9) under the hood, so a subscribed dataset shows up as a live, governed table in your own Unity Catalog without any data movement/ETL required on your end.

---

### 8.13 Pricing Tiers & Feature Gating

Not itself a technical concept, but a genuinely common "gotcha" in system-design interviews: **not every feature discussed above is available on every Databricks pricing plan.**

| Tier | Notable gating |
|---|---|
| **Standard** | Core Spark/notebooks/jobs; no Unity Catalog |
| **Premium** | Unity Catalog, RBAC, Databricks SQL, most governance/security features |
| **Enterprise** | Everything in Premium + advanced compliance/security controls (e.g., customer-managed keys, enhanced audit, PrivateLink in some clouds) |

**Interview answer:** *"If a system design answer leans on Unity Catalog governance, row filters, or column masking, it's worth noting out loud that these require at least a Premium-tier workspace — an interviewer designing for a cost-constrained or legacy Standard-tier environment may specifically want to hear that trade-off acknowledged."*

---

## Module 9: PySpark & SQL Command/Function Cookbook

A quick-reference cookbook of the functions and commands you'll actually type in an interview live-coding round. Import assumed for all examples:

```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window
```

### 9.1 DataFrame Creation & I/O

```python
# Create from Python data
df = spark.createDataFrame([(1, "Alice", 5000), (2, "Bob", 6000)], ["id", "name", "salary"])

# Read
df_csv = spark.read.option("header", True).option("inferSchema", True).csv("dbfs:/data/file.csv")
df_json = spark.read.json("dbfs:/data/file.json")
df_parquet = spark.read.parquet("dbfs:/data/file.parquet")
df_delta = spark.read.format("delta").load("dbfs:/data/delta_table")
df_table = spark.table("main.sales.orders")
df_sql = spark.sql("SELECT * FROM main.sales.orders WHERE amount > 100")

# Write
df.write.mode("overwrite").parquet("dbfs:/out/parquet/")
df.write.mode("append").format("delta").saveAsTable("main.sales.orders")
df.write.mode("overwrite").partitionBy("year", "month").format("delta").save("dbfs:/out/delta/")

# Schema
df.printSchema()
df.schema
df.columns
df.dtypes
```

### 9.2 Column Selection & Filtering

```python
df.select("id", "name").show()
df.select(F.col("id"), F.col("salary").alias("pay")).show()
df.filter(F.col("salary") > 5000).show()
df.where("salary > 5000 AND name != 'Bob'").show()          # SQL-string style filter
df.filter(F.col("name").isin("Alice", "Bob")).show()
df.filter(F.col("name").like("A%")).show()
df.filter(F.col("email").isNull()).show()
df.filter(F.col("email").isNotNull()).show()
df.distinct().show()
df.dropDuplicates(["id"]).show()
df.limit(10).show()
```

### 9.3 Adding, Modifying, Dropping, Renaming Columns

```python
df.withColumn("bonus", F.col("salary") * 0.1)
df.withColumn("level", F.when(F.col("salary") > 5500, "Senior").otherwise("Junior"))
df.withColumnRenamed("name", "employee_name")
df.drop("bonus")
df.withColumn("salary", F.col("salary").cast("double"))
df.select(*[F.col(c).alias(c.upper()) for c in df.columns])   # bulk rename pattern
```

### 9.4 Aggregations & GroupBy

```python
df.groupBy("dept").count()
df.groupBy("dept").agg(
    F.avg("salary").alias("avg_salary"),
    F.max("salary").alias("max_salary"),
    F.min("salary").alias("min_salary"),
    F.sum("salary").alias("total_salary"),
    F.countDistinct("name").alias("distinct_names")
)
df.agg(F.avg("salary")).collect()[0][0]     # single scalar result
df.groupBy("dept").agg(F.collect_list("name").alias("names"))
df.groupBy("dept").agg(F.collect_set("name").alias("unique_names"))

# Multiple grouping sets in one query
df.cube("dept", "level").count()      # all combinations incl. grand total
df.rollup("dept", "level").count()    # hierarchical subtotals
```

### 9.5 Sorting, Ranking & Deduplication

```python
df.orderBy(F.col("salary").desc())
df.sort("dept", F.col("salary").desc())

# Deduplicate keeping the highest-salary row per dept (classic interview question)
w = Window.partitionBy("dept").orderBy(F.col("salary").desc())
df.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")

# Window ranking functions
df.withColumn("rank", F.rank().over(w))
df.withColumn("dense_rank", F.dense_rank().over(w))
df.withColumn("row_num", F.row_number().over(w))
df.withColumn("pct_rank", F.percent_rank().over(w))
df.withColumn("ntile", F.ntile(4).over(w))

# lag/lead - compare to previous/next row
df.withColumn("prev_salary", F.lag("salary", 1).over(w))
df.withColumn("next_salary", F.lead("salary", 1).over(w))

# Running total
w2 = Window.partitionBy("dept").orderBy("id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
df.withColumn("running_total", F.sum("salary").over(w2))
```

### 9.6 Joins Reference

```python
left = spark.createDataFrame([(1, "Alice", 100)], ["id", "name", "dept_id"])
right = spark.createDataFrame([(100, "Sales")], ["dept_id", "dept_name"])

left.join(right, "dept_id", "inner")
left.join(right, "dept_id", "left")        # or "left_outer"
left.join(right, "dept_id", "right")       # or "right_outer"
left.join(right, "dept_id", "full")        # or "outer" / "full_outer"
left.join(right, "dept_id", "left_semi")   # only left columns, rows that DO match
left.join(right, "dept_id", "left_anti")   # only left columns, rows that DON'T match
left.crossJoin(right)                      # cartesian product - use with caution

# Join with different column names
left.join(right, left.dept_id == right.dept_id, "inner")

# Broadcast hint
left.join(F.broadcast(right), "dept_id")
```

### 9.7 String Functions

```python
df.select(
    F.upper("name"), F.lower("name"), F.length("name"),
    F.trim("name"), F.ltrim("name"), F.rtrim("name"),
    F.concat(F.col("name"), F.lit(" - "), F.col("dept")),
    F.concat_ws(", ", "name", "dept"),
    F.substring("name", 1, 3),
    F.split("name", " "),
    F.regexp_extract("email", r"@(.+)\.com", 1),
    F.regexp_replace("phone", "[^0-9]", ""),
    F.instr("name", "A"),
    F.lpad("id", 5, "0"), F.rpad("id", 5, "0"),
    F.initcap("name")
)
```

**Higher-order array functions** — manipulate array columns using a lambda-like expression, entirely without a UDF (avoids the Python serialization tax from Module 1.9):

```python
arr_df = spark.createDataFrame([(1, [1, -2, 3, -4], ["Alice", "bob", "CHARLIE"])],
                                ["id", "numbers", "names"])

arr_df.select(
    F.transform("numbers", lambda x: x * 2).alias("doubled"),               # map every element
    F.filter("numbers", lambda x: x > 0).alias("positives"),                # keep matching elements
    F.aggregate("numbers", F.lit(0), lambda acc, x: acc + x).alias("total"), # reduce to a single value
    F.exists("numbers", lambda x: x < 0).alias("has_negative"),             # boolean: any match?
    F.forall("numbers", lambda x: x > 0).alias("all_positive"),             # boolean: all match?
    F.transform("names", lambda x: F.lower(x)).alias("lower_names")
).show(truncate=False)
```

**Interview answer:** *"Before higher-order functions were added to Spark SQL, transforming an array column required either `explode` + re-aggregate (extra shuffle-prone round trip) or a Python UDF (serialization overhead). `transform`/`filter`/`aggregate`/`exists`/`forall` operate directly inside Catalyst's expression tree, so they get the same codegen/Tungsten benefits as any other built-in function."*

---

### 9.8 Date & Time Functions

```python
df.select(
    F.current_date(), F.current_timestamp(),
    F.to_date("date_str", "yyyy-MM-dd"),
    F.to_timestamp("ts_str", "yyyy-MM-dd HH:mm:ss"),
    F.date_format("dt", "MM/dd/yyyy"),
    F.date_add("dt", 7), F.date_sub("dt", 7),
    F.datediff(F.col("end_date"), F.col("start_date")),
    F.months_between("end_date", "start_date"),
    F.year("dt"), F.month("dt"), F.dayofmonth("dt"), F.dayofweek("dt"),
    F.weekofyear("dt"), F.quarter("dt"),
    F.last_day("dt"),
    F.unix_timestamp("ts_str"),
    F.from_unixtime(F.col("epoch_col"))
)
```

### 9.9 Null Handling

```python
df.na.drop()                                  # drop rows with any null
df.na.drop(subset=["salary"])                 # drop rows null in specific column
df.na.drop(how="all")                         # drop only if ALL columns are null
df.na.fill(0)                                 # fill all numeric nulls with 0
df.na.fill({"salary": 0, "name": "Unknown"})  # fill per-column
df.na.replace("N/A", None)

df.select(F.coalesce(F.col("phone"), F.col("mobile"), F.lit("Unknown")))
df.select(F.isnull("salary"))
df.filter(F.col("salary").isNotNull())
```

### 9.10 Type Casting & Schema Manipulation

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Explicit schema (avoids costly inferSchema pass on large files)
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("salary", DoubleType(), True)
])
df = spark.read.schema(schema).csv("dbfs:/data/file.csv")

df.withColumn("id", F.col("id").cast("string"))
df.withColumn("salary", F.col("salary").cast(DoubleType()))
df.selectExpr("id", "CAST(salary AS INT) AS salary_int")
```

### 9.11 RDD Common Operations (still occasionally asked)

```python
rdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5], 4)

rdd.map(lambda x: x * 2).collect()
rdd.filter(lambda x: x % 2 == 0).collect()
rdd.flatMap(lambda x: (x, x * 10)).collect()
rdd.reduce(lambda a, b: a + b)
rdd.take(2)
rdd.count()
rdd.distinct().collect()
rdd.foreach(lambda x: print(x))              # runs on executors, no return value
pair_rdd = spark.sparkContext.parallelize([("a", 1), ("b", 2), ("a", 3)])
pair_rdd.reduceByKey(lambda a, b: a + b).collect()
pair_rdd.groupByKey().mapValues(list).collect()
```

### 9.12 Delta Table SQL Commands Reference

```sql
-- DDL
CREATE TABLE main.sales.orders (id INT, amount DOUBLE) USING DELTA;
ALTER TABLE main.sales.orders ADD COLUMN region STRING;
ALTER TABLE main.sales.orders RENAME COLUMN region TO country;
DROP TABLE main.sales.orders;

-- DML
INSERT INTO main.sales.orders VALUES (1, 100.0);
UPDATE main.sales.orders SET amount = amount * 1.1 WHERE id = 1;
DELETE FROM main.sales.orders WHERE amount < 0;

MERGE INTO main.sales.orders t
USING staged_orders s
ON t.id = s.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- Maintenance
OPTIMIZE main.sales.orders ZORDER BY (customer_id);
VACUUM main.sales.orders RETAIN 168 HOURS;
DESCRIBE HISTORY main.sales.orders;
DESCRIBE DETAIL main.sales.orders;
ANALYZE TABLE main.sales.orders COMPUTE STATISTICS;

-- Time travel
SELECT * FROM main.sales.orders VERSION AS OF 3;
SELECT * FROM main.sales.orders TIMESTAMP AS OF '2024-01-01';
RESTORE TABLE main.sales.orders TO VERSION AS OF 3;
```

```python
# Equivalent PySpark DeltaTable API for MERGE (used constantly in real pipelines)
from delta.tables import DeltaTable

target = DeltaTable.forName(spark, "main.sales.orders")
(target.alias("t")
 .merge(staged_df.alias("s"), "t.id = s.id")
 .whenMatchedUpdateAll()
 .whenNotMatchedInsertAll()
 .execute())
```

### 9.13 File Format Handling Deep Dive (JSON, Avro, ORC, XML, Binary & More)

**CSV — full option reference**

```python
df = (spark.read
      .option("header", True)
      .option("inferSchema", True)
      .option("delimiter", ",")           # or "\t", "|", etc.
      .option("quote", '"')
      .option("escape", '"')
      .option("multiLine", True)          # handle fields with embedded newlines
      .option("encoding", "UTF-8")
      .option("dateFormat", "yyyy-MM-dd")
      .option("mode", "PERMISSIVE")       # PERMISSIVE | DROPMALFORMED | FAILFAST
      .option("columnNameOfCorruptRecord", "_corrupt_record")
      .csv("dbfs:/data/file.csv"))

df.write.option("header", True).option("delimiter", "|").mode("overwrite").csv("dbfs:/out/csv/")
```
- `PERMISSIVE` (default): puts malformed rows into `_corrupt_record` instead of failing the whole read.
- `DROPMALFORMED`: silently drops bad rows.
- `FAILFAST`: throws an exception immediately on the first bad row — useful in strict pipelines where silent data loss is unacceptable.

**JSON — single-line, multi-line, and nested**

```python
# Standard JSON-lines file (one JSON object per line) - the common Auto Loader case
df = spark.read.json("dbfs:/data/events.jsonl")

# A single large JSON array spanning multiple lines needs multiLine
df = spark.read.option("multiLine", True).json("dbfs:/data/events_array.json")

# Explicit schema avoids an extra inference pass (big perf win on large JSON sets)
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
schema = StructType([
    StructField("id", StringType()),
    StructField("tags", ArrayType(StringType())),
])
df = spark.read.schema(schema).json("dbfs:/data/events.jsonl")

df.write.mode("overwrite").json("dbfs:/out/json/")
```

```python
# Working with a JSON STRING column stored inside another format (e.g. a CSV/Delta column
# that itself contains a JSON payload) - extremely common in real ETL work
from pyspark.sql.functions import from_json, to_json, schema_of_json, get_json_object, col

json_str_df = spark.createDataFrame([('{"id":1,"name":"Alice","tags":["a","b"]}',)], ["payload"])

# Infer a schema string from a sample JSON value, then parse the column into a struct
inferred_schema = json_str_df.select(schema_of_json(json_str_df.payload)).first()[0]
parsed_df = json_str_df.withColumn("parsed", from_json(col("payload"), inferred_schema))
parsed_df.select("parsed.id", "parsed.name", "parsed.tags").show()

# Cheap single-field extraction without a full schema (good for one-off lookups)
json_str_df.select(get_json_object(col("payload"), "$.name").alias("name")).show()

# Reverse direction - serialize a struct/column back into a JSON string
parsed_df.select(to_json(col("parsed")).alias("json_out")).show()
```

**Avro** — requires the `spark-avro` package (pre-bundled on Databricks Runtime, no install needed).

```python
df = spark.read.format("avro").load("dbfs:/data/events.avro")
df.write.format("avro").mode("overwrite").save("dbfs:/out/avro/")

# Reading/writing Avro with an explicit .avsc schema string
avro_schema = open("/dbfs/schemas/event.avsc").read()
df = spark.read.format("avro").option("avroSchema", avro_schema).load("dbfs:/data/events.avro")
```
Avro is the standard interchange format with **Kafka + Schema Registry** pipelines — expect a question on why Avro (compact binary, embedded schema, strong schema-evolution support) is preferred over JSON for high-throughput streaming ingestion.

**ORC**

```python
df = spark.read.orc("dbfs:/data/file.orc")
df.write.mode("overwrite").orc("dbfs:/out/orc/")
```
ORC vs Parquet: both columnar with predicate pushdown and compression; Parquet is the de facto Databricks/Delta default, ORC is more common in classic Hive/Hadoop ecosystems. Functionally similar for most interview purposes.

**Plain Text files**

```python
df = spark.read.text("dbfs:/data/logs.txt")          # one column "value" per line
df.write.mode("overwrite").text("dbfs:/out/text/")

# Fixed-width text parsing (no built-in reader - use substring on the fixed positions)
fixed_df = spark.read.text("dbfs:/data/fixedwidth.txt")
parsed = fixed_df.select(
    F.substring("value", 1, 5).alias("id"),
    F.substring("value", 6, 20).alias("name"),
    F.substring("value", 26, 10).alias("amount")
)
```

**Binary files (images, PDFs, arbitrary blobs)**

```python
# binaryFile format - reads any file as raw bytes + metadata, common for image/ML pipelines
bin_df = spark.read.format("binaryFile") \
    .option("pathGlobFilter", "*.png") \
    .load("dbfs:/data/images/")

bin_df.select("path", "modificationTime", "length", "content").show()
# "content" column is the raw bytes (BinaryType) - typically decoded by a downstream UDF/model
```

**XML** — requires the `spark-xml` library (`com.databricks:spark-xml`, install via cluster libraries or `%pip`/Maven coordinate).

```python
df = (spark.read.format("xml")
      .option("rowTag", "employee")          # the XML element that represents one row
      .load("dbfs:/data/employees.xml"))

df.write.format("xml").option("rootTag", "employees").option("rowTag", "employee") \
  .mode("overwrite").save("dbfs:/out/xml/")
```

**Excel** — via the `com.crealytics:spark-excel` library, or `pandas`/`openpyxl` for small files.

```python
# Using the spark-excel library (better for large files, distributed read)
df = (spark.read.format("com.crealytics.spark.excel")
      .option("header", True)
      .option("dataAddress", "'Sheet1'!A1")
      .load("dbfs:/data/report.xlsx"))

# Small-file alternative with pandas (runs on the driver only - not distributed)
import pandas as pd
pdf = pd.read_excel("/dbfs/data/report.xlsx", sheet_name="Sheet1")
df = spark.createDataFrame(pdf)
```

**Format cheat table**

| Format | Splittable | Schema Embedded? | Compression | Typical Use Case |
|---|---|---|---|---|
| CSV | Yes (mostly) | No | Optional (gzip breaks splittability) | Legacy exports, human-readable |
| JSON | Yes (line-delimited) | No (self-describing but inferred) | Optional | Semi-structured events, APIs |
| Parquet | Yes | Yes (embedded) | Yes (Snappy default) | Analytical storage, Delta's underlying format |
| Avro | Yes | Yes (embedded, evolvable) | Yes | Streaming/Kafka interchange |
| ORC | Yes | Yes (embedded) | Yes | Classic Hive/Hadoop ecosystems |
| Delta | Yes | Yes (via `_delta_log`) | Yes (Parquet-based) | Lakehouse tables, ACID, time travel |
| XML | Depends on library | No | Optional | Legacy enterprise system exports |
| binaryFile | N/A (per-file) | N/A | N/A | Images, PDFs, ML/unstructured data |

---

## Module 10: Performance & Optimization Playbook — Scenario-Based Decisions

This module ties together techniques from every prior module, organized the way a real performance problem actually presents: **symptom first, decision second**. Each scenario states what you'd observe, why it happens, the fix, and when *not* to reach for that fix.

### 10.1 Join Performance Scenarios

**Scenario: joining a large fact table to a small dimension table is slow, plan shows a Sort-Merge Join**

- **Cause:** the small table's actual size exceeded `autoBroadcastJoinThreshold` (default 10MB), or statistics were stale so Spark didn't realize it qualified for a broadcast.
- **Fix:** force a broadcast.
```python
from pyspark.sql.functions import broadcast
fact.join(broadcast(dim_store), "store_id")
```
- **When NOT to use it:** if the "small" table is actually large enough to threaten driver/executor memory during collection — forcing a broadcast on a misjudged table causes a driver or executor OOM (Module 7.3/7.4) instead of just being slow. Verify actual size first.

**Scenario: two large tables are joined on the same key repeatedly across many jobs/queries**

- **Cause:** every join re-shuffles both sides from scratch even though the join key never changes.
- **Fix:** bucket both tables identically at write time (Module 1.14) so the join reads pre-partitioned data with no shuffle.
```python
df.write.bucketBy(8, "customer_id").sortBy("customer_id").saveAsTable("orders_bucketed")
```
- **When NOT to use it:** one-off/ad-hoc joins, or when the join key changes across different query patterns — bucketing locks in one specific key, and rebucketing means a full table rewrite. Prefer Liquid Clustering (Module 2.5) for tables with evolving query patterns instead.

**Scenario: a join has a handful of tasks that take 10-100x longer than the rest**

- **Cause:** data skew — one or a few join keys have disproportionately many rows (Module 7.5).
- **Fix:** let AQE handle it automatically first; salt manually only if AQE isn't sufficient.
```python
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")   # try this first, zero code change
# Manual salting fallback:
from pyspark.sql.functions import rand, floor
big_salted = big_df.withColumn("salt", floor(rand() * 10))
```
- **When NOT to use it:** don't reach for manual salting before confirming AQE skew handling is actually enabled and still insufficient — it adds real code complexity that's easy to get wrong (forgetting to explode the salt on the small side, for instance).

**Scenario: a join has no equality condition, or the plan shows a Cartesian/Broadcast Nested Loop Join**

- **Cause:** a non-equi join condition (`<`, `BETWEEN`, `!=`) or a genuinely missing join key.
- **Fix:** restructure to filter down each side *before* the join wherever possible, or convert a range join into a bucketed range comparison; there's no clean "optimization flag" for a true cartesian product — the fix is query redesign, not a config.

---

### 10.2 Data Skew Scenarios (Beyond Joins)

**Scenario: `groupBy` on a column with a few dominant values takes far longer than expected**

- **Cause:** one or two group keys hold a large fraction of total rows — a single reducer task ends up doing most of the work.
- **Fix:** two-phase aggregation — pre-aggregate with a salt, then combine.
```python
from pyspark.sql.functions import floor, rand, sum as _sum

salted = df.withColumn("salt", floor(rand() * 20))
partial = salted.groupBy("dept", "salt").agg(_sum("salary").alias("partial_sum"))
final = partial.groupBy("dept").agg(_sum("partial_sum").alias("total_salary"))
```
- **When NOT to use it:** if the skew is mild (task times within 2-3x of each other), this adds unnecessary complexity — check the Spark UI Stages tab (Module 6.5) task duration distribution before assuming skew is the bottleneck at all.

---

### 10.3 File & Storage Scenarios

**Scenario: a Delta table accumulates thousands of tiny files after streaming writes**

- **Cause:** short micro-batch trigger intervals each writing a small file (Module 7.6).
- **Fix:** enable auto-compaction and/or widen the trigger interval.
```python
spark.sql("ALTER TABLE events SET TBLPROPERTIES ('delta.autoOptimize.autoCompact' = 'true')")
# and/or:
stream.trigger(processingTime="2 minutes")   # fewer, larger files vs "10 seconds"
```
- **When NOT to use it:** if the use case genuinely needs sub-minute latency, don't just widen the trigger — accept more small files and run a *separate scheduled* `OPTIMIZE` job instead, decoupling latency from file-size concerns.

**Scenario: queries filtering on a column always do a full table scan despite the filter**

- **Cause:** no data-skipping statistics are useful for that column — either it's not part of any partitioning/clustering scheme, or file-level min/max stats aren't selective (e.g., the column's values are scattered across every file).
- **Fix:** Z-Order or Liquid Cluster on that column.
```sql
OPTIMIZE sales ZORDER BY (customer_id, order_date);
-- or, for a new table:
CREATE TABLE sales CLUSTER BY (customer_id, order_date);
```
- **When NOT to use it:** low-cardinality columns with even distribution (e.g., a boolean flag) rarely benefit — Z-Order pays off most on medium-to-high-cardinality columns used in frequent equality/range filters.

**Decision table: which storage layout technique?**

| Situation | Use |
|---|---|
| Coarse, low-cardinality filter column (e.g., `year`, `region`) known upfront and stable | Hive-style partitioning |
| Multiple medium/high-cardinality filter columns, query patterns fairly stable | Z-Ordering |
| High-cardinality columns, evolving/unpredictable query patterns, want to avoid committing to fixed partition columns | Liquid Clustering (modern default recommendation) |
| Same join key reused across many repeated joins | Bucketing |

---

### 10.4 Memory & OOM Scenarios

**Scenario: driver crashes with an OOM error after calling `.toPandas()` or `.collect()`**

- **Cause:** the entire (possibly huge) DataFrame is pulled into single-machine driver memory (Module 7.3).
- **Fix:** aggregate/sample before collecting, or write results to a table instead.
```python
# Instead of: pdf = big_df.toPandas()
pdf = big_df.groupBy("category").count().toPandas()   # collect an aggregate, not raw rows
```
- **When NOT to use it:** if you genuinely need every row on the driver (rare), increase `spark.driver.memory` deliberately rather than fighting the pattern — but this is usually a sign the architecture needs rethinking (why does a distributed job need all rows in one process?).

**Scenario: executors spill to disk or OOM during a wide shuffle**

- **Cause:** too much data landing on too few tasks, or too many cores per executor competing for the same memory pool (Module 7.4).
- **Fix:** reduce cores-per-executor, and/or increase `memoryOverhead` for off-heap-heavy workloads (Pandas UDFs, native libraries).
```
--executor-cores 4        (down from 8)
--conf spark.executor.memoryOverhead=4g
```
- **When NOT to use it:** if the spill is caused by genuine skew rather than general under-provisioning, fix the skew (10.2) first — throwing more memory at a skewed task just delays the same failure at a larger scale.

**Scenario: a broadcast join OOMs an executor**

- **Cause:** the "small" side wasn't actually small — stale statistics, or a filter that was expected to shrink it didn't apply as expected.
- **Fix:** disable auto-broadcast globally for that query, or explicitly check the size first.
```python
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)   # force SMJ, no risk of a bad broadcast decision
```

---

### 10.5 Read, Scan & Filtering Scenarios

**Scenario: a query only needs 3 of 50 columns but scans feel slow anyway**

- **Cause:** column pruning didn't get pushed all the way to the file scan — often because a `.select()` happened *after* an expensive intermediate operation instead of before it.
- **Fix:** select early, filter early — let Catalyst's projection/predicate pushdown (Module 1.4) do its job against the source format.
```python
# Prefer this - narrows the scan itself
df.select("id", "amount", "region").filter(col("region") == "US")

# Over this - same logical result, but obscures the early narrowing if buried in a long chain
df.withColumn(...).withColumn(...).select("id", "amount", "region").filter(...)
```
- Parquet/Delta are columnar, so this matters far more there than on row-oriented formats like CSV/JSON, where pruning can't skip unread bytes within a row.

**Scenario: `COUNT(DISTINCT ...)` on a very high-cardinality column is slow**

- **Cause:** exact distinct count requires a full shuffle to deduplicate every value globally.
- **Fix:** use an approximate count when exactness isn't required (dashboards, monitoring).
```python
from pyspark.sql.functions import approx_count_distinct
df.select(approx_count_distinct("user_id", rsd=0.01))   # ~1% error, dramatically cheaper
```
- **When NOT to use it:** financial reporting, billing, or any context where the approximation's error tolerance isn't acceptable — use exact `countDistinct` there and accept the cost.

---

### 10.6 Caching & Iterative Workload Scenarios

**Scenario: the same DataFrame is reused across several actions in one job**

- **Cause:** without caching, Spark recomputes the full lineage from scratch for every action.
- **Fix:** `.cache()`/`.persist()` after the expensive shared computation, materialize once.
```python
shared = expensive_transform(df).cache()
shared.count()          # materializes the cache
shared.filter(...).show()
shared.groupBy(...).count().show()   # reuses the cached data, no recompute
```
- **When NOT to use it:** a DataFrame used only once — caching it adds memory pressure for zero benefit. Also remember to `.unpersist()` once done to free the memory pool.

**Scenario: a long iterative loop (ML training, graph algorithm) gets progressively slower each iteration**

- **Cause:** an ever-growing lineage graph being replayed/serialized on every step (Module 1.12).
- **Fix:** periodic `.checkpoint()` to truncate lineage.
```python
spark.sparkContext.setCheckpointDir("dbfs:/tmp/checkpoints/")
if i % 5 == 0:
    df = df.checkpoint()
```
- **When NOT to use it:** short chains (a handful of transformations) — the eager disk write cost isn't worth it until the lineage is genuinely large.

---

### 10.7 UDF & Function Choice Scenarios

**Scenario: a plain Python UDF is the slowest part of the pipeline**

- **Cause:** every row round-trips through Py4J serialization into a Python subprocess (Module 1.9).
- **Decision order** (fastest to slowest, prefer top-down):
```python
# 1. Best - built-in function, fully in Catalyst/Tungsten, zero serialization
df.withColumn("upper_name", F.upper("name"))

# 2. Good - higher-order array function, still Catalyst-native (Module 9.7)
df.withColumn("doubled", F.transform("numbers", lambda x: x * 2))

# 3. Acceptable - Pandas UDF, Arrow-vectorized batches instead of row-by-row
@F.pandas_udf("double")
def custom_calc(s: pd.Series) -> pd.Series:
    return s * 1.1

# 4. Last resort - plain Python UDF, row-by-row, full serialization cost
@F.udf("double")
def slow_calc(x):
    return x * 1.1
```
- **When a plain UDF is still fine:** low-volume tables, or truly one-off logic that isn't worth the engineering time to vectorize — don't over-optimize a UDF that runs on 500 rows once a day.

---

### 10.8 Streaming Performance Scenarios

**Scenario: a Kafka-based stream can't keep up with incoming volume (growing consumer lag)**

- **Cause:** insufficient parallelism relative to partition count, or per-record processing cost too high (Module 4.11).
- **Fix, in order of effort:** increase `minPartitions` first (config-only), then scale executors, then optimize the actual per-record logic (remove UDFs from the hot path) last.
```python
.option("minPartitions", 40)   # cheapest lever - try first
```

**Scenario: state store memory grows unbounded on a long-running stateful stream**

- **Cause:** no watermark, or a watermark far wider than the real-world lateness of events (Module 4.4).
- **Fix:** tighten the watermark to the realistic worst-case lateness, switch to RocksDB state store if not already.
```python
events.withWatermark("event_time", "15 minutes")   # not "6 hours" if 15 min genuinely covers real lateness
```

---

### 10.9 Cluster & Configuration Choice Scenarios

**Master decision table — "when to reach for what":**

| Symptom / Situation | Reach For | Module Reference |
|---|---|---|
| Small table joined to large table, plan shows SMJ | `broadcast()` hint | 1.8, 10.1 |
| Same join key reused across many jobs | Bucketing | 1.14, 10.1 |
| A few tasks take far longer than the rest | AQE skew join → manual salting | 3.3, 7.5, 10.1/10.2 |
| Repeated filters on a medium/high-cardinality column | Z-Order / Liquid Clustering | 2.5, 10.3 |
| Millions of tiny files | `OPTIMIZE` + autoCompact, wider trigger interval | 2.3, 10.3 |
| Driver crashes after `.collect()`/`.toPandas()` | Aggregate first, or write to a table | 7.3, 10.4 |
| Executor spill/OOM during shuffle | Fewer cores/executor, more `memoryOverhead` | 7.4, 10.4 |
| Query plan chooses a risky broadcast that OOMs | Disable auto-broadcast (`threshold = -1`) | 10.4 |
| Need only a few columns from a wide table | Select/project early | 1.4, 10.5 |
| Approximate distinct count is acceptable | `approx_count_distinct` | 10.5 |
| Same DataFrame reused across multiple actions | `.cache()`/`.persist()` | 1.6, 10.6 |
| Very long iterative transformation chain | `.checkpoint()` | 1.12, 10.6 |
| Row-by-row Python UDF is the bottleneck | Built-in fn → higher-order fn → Pandas UDF → plain UDF (in that order) | 1.9, 9.7, 10.7 |
| Kafka consumer lag growing | `minPartitions`, then scale executors | 4.11, 10.8 |
| Stateful stream memory growing unbounded | Tighter watermark, RocksDB state store | 4.4, 10.8 |
| CPU-bound heavy shuffle workload | Compute-optimized instances | 3.6 |
| Large joins/aggregations/caching-heavy workload | Memory-optimized instances | 3.6 |
| General SQL/DataFrame workload wanting a free speed boost | Enable Photon | 1.10, 3.11 |
| RDD-heavy or UDF-object-heavy workload | Kryo serialization | 3.9 |
| Cluster takes too long to start/scale | Instance Pools | 3.10 |
| Need to restrict what configs users can launch | Cluster Policies | 3.10 |
| Stale join/aggregation plan decisions | `ANALYZE TABLE ... COMPUTE STATISTICS` | 1.4 |

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
| What is Photon? | Databricks' native vectorized C++ execution engine, transparent drop-in for supported operators, falls back to JVM Spark for the rest |
| Cache vs Checkpoint? | Cache keeps data in memory and retains lineage for recovery; checkpoint writes to durable storage and truncates lineage entirely |
| Accumulator gotcha? | Exactly-once only guaranteed inside actions; used inside transformations, retried/recomputed tasks can double-count |
| Why run `ANALYZE TABLE`? | Populates statistics the cost-based optimizer needs for join reordering and strategy selection; without it, CBO falls back to conservative defaults |
| MLflow Tracking vs Registry? | Tracking logs params/metrics/artifacts per run; Registry manages model lifecycle stages (Staging/Production) for deployment |
| Purpose of a Feature Store? | Define features once, reuse identically for training and serving, eliminating training/serving skew |
| What does Lakehouse Federation do? | Query external databases (Postgres, Snowflake, etc.) live through Unity Catalog without copying data via ETL first |
| DABs vs Terraform provider? | DABs = app-level CI/CD for jobs/pipelines; Terraform = platform-level infra provisioning (workspaces, catalogs, networking) |
