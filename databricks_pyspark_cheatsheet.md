# Databricks & PySpark — Quick Revision Cheat Sheet

*Condensed companion to the full interview prep guide. One-liners, key code, and decision tables only — no long explanations.*

---

## 1. Spark Core & PySpark Internals

- **Hierarchy:** Application → Job (1 per action) → Stage (split at shuffle boundaries) → Task (1 per partition)
- **Driver** = builds DAG, schedules tasks. **Executors** = run tasks, cache data. **Cluster Manager** = allocates resources.
- **Lazy evaluation:** transformations build a plan; nothing runs until an **action** (`collect`, `count`, `show`, `write`).
- **Narrow** (no shuffle): `map`, `filter`, `select`. **Wide** (shuffle): `groupBy`, `join`, `distinct`, `repartition`.
- **Catalyst phases:** Analysis → Logical Optimization (predicate/projection pushdown, constant folding) → Physical Planning (cost-based) → Code Gen (Janino).
- **`ANALYZE TABLE ... COMPUTE STATISTICS`** — required for CBO join reordering; without it, planner uses defaults.
- **Tungsten:** off-heap binary memory (`UnsafeRow`), Whole-Stage Code Gen (fuses operators into one JVM function).
- **Photon:** Databricks-only native **C++ vectorized** engine, transparent, falls back to JVM Spark for unsupported ops. Enabled at cluster level, not a Spark conf.
- **Memory layout:** Reserved (~300MB) + User Memory + Spark Memory (shared Storage↔Execution pool; Execution can evict Storage).
- **`repartition(n)`** = full shuffle, can increase partitions. **`coalesce(n)`** = no shuffle, decrease-only.
- Target partition size: **100–200MB** uncompressed.
- **Join strategies:**

| Strategy | When |
|---|---|
| Broadcast Hash Join (BHJ) | One side < `autoBroadcastJoinThreshold` (10MB default) |
| Shuffle Hash Join (SHJ) | Both large, one small enough to hash-map per partition |
| Sort-Merge Join (SMJ) | Both large — Spark's default |
| Cartesian/BNLJ | No join key / non-equi join |

```python
from pyspark.sql.functions import broadcast
big.join(broadcast(small), "id")
```
```sql
SELECT /*+ BROADCAST(t) */ ...   -- or MERGE(a,b) / SHUFFLE_HASH(a,b)
```
- **PySpark internals:** Py4J (driver↔JVM, not slow) vs Python worker IPC (pickling, slow for row UDFs). Fix: **Pandas UDFs** (Arrow-vectorized).
- **Accumulators:** write-only counters to driver; exactly-once only inside **actions**, not transformations (retries can double-count).
- **Broadcast variables:** read-only value cached once per executor (for UDF lookups).
- **Checkpoint vs Cache:**

| | Cache/Persist | Checkpoint |
|---|---|---|
| Storage | Memory (+disk spill) | Durable storage |
| Lineage | Kept (recovery fallback) | **Truncated** |
| Use case | Reuse across actions in one job | Long iterative chains, avoid huge DAG |

- **Bucketing:** `df.write.bucketBy(n, "key").sortBy("key")` — pre-shuffles at write time so repeated joins on that key skip the shuffle. Fixed bucket count; superseded by Liquid Clustering for most new tables.
- **`mapPartitions`/`foreachPartition`:** run once **per partition** (not per row) — use for expensive setup (DB connections, model loading).
- **Higher-order array functions** (no UDF needed): `F.transform`, `F.filter`, `F.aggregate`, `F.exists`, `F.forall`.

---

## 2. Delta Lake

- **`_delta_log`**: JSON commit files (`0000N.json`) + checkpoint Parquet every 10 commits + CRC stats.
- **OCC (Optimistic Concurrency Control):** writer commits a new version; conflicts auto-retry if non-overlapping. Isolation: `WriteSerializable` (default, more concurrency) vs `Serializable` (strict).

```sql
OPTIMIZE my_table ZORDER BY (customer_id);      -- compact small files (~1GB target)
VACUUM my_table RETAIN 168 HOURS;               -- delete unreferenced files (default 7 days)
```
```python
df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable("t")     # additive
df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("t")  # destructive
```
- **Deletion Vectors:** merge-on-read — bitmap (`.pvd`) marks deleted/updated rows instead of rewriting the whole Parquet file. Reduces write amplification.
- **Partitioning strategy decision:**

| Situation | Use |
|---|---|
| Low-cardinality, stable filter column | Hive partitioning |
| Multiple medium/high-cardinality filters | Z-Order |
| High-cardinality, evolving query patterns | **Liquid Clustering** (`CLUSTER BY`) — modern default |
| Same join key reused repeatedly | Bucketing |

```python
spark.read.format("delta").option("versionAsOf", 5).load(path)          # time travel
spark.read.format("delta").option("timestampAsOf", "2024-01-01").load(path)
spark.sql("RESTORE TABLE t TO VERSION AS OF 5")
```
- **CDF:** `ALTER TABLE t SET TBLPROPERTIES (delta.enableChangeDataFeed=true)` → read via `.option("readChangeFeed","true")`. Change types: `insert`, `update_preimage`, `update_postimage`, `delete`.
- **Clone:** `SHALLOW CLONE` = metadata-only, zero-copy. `DEEP CLONE` = full physical copy.
- **UniForm:** one Delta table, readable as Iceberg/Hudi too (`delta.universalFormat.enabledFormats`).
- **Constraints/Generated/Identity columns:**
```sql
ALTER TABLE t ADD CONSTRAINT positive_amount CHECK (amount > 0);
CREATE TABLE t (ts TIMESTAMP, dt DATE GENERATED ALWAYS AS (CAST(ts AS DATE))) PARTITIONED BY (dt);
CREATE TABLE t (id BIGINT GENERATED ALWAYS AS IDENTITY, name STRING);   -- or GENERATED BY DEFAULT (allows override)
```
- **`COPY INTO` vs Auto Loader:**

| | `COPY INTO` | Auto Loader |
|---|---|---|
| Model | Single idempotent SQL statement, run repeatedly | Streaming source, continuous or `availableNow` |
| Scale | Thousands of files/run | Millions+ files, File Notification mode |
| Schema evolution | Basic (`mergeSchema`) | Rich (`addNewColumns`/`rescue`/`failOnNewColumns`/`none`) |
| Use case | Simple periodic batch loads | High-volume continuous ingestion |

```sql
COPY INTO t FROM 's3://bucket/raw/' FILEFORMAT = JSON FORMAT_OPTIONS ('mergeSchema'='true');
```
- **Predictive Optimization:** managed service that auto-runs `OPTIMIZE`/`VACUUM`/`ANALYZE` on UC tables based on actual activity — no manual maintenance scheduling. `ALTER TABLE t SET TBLPROPERTIES ('delta.enablePredictiveOptimization'='true')`.

---

## 3. Compute Architecture & Cluster Sizing

| Compute Type | Use Case |
|---|---|
| All-Purpose Cluster | Interactive/dev |
| Job Cluster | Scheduled prod jobs, ephemeral |
| Serverless | Instant start, fully managed |

- **Isolation modes:** Single User / Shared User Isolation (UC-enforced) / No Isolation (legacy, avoid).
- **AQE** (`spark.sql.adaptive.enabled=true`, default on): dynamic partition coalescing, dynamic join-strategy switching (SMJ→BHJ at runtime), dynamic skew handling.
- **DPP/DFP:** pushes a filtered dimension's keys into the fact table scan to skip partitions/files.
- **Cache types:** Spark `.cache()` (in-memory, job-scoped) vs Databricks Disk Cache (local NVMe, automatic, survives across jobs on same cluster).
- **Executor sizing:** 4–5 cores/executor is the sweet spot.
- **Cost:** Spot workers + On-Demand driver; Enhanced Autoscaling; auto-termination.
- **Dynamic Allocation:** `spark.dynamicAllocation.enabled/minExecutors/maxExecutors`.
- **Speculative Execution:** `spark.speculation=true` — reruns slow straggler tasks. Risky for non-idempotent side effects.
- **Serialization:** Kryo (`spark.serializer=...KryoSerializer`) faster/compact vs Java default (flexible, slower) — matters most for RDD/UDF-heavy workloads, not DataFrame (Tungsten handles that).
- **Cluster Policies** = restrict what users CAN launch (governance). **Instance Pools** = pre-warmed VMs, faster cluster START (not a perf feature for running jobs).
- **Init scripts:** custom shell setup at cluster start (OS packages, JARs).
- **DBR variants:** Standard / ML Runtime (pre-installed ML libs) / Photon (checkbox on top) / LTS (extended support).

---

## 4. Ingestion, Streaming, DLT & Kafka

**Auto Loader key options:**

| Option | Purpose |
|---|---|
| `cloudFiles.format` | json/csv/parquet/avro/orc/binaryFile |
| `cloudFiles.schemaLocation` | **Required** — tracks schema across restarts |
| `cloudFiles.schemaEvolutionMode` | addNewColumns / rescue / failOnNewColumns / none |
| `cloudFiles.rescuedDataColumn` | captures unmatched fields |
| `cloudFiles.useNotifications` | false=Directory Listing (simple), true=File Notification (scales, needs cloud events) |
| `cloudFiles.maxFilesPerTrigger` / `maxBytesPerTrigger` | throughput throttle |
| `cloudFiles.cleanSource` | move/delete source after ingest |

- **Micro-batch** (default) vs **Continuous** (experimental, ~1ms latency, limited operators).
- **`foreachBatch`** = run arbitrary batch logic (e.g., `MERGE`) per micro-batch — standard streaming-upsert pattern.
- **Watermarking:** `withWatermark("event_time","10 minutes")` bounds state size, drops data later than threshold.
- **Output modes:** Append (no updates to past output) / Update (changed rows only) / Complete (full result every trigger, aggregations only).
- **Trigger types:** `processingTime`, `availableNow` (process all then stop — good for scheduled jobs; **replaces deprecated `trigger(once=True)`**), `continuous`.
- **Checkpoint dir** stores offsets/commits/state — enables exact recovery.

**DLT:**
- **Pipeline modes:** Triggered (run once, process available data, stop — cheaper) vs Continuous (cluster stays up, lowest latency, higher cost).
```python
@dlt.table
@dlt.expect("valid_amount", "amount > 0")            # track & warn
@dlt.expect_or_drop("valid_id", "id IS NOT NULL")     # drop bad rows
@dlt.expect_or_fail("valid_currency", "...")          # halt pipeline
def silver_orders(): return dlt.read_stream("bronze_orders")
```
Streaming Tables (incremental) / Materialized Views (managed aggregates) / Views (temp, pipeline-scoped).

**Kafka:**
```python
spark.readStream.format("kafka") \
  .option("kafka.bootstrap.servers", "...") \
  .option("subscribe", "topic") \
  .option("startingOffsets", "latest") \
  .option("maxOffsetsPerTrigger", 100000) \
  .option("minPartitions", 20) \
  .option("failOnDataLoss", "false").load()
```
- Kafka = **at-least-once** delivery. Exactly-once **end-to-end** = checkpoint (read side) + **idempotent sink** (Delta `MERGE` in `foreachBatch`).
- **Issues → Fixes:** consumer lag → `minPartitions`↑/scale executors; duplicates → idempotent MERGE; small files → longer trigger + autoCompact; partition skew → fix producer key / `minPartitions`↑; schema breaks → Avro+Schema Registry; late data → watermark; replay → new checkpoint dir + explicit offsets.

---

## 5. Governance, Security & Unity Catalog

- **Namespace:** `catalog.schema.table`. One metastore → many workspaces.
```sql
CREATE STORAGE CREDENTIAL cred WITH (AZURE_MANAGED_IDENTITY='...');
CREATE EXTERNAL LOCATION loc URL 's3://bucket/' WITH (STORAGE CREDENTIAL cred);
```
- **Managed table:** UC controls storage, `DROP TABLE` deletes data. **External table:** user-specified path, `DROP TABLE` keeps data.
- **Volumes** = governed non-tabular file access (`/Volumes/cat/schema/vol/...`). **Mounts are deprecated** (credential-based, no per-user audit) — use Volumes.
```sql
ALTER TABLE t ALTER COLUMN ssn SET MASK mask_fn;           -- dynamic column masking
ALTER TABLE t SET ROW FILTER region_filter ON (region);    -- row-level security
GRANT SELECT ON TABLE t TO `group`;
```
- **Lineage:** automatic, table + column level, no extra instrumentation.
- **System tables:** `system.billing.usage` (cost), `system.access.audit` (security), `system.lakeflow.*` (jobs).
- **Delta Sharing:** open protocol, share live tables cross-org/cross-cloud, zero-copy.
- **Secrets:** `dbutils.secrets.get(scope, key)` — value auto-redacted in output. Backends: Databricks-managed or Azure Key Vault-backed.

---

## 6. Production Ops, Orchestration & CI/CD

- **Workflows:** multi-task DAGs, `dbutils.jobs.taskValues` to pass data between tasks, conditional/For-Each branching.
- **Job cluster reuse:** same `job_cluster_key` across tasks = shared cluster, avoids repeated startup cost (trade-off: shared resources).
- Task retries: `max_retries`, `min_retry_interval_millis`. **Partial run repair** reruns only failed tasks.
- **DABs (`databricks.yml`):** IaC for jobs/pipelines. `databricks bundle validate/deploy/run -t <target>`.
- **Databricks Connect v2:** local IDE ↔ remote cluster via Spark Connect, real debugging.
```python
dbutils.fs.ls("/mnt/data/")
dbutils.widgets.text("run_date", "2024-01-01"); dbutils.widgets.get("run_date")
dbutils.notebook.run("/path/child", 600, {"key":"val"})   # separate job run, returns string
# %run ./utils   -> textual include, shared context
```
- **Spark UI tabs:** Jobs (timeline bottleneck) / Stages (task skew, GC time, shuffle R/W) / SQL (physical plan) / Executors (spill, thread dumps).
- **Testing:** `pytest` + local SparkSession fixture + `chispa.assert_df_equality`; mock `dbutils`.
- **Auth methods:** PAT (dev/scripting, tied to a user) → OAuth U2M (interactive login) → **Service Principal + OAuth M2M** (recommended for CI/CD/production, not tied to a person).

---

## 7. Lakehouse System Design, CDC & Troubleshooting

- **Medallion:** Bronze (raw, append-only, audit cols) → Silver (dedup, conformed) → Gold (aggregates, star schema).
- **SCD Type 1** = overwrite in place (`MERGE ... whenMatchedUpdateAll`). **SCD Type 2** = history tracked (`is_current`, `start_date`, `end_date`).
- **Driver OOM:** caused by `.collect()`/`.toPandas()` or oversized broadcast → aggregate first, disable auto-broadcast (`threshold=-1`).
- **Executor OOM/spill:** skew or too many cores/executor → reduce cores, increase `memoryOverhead`.
- **Skew fix:** AQE skew join first, salting (`rand()*N`) as fallback.
- **Small files:** `OPTIMIZE` + autoCompact. **Over-partitioning:** prefer Z-Order/Liquid Clustering over Hive partitioning on high-cardinality columns.
- **Streaming triage:** RocksDB state exhaustion → tighter TTL/watermark; watermark lag → check upstream delay; cloud throttling (503/429) → backoff, File Notification mode.

---

## 8. ML, GenAI & Emerging Services

```python
with mlflow.start_run():
    mlflow.log_param(...); mlflow.log_metric(...)
    mlflow.sklearn.log_model(model, "model", registered_model_name="m")
client.transition_model_version_stage(name="m", version=3, stage="Production")
model = mlflow.pyfunc.load_model("models:/m/Production")
```
- **Feature Store:** define features once (`FeatureLookup`), reuse identically in training + serving — avoids train/serve skew.
- **Model Serving:** managed REST endpoints, scale-to-zero.
- **Vector Search:** `create_delta_sync_index` — auto-synced embedding index for RAG; typical flow: Delta text chunks → embeddings → Vector Search → LLM context.
- **DBSQL:** Classic/Serverless/Pro Warehouses, Photon-accelerated. **Genie** = NL-to-SQL over governed UC tables.
- **Lakehouse Federation:** query external DBs (Postgres, Snowflake, etc.) live through UC, no ETL copy — `CREATE CONNECTION` / `CREATE FOREIGN CATALOG`.
- **AI Functions (SQL):** `ai_query()`, `ai_classify()`, `ai_translate()`, `ai_summarize()`, `ai_extract()` — inline LLM calls in SQL, governed like any query.
- **Lakehouse Monitoring:** auto profile + drift metrics on a table (Snapshot / Time Series / Inference Log monitors).
- **Networking:** VNet Injection/Customer-Managed VPC + PrivateLink (private control↔compute↔storage traffic).
- **DABs vs Terraform:** DABs = app-level (jobs/pipelines) CI/CD. Terraform = platform-level (workspaces, catalogs, network) IaC.
- **AutoML:** `automl.classify(dataset, target_col, timeout_minutes)` — generates baseline models + fully editable notebooks per trial, tracked in MLflow. A starting point, not a final answer.
- **Marketplace:** discover/subscribe to third-party datasets & models via Delta Sharing — appears as a live governed UC table, no data movement.
- **Pricing tiers:** Standard (core Spark/jobs, no UC) → Premium (Unity Catalog, RBAC, DBSQL) → Enterprise (+ advanced compliance/security). UC-dependent features assume **Premium+**.

---

## 9. Function Cookbook (Fast Lookup)

```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window
```

**Select/Filter:** `.select()`, `.filter()`/`.where()`, `.isin()`, `.like()`, `.isNull()`/`.isNotNull()`, `.distinct()`, `.dropDuplicates([cols])`

**Columns:** `.withColumn()`, `.withColumnRenamed()`, `.drop()`, `F.when().otherwise()`, `.cast()`

**Aggregation:** `.groupBy().agg(F.avg, F.sum, F.max, F.min, F.countDistinct, F.collect_list, F.collect_set)`, `.cube()`, `.rollup()`

**Window:**
```python
w = Window.partitionBy("dept").orderBy(F.col("salary").desc())
F.rank().over(w); F.dense_rank().over(w); F.row_number().over(w)
F.lag("col",1).over(w); F.lead("col",1).over(w)
w2 = Window.partitionBy("d").orderBy("id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
F.sum("x").over(w2)   # running total
```

**Joins:** `inner`, `left`/`left_outer`, `right`/`right_outer`, `full`/`outer`, `left_semi` (left cols, matches only), `left_anti` (left cols, non-matches only), `crossJoin`

**String:** `F.upper/lower/length/trim/concat/concat_ws/substring/split/regexp_extract/regexp_replace/lpad/rpad/initcap`

**Date:** `F.current_date/current_timestamp/to_date/to_timestamp/date_format/date_add/date_sub/datediff/months_between/year/month/dayofweek/last_day`

**Null handling:** `.na.drop()/.na.fill()/.na.replace()`, `F.coalesce()`, `F.isnull()`

**Array (higher-order, no UDF):** `F.transform`, `F.filter`, `F.aggregate`, `F.exists`, `F.forall`; `F.explode`, `F.posexplode`

**Schema:** `StructType([StructField(...)])`, `.printSchema()`, `.selectExpr()`

**RDD:** `.map/.filter/.flatMap/.reduce/.foreach/.foreachPartition/.mapPartitions`, `.reduceByKey/.groupByKey`

**Delta SQL:**
```sql
MERGE INTO t USING s ON t.id=s.id WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *;
OPTIMIZE t ZORDER BY (col); VACUUM t RETAIN 168 HOURS;
DESCRIBE HISTORY t; DESCRIBE DETAIL t; ANALYZE TABLE t COMPUTE STATISTICS;
SELECT * FROM t VERSION AS OF 3;  RESTORE TABLE t TO VERSION AS OF 3;
```

**File formats:**

| Format | Read | Notes |
|---|---|---|
| CSV | `.option("header",True).option("multiLine",True).csv()` | `mode`: PERMISSIVE/DROPMALFORMED/FAILFAST |
| JSON | `.option("multiLine",True).json()` | `from_json`/`to_json`/`get_json_object` for JSON-string columns |
| Parquet/ORC | `.parquet()` / `.orc()` | Columnar, embedded schema |
| Avro | `.format("avro")` | Kafka/Schema Registry standard |
| binaryFile | `.format("binaryFile")` | Images/PDFs, raw bytes |
| XML | `.format("xml").option("rowTag",...)` | needs spark-xml lib |
| Excel | `.format("com.crealytics.spark.excel")` | or pandas for small files |

---

## 10. Performance Decision Table (Master Cheat Sheet)

| Symptom | Fix |
|---|---|
| Small table joined to large, plan shows SMJ | `broadcast()` hint |
| Same join key reused across many jobs | Bucketing |
| A few tasks take 10-100x longer | AQE skew join → manual salting |
| Repeated filters on medium/high-cardinality col | Z-Order / Liquid Clustering |
| Millions of tiny files | `OPTIMIZE` + autoCompact, wider trigger interval |
| Driver OOM after `.collect()`/`.toPandas()` | Aggregate first, or write to table |
| Executor spill/OOM during shuffle | Fewer cores/executor, ↑`memoryOverhead` |
| Broadcast join risk of OOM | `autoBroadcastJoinThreshold = -1` |
| Need only a few columns from wide table | Select/project early (pushdown) |
| Approx distinct count acceptable | `approx_count_distinct` |
| Same DataFrame reused across actions | `.cache()`/`.persist()` |
| Very long iterative chain | `.checkpoint()` |
| Row-by-row Python UDF bottleneck | built-in fn → higher-order fn → Pandas UDF → plain UDF |
| Kafka consumer lag growing | `minPartitions`↑, then scale executors |
| Stateful stream memory growing unbounded | Tighter watermark, RocksDB state store |
| CPU-bound heavy shuffle | Compute-optimized instances |
| Large joins/aggregations/caching-heavy | Memory-optimized instances |
| Free general speed boost | Enable Photon |
| RDD/UDF-object-heavy workload | Kryo serialization |
| Cluster slow to start/scale | Instance Pools |
| Restrict what users can launch | Cluster Policies |
| Stale join/agg plan decisions | `ANALYZE TABLE ... COMPUTE STATISTICS` |

---

## Rapid-Fire One-Liners

| Q | A |
|---|---|
| Narrow vs wide transformation? | No shuffle vs shuffle required |
| `repartition` vs `coalesce`? | Full shuffle, ↑/↓ vs no shuffle, ↓ only |
| What triggers a job? | An action |
| `OPTIMIZE` vs `VACUUM`? | Compact small files vs delete old unreferenced files |
| Deletion Vectors? | Merge-on-read update/delete, no full file rewrite |
| Z-Order vs partitioning? | Multi-column data skipping vs physical directory split |
| Why Pandas UDF over plain UDF? | Arrow vectorization avoids row-by-row serialization |
| AQE benefits? | Runtime coalescing, join-strategy switch, auto skew handling |
| Broadcast threshold default? | 10MB |
| SCD1 vs SCD2? | Overwrite vs full history (`is_current`/dates) |
| Shallow vs Deep clone? | Metadata-only vs full data copy |
| Photon? | Native C++ vectorized engine, transparent, partial fallback to JVM |
| Cache vs Checkpoint? | Memory+lineage kept vs durable storage+lineage truncated |
| Accumulator gotcha? | Exactly-once only in actions, not transformations |
| Why `ANALYZE TABLE`? | Populates stats for CBO join reordering |
| MLflow Tracking vs Registry? | Log runs vs manage model lifecycle stages |
| Feature Store purpose? | One feature definition, reused train+serve |
| Lakehouse Federation? | Query external DBs live via UC, no ETL copy |
| DABs vs Terraform? | App-level CI/CD vs platform-level infra IaC |
| Kafka delivery guarantee? | At-least-once; effective exactly-once needs idempotent sink |
| Mounts vs UC Volumes? | Credential-based/legacy vs identity-governed/current |
