# Databricks notebook source

# MAGIC %md
# MAGIC # Section 3 — Spark Tuning Parameters
# MAGIC
# MAGIC **Exam objectives covered:**
# MAGIC - Understand basic tuning parameters and re-measure performance
# MAGIC - Identify common bottlenecks: data skew, shuffling, disk spilling
# MAGIC - Analyze the Spark UI to optimize queries

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Spark Configuration Parameters
# MAGIC
# MAGIC | Parameter | Default | Effect |
# MAGIC |---|---|---|
# MAGIC | `spark.sql.shuffle.partitions` | 200 | Number of partitions after a shuffle (join/groupBy). Lower for small data, higher for large data. |
# MAGIC | `spark.default.parallelism` | 2 × CPU cores | Default parallelism for RDD operations (non-SQL). |
# MAGIC | `spark.executor.memory` | 1g | Heap memory per executor JVM process. |
# MAGIC | `spark.driver.memory` | 1g | Heap memory for the driver (collects results). |
# MAGIC | `spark.sql.autoBroadcastJoinThreshold` | 10MB | Tables ≤ this size are auto-broadcast in joins. Set to `-1` to disable. |
# MAGIC | `spark.executor.cores` | 1 | vCPUs per executor. More = more tasks in parallel per executor. |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setting Parameters

# COMMAND ----------

# At runtime (overrides cluster defaults for this session)
spark.conf.set("spark.sql.shuffle.partitions", "50")      # for a medium-sized dataset
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(50 * 1024 * 1024))  # 50 MB

# Verify
print(spark.conf.get("spark.sql.shuffle.partitions"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Diagnosing Data Skew
# MAGIC
# MAGIC Data skew occurs when one partition has significantly more data than others.
# MAGIC
# MAGIC **Symptoms in Spark UI:**
# MAGIC - Stage has mostly fast tasks, but one or a few tasks take much longer ("stragglers")
# MAGIC - "Max task duration" >> "Median task duration" in the Stage detail page
# MAGIC - Large "Shuffle Read Size" for a single task
# MAGIC
# MAGIC **Remedies:**
# MAGIC 1. **Salting**: add a random salt to the join key to spread the skewed key across partitions
# MAGIC 2. **Broadcast join**: broadcast the smaller table to avoid shuffle altogether
# MAGIC 3. **AQE (Adaptive Query Execution)**: enabled by default in DBR 7+; auto-coalesces small partitions and handles skew join splitting

# COMMAND ----------

# AQE is ON by default — verify
print(spark.conf.get("spark.sql.adaptive.enabled"))           # true
print(spark.conf.get("spark.sql.adaptive.skewJoin.enabled"))  # true

# COMMAND ----------

# MAGIC %md
# MAGIC ## Diagnosing Shuffle Overhead
# MAGIC
# MAGIC **Shuffles** occur on `groupBy`, `join` (non-broadcast), `distinct`, `repartition`.
# MAGIC They write data to disk and transfer over the network.
# MAGIC
# MAGIC **Reduce shuffles:**
# MAGIC - Use `broadcast()` for small dimension tables
# MAGIC - Pre-sort / bucket tables on the join key
# MAGIC - Lower `shuffle.partitions` if each partition is tiny (reduces scheduling overhead)

# COMMAND ----------

from pyspark.sql import functions as F

large_df = spark.range(10_000_000).withColumn("grp", (F.col("id") % 100).cast("string"))
small_df = spark.createDataFrame([(str(i), f"label_{i}") for i in range(100)], ["grp", "label"])

# Without broadcast hint — triggers sort-merge join (shuffle)
no_bcast = large_df.join(small_df, on="grp")

# With broadcast hint — no shuffle for the small table
with_bcast = large_df.join(F.broadcast(small_df), on="grp")

# Compare plans
no_bcast.explain(mode="formatted")
with_bcast.explain(mode="formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Diagnosing Disk Spilling
# MAGIC
# MAGIC Disk spilling happens when executor memory is insufficient to hold shuffle data or sort buffers.
# MAGIC
# MAGIC **Symptoms in Spark UI:**
# MAGIC - "Spill (Memory)" and "Spill (Disk)" columns in the Stage detail are non-zero
# MAGIC
# MAGIC **Remedies:**
# MAGIC - Increase `spark.executor.memory`
# MAGIC - Reduce `spark.sql.shuffle.partitions` to make each partition smaller
# MAGIC - Use memory-optimised instance types
# MAGIC - Enable Delta Cache (local NVMe caching of Parquet/Delta data)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading the Spark UI — Cheat Sheet
# MAGIC
# MAGIC | UI Tab | What to look at | Common issue |
# MAGIC |---|---|---|
# MAGIC | **Jobs** | Job duration, failed jobs | Long-running or failed jobs |
# MAGIC | **Stages** | Stage duration, task distribution | Skew (one long task), spill |
# MAGIC | **Tasks (in Stage)** | Individual task times | Stragglers |
# MAGIC | **SQL / DataFrame** | Annotated DAG with row counts | Wrong join type, missing filter pushdown |
# MAGIC | **Executors** | GC time, spill | High GC → increase memory |
# MAGIC | **Storage** | Cached RDDs/DataFrames | Unnecessary caching |
# MAGIC
# MAGIC Access Spark UI: **Cluster UI → Spark UI** or via the running job in Lakeflow Jobs.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Repartition vs Coalesce
# MAGIC
# MAGIC | Operation | When to use | Shuffles? |
# MAGIC |---|---|---|
# MAGIC | `repartition(n)` | Increase OR decrease partitions; even distribution | Yes |
# MAGIC | `repartition(n, col)` | Hash-partition by column (e.g., before a join) | Yes |
# MAGIC | `coalesce(n)` | Decrease partitions only; merges local partitions | No (preferred for final write) |

# COMMAND ----------

df = spark.range(1_000_000)

# Check current number of partitions
print(f"Initial partitions: {df.rdd.getNumPartitions()}")

# Increase to 16
df_repartitioned = df.repartition(16, "id")
print(f"After repartition(16): {df_repartitioned.rdd.getNumPartitions()}")

# Reduce to 4 without shuffle
df_coalesced = df_repartitioned.coalesce(4)
print(f"After coalesce(4): {df_coalesced.rdd.getNumPartitions()}")
