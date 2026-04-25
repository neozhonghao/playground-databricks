# Databricks notebook source

# MAGIC %md
# MAGIC # Section 7 — Troubleshooting, Monitoring & Optimization
# MAGIC
# MAGIC **Exam objectives covered:**
# MAGIC - Identify trends using Lakeflow Jobs run history
# MAGIC - Monitor pipeline health via the Jobs UI (DAG, task statuses, failure rates)
# MAGIC - Identify bottlenecks: data skew, shuffling, disk spilling (Spark UI)
# MAGIC - Liquid Clustering and Predictive Optimization
# MAGIC - Diagnose cluster startup failures, library conflicts, OOM issues

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lakeflow Jobs Run History — Trend Analysis
# MAGIC
# MAGIC | What to look at | Where | What it tells you |
# MAGIC |---|---|---|
# MAGIC | **Run duration chart** | Job page → Runs tab → Timeline view | Compare today's run vs historical baseline |
# MAGIC | **Task-level duration** | Click a run → DAG view | Which task is the bottleneck |
# MAGIC | **Failure rate** | Runs tab — count of FAILED vs SUCCEEDED | Reliability issues |
# MAGIC | **Queue time** | Run details → Task → Start time vs scheduled time | Cluster startup or queue contention |
# MAGIC
# MAGIC **Red flags:**
# MAGIC - Today's run duration is 3× the median → data volume spike or code regression
# MAGIC - Queue time > 5 min → cluster cold-start; switch to serverless or pre-warm clusters
# MAGIC - Task intermittently fails → flaky external dependency; add retries

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spark UI — Stage-Level Diagnostics

# COMMAND ----------

# Generate a shuffle-heavy workload to demonstrate Spark UI metrics
from pyspark.sql import functions as F

large_df = (
    spark.range(5_000_000)
    .withColumn("key",   (F.col("id") % 50).cast("string"))
    .withColumn("value", (F.col("id") * 3.14).cast("double"))
)

# groupBy triggers a shuffle — visible as "Exchange" in Spark UI → SQL tab
result = large_df.groupBy("key").agg(F.sum("value").alias("total")).collect()
print(f"Groups: {len(result)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spark UI Navigation for Performance Issues
# MAGIC
# MAGIC ### 1. Find the slow stage
# MAGIC - **Jobs** tab → click the job → find the stage with the longest blue bar
# MAGIC
# MAGIC ### 2. Identify skew
# MAGIC - **Stages** tab → click the stage → scroll to **Tasks** section
# MAGIC - Look for: Max duration >> Median duration (one task takes 10× longer than others)
# MAGIC - Look for: Large "Shuffle Read Size" on a single task
# MAGIC
# MAGIC ### 3. Identify spill
# MAGIC - **Stages** tab → "Spill (Memory)" and "Spill (Disk)" columns
# MAGIC - Non-zero spill = executor ran out of memory; remedy: increase executor memory or reduce shuffle partitions
# MAGIC
# MAGIC ### 4. Check the SQL plan
# MAGIC - **SQL/DataFrame** tab → click the query → annotated DAG with row counts
# MAGIC - Check for `SortMergeJoin` (shuffle) vs `BroadcastHashJoin` (no shuffle)
# MAGIC - Check for `Filter` nodes — are they pushed down below `Scan`?

# COMMAND ----------

# Inspect the query plan programmatically
(
    large_df
    .filter(F.col("value") > 1000)
    .groupBy("key")
    .agg(F.sum("value"))
    .explain(mode="formatted")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Liquid Clustering — Key Features
# MAGIC
# MAGIC | Feature | Liquid Clustering | Z-Ordering | Static Partitioning |
# MAGIC |---|---|---|---|
# MAGIC | Column limit | Multiple | Multiple | 1–2 recommended |
# MAGIC | Incremental re-cluster | Yes (OPTIMIZE runs partially) | No (full OPTIMIZE needed) | No |
# MAGIC | Flexible changes | Add/change cluster keys without rewrite | Rewrite required | Requires migration |
# MAGIC | Best for | Frequently changing query patterns | Moderate cardinality | High cardinality (e.g., date) |
# MAGIC
# MAGIC ```sql
# MAGIC -- Add clustering to an existing table
# MAGIC ALTER TABLE main.silver.patient_billing
# MAGIC CLUSTER BY (department, billing_date);
# MAGIC
# MAGIC -- Run OPTIMIZE to apply clustering (or let Predictive Optimization do it)
# MAGIC OPTIMIZE main.silver.patient_billing;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Predictive Optimization
# MAGIC
# MAGIC When enabled, Databricks automatically identifies tables that need maintenance
# MAGIC (`OPTIMIZE`, `VACUUM`) and runs them on serverless compute — no cron jobs needed.
# MAGIC
# MAGIC **Enable:**
# MAGIC ```sql
# MAGIC ALTER CATALOG main SET PREDICTIVE_OPTIMIZATION = ON;
# MAGIC -- Schemas/tables inherit INHERIT by default
# MAGIC ```
# MAGIC
# MAGIC **Disable for a specific schema:**
# MAGIC ```sql
# MAGIC ALTER SCHEMA main.bronze SET PREDICTIVE_OPTIMIZATION = OFF;
# MAGIC ```
# MAGIC
# MAGIC **Monitor:**
# MAGIC ```sql
# MAGIC SELECT * FROM system.storage.predictive_optimization_operations_history
# MAGIC WHERE operation_timestamp > current_timestamp() - INTERVAL 7 DAYS;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Diagnosing Cluster Issues
# MAGIC
# MAGIC | Symptom | Likely cause | Investigation / Fix |
# MAGIC |---|---|---|
# MAGIC | **Cluster stuck at "Pending"** | Cloud quota exceeded; instance type unavailable | Check cloud console quotas; switch instance type |
# MAGIC | **Cluster fails to start** | Init script error; library install failure | Check **Event Log** → Init script output; check Driver logs |
# MAGIC | **Library conflict** | Two libraries require incompatible versions | Use cluster-scoped libraries; pin versions; use virtual envs |
# MAGIC | **OutOfMemoryError (executor)** | Executor heap exhausted | Increase `spark.executor.memory`; use memory-optimised instances; reduce shuffle partitions |
# MAGIC | **OutOfMemoryError (driver)** | Large `collect()` or toPandas() | Avoid collecting large DataFrames; use `display(df.limit(n))` |
# MAGIC | **GC overhead limit exceeded** | Too many small objects | Increase executor memory; reduce partition count so fewer objects per partition |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cluster Event Log — Reading Startup Errors
# MAGIC
# MAGIC Navigate to: **Compute → your cluster → Event Log**
# MAGIC
# MAGIC Events to look for:
# MAGIC - `INIT_SCRIPTS_FINISHED` with errors → init script failed
# MAGIC - `DRIVER_NOT_FOUND` → instance type unavailable in the AZ
# MAGIC - `CLUSTER_START_TIMED_OUT` → provisioning took too long (cloud issue)
# MAGIC
# MAGIC **Driver logs:**
# MAGIC - `Compute → cluster → Driver logs → stderr` — shows library install errors, OOM stack traces

# COMMAND ----------

# MAGIC %md
# MAGIC ## Caching — When and How

# COMMAND ----------

# Cache a DataFrame when it will be used multiple times in different operations
expensive_df = (
    spark.table("main.silver.patient_billing")
    .filter(F.col("billing_date") >= "2024-01-01")
    .groupBy("department")
    .agg(F.sum("amount_billed").alias("dept_total"))
)

expensive_df.cache()          # lazy — actual caching happens on first action
expensive_df.count()          # trigger caching

# Use the cached result multiple times
display(expensive_df.filter(F.col("dept_total") > 10_000))
display(expensive_df.orderBy(F.col("dept_total").desc()))

# Unpersist when done
expensive_df.unpersist()
