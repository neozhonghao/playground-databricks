# Databricks notebook source

# MAGIC %md
# MAGIC # Section 1 — Compute Types
# MAGIC
# MAGIC **Exam objectives covered:**
# MAGIC - Identify the applicable compute to use for a specific use case
# MAGIC - Understand compute characteristics, limitations, and cost models
# MAGIC - Use serverless for hands-off, auto-optimized compute

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1 Compute Options Comparison
# MAGIC
# MAGIC | Compute Type | Best For | Billing | Auto-scaling |
# MAGIC |---|---|---|---|
# MAGIC | **All-Purpose Cluster** | Interactive notebooks, ad-hoc dev | DBU/hour (running) | Optional |
# MAGIC | **Job Cluster** | Scheduled batch jobs | DBU/hour (job only) | Optional |
# MAGIC | **SQL Warehouse (Classic)** | SQL analytics, BI tools | DBU/hour (active) | Yes |
# MAGIC | **Serverless SQL Warehouse** | SQL analytics (instant-on) | DBU/second (active) | Fully managed |
# MAGIC | **Serverless Jobs** | Lakeflow Jobs, notebooks | DBU/second (active) | Fully managed |
# MAGIC | **Serverless Pipelines** | Lakeflow Spark Declarative Pipelines | DBU/second | Fully managed |
# MAGIC
# MAGIC **Key distinctions for the exam:**
# MAGIC - **All-purpose clusters** keep running even when idle — costly for production jobs
# MAGIC - **Job clusters** start fresh per run — cheaper, more isolated
# MAGIC - **Serverless** eliminates cluster config entirely; Databricks manages the infrastructure

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2 Cluster Modes
# MAGIC
# MAGIC | Mode | Description | Use case |
# MAGIC |---|---|---|
# MAGIC | **Standard (Single User)** | One user, full Spark capabilities | Default for notebooks |
# MAGIC | **Shared** | Multi-user, kernel isolation per user | Cost sharing in dev |
# MAGIC | **No Isolation Shared** (legacy) | No per-user isolation | Not recommended |
# MAGIC
# MAGIC **Access modes and Unity Catalog:**
# MAGIC - Unity Catalog requires **Single User** or **Shared** access mode
# MAGIC - `No Isolation Shared` does NOT support UC external locations or row-level security

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.3 Choosing Cluster Configuration
# MAGIC
# MAGIC **Memory-optimised instances** → wide transformations, large shuffles (joins on big datasets)
# MAGIC **Compute-optimised instances** → CPU-intensive ML training, feature engineering
# MAGIC **Storage-optimised (with local NVMe)** → cache-heavy workloads, Delta Cache reads
# MAGIC
# MAGIC ### Auto-scaling
# MAGIC - Set `min_workers` and `max_workers`
# MAGIC - Databricks adds/removes workers based on the backlog of pending tasks
# MAGIC - Not recommended for streaming jobs (causes rebalancing)

# COMMAND ----------

# Programmatically read current cluster config (works in notebook context)
import json

try:
    ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    cluster_id = ctx.clusterId().get()
    print(f"Current cluster ID: {cluster_id}")
except Exception:
    print("Run inside a Databricks notebook to see cluster context.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.4 Serverless — Key Points
# MAGIC
# MAGIC - No cluster selection or sizing needed — Databricks manages all infrastructure
# MAGIC - Instant start (seconds vs minutes for classic clusters)
# MAGIC - Billed per second of active compute, not per hour of uptime
# MAGIC - Supports: SQL Warehouses, Jobs (notebooks, Python, SQL tasks), Pipelines
# MAGIC - State is stored in Unity Catalog volumes; no local disk dependency
# MAGIC
# MAGIC **Enable serverless for a job:**
# MAGIC In the Lakeflow Jobs UI → Compute → select **Serverless**
# MAGIC
# MAGIC Or in `databricks.yml`:
# MAGIC ```yaml
# MAGIC jobs:
# MAGIC   - name: my_job
# MAGIC     job_clusters: []          # no cluster definition needed
# MAGIC     tasks:
# MAGIC       - task_key: ingest
# MAGIC         notebook_task:
# MAGIC           notebook_path: ./02_ingestion/02_autoloader.py
# MAGIC         environment_key: default
# MAGIC environments:
# MAGIC   - environment_key: default
# MAGIC     spec:
# MAGIC       client: "1"             # serverless runtime version
# MAGIC ```
