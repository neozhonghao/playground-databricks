# Databricks notebook source

# MAGIC %md
# MAGIC # Section 1 — Databricks Intelligence Platform
# MAGIC
# MAGIC **Exam objectives covered:**
# MAGIC - Understand the core components: architecture, Delta Lake, Unity Catalog
# MAGIC - Explain the value of the Data Intelligence Platform
# MAGIC - Enable features that simplify data layout decisions and optimize query performance

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.1 Platform Architecture Overview
# MAGIC
# MAGIC The Databricks Data Intelligence Platform consists of:
# MAGIC
# MAGIC | Layer | Component | Role |
# MAGIC |---|---|---|
# MAGIC | Storage | **Delta Lake** | Open-format ACID table layer on cloud object storage |
# MAGIC | Metadata | **Unity Catalog** | Unified governance, lineage, and access control |
# MAGIC | Compute | Clusters / Serverless / SQL Warehouses | Execute notebooks, jobs, queries |
# MAGIC | Orchestration | **Lakeflow Jobs** | Schedule and monitor data pipelines |
# MAGIC | Ingestion | **Auto Loader / Lakeflow Connect** | Ingest batch and streaming data |
# MAGIC | Intelligence | AI/BI, Genie, DatabricksIQ | Embedded AI across the platform |
# MAGIC
# MAGIC **Key value propositions:**
# MAGIC - Single platform for all data personas (engineers, analysts, scientists)
# MAGIC - Lakehouse pattern: warehouse-like reliability + data-lake flexibility + AI-native
# MAGIC - Open standards (Delta Lake, Apache Spark, MLflow) avoid vendor lock-in

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.2 Delta Lake Core Concepts
# MAGIC
# MAGIC Delta Lake adds a **transaction log** (`_delta_log/`) on top of Parquet files.
# MAGIC
# MAGIC Features that simplify data layout and optimize query performance:
# MAGIC
# MAGIC | Feature | What it does |
# MAGIC |---|---|
# MAGIC | **ACID transactions** | Serializable isolation; concurrent reads/writes |
# MAGIC | **Schema enforcement** | Rejects writes that violate the table schema |
# MAGIC | **Schema evolution** | `mergeSchema` option adds new columns safely |
# MAGIC | **Time travel** | Query previous versions with `VERSION AS OF` / `TIMESTAMP AS OF` |
# MAGIC | **Z-Ordering** | Co-locates related data in files for faster range predicates |
# MAGIC | **Liquid Clustering** | Replaces static partitioning; auto re-clusters incrementally |
# MAGIC | **Predictive Optimization** | Databricks auto-runs OPTIMIZE/VACUUM based on usage patterns |
# MAGIC | **Data skipping** | Min/max stats per file; skip irrelevant files at query time |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.3 Liquid Clustering vs Traditional Partitioning
# MAGIC
# MAGIC Traditional `PARTITIONED BY` creates a static directory layout chosen at DDL time.
# MAGIC **Liquid Clustering** is flexible and incremental:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a table with Liquid Clustering on two columns
# MAGIC CREATE TABLE IF NOT EXISTS main.default.sensor_readings (
# MAGIC     sensor_id  STRING,
# MAGIC     event_time TIMESTAMP,
# MAGIC     region     STRING,
# MAGIC     value      DOUBLE
# MAGIC )
# MAGIC CLUSTER BY (region, event_time);
# MAGIC
# MAGIC -- Re-cluster on demand (or let Predictive Optimization do it automatically)
# MAGIC OPTIMIZE main.default.sensor_readings;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.4 Predictive Optimization (Serverless)
# MAGIC
# MAGIC When enabled at the catalog/schema level, Databricks automatically:
# MAGIC 1. Determines which tables need `OPTIMIZE` or `VACUUM`
# MAGIC 2. Schedules and runs these operations serverlessly — no manual cron jobs
# MAGIC
# MAGIC Enable at the metastore or catalog level via the UI:
# MAGIC **Catalog Explorer → Catalog → Edit → Predictive Optimization: Enabled**
# MAGIC
# MAGIC Or via SQL:
# MAGIC ```sql
# MAGIC ALTER CATALOG main SET PREDICTIVE_OPTIMIZATION = ON;
# MAGIC ALTER SCHEMA main.default SET PREDICTIVE_OPTIMIZATION = INHERIT;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.5 Unity Catalog Architecture
# MAGIC
# MAGIC Unity Catalog uses a three-level namespace: **`catalog.schema.table`**
# MAGIC
# MAGIC ```
# MAGIC Metastore (one per region, attached to workspace)
# MAGIC └── Catalog
# MAGIC     └── Schema (Database)
# MAGIC         ├── Tables / Views / Materialized Views
# MAGIC         ├── Volumes (files)
# MAGIC         └── Functions
# MAGIC ```
# MAGIC
# MAGIC **Key roles:**
# MAGIC
# MAGIC | Role | Scope | Capabilities |
# MAGIC |---|---|---|
# MAGIC | Metastore Admin | Metastore | Assign catalog/workspace permissions, configure storage |
# MAGIC | Catalog Owner | Catalog | Grant privileges within the catalog |
# MAGIC | Data Steward / Schema Owner | Schema | Manage objects inside the schema |
# MAGIC | Data Consumer | Table/View | READ access granted explicitly |
# MAGIC
# MAGIC See `06_governance/` for hands-on GRANT / REVOKE examples.
