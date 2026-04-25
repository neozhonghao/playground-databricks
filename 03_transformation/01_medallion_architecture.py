# Databricks notebook source

# MAGIC %md
# MAGIC # Section 3 — Medallion Architecture
# MAGIC
# MAGIC **Exam objectives covered:**
# MAGIC - Describe the three layers and the purpose of each
# MAGIC - Implement data cleaning: bronze → silver
# MAGIC - Understand Gold layer objects (materialized views, views, streaming tables)

# COMMAND ----------

# MAGIC %md
# MAGIC ## The Three Layers
# MAGIC
# MAGIC | Layer | Purpose | Data State | Typical Format |
# MAGIC |---|---|---|---|
# MAGIC | **Bronze** | Raw ingestion; preserve source data | As-is (may have nulls, duplicates, bad types) | Delta table (append-only) |
# MAGIC | **Silver** | Cleaned, validated, conformed | Deduplicated, typed, enriched | Delta table (MERGE / overwrite) |
# MAGIC | **Gold** | Business-ready aggregates / metrics | Aggregated, joined, business logic applied | Delta table, Materialized View, View |
# MAGIC
# MAGIC **Key principle:** each layer only transforms data from the layer below; you can always
# MAGIC reprocess by replaying from Bronze.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze — Raw Ingest (Auto Loader pattern)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS main.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS main.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS main.gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Bronze: raw patient billing, loaded with Auto Loader or COPY INTO
# MAGIC CREATE TABLE IF NOT EXISTS main.bronze.patient_billing (
# MAGIC     billing_id   STRING,
# MAGIC     patient_id   STRING,
# MAGIC     department   STRING,
# MAGIC     billing_date STRING,   -- raw string; will be cast in silver
# MAGIC     amount_billed STRING,  -- raw string
# MAGIC     quantity     STRING,
# MAGIC     _source_file STRING,   -- Auto Loader metadata column
# MAGIC     _load_time   TIMESTAMP
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver — Clean & Validate

# COMMAND ----------

from pyspark.sql.functions import (
    col, to_date, to_timestamp, trim, upper, when, isnan,
    current_timestamp, lit
)
from pyspark.sql.types import DoubleType, IntegerType

bronze_df = spark.table("main.bronze.patient_billing")

silver_df = (
    bronze_df
    # Cast data types
    .withColumn("billing_date",  to_date(col("billing_date"), "yyyy-MM-dd"))
    .withColumn("amount_billed", col("amount_billed").cast(DoubleType()))
    .withColumn("quantity",      col("quantity").cast(IntegerType()))
    # Standardise strings
    .withColumn("department",    upper(trim(col("department"))))
    # Remove rows where key identifiers are null
    .filter(col("billing_id").isNotNull() & col("patient_id").isNotNull())
    # Remove rows with invalid amounts
    .filter(col("amount_billed") > 0)
    # Add audit column
    .withColumn("silver_load_ts", current_timestamp())
    # Drop bronze metadata columns
    .drop("_source_file", "_load_time")
)

# COMMAND ----------

# Write to silver using MERGE to avoid duplicates (upsert by billing_id)
silver_df.createOrReplaceTempView("silver_staging")

spark.sql("""
    MERGE INTO main.silver.patient_billing AS target
    USING silver_staging AS source
    ON target.billing_id = source.billing_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold — Aggregations & Business Metrics

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Gold: daily revenue fact table
# MAGIC CREATE OR REPLACE TABLE main.gold.daily_revenue AS
# MAGIC SELECT
# MAGIC     billing_date,
# MAGIC     department,
# MAGIC     SUM(amount_billed)           AS total_revenue,
# MAGIC     COUNT_IF(quantity > 0)       AS total_invoices,
# MAGIC     COUNT(DISTINCT patient_id)   AS unique_patients
# MAGIC FROM main.silver.patient_billing
# MAGIC GROUP BY billing_date, department;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer Object Types
# MAGIC
# MAGIC | Object | Storage | Auto-refresh | Streaming | Use case |
# MAGIC |---|---|---|---|---|
# MAGIC | **Table** | Materialised on disk | Manual | No | Heavy aggregates computed once |
# MAGIC | **View** | No storage (query on read) | Always fresh | No | Simple projections / filters |
# MAGIC | **Materialized View** | Materialised on disk | Incremental (Delta) | No | Expensive aggregations, refreshed periodically |
# MAGIC | **Streaming Table** | Materialised on disk | Continuous / triggered | Yes | Near-real-time metrics |

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Materialized View (requires Lakeflow Spark Declarative Pipelines or UC serverless)
# MAGIC CREATE MATERIALIZED VIEW IF NOT EXISTS main.gold.mv_dept_revenue AS
# MAGIC SELECT
# MAGIC     department,
# MAGIC     DATE_TRUNC('month', billing_date) AS month,
# MAGIC     SUM(amount_billed) AS monthly_revenue
# MAGIC FROM main.silver.patient_billing
# MAGIC GROUP BY department, DATE_TRUNC('month', billing_date);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Regular view: always reads from the current silver table — no storage cost
# MAGIC CREATE OR REPLACE VIEW main.gold.v_cardiology_revenue AS
# MAGIC SELECT billing_date, SUM(amount_billed) AS total
# MAGIC FROM main.silver.patient_billing
# MAGIC WHERE department = 'CARDIOLOGY'
# MAGIC GROUP BY billing_date;
