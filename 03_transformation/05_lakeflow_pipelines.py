# Databricks notebook source

# MAGIC %md
# MAGIC # Section 3 — Lakeflow Spark Declarative Pipelines (LDP)
# MAGIC
# MAGIC **Exam objectives covered:**
# MAGIC - Emphasize the advantages of LDP for ETL in Databricks
# MAGIC - Implement data pipelines using LDP
# MAGIC - Apply data quality checks with expectations
# MAGIC - Understand Streaming Tables and Materialized Views in LDP

# COMMAND ----------

# MAGIC %md
# MAGIC ## What is Lakeflow Spark Declarative Pipelines?
# MAGIC
# MAGIC LDP (formerly Delta Live Tables / DLT) lets you define ETL pipelines **declaratively**
# MAGIC using Python or SQL. Databricks manages orchestration, infrastructure, and data quality.
# MAGIC
# MAGIC **Key advantages:**
# MAGIC
# MAGIC | Advantage | Detail |
# MAGIC |---|---|
# MAGIC | **Declarative** | You define WHAT you want, not HOW to execute it |
# MAGIC | **Automatic dependency resolution** | Databricks builds the DAG from table references |
# MAGIC | **Data quality enforcement** | `@dlt.expect` / `CONSTRAINT` for built-in quality checks |
# MAGIC | **Auto-scaling** | Uses Delta Live Tables compute; scales automatically |
# MAGIC | **Incremental processing** | Streaming Tables process only new data |
# MAGIC | **Simplified error handling** | Quarantine bad records automatically |
# MAGIC | **Unity Catalog native** | All tables are UC-governed by default |

# COMMAND ----------

# MAGIC %md
# MAGIC ## LDP Pipeline — Python API
# MAGIC
# MAGIC > **Note:** LDP notebooks must be attached to a **Pipeline** (not a regular cluster).
# MAGIC > The `import dlt` is only available in the LDP runtime.

# COMMAND ----------

import dlt
from pyspark.sql.functions import col, to_date, upper, trim, current_timestamp

# COMMAND ----------

# Bronze — Streaming Table: ingest raw events via Auto Loader
@dlt.table(
    name="bronze_patient_billing",
    comment="Raw billing records from the data lake",
    table_properties={"quality": "bronze"},
)
def bronze_patient_billing():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("cloudFiles.schemaLocation", "/pipelines/schema/billing")
            .load("abfss://raw@storageacct.dfs.core.windows.net/billing/")
    )

# COMMAND ----------

# Silver — Streaming Table with data quality expectations
@dlt.table(
    name="silver_patient_billing",
    comment="Cleaned and validated billing records",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_billing_id", "billing_id IS NOT NULL")
@dlt.expect_or_drop("positive_amount",  "amount_billed > 0")
@dlt.expect("valid_date", "billing_date IS NOT NULL")   # warn but keep
def silver_patient_billing():
    return (
        dlt.read_stream("bronze_patient_billing")
        .withColumn("billing_date",  to_date(col("billing_date"), "yyyy-MM-dd"))
        .withColumn("amount_billed", col("amount_billed").cast("double"))
        .withColumn("department",    upper(trim(col("department"))))
        .withColumn("silver_ts",     current_timestamp())
    )

# COMMAND ----------

# Gold — Materialized View: auto-refreshes when silver changes
@dlt.table(
    name="gold_daily_revenue",
    comment="Daily revenue aggregated by department",
)
def gold_daily_revenue():
    return (
        dlt.read("silver_patient_billing")
        .groupBy("billing_date", "department")
        .agg(
            {"amount_billed": "sum", "billing_id": "count"}
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality: Expect Modes
# MAGIC
# MAGIC | Decorator | Behaviour on violation |
# MAGIC |---|---|
# MAGIC | `@dlt.expect(name, condition)` | Record the failure metric, keep the row |
# MAGIC | `@dlt.expect_or_drop(name, condition)` | Drop violating rows, record metric |
# MAGIC | `@dlt.expect_or_fail(name, condition)` | Halt the pipeline on any violation |
# MAGIC | `@dlt.expect_all(rules_dict)` | Apply multiple rules with default "keep" |
# MAGIC | `@dlt.expect_all_or_drop(rules_dict)` | Apply multiple rules with "drop" |

# COMMAND ----------

# MAGIC %md
# MAGIC ## LDP Pipeline — SQL Syntax (Alternative)
# MAGIC
# MAGIC ```sql
# MAGIC -- Streaming Table (bronze)
# MAGIC CREATE OR REFRESH STREAMING TABLE bronze_orders
# MAGIC COMMENT "Raw orders from cloud storage"
# MAGIC AS SELECT * FROM cloud_files(
# MAGIC     'abfss://raw@storage.dfs.core.windows.net/orders/',
# MAGIC     'json',
# MAGIC     map('cloudFiles.schemaLocation', '/pipelines/schema/orders')
# MAGIC );
# MAGIC
# MAGIC -- Silver with constraint
# MAGIC CREATE OR REFRESH STREAMING TABLE silver_orders
# MAGIC (CONSTRAINT valid_order_id EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW)
# MAGIC AS SELECT
# MAGIC     order_id,
# MAGIC     CAST(order_date AS DATE) AS order_date,
# MAGIC     CAST(amount AS DOUBLE)   AS amount
# MAGIC FROM STREAM(bronze_orders);
# MAGIC
# MAGIC -- Materialized View (gold)
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW gold_monthly_orders AS
# MAGIC SELECT
# MAGIC     DATE_TRUNC('month', order_date) AS month,
# MAGIC     COUNT(*)                        AS order_count,
# MAGIC     SUM(amount)                     AS total_revenue
# MAGIC FROM silver_orders
# MAGIC GROUP BY DATE_TRUNC('month', order_date);
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## LDP Pipeline Modes
# MAGIC
# MAGIC | Mode | Trigger | Use case |
# MAGIC |---|---|---|
# MAGIC | **Triggered** | Run once (like `availableNow`) | Batch / scheduled jobs |
# MAGIC | **Continuous** | Runs indefinitely as a stream | Near-real-time latency |
# MAGIC
# MAGIC Configure in the pipeline settings or `databricks.yml`:
# MAGIC ```yaml
# MAGIC pipelines:
# MAGIC   - name: billing_pipeline
# MAGIC     continuous: false   # triggered mode
# MAGIC     target: main.silver
# MAGIC ```
