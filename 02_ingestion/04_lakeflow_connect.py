# Databricks notebook source

# MAGIC %md
# MAGIC # Section 2 — Lakeflow Connect
# MAGIC
# MAGIC **Exam objectives covered:**
# MAGIC - Configure Lakeflow Connect to reliably ingest data from enterprise sources
# MAGIC - Classify ingestion methods: Auto Loader vs Lakeflow Connect (standard/managed) vs partner connectors
# MAGIC - Ingest semi-structured and unstructured data (JSON, nested) into UC-governed Delta tables

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lakeflow Connect Overview
# MAGIC
# MAGIC Lakeflow Connect (formerly Databricks Ingest) is the native Databricks ingestion service
# MAGIC that replaces third-party ETL tools for common enterprise sources.
# MAGIC
# MAGIC | Connector Type | Examples | Notes |
# MAGIC |---|---|---|
# MAGIC | **Standard connectors** | Salesforce, ServiceNow, Google Analytics, HubSpot | Managed by Databricks; CDC support |
# MAGIC | **Managed connectors** | Fivetran-powered (500+ sources) | Partner-managed; billed separately |
# MAGIC | **JDBC-based** | PostgreSQL, MySQL, SQL Server, Oracle | Custom notebook + JDBC driver |
# MAGIC | **File-based** | S3, ADLS, GCS | Auto Loader / COPY INTO |
# MAGIC
# MAGIC **All connectors write to Unity Catalog–governed Delta tables.**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingestion Method Selection Guide
# MAGIC
# MAGIC | Requirement | Recommended Method |
# MAGIC |---|---|
# MAGIC | SaaS source (Salesforce, HubSpot...) | Lakeflow Connect standard connector |
# MAGIC | 500+ external source types | Lakeflow Connect managed (Fivetran) connector |
# MAGIC | Cloud files, high volume (billions) | Auto Loader with file notification |
# MAGIC | Cloud files, simple batch | COPY INTO |
# MAGIC | Relational DB, incremental | JDBC with watermark or CDC |
# MAGIC | Custom REST API | Python notebook + `requests` scheduled in Lakeflow Jobs |
# MAGIC | Real-time streaming (Kafka, Event Hubs) | PySpark Structured Streaming |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuring a Lakeflow Connect Pipeline (UI Flow)
# MAGIC
# MAGIC 1. **Data Ingestion** → **Add ingestion pipeline**
# MAGIC 2. Select a **source** (e.g., Salesforce)
# MAGIC 3. Configure **credentials** (stored as Databricks secrets or UC connections)
# MAGIC 4. Choose **destination catalog, schema, and table naming convention**
# MAGIC 5. Select **tables / objects** to sync
# MAGIC 6. Set **sync mode**: Full refresh, Append, or CDC (change data capture)
# MAGIC 7. Attach to a **Lakeflow Job** or let Databricks manage the schedule
# MAGIC
# MAGIC The pipeline creates a managed Delta table per source object under the destination schema.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Handling Semi-Structured / Nested JSON
# MAGIC
# MAGIC A common pattern: ingest JSON into a bronze `STRING` or `VARIANT` column,
# MAGIC then parse in the silver layer.

# COMMAND ----------

from pyspark.sql.functions import col, from_json, explode, schema_of_json
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, ArrayType, TimestampType
)

# Suppose bronze table has a raw_payload STRING column
raw_df = spark.table("main.bronze.raw_events")

# Define the schema of the nested JSON
event_schema = StructType([
    StructField("event_id",   StringType()),
    StructField("event_time", TimestampType()),
    StructField("user",       StructType([
        StructField("id",      StringType()),
        StructField("country", StringType()),
    ])),
    StructField("items", ArrayType(StructType([
        StructField("sku",      StringType()),
        StructField("quantity", DoubleType()),
        StructField("price",    DoubleType()),
    ]))),
])

parsed_df = (
    raw_df
    .select(from_json(col("raw_payload"), event_schema).alias("e"))
    .select(
        col("e.event_id"),
        col("e.event_time"),
        col("e.user.id").alias("user_id"),
        col("e.user.country"),
        explode(col("e.items")).alias("item"),   # one row per item
    )
    .select(
        "event_id", "event_time", "user_id", "country",
        col("item.sku"),
        col("item.quantity"),
        col("item.price"),
    )
)

display(parsed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Auto-Detect Schema from JSON Sample

# COMMAND ----------

sample_json = """{"event_id":"e1","amount":9.99,"tags":["sale","promo"]}"""

# Spark can infer a DDL schema string from a JSON sample
inferred_schema_ddl = schema_of_json(sample_json)
print(inferred_schema_ddl)   # → event_id STRING, amount DOUBLE, tags ARRAY<STRING>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unity Catalog Connections (for Lakeflow Connect)
# MAGIC
# MAGIC UC Connections store credentials centrally so they can be reused across pipelines.
# MAGIC
# MAGIC ```sql
# MAGIC -- Create a connection to PostgreSQL
# MAGIC CREATE CONNECTION my_pg_conn
# MAGIC TYPE postgresql
# MAGIC OPTIONS (
# MAGIC     host 'db.example.com',
# MAGIC     port '5432',
# MAGIC     user SECRET ('my_scope', 'pg_user'),
# MAGIC     password SECRET ('my_scope', 'pg_password')
# MAGIC );
# MAGIC
# MAGIC -- Grant access to a data engineering group
# MAGIC GRANT USE CONNECTION ON CONNECTION my_pg_conn TO `data-engineers`;
# MAGIC ```
