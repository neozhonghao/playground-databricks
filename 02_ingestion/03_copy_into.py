# Databricks notebook source

# MAGIC %md
# MAGIC # Section 2 — COPY INTO & JDBC/ODBC
# MAGIC
# MAGIC **Exam objectives covered:**
# MAGIC - Use COPY INTO to incrementally load files from cloud object storage into UC-governed tables
# MAGIC - Use JDBC/ODBC or REST clients to land data into Unity Catalog–governed tables

# COMMAND ----------

# MAGIC %md
# MAGIC ## COPY INTO — Incremental File Loading
# MAGIC
# MAGIC `COPY INTO` reads files from a source location into a Delta table.
# MAGIC It tracks which files have already been loaded (idempotent by default).
# MAGIC
# MAGIC **Syntax:**
# MAGIC ```sql
# MAGIC COPY INTO target_table
# MAGIC FROM source_location
# MAGIC FILEFORMAT = format
# MAGIC [FORMAT_OPTIONS (key = 'value', ...)]
# MAGIC [COPY_OPTIONS (key = 'value', ...)]
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a target table first (COPY INTO requires a pre-existing table)
# MAGIC CREATE TABLE IF NOT EXISTS main.bronze.sales_raw (
# MAGIC     order_id    STRING,
# MAGIC     customer_id STRING,
# MAGIC     product_id  STRING,
# MAGIC     quantity    INT,
# MAGIC     price       DOUBLE,
# MAGIC     order_date  STRING
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Load CSV files incrementally; already-processed files are skipped automatically
# MAGIC COPY INTO main.bronze.sales_raw
# MAGIC FROM 'dbfs:/FileStore/copy_into_demo/sales/'
# MAGIC FILEFORMAT = CSV
# MAGIC FORMAT_OPTIONS (
# MAGIC     'header' = 'true',
# MAGIC     'inferSchema' = 'true',
# MAGIC     'mergeSchema' = 'true'
# MAGIC )
# MAGIC COPY_OPTIONS (
# MAGIC     'mergeSchema' = 'true'
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ## COPY INTO — JSON Example

# COMMAND ----------

# MAGIC %sql
# MAGIC COPY INTO main.bronze.raw_events
# MAGIC FROM 'abfss://container@storageaccount.dfs.core.windows.net/events/'
# MAGIC FILEFORMAT = JSON
# MAGIC FORMAT_OPTIONS ('multiLine' = 'false')
# MAGIC COPY_OPTIONS  ('mergeSchema' = 'true');

# COMMAND ----------

# MAGIC %md
# MAGIC ## COPY INTO vs Auto Loader — Decision Guide
# MAGIC
# MAGIC Use **COPY INTO** when:
# MAGIC - Simple one-shot or scheduled batch loads
# MAGIC - Source has up to a few million files
# MAGIC - You want a pure SQL interface
# MAGIC
# MAGIC Use **Auto Loader** when:
# MAGIC - Continuous streaming ingestion
# MAGIC - Billions of files (file notification mode)
# MAGIC - You need fine-grained schema evolution control

# COMMAND ----------

# MAGIC %md
# MAGIC ## JDBC Ingestion
# MAGIC
# MAGIC Use JDBC to read from relational databases (PostgreSQL, MySQL, SQL Server, etc.)
# MAGIC and write into Unity Catalog Delta tables.

# COMMAND ----------

# Read a table from a PostgreSQL database via JDBC
jdbc_url = "jdbc:postgresql://your-db-host:5432/salesdb"
jdbc_props = {
    "user": dbutils.secrets.get(scope="db-secrets", key="pg-user"),
    "password": dbutils.secrets.get(scope="db-secrets", key="pg-password"),
    "driver": "org.postgresql.Driver",
    "fetchsize": "10000",          # rows per fetch — tune for large tables
}

orders_df = (
    spark.read
        .jdbc(
            url=jdbc_url,
            table="(SELECT * FROM orders WHERE updated_at > '2024-01-01') AS t",
            properties=jdbc_props,
            numPartitions=8,                   # parallelism
            column="order_id",                 # partition column (numeric or date)
            lowerBound=1,
            upperBound=10_000_000,
        )
)

# Write to UC-governed Delta table
(
    orders_df.write
        .format("delta")
        .mode("append")                         # or "overwrite"
        .saveAsTable("main.bronze.orders_jdbc")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Secrets — Never Hard-code Credentials
# MAGIC
# MAGIC ```bash
# MAGIC # CLI: create a secret scope and store the password
# MAGIC databricks secrets create-scope --scope db-secrets
# MAGIC databricks secrets put --scope db-secrets --key pg-password
# MAGIC ```
# MAGIC
# MAGIC ```python
# MAGIC # In notebook: retrieve at runtime
# MAGIC password = dbutils.secrets.get(scope="db-secrets", key="pg-password")
# MAGIC ```
# MAGIC
# MAGIC Secret values are **redacted** in notebook output — they never appear in logs.

# COMMAND ----------

# MAGIC %md
# MAGIC ## REST API Ingestion (Pattern)
# MAGIC
# MAGIC When a connector does not exist, call the API with `requests` and land data as Delta.

# COMMAND ----------

import requests
from pyspark.sql import Row

def fetch_api_page(url: str, token: str, page: int) -> list[dict]:
    resp = requests.get(
        url,
        headers={"Authorization": f"Bearer {token}"},
        params={"page": page, "per_page": 200},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json().get("data", [])

# Example: paginate and collect
# token = dbutils.secrets.get("my-scope", "api-token")
# records = []
# for page in range(1, 11):
#     records.extend(fetch_api_page("https://api.example.com/orders", token, page))
#
# df = spark.createDataFrame([Row(**r) for r in records])
# df.write.format("delta").mode("append").saveAsTable("main.bronze.orders_api")
