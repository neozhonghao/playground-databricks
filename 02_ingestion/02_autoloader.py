# Databricks notebook source

# MAGIC %md
# MAGIC # Section 2 — Auto Loader
# MAGIC
# MAGIC **Exam objectives covered:**
# MAGIC - Classify valid Auto Loader sources and use cases
# MAGIC - Demonstrate knowledge of Auto Loader syntax
# MAGIC - Use Auto Loader with schema enforcement and schema evolution (batch modes)
# MAGIC - Land data into Unity Catalog–governed tables

# COMMAND ----------

# MAGIC %md
# MAGIC ## Auto Loader Overview
# MAGIC
# MAGIC Auto Loader (`cloudFiles`) incrementally ingests new files from cloud storage as they arrive.
# MAGIC
# MAGIC | Capability | Details |
# MAGIC |---|---|
# MAGIC | **Sources** | AWS S3, Azure ADLS Gen2, Google Cloud Storage, DBFS |
# MAGIC | **File formats** | JSON, CSV, Parquet, Avro, ORC, text, binaryFile |
# MAGIC | **Discovery modes** | `directory listing` (polling) or `file notification` (event-driven) |
# MAGIC | **Schema inference** | Infers schema from sampled files; stores in a checkpoint location |
# MAGIC | **Schema enforcement** | Rejects or quarantines files that don't match the inferred schema |
# MAGIC | **Schema evolution** | Adds new columns automatically with `cloudFiles.schemaEvolutionMode` |
# MAGIC | **Exactly-once delivery** | Tracks processed files in checkpoint; no duplicates on restart |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Discovery Modes
# MAGIC
# MAGIC | Mode | How it works | When to use |
# MAGIC |---|---|---|
# MAGIC | **directory listing** (default) | Periodically lists the source directory and compares to checkpoint | Simple setup; works everywhere |
# MAGIC | **file notification** | Subscribes to cloud storage events (S3 SNS/SQS, ADLS Event Grid) | High-volume; millions of files |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Basic Auto Loader — JSON → Bronze Delta table

# COMMAND ----------

CATALOG = "main"
SCHEMA  = "bronze"
TABLE   = "raw_events"
SOURCE_PATH  = "dbfs:/FileStore/autoloader_demo/events/"   # replace with your cloud path
CHECKPOINT   = "dbfs:/FileStore/autoloader_demo/checkpoints/raw_events"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

(
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", CHECKPOINT + "/schema")
        # Schema enforcement: reject rows with extra/missing fields
        .option("cloudFiles.schemaEvolutionMode", "rescue")   # or "addNewColumns" / "failOnNewColumns" / "none"
        .load(SOURCE_PATH)
    .writeStream
        .format("delta")
        .option("checkpointLocation", CHECKPOINT)
        .trigger(availableNow=True)           # batch mode: process all available files then stop
        .toTable(f"{CATALOG}.{SCHEMA}.{TABLE}")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema Evolution Modes
# MAGIC
# MAGIC | Mode | Behaviour on new column |
# MAGIC |---|---|
# MAGIC | `addNewColumns` | Evolves target schema; adds the new column |
# MAGIC | `rescue` | Puts unmatched fields into `_rescued_data` JSON column |
# MAGIC | `failOnNewColumns` | Fails the stream with an error |
# MAGIC | `none` | Silently drops unmatched fields |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Trigger Options (Batch vs Continuous)
# MAGIC
# MAGIC | Trigger | Code | Behaviour |
# MAGIC |---|---|---|
# MAGIC | **Once** (deprecated) | `trigger(once=True)` | Processes a micro-batch then stops |
# MAGIC | **Available Now** | `trigger(availableNow=True)` | Processes ALL available files in multiple micro-batches then stops — preferred for scheduled jobs |
# MAGIC | **Fixed interval** | `trigger(processingTime="5 minutes")` | Runs every N seconds/minutes |
# MAGIC | **Continuous** | `trigger(continuous="1 second")` | Low-latency, sub-second |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Auto Loader with CSV — schema hints

# COMMAND ----------

CSV_SOURCE = "dbfs:/FileStore/autoloader_demo/csv_events/"
CSV_CHECKPOINT = "dbfs:/FileStore/autoloader_demo/checkpoints/csv_events"

(
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        # Provide hints so Auto Loader doesn't infer everything as STRING
        .option("cloudFiles.schemaHints", "event_time TIMESTAMP, amount DOUBLE, qty INT")
        .option("cloudFiles.schemaLocation", CSV_CHECKPOINT + "/schema")
        .load(CSV_SOURCE)
    .writeStream
        .format("delta")
        .option("checkpointLocation", CSV_CHECKPOINT)
        .trigger(availableNow=True)
        .toTable(f"{CATALOG}.{SCHEMA}.raw_csv_events")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Auto Loader vs COPY INTO
# MAGIC
# MAGIC | | Auto Loader | COPY INTO |
# MAGIC |---|---|---|
# MAGIC | **API** | Structured Streaming | SQL / Python |
# MAGIC | **Scale** | Billions of files | Up to millions of files |
# MAGIC | **Discovery** | Directory listing or file notification | Directory listing only |
# MAGIC | **Schema inference** | Yes (with evolution) | Yes (limited) |
# MAGIC | **Streaming** | Yes | No (batch only) |
# MAGIC | **Use case** | Continuous / large-scale ingestion | Simple, one-shot or periodic batch loads |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Auto Loader — File Notification Mode (concept)
# MAGIC
# MAGIC ```python
# MAGIC (
# MAGIC     spark.readStream
# MAGIC         .format("cloudFiles")
# MAGIC         .option("cloudFiles.format", "json")
# MAGIC         .option("cloudFiles.useNotifications", "true")        # enables file notification
# MAGIC         .option("cloudFiles.region", "us-east-1")             # AWS region
# MAGIC         .option("cloudFiles.resourceTags.team", "data-eng")   # optional tagging
# MAGIC         .option("cloudFiles.schemaLocation", CHECKPOINT + "/schema")
# MAGIC         .load("s3://my-bucket/events/")
# MAGIC     .writeStream
# MAGIC         ...
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC Databricks auto-creates and manages the SNS/SQS resources when `useNotifications=true`.
