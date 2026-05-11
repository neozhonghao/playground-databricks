# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Autoloader — S3 Tutorial
# MAGIC
# MAGIC **What you'll learn:**
# MAGIC 1. What Autoloader is and why it matters
# MAGIC 2. Read a stream of JSON files from S3 using `cloudFiles`
# MAGIC 3. Use schema inference and schema evolution
# MAGIC 4. Write to a Delta table with checkpointing
# MAGIC 5. Trigger incremental loads and watch the table grow
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - S3 bucket configured (see `02_configure_s3_databricks.md`)
# MAGIC - At least one batch file uploaded by `01_generate_and_upload_to_s3.py`
# MAGIC - Databricks Runtime 11.x+ (Autoloader is built in — no extra installs)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Configuration
# MAGIC
# MAGIC Set your S3 paths and checkpoint location here. Adjust to match your bucket.

# COMMAND ----------

# ─── Edit these ────────────────────────────────────────────────────────────────
S3_SOURCE_PATH      = "s3://your-databricks-autoloader-demo/autoloader-tutorial/orders-raw/"
CHECKPOINT_LOCATION = "s3://your-databricks-autoloader-demo/autoloader-tutorial/_checkpoints/orders/"
SCHEMA_LOCATION     = "s3://your-databricks-autoloader-demo/autoloader-tutorial/_schema/orders/"
TARGET_TABLE        = "autoloader_demo.orders_bronze"  # <catalog>.<schema>.<table>
# ───────────────────────────────────────────────────────────────────────────────

print(f"Source :    {S3_SOURCE_PATH}")
print(f"Checkpoint: {CHECKPOINT_LOCATION}")
print(f"Table:      {TARGET_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. What is Autoloader?
# MAGIC
# MAGIC Autoloader (`cloudFiles` format) is Databricks' native incremental file
# MAGIC ingestion engine. It solves the hard problem of landing-zone ingestion:
# MAGIC
# MAGIC | Naive approach | Autoloader |
# MAGIC |---------------|------------|
# MAGIC | List all files every run — slow at scale | Uses S3 Event Notifications or incremental listing — only detects **new** files |
# MAGIC | You track "what was processed" manually | **Checkpoint** tracks exactly where the stream left off |
# MAGIC | Schema changes break your pipeline | **Schema evolution** handles new columns automatically |
# MAGIC | Full reprocessing on failure | **Exactly-once** delivery via Delta + checkpoints |
# MAGIC
# MAGIC Under the hood it is a **Structured Streaming** source, so you get all the
# MAGIC Spark Streaming guarantees: fault tolerance, scalable parallelism, and
# MAGIC Delta ACID writes.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Explore the Source Directory
# MAGIC
# MAGIC First, confirm files are visible from Databricks.

# COMMAND ----------

files = dbutils.fs.ls(S3_SOURCE_PATH)
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Peek at a Raw File
# MAGIC
# MAGIC Read one file directly (outside of streaming) to understand the shape.

# COMMAND ----------

sample = spark.read.format("json").load(S3_SOURCE_PATH)
display(sample.limit(5))
print(f"Schema inferred from static read:")
sample.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Create the Target Database
# MAGIC
# MAGIC We'll write to a managed Delta table. Adjust the catalog/schema name to
# MAGIC match your Unity Catalog setup, or use `default` for a simple workspace.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- If using Unity Catalog:
# MAGIC -- CREATE CATALOG IF NOT EXISTS autoloader_demo;
# MAGIC -- CREATE SCHEMA IF NOT EXISTS autoloader_demo.autoloader_demo;
# MAGIC
# MAGIC -- For a basic workspace without Unity Catalog:
# MAGIC CREATE DATABASE IF NOT EXISTS autoloader_demo;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Start the Autoloader Stream
# MAGIC
# MAGIC ### Key options explained:
# MAGIC
# MAGIC | Option | Value | Meaning |
# MAGIC |--------|-------|---------|
# MAGIC | `format` | `cloudFiles` | Tells Spark to use Autoloader |
# MAGIC | `cloudFiles.format` | `json` | Format of the source files |
# MAGIC | `cloudFiles.schemaLocation` | S3 path | Where Autoloader stores the inferred schema |
# MAGIC | `cloudFiles.schemaEvolutionMode` | `addNewColumns` | Auto-add new columns; don't fail on schema change |
# MAGIC | `cloudFiles.inferColumnTypes` | `true` | Infer proper types (int, timestamp) instead of all-string |
# MAGIC | `checkpointLocation` | S3 path | Stream state — tracks which files have been processed |
# MAGIC | `.table(...)` | Delta table name | Sink — creates the table if it doesn't exist |

# COMMAND ----------

stream = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", SCHEMA_LOCATION)
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")  # handle new columns gracefully
        .option("cloudFiles.inferColumnTypes", "true")              # proper types, not all-string
        .load(S3_SOURCE_PATH)

        # Autoloader automatically adds metadata columns:
        #   _metadata.file_path   — which S3 file this row came from
        #   _metadata.file_name   — filename only
        #   _metadata.file_size   — size in bytes
        #   _metadata.file_modification_time — last modified timestamp
    .withColumn("source_file", col("_metadata.file_path"))
    .withColumn("processing_time", current_timestamp())

    .writeStream
        .format("delta")
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .option("mergeSchema", "true")        # allows schema evolution on the Delta side
        .outputMode("append")                 # append-only; each new file adds new rows
        .trigger(availableNow=True)           # process all available files, then stop (batch-style)
        # Use .trigger(processingTime="30 seconds") for a continuous stream instead
        .toTable(TARGET_TABLE)
)

stream.awaitTermination()
print("Initial load complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Query the Bronze Table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM autoloader_demo.orders_bronze
# MAGIC ORDER BY ingested_at DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS total_records FROM autoloader_demo.orders_bronze;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Land New Files and Re-trigger
# MAGIC
# MAGIC Go back to your terminal and run:
# MAGIC ```bash
# MAGIC python 01_generate_and_upload_to_s3.py
# MAGIC ```
# MAGIC Then re-run the stream cell above. Because of the checkpoint, Autoloader
# MAGIC will **only process the new file** — not reprocess the old ones.

# COMMAND ----------

# Re-run stream (processes only NEW files since last checkpoint)
stream = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", SCHEMA_LOCATION)
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(S3_SOURCE_PATH)
    .withColumn("source_file", col("_metadata.file_path"))
    .withColumn("processing_time", current_timestamp())
    .writeStream
        .format("delta")
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .option("mergeSchema", "true")
        .outputMode("append")
        .trigger(availableNow=True)
        .toTable(TARGET_TABLE)
)

stream.awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Count should have grown
# MAGIC SELECT COUNT(*) AS total_records FROM autoloader_demo.orders_bronze;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Inspect the Checkpoint
# MAGIC
# MAGIC The checkpoint is what makes Autoloader idempotent. It records:
# MAGIC - Which files have been committed
# MAGIC - The stream offset (position in the source)

# COMMAND ----------

# List checkpoint contents
display(dbutils.fs.ls(CHECKPOINT_LOCATION))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Delta Table History
# MAGIC
# MAGIC Each Autoloader trigger creates a new Delta commit. You get full audit
# MAGIC history and time-travel for free.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY autoloader_demo.orders_bronze;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Schema Evolution Demo
# MAGIC
# MAGIC Autoloader handles new columns automatically when
# MAGIC `cloudFiles.schemaEvolutionMode = addNewColumns`.
# MAGIC
# MAGIC To test: modify `01_generate_and_upload_to_s3.py` to add a new field
# MAGIC (e.g., `"loyalty_points": random.randint(0, 500)`) to `generate_order()`,
# MAGIC upload a new batch, then re-run the stream. The new column will appear in
# MAGIC the Delta table automatically — no manual `ALTER TABLE` needed.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Continuous Streaming Mode (Optional)
# MAGIC
# MAGIC For real-time ingestion, replace `.trigger(availableNow=True)` with
# MAGIC `.trigger(processingTime="30 seconds")`. The stream will stay alive and
# MAGIC check for new files every 30 seconds.
# MAGIC
# MAGIC Stop it when done:

# COMMAND ----------

# continuous_stream = (
#     spark.readStream
#         .format("cloudFiles")
#         .option("cloudFiles.format", "json")
#         .option("cloudFiles.schemaLocation", SCHEMA_LOCATION)
#         .option("cloudFiles.inferColumnTypes", "true")
#         .load(S3_SOURCE_PATH)
#     .withColumn("source_file", col("_metadata.file_path"))
#     .withColumn("processing_time", current_timestamp())
#     .writeStream
#         .format("delta")
#         .option("checkpointLocation", CHECKPOINT_LOCATION)
#         .outputMode("append")
#         .trigger(processingTime="30 seconds")  # poll every 30s
#         .toTable(TARGET_TABLE)
# )
#
# # To stop the stream:
# # continuous_stream.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Cleanup

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Drop the demo table
# MAGIC DROP TABLE IF EXISTS autoloader_demo.orders_bronze;
# MAGIC DROP DATABASE IF EXISTS autoloader_demo;

# COMMAND ----------

# Remove checkpoint and schema locations
dbutils.fs.rm(CHECKPOINT_LOCATION, recurse=True)
dbutils.fs.rm(SCHEMA_LOCATION, recurse=True)
print("Cleanup complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | Concept | What you used |
# MAGIC |---------|--------------|
# MAGIC | **Source format** | `cloudFiles` (Autoloader) |
# MAGIC | **File format** | JSON (supports parquet, csv, avro, orc, text, binaryFile) |
# MAGIC | **Schema inference** | `cloudFiles.schemaLocation` — stored and reused |
# MAGIC | **Schema evolution** | `addNewColumns` — new fields added automatically |
# MAGIC | **Fault tolerance** | `checkpointLocation` — tracks processed files |
# MAGIC | **Sink** | Delta table via `.toTable()` |
# MAGIC | **Trigger modes** | `availableNow` (batch) or `processingTime` (continuous) |
# MAGIC | **Metadata** | `_metadata.file_path`, `_metadata.file_modification_time` |
# MAGIC
# MAGIC **Next steps:**
# MAGIC - Add a Silver layer transformation (dedup, type casting, business rules)
# MAGIC - Build a Gold aggregation (e.g., daily revenue by product)
# MAGIC - Wire up DLT (Delta Live Tables) to orchestrate the full pipeline
