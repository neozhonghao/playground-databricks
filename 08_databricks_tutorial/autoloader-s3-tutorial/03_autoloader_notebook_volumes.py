# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Autoloader — Unity Catalog Volumes Edition
# MAGIC
# MAGIC **No S3 IAM config required.** Files are uploaded from your local machine
# MAGIC to a Unity Catalog Volume using the Databricks Files API. Autoloader reads
# MAGIC directly from the Volume path.
# MAGIC
# MAGIC **Workflow:**
# MAGIC 1. Run `01_generate_and_upload_to_volume.py` locally → files land in the Volume
# MAGIC 2. Run this notebook → Autoloader reads those files into a Delta table
# MAGIC 3. Upload more files → re-run to see incremental ingestion

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Setup — Create the Volume
# MAGIC
# MAGIC Run this once. Creates the catalog, schema, volume, and target table namespace.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create catalog (skip if using an existing one)
# MAGIC CREATE CATALOG IF NOT EXISTS main;
# MAGIC
# MAGIC -- Create schema
# MAGIC CREATE SCHEMA IF NOT EXISTS main.autoloader_demo;
# MAGIC
# MAGIC -- Create a managed volume (Databricks handles the storage location)
# MAGIC CREATE VOLUME IF NOT EXISTS main.autoloader_demo.autoloader_demo;

# COMMAND ----------

# Verify the volume path is accessible
display(dbutils.fs.ls("/Volumes/main/autoloader_demo/autoloader_demo/"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

# ─── Edit if you used a different catalog/schema/volume name ───────────────────
VOLUME_SOURCE_PATH  = "/Volumes/main/autoloader_demo/autoloader_demo/orders-raw/"
CHECKPOINT_LOCATION = "/Volumes/main/autoloader_demo/autoloader_demo/_checkpoints/orders/"
SCHEMA_LOCATION     = "/Volumes/main/autoloader_demo/autoloader_demo/_schema/orders/"
TARGET_TABLE        = "main.autoloader_demo.orders_bronze"
# ───────────────────────────────────────────────────────────────────────────────

print(f"Source:     {VOLUME_SOURCE_PATH}")
print(f"Checkpoint: {CHECKPOINT_LOCATION}")
print(f"Table:      {TARGET_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create the `orders-raw` subfolder
# MAGIC
# MAGIC Volumes don't auto-create subdirectories. Create the landing folder once.

# COMMAND ----------

dbutils.fs.mkdirs(VOLUME_SOURCE_PATH)
print(f"Created: {VOLUME_SOURCE_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Upload Files from Your Local Machine
# MAGIC
# MAGIC **Option A — Python script (recommended):**
# MAGIC ```bash
# MAGIC pip install requests faker
# MAGIC export DATABRICKS_HOST=https://your-workspace.azuredatabricks.net
# MAGIC export DATABRICKS_TOKEN=dapiXXXX...
# MAGIC export VOLUME_PATH=/Volumes/main/autoloader_demo/autoloader_demo/orders-raw
# MAGIC python 01_generate_and_upload_to_volume.py
# MAGIC ```
# MAGIC
# MAGIC **Option B — Databricks UI:**
# MAGIC - Unity Catalog → Volumes → `main.autoloader_demo.autoloader_demo`
# MAGIC - Navigate to `orders-raw/` → Upload Files button
# MAGIC
# MAGIC **Option C — Databricks CLI:**
# MAGIC ```bash
# MAGIC databricks fs cp local_file.json dbfs:/Volumes/main/autoloader_demo/autoloader_demo/orders-raw/
# MAGIC ```

# COMMAND ----------

# Confirm files are visible
files = dbutils.fs.ls(VOLUME_SOURCE_PATH)
display(files)
print(f"Found {len(files)} file(s) in landing zone.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Peek at a Raw File

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp

sample = spark.read.format("json").load(VOLUME_SOURCE_PATH)
display(sample.limit(5))
sample.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Run Autoloader
# MAGIC
# MAGIC **How it works:**
# MAGIC - `cloudFiles` format = Autoloader engine
# MAGIC - `cloudFiles.schemaLocation` → Autoloader saves the inferred schema here so it doesn't re-infer on every run
# MAGIC - `checkpointLocation` → tracks which files have already been processed (the key to incremental ingestion)
# MAGIC - `trigger(availableNow=True)` → process all available files, then stop (like a batch job)

# COMMAND ----------

stream = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", SCHEMA_LOCATION)
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")   # handle new columns gracefully
        .option("cloudFiles.inferColumnTypes", "true")               # infer proper types
        .load(VOLUME_SOURCE_PATH)

    # Autoloader metadata — which file did this row come from?
    .withColumn("source_file", col("_metadata.file_path"))
    .withColumn("processing_time", current_timestamp())

    .writeStream
        .format("delta")
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .option("mergeSchema", "true")
        .outputMode("append")
        .trigger(availableNow=True)    # batch mode: process available files then stop
        .toTable(TARGET_TABLE)
)

stream.awaitTermination()
print("Load complete.")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS total_records FROM main.autoloader_demo.orders_bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM main.autoloader_demo.orders_bronze
# MAGIC ORDER BY processing_time DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Upload Another Batch and Re-trigger
# MAGIC
# MAGIC Go back to your terminal and run `01_generate_and_upload_to_volume.py` again.
# MAGIC Then re-run the stream cell. Because of the checkpoint, **only the new file
# MAGIC is processed** — the old files are skipped.

# COMMAND ----------

# Re-run after uploading more files
stream = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", SCHEMA_LOCATION)
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(VOLUME_SOURCE_PATH)
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
# MAGIC -- Row count should have increased
# MAGIC SELECT COUNT(*) AS total_records FROM main.autoloader_demo.orders_bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Full Delta history — one commit per Autoloader trigger
# MAGIC DESCRIBE HISTORY main.autoloader_demo.orders_bronze;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Check Which Files Were Processed
# MAGIC
# MAGIC The `source_file` column we added tells you exactly which Volume file
# MAGIC each row came from — great for auditing and debugging.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   source_file,
# MAGIC   COUNT(*) AS record_count,
# MAGIC   MIN(processing_time) AS processed_at
# MAGIC FROM main.autoloader_demo.orders_bronze
# MAGIC GROUP BY source_file
# MAGIC ORDER BY processed_at;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Schema Evolution
# MAGIC
# MAGIC To test schema evolution:
# MAGIC 1. Add a new field to `generate_order()` in `01_generate_and_upload_to_volume.py`
# MAGIC    (e.g., `"loyalty_points": random.randint(0, 500)`)
# MAGIC 2. Upload a new batch
# MAGIC 3. Re-run the stream cell
# MAGIC
# MAGIC The new column appears in the Delta table automatically — no `ALTER TABLE` needed.
# MAGIC Older rows will have `NULL` for the new column.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Cleanup

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS main.autoloader_demo.orders_bronze;

# COMMAND ----------

dbutils.fs.rm(CHECKPOINT_LOCATION, recurse=True)
dbutils.fs.rm(SCHEMA_LOCATION, recurse=True)
dbutils.fs.rm(VOLUME_SOURCE_PATH, recurse=True)
print("Cleanup complete.")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP VOLUME IF EXISTS main.autoloader_demo.autoloader_demo;
# MAGIC DROP SCHEMA IF EXISTS main.autoloader_demo;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | Concept | Detail |
# MAGIC |---------|--------|
# MAGIC | **Source** | Unity Catalog Volume (`/Volumes/...`) |
# MAGIC | **Format** | `cloudFiles` with `json` |
# MAGIC | **Schema inference** | `cloudFiles.schemaLocation` — stored on first run, reused after |
# MAGIC | **Schema evolution** | `addNewColumns` — new fields auto-added to Delta table |
# MAGIC | **Incremental** | `checkpointLocation` — processed files are never reprocessed |
# MAGIC | **Sink** | Delta table via `.toTable()` |
# MAGIC | **Trigger** | `availableNow=True` (batch) |
# MAGIC | **Audit** | `source_file` column — tracks which file each row came from |

