# Databricks notebook source

# MAGIC %md
# MAGIC # Section 3 — DDL & DML Features
# MAGIC
# MAGIC **Exam objectives covered:**
# MAGIC - Identify DDL (Data Definition Language) / DML features
# MAGIC - CREATE OR REPLACE TABLE, CREATE TABLE IF NOT EXISTS
# MAGIC - INSERT INTO, MERGE INTO, UPDATE, DELETE
# MAGIC - Delta-specific: time travel, OPTIMIZE, VACUUM, CLONE

# COMMAND ----------

# MAGIC %md
# MAGIC ## DDL — Table Creation Patterns
# MAGIC
# MAGIC | Statement | Behaviour |
# MAGIC |---|---|
# MAGIC | `CREATE TABLE` | Fails if the table already exists |
# MAGIC | `CREATE TABLE IF NOT EXISTS` | Silently skips creation if table exists; preserves existing data |
# MAGIC | `CREATE OR REPLACE TABLE` | Drops & recreates; **destroys existing data** |
# MAGIC | `CREATE TABLE AS SELECT` (CTAS) | Creates table and populates from a query |

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Exam Q4 answer: CREATE OR REPLACE ignores whether the table exists
# MAGIC CREATE OR REPLACE TABLE main.default.employee_ratings (
# MAGIC     employeeId STRING,
# MAGIC     startDate  DATE,
# MAGIC     avgRating  FLOAT
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- IF NOT EXISTS: safe to run repeatedly; does NOT reset data
# MAGIC CREATE TABLE IF NOT EXISTS main.default.employee_ratings (
# MAGIC     employeeId STRING,
# MAGIC     startDate  DATE,
# MAGIC     avgRating  FLOAT
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CTAS: create and populate in one statement
# MAGIC CREATE OR REPLACE TABLE main.gold.top_patients AS
# MAGIC SELECT patient_id, SUM(amount_billed) AS lifetime_value
# MAGIC FROM main.silver.patient_billing
# MAGIC GROUP BY patient_id
# MAGIC HAVING lifetime_value > 5000;

# COMMAND ----------

# MAGIC %md
# MAGIC ## DML — Inserting, Updating, Deleting

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Exam Q5 answer: INSERT INTO … VALUES
# MAGIC INSERT INTO main.default.employee_ratings VALUES ('a1', '2009-01-06', 5.5);
# MAGIC INSERT INTO main.default.employee_ratings VALUES ('a2', '2018-11-21', 7.1);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- INSERT INTO from a SELECT
# MAGIC INSERT INTO main.default.employee_ratings
# MAGIC SELECT employeeId, startDate, avgRating
# MAGIC FROM main.bronze.employee_staging
# MAGIC WHERE avgRating IS NOT NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- UPDATE: modify existing rows
# MAGIC UPDATE main.default.employee_ratings
# MAGIC SET avgRating = 9.0
# MAGIC WHERE employeeId = 'a1';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DELETE: remove rows matching a predicate
# MAGIC DELETE FROM main.default.employee_ratings
# MAGIC WHERE avgRating < 1.0;

# COMMAND ----------

# MAGIC %md
# MAGIC ## MERGE INTO — Upsert Pattern

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO main.silver.patient_billing AS target
# MAGIC USING (
# MAGIC     SELECT * FROM main.bronze.patient_billing_staging
# MAGIC ) AS source
# MAGIC ON target.billing_id = source.billing_id
# MAGIC WHEN MATCHED AND source.amount_billed != target.amount_billed
# MAGIC     THEN UPDATE SET target.amount_billed = source.amount_billed,
# MAGIC                     target.silver_load_ts = current_timestamp()
# MAGIC WHEN NOT MATCHED
# MAGIC     THEN INSERT (billing_id, patient_id, department, billing_date,
# MAGIC                  amount_billed, quantity, silver_load_ts)
# MAGIC          VALUES (source.billing_id, source.patient_id, source.department,
# MAGIC                  source.billing_date, source.amount_billed, source.quantity,
# MAGIC                  current_timestamp())
# MAGIC WHEN NOT MATCHED BY SOURCE
# MAGIC     THEN DELETE;   -- remove rows in target that no longer exist in source

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Time Travel

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View table history
# MAGIC DESCRIBE HISTORY main.default.employee_ratings;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query a previous version by version number
# MAGIC SELECT * FROM main.default.employee_ratings VERSION AS OF 0;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query a previous version by timestamp
# MAGIC SELECT * FROM main.default.employee_ratings
# MAGIC TIMESTAMP AS OF '2024-01-15 10:00:00';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Restore a table to a previous version
# MAGIC RESTORE TABLE main.default.employee_ratings TO VERSION AS OF 2;

# COMMAND ----------

# MAGIC %md
# MAGIC ## OPTIMIZE & VACUUM

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Compact small files into larger ones (improves read performance)
# MAGIC OPTIMIZE main.silver.patient_billing;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Z-ORDER: co-locate rows with similar values in the same files
# MAGIC OPTIMIZE main.silver.patient_billing ZORDER BY (department, billing_date);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Remove old files no longer referenced by the transaction log
# MAGIC -- Default retention is 7 days; override with RETAIN n HOURS
# MAGIC VACUUM main.silver.patient_billing RETAIN 168 HOURS;   -- 7 days

# COMMAND ----------

# MAGIC %md
# MAGIC ## Shallow & Deep Clone

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Deep clone: full copy of data + metadata; independent from source
# MAGIC CREATE TABLE main.dev.patient_billing_backup
# MAGIC DEEP CLONE main.silver.patient_billing;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Shallow clone: metadata pointer only; shares files with source (storage-efficient)
# MAGIC -- Changes to shallow clone do NOT affect source (copy-on-write)
# MAGIC CREATE TABLE main.dev.patient_billing_shallow
# MAGIC SHALLOW CLONE main.silver.patient_billing;
