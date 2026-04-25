# Databricks notebook source

# MAGIC %md
# MAGIC # Section 6 — Lakehouse Federation
# MAGIC
# MAGIC **Exam objectives covered:**
# MAGIC - Identify use cases of Lakehouse Federation when connected to external sources
# MAGIC - Differentiate from Lakeflow Connect (ingestion vs federation)

# COMMAND ----------

# MAGIC %md
# MAGIC ## What is Lakehouse Federation?
# MAGIC
# MAGIC Lakehouse Federation lets you **query external databases directly from Databricks**
# MAGIC without ingesting or copying data into Delta tables.
# MAGIC
# MAGIC Unity Catalog creates a **foreign catalog** that maps to the external database.
# MAGIC Users query it using the same `catalog.schema.table` syntax.
# MAGIC
# MAGIC | External Source | Support |
# MAGIC |---|---|
# MAGIC | MySQL | Yes |
# MAGIC | PostgreSQL | Yes |
# MAGIC | SQL Server | Yes |
# MAGIC | Snowflake | Yes |
# MAGIC | BigQuery | Yes |
# MAGIC | Redshift | Yes |
# MAGIC | Azure Synapse | Yes |
# MAGIC | Salesforce Data Cloud | Yes |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Federation vs Lakeflow Connect — When to Use Which
# MAGIC
# MAGIC | | **Lakehouse Federation** | **Lakeflow Connect** |
# MAGIC |---|---|---|
# MAGIC | **Data movement** | None — query in place | Copies data into Delta tables |
# MAGIC | **Latency** | Query-time (external DB latency) | Near-real-time or scheduled |
# MAGIC | **Use case** | Ad-hoc exploration, cross-system JOINs, avoiding duplication | Production pipelines, repeated analytics, offline access |
# MAGIC | **Governance** | UC governs access to the foreign catalog | UC governs the destination Delta tables |
# MAGIC | **Data freshness** | Always current | Depends on ingestion frequency |
# MAGIC | **Performance** | Limited by external DB | Full Photon/Spark performance on Delta |
# MAGIC
# MAGIC **Choose Federation when:**
# MAGIC - Data doesn't need to be in Databricks permanently
# MAGIC - You need to join external data with Databricks tables ad-hoc
# MAGIC - Compliance prevents copying data out of the source system
# MAGIC
# MAGIC **Choose Lakeflow Connect when:**
# MAGIC - Data will be used repeatedly in Databricks pipelines
# MAGIC - You need full Spark/Photon performance
# MAGIC - You want historical data and time travel

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setting Up Lakehouse Federation (Step by Step)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 1: Create a UC Storage Credential (if not already done)
# MAGIC -- (Handled in Account Console UI → External Data → Storage Credentials)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 2: Create a UC Connection to the external database
# MAGIC CREATE CONNECTION IF NOT EXISTS prod_postgres
# MAGIC TYPE POSTGRESQL
# MAGIC OPTIONS (
# MAGIC     host     'prod-db.example.com',
# MAGIC     port     '5432',
# MAGIC     database 'salesdb',
# MAGIC     user     SECRET ('db-secrets', 'pg-user'),
# MAGIC     password SECRET ('db-secrets', 'pg-password')
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 3: Create a Foreign Catalog pointing at the external DB
# MAGIC CREATE FOREIGN CATALOG IF NOT EXISTS prod_postgres_catalog
# MAGIC USING CONNECTION prod_postgres
# MAGIC OPTIONS (database 'salesdb');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 4: Grant access to the foreign catalog
# MAGIC GRANT USE CATALOG ON CATALOG prod_postgres_catalog TO `data-engineers`;
# MAGIC GRANT USE SCHEMA  ON CATALOG prod_postgres_catalog TO `data-engineers`;
# MAGIC GRANT SELECT      ON ALL TABLES IN CATALOG prod_postgres_catalog TO `data-engineers`;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 5: Query the external table as if it were local
# MAGIC SELECT *
# MAGIC FROM prod_postgres_catalog.public.orders
# MAGIC WHERE order_date >= '2024-01-01'
# MAGIC LIMIT 100;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cross-System JOIN (Federation + Delta)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Join external CRM data (via federation) with Databricks billing data
# MAGIC SELECT
# MAGIC     b.patient_id,
# MAGIC     c.full_name,
# MAGIC     c.email,
# MAGIC     SUM(b.amount_billed) AS total_billed
# MAGIC FROM main.silver.patient_billing b
# MAGIC JOIN prod_postgres_catalog.public.crm_patients c
# MAGIC     ON b.patient_id = c.patient_id
# MAGIC GROUP BY b.patient_id, c.full_name, c.email
# MAGIC ORDER BY total_billed DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Points for the Exam
# MAGIC
# MAGIC - Lakehouse Federation uses **Unity Catalog Connections** and **Foreign Catalogs**
# MAGIC - Data stays in the source — no ingestion, no storage cost in Databricks
# MAGIC - Governance (access control, lineage, auditing) still applies via UC
# MAGIC - Federation queries are **pushed down** to the external database where possible
# MAGIC   (predicate pushdown reduces data scanned)
# MAGIC - For heavy analytics workloads, prefer ingesting with Lakeflow Connect to avoid
# MAGIC   repeated query load on the source system
