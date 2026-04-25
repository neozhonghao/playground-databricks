# Databricks notebook source

# MAGIC %md
# MAGIC # Section 6 — Unity Catalog: Tables, Roles & Lineage
# MAGIC
# MAGIC **Exam objectives covered:**
# MAGIC - Explain the difference between managed and external tables
# MAGIC - Identify key roles in UC
# MAGIC - Use lineage features in Unity Catalog
# MAGIC - Identify how audit logs are stored

# COMMAND ----------

# MAGIC %md
# MAGIC ## Managed vs External Tables
# MAGIC
# MAGIC | | Managed Table | External Table |
# MAGIC |---|---|---|
# MAGIC | **Data location** | UC-managed storage (metastore default or catalog storage root) | You specify the external location |
# MAGIC | **DROP TABLE** | Deletes both metadata AND data files | Deletes metadata ONLY; data files remain |
# MAGIC | **Use case** | Standard tables; UC manages lifecycle | Bring-your-own storage; data owned outside UC |
# MAGIC | **Requirement** | None | External Location + Storage Credential |

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Managed table (data stored in UC-controlled location)
# MAGIC CREATE TABLE IF NOT EXISTS main.silver.patient_billing_managed (
# MAGIC     billing_id   STRING,
# MAGIC     patient_id   STRING,
# MAGIC     billing_date DATE,
# MAGIC     amount       DOUBLE
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- External table (data lives in your own storage account)
# MAGIC CREATE TABLE IF NOT EXISTS main.silver.patient_billing_external (
# MAGIC     billing_id   STRING,
# MAGIC     patient_id   STRING,
# MAGIC     billing_date DATE,
# MAGIC     amount       DOUBLE
# MAGIC )
# MAGIC LOCATION 'abfss://silver@mystorageacct.dfs.core.windows.net/patient_billing/';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Convert a managed table to external (move data first, then repoint)
# MAGIC -- Step 1: Copy data to the external location
# MAGIC CREATE TABLE main.silver.patient_billing_external_v2
# MAGIC LOCATION 'abfss://silver@mystorageacct.dfs.core.windows.net/patient_billing_v2/'
# MAGIC AS SELECT * FROM main.silver.patient_billing_managed;
# MAGIC
# MAGIC -- Step 2: Drop the managed table (data is now in external location)
# MAGIC DROP TABLE main.silver.patient_billing_managed;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Roles in Unity Catalog
# MAGIC
# MAGIC | Role | Scope | Key Capabilities |
# MAGIC |---|---|---|
# MAGIC | **Metastore Admin** | Metastore | Create catalogs, manage storage credentials, assign workspace admins |
# MAGIC | **Account Admin** | Account | Manage users, groups, workspaces, assign metastore admin |
# MAGIC | **Workspace Admin** | Workspace | Manage cluster policies, assign users to workspace, configure settings |
# MAGIC | **Catalog Owner** | Catalog | Grant/revoke any privilege within the catalog |
# MAGIC | **Schema Owner** | Schema | Grant/revoke privileges on objects within the schema |
# MAGIC | **Table Owner** | Table/View | Grant SELECT, MODIFY on the table |
# MAGIC | **Data Steward** | Multiple objects | Manage data quality, lineage, tagging |
# MAGIC
# MAGIC **Exam tip:** Only the *owner* of an object or a higher-level admin can `GRANT` privileges
# MAGIC on that object to others.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Audit Logs
# MAGIC
# MAGIC Unity Catalog records all data access events in audit logs.
# MAGIC
# MAGIC | Log destination | How to access |
# MAGIC |---|---|
# MAGIC | **Databricks system tables** (recommended) | `system.access.audit` — queryable Delta table |
# MAGIC | **Cloud provider log** (legacy) | AWS CloudTrail, Azure Monitor, GCP Audit Logs |
# MAGIC
# MAGIC The `system` catalog is automatically created by Unity Catalog.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query audit logs for SELECT events on a specific table
# MAGIC SELECT
# MAGIC     event_time,
# MAGIC     user_identity.email AS user,
# MAGIC     action_name,
# MAGIC     request_params.table_full_name
# MAGIC FROM system.access.audit
# MAGIC WHERE action_name = 'selectTable'
# MAGIC   AND request_params.table_full_name = 'main.silver.patient_billing'
# MAGIC   AND event_time > current_timestamp() - INTERVAL 7 DAYS
# MAGIC ORDER BY event_time DESC
# MAGIC LIMIT 100;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Lineage in Unity Catalog
# MAGIC
# MAGIC UC automatically tracks **column-level lineage**: which source columns produced which
# MAGIC target columns across notebooks, jobs, and pipelines.
# MAGIC
# MAGIC **Access lineage:**
# MAGIC - **Catalog Explorer** → select a table → **Lineage** tab → view upstream/downstream
# MAGIC - **REST API:** `GET /api/2.0/lineage-tracking/table-lineage?table_name=...`
# MAGIC - **System tables:**

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Table-level lineage
# MAGIC SELECT * FROM system.access.table_lineage
# MAGIC WHERE target_table_full_name = 'main.silver.patient_billing'
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Column-level lineage
# MAGIC SELECT * FROM system.access.column_lineage
# MAGIC WHERE target_table_full_name = 'main.silver.patient_billing'
# MAGIC   AND target_column_name = 'amount_billed'
# MAGIC LIMIT 20;
